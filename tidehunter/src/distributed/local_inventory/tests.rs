use super::*;
use crate::distributed::codec::{CompleteBatch, Mutation};
use crate::distributed::types::{BatchId, BatchVersion, RequestAuthority};
use crate::key_shape::{KeySpace, KeyType};
use std::process::Command;

fn shape() -> KeyShape {
    KeyShape::new_single(4, 1, KeyType::uniform(1))
}

fn config() -> LocalInventoryConfig {
    LocalInventoryConfig {
        database: [42; 16],
        partitions: vec![2, 5],
        batch_limits: BatchLimits {
            max_operations: 8,
            max_encoded_bytes: 2048,
        },
        group_limits: GroupLimits {
            max_batches: 8,
            max_encoded_bytes: 4088,
        },
        fragment_bytes: 4096,
        wal_file_bytes: 8192,
        max_batches_per_partition: 32,
        max_reference_metadata_bytes: 1024 * 1024,
        max_epochs: 8,
    }
}

fn metrics() -> Arc<Metrics> {
    Metrics::new_in_enabled(&prometheus::Registry::new(), false)
}

fn make_batch(owner: &LocalInventory, sequence: u64, partition: u32) -> CompleteBatch {
    let inventory = owner.active_inventory();
    let id = (sequence * 1024..)
        .map(|number| {
            let mut operation = [0; 16];
            operation[..8].copy_from_slice(&number.to_be_bytes());
            operation[8..].copy_from_slice(&inventory.epoch.to_be_bytes());
            BatchId {
                database: inventory.database,
                operation,
            }
        })
        .find(|id| inventory.partition_for(*id).unwrap() == partition)
        .unwrap();
    CompleteBatch::new(
        owner.shape(),
        id,
        BatchVersion {
            epoch: inventory.epoch,
            sequence,
        },
        vec![Mutation::Put {
            keyspace: KeySpace(0),
            key: b"key1"[..].into(),
            value: vec![sequence as u8; 32].into(),
        }],
        owner.config().batch_limits,
    )
    .unwrap()
}

fn append(owner: &LocalInventory, sequence: u64, partition: u32, persist: bool) -> CompleteBatch {
    let batch = make_batch(owner, sequence, partition);
    let log = owner
        .active_partitions()
        .iter()
        .find(|log| log.config().partition == partition)
        .unwrap();
    let receipt = log
        .append_complete_batch(
            RequestAuthority {
                epoch: owner.active_inventory().epoch,
            },
            &batch,
        )
        .unwrap();
    if persist {
        log.persist_through(receipt).unwrap();
    }
    batch
}

fn open_error(path: &Path, config: &LocalInventoryConfig) -> LocalInventoryError {
    match LocalInventory::open(path, config, &shape(), metrics()) {
        Ok(_) => panic!("unexpectedly opened an invalid/owned inventory"),
        Err(error) => error,
    }
}

#[test]
fn reopening_recovers_all_streams_and_never_reuses_old_epoch() {
    let temp = tempdir::TempDir::new("local-inventory-reopen").unwrap();
    let path = temp.path().join("db");
    let owner = LocalInventory::create(&path, config(), &shape(), metrics()).unwrap();
    assert_eq!(owner.active_inventory().epoch, 1);
    let first = append(&owner, 0, 2, true);
    let future = append(&owner, 2, 5, false);
    drop(owner);
    let owner = LocalInventory::open(&path, &config(), &shape(), metrics()).unwrap();
    assert_eq!(owner.active_inventory().epoch, 2);
    assert_eq!(owner.active_inventory().segment_generation, 2);
    assert_eq!(owner.recovered_partitions().len(), 2);
    let mut recovered = Vec::new();
    for log in owner.recovered_partitions() {
        for receipt in log.receipt_page(0, 256).unwrap() {
            log.persist_through(receipt).unwrap();
            recovered.push(log.read_batch(receipt).unwrap());
        }
        assert!(matches!(
            log.append_complete_batch(RequestAuthority { epoch: 1 }, &first),
            Err(StorageError::Sealed)
        ));
    }
    recovered.sort_by_key(CompleteBatch::version);
    assert_eq!(recovered[0].encoded(), first.encoded());
    assert_eq!(recovered[1].encoded(), future.encoded());
    append(&owner, 0, 2, true);
    drop(owner);
    let owner = LocalInventory::open(&path, &config(), &shape(), metrics()).unwrap();
    assert_eq!(owner.active_inventory().epoch, 3);
    assert_eq!(owner.recovered_partitions().len(), 4);
    assert_eq!(
        owner
            .recovered_partitions()
            .iter()
            .map(|log| log.receipt_count())
            .sum::<usize>(),
        3
    );
}

#[test]
fn root_lock_outlives_active_and_historical_partition_handles() {
    let temp = tempdir::TempDir::new("local-inventory-lifetime").unwrap();
    let path = temp.path().join("db");
    let owner = LocalInventory::create(&path, config(), &shape(), metrics()).unwrap();
    let active_reader = owner.active_partitions()[0].clone();
    drop(owner);
    assert!(
        matches!(open_error(&path, &config()), LocalInventoryError::Io(error) if error.kind() == io::ErrorKind::AlreadyExists)
    );
    drop(active_reader);
    let owner = LocalInventory::open(&path, &config(), &shape(), metrics()).unwrap();
    let historical_reader = owner.recovered_partitions()[0].clone();
    drop(owner);
    assert!(
        matches!(open_error(&path, &config()), LocalInventoryError::Io(error) if error.kind() == io::ErrorKind::AlreadyExists)
    );
    drop(historical_reader);
    let owner = LocalInventory::open(&path, &config(), &shape(), metrics()).unwrap();
    assert_eq!(owner.active_inventory().epoch, 3);
}

#[test]
fn interrupted_preparation_is_abandoned_without_reuse_or_deletion() {
    let temp = tempdir::TempDir::new("local-inventory-preparing").unwrap();
    let path = temp.path().join("db");
    let owner = LocalInventory::create(&path, config(), &shape(), metrics()).unwrap();
    append(&owner, 0, 2, true);
    drop(owner);
    // Crash cut after reservation and one registration, before Active metadata.
    let lock = DbLock::acquire(&path, Duration::ZERO).unwrap();
    let mut manifest: Manifest = read_metadata(&path.join(MANIFEST)).unwrap();
    manifest.epochs.push(EpochRecord {
        epoch: 2,
        state: EpochState::Preparing,
    });
    persist_manifest(&path, &manifest).unwrap();
    fs::create_dir(epoch_path(&path, 2)).unwrap();
    File::open(&path).unwrap().sync_all().unwrap();
    let partial = PartitionLog::create(
        &partition_path(&path, 2, 2),
        config().partition_config(2, 2),
        &shape(),
        metrics(),
    )
    .unwrap();
    drop(partial);
    drop(lock);
    let owner = LocalInventory::open(&path, &config(), &shape(), metrics()).unwrap();
    assert_eq!(owner.active_inventory().epoch, 3);
    assert_eq!(owner.recovered_partitions().len(), 2);
    assert_eq!(owner.manifest.epochs[1].state, EpochState::Abandoned);
    assert!(partition_path(&path, 2, 2).is_dir());
    assert!(!partition_path(&path, 2, 5).exists());
    assert_eq!(
        owner
            .recovered_partitions()
            .iter()
            .map(|log| log.receipt_count())
            .sum::<usize>(),
        1
    );
}

#[test]
fn missing_active_partition_blocks_recovery_and_epoch_activation() {
    let temp = tempdir::TempDir::new("local-inventory-missing").unwrap();
    let path = temp.path().join("db");
    let owner = LocalInventory::create(&path, config(), &shape(), metrics()).unwrap();
    append(&owner, 0, 2, true);
    drop(owner);
    let missing = partition_path(&path, 1, 5);
    let saved = temp.path().join("temporarily-unavailable-stream");
    fs::rename(&missing, &saved).unwrap();
    assert!(
        matches!(open_error(&path, &config()), LocalInventoryError::Io(error) if error.kind() == io::ErrorKind::NotFound)
    );
    let manifest: Manifest = read_metadata(&path.join(MANIFEST)).unwrap();
    assert_eq!(manifest.epochs.len(), 1);
    assert!(!epoch_path(&path, 2).exists());
    fs::rename(saved, missing).unwrap();
    let owner = LocalInventory::open(&path, &config(), &shape(), metrics()).unwrap();
    assert_eq!(owner.active_inventory().epoch, 2);
}

#[test]
fn conflicting_global_version_fails_before_new_authority() {
    let temp = tempdir::TempDir::new("local-inventory-version").unwrap();
    let path = temp.path().join("db");
    let owner = LocalInventory::create(&path, config(), &shape(), metrics()).unwrap();
    append(&owner, 0, 2, true);
    append(&owner, 0, 5, true);
    drop(owner);
    assert!(matches!(
        open_error(&path, &config()),
        LocalInventoryError::Storage(StorageError::VersionConflict)
    ));
    assert!(!epoch_path(&path, 2).exists());
}

#[test]
fn schema_and_config_must_match_and_missing_manifest_never_resets_data() {
    let temp = tempdir::TempDir::new("local-inventory-config").unwrap();
    let path = temp.path().join("db");
    let owner = LocalInventory::create(&path, config(), &shape(), metrics()).unwrap();
    drop(owner);
    let mut changed = config();
    changed.partitions.push(9);
    assert!(matches!(
        open_error(&path, &changed),
        LocalInventoryError::Invalid(_)
    ));
    let wrong_shape = KeyShape::new_single(5, 1, KeyType::uniform(1));
    assert!(matches!(
        LocalInventory::open(&path, &config(), &wrong_shape, metrics()),
        Err(LocalInventoryError::Invalid(_))
    ));
    fs::remove_file(path.join(MANIFEST)).unwrap();
    assert!(matches!(
        LocalInventory::open_or_create(&path, config(), &shape(), metrics()),
        Err(LocalInventoryError::Invalid(_))
    ));
    assert!(epoch_path(&path, 1).is_dir());
    assert!(!path.join(MANIFEST).exists());
}

#[test]
fn metadata_corruption_and_unregistered_epochs_fail_closed() {
    let temp = tempdir::TempDir::new("local-inventory-metadata").unwrap();
    let path = temp.path().join("db");
    drop(LocalInventory::create(&path, config(), &shape(), metrics()).unwrap());
    let manifest_bytes = fs::read(path.join(MANIFEST)).unwrap();
    fs::write(
        path.join(MANIFEST),
        &manifest_bytes[..manifest_bytes.len() - 1],
    )
    .unwrap();
    assert!(matches!(
        open_error(&path, &config()),
        LocalInventoryError::Storage(_)
    ));
    fs::write(path.join(MANIFEST), &manifest_bytes).unwrap();
    fs::create_dir(epoch_path(&path, 99)).unwrap();
    assert!(matches!(
        open_error(&path, &config()),
        LocalInventoryError::Invalid(_)
    ));
    assert!(!epoch_path(&path, 2).exists());
}

#[test]
fn unexpected_partition_streams_are_not_silently_omitted() {
    let temp = tempdir::TempDir::new("local-inventory-extra-partition").unwrap();
    let path = temp.path().join("db");
    drop(LocalInventory::create(&path, config(), &shape(), metrics()).unwrap());
    let unexpected = partition_path(&path, 1, 99);
    fs::create_dir(&unexpected).unwrap();
    assert!(matches!(
        open_error(&path, &config()),
        LocalInventoryError::Invalid(_)
    ));
    assert!(!epoch_path(&path, 2).exists());
    fs::remove_dir(unexpected).unwrap();
    // Derived index images live at the root and do not claim WAL membership.
    fs::write(path.join("index-1-1.snapshot"), b"derived index").unwrap();
    let owner = LocalInventory::open(&path, &config(), &shape(), metrics()).unwrap();
    assert_eq!(owner.active_inventory().epoch, 2);
}

#[test]
fn bootstrap_empty_existing_root_and_enforce_retained_epoch_capacity() {
    let temp = tempdir::TempDir::new("local-inventory-empty").unwrap();
    let path = temp.path().join("db");
    fs::create_dir(&path).unwrap();
    fs::write(
        path.join(MANIFEST_TEMPORARY),
        b"interrupted uncommitted metadata",
    )
    .unwrap();
    let mut config = config();
    config.max_epochs = 2;
    drop(LocalInventory::open_or_create(&path, config.clone(), &shape(), metrics()).unwrap());
    let owner = LocalInventory::open(&path, &config, &shape(), metrics()).unwrap();
    assert_eq!(owner.active_inventory().epoch, 2);
    drop(owner);
    assert!(matches!(
        open_error(&path, &config),
        LocalInventoryError::EpochCapacity
    ));
    assert!(!epoch_path(&path, 3).exists());
}

// Separate process exits without Drop/seal/worker joins, exercising automatic OS
// file-lock release and recovery of an unsealed write-eligible inventory. This
// is an application-crash test, not an SSD power-loss emulation.
#[test]
fn _subprocess_inventory_crash_writer() {
    let Ok(path) = std::env::var("TIDEHUNTER_LOCAL_INVENTORY_CRASH_PATH") else {
        return;
    };
    let owner = LocalInventory::create(Path::new(&path), config(), &shape(), metrics()).unwrap();
    append(&owner, 0, 2, true);
    append(&owner, 2, 5, false);
    std::process::exit(77);
}

#[test]
fn process_crash_recovers_unsealed_complete_batches_from_every_stream() {
    let temp = tempdir::TempDir::new("local-inventory-process-crash").unwrap();
    let path = temp.path().join("db");
    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "distributed::local_inventory::tests::_subprocess_inventory_crash_writer",
            "--nocapture",
        ])
        .env("TIDEHUNTER_LOCAL_INVENTORY_CRASH_PATH", &path)
        .output()
        .unwrap();
    assert_eq!(
        output.status.code(),
        Some(77),
        "child output: {} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let owner = LocalInventory::open(&path, &config(), &shape(), metrics()).unwrap();
    assert_eq!(owner.active_inventory().epoch, 2);
    let mut versions = Vec::new();
    for log in owner.recovered_partitions() {
        for receipt in log.receipt_page(0, 256).unwrap() {
            log.persist_through(receipt).unwrap();
            assert_eq!(
                log.read_batch(receipt).unwrap().version(),
                receipt.version()
            );
            versions.push(receipt.version());
        }
    }
    versions.sort();
    assert_eq!(
        versions,
        vec![
            BatchVersion {
                epoch: 1,
                sequence: 0
            },
            BatchVersion {
                epoch: 1,
                sequence: 2
            }
        ]
    );
}
