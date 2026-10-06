use super::*;
use crate::distributed::codec::Mutation;
use crate::distributed::sequence::LogicalClock;
use crate::key_shape::{KeySpace, KeyType};
use std::os::unix::fs::FileExt;

fn shape() -> KeyShape {
    KeyShape::new_single(4, 1, KeyType::uniform(1))
}

fn config(partitions: Vec<u32>, partition: u32) -> PartitionConfig {
    PartitionConfig {
        inventory: StaticInventory {
            database: [1; 16],
            epoch: 5,
            segment_generation: 27,
            partitions,
        },
        partition,
        batch_limits: BatchLimits {
            max_operations: 100,
            max_encoded_bytes: 500,
        },
        fragment_bytes: 512,
        wal_file_bytes: 1024,
        max_batches: 64,
    }
}

fn id_for(config: &PartitionConfig, number: u64) -> BatchId {
    let mut operation = [0; 16];
    operation[..8].copy_from_slice(&number.to_be_bytes());
    let mut id = BatchId {
        database: config.inventory.database,
        operation,
    };
    for b in 0..=255 {
        id.operation[15] = b;
        if config.inventory.partition_for(id).unwrap() == config.partition {
            return id;
        }
    }
    panic!("test inventory cannot place identity");
}

fn batch(config: &PartitionConfig, n: u64) -> CompleteBatch {
    CompleteBatch::new(
        &shape(),
        id_for(config, n),
        BatchVersion {
            epoch: config.inventory.epoch,
            sequence: n,
        },
        vec![
            Mutation::Put {
                keyspace: KeySpace(0),
                key: b"key1"[..].into(),
                value: vec![n as u8; 64].into(),
            },
            Mutation::Delete {
                keyspace: KeySpace(0),
                key: b"key2"[..].into(),
            },
        ],
        config.batch_limits,
    )
    .unwrap()
}

fn metrics() -> Arc<Metrics> {
    Metrics::new_in_enabled(&prometheus::Registry::new(), false)
}
fn authority(config: &PartitionConfig) -> RequestAuthority {
    RequestAuthority {
        epoch: config.inventory.epoch,
    }
}

#[test]
fn native_wal_values_survive_multiple_files_and_sealed_reopen() {
    let temp = tempdir::TempDir::new("native-partition").unwrap();
    let config = config(vec![3], 3);
    let path = temp.path().join("stream");
    let log = PartitionLog::create(&path, config.clone(), &shape(), metrics()).unwrap();
    let receipts: Vec<_> = (0..12)
        .map(|n| {
            let receipt = log
                .append_complete_batch(authority(&config), &batch(&config, n))
                .unwrap();
            log.persist_through(receipt).unwrap();
            receipt
        })
        .collect();
    assert!(receipts.last().unwrap().address.offset > config.wal_file_bytes);
    // Persist through an older file even if mapper has opened later files.
    log.persist_through(receipts[0]).unwrap();
    for (n, receipt) in receipts.iter().enumerate() {
        assert_eq!(
            log.read_batch(*receipt).unwrap().mutations(),
            batch(&config, n as u64).mutations()
        );
    }
    log.seal_epoch(authority(&config)).unwrap();
    assert!(matches!(
        log.append_complete_batch(authority(&config), &batch(&config, 20)),
        Err(StorageError::Sealed)
    ));
    drop(log);
    let reopened = PartitionLog::recover_sealed(&path, &config, metrics()).unwrap();
    assert_eq!(reopened.recovered_batches(), receipts);
    for receipt in receipts {
        assert_eq!(
            reopened.persist_through(receipt).unwrap().appended(),
            receipt
        );
    }
    assert!(matches!(
        reopened.append_complete_batch(authority(&config), &batch(&config, 20)),
        Err(StorageError::Sealed)
    ));
}

#[test]
fn retries_are_idempotent_and_conflicts_reject_before_authority() {
    let temp = tempdir::TempDir::new("native-retry").unwrap();
    let mut config = config(vec![3], 3);
    config.max_batches = 1;
    let log = PartitionLog::create(
        &temp.path().join("stream"),
        config.clone(),
        &shape(),
        metrics(),
    )
    .unwrap();
    let first = batch(&config, 0);
    let receipt = log
        .append_complete_batch(authority(&config), &first)
        .unwrap();
    assert_eq!(
        log.append_complete_batch(authority(&config), &first)
            .unwrap(),
        receipt
    );
    let conflicting = CompleteBatch::new(
        &shape(),
        first.id(),
        first.version(),
        vec![Mutation::Delete {
            keyspace: KeySpace(0),
            key: b"key1"[..].into(),
        }],
        config.batch_limits,
    )
    .unwrap();
    assert!(matches!(
        log.append_complete_batch(authority(&config), &conflicting),
        Err(StorageError::IdentityConflict)
    ));
    let duplicate_version = CompleteBatch::new(
        &shape(),
        id_for(&config, 4),
        first.version(),
        first.mutations().to_vec(),
        config.batch_limits,
    )
    .unwrap();
    assert!(matches!(
        log.append_complete_batch(authority(&config), &duplicate_version),
        Err(StorageError::VersionConflict)
    ));
    assert!(matches!(
        log.append_complete_batch(authority(&config), &batch(&config, 1)),
        Err(StorageError::Capacity)
    ));
    assert!(matches!(
        log.append_complete_batch(RequestAuthority { epoch: 4 }, &first),
        Err(StorageError::Fenced)
    ));
    assert_eq!(log.recovered_batches(), vec![receipt]);
}

#[test]
fn complete_unacknowledged_batch_recovers_but_torn_batch_is_never_partial() {
    let temp = tempdir::TempDir::new("native-tail").unwrap();
    let config = config(vec![3], 3);
    let path = temp.path().join("stream");
    let log = PartitionLog::create(&path, config.clone(), &shape(), metrics()).unwrap();
    let first = log
        .append_complete_batch(authority(&config), &batch(&config, 0))
        .unwrap();
    log.persist_through(first).unwrap();
    let unacknowledged = log
        .append_complete_batch(authority(&config), &batch(&config, 1))
        .unwrap();
    // Persist but discard the reply: caller cannot infer the result from its
    // missing acknowledgement, while storage can admit the next batch.
    let _lost_reply = log.persist_through(unacknowledged).unwrap();
    let torn = log
        .append_complete_batch(authority(&config), &batch(&config, 2))
        .unwrap();
    drop(log);
    // Simulate a torn suffix after writer exit. This is fault injection, not a
    // hardware power-loss experiment. Prior durable prefix is unchanged.
    let filename = format!("wal_{:016x}", torn.address.offset / config.wal_file_bytes);
    let file = OpenOptions::new()
        .write(true)
        .open(path.join(filename))
        .unwrap();
    file.write_all_at(
        &[0xff],
        torn.address.offset % config.wal_file_bytes + u64::from(torn.address.len) - 1,
    )
    .unwrap();
    file.sync_all().unwrap();
    let recovered = PartitionLog::recover_sealed(&path, &config, metrics()).unwrap();
    assert_eq!(recovered.lookup(first.id()), Some(first));
    assert_eq!(recovered.lookup(unacknowledged.id()), Some(unacknowledged));
    assert_eq!(recovered.lookup(torn.id()), None);
    assert_eq!(
        recovered
            .read_batch(unacknowledged)
            .unwrap()
            .mutations()
            .len(),
        2
    );
    assert!(matches!(
        recovered.append_complete_batch(authority(&config), &batch(&config, 2)),
        Err(StorageError::Sealed)
    ));
}

#[test]
fn recovery_requires_every_partition_and_does_not_confuse_global_versions_with_offsets() {
    let temp = tempdir::TempDir::new("native-inventory").unwrap();
    let c0 = config(vec![0, 1], 0);
    let c1 = config(vec![0, 1], 1);
    let p0 = temp.path().join("zero");
    let p1 = temp.path().join("one");
    let clock = LogicalClock::new_fenced_epoch(5);
    let a = PartitionLog::create(&p0, c0.clone(), &shape(), metrics()).unwrap();
    let b = PartitionLog::create(&p1, c1.clone(), &shape(), metrics()).unwrap();
    let v0 = clock.allocate().unwrap();
    let v1 = clock.allocate().unwrap();
    let first = a
        .append_complete_batch(authority(&c0), &batch(&c0, v0.sequence))
        .unwrap();
    let later = b
        .append_complete_batch(authority(&c1), &batch(&c1, v1.sequence))
        .unwrap();
    assert_eq!(first.address.offset, later.address.offset); // independent physical streams
    assert!(first.version < later.version); // shared logical order
    b.persist_through(later).unwrap();
    drop(a);
    drop(b);
    assert!(recover_inventory(&c0.inventory, &[(p1.clone(), c1.clone())], metrics()).is_err());
    let logs = recover_inventory(&c0.inventory.clone(), &[(p0, c0), (p1, c1)], metrics()).unwrap();
    assert_eq!(logs[0].lookup(first.id()), Some(first));
    assert_eq!(logs[1].lookup(later.id()), Some(later));
}

#[test]
fn foreign_schema_identity_and_address_do_not_cross_partition_boundaries() {
    let temp = tempdir::TempDir::new("native-validation").unwrap();
    let c0 = config(vec![0, 1], 0);
    let c1 = config(vec![0, 1], 1);
    let a = PartitionLog::create(&temp.path().join("a"), c0.clone(), &shape(), metrics()).unwrap();
    let b = PartitionLog::create(&temp.path().join("b"), c1.clone(), &shape(), metrics()).unwrap();
    let receipt = a
        .append_complete_batch(authority(&c0), &batch(&c0, 0))
        .unwrap();
    assert!(b.read_batch(receipt).is_err());
    assert!(b.persist_through(receipt).is_err());
    assert!(matches!(
        b.append_complete_batch(authority(&c1), &batch(&c0, 0)),
        Err(StorageError::WrongPartition)
    ));
    let wrong_shape = KeyShape::new_single(2, 1, KeyType::uniform(1));
    let wrong_batch = CompleteBatch::new(
        &wrong_shape,
        id_for(&c0, 1),
        BatchVersion {
            epoch: 5,
            sequence: 1,
        },
        vec![Mutation::Delete {
            keyspace: KeySpace(0),
            key: b"xx"[..].into(),
        }],
        c0.batch_limits,
    )
    .unwrap();
    assert!(matches!(
        a.append_complete_batch(authority(&c0), &wrong_batch),
        Err(StorageError::Codec(_))
    ));
    assert_eq!(a.recovered_batches().len(), 1);
}

#[test]
fn sealed_history_corruption_missing_files_and_competing_open_fail_closed() {
    let temp = tempdir::TempDir::new("native-fail-closed").unwrap();
    let config = config(vec![3], 3);
    let path = temp.path().join("stream");
    let log = PartitionLog::create(&path, config.clone(), &shape(), metrics()).unwrap();
    assert!(PartitionLog::create(&path, config.clone(), &shape(), metrics()).is_err());
    assert!(PartitionLog::recover_sealed(&path, &config, metrics()).is_err());
    let receipt = log
        .append_complete_batch(authority(&config), &batch(&config, 0))
        .unwrap();
    log.seal_epoch(authority(&config)).unwrap();
    drop(log);
    let file = OpenOptions::new()
        .write(true)
        .open(path.join("wal_0000000000000000"))
        .unwrap();
    file.write_all_at(
        &[0xff],
        receipt.address.offset + u64::from(receipt.address.len) - 1,
    )
    .unwrap();
    file.sync_all().unwrap();
    assert!(PartitionLog::recover_sealed(&path, &config, metrics()).is_err());
    fs::remove_file(path.join("wal_0000000000000000")).unwrap();
    assert!(PartitionLog::recover_sealed(&path, &config, metrics()).is_err());
}

#[test]
fn concurrent_duplicate_requests_share_one_authority_frame() {
    let temp = tempdir::TempDir::new("native-concurrent-retry").unwrap();
    let config = config(vec![3], 3);
    let log = PartitionLog::create(
        &temp.path().join("stream"),
        config.clone(),
        &shape(),
        metrics(),
    )
    .unwrap();
    let batch = batch(&config, 0);
    let receipts = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..8)
            .map(|_| {
                scope.spawn(|| {
                    log.append_complete_batch(authority(&config), &batch)
                        .unwrap()
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert!(receipts.iter().all(|r| *r == receipts[0]));
    assert_eq!(log.recovered_batches().len(), 1);
}
