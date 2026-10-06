//! Explicit fault-cut tests for the first storage substrate. These mutate files
//! after stopping writers; they are not a physical power-loss experiment.

use super::*;
use crate::distributed::codec::Mutation;
use crate::key_shape::{KeySpace, KeyType};
use std::os::unix::fs::FileExt;

fn shape() -> KeyShape {
    KeyShape::new_single(4, 1, KeyType::uniform(1))
}

fn metrics() -> Arc<Metrics> {
    Metrics::new_in_enabled(&prometheus::Registry::new(), false)
}

fn config(partitions: Vec<u32>, partition: u32) -> PartitionConfig {
    PartitionConfig {
        inventory: StaticInventory {
            database: [11; 16],
            epoch: 17,
            segment_generation: 41,
            partitions,
        },
        partition,
        batch_limits: BatchLimits {
            max_operations: 10,
            max_encoded_bytes: 500,
        },
        fragment_bytes: 512,
        wal_file_bytes: 1024,
        max_batches: 64,
    }
}

fn authority(config: &PartitionConfig) -> RequestAuthority {
    RequestAuthority {
        epoch: config.inventory.epoch,
    }
}

fn batch(config: &PartitionConfig, operation: u64, sequence: u64) -> CompleteBatch {
    let mut id = BatchId {
        database: config.inventory.database,
        operation: [0; 16],
    };
    id.operation[..8].copy_from_slice(&operation.to_be_bytes());
    for suffix in 0..=255 {
        id.operation[15] = suffix;
        if config.inventory.partition_for(id).unwrap() == config.partition {
            return CompleteBatch::new(
                &shape(),
                id,
                BatchVersion {
                    epoch: config.inventory.epoch,
                    sequence,
                },
                vec![
                    Mutation::Put {
                        keyspace: KeySpace(0),
                        key: b"key1"[..].into(),
                        value: vec![operation as u8; 64].into(),
                    },
                    Mutation::Delete {
                        keyspace: KeySpace(0),
                        key: b"key2"[..].into(),
                    },
                ],
                config.batch_limits,
            )
            .unwrap();
        }
    }
    panic!("test identity could not be placed");
}

fn wal_file(path: &Path, config: &PartitionConfig, offset: u64) -> File {
    OpenOptions::new()
        .write(true)
        .open(path.join(format!("wal_{:016x}", offset / config.wal_file_bytes)))
        .unwrap()
}

#[test]
fn complete_next_fragment_survives_lost_skip_marker() {
    let temp = tempdir::TempDir::new("sharded-lost-skip").unwrap();
    let config = config(vec![3], 3);
    let path = temp.path().join("stream");
    let log = PartitionLog::create(&path, config.clone(), &shape(), metrics()).unwrap();
    let first = log
        .append_complete_batch(authority(&config), &batch(&config, 0, 0))
        .unwrap();
    log.persist_through(first).unwrap();
    let second = log
        .append_complete_batch(authority(&config), &batch(&config, 1, 1))
        .unwrap();
    log.persist_through(second).unwrap();
    let complete = log
        .append_complete_batch(authority(&config), &batch(&config, 2, 2))
        .unwrap();
    assert_eq!(complete.address().offset, config.fragment_bytes);
    let old_tail = (second.address().offset + u64::from(second.address().len)).div_ceil(8) * 8;
    assert!(old_tail + 8 < config.fragment_bytes);
    drop(log);

    // The next-fragment frame survives, but the allocator's skip marker does
    // not. CRC failure in the old fragment must not hide eligible authority in
    // the new fragment. The prior two complete records remain byte-for-byte.
    let file = wal_file(&path, &config, old_tail);
    file.write_all_at(&[0; 8], old_tail % config.wal_file_bytes)
        .unwrap();
    file.sync_all().unwrap();
    let recovered = PartitionLog::recover_sealed(&path, &config, metrics()).unwrap();
    assert_eq!(recovered.recovered_batches(), vec![first, second, complete]);
    assert_eq!(
        recovered.read_batch(complete).unwrap().mutations(),
        batch(&config, 2, 2).mutations()
    );
    drop(recovered);
    // A second recovery agrees with the sealed result rather than depending on
    // a transient in-memory record list from the first pass.
    let again = PartitionLog::recover_sealed(&path, &config, metrics()).unwrap();
    assert_eq!(again.recovered_batches(), vec![first, second, complete]);
}

#[test]
fn truncated_final_frame_never_replays_a_subset_of_its_mutations() {
    let temp = tempdir::TempDir::new("sharded-truncated-tail").unwrap();
    let config = config(vec![3], 3);
    let path = temp.path().join("stream");
    let log = PartitionLog::create(&path, config.clone(), &shape(), metrics()).unwrap();
    let prior = log
        .append_complete_batch(authority(&config), &batch(&config, 0, 0))
        .unwrap();
    log.persist_through(prior).unwrap();
    let torn = log
        .append_complete_batch(authority(&config), &batch(&config, 1, 1))
        .unwrap();
    drop(log);
    let file = wal_file(&path, &config, torn.address().offset);
    let truncated_end =
        torn.address().offset % config.wal_file_bytes + u64::from(torn.address().len) - 5;
    file.set_len(truncated_end).unwrap();
    file.sync_all().unwrap();
    let recovered = PartitionLog::recover_sealed(&path, &config, metrics()).unwrap();
    assert_eq!(recovered.recovered_batches(), vec![prior]);
    assert_eq!(recovered.lookup(torn.id()), None);
    assert_eq!(recovered.read_batch(prior).unwrap().mutations().len(), 2);
}

#[test]
fn missing_durability_evidence_blocks_the_next_authoritative_batch() {
    let temp = tempdir::TempDir::new("sharded-persist-admission").unwrap();
    let config = config(vec![3], 3);
    let path = temp.path().join("stream");
    let log = PartitionLog::create(&path, config.clone(), &shape(), metrics()).unwrap();
    let original = batch(&config, 0, 0);
    let first = log
        .append_complete_batch(authority(&config), &original)
        .unwrap();
    let later = batch(&config, 1, 1);
    assert!(matches!(
        log.append_complete_batch(authority(&config), &later),
        Err(StorageError::NeedsPersistence)
    ));
    assert_eq!(log.lookup(later.id()), None);
    assert_eq!(
        log.append_complete_batch(authority(&config), &original)
            .unwrap(),
        first
    );
    log.persist_through(first).unwrap();
    let second = log
        .append_complete_batch(authority(&config), &later)
        .unwrap();
    assert_eq!(log.recovered_batches(), vec![first, second]);
}

#[test]
fn inventory_recovery_rejects_a_logical_version_reused_on_another_partition() {
    let temp = tempdir::TempDir::new("sharded-global-version").unwrap();
    let left_config = config(vec![3, 5], 3);
    let right_config = config(vec![3, 5], 5);
    let left_path = temp.path().join("left");
    let right_path = temp.path().join("right");
    let left = PartitionLog::create(&left_path, left_config.clone(), &shape(), metrics()).unwrap();
    let right =
        PartitionLog::create(&right_path, right_config.clone(), &shape(), metrics()).unwrap();
    let l = left
        .append_complete_batch(authority(&left_config), &batch(&left_config, 0, 7))
        .unwrap();
    let r = right
        .append_complete_batch(authority(&right_config), &batch(&right_config, 1, 7))
        .unwrap();
    left.persist_through(l).unwrap();
    right.persist_through(r).unwrap();
    drop(left);
    drop(right);
    assert!(matches!(
        recover_inventory(
            &left_config.inventory,
            &[(left_path, left_config.clone()), (right_path, right_config),],
            metrics()
        ),
        Err(StorageError::VersionConflict)
    ));
}
