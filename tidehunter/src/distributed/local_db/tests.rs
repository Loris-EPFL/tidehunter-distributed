use super::*;
use crate::config::Config;
use crate::db::Db;
use crate::distributed::codec::BatchLimits;
use crate::distributed::group::GroupLimits;
use crate::key_shape::KeyType;
use std::time::Duration;

fn shape() -> KeyShape {
    KeyShape::new_single(8, 2, KeyType::uniform(4))
}
fn metrics() -> Arc<Metrics> {
    Metrics::new_in_enabled(&prometheus::Registry::new(), false)
}
fn config() -> LocalDatabaseConfig {
    LocalDatabaseConfig {
        storage: LocalInventoryConfig {
            database: [19; 16],
            partitions: vec![0, 1, 2, 3, 4],
            batch_limits: BatchLimits {
                max_operations: 32,
                max_encoded_bytes: 16 * 1024,
            },
            group_limits: GroupLimits {
                max_batches: 16,
                max_encoded_bytes: 64 * 1024 - 8,
            },
            fragment_bytes: 64 * 1024,
            wal_file_bytes: 256 * 1024,
            max_batches_per_partition: 256,
            max_reference_metadata_bytes: 4 * 1024 * 1024,
            max_epochs: 8,
        },
        worker: WorkerConfig {
            max_pending_requests: 64,
            max_pending_bytes: 1024 * 1024,
            max_group_wait: Duration::from_micros(100),
        },
        max_admitted_batches: 256,
        max_index_bytes: 16 * 1024 * 1024,
    }
}
fn operation(n: u64) -> [u8; 16] {
    let mut b = [0; 16];
    b[..8].copy_from_slice(&n.to_be_bytes());
    b
}
fn put(key: u64, value: u8) -> Mutation {
    Mutation::Put {
        keyspace: KeySpace(0),
        key: key.to_be_bytes().to_vec().into(),
        value: vec![value; 128].into(),
    }
}
fn delete(key: u64) -> Mutation {
    Mutation::Delete {
        keyspace: KeySpace(0),
        key: key.to_be_bytes().to_vec().into(),
    }
}

#[test]
fn serving_history_matches_native_through_checkpoint_unload_and_reopen() {
    let temporary = tempdir::TempDir::new("local-db-native-differential").unwrap();
    let path = temporary.path().join("distributed");
    let native_path = temporary.path().join("native");
    std::fs::create_dir(&native_path).unwrap();
    let native = Db::open(&native_path, shape(), Arc::new(Config::small()), metrics()).unwrap();
    let mut db = LocalDatabase::open(&path, &shape(), config(), metrics()).unwrap();
    assert_eq!(db.backend_id(), BACKEND_ID);
    let empty = db.checkpoint().unwrap();
    for n in 0..48u64 {
        let changes = vec![put(n, 1), delete(n), put(n, 2)];
        db.submit(operation(n), changes.clone()).unwrap();
        let mut batch = native.write_batch();
        for change in changes {
            match change {
                Mutation::Put {
                    keyspace,
                    key,
                    value,
                } => batch.write(keyspace, key, value),
                Mutation::Delete { keyspace, key } => batch.delete(keyspace, key),
            }
        }
        batch.commit().unwrap();
        if n % 8 == 7 {
            db.drain().unwrap();
        }
    }
    let old = db.checkpoint().unwrap();
    for n in 0..48u64 {
        let change = if n % 3 == 0 { delete(n) } else { put(n, 3) };
        db.submit(operation(100 + n), vec![change.clone()]).unwrap();
        match change {
            Mutation::Put {
                keyspace,
                key,
                value,
            } => native.insert(keyspace, key, value).unwrap(),
            Mutation::Delete { keyspace, key } => native.remove(keyspace, key).unwrap(),
        }
        if n % 8 == 7 {
            db.drain().unwrap();
        }
    }
    db.flush_index().unwrap();
    for n in 0..48u64 {
        let key = n.to_be_bytes();
        assert_eq!(
            db.get(KeySpace(0), &key).unwrap(),
            native.get(KeySpace(0), &key).unwrap()
        );
        assert_eq!(
            db.get_at(KeySpace(0), &key, &old).unwrap(),
            Some(vec![2; 128].into())
        );
        assert_eq!(db.get_at(KeySpace(0), &key, &empty).unwrap(), None);
    }
    let before = db.scan(KeySpace(0)).unwrap();
    assert_eq!(before.len(), 32);
    assert!(before.windows(2).all(|w| w[0].0 < w[1].0));
    let epoch = old.cut().unwrap().batch.epoch;
    db.shutdown().unwrap();
    drop(db);
    let mut db = LocalDatabase::open(&path, &shape(), config(), metrics()).unwrap();
    assert_eq!(db.scan(KeySpace(0)).unwrap(), before);
    assert_eq!(
        db.get_at(KeySpace(0), &1u64.to_be_bytes(), &old).unwrap(),
        Some(vec![2; 128].into())
    );
    let retry = db
        .write_batch(operation(0), vec![put(0, 1), delete(0), put(0, 2)])
        .unwrap();
    assert_eq!(retry.epoch, epoch);
    assert_eq!(db.stats().admitted_batches, 0);
    assert!(db.write_batch(operation(0), vec![put(0, 9)]).is_err());
    let next = db.write_batch(operation(1000), vec![put(0, 4)]).unwrap();
    assert!(next.epoch > epoch);
    assert_eq!(next.sequence, 0);
    assert_eq!(
        db.get(KeySpace(0), &0u64.to_be_bytes()).unwrap(),
        Some(vec![4; 128].into())
    );
    db.shutdown().unwrap();
    native.wait_for_background_threads_to_finish();
}

#[test]
fn before_effects_rejections_do_not_consume_versions_or_hide_published_reads() {
    let temp = tempdir::TempDir::new("local-db-admission").unwrap();
    let mut limits = config();
    limits.max_admitted_batches = 1;
    let mut db = LocalDatabase::open(&temp.path().join("db"), &shape(), limits, metrics()).unwrap();
    let bad = Mutation::Put {
        keyspace: KeySpace(0),
        key: vec![1].into(),
        value: vec![2].into(),
    };
    assert!(db.submit(operation(1), vec![bad]).is_err());
    assert_eq!(db.stats().admitted_batches, 0);
    assert_eq!(
        db.submit(operation(2), vec![put(0, 1)]).unwrap().sequence,
        0
    );
    // Exact retry while still outstanding returns the retained version.
    assert_eq!(
        db.submit(operation(2), vec![put(0, 1)]).unwrap().sequence,
        0
    );
    assert!(db.submit(operation(2), vec![put(0, 2)]).is_err());
    assert!(db.submit(operation(3), vec![put(1, 3)]).is_err());
    db.drain().unwrap();
    assert_eq!(
        db.get(KeySpace(0), &0u64.to_be_bytes()).unwrap(),
        Some(vec![1; 128].into())
    );
    assert_eq!(db.stats().published_batches, 1);
}

#[test]
fn durability_failure_blocks_all_serving_and_reopen_recovers_original_identity() {
    let temp = tempdir::TempDir::new("local-db-unknown").unwrap();
    let path = temp.path().join("db");
    let mut db = LocalDatabase::open(&path, &shape(), config(), metrics()).unwrap();
    let id = BatchId {
        database: config().storage.database,
        operation: operation(42),
    };
    let partition = db.inventory.active_inventory().partition_for(id).unwrap();
    let log = db
        .inventory
        .active_partitions()
        .iter()
        .find(|l| l.config().partition == partition)
        .unwrap();
    log.fail_next_persist();
    let original = db.submit(id.operation, vec![put(5, 7)]).unwrap();
    assert!(db.drain().is_err());
    assert!(db.get(KeySpace(0), &5u64.to_be_bytes()).is_err());
    assert!(db.checkpoint().is_err());
    assert!(db.submit(operation(43), vec![put(5, 8)]).is_err());
    drop(db);
    // Injected failure left a complete frame. Offline local fencing/recovery
    // persists eligible bytes and must include this unacknowledged mutation.
    let mut db = LocalDatabase::open(&path, &shape(), config(), metrics()).unwrap();
    assert_eq!(
        db.get(KeySpace(0), &5u64.to_be_bytes()).unwrap(),
        Some(vec![7; 128].into())
    );
    assert_eq!(
        db.write_batch(id.operation, vec![put(5, 7)]).unwrap(),
        original
    );
    assert_eq!(db.stats().admitted_batches, 0);
}

#[test]
fn checkpoint_cannot_cross_database_and_empty_checkpoint_stays_empty() {
    let temp = tempdir::TempDir::new("local-db-checkpoint-domain").unwrap();
    let mut first =
        LocalDatabase::open(&temp.path().join("a"), &shape(), config(), metrics()).unwrap();
    let empty = first.checkpoint().unwrap();
    first.write_batch(operation(0), vec![put(1, 3)]).unwrap();
    assert!(
        first
            .get_at(KeySpace(0), &1u64.to_be_bytes(), &empty)
            .unwrap()
            .is_none()
    );
    let mut other = config();
    other.storage.database = [20; 16];
    let mut second =
        LocalDatabase::open(&temp.path().join("b"), &shape(), other, metrics()).unwrap();
    assert!(
        second
            .get_at(KeySpace(0), &1u64.to_be_bytes(), &empty)
            .is_err()
    );
}

#[test]
fn reference_metadata_is_reserved_before_sequence_allocation() {
    let temp = tempdir::TempDir::new("local-db-reference-credits").unwrap();
    let mut limits = config();
    limits.storage.partitions = vec![0];
    limits.storage.max_batches_per_partition = 8;
    let mut probe = LocalDatabase::open(
        &temp.path().join("probe"),
        &shape(),
        limits.clone(),
        metrics(),
    )
    .unwrap();
    probe.write_batch(operation(0), vec![put(1, 1)]).unwrap();
    limits.storage.max_reference_metadata_bytes =
        super::super::reference_directory::ReferenceDirectory::metadata_bytes(1, 1).unwrap();
    drop(probe);
    let mut db =
        LocalDatabase::open(&temp.path().join("limited"), &shape(), limits, metrics()).unwrap();
    assert_eq!(
        db.submit(operation(0), vec![put(1, 1)]).unwrap().sequence,
        0
    );
    // Reject a whole batch that cannot fit one fresh descriptor window, not a
    // valid sequence of separately spillable groups in the caller's window.
    assert!(db.submit(operation(1), vec![put(2, 2), put(3, 3)]).is_err());
    assert_eq!(db.stats().admitted_batches, 1);
    db.drain().unwrap();
    assert_eq!(db.stats().published_batches, 1);
    assert_eq!(
        db.get(KeySpace(0), &1u64.to_be_bytes()).unwrap(),
        Some(vec![1; 128].into())
    );
    assert_eq!(db.storage_stats().registered_records, 1);
}

mod lifecycle;
