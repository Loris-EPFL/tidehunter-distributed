use super::*;

#[test]
fn bounded_windows_pins_compaction_retirement_and_exact_retries_survive_reopen() {
    let temp = tempdir::TempDir::new("local-storage-lifecycle").unwrap();
    let path = temp.path().join("db");
    let mut config = config();
    config.storage.partitions = vec![0];
    config.storage.batch_limits.max_encoded_bytes = 500;
    config.storage.group_limits.max_batches = 2;
    config.storage.group_limits.max_encoded_bytes = 1016;
    config.storage.fragment_bytes = 1024;
    config.storage.wal_file_bytes = 2048;
    config.storage.max_batches_per_partition = 2;
    config.max_admitted_batches = 2;
    let mut db = LocalDatabase::open(&path, &shape(), config.clone(), metrics()).unwrap();
    let first = db.write_batch(operation(0), vec![put(0, 0)]).unwrap();
    let held = db.checkpoint().unwrap();
    let old_cut = held.cut().map(IndexReadCut::At).unwrap();
    let mut last = first;
    for n in 1..40 {
        last = db.write_batch(operation(n), vec![put(0, n as u8)]).unwrap();
        assert_eq!(last.sequence, n);
        assert!(db.submissions.is_empty());
        assert_eq!(db.publication.retained_batches(), 0);
        assert_eq!(db.completions.retained_batches(), 0);
        assert!(
            db.storage_stats().reference_metadata_bytes
                <= config.storage.max_reference_metadata_bytes
        );
    }
    let latest = IndexReadCut::At(MutationVersion {
        batch: last,
        operation_index: 0,
    });
    db.compact_history(latest.clone()).unwrap();
    assert_eq!(
        db.get_at(KeySpace(0), &0u64.to_be_bytes(), &held).unwrap(),
        Some(vec![0; 128].into())
    );
    assert_eq!(
        db.index.retention_floor(),
        old_cut,
        "live pin must clamp the persisted supported floor"
    );
    let admitted = db.stats().admitted_batches;
    assert_eq!(
        db.write_batch(operation(0), vec![put(0, 0)]).unwrap(),
        first
    );
    assert_eq!(
        db.stats().admitted_batches,
        admitted,
        "paged exact retry must not reexecute"
    );
    drop(held);
    db.compact_history(latest.clone()).unwrap();
    assert!(db.index.validate_cut(old_cut.clone()).is_err());
    db.shutdown().unwrap();
    drop(db);

    let mut db = LocalDatabase::open(&path, &shape(), config.clone(), metrics()).unwrap();
    let old = db.logs[&(1, 0)]
        .lookup(BatchId {
            database: config.storage.database,
            operation: operation(0),
        })
        .unwrap()
        .unwrap();
    assert!(
        !db.logs[&(1, 0)]
            .wal_file_page(None, 256)
            .unwrap()
            .is_empty()
    );
    db.compact_history(latest.clone()).unwrap();
    assert!(
        db.logs[&(1, 0)].read_batch(old).is_err(),
        "sealed obsolete payload should be reclaimed after root adoption"
    );
    assert_eq!(db.logs[&(1, 0)].lookup(old.id()).unwrap(), Some(old));
    assert_eq!(
        db.write_batch(operation(0), vec![put(0, 0)]).unwrap(),
        first
    );
    assert_eq!(db.stats().admitted_batches, 0);
    assert_eq!(
        db.get(KeySpace(0), &0u64.to_be_bytes()).unwrap(),
        Some(vec![39; 128].into())
    );
    db.shutdown().unwrap();
    drop(db);

    let mut db = LocalDatabase::open(&path, &shape(), config, metrics()).unwrap();
    assert_eq!(
        db.write_batch(operation(0), vec![put(0, 0)]).unwrap(),
        first
    );
    assert_eq!(
        db.get(KeySpace(0), &0u64.to_be_bytes()).unwrap(),
        Some(vec![39; 128].into())
    );
    assert!(db.index.validate_cut(old_cut.clone()).is_err());
}

fn placed_operation(db: &LocalDatabase, partition: u32, seed: u64) -> [u8; 16] {
    (seed..seed + 256)
        .map(operation)
        .find(|operation| {
            db.inventory
                .active_inventory()
                .partition_for(BatchId {
                    database: db.config.storage.database,
                    operation: *operation,
                })
                .unwrap()
                == partition
        })
        .unwrap()
}

#[test]
fn authenticated_root_recovery_rejects_below_cut_and_duplicate_suffix_versions() {
    for duplicate_above_cut in [false, true] {
        let temp = tempdir::TempDir::new("local-root-suffix-order").unwrap();
        let path = temp.path().join("db");
        let mut config = config();
        config.storage.partitions = vec![0, 1];
        let mut db = LocalDatabase::open(&path, &shape(), config.clone(), metrics()).unwrap();
        for partition in 0..2 {
            let operation = placed_operation(&db, partition, partition as u64 * 256);
            db.write_batch(operation, vec![put(partition as u64, partition as u8)])
                .unwrap();
        }
        db.flush_index().unwrap();
        for partition in 0..if duplicate_above_cut { 2 } else { 1 } {
            let operation = placed_operation(&db, partition, 1024 + partition as u64 * 256);
            let log = db
                .inventory
                .active_partitions()
                .iter()
                .find(|log| log.config().partition == partition)
                .unwrap();
            let batch = CompleteBatch::new(
                &shape(),
                BatchId {
                    database: config.storage.database,
                    operation,
                },
                BatchVersion {
                    epoch: 1,
                    sequence: if duplicate_above_cut { 2 } else { 1 },
                },
                vec![put(100 + partition as u64, 7)],
                config.storage.batch_limits,
            )
            .unwrap();
            let receipt = log
                .append_complete_batch(RequestAuthority { epoch: 1 }, &batch)
                .unwrap();
            log.persist_through(receipt).unwrap();
        }
        db.shutdown().unwrap();
        drop(db);
        let error = match LocalDatabase::open(&path, &shape(), config, metrics()) {
            Ok(_) => panic!("invalid suffix was admitted after authenticated checkpoint root"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("duplicate or unordered recovered logical version"),
            "checkpoint-specific recovery must validate its bounded suffix: {error}"
        );
    }
}

#[test]
fn corrupted_claimed_root_or_stream_proof_never_falls_back_to_full_replay() {
    for corrupt_crc in [false, true] {
        let temp = tempdir::TempDir::new("local-root-certificate-corrupt").unwrap();
        let path = temp.path().join("db");
        let config = config();
        let mut db = LocalDatabase::open(&path, &shape(), config.clone(), metrics()).unwrap();
        db.write_batch(operation(0), vec![put(0, 3)]).unwrap();
        db.flush_index().unwrap();
        db.shutdown().unwrap();
        drop(db);
        let root_path = path.join("local-index-root-v2");
        if corrupt_crc {
            let mut bytes = std::fs::read(&root_path).unwrap();
            bytes[0] ^= 1;
            std::fs::write(&root_path, bytes).unwrap();
        } else {
            let mut root: checkpoint::LocalRoot =
                super::super::super::storage::read_metadata(&root_path).unwrap();
            root.prefixes[0].digest[0] ^= 1;
            let bytes = super::super::super::storage::encode_metadata(&root).unwrap();
            super::super::super::storage::install_metadata(&path, "local-index-root-v2", &bytes)
                .unwrap();
        }
        assert!(LocalDatabase::open(&path, &shape(), config, metrics()).is_err());
    }
}

#[test]
fn obsolete_checkpoint_root_does_not_trigger_fresh_recovery() {
    let temp = tempdir::TempDir::new("local-root-old-format").unwrap();
    let path = temp.path().join("db");
    let config = config();
    let mut db = LocalDatabase::open(&path, &shape(), config.clone(), metrics()).unwrap();
    db.write_batch(operation(0), vec![put(0, 3)]).unwrap();
    db.flush_index().unwrap();
    db.shutdown().unwrap();
    drop(db);
    std::fs::rename(
        path.join("local-index-root-v2"),
        path.join("local-index-root-v1"),
    )
    .unwrap();
    assert!(LocalDatabase::open(&path, &shape(), config, metrics()).is_err());
}
