//! Exactness of the owner's identity/version conflict lookup, which uses a
//! hash index over pending entries and a never-forgetting filter over history.
use super::*;

fn batch_with(config: &PartitionConfig, id_number: u64, sequence: u64, fill: u8) -> CompleteBatch {
    CompleteBatch::new(
        &shape(),
        id_for(config, id_number),
        BatchVersion {
            epoch: config.inventory.epoch,
            sequence,
        },
        vec![Mutation::Put {
            keyspace: KeySpace(0),
            key: b"key1"[..].into(),
            value: vec![fill; 64].into(),
        }],
        config.batch_limits,
    )
    .unwrap()
}

fn group_of(config: &PartitionConfig, batch: CompleteBatch) -> CompleteGroup {
    CompleteGroup::new(
        &shape(),
        vec![batch],
        config.batch_limits,
        config.group_limits,
    )
    .unwrap()
}

#[test]
fn pending_conflicts_are_exact_and_cost_is_independent_of_pending_length() {
    let temp = tempdir::TempDir::new("storage-conflict-pending").unwrap();
    let config = config(vec![3], 3);
    let log = PartitionLog::create(
        &temp.path().join("stream"),
        config.clone(),
        &shape(),
        metrics(),
    )
    .unwrap();
    let mut receipts = Vec::new();
    for n in 0..40 {
        receipts.push(
            log.append_complete_batch(authority(&config), &batch_with(&config, n, n, n as u8))
                .unwrap(),
        );
    }
    // Exact retry returns the original receipt without a new append.
    let before = log.stats().appended_groups;
    let retry = log
        .append_group(
            authority(&config),
            &group_of(&config, batch_with(&config, 7, 7, 7)),
        )
        .unwrap();
    assert_eq!(retry, vec![receipts[7]]);
    assert_eq!(log.stats().appended_groups, before);
    // Same identity, different payload or different version: identity conflict.
    assert!(matches!(
        log.append_group(
            authority(&config),
            &group_of(&config, batch_with(&config, 7, 7, 99))
        ),
        Err(StorageError::IdentityConflict)
    ));
    assert!(matches!(
        log.append_group(
            authority(&config),
            &group_of(&config, batch_with(&config, 7, 90, 7))
        ),
        Err(StorageError::IdentityConflict)
    ));
    // A new identity may not reuse an older, already appended version.
    assert!(matches!(
        log.append_group(
            authority(&config),
            &group_of(&config, batch_with(&config, 100, 3, 1))
        ),
        Err(StorageError::VersionConflict)
    ));
    // Neither rejected append left an effect.
    assert_eq!(log.stats().appended_groups, before);
    assert_eq!(log.lookup(id_for(&config, 100)).unwrap(), None);
    assert_eq!(
        log.lookup_version(BatchVersion {
            epoch: 5,
            sequence: 3
        })
        .unwrap(),
        Some(receipts[3])
    );
    // A fresh identity and version are admitted, and the lookup work for it
    // does not grow with the 40 pending entries.
    let mut profile = StorageWriteProfile::default();
    let fresh = group_of(&config, batch_with(&config, 200, 200, 2));
    log.append_group_profiled(authority(&config), &fresh, &mut profile)
        .unwrap();
    assert_eq!(profile.append_conflict_lookups, 1);
    assert!(
        profile.append_conflict_pending_probes <= 8,
        "probes {} for 40 pending entries",
        profile.append_conflict_pending_probes
    );
    assert_eq!(profile.append_conflict_history_lookups, 0);
}

#[test]
fn conflicts_stay_exact_after_pending_entries_move_to_history() {
    let temp = tempdir::TempDir::new("storage-conflict-history").unwrap();
    let mut config = config(vec![3], 3);
    config.max_batches = 4;
    let log = PartitionLog::create(
        &temp.path().join("stream"),
        config.clone(),
        &shape(),
        metrics(),
    )
    .unwrap();
    let mut receipts = Vec::new();
    for n in 0..12 {
        let group = group_of(&config, batch_with(&config, n, n, n as u8));
        let receipt = loop {
            match log.try_append_group(authority(&config), &group, None) {
                Ok(mut receipts) => break receipts.remove(0),
                Err(StorageError::NeedsPersistence) => {
                    log.persist_through(*receipts.last().unwrap()).unwrap();
                    log.wait_history().unwrap();
                }
                Err(error) => panic!("append failed: {error}"),
            }
        };
        log.persist_through(receipt).unwrap();
        receipts.push(receipt);
    }
    log.wait_history().unwrap();
    assert!(log.references.root().count > 0, "history must hold entries");
    // Entries now rooted on disk are found, retried and conflict exactly.
    for n in [0u64, 1, 5] {
        let found = log.lookup(id_for(&config, n)).unwrap();
        assert_eq!(found, Some(receipts[n as usize]));
        let retry = log
            .append_group(
                authority(&config),
                &group_of(&config, batch_with(&config, n, n, n as u8)),
            )
            .unwrap();
        assert_eq!(retry, vec![receipts[n as usize]]);
        assert!(matches!(
            log.append_group(
                authority(&config),
                &group_of(&config, batch_with(&config, n, n, 250))
            ),
            Err(StorageError::IdentityConflict)
        ));
    }
    assert!(matches!(
        log.append_group(
            authority(&config),
            &group_of(&config, batch_with(&config, 300, 2, 1))
        ),
        Err(StorageError::VersionConflict)
    ));
    // A key never published is proven absent without a rooted-index read.
    let mut profile = StorageWriteProfile::default();
    log.append_group_profiled(
        authority(&config),
        &group_of(&config, batch_with(&config, 400, 400, 3)),
        &mut profile,
    )
    .unwrap();
    assert_eq!(profile.append_conflict_history_lookups, 0);
    assert!(profile.append_conflict_filter_skips >= 1);
}

#[test]
fn conflict_index_stays_exact_under_concurrent_appends_and_lookups() {
    let temp = tempdir::TempDir::new("storage-conflict-concurrent").unwrap();
    let config = config(vec![3], 3);
    let log = Arc::new(
        PartitionLog::create(
            &temp.path().join("stream"),
            config.clone(),
            &shape(),
            metrics(),
        )
        .unwrap(),
    );
    let threads = 4u64;
    let per_thread = 8u64;
    let handles: Vec<_> = (0..threads)
        .map(|thread| {
            let (log, config) = (Arc::clone(&log), config.clone());
            std::thread::spawn(move || {
                let mut mine = Vec::new();
                for item in 0..per_thread {
                    let n = thread * per_thread + item;
                    let batch = batch_with(&config, n, n, n as u8);
                    mine.push(
                        log.append_group(authority(&config), &group_of(&config, batch))
                            .unwrap()[0],
                    );
                    // Concurrent lookups of other threads' keys never see a torn answer.
                    let other = (n + 1) % (threads * per_thread);
                    let _ = log.lookup(id_for(&config, other)).unwrap();
                }
                mine
            })
        })
        .collect();
    let receipts: Vec<_> = handles
        .into_iter()
        .flat_map(|handle| handle.join().unwrap())
        .collect();
    assert_eq!(receipts.len() as u64, threads * per_thread);
    assert_eq!(log.receipt_count() as u64, threads * per_thread);
    for receipt in &receipts {
        assert_eq!(log.lookup(receipt.id()).unwrap(), Some(*receipt));
        assert_eq!(
            log.lookup_version(receipt.version()).unwrap(),
            Some(*receipt)
        );
    }
    // Every identity and version is now taken: any conflicting reuse is rejected.
    let n = 5;
    assert!(matches!(
        log.append_group(
            authority(&config),
            &group_of(&config, batch_with(&config, n, n, 250))
        ),
        Err(StorageError::IdentityConflict)
    ));
    assert!(matches!(
        log.append_group(
            authority(&config),
            &group_of(&config, batch_with(&config, 500, n, 1))
        ),
        Err(StorageError::VersionConflict)
    ));
}
