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
        group_limits: GroupLimits {
            max_batches: 4,
            max_encoded_bytes: 1008,
        },
        fragment_bytes: 1024,
        wal_file_bytes: 2048,
        max_batches: 64,
        max_reference_metadata_bytes: 1024 * 1024,
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

#[test]
fn direct_append_panic_after_native_allocation_requires_recovery() {
    use std::panic::{AssertUnwindSafe, catch_unwind};

    let temp = tempdir::TempDir::new("native-append-unwind").unwrap();
    let config = config(vec![3], 3);
    let log = PartitionLog::create(
        &temp.path().join("stream"),
        config.clone(),
        &shape(),
        metrics(),
    )
    .unwrap();
    let first = log
        .append_complete_batch(authority(&config), &batch(&config, 0))
        .unwrap();
    log.persist_through(first).unwrap();
    log.state
        .lock()
        .writer
        .as_ref()
        .unwrap()
        .fp
        .0
        .store(Arc::new(crate::wal::WalFailPointsInner {
            fp_multi_write_before_write_buf: crate::failpoints::FailPoint::panic_after_n_calls(0),
        }));
    let group = CompleteGroup::new(
        &shape(),
        vec![batch(&config, 1)],
        config.batch_limits,
        config.group_limits,
    )
    .unwrap();
    let mut profile = StorageWriteProfile::default();
    // Exercise the public direct API, outside GroupCommitWorker's panic guard.
    let result = catch_unwind(AssertUnwindSafe(|| {
        log.append_group_profiled(authority(&config), &group, &mut profile)
    }));
    assert!(result.is_err());
    assert_eq!(profile.append_calls, 1);
    assert_eq!(profile.append_errors, 1);
    assert_eq!(profile.appended_groups, 0);
    assert_eq!(profile.appended_receipts, 0);
    assert_eq!(profile.append_publication_ns, 0);
    assert_profile_stage_bounds(profile);
    log.state
        .lock()
        .writer
        .as_ref()
        .unwrap()
        .fp
        .0
        .store(Arc::new(crate::wal::WalFailPointsInner::default()));
    assert!(matches!(
        log.append_complete_batch(authority(&config), &batch(&config, 2)),
        Err(StorageError::Poisoned)
    ));
    assert!(matches!(
        log.persist_through(first),
        Err(StorageError::Poisoned)
    ));
    assert!(matches!(
        log.seal_epoch(authority(&config)),
        Err(StorageError::Poisoned)
    ));
    // Earlier immutable authority remains readable for diagnosis/recovery; this
    // does not authorize a serving database to bypass its own health gate.
    assert_eq!(log.read_batch(first).unwrap().id(), first.id());
    assert_eq!(log.stats().appended_groups, 1);
}

#[test]
fn group_members_share_one_persisted_boundary_and_retry_without_reappend() {
    let temp = tempdir::TempDir::new("native-group-persist").unwrap();
    let config = config(vec![3], 3);
    let path = temp.path().join("stream");
    let log = PartitionLog::create(&path, config.clone(), &shape(), metrics()).unwrap();
    let group = CompleteGroup::new(
        &shape(),
        vec![batch(&config, 0), batch(&config, 1)],
        config.batch_limits,
        config.group_limits,
    )
    .unwrap();
    let receipts = log.append_group(authority(&config), &group).unwrap();
    assert_eq!(receipts.len(), 2);
    assert_eq!(log.stats().appended_groups, 1);
    assert_eq!(log.stats().persistence_barriers, 0);
    let third = batch(&config, 2);
    let third_receipt = log
        .append_complete_batch(authority(&config), &third)
        .unwrap();
    assert_eq!(log.stats().appended_groups, 2);
    assert!(matches!(
        log.validate_durable_receipt(DurableBatch(third_receipt)),
        Err(StorageError::NeedsPersistence)
    ));
    // Even the FIRST member's receipt syncs the entire captured appended prefix.
    log.persist_through(receipts[0]).unwrap();
    log.persist_through(receipts[1]).unwrap();
    log.persist_through(third_receipt).unwrap();
    assert_eq!(log.stats().persistence_barriers, 1);
    assert_eq!(
        log.append_group(authority(&config), &group).unwrap(),
        receipts
    );
    assert_eq!(log.stats().appended_groups, 2);
    let fourth = batch(&config, 3);
    let mixed = CompleteGroup::new(
        &shape(),
        vec![batch(&config, 0), fourth.clone()],
        config.batch_limits,
        config.group_limits,
    )
    .unwrap();
    assert!(matches!(
        log.append_group(authority(&config), &mixed),
        Err(StorageError::MixedRetry)
    ));
    assert_eq!(log.lookup(fourth.id()).unwrap(), None);
    assert_eq!(log.lookup(third.id()).unwrap(), Some(third_receipt));
    drop(log);
    let recovered = PartitionLog::recover_sealed(&path, &config, metrics()).unwrap();
    let recovered_receipts = [receipts[0], receipts[1], third_receipt];
    assert_eq!(recovered.receipt_page(0, 256).unwrap(), recovered_receipts);
    for receipt in recovered_receipts {
        let reference = recovered.record_reference(receipt, 0).unwrap();
        assert_eq!(
            recovered.read_record(&reference).unwrap(),
            batch(&config, receipt.version.sequence).mutations()[0]
        );
    }
}

#[test]
fn bulk_persistence_validates_every_receipt_before_io_and_keeps_exact_order() {
    let temp = tempdir::TempDir::new("bulk-persist-receipts").unwrap();
    let config = config(vec![3], 3);
    let path = temp.path().join("stream");
    let log = PartitionLog::create(&path, config.clone(), &shape(), metrics()).unwrap();
    assert!(log.persist_many(&[]).unwrap().is_empty());
    let group = CompleteGroup::new(
        &shape(),
        vec![batch(&config, 0), batch(&config, 1)],
        config.batch_limits,
        config.group_limits,
    )
    .unwrap();
    let receipts = log.append_group(authority(&config), &group).unwrap();
    let mut foreign = receipts[1];
    foreign.address.segment_generation += 1;
    assert!(matches!(
        log.persist_many(&[receipts[0], foreign]),
        Err(StorageError::Invalid(_))
    ));
    assert_eq!(log.stats().persistence_barriers, 0);
    assert_eq!(log.stats().namespace_syncs, 0);
    let later = log
        .append_complete_batch(authority(&config), &batch(&config, 2))
        .unwrap();
    let reordered = [receipts[1], receipts[0], receipts[1]];
    let durable = log.persist_many(&reordered).unwrap();
    assert_eq!(
        durable.iter().map(|r| r.appended()).collect::<Vec<_>>(),
        reordered
    );
    assert_eq!(log.stats().persistence_barriers, 1);
    assert_eq!(log.stats().namespace_syncs, 1);
    // A persistence request captures all complete groups appended so far.
    log.persist_many(&reordered).unwrap();
    assert_eq!(log.stats().persistence_barriers, 1);
    assert!(
        pending_persistence(&log.state.lock(), later)
            .unwrap()
            .is_none()
    );
    let mixed = [receipts[0], later, receipts[1]];
    assert_eq!(
        log.persist_many(&mixed)
            .unwrap()
            .iter()
            .map(|r| r.appended())
            .collect::<Vec<_>>(),
        mixed
    );
    assert_eq!(log.stats().persistence_barriers, 1);
    drop(log);
    let recovered = PartitionLog::recover_sealed(&path, &config, metrics()).unwrap();
    assert_eq!(recovered.persist_many(&mixed).unwrap().len(), 3);
    assert_eq!(recovered.stats().persistence_barriers, 0);
}

#[test]
fn namespace_sync_is_amortized_only_for_the_exact_wal_file() {
    use std::collections::BTreeSet;
    let temp = tempdir::TempDir::new("persist-namespace-files").unwrap();
    let config = config(vec![3], 3);
    let path = temp.path().join("stream");
    let log = PartitionLog::create(&path, config.clone(), &shape(), metrics()).unwrap();
    let mut files = BTreeSet::new();
    let mut receipts = Vec::new();
    for n in 0..12 {
        let receipt = log
            .append_complete_batch(authority(&config), &batch(&config, n))
            .unwrap();
        files.insert(config.layout().locate_file(receipt.address.offset));
        log.persist_many(&[receipt]).unwrap();
        receipts.push(receipt);
        assert_eq!(log.stats().persistence_barriers, n + 1);
        assert_eq!(log.stats().namespace_syncs, files.len() as u64);
        assert_eq!(
            log.state.lock().namespace_synced_file,
            Some(config.layout().locate_file(receipt.address.offset))
        );
    }
    assert!(files.len() > 1);
    assert!(log.stats().namespace_syncs < log.stats().persistence_barriers);
    drop(log);
    let recovered = PartitionLog::recover_sealed(&path, &config, metrics()).unwrap();
    assert_eq!(recovered.receipt_page(0, 256).unwrap(), receipts);
    assert!(recovered.state.lock().namespace_synced_file.is_none());
    assert_eq!(recovered.persist_many(&receipts).unwrap().len(), 12);
    assert_eq!(recovered.stats().namespace_syncs, 0);
}

#[test]
fn failed_new_file_namespace_sync_never_acknowledges_or_advances_proof() {
    let temp = tempdir::TempDir::new("persist-namespace-failure").unwrap();
    let config = config(vec![3], 3);
    let path = temp.path().join("stream");
    let log = PartitionLog::create(&path, config.clone(), &shape(), metrics()).unwrap();
    let mut all = Vec::new();
    let first_file = config.layout().locate_file(0);
    let pending = loop {
        let n = all.len() as u64;
        let receipt = log
            .append_complete_batch(authority(&config), &batch(&config, n))
            .unwrap();
        all.push(receipt);
        if config.layout().locate_file(receipt.address.offset) != first_file {
            break receipt;
        }
        log.persist_through(receipt).unwrap();
    };
    let before = log.stats();
    let durable_end = log.state.lock().durable_end;
    log.fail_namespace_persist.store(true, Ordering::Release);
    let mut profile = StorageWriteProfile::default();
    assert!(matches!(
        log.persist_many_profiled(&[all[0], pending], &mut profile),
        Err(StorageError::Unknown(_))
    ));
    assert_eq!(profile.persist_calls, 1);
    assert_eq!(profile.persist_errors, 1);
    assert_eq!(profile.persist_noops, 0);
    assert_eq!(profile.persisted_receipts, 0);
    assert!(profile.file_persist_calls > 0);
    assert!(profile.file_persist_successes > 0);
    assert_eq!(profile.namespace_persist_calls, 1);
    assert_eq!(profile.namespace_persist_successes, 0);
    assert_eq!(profile.durable_publications, 0);
    assert_profile_stage_bounds(profile);
    assert_eq!(log.stats(), before);
    assert_eq!(log.state.lock().durable_end, durable_end);
    assert_eq!(log.state.lock().namespace_synced_file, Some(first_file));
    assert!(matches!(
        log.persist_through(pending),
        Err(StorageError::Poisoned)
    ));
    assert!(matches!(
        log.append_complete_batch(authority(&config), &batch(&config, all.len() as u64)),
        Err(StorageError::Poisoned)
    ));
    drop(log);
    // Fault injection occurs after file fsync; a process restart can find all
    // records. It remains UNKNOWN at the failed call, never a definite abort.
    let recovered = PartitionLog::recover_sealed(&path, &config, metrics()).unwrap();
    assert_eq!(recovered.receipt_page(0, 256).unwrap(), all);
    let retry = recovered.persist_many(&[pending]).unwrap();
    assert_eq!(retry[0].appended(), pending);
}

#[test]
fn bulk_receipts_are_not_returned_before_the_shared_barrier() {
    use std::sync::mpsc;
    let temp = tempdir::TempDir::new("bulk-barrier-gate").unwrap();
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
    let group = CompleteGroup::new(
        &shape(),
        vec![batch(&config, 0), batch(&config, 1)],
        config.batch_limits,
        config.group_limits,
    )
    .unwrap();
    let receipts = log.append_group(authority(&config), &group).unwrap();
    let (entered, entered_rx) = mpsc::sync_channel(1);
    let (resume, resume_rx) = mpsc::sync_channel(1);
    *log.before_persist.lock() = Some((entered, resume_rx));
    let (done, result) = mpsc::sync_channel(1);
    let thread_log = log.clone();
    let writer = std::thread::spawn(move || {
        let mut profile = StorageWriteProfile::default();
        let durable = thread_log.persist_many_profiled(&receipts, &mut profile);
        done.send((durable, profile))
    });
    entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(matches!(result.try_recv(), Err(mpsc::TryRecvError::Empty)));
    assert_eq!(log.stats().persistence_barriers, 0);
    let next = log
        .append_complete_batch(authority(&config), &batch(&config, 2))
        .unwrap();
    assert!(matches!(
        log.validate_durable_receipt(DurableBatch(next)),
        Err(StorageError::NeedsPersistence)
    ));
    resume.send(()).unwrap();
    writer.join().unwrap().unwrap();
    let (durable, profile) = result.recv().unwrap();
    assert_eq!(durable.unwrap().len(), 2);
    assert_eq!(profile.persist_calls, 1);
    assert_eq!(profile.persist_errors, 0);
    assert_eq!(profile.persisted_receipts, 2);
    assert_eq!(profile.file_persist_calls, 1);
    assert_eq!(profile.file_persist_successes, 1);
    assert_eq!(profile.namespace_persist_calls, 1);
    assert_eq!(profile.namespace_persist_successes, 1);

    assert_eq!(profile.durable_publications, 1);
    assert_profile_stage_bounds(profile);
    assert_eq!(log.stats().persistence_barriers, 1);
    log.persist_through(next).unwrap();
}

#[test]
fn one_captured_cohort_syncs_every_touched_wal_file() {
    let temp = tempdir::TempDir::new("captured-multifile-cohort").unwrap();
    let config = config(vec![3], 3);
    let log = PartitionLog::create(
        &temp.path().join("stream"),
        config.clone(),
        &shape(),
        metrics(),
    )
    .unwrap();
    let mut receipts = Vec::new();
    for number in 0..12 {
        let receipt = log
            .append_complete_batch(authority(&config), &batch(&config, number))
            .unwrap();
        receipts.push(receipt);
    }
    let mut profile = StorageWriteProfile::default();
    assert_eq!(
        log.persist_many_profiled(&receipts, &mut profile)
            .unwrap()
            .len(),
        receipts.len()
    );
    let state = log.state.lock();
    let touched_files = config.layout().locate_file(state.appended_end - 1).as_u64() + 1;
    drop(state);
    assert!(touched_files > 1);
    assert_eq!(profile.file_persist_calls, touched_files);
    assert_eq!(profile.namespace_persist_calls, 1);
    assert_eq!(log.stats().persistence_barriers, 1);
    let state = log.state.lock();
    assert_eq!(state.durable_end, state.appended_end);
}

#[test]
fn scalar_read_checks_membership_and_reads_only_the_selected_record() {
    let temp = tempdir::TempDir::new("native-scalar-record").unwrap();
    let mut config = config(vec![3], 3);
    config.batch_limits.max_encoded_bytes = 2 * 1024 * 1024;
    config.group_limits.max_encoded_bytes = 3 * 1024 * 1024;
    config.fragment_bytes = 4 * 1024 * 1024;
    config.wal_file_bytes = 8 * 1024 * 1024;
    let path = temp.path().join("stream");
    let log = PartitionLog::create(&path, config.clone(), &shape(), metrics()).unwrap();
    let value = b"tiny".to_vec();
    let batch = CompleteBatch::new(
        &shape(),
        id_for(&config, 0),
        BatchVersion {
            epoch: config.inventory.epoch,
            sequence: 0,
        },
        vec![
            Mutation::Put {
                keyspace: KeySpace(0),
                key: b"key1"[..].into(),
                value: value.clone().into(),
            },
            Mutation::Put {
                keyspace: KeySpace(0),
                key: b"key2"[..].into(),
                value: vec![17; 1024 * 1024].into(),
            },
        ],
        config.batch_limits,
    )
    .unwrap();
    let receipt = log
        .append_complete_batch(authority(&config), &batch)
        .unwrap();
    log.persist_through(receipt).unwrap();
    let reference = log.record_reference(receipt, 0).unwrap();
    assert_eq!(log.read_record(&reference).unwrap(), batch.mutations()[0]); // mmap path
    let check_policies = |log: &PartitionLog| {
        let parent = WalPosition::new(reference.batch_address.offset, reference.batch_address.len);
        let start = reference.descriptor.offset as usize;
        let end = start + reference.descriptor.len as usize;
        let mut encoded = None;
        for policy in [WalMemberReadPolicy::Mapped, WalMemberReadPolicy::Positional] {
            let (kind, bytes) = log
                .wal
                .read_record_payload_range(parent, start..end, policy)
                .unwrap();
            assert!(matches!(
                (policy, kind),
                (
                    WalMemberReadPolicy::Mapped,
                    crate::context::ReadType::Mapped
                ) | (
                    WalMemberReadPolicy::Positional,
                    crate::context::ReadType::Syscall
                )
            ));
            let bytes = bytes.unwrap();
            assert_eq!(bytes.len(), reference.descriptor.len as usize);
            assert_eq!(
                CompleteBatch::decode_record(&shape(), &reference, bytes.clone()).unwrap(),
                batch.mutations()[0]
            );
            if let Some(previous) = &encoded {
                assert_eq!(previous, &bytes);
            }
            encoded = Some(bytes);
            assert_eq!(
                log.wal
                    .read_record_payload_range(parent, end..end, policy)
                    .unwrap()
                    .1
                    .unwrap()
                    .len(),
                0
            );
            assert!(
                log.wal
                    .read_record_payload_range(parent, (end + 1)..end, policy)
                    .is_err()
            );
            assert!(
                log.wal
                    .read_record_payload_range(parent, 0..parent.frame_len(), policy)
                    .is_err()
            );
            assert!(
                log.wal
                    .read_record_payload_range(WalPosition::INVALID, 0..1, policy)
                    .is_err()
            );
            let crossing = WalPosition::new(config.fragment_bytes - 8, 32);
            assert!(
                log.wal
                    .read_record_payload_range(crossing, 0..1, policy)
                    .is_err()
            );
            let missing = WalPosition::new(128 * config.wal_file_bytes, 32);
            assert!(
                log.wal
                    .read_record_payload_range(missing, 0..1, policy)
                    .unwrap()
                    .1
                    .is_none()
            );
        }
    };
    check_policies(&log);
    let mut forged = reference;
    forged.batch_address.segment_generation += 1;
    assert!(log.read_record(&forged).is_err());
    forged = reference;
    forged.descriptor.offset += 1;
    assert!(log.read_record(&forged).is_err());
    drop(log);
    let recovered = PartitionLog::recover_sealed(&path, &config, metrics()).unwrap();
    check_policies(&recovered);
    assert_eq!(
        recovered.read_record(&reference).unwrap(),
        batch.mutations()[0]
    ); // explicit mapped fallback for recovered, nonresident history
    let sibling = recovered.record_reference(receipt, 1).unwrap();
    let file = OpenOptions::new()
        .write(true)
        .open(path.join("wal_0000000000000000"))
        .unwrap();
    // After complete authority validation, damage an unrelated member. Reading
    // the selected scalar must not scan/copy/hash the whole parent batch.
    file.write_all_at(
        &[99],
        receipt.address.offset
            + 8
            + u64::from(sibling.descriptor.offset)
            + u64::from(sibling.descriptor.value_offset),
    )
    .unwrap();
    assert_eq!(
        recovered.read_record(&reference).unwrap(),
        batch.mutations()[0]
    );
    assert!(recovered.read_record(&sibling).is_err());
    for policy in [WalMemberReadPolicy::Mapped, WalMemberReadPolicy::Positional] {
        let bytes = recovered
            .read_record_encoded_with_policy(&reference, policy)
            .unwrap();
        assert_eq!(
            CompleteBatch::decode_record(&shape(), &reference, bytes).unwrap(),
            batch.mutations()[0]
        );
        let bytes = recovered
            .read_record_encoded_with_policy(&sibling, policy)
            .unwrap();
        assert!(CompleteBatch::decode_record(&shape(), &sibling, bytes).is_err());
    }
    assert!(recovered.read_batch(receipt).is_err());
    file.write_all_at(
        &[99],
        receipt.address.offset
            + 8
            + u64::from(reference.descriptor.offset)
            + u64::from(reference.descriptor.value_offset),
    )
    .unwrap();
    assert!(recovered.read_record(&reference).is_err());
}

#[test]
fn persistence_io_does_not_block_value_reads_or_already_durable_retries() {
    use std::sync::mpsc;
    let temp = tempdir::TempDir::new("native-read-during-persist").unwrap();
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
    let old = log
        .append_complete_batch(authority(&config), &batch(&config, 0))
        .unwrap();
    log.persist_through(old).unwrap();
    let reference = log.record_reference(old, 0).unwrap();
    let current = log
        .append_complete_batch(authority(&config), &batch(&config, 1))
        .unwrap();
    let (entered, entered_rx) = mpsc::sync_channel(1);
    let (resume, resume_rx) = mpsc::sync_channel(1);
    *log.before_persist.lock() = Some((entered, resume_rx));
    let writer_log = log.clone();
    let writer = std::thread::spawn(move || writer_log.persist_through(current));
    entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let (read_done, read_rx) = mpsc::sync_channel(1);
    let reader_log = log.clone();
    let read_config = config.clone();
    let reader = std::thread::spawn(move || {
        let read = reader_log.read_record(&reference);
        let retry = reader_log.persist_through(old);
        let next =
            reader_log.append_complete_batch(authority(&read_config), &batch(&read_config, 2));
        let barriers = reader_log.stats().persistence_barriers;
        read_done.send((read, retry, next, barriers)).unwrap();
    });
    let outcome = read_rx.recv_timeout(Duration::from_secs(2));
    // Always release/join before asserting so a regression cannot deadlock the
    // test runner's shutdown. The barrier is demonstrably still pending here.
    resume.send(()).unwrap();
    writer.join().unwrap().unwrap();
    reader.join().unwrap();
    let (value, retry, next, barriers) =
        outcome.expect("disk persistence held the reference lookup mutex");
    assert_eq!(barriers, 1);
    assert!(next.is_ok());
    assert_eq!(value.unwrap(), batch(&config, 0).mutations()[0]);
    assert_eq!(retry.unwrap().appended(), old);
    assert_eq!(log.stats().persistence_barriers, 2);
}

#[test]
fn registered_value_reads_do_not_acquire_the_partition_metadata_mutex() {
    use std::sync::mpsc;
    let temp = tempdir::TempDir::new("native-mutex-free-reference").unwrap();
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
    let batch = batch(&config, 0);
    let receipt = log
        .append_complete_batch(authority(&config), &batch)
        .unwrap();
    log.persist_through(receipt).unwrap();
    let guard = log.state.lock();
    let (sent, received) = mpsc::sync_channel(1);
    let reader_log = log.clone();
    let reader = std::thread::spawn(move || {
        let reference = reader_log.record_reference(receipt, 0).unwrap();
        let value = reader_log.read_record(&reference);
        let whole = reader_log.read_batch(receipt);
        sent.send((value, whole)).unwrap();
    });
    let outcome = received.recv_timeout(Duration::from_secs(2));
    drop(guard); // Always unblock a regressed implementation before joining.
    reader.join().unwrap();
    let (value, whole) = outcome.expect("registered read waited for writer metadata ownership");
    assert_eq!(value.unwrap(), batch.mutations()[0]);
    assert_eq!(whole.unwrap().mutations(), batch.mutations());
}

#[test]
fn reference_metadata_budget_rejects_before_any_authority_bytes() {
    let temp = tempdir::TempDir::new("native-reference-budget").unwrap();
    let original = config(vec![3], 3);
    let probe = PartitionLog::create(
        &temp.path().join("probe"),
        original.clone(),
        &shape(),
        metrics(),
    )
    .unwrap();
    let first = probe
        .append_complete_batch(authority(&original), &batch(&original, 0))
        .unwrap();
    let one_batch_budget = probe.stats().reference_metadata_bytes;
    probe.persist_through(first).unwrap();
    drop(probe);
    let mut config = original;
    config.max_reference_metadata_bytes = one_batch_budget;
    let log = PartitionLog::create(
        &temp.path().join("bounded"),
        config.clone(),
        &shape(),
        metrics(),
    )
    .unwrap();
    let first = log
        .append_complete_batch(authority(&config), &batch(&config, 0))
        .unwrap();
    log.persist_through(first).unwrap();
    assert_eq!(log.stats().registered_records, 2);
    let later = batch(&config, 1);
    let oversized = CompleteGroup::new(
        &shape(),
        vec![later.clone(), batch(&config, 2)],
        config.batch_limits,
        config.group_limits,
    )
    .unwrap();
    assert!(matches!(
        log.append_group(authority(&config), &oversized),
        Err(StorageError::Capacity)
    ));
    assert_eq!(log.lookup(later.id()).unwrap(), None);
    assert_eq!(log.stats().appended_groups, 1);
    assert_eq!(log.stats().reference_metadata_bytes, one_batch_budget);
    assert_eq!(
        log.append_complete_batch(authority(&config), &batch(&config, 0))
            .unwrap(),
        first
    );
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
    assert_eq!(reopened.receipt_page(0, 256).unwrap(), receipts);
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
        Err(StorageError::NeedsPersistence)
    ));
    assert!(matches!(
        log.append_complete_batch(RequestAuthority { epoch: 4 }, &first),
        Err(StorageError::Fenced)
    ));
    assert_eq!(log.receipt_page(0, 256).unwrap(), vec![receipt]);
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
    assert_eq!(recovered.lookup(first.id()).unwrap(), Some(first));
    assert_eq!(
        recovered.lookup(unacknowledged.id()).unwrap(),
        Some(unacknowledged)
    );
    assert_eq!(recovered.lookup(torn.id()).unwrap(), None);
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
    assert_eq!(logs[0].lookup(first.id()).unwrap(), Some(first));
    assert_eq!(logs[1].lookup(later.id()).unwrap(), Some(later));
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
        Err(StorageError::Group(GroupError::Batch(_)))
    ));
    assert_eq!(a.receipt_count(), 1);
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
    // The authenticated root avoids replaying the lifetime WAL prefix. Payload
    // integrity is checked at the consuming read boundary, before returning data.
    let recovered = PartitionLog::recover_sealed(&path, &config, metrics()).unwrap();
    assert_eq!(recovered.lookup(receipt.id()).unwrap(), Some(receipt));
    assert!(recovered.read_batch(receipt).is_err());
    drop(recovered);
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
    assert_eq!(log.receipt_count(), 1);
}

fn assert_profile_stage_bounds(profile: StorageWriteProfile) {
    assert!(
        profile.append_state_lock_wait_ns
            + profile.append_validation_ns
            + profile.append_history_ns
            + profile.append_wal_ns
            + profile.append_publication_ns
            <= profile.append_total_ns
    );
    assert!(
        profile.persist_serialization_lock_wait_ns
            + profile.persist_state_lock_wait_ns
            + profile.persist_validation_ns
            + profile.persist_wal_file_ns
            + profile.persist_namespace_ns
            + profile.persist_inventory_file_ns
            + profile.persist_inventory_namespace_ns
            + profile.persist_publication_ns
            <= profile.persist_total_ns
    );
}

#[test]
fn profiled_writes_preserve_retries_admission_and_durability_frontiers() {
    let temp = tempdir::TempDir::new("profiled-storage-write").unwrap();
    let config = config(vec![3], 3);
    let path = temp.path().join("stream");
    let log = PartitionLog::create(&path, config.clone(), &shape(), metrics()).unwrap();
    let group = CompleteGroup::new(
        &shape(),
        vec![batch(&config, 0), batch(&config, 1)],
        config.batch_limits,
        config.group_limits,
    )
    .unwrap();
    let mut append = StorageWriteProfile::default();
    let receipts = log
        .append_group_profiled(authority(&config), &group, &mut append)
        .unwrap();
    assert_eq!(append.append_calls, 1);
    assert_eq!(append.append_errors, 0);
    assert_eq!(append.appended_groups, 1);
    assert_eq!(append.appended_receipts, 2);
    assert_eq!(append.append_retries, 0);
    assert_eq!(append.persist_total_ns, 0);
    assert_profile_stage_bounds(append);
    assert_eq!(log.stats().persistence_barriers, 0);
    assert_eq!(log.state.lock().durable_end, 0);

    let mut retry = StorageWriteProfile::default();
    assert_eq!(
        log.append_group_profiled(authority(&config), &group, &mut retry)
            .unwrap(),
        receipts
    );
    assert_eq!(retry.append_calls, 1);
    assert_eq!(retry.append_retries, 1);
    assert_eq!(retry.appended_groups, 0);
    assert_eq!(retry.appended_receipts, 2);
    assert_eq!(retry.append_wal_ns, 0);
    assert_eq!(retry.append_publication_ns, 0);
    assert_profile_stage_bounds(retry);

    let next = CompleteGroup::new(
        &shape(),
        vec![batch(&config, 2)],
        config.batch_limits,
        config.group_limits,
    )
    .unwrap();
    let mut next_append = StorageWriteProfile::default();
    let appended_next = log
        .append_group_profiled(authority(&config), &next, &mut next_append)
        .unwrap();
    assert_eq!(appended_next.len(), 1);
    assert_eq!(next_append.append_calls, 1);
    assert_eq!(next_append.append_errors, 0);
    assert_eq!(next_append.appended_groups, 1);
    assert_eq!(next_append.appended_receipts, 1);
    assert!(next_append.append_wal_ns > 0);
    assert!(next_append.append_publication_ns > 0);
    assert_profile_stage_bounds(next_append);

    let mut foreign = receipts[0];
    foreign.address.segment_generation += 1;
    let mut invalid = StorageWriteProfile::default();
    assert!(
        log.persist_many_profiled(&[receipts[0], foreign], &mut invalid)
            .is_err()
    );
    assert_eq!(invalid.persist_calls, 1);
    assert_eq!(invalid.persist_errors, 1);
    assert_eq!(invalid.persisted_receipts, 0);
    assert_eq!(invalid.persist_serialization_lock_wait_ns, 0);
    assert_eq!(invalid.persist_wal_file_ns, 0);
    assert_eq!(invalid.persist_namespace_ns, 0);
    assert_eq!(invalid.file_persist_calls, 0);
    assert_eq!(invalid.namespace_persist_calls, 0);
    assert_eq!(invalid.durable_publications, 0);
    assert_profile_stage_bounds(invalid);

    let mut persist = StorageWriteProfile::default();
    let reordered = [receipts[1], receipts[0], receipts[1]];
    assert_eq!(
        log.persist_many_profiled(&reordered, &mut persist)
            .unwrap()
            .iter()
            .map(|durable| durable.appended())
            .collect::<Vec<_>>(),
        reordered
    );
    assert_eq!(persist.persist_calls, 1);
    assert_eq!(persist.persist_errors, 0);
    assert_eq!(persist.persisted_receipts, 3);
    assert_eq!(persist.file_persist_calls, 1);
    assert_eq!(persist.file_persist_successes, 1);
    assert_eq!(persist.namespace_persist_calls, 1);
    assert_eq!(persist.namespace_persist_successes, 1);
    assert_eq!(persist.durable_publications, 1);
    assert_profile_stage_bounds(persist);
    for receipt in &receipts {
        assert!(
            pending_persistence(&log.state.lock(), *receipt)
                .unwrap()
                .is_none()
        );
    }

    let mut noops = StorageWriteProfile::default();
    log.persist_many_profiled(&reordered, &mut noops).unwrap();
    log.persist_many_profiled(&[], &mut noops).unwrap();
    assert_eq!(noops.persist_calls, 2);
    assert_eq!(noops.persist_noops, 2);
    assert_eq!(noops.persist_errors, 0);
    assert_eq!(noops.persisted_receipts, 3);
    assert_eq!(noops.persist_serialization_lock_wait_ns, 0);
    assert_eq!(noops.persist_wal_file_ns, 0);
    assert_eq!(noops.persist_namespace_ns, 0);
    assert_eq!(noops.persist_publication_ns, 0);
    assert_eq!(noops.file_persist_calls, 0);
    assert_eq!(noops.durable_publications, 0);
    assert_profile_stage_bounds(noops);
    let later_batch = CompleteGroup::new(
        &shape(),
        vec![batch(&config, 3)],
        config.batch_limits,
        config.group_limits,
    )
    .unwrap();
    let mut later_append = StorageWriteProfile::default();
    let later = log
        .append_group_profiled(authority(&config), &later_batch, &mut later_append)
        .unwrap();
    assert_eq!(later_append.append_calls, 1);
    assert_eq!(later_append.append_errors, 0);
    assert_eq!(later_append.appended_groups, 1);
    assert_eq!(later_append.appended_receipts, 1);
    assert!(later_append.append_wal_ns > 0);
    assert!(later_append.append_publication_ns > 0);
    assert_profile_stage_bounds(later_append);
    assert!(
        pending_persistence(&log.state.lock(), later[0])
            .unwrap()
            .is_some()
    );
    log.persist_many_profiled(&later, &mut persist).unwrap();
    assert_eq!(persist.persist_calls, 2);
    assert_eq!(persist.persisted_receipts, 4);
    assert_eq!(persist.file_persist_calls, 2);
    assert_eq!(persist.file_persist_successes, 2);
    assert_eq!(persist.durable_publications, 2);
    assert_eq!(
        persist.namespace_persist_successes,
        log.stats().namespace_syncs
    );
    assert_eq!(
        persist.file_persist_successes,
        log.stats().persistence_barriers
    );
    assert_profile_stage_bounds(persist);

    drop(log);
    let recovered = PartitionLog::recover_sealed(&path, &config, metrics()).unwrap();
    assert_eq!(
        recovered.receipt_page(0, 256).unwrap(),
        [receipts, appended_next, later].concat()
    );
}

#[test]
fn profiled_pre_io_failure_never_counts_a_barrier_or_durable_receipt() {
    let temp = tempdir::TempDir::new("profiled-persist-failure").unwrap();
    let config = config(vec![3], 3);
    let log = PartitionLog::create(
        &temp.path().join("stream"),
        config.clone(),
        &shape(),
        metrics(),
    )
    .unwrap();
    let receipt = log
        .append_complete_batch(authority(&config), &batch(&config, 0))
        .unwrap();
    log.fail_next_persist();
    let mut profile = StorageWriteProfile::default();
    assert!(matches!(
        log.persist_many_profiled(&[receipt], &mut profile),
        Err(StorageError::Unknown(_))
    ));
    assert_eq!(profile.persist_calls, 1);
    assert_eq!(profile.persist_errors, 1);
    assert_eq!(profile.persist_noops, 0);
    assert_eq!(profile.persisted_receipts, 0);
    assert_eq!(profile.file_persist_calls, 0);
    assert_eq!(profile.file_persist_successes, 0);
    assert_eq!(profile.namespace_persist_calls, 0);
    assert_eq!(profile.namespace_persist_successes, 0);
    assert_eq!(profile.durable_publications, 0);
    assert_eq!(profile.persist_wal_file_ns, 0);
    assert_eq!(profile.persist_namespace_ns, 0);
    assert_profile_stage_bounds(profile);
    assert_eq!(log.state.lock().durable_end, 0);
    assert_eq!(log.stats().persistence_barriers, 0);
    assert!(matches!(
        log.persist_many_profiled(&[receipt], &mut profile),
        Err(StorageError::Poisoned)
    ));
    assert_eq!(profile.persist_calls, 2);
    assert_eq!(profile.persist_errors, 2);
    assert_eq!(profile.persisted_receipts, 0);
    assert_eq!(profile.file_persist_calls, 0);
    assert_profile_stage_bounds(profile);
}

mod history;
