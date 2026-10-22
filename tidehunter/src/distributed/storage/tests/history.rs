use super::*;

#[test]
fn completed_history_exceeds_resident_limit_with_stable_paged_retries_and_prefixes() {
    let temp = tempdir::TempDir::new("storage-paged-retries").unwrap();
    let mut config = config(vec![3], 3);
    config.max_batches = 2;
    let path = temp.path().join("stream");
    let log = PartitionLog::create(&path, config.clone(), &shape(), metrics()).unwrap();
    let mut receipts = Vec::new();
    let mut proof = None;
    for n in 0..18 {
        let receipt = log
            .append_complete_batch(authority(&config), &batch(&config, n))
            .unwrap();
        log.persist_through(receipt).unwrap();
        receipts.push(receipt);
        assert!(
            log.stats().registered_records <= 4,
            "completed descriptors exceeded bounded window"
        );
        assert!(log.stats().reference_metadata_bytes <= config.max_reference_metadata_bytes);
        if n == 1 {
            proof = Some(log.checkpoint_prefix(2).unwrap());
        }
    }
    assert_eq!(log.receipt_count(), 18);
    assert_eq!(log.checkpoint_prefix(2).unwrap(), proof.unwrap());
    assert_eq!(log.receipt_page(3, 2).unwrap(), receipts[3..5]);
    assert!(log.receipt_page(0, 257).is_err());
    assert!(log.receipt_page(19, 1).is_err());
    assert_eq!(log.lookup(receipts[0].id()).unwrap(), Some(receipts[0]));
    assert_eq!(
        log.lookup_version(receipts[0].version()).unwrap(),
        Some(receipts[0])
    );
    assert_eq!(
        log.append_complete_batch(authority(&config), &batch(&config, 0))
            .unwrap(),
        receipts[0]
    );
    assert_eq!(log.stats().appended_groups, 18);
    log.seal_epoch(authority(&config)).unwrap();
    drop(log);
    for _ in 0..3 {
        let log = PartitionLog::recover_sealed(&path, &config, metrics()).unwrap();
        assert_eq!(log.receipt_count(), receipts.len());
        assert_eq!(log.receipt_page(0, 256).unwrap(), receipts);
        assert_eq!(log.checkpoint_prefix(2).unwrap(), proof.unwrap());
        assert_eq!(
            log.persist_through(receipts[0]).unwrap().appended(),
            receipts[0]
        );
        assert_eq!(log.stats().registered_records, 0);
    }
}

#[test]
fn rooted_retirement_replays_before_or_after_interrupted_unlink() {
    for unlink in [false, true] {
        let temp = tempdir::TempDir::new("storage-retire-cut").unwrap();
        let config = config(vec![3], 3);
        let path = temp.path().join("stream");
        let log = PartitionLog::create(&path, config.clone(), &shape(), metrics()).unwrap();
        let first = log
            .append_complete_batch(authority(&config), &batch(&config, 0))
            .unwrap();
        log.persist_through(first).unwrap();
        log.seal_epoch(authority(&config)).unwrap();
        let proof = log.checkpoint_prefix(1).unwrap();
        // Explicit fault cut: the recovery/reachability decision is durable,
        // but native admission removal/unlink has not yet completed.
        log.state.lock().references.retire(&[0]).unwrap();
        if unlink {
            fs::remove_file(path.join("wal_0000000000000000")).unwrap();
        }
        drop(log);
        let log = PartitionLog::recover_sealed(&path, &config, metrics()).unwrap();
        assert!(!path.join("wal_0000000000000000").exists());
        assert_eq!(log.lookup(first.id()).unwrap(), Some(first));
        assert_eq!(log.lookup_version(first.version()).unwrap(), Some(first));
        assert_eq!(log.persist_through(first).unwrap().appended(), first);
        assert_eq!(log.checkpoint_prefix(1).unwrap(), proof);
        assert!(log.read_batch(first).is_err());
        log.retire_files(1, &[0]).unwrap();
        drop(log);
        let log = PartitionLog::recover_sealed(&path, &config, metrics()).unwrap();
        assert!(!path.join("wal_0000000000000000").exists());
        assert_eq!(log.lookup(first.id()).unwrap(), Some(first));
    }
}

#[test]
fn sealed_empty_history_does_not_recreate_retired_wal_zero() {
    let temp = tempdir::TempDir::new("storage-retired-empty").unwrap();
    let config = config(vec![3], 3);
    let path = temp.path().join("stream");
    let log = PartitionLog::create(&path, config.clone(), &shape(), metrics()).unwrap();
    log.seal_epoch(authority(&config)).unwrap();
    let files = log.wal_file_page(None, 256).unwrap();
    assert!(!files.is_empty());
    log.retire_files(0, &files).unwrap();
    drop(log);
    for _ in 0..2 {
        let log = PartitionLog::recover_sealed(&path, &config, metrics()).unwrap();
        assert_eq!(log.receipt_count(), 0);
        assert!(log.wal_file_page(None, 256).unwrap().is_empty());
        assert!(!path.join("wal_0000000000000000").exists());
    }
}

#[test]
fn in_flight_file_and_descriptor_owners_survive_retirement_but_new_reads_fail() {
    let temp = tempdir::TempDir::new("storage-read-retire").unwrap();
    let config = config(vec![3], 3);
    let path = temp.path().join("stream");
    let log = Arc::new(PartitionLog::create(&path, config.clone(), &shape(), metrics()).unwrap());
    let receipt = log
        .append_complete_batch(authority(&config), &batch(&config, 0))
        .unwrap();
    log.persist_through(receipt).unwrap();
    let reference = log.record_reference(receipt, 0).unwrap();
    let bytes = log.read_record_encoded(&reference).unwrap();
    let plan = log.try_plan_record_read(&reference).unwrap().unwrap();
    assert!(plan.authority_bytes() > 0);
    log.seal_epoch(authority(&config)).unwrap();
    let charged = log.stats().reference_metadata_bytes;
    log.evict_authority_cache();
    assert_eq!(
        log.stats().reference_metadata_bytes,
        charged,
        "eviction released live descriptor pin credit"
    );
    let cached = log.plan_record_read(&reference).unwrap();
    drop(cached);
    log.retire_files(1, &[0]).unwrap();
    assert!(log.try_plan_record_read(&reference).is_err());
    assert!(log.read_record_encoded(&reference).is_err());
    let mut encoded = vec![0; plan.member.len];
    plan.member
        .file
        .read_exact_at(&mut encoded, plan.member.offset)
        .unwrap();
    assert_eq!(encoded.as_slice(), bytes.as_ref());
    assert_eq!(
        CompleteBatch::decode_record(&shape(), &reference, bytes).unwrap(),
        batch(&config, 0).mutations()[0]
    );
    drop(plan);
    log.evict_authority_cache();
    assert_eq!(log.stats().registered_records, 0);
}

#[test]
fn missing_or_corrupt_authority_and_unknown_missing_payload_fail_closed() {
    for corrupt_page in [false, true] {
        let temp = tempdir::TempDir::new("storage-history-corrupt").unwrap();
        let mut config = config(vec![3], 3);
        config.max_batches = 1;
        let path = temp.path().join("stream");
        let log = PartitionLog::create(&path, config.clone(), &shape(), metrics()).unwrap();
        let receipt = log
            .append_complete_batch(authority(&config), &batch(&config, 0))
            .unwrap();
        log.persist_through(receipt).unwrap();
        log.seal_epoch(authority(&config)).unwrap();
        let page = path.join("storage-history-v2/receipts.pages");
        if corrupt_page {
            let file = OpenOptions::new().write(true).open(page).unwrap();
            file.write_all_at(&[0xff], 8).unwrap();
            file.sync_all().unwrap();
            assert!(log.lookup(receipt.id()).is_err());
        } else {
            fs::remove_file(path.join("wal_0000000000000000")).unwrap();
        }
        drop(log);
        assert!(PartitionLog::recover_sealed(&path, &config, metrics()).is_err());
    }
}

#[test]
fn retirement_rejects_unsealed_partial_prefix_and_unregistered_files() {
    let temp = tempdir::TempDir::new("storage-retire-preflight").unwrap();
    let config = config(vec![3], 3);
    let path = temp.path().join("stream");
    let log = PartitionLog::create(&path, config.clone(), &shape(), metrics()).unwrap();
    let receipt = log
        .append_complete_batch(authority(&config), &batch(&config, 0))
        .unwrap();
    log.persist_through(receipt).unwrap();
    assert!(log.retire_files(1, &[0]).is_err());
    log.seal_epoch(authority(&config)).unwrap();
    assert!(log.retire_files(0, &[0]).is_err());
    assert!(log.retire_files(1, &[1]).is_err());
    assert!(log.retire_files(1, &[u64::MAX - 2047]).is_err());
    assert!(path.join("wal_0000000000000000").exists());
    assert_eq!(
        log.read_batch(receipt).unwrap().mutations(),
        batch(&config, 0).mutations()
    );
}

#[test]
fn unadopted_history_pages_and_temporary_root_do_not_hide_durable_wal_suffix() {
    let temp = tempdir::TempDir::new("storage-unadopted-history").unwrap();
    let config = config(vec![3], 3);
    let path = temp.path().join("stream");
    let log = PartitionLog::create(&path, config.clone(), &shape(), metrics()).unwrap();
    let receipt = log
        .append_complete_batch(authority(&config), &batch(&config, 0))
        .unwrap();
    log.persist_through(receipt).unwrap();
    // No spill/seal: root count is still zero. These simulate interrupted
    // derived metadata writes, not independent authority for the inner record.
    fs::write(path.join("storage-history-v2/receipts.pages"), [1, 2, 3]).unwrap();
    fs::write(path.join(".storage-history-root-v2.pending"), [4, 5, 6]).unwrap();
    drop(log);
    let log = PartitionLog::recover_sealed(&path, &config, metrics()).unwrap();
    assert_eq!(log.receipt_page(0, 1).unwrap(), vec![receipt]);
    assert_eq!(
        log.read_batch(receipt).unwrap().mutations(),
        batch(&config, 0).mutations()
    );
    drop(log);
    assert!(PartitionLog::recover_sealed(&path, &config, metrics()).is_ok());
}

#[test]
fn corrupt_storage_root_is_not_replaced_by_wal_inference() {
    let temp = tempdir::TempDir::new("storage-root-corrupt").unwrap();
    let config = config(vec![3], 3);
    let path = temp.path().join("stream");
    let log = PartitionLog::create(&path, config.clone(), &shape(), metrics()).unwrap();
    let receipt = log
        .append_complete_batch(authority(&config), &batch(&config, 0))
        .unwrap();
    log.persist_through(receipt).unwrap();
    log.seal_epoch(authority(&config)).unwrap();
    drop(log);
    let root = path.join("storage-history-root-v2");
    let mut bytes = fs::read(&root).unwrap();
    bytes[0] ^= 1;
    fs::write(root, bytes).unwrap();
    assert!(PartitionLog::recover_sealed(&path, &config, metrics()).is_err());
}

#[test]
fn extra_suffix_or_unfinished_retired_name_cannot_hide_missing_rooted_file() {
    for pending_retired_name in [false, true] {
        let temp = tempdir::TempDir::new("storage-inventory-compensation").unwrap();
        let config = config(vec![3], 3);
        let path = temp.path().join("stream");
        let log = PartitionLog::create(&path, config.clone(), &shape(), metrics()).unwrap();
        for n in 0..8 {
            let receipt = log
                .append_complete_batch(authority(&config), &batch(&config, n))
                .unwrap();
            log.persist_through(receipt).unwrap();
        }
        log.seal_epoch(authority(&config)).unwrap();
        let root = log.references.root();
        assert!(root.file_count > 1);
        let missing = if pending_retired_name {
            log.state.lock().references.retire(&[0]).unwrap();
            1
        } else {
            0
        };
        fs::remove_file(path.join(format!("wal_{missing:016x}"))).unwrap();
        let extra = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path.join(format!("wal_{:016x}", root.file_count)))
            .unwrap();
        extra.set_len(config.wal_file_bytes).unwrap();
        extra.sync_all().unwrap();
        drop(extra);
        drop(log);
        assert!(PartitionLog::recover_sealed(&path, &config, metrics()).is_err());
    }
}

#[test]
fn cold_old_authority_becomes_hot_without_lifetime_descriptor_residency() {
    let temp = tempdir::TempDir::new("storage-cold-authority-cache").unwrap();
    let mut config = config(vec![3], 3);
    config.max_batches = 1;
    let log = Arc::new(
        PartitionLog::create(
            &temp.path().join("stream"),
            config.clone(),
            &shape(),
            metrics(),
        )
        .unwrap(),
    );
    let first = log
        .append_complete_batch(authority(&config), &batch(&config, 0))
        .unwrap();
    log.persist_through(first).unwrap();
    let reference = log.record_reference(first, 0).unwrap();
    let second = log
        .append_complete_batch(authority(&config), &batch(&config, 1))
        .unwrap();
    log.persist_through(second).unwrap();
    assert!(log.try_plan_record_read(&reference).unwrap().is_none());
    drop(log.plan_record_read(&reference).unwrap());
    assert!(log.try_plan_record_read(&reference).unwrap().is_some());
    assert!(log.stats().reference_metadata_bytes <= config.max_reference_metadata_bytes);
    assert_eq!(log.stats().registered_records, 4);
    log.evict_authority_cache();
    assert_eq!(log.stats().registered_records, 2);
}

#[test]
fn final_read_token_holds_metadata_credit_and_backpressures_before_new_wal_bytes() {
    let temp = tempdir::TempDir::new("storage-authority-credit-lifetime").unwrap();
    let mut config = config(vec![3], 3);
    config.max_batches = 1;
    config.max_reference_metadata_bytes = ReferenceDirectory::metadata_bytes(1, 2).unwrap();
    let log = Arc::new(
        PartitionLog::create(
            &temp.path().join("stream"),
            config.clone(),
            &shape(),
            metrics(),
        )
        .unwrap(),
    );
    let first = log
        .append_complete_batch(authority(&config), &batch(&config, 0))
        .unwrap();
    log.persist_through(first).unwrap();
    let reference = log.record_reference(first, 0).unwrap();
    let pin = log.try_plan_record_read(&reference).unwrap().unwrap();
    let second_batch = batch(&config, 1);
    assert!(matches!(
        log.append_complete_batch(authority(&config), &second_batch),
        Err(StorageError::Capacity)
    ));
    assert_eq!(log.receipt_count(), 1);
    assert_eq!(log.lookup(second_batch.id()).unwrap(), None);
    assert_eq!(log.stats().appended_groups, 1);
    log.evict_authority_cache();
    assert_eq!(
        log.stats().reference_metadata_bytes,
        config.max_reference_metadata_bytes
    );
    drop(pin);
    let second = log
        .append_complete_batch(authority(&config), &second_batch)
        .unwrap();
    log.persist_through(second).unwrap();
    assert_eq!(log.lookup(first.id()).unwrap(), Some(first));
    log.seal_epoch(authority(&config)).unwrap();
    // An exact one-batch metadata budget can still own an uncached cold token;
    // cache bookkeeping must not make a previously readable batch unavailable.
    drop(log.plan_record_read(&reference).unwrap());
    assert_eq!(log.stats().registered_records, 0);
}

fn first_pending_batch_in_next_file(log: &PartitionLog, config: &PartitionConfig) -> AppendedBatch {
    for n in 0..32 {
        let receipt = log
            .append_complete_batch(authority(config), &batch(config, n))
            .unwrap();
        if receipt.address().offset >= config.wal_file_bytes {
            return receipt;
        }
        log.persist_through(receipt).unwrap();
    }
    panic!("fixture did not advance to another WAL file");
}

#[test]
fn file_witness_crash_cuts_do_not_turn_surviving_unacknowledged_groups_into_corruption() {
    for after in [false, true] {
        let temp = tempdir::TempDir::new("storage-file-witness-cut").unwrap();
        let config = config(vec![3], 3);
        let path = temp.path().join("stream");
        let log = PartitionLog::create(&path, config.clone(), &shape(), metrics()).unwrap();
        let pending = first_pending_batch_in_next_file(&log, &config);
        let file = pending.address().offset / config.wal_file_bytes;
        let witness = format!("partition-required-wal-v1-{file:016x}");
        log.fail_next_inventory_witness(after);
        let mut profile = StorageWriteProfile::default();
        assert!(matches!(
            log.persist_many_profiled(&[pending], &mut profile),
            Err(StorageError::Unknown(_))
        ));
        assert!(profile.file_persist_successes > 0);
        assert!(
            profile.namespace_persist_successes > 0,
            "WAL name must precede witness publication"
        );
        assert_eq!(profile.inventory_file_persist_successes, u64::from(after));
        assert_eq!(
            profile.inventory_namespace_persist_successes,
            u64::from(after)
        );
        assert_eq!(profile.durable_publications, 0);
        assert_eq!(path.join(&witness).exists(), after);
        fs::write(path.join(format!(".{witness}.pending")), [1, 2, 3]).unwrap();
        drop(log);
        let log = PartitionLog::recover_sealed(&path, &config, metrics()).unwrap();
        assert_eq!(log.lookup(pending.id()).unwrap(), Some(pending));
        assert_eq!(log.persist_through(pending).unwrap().appended(), pending);
        assert_eq!(
            log.read_batch(pending).unwrap().version(),
            pending.version()
        );
        assert!(
            !path.join(&witness).exists(),
            "adopted root must release covered witnesses"
        );
        assert!(!path.join(format!(".{witness}.pending")).exists());
    }
}

#[test]
fn missing_highest_acknowledged_suffix_file_is_detected_without_later_wal_names() {
    let temp = tempdir::TempDir::new("storage-missing-witnessed-tail").unwrap();
    let config = config(vec![3], 3);
    let path = temp.path().join("stream");
    let log = PartitionLog::create(&path, config.clone(), &shape(), metrics()).unwrap();
    let last = first_pending_batch_in_next_file(&log, &config);
    log.persist_through(last).unwrap();
    let last_file = last.address().offset / config.wal_file_bytes;
    assert!(
        path.join(format!("partition-required-wal-v1-{last_file:016x}"))
            .exists()
    );
    assert_eq!(
        log.references.root().file_count,
        1,
        "fixture must stay beyond unadvanced metadata root"
    );
    drop(log);
    for entry in fs::read_dir(&path).unwrap() {
        let entry = entry.unwrap();
        let Some((id, _)) = wal_file_entry(&entry, &config).unwrap() else {
            continue;
        };
        if id >= last_file {
            fs::remove_file(entry.path()).unwrap();
        }
    }
    // No internal numeric gap remains. Only the correctly ordered durable
    // witness can distinguish this lost last ACKed file from an empty suffix.
    assert!(PartitionLog::recover_sealed(&path, &config, metrics()).is_err());
}

#[test]
fn read_only_cold_authority_pressure_spills_completed_window_once() {
    let temp = tempdir::TempDir::new("storage-cold-read-progress").unwrap();
    let mut config = config(vec![3], 3);
    config.max_batches = 1;
    config.max_reference_metadata_bytes = ReferenceDirectory::metadata_bytes(1, 2).unwrap();
    let log = Arc::new(
        PartitionLog::create(
            &temp.path().join("stream"),
            config.clone(),
            &shape(),
            metrics(),
        )
        .unwrap(),
    );
    let first = log
        .append_complete_batch(authority(&config), &batch(&config, 0))
        .unwrap();
    log.persist_through(first).unwrap();
    let reference = log.record_reference(first, 0).unwrap();
    let second = log
        .append_complete_batch(authority(&config), &batch(&config, 1))
        .unwrap();
    log.persist_through(second).unwrap();
    assert_eq!(
        log.stats().reference_metadata_bytes,
        config.max_reference_metadata_bytes
    );
    assert_eq!(log.references.root().count, 1);
    assert!(log.try_plan_record_read(&reference).unwrap().is_none());
    let plan = log.plan_record_read(&reference).unwrap();
    assert_eq!(
        log.references.root().count,
        2,
        "cold worker must release completed residency without another append"
    );
    assert_eq!(log.receipt_count(), 2);
    let mut encoded = vec![0; plan.member.len];
    plan.member
        .file
        .read_exact_at(&mut encoded, plan.member.offset)
        .unwrap();
    assert_eq!(
        CompleteBatch::decode_record(&shape(), &reference, encoded.into()).unwrap(),
        batch(&config, 0).mutations()[0]
    );
    drop(plan);
    assert_eq!(
        log.read_record(&reference).unwrap(),
        batch(&config, 0).mutations()[0]
    );
    assert_eq!(log.stats().appended_groups, 2);
}

#[test]
fn cold_pressure_never_reclaims_live_credit_or_repeats_an_empty_root() {
    let temp = tempdir::TempDir::new("storage-cold-pinned-credit").unwrap();
    let mut config = config(vec![3], 3);
    config.max_batches = 1;
    config.max_reference_metadata_bytes = ReferenceDirectory::metadata_bytes(1, 2).unwrap();
    let path = temp.path().join("stream");
    let log = Arc::new(PartitionLog::create(&path, config.clone(), &shape(), metrics()).unwrap());
    let first = log
        .append_complete_batch(authority(&config), &batch(&config, 0))
        .unwrap();
    log.persist_through(first).unwrap();
    let reference = log.record_reference(first, 0).unwrap();
    let held = log.try_plan_record_read(&reference).unwrap().unwrap();
    assert!(matches!(
        log.append_complete_batch(authority(&config), &batch(&config, 1)),
        Err(StorageError::Capacity)
    ));
    let before = fs::read(path.join("storage-history-root-v2")).unwrap();
    assert!(matches!(
        log.plan_record_read(&reference),
        Err(StorageError::Capacity)
    ));
    assert_eq!(
        fs::read(path.join("storage-history-root-v2")).unwrap(),
        before
    );
    assert_eq!(
        log.stats().reference_metadata_bytes,
        config.max_reference_metadata_bytes
    );
    drop(held);
    assert!(log.plan_record_read(&reference).is_ok());
}

#[test]
fn cold_pressure_root_io_error_is_unknown_and_closes_read_admission() {
    let temp = tempdir::TempDir::new("storage-cold-root-failure").unwrap();
    let mut config = config(vec![3], 3);
    config.max_batches = 1;
    config.max_reference_metadata_bytes = ReferenceDirectory::metadata_bytes(1, 2).unwrap();
    let path = temp.path().join("stream");
    let log = Arc::new(PartitionLog::create(&path, config.clone(), &shape(), metrics()).unwrap());
    let first = log
        .append_complete_batch(authority(&config), &batch(&config, 0))
        .unwrap();
    log.persist_through(first).unwrap();
    let reference = log.record_reference(first, 0).unwrap();
    let second = log
        .append_complete_batch(authority(&config), &batch(&config, 1))
        .unwrap();
    log.persist_through(second).unwrap();
    let temporary = path.join(".storage-history-root-v2.pending");
    fs::create_dir(&temporary).unwrap();
    assert!(matches!(
        log.plan_record_read(&reference),
        Err(StorageError::Unknown(_))
    ));
    assert!(matches!(
        log.try_plan_record_read(&reference),
        Err(StorageError::Poisoned)
    ));
    assert!(matches!(
        log.append_complete_batch(authority(&config), &batch(&config, 2)),
        Err(StorageError::Poisoned)
    ));
    fs::remove_dir(temporary).unwrap();
    drop(log);
    let log = PartitionLog::recover_sealed(&path, &config, metrics()).unwrap();
    assert_eq!(log.receipt_count(), 2);
    assert_eq!(log.lookup(first.id()).unwrap(), Some(first));
    assert_eq!(log.lookup(second.id()).unwrap(), Some(second));
}

#[test]
fn pooled_history_has_constant_file_set_and_frozen_prefix_readers() {
    let temp = tempdir::TempDir::new("storage-pooled-files").unwrap();
    let mut cfg = config(vec![3], 3);
    cfg.max_batches = 1;
    let path = temp.path().join("stream");
    let log = PartitionLog::create(&path, cfg.clone(), &shape(), metrics()).unwrap();
    let first = batch(&cfg, 0);
    for sequence in 0..2 {
        let receipt = log
            .append_complete_batch(authority(&cfg), &batch(&cfg, sequence))
            .unwrap();
        log.persist_through(receipt).unwrap();
    }
    let frozen = log.references.freeze_history();
    assert_eq!(frozen.root().count, 1);
    let proof = log.checkpoint_prefix(1).unwrap();
    for sequence in 2..24 {
        let receipt = log
            .append_complete_batch(authority(&cfg), &batch(&cfg, sequence))
            .unwrap();
        log.persist_through(receipt).unwrap();
    }
    log.seal_epoch(authority(&cfg)).unwrap();
    assert_eq!(
        fs::read_dir(path.join("storage-history-v2"))
            .unwrap()
            .count(),
        4
    );
    assert_eq!(
        frozen.lookup(first.id()).unwrap().unwrap().receipt().id(),
        first.id()
    );
    assert!(frozen.lookup(batch(&cfg, 23).id()).unwrap().is_none());
    assert_eq!(log.checkpoint_prefix(1).unwrap(), proof);
}

#[test]
fn unadopted_partial_pooled_tails_are_not_promoted_by_empty_recovery() {
    let temp = tempdir::TempDir::new("storage-pooled-orphan-tails").unwrap();
    let cfg = config(vec![3], 3);
    let path = temp.path().join("stream");
    let log = PartitionLog::create(&path, cfg.clone(), &shape(), metrics()).unwrap();
    let first = batch(&cfg, 0);
    let receipt = log.append_complete_batch(authority(&cfg), &first).unwrap();
    log.persist_through(receipt).unwrap();
    log.seal_epoch(authority(&cfg)).unwrap();
    drop(log);
    for name in [
        "nodes.pages",
        "receipts.pages",
        "descriptors.pages",
        "retired.pages",
    ] {
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(path.join("storage-history-v2").join(name))
            .unwrap();
        std::io::Write::write_all(&mut file, b"bad").unwrap();
        file.sync_all().unwrap();
    }
    for _ in 0..2 {
        let recovered = PartitionLog::recover_sealed(&path, &cfg, metrics()).unwrap();
        assert_eq!(recovered.lookup(first.id()).unwrap(), Some(receipt));
        let decoded = recovered.read_batch(receipt).unwrap();
        assert_eq!(decoded.id(), first.id());
        assert_eq!(decoded.version(), first.version());
        assert_eq!(decoded.digest(), first.digest());
        assert_eq!(decoded.mutations(), first.mutations());
        assert_eq!(decoded.encoded().as_ref(), first.encoded().as_ref());
    }
}

#[test]
fn pooled_node_and_descriptor_corruption_fail_closed_on_cold_access() {
    for corrupt_node in [false, true] {
        let temp = tempdir::TempDir::new("storage-pooled-corrupt").unwrap();
        let cfg = config(vec![3], 3);
        let path = temp.path().join("stream");
        let log = PartitionLog::create(&path, cfg.clone(), &shape(), metrics()).unwrap();
        let first = batch(&cfg, 0);
        let receipt = log.append_complete_batch(authority(&cfg), &first).unwrap();
        log.persist_through(receipt).unwrap();
        let reference = log.record_reference(receipt, 0).unwrap();
        log.seal_epoch(authority(&cfg)).unwrap();
        log.evict_authority_cache();
        let name = if corrupt_node {
            "nodes.pages"
        } else {
            "descriptors.pages"
        };
        let file = fs::OpenOptions::new()
            .write(true)
            .open(path.join("storage-history-v2").join(name))
            .unwrap();
        file.write_all_at(&[0xff], 8).unwrap();
        file.sync_all().unwrap();
        if corrupt_node {
            assert!(log.lookup(first.id()).is_err());
        } else {
            assert!(log.read_record_encoded(&reference).is_err());
            assert_eq!(log.lookup(first.id()).unwrap(), Some(receipt));
        }
    }
}
