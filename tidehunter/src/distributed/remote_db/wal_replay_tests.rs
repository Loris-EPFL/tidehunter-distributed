//! WAL-position checkpoint and retry-window regressions over real owner files.
use super::pipeline_tests::Fixture;
use super::tests::{checkpoint_put, fail_stop_without_checkpoint, reopen_result};
use super::*;

#[test]
fn sui_database_document_selects_history_mode_and_preserves_legacy_default() {
    let fixture = Fixture::new();
    let mut document = serde_json::to_value(&fixture.config).unwrap();
    assert_eq!(document["history_persistence"], "wal_replay");
    let config: RemoteDatabaseConfig = serde_json::from_value(document.clone()).unwrap();
    assert_eq!(config.history_persistence, HistoryPersistence::WalReplay);
    document
        .as_object_mut()
        .unwrap()
        .remove("history_persistence");
    let config: RemoteDatabaseConfig = serde_json::from_value(document).unwrap();
    assert_eq!(config.history_persistence, HistoryPersistence::OnDisk);
}

#[test]
fn wal_replay_retry_older_than_window_is_written_again_and_newest_wins() {
    let mut fixture = Fixture::new_with_capacity(8192);
    fixture.config.max_index_bytes = 64 * 1024 * 1024;
    let mut db = fixture.open();
    let operation = fixture.operation(0, 0);
    let original = db
        .write_batch(operation, vec![checkpoint_put(1, 1)])
        .unwrap();
    for n in 1..5000 {
        db.write_batch(fixture.operation(0, n), vec![checkpoint_put(1, 2)])
            .unwrap();
    }
    db.flush_index().unwrap();
    assert!(db.core.publisher.lock().receipt_history.is_none());
    assert!(!db.core.path.join("receipt-history-v2").exists());
    assert_eq!(
        db.core.publisher.lock().recent.len(),
        WAL_REPLAY_WINDOW_BATCHES
    );
    assert!(!db.core.recent_identity(operation));
    let newest = db
        .write_batch(operation, vec![checkpoint_put(1, 1)])
        .unwrap();
    assert!(newest > original);
    assert_eq!(
        db.get(KeySpace(0), &1u32.to_be_bytes())
            .unwrap()
            .unwrap()
            .as_ref(),
        &[1; 32]
    );
    fail_stop_without_checkpoint(db);
    let mut reopened = fixture.open();
    assert_eq!(
        reopened
            .write_batch(operation, vec![checkpoint_put(1, 1)])
            .unwrap(),
        newest
    );
    assert_eq!(
        reopened
            .get(KeySpace(0), &1u32.to_be_bytes())
            .unwrap()
            .unwrap()
            .as_ref(),
        &[1; 32]
    );
    assert!(!reopened.core.path.join("receipt-history-v2").exists());
}

#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
#[test]
fn wal_replay_checkpointed_dataset_exports_all_keys_and_owner_restart_reads_them() {
    use crate::distributed::nvmeof::ExportConfig;
    use crate::distributed::tcp::SealedOwnerExport;
    use std::collections::HashSet;
    use std::os::unix::fs::FileExt;

    let mut fixture = Fixture::new_with_capacity(8192).synchronous();
    fixture.config.max_index_bytes = 64 * 1024 * 1024;
    fixture.config.read_cache_bytes = 0;
    let db = fixture.open();
    let epoch = db.epoch();
    for id in 0..5000u32 {
        db.write_batch(
            fixture.operation(0, u64::from(id)),
            vec![checkpoint_put(id, id as u8)],
        )
        .unwrap();
        if id == 2499 {
            db.flush_index().unwrap();
            // The position proof persists the owner root at this fragment.
            // Thus the restart window no longer starts at WAL offset zero.
            assert!(db.core.publisher.lock().positions[&(0, epoch)].replay_from > 0);
        }
    }
    db.shutdown().unwrap();
    drop(db);
    fixture.stop_owners();

    let owner = SealedOwnerExport::open(
        &fixture.startups[0].0,
        epoch,
        super::tests::recovery_metrics(),
    )
    .unwrap();
    assert_eq!(
        owner.log().history_persistence(),
        HistoryPersistence::WalReplay
    );
    let mut by_file: BTreeMap<u64, Vec<ValueRecordReference>> = BTreeMap::new();
    let mut ids = HashSet::new();
    let mut from = 0;
    loop {
        let page = owner.log().replay_page(from, 256, usize::MAX, 0).unwrap();
        for decoded in page.groups {
            for (batch, receipt) in decoded.group.batches().iter().zip(decoded.receipts) {
                let reference = owner.log().record_reference(receipt, 0).unwrap();
                let key = batch.mutations()[0].key();
                assert!(ids.insert(u32::from_be_bytes(key.as_ref().try_into().unwrap())));
                by_file
                    .entry(reference.batch_address.offset / fixture.config.storage.wal_file_bytes)
                    .or_default()
                    .push(reference);
            }
        }
        let Some(next) = page.next else { break };
        from = next;
    }
    assert_eq!(ids.len(), 5000);
    assert!((0..5000).all(|id| ids.contains(&id)));
    let mut exported = 0;
    for (file, references) in by_file {
        let mut uuid = [7; 16];
        uuid[..8].copy_from_slice(&file.to_le_bytes());
        let directory = fixture.temp.path().join(format!("offline-export-{file}"));
        let manifest = owner
            .export_sealed_readset(
                &directory,
                &references,
                &ExportConfig {
                    export_uuid: uuid,
                    nqn: format!("nqn.2026-01.test:checkpointed-{file}"),
                    nsid: 1,
                    block_size: 512,
                    max_catalog_bytes: references.len() * 4096 + 16384,
                },
            )
            .unwrap();
        assert_eq!(manifest.catalog.len(), references.len());
        let image = fs::File::open(directory.join("image.wal")).unwrap();
        for (entry, reference) in manifest.catalog.iter().zip(&references) {
            let mut bytes = vec![0; entry.len];
            image.read_exact_at(&mut bytes, entry.offset).unwrap();
            let mutation =
                CompleteBatch::decode_record(&fixture.shape, reference, bytes.into()).unwrap();
            let id = u32::from_be_bytes(mutation.key().as_ref().try_into().unwrap());
            assert_eq!(mutation, checkpoint_put(id, id as u8));
            exported += 1;
        }
    }
    assert_eq!(exported, 5000);
    drop(owner);
    fixture.restart_owners().unwrap();
    let mut reopened = fixture.open();
    for id in [0u32, 1, 899, 2499, 2500, 4095, 4096, 4999] {
        assert_eq!(
            reopened
                .get(KeySpace(0), &id.to_be_bytes())
                .unwrap()
                .unwrap()
                .as_ref(),
            &[id as u8; 32]
        );
    }
    reopened.shutdown().unwrap();
}

#[test]
fn wal_replay_mode_mismatch_fails_open() {
    let mut fixture = Fixture::new();
    let db = fixture.open();
    fail_stop_without_checkpoint(db);
    fixture.config.history_persistence = HistoryPersistence::OnDisk;
    assert!(
        reopen_result(&fixture)
            .err()
            .unwrap()
            .to_string()
            .contains("authority manifest history persistence differs")
    );

    let fixture = Fixture::new();
    fixture.nodes[0]
        .set_history_persistence(HistoryPersistence::OnDisk)
        .unwrap();
    assert!(
        reopen_result(&fixture)
            .err()
            .unwrap()
            .to_string()
            .contains("owner history persistence differs from the coordinator")
    );
}

#[test]
fn wal_replay_reopen_fails_when_owner_wal_ends_below_checkpoint_position() {
    let mut fixture = Fixture::new();
    let db = fixture.open();
    db.write_batch(fixture.operation(0, 1), vec![checkpoint_put(1, 7)])
        .unwrap();
    db.flush_index().unwrap();
    let position = db.core.publisher.lock().positions[&(0, 1)].clone();
    assert!(position.durable_end > 0);
    fail_stop_without_checkpoint(db);
    fixture.stop_owners();
    let file = fixture
        .temp
        .path()
        .join("node-0/epoch_0000000000000001/wal_0000000000000000");
    fs::OpenOptions::new()
        .write(true)
        .open(file)
        .unwrap()
        .set_len(0)
        .unwrap();
    // The owner scan must fail below synced_end. There is no serving coordinator.
    let error = fixture.restart_owners().err().unwrap();
    assert!(error.to_string().contains("UnexpectedEof"), "{error}");
}

#[test]
fn wal_replay_replay_completion_is_not_committed_before_the_next_image() {
    let fixture = Fixture::new();
    let db = fixture.open();
    db.write_batch(fixture.operation(0, 1), vec![checkpoint_put(1, 1)])
        .unwrap();
    db.flush_index().unwrap();
    assert!(!db.core.publisher.lock().positions[&(0, 1)].complete);
    fail_stop_without_checkpoint(db);
    let db = fixture.open();
    assert_eq!(db.recovery_stats().tail_batches, 0);
    assert!(db.core.publisher.lock().positions[&(0, 1)].complete);
    assert!(
        !db.core
            .publisher
            .lock()
            .committed_complete
            .contains(&(0, 1))
    );
    // A second crash before an image adopts completion repeats the old scan.
    fail_stop_without_checkpoint(db);
    let mut db = fixture.open();
    assert_eq!(db.recovery_stats().tail_batches, 0);
    assert!(
        !db.core
            .publisher
            .lock()
            .committed_complete
            .contains(&(0, 1))
    );
    db.write_batch(fixture.operation(0, 2), vec![checkpoint_put(1, 2)])
        .unwrap();
    db.flush_index().unwrap();
    assert!(
        db.core
            .publisher
            .lock()
            .committed_complete
            .contains(&(0, 1))
    );
    assert_eq!(
        db.get(KeySpace(0), &1u32.to_be_bytes())
            .unwrap()
            .unwrap()
            .as_ref(),
        &[2; 32]
    );
}

#[test]
fn wal_replay_retires_sealed_file_only_after_complete_image() {
    let mut fixture = Fixture::new_with_capacity(256);
    fixture.config.max_index_bytes = 16 * 1024 * 1024;
    let db = fixture.open();
    for n in 0..80 {
        db.write_batch(fixture.operation(0, n), vec![checkpoint_put(1, n as u8)])
            .unwrap();
    }
    db.flush_index().unwrap();
    let file = fixture
        .temp
        .path()
        .join("node-0/epoch_0000000000000001/wal_0000000000000000");
    assert!(file.exists());
    fail_stop_without_checkpoint(db);
    let db = fixture.open();
    assert!(db.core.publisher.lock().positions[&(0, 1)].complete);
    assert!(
        !db.core
            .publisher
            .lock()
            .committed_complete
            .contains(&(0, 1))
    );
    super::checkpoint_lifecycle::core_retire_unreachable_sealed_files(&db.core).unwrap();
    assert!(file.exists());
    db.write_batch(fixture.operation(0, 81), vec![checkpoint_put(1, 99)])
        .unwrap();
    db.core.retire_history_before(IndexReadCut::Latest).unwrap();
    assert!(
        db.core
            .publisher
            .lock()
            .committed_complete
            .contains(&(0, 1))
    );
    assert!(!file.exists());
    fail_stop_without_checkpoint(db);
    let mut db = fixture.open();
    assert_eq!(
        db.get(KeySpace(0), &1u32.to_be_bytes())
            .unwrap()
            .unwrap()
            .as_ref(),
        &[99; 32]
    );
}

#[test]
fn wal_replay_checkpoint_timeline_has_control_without_receipt_io() {
    let fixture = Fixture::new();
    let db = fixture.open();
    db.core.write_timeline.arm(1, 1).unwrap();
    db.write_batch(fixture.operation(0, 1), vec![checkpoint_put(1, 1)])
        .unwrap();
    db.flush_index().unwrap();
    let timeline = db.core.write_timeline.finish().unwrap();
    let stages: Vec<_> = timeline
        .checkpoint_events
        .iter()
        .map(|event| event.stage)
        .collect();
    assert!(stages.contains(&"control_enqueue"));
    assert!(stages.contains(&"control_observed"));
    assert!(!stages.iter().any(|stage| stage.starts_with("receipt_")));
}

#[test]
fn wal_replay_pending_budget_requires_one_whole_group_per_owner() {
    let fixture = Fixture::new();
    let mut config = fixture.config.clone();
    config.max_pending_bytes =
        config.storage.group_limits.max_encoded_bytes * config.endpoints.len() - 1;
    assert!(
        config
            .validate()
            .unwrap_err()
            .to_string()
            .contains("pending byte budget is below one replay group per owner")
    );
    config.max_pending_bytes = 64 * 1024 * 1024;
    config.storage.group_limits.max_encoded_bytes = 4 * 1024 * 1024;
    // KV preparation: five groups of at most 4 MiB fit in 64 MiB.
    assert_eq!(
        config.endpoints.len() * config.storage.group_limits.max_encoded_bytes,
        20 * 1024 * 1024
    );
    assert!(config.recovery_page_limit().is_ok());
    // Sui deployment uses five 12 MiB groups and a 128 MiB pending budget.
    config.max_pending_bytes = 128 * 1024 * 1024;
    config.storage.group_limits.max_encoded_bytes = 12 * 1024 * 1024;
    assert_eq!(
        config.endpoints.len() * config.storage.group_limits.max_encoded_bytes,
        60 * 1024 * 1024
    );
    assert!(config.recovery_page_limit().is_ok());
}

#[test]
fn wal_replay_second_crash_repeats_suffix_before_committing_completion() {
    let fixture = Fixture::new();
    let db = fixture.open();
    db.write_batch(fixture.operation(0, 1), vec![checkpoint_put(1, 1)])
        .unwrap();
    db.flush_index().unwrap();
    for n in 2..5 {
        db.write_batch(fixture.operation(0, n), vec![checkpoint_put(1, n as u8)])
            .unwrap();
    }
    fail_stop_without_checkpoint(db);
    let error = RemoteDatabase::open_inner(
        &fixture.temp.path().join("authority"),
        &fixture.shape,
        fixture.config.clone(),
        None,
        Some(OpenInterruption::DuringReplay),
    )
    .err()
    .unwrap();
    assert!(
        error
            .to_string()
            .contains("injected interruption during WAL replay")
    );
    let mut reopened = fixture.open();
    assert_eq!(reopened.recovery_stats().tail_batches, 3);
    assert_eq!(
        reopened
            .get(KeySpace(0), &1u32.to_be_bytes())
            .unwrap()
            .unwrap()
            .as_ref(),
        &[4; 32]
    );
}

#[test]
fn wal_replay_native_owner_loss_beyond_durable_end_keeps_checkpoint() {
    use std::os::unix::fs::FileExt;
    let mut fixture = Fixture::new();
    let db = fixture.open();
    db.write_batch(fixture.operation(0, 1), vec![checkpoint_put(1, 1)])
        .unwrap();
    db.flush_index().unwrap();
    let frame_end = fixture.nodes[0].log_for_test(1).appended_frontier();
    assert!(frame_end >= db.core.publisher.lock().positions[&(0, 1)].durable_end);
    let operation = fixture.operation(0, 2);
    let lost = db
        .write_batch(operation, vec![checkpoint_put(1, 2)])
        .unwrap();
    fail_stop_without_checkpoint(db);
    fixture.stop_owners();
    // Simulate lost native-tier page-cache bytes, not a synchronous-tier loss.
    let path = fixture
        .temp
        .path()
        .join("node-0/epoch_0000000000000001/wal_0000000000000000");
    let file = fs::OpenOptions::new().write(true).open(path).unwrap();
    file.write_all_at(&[0; 8], frame_end).unwrap();
    file.sync_all().unwrap();
    drop(file);
    fixture.restart_owners().unwrap();
    let mut reopened = fixture.open();
    assert_eq!(reopened.recovery_stats().tail_batches, 0);
    assert_eq!(
        reopened
            .get(KeySpace(0), &1u32.to_be_bytes())
            .unwrap()
            .unwrap()
            .as_ref(),
        &[1; 32]
    );
    assert!(
        reopened
            .write_batch(operation, vec![checkpoint_put(1, 2)])
            .unwrap()
            > lost
    );
}

#[test]
fn wal_replay_short_sealed_stream_and_wrong_image_format_fail_open() {
    for wrong_format in [false, true] {
        let fixture = Fixture::new();
        let db = fixture.open();
        db.write_batch(fixture.operation(0, 1), vec![checkpoint_put(1, 1)])
            .unwrap();
        db.flush_index().unwrap();
        let path = db.core.path.clone();
        fail_stop_without_checkpoint(db);
        let mut manifest: AuthorityManifest = read_metadata(&path.join(MANIFEST)).unwrap();
        let commit = manifest.checkpoint.as_mut().unwrap();
        let mut image: CheckpointImageV3 =
            read_checkpoint(&path.join(commit.metadata_name())).unwrap();
        if wrong_format {
            image.format = 2;
        } else {
            image
                .positions
                .iter_mut()
                .find(|position| position.partition == 0)
                .unwrap()
                .durable_end += 4096;
        }
        let encoded = encode_checkpoint(&image).unwrap();
        commit.metadata_bytes = encoded.len() as u64;
        commit.metadata_digest = Blake2b::<U32>::digest(&encoded).into();
        install_metadata(&path, &commit.metadata_name(), &encoded).unwrap();
        persist_manifest(&path, &manifest).unwrap();
        let error = reopen_result(&fixture).err().unwrap().to_string();
        assert!(
            error.contains(if wrong_format {
                "checkpoint image format differs from the history persistence"
            } else {
                "sealed WAL ends below the required position"
            }),
            "{error}"
        );
    }
}

#[cfg(feature = "test-utils")]
#[test]
fn memory_only_coordinator_keeps_format_two_and_receipt_history() {
    let fixture = Fixture::new().history(HistoryPersistence::MemoryOnly);
    let db = fixture.open();
    let operation = fixture.operation(0, 1);
    let version = db
        .write_batch(operation, vec![checkpoint_put(1, 1)])
        .unwrap();
    db.flush_index().unwrap();
    assert!(
        db.core
            .path
            .join("receipt-history-v2/nodes.pages")
            .is_file()
    );
    let metadata_name = db
        .core
        .publisher
        .lock()
        .manifest
        .checkpoint
        .as_ref()
        .unwrap()
        .metadata_name();
    let image: CheckpointImage = read_checkpoint(&db.core.path.join(metadata_name)).unwrap();
    assert_eq!(image.format, 2);
    fail_stop_without_checkpoint(db);
    let db = fixture.open();
    assert_eq!(
        db.write_batch(operation, vec![checkpoint_put(1, 1)])
            .unwrap(),
        version
    );
}
