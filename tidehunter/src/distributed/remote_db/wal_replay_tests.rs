//! WAL-position checkpoint and retry-window regressions over real owner files.
use super::pipeline_tests::Fixture;
use super::tests::{checkpoint_put, fail_stop_without_checkpoint, reopen_result};
use super::*;

#[test]
fn removed_mode_keys_fail_config_parse() {
    let fixture = Fixture::new();
    let document = serde_json::to_value(&fixture.config).unwrap();
    for key in ["history_persistence", "checkpoint_mode"] {
        assert!(document.get(key).is_none());
        let mut legacy = document.clone();
        legacy
            .as_object_mut()
            .unwrap()
            .insert(key.into(), "on_disk".into());
        let error = serde_json::from_value::<RemoteDatabaseConfig>(legacy)
            .unwrap_err()
            .to_string();
        assert!(error.contains("unknown field"), "{key}: {error}");
    }
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

#[test]
fn wal_replay_checkpointed_dataset_owner_restart_reads_all_keys() {
    // Leased extents hold many frames only with room: at the default 4 KiB
    // fragment and 8 KiB file every write would lease a fresh extent and every
    // second write would roll a WAL file.
    let mut fixture = Fixture::new_with_config(
        8192,
        KeyShape::new_single(4, 1, crate::key_shape::KeyType::uniform(1)),
        2,
        |config| {
            config.storage.fragment_bytes = 64 * 1024;
            config.storage.wal_file_bytes = 1024 * 1024;
        },
    )
    .synchronous();
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
    // The owner must fail before serving: the leased target binding detects
    // the truncated backing file before the WAL scan reaches its end.
    let error = fixture.restart_owners().err().unwrap();
    assert!(
        error
            .to_string()
            .contains("WAL backing file identity, size or allocation changed"),
        "{error}"
    );
}

/// The scan half of the test above: the WAL file keeps its identity, size
/// and allocation (so the leased target binding accepts it), but its frames
/// below the persisted position root are gone, as after dropped pages on a
/// preallocated file. The owner scan finds no frame where the root says one
/// ends and fails before it serves; there is no serving coordinator.
#[test]
fn wal_replay_reopen_fails_when_owner_wal_content_ends_below_checkpoint_position() {
    let mut fixture = Fixture::new();
    let db = fixture.open();
    db.write_batch(fixture.operation(0, 1), vec![checkpoint_put(1, 7)])
        .unwrap();
    db.flush_index().unwrap();
    let position = db.core.publisher.lock().positions[&(0, 1)].clone();
    assert!(position.durable_end > 0);
    fail_stop_without_checkpoint(db);
    fixture.stop_owners();
    let file = fs::OpenOptions::new()
        .write(true)
        .open(
            fixture
                .temp
                .path()
                .join("node-0/epoch_0000000000000001/wal_0000000000000000"),
        )
        .unwrap();
    let len = file.metadata().unwrap().len();
    std::os::unix::fs::FileExt::write_all_at(&file, &vec![0; len as usize], 0).unwrap();
    drop(file);
    let error = fixture.restart_owners().err().unwrap();
    assert!(
        error
            .to_string()
            .contains("replay position is not a frame boundary"),
        "{error}"
    );
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
    let error = RemoteDatabase::open_inner_local_fixture(
        &fixture.temp.path().join("authority"),
        &fixture.shape,
        fixture.config.clone(),
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
        // The writer emits the callback-start wrapper (format 4).
        let mut image: CheckpointImageV4 =
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
        persist_manifest(&path, &manifest, CoordinatorIoCategory::Open).unwrap();
        let error = reopen_result(&fixture).err().unwrap().to_string();
        assert!(
            error.contains(if wrong_format {
                "checkpoint image format is not a WAL-replay format (3 or 4)"
            } else {
                "sealed WAL ends below the required position"
            }),
            "{error}"
        );
    }
}
