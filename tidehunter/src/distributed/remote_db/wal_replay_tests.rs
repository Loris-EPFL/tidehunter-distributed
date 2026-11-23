//! WAL-position checkpoint and retry-window regressions over real owner files.
use super::pipeline_tests::Fixture;
use super::tests::{checkpoint_put, fail_stop_without_checkpoint};
use super::*;
use std::fs;

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
    // A retry window of `WAL_REPLAY_WINDOW_BATCHES` (4096) batches must be
    // exceeded, so this test writes just over 5000 batches. Give the owner room
    // for them: with the default 4 KiB fragment and 8 KiB WAL file every second
    // write rolls a file, so the run creates about 2500 WAL files. Each export
    // of a file verifies every registered object and rewrites and syncs the
    // whole live-target registry, in the write phase and again in replay, so
    // the write rate falls as the files accumulate and the run takes minutes.
    // A replay page can then pass the lifecycle deadline under disk
    // contention. The retry premise does not depend on the geometry.
    let fixture = Fixture::new_with_config(
        8192,
        KeyShape::new_single(4, 1, crate::key_shape::KeyType::uniform(1)),
        2,
        |config| {
            config.storage.fragment_bytes = 64 * 1024;
            config.storage.wal_file_bytes = 1024 * 1024;
        },
    );
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
    // `original` left the window above, so this retry is written again and its
    // newer version wins the read.
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
    // OWNER-ACCEPTED WEAKENING (`remote_db.rs` builds the publisher with an
    // empty `recent: ReceiptWindow::new()`, and the window fills only at
    // publication): the retry window is empty after every open, so a retry after
    // the reopen is a new write at the new epoch and does not answer with
    // `newest`. The put is idempotent, so the value is unchanged. The same
    // decision and shape appear in
    // `durable_cut_tests::extend_keeps_the_surviving_run_after_a_coordinator_crash`.
    let retried = reopened
        .write_batch(operation, vec![checkpoint_put(1, 1)])
        .unwrap();
    assert!(
        retried > newest,
        "a retry after reopen is a new write: {retried:?} must follow {newest:?}"
    );
    assert_eq!(
        retried,
        BatchVersion {
            epoch: reopened.epoch(),
            sequence: 0
        },
        "the retry is re-applied as the first batch of the new epoch"
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
            // Thus the restart window no longer starts at WAL offset zero: the
            // stored region row of this stream is the owner's tail, a byte start
            // above zero.
            let fronts = db.core.table.stored_fronts();
            let front = fronts
                .iter()
                .find(|front| front.partition == 0 && front.generation == epoch)
                .expect("the stored region names the serving generation");
            assert!(
                front.replay_from > 0,
                "the stored replay start of (0, {epoch}) is {}",
                front.replay_from
            );
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
        .owner_database_root(0)
        .join("epoch_0000000000000001/wal_0000000000000000");
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

#[test]
fn wal_replay_reopen_fails_when_owner_wal_is_short_below_its_durable_end() {
    let mut fixture = Fixture::new();
    let db = fixture.open();
    db.write_batch(fixture.operation(0, 1), vec![checkpoint_put(1, 7)])
        .unwrap();
    db.flush_index().unwrap();
    let position = db.core.publisher.lock().positions[&(0, 1)].clone();
    let end = position.durable_end;
    assert!(end > 1, "the durable end must cover the batch");
    fail_stop_without_checkpoint(db);
    fixture.stop_owners();
    let file = fixture
        .owner_database_root(0)
        .join("epoch_0000000000000001/wal_0000000000000000");
    fs::OpenOptions::new()
        .write(true)
        .open(file)
        .unwrap()
        .set_len(end - 1)
        .unwrap();
    // A nonzero truncation below the durable root must name the same cause
    // before the scan reads a frame the root says exists.
    let error = fixture.restart_owners().err().unwrap();
    assert!(
        error
            .to_string()
            .contains("WAL backing file identity, size or allocation changed"),
        "{error}"
    );
}

#[test]
fn empty_unallocated_successor_wal_file_reopens() {
    let mut fixture = Fixture::new();
    let db = fixture.open();
    db.write_batch(fixture.operation(0, 1), vec![checkpoint_put(1, 7)])
        .unwrap();
    db.flush_index().unwrap();
    let position = db.core.publisher.lock().positions[&(0, 1)].clone();
    assert!(position.durable_end > 0);
    fail_stop_without_checkpoint(db);
    fixture.stop_owners();
    let epoch = fixture
        .owner_database_root(0)
        .join("epoch_0000000000000001");
    let mut last = None;
    for entry in fs::read_dir(&epoch).unwrap() {
        let path = entry.unwrap().path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if let Some(index) = name
            .strip_prefix("wal_")
            .and_then(|index| u64::from_str_radix(index, 16).ok())
        {
            last = last.max(Some(index));
        }
    }
    let successor = epoch.join(format!("wal_{:016x}", last.expect("a WAL file") + 1));
    // `ensure_external_file` creates the successor at length 0 before
    // `ExternalPreparation::prepare` grows it. A crash or ENOSPC between the
    // two must leave an owner that still opens; the empty file lies above the
    // durable end, so it is a normal unallocated successor, not truncated data.
    fs::File::create(&successor).unwrap();
    assert_eq!(fs::metadata(&successor).unwrap().len(), 0);
    fixture.restart_owners().unwrap();
    let db = fixture.open();
    db.shutdown().unwrap();
}

/// The scan half of the test above: the WAL file keeps its identity, size
/// and allocation (so the leased target binding accepts it), but its frames
/// below the persisted position root are gone, as after dropped pages on a
/// preallocated file. The owner scan finds no frame where the root says one
/// ends and fails before it serves; there is no serving coordinator.
#[test]
fn wal_replay_reopen_fails_when_owner_wal_content_ends_below_checkpoint_position() {
    let mut fixture = Fixture::new();
    // The checkpoint covers a batch only when the cell's blob holds it. Pass 1
    // flushes a dirty cell only when it is stale by this threshold, so flush
    // every dirty cell at the checkpoint.
    fixture.config.index.snapshot_unload_threshold = 0;
    let db = fixture.open();
    db.write_batch(fixture.operation(0, 1), vec![checkpoint_put(1, 7)])
        .unwrap();
    // A native-tier checkpoint never forces an owner sync and `flush_index`
    // stores at `min(durable, applied, held)`. Make the batch durable first:
    // the checkpoint then stores the position root that the zero-filled WAL is
    // checked against.
    db.core.flush_durable().unwrap();
    db.flush_index().unwrap();
    let position = db.core.publisher.lock().positions[&(0, 1)].clone();
    assert!(position.durable_end > 0);
    fail_stop_without_checkpoint(db);
    fixture.stop_owners();
    let file = fs::OpenOptions::new()
        .write(true)
        .open(
            fixture
                .owner_database_root(0)
                .join("epoch_0000000000000001/wal_0000000000000000"),
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
    let mut fixture = Fixture::new();
    // The checkpoint covers a batch only when the cell's blob holds it. Pass 1
    // flushes a dirty cell only when it is stale by this threshold, so flush
    // every dirty cell at the checkpoint.
    fixture.config.index.snapshot_unload_threshold = 0;
    let db = fixture.open();
    db.write_batch(fixture.operation(0, 1), vec![checkpoint_put(1, 1)])
        .unwrap();
    // A native-tier checkpoint never forces an owner sync (A25; see
    // `tests::native_checkpoint_commits_no_image_while_an_owner_sync_is_stalled`)
    // and `flush_index` stores at `min(durable, applied, held)`. This fixture is
    // the native tier and its durable-cut record lags, so without this wait the
    // pass stores no region and the restart replays the whole WAL instead of the
    // suffix. Make the batch durable first: the checkpoint then covers it.
    db.core.flush_durable().unwrap();
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
fn wal_replay_durable_owner_loss_above_the_checkpoint_fails_closed() {
    // A checkpoint that covers a batch must not make recovery read a shorter
    // WAL when the owner also made a later frame durable: the lost frame is
    // acknowledged data, so the owner refuses the open instead of truncating
    // the stream. (The tolerated case, a loss beyond the durable end, needs the
    // owner not to name the frame; this test makes the later frame durable with
    // the explicit `flush_durable` barrier below, so the owner names it and the
    // tolerated case is unreachable here.
    // `wal_replay_reopen_fails_when_owner_wal_ends_below_checkpoint_position`
    // covers the refusal of a truncated stream.)
    //
    // The checkpoint covers batch 1 only when the cell's blob holds it, and the
    // cell flushes only when it is stale by the threshold.
    use std::os::unix::fs::FileExt;
    let mut fixture = Fixture::new();
    fixture.config.index.snapshot_unload_threshold = 0;
    let db = fixture.open();
    let first = db
        .write_batch(fixture.operation(0, 1), vec![checkpoint_put(1, 1)])
        .unwrap();
    // A native-tier checkpoint never forces an owner sync (A25; see
    // `tests::native_checkpoint_commits_no_image_while_an_owner_sync_is_stalled`)
    // and `flush_index` stores at `min(durable, applied, held)`. Make the batch
    // durable first: the checkpoint then covers it.
    db.core.flush_durable().unwrap();
    db.flush_index().unwrap();
    let stored = db.core.table.stored_cut();
    assert!(
        stored.sequence > first.sequence,
        "the stored cut {stored:?} must be above the checkpointed batch {first:?}"
    );
    let frame_end = fixture.nodes[0].log_for_test(1).appended_frontier();
    db.write_batch(fixture.operation(0, 2), vec![checkpoint_put(1, 2)])
        .unwrap();
    // Make the later frame durable too, so losing its bytes is a durable loss.
    db.core.flush_durable().unwrap();
    fail_stop_without_checkpoint(db);
    fixture.stop_owners();
    // Lost page-cache bytes leave an allocated frame whose header is zero.
    let path = fixture
        .owner_database_root(0)
        .join("epoch_0000000000000001/wal_0000000000000000");
    let file = fs::OpenOptions::new().write(true).open(path).unwrap();
    file.write_all_at(&[0; 8], frame_end).unwrap();
    file.sync_all().unwrap();
    drop(file);
    let error = fixture
        .restart_owners()
        .expect_err("a lost durable frame must refuse the owner open");
    assert!(
        error
            .to_string()
            .contains("replay position is not a frame boundary"),
        "unexpected owner open error: {error}"
    );
}

#[test]
fn a_corrupted_durable_value_is_refused_and_counted() {
    // End-to-end integrity: the index stores each member's payload CRC, so a
    // corrupted value on the owner is refused instead of served, and the
    // refusal is counted in the operator-visible `value_decode_failures`.
    let mut fixture = Fixture::new_with_config(
        8192,
        KeyShape::new_single(4, 1, crate::key_shape::KeyType::uniform(1)),
        2,
        |config| {
            config.storage.fragment_bytes = 64 * 1024;
            config.storage.wal_file_bytes = 1024 * 1024;
            // The read must reach the owner, not the coordinator cache.
            config.read_cache_bytes = 0;
        },
    );
    let mut db = fixture.open();
    db.write_batch(
        fixture.operation(0, 0),
        vec![checkpoint_put(7, 0xAB), checkpoint_put(8, 0xCD)],
    )
    .unwrap();
    db.core.flush_durable().unwrap();
    // Key 7's value is `[0xAB; 32]`; flip one of its durable bytes in the
    // owner's WAL. The owner serves member bytes from the file, so the next
    // read sees the damage while key 8's frame stays intact.
    let root = fixture.owner_database_root(0).to_path_buf();
    let marker = [0xABu8; 32];
    let mut flipped = false;
    for entry in std::fs::read_dir(root.join("epoch_0000000000000001")).unwrap() {
        let path = entry.unwrap().path();
        // The same value bytes can also sit in a control or metadata file;
        // flipping them there would fail the read for an unrelated reason, so
        // only the owner's `wal_` files are candidates.
        if !path
            .file_name()
            .is_some_and(|name| name.to_string_lossy().starts_with("wal_"))
        {
            continue;
        }
        let bytes = std::fs::read(&path).unwrap();
        if let Some(offset) = bytes
            .windows(marker.len())
            .position(|window| window == marker)
        {
            let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
            std::os::unix::fs::FileExt::write_all_at(&file, &[0xAA], offset as u64).unwrap();
            file.sync_all().unwrap();
            flipped = true;
            break;
        }
    }
    assert!(flipped, "the written value must be in the owner WAL");
    let error = db
        .get(KeySpace(0), &7u32.to_be_bytes())
        .expect_err("a corrupted durable value must be refused");
    assert!(
        error.to_string().contains("member payload CRC mismatch"),
        "unexpected error: {error}"
    );
    assert_eq!(
        db.reader.decode_failure_count(),
        1,
        "the CRC refusal is counted once"
    );
    assert_eq!(
        db.get(KeySpace(0), &8u32.to_be_bytes())
            .unwrap()
            .unwrap()
            .as_ref(),
        &[0xCD; 32],
        "an untouched frame still reads"
    );
    drop(db);
    fixture.stop_owners();
}
