//! A0 counters on a deterministic local checkpoint sequence: every per-row
//! byte and sync count is checked against the files the generation left.

use super::super::pipeline_tests::Fixture;
use super::super::tests::fail_stop_without_checkpoint;
use super::*;
use crate::key_shape::KeyType;

/// Sixteen single-cell mutexes; each key below lands in its own cell.
fn spread_fixture() -> Fixture {
    Fixture::new_with_shape(128, KeyShape::new_single(4, 16, KeyType::uniform(1)))
}

fn spread_key(index: u32) -> u32 {
    index * (u32::MAX / 16)
}

fn put(key: u32, value: u8) -> Mutation {
    Mutation::Put {
        keyspace: KeySpace(0),
        key: key.to_be_bytes().to_vec().into(),
        value: vec![value; 32].into(),
    }
}

fn write(fixture: &Fixture, db: &RemoteDatabase, seed: u64, key: u32, value: u8) {
    let partition = (seed % fixture.config.storage.partitions.len() as u64) as u32;
    db.write_batch(fixture.operation(partition, seed), vec![put(key, value)])
        .unwrap();
}

fn last_row(db: &RemoteDatabase) -> (CheckpointDiagnostic, CheckpointAttribution) {
    let record = *db
        .checkpoint_diagnostics_since(0)
        .records
        .last()
        .expect("one checkpoint row");
    (record, record.attribution.expect("attributed row"))
}

fn file_len(path: &Path) -> u64 {
    fs::metadata(path).unwrap().len()
}

fn installed(bytes: u64) -> FileIo {
    FileIo {
        bytes,
        file_syncs: 1,
        dir_syncs: 1,
        allocated_bytes: 0,
    }
}

/// `allocated_bytes` depends on a process-wide switch other tests may flip;
/// compare every other field exactly.
fn without_allocation(io: FileIo) -> FileIo {
    FileIo {
        allocated_bytes: 0,
        ..io
    }
}

#[test]
fn exact_generation_row_matches_the_files_it_wrote() {
    let fixture = spread_fixture();
    let db = fixture.open();
    db.arm_index_checkpoint_diagnostics();
    for index in 0..16 {
        write(
            &fixture,
            &db,
            u64::from(index),
            spread_key(index),
            index as u8,
        );
    }
    db.flush_index().unwrap();
    let (record, row) = last_row(&db);
    assert_eq!(row.status, GenerationStatus::Cleaned);
    assert_eq!(row.last_completed_stage, Some(CheckpointStage::Cleaned));
    assert!(row.trigger_flags.contains(CheckpointTriggerFlags::EXPLICIT));
    assert_eq!(row.open_id, db.recovery_stats().open_id);
    assert_ne!(row.open_id, 0);
    assert_eq!(row.fencing_generation, db.core.epoch);
    assert_eq!(row.database, fixture.config.storage.database);
    assert_eq!(
        record.generation,
        Some(db.core.publisher.lock().snapshot_generation)
    );

    // Publisher pair: the captured suffix is exactly the checked subtraction.
    let captured = TailCount::new(
        record.captured_batches.unwrap(),
        record.captured_bytes.unwrap(),
    );
    let (before, after) = (row.published_before.unwrap(), row.published_after.unwrap());
    assert_eq!(captured.batches, 16);
    assert_eq!(before.batches - after.batches, captured.batches);
    assert_eq!(before.bytes - after.bytes, captured.bytes);
    assert_eq!(row.sparse_beyond_cut, Some(0));
    // Admission pair: a separate population, released by the actual credits.
    let returned = row.returned.unwrap();
    let (granted_before, granted_after) = (row.granted_before.unwrap(), row.granted_after.unwrap());
    assert_eq!(returned.batches, captured.batches);
    assert_eq!(returned.bytes, captured.bytes);
    assert_eq!(
        granted_before.batches - granted_after.batches,
        returned.batches
    );
    assert_eq!(granted_before.bytes - granted_after.bytes, returned.bytes);

    // Cell identities hold without a second walk.
    let cells = row.cells.unwrap();
    assert!(
        cells.rewritten_cells > 1,
        "premise: keys span cells {cells:?}"
    );
    assert_eq!(
        cells.rewritten_cells,
        cells.buffered_cells + cells.streamed_cells
    );
    assert_eq!(
        cells.captured_cells,
        cells.reused_cells + cells.rewritten_cells + cells.empty_cells
    );
    assert_eq!(cells.root_cells, cells.reused_cells + cells.rewritten_cells);
    assert_eq!(cells.reused_cells, 0);

    // Bytes and syncs equal the files on disk.
    let commit = db
        .core
        .publisher
        .lock()
        .manifest
        .checkpoint
        .clone()
        .unwrap();
    let root = db.core.path.join(commit.index_name());
    let mut container = root.as_os_str().to_owned();
    container.push(".cells");
    assert_eq!(
        row.io.cells.bytes,
        cells.buffered_bytes + cells.streamed_bytes
    );
    assert_eq!(row.io.cells.bytes, file_len(Path::new(&container)));
    assert_eq!((row.io.cells.file_syncs, row.io.cells.dir_syncs), (1, 0));
    assert_eq!(without_allocation(row.io.root), installed(file_len(&root)));
    assert_eq!(
        without_allocation(row.io.wrapper),
        installed(commit.metadata_bytes)
    );
    assert_eq!(
        commit.metadata_bytes,
        file_len(&db.core.path.join(commit.metadata_name()))
    );
    assert_eq!(row.io.manifest_stage.file_syncs, 1);
    assert_eq!(row.io.manifest_adopt.file_syncs, 1);
    // There is no obsolete root to clean after the inaugural adoption.
    assert_eq!(row.io.manifest_cleanup, FileIo::default());
    assert_eq!(row.io.cleanup_names, FileIo::default());

    // Boundaries are ordered on one monotonic clock.
    let order = [
        Some(row.requested_at_ns),
        row.worker_started_at_ns,
        row.capture_start_at_ns,
        row.capture_end_at_ns,
        row.adopt_start_at_ns,
        row.adopt_end_at_ns,
        row.live_install_start_at_ns,
        row.live_install_end_at_ns,
        row.credits_returned_at_ns,
        row.cleanup_end_at_ns,
    ];
    let order: Vec<u64> = order.into_iter().map(Option::unwrap).collect();
    assert!(order.windows(2).all(|pair| pair[0] <= pair[1]), "{order:?}");
    assert_eq!(row.charged_index_at_capture_at_ns, row.capture_start_at_ns);
    assert_eq!(
        row.charged_index_at_credit_return_at_ns,
        row.credits_returned_at_ns
    );

    // Process-wide counters include at least this generation's work.
    let global = coordinator_io_snapshot();
    assert!(global.wrapper.bytes >= row.io.wrapper.bytes);
    assert!(global.root.bytes >= row.io.root.bytes);
    assert!(global.open.bytes > 0);
    let summary = db.checkpoint_diagnostic_summary().attribution;
    assert_eq!(summary.cleaned, 1);
    assert_eq!(summary.io, row.io.total());
    db.shutdown().unwrap();
}

#[test]
fn second_generation_reuses_untouched_cells_without_writing_them() {
    let fixture = spread_fixture();
    let db = fixture.open();
    db.arm_index_checkpoint_diagnostics();
    for index in 0..16 {
        write(&fixture, &db, u64::from(index), spread_key(index), 1);
    }
    db.flush_index().unwrap();
    let (_, first) = last_row(&db);
    write(&fixture, &db, 100, spread_key(3), 2);
    db.flush_index().unwrap();
    let (record, second) = last_row(&db);
    let (first_cells, second_cells) = (first.cells.unwrap(), second.cells.unwrap());
    assert_eq!(record.captured_batches, Some(1));
    assert_eq!(second_cells.rewritten_cells, 1);
    assert_eq!(
        second_cells.reused_cells,
        first_cells.root_cells - second_cells.rewritten_cells
    );
    assert!(second_cells.reused_frame_bytes > 0);
    // Reused frames are old bytes, never written again.
    assert_eq!(
        second.io.cells.bytes,
        second_cells.buffered_bytes + second_cells.streamed_bytes
    );
    assert!(second.io.cells.bytes < first.io.cells.bytes);
    // The second root supersedes the first. Cleanup syncs the unlink and
    // publishes the last authority image of the generation.
    assert_eq!(
        without_allocation(second.io.manifest_cleanup),
        installed(file_len(&db.core.path.join(MANIFEST)))
    );
    assert_eq!(
        second.io.cleanup_names,
        FileIo {
            dir_syncs: 1,
            ..FileIo::default()
        }
    );
    let summary = db.checkpoint_diagnostic_summary().attribution;
    assert_eq!(summary.cleaned, 2);
    assert_eq!(summary.reused_cells, second_cells.reused_cells);
    assert!(summary.min_capture_to_cleanup_ns <= summary.max_capture_to_cleanup_ns);
    db.shutdown().unwrap();
}

#[test]
fn failed_generation_keeps_last_boundary_and_observed_files() {
    let fixture = Fixture::new();
    let db = fixture.open();
    db.arm_index_checkpoint_diagnostics();
    write(&fixture, &db, 1, 1, 1);
    db.drain().unwrap();
    // Interruption 5: after the wrapper, before root adoption.
    db.core.test_hooks.lock().checkpoint_interruption = Some(5);
    assert!(db.flush_index().is_err());
    let (_, row) = last_row(&db);
    assert_eq!(row.status, GenerationStatus::Failed);
    assert_eq!(row.last_completed_stage, Some(CheckpointStage::Wrapper));
    assert!(row.io.manifest_stage.bytes > 0);
    assert!(row.io.root.bytes > 0);
    assert!(row.io.wrapper.bytes > 0);
    assert_eq!(row.io.manifest_adopt, FileIo::default());
    assert_eq!(row.adopt_start_at_ns, None);
    assert_eq!(row.live_install_start_at_ns, None);
    assert_eq!(row.returned, None);
    let summary = db.checkpoint_diagnostic_summary().attribution;
    assert_eq!((summary.cleaned, summary.failed), (0, 1));
    fail_stop_without_checkpoint(db);
}

#[test]
fn suffix_peaks_track_publication_and_reset_at_credit_return() {
    let fixture = Fixture::new();
    let db = fixture.open();
    db.arm_index_checkpoint_diagnostics();
    for seed in 0..3 {
        write(&fixture, &db, seed, seed as u32, 1);
    }
    db.drain().unwrap();
    let loaded = db.index_checkpoint_snapshot();
    assert!(loaded.armed);
    assert_eq!(loaded.suffix.batches, 3);
    assert_eq!(loaded.suffix.age_quality, AgeQuality::Exact);
    assert_eq!(loaded.suffix_peaks.batches.batches, 3);
    assert!(loaded.suffix_peaks.bytes.bytes >= loaded.suffix.bytes);
    assert!(loaded.granted.batches >= 3);
    assert!(loaded.granted_peak.batches >= 3);
    db.flush_index().unwrap();
    let drained = db.index_checkpoint_snapshot();
    assert_eq!(drained.suffix.batches, 0);
    assert_eq!(drained.suffix.oldest_age_ns, 0);
    assert_eq!(drained.suffix_peaks.batches.batches, 3);
    assert!(!drained.active_worker);
    assert_eq!(drained.hard_wait.any_hard_wait_ns, 0);
    db.shutdown().unwrap();
}

#[test]
fn reopen_reports_the_replayed_tail_under_a_new_open_id() {
    let fixture = Fixture::new();
    let db = fixture.open();
    let first_open = db.recovery_stats().open_id;
    for seed in 0..3 {
        write(&fixture, &db, seed, seed as u32, 1);
    }
    db.drain().unwrap();
    let tail_bytes = db.core.publisher.lock().checkpoint_tail_bytes as u64;
    fail_stop_without_checkpoint(db);
    let reopened = fixture.open();
    let recovery = reopened.recovery_stats();
    assert_ne!(recovery.open_id, first_open);
    assert_eq!(recovery.tail_batches, 3);
    assert_eq!(recovery.tail_bytes, tail_bytes);
    // WAL replay validates no receipt-prefix proof.
    assert_eq!(recovery.proof_validation_ns, 0);
    // The recovery baseline was adopted before serving.
    reopened.arm_index_checkpoint_diagnostics();
    assert_eq!(reopened.index_checkpoint_snapshot().suffix.batches, 0);
    reopened.shutdown().unwrap();
}

#[test]
fn suffix_age_bounds_follow_carried_records() {
    let mut tracker = SuffixTracker::armed(100, true);
    assert_eq!(
        tracker.sample(150, 1, 8).age_quality,
        AgeQuality::LowerBound
    );
    assert_eq!(tracker.sample(150, 1, 8).oldest_age_ns, 50);
    // Capture with nothing beyond the cut: retained records are post-capture.
    tracker.on_capture(false);
    tracker.on_publish(200, TailCount::new(2, 16), 64);
    tracker.on_install(true);
    let exact = tracker.sample(260, 1, 8);
    assert_eq!(
        (exact.oldest_age_ns, exact.age_quality),
        (60, AgeQuality::Exact)
    );
    // A sparse record carried over the next capture keeps the older time.
    tracker.on_capture(true);
    tracker.on_publish(300, TailCount::new(2, 16), 32);
    tracker.on_install(true);
    let carried = tracker.sample(400, 2, 16);
    assert_eq!(
        (carried.oldest_age_ns, carried.age_quality),
        (200, AgeQuality::UpperBound)
    );
    tracker.on_capture(false);
    tracker.on_install(false);
    let empty = tracker.sample(500, 0, 0);
    assert_eq!(
        (empty.oldest_age_ns, empty.age_quality),
        (0, AgeQuality::Exact)
    );
    assert_eq!(tracker.peaks().charged_index_bytes, 64);
    assert_eq!(tracker.peaks().batches.batches, 2);
}

#[test]
fn trigger_flags_name_hard_admission_reasons_only() {
    assert_eq!(
        CheckpointTriggerFlags::hard("identity"),
        CheckpointTriggerFlags::HARD_IDENTITY
    );
    assert_eq!(
        CheckpointTriggerFlags::hard("index"),
        CheckpointTriggerFlags::HARD_INDEX
    );
    assert!(CheckpointTriggerFlags::hard("tail_batches").is_empty());
    let flags = CheckpointTriggerFlags::EXPLICIT | CheckpointTriggerFlags::RECOVERY;
    assert_eq!(flags.names().collect::<Vec<_>>(), ["explicit", "recovery"]);
    let distinct: std::collections::BTreeSet<u32> = CheckpointTriggerFlags::NAMES
        .iter()
        .map(|(flag, _)| flag.bits())
        .collect();
    assert_eq!(distinct.len(), CheckpointTriggerFlags::NAMES.len());
}
