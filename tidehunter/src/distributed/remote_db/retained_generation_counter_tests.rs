//! Counters of the retained-generation rows: each stays at zero on the
//! ordinary path and counts when its alarm condition is met.
//!
//! Kept apart from `retained_generation_tests` so that module stays under the
//! file-length cap; it shares that module's fixture and helpers.

use super::retained_generation_tests::{
    EXTRA, WRITES, epoch, fill, fixture_eager, open, open_report, partition, read_back,
    retire_generation_one, row, store, write_range,
};
use crate::distributed::hybrid::{SnapshotBoundary, inject_snapshot_fault};
use crate::distributed::sui::Db;
use std::sync::atomic::Ordering;

fn counters(db: &Db) -> (u64, u64, u64, u64) {
    let core = db.remote_for_test().write_core();
    let stats = db.remote_for_test().recovery_stats();
    (
        stats.old_generation_replayed_from_zero,
        stats.stale_region_rows_skipped,
        core.retirement_missing_rows.load(Ordering::Acquire),
        core.table.older_rows_without_end(),
    )
}

/// After a store that names generation 1, the next open replays no retained
/// generation from zero, skips no stale row, and every retained stream had a
/// known end when its row was built: all four counters are zero.
///
/// Negative control (mutation run): `older_streams()` returns nothing (no
/// carry). The region of generation 2 then lacks the row of generation 1 and
/// the open counts `old_generation_replayed_from_zero`.
#[test]
fn the_ordinary_retained_generation_path_leaves_every_alarm_counter_at_zero() {
    let fixture = fixture_eager();
    let db = open(&fixture);
    fill(&db);
    db.wait_for_background_threads_to_finish().unwrap();

    let db = open(&fixture);
    write_range(&db, WRITES..WRITES + EXTRA);
    let core = db.remote_for_test().write_core();
    core.flush_durable().expect("certify");
    core.flush_index().expect("region store");
    eprintln!("generation 2 after its store: counters {:?}", counters(&db));
    assert_eq!(counters(&db).3, 0, "every retained stream had a known end");
    db.wait_for_background_threads_to_finish().unwrap();

    let db = open(&fixture);
    open_report(&db, "open 3");
    assert_eq!(
        counters(&db),
        (0, 0, 0, 0),
        "(replayed from zero, stale rows skipped, missing rows, rows without end)"
    );
    read_back(&db, "open 3", 0..WRITES + EXTRA);
    db.wait_for_background_threads_to_finish().unwrap();
}

/// Trim counter: the open that trims generation 1 replays it first (trim is the
/// last step of an open) and loads a region that still names it, so it skips
/// nothing. The open after it, which no store separated from the trim, loads
/// that same region while the manifest no longer retains generation 1: the
/// stale row is skipped and counted exactly once, and no retained generation is
/// replayed from zero.
///
/// Negative control (mutation run): stop skipping the stale row at open (the
/// loader keeps it): the counter stays 0, or the open fails on the unknown
/// generation.
#[test]
fn the_stale_row_of_a_trimmed_generation_is_skipped_and_counted_once() {
    let (fixture, first, _second) = retire_generation_one();
    let db = open(&fixture);
    let (opened, _, _) = open_report(&db, "open 3");
    assert!(
        opened.iter().any(|(_, generation, _)| *generation == first),
        "premise: the loaded region names generation {first}: {opened:?}"
    );
    let (from_zero, skipped, _, _) = counters(&db);
    assert_eq!(
        (from_zero, skipped),
        (0, 0),
        "open 3 replays generation {first}, it is not stale"
    );
    read_back(&db, "open 3", 0..WRITES + EXTRA);
    // A clean close stores a region that no longer names generation 1. A
    // fail-stop stores none, so the region on disk is the one the trim left.
    db.remote_for_test()
        .write_core()
        .begin_failure(&super::LocalDbError::new(
            "injected fail-stop before reopen",
        ));
    drop(db);
    let db = open(&fixture);
    let (opened, _, _) = open_report(&db, "open 4");
    assert!(
        opened.iter().any(|(_, generation, _)| *generation == first),
        "premise: the region loaded at open 4 still names trimmed generation {first}: {opened:?}"
    );
    let (from_zero, skipped, missing, without_end) = counters(&db);
    eprintln!("open 4 counters: {from_zero} {skipped} {missing} {without_end}");
    assert_eq!(skipped, 1, "the stale row of generation {first}");
    // Only generation 3 replays from zero: it crashed before its first store
    // and has no row. Generation 1's stale row is skipped, not replayed.
    assert_eq!(
        from_zero, 1,
        "only the never-stored generation 3 replays from zero"
    );
    read_back(&db, "open 4", 0..WRITES + EXTRA);
    db.wait_for_background_threads_to_finish().unwrap();
}

/// A retained generation that never stored a region has no row: the open
/// replays it from zero and counts it once. Generation 1 fills and its only
/// store crashes before it writes the region (a clean close would store one).
///
/// Negative control (mutation run): leave `old_generation_replayed_from_zero`
/// uncounted at the `None` arm in `remote_db.rs`; the counter stays 0.
#[test]
fn a_retained_generation_without_a_stored_row_is_replayed_from_zero_and_counted_once() {
    let fixture = fixture_eager();
    let db = open(&fixture);
    let first = epoch(&db);
    fill(&db);
    let core = db.remote_for_test().write_core();
    inject_snapshot_fault(SnapshotBoundary::AfterFlush);
    core.flush_index()
        .expect_err("the only store of generation 1 crashes");
    drop(db);

    let db = open(&fixture);
    let (opened, tail_batches, tail_bytes) = open_report(&db, "open 2");
    assert!(
        opened.iter().all(|(_, generation, _)| *generation != first),
        "premise: no region names generation {first}: {opened:?}"
    );
    let (from_zero, skipped, missing, without_end) = counters(&db);
    eprintln!("open 2 counters: {from_zero} {skipped} {missing} {without_end}");
    assert_eq!(from_zero, 1, "generation {first} has no row");
    assert_eq!((skipped, missing, without_end), (0, 0, 0));
    assert!(
        tail_batches >= WRITES,
        "replayed from zero: every batch of the stream ({tail_batches} of {WRITES}, {tail_bytes} bytes)"
    );
    read_back(&db, "open 2", 0..WRITES);
    db.wait_for_background_threads_to_finish().unwrap();
}

/// A retained stream whose owner end the coordinator does not know is carried
/// at its previous row, counted, and never dropped from the region.
///
/// Negative control (mutation run): drop the `record_older_row_without_end`
/// call in `region_rows`; the counter stays 0.
#[test]
fn an_older_row_without_a_known_end_is_carried_counted_and_never_dropped() {
    let fixture = fixture_eager();
    let db = open(&fixture);
    let first = epoch(&db);
    fill(&db);
    db.wait_for_background_threads_to_finish().unwrap();

    let db = open(&fixture);
    let (opened, _, _) = open_report(&db, "open 2");
    let carried = row(&opened, first);
    assert!(
        carried.is_some(),
        "premise: generation {first} has a stored row: {opened:?}"
    );
    let core = db.remote_for_test().write_core();
    let removed = core
        .publisher
        .lock()
        .checkpoint_state()
        .positions
        .remove(&(partition(&db), first));
    assert!(
        removed.is_some(),
        "premise: the coordinator held a position for {first}"
    );
    write_range(&db, WRITES..WRITES + EXTRA);
    let rows = store(&db, "store without the end of generation 1", &[]);
    assert_eq!(
        row(&rows, first),
        carried,
        "carried, never advanced and never dropped: {rows:?}"
    );
    assert_eq!(
        counters(&db).3,
        1,
        "one older row was written without a known end"
    );
    db.wait_for_background_threads_to_finish().unwrap();
}
