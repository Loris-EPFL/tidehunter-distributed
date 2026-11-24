//! The retirement snapshot is fail-closed: a stream with no proof gets no
//! watermark. The watermark is the two stored terms alone, as native's
//! relocation GC: a held checkpoint adds no term.
//!
//! Negative control is a mutation run of production code: replace the
//! `positions.get(&key)` `else { continue }` by a default of
//! `0`/`watermark.end` and `a_stream_without_a_position_gets_no_watermark`
//! fails (the stream would then be proven dead without evidence).

use super::super::pipeline_tests::Fixture;
use super::super::tests::checkpoint_put;
use super::*;
use crate::distributed::types::BatchVersion;
use crate::wal::position::IndexCut;

/// A database with one acknowledged write and a stored control region that
/// names the serving stream.
fn stored() -> (Fixture, super::super::RemoteDatabase) {
    let fixture = Fixture::new();
    let db = fixture.open();
    db.write_batch(fixture.operation(0, 1), vec![checkpoint_put(1, 1)])
        .expect("write");
    db.core.flush_durable().expect("certify the write");
    db.flush_index().expect("store a region");
    assert_ne!(
        db.core.table.stored_cut(),
        <BatchVersion as IndexCut>::NONE,
        "a region was stored"
    );
    (fixture, db)
}

#[test]
fn a_stream_with_proof_gets_a_watermark() {
    let (_fixture, db) = stored();
    let snapshot = take_snapshot(&db.core);
    assert!(
        !snapshot.watermark.is_empty(),
        "a stream with a position and a stored front is covered"
    );
    assert_eq!(
        snapshot.watermark.keys().collect::<Vec<_>>(),
        snapshot.durable_end.keys().collect::<Vec<_>>(),
        "watermark and durable end name the same streams"
    );
}

#[test]
fn a_stream_without_a_position_gets_no_watermark() {
    let (_fixture, db) = stored();
    db.core
        .publisher
        .lock()
        .checkpoint_state()
        .positions
        .clear();
    let snapshot = take_snapshot(&db.core);
    assert!(
        snapshot.watermark.is_empty() && snapshot.durable_end.is_empty(),
        "no position is no proof: nothing may be proven dead"
    );
}

// Boundary tables (RustReview4 audit rows B19, B20, B21). A watermark `end` is
// the exclusive end of the dead prefix; a file's own end is exclusive; a seal's
// `last_batch_sequence` is an inclusive last and `persisted_cut_next` an
// exclusive next. Each table asserts limit - 1, limit and limit + 1.
//
// Negative controls (mutation runs of production code): B19 `file_end >
// watermark.end` -> `>=`; B20 `last >= next` -> `last > next`; B21 `watermark_end
// >= stream_end` -> `>` and `start < watermark_end` -> `<=`.

use super::super::file_inventory::TargetAccess;
use crate::distributed::storage::FileSeal;

/// A used, unsealed file at the start of the stream so the span budget has a
/// lower bound, then one durably sealed file right after it.
fn rows_with_one_sealed_file(
    size: u64,
    last_batch_sequence: Option<u64>,
) -> Vec<WalFileObservation> {
    let observation = |file_start: u64, state, seal| WalFileObservation {
        file_start,
        device: 3,
        inode: 100 + file_start / size,
        logical_bytes: size,
        allocated_bytes: size,
        state,
        allocator_past: true,
        write_lease: false,
        validated_through: Some(file_start + size),
        read_pins: Some(0),
        target_access: TargetAccess::None,
        seal,
    };
    vec![
        observation(0, FileUseState::Used, None),
        observation(
            size,
            FileUseState::Used,
            Some(FileSeal {
                start: size,
                end: 2 * size,
                device: 3,
                inode: 101,
                root_generation: 1,
                last_batch_sequence,
            }),
        ),
    ]
}

/// The proofs `victims` yields for `rows`: watermark end `end`, the persisted
/// cut `persisted_cut_next`, and a processed end far above every file so the
/// span budget (100 % in the default config) never binds.
fn victims_for(
    db: &super::super::RemoteDatabase,
    rows: &[WalFileObservation],
    end: u64,
    persisted_cut_next: Option<u64>,
) -> Vec<u64> {
    let size = db.core.config.storage.wal_file_bytes;
    let watermark = StreamWatermark {
        end,
        replay_from: end,
        callback_from: end,
    };
    victims(&db.core, rows, watermark, 16 * size, persisted_cut_next)
        .expect("victims")
        .into_iter()
        .map(|proof| proof.file_start)
        .collect()
}

/// B19: a sealed file is a victim iff its end is at or below the watermark end.
#[test]
fn a_file_is_a_victim_iff_its_end_is_at_or_below_the_watermark() {
    let (_fixture, db) = stored();
    let size = db.core.config.storage.wal_file_bytes;
    let rows = rows_with_one_sealed_file(size, Some(0));
    let file_end = 2 * size;
    for (end, victim) in [
        (file_end - 1, false),
        (file_end, true),
        (file_end + 1, true),
    ] {
        assert_eq!(
            victims_for(&db, &rows, end, None),
            if victim { vec![size] } else { Vec::new() },
            "watermark end {end} against file end {file_end}"
        );
    }
}

/// B20: a sealed file is withheld iff its last batch is at or above the
/// persisted Version B cut (`last` inclusive, `next` exclusive); a skip-only
/// file (`None`) and a missing cut (native tier) never withhold.
#[test]
fn a_file_is_withheld_iff_its_last_batch_is_at_or_above_the_persisted_cut() {
    let (_fixture, db) = stored();
    let size = db.core.config.storage.wal_file_bytes;
    let end = 8 * size;
    for (last, next, victim) in [
        (Some(4), Some(5), true),
        (Some(5), Some(5), false),
        (Some(6), Some(5), false),
        (None, Some(5), true),
        (Some(6), None, true),
    ] {
        let rows = rows_with_one_sealed_file(size, last);
        assert_eq!(
            victims_for(&db, &rows, end, next),
            if victim { vec![size] } else { Vec::new() },
            "last {last:?} against persisted next {next:?}"
        );
    }
}

/// B21: a sealed stream's file is dead when the watermark covers the whole
/// stream and the file starts below it (the last file's tail past the stream end
/// is the seal scan's suffix), otherwise only when the file's own end is at or
/// below the watermark.
#[test]
fn a_sealed_stream_file_is_dead_at_the_exact_coverage_boundaries() {
    const SIZE: u64 = 100;
    // (start, watermark_end, stream_end, dead)
    for (start, watermark_end, stream_end, dead) in [
        // Partial coverage: the file end (start + 100) decides, inclusive.
        (200, 299, 1000, false),
        (200, 300, 1000, true),
        (200, 301, 1000, true),
        // The full-coverage switch: stream end 250, the file at 200 ends at 300.
        (200, 249, 250, false),
        (200, 250, 250, true),
        (200, 251, 250, true),
        // Full coverage: the file must still start strictly below the watermark.
        (249, 250, 250, true),
        (250, 250, 250, false),
        // The file bound overflows: never dead under partial coverage.
        (u64::MAX - 1, 1000, u64::MAX, false),
    ] {
        assert_eq!(
            sealed_file_is_dead(start, SIZE, watermark_end, stream_end),
            dead,
            "start {start}, watermark {watermark_end}, stream end {stream_end}"
        );
    }
}

/// T5 (legitimate state): generation 1 writes and closes before any region
/// store. At the next open its start is seeded and no region names it. The
/// snapshot must give it no watermark (nothing can be proven dead without a
/// replay floor) and must not raise the invariant alarm that "every start has
/// a front": that alarm is valid only after this process stored a region.
///
/// Negative control (mutation run): restore the unconditional
/// `debug_assert_eq!(starts.keys(), fronts.keys())` in `take_snapshot`; this test
/// panics on the legitimate state. A second control replaces the `fronts.get`
/// skip by a default of `callback_from`: the stream gets a watermark with no
/// replay floor and the `watermark` assertion fails.
#[test]
fn a_generation_closed_before_any_region_store_gets_no_watermark_and_raises_no_alarm() {
    let fixture = Fixture::new();
    let db = fixture.open();
    let first = db.epoch();
    db.write_batch(fixture.operation(0, 1), vec![checkpoint_put(1, 1)])
        .expect("write");
    db.core.flush_durable().expect("certify the write");
    // A fail-stop writes no shutdown checkpoint, so no region names this
    // generation (`shutdown` stores one).
    super::super::tests::fail_stop_without_checkpoint(db);

    let db = fixture.open();
    let partition = db.core.config.endpoints[0].partition;
    let key = (partition, first);
    let fronts = db.core.table.stored_fronts();
    eprintln!(
        "open 2: region_loaded {} stored fronts {fronts:?}",
        db.core.table.region_loaded()
    );
    assert!(
        fronts.iter().all(|front| front.generation != first),
        "premise: no region names generation {first}: {fronts:?}"
    );
    let snapshot = take_snapshot(&db.core);
    eprintln!(
        "snapshot: watermark {:?} durable_end {:?}",
        snapshot.watermark, snapshot.durable_end
    );
    assert!(
        snapshot.durable_end.contains_key(&key),
        "premise: the stream has a position and a start: {:?}",
        snapshot.durable_end
    );
    assert!(
        !snapshot.watermark.contains_key(&key),
        "no replay floor, so no watermark: {:?}",
        snapshot.watermark
    );
}

/// T5 (after this process's first store): the region names every retained
/// generation, so the older stream has a watermark (the replay floor retirement
/// needs) and the snapshot stays quiet.
///
/// Negative control (mutation run): drop the `older` rows from `region_rows`;
/// the older stream has no watermark and the assertion fails (the restored
/// alarm would also fire here).
#[test]
fn after_the_first_store_every_retained_generation_has_a_watermark() {
    let fixture = Fixture::new();
    let db = fixture.open();
    let first = db.epoch();
    db.write_batch(fixture.operation(0, 1), vec![checkpoint_put(1, 1)])
        .expect("write");
    db.core.flush_durable().expect("certify the write");
    db.shutdown().expect("shutdown");
    drop(db);

    let db = fixture.open();
    let partition = db.core.config.endpoints[0].partition;
    db.write_batch(fixture.operation(0, 2), vec![checkpoint_put(2, 2)])
        .expect("write");
    db.core.flush_durable().expect("certify the write");
    // A cadence pass leaves a fresh dirty cell alone and stores nothing, so the
    // store is forced: `NONE` is below every relocation cut, hence the pass
    // flushes every dirty cell and writes a region for this process.
    let pass = db
        .core
        .snapshot_table_for_relocation(<BatchVersion as IndexCut>::NONE)
        .expect("this process's first store");
    assert!(pass.stored, "premise: the pass stored a region");
    let fronts = db.core.table.stored_fronts();
    assert!(
        fronts.iter().any(|front| front.generation == db.epoch()),
        "premise: the stored region names the serving generation: {fronts:?}"
    );
    let snapshot = take_snapshot(&db.core);
    eprintln!(
        "after the first store: fronts {fronts:?} watermark {:?}",
        snapshot.watermark
    );
    assert!(
        snapshot.watermark.contains_key(&(partition, first)),
        "generation {first} has a row, hence a watermark: {:?}",
        snapshot.watermark
    );
}
