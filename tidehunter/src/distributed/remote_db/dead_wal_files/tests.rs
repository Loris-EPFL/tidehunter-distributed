//! The retirement snapshot is fail-closed: a stream with no proof gets no
//! watermark, and a reader latch below the stored cut defers every stream.
//!
//! Negative controls are mutation runs of production code:
//! * `take_snapshot`: replace the `positions.get(&key)` `else { continue }` by a
//!   default of `0`/`watermark.end` and `a_stream_without_a_position_gets_no_watermark`
//!   fails (the stream would then be proven dead without evidence);
//! * `take_snapshot`: remove the `filter(|_| !held_below_stored)` and
//!   `a_reader_latch_below_the_stored_cut_defers_every_stream` fails.

use super::super::pipeline_tests::Fixture;
use super::super::tests::checkpoint_put;
use super::*;
use crate::distributed::hybrid::position::VersionPosition;
use crate::distributed::types::BatchVersion;
use crate::wal::position::IndexCut;
use std::sync::Arc;

/// An empty relocation shadow witness: these tests exercise the watermark and
/// latch terms, not the shadow cap (`dead_wal_files_tests.rs` and
/// `relocation_promoted_l1_tests.rs` cover that end to end).
fn no_witness() -> RetirementGate {
    RetirementGate::default()
}

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
    let snapshot = take_snapshot(&db.core, &mut no_witness());
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
    let snapshot = take_snapshot(&db.core, &mut no_witness());
    assert!(
        snapshot.watermark.is_empty() && snapshot.durable_end.is_empty(),
        "no position is no proof: nothing may be proven dead"
    );
}

#[test]
fn a_reader_latch_below_the_stored_cut_defers_every_stream() {
    let (_fixture, db) = stored();
    assert!(
        !take_snapshot(&db.core, &mut no_witness())
            .watermark
            .is_empty()
    );
    // A reader pinned below the stored cut still needs older versions that the
    // relocation pass never copied.
    let latch = db.core.table.pin_cut(<BatchVersion as IndexCut>::NONE);
    assert!(db.core.table.cuts().lowest_held() < Some(db.core.table.stored_cut()));
    let held = take_snapshot(&db.core, &mut no_witness());
    assert!(
        held.watermark.is_empty(),
        "no stream retires while a latch is below the stored cut"
    );
    drop(latch);
    assert!(
        !take_snapshot(&db.core, &mut no_witness())
            .watermark
            .is_empty(),
        "the next pass after the latch drops proceeds"
    );
}

fn caps(db: &super::super::RemoteDatabase) -> u64 {
    db.core
        .retirement_latch_caps
        .load(std::sync::atomic::Ordering::Relaxed)
}

/// (a) A checkpoint held across several cadence and forced stores and several
/// retirement decisions never trips the cap: `stored_target` is
/// `max(previous, min(durable, applied, lowest held))`, so the stored cut stays
/// at or below the held cut and no latch is ever below it.
///
/// Negative control (mutation run): make `stored_target` ignore the held latch
/// (`lowest_held` replaced by `None` in the call of `next_stored_target`): the
/// stored cut passes the held cut, `take_snapshot` caps, and `caps == 0` fails.
#[test]
fn a_held_checkpoint_across_stores_and_retirement_passes_never_trips_the_cap() {
    let (fixture, db) = stored();
    let (held, latch) = db.core.table.pin_checkpoint();
    for round in 0..3u64 {
        for seed in 0..4u64 {
            let seed = 10 * (round + 1) + seed;
            db.write_batch(
                fixture.operation(0, seed),
                vec![checkpoint_put(seed as u32, 3)],
            )
            .expect("write");
        }
        db.core.flush_durable().expect("certify");
        db.flush_index()
            .expect("cadence store under the held checkpoint");
        let forced = db
            .core
            .table
            .snapshot_for_relocation(db.core.table.last_processed());
        assert!(forced.is_ok(), "{forced:?}");
        assert!(
            db.core.table.stored_cut() <= held,
            "the held checkpoint clamps every store"
        );
        drop(take_snapshot(&db.core, &mut no_witness()));
        assert_eq!(
            caps(&db),
            0,
            "round {round}: no latch is below the stored cut"
        );
    }
    drop(latch);
    db.flush_index().expect("store after the latch drops");
    drop(take_snapshot(&db.core, &mut no_witness()));
    assert_eq!(caps(&db), 0);
}

/// (b) The NONE window: `pin_checkpoint` holds the cut-NONE low latch from
/// before its sample to the end of the sample (`set_after_pin_sample` runs in
/// that window). A retirement decision taken exactly there sees
/// `lowest_held == NONE`, below every stored cut: it defers every stream and
/// counts one cap. The next pass, after the window, proceeds without a cap.
///
/// Negative control (mutation run): remove the `held_below_stored` filter in
/// `take_snapshot`: the in-window decision returns a watermark and the first
/// assertion fails; remove the counter increment and the count assertion fails.
#[test]
fn the_none_window_fires_the_cap_once_and_the_next_pass_proceeds() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let (fixture, db) = stored();
    assert_eq!(caps(&db), 0);
    let fired = Arc::new(AtomicBool::new(false));
    let in_window: Arc<parking_lot::Mutex<Option<bool>>> = Arc::new(parking_lot::Mutex::new(None));
    {
        let core = Arc::clone(&db.core);
        let fired = Arc::clone(&fired);
        let in_window = Arc::clone(&in_window);
        db.core.table.set_after_pin_sample(Box::new(move || {
            // Once: the hook stays installed for later passes.
            if !fired.swap(true, Ordering::AcqRel) {
                *in_window.lock() =
                    Some(take_snapshot(&core, &mut no_witness()).watermark.is_empty());
            }
        }));
    }
    // `pin_checkpoint` is the production sample-then-latch step; calling it
    // directly runs the hook inside its NONE window deterministically (no other
    // thread has to reach it).
    db.write_batch(fixture.operation(0, 2), vec![checkpoint_put(2, 2)])
        .expect("write");
    db.core.flush_durable().expect("certify");
    let (_, latch) = db.core.table.pin_checkpoint();
    drop(latch);
    assert!(
        fired.load(Ordering::Acquire),
        "the pass reached its pin sample"
    );
    assert_eq!(
        *in_window.lock(),
        Some(true),
        "the decision in the window defers"
    );
    assert_eq!(caps(&db), 1, "exactly one cap");
    assert!(
        !take_snapshot(&db.core, &mut no_witness())
            .watermark
            .is_empty(),
        "the next pass proceeds"
    );
    assert_eq!(caps(&db), 1, "and is not capped");
}

/// (c) A stale-q relocation latch: the pass froze `q`, the cadence store then
/// passed `q` between two cells (no latch at `q` was held at that instant), and
/// the next cell re-pins `q`, below the stored cut. The decision taken now is
/// capped (fail closed, counted); the stored cut does not regress under the
/// latch; after the drop the next pass proceeds.
///
/// Negative control (mutation run): remove the `held_below_stored` filter and
/// the capped decision returns a watermark.
#[test]
fn a_stale_q_relocation_latch_below_a_store_caps_retirement_until_it_drops() {
    let (fixture, db) = stored();
    let q = db.core.table.last_processed();
    for seed in 20..24u64 {
        db.write_batch(
            fixture.operation(0, seed),
            vec![checkpoint_put(seed as u32, 5)],
        )
        .expect("write");
    }
    db.core.flush_durable().expect("certify");
    // The cadence pass leaves a byte-fresh cell dirty and names its frontier, so
    // the store that passes `q` is the forced one (the cell is flushed at the
    // head): exactly what a relocation pass between two cells would cause.
    db.core
        .table
        .snapshot_for_relocation(db.core.table.last_processed())
        .expect("the forced store passes q");
    let stored_cut = db.core.table.stored_cut();
    assert!(
        stored_cut > q,
        "the store passed q: {stored_cut:?} vs {q:?}"
    );
    let latch = db.core.table.pin_cut(q);
    assert_eq!(caps(&db), 0);
    assert!(
        take_snapshot(&db.core, &mut no_witness())
            .watermark
            .is_empty(),
        "a latch below the stored cut defers every stream"
    );
    assert_eq!(caps(&db), 1);
    db.flush_index().expect("a store under the stale latch");
    assert!(
        db.core.table.stored_cut() >= stored_cut,
        "the stored cut never moves back under a latch"
    );
    drop(latch);
    assert!(
        !take_snapshot(&db.core, &mut no_witness())
            .watermark
            .is_empty(),
        "the next pass after the drop proceeds"
    );
    assert_eq!(caps(&db), 1);
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
        export_pins: Some(0),
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
    let snapshot = take_snapshot(&db.core, &mut no_witness());
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
    let snapshot = take_snapshot(&db.core, &mut no_witness());
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

/// The retained witness keeps, per stream, the lowest floor and the highest
/// shadowing version, and every retirement decision re-reads it: a later pass
/// with no new merge still caps, and pruning happens only when no held cut can
/// name the position any more.
///
/// This is the later-pass coverage at the decision boundary: the end-to-end
/// pass in `dead_wal_files_tests::a_no_work_pass_keeps_the_shadow_cap` and the
/// two-stream pass in `relocation_witness_tests.rs` cover the same rule through
/// a real database.
#[test]
fn the_witness_merges_per_stream_survives_passes_and_prunes_when_covered() {
    let file = 1024 * 1024u64;
    let version = |epoch: u64, sequence: u64| BatchVersion { epoch, sequence };
    let position =
        |partition: u32, generation: u64, offset: u64, batch: BatchVersion| VersionPosition {
            batch,
            operation_index: 0,
            payload_crc: 7,
            address: crate::distributed::types::PhysicalValueAddress {
                partition,
                segment_generation: generation,
                offset,
                len: 64,
            },
        };
    let mut gate = RetirementGate::default();
    gate.merge(&[
        (
            position(3, 1, 2 * file + 100, version(9, 40)),
            version(9, 80),
        ),
        (position(3, 1, file + 100, version(9, 40)), version(9, 60)),
        (position(3, 2, 4096, version(9, 30)), version(9, 50)),
        // An older generation's position: a serving-epoch latch is above it.
        (position(4, 7, 512, version(6, 5)), version(6, 500)),
    ]);

    // Under a latch at the lower shadowing version both streams are capped at
    // their own file: 0 for stream (3, 2). The lowest floor wins over the higher
    // one merged for stream (3, 1).
    let caps = gate.caps(Some(version(9, 50)), version(9, 99), file);
    assert_eq!(
        caps.get(&(3, 1)),
        Some(&file),
        "the merged floor {file} of stream (3, 1) survives, not the 2-file floor: {caps:?}"
    );
    assert_eq!(
        caps.get(&(3, 2)),
        Some(&0),
        "stream (3, 2) keeps its own floor: {caps:?}"
    );
    assert!(
        !caps.contains_key(&(4, 7)),
        "a serving-epoch latch is above an older generation's position: {caps:?}"
    );

    // No new merge, a higher latch: the decision repeats with the same witness.
    // A latch at 70 is still at or below stream (3, 1)'s HIGHEST shadowing
    // version (80, not the 60 merged later), so that stream stays capped;
    // stream (3, 2) (shadowing 50) is pruned, because its old position is no
    // longer reachable through that cut.
    let higher = gate.caps(Some(version(9, 70)), version(9, 99), file);
    assert_eq!(
        higher.get(&(3, 1)),
        Some(&file),
        "the highest shadowing version 80, not 60, decides the cap: {higher:?}"
    );
    assert!(
        !higher.contains_key(&(3, 2)),
        "a latch above 50 cannot name stream (3, 2)'s old position: {higher:?}"
    );

    // A latch above the highest shadowing version needs no cap, and the entry is
    // dropped for good.
    assert!(
        gate.caps(Some(version(9, 81)), version(9, 99), file)
            .is_empty(),
        "no held cut can name a position below an entry at 80"
    );
    assert!(
        gate.caps(None, version(9, 99), file).is_empty(),
        "the pruned stream is not resurrected"
    );

    // Without a latch, publication decides: an uncovered shadowing version is
    // capped and a covered one is not.
    gate.merge(&[(position(5, 1, file, version(9, 95)), version(9, 95))]);
    assert_eq!(
        gate.caps(None, version(9, 95), file).get(&(5, 1)),
        Some(&file),
        "a shadowing version at the published cut is not covered yet"
    );
    assert!(
        gate.caps(None, version(9, 96), file).is_empty(),
        "a covered shadowing version cannot be named by a latch created later"
    );
}
