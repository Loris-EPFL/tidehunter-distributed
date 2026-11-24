//! Retained older fencing generations: the control region a later generation
//! stores must name every retained older generation, with a replay start the
//! cover rule justifies.
//!
//! Without the retained rows the first store of generation G named only G:
//! every older generation lost its row, so `generation_unused` could never cover
//! it, its files never retired, and every open replayed it from its first byte.
//!
//! Every test prints the rows, the verdict of `generation_unused` and the
//! replay volume at each step, then asserts. Each test names its negative
//! control.
//!
//! Cover rule under test: epoch and fencing generation are one counter, so every
//! version of generation `k` is below `{k + 1, 0}`. A store at `cut` with
//! `cut.epoch > k` has every batch of `k` in the stored blobs, so the row of
//! `k` is its owner `durable_end`; below that cut the row only carries what was
//! stored before (0 for a first row) and never moves back.

use super::fencing_generations::generation_unused;
use super::pipeline_tests::{Fixture, newest_cut};
use super::*;
use crate::distributed::codec::BatchLimits;
use crate::distributed::group::GroupLimits;
use crate::distributed::hybrid::{RelocationApply, SnapshotBoundary, inject_snapshot_fault};
use crate::distributed::sui::Db;
use crate::key_shape::{KeyShapeBuilder, KeySpaceConfig, KeyType};
use std::ops::Range;

// Support of the engine-level tests of this part: the leased ordinary-file
// fixture with the proven relocation geometry, a one-key-space data set,
// relocation passes and the stored-front accessors. `retained_generation_counter_tests`
// imports the same items from here.

pub(super) const KIB: u64 = 1024;
pub(super) const FILE: u64 = 1024 * KIB;
/// The geometry of the proven relocation fixture (`relocation_tests.rs`): two
/// fragments per 1 MiB file, batch and group limits below the fragment.
pub(super) const FRAGMENT: u64 = 512 * KIB;
pub(super) const VALUE: usize = 8 * KIB as usize;
/// About 2.4 MiB of 8 KiB values: more than two whole 1 MiB files, so a pass
/// has a complete file to reclaim.
pub(super) const WRITES: u64 = 300;
/// Quiet but reactive: the owner pollers sample their nudge flag every quarter
/// lag, so the lag must be short against the wait's production bound
/// (`8 x lag + timeout`, capped at 120 s) for the wait's own request to be
/// served. The timer nudge (a lag after the first unsynced receipt) is not what
/// the tests rely on: they assert on the wait's `targeted_syncs`.
const QUIET_LAG_MS: u64 = 10_000;
/// The longest durable-cut lag the configuration accepts (one hour): the
/// coordinator never asks the owner for a sync on its own while a test runs.
const IDLE_LAG_MS: u64 = 3_600_000;
/// Bound of every coordinator and owner operation of the fixture. It only
/// turns a stuck operation into a failure; no test relies on it expiring.
const OPERATION_TIMEOUT: Duration = Duration::from_secs(60);
/// Hang guard of the pause hooks: a hook that never engages fails the test
/// after this bound. No test relies on it expiring.
pub(super) const WAIT: Duration = Duration::from_secs(120);

/// One keyspace with no relocation filter: the WAL-based pass copies every
/// live entry below its stream's target and re-points it, which is what these
/// tests observe. A filtered keyspace copies nothing at the parity tip (D24),
/// so a filter would empty the copy path under test.
pub(super) fn shape() -> KeyShape {
    let mut builder = KeyShapeBuilder::new();
    builder.add_key_space_config("a", 8, 1, KeyType::uniform(1), KeySpaceConfig::new());
    builder.build()
}

/// A zero stale-unload window: every dirty cell is stale, so the ordinary
/// cadence store (`flush_index`) flushes the cells a replay dirtied and its cut
/// advances to the head. With the production window (7.5 GiB) such a cell
/// holds the cut back at the replay start, which is the native lag and not what
/// the retained-generation tests exercise.
pub(super) fn fixture_eager() -> Fixture {
    Fixture::new_with_root(
        tempdir::TempDir::new("hybrid-retained-generation").unwrap(),
        1,
        8192,
        shape(),
        32,
        |config| {
            config.index.snapshot_unload_threshold = 0;
            config.durable_cut = DurableCutConfig {
                lag_ms: QUIET_LAG_MS,
                ..DurableCutConfig::default()
            };
            config.storage.batch_limits = BatchLimits {
                max_operations: 32,
                max_encoded_bytes: 64 * KIB as usize,
            };
            config.storage.group_limits = GroupLimits {
                max_batches: 32,
                max_encoded_bytes: 256 * KIB as usize,
            };
            config.storage.fragment_bytes = FRAGMENT;
            config.storage.wal_file_bytes = FILE;
            config.timeout = OPERATION_TIMEOUT;
        },
        |_| {},
    )
}

pub(super) fn open(fixture: &Fixture) -> Arc<Db> {
    let db = Db::open_remote_local_fixture(
        &fixture.authority(),
        fixture.shape.clone(),
        fixture.config.clone(),
    )
    .expect("open");
    // `pass()` calls `relocation_pass` directly, so the relocator worker must be
    // idle: open only starts it when stream pressure is soft (sui.rs:726-734),
    // and no test in these modules calls `Db::start_relocation`. A fixture that
    // becomes soft at open must not use `pass()`; it needs the Busy-serialized
    // `run_for_test` route instead.
    assert!(
        !db.remote_for_test().stream_pressure().is_soft(),
        "fixture open at soft stream pressure: the relocator worker is live, so `pass()` \
         (a direct `relocation_pass`) can overlap it"
    );
    db
}

pub(super) fn key(sequence: u64) -> Vec<u8> {
    sequence.to_be_bytes().to_vec()
}

/// Writes the data set and certifies it: every later unsynced byte is a copy.
pub(super) fn fill(db: &Arc<Db>) -> KeySpace {
    let ks = db.ks("a");
    for sequence in 0..WRITES {
        let mut batch = db.write_batch();
        batch.write(ks, key(sequence), vec![(sequence % 251) as u8; VALUE]);
        batch.commit().expect("write");
    }
    db.remote_for_test()
        .write_core()
        .flush_durable()
        .expect("certify the data set");
    ks
}

pub(super) fn pass(db: &Db) -> crate::distributed::remote_db::relocation::RelocationReport {
    db.remote_for_test()
        .relocation_pass(false, &std::sync::atomic::AtomicBool::new(false))
        .expect("relocation pass")
}

/// One pass whose failure is returned, not raised: a test can assert what the
/// pass did before the failing step (the adoption of a callback start happens
/// before the retirement of the files it covers).
pub(super) fn try_pass(
    db: &Db,
) -> Result<crate::distributed::remote_db::relocation::RelocationReport, String> {
    db.remote_for_test()
        .relocation_pass(false, &std::sync::atomic::AtomicBool::new(false))
        .map_err(|error| format!("{error:?}"))
}

/// `(partition, generation, replay_from)` of every stored region front.
pub(super) fn stored_fronts(db: &Db) -> Vec<(u32, u64, u64)> {
    db.remote_for_test()
        .hybrid_table()
        .stored_fronts()
        .iter()
        .map(|front| (front.partition, front.generation, front.replay_from))
        .collect()
}

/// The fronts the open loaded from the region, and what its replay did:
/// `(fronts, tail_batches, tail_bytes)`.
pub(super) fn open_report(db: &Db, label: &str) -> (Vec<(u32, u64, u64)>, u64, u64) {
    let opened: Vec<_> = db
        .remote_for_test()
        .hybrid_table()
        .opened_fronts()
        .iter()
        .map(|front| (front.partition, front.generation, front.replay_from))
        .collect();
    let stats = db.remote_for_test().recovery_stats();
    eprintln!(
        "{label}: epoch {} opened fronts {opened:?} tail_batches {} tail_bytes {}",
        db.remote_for_test().write_core().epoch,
        stats.tail_batches,
        stats.tail_bytes
    );
    (opened, stats.tail_batches, stats.tail_bytes)
}

/// Batches generation 2 writes after the data set of generation 1.
pub(super) const EXTRA: u64 = 10;

pub(super) fn epoch(db: &Db) -> u64 {
    db.remote_for_test().write_core().epoch
}

pub(super) fn partition(db: &Db) -> u32 {
    db.remote_for_test().write_core().config.endpoints[0].partition
}

pub(super) fn write_range(db: &Arc<Db>, sequences: Range<u64>) {
    let ks = db.ks("a");
    for sequence in sequences {
        let mut batch = db.write_batch();
        batch.write(ks, key(sequence), vec![(sequence % 251) as u8; VALUE]);
        batch.commit().expect("write");
    }
}

/// Reads every key with `get` and compares the bytes: `exists` is index-only.
pub(super) fn read_back(db: &Db, label: &str, sequences: Range<u64>) {
    let ks = db.ks("a");
    for sequence in sequences {
        let value = db
            .get(ks, &key(sequence))
            .unwrap_or_else(|error| panic!("{label}: read {sequence}: {error:?}"))
            .unwrap_or_else(|| panic!("{label}: key {sequence} lost"));
        assert_eq!(
            value.as_ref(),
            vec![(sequence % 251) as u8; VALUE].as_slice(),
            "{label}: key {sequence} has the wrong bytes"
        );
    }
}

/// The owner `durable_end` the coordinator holds for a retained stream.
pub(super) fn durable_end(db: &Db, generation: u64) -> u64 {
    let partition = partition(db);
    db.remote_for_test()
        .write_core()
        .publisher
        .lock()
        .checkpoint_state()
        .positions
        .get(&(partition, generation))
        .unwrap_or_else(|| panic!("no position for generation {generation}"))
        .durable_end
}

/// The verdict trim asks for, from the same inputs `coordinator_eligible` uses.
fn unused(db: &Db, generation: u64) -> bool {
    let core = db.remote_for_test().write_core();
    let fronts = core.table.stored_fronts();
    let partitions: Vec<u32> = core
        .config
        .endpoints
        .iter()
        .map(|endpoint| endpoint.partition)
        .collect();
    let mut publisher = core.publisher.lock();
    let state = publisher.checkpoint_state();
    generation_unused(
        state.positions,
        state.callback_starts,
        &fronts,
        generation,
        &partitions,
    )
}

/// Certifies the writes and stores one region; prints the rows, the cut and the
/// verdict for `older`.
pub(super) fn store(db: &Db, label: &str, older: &[u64]) -> Vec<(u32, u64, u64)> {
    let core = db.remote_for_test().write_core();
    core.flush_durable().expect("certify");
    core.flush_index().expect("region store");
    let rows = stored_fronts(db);
    eprintln!(
        "{label}: epoch {} cut {:?} rows {rows:?}",
        core.epoch,
        core.table.stored_cut()
    );
    // F2: the rows are exactly the manifest's retained set plus the serving
    // generation, never the previous region's rows.
    let mut expected: Vec<(u32, u64)> = retained_generations(db)
        .into_iter()
        .chain(std::iter::once(core.epoch))
        .map(|generation| (partition(db), generation))
        .collect();
    expected.sort_unstable();
    expected.dedup();
    for &generation in older {
        eprintln!(
            "{label}: generation {generation}: durable_end {} generation_unused {}",
            durable_end(db, generation),
            unused(db, generation)
        );
    }
    assert_eq!(
        streams(&rows),
        expected,
        "{label}: the stored rows must be the manifest's retained set plus the serving generation"
    );
    rows
}

pub(super) fn row(rows: &[(u32, u64, u64)], generation: u64) -> Option<u64> {
    rows.iter()
        .find(|(_, row_generation, _)| *row_generation == generation)
        .map(|(_, _, replay_from)| *replay_from)
}

fn streams(rows: &[(u32, u64, u64)]) -> Vec<(u32, u64)> {
    let mut named: Vec<_> = rows
        .iter()
        .map(|(partition, generation, _)| (*partition, *generation))
        .collect();
    named.sort_unstable();
    named
}

/// The most an open can replay, from the rows it loaded: each retained older
/// stream is replayed from its row aligned down to a fragment
/// (`aligned_replay_start`) up to its owner `durable_end`. Replay from zero
/// would read the whole stream.
fn replay_bound(db: &Db, opened: &[(u32, u64, u64)]) -> u64 {
    let retained = retained_generations(db);
    opened
        .iter()
        .filter(|(_, generation, _)| retained.contains(generation) && *generation != epoch(db))
        .map(|(_, generation, from)| durable_end(db, *generation) - (from - from % FRAGMENT))
        .sum()
}

/// The two-reopen decider. Generation 1 writes and closes, generation 2
/// opens and stores, generation 3 opens and stores, generation 4 opens.
///
/// Without the retained rows the region of generation 2 names only generation
/// 2, so the stream set below is `[(p, 2)]` and generation 1 is replayed from
/// its first byte at every later open (`tail_batches >= WRITES`).
///
/// Negative control (mutation run): `region_rows` returns `table.stream_fronts()`
/// without the `older` rows (the code before the retained rows); the
/// stream-set assertion fails.
#[test]
fn a_region_stored_by_a_later_generation_names_every_retained_generation() {
    let fixture = fixture_eager();
    let db = open(&fixture);
    let first = epoch(&db);
    let partition = partition(&db);
    fill(&db);
    db.wait_for_background_threads_to_finish().unwrap();

    let db = open(&fixture);
    let second = epoch(&db);
    assert!(second > first, "premise: a new generation per open");
    let (opened, _, _) = open_report(&db, "open 2");
    eprintln!("open 2: generation {first} was stored at its own shutdown: {opened:?}");
    write_range(&db, WRITES..WRITES + EXTRA);
    let rows = store(&db, "generation 2 store", &[first]);
    let first_end = durable_end(&db, first);
    assert!(
        db.remote_for_test().write_core().table.stored_cut().epoch > first,
        "premise: the store's cut crossed {{{}, 0}} (replay-dirty cells flushed)",
        first + 1
    );
    assert_eq!(
        streams(&rows),
        vec![(partition, first), (partition, second)],
        "the region must name the serving generation and every retained one: {rows:?}"
    );
    assert_eq!(
        row(&rows, first),
        Some(first_end),
        "the cut crossed generation 1, so its row is its owner durable_end: {rows:?}"
    );
    db.wait_for_background_threads_to_finish().unwrap();

    let db = open(&fixture);
    let third = epoch(&db);
    let (opened, tail_batches, tail_bytes) = open_report(&db, "open 3");
    assert_eq!(
        row(&opened, first),
        Some(first_end),
        "open 3 loaded {opened:?}"
    );
    // Each older stream replays at most from its row aligned down to a fragment
    // to its end (`replay_bound`); from zero it would replay the 300 batches of
    // generation 1 alone, more than that bound.
    let bound = replay_bound(&db, &opened);
    eprintln!("open 3: replayed {tail_batches} batches, {tail_bytes} bytes, bound {bound}");
    assert!(
        bound < WRITES * VALUE as u64,
        "premise: the bound is below the volume of generation {first} ({bound})"
    );
    assert!(
        tail_bytes <= bound,
        "open 3 replayed {tail_bytes} bytes, above the bound {bound}: generation {first} is \
         replayed from far below its row"
    );
    read_back(&db, "open 3", 0..WRITES + EXTRA);
    write_range(&db, WRITES + EXTRA..WRITES + 2 * EXTRA);
    let rows = store(&db, "generation 3 store", &[first, second]);
    let second_end = durable_end(&db, second);
    assert_eq!(
        streams(&rows),
        vec![(partition, first), (partition, second), (partition, third)],
        "{rows:?}"
    );
    assert_eq!(
        row(&rows, first),
        Some(first_end),
        "never moves back: {rows:?}"
    );
    assert_eq!(row(&rows, second), Some(second_end), "{rows:?}");
    db.wait_for_background_threads_to_finish().unwrap();

    let db = open(&fixture);
    let (opened, tail_batches, tail_bytes) = open_report(&db, "open 4");
    assert_eq!(streams(&opened).len(), 3, "open 4 loaded {opened:?}");
    assert_eq!(
        row(&opened, first),
        Some(first_end),
        "open 4 loaded {opened:?}"
    );
    let bound = replay_bound(&db, &opened);
    eprintln!("open 4: replayed {tail_batches} batches, {tail_bytes} bytes, bound {bound}");
    assert!(
        tail_bytes <= bound,
        "open 4 replayed {tail_bytes} bytes, above the bound {bound}: an older generation is \
         replayed from far below its row"
    );
    read_back(&db, "open 4", 0..WRITES + 2 * EXTRA);
    db.wait_for_background_threads_to_finish().unwrap();
}

/// Generation 1 writes the data set and closes, generation 2 opens and stores,
/// then relocation passes run until every WAL file that holds a frame of
/// generation 1 is gone and the generation is unused (at most four passes).
/// The lookahead files after the end of the stream hold no frame: the owner
/// keeps them, and the trim at the next open removes them. Returns the fixture
/// with generation 2 closed.
pub(super) fn retire_generation_one() -> (Fixture, u64, u64) {
    let fixture = fixture_eager();
    let db = open(&fixture);
    let first = epoch(&db);
    fill(&db);
    db.wait_for_background_threads_to_finish().unwrap();

    let db = open(&fixture);
    let second = epoch(&db);
    write_range(&db, WRITES..WRITES + EXTRA);
    store(&db, "generation 2 store", &[first]);
    let directory = fixture
        .owner_database_root(0)
        .join(format!("epoch_{first:016x}"));
    let wal_files = || -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(&directory)
            .map(|entries| {
                entries
                    .map(|entry| {
                        entry
                            .expect("entry")
                            .file_name()
                            .to_string_lossy()
                            .into_owned()
                    })
                    .filter(|name| name.starts_with("wal_"))
                    .collect()
            })
            .unwrap_or_default();
        names.sort();
        names
    };
    // The last file with a frame is the file of the last durable byte.
    let stream_end = durable_end(&db, first);
    assert!(
        stream_end > 0,
        "premise: generation {first} has a stream end"
    );
    let last_frame_file = (stream_end - 1) / FILE;
    let frame_files = || -> Vec<String> {
        wal_files()
            .into_iter()
            .filter(|name| {
                let index = u64::from_str_radix(&name["wal_".len()..], 16)
                    .unwrap_or_else(|error| panic!("WAL file name {name}: {error}"));
                index <= last_frame_file
            })
            .collect()
    };
    let before = frame_files();
    assert!(
        !before.is_empty(),
        "premise: generation 1 holds frame files {before:?}"
    );
    eprintln!("stream end {stream_end}, last frame file {last_frame_file}, frame files {before:?}");
    let mut retired_after = None;
    for number in 1..=4 {
        let receipt = pass(&db);
        eprintln!(
            "pass {number}: {receipt:?} rows {:?} generation_unused {} files {:?}",
            stored_fronts(&db),
            unused(&db, first),
            wal_files()
        );
        assert!(!receipt.deferred, "pass {number}: {receipt:?}");
        if frame_files().is_empty() && unused(&db, first) {
            retired_after = Some(number);
            break;
        }
    }
    assert!(
        retired_after.is_some(),
        "generation {first} kept frame files {:?} (unused {}) after four passes: the \
         retirement never got its watermark (no row, or a row below its end)",
        frame_files(),
        unused(&db, first)
    );
    // The covering row is the owner durable end EXACTLY (not rounded, not
    // below): `generation_unused` compares `replay_from >= durable_end`.
    let final_rows = stored_fronts(&db);
    assert_eq!(
        row(&final_rows, first),
        Some(stream_end),
        "the older row of generation {first} is its durable end: {final_rows:?}"
    );
    db.wait_for_background_threads_to_finish().unwrap();
    (fixture, first, second)
}

fn retained_generations(db: &Db) -> Vec<u64> {
    db.remote_for_test()
        .write_core()
        .publisher
        .lock()
        .manifest
        .generations
        .iter()
        .map(|entry| entry.generation)
        .collect()
}

/// Prints every input of the trim decision for `generation`: coordinator
/// position, adopted start, stored row and the files the owner still holds.
fn trim_inputs(db: &Db, fixture: &Fixture, generation: u64, label: &str) {
    let partition = partition(db);
    let verdict = unused(db, generation);
    let stored_row = row(&stored_fronts(db), generation);
    let core = db.remote_for_test().write_core();
    {
        let mut publisher = core.publisher.lock();
        let state = publisher.checkpoint_state();
        let position = state
            .positions
            .get(&(partition, generation))
            .map(|position| (position.complete, position.durable_end));
        let start = state.callback_starts.get(&(partition, generation)).copied();
        eprintln!(
            "{label}: generation {generation}: (complete, durable_end) {position:?} adopted start \
             {start:?} stored row {stored_row:?} unused {verdict}"
        );
    }
    let directory = fixture
        .owner_database_root(0)
        .join(format!("epoch_{generation:016x}"));
    let mut files: Vec<String> = std::fs::read_dir(&directory)
        .map(|entries| {
            entries
                .map(|entry| {
                    entry
                        .expect("entry")
                        .file_name()
                        .to_string_lossy()
                        .into_owned()
                })
                .collect()
        })
        .unwrap_or_default();
    files.sort();
    eprintln!(
        "{label}: owner directory {} holds {files:?}",
        directory.display()
    );
    let authority = core.epoch;
    let readiness = core
        .control_owner(0, move |client| {
            client.check_trim_generation(authority, generation)
        })
        .expect("owner trim readiness");
    eprintln!("{label}: owner readiness for trimming generation {generation}: {readiness:?}");
}

/// End to end. The stored row covers generation 1, so relocation passes
/// give its files a watermark and retire them, `generation_unused` becomes
/// true, the next open trims the generation and every key still reads.
///
/// Negative control (mutation run): drop the `older` rows: the
/// generation has no watermark, keeps its files, and `retire_generation_one`
/// fails after four passes.
#[test]
fn a_covered_retained_generation_retires_its_files_and_is_trimmed_at_the_next_open() {
    let (fixture, first, _second) = retire_generation_one();
    let db = open(&fixture);
    let retained = retained_generations(&db);
    eprintln!("open 3: retained generations {retained:?}");
    trim_inputs(&db, &fixture, first, "open 3");
    assert!(
        !retained.contains(&first),
        "generation {first} is unused and covered: the open must trim it (retained {retained:?})"
    );
    read_back(&db, "after the trim", 0..WRITES + EXTRA);
    db.wait_for_background_threads_to_finish().unwrap();
}

/// A trimmed generation leaves the region. The open that trims generation
/// 1 loads a region that still names it (the trim happened after that store):
/// the stale row is skipped, and the next store derives its rows from the
/// manifest, so the row is gone.
///
/// Negative control (mutation run): carry the previous region's rows instead of
/// the manifest's retained set; the row of the trimmed generation survives.
#[test]
fn a_trimmed_generation_is_dropped_from_the_next_stored_region() {
    let (fixture, first, _second) = retire_generation_one();
    let db = open(&fixture);
    let (opened, _, _) = open_report(&db, "open 3");
    assert!(
        row(&opened, first).is_some(),
        "premise: the region loaded at open 3 still names generation {first}: {opened:?}"
    );
    assert!(
        !retained_generations(&db).contains(&first),
        "premise: the open trimmed generation {first}"
    );
    write_range(&db, WRITES + EXTRA..WRITES + 2 * EXTRA);
    let rows = store(&db, "generation 3 store", &[]);
    assert!(
        row(&rows, first).is_none(),
        "the trimmed generation {first} must leave the region: {rows:?}"
    );
    read_back(&db, "generation 3", 0..WRITES + 2 * EXTRA);
    db.wait_for_background_threads_to_finish().unwrap();
}

/// The cut does not cross generation 1. A latch pinned before generation 2
/// writes holds the stored cut below `{2, 0}`: the row of generation 1 stays at
/// its carried value (0), nothing of generation 1 retires, and every key reads
/// after the next open.
///
/// Negative control (mutation run): advance the row without the cover check
/// (`cut.epoch >= generation` or no cut test in `older_replay_from`); the row
/// jumps to `durable_end` under the held latch.
#[test]
fn an_older_row_does_not_advance_while_the_cut_has_not_crossed_its_generation() {
    let fixture = fixture_eager();
    let db = open(&fixture);
    let first = epoch(&db);
    fill(&db);
    db.wait_for_background_threads_to_finish().unwrap();

    let db = open(&fixture);
    let core = db.remote_for_test().write_core();
    let (opened, _, _) = open_report(&db, "open 2");
    // The row generation 1 already has in the region it left behind: the value
    // a store below the cover must keep.
    let carried = row(&opened, first).unwrap_or(0);
    // The head of generation 2 is `{2, 0}` already; a latch one version below
    // generation 2's first version holds every store's cut inside generation 1.
    let pinned = BatchVersion {
        epoch: first,
        sequence: u64::MAX,
    };
    assert!(
        pinned < core.table.current(),
        "premise: the latch lies below the head {:?}",
        core.table.current()
    );
    let latch = core.table.pin_cut(pinned);
    write_range(&db, WRITES..WRITES + EXTRA);
    let rows = store(&db, "generation 2 store under a held latch", &[first]);
    assert!(
        core.table.stored_cut().epoch <= first,
        "premise: the held latch kept the cut below generation 2: {:?}",
        core.table.stored_cut()
    );
    assert_eq!(
        row(&rows, first),
        Some(carried),
        "carried, not advanced: {rows:?}"
    );
    assert!(
        !unused(&db, first),
        "a row below the end never covers the stream"
    );
    drop(latch);
    db.wait_for_background_threads_to_finish().unwrap();

    let db = open(&fixture);
    let (opened, tail_batches, _) = open_report(&db, "open 3");
    eprintln!("open 3 replayed {tail_batches} batches (generation 1 holds {WRITES})");
    // The shutdown of generation 2 stores once more with the latch released, so
    // the row may have advanced by now; it never moves back and never passes the
    // end.
    let reopened = row(&opened, first).expect("generation 1 keeps its row");
    assert!(
        carried <= reopened && reopened <= durable_end(&db, first),
        "carried {carried}, reopened {reopened}: {opened:?}"
    );
    read_back(&db, "open 3", 0..WRITES + EXTRA);
    db.wait_for_background_threads_to_finish().unwrap();
}

/// A crash at every snapshot boundary of the store that would advance the
/// older row. Each cut leaves a region that never names generation 1 above its
/// durable end; the reopen reads every key, and the next store advances the row
/// to the durable end. The fifth case crashes after the store succeeded.
///
/// Negative control (mutation run): write the older row as `durable_end +
/// fragment`; the safety assertion fails in the after-store case.
#[test]
fn a_crash_at_every_snapshot_boundary_never_leaves_an_older_row_above_its_end() {
    let boundaries = [
        Some(SnapshotBoundary::AfterFlush),
        Some(SnapshotBoundary::AfterDurableTarget),
        Some(SnapshotBoundary::AfterDrain),
        Some(SnapshotBoundary::AfterIndexSync),
        None,
    ];
    for boundary in boundaries {
        let fixture = fixture_eager();
        let db = open(&fixture);
        let first = epoch(&db);
        fill(&db);
        db.wait_for_background_threads_to_finish().unwrap();

        let db = open(&fixture);
        // The row generation 1 already has in the region it left behind: what a
        // crashed store of generation 2 must leave in place.
        let (opened, _, _) = open_report(&db, &format!("{boundary:?} open 2"));
        let carried = row(&opened, first);
        write_range(&db, WRITES..WRITES + EXTRA);
        let core = db.remote_for_test().write_core();
        core.flush_durable().expect("certify");
        let first_end = durable_end(&db, first);
        assert_ne!(
            carried,
            Some(first_end),
            "premise: the region generation 2 loaded does not already name the end ({carried:?})"
        );
        match boundary {
            Some(boundary) => {
                inject_snapshot_fault(boundary);
                let error = core.flush_index().expect_err("the store crashes");
                eprintln!("{boundary:?}: {}", error.message());
            }
            None => {
                core.flush_index().expect("the store succeeds");
                eprintln!("after the store: rows {:?}", stored_fronts(&db));
            }
        }
        drop(db);

        let db = open(&fixture);
        let (opened, tail_batches, _) = open_report(&db, &format!("{boundary:?} reopen"));
        let reopened = row(&opened, first);
        match boundary {
            // The crashed store wrote no region: the loaded region is the one
            // generation 2 itself loaded.
            Some(_) => assert_eq!(
                reopened, carried,
                "{boundary:?}: a store that crashed before the region write changed generation \
                 {first}'s row: {opened:?}"
            ),
            // The completed store advanced the row to the durable end, no
            // further: a row above it would make recovery skip bytes the owner holds.
            None => assert_eq!(
                reopened,
                Some(first_end),
                "after the store: the region names generation {first} at its durable end: \
                 {opened:?}"
            ),
        }
        eprintln!("{boundary:?}: reopen replayed {tail_batches} batches");
        read_back(&db, &format!("{boundary:?} reopen"), 0..WRITES + EXTRA);
        let rows = store(&db, &format!("{boundary:?} next store"), &[first]);
        assert_eq!(
            row(&rows, first),
            Some(first_end),
            "{boundary:?}: the next store advances the older row to its end: {rows:?}"
        );
        db.wait_for_background_threads_to_finish().unwrap();
    }
}

/// A retained generation whose stream ENDS MID-FILE gets an aligned
/// callback start. Generation 1 ends at `durable_end` inside a 1 MiB file; the
/// pass rounds the clean tail up to the end of that file, the manifest adopts
/// the aligned start, later passes over the stream are no-ops (no error, no
/// change), the stream becomes trim-eligible, and the next open trims it.
///
/// Before the aligned start the second pass failed with "relocation callback
/// start names no retained stream or is not file-aligned".
///
/// Negative control (mutation run): the clean tail
/// stays at the stream end; the pass fails with that error.
#[test]
fn a_sealed_stream_ending_mid_file_gets_its_callback_start_adopted() {
    let fixture = fixture_eager();
    let db = open(&fixture);
    let first = epoch(&db);
    let partition = partition(&db);
    fill(&db);
    db.wait_for_background_threads_to_finish().unwrap();

    let db = open(&fixture);
    write_range(&db, WRITES..WRITES + EXTRA);
    store(&db, "generation 2 store", &[first]);
    let end = durable_end(&db, first);
    let serving_end = durable_end(&db, epoch(&db));
    let pct = u64::from(
        db.remote_for_test()
            .write_core()
            .config
            .index
            .relocation_max_reclaim_pct,
    );
    let budget = ((end + serving_end) * pct / 100).max(FILE) / FILE * FILE;
    let limit = end.min(budget);
    let expected_first = if limit == end {
        end.next_multiple_of(FILE)
    } else {
        limit / FILE * FILE
    };
    assert!(
        !end.is_multiple_of(FILE),
        "premise: the stream ends mid-file ({end})"
    );
    let adopted = || {
        db.remote_for_test()
            .write_core()
            .publisher
            .lock()
            .checkpoint_state()
            .callback_starts
            .get(&(partition, first))
            .copied()
    };
    let expected = end.next_multiple_of(FILE);
    // A pass whose target is a budget cut (the per-owner percentage) rounds DOWN
    // to a file, as native does; the pass that reaches the stream end rounds the
    // clean tail UP to the end of its last file. Every adopted start is
    // file-aligned, monotone and never above the end of the last file; the
    // stream reaches `expected` within four passes.
    let mut previous = 0;
    let mut reached = None;
    read_back(&db, "before pass 1", 0..WRITES + EXTRA);
    for number in 1..=4 {
        let report = pass(&db);
        let start = adopted();
        eprintln!("pass {number}: {report:?} adopted start {start:?}");
        assert!(!report.deferred, "pass {number}: {report:?}");
        if number == 1 {
            // No stream has a callback start yet. The older stream consumes
            // the whole-file share of the configured owner span budget first.
            assert_eq!(
                start,
                Some(expected_first),
                "pass 1 must use the configured span budget"
            );
        }
        read_back(&db, &format!("after pass {number}"), 0..WRITES + EXTRA);
        if let Some(start) = start {
            assert_eq!(
                start % FILE,
                0,
                "pass {number}: adopted start {start} is not file-aligned"
            );
            assert!(
                start >= previous,
                "pass {number}: the start moved back from {previous}"
            );
            assert!(
                start <= expected,
                "pass {number}: the start {start} is above {expected}"
            );
            previous = start;
        }
        if start == Some(expected) {
            reached = Some(number);
            break;
        }
    }
    let reached = reached.unwrap_or_else(|| {
        panic!("the adopted start never reached the end of the last file {expected}: {previous}")
    });
    assert_eq!(reached, 2, "the fixture needs two filtered passes");
    assert!(
        unused(&db, first),
        "the cut covers the stream and its start reaches its end"
    );
    // A pass over the fully covered sealed stream changes nothing.
    let outcome = try_pass(&db).unwrap_or_else(|error| panic!("pass {}: {error}", reached + 1));
    assert!(!outcome.deferred, "{outcome:?}");
    assert_eq!(
        adopted(),
        Some(expected),
        "a later pass moved a fully covered start"
    );
    read_back(&db, "after the passes", 0..WRITES + EXTRA);
    db.wait_for_background_threads_to_finish().unwrap();
    let reopened = open(&fixture);
    read_back(&reopened, "after reopen", 0..WRITES + EXTRA);
    reopened.wait_for_background_threads_to_finish().unwrap();
}

/// A stale relocation update must not replace a foreground overwrite.
/// Mutation control: remove the `same_physical` check in `relocation_apply`.
#[test]
fn a_stale_relocation_update_does_not_replace_an_overwrite() {
    let fixture = fixture_eager();
    let db = open(&fixture);
    write_range(&db, 0..1);
    db.wait_for_background_threads_to_finish().unwrap();

    let db = open(&fixture);
    let ks = db.ks("a");
    let raw_key = key(0);
    let shape = shape();
    let descriptor = shape.ks(ks);
    let cell = descriptor.cell_id(&raw_key);
    let reduced = Bytes::from(descriptor.reduce_key(&raw_key).into_owned());
    let table = db.remote_for_test().hybrid_table();
    let old = table.get(ks, &raw_key, newest_cut()).unwrap().unwrap();

    let replacement = vec![73; VALUE];
    let mut batch = db.write_batch();
    batch.write(ks, raw_key.clone(), replacement.clone());
    batch.commit().unwrap();
    let current = table.get(ks, &raw_key, newest_cut()).unwrap().unwrap();
    assert!(
        !old.same_physical(&current),
        "premise: the overwrite has a new position"
    );
    assert_ne!(
        old.address.segment_generation,
        current.address.segment_generation
    );

    let stale_copy = old.moved_to(current.address);
    let target = [(
        old.address.partition,
        old.address.segment_generation,
        old.address.offset + 1,
    )];
    let updates = [(reduced, old, stale_copy)];
    table
        .relocation_apply(ks, &cell, RelocationApply::Repoint(&updates), &target)
        .unwrap();
    assert!(
        table
            .get(ks, &raw_key, newest_cut())
            .unwrap()
            .unwrap()
            .same_physical(&current)
    );
    // Also put the current stream below a target to isolate the physical
    // compare from the target check. A logical or unconditional compare would
    // then install the stale copy.
    let both_targets = [
        target[0],
        (
            current.address.partition,
            current.address.segment_generation,
            current.address.offset + 1,
        ),
    ];
    table
        .relocation_apply(ks, &cell, RelocationApply::Repoint(&updates), &both_targets)
        .unwrap();
    let after = table.get(ks, &raw_key, newest_cut()).unwrap().unwrap();
    assert!(
        after.same_physical(&current),
        "a stale relocation changed the overwrite"
    );
    assert_eq!(
        db.get(ks, &raw_key).unwrap().unwrap().as_ref(),
        replacement.as_slice()
    );
    db.remote_for_test().write_core().flush_index().unwrap();
    db.wait_for_background_threads_to_finish().unwrap();

    let reopened = open(&fixture);
    assert_eq!(
        reopened
            .get(reopened.ks("a"), &raw_key)
            .unwrap()
            .unwrap()
            .as_ref(),
        replacement.as_slice()
    );
    reopened.wait_for_background_threads_to_finish().unwrap();
}

/// A foreground overwrite after the copy is durable must survive the stale
/// re-point and the following snapshot. The frozen target check independently
/// protects this overwrite; the physical CAS protects stale heads as defense
/// in depth. The end-to-end control disables both checks; the preceding test
/// controls the physical CAS alone.
#[test]
fn an_overwrite_during_a_durable_relocation_copy_survives_reopen() {
    struct ResumeOnDrop(Option<std::sync::mpsc::SyncSender<()>>);
    impl Drop for ResumeOnDrop {
        fn drop(&mut self) {
            // Closing the channel also releases the one-shot hook on panic.
            self.0.take();
        }
    }

    let fixture = fixture_eager();
    let db = open(&fixture);
    fill(&db);
    db.wait_for_background_threads_to_finish().unwrap();
    let db = open(&fixture);
    let (entered, waiting) = std::sync::mpsc::sync_channel(1);
    let (resume, gate) = std::sync::mpsc::sync_channel(1);
    db.remote_for_test()
        .write_core()
        .test_hooks
        .lock()
        .relocation_copy_pause = Some((entered, gate));
    let replacement = vec![83; VALUE];
    let raw_key = std::thread::scope(|scope| {
        let worker = scope.spawn(|| try_pass(&db));
        let release = ResumeOnDrop(Some(resume));
        let (ks, reduced) = waiting.recv_timeout(WAIT).expect("copy pause");
        let reduced = reduced.expect("copy chunk has a key");
        let shape = shape();
        let raw_key = (0..WRITES)
            .map(key)
            .find(|candidate| shape.ks(ks).reduce_key(candidate).as_ref() == reduced.as_ref())
            .expect("copied reduced key belongs to the fixture");
        let table = db.remote_for_test().hybrid_table();
        let old = table.get(ks, &raw_key, newest_cut()).unwrap().unwrap();
        assert!(
            db.get(ks, &raw_key).unwrap().is_some(),
            "copied key must be readable"
        );

        let mut batch = db.write_batch();
        batch.write(ks, raw_key.clone(), replacement.clone());
        batch.commit().unwrap();
        let current = table.get(ks, &raw_key, newest_cut()).unwrap().unwrap();
        assert!(
            !old.same_physical(&current),
            "overwrite must publish a new position"
        );
        drop(release);
        let receipt = worker.join().unwrap().expect("relocation pass");
        assert!(!receipt.deferred, "{receipt:?}");
        let after = table.get(ks, &raw_key, newest_cut()).unwrap().unwrap();
        assert!(
            after.same_physical(&current),
            "stale copy replaced the overwrite"
        );
        assert_eq!(
            db.get(ks, &raw_key).unwrap().unwrap().as_ref(),
            replacement.as_slice()
        );
        raw_key
    });
    db.remote_for_test().write_core().flush_index().unwrap();
    db.wait_for_background_threads_to_finish().unwrap();
    let reopened = open(&fixture);
    assert_eq!(
        reopened
            .get(reopened.ks("a"), &raw_key)
            .unwrap()
            .unwrap()
            .as_ref(),
        replacement.as_slice()
    );
    reopened.wait_for_background_threads_to_finish().unwrap();
}

/// What a retained older generation adds to the replay of an open. Generation 2
/// writes nothing, so every replayed batch of open 3 belongs to generation 1
/// (the stored row names its exact `durable_end`, mid-fragment): the replay
/// start is aligned down to the fragment (`aligned_replay_start`), so the open
/// re-reads at most that last fragment (a handful of batches, never the 300
/// the stream holds). Prints the row, the end and the count.
///
/// Negative control (mutation run): the row is
/// missing, the open counts `old_generation_replayed_from_zero`, replays all
/// 300 batches, and the bound fails.
#[test]
fn an_older_generation_adds_at_most_its_last_fragment_to_the_replay() {
    let fixture = fixture_eager();
    let db = open(&fixture);
    let first = epoch(&db);
    fill(&db);
    db.wait_for_background_threads_to_finish().unwrap();

    let db = open(&fixture);
    let rows = store(&db, "generation 2 store (no writes)", &[first]);
    let end = durable_end(&db, first);
    assert_eq!(
        row(&rows, first),
        Some(end),
        "the row is the exact end: {rows:?}"
    );
    db.wait_for_background_threads_to_finish().unwrap();

    let db = open(&fixture);
    let (opened, tail_batches, tail_bytes) = open_report(&db, "open 3");
    eprintln!(
        "generation {first}: row {:?}, durable_end {end}, replayed {tail_batches} batches \
         ({tail_bytes} bytes) of {WRITES}",
        row(&opened, first)
    );
    assert_eq!(row(&opened, first), Some(end), "{opened:?}");
    // Generation 2 wrote nothing, so the whole bound is generation 1's last
    // fragment: the span from its row aligned down to a fragment to its end.
    let bound = replay_bound(&db, &opened);
    eprintln!("replay bound {bound} bytes (fragment {FRAGMENT})");
    assert!(bound <= FRAGMENT, "premise: one fragment at most ({bound})");
    assert!(
        tail_bytes <= bound,
        "open 3 replayed {tail_bytes} bytes of {WRITES} batches, above the bound {bound}: \
         generation {first} is replayed from far below its stored row"
    );
    read_back(&db, "open 3", 0..WRITES);
    db.wait_for_background_threads_to_finish().unwrap();
}

/// The variant of the two-reopen decider: generation 1 DID store a region of its own (a front for
/// itself, before generation 2 existed); generation 2's first store must keep
/// that row (never drop it, never move it back) and advance it to the durable
/// end once the cut crossed the generation.
///
/// Negative control (mutation run): the region of
/// generation 2 is built without the older rows; generation 1's front is gone.
#[test]
fn a_front_stored_for_generation_one_survives_the_first_store_of_generation_two() {
    let fixture = fixture_eager();
    let db = open(&fixture);
    let first = epoch(&db);
    fill(&db);
    db.remote_for_test()
        .write_core()
        .flush_index()
        .expect("generation 1 store");
    let before = stored_fronts(&db);
    eprintln!("generation 1 stored fronts {before:?}");
    let stored = row(&before, first).unwrap_or_else(|| {
        panic!("premise: generation 1 names itself in its own region: {before:?}")
    });
    db.wait_for_background_threads_to_finish().unwrap();

    let db = open(&fixture);
    open_report(&db, "open 2");
    write_range(&db, WRITES..WRITES + EXTRA);
    let after = store(&db, "generation 2's first store", &[first]);
    let end = durable_end(&db, first);
    let kept = row(&after, first)
        .unwrap_or_else(|| panic!("generation 2's first store dropped generation 1: {after:?}"));
    assert!(
        stored <= kept && kept <= end,
        "generation 1's row moved from {stored} to {kept}: it never moves back and never passes \
         its end {end}"
    );
    db.wait_for_background_threads_to_finish().unwrap();
}

/// A native-tier acknowledged suffix can be lost after an owner crash. Inject
/// a bad CRC into the first suffix frame and retain a complete later frame in
/// the same fragment. This is deliberate post-ack corruption, not a claim that
/// a synchronous acknowledged frame may be lost. The bad frame holds a
/// two-operation batch: a recovery that applied a prefix of a torn batch would
/// show one of its keys.
///
/// The unsynced boundary is a property of the setup, not of timing: the second
/// barrier covers key 1; a pause on the owner syncer's pre-persist hold point
/// (taken only by the syncer thread) is armed before the third write; the
/// injected persist failure is armed only while the syncer is parked, so the
/// persist it fails is the syncer's own. Key 3 appends while the syncer is
/// parked. Negative controls (mutation runs): an owner scan that resumes behind
/// the invalid frame adopts a longer end (the adopted-end assertion fails); a
/// recovery plan that drops the last covered version (`RecoveryPlan::keep`)
/// fails the prefix assertion on key 1; one that drops every version fails the
/// assertion on key 0.
#[test]
fn a_torn_same_fragment_tail_cannot_resurrect_after_generation_trim() {
    use std::os::unix::fs::FileExt;

    let mut fixture = fixture_eager();
    fixture.config.durable_cut.lag_ms = IDLE_LAG_MS;
    let db = fixture.open();
    let generation = db.epoch();
    let ks = KeySpace(0);
    let other = [0xAB_u8; 8].to_vec();
    let put = |n: u64, byte: u8| Mutation::Put {
        keyspace: ks,
        key: key(n).into(),
        value: vec![byte; 128].into(),
    };
    let put_other = || Mutation::Put {
        keyspace: ks,
        key: other.clone().into(),
        value: vec![55; 128].into(),
    };
    let first = db
        .write_batch(fixture.operation(0, 1), vec![put(0, 11)])
        .unwrap();
    db.core.flush_durable().unwrap();
    let persisted = db.durable_cut_stats().persisted_next;
    let second = db
        .write_batch(fixture.operation(0, 2), vec![put(1, 22)])
        .unwrap();
    // The second barrier puts key 1 below the durable cut, so key 1 is the last
    // kept prefix version and key 2 is the first suffix frame that no owner sync
    // covers.
    db.core.flush_durable().unwrap();
    let covered = db.durable_cut_stats().persisted_next;
    assert_eq!((first.sequence, second.sequence), (0, 1));
    assert_eq!(persisted, 1, "only the first version is covered");
    assert_eq!(
        covered, 2,
        "premise: the second barrier covers keys 0 and 1"
    );
    let log = fixture.nodes[0].log_for_test(generation);
    // Arm the owner syncer's pre-persist hold before the third write. That
    // point is taken only on the syncer thread, so no barrier, read or seal
    // persist can engage it.
    let (entered, resume) =
        log.pause_live(crate::distributed::storage::LivePausePoint::SyncerBeforePersist);
    let third = db
        .write_batch(fixture.operation(0, 3), vec![put(2, 33), put_other()])
        .unwrap();
    // The frontier only learns of a page-cache frame when the live extent is
    // adopted, so adopt after the write: the owner sync below then has key 2 to
    // persist and reaches the armed hold. A sync requested before the adoption
    // would find nothing pending and never wake the syncer.
    log.self_adopt_live_extent().unwrap();
    let frontier = log.frontier().unwrap().clone();
    let sync_frontier = frontier.clone();
    let sync = std::thread::spawn(move || {
        crate::distributed::durability::frontier::flush_all(&[sync_frontier], None).into_synced()
    });
    entered
        .recv_timeout(WAIT)
        .expect("the owner syncer reaches its pre-persist hold");
    assert!(
        log.syncer_persist_hold_reached(),
        "the parked pass is the syncer's own"
    );
    // Appended while the syncer is parked: the failing persist covers it, so
    // key 3 stays a complete later frame in the same fragment that no sync holds.
    let fourth = db
        .write_batch(fixture.operation(0, 4), vec![put(3, 44)])
        .unwrap();
    // Armed only now: the syncer is parked before its persist, so the first
    // appended persist after this point is the one it runs on release.
    log.fail_next_appended_persist();
    resume.send(()).unwrap();
    assert!(
        sync.join().unwrap().is_err(),
        "the injected owner sync must fail"
    );
    assert!(
        log.appended_persist_failure_consumed(),
        "the fault hook must be consumed"
    );
    assert_eq!((third.sequence, fourth.sequence), (2, 3));
    // The barrier above and the failed persist make the unsynced boundary a
    // property of the setup: key 1 is durable, key 2 and key 3 are not. The
    // index is the first suffix frame at or above the owner sync.
    let table = &db.core.table;
    let suffixes: Vec<_> = (1..=3)
        .map(|n| {
            table
                .get(ks, &key(n), newest_cut())
                .unwrap()
                .unwrap()
                .address
        })
        .collect();
    let index = suffixes
        .iter()
        .position(|address| frontier.synced() <= address.offset / 4096 * 4096)
        .unwrap_or_else(|| {
            panic!(
                "premise: the owner synced every suffix frame (synced {}): \
                 {suffixes:?}; the injected failure must leave one unsynced",
                frontier.synced()
            )
        });
    assert_eq!(
        index, 1,
        "premise: key 1 is synced and key 2 is the first unsynced frame"
    );
    assert!(
        index + 1 < suffixes.len(),
        "premise: a later suffix frame exists after the bad one"
    );
    let bad = suffixes[index];
    let later = suffixes[index + 1];
    assert_eq!(bad.segment_generation, generation);
    assert_eq!(later.segment_generation, generation);
    assert_eq!(
        bad.offset / FRAGMENT,
        later.offset / FRAGMENT,
        "same fragment"
    );
    assert_eq!(bad.offset / FILE, later.offset / FILE, "same WAL file");
    assert!(
        frontier.synced() <= bad.offset / 4096 * 4096,
        "the frame with the invalid checksum is beyond the owner sync"
    );
    let later_frame = later.offset / 4096 * 4096;
    assert!(
        log.decode_group_at(later_frame).is_ok(),
        "later frame is valid before crash"
    );
    eprintln!(
        "native tier, page-cache acknowledgements: {first:?} {second:?} {third:?} {fourth:?}; \
         persisted_next {persisted}; bad {bad:?}; later {later:?}; appended {} synced {}",
        frontier.appended(),
        frontier.synced()
    );
    drop(log);
    super::tests::fail_stop_without_checkpoint(db);
    fixture.stop_owners();
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(fixture.owner_database_root(0).join(format!(
            "epoch_{generation:016x}/wal_{:016x}",
            bad.offset / FILE
        )))
        .unwrap();
    let mut later_before = [0; 4096];
    file.read_exact_at(&mut later_before, later_frame % FILE)
        .unwrap();
    let offset = bad.offset % FILE + u64::from(bad.len) - 1;
    let mut byte = [0];
    file.read_exact_at(&mut byte, offset).unwrap();
    byte[0] ^= 1;
    file.write_all_at(&byte, offset).unwrap();
    let mut later_after = [0; 4096];
    file.read_exact_at(&mut later_after, later_frame % FILE)
        .unwrap();
    assert_eq!(
        later_after, later_before,
        "corruption did not change the later valid frame"
    );
    file.sync_all().unwrap();
    drop(file);
    // The hour-long lag only kept the suffix unsynced before the crash. The
    // reopened coordinator needs a reactive durable cut for its relocation passes.
    fixture.config.durable_cut.lag_ms = QUIET_LAG_MS;
    fixture.restart_owners().unwrap();
    // Checked before the coordinator opens: an owner scan that resumed behind
    // the invalid frame would also break the coordinator's replay, and the
    // adopted end is the observable that names the cause.
    {
        let sealed = fixture.nodes[0].sealed_log_for_test(generation);
        eprintln!(
            "owner generation {generation} adopted end after restart: {}",
            sealed.appended_frontier()
        );
        assert_eq!(
            sealed.appended_frontier(),
            frontier.synced(),
            "the owner scan stops at the invalid frame: the adopted end is the synced end"
        );
    }
    let db = open(&fixture);
    assert_eq!(db.get(ks, &key(0)).unwrap().unwrap().as_ref(), &[11; 128]);
    // `index` is the position of the corrupted frame among keys 1..=3. Frames
    // below it were covered by the second barrier and must read back byte-exact;
    // that frame and every frame after it were never synced.
    let bad_key = u64::try_from(index).expect("index fits u64") + 1;
    for (n, byte) in [(1u64, 22u8), (2, 33), (3, 44)].into_iter().take(index) {
        assert_eq!(db.get(ks, &key(n)).unwrap().unwrap().as_ref(), &[byte; 128]);
    }
    for n in bad_key..=3u64 {
        assert!(db.get(ks, &key(n)).unwrap().is_none());
    }
    assert!(
        db.get(ks, &other).unwrap().is_none(),
        "no key of the torn two-operation batch is visible"
    );
    store(&db, "post-crash store", &[generation]);
    for _ in 0..4 {
        pass(&db);
    }
    db.wait_for_background_threads_to_finish().unwrap();
    let db = open(&fixture);
    assert_eq!(db.get(ks, &key(0)).unwrap().unwrap().as_ref(), &[11; 128]);
    for (n, byte) in [(1u64, 22u8), (2, 33), (3, 44)].into_iter().take(index) {
        assert_eq!(db.get(ks, &key(n)).unwrap().unwrap().as_ref(), &[byte; 128]);
    }
    for n in bad_key..=3u64 {
        assert!(db.get(ks, &key(n)).unwrap().is_none());
    }
    assert!(
        db.get(ks, &other).unwrap().is_none(),
        "no key of the torn two-operation batch is visible after the trim"
    );
    assert!(
        !retained_generations(&db).contains(&generation),
        "older generation was trimmed"
    );
    db.wait_for_background_threads_to_finish().unwrap();
}
