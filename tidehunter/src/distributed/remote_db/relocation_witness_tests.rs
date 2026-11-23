//! The relocation shadow witness must survive the pass that produced it.
//!
//! A pass that copies an overwritten key's as-of value cannot re-point the cell
//! (its latest entry is newer), so a held read cut at or below the newer version
//! still resolves the old position and the file that holds it must stay. The
//! pass that retires that file need not be the pass that copied the position:
//! when this stream has no whole file before its target any more, another
//! stream's work is what makes the pass run retirement, and that pass produces
//! no shadowed expectation of its own. The witness is therefore kept between
//! passes (`RetirementGate`) and re-evaluated on every retirement.
//!
//! Geometry, two owners and three generations:
//! * generation 1 (partition 0) is the pinned stream: one partial file, key 0's
//!   first write at its start, sealed by the next open;
//! * generation 2 (partition 1) is the other stream: a span that is not a
//!   whole number of files, so its per-owner budget leaves it a target for the
//!   pass after the one that first reclaims it;
//! * generation 3 serves. The checkpoint latch is taken before its first
//!   write, so the overwrite of key 0 gets that exact version: the pinned cut
//!   still resolves the old position and can never resolve the overwrite.
//!
//! Pass 1 targets generations 1 and 2 and its own shadow cap keeps partition
//! 0's file 0. Pass 2 no longer scans generation 1 at all (its start reached
//! its end, so it has neither a target nor a shadowed expectation) while
//! generation 2 still has a target: retirement for the pinned stream runs on
//! another stream's work.
//!
//! Negative controls (mutation runs): derive the cap only from the current
//! pass's `shadowed` list (`relocation_pass` before this patch), or drop the
//! witness in `take_snapshot` before re-evaluating it;
//! `a_latched_position_survives_a_pass_driven_by_another_stream` fails because
//! pass 2 retires partition 0's file 0.

use super::super::pipeline_tests::{Fixture, newest_cut, read_at_cut};
use super::super::retained_generation_tests::{FILE, FRAGMENT, VALUE, WRITES, key, shape};
use crate::distributed::codec::{BatchLimits, Mutation};
use crate::distributed::durability::DurableCutConfig;
use crate::distributed::group::GroupLimits;
use crate::distributed::sui::Db;
use crate::key_shape::KeySpace;
use std::fs;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

/// The owner pollers sample their nudge flag every quarter lag; a short lag
/// keeps the copy durability wait's own request served.
const QUIET_LAG_MS: u64 = 10_000;
/// Bound of every coordinator and owner operation; only a stuck operation
/// turns into a failure.
const OPERATION_TIMEOUT: Duration = Duration::from_secs(60);
/// Partition 0 carries the pinned stream and the overwrite's serving stream,
/// partition 1 the other stream whose work drives the second pass.
const PINNED: u32 = 0;
const OTHER: u32 = 1;
/// The overwritten key. Its first write is the first frame of partition 0's
/// generation 1 stream, so its old position sits in WAL file 0.
const KEY: u64 = 0;
/// Generation 1's size: one partial file, so the sealed clean tail rounds up
/// to exactly one file and the stream is finished after one pass.
const PINNED_WRITES: u64 = 32;
/// Operation seeds per generation and partition; ranges must never overlap on
/// one partition, or the authority deduplicates the retry and applies nothing.
const OVERWRITE_SEED: u64 = 1000;
const RECLAIM_SEED_BASE: u64 = 1100;
/// Reclaim writes per pass: enough for a whole file beyond the callback start.
const RECLAIM_WRITES: u64 = 150;
/// Passes the reclaim loop may need after the latch drops; each one follows a
/// write, so the loop is not a wait.
const MAX_RECLAIM_PASSES: usize = 4;

fn witness_fixture() -> Fixture {
    Fixture::new_with_root(
        tempdir::TempDir::new("hybrid-relocation-witness").expect("temp dir"),
        2,
        8192,
        shape(),
        32,
        |config| {
            // Zero stale window: the ordinary store flushes every dirty cell, so
            // a store's cut advances to the head and a retained generation gets
            // its row on the first store of the next generation.
            config.index.snapshot_unload_threshold = 0;
            config.durable_cut = DurableCutConfig {
                lag_ms: QUIET_LAG_MS,
                ..DurableCutConfig::default()
            };
            config.storage.batch_limits = BatchLimits {
                max_operations: 32,
                max_encoded_bytes: 64 * 1024,
            };
            config.storage.group_limits = GroupLimits {
                max_batches: 32,
                max_encoded_bytes: 256 * 1024,
            };
            config.storage.fragment_bytes = FRAGMENT;
            config.storage.wal_file_bytes = FILE;
            config.timeout = OPERATION_TIMEOUT;
        },
        |_| {},
    )
}

fn open(fixture: &Fixture) -> Arc<Db> {
    Db::open_remote_local_fixture(
        &fixture.authority(),
        fixture.shape.clone(),
        fixture.config.clone(),
    )
    .expect("open")
}

/// One value per write: the write sequence in the first bytes, then a fill
/// byte, so a pinned read is compared for identity, not only for length.
fn value(sequence: u64) -> Vec<u8> {
    let mut value = vec![(sequence % 251) as u8; VALUE];
    value[..8].copy_from_slice(&sequence.to_be_bytes());
    value
}

/// One write of `key_sequence` with the value of `value_sequence`. The operation
/// id comes from `operation_seed`, which must never repeat on a partition: the
/// authority deduplicates a repeated operation id and would not apply the
/// mutation again.
fn write_one(
    db: &Arc<Db>,
    fixture: &Fixture,
    partition: u32,
    operation_seed: u64,
    key_sequence: u64,
    value_sequence: u64,
) {
    db.remote_for_test()
        .write_batch(
            fixture.operation(partition, operation_seed),
            vec![Mutation::Put {
                keyspace: KeySpace(0),
                key: key(key_sequence).into(),
                value: value(value_sequence).into(),
            }],
        )
        .expect("write");
}

/// `count` writes with operation seeds and keys from `first`.
fn write_range(db: &Arc<Db>, fixture: &Fixture, partition: u32, first: u64, count: u64) {
    for sequence in first..first + count {
        write_one(db, fixture, partition, sequence, sequence, sequence);
    }
}

/// The owner `durable_end` the coordinator holds for `(partition, generation)`.
fn durable_end(db: &Arc<Db>, partition: u32, generation: u64) -> u64 {
    db.remote_for_test()
        .write_core()
        .publisher
        .lock()
        .checkpoint_state()
        .positions
        .get(&(partition, generation))
        .unwrap_or_else(|| panic!("no position for partition {partition} generation {generation}"))
        .durable_end
}

/// Sorted WAL file indexes of `generation` on owner `owner`.
fn wal_files(fixture: &Fixture, owner: usize, generation: u64) -> Vec<u64> {
    let directory = fixture
        .owner_database_root(owner)
        .join(format!("epoch_{generation:016x}"));
    let mut files: Vec<u64> = fs::read_dir(directory)
        .expect("owner generation directory")
        .flatten()
        .filter_map(|entry| {
            let name = entry.file_name().to_string_lossy().into_owned();
            u64::from_str_radix(name.strip_prefix("wal_")?, 16).ok()
        })
        .collect();
    files.sort_unstable();
    files
}

fn pass(db: &Arc<Db>, label: &str) -> super::RelocationReport {
    let report = db
        .remote_for_test()
        .relocation_pass(false, &AtomicBool::new(false))
        .unwrap_or_else(|error| panic!("{label}: relocation pass: {error:?}"));
    assert!(
        !report.deferred,
        "{label}: the pass stored its copies: {report:?}"
    );
    report
}

/// A latched shadowed position survives a pass whose retirement is driven by
/// another stream, and is reclaimed after the latch drops.
#[test]
fn a_latched_position_survives_a_pass_driven_by_another_stream() {
    let fixture = witness_fixture();
    // Generation 1: the pinned stream, one partial file on partition 0.
    let db = open(&fixture);
    let pinned_generation = db.remote_for_test().write_core().epoch;
    write_one(&db, &fixture, PINNED, 0, KEY, 0);
    write_range(&db, &fixture, PINNED, 1, PINNED_WRITES - 1);
    let core = db.remote_for_test().write_core();
    core.flush_durable().expect("certify generation 1");
    core.flush_index().expect("generation 1 store");
    let pinned_end = durable_end(&db, PINNED, pinned_generation);
    assert!(
        pinned_end < FILE && !pinned_end.is_multiple_of(FILE),
        "premise: generation 1 is one partial file: {pinned_end} bytes"
    );
    db.wait_for_background_threads_to_finish()
        .expect("close generation 1");

    // Generation 2: the other stream, a span that is not a whole number of
    // files, on partition 1.
    let db = open(&fixture);
    let other_generation = db.remote_for_test().write_core().epoch;
    assert!(
        other_generation > pinned_generation,
        "premise: a new fencing generation per open"
    );
    // Generation 2 must not write the pinned key: the pinned cut has to resolve
    // generation 1's position, whichever stream wrote last.
    write_range(&db, &fixture, OTHER, 1, WRITES);
    let core = db.remote_for_test().write_core();
    core.flush_durable().expect("certify generation 2");
    core.flush_index().expect("generation 2 store");
    let other_end = durable_end(&db, OTHER, other_generation);
    assert!(
        !other_end.is_multiple_of(FILE),
        "premise: generation 2's stream end {other_end} is not a whole number of files, so the \
         per-owner budget leaves it a target after its first pass"
    );
    db.wait_for_background_threads_to_finish()
        .expect("close generation 2");

    // Generation 3 serves. The latch comes first, so the overwrite's version is
    // the pinned cut itself: `below(cut)` resolves the old write and never the
    // overwrite, and the cap's `cut_at_or_below` includes it.
    let db = open(&fixture);
    let generation = db.remote_for_test().write_core().epoch;
    assert!(
        generation > other_generation,
        "premise: a new fencing generation per open"
    );
    let ks = KeySpace(0);
    let (cut, pin) = db.remote_for_test().hybrid_table().pin_checkpoint();
    assert_eq!(
        (cut.epoch, cut.sequence),
        (generation, 0),
        "premise: the latch is the first cut of the serving generation"
    );
    write_one(&db, &fixture, PINNED, OVERWRITE_SEED, KEY, OVERWRITE_SEED);
    db.remote_for_test()
        .write_core()
        .flush_durable()
        .expect("certify the overwrite");
    let overwrite = db
        .remote_for_test()
        .hybrid_table()
        .get(ks, &key(KEY), newest_cut())
        .expect("index read")
        .expect("the key is indexed");
    assert_eq!(
        overwrite.batch, cut,
        "premise: the overwrite's version is the pinned cut"
    );
    let old_position = db
        .remote_for_test()
        .hybrid_table()
        .get(ks, &key(KEY), cut)
        .expect("pin-cut index read")
        .expect("the pinned cut names the old position");
    assert_eq!(
        old_position.address.segment_generation, pinned_generation,
        "premise: the pinned cut resolves generation 1's position: {old_position:?}"
    );
    let old_file = old_position.address.offset / FILE;
    assert_eq!(
        old_file, 0,
        "premise: generation 1's first write is in WAL file 0"
    );
    let pinned_value = read_at_cut(db.remote_for_test(), ks, &key(KEY), cut)
        .expect("pinned read")
        .expect("the pinned cut sees the first write");
    assert_eq!(
        pinned_value.as_ref(),
        value(0).as_slice(),
        "premise: the pinned cut resolves the first write of key {KEY}"
    );

    // Pass 1: the pinned stream and the other stream both have a target. The
    // pass's own shadow cap keeps partition 0's file 0.
    let first = pass(&db, "pass 1");
    assert!(
        db.remote_for_test().retirement_pin_caps() > 0,
        "premise: pass 1 saw the shadowed re-point and capped a watermark: {first:?}"
    );
    assert!(
        wal_files(&fixture, PINNED as usize, pinned_generation).contains(&old_file),
        "pass 1 retired the file a held cut still resolves"
    );

    // Pass 2: the pinned stream has no target of its own; the other stream does,
    // so retirement runs for every stream again. The retained witness must cap
    // the pinned stream's watermark.
    let second = pass(&db, "pass 2");
    let other_report = second
        .streams
        .iter()
        .find(|stream| stream.partition == OTHER && stream.fencing_generation == other_generation)
        .unwrap_or_else(|| {
            panic!(
                "premise: pass 2 scanned partition {OTHER}'s retained generation: {:?}",
                second.streams
            )
        });
    assert!(
        other_report.rounded_target > other_report.lo,
        "premise: pass 2 has a target of its own, so it runs retirement: {other_report:?}"
    );
    assert!(
        second
            .streams
            .iter()
            .all(|stream| !(stream.partition == PINNED
                && stream.fencing_generation == pinned_generation)),
        "premise: pass 2 does not scan the pinned stream (its start reached its end, so it has no \
         target and no shadowed expectation): {:?}",
        second.streams
    );
    assert!(
        wal_files(&fixture, PINNED as usize, pinned_generation).contains(&old_file),
        "pass 2 retired the file that the retained witness still caps"
    );
    assert!(
        db.remote_for_test().retirement_pin_caps() >= 2,
        "premise: pass 2 capped the pinned stream's watermark again"
    );
    let still_pinned = read_at_cut(db.remote_for_test(), ks, &key(KEY), cut)
        .expect("pinned read after pass 2")
        .expect("the pinned cut still sees the first write");
    assert_eq!(
        still_pinned.as_ref(),
        value(0).as_slice(),
        "the pinned view reads the old value after the retiring pass"
    );

    // Drop the latch: no cut can name the old position any more, so a pass that
    // reclaims the serving stream retires generation 1's file.
    drop(pin);
    let mut seed = RECLAIM_SEED_BASE;
    let mut reclaimed = false;
    for attempt in 0..MAX_RECLAIM_PASSES {
        write_range(&db, &fixture, PINNED, seed, RECLAIM_WRITES);
        seed += RECLAIM_WRITES;
        db.remote_for_test()
            .write_core()
            .flush_durable()
            .expect("certify the reclaim writes");
        pass(&db, "reclaim pass");
        if !wal_files(&fixture, PINNED as usize, pinned_generation).contains(&old_file) {
            reclaimed = true;
            break;
        }
        eprintln!("reclaim attempt {attempt}: file {old_file} still present");
    }
    assert!(
        reclaimed,
        "file {old_file} never retired after the latch dropped: the witness kept it forever"
    );
    db.wait_for_background_threads_to_finish()
        .expect("close the serving generation");
    drop(fixture);
}
