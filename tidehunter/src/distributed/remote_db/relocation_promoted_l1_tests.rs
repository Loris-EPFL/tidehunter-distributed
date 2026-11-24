//! Relocation must re-point an entry that lives only in the cell's on-disk
//! levels. `IndexTable::apply_update` searches only the in-memory table, so a
//! post-promote `[INVALID, L1]` cell (or a sharded one) hides every entry from
//! the compare-and-set unless the pass pre-loads the L1/shard rows first, as
//! native `flusher.rs` `handle_command` does before `RelocationUpdates::apply`.
//!
//! The fixture makes that state deterministic instead of incidental: with
//! `l0_max_entries = 2` the single cell of the one-key-space shape promotes to
//! L1 on its first store (`[INVALID, L1]`) and unloads, so nothing of it is in
//! memory when the pass runs.
//!
//! Negative control (mutation run): delete the L1/shard pre-load in
//! `TableInner::relocation_apply`;
//! `a_promoted_l1_entry_is_repointed_before_its_file_is_retired` fails because
//! the entry keeps its old address while the file that holds it is retired.

use super::super::pipeline_tests::{Fixture, newest_cut};
use super::super::retained_generation_tests::{FILE, FRAGMENT, VALUE, WRITES, key, shape};
use crate::distributed::codec::BatchLimits;
use crate::distributed::durability::DurableCutConfig;
use crate::distributed::group::GroupLimits;
use crate::distributed::sui::Db;
use std::fs;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

/// The owner pollers sample their nudge flag every quarter lag: a short lag
/// keeps the copy durability wait's own request served, and no test relies on
/// the timer.
const QUIET_LAG_MS: u64 = 10_000;
/// Bound of every coordinator and owner operation; only a stuck operation
/// turns into a failure, no test relies on it expiring.
const OPERATION_TIMEOUT: Duration = Duration::from_secs(60);
/// The key whose old address the pass must move. Its first write is the first
/// frame of the serving stream, so it sits in WAL file 0.
const KEY: u64 = 0;

/// The eager fixture with one promotion trigger: every cell holding more than
/// two keys flushes as L1 rather than as L0.
fn promoted_fixture() -> Fixture {
    Fixture::new_with_root(
        tempdir::TempDir::new("hybrid-promoted-l1").expect("temp dir"),
        1,
        8192,
        shape(),
        32,
        |config| {
            // Zero stale window: every dirty cell is flushed by `flush_index`,
            // so the promotion happens on that store and is observable.
            config.index.snapshot_unload_threshold = 0;
            config.index.l0_max_entries = Some(2);
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

/// Writes the data set, then certifies and stores it: the store's flush
/// promotes the single cell to L1 and unloads it.
fn fill_and_promote(db: &Arc<Db>) {
    let ks = db.ks("a");
    for sequence in 0..WRITES {
        let mut batch = db.write_batch();
        batch.write(ks, key(sequence), vec![(sequence % 251) as u8; VALUE]);
        batch.commit().expect("write");
    }
    // The Stop above the data set gives the pass a whole-file target.
    super::retained_generation_tests::stop_marker(db);
    let core = db.remote_for_test().write_core();
    core.flush_durable().expect("certify the data set");
    core.flush_index().expect("promote store");
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

/// The pass copies the promoted cell's as-of entry, re-points it and stores
/// the re-point; the file that held the old position is reclaimed only after
/// that, so the entry never names a retired file.
#[test]
fn a_promoted_l1_entry_is_repointed_before_its_file_is_retired() {
    let fixture = promoted_fixture();
    let db = open(&fixture);
    let generation = db.remote_for_test().write_core().epoch;
    let ks = db.ks("a");
    fill_and_promote(&db);
    let before = db
        .remote_for_test()
        .hybrid_table()
        .get(ks, &key(KEY), newest_cut())
        .expect("index read")
        .expect("the promoted cell holds every key");
    let old_file = before.address.offset / FILE;
    assert_eq!(
        before.address.segment_generation, generation,
        "premise: the entry is in the serving stream"
    );
    assert_eq!(old_file, 0, "premise: the first write is in WAL file 0");
    assert!(
        wal_files(&fixture, 0, generation).contains(&old_file),
        "premise: file 0 exists before the pass"
    );

    let report = db
        .remote_for_test()
        .relocation_pass(false, &AtomicBool::new(false))
        .expect("relocation pass");
    assert!(
        report.copied_survivors > 0,
        "the pass copied the promoted cell's survivors: {report:?}"
    );
    assert!(
        !report.deferred,
        "the pass stored its re-points: {report:?}"
    );

    let after = db
        .remote_for_test()
        .hybrid_table()
        .get(ks, &key(KEY), newest_cut())
        .expect("index read")
        .expect("the key stays indexed");
    assert!(
        after.address != before.address,
        "the promoted L1 entry was not re-pointed: {before:?} -> {after:?}"
    );
    assert_eq!(
        after.batch, before.batch,
        "a re-point keeps the logical version: {before:?} -> {after:?}"
    );
    assert!(
        !wal_files(&fixture, 0, generation).contains(&old_file),
        "the file below the advanced watermark was not reclaimed: {report:?}"
    );
    db.wait_for_background_threads_to_finish().expect("settle");
    drop(fixture);
}
