//! Remote `DropCells` end to end over real loopback owners: one batch through
//! the Sui adapter whatever the key count, validation before any slot, the
//! shared write gate, native-equivalent digests before and after restart,
//! cache eviction beside historical reads, reopen in both ACK tiers and both
//! checkpoint orderings, retry identity, the preflight capacity abort and
//! publication-barrier concurrency with point writers.

use super::pipeline_tests::Fixture;
use super::tests::{fail_stop_without_checkpoint, recovery_metrics, reopen_result};
use super::*;
use crate::cell::CellId;
use crate::distributed::native_index::drop_mask_charge;
use crate::distributed::sui::Db;
use crate::key_shape::{KeyIndexing, KeyShapeBuilder, KeyType};
use rand::{Rng, SeedableRng, rngs::StdRng};
use smallvec::SmallVec;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

/// One keyspace `root`: 4-byte keys over 16 uniform cells of 2^28 keys.
fn uniform_shape() -> KeyShape {
    KeyShape::new_single(4, 4, KeyType::uniform(4))
}

/// `n` of `total` keys spread evenly over the whole 32-bit key space.
fn spread(n: u32, total: u32) -> Vec<u8> {
    (((u64::from(n) << 32) / u64::from(total)) as u32)
        .to_be_bytes()
        .to_vec()
}

fn bounds(db: &Db, ks: KeySpace, first: CellId, last: CellId) -> (Vec<u8>, Vec<u8>) {
    let descriptor = db.shape().ks(ks);
    (
        descriptor.cell_range(&first).0,
        descriptor.cell_range(&last).1,
    )
}

fn integer_bounds(db: &Db, ks: KeySpace, first: usize, last: usize) -> (Vec<u8>, Vec<u8>) {
    bounds(db, ks, CellId::Integer(first), CellId::Integer(last))
}

/// (admitted batches, published batches, owner read RPCs) of a remote DB.
fn counters(db: &Db) -> (u64, u64, u64) {
    let (_, stats, reads) = db.remote_counters().unwrap();
    (stats.admitted_batches, stats.published_batches, reads)
}

fn open(fixture: &Fixture) -> Arc<Db> {
    Db::open_remote_local_fixture(
        &fixture.temp.path().join("authority"),
        fixture.shape.clone(),
        fixture.config.clone(),
    )
    .unwrap()
}

/// Runs `work` on another thread and fails instead of hanging when it blocks.
fn within_deadline<R: Send + 'static>(work: impl FnOnce() -> R + Send + 'static) -> R {
    let (done, result) = mpsc::channel();
    let worker = thread::spawn(move || {
        let _ = done.send(work());
    });
    let value = result
        .recv_timeout(Duration::from_secs(30))
        .expect("operation blocked");
    worker.join().unwrap();
    value
}

#[test]
fn sui_drop_writes_one_batch_and_reads_no_value_whatever_the_key_count() {
    // The fixture admits at most 4 operations per batch: the old
    // enumerate-and-delete path could not drop more than four keys at once.
    for keys in [16u32, 512] {
        let fixture = Fixture::new_with_shape(128, uniform_shape());
        assert!(keys > fixture.config.storage.batch_limits.max_operations);
        let db = open(&fixture);
        let ks = db.ks("root");
        for n in 0..keys {
            db.insert(ks, spread(n, keys), vec![7; 8]).unwrap();
        }
        let (from, to) = integer_bounds(&db, ks, 0, 15);
        let before = counters(&db);
        db.drop_cells_in_range(ks, &from, &to).unwrap();
        let after = counters(&db);
        assert_eq!(after.0 - before.0, 1, "one admitted batch for {keys} keys");
        assert_eq!(after.1 - before.1, 1, "one published batch for {keys} keys");
        assert_eq!(after.2, before.2, "no value read for {keys} keys");
        assert!(db.iterator(ks).next().is_none());
        for n in (0..keys).step_by(37) {
            assert!(!db.exists(ks, &spread(n, keys)).unwrap());
        }
        // A later point in the dropped range is a normal newer write.
        db.insert(ks, spread(3, keys), vec![9; 8]).unwrap();
        assert_eq!(
            db.get(ks, &spread(3, keys)).unwrap().unwrap().as_ref(),
            [9; 8]
        );
    }
}

#[test]
fn invalid_drop_requests_fail_before_any_slot() {
    let mut builder = KeyShapeBuilder::new();
    builder.add_key_space("root", 8, 4, KeyType::uniform(4));
    builder.add_key_space_config_indexing(
        "var",
        KeyIndexing::variable_length(),
        1,
        KeyType::uniform(1),
        Default::default(),
    );
    let fixture = Fixture::new_with_shape(128, builder.build());
    let db = open(&fixture);
    let ks = db.ks("root");
    let var = db.ks("var");
    db.insert(ks, vec![1; 8], vec![1]).unwrap();
    let (first, last) = integer_bounds(&db, ks, 2, 5);
    let mut not_first = first.clone();
    *not_first.last_mut().unwrap() = 1;
    let mut not_last = last.clone();
    *not_last.last_mut().unwrap() = 0xfe;
    let reversed = integer_bounds(&db, ks, 5, 2);
    let before = counters(&db);
    for (space, from, to, expected) in [
        (
            KeySpace(9),
            first.clone(),
            last.clone(),
            "drop_cells_in_range: unknown keyspace 9".to_owned(),
        ),
        (
            var,
            vec![0; 4],
            vec![255; 4],
            "drop_cells_in_range: keyspace var requires fixed-length index keys".to_owned(),
        ),
        (
            ks,
            first[..7].to_vec(),
            last.clone(),
            "drop_cells_in_range: from has invalid key length/reduction".to_owned(),
        ),
        (
            ks,
            first.clone(),
            [last.as_slice(), &[0]].concat(),
            "drop_cells_in_range: to has invalid key length/reduction".to_owned(),
        ),
        (
            ks,
            not_first,
            last.clone(),
            "from_inclusive is not the first key in its cell".to_owned(),
        ),
        (
            ks,
            first.clone(),
            not_last,
            "to_inclusive is not the last key in its cell".to_owned(),
        ),
        (
            ks,
            reversed.0,
            reversed.1,
            "drop_cells_in_range: reversed cell range".to_owned(),
        ),
    ] {
        let message = format!(
            "{:?}",
            db.drop_cells_in_range(space, &from, &to).unwrap_err()
        );
        assert!(
            message.contains(&expected),
            "{message:?} lacks {expected:?}"
        );
    }
    assert_eq!(counters(&db), before, "a rejected drop allocated a slot");
    assert!(db.exists(ks, &[1; 8]).unwrap());
    // A DropCells member never rides in an ordinary point window.
    let error = db
        .remote_for_test()
        .write_batch(
            [0xd0; 16],
            vec![Mutation::DropCells {
                keyspace: ks,
                first_cell: CellId::Integer(0),
                last_cell: CellId::Integer(1),
            }],
        )
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("DropCells must be the only member in a batch")
    );
}

#[test]
fn sui_drop_takes_only_the_shared_write_gate() {
    let fixture = Fixture::new_with_shape(128, uniform_shape());
    let db = open(&fixture);
    let ks = db.ks("root");
    db.insert(ks, spread(1, 16), vec![1]).unwrap();
    let (from, to) = integer_bounds(&db, ks, 0, 15);
    // Another holder of the shared gate (a foreground writer) stays inside
    // it for the whole drop: an exclusive acquisition would block here.
    let shared = db.shared_write_gate_for_test();
    let dropping = Arc::clone(&db);
    within_deadline(move || dropping.drop_cells_in_range(ks, &from, &to)).unwrap();
    drop(shared);
    assert!(db.iterator(ks).next().is_none());
}

/// Keyspaces of the native-equivalence fixture: 16 uniform cells, 2-byte
/// prefixes with three cluster bits (non-byte-aligned cells), and 12-byte
/// keys reduced to their first 8 bytes (colliding full-key suffixes).
fn equivalence_shape() -> KeyShape {
    let mut builder = KeyShapeBuilder::new();
    builder.add_key_space("uniform", 8, 4, KeyType::uniform(4));
    builder.add_key_space("prefixed", 8, 2, KeyType::prefix_uniform(2, 3));
    builder.add_key_space_config_indexing(
        "reduced",
        KeyIndexing::key_reduction(12, 0..8),
        2,
        KeyType::uniform(2),
        Default::default(),
    );
    builder.build()
}

fn universe(db: &Db) -> Vec<(KeySpace, Vec<u8>)> {
    let mut keys = Vec::new();
    for n in 0..48u32 {
        // Cells 0..=11 hold keys; cells 12..=15 stay empty.
        let mut key = (n << 26).to_be_bytes().to_vec();
        key.extend_from_slice(&n.to_be_bytes());
        keys.push((db.ks("uniform"), key));
        keys.push((
            db.ks("prefixed"),
            vec![0, (n * 4) as u8, n as u8, 0, 0, 0, 0, 1],
        ));
        let mut reduced = vec![(n * 5) as u8; 8];
        reduced.extend_from_slice(&[1, 1, 1, 1]);
        keys.push((db.ks("reduced"), reduced.clone()));
        if n % 3 == 0 {
            // Same reduced key, different full key.
            *reduced.last_mut().unwrap() = 2;
            keys.push((db.ks("reduced"), reduced));
        }
    }
    keys
}

#[derive(Debug, PartialEq)]
struct Digest {
    forward: Vec<(u8, Vec<u8>, Vec<u8>)>,
    reverse: Vec<(u8, Vec<u8>, Vec<u8>)>,
    points: Vec<Option<Vec<u8>>>,
    multi: Vec<Option<Vec<u8>>>,
    exists: Vec<bool>,
}

fn digest(db: &Arc<Db>, keys: &[(KeySpace, Vec<u8>)]) -> Digest {
    let rows = |reverse: bool| {
        let mut rows = Vec::new();
        for name in ["uniform", "prefixed", "reduced"] {
            let ks = db.ks(name);
            let mut iterator = db.iterator(ks);
            if reverse {
                iterator.reverse();
            }
            for row in iterator {
                let (key, value) = row.unwrap();
                rows.push((ks.0, key.to_vec(), value.to_vec()));
            }
        }
        rows
    };
    let mut multi = Vec::new();
    for name in ["uniform", "prefixed", "reduced"] {
        let ks = db.ks(name);
        let batch: Vec<&[u8]> = keys
            .iter()
            .filter(|(space, _)| *space == ks)
            .map(|(_, key)| key.as_slice())
            .collect();
        multi.extend(
            db.multi_get(ks, &batch)
                .into_iter()
                .map(|value| value.unwrap().map(|value| value.to_vec())),
        );
    }
    Digest {
        forward: rows(false),
        reverse: rows(true),
        points: keys
            .iter()
            .map(|(ks, key)| db.get(*ks, key).unwrap().map(|value| value.to_vec()))
            .collect(),
        multi,
        exists: keys
            .iter()
            .map(|(ks, key)| db.exists(*ks, key).unwrap())
            .collect(),
    }
}

/// The same deterministic history on any backend: puts, deletes, single-cell,
/// multi-cell, empty-occupied, prefixed and reduced drops, rewrites inside
/// dropped cells, then an overlapping drop.
fn equivalence_history(db: &Arc<Db>, keys: &[(KeySpace, Vec<u8>)]) {
    let value = |phase: u8, key: &[u8]| [&[phase][..], key].concat();
    for (ks, key) in keys {
        db.insert(*ks, key.clone(), value(1, key)).unwrap();
    }
    for (ks, key) in keys.iter().step_by(7) {
        db.remove(*ks, key.clone()).unwrap();
    }
    let uniform = db.ks("uniform");
    let prefixed = db.ks("prefixed");
    let reduced = db.ks("reduced");
    for (first, last) in [(3, 3), (5, 10), (13, 15)] {
        let (from, to) = integer_bounds(db, uniform, first, last);
        db.drop_cells_in_range(uniform, &from, &to).unwrap();
    }
    let (from, to) = bounds(
        db,
        prefixed,
        CellId::Bytes(SmallVec::from_slice(&[0, 8])),
        CellId::Bytes(SmallVec::from_slice(&[0, 48])),
    );
    db.drop_cells_in_range(prefixed, &from, &to).unwrap();
    // Reduced endpoints are full 12-byte keys whose reductions bound cell 0.
    let (first, last) = db.shape().ks(reduced).cell_range(&CellId::Integer(0));
    db.drop_cells_in_range(
        reduced,
        &[first.as_slice(), &[0; 4]].concat(),
        &[last.as_slice(), &[255; 4]].concat(),
    )
    .unwrap();
    for (ks, key) in keys.iter().step_by(3) {
        db.insert(*ks, key.clone(), value(2, key)).unwrap();
    }
    let (from, to) = integer_bounds(db, uniform, 2, 6);
    db.drop_cells_in_range(uniform, &from, &to).unwrap();
    for (ks, key) in keys.iter().skip(1).step_by(11) {
        db.insert(*ks, key.clone(), value(3, key)).unwrap();
    }
}

#[test]
fn remote_drops_match_native_digests_before_and_after_restart() {
    let shape = equivalence_shape();
    let native_dir = tempdir::TempDir::new("native-drop-equivalence").unwrap();
    let native_path = native_dir.path().join("db");
    std::fs::create_dir(&native_path).unwrap();
    let config = Arc::new(crate::config::Config::small());
    let open_native = || {
        Db::open_native(
            &native_path,
            shape.clone(),
            config.clone(),
            recovery_metrics(),
        )
        .unwrap()
    };
    let native = open_native();
    let fixture = Fixture::new_with_shape(128, shape.clone());
    let remote = open(&fixture);
    let keys = universe(&remote);
    equivalence_history(&native, &keys);
    equivalence_history(&remote, &keys);
    let expected = digest(&native, &keys);
    assert!(expected.points.iter().any(Option::is_some));
    assert!(expected.points.iter().any(Option::is_none));
    assert_eq!(digest(&remote, &keys), expected);
    native.wait_for_background_threads_to_finish().unwrap();
    drop(remote);
    let native = open_native();
    let remote = open(&fixture);
    let restarted = digest(&native, &keys);
    assert_eq!(restarted, expected, "native restart changed its own digest");
    assert_eq!(digest(&remote, &keys), restarted);
    native.wait_for_background_threads_to_finish().unwrap();
}

#[test]
fn drop_evicts_cached_values_and_historical_reads_keep_old_rows() {
    let fixture = Fixture::new_with_shape(128, uniform_shape());
    let db = open(&fixture);
    let ks = db.ks("root");
    // Even keys land in cells 0..=7, odd keys in cells 8..=15.
    let key = |n: u32| ((n % 2) << 31 | n << 8).to_be_bytes().to_vec();
    for n in 0..16 {
        db.insert(ks, key(n), vec![n as u8; 8]).unwrap();
        assert!(db.get(ks, &key(n)).unwrap().is_some());
    }
    let cache = db.remote_for_test().read_cache_stats();
    assert!(cache.entries > 0, "post-publish warming caches the puts");
    let checkpoint = db.checkpoint();
    let (from, to) = integer_bounds(&db, ks, 0, 7);
    db.drop_cells_in_range(ks, &from, &to).unwrap();
    let after = db.remote_for_test().read_cache_stats();
    assert!(after.entries <= 8, "dropped entries stay cached: {after:?}");
    assert_eq!(
        after.entries + (after.evictions - cache.evictions) as usize,
        cache.entries
    );
    // A pinned historical cut still reads (and may refill) the old rows.
    let old: Vec<_> = checkpoint
        .iterator(ks)
        .map(|row| row.unwrap().0.to_vec())
        .collect();
    assert_eq!(old.len(), 16);
    // Current reads never see them, refilled or not.
    for n in 0..16 {
        assert_eq!(
            db.get(ks, &key(n)).unwrap().is_some(),
            n % 2 == 1,
            "key {n}"
        );
    }
    // A newer point inside the range survives the older drop.
    db.insert(ks, key(0), vec![99]).unwrap();
    assert_eq!(db.get(ks, &key(0)).unwrap().unwrap().as_ref(), [99]);
}

#[derive(Clone, Copy, Debug)]
enum CheckpointOrder {
    /// The drop stays in the replay tail.
    DropOnly,
    /// A checkpoint precedes the drop; the drop is in the tail.
    CheckpointThenDrop,
    /// The drop is inside the adopted root (absorbed or retained).
    DropThenCheckpoint,
    /// A newer point in range follows the drop; both are checkpointed.
    DropPutThenCheckpoint,
}

fn put(key: u32, value: u8) -> Mutation {
    Mutation::Put {
        keyspace: KeySpace(0),
        key: key.to_be_bytes().to_vec().into(),
        value: vec![value; 8].into(),
    }
}

/// Keys `n << 27`: two per cell over 16 cells. The drop covers cells 4..=11.
fn tier_key(n: u32) -> u32 {
    n << 27
}

fn dropped(n: u32) -> bool {
    (8..24).contains(&n)
}

#[test]
fn reopen_recovers_drops_in_both_tiers_and_checkpoint_orderings() {
    for synchronous in [false, true] {
        for ordering in [
            CheckpointOrder::DropOnly,
            CheckpointOrder::CheckpointThenDrop,
            CheckpointOrder::DropThenCheckpoint,
            CheckpointOrder::DropPutThenCheckpoint,
        ] {
            let case = format!("synchronous={synchronous} {ordering:?}");
            let fixture = Fixture::new_with_shape(128, uniform_shape());
            let fixture = if synchronous {
                fixture.synchronous()
            } else {
                fixture
            };
            let db = fixture.open();
            for n in 0..32 {
                db.write_batch(
                    fixture.operation(n % 5, u64::from(n) + 1),
                    vec![put(tier_key(n), 1)],
                )
                .unwrap();
            }
            let drop_operation = [0xdc; 16];
            let mutation = Mutation::DropCells {
                keyspace: KeySpace(0),
                first_cell: CellId::Integer(4),
                last_cell: CellId::Integer(11),
            };
            if matches!(ordering, CheckpointOrder::CheckpointThenDrop) {
                db.core.flush_index().unwrap();
            }
            let version = db
                .core
                .write_drop_cells(drop_operation, mutation.clone())
                .unwrap();
            let rewritten = tier_key(10);
            if matches!(ordering, CheckpointOrder::DropPutThenCheckpoint) {
                db.write_batch([0xee; 16], vec![put(rewritten, 2)]).unwrap();
            }
            if matches!(
                ordering,
                CheckpointOrder::DropThenCheckpoint | CheckpointOrder::DropPutThenCheckpoint
            ) {
                db.core.flush_index().unwrap();
            }
            fail_stop_without_checkpoint(db);
            for reopen in 0..2 {
                let mut db = reopen_result(&fixture).unwrap();
                for n in 0..32 {
                    let expected = if matches!(ordering, CheckpointOrder::DropPutThenCheckpoint)
                        && tier_key(n) == rewritten
                    {
                        Some(vec![2; 8])
                    } else if dropped(n) {
                        None
                    } else {
                        Some(vec![1; 8])
                    };
                    assert_eq!(
                        db.get(KeySpace(0), &tier_key(n).to_be_bytes())
                            .unwrap()
                            .map(|value| value.to_vec()),
                        expected,
                        "{case} reopen {reopen} key {n}"
                    );
                }
                // The acknowledged range retries to its original version.
                assert_eq!(
                    db.core
                        .write_drop_cells(drop_operation, mutation.clone())
                        .unwrap(),
                    version,
                    "{case} reopen {reopen}"
                );
                fail_stop_without_checkpoint(db);
            }
        }
    }
}

#[test]
fn preflight_capacity_failure_aborts_without_effect_and_releases_the_barrier() {
    let mut builder = KeyShapeBuilder::new();
    builder.add_key_space("cells", 8, 4, KeyType::prefix_uniform(2, 0));
    let budget = 256 * 1024;
    let fixture = Fixture::new_with_config(128, builder.build(), 2, |config| {
        config.max_index_bytes = budget;
    });
    let db = open(&fixture);
    let ks = db.ks("cells");
    let key = |cell: u16| [&cell.to_be_bytes()[..], &[0; 6]].concat();
    // Fill fresh cells until the masks of all of them no longer fit. The decision must
    // rest on a settled total: a flush folds the transient per-cell charges into the
    // resident total, which can free room, and deciding on an unsettled total made this
    // test pass with the drop still admitted. The preflight (write_core/drop_cells.rs)
    // adds the target masks to the resident total and to the index bytes already
    // reserved for the slot, so a settled resident total that cannot hold the masks
    // cannot be admitted either.
    let mut cells = 0u16;
    loop {
        while budget.saturating_sub(db.remote_for_test().index_stats().charged_metadata_bytes)
            >= drop_mask_charge(usize::from(cells)).unwrap()
        {
            db.insert(ks, key(cells), vec![1]).unwrap();
            cells += 1;
            assert!(cells < 4096, "the budget never filled");
        }
        let remote = db.remote_for_test();
        remote.flush_index().unwrap();
        let charged = remote.index_stats().charged_metadata_bytes;
        if budget.saturating_sub(charged) < drop_mask_charge(usize::from(cells)).unwrap() {
            break;
        }
    }
    let all = (
        CellId::Bytes(SmallVec::from_slice(&[0, 0])),
        CellId::Bytes(SmallVec::from_slice(&[255, 255])),
    );
    let (from, to) = bounds(&db, ks, all.0.clone(), all.1.clone());
    let message = format!("{:?}", db.drop_cells_in_range(ks, &from, &to).unwrap_err());
    assert!(
        message.contains("DropCells exceeds index metadata capacity"),
        "{message}"
    );
    for cell in (0..cells).step_by(17) {
        assert!(db.exists(ks, &key(cell)).unwrap(), "cell {cell} lost a row");
    }
    // The aborted slot left no hole and released the barrier: points and a
    // smaller range publish normally afterwards.
    db.insert(ks, key(1), vec![2]).unwrap();
    let one = CellId::Bytes(SmallVec::from_slice(&[0, 1]));
    let (from, to) = bounds(&db, ks, one.clone(), one);
    db.drop_cells_in_range(ks, &from, &to).unwrap();
    assert!(!db.exists(ks, &key(1)).unwrap());
    assert!(db.exists(ks, &key(0)).unwrap());
}

#[test]
fn concurrent_point_writers_order_around_the_range_barrier() {
    let fixture = Fixture::new_with_shape(128, uniform_shape());
    let db = open(&fixture);
    let ks = db.ks("root");
    for n in 0..32 {
        db.insert(ks, tier_key(n).to_be_bytes().to_vec(), vec![0; 8])
            .unwrap();
    }
    let writes = Arc::new(Mutex::new(Vec::<(u32, BatchVersion)>::new()));
    let start = Arc::new(std::sync::Barrier::new(5));
    let range = within_deadline({
        let db = Arc::clone(&db);
        let writes = Arc::clone(&writes);
        let start = Arc::clone(&start);
        move || {
            thread::scope(|scope| {
                for writer in 0..4u8 {
                    let (db, writes, start) = (&db, &writes, &start);
                    scope.spawn(move || {
                        let mut rng = StdRng::seed_from_u64(u64::from(writer));
                        start.wait();
                        for step in 0..24u8 {
                            let n = rng.gen_range(0..32);
                            let mut operation = [writer; 16];
                            operation[15] = step;
                            operation[0] = 0xa0;
                            let version = db
                                .remote_for_test()
                                .write_batch(
                                    operation,
                                    vec![Mutation::Put {
                                        keyspace: KeySpace(0),
                                        key: tier_key(n).to_be_bytes().to_vec().into(),
                                        value: vec![writer + 1; 8].into(),
                                    }],
                                )
                                .unwrap();
                            writes.lock().push((n, version));
                        }
                    });
                }
                start.wait();
                thread::sleep(Duration::from_millis(2));
                db.remote_for_test()
                    .drop_cells(
                        [0xdd; 16],
                        KeySpace(0),
                        CellId::Integer(4),
                        CellId::Integer(11),
                    )
                    .unwrap()
            })
        }
    });
    let writes = writes.lock();
    assert_eq!(writes.len(), 96, "every point writer finished");
    for n in 0..32 {
        let newest = writes
            .iter()
            .filter(|(key, _)| *key == n)
            .map(|(_, version)| *version)
            .max();
        let visible = db.get(ks, &tier_key(n).to_be_bytes()).unwrap().is_some();
        // Logical order decides: a range hides only lower versions.
        let expected = !dropped(n) || newest.is_some_and(|version| version > range);
        assert_eq!(
            visible, expected,
            "key {n} newest {newest:?} range {range:?}"
        );
    }
    // Interleaving with the writers is best effort; a write after the range
    // returns always orders above it and stays visible.
    let after = db
        .remote_for_test()
        .write_batch(
            [0xaf; 16],
            vec![Mutation::Put {
                keyspace: KeySpace(0),
                key: tier_key(9).to_be_bytes().to_vec().into(),
                value: vec![9; 8].into(),
            }],
        )
        .unwrap();
    assert!(after > range);
    assert!(db.get(ks, &tier_key(9).to_be_bytes()).unwrap().is_some());
}
