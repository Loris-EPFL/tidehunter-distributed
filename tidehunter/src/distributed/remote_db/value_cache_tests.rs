//! The coordinator value cache end to end: the native per-cell LRU that Sui
//! enables with `value_cache_size`.
//!
//! These tests go through the real read path over fixture owners, so they cover
//! what the index-level tests cannot: a resident value of a loaded cell costs no
//! owner round trip, a value fetched after a reopen refills the LRU, a
//! checkpoint read serves the as-of value without seeding the LRU with it, a
//! refused write leaves nothing resident, and a cached database answers exactly
//! like an uncached one through a mixed workload and a reopen.
//!
//! Every read is resolved at a sampled cut, so the LRU answers only while the
//! key is unchanged since that cut and its cell is loaded; the resident value of
//! an unloaded cell is never served (see `docs/distributed/ARCHITECTURE.md`).
//!
//! The scalar cases use the exclusive [`RemoteDatabase`], whose value reader
//! counts identity-cache misses: a miss is exactly a read that fetches from an
//! owner, so it measures what a read costs without asserting anything about
//! timing. The mixed workload uses the Sui-facing [`Db`], which owns the batch
//! reads, the iterator and the range drop.

use super::pipeline_tests::Fixture;
use super::*;
use crate::cell::CellId;
use crate::distributed::codec::Mutation;
use crate::distributed::hybrid::table::{RelocationApply, ResolvedValue};
use crate::distributed::sui::Db;
use crate::index::index_format::IndexIterCaches;
use crate::key_shape::{KeyShape, KeySpaceConfig, KeyType};
use crate::relocation::Decision;
use minibytes::Bytes;
use std::collections::BTreeSet;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// 4-byte keys over 16 uniform cells (one cell per 4-bit prefix), with a native
/// per-cell value cache of `cache` entries. `cache = 0` is native's "no cache".
fn cached_shape(cache: usize) -> KeyShape {
    KeyShape::new_single_config(
        4,
        4,
        KeyType::uniform(4),
        KeySpaceConfig::new().with_value_cache_size(cache),
    )
}

/// A two-owner fixture: the value cache has nothing to do with owner count, and
/// the suite already pays for enough five-owner fixtures.
fn fixture(name: &str, shape: KeyShape) -> Fixture {
    Fixture::new_with_root(
        tempdir::TempDir::new(name).expect("scratch directory"),
        2,
        128,
        shape,
        2,
        |_| {},
        |_| {},
    )
}

fn open(fixture: &Fixture) -> Arc<Db> {
    Db::open_remote_local_fixture(
        &fixture.temp.path().join("authority"),
        fixture.shape.clone(),
        fixture.config.clone(),
    )
    .expect("open the fixture database")
}

/// The key of cell `cell` in the fixture shape.
fn key(cell: u32) -> Vec<u8> {
    (cell << 28).to_be_bytes().to_vec()
}

fn put(key: &[u8], value: u8) -> Mutation {
    Mutation::Put {
        keyspace: KeySpace(0),
        key: Bytes::copy_from_slice(key),
        value: Bytes::from(vec![value; 8]),
    }
}

/// The value reader's identity-cache miss count: one miss is one owner fetch.
fn owner_fetches(db: &RemoteDatabase) -> u64 {
    db.reader.cache_misses()
}

#[test]
fn a_published_value_is_resident_and_costs_no_owner_read() {
    let fixture = fixture("remote-cache-resident", cached_shape(16));
    let mut db = fixture.open();
    db.write_batch([1; 16], vec![put(&key(1), 1)])
        .expect("write");
    db.drain().expect("drain");
    db.core.wait_settled().expect("settled");

    // Drop the derivative position cache the publication warmed, so only the
    // native per-cell LRU can answer the next reads.
    db.clear_read_cache();
    let before = owner_fetches(&db);
    assert_eq!(
        db.get(KeySpace(0), &key(1)).unwrap().as_deref(),
        Some(&[1u8; 8][..]),
        "the value the write published is served",
    );
    assert_eq!(
        owner_fetches(&db),
        before,
        "a resident value needed no owner fetch",
    );
    assert!(
        db.exists(KeySpace(0), &key(1)).unwrap(),
        "the key is present",
    );
    assert_eq!(
        owner_fetches(&db),
        before,
        "exists also answered from the LRU",
    );
}

#[test]
fn a_reopen_refills_the_cache_from_the_owner() {
    let fixture = fixture("remote-cache-refill", cached_shape(16));
    let ks = KeySpace(0);
    {
        let db = fixture.open();
        db.write_batch([1; 16], vec![put(&key(1), 1)])
            .expect("write");
        db.drain().expect("drain");
        db.shutdown().expect("shutdown");
    }

    let mut db = fixture.open();
    let before = owner_fetches(&db);
    assert_eq!(
        db.get(ks, &key(1)).unwrap().as_deref(),
        Some(&[1u8; 8][..]),
        "the first read after a reopen fetches the value from its owner",
    );
    let after_fetch = owner_fetches(&db);
    assert_eq!(
        after_fetch,
        before + 1,
        "a cold value cache means exactly one owner fetch",
    );
    // Drop the derivative position cache so the value LRU is the only cache that
    // can answer; the read must still cost no owner contact.
    db.clear_read_cache();
    assert_eq!(db.get(ks, &key(1)).unwrap().as_deref(), Some(&[1u8; 8][..]),);
    assert_eq!(
        owner_fetches(&db),
        after_fetch,
        "the live read refilled the LRU from its owner fetch",
    );
}

/// Wait until `key` reads `value` through the public path: a write becomes
/// visible (index row and resident value) only when it publishes, and that is
/// the state the assertions below need. Waits on observed progress, never a
/// fixed sleep.
fn wait_visible(db: &mut RemoteDatabase, ks: KeySpace, key: &[u8], value: &[u8]) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while db.get(ks, key).expect("read").as_deref() != Some(value) {
        db.core.ensure_healthy().expect("owner append failed");
        assert!(
            Instant::now() < deadline,
            "the write to {key:?} did not become visible",
        );
        std::thread::yield_now();
    }
}

#[test]
fn a_checkpoint_read_serves_the_as_of_value_without_refilling() {
    let fixture = fixture("remote-cache-checkpoint", cached_shape(16));
    let ks = KeySpace(0);
    let mut db = fixture.open();
    db.write_batch([1; 16], vec![put(&key(1), 1)])
        .expect("first write");
    wait_visible(&mut db, ks, &key(1), &[1; 8]);
    let checkpoint = db.checkpoint().expect("checkpoint");
    db.write_batch([2; 16], vec![put(&key(1), 2)])
        .expect("second write");
    wait_visible(&mut db, ks, &key(1), &[2; 8]);
    // Only the native per-cell LRU (holding the newer value) and the owners can
    // answer now.
    db.clear_read_cache();

    let before = owner_fetches(&db);
    assert_eq!(
        db.get_at(ks, &key(1), &checkpoint).unwrap().as_deref(),
        Some(&[1u8; 8][..]),
        "the latched read sees the value as of its cut, not the newer resident one",
    );
    assert_eq!(
        owner_fetches(&db),
        before + 1,
        "the as-of value came from its owner",
    );
    assert_eq!(
        db.get(ks, &key(1)).unwrap().as_deref(),
        Some(&[2u8; 8][..]),
        "the live read still sees the newer value",
    );
    assert_eq!(
        owner_fetches(&db),
        before + 1,
        "a checkpoint read did not seed the LRU with its as-of value",
    );
}

#[test]
fn a_refused_write_leaves_no_resident_value() {
    let fixture = fixture("remote-cache-refused", cached_shape(16));
    let ks = KeySpace(0);
    let mut db = fixture.open();
    // The fixture admits at most 2048 encoded bytes per batch.
    db.write_batch(
        [1; 16],
        vec![Mutation::Put {
            keyspace: ks,
            key: Bytes::copy_from_slice(&key(1)),
            value: Bytes::from(vec![7; 8192]),
        }],
    )
    .expect_err("an oversized member is refused before admission");
    assert_eq!(
        db.get(ks, &key(1)).unwrap(),
        None,
        "a write that never published leaves nothing readable",
    );
    db.write_batch([2; 16], vec![put(&key(1), 8)])
        .expect("write");
    db.drain().expect("drain");
    assert_eq!(
        db.get(ks, &key(1)).unwrap().as_deref(),
        Some(&[8u8; 8][..]),
        "the next write is served from its own value",
    );
}

/// The observable reads of the mixed workload, in call order.
fn mixed_reads(db: &Arc<Db>) -> Vec<String> {
    let ks = db.ks("root");
    let mut seen = Vec::new();
    for cell in 0..8u32 {
        seen.push(format!("get {cell}: {:?}", db.get(ks, &key(cell)).unwrap()));
    }
    for cell in 0..8u32 {
        seen.push(format!(
            "exists {cell}: {:?}",
            db.exists(ks, &key(cell)).unwrap()
        ));
    }
    let mut scanned = Vec::new();
    for row in db.iterator(ks) {
        let (key, value) = row.expect("iterate");
        scanned.push(format!("{:?}={:?}", key.as_ref(), value.as_ref()));
    }
    seen.push(format!("scan: {scanned:?}"));
    seen
}

/// Writes, overwrites, a delete, a range drop, batch reads and scans: one
/// shape-independent workload both databases run.
fn mixed_workload(db: &Arc<Db>) -> Vec<String> {
    let ks = db.ks("root");
    let mut seen = Vec::new();
    for cell in 0..8u32 {
        db.insert(ks, key(cell), vec![cell as u8; 8]).unwrap();
    }
    seen.extend(mixed_reads(db));
    db.insert(ks, key(1), vec![0xAA; 8]).unwrap();
    db.remove(ks, key(2)).unwrap();
    let keys: Vec<Vec<u8>> = (0..8u32).map(key).collect();
    let batch: Vec<_> = db
        .multi_get(ks, &keys)
        .into_iter()
        .map(|value| format!("{:?}", value.expect("multi_get")))
        .collect();
    seen.push(format!("multi_get: {batch:?}"));
    let (from, to) = {
        let descriptor = db.shape().ks(ks);
        (
            descriptor.cell_range(&CellId::Integer(0)).0,
            descriptor.cell_range(&CellId::Integer(3)).1,
        )
    };
    db.drop_cells_in_range(ks, &from, &to).unwrap();
    seen.extend(mixed_reads(db));
    seen
}

#[test]
fn a_cached_and_an_uncached_database_answer_identically() {
    // The synchronous tier makes every acknowledged write survive the reopen the
    // second half of this test relies on.
    let cached = fixture("remote-cache-on", cached_shape(16)).synchronous();
    let uncached = fixture("remote-cache-off", cached_shape(0)).synchronous();
    let cached_db = open(&cached);
    let uncached_db = open(&uncached);
    let cached_seen = mixed_workload(&cached_db);
    let uncached_seen = mixed_workload(&uncached_db);
    assert_eq!(
        cached_seen, uncached_seen,
        "cache-on and cache-off answer the same through the workload",
    );

    // Reopen both: the cache is empty again and must not change an answer.
    drop(cached_db);
    drop(uncached_db);
    let cached_reopened = open(&cached);
    let uncached_reopened = open(&uncached);
    let cached_after = mixed_reads(&cached_reopened);
    let uncached_after = mixed_reads(&uncached_reopened);
    assert_eq!(
        cached_after, uncached_after,
        "cache-on and cache-off answer the same after a reopen",
    );
    assert_eq!(
        cached_after,
        mixed_reads(&cached_reopened),
        "a second read of the reopened database is stable",
    );
}

/// The unloaded-cell gap, deliberately kept: after a flush folds the dirty
/// overlay into a blob and unloads the cell, the coordinator's per-cell LRU
/// still holds the published value, but the cut-checked lookup cannot consult
/// it, so the scalar read fetches the member from its owner. Serving the LRU
/// here would require native `Db::get` semantics, which also see a batch that
/// sparse publication installed above the published prefix (see
/// `docs/distributed/ARCHITECTURE.md`).
#[test]
fn an_unloaded_cell_reads_from_its_owner() {
    let fixture = Fixture::new_with_root(
        tempdir::TempDir::new("remote-cache-unloaded").expect("scratch directory"),
        2,
        128,
        cached_shape(16),
        2,
        // Every dirty cell is stale: the next snapshot pass flushes and unloads.
        |config| config.index.snapshot_unload_threshold = 0,
        |_| {},
    );
    let ks = KeySpace(0);
    let mut db = fixture.open();
    db.write_batch([1; 16], vec![put(&key(1), 1)])
        .expect("write");
    db.drain().expect("drain");
    db.core.wait_settled().expect("settled");
    // The durable-cut record of this fixture lags the published prefix; the
    // native flush frontier is `min(durable, applied)`, so advance the durable
    // front to the write to let one pass cover it (as the record itself would).
    db.core.table.cuts().set_durable(db.core.table.current());

    db.flush_index()
        .expect("flush the dirty cell into an index blob");
    let cell = fixture.shape.ks(ks).cell_id(&key(1));
    assert_eq!(
        db.core
            .table
            .large_table()
            .entry_state_for_test(db.core.table.contexts().ks_context(ks), &cell,),
        "Unloaded",
        "premise: the flush unloaded the cell and kept its value LRU",
    );

    // Drop the derivative position cache so the read cannot answer from it.
    db.clear_read_cache();
    let before = owner_fetches(&db);
    assert_eq!(
        db.get(ks, &key(1)).unwrap().as_deref(),
        Some(&[1u8; 8][..]),
        "the read answers the value the unloaded cell's member holds",
    );
    assert_eq!(
        owner_fetches(&db),
        before + 1,
        "the cut-checked read of an unloaded cell fetches its member",
    );
    assert!(db.exists(ks, &key(1)).unwrap(), "the key is present",);
    assert_eq!(
        owner_fetches(&db),
        before + 1,
        "exists is answered by the index alone, with no owner fetch",
    );
}

/// The same shape with a relocation filter: a filter drop marks a key
/// `Removed` while deliberately leaving its resident value, so only the
/// index-first cut-checked lookup answers not-found for it.
fn filtered_shape(cache: usize) -> KeyShape {
    KeyShape::new_single_config(
        4,
        4,
        KeyType::uniform(4),
        KeySpaceConfig::new()
            .with_value_cache_size(cache)
            .with_relocation_filter(|_, _| Decision::Keep),
    )
}

/// A batch whose keys all live in unloaded cells: the cut-checked lookup names
/// each member position, so the batch fetches every miss in one batched owner
/// read per owner. The derivative position cache is cleared first so no slot can
/// answer from it.
#[test]
fn a_batch_of_unloaded_cells_fetches_each_owner_once() {
    let fixture = Fixture::new_with_root(
        tempdir::TempDir::new("remote-cache-batch-unloaded").expect("scratch directory"),
        2,
        128,
        cached_shape(16),
        2,
        // Every dirty cell is stale: the forced pass flushes and unloads.
        |config| config.index.snapshot_unload_threshold = 0,
        |_| {},
    );
    let ks = KeySpace(0);
    let db = open(&fixture);
    for cell in [1u32, 2] {
        db.insert(ks, key(cell), vec![cell as u8; 8])
            .expect("write");
    }

    let core = db.remote_for_test().write_core();
    core.wait_settled().expect("settled");
    let table = db.remote_for_test().hybrid_table();
    // The fixture's durable-cut record lags the published prefix, and the
    // native flush frontier is `min(durable, applied)`.
    table.cuts().set_durable(table.current());
    core.flush_index()
        .expect("flush the dirty cell into an index blob");
    for cell in [1u32, 2] {
        let entry = fixture.shape.ks(ks).cell_id(&key(cell));
        assert_eq!(
            table
                .large_table()
                .entry_state_for_test(table.contexts().ks_context(ks), &entry),
            "Unloaded",
            "premise: the forced flush unloaded cell {cell} and kept its value LRU",
        );
    }

    let remote = db.remote_for_test();
    remote.cache.clear();
    let cut = remote.published_cut().unwrap();
    let keys = [key(1), key(2)];
    let owners: BTreeSet<u32> = keys
        .iter()
        .map(|key| match table.get_value(ks, key, cut) {
            Ok(Some(ResolvedValue::Position(position))) => position.address.partition,
            other => panic!("premise: key {key:?} has an owner position, got {other:?}"),
        })
        .collect();
    let submissions = remote.live_read_submission_count();
    let fetches = remote.reader.cache_misses();
    let values = db.multi_get(ks, &keys);
    for (slot, cell) in [1u8, 2].into_iter().enumerate() {
        assert_eq!(
            values[slot].as_ref().unwrap().as_deref(),
            Some([cell; 8].as_slice()),
            "the value of the unloaded cell is fetched from its member",
        );
    }
    assert_eq!(
        remote.reader.cache_misses() - fetches,
        keys.len() as u64,
        "every unloaded cell needed one owner fetch",
    );
    assert_eq!(
        remote.live_read_submission_count() - submissions,
        owners.len() as u64,
        "the misses went out as one batched read per owner",
    );
    db.wait_for_background_threads_to_finish().unwrap();
}

/// A mixed batch serves the resident value and fetches the misses in one
/// batched owner read per owner. Both owners are covered, duplicate keys are
/// folded into the same batch, and an absent key costs nothing.
#[test]
fn a_mixed_batch_fetches_each_owner_once_for_the_misses_only() {
    let fixture = fixture("remote-cache-batch-mixed", cached_shape(16)).synchronous();
    let ks = KeySpace(0);
    let db = open(&fixture);
    for cell in 0..4u32 {
        db.insert(ks, key(cell), vec![cell as u8; 8])
            .expect("write");
    }
    db.wait_for_background_threads_to_finish()
        .expect("shut down before the reopen");
    let db = open(&fixture);
    // The reopen leaves the LRU and the derivative cache empty. Read one key so
    // it is the only resident value; the other keys are misses.
    assert_eq!(
        db.get(ks, &key(0)).unwrap().as_deref(),
        Some(&[0u8; 8][..]),
        "the reopened value is fetched and refills the LRU",
    );
    let remote = db.remote_for_test();
    remote.cache.clear();
    let cut = remote.published_cut().unwrap();
    let misses: Vec<Vec<u8>> = (1..4u32).map(key).collect();
    let owners: BTreeSet<u32> = misses
        .iter()
        .map(|key| match remote.hybrid_table().get_value(ks, key, cut) {
            Ok(Some(ResolvedValue::Position(position))) => position.address.partition,
            other => panic!("premise: key {key:?} has an owner position, got {other:?}"),
        })
        .collect();
    assert!(
        owners.len() >= 2,
        "premise: the misses span both owners, got {owners:?}",
    );

    let submissions = remote.live_read_submission_count();
    let fetches = remote.reader.cache_misses();
    let keys: Vec<Vec<u8>> = (0..4u32).map(key).chain(std::iter::once(key(9))).collect();
    let values = db.multi_get(ks, &keys);
    for (slot, cell) in (0..4u32).enumerate() {
        assert_eq!(
            values[slot].as_ref().unwrap().as_deref(),
            Some([cell as u8; 8].as_slice()),
            "slot {slot} is the value cell {cell} wrote",
        );
    }
    assert!(
        values[4].as_ref().unwrap().is_none(),
        "an absent key answers not-found",
    );
    assert_eq!(
        remote.reader.cache_misses() - fetches,
        misses.len() as u64,
        "only the misses touched an owner, the resident key did not",
    );
    assert_eq!(
        remote.live_read_submission_count() - submissions,
        owners.len() as u64,
        "the misses went out as one batched read per owner",
    );
    db.wait_for_background_threads_to_finish().unwrap();
}

/// The live batch answers exactly what native `multi_get` answers for the same
/// data, with the cache on and off. Native runs one `Db::get` per key, so the
/// batch result is per-slot and order-preserving, duplicates included.
#[test]
fn a_batch_matches_native_multi_get_with_the_cache_on_and_off() {
    /// Puts, an overwrite, a remove, then a batch with duplicates and an absent
    /// key: the whole per-slot answer, formatted for comparison.
    fn workload(db: &Arc<Db>) -> Vec<String> {
        let ks = db.ks("root");
        for cell in 0..4u32 {
            db.insert(ks, key(cell), vec![cell as u8; 8]).expect("put");
        }
        db.insert(ks, key(1), vec![0xAA; 8]).expect("overwrite");
        db.remove(ks, key(2)).expect("remove");
        let keys: Vec<Vec<u8>> = [0u32, 1, 2, 3, 9, 1].into_iter().map(key).collect();
        db.multi_get(ks, &keys)
            .into_iter()
            .map(|value| format!("{:?}", value.expect("multi_get")))
            .collect()
    }

    let cached = fixture("remote-cache-batch-parity-on", cached_shape(16));
    let uncached = fixture("remote-cache-batch-parity-off", cached_shape(0));
    let cached_db = open(&cached);
    let uncached_db = open(&uncached);
    let native_dir = tempdir::TempDir::new("native-cache-batch-parity").expect("scratch directory");
    let native = Db::open_native(
        native_dir.path(),
        cached_shape(0),
        Arc::new(crate::config::Config::small()),
        super::tests::recovery_metrics(),
    )
    .expect("open native");
    let expected = workload(&native);
    assert_eq!(
        workload(&cached_db),
        expected,
        "the cache-on hybrid answers every batch slot like native multi_get",
    );
    assert_eq!(
        workload(&uncached_db),
        expected,
        "the cache-off hybrid answers every batch slot like native multi_get",
    );
}

/// Per-key live semantics of a batch: a slot never returns a state older than
/// the newest one published when the call started. Keys are published with
/// increasing values, then removed. A slot may observe a concurrent overwrite
/// or the remove (the value it returns was visible when the call started), but
/// a slot never rewinds, and a completed remove before the call answers
/// not-found.
#[test]
fn a_concurrent_overwrite_and_remove_never_rewinds_a_batch_slot() {
    const LAST: u64 = 64;
    const READS: usize = 4_000;
    let fixture = fixture("remote-cache-batch-race", cached_shape(16));
    let ks = KeySpace(0);
    let db = open(&fixture);
    let raw = key(0);
    db.insert(ks, raw.clone(), 0u64.to_be_bytes().to_vec())
        .expect("seed");
    let floor = AtomicU64::new(0);
    let removed = AtomicBool::new(false);
    std::thread::scope(|scope| {
        let db = &db;
        let raw = &raw;
        let floor = &floor;
        let removed = &removed;
        let writer = scope.spawn(move || {
            for sequence in 1..=LAST {
                db.insert(ks, raw.clone(), sequence.to_be_bytes().to_vec())
                    .expect("overwrite");
                // The value is installed before the acknowledgement returns, so
                // `floor` only names a value every later read can see.
                floor.store(sequence, Ordering::Release);
            }
            db.remove(ks, raw.clone()).expect("remove");
            removed.store(true, Ordering::Release);
        });
        let reader = scope.spawn(move || {
            let mut last = 0u64;
            for _ in 0..READS {
                // Both facts are read before the call: a value at or above
                // `floor` was published, and a set `removed` completed first.
                let visible = floor.load(Ordering::Acquire);
                let removed_before = removed.load(Ordering::Acquire);
                let values = db.multi_get(ks, std::slice::from_ref(raw));
                let slot = values
                    .into_iter()
                    .next()
                    .expect("one slot")
                    .expect("batch read");
                if removed_before {
                    assert!(
                        slot.is_none(),
                        "a remove completed before the batch, yet the slot held {slot:?}",
                    );
                }
                let Some(value) = slot else {
                    continue;
                };
                let seen = u64::from_be_bytes(
                    value
                        .as_ref()
                        .try_into()
                        .expect("the fixture publishes eight-byte values"),
                );
                assert!(
                    seen >= visible,
                    "a batch slot returned {seen} older than the value {visible} visible when the call started",
                );
                assert!(
                    seen >= last,
                    "batch slots regressed from {last} to {seen}",
                );
                last = seen;
            }
        });
        writer.join().expect("the writer thread must not panic");
        reader.join().expect("the reader thread must not panic");
    });
    db.wait_for_background_threads_to_finish().unwrap();
}

/// A relocation-filtered keyspace keeps the index-first cut-checked batch: a
/// filter drop hides the key even though its value is still resident.
#[test]
fn a_filtered_keyspace_batch_stays_index_first() {
    let fixture = fixture("remote-cache-batch-filtered", filtered_shape(16));
    let ks = KeySpace(0);
    let db = open(&fixture);
    let raw = key(1);
    db.insert(ks, raw.clone(), vec![7; 8]).expect("write");
    assert_eq!(
        db.multi_get(ks, std::slice::from_ref(&raw))[0]
            .as_ref()
            .unwrap()
            .as_deref(),
        Some(&[7u8; 8][..]),
        "the published value is readable",
    );

    let table = db.remote_for_test().hybrid_table();
    // A latched index walk never consults the value LRU, so it names the member
    // position even though the published value is still resident.
    let mut caches = IndexIterCaches::new();
    let mut cell = table.ks_desc(ks).first_cell();
    let mut previous = None;
    let position = loop {
        let entry = table
            .next_entry(
                ks,
                cell,
                previous,
                &None,
                false,
                &mut caches,
                Some(table.current()),
            )
            .expect("index walk")
            .expect("the published key has an index entry");
        cell = entry.cell.expect("an index entry names its cell");
        if entry.key.as_ref() == raw.as_slice() {
            match entry.value {
                ResolvedValue::Position(position) => break position,
                other => panic!("a latched walk answered from the value cache: {other:?}"),
            }
        }
        previous = Some(entry.key);
    };
    let cell = table.shape().ks(ks).cell_id(&raw);
    let target = [(
        position.address.partition,
        position.address.segment_generation,
        position.address.offset + 1,
    )];
    table
        .relocation_apply(ks, &cell, RelocationApply::Drop, &target)
        .expect("filter drop");

    let submissions = db.remote_for_test().live_read_submission_count();
    assert!(
        db.multi_get(ks, std::slice::from_ref(&raw))[0]
            .as_ref()
            .unwrap()
            .is_none(),
        "a filtered drop answers not-found before the resident value",
    );
    assert_eq!(
        db.remote_for_test().live_read_submission_count(),
        submissions,
        "the filtered batch resolved at the index with no owner read",
    );
    db.wait_for_background_threads_to_finish().unwrap();
}

/// The per-slot batch re-reads every slot through the scalar path when the
/// batched fetch reports a retired member (`NotFound`), and fails the call for
/// any other kind. The strict batch keeps its contract: a failed owner page
/// fails the call.
#[test]
fn a_retired_batch_member_falls_back_and_other_kinds_fail() {
    let slot_fixture = fixture("remote-cache-batch-fallback", cached_shape(16)).synchronous();
    let ks = KeySpace(0);
    let db = open(&slot_fixture);
    for cell in 1..=4u32 {
        db.insert(ks, key(cell), vec![cell as u8; 8])
            .expect("write");
    }
    db.wait_for_background_threads_to_finish()
        .expect("shut down before the reopen");
    // The reopen leaves the LRU and the derivative cache cold, so each batch
    // must reach the injected batched-fetch failure.
    let db = open(&slot_fixture);
    let reader = &db.remote_for_test().reader;

    // (a) `NotFound` is the retired-member kind: the per-slot batch re-reads
    // every slot through the scalar path and answers the values it resolved.
    let keys = [key(1), key(2)];
    reader.set_read_many_fault_for_test(Some(io::ErrorKind::NotFound));
    let values = db.multi_get(ks, &keys);
    reader.set_read_many_fault_for_test(None);
    for (slot, cell) in [1u8, 2].into_iter().enumerate() {
        assert_eq!(
            values[slot].as_ref().unwrap().as_deref(),
            Some([cell; 8].as_slice()),
            "the scalar fallback answers slot {slot}",
        );
    }

    // (c) A non-retryable kind fails the per-slot batch call, and so every slot.
    // Cells 3 and 4 are still cold: (a) read only cells 1 and 2.
    let keys = [key(3), key(4)];
    reader.set_read_many_fault_for_test(Some(io::ErrorKind::PermissionDenied));
    let values = db.multi_get(ks, &keys);
    reader.set_read_many_fault_for_test(None);
    for (slot, value) in values.iter().enumerate() {
        assert!(
            value.is_err(),
            "a non-retryable batched failure fails slot {slot}",
        );
    }

    // (b) The strict batch keeps its contract: the failed page fails the call.
    let cut_fixture = fixture("remote-cache-batch-fallback-cut", cached_shape(0));
    let cut_db = cut_fixture.open();
    cut_db
        .write_batch([7; 16], vec![put(&key(1), 1), put(&key(2), 2)])
        .expect("write");
    cut_db.drain().expect("drain");
    cut_db
        .reader
        .set_read_many_fault_for_test(Some(io::ErrorKind::NotFound));
    let cut = cut_db.core.table.current();
    let outcome = cut_db.get_many_at_cut(ks, &[key(1), key(2)], cut, None, false);
    cut_db.reader.set_read_many_fault_for_test(None);
    assert!(
        outcome.is_err(),
        "a cut batch fails the call on a failed owner page",
    );

    db.wait_for_background_threads_to_finish().unwrap();
    cut_db.shutdown().unwrap();
}
