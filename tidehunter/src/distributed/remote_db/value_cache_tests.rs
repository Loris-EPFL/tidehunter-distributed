//! The coordinator value cache end to end: the native per-cell LRU that Sui
//! enables with `value_cache_size`.
//!
//! These tests go through the real read path over fixture owners, so they cover
//! what the index-level tests cannot: a resident value costs no owner round trip,
//! a value fetched after a reopen refills the LRU, a checkpoint read serves the
//! as-of value without seeding the LRU with it, a refused write leaves nothing
//! resident, and a cached database answers exactly like an uncached one through a
//! mixed workload and a reopen.
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
use crate::distributed::sui::Db;
use crate::key_shape::{KeyShape, KeySpaceConfig, KeyType};
use minibytes::Bytes;
use std::sync::Arc;

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
