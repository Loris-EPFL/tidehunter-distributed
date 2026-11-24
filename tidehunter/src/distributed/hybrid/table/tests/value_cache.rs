//! The coordinator's native per-cell value cache (`value_cache_size`).
//!
//! Sui sets a value cache on its hot keyspaces, so the hybrid runs the native
//! `LargeTable` LRU unchanged: publication fills it with the value the batch
//! wrote, a live read refills it, and the native invalidation paths must never
//! leave a stale value servable. These tests pin the index half of that contract
//! with no owner and no value reader: what [`HybridTable::get_value`] answers,
//! what the position-only [`HybridTable::get`] refuses, and what survives a
//! remove, a dropped cell and a relocation-filter drop.
//!
//! The owner-read count, the refill after an owner fetch and the rule that a
//! checkpoint read never refills belong to the remote-database suite, where a
//! real value path and a checkpoint exist.

use super::{position, version};
use crate::distributed::codec::RecordKind;
use crate::distributed::durability::DurabilityTier;
use crate::distributed::hybrid::config::HybridIndexConfig;
use crate::distributed::hybrid::loader::CutSources;
use crate::distributed::hybrid::table::{
    HybridTable, PublishedMutation, RelocationApply, ResolvedValue,
};
use crate::distributed::types::BatchVersion;
use crate::key_shape::{KeyShape, KeySpace, KeySpaceConfig, KeyType};
use crate::metrics::Metrics;
use minibytes::Bytes;
use std::sync::Arc;

/// One keyspace `root` of 4-byte keys over `cells` uniform cells, with a native
/// per-cell value cache of `cache` entries (`0` is native's "no cache").
fn cached_shape(cache: usize, cells: usize) -> Arc<KeyShape> {
    Arc::new(KeyShape::new_single_config(
        4,
        cells,
        KeyType::uniform(1),
        KeySpaceConfig::new().with_value_cache_size(cache),
    ))
}

struct Cached {
    _dir: tempdir::TempDir,
    table: HybridTable,
    metrics: Arc<Metrics>,
}

impl Cached {
    fn new(name: &str, cache: usize, cells: usize) -> Self {
        let dir = tempdir::TempDir::new(name).expect("scratch directory");
        let metrics = Metrics::new();
        let table = HybridTable::open(
            dir.path(),
            cached_shape(cache, cells),
            &HybridIndexConfig::default(),
            CutSources::new(1, DurabilityTier::Native),
            metrics.clone(),
        )
        .expect("a keyspace with a value cache opens");
        Self {
            _dir: dir,
            table,
            metrics,
        }
    }

    fn ks(&self) -> KeySpace {
        self.table
            .shape()
            .iter_ks()
            .next()
            .expect("one keyspace")
            .id()
    }

    fn publish(&self, kind: RecordKind, key: u8, sequence: u64, value: &[u8]) {
        let value = Bytes::copy_from_slice(value);
        self.table
            .insert_published(
                &[PublishedMutation {
                    keyspace: self.ks(),
                    key: &[key; 4],
                    kind,
                    position: position(sequence, u64::from(key) * 1_000),
                    value: Some(&value),
                }],
                64,
            )
            .expect("publish");
    }

    fn lookup(&self, key: u8, cut: BatchVersion) -> Option<ResolvedValue> {
        self.table
            .get_value(self.ks(), &[key; 4], cut)
            .expect("lookup")
    }

    /// The value the LRU holds for `key`, failing the test when the index names
    /// a member position instead: a caller that expects a resident value must not
    /// silently accept a position.
    fn resident_value(&self, key: u8, cut: BatchVersion) -> Option<Bytes> {
        match self.lookup(key, cut) {
            Some(ResolvedValue::Resident { value, .. }) => Some(value),
            Some(ResolvedValue::Position(found)) => {
                panic!(
                    "key {key} at {cut:?} was not resident; the index named {}:{} member {}",
                    found.batch.epoch, found.batch.sequence, found.operation_index,
                )
            }
            None => None,
        }
    }

    fn lru_hits(&self) -> u64 {
        let name = self.table.ks_desc(self.ks()).name();
        self.metrics
            .lookup_result
            .with_label_values(&[name, "found", "lru"])
            .get()
    }
}

#[test]
fn a_value_cache_opens_and_a_published_put_is_resident() {
    let fixture = Cached::new("hybrid-cache-open", 16, 1);
    fixture.publish(RecordKind::Put, 1, 2, b"one");
    fixture.table.advance_applied(version(3));
    assert_eq!(
        fixture.resident_value(1, version(3)).as_deref(),
        Some(&b"one"[..]),
        "the value the batch wrote is resident after publication",
    );
    assert!(
        fixture.resident_value(2, version(3)).is_none(),
        "a key the batch did not write is absent",
    );
    // The position-only lookup refuses a resident answer instead of inventing a
    // position; only the value path can serve the LRU.
    let error = fixture
        .table
        .get(fixture.ks(), &[1; 4], version(3))
        .expect_err("a resident answer is not a position");
    assert!(
        error.to_string().contains("value cache"),
        "the refusal names the value cache: {error}",
    );
}

#[test]
fn an_overwrite_replaces_the_resident_value() {
    let fixture = Cached::new("hybrid-cache-overwrite", 16, 1);
    fixture.publish(RecordKind::Put, 1, 2, b"one");
    fixture.publish(RecordKind::Put, 1, 4, b"two");
    fixture.table.advance_applied(version(5));
    assert_eq!(
        fixture.resident_value(1, version(5)).as_deref(),
        Some(&b"two"[..]),
        "the later put is the resident value",
    );
}

/// Native's as-of guard: the LRU holds the latest value, so a cut below an
/// overwrite must not be answered from it. The index names the older member and
/// the value path fetches that, exactly as native `LargeTable::get_checkpoint`.
#[test]
fn a_cut_below_an_overwrite_never_serves_the_newer_resident_value() {
    let fixture = Cached::new("hybrid-cache-as-of", 16, 1);
    fixture.publish(RecordKind::Put, 1, 2, b"one");
    fixture.publish(RecordKind::Put, 1, 4, b"two");
    fixture.table.advance_applied(version(5));
    assert_eq!(
        fixture.lookup(1, version(3)).map(|found| match found {
            ResolvedValue::Position(position) => position.batch,
            ResolvedValue::Resident { .. } => panic!("a cut below the overwrite is not resident"),
        }),
        Some(version(2)),
        "the cut resolves the member the older put wrote",
    );
}

#[test]
fn a_delete_leaves_no_resident_value() {
    let fixture = Cached::new("hybrid-cache-delete", 16, 1);
    fixture.publish(RecordKind::Put, 1, 2, b"one");
    fixture.publish(RecordKind::Delete, 1, 4, b"");
    fixture.table.advance_applied(version(5));
    assert!(
        fixture.lookup(1, version(5)).is_none(),
        "the native remove pops the resident value and the tombstone hides the put",
    );
}

#[test]
fn dropping_a_cell_clears_its_resident_values() {
    let fixture = Cached::new("hybrid-cache-drop-cells", 16, 2);
    let key_of = |key: u8| [key; 4];
    let cell_of =
        |fixture: &Cached, key: u8| fixture.table.shape().ks(fixture.ks()).cell_id(&key_of(key));
    assert_ne!(
        cell_of(&fixture, 0),
        cell_of(&fixture, 255),
        "the fixture keys land in different cells",
    );
    fixture.publish(RecordKind::Put, 0, 2, b"left");
    fixture.publish(RecordKind::Put, 255, 2, b"right");
    fixture.table.advance_applied(version(3));
    assert_eq!(
        fixture.resident_value(0, version(3)).as_deref(),
        Some(&b"left"[..]),
    );
    assert_eq!(
        fixture.resident_value(255, version(3)).as_deref(),
        Some(&b"right"[..]),
    );

    let ks = fixture.ks();
    let dropped = cell_of(&fixture, 0);
    fixture
        .table
        .drop_cells(ks, dropped.clone(), dropped, version(3))
        .expect("drop one cell");

    assert!(
        fixture.lookup(0, version(3)).is_none(),
        "a dropped cell never serves its cached value",
    );
    assert_eq!(
        fixture.resident_value(255, version(3)).as_deref(),
        Some(&b"right"[..]),
        "the untouched cell keeps its resident value",
    );
}

#[test]
fn a_relocation_filter_drop_leaves_no_stale_resident_value() {
    let fixture = Cached::new("hybrid-cache-relocation-drop", 16, 1);
    fixture.publish(RecordKind::Put, 1, 2, b"one");
    fixture.table.advance_applied(version(3));
    assert_eq!(
        fixture.resident_value(1, version(3)).as_deref(),
        Some(&b"one"[..]),
    );

    let ks = fixture.ks();
    let key = [1; 4];
    let cell = fixture.table.shape().ks(ks).cell_id(&key);
    // The member is below the target span (partition 0, generation 1), so the
    // filter drops the key: every version of it leaves the index and one
    // `Removed` tombstone at the dropped live position takes its place, and the
    // resident value must never be served for it again.
    fixture
        .table
        .relocation_apply(ks, &cell, RelocationApply::Drop, &[(0u32, 1u64, u64::MAX)])
        .expect("filter drop");
    assert!(
        fixture.lookup(1, version(3)).is_none(),
        "a filtered key is not-found before the LRU is consulted",
    );
}

#[test]
fn a_publish_without_a_resident_value_inserts_nothing() {
    let fixture = Cached::new("hybrid-cache-no-value", 16, 1);
    let ks = fixture.ks();
    let one = Bytes::from_static(b"one");
    let error = fixture
        .table
        .insert_published(
            &[
                PublishedMutation {
                    keyspace: ks,
                    key: &[1; 4],
                    kind: RecordKind::Put,
                    position: position(2, 1_000),
                    value: Some(&one),
                },
                // The second member is a construction bug: a put with no value.
                PublishedMutation {
                    keyspace: ks,
                    key: &[2; 4],
                    kind: RecordKind::Put,
                    position: position(2, 2_000),
                    value: None,
                },
            ],
            128,
        )
        .expect_err("a put must carry the value it wrote");
    assert!(
        error.to_string().contains("no resident value"),
        "the refusal names the missing value: {error}",
    );
    assert!(
        fixture.lookup(1, version(3)).is_none(),
        "the valid member before the bad one was not inserted either",
    );
    assert!(
        fixture.lookup(2, version(3)).is_none(),
        "a refused publish inserted nothing",
    );
}

/// The bound is native's: one `LruCache` of `value_cache_size` entries per cell,
/// so the oldest keys are evicted and a miss is reported as a position.
#[test]
fn the_cache_holds_at_most_value_cache_size_keys() {
    let fixture = Cached::new("hybrid-cache-bound", 2, 1);
    for key in 1..=4u8 {
        fixture.publish(RecordKind::Put, key, u64::from(key), &[key; 4]);
    }
    fixture.table.advance_applied(version(5));
    for key in 1..=4u8 {
        // A table-level lookup never refills: the read path owns the refill.
        fixture.lookup(key, version(5));
    }
    assert_eq!(
        fixture.lru_hits(),
        2,
        "a two-entry cache answers only the two newest keys",
    );
}
