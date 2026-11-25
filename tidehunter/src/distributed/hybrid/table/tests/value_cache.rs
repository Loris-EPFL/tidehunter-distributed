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
use crate::distributed::hybrid::snapshot::rebuild_control_region;
use crate::distributed::hybrid::table::{
    HybridTable, PublishedMutation, RelocationApply, ResolvedValue,
};
use crate::distributed::types::BatchVersion;
use crate::key_shape::{KeyShape, KeySpace, KeySpaceConfig, KeyType};
use crate::metrics::Metrics;
use crate::relocation::Decision;
use minibytes::Bytes;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

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

/// The same shape, but filtered so relocation may drop index rows. Reads must
/// resolve the index before any resident value: a filter drop keeps the LRU.
fn filtered_shape(cache: usize, cells: usize) -> Arc<KeyShape> {
    Arc::new(KeyShape::new_single_config(
        4,
        cells,
        KeyType::uniform(1),
        KeySpaceConfig::new()
            .with_value_cache_size(cache)
            .with_relocation_filter(|_, _| Decision::Keep),
    ))
}

struct Cached {
    _dir: tempdir::TempDir,
    table: HybridTable,
    metrics: Arc<Metrics>,
    cuts: CutSources,
}

impl Cached {
    fn new(name: &str, cache: usize, cells: usize) -> Self {
        Self::with_shape(name, cached_shape(cache, cells))
    }

    fn filtered(name: &str, cache: usize, cells: usize) -> Self {
        Self::with_shape(name, filtered_shape(cache, cells))
    }

    fn with_shape(name: &str, shape: Arc<KeyShape>) -> Self {
        let dir = tempdir::TempDir::new(name).expect("scratch directory");
        let metrics = Metrics::new();
        let cuts = CutSources::new(1, DurabilityTier::Native);
        let table = HybridTable::open(
            dir.path(),
            shape,
            &HybridIndexConfig::default(),
            cuts.clone(),
            metrics.clone(),
        )
        .expect("a keyspace with a value cache opens");
        Self {
            _dir: dir,
            table,
            metrics,
            cuts,
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

    /// The lookup at the table's current cut.
    fn lookup_now(&self, key: u8) -> Option<ResolvedValue> {
        self.lookup(key, self.table.current())
    }

    /// The cell `key` routes to.
    fn cell(&self, key: u8) -> crate::cell::CellId {
        self.table.shape().ks(self.ks()).cell_id(&[key; 4])
    }

    /// The cell's native entry state, as the state enum's `Debug` name.
    fn state(&self, key: u8) -> String {
        self.table
            .large_table()
            .entry_state_for_test(self.table.contexts().ks_context(self.ks()), &self.cell(key))
    }

    /// Forces one snapshot pass that flushes every dirty cell and unloads it,
    /// leaving the published value only in the native per-cell LRU.
    fn flush_to_unloaded(&self, cut: BatchVersion) {
        self.cuts.set_durable(cut);
        rebuild_control_region(&self.table, true, Some(cut), &[]).expect("forced flush");
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

    /// The value a lookup at the table's current cut answers, failing the test
    /// when it names a member position: a key its cell still holds resident must
    /// be served without the owner.
    fn value_now(&self, key: u8) -> Option<Bytes> {
        match self.lookup_now(key) {
            Some(ResolvedValue::Resident { value, .. }) => Some(value),
            Some(ResolvedValue::Position(found)) => panic!(
                "key {key} was not resident; the index named {}:{} member {}",
                found.batch.epoch, found.batch.sequence, found.operation_index,
            ),
            None => None,
        }
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

/// A refill must not install a value whose cell changed after the index
/// resolution. A remote fetch resolves a position at a cut, reads the member
/// over the network and only then fills, so a publish can land in between;
/// installing the older value would leave the LRU serving it to every read
/// whose cut covers the newer write (the cut check serves a resident entry on
/// the premise that it is the key's latest value).
#[test]
fn a_refill_of_a_superseded_position_keeps_the_latest_value() {
    let fixture = Cached::new("hybrid-cache-refill-guard", 16, 1);
    fixture.publish(RecordKind::Put, 1, 2, b"one");
    let key = Bytes::copy_from_slice(&[1; 4]);
    // The stamp a fetch racing the overwrite sampled before it resolved.
    let stale_epoch = fixture.table.value_epoch(fixture.ks(), &key);
    fixture.publish(RecordKind::Put, 1, 4, b"two");
    fixture.table.advance_applied(version(5));
    fixture.table.refill_value(
        fixture.ks(),
        key.clone(),
        key,
        Bytes::from_static(b"one"),
        stale_epoch,
    );
    assert_eq!(
        fixture.resident_value(1, version(5)).as_deref(),
        Some(&b"two"[..]),
        "a superseded refill does not replace the key's latest value",
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

/// The unloaded-cell gap, deliberately kept: a flush that unloads a cell leaves
/// the published value in the coordinator's per-cell LRU, but the cut-checked
/// lookup cannot consult it, so the read names the member position and fetches
/// from the owner. Serving that LRU would require native `Db::get` semantics,
/// which also see a batch sparse publication installed above the published
/// prefix; the total-order read contract wins over the saved owner round trip.
#[test]
fn an_unloaded_cell_is_read_from_its_owner_not_its_lru() {
    let fixture = Cached::new("hybrid-cache-unloaded", 16, 1);
    fixture.publish(RecordKind::Put, 1, 2, b"one");
    fixture.table.advance_applied(version(3));
    fixture.flush_to_unloaded(version(3));
    assert_eq!(
        fixture.state(1),
        "Unloaded",
        "premise: the forced flush folded the overlay into the blob and unloaded the cell",
    );

    // The cut-checked lookup names the member position the owner read needs.
    match fixture.lookup(1, version(3)) {
        Some(ResolvedValue::Position(found)) => assert_eq!(
            found.batch,
            version(2),
            "the cut resolves the member the put wrote",
        ),
        Some(ResolvedValue::Resident { .. }) => {
            panic!("the cut-checked path must not serve the LRU of an unloaded cell")
        }
        None => panic!("the put is in the blob"),
    }
    assert_eq!(
        fixture.lru_hits(),
        0,
        "the unloaded cell's resident value is never consulted",
    );
}

/// A later read never returns the old value: the overwrite replaces the
/// resident one under the row lock, and a delete pops it and writes the
/// tombstone under the same lock.
#[test]
fn a_read_after_an_overwrite_and_a_delete_sees_the_new_state() {
    let fixture = Cached::new("hybrid-cache-overwrite-resident", 16, 1);
    fixture.publish(RecordKind::Put, 1, 2, b"one");
    fixture.table.advance_applied(version(3));
    assert_eq!(fixture.value_now(1).as_deref(), Some(&b"one"[..]));

    fixture.publish(RecordKind::Put, 1, 4, b"two");
    fixture.table.advance_applied(version(5));
    assert_eq!(
        fixture.value_now(1).as_deref(),
        Some(&b"two"[..]),
        "a read after the overwrite sees the newer value, never the resident old one",
    );

    fixture.publish(RecordKind::Delete, 1, 6, b"");
    fixture.table.advance_applied(version(7));
    assert!(
        fixture.lookup_now(1).is_none(),
        "the delete popped the resident value and the lookup answers not-found",
    );
}

/// A filtered keyspace stays index-first: a relocation drop removes the key's
/// versions and leaves one `Removed` tombstone but deliberately keeps the
/// resident value, and only the cut-checked lookup answers not-found before it.
#[test]
fn a_filtered_keyspace_never_serves_a_relocation_dropped_resident_value() {
    let fixture = Cached::filtered("hybrid-cache-filtered", 16, 1);
    fixture.publish(RecordKind::Put, 1, 2, b"one");
    fixture.table.advance_applied(version(3));
    assert_eq!(
        fixture.value_now(1).as_deref(),
        Some(&b"one"[..]),
        "premise: the published value is resident",
    );

    fixture
        .table
        .relocation_apply(
            fixture.ks(),
            &fixture.cell(1),
            RelocationApply::Drop,
            &[(0u32, 1u64, u64::MAX)],
        )
        .expect("filter drop");
    assert!(
        fixture.lookup_now(1).is_none(),
        "the lookup resolves the index first for a filtered keyspace",
    );
}

/// Publication writes the overlay and the LRU under one row lock, before it
/// advances the cut. A reader that has observed a later publication must never
/// see an older value afterwards, whether the cut-checked lookup answers from
/// the LRU or names the member the cut resolves.
#[test]
fn a_concurrent_overwrite_never_lets_a_later_read_return_the_old_value() {
    /// The sequence a lookup answer stands for: the value itself, or the batch
    /// the resolved member position was written by.
    fn sequence(found: Option<ResolvedValue>) -> u64 {
        match found {
            Some(ResolvedValue::Resident { value, .. }) => u64::from_be_bytes(
                value
                    .as_ref()
                    .try_into()
                    .expect("the fixture publishes eight-byte values"),
            ),
            Some(ResolvedValue::Position(position)) => position.batch.sequence,
            None => panic!("the key is present at every cut this test samples"),
        }
    }

    const LAST: u64 = 2_002;
    let fixture = Cached::new("hybrid-cache-race", 16, 1);
    fixture.publish(RecordKind::Put, 1, 2, &2u64.to_be_bytes());
    fixture.table.advance_applied(version(3));
    let visible = AtomicU64::new(2);
    let table = fixture.table.clone();
    let ks = fixture.ks();
    std::thread::scope(|scope| {
        let visible = &visible;
        let writer_table = table.clone();
        let writer = scope.spawn(move || {
            for sequence in 3..=LAST {
                let value = Bytes::copy_from_slice(&sequence.to_be_bytes());
                writer_table
                    .insert_published(
                        &[PublishedMutation {
                            keyspace: ks,
                            key: &[1; 4],
                            kind: RecordKind::Put,
                            position: position(sequence, sequence * 1_000),
                            value: Some(&value),
                        }],
                        64,
                    )
                    .expect("publish");
                writer_table.advance_applied(version(sequence + 1));
                // The value is already installed in the cell (insert precedes
                // the advance), so a reader that observes this floor and then
                // samples the cut must resolve at least this sequence.
                visible.store(sequence, Ordering::Release);
            }
        });
        let reader_table = table.clone();
        let reader = scope.spawn(move || {
            let mut last = 0u64;
            for _ in 0..10_000 {
                let floor = visible.load(Ordering::Acquire);
                let cut = reader_table.current();
                let seen = sequence(reader_table.get_value(ks, &[1; 4], cut).expect("lookup"));
                assert!(
                    seen >= floor,
                    "a read at cut {cut:?} after observing publication {floor} returned {seen}",
                );
                assert!(seen >= last, "reads regressed from {last} to {seen}",);
                last = last.max(seen);
            }
        });
        writer.join().expect("the writer thread must not panic");
        reader.join().expect("the reader thread must not panic");
    });
    assert_eq!(
        fixture.value_now(1).as_deref(),
        Some(&LAST.to_be_bytes()[..]),
        "after the last publication every later read sees it",
    );
}
