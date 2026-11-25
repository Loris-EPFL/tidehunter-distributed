//! Sharded-cell pre-load in `TableInner::relocation_apply` (S14 P3).
//!
//! Relocation on a sharded cell must load only the L1 shards that own
//! below-target keys absent from the in-memory table, matching native
//! `flusher.rs` `lift_shard_entries`. Loading every shard does I/O in
//! proportion to the whole cell, copies untouched shard rows into live
//! `entry.data`, and forces untouched shards to rewrite on a later promote.
//!
//! Load count is observed through the existing `metrics.read` counter for
//! `WalEntryKind::Index` (summed across `Mapped` and `Syscall`), which
//! `HybridLoader::load` increments on every blob read, with no test hook in
//! production code.
//!
//! Negative control (mutation run, observed): restoring the pre-S22
//! whole-shard pre-load in `TableInner::relocation_apply` (load and merge
//! every `levels.shards()` blob before the compare-and-set) makes
//! `sharded_cell_relocation_loads_only_the_needed_shard_and_preserves_other_shards`
//! fail at `loads == 1` with `loaded 4` (total shards = 4).

use super::{HybridTable, PublishedMutation, RelocationApply, member_offset, position, version};
use crate::cell::CellId;
use crate::context::{ReadType, WalEntryKind};
use crate::distributed::codec::RecordKind;
use crate::distributed::durability::DurabilityTier;
use crate::distributed::hybrid::config::HybridIndexConfig;
use crate::distributed::hybrid::loader::CutSources;
use crate::distributed::hybrid::position::VersionPosition;
use crate::distributed::hybrid::snapshot::rebuild_control_region;
use crate::index::levels::IndexShard;
use crate::key_shape::{KeyShape, KeySpace, KeyType};
use crate::metrics::Metrics;
use crate::wal::position::WalPosition;
use minibytes::Bytes;
use std::collections::BTreeMap;
use std::sync::Arc;

/// Number of keys written into the single cell.
const SHARDED_KEYS: u8 = 40;
/// Auto-shard threshold that the fixture promotes under.
///
/// The header index format writes a fixed 1024-byte header (`HEADER_SIZE`) in
/// front of every blob, so `n` entries serialize to `1024 + n * 52` (4-byte
/// key plus the 48-byte position). A 1600-byte threshold is above ten entries
/// (1544) and below twenty (2064), so the 40 keys split by median into four
/// shards of ten keys each, and a touched shard stays one blob.
const SHARD_THRESHOLD: usize = 1600;
/// Stride between consecutive key leading bytes. Leaving three unused byte
/// values between adjacent keys creates deterministic in-shard absent keys and
/// between-shard gap keys.
const KEY_STRIDE: u8 = 4;

fn sharded_shape() -> Arc<KeyShape> {
    Arc::new(KeyShape::new_single(4, 1, KeyType::uniform(1)))
}

fn spaced_key(seed: u8) -> [u8; 4] {
    [seed * KEY_STRIDE, 0, 0, 0]
}

fn reduced_key(table: &HybridTable, ks: KeySpace, raw: &[u8; 4]) -> Bytes {
    Bytes::copy_from_slice(table.shape().ks(ks).reduce_key(raw).as_ref())
}

fn initial_offset(seed: u8) -> u64 {
    1_000 + u64::from(seed) * 64
}

/// Total index blob loads performed by `HybridLoader::load` for `ks`.
fn index_loads(table: &HybridTable, ks: KeySpace) -> u64 {
    let ks_name = table.ks_desc(ks).name();
    let kind = WalEntryKind::Index.as_ref();
    [ReadType::Mapped, ReadType::Syscall]
        .into_iter()
        .map(|read_type| {
            table
                .metrics()
                .read
                .with_label_values(&[ks_name, kind, read_type.as_ref()])
                .get()
        })
        .sum()
}

/// `(l0 position, shard btree)` of the fixture's single cell.
fn cell_levels(
    table: &HybridTable,
    ks: KeySpace,
    cell: &CellId,
) -> (Option<WalPosition>, BTreeMap<Vec<u8>, IndexShard>) {
    table
        .large_table()
        .with_entry_for_test(table.contexts().ks_context(ks), cell, |entry| {
            let levels = entry.levels();
            (levels.l0(), levels.shards().clone())
        })
}

struct ShardedFixture {
    table: HybridTable,
    cuts: CutSources,
    ks: KeySpace,
    cell: CellId,
    shards: Vec<(Vec<u8>, IndexShard)>,
    /// Removed last: the table's files live under this directory.
    _dir: tempdir::TempDir,
}

impl ShardedFixture {
    /// Opens a single-cell table, populates [`SHARDED_KEYS`] spaced keys, and
    /// promotes + unloads the cell into at least three disjoint L1 shards with
    /// no L0 blob.
    fn new(name: &str, l0_max_entries: usize) -> Self {
        let dir = tempdir::TempDir::new(name).expect("scratch directory");
        let cuts = CutSources::new(1, DurabilityTier::Native);
        let config = HybridIndexConfig {
            snapshot_unload_threshold: 0,
            frag_size: 4096,
            wal_file_size: 4096,
            l0_max_entries: Some(l0_max_entries),
            index_auto_shard_threshold: Some(SHARD_THRESHOLD),
            ..HybridIndexConfig::default()
        };
        let table = HybridTable::open(
            dir.path(),
            sharded_shape(),
            &config,
            cuts.clone(),
            Metrics::new(),
        )
        .expect("open sharded table");
        let ks = table.shape().iter_ks().next().expect("one keyspace").id();
        let cell = table.shape().ks(ks).cell_id(&spaced_key(1));

        for seed in 1..=SHARDED_KEYS {
            let key = spaced_key(seed);
            table
                .insert_published(
                    &[PublishedMutation {
                        keyspace: ks,
                        key: &key,
                        kind: RecordKind::Put,
                        position: position(u64::from(seed), initial_offset(seed)),
                        value: Some(&Bytes::from_static(b"fixture value")),
                    }],
                    64,
                )
                .expect("publish seed key");
        }
        let head = version(u64::from(SHARDED_KEYS) + 1);
        table.advance_applied(head);
        cuts.set_durable(head);
        let stored = rebuild_control_region(&table, true, Some(head), &[])
            .expect("promote and unload sharded cell");
        assert!(stored.stored, "initial promotion must store a region");

        let (l0, shard_map) = cell_levels(&table, ks, &cell);
        assert!(
            l0.is_none(),
            "premise: post-promote cell must have empty L0, got {l0:?}"
        );
        assert!(
            shard_map.len() >= 3,
            "premise: expected at least 3 shards, got {}",
            shard_map.len()
        );
        let state = table
            .large_table()
            .entry_state_for_test(table.contexts().ks_context(ks), &cell);
        assert_eq!(
            state, "Unloaded",
            "premise: cell must be unloaded before relocation_apply, got {state}"
        );
        let shards: Vec<(Vec<u8>, IndexShard)> = shard_map.into_iter().collect();
        Self {
            table,
            cuts,
            ks,
            cell,
            shards,
            _dir: dir,
        }
    }

    fn key_seed_at(raw: &[u8]) -> u8 {
        assert_eq!(raw.len(), 4, "expected 4-byte key, got {raw:?}");
        assert_eq!(raw[0] % KEY_STRIDE, 0, "expected strided key, got {raw:?}");
        raw[0] / KEY_STRIDE
    }

    fn shard_min_seed(&self, shard_idx: usize) -> u8 {
        Self::key_seed_at(&self.shards[shard_idx].0)
    }

    fn shard_max_seed(&self, shard_idx: usize) -> u8 {
        Self::key_seed_at(&self.shards[shard_idx].1.max_key)
    }

    fn expected_position(&self, seed: u8) -> VersionPosition {
        position(u64::from(seed), initial_offset(seed))
    }

    fn get_offset(&self, seed: u8, cut: crate::distributed::types::BatchVersion) -> Option<u64> {
        self.table
            .get(self.ks, &spaced_key(seed), cut)
            .expect("lookup")
            .map(|found| found.address.offset)
    }
}

#[test]
fn sharded_cell_relocation_loads_only_the_needed_shard_and_preserves_other_shards() {
    // `l0_max_entries = 0` forces a re-sharding promote on the next snapshot so
    // we also verify that untouched shards are never rewritten.
    let fixture = ShardedFixture::new("hybrid-shard-preload-single", 0);
    let target_seed = fixture.shard_min_seed(1);
    let untouched_left = fixture.shard_min_seed(0);
    let untouched_right = fixture.shard_min_seed(2);
    let old = fixture.expected_position(target_seed);
    let moved = old.moved_to(position(u64::from(target_seed), 50_000).address);
    let targets = [(0u32, 1u64, 20_000u64)];

    let loads_before = index_loads(&fixture.table, fixture.ks);
    let shadowed = fixture
        .table
        .relocation_apply(
            fixture.ks,
            &fixture.cell,
            RelocationApply::Repoint(&[(
                reduced_key(&fixture.table, fixture.ks, &spaced_key(target_seed)),
                old,
                moved,
            )]),
            &targets,
        )
        .expect("relocation_apply on one shard");
    let loads = index_loads(&fixture.table, fixture.ks) - loads_before;
    assert!(
        shadowed.is_empty(),
        "matching expectation must apply cleanly: {shadowed:?}"
    );
    assert_eq!(
        loads,
        1,
        "relocation_apply must load only the owning shard (total shards = {}), loaded {loads}",
        fixture.shards.len()
    );

    let read_cut = version(u64::from(SHARDED_KEYS) + 2);
    assert_eq!(
        fixture.get_offset(target_seed, read_cut),
        Some(member_offset(50_000)),
        "re-pointed key in shard 1 must serve the new address"
    );
    assert_eq!(
        fixture.get_offset(untouched_left, read_cut),
        Some(member_offset(initial_offset(untouched_left))),
        "key in untouched shard 0 must keep its original address"
    );
    assert_eq!(
        fixture.get_offset(untouched_right, read_cut),
        Some(member_offset(initial_offset(untouched_right))),
        "key in untouched shard 2 must keep its original address"
    );

    // Promote the dirty cell: only shard 1 received the lifted entry, so every
    // untouched shard must keep its exact pre-relocation WAL position.
    let stored = fixture
        .table
        .snapshot_for_relocation(version(u64::from(SHARDED_KEYS) + 1))
        .expect("forced snapshot after re-point");
    assert!(stored.stored, "forced snapshot must persist the re-point");
    let (l0_after, shards_after) = cell_levels(&fixture.table, fixture.ks, &fixture.cell);
    assert!(
        l0_after.is_none(),
        "l0_max_entries=0 promotes directly into shards, got l0 {l0_after:?}"
    );
    assert_eq!(
        shards_after.get(&fixture.shards[0].0).map(|s| s.position),
        Some(fixture.shards[0].1.position),
        "untouched shard 0 must not be rewritten on promote"
    );
    assert_ne!(
        shards_after.get(&fixture.shards[1].0).map(|s| s.position),
        Some(fixture.shards[1].1.position),
        "touched shard 1 must be rewritten with the re-pointed entry"
    );
    assert_eq!(
        shards_after.get(&fixture.shards[2].0).map(|s| s.position),
        Some(fixture.shards[2].1.position),
        "untouched shard 2 must not be rewritten on promote"
    );
    assert_eq!(
        fixture.get_offset(target_seed, read_cut),
        Some(member_offset(50_000)),
        "re-pointed key must still resolve after promote and unload"
    );
}

#[test]
fn sharded_cell_relocation_caches_shards_for_unsorted_updates_and_skips_l0_and_out_of_target() {
    let fixture = ShardedFixture::new("hybrid-shard-preload-unsorted", 8);
    let s0_out = fixture.shard_min_seed(0);
    let s0_l0 = fixture.shard_max_seed(0);
    let s1_a = fixture.shard_min_seed(1);
    let s1_b = fixture.shard_max_seed(1);
    assert_ne!(s1_a, s1_b, "premise: shard 1 holds at least two keys");
    let s2_a = fixture.shard_min_seed(2);

    // Put a newer overwrite of `s0_l0` in the live overlay (unflushed), so L0
    // shadows its shard-0 entry without loading shard 0.
    let overwrite_seq = u64::from(SHARDED_KEYS) + 2;
    let overwrite_key = spaced_key(s0_l0);
    fixture
        .table
        .insert_published(
            &[PublishedMutation {
                keyspace: fixture.ks,
                key: &overwrite_key,
                kind: RecordKind::Put,
                position: position(overwrite_seq, 90_000),
                value: Some(&Bytes::from_static(b"fixture value")),
            }],
            64,
        )
        .expect("publish L0 overwrite");
    fixture.table.advance_applied(version(overwrite_seq + 1));
    fixture.cuts.set_durable(version(overwrite_seq + 1));

    let old_s1_a = fixture.expected_position(s1_a);
    let old_s1_b = fixture.expected_position(s1_b);
    let old_s2_a = fixture.expected_position(s2_a);
    let old_s0_l0 = fixture.expected_position(s0_l0);
    let old_s0_out = fixture.expected_position(s0_out);
    // Deliberately unsorted across shard 1 and shard 2, plus one L0-shadowed
    // key in shard 0 and one out-of-target key in shard 0.
    let updates = [
        (
            reduced_key(&fixture.table, fixture.ks, &spaced_key(s1_a)),
            old_s1_a,
            old_s1_a.moved_to(position(u64::from(s1_a), 61_000).address),
        ),
        (
            reduced_key(&fixture.table, fixture.ks, &spaced_key(s2_a)),
            old_s2_a,
            old_s2_a.moved_to(position(u64::from(s2_a), 62_000).address),
        ),
        (
            reduced_key(&fixture.table, fixture.ks, &spaced_key(s1_b)),
            old_s1_b,
            old_s1_b.moved_to(position(u64::from(s1_b), 63_000).address),
        ),
        (
            reduced_key(&fixture.table, fixture.ks, &spaced_key(s0_l0)),
            old_s0_l0,
            old_s0_l0.moved_to(position(u64::from(s0_l0), 64_000).address),
        ),
        // Out-of-target: `old_s0_out` is at `member_offset(initial_offset(s0_out))`,
        // but we give an expectation with offset above target end so it must be
        // skipped with zero shard reads.
        (
            reduced_key(&fixture.table, fixture.ks, &spaced_key(s0_out)),
            old_s0_out.moved_to(position(u64::from(s0_out), 40_000).address),
            old_s0_out.moved_to(position(u64::from(s0_out), 65_000).address),
        ),
    ];
    let targets = [(0u32, 1u64, 20_000u64)];

    let loads_before = index_loads(&fixture.table, fixture.ks);
    let shadowed = fixture
        .table
        .relocation_apply(
            fixture.ks,
            &fixture.cell,
            RelocationApply::Repoint(&updates),
            &targets,
        )
        .expect("relocation_apply unsorted multi-shard");
    let loads = index_loads(&fixture.table, fixture.ks) - loads_before;
    assert_eq!(
        loads, 2,
        "must load only shard 1 and shard 2 once each (shard 0 skipped), loaded {loads}"
    );
    assert_eq!(
        shadowed.len(),
        1,
        "only the L0-overwritten key must join shadowed: {shadowed:?}"
    );
    assert!(
        shadowed[0].0.same_physical(&old_s0_l0) && shadowed[0].1 == version(overwrite_seq),
        "L0-overwritten expectation must record the L0 shadowing version: {shadowed:?}"
    );

    let read_cut = version(overwrite_seq + 1);
    assert_eq!(
        fixture.get_offset(s1_a, read_cut),
        Some(member_offset(61_000)),
        "s1_a re-pointed"
    );
    assert_eq!(
        fixture.get_offset(s2_a, read_cut),
        Some(member_offset(62_000)),
        "s2_a re-pointed"
    );
    assert_eq!(
        fixture.get_offset(s1_b, read_cut),
        Some(member_offset(63_000)),
        "s1_b re-pointed"
    );
    assert_eq!(
        fixture.get_offset(s0_l0, read_cut),
        Some(member_offset(90_000)),
        "L0 overwrite preserved"
    );
    assert_eq!(
        fixture.get_offset(s0_out, read_cut),
        Some(member_offset(initial_offset(s0_out))),
        "out-of-target shard-0 key untouched"
    );
}

#[test]
fn sharded_cell_shadowed_unexamined_and_drop_removed_keep_narrow_loads_and_witness() {
    let fixture = ShardedFixture::new("hybrid-shard-preload-witness", 8);
    let published = fixture.table.current();
    let expected_unexamined_version = published;
    let targets = [(0u32, 1u64, 20_000u64)];

    // (a) Stale expectation in shard 1: loads shard 1 once, joins `shadowed`
    // with the shard entry's version, and keeps the cell clean (`Loaded`).
    let s1_min = fixture.shard_min_seed(1);
    let s1_actual = fixture.expected_position(s1_min);
    let stale_expected = position(u64::from(s1_min), 500);
    let loads_before = index_loads(&fixture.table, fixture.ks);
    let shadowed = fixture
        .table
        .relocation_apply(
            fixture.ks,
            &fixture.cell,
            RelocationApply::Repoint(&[(
                reduced_key(&fixture.table, fixture.ks, &spaced_key(s1_min)),
                stale_expected,
                stale_expected.moved_to(position(u64::from(s1_min), 70_000).address),
            )]),
            &targets,
        )
        .expect("stale shard expectation");
    assert_eq!(
        index_loads(&fixture.table, fixture.ks) - loads_before,
        1,
        "stale shard check must load only shard 1"
    );
    assert_eq!(
        shadowed.len(),
        1,
        "stale shard expectation must be reported: {shadowed:?}"
    );
    assert!(
        shadowed[0].0.same_physical(&stale_expected) && shadowed[0].1 == s1_actual.batch,
        "stale shard expectation must report shard entry's version: {shadowed:?}"
    );
    let state = fixture.table.large_table().entry_state_for_test(
        fixture.table.contexts().ks_context(fixture.ks),
        &fixture.cell,
    );
    assert_eq!(
        state, "Loaded",
        "unchanged relocation_apply must not dirty the cell, got {state}"
    );

    // (b) Key in the gap between shard 0's max_key and shard 1's min_key:
    // `shard_for` returns `None`, 0 shard loads, joins shadow witness.
    let gap_raw = [fixture.shard_max_seed(0) * KEY_STRIDE + 1, 0, 0, 0];
    let gap_expected = position(2, 600);
    let loads_before = index_loads(&fixture.table, fixture.ks);
    let shadowed = fixture
        .table
        .relocation_apply(
            fixture.ks,
            &fixture.cell,
            RelocationApply::Repoint(&[(
                reduced_key(&fixture.table, fixture.ks, &gap_raw),
                gap_expected,
                gap_expected.moved_to(position(2, 71_000).address),
            )]),
            &targets,
        )
        .expect("gap key expectation");
    assert_eq!(
        index_loads(&fixture.table, fixture.ks) - loads_before,
        0,
        "gap key between shards must load 0 shards"
    );
    assert_eq!(
        shadowed.len(),
        1,
        "unexamined gap key must join shadow witness: {shadowed:?}"
    );
    assert!(
        shadowed[0].0.same_physical(&gap_expected) && shadowed[0].1 == expected_unexamined_version,
        "gap key must record published shadowing version: {shadowed:?}"
    );

    // (c) Key strictly inside shard 1's `[min_key, max_key]` that is absent
    // from shard 1: loads shard 1 once, joins shadow witness.
    let absent_in_s1_raw = [s1_min * KEY_STRIDE + 1, 0, 0, 0];
    assert!(
        absent_in_s1_raw.as_slice() < fixture.shards[1].1.max_key.as_slice(),
        "premise: absent key falls strictly inside shard 1's range"
    );
    let absent_expected = position(3, 700);
    let loads_before = index_loads(&fixture.table, fixture.ks);
    let shadowed = fixture
        .table
        .relocation_apply(
            fixture.ks,
            &fixture.cell,
            RelocationApply::Repoint(&[(
                reduced_key(&fixture.table, fixture.ks, &absent_in_s1_raw),
                absent_expected,
                absent_expected.moved_to(position(3, 72_000).address),
            )]),
            &targets,
        )
        .expect("absent-in-shard expectation");
    assert_eq!(
        index_loads(&fixture.table, fixture.ks) - loads_before,
        1,
        "absent key inside shard 1 range must load only shard 1"
    );
    assert_eq!(
        shadowed.len(),
        1,
        "unexamined absent-in-shard key must join shadow witness: {shadowed:?}"
    );
    assert!(
        shadowed[0].0.same_physical(&absent_expected)
            && shadowed[0].1 == expected_unexamined_version,
        "absent-in-shard key must record published shadowing version: {shadowed:?}"
    );

    // (d) A re-point on a sharded cell keeps narrow shard loading (matching
    // native `lift_shard_entries`).
    let moved_s1 = s1_actual.moved_to(position(u64::from(s1_min), 73_000).address);
    let loads_before = index_loads(&fixture.table, fixture.ks);
    let shadowed = fixture
        .table
        .relocation_apply(
            fixture.ks,
            &fixture.cell,
            RelocationApply::Repoint(&[(
                reduced_key(&fixture.table, fixture.ks, &spaced_key(s1_min)),
                s1_actual,
                moved_s1,
            )]),
            &targets,
        )
        .expect("re-point with one shard update");
    assert_eq!(
        index_loads(&fixture.table, fixture.ks) - loads_before,
        1,
        "a re-point on a sharded cell must still load only the owning shard"
    );
    assert!(
        shadowed.is_empty(),
        "matching update must apply cleanly: {shadowed:?}"
    );
    assert_eq!(
        fixture.get_offset(s1_min, version(published.sequence + 1)),
        Some(member_offset(73_000)),
        "s1_min re-pointed"
    );

    // (e) Dropped cell: 0 shard loads and every below-target expectation joins
    // the shadow witness.
    fixture
        .table
        .drop_cells(
            fixture.ks,
            fixture.cell.clone(),
            fixture.cell.clone(),
            version(published.sequence + 1),
        )
        .expect("drop cell");
    let dropped_expected = position(4, 800);
    let loads_before = index_loads(&fixture.table, fixture.ks);
    let shadowed = fixture
        .table
        .relocation_apply(
            fixture.ks,
            &fixture.cell,
            RelocationApply::Repoint(&[(
                reduced_key(&fixture.table, fixture.ks, &spaced_key(s1_min)),
                dropped_expected,
                dropped_expected.moved_to(position(4, 74_000).address),
            )]),
            &targets,
        )
        .expect("relocation_apply on dropped cell");
    assert_eq!(
        index_loads(&fixture.table, fixture.ks) - loads_before,
        0,
        "dropped cell must load 0 shards"
    );
    assert_eq!(
        shadowed.len(),
        1,
        "expectation on a dropped cell must join shadow witness: {shadowed:?}"
    );
    assert!(
        shadowed[0].0.same_physical(&dropped_expected)
            && shadowed[0].1 == expected_unexamined_version,
        "dropped-cell expectation must record published shadowing version: {shadowed:?}"
    );
}

#[test]
fn unexamined_witness_at_the_serving_epoch_start_keeps_the_published_cut() {
    // A latch can hold cut `(epoch, 0)` while the epoch's first `DropCells`
    // batch is already in the index but not yet published (the index mutation
    // runs before the watermark advance). The expectation loaded from the
    // previous epoch lies below that cut, so the witness must be the cut, not
    // the expectation's own batch: `cut_at_or_below((E, 0), (E-1, x))` is
    // false, and a latch pinned at `(E, 0)` would lose the file. Serving
    // epoch 2 with no publication in it reaches that state deterministically;
    // on the old `None => expected` fallback this test fails.
    let dir = tempdir::TempDir::new("hybrid-shard-preload-unpublished").expect("scratch directory");
    let cuts = CutSources::new(2, DurabilityTier::Native);
    let table = HybridTable::open(
        dir.path(),
        sharded_shape(),
        &HybridIndexConfig::default(),
        cuts,
        Metrics::new(),
    )
    .expect("open table");
    let ks = table.shape().iter_ks().next().expect("one keyspace").id();
    let cell = table.shape().ks(ks).cell_id(&spaced_key(1));
    let epoch_start = crate::distributed::types::BatchVersion {
        epoch: 2,
        sequence: 0,
    };
    assert_eq!(
        table.current(),
        epoch_start,
        "premise: nothing is published in the serving epoch yet"
    );
    // One key of the previous epoch, without a watermark advance: the cell
    // exists while `published` stays at `(2, 0)`. The cell's frontier is the
    // minimum of the entry and the applied bound, so inserting below it is
    // legal and is exactly the retained-epoch case under test.
    let stored_key = spaced_key(7);
    table
        .insert_published(
            &[PublishedMutation {
                keyspace: ks,
                key: &stored_key,
                kind: RecordKind::Put,
                position: position(1, initial_offset(7)),
                value: Some(&Bytes::from_static(b"stored")),
            }],
            64,
        )
        .expect("publish one key");
    let targets = [(0u32, 1u64, 20_000u64)];

    // (a) A live cell, with an expectation for a key it does not hold.
    let absent_expected = position(1, 500);
    assert!(
        absent_expected.batch < epoch_start,
        "premise: the expectation is below the serving epoch's start"
    );
    let shadowed = table
        .relocation_apply(
            ks,
            &cell,
            RelocationApply::Repoint(&[(
                reduced_key(&table, ks, &spaced_key(11)),
                absent_expected,
                absent_expected.moved_to(position(1, 60_000).address),
            )]),
            &targets,
        )
        .expect("relocation_apply before any publication");
    assert_eq!(
        shadowed.len(),
        1,
        "unexamined expectation must join the witness: {shadowed:?}"
    );
    assert!(
        shadowed[0].0.same_physical(&absent_expected) && shadowed[0].1 == table.current(),
        "the witness must keep the serving epoch's start cut: {shadowed:?}"
    );

    // (b) The cell dropped before the drop's publication: the `Ok(None)`
    // branch applies the same bound.
    table
        .drop_cells(ks, cell.clone(), cell.clone(), version(1))
        .expect("drop cell");
    let dropped_expected = position(1, 600);
    let shadowed = table
        .relocation_apply(
            ks,
            &cell,
            RelocationApply::Repoint(&[(
                reduced_key(&table, ks, &spaced_key(11)),
                dropped_expected,
                dropped_expected.moved_to(position(1, 61_000).address),
            )]),
            &targets,
        )
        .expect("relocation_apply on a dropped cell before any publication");
    assert_eq!(
        shadowed.len(),
        1,
        "dropped-cell expectation must join the witness: {shadowed:?}"
    );
    assert!(
        shadowed[0].0.same_physical(&dropped_expected) && shadowed[0].1 == table.current(),
        "the dropped-cell witness must keep the serving epoch's start cut: {shadowed:?}"
    );
}
