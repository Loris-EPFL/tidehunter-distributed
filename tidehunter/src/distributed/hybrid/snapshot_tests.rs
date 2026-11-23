//! Table-level crash cuts for the snapshot pass.
//!
//! Each test is one durable boundary of `rebuild_control_region`: the pass
//! performs every step up to the boundary and none after, which is exactly what
//! a process crash there leaves behind. The property under test is always the
//! same — the region on disk still names a cut the index can serve, and a
//! reopen replays from the stored per-stream fronts to the same logical state.
//!
//! Uses the coordinator's own local index WAL and control region; no owner is
//! involved, because the durable-cut record is what proves the value bytes
//! (native's value-WAL fsync has no counterpart on the hybrid side).

use super::config::HybridIndexConfig;
use super::loader::CutSources;
use super::snapshot::{SnapshotBoundary, inject_snapshot_fault, rebuild_control_region};
use super::table::HybridTable;
use crate::crc::CrcFrame;
use crate::distributed::durability::DurabilityTier;
use crate::distributed::types::{BatchVersion, PhysicalValueAddress, RecordKind};
use crate::key_shape::{KeyShape, KeyType};
use crate::metrics::Metrics;
use std::sync::Arc;

/// Every test below covers an index-only table (`HybridTable::snapshot` does the
/// same): it has no retained older generation, so its older-row slice is empty
/// and each call passes `&[]` for the generation's rows. Tests that store real
/// older rows are `hybrid/region.rs` and `remote_db/dead_wal_files/tests.rs`,
/// where the rows come from the manifest.
/// One cell: every key of the tests below lands in it, so "a dirty cell" is
/// unambiguous. Tests that need two cells use [`Fixture::with_shape`] and
/// [`two_cell_shape`] and assert through the table's routing that their keys
/// really fall into different cells.
fn shape() -> Arc<KeyShape> {
    Arc::new(KeyShape::new_single(4, 1, KeyType::uniform(1)))
}

/// Two cells (two mutexes of one cell each).
fn two_cell_shape() -> Arc<KeyShape> {
    Arc::new(KeyShape::new_single(4, 2, KeyType::uniform(1)))
}

/// The member descriptor offset `put_key` gives every put.
const DESCRIPTOR_OFFSET: u32 = 8;

/// `VersionPosition::member` names the member frame, not the batch frame: the
/// batch offset plus the CRC frame header plus the member descriptor offset.
/// Derived from the same constants the production constructor uses, so the
/// test follows a header change instead of pinning a literal. The tests below
/// pass batch offsets and compare member offsets.
const MEMBER_FRAME_SHIFT: u64 = CrcFrame::CRC_HEADER_LENGTH as u64 + DESCRIPTOR_OFFSET as u64;

fn version(sequence: u64) -> BatchVersion {
    BatchVersion { epoch: 1, sequence }
}

struct Fixture {
    dir: tempdir::TempDir,
    table: Arc<HybridTable>,
    cuts: CutSources,
    config: HybridIndexConfig,
    shape: Arc<KeyShape>,
}

impl Fixture {
    /// Sui's production index values: the stale-unload window is 7.5 GiB, so a
    /// freshly dirtied cell is never stale and the cadence pass leaves it alone.
    fn new(name: &str) -> Self {
        Self::with_config(name, HybridIndexConfig::default())
    }

    /// A zero stale-unload window: every dirty cell is stale, so the ordinary
    /// (non-forced) cadence pass flushes everything and its cut advances. This
    /// is how the cadence path — the one Sui runs every 8 GiB — is exercised
    /// deterministically, without the forced path's flush-everything threshold.
    fn eager(name: &str) -> Self {
        Self::with_config(
            name,
            HybridIndexConfig {
                snapshot_unload_threshold: 0,
                ..HybridIndexConfig::default()
            },
        )
    }

    fn with_config(name: &str, config: HybridIndexConfig) -> Self {
        Self::with_shape(name, config, shape())
    }

    fn with_shape(name: &str, config: HybridIndexConfig, shape: Arc<KeyShape>) -> Self {
        let dir = tempdir::TempDir::new(name).expect("scratch directory");
        let cuts = CutSources::new(1, DurabilityTier::Native);
        let table = Arc::new(
            HybridTable::open(
                dir.path(),
                shape.clone(),
                &config,
                cuts.clone(),
                Metrics::new(),
            )
            .expect("open table"),
        );
        Self {
            dir,
            table,
            cuts,
            config,
            shape,
        }
    }

    fn reopen(&mut self) {
        self.table = Arc::new(
            HybridTable::open(
                self.dir.path(),
                self.shape.clone(),
                &self.config,
                self.cuts.clone(),
                Metrics::new(),
            )
            .expect("reopen table"),
        );
    }

    /// The cell the table's own routing gives `key_seed`'s key. Tests that need
    /// "different cells" assert it through this, not through key parity.
    fn cell_of(&self, key_seed: u8) -> crate::cell::CellId {
        self.shape
            .iter_ks()
            .next()
            .expect("one keyspace")
            .cell_id(&[key_seed; 4])
    }

    /// One published put at `sequence`, so the pass has a dirty cell whose
    /// frontier the stored region must name.
    fn put(&self, sequence: u64) {
        self.put_key(sequence, sequence as u8, 128);
    }

    /// A published put of key `key_seed` at `sequence`, backed by a member frame
    /// at `offset`. Two puts with the same sequence are two entries of one
    /// version in the same cell, which is how a dirty cell below the stored head
    /// is produced for the forced-store test.
    fn put_key(&self, sequence: u64, key_seed: u8, offset: u64) {
        let keyspace = self.shape.iter_ks().next().expect("one keyspace").id();
        let position = super::position::VersionPosition::member(
            version(sequence),
            PhysicalValueAddress {
                partition: 0,
                segment_generation: 1,
                offset,
                len: 64,
            },
            0,
            crate::distributed::types::RecordDescriptor {
                operation_index: 0,
                offset: DESCRIPTOR_OFFSET,
                len: 24,
                keyspace: 0,
                kind: RecordKind::Put,
                key_len: 4,
                value_offset: 14,
                value_len: 2,
                checksum: crate::distributed::types::MemberChecksum(0),
            },
            crc32fast::hash(b"value"),
        );
        let key = [key_seed; 4];
        self.table
            .insert_published(
                &[super::table::PublishedMutation {
                    keyspace,
                    key: &key,
                    kind: RecordKind::Put,
                    position,
                }],
                64,
            )
            .expect("publish");
    }

    fn advance(&self, sequence: u64) {
        self.cuts.set_applied(version(sequence));
        self.cuts.set_published(version(sequence));
        self.cuts.set_durable(version(sequence));
    }

    /// The index position of `key_seed` at the stored head.
    ///
    /// A cut is exclusive, so the query uses one sequence above the stored head;
    /// asking at the head itself would hide the head's own positions. This
    /// fixture has no owners, so a value read is impossible; the index half —
    /// the position the region and the replayed suffix must agree on — is what a
    /// crash cut is allowed to disturb.
    fn indexed_key(&self, key_seed: u8) -> Option<super::position::VersionPosition> {
        let keyspace = self.shape.iter_ks().next().expect("one keyspace").id();
        let head = self.table.stored_cut();
        let cut = BatchVersion {
            epoch: head.epoch,
            sequence: head.sequence.saturating_add(1),
        };
        let key = [key_seed; 4];
        self.table.get(keyspace, &key, cut).expect("index lookup")
    }

    fn indexed(&self, sequence: u64) -> Option<super::position::VersionPosition> {
        self.indexed_key(sequence as u8)
    }
}

/// Every put in these tests is published one version below the cut the pass
/// advances to (`put(s)` then `advance(s + 1)`): a position equal to the cut is
/// not below it, so it stays in the overlay and a reopen without owners cannot
/// recover it. The ordinary cadence path runs on [`Fixture::eager`] (a zero
/// stale-unload window, so its pass flushes and advances); the forced path
/// flushes every dirty cell whatever the window.
#[test]
fn a_cadence_pass_stores_the_cut_it_covered_and_survives_a_reopen() {
    let mut fixture = Fixture::eager("hybrid-snapshot-cadence");
    fixture.put(5);
    fixture.advance(6);
    let pass = rebuild_control_region(&fixture.table, false, None, &[]).expect("cadence pass");
    assert!(pass.stored, "a moved cut must write a region");
    assert!(!pass.forced);
    assert_eq!(pass.cut, version(6));
    assert_eq!(pass.promotion_cut, version(6));
    assert_eq!(fixture.table.stored_cut(), version(6));
    fixture.reopen();
    assert_eq!(fixture.table.stored_cut(), version(6));
    assert!(
        fixture.indexed(5).is_some(),
        "the stored put must be in a blob"
    );
}

#[test]
fn a_forced_pass_that_moves_the_cut_is_an_ordinary_store() {
    let fixture = Fixture::new("hybrid-snapshot-store");
    fixture.put(5);
    fixture.advance(6);
    let pass =
        rebuild_control_region(&fixture.table, true, Some(version(6)), &[]).expect("snapshot");
    assert!(pass.stored, "a moved cut must write a region");
    assert!(!pass.forced, "the cut moved, so this is an ordinary store");
    assert_eq!(pass.cut, version(6));
    assert_eq!(fixture.table.stored_cut(), version(6));
}

#[test]
fn a_dirty_unflushed_cell_below_the_head_holds_the_cut_back() {
    // This asserts the region CUT, not recovery: the fixture has no owners, so
    // the claim "recovery replays the put" is executed only by the owners-level
    // test in S12. The production line under test is
    // `cut = head.min(snapshot.replay_from)`. A put at v3 is below the head v5
    // but newer than its cell's last flush, and the cell is not stale by byte
    // age, so the cadence pass does not flush it. Native stores the minimum
    // cell frontier as the region cut, so recovery replays the put; headed at
    // the head instead, the put would be in neither a blob nor the replay range.
    // The cut assertion is the negative control — the unfixed pass stores
    // `version(5)` here.
    let mut fixture = Fixture::new("hybrid-snapshot-dirty-cell");
    // A non-zero byte counter keeps the cell's byte age above the stale
    // threshold (which is zero while the counter is below the unload window).
    fixture.cuts.add_bytes(1 << 20);
    fixture.put(3);
    fixture.advance(5);
    let pass = rebuild_control_region(&fixture.table, false, None, &[]).expect("cadence pass");
    assert!(
        pass.stored,
        "the dirty cell's frontier is above the empty region"
    );
    assert!(
        pass.cut < version(5),
        "the region must not be headed above a dirty unflushed cell"
    );
    assert!(pass.cut <= version(3), "the cut is the cell's own frontier");
    assert_eq!(fixture.table.stored_cut(), pass.cut);

    // Once the cell is flushed (a forced pass flushes every dirty cell) the cut
    // moves to the head and the put is in a blob: a reopen recovers it with no
    // replay at all.
    let flushed =
        rebuild_control_region(&fixture.table, true, Some(version(5)), &[]).expect("flush");
    assert!(flushed.stored && !flushed.forced);
    assert_eq!(flushed.cut, version(5));
    fixture.reopen();
    assert_eq!(fixture.table.stored_cut(), version(5));
    assert!(
        fixture.indexed(3).is_some(),
        "the flushed put must be in a blob"
    );
}

#[test]
fn a_head_above_the_durable_cut_is_refused() {
    // The pass cannot reach this by construction (`min(durable, ...)` bounds the
    // head), so the release check is defence in depth against a Version B
    // regression: a head the durable record has not proven must fail closed with
    // the named error rather than be stored.
    let fixture = Fixture::new("hybrid-snapshot-durable");
    fixture.put(2);
    fixture.advance(2);
    let before = fixture.table.stored_cut();
    let error = fixture
        .table
        .loader()
        .ensure_durable_target(version(3))
        .expect_err("a head above the durable cut is not provable");
    assert!(
        error.to_string().contains("is above the durable cut"),
        "unexpected error: {error}"
    );
    // A refused proof leaves the stored region alone.
    assert_eq!(fixture.table.stored_cut(), before);
}

#[test]
fn a_pass_that_fails_at_a_boundary_stores_nothing() {
    // Both modes: the ordinary cadence pass (eager fixture, threshold = byte
    // age) and the forced pass (threshold = byte position). Each must reach
    // every boundary and leave the previous region untouched.
    for force in [false, true] {
        for boundary in [
            SnapshotBoundary::AfterFlush,
            SnapshotBoundary::AfterDurableTarget,
            SnapshotBoundary::AfterDrain,
            SnapshotBoundary::AfterIndexSync,
        ] {
            let mut fixture = Fixture::eager("hybrid-snapshot-crash");
            let pass = |fixture: &Fixture, frontier: u64| {
                if force {
                    rebuild_control_region(&fixture.table, true, Some(version(frontier)), &[])
                } else {
                    rebuild_control_region(&fixture.table, false, None, &[])
                }
            };
            // A stored baseline first: this fixture has no owners, so a reopen
            // can only recover what a region already names. The crash must then
            // hit a later pass that would have moved the cut.
            fixture.put(3);
            fixture.advance(4);
            let baseline = pass(&fixture, 4).expect("baseline");
            assert!(baseline.stored);
            let stored_before = fixture.table.stored_cut();
            assert_eq!(stored_before, version(4));
            let indexed_before = fixture.indexed(3);
            assert_eq!(
                indexed_before.map(|position| position.batch),
                Some(version(3)),
                "the baseline region must make the put resolvable"
            );

            // The later pass flushes every dirty cell in either mode, so its cut
            // moves and every boundary after the no-op decision is reachable.
            fixture.put(5);
            fixture.advance(6);
            inject_snapshot_fault(boundary);
            let error = pass(&fixture, 6)
                .err()
                .unwrap_or_else(|| panic!("{boundary:?} (force={force}) must abort the pass"));
            assert!(error.to_string().contains("injected snapshot crash"));
            // The old region is intact and still names the baseline cut.
            assert_eq!(fixture.table.stored_cut(), stored_before);
            assert_eq!(fixture.indexed(3), indexed_before);
            // Reopening from it reaches the same index state; the crash cost a
            // pass, not data. (No owners here, so the value itself is out of
            // scope; the position the stored region resolves to is the
            // observable.)
            fixture.reopen();
            assert_eq!(fixture.table.stored_cut(), stored_before);
            assert_eq!(fixture.indexed(3), indexed_before);
        }
    }
}

#[test]
fn a_latch_below_the_stored_cut_does_not_lower_a_clean_cell() {
    // The checkpoint pin race: a reader latch ends up below the stored head. The
    // native snapshot advances a clean cell to the loader's `last_processed()`
    // only when that value is not below the cell's own, so a held latch cannot
    // lower the cut: the cadence pass is an unmoved cut (no store, no deferral)
    // and a forced pass is a forced re-cover at the stored cut, not an error.
    // The typed `CutBelowStored` error needs a dirty cell below the stored cut
    // at the snapshot step of a FORCED pass, which the forced flush rules out.
    let fixture = Fixture::eager("hybrid-snapshot-low-latch");
    fixture.put(3);
    fixture.advance(4);
    let baseline = rebuild_control_region(&fixture.table, false, None, &[]).expect("baseline");
    assert!(baseline.stored);
    assert_eq!(baseline.cut, version(4));
    let fronts = fixture.table.stored_fronts().to_vec();
    let deferrals = fixture.table.snapshot_latch_deferrals();

    let latch = fixture.cuts.pin_cut(version(2));
    let cadence = rebuild_control_region(&fixture.table, false, None, &[]).expect("cadence pass");
    assert!(!cadence.stored, "an unmoved cut adds nothing to store");
    assert_eq!(cadence.cut, version(4));
    assert_eq!(
        fixture.table.snapshot_latch_deferrals(),
        deferrals,
        "a latch below the head must not lower a clean cell, so no deferral"
    );
    assert_eq!(fixture.table.stored_fronts(), fronts.as_slice());
    let forced = rebuild_control_region(&fixture.table, true, Some(version(4)), &[])
        .expect("a forced pass re-covers the stored cut");
    assert!(forced.stored && forced.forced, "{forced:?}");
    assert_eq!(forced.cut, version(4));
    assert_eq!(fixture.table.stored_cut(), version(4));
    drop(latch);
}

#[test]
fn a_pass_with_no_advance_keeps_the_region_identical() {
    // A cut that did not move must not rewrite the region: the stored cut and
    // every per-stream front stay exactly as the previous store left them.
    let fixture = Fixture::new("hybrid-snapshot-noop");
    fixture.put(6);
    fixture.advance(7);
    let first = rebuild_control_region(&fixture.table, true, Some(version(7)), &[]).expect("first");
    assert!(first.stored);
    let fronts = fixture.table.stored_fronts().to_vec();
    let deferrals = fixture.table.snapshot_latch_deferrals();
    // Nothing advanced and every cell is clean, so the snapshot's frontier is
    // the stored cut again.
    let again = rebuild_control_region(&fixture.table, false, None, &[]).expect("no-op snapshot");
    assert!(
        !again.stored,
        "an unchanged cut must not rewrite the region"
    );
    assert_eq!(again.cut, first.cut);
    assert_eq!(fixture.table.stored_cut(), first.cut);
    assert_eq!(fixture.table.stored_fronts(), fronts.as_slice());
    assert_eq!(
        fixture.table.snapshot_latch_deferrals(),
        deferrals,
        "an unmoved cut is not a latch deferral"
    );
}

#[test]
fn two_deferrals_under_a_dirty_cell_below_stored_are_counted() {
    // A dirty cell whose FIRST write lies below the stored head: a cell that was
    // empty when the region was stored (a second cell here) gets an older
    // version, as a relocation re-point into an evicted cell would. A cell that
    // already has a flushed blob keeps its own, higher frontier when it is
    // dirtied (so that case never lowers the cut). The cadence pass does not
    // flush the new cell (its byte age is below the stale window), so its
    // frontier is its own anchor and the pass cut falls below the stored cut:
    // each such pass is a deferral. Each one must leave the region file and the
    // stored fronts byte-identical, and each must be counted, so a cell stuck
    // below the head is visible instead of silently holding every later
    // snapshot back.
    //
    // A reader latch cannot produce this state any more: a clean cell is never
    // lowered by a held latch (see
    // `a_latch_below_the_stored_cut_does_not_lower_a_clean_cell`).
    let fixture = Fixture::with_shape(
        "hybrid-snapshot-deferrals",
        HybridIndexConfig::default(),
        two_cell_shape(),
    );
    // A non-zero byte counter keeps every cell's byte age above the stale
    // threshold, so the cadence pass leaves a dirty cell alone.
    fixture.cuts.add_bytes(1 << 20);
    fixture.put(3);
    fixture.advance(4);
    let baseline =
        rebuild_control_region(&fixture.table, true, Some(version(4)), &[]).expect("baseline");
    assert!(baseline.stored && !baseline.forced);
    assert_eq!(baseline.cut, version(4));
    // The older version lands in the other, still empty cell. The premise
    // "different cells" is asserted through the table's own routing, not
    // through key parity or bit patterns.
    assert_ne!(
        fixture.cell_of(3),
        fixture.cell_of(254),
        "premise: the baseline key and the older key are in different cells"
    );
    fixture.put_key(2, 254, 300);
    let region_file = |fixture: &Fixture| {
        std::fs::read(fixture.table.region().lock().path()).expect("region file")
    };
    let bytes = region_file(&fixture);
    let fronts = fixture.table.stored_fronts().to_vec();
    let before = fixture.table.snapshot_latch_deferrals();

    for round in 0..2 {
        let pass = rebuild_control_region(&fixture.table, false, None, &[]).expect("deferred pass");
        assert!(!pass.stored, "round {round}: nothing the region could add");
        assert_eq!(pass.cut, baseline.cut, "round {round}");
        assert_eq!(
            fixture.table.snapshot_latch_deferrals(),
            before + round + 1,
            "round {round}: each deferral must be counted"
        );
    }
    assert_eq!(
        region_file(&fixture),
        bytes,
        "the region file must not change"
    );
    assert_eq!(fixture.table.stored_fronts(), fronts.as_slice());
    assert_eq!(fixture.table.stored_cut(), baseline.cut);
    // A forced pass flushes the dirty cell, so the cut is the stored one again
    // and the pass is a forced re-cover, never a deferral.
    let forced =
        rebuild_control_region(&fixture.table, true, Some(baseline.cut), &[]).expect("forced");
    assert!(forced.stored && forced.forced, "{forced:?}");
    assert_eq!(fixture.table.snapshot_latch_deferrals(), before + 2);
}

#[test]
fn a_forced_store_re_covers_the_same_cut_and_flushes_every_dirty_cell() {
    let mut fixture = Fixture::new("hybrid-snapshot-forced");
    fixture.put(2);
    fixture.advance(3);
    let first = rebuild_control_region(&fixture.table, true, Some(version(3)), &[]).expect("first");
    assert!(first.stored && !first.forced);
    assert_eq!(first.cut, version(3));
    assert_eq!(
        fixture.indexed(2).map(|position| position.address.offset),
        Some(128 + MEMBER_FRAME_SHIFT)
    );

    // Dirty a second key below the stored cut — the state a relocation re-point
    // leaves behind. Its byte age is zero, so only a real force-flush promotes
    // it. Nothing advanced, so the forced store lands at the same cut.
    fixture.put_key(2, 9, 300);
    let forced = rebuild_control_region(&fixture.table, true, Some(first.cut), &[])
        .expect("forced snapshot");
    assert!(forced.stored && forced.forced);
    assert_eq!(forced.cut, first.cut, "a forced store keeps the cut");
    assert_eq!(forced.promotion_cut, first.cut);
    assert!(
        fixture.table.large_table().is_all_clean(),
        "the forced pass must flush every dirty cell"
    );
    assert_eq!(
        fixture
            .indexed_key(9)
            .map(|position| position.address.offset),
        Some(300 + MEMBER_FRAME_SHIFT)
    );
    // The re-covered region persists it: a reopen loads the new cell base.
    fixture.reopen();
    assert_eq!(
        fixture
            .indexed_key(9)
            .map(|position| position.address.offset),
        Some(300 + MEMBER_FRAME_SHIFT),
        "the forced region must persist the re-pointed cell"
    );

    // A forced pass whose cut did advance is an ordinary store.
    fixture.put(4);
    fixture.advance(5);
    let advanced = rebuild_control_region(&fixture.table, true, Some(version(5)), &[])
        .expect("advanced store");
    assert!(advanced.stored && !advanced.forced);
    assert_eq!(advanced.cut, version(5));
    assert_eq!(advanced.promotion_cut, version(5));

    // A frontier above the stored head is refused before any work.
    let error = rebuild_control_region(&fixture.table, true, Some(version(9)), &[])
        .expect_err("a clamp above the head must fail");
    assert!(
        error
            .to_string()
            .contains("forced promotion cut above the stored target")
    );
    // A forced pass without a frontier is a caller bug, refused the same way.
    let error = rebuild_control_region(&fixture.table, true, None, &[])
        .expect_err("a forced store needs a frontier");
    assert!(error.to_string().contains("requires a promotion frontier"));
}

#[test]
fn a_completed_pass_survives_a_reopen() {
    let mut fixture = Fixture::new("hybrid-snapshot-reopen");
    fixture.put(4);
    fixture.advance(5);
    let pass =
        rebuild_control_region(&fixture.table, true, Some(version(5)), &[]).expect("snapshot");
    assert!(pass.stored);
    let indexed = fixture.indexed(4);
    assert_eq!(
        indexed.map(|position| position.batch),
        Some(version(4)),
        "the snapshot must keep the put resolvable"
    );
    fixture.reopen();
    assert_eq!(fixture.table.stored_cut(), pass.cut);
    assert_eq!(fixture.indexed(4), indexed);
}

#[test]
fn a_held_latch_keeps_the_cut_below_the_published_but_unstored_extent() {
    // A reader holds a checkpoint at v3 while v4 is published and applied up to
    // v6. The pass runs under the latch, so its flush frontier — and therefore
    // the region cut, the owner drain point and the stored fronts — stay at or
    // below v3: the put at v4 is in no stored blob and must stay inside the
    // replay range. Naming a cut at the head (v6) here would drop it after a
    // crash. With owners the stored fronts start at that extent; this fixture
    // has none, so the observable is the cut itself.
    let mut fixture = Fixture::new("hybrid-snapshot-latch");
    fixture.put(2);
    fixture.advance(3);
    let latch = fixture.cuts.pin_cut(version(3));
    fixture.put(4);
    fixture.advance(6);
    let held =
        rebuild_control_region(&fixture.table, true, Some(version(3)), &[]).expect("held pass");
    assert!(
        held.cut <= version(3),
        "a held latch must keep the region cut below the unstored extent"
    );
    assert!(fixture.table.stored_cut() <= version(3));

    // Releasing the latch lets the next pass flush through the head: the cut
    // follows, and the put at v4 is in a blob a reopen can serve.
    drop(latch);
    let released =
        rebuild_control_region(&fixture.table, true, Some(version(6)), &[]).expect("released pass");
    assert_eq!(released.cut, version(6));
    fixture.reopen();
    assert!(
        fixture.indexed(4).is_some(),
        "the flushed put must be in a blob"
    );
}

/// Copies a directory tree (files only), the way a crash freezes what is on
/// disk at one instant.
fn copy_dir(from: &std::path::Path, to: &std::path::Path) {
    std::fs::create_dir_all(to).expect("create crash state directory");
    for entry in std::fs::read_dir(from).expect("read state directory") {
        let entry = entry.expect("directory entry");
        let target = to.join(entry.file_name());
        if entry.file_type().expect("entry type").is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), &target).expect("copy state file");
        }
    }
}

impl Fixture {
    /// Opens a table on a frozen copy of another table's directory, with the
    /// same cut sources and index configuration.
    fn adopt(&self, dir: tempdir::TempDir) -> Fixture {
        let table = Arc::new(
            HybridTable::open(
                dir.path(),
                self.shape.clone(),
                &self.config,
                self.cuts.clone(),
                Metrics::new(),
            )
            .expect("open crash state"),
        );
        Fixture {
            dir,
            table,
            cuts: self.cuts.clone(),
            config: self.config.clone(),
            shape: self.shape.clone(),
        }
    }
}

/// Every file under `dir`, relative to it.
fn files_under(dir: &std::path::Path) -> std::collections::BTreeSet<std::path::PathBuf> {
    fn walk(
        root: &std::path::Path,
        dir: &std::path::Path,
        files: &mut std::collections::BTreeSet<std::path::PathBuf>,
    ) {
        for entry in std::fs::read_dir(dir).expect("read directory") {
            let entry = entry.expect("directory entry");
            if entry.file_type().expect("entry type").is_dir() {
                walk(root, &entry.path(), files);
            } else {
                files.insert(
                    entry
                        .path()
                        .strip_prefix(root)
                        .expect("under the root")
                        .to_path_buf(),
                );
            }
        }
    }
    let mut files = std::collections::BTreeSet::new();
    walk(dir, dir, &mut files);
    files
}

#[test]
fn a_crash_after_the_region_rename_reopens_in_either_directory_state() {
    // Between the rename of the new region and the directory sync that makes the
    // new name durable, a crash can reopen either the previous region or the new
    // one, and the pass has not yet unlinked the index files only the previous
    // region references. The hook freezes the directory at exactly that instant;
    // the two reopen states are the frozen directory as is (new region present)
    // and the same directory with the previous region restored (old region
    // present). The production line under test is the order in
    // `rebuild_control_region`: `region.store(..)` precedes
    // `index_writer.delete_files(to_delete)`.
    //
    // The control only has teeth if the pass really unlinks something, so the
    // index WAL uses one-fragment files and the test keeps running passes until
    // one removes an index file, then freezes that pass. Moving `delete_files`
    // above `store` makes the frozen directory lack the blob's file, and the old
    // region's `indexed(previous)` below then fails to load it (the negative
    // control to run at integration; it has not been run against the real table).
    //
    // Both states reopen with the cuts at the pass's head and no owners, so the
    // put the new region does not cover is not recovered here: replay after a
    // crash is the owners-level test (S12), not this fixture.
    // One-fragment index WAL files roll quickly. Two repeatedly overwritten
    // keys keep the cell blobs small; auto-sharding is not required here.
    let frag_size = 4096;
    let fixture = Fixture::with_shape(
        "hybrid-snapshot-rename-cut",
        HybridIndexConfig {
            snapshot_unload_threshold: 0,
            frag_size,
            wal_file_size: frag_size,
            ..HybridIndexConfig::default()
        },
        two_cell_shape(),
    );
    let index_files = |fixture: &Fixture| -> std::collections::BTreeSet<_> {
        fixture
            .table
            .loader()
            .index_wal()
            .file_ids()
            .into_iter()
            .collect()
    };
    let region_path = fixture.table.region().lock().path().to_path_buf();
    let region_name = region_path
        .file_name()
        .expect("region file name")
        .to_owned();

    // Two keys in two different cells, rewritten at every version: each cell
    // blob stays a few dozen bytes, so the 4 KiB index WAL rolls past a
    // referenced file long before a blob could outgrow a fragment. The property
    // under test: the
    // crash point is the instant after the region rename and before the unlink,
    // the two directory states are the frozen directory (new region) and the
    // same directory with the previous region restored, both must reopen, and
    // the pass must really unlink a file the previous region references. That
    // needs at least two stored passes across an index-file roll: the loop runs
    // until one pass unlinks such a file, and fails if none does within 120
    // passes.
    const ROLL_KEYS: [u8; 2] = [0, 255];
    assert_ne!(
        fixture.cell_of(ROLL_KEYS[0]),
        fixture.cell_of(ROLL_KEYS[1]),
        "premise: the two roll keys are in different cells"
    );
    let mut previous = 1;
    fixture.put_key(previous, ROLL_KEYS[1], 128);
    fixture.advance(previous + 1);
    let first = rebuild_control_region(&fixture.table, false, None, &[]).expect("first pass");
    assert!(first.stored);

    let mut crash = None;
    for _ in 0..120 {
        let sequence = previous + 2;
        // Alternate the two cells: one pass rewrites one cell, so the other
        // cell's blob sits in an older file that the rewritten cell's previous
        // blob may share, and the pass can unlink a file the previous region
        // references once the WAL rolled between the two.
        let roll_key = ROLL_KEYS[(sequence / 2 % 2) as usize];
        fixture.put_key(sequence, roll_key, 128);
        fixture.advance(sequence + 1);
        let old_region = std::fs::read(&region_path).expect("previous region");
        let indexed_old = fixture.indexed_key(roll_key);
        let files_before = index_files(&fixture);
        // The index WAL files the previous (currently stored) region references,
        // collected the way `rebuild_control_region` collects live files. Reading
        // the region, not opening a broken directory, keeps this deterministic.
        let layout = fixture.table.loader().index_wal().layout();
        let depends_on: std::collections::BTreeSet<_> =
            super::region::HybridRegionStore::read(&region_path)
                .expect("read the stored region")
                .expect("a region is stored")
                .region
                .snapshot()
                .iter_valid_val_positions()
                .map(|position| layout.locate_file(position.offset()))
                .collect();
        let frozen = tempdir::TempDir::new("hybrid-snapshot-rename-frozen").expect("frozen dir");
        let live = fixture.dir.path().to_path_buf();
        let frozen_path = frozen.path().to_path_buf();
        fixture
            .table
            .region()
            .lock()
            .set_after_rename_hook(Box::new(move || copy_dir(&live, &frozen_path)));
        let pass = rebuild_control_region(&fixture.table, false, None, &[]).expect("pass");
        assert!(pass.stored);
        assert_eq!(pass.cut, version(sequence + 1));
        let removed: std::collections::BTreeSet<_> = files_before
            .difference(&index_files(&fixture))
            .copied()
            .collect();
        // Only a pass that unlinks a file the previous region references proves
        // the ordering: a file can outlive a pass because of the
        // `pre_snapshot_file` cutoff, and unlinking one the previous region never
        // named would leave state 2 passing with nothing at stake.
        if removed.is_disjoint(&depends_on) || indexed_old.is_none() {
            previous = sequence;
            continue;
        }
        let layout = fixture.table.loader().index_wal().layout();
        let unlinked_names: std::collections::BTreeSet<_> = removed
            .iter()
            .filter_map(|id| {
                layout
                    .wal_file_name(std::path::Path::new(""), *id)
                    .file_name()
                    .map(std::ffi::OsStr::to_owned)
            })
            .collect();
        crash = Some((
            frozen,
            old_region,
            indexed_old,
            previous,
            sequence,
            roll_key,
            unlinked_names,
        ));
        break;
    }
    let (frozen, old_region, indexed_old, previous, sequence, roll_key, unlinked_names) = crash.expect(
        "no pass unlinked an index file the previous region references: the index WAL never rolled",
    );
    assert!(
        indexed_old.is_some(),
        "the previous region must cover its put, or state 2 would pass on None == None"
    );
    // The frozen directory holds every file the finished pass dropped from the
    // index WAL (checked by name, because the unlink itself is the background
    // unlinker's and may or may not have run by now).
    let frozen_names: std::collections::BTreeSet<_> = files_under(frozen.path())
        .iter()
        .filter_map(|path| path.file_name().map(std::ffi::OsStr::to_owned))
        .collect();
    assert!(
        !unlinked_names.is_empty() && unlinked_names.is_subset(&frozen_names),
        "the frozen directory must still hold the files the pass removed: removed {unlinked_names:?}, frozen {frozen_names:?}"
    );

    // Build the second state from the frozen bytes before anything opens them:
    // opening a table may itself touch the directory.
    let old_dir = tempdir::TempDir::new("hybrid-snapshot-rename-old").expect("old dir");
    copy_dir(frozen.path(), old_dir.path());
    std::fs::write(old_dir.path().join(&region_name), &old_region).expect("restore old region");

    // State 1: the new name made it to disk.
    let new_present = fixture.adopt(frozen);
    assert_eq!(new_present.table.stored_cut(), version(sequence + 1));
    // The two regions differ in the batch that wrote `roll_key`: the new region
    // names the put of this pass, the previous one an older put. Plain presence
    // would be true in both states.
    let new_batch = new_present
        .indexed_key(roll_key)
        .map(|position| position.batch);
    assert_eq!(
        new_batch,
        Some(version(sequence)),
        "the new region must serve the put it covered"
    );
    assert_ne!(
        indexed_old.as_ref().map(|position| position.batch),
        new_batch,
        "premise: the two regions name different batches for the key"
    );

    // State 2: the rename was lost, so the previous region is what reopens, and
    // the file only it references must still be there.
    let old_present = fixture.adopt(old_dir);
    assert_eq!(old_present.table.stored_cut(), version(previous + 1));
    assert_eq!(
        old_present.indexed_key(roll_key),
        indexed_old,
        "the previous region must still serve what it covered"
    );
}

/// `(shard count, has an L1 blob)` of the fixture's single cell.
fn cell_layout(table: &HybridTable) -> (usize, bool) {
    let shards = std::sync::atomic::AtomicUsize::new(0);
    let l1 = std::sync::atomic::AtomicBool::new(false);
    table.large_table().each_entry(|entry| {
        let levels = entry.levels();
        shards.fetch_max(levels.shards().len(), std::sync::atomic::Ordering::Relaxed);
        if levels.l1().is_some() {
            l1.store(true, std::sync::atomic::Ordering::Relaxed);
        }
    });
    (
        shards.load(std::sync::atomic::Ordering::Relaxed),
        l1.load(std::sync::atomic::Ordering::Relaxed),
    )
}

/// The native opt-in `index_auto_shard_threshold` decides the shape of a promoted
/// cell, and the shape never changes what a key resolves to. The native
/// fragment is 4 KiB and one key costs about 66 index bytes, so a cell of 55
/// keys (about 3.6 KiB) fits one fragment as one blob but is above a 1 KiB
/// threshold. With `None` the cell is one L1 blob (a larger one would abort the
/// flusher at the fragment size, which is why the fixture stops at 55); with
/// `Some(1024)` it flushes as shards. Both reload every position byte-exact,
/// and a second promotion (re-promotion, 15 more keys into the sharded cell)
/// keeps every earlier key.
///
/// Negative control (production mutation): `HybridIndexConfig::to_native_config`
/// or `from_native` passes `None` instead of the threshold: the `Some` arm finds
/// no shards and fails at "first promotion must produce shards".
#[test]
fn the_native_shard_threshold_controls_the_promoted_cell_and_no_key_is_lost() {
    let offset = |seed: u64| 1000 + seed * 64;
    for threshold in [None, Some(1024usize)] {
        let mut fixture = Fixture::with_config(
            "hybrid-snapshot-shard-option",
            HybridIndexConfig {
                snapshot_unload_threshold: 0,
                frag_size: 4096,
                wal_file_size: 4096,
                l0_max_entries: Some(8),
                index_auto_shard_threshold: threshold,
                ..HybridIndexConfig::default()
            },
        );
        let assert_every_key = |fixture: &Fixture, upto: u64, step: &str| {
            for seed in 1..=upto {
                let found = fixture
                    .indexed_key(seed as u8)
                    .unwrap_or_else(|| panic!("{threshold:?} {step}: key {seed} lost"));
                assert_eq!(
                    found.batch,
                    version(seed),
                    "{threshold:?} {step}: key {seed}"
                );
                assert_eq!(
                    found.address.offset,
                    offset(seed) + MEMBER_FRAME_SHIFT,
                    "{threshold:?} {step}: key {seed}"
                );
            }
        };

        for seed in 1..=40u64 {
            fixture.put_key(seed, seed as u8, offset(seed));
        }
        fixture.advance(41);
        let first = rebuild_control_region(&fixture.table, true, Some(version(41)), &[])
            .expect("first promotion");
        assert!(first.stored, "{threshold:?}: the pass stores a region");
        let (shards, l1) = cell_layout(&fixture.table);
        match threshold {
            None => assert!(l1 && shards == 0, "None: one L1 blob, got {shards} shards"),
            Some(_) => assert!(
                shards > 1,
                "first promotion must produce shards, got {shards}"
            ),
        }
        assert_every_key(&fixture, 40, "after the first promotion");
        fixture.reopen();
        assert_eq!(
            cell_layout(&fixture.table).0,
            shards,
            "{threshold:?}: reopen keeps the layout"
        );
        assert_every_key(&fixture, 40, "after the first reopen");

        for seed in 41..=55u64 {
            fixture.put_key(seed, seed as u8, offset(seed));
        }
        fixture.advance(56);
        rebuild_control_region(&fixture.table, true, Some(version(56)), &[]).expect("re-promotion");
        let (reshards, _) = cell_layout(&fixture.table);
        match threshold {
            None => assert_eq!(reshards, 0, "None never shards"),
            Some(_) => assert!(
                reshards >= shards,
                "re-promotion lost shards: {reshards} < {shards}"
            ),
        }
        assert_every_key(&fixture, 55, "after the re-promotion");
        fixture.reopen();
        assert_every_key(&fixture, 55, "after the second reopen");
    }
}
