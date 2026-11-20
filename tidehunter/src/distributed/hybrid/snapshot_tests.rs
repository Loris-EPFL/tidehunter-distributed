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
use crate::distributed::durability::DurabilityTier;
use crate::distributed::types::{BatchVersion, PhysicalValueAddress, RecordKind};
use crate::key_shape::{KeyShape, KeyType};
use crate::metrics::Metrics;
use std::sync::Arc;

fn shape() -> Arc<KeyShape> {
    Arc::new(KeyShape::new_single(4, 1, KeyType::uniform(1)))
}

fn version(sequence: u64) -> BatchVersion {
    BatchVersion { epoch: 1, sequence }
}

struct Fixture {
    dir: tempdir::TempDir,
    table: Arc<HybridTable>,
    cuts: CutSources,
    config: HybridIndexConfig,
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
        let dir = tempdir::TempDir::new(name).expect("scratch directory");
        let cuts = CutSources::new(1, DurabilityTier::Native);
        let table = Arc::new(
            HybridTable::open(dir.path(), shape(), &config, cuts.clone(), Metrics::new())
                .expect("open table"),
        );
        Self {
            dir,
            table,
            cuts,
            config,
        }
    }

    fn reopen(&mut self) {
        self.table = Arc::new(
            HybridTable::open(
                self.dir.path(),
                shape(),
                &self.config,
                self.cuts.clone(),
                Metrics::new(),
            )
            .expect("reopen table"),
        );
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
        let keyspace = shape().iter_ks().next().expect("one keyspace").id();
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
                offset: 8,
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
        let keyspace = shape().iter_ks().next().expect("one keyspace").id();
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
    let pass = rebuild_control_region(&fixture.table, false, None).expect("cadence pass");
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
    let pass = rebuild_control_region(&fixture.table, true, Some(version(6))).expect("snapshot");
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
    let pass = rebuild_control_region(&fixture.table, false, None).expect("cadence pass");
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
    let flushed = rebuild_control_region(&fixture.table, true, Some(version(5))).expect("flush");
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
                    rebuild_control_region(&fixture.table, true, Some(version(frontier)))
                } else {
                    rebuild_control_region(&fixture.table, false, None)
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
fn a_latch_below_the_stored_cut_fails_a_forced_pass_and_is_a_safe_cadence_noop() {
    // The checkpoint pin race: a reader latch ends up below the stored head. A
    // clean cell takes the loader's `last_processed()`, which a held latch
    // lowers, so the snapshot's cut falls below the stored one. The cadence pass
    // stores nothing (the previous region stays valid and byte-identical); a
    // forced pass cannot store at the stored cut, so it fails with the typed
    // invariant error rather than a silent no-op.
    let fixture = Fixture::eager("hybrid-snapshot-low-latch");
    fixture.put(3);
    fixture.advance(4);
    let baseline = rebuild_control_region(&fixture.table, false, None).expect("baseline");
    assert!(baseline.stored);
    assert_eq!(baseline.cut, version(4));
    let fronts = fixture.table.stored_fronts().to_vec();

    let latch = fixture.cuts.pin_cut(version(2));
    let cadence = rebuild_control_region(&fixture.table, false, None).expect("cadence pass");
    assert!(!cadence.stored, "a lowered cut adds nothing to store");
    assert_eq!(cadence.cut, version(4));
    let error = rebuild_control_region(&fixture.table, true, Some(version(4)))
        .err()
        .expect("a forced pass cannot store below the stored cut");
    assert!(
        error.to_string().contains("below the stored region cut"),
        "unexpected error: {error}"
    );
    assert_eq!(fixture.table.stored_cut(), version(4));
    assert_eq!(fixture.table.stored_fronts(), fronts.as_slice());
    drop(latch);
}

#[test]
fn a_pass_with_no_advance_keeps_the_region_identical() {
    // A cut that did not move must not rewrite the region: the stored cut and
    // every per-stream front stay exactly as the previous store left them.
    let fixture = Fixture::new("hybrid-snapshot-noop");
    fixture.put(6);
    fixture.advance(7);
    let first = rebuild_control_region(&fixture.table, true, Some(version(7))).expect("first");
    assert!(first.stored);
    let fronts = fixture.table.stored_fronts().to_vec();
    let deferrals = fixture.table.snapshot_latch_deferrals();
    // Nothing advanced and every cell is clean, so the snapshot's frontier is
    // the stored cut again.
    let again = rebuild_control_region(&fixture.table, false, None).expect("no-op snapshot");
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
fn two_deferrals_under_a_held_latch_change_nothing_and_are_counted_twice() {
    // A latch pinned below the stored head turns every cadence pass into a
    // deferral. Each one must leave the region file and the stored fronts
    // byte-identical, and each must be counted, so a latch stuck below the head
    // is visible instead of silently holding every later snapshot back.
    let fixture = Fixture::eager("hybrid-snapshot-deferrals");
    fixture.put(3);
    fixture.advance(4);
    let baseline = rebuild_control_region(&fixture.table, false, None).expect("baseline");
    assert!(baseline.stored);
    let region_file = |fixture: &Fixture| {
        std::fs::read(fixture.table.region().lock().path()).expect("region file")
    };
    let bytes = region_file(&fixture);
    let fronts = fixture.table.stored_fronts().to_vec();
    let before = fixture.table.snapshot_latch_deferrals();

    let latch = fixture.cuts.pin_cut(version(2));
    for _ in 0..2 {
        let pass = rebuild_control_region(&fixture.table, false, None).expect("deferred pass");
        assert!(!pass.stored);
        assert_eq!(pass.cut, baseline.cut);
    }
    assert_eq!(
        fixture.table.snapshot_latch_deferrals(),
        before + 2,
        "each deferral must be counted"
    );
    assert_eq!(
        region_file(&fixture),
        bytes,
        "the region file must not change"
    );
    assert_eq!(fixture.table.stored_fronts(), fronts.as_slice());
    drop(latch);
}

#[test]
fn a_forced_store_re_covers_the_same_cut_and_flushes_every_dirty_cell() {
    let mut fixture = Fixture::new("hybrid-snapshot-forced");
    fixture.put(2);
    fixture.advance(3);
    let first = rebuild_control_region(&fixture.table, true, Some(version(3))).expect("first");
    assert!(first.stored && !first.forced);
    assert_eq!(first.cut, version(3));
    assert_eq!(
        fixture.indexed(2).map(|position| position.address.offset),
        Some(128)
    );

    // Dirty a second key below the stored cut — the state a relocation re-point
    // leaves behind. Its byte age is zero, so only a real force-flush promotes
    // it. Nothing advanced, so the forced store lands at the same cut.
    fixture.put_key(2, 9, 300);
    let forced =
        rebuild_control_region(&fixture.table, true, Some(first.cut)).expect("forced snapshot");
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
        Some(300)
    );
    // The re-covered region persists it: a reopen loads the new cell base.
    fixture.reopen();
    assert_eq!(
        fixture
            .indexed_key(9)
            .map(|position| position.address.offset),
        Some(300),
        "the forced region must persist the re-pointed cell"
    );

    // A forced pass whose cut did advance is an ordinary store.
    fixture.put(4);
    fixture.advance(5);
    let advanced =
        rebuild_control_region(&fixture.table, true, Some(version(5))).expect("advanced store");
    assert!(advanced.stored && !advanced.forced);
    assert_eq!(advanced.cut, version(5));
    assert_eq!(advanced.promotion_cut, version(5));

    // A frontier above the stored head is refused before any work.
    let error = rebuild_control_region(&fixture.table, true, Some(version(9)))
        .err()
        .expect("a clamp above the head must fail");
    assert!(
        error
            .to_string()
            .contains("forced promotion cut above the stored target")
    );
    // A forced pass without a frontier is a caller bug, refused the same way.
    let error = rebuild_control_region(&fixture.table, true, None)
        .err()
        .expect("a forced store needs a frontier");
    assert!(error.to_string().contains("requires a promotion frontier"));
}

#[test]
fn a_completed_pass_survives_a_reopen() {
    let mut fixture = Fixture::new("hybrid-snapshot-reopen");
    fixture.put(4);
    fixture.advance(5);
    let pass = rebuild_control_region(&fixture.table, true, Some(version(5))).expect("snapshot");
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
    let held = rebuild_control_region(&fixture.table, true, Some(version(3))).expect("held pass");
    assert!(
        held.cut <= version(3),
        "a held latch must keep the region cut below the unstored extent"
    );
    assert!(fixture.table.stored_cut() <= version(3));

    // Releasing the latch lets the next pass flush through the head: the cut
    // follows, and the put at v4 is in a blob a reopen can serve.
    drop(latch);
    let released =
        rebuild_control_region(&fixture.table, true, Some(version(6))).expect("released pass");
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
                shape(),
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
    let fixture = Fixture::with_config(
        "hybrid-snapshot-rename-cut",
        HybridIndexConfig {
            snapshot_unload_threshold: 0,
            frag_size: 4096,
            wal_file_size: 4096,
            ..HybridIndexConfig::default()
        },
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

    let mut previous = 1;
    fixture.put(previous);
    fixture.advance(previous + 1);
    let first = rebuild_control_region(&fixture.table, false, None).expect("first pass");
    assert!(first.stored);

    let mut crash = None;
    for _ in 0..120 {
        let sequence = previous + 2;
        fixture.put(sequence);
        fixture.advance(sequence + 1);
        let old_region = std::fs::read(&region_path).expect("previous region");
        let indexed_old = fixture.indexed(previous);
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
        let pass = rebuild_control_region(&fixture.table, false, None).expect("pass");
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
        if removed.is_disjoint(&depends_on) {
            previous = sequence;
            continue;
        }
        crash = Some((frozen, old_region, indexed_old, previous, sequence));
        break;
    }
    let (frozen, old_region, indexed_old, previous, sequence) = crash.expect(
        "no pass unlinked an index file the previous region references: the index WAL never rolled",
    );
    assert!(
        indexed_old.is_some(),
        "the previous region must cover its put, or state 2 would pass on None == None"
    );
    // The frozen directory holds a file the finished pass unlinked afterwards.
    assert!(
        !files_under(frozen.path()).is_subset(&files_under(fixture.dir.path())),
        "the frozen directory must still hold a file the pass unlinked afterwards"
    );

    // Build the second state from the frozen bytes before anything opens them:
    // opening a table may itself touch the directory.
    let old_dir = tempdir::TempDir::new("hybrid-snapshot-rename-old").expect("old dir");
    copy_dir(frozen.path(), old_dir.path());
    std::fs::write(old_dir.path().join(&region_name), &old_region).expect("restore old region");

    // State 1: the new name made it to disk.
    let new_present = fixture.adopt(frozen);
    assert_eq!(new_present.table.stored_cut(), version(sequence + 1));
    assert!(
        new_present.indexed(sequence).is_some(),
        "the new region must serve the put it covered"
    );

    // State 2: the rename was lost, so the previous region is what reopens, and
    // the file only it references must still be there.
    let old_present = fixture.adopt(old_dir);
    assert_eq!(old_present.table.stored_cut(), version(previous + 1));
    assert_eq!(
        old_present.indexed(previous),
        indexed_old,
        "the previous region must still serve what it covered"
    );
}
