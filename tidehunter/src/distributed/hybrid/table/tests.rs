//! Table-level behaviour that needs no owner: the index half of put → get →
//! reopen, cut visibility, tombstones, the monotone stored target and the
//! relocation clamp. Owner drain and value reads are covered where an owner
//! exists (the leased fixture tests).

use super::{HybridTable, PublishedMutation, RelocationApply};
use crate::db::WalEntry;
use crate::distributed::codec::RecordKind;
use crate::distributed::durability::DurabilityTier;
use crate::distributed::hybrid::config::HybridIndexConfig;
use crate::distributed::hybrid::loader::CutSources;
use crate::distributed::hybrid::position::VersionPosition;
use crate::distributed::types::{
    BatchVersion, MemberChecksum, PhysicalValueAddress, RecordDescriptor,
};
use crate::index::index_format::{HEADER_SIZE, IndexFormat};
use crate::index::index_table::IndexTable;
use crate::key_shape::{KeyShape, KeyShapeBuilder, KeySpace, KeySpaceConfig, KeyType};
use crate::large_table::Loader;
use crate::metrics::Metrics;
use crate::wal::PreparedWalWrite;
use crate::wal::position::IndexPosition;
use minibytes::Bytes;
use parking_lot::Mutex;
use std::sync::Arc;

mod anchor;
mod boundary;
mod layout;
mod shard_preload;

fn shape() -> Arc<KeyShape> {
    Arc::new(KeyShape::new_single(4, 1, KeyType::uniform(1)))
}

fn version(sequence: u64) -> BatchVersion {
    BatchVersion { epoch: 1, sequence }
}

/// Offset of the fixture member inside its batch payload (the descriptor's
/// `offset`, which excludes the outer native CRC header).
const DESCRIPTOR_OFFSET: u32 = 8;
/// Native CRC frame header in front of every batch payload.
const CRC_HEADER: u64 = 8;

/// Where `VersionPosition::member` puts the member frame of a batch at
/// `batch_offset`: the address a read fetches, not the batch address. The
/// literal layout is pinned by `the_fixture_member_address_is_the_member_frame`
/// and `position/tests.rs`.
fn member_offset(batch_offset: u64) -> u64 {
    batch_offset + CRC_HEADER + u64::from(DESCRIPTOR_OFFSET)
}

fn position(sequence: u64, offset: u64) -> VersionPosition {
    VersionPosition::member(
        version(sequence),
        PhysicalValueAddress {
            partition: 0,
            segment_generation: 1,
            offset,
            len: 64,
        },
        0,
        RecordDescriptor {
            operation_index: 0,
            offset: DESCRIPTOR_OFFSET,
            len: 24,
            keyspace: 0,
            kind: RecordKind::Put,
            key_len: 4,
            value_offset: 14,
            value_len: 2,
            checksum: MemberChecksum(0),
        },
        crc32fast::hash(b"value"),
    )
}

struct Fixture {
    dir: tempdir::TempDir,
    table: HybridTable,
    cuts: CutSources,
}

impl Fixture {
    fn new(name: &str) -> Self {
        Self::with_config(name, HybridIndexConfig::default())
    }

    fn with_config(name: &str, config: HybridIndexConfig) -> Self {
        let dir = tempdir::TempDir::new(name).expect("scratch directory");
        let cuts = CutSources::new(1, DurabilityTier::Native);
        let table = HybridTable::open(dir.path(), shape(), &config, cuts.clone(), Metrics::new())
            .expect("open table");
        Self { dir, table, cuts }
    }

    fn reopen(&mut self) {
        self.cuts = CutSources::new(2, DurabilityTier::Native);
        self.table = open(&self.dir, &self.cuts);
    }

    fn ks(&self) -> KeySpace {
        self.table
            .shape()
            .iter_ks()
            .next()
            .expect("one keyspace")
            .id()
    }

    fn publish(&self, kind: RecordKind, key: u8, sequence: u64, offset: u64) {
        self.table
            .insert_published(
                &[PublishedMutation {
                    keyspace: self.ks(),
                    key: &[key; 4],
                    kind,
                    position: position(sequence, offset),
                }],
                64,
            )
            .expect("publish");
    }

    fn get(&self, key: u8, cut: BatchVersion) -> Option<VersionPosition> {
        self.table.get(self.ks(), &[key; 4], cut).expect("lookup")
    }
}

fn open(dir: &tempdir::TempDir, cuts: &CutSources) -> HybridTable {
    HybridTable::open(
        dir.path(),
        shape(),
        &HybridIndexConfig::default(),
        cuts.clone(),
        Metrics::new(),
    )
    .expect("open table")
}

#[test]
fn a_published_put_is_visible_only_above_its_version() {
    let fixture = Fixture::new("hybrid-table-visible");
    fixture.publish(RecordKind::Put, 1, 3, 128);
    fixture.table.advance_applied(version(4));
    assert_eq!(
        fixture.get(1, version(4)).map(|found| found.address.offset),
        Some(member_offset(128)),
        "a cut above the version sees the put",
    );
    assert!(
        fixture.get(1, version(3)).is_none(),
        "a cut is exclusive: the version itself is not yet below it",
    );
    assert!(
        fixture.get(2, version(4)).is_none(),
        "an absent key has no position"
    );
}

#[test]
fn a_tombstone_hides_the_older_put_from_later_cuts_only() {
    let fixture = Fixture::new("hybrid-table-delete");
    fixture.publish(RecordKind::Put, 1, 2, 100);
    fixture.publish(RecordKind::Delete, 1, 5, 200);
    fixture.table.advance_applied(version(6));
    assert!(
        fixture.get(1, version(6)).is_none(),
        "deleted at the live cut"
    );
    assert_eq!(
        fixture.get(1, version(4)).map(|found| found.address.offset),
        Some(member_offset(100)),
        "a cut below the delete still reads the put",
    );
}

#[test]
fn a_later_write_to_the_same_key_wins_by_version() {
    let fixture = Fixture::new("hybrid-table-overwrite");
    fixture.publish(RecordKind::Put, 1, 2, 100);
    fixture.publish(RecordKind::Put, 1, 4, 300);
    fixture.table.advance_applied(version(5));
    assert_eq!(
        fixture.get(1, version(5)).map(|found| found.address.offset),
        Some(member_offset(300)),
    );
    assert_eq!(
        fixture.get(1, version(3)).map(|found| found.address.offset),
        Some(member_offset(100)),
    );
}

/// A relocation update prepared before an overwrite or another re-point must
/// not replace the new head. The last-chunk drop must not remove it either.
#[test]
fn relocation_compare_and_set_preserves_a_newer_head() {
    for overwrite in [false, true] {
        let fixture = Fixture::new("hybrid-relocation-stale");
        let ks = fixture.ks();
        let key = [1; 4];
        let cell = fixture.table.shape().ks(ks).cell_id(&key);
        let reduced =
            Bytes::copy_from_slice(fixture.table.shape().ks(ks).reduce_key(&key).as_ref());
        fixture.publish(RecordKind::Put, 1, 2, 100);
        fixture.table.advance_applied(version(3));
        let old = fixture.get(1, version(3)).expect("original position");
        let first_copy = old.moved_to(position(2, 200).address);
        let stale_copy = old.moved_to(position(2, 900).address);
        if overwrite {
            fixture.publish(RecordKind::Put, 1, 4, 900);
            fixture.table.advance_applied(version(5));
        } else {
            fixture
                .table
                .relocation_apply(
                    ks,
                    &cell,
                    RelocationApply::Repoint(&[(reduced.clone(), old, first_copy)]),
                    &[(0, 1, 500)],
                )
                .expect("first re-point");
        }
        let current = fixture.get(1, version(6)).expect("new head");
        fixture
            .table
            .relocation_apply(
                ks,
                &cell,
                RelocationApply::Repoint(&[(reduced, old, stale_copy)]),
                &[(0, 1, 500)],
            )
            .expect("stale re-point");
        let after = fixture.get(1, version(6)).expect("head survives");
        assert!(
            after.same_physical(&current),
            "overwrite={overwrite}: {after:?} != {current:?}"
        );
    }
}

#[test]
fn advance_applied_moves_the_applied_and_publication_cuts_together() {
    let fixture = Fixture::new("hybrid-table-applied");
    let start = fixture.table.current();
    fixture.table.advance_applied(version(7));
    assert_eq!(fixture.table.current(), version(7));
    assert_eq!(fixture.table.cuts().applied(), version(7));
    assert!(version(7) > start);
    // Monotone: an older value never lowers either cut.
    fixture.table.advance_applied(version(3));
    assert_eq!(fixture.table.current(), version(7));
}

#[test]
fn the_stored_target_never_moves_back_under_a_held_checkpoint() {
    let fixture = Fixture::new("hybrid-table-target");
    fixture.publish(RecordKind::Put, 1, 2, 100);
    fixture.table.advance_applied(version(8));
    fixture.cuts.set_durable(version(8));
    assert_eq!(fixture.table.stored_target(), version(8));
    let (cut, latch) = fixture.table.pin_checkpoint();
    assert_eq!(cut, version(8), "the pin samples the publication watermark");
    fixture.table.advance_applied(version(10));
    fixture.cuts.set_durable(version(10));
    assert_eq!(
        fixture.table.last_processed(),
        version(8),
        "a held latch clamps the flush frontier",
    );
    assert_eq!(
        fixture.table.stored_target(),
        version(8),
        "the target is the clamp while the checkpoint is held",
    );
    drop(latch);
    assert_eq!(fixture.table.stored_target(), version(10));
}

#[test]
fn a_snapshot_stores_once_and_is_a_no_op_when_nothing_moved() {
    let fixture = Fixture::new("hybrid-table-snapshot");
    fixture.publish(RecordKind::Put, 1, 2, 100);
    fixture.table.advance_applied(version(3));
    fixture.cuts.set_durable(version(3));
    let first = fixture.table.snapshot().expect("first snapshot");
    assert!(first.stored && !first.forced);
    // The put's cell is dirty and unflushed: the cadence pass does not flush a
    // byte-fresh cell, so the region names the cell's anchor instead of the
    // head `3`, and recovery replays the put (native `replay_from`,
    // snapshot.rs rule 3). The anchor is `min(first batch, last_processed at
    // the first insert)` (S4e `anchor_cut`): the put was inserted while nothing
    // was applied yet, so it is the epoch start `0`, below the put's batch `2`.
    // A forced pass names the head, see
    // `a_flushed_cell_lets_the_stored_cut_reach_the_head`.
    assert_eq!(first.cut, version(0));
    assert_eq!(first.promotion_cut, first.cut);
    assert_eq!(fixture.table.stored_cut(), version(0));
    let second = fixture.table.snapshot().expect("second snapshot");
    assert!(!second.stored, "an unchanged head stores nothing");
    assert_eq!(second.cut, first.cut);
}

#[test]
fn a_flushed_cell_lets_the_stored_cut_reach_the_head() {
    let fixture = Fixture::new("hybrid-table-flushed-head");
    fixture.publish(RecordKind::Put, 1, 2, 100);
    fixture.table.advance_applied(version(3));
    fixture.cuts.set_durable(version(3));
    let forced = fixture
        .table
        .snapshot_for_relocation(version(3))
        .expect("forced pass flushes the dirty cell");
    assert!(forced.stored);
    assert_eq!(forced.cut, version(3));
    assert_eq!(fixture.table.stored_cut(), version(3));
    let again = fixture.table.snapshot().expect("cadence pass");
    assert!(!again.stored, "the head is already stored");
}

#[test]
fn a_reopen_reads_what_the_stored_region_names() {
    let mut fixture = Fixture::new("hybrid-table-reopen");
    fixture.publish(RecordKind::Put, 1, 2, 100);
    fixture.table.advance_applied(version(3));
    fixture.cuts.set_durable(version(3));
    // A forced pass flushes every dirty cell; the cadence pass would leave this
    // freshly written cell in memory (it is not byte-stale), and with no owner to
    // replay from, only a flushed cell survives a reopen.
    let stored = fixture
        .table
        .snapshot_for_relocation(version(3))
        .expect("forced store");
    assert!(stored.stored);
    fixture.reopen();
    // A new open is a new generation whose fronts start at `{2, 0}`, above every
    // earlier-generation batch (A17), so the stored put is below any cut taken
    // in the new epoch.
    let above = BatchVersion {
        epoch: 2,
        sequence: 1,
    };
    assert_eq!(
        fixture.get(1, above).map(|found| found.address.offset),
        Some(member_offset(100)),
    );
    assert_eq!(fixture.table.stored_cut(), version(3));
}

#[test]
fn the_first_snapshot_after_a_reopen_advances_to_the_new_epoch() {
    let mut fixture = Fixture::new("hybrid-table-first-snapshot");
    fixture.publish(RecordKind::Put, 1, 2, 100);
    fixture.table.advance_applied(version(3));
    fixture.cuts.set_durable(version(3));
    fixture.table.snapshot().expect("snapshot");
    fixture.reopen();
    // Zero post-recovery writes: the fronts sit at `{2, 0}`, above the stored
    // head, so one store advances the head and flushes whatever replay applied.
    assert_eq!(
        fixture.table.last_processed(),
        BatchVersion {
            epoch: 2,
            sequence: 0
        }
    );
    let pass = fixture
        .table
        .snapshot()
        .expect("first snapshot after reopen");
    assert!(pass.stored);
    assert_eq!(
        pass.cut,
        BatchVersion {
            epoch: 2,
            sequence: 0
        }
    );
}

/// The flusher's L1 clean-up drops every entry below `Loader::min_position`.
/// A hybrid index entry is never dead by version, so the floor must be NONE:
/// a floor at the serving epoch's first version (the earlier default) deleted
/// every earlier-epoch entry after a reopen.
#[test]
fn a_reopened_epoch_keeps_earlier_epoch_entries_through_the_l1_clean_up() {
    let mut fixture = Fixture::new("hybrid-table-min-position");
    fixture.publish(RecordKind::Put, 1, 2, 100);
    fixture.reopen();
    let floor = Loader::min_position(&*fixture.table);
    let earlier_epoch = position(2, 100);
    assert!(!earlier_epoch.below(floor), "{floor:?} drops live entries");

    // Exactly what the flusher runs over an L1 blob.
    let mut blob = IndexTable::<VersionPosition>::default();
    blob.insert(vec![1u8; 4].into(), earlier_epoch);
    blob.retain_above_position(floor);
    assert_eq!(blob.len(), 1, "the clean-up dropped an earlier-epoch entry");

    // Negative control: a floor at the new epoch's first version is above
    // every earlier-epoch position and empties the blob.
    let old_default = BatchVersion {
        epoch: 2,
        sequence: 0,
    };
    assert!(earlier_epoch.below(old_default));
    blob.retain_above_position(old_default);
    assert_eq!(blob.len(), 0);
}

/// What a flusher samples, through production code: `min(durable, applied,
/// lowest held latch)`.
fn flusher_frontier(fixture: &Fixture) -> BatchVersion {
    fixture.table.last_processed()
}

/// A fixture with one published put, the watermark at `3` and a pin whose hook
/// runs where concurrent writers and a flusher would act: `applied` and
/// `durable` move to `9` and `window` observes the table.
fn pin_with_window<T: Send + 'static>(
    name: &str,
    naive: bool,
    window: impl Fn(&HybridTable) -> T + Send + Sync + 'static,
) -> (Fixture, BatchVersion, super::CutLatch, T) {
    let fixture = Fixture::new(name);
    fixture.publish(RecordKind::Put, 1, 2, 100);
    fixture.table.advance_applied(version(3));
    fixture.cuts.set_durable(version(3));
    if naive {
        fixture.table.set_naive_pin();
    }
    let seen = Arc::new(Mutex::new(None));
    let cuts = fixture.cuts.clone();
    let table = fixture.table.clone();
    let observed = Arc::clone(&seen);
    fixture.table.set_after_pin_sample(Box::new(move || {
        cuts.set_applied(version(9));
        cuts.set_durable(version(9));
        *observed.lock() = Some(window(&table));
    }));
    let (cut, latch) = fixture.table.pin_checkpoint();
    let observed = seen.lock().take().expect("the hook ran");
    (fixture, cut, latch, observed)
}

#[test]
fn a_flusher_between_the_sample_and_the_latch_cannot_promote_past_the_cut() {
    // Writers keep publishing between the publication sample and the real latch:
    // `applied` and `durable` move past the sample, and a flusher computes its
    // frontier right there.
    let (fixture, cut, latch, frontier) =
        pin_with_window("hybrid-table-pin-race", false, |table: &HybridTable| {
            table.last_processed()
        });
    assert_eq!(
        cut,
        version(3),
        "the sample is the watermark before the writers moved"
    );
    assert!(
        frontier <= cut,
        "a frontier computed in the window must not pass the cut: {frontier:?} > {cut:?}",
    );
    assert_eq!(
        flusher_frontier(&fixture),
        cut,
        "once latched, the frontier is clamped at the cut",
    );
    drop(latch);
}

/// Negative control, through production code: the naive order (sample, then
/// latch) has the window the low latch closes, and the flusher's frontier
/// passes the cut in it.
#[test]
fn the_naive_sample_then_latch_pin_lets_a_flusher_pass_the_cut() {
    let (_fixture, cut, _latch, frontier) =
        pin_with_window("hybrid-table-pin-naive", true, |table: &HybridTable| {
            table.last_processed()
        });
    assert!(
        frontier > cut,
        "without the low latch the frontier passes the cut: {frontier:?}",
    );
}

#[test]
fn a_cadence_or_forced_pass_inside_the_none_window_stores_nothing_past_the_cut() {
    // Both passes run in the window with writers already past the sample.
    let pass = |table: &HybridTable| {
        let cadence = table.snapshot().expect("cadence pass");
        // A relocation scan cut above the clamp: refused while the NONE latch
        // holds the clamp at NONE.
        let forced = table
            .snapshot_for_relocation(version(3))
            .expect("forced pass");
        (cadence, forced, table.stored_cut())
    };
    let (_fixture, cut, _latch, (cadence, forced, stored)) =
        pin_with_window("hybrid-table-pin-passes", false, pass);
    assert!(!cadence.stored && !forced.stored, "{cadence:?} {forced:?}");
    assert!(
        forced.promotion_cut <= cut,
        "the forced pass's flush clamp passed the cut: {:?}",
        forced.promotion_cut
    );
    assert!(
        stored <= cut,
        "a region was stored past the pin: {stored:?} > {cut:?}"
    );
}

#[test]
fn the_same_passes_with_the_naive_pin_store_past_the_cut() {
    let pass = |table: &HybridTable| {
        let cadence = table.snapshot().expect("cadence pass");
        let forced = table
            .snapshot_for_relocation(version(3))
            .expect("forced pass");
        (cadence, forced, table.stored_cut())
    };
    let (_fixture, cut, _latch, (cadence, forced, stored)) =
        pin_with_window("hybrid-table-pin-passes-naive", true, pass);
    assert!(
        cadence.stored || forced.stored,
        "the control must reach a store: {cadence:?} {forced:?}"
    );
    assert!(
        stored > cut || forced.promotion_cut > cut,
        "without the low latch a pass promotes past the cut"
    );
}

#[test]
fn a_forced_relocation_store_is_refused_above_the_flush_clamp() {
    let fixture = Fixture::new("hybrid-table-relocation");
    fixture.publish(RecordKind::Put, 1, 2, 100);
    fixture.table.advance_applied(version(4));
    fixture.cuts.set_durable(version(4));
    // The baseline flushes the dirty cell, so the stored region names the head
    // itself and the later forced pass re-covers the same cut. (A baseline taken
    // with the cadence pass would leave the cell dirty, name its frontier `2`,
    // and the forced pass below would then move the cut: an ordinary store, see
    // `a_forced_pass_whose_cut_moved_is_an_ordinary_store`.)
    let baseline = fixture
        .table
        .snapshot_for_relocation(version(4))
        .expect("baseline");
    assert_eq!(baseline.cut, version(4));
    let (_, latch) = fixture.table.pin_checkpoint();
    fixture.table.advance_applied(version(9));
    fixture.cuts.set_durable(version(9));
    // The latch holds the clamp at 4: a scan cut of 6 cannot be covered, so the
    // pass defers instead of promoting past the held cut.
    let deferred = fixture
        .table
        .snapshot_for_relocation(version(6))
        .expect("deferral is not an error");
    assert!(!deferred.stored && !deferred.forced);
    assert_eq!(deferred.promotion_cut, version(4));
    let covered = fixture
        .table
        .snapshot_for_relocation(version(4))
        .expect("a scan cut at the clamp is covered");
    assert!(covered.stored && covered.forced);
    assert!(covered.promotion_cut >= version(4));
    drop(latch);
}

/// A24: a forced pass whose cut moved is an ordinary store; `forced` is set
/// only when the same cut is re-covered.
#[test]
fn a_forced_pass_whose_cut_moved_is_an_ordinary_store() {
    let fixture = Fixture::new("hybrid-table-forced-moved");
    fixture.publish(RecordKind::Put, 1, 2, 100);
    fixture.table.advance_applied(version(4));
    fixture.cuts.set_durable(version(4));
    // The cadence pass leaves the fresh cell dirty: the region names its
    // anchor, which is bounded by the loader's `last_processed` at the first
    // insert (S4e `anchor_cut`: `min(first batch, bound)`), here the start of
    // the epoch `0`, below the put's batch `2`.
    let baseline = fixture.table.snapshot().expect("baseline");
    assert_eq!(baseline.cut, version(0));
    let moved = fixture
        .table
        .snapshot_for_relocation(version(4))
        .expect("forced pass");
    assert!(moved.stored, "the cut moved from 2 to the flushed head");
    assert!(!moved.forced, "a moved cut is an ordinary store");
    assert_eq!(moved.cut, version(4));
    assert_eq!(moved.promotion_cut, moved.cut);
}

/// Layout pin for the fixture: the stored address is the member frame, i.e. the
/// batch offset plus the native CRC header plus the descriptor offset, never the
/// batch address (EFFECTIVE §1).
#[test]
fn the_fixture_member_address_is_the_member_frame() {
    let member = position(3, 128).address;
    assert_eq!(member.offset, 144, "128 + 8 (CRC header) + 8 (descriptor)");
    assert_eq!(member.len, 24, "the descriptor length, not the batch's 64");
    assert_eq!((member.partition, member.segment_generation), (0, 1));
}

#[test]
fn a_value_cache_is_refused_because_values_live_on_their_owners() {
    let dir = tempdir::TempDir::new("hybrid-table-cache").expect("scratch directory");
    let cached = Arc::new(KeyShape::new_single_config(
        4,
        1,
        KeyType::uniform(1),
        KeySpaceConfig::new().with_value_cache_size(16),
    ));
    let error = HybridTable::open(
        dir.path(),
        cached,
        &HybridIndexConfig::default(),
        CutSources::new(1, DurabilityTier::Native),
        Metrics::new(),
    )
    .err()
    .expect("a keyspace with a value cache cannot be hybrid");
    assert!(
        error.to_string().contains("value_cache_size = 0"),
        "{error}"
    );
}

#[test]
fn a_region_naming_an_unconfigured_partition_is_refused() {
    let dir = tempdir::TempDir::new("hybrid-table-partitions").expect("scratch directory");
    let cuts = CutSources::new(1, DurabilityTier::Native);
    let first = HybridTable::open_with_partitions(
        dir.path(),
        shape(),
        &HybridIndexConfig::default(),
        cuts.clone(),
        Metrics::new(),
        &[0, 1],
    )
    .expect("open table");
    // A first open has stored nothing, so any configuration is acceptable.
    first
        .validate_partitions(&[0, 1])
        .expect("no stored fronts");
    assert!(first.opened_fronts().is_empty());
    first.advance_applied(version(2));
    cuts.set_durable(version(2));
    first.snapshot().expect("snapshot stores the two streams");
    drop(first);
    let reopened = HybridTable::open_with_partitions(
        dir.path(),
        shape(),
        &HybridIndexConfig::default(),
        CutSources::new(2, DurabilityTier::Native),
        Metrics::new(),
        &[0, 1],
    )
    .expect("reopen table");
    assert_eq!(reopened.opened_fronts().len(), 2);
    reopened
        .validate_partitions(&[0, 1])
        .expect("the stored partitions are configured");
    let error = reopened
        .validate_partitions(&[1])
        .expect_err("partition 0 is no longer configured");
    assert!(error.to_string().contains("partition 0"), "{error}");
}

#[test]
fn the_stream_fronts_follow_the_partitions_the_table_was_opened_with() {
    let dir = tempdir::TempDir::new("hybrid-table-streams").expect("scratch directory");
    let table = HybridTable::open_with_partitions(
        dir.path(),
        shape(),
        &HybridIndexConfig::default(),
        CutSources::new(1, DurabilityTier::Native),
        Metrics::new(),
        &[7, 3],
    )
    .expect("open table");
    let fronts = table.stream_fronts();
    assert_eq!(
        fronts
            .iter()
            .map(|front| front.partition)
            .collect::<Vec<_>>(),
        vec![7, 3],
        "the dense stream order is the configured partition order",
    );
    // Nothing allocated: each stream reports its (zero) tail, never a stale front.
    assert!(fronts.iter().all(|front| front.replay_from == 0));
}

/// Single-flight of the snapshot pass: every entry point (`snapshot`,
/// `snapshot_for_relocation`; the flush, the shutdown snapshot, the cadence
/// thread and the named force rebuild all call one of the two) takes
/// `TableInner::pass` first. A parked pass holds it, and the next pass runs
/// after it on the fresh stored cut instead of racing it.
///
/// Negative control (mutation run): remove the `pass_lock()` use in
/// `rebuild_control_region` and the `try_lock` assertion fails. That assertion
/// is the deterministic control; the second pass below is a smoke test of the
/// outcome (a thread that only starts after the first pass resumed would also
/// succeed without the lock), and without the lock a pass that overlaps fails
/// with "last_position regressed".
#[test]
fn a_parked_pass_holds_the_single_flight_lock_and_the_next_pass_runs_after_it() {
    let fixture = Fixture::new("hybrid-table-single-flight");
    fixture.publish(RecordKind::Put, 1, 2, 100);
    fixture.table.advance_applied(version(3));
    fixture.cuts.set_durable(version(3));
    let (entered, resume) = fixture.table.pause_next_pass();
    std::thread::scope(|scope| {
        let first = scope.spawn(|| fixture.table.snapshot());
        entered
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("the first pass parked after choosing its head");
        assert!(
            fixture.table.pass_lock().try_lock().is_none(),
            "a pass in flight holds the single-flight lock"
        );
        let second = scope.spawn(|| fixture.table.snapshot_for_relocation(version(3)));
        fixture.table.advance_applied(version(5));
        fixture.cuts.set_durable(version(5));
        resume.send(()).expect("resume the first pass");
        let first = first.join().expect("first thread").expect("first pass");
        let second = second.join().expect("second thread").expect("second pass");
        assert!(first.stored);
        assert!(
            second.cut >= first.cut,
            "the second pass starts from the first one's stored cut: {first:?} {second:?}"
        );
        assert_eq!(fixture.table.stored_cut(), second.cut);
    });
}

/// Passes of every kind in parallel with publication: none fails, none
/// regresses the stored cut. (A smoke test of the lock; the parked-pass test
/// above is the deterministic one.)
#[test]
fn concurrent_forced_and_cadence_passes_all_complete() {
    let fixture = Fixture::new("hybrid-table-pass-storm");
    let done = std::sync::atomic::AtomicBool::new(false);
    std::thread::scope(|scope| {
        let passes: Vec<_> = (0..4u8)
            .map(|kind| {
                let fixture = &fixture;
                let done = &done;
                scope.spawn(move || {
                    let mut results = Vec::new();
                    while !done.load(std::sync::atomic::Ordering::Acquire) {
                        let pass = if kind % 2 == 0 {
                            fixture.table.snapshot()
                        } else {
                            let q = fixture.table.last_processed();
                            fixture.table.snapshot_for_relocation(q)
                        };
                        results.push(pass);
                    }
                    results
                })
            })
            .collect();
        for sequence in 2..60u64 {
            fixture.publish(
                RecordKind::Put,
                (sequence % 200) as u8,
                sequence,
                sequence * 128,
            );
            fixture.table.advance_applied(version(sequence + 1));
            fixture.cuts.set_durable(version(sequence + 1));
        }
        done.store(true, std::sync::atomic::Ordering::Release);
        for pass in passes {
            for result in pass.join().expect("pass thread") {
                result.expect("every pass completes with its own store or a no-op");
            }
        }
    });
    let stored = fixture.table.stored_cut();
    assert!(stored <= fixture.table.current(), "{stored:?}");
}

/// Invariant I(cell): `cell.last_processed <= min version of the cell's
/// uncovered overlay entries`. Publication inserts in durable-arrival order, so
/// v300 can reach the cell before v200 (`publish_work` inserts a durable slot as
/// soon as its same-key predecessors resolved). A dirty cell anchored at the
/// FIRST insert (300) would let a snapshot at head > 300 name replay_from 300
/// while v200 is only in the overlay, and a crash would lose it. The anchor is
/// the minimum version inserted, so the region cut stays at or below 200.
/// Negative control: anchor at `v.frontier()` (the S1S2 default) and the cut is
/// above 200.
#[test]
fn a_lower_version_inserted_after_the_first_keeps_the_cell_anchored_at_it() {
    let fixture = Fixture::new("hybrid-table-anchor");
    fixture.publish(RecordKind::Put, 1, 300, 1000);
    fixture.publish(RecordKind::Put, 2, 200, 2000);
    fixture.table.advance_applied(version(301));
    fixture.cuts.set_durable(version(301));
    let stored = fixture.table.snapshot().expect("cadence snapshot");
    assert!(
        stored.cut <= version(200),
        "replay must restart at or below the lower version: {stored:?}"
    );
}

/// The same scenario with a flush requested at a LOW cut through a latch (the
/// S4d interaction): the cell frontier stays at the anchor bound, so after the
/// latch drops the next snapshot still names a cut at or below v200.
#[test]
fn a_flush_at_a_low_cut_keeps_the_anchor_below_the_lower_version() {
    let fixture = Fixture::new("hybrid-table-anchor-latch");
    fixture.publish(RecordKind::Put, 1, 300, 1000);
    fixture.table.advance_applied(version(150));
    fixture.cuts.set_durable(version(150));
    let latch = fixture.table.pin_cut(version(150));
    // The forced pass at the clamp flushes the dirty cell at 150.
    fixture
        .table
        .snapshot_for_relocation(version(150))
        .expect("flush at the low cut");
    fixture.publish(RecordKind::Put, 2, 200, 2000);
    fixture.table.advance_applied(version(301));
    fixture.cuts.set_durable(version(301));
    drop(latch);
    let stored = fixture.table.snapshot().expect("cadence snapshot");
    assert!(
        stored.cut <= version(200),
        "the low flush must not raise the cell above the lower version: {stored:?}"
    );
}

/// The crash half of the anchor scenario. v300 reaches the empty cell first,
/// v200 second (durable-arrival order across owners), `applied` advances only
/// after both inserts, a cadence snapshot stores, the process dies and a new one
/// opens the region. Replay restarts at the stored cut (inclusive), so the
/// cut must be at or below v200, and the replayed batches then rebuild both
/// versions, v200 included.
///
/// Negative control (mutation run): the S1S2 default anchor
/// (`first.frontier()` with no `anchor_cut` override) stores a cut above v200
/// and the first assertion fails.
#[test]
fn an_out_of_order_cell_reopens_with_a_cut_that_replays_the_lower_version() {
    let mut fixture = Fixture::new("hybrid-table-anchor-reopen");
    fixture.publish(RecordKind::Put, 1, 300, 1000);
    fixture.publish(RecordKind::Put, 2, 200, 2000);
    // Both inserts are in; only now may the publication prefix cover them.
    fixture.table.advance_applied(version(301));
    fixture.cuts.set_durable(version(301));
    let stored = fixture.table.snapshot().expect("cadence snapshot");
    assert!(stored.stored);
    assert!(
        stored.cut <= version(200),
        "replay must restart at or below the lower version: {stored:?}"
    );
    fixture.reopen();
    let reopened = fixture.table.stored_cut();
    assert!(reopened <= version(200), "{reopened:?}");
    // Replay from the stored cut inserts what the owners hold at or above it,
    // in version order, and the lower version is back.
    // Only the batches at or above the stored cut are replayed (inclusive), so a
    // cut above v200 would lose it and the `get` below would fail.
    for (key, sequence, offset) in [(2u8, 200u64, 2000u64), (1, 300, 1000)] {
        if version(sequence) >= reopened {
            fixture.publish(RecordKind::Put, key, sequence, offset);
        }
    }
    fixture.table.advance_applied(version(301));
    assert_eq!(
        fixture
            .get(2, version(301))
            .map(|found| found.address.offset),
        Some(member_offset(2000)),
        "v200 survives the crash"
    );
    assert_eq!(
        fixture
            .get(1, version(301))
            .map(|found| found.address.offset),
        Some(member_offset(1000))
    );
}

/// `relocation_view` shows every entry at or above `q`: an entry newer than `q`
/// that an earlier flush already promoted (here v4 over v2 of one key, flushed
/// before the latch at q = 3) is what the pass sees. The pass re-points it only
/// by the compare-and-set against the physical entry it read: a stale
/// expectation (the v2 position the view no longer holds) is skipped, the
/// matching one is applied, and a newer overwrite that lands afterwards is left
/// alone.
///
/// Negative control (mutation run): drop the `below_target` test in
/// `relocation_apply` and the re-point outside the target spans is applied
/// ("an entry outside the target spans is not re-pointed"). Comparing by logical
/// identity (`batch ==` instead of `same_physical`) is not caught here, because
/// every stale expectation below differs in its batch; that control is
/// `relocation_compare_and_set_preserves_a_newer_head`.
#[test]
fn a_relocation_view_shows_a_flat_entry_above_q_and_apply_repoints_only_what_it_read() {
    let fixture = Fixture::new("hybrid-table-flat-above-q");
    fixture.publish(RecordKind::Put, 1, 2, 100);
    fixture.publish(RecordKind::Put, 1, 4, 200);
    fixture.table.advance_applied(version(5));
    fixture.cuts.set_durable(version(5));
    fixture
        .table
        .snapshot_for_relocation(version(5))
        .expect("the forced pass flushes both versions");
    let ks = fixture.ks();
    let cell = fixture.table.shape().ks(ks).cell_id(&[1; 4]);
    let (view, latch) = fixture
        .table
        .relocation_view(ks, &cell, version(3))
        .expect("view at q = 3");
    let (key, seen) = view
        .iter()
        .find(|(_, found)| found.batch == version(4))
        .unwrap_or_else(|| {
            panic!(
                "the flushed entry at q + 1 is in the view: state {}, view {:?}",
                fixture
                    .table
                    .large_table()
                    .entry_state_for_test(fixture.table.contexts().ks_context(ks), &cell),
                view.iter().collect::<Vec<_>>()
            )
        });
    assert_eq!(seen.address.offset, member_offset(200));
    let targets = [(0u32, 1u64, 10_000u64)];

    fixture
        .table
        .relocation_apply(
            ks,
            &cell,
            RelocationApply::Repoint(&[(key.clone(), position(2, 100), position(2, 800))]),
            &targets,
        )
        .expect("stale expectation");
    assert_eq!(
        fixture.get(1, version(5)).map(|found| found.address.offset),
        Some(member_offset(200)),
        "an expectation the index no longer holds is skipped"
    );

    let outside = [(0u32, 9u64, 10_000u64)];
    fixture
        .table
        .relocation_apply(
            ks,
            &cell,
            RelocationApply::Repoint(&[(key.clone(), seen, position(4, 600))]),
            &outside,
        )
        .expect("target span of another stream");
    assert_eq!(
        fixture.get(1, version(5)).map(|found| found.address.offset),
        Some(member_offset(200)),
        "an entry outside the target spans is not re-pointed"
    );

    let moved = position(4, 900);
    fixture
        .table
        .relocation_apply(
            ks,
            &cell,
            RelocationApply::Repoint(&[(key.clone(), seen, moved)]),
            &targets,
        )
        .expect("matching expectation");
    assert_eq!(
        fixture.get(1, version(5)).map(|found| found.address.offset),
        Some(member_offset(900)),
        "the entry the pass read is re-pointed"
    );

    fixture.publish(RecordKind::Put, 1, 6, 300);
    fixture.table.advance_applied(version(7));
    fixture
        .table
        .relocation_apply(
            ks,
            &cell,
            RelocationApply::Repoint(&[(key, moved, position(4, 950))]),
            &targets,
        )
        .expect("overwritten expectation");
    assert_eq!(
        fixture.get(1, version(7)).map(|found| found.address.offset),
        Some(member_offset(300)),
        "a newer overwrite is never replaced by a re-point of its predecessor"
    );
    drop(latch);
}

// ---- real index WAL: `HybridLoader::load` / `index_reader` ----
//
// Found by the S6t run: `load` once handed the index-WAL frame, with its
// 2-byte `[type][keyspace]` prefix, to the index decoder. Every test below
// writes through the loader's real index WAL writer (`Loader::flush`).

/// A position whose operation index and payload CRC differ per entry, so a
/// loaded row proves those fields survive the 48-byte encoding.
fn varied(sequence: u64, offset: u64, operation_index: u32, payload_crc: u32) -> VersionPosition {
    VersionPosition::member(
        version(sequence),
        PhysicalValueAddress {
            partition: 0,
            segment_generation: 1,
            offset,
            len: 64,
        },
        operation_index,
        RecordDescriptor {
            operation_index,
            offset: DESCRIPTOR_OFFSET,
            len: 24,
            keyspace: 0,
            kind: RecordKind::Put,
            key_len: 4,
            value_offset: 14,
            value_len: 2,
            checksum: MemberChecksum(0),
        },
        payload_crc,
    )
}

/// Rows are compared on every field the encoding carries: the position's
/// `PartialEq` is logical only (batch, operation index).
fn assert_rows(table: &IndexTable<VersionPosition>, expected: &[(Bytes, VersionPosition)]) {
    let loaded: Vec<_> = table.iter().collect();
    assert_eq!(loaded.len(), expected.len(), "entry count");
    for ((loaded_key, loaded), (key, expected)) in loaded.iter().zip(expected) {
        assert_eq!(loaded_key, key);
        assert_eq!(loaded.batch, expected.batch, "{key:?}");
        assert_eq!(loaded.operation_index, expected.operation_index, "{key:?}");
        assert_eq!(loaded.payload_crc, expected.payload_crc, "{key:?}");
        assert_eq!(loaded.address, expected.address, "{key:?}");
        assert!(loaded.same_physical(expected));
    }
}

/// Two keyspaces of the same key size: one table, one index WAL.
fn two_keyspace_table(dir: &tempdir::TempDir, cuts: &CutSources) -> HybridTable {
    let mut builder = KeyShapeBuilder::new();
    builder.add_key_space("first", 4, 1, KeyType::uniform(1));
    builder.add_key_space("second", 4, 1, KeyType::uniform(1));
    HybridTable::open(
        dir.path(),
        Arc::new(builder.build()),
        &HybridIndexConfig::default(),
        cuts.clone(),
        Metrics::new(),
    )
    .expect("open table")
}

/// (a)+(b)+(c): flush a cell of each keyspace, then
/// * the raw frame is `[WAL_ENTRY_INDEX = 2][keyspace id][blob]`, the blob a
///   whole number of `(key + 48 B position)` elements;
/// * `load` returns the same rows (same physical address and CRC) per keyspace;
/// * `index_reader` (random reads, prefix skipped) finds every key at the same
///   position, and misses an absent key: the lazy-read consumer
///   (`LargeTable::lookup_disk_levels` calls exactly this `lookup_unloaded`).
///
/// Negative control (mutation run): hand the raw frame to `deserialize_index`
/// in `HybridLoader::load`: the table decode panics with "fixed-size index blob
/// length 54 is not a multiple of element size 52" (one entry).
#[test]
fn a_flushed_cell_loads_back_through_the_real_index_wal_and_agrees_with_the_reader() {
    let dir = tempdir::TempDir::new("hybrid-loader-roundtrip").expect("scratch directory");
    let cuts = CutSources::new(1, DurabilityTier::Native);
    let table = two_keyspace_table(&dir, &cuts);
    let loader = table.loader();
    let spaces: Vec<_> = table.shape().iter_ks().collect();
    assert_eq!(spaces.len(), 2);
    for (index, desc) in spaces.into_iter().enumerate() {
        let ks = desc.id();
        for entries in [1u8, 2, 5] {
            let mut data = IndexTable::<VersionPosition>::default();
            let mut expected = Vec::new();
            for key in 1..=entries {
                let written = varied(
                    10 + u64::from(key) + 100 * index as u64,
                    1_000 * u64::from(key) + 100_000 * index as u64,
                    u32::from(key) * 7,
                    crc32fast::hash(&[key; 3]),
                );
                let key = Bytes::copy_from_slice(&[key; 4]);
                data.insert(key.clone(), written);
                expected.push((key, written));
            }
            let at = loader.flush(ks, &data).expect("write the index frame");

            let (_, frame) = loader.index_wal().read(at).expect("raw frame");
            let frame = frame.expect("the frame is present");
            assert_eq!(frame[0], 2, "WAL_ENTRY_INDEX");
            assert_eq!(frame[1], desc.id().0, "keyspace id");
            // The Header index format puts a fixed header before the elements.
            let elements = frame.len() - WalEntry::INDEX_PREFIX_SIZE - HEADER_SIZE;
            assert_eq!(
                elements % (4 + VersionPosition::LENGTH),
                0,
                "the blob is a header plus whole 52-byte elements"
            );
            assert_eq!(
                elements / (4 + VersionPosition::LENGTH),
                usize::from(entries)
            );

            let loaded = loader.load(desc, at).expect("load the frame");
            assert_rows(&loaded, &expected);

            let reader = loader.index_reader(at).expect("random reader");
            for (key, written) in &expected {
                let found = desc
                    .index_format()
                    .lookup_unloaded::<VersionPosition>(desc, &reader, key, table.metrics())
                    .expect("the reader finds the key");
                assert!(found.same_physical(written), "{key:?}: reader {found:?}");
            }
            let absent = [0xEE; 4];
            assert!(
                desc.index_format()
                    .lookup_unloaded::<VersionPosition>(desc, &reader, &absent, table.metrics())
                    .is_none()
            );
        }
    }
}

/// A stored index tombstone (what a relocation drop leaves in a cell) survives
/// flush and load: the loaded table keeps it, and its live view omits it.
#[test]
fn a_flushed_tombstone_loads_back_as_a_tombstone() {
    let fixture = Fixture::new("hybrid-loader-tombstone");
    let ks = fixture.ks();
    let desc = fixture.table.ks_desc(ks);
    let loader = fixture.table.loader();
    let mut data = IndexTable::<VersionPosition>::default();
    let put = Bytes::copy_from_slice(&[1; 4]);
    let removed = Bytes::copy_from_slice(&[2; 4]);
    data.insert(put.clone(), position(3, 100));
    data.remove(removed.clone(), position(4, 200));
    let at = loader.flush(ks, &data).expect("write the index frame");
    let loaded = loader.load(desc, at).expect("load the frame");
    assert_eq!(
        loaded.iter_with_tombstones().count(),
        data.iter_with_tombstones().count(),
        "the tombstone is stored"
    );
    assert_eq!(loaded.iter_with_tombstones().count(), 2);
    let live: Vec<_> = loaded.iter().map(|(key, _)| key).collect();
    assert_eq!(live, vec![put], "the live view omits the tombstoned key");
}

/// One flipped byte of a flushed blob is a typed WAL-frame CRC error from the
/// index WAL read and from `load`: never a panic in the index decoder and
/// never a table built from damaged bytes.
#[test]
fn a_damaged_index_frame_is_a_typed_crc_error_not_a_panic() {
    use std::os::unix::fs::FileExt;
    let fixture = Fixture::new("hybrid-loader-crc");
    let ks = fixture.ks();
    let desc = fixture.table.ks_desc(ks);
    let loader = fixture.table.loader();
    let mut data = IndexTable::<VersionPosition>::default();
    data.insert(Bytes::copy_from_slice(&[1; 4]), position(3, 100));
    let at = loader.flush(ks, &data).expect("write the index frame");
    loader.load(desc, at).expect("the intact frame loads");

    let layout = loader.index_wal().layout();
    let file = layout.wal_file_name(
        fixture.dir.path(),
        crate::wal::position::WalFileId::from_u64(at.offset() / layout.wal_file_size),
    );
    let in_file = at.offset() % layout.wal_file_size;
    // Past the 8-byte frame header and the 2-byte entry prefix: inside the blob.
    let target = in_file + 8 + 2 + 3;
    let handle = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&file)
        .expect("the index WAL file");
    let mut byte = [0u8];
    handle
        .read_exact_at(&mut byte, target)
        .expect("read the byte");
    handle
        .write_all_at(&[byte[0] ^ 0xFF], target)
        .expect("flip it");

    assert!(
        loader.index_wal().read(at).is_err(),
        "the WAL read reports the CRC failure as an error"
    );
    let error = loader
        .load(desc, at)
        .expect_err("a damaged frame does not load");
    assert!(error.to_string().contains("index WAL read"), "{error}");
}

/// A frame that is not an index entry of the asked keyspace is a typed error
/// from `load`, never a panic and never bytes handed to the index decoder.
#[test]
fn a_frame_of_another_type_or_keyspace_is_a_typed_load_error() {
    let dir = tempdir::TempDir::new("hybrid-loader-wrong-entry").expect("scratch directory");
    let cuts = CutSources::new(1, DurabilityTier::Native);
    let table = two_keyspace_table(&dir, &cuts);
    let loader = table.loader();
    let spaces: Vec<_> = table.shape().iter_ks().collect();
    let (first, second) = (spaces[0], spaces[1]);
    // Another entry type.
    let write = PreparedWalWrite::new(&WalEntry::Remove(first.id(), Bytes::from_static(&[7; 12])));
    let at = *loader
        .index_writer()
        .write(&write)
        .expect("write a non-index frame")
        .wal_position();
    let error = loader.load(first, at).expect_err("not an index frame");
    assert!(
        error.to_string().contains("does not hold an index entry"),
        "{error}"
    );
    // An index entry of the other keyspace.
    let mut data = IndexTable::<VersionPosition>::default();
    data.insert(Bytes::copy_from_slice(&[1; 4]), position(3, 100));
    let at = loader
        .flush(second.id(), &data)
        .expect("write the index frame");
    let error = loader.load(first, at).expect_err("another keyspace");
    assert!(
        error.to_string().contains("does not hold an index entry"),
        "{error}"
    );
    loader
        .load(second, at)
        .expect("the owning keyspace loads it");
}

/// The reopened table's cells are unloaded: a relocation view loads one through
/// `HybridLoader::load` from the index WAL the first life wrote.
#[test]
fn a_reopened_cell_loads_its_flushed_entries_through_the_loader() {
    let mut fixture = Fixture::new("hybrid-loader-reopen");
    for (key, sequence, offset) in [(1u8, 2u64, 100u64), (1, 4, 200), (2, 3, 300)] {
        fixture.publish(RecordKind::Put, key, sequence, offset);
    }
    fixture.table.advance_applied(version(5));
    fixture.cuts.set_durable(version(5));
    fixture
        .table
        .snapshot_for_relocation(version(5))
        .expect("forced store flushes the cell");
    let ks = fixture.ks();
    fixture.reopen();
    let cell = fixture.table.shape().ks(ks).cell_id(&[1; 4]);
    let (view, latch) = fixture
        .table
        .relocation_view(ks, &cell, version(6))
        .expect("the reopened cell loads");
    let found: Vec<_> = view
        .iter()
        .map(|(key, found)| (key.to_vec(), found.address.offset))
        .collect();
    assert!(
        found.contains(&(vec![1; 4], member_offset(200))),
        "the newer version of key 1 is loaded: {found:?}"
    );
    assert!(
        found.contains(&(vec![2; 4], member_offset(300))),
        "key 2 is loaded: {found:?}"
    );
    drop(latch);
}

/// D30: native's relocation cutoff (`retain_above_position`) leaves no entry
/// for a key whose live positions all sit below it, so a read at any cut
/// resolves the key absent. A vector that still held the older version would
/// answer a cut between the two versions instead. The tombstone the drop
/// installs still shadows a deeper level and does not hide a later write.
#[test]
fn a_dropped_key_resolves_absent_at_every_cut() {
    let fixture = Fixture::new("hybrid-table-drop-whole-key");
    fixture.publish(RecordKind::Put, 1, 2, 100);
    fixture.publish(RecordKind::Put, 1, 4, 300);
    fixture.table.advance_applied(version(5));
    let ks = fixture.ks();
    let cell = fixture.table.shape().ks(ks).cell_id(&[1; 4]);
    assert_eq!(
        fixture.get(1, version(3)).map(|found| found.address.offset),
        Some(member_offset(100)),
        "premise: a cut below the latest version resolves the older one",
    );
    fixture
        .table
        .relocation_apply(ks, &cell, RelocationApply::Drop, &[(0, 1, 1000)])
        .expect("drop every entry of the key below the target");
    for cut in [version(3), version(5), version(9)] {
        assert!(
            fixture.get(1, cut).is_none(),
            "cut {cut:?}: a dropped key has no entry at any cut",
        );
    }
    fixture.publish(RecordKind::Put, 1, 10, 1000);
    fixture.table.advance_applied(version(11));
    assert_eq!(
        fixture
            .get(1, version(11))
            .map(|found| found.address.offset),
        Some(member_offset(1000)),
        "the drop tombstone does not hide a later write",
    );
}
/// Native's relocation cutoff prunes only the L0 it rebuilds, never a deeper
/// level (`flusher.rs` `handle_command`, `RelocationFlush::Cutoff`). A key that
/// lives only in a promoted, unloaded L1 blob therefore stays readable after a
/// relocation pass. The hybrid's `Drop` applies to the cell's in-memory level
/// alone, so it keeps that key too.
///
/// Negative control (mutation run): drop through the cell's disk levels (load
/// L1 into the drop's table) and the key reads absent below.
#[test]
fn a_promoted_cell_is_not_pruned_by_the_drop() {
    let fixture = Fixture::with_config(
        "hybrid-table-drop-promoted",
        HybridIndexConfig {
            // Two rows promote the single cell, and the zero stale window makes
            // the store unload it, so nothing of it is in memory.
            l0_max_entries: Some(1),
            snapshot_unload_threshold: 0,
            ..HybridIndexConfig::default()
        },
    );
    fixture.publish(RecordKind::Put, 1, 2, 100);
    fixture.publish(RecordKind::Put, 2, 2, 200);
    fixture.table.advance_applied(version(3));
    fixture.cuts.set_durable(version(3));
    fixture
        .table
        .snapshot_for_relocation(version(3))
        .expect("the store promotes and unloads the cell");
    let ks = fixture.ks();
    let cell = fixture.table.shape().ks(ks).cell_id(&[1; 4]);
    fixture
        .table
        .relocation_apply(ks, &cell, RelocationApply::Drop, &[(0, 1, 1000)])
        .expect("a drop with nothing in the in-memory level");
    for cut in [version(3), version(5)] {
        assert_eq!(
            fixture.get(1, cut).map(|found| found.address.offset),
            Some(member_offset(100)),
            "cut {cut:?}: a promoted cell's entry is not pruned by the drop",
        );
    }
}
