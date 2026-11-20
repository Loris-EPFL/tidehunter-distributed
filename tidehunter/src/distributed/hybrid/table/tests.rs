//! Table-level behaviour that needs no owner: the index half of put → get →
//! reopen, cut visibility, tombstones, the monotone stored target and the
//! relocation clamp. Owner drain and value reads are covered where an owner
//! exists (the leased fixture tests).

use super::{HybridTable, PublishedMutation};
use crate::distributed::codec::RecordKind;
use crate::distributed::durability::DurabilityTier;
use crate::distributed::hybrid::config::HybridIndexConfig;
use crate::distributed::hybrid::loader::CutSources;
use crate::distributed::hybrid::position::VersionPosition;
use crate::distributed::types::{
    BatchVersion, MemberChecksum, PhysicalValueAddress, RecordDescriptor,
};
use crate::index::index_table::IndexTable;
use crate::key_shape::{KeyShape, KeySpace, KeySpaceConfig, KeyType};
use crate::large_table::Loader;
use crate::metrics::Metrics;
use crate::wal::position::IndexPosition;
use parking_lot::Mutex;
use std::sync::Arc;

fn shape() -> Arc<KeyShape> {
    Arc::new(KeyShape::new_single(4, 1, KeyType::uniform(1)))
}

fn version(sequence: u64) -> BatchVersion {
    BatchVersion { epoch: 1, sequence }
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
            offset: 8,
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
        let dir = tempdir::TempDir::new(name).expect("scratch directory");
        let cuts = CutSources::new(1, DurabilityTier::Native);
        let table = open(&dir, &cuts);
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
        Some(128),
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
        Some(100),
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
        Some(300),
    );
    assert_eq!(
        fixture.get(1, version(3)).map(|found| found.address.offset),
        Some(100),
    );
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
    assert_eq!(first.cut, version(3));
    assert_eq!(first.promotion_cut, first.cut);
    assert_eq!(fixture.table.stored_cut(), version(3));
    let second = fixture.table.snapshot().expect("second snapshot");
    assert!(!second.stored, "an unchanged head stores nothing");
    assert_eq!(second.cut, first.cut);
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
        Some(100),
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
    fixture.table.snapshot().expect("baseline");
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
