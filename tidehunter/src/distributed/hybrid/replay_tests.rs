//! Source-level tests for the hybrid replay merge.
//!
//! These drive [`HybridReplaySource`] through a scripted owner: no cluster, no
//! kernel target, no timing. The table-level crash cuts (an acknowledged batch
//! above the stored region, a held latch, the allocation floor next to an
//! applied batch, a snapshot whose `stored_target` must advance past a skipped
//! epoch) belong to the composition that owns the durable cut, the latches and
//! the owner drain. This file proves the half that decides *which* batches reach
//! the index, in what order, and which streams are replayed at all.

use super::position::VersionPosition;
use super::region::StreamFront;
use super::replay::{HybridReplaySource, OwnerPages, ReplayOutcome, ReplaySourceError};
use crate::cell::CellId;
use crate::crc::CrcFrame;
use crate::db::WalEntry;
use crate::distributed::codec::{BatchLimits, CompleteBatch, Mutation};
use crate::distributed::storage::{DurableBatch, aligned_replay_start};
use crate::distributed::tcp::RecoveryPage;
use crate::distributed::types::{
    BatchId, BatchVersion, MemberChecksum, PhysicalValueAddress, RecordDescriptor, RecordKind,
};
use crate::key_shape::{KeyShape, KeyType};
use crate::wal_replay::{ReplayFrame, ReplaySource};
use std::collections::HashMap;

fn shape() -> KeyShape {
    KeyShape::new_single(4, 1, KeyType::uniform(1))
}

fn limits() -> BatchLimits {
    BatchLimits {
        max_operations: 16,
        max_encoded_bytes: 4096,
    }
}

fn version(sequence: u64) -> BatchVersion {
    version_of(1, sequence)
}

fn version_of(epoch: u64, sequence: u64) -> BatchVersion {
    BatchVersion { epoch, sequence }
}

fn batch(sequence: u64, keys: &[([u8; 4], &[u8])]) -> CompleteBatch {
    batch_of(1, sequence, keys)
}

fn batch_of(epoch: u64, sequence: u64, keys: &[([u8; 4], &[u8])]) -> CompleteBatch {
    let shape = shape();
    let keyspace = shape.iter_ks().next().expect("one keyspace").id();
    let mutations = keys
        .iter()
        .map(|(key, value)| Mutation::Put {
            keyspace,
            key: key.to_vec().into(),
            value: value.to_vec().into(),
        })
        .collect();
    CompleteBatch::new(
        &shape,
        BatchId {
            database: [7; 16],
            operation: [sequence as u8; 16],
        },
        version_of(epoch, sequence),
        mutations,
        limits(),
    )
    .expect("valid batch")
    .into_canonical_owned()
}

fn address(partition: u32, offset: u64, batch: &CompleteBatch) -> PhysicalValueAddress {
    PhysicalValueAddress {
        partition,
        segment_generation: batch.version().epoch,
        offset,
        len: batch.encoded().len() as u32 + CrcFrame::CRC_HEADER_LENGTH as u32,
    }
}

/// Owner pages keyed by their `(partition, generation, from)` request. A page is
/// consumed by the read, so an unexpected or repeated request fails the test.
struct ScriptedPages {
    pages: HashMap<(u32, u64, u64), RecoveryPage>,
    reads: usize,
}

impl ScriptedPages {
    fn new() -> Self {
        Self {
            pages: HashMap::new(),
            reads: 0,
        }
    }

    fn page(
        mut self,
        partition: u32,
        generation: u64,
        from: u64,
        batches: Vec<(CompleteBatch, u64)>,
        next: Option<usize>,
    ) -> Self {
        let entries = batches
            .into_iter()
            .map(|(batch, offset)| {
                let receipt = DurableBatch::from_validated_parts(
                    batch.id(),
                    batch.version(),
                    batch.digest(),
                    address(partition, offset, &batch),
                );
                (batch, receipt)
            })
            .collect();
        self.pages.insert(
            (partition, generation, from),
            RecoveryPage {
                batches: entries,
                next,
            },
        );
        self
    }
}

impl OwnerPages for ScriptedPages {
    fn read_page(
        &mut self,
        partition: u32,
        generation: u64,
        from: u64,
        _limit: usize,
        _require_end: u64,
    ) -> Result<RecoveryPage, ReplaySourceError> {
        self.reads += 1;
        self.pages
            .remove(&(partition, generation, from))
            .ok_or(ReplaySourceError::InvalidPage("unexpected page request"))
    }
}

fn front(partition: u32, generation: u64, replay_from: u64) -> StreamFront {
    StreamFront {
        partition,
        generation,
        replay_from,
    }
}

fn live_of(fronts: &[StreamFront]) -> Vec<(u32, u64)> {
    fronts
        .iter()
        .map(|front| (front.partition, front.generation))
        .collect()
}

/// A source with every front live and a page limit of 8.
fn source<P: OwnerPages, K: FnMut(BatchVersion) -> bool>(
    pages: P,
    keep: K,
    fronts: &[StreamFront],
) -> Result<HybridReplaySource<P, K>, ReplaySourceError> {
    let live = live_of(fronts);
    HybridReplaySource::new(pages, keep, fronts, &live, 8, 64)
}

/// Acknowledges everything below `exclusive` in epoch 1, like a plan whose open
/// cut is `exclusive`.
fn below(exclusive: u64) -> impl FnMut(BatchVersion) -> bool {
    move |version| version.epoch != 1 || version.sequence < exclusive
}

/// Drains every frame in the order the index would receive it.
fn drain(
    source: impl ReplaySource<VersionPosition, Output = ReplayOutcome>,
) -> (Vec<BatchVersion>, ReplayOutcome) {
    let mut source = source;
    let mut versions = Vec::new();
    loop {
        match source.next().expect("scripted source cannot fail") {
            Some(ReplayFrame::BatchStart { position, count }) => {
                assert!(count > 0, "an empty batch group");
                versions.push(position.batch);
            }
            Some(ReplayFrame::Entry { position, entry }) => {
                assert!(
                    matches!(
                        entry,
                        WalEntry::Record(..) | WalEntry::Remove(..) | WalEntry::DropCells(..)
                    ),
                    "unexpected replayed entry"
                );
                versions.push(position.batch);
            }
            None => break,
        }
    }
    let outcome = source.finish(None).expect("clean end");
    (versions, outcome)
}

fn sequences(per_frame: &[BatchVersion]) -> Vec<u64> {
    let mut seen: Vec<u64> = per_frame.iter().map(|version| version.sequence).collect();
    seen.dedup();
    seen
}

#[test]
fn merges_two_owners_by_version() {
    let a1 = batch(1, &[([1, 0, 0, 0], b"a1")]);
    let b2 = batch(2, &[([2, 0, 0, 0], b"b2")]);
    let a3 = batch(3, &[([3, 0, 0, 0], b"a3")]);
    let pages = ScriptedPages::new()
        .page(0, 1, 0, vec![(a1, 0), (a3, 128)], None)
        .page(1, 1, 0, vec![(b2, 0)], None);
    let source = source(pages, |_| true, &[front(0, 1, 0), front(1, 1, 0)]).expect("source");
    let (frames, outcome) = drain(source);
    // One `BatchStart` then the member, per batch, in version order.
    assert_eq!(sequences(&frames), vec![1, 2, 3]);
    assert_eq!(frames.len(), 6);
    assert_eq!(outcome.merged, 3);
    assert_eq!(outcome.applied, Some(version(3)));
    assert_eq!(outcome.discarded, 0);
}

#[test]
fn the_acknowledgement_filter_drops_later_batches() {
    let a1 = batch(1, &[([1, 0, 0, 0], b"a1")]);
    let a2 = batch(2, &[([2, 0, 0, 0], b"a2")]);
    let pages = ScriptedPages::new().page(0, 1, 0, vec![(a1, 0), (a2, 128)], None);
    let source = source(pages, below(2), &[front(0, 1, 0)]).expect("source");
    let (frames, outcome) = drain(source);
    assert_eq!(sequences(&frames), vec![1]);
    assert_eq!(outcome.applied, Some(version(1)));
    assert_eq!(outcome.discarded, 1);
}

#[test]
fn without_the_filter_the_unacknowledged_batch_appears() {
    // Negative control for `the_acknowledgement_filter_drops_later_batches`: an
    // accept-all filter indexes the batch Version B refused, so this test fails
    // if the filter stops being applied.
    let a1 = batch(1, &[([1, 0, 0, 0], b"a1")]);
    let a2 = batch(2, &[([2, 0, 0, 0], b"a2")]);
    let pages = ScriptedPages::new().page(0, 1, 0, vec![(a1, 0), (a2, 128)], None);
    let source = source(pages, |_| true, &[front(0, 1, 0)]).expect("source");
    let (frames, outcome) = drain(source);
    assert_eq!(sequences(&frames), vec![1, 2]);
    assert_eq!(outcome.discarded, 0);
}

#[test]
fn a_recorded_truncation_drops_an_older_generations_tail() {
    // Native-tier crash in epoch 1: the owners appended past the synced point,
    // so the plan records `{1, next: 5}`; the next open runs in epoch 2 and the
    // unacknowledged epoch 1 tail is still in the retained WALs. It sorts below
    // epoch 2, so the merge must drop it and keep draining, not stop.
    let e1_tail = batch_of(1, 5, &[([5, 0, 0, 0], b"lost")]);
    let e1_later = batch_of(1, 7, &[([7, 0, 0, 0], b"lost")]);
    let e2 = batch_of(2, 1, &[([1, 2, 0, 0], b"kept")]);
    let pages = ScriptedPages::new()
        .page(0, 1, 0, vec![(e1_tail, 0), (e1_later, 128)], None)
        .page(0, 2, 0, vec![(e2, 0)], None);
    let fronts = [front(0, 1, 0), front(0, 2, 0)];
    let source = source(
        pages,
        |version| version.epoch != 1 || version.sequence < 5,
        &fronts,
    )
    .expect("source");
    let (frames, outcome) = drain(source);
    assert_eq!(sequences(&frames), vec![1], "only the new epoch survives");
    assert_eq!(outcome.applied, Some(version_of(2, 1)));
    assert_eq!(outcome.discarded, 2);
}

#[test]
fn without_the_recorded_truncation_the_old_tail_resurrects() {
    // Negative control for `a_recorded_truncation_drops_an_older_generations_tail`.
    // A single current-epoch cut keeps every older generation, which brings the
    // unacknowledged epoch 1 tail back into the index.
    let e1_tail = batch_of(1, 5, &[([5, 0, 0, 0], b"lost")]);
    let e2 = batch_of(2, 1, &[([1, 2, 0, 0], b"kept")]);
    let pages = ScriptedPages::new()
        .page(0, 1, 0, vec![(e1_tail, 0)], None)
        .page(0, 2, 0, vec![(e2, 0)], None);
    let fronts = [front(0, 1, 0), front(0, 2, 0)];
    let source = source(
        pages,
        |version| version.epoch != 2 || version.sequence < u64::MAX,
        &fronts,
    )
    .expect("source");
    let (frames, _) = drain(source);
    assert_eq!(sequences(&frames), vec![5, 1]);
}

#[test]
fn a_rejected_batch_never_becomes_the_applied_cut() {
    // Amendment 16: `applied` is the highest *kept* batch. A rejected batch must
    // not be reported as applied, or the served cut would pass data the index
    // never received.
    let e1_tail = batch_of(1, 9, &[([9, 0, 0, 0], b"lost")]);
    let pages = ScriptedPages::new().page(0, 1, 0, vec![(e1_tail, 0)], None);
    let source = source(pages, |_| false, &[front(0, 1, 0)]).expect("source");
    let (frames, outcome) = drain(source);
    assert!(frames.is_empty());
    assert_eq!(outcome.applied, None);
    assert_eq!(outcome.merged, 1);
    assert_eq!(outcome.discarded, 1);
}

#[test]
fn three_owners_interleave_in_version_order() {
    // The middle batch of the lowest stream must not be emitted before the
    // other streams' earlier versions.
    let a1 = batch(1, &[([1, 0, 0, 0], b"a1")]);
    let a3 = batch(3, &[([3, 0, 0, 0], b"a3")]);
    let a5 = batch(5, &[([5, 0, 0, 0], b"a5")]);
    let b2 = batch(2, &[([2, 1, 0, 0], b"b2")]);
    let b4 = batch(4, &[([4, 1, 0, 0], b"b4")]);
    let c6 = batch(6, &[([6, 2, 0, 0], b"c6")]);
    let pages = ScriptedPages::new()
        .page(0, 1, 0, vec![(a1, 0), (a3, 128), (a5, 256)], None)
        .page(1, 1, 0, vec![(b2, 0), (b4, 128)], None)
        .page(2, 1, 0, vec![(c6, 0)], None);
    let source = source(
        pages,
        |_| true,
        &[front(0, 1, 0), front(1, 1, 0), front(2, 1, 0)],
    )
    .expect("source");
    let (frames, outcome) = drain(source);
    assert_eq!(sequences(&frames), vec![1, 2, 3, 4, 5, 6]);
    assert_eq!(outcome.applied, Some(version(6)));
}

#[test]
fn a_discarded_batch_between_two_kept_ones_does_not_stop_the_drain() {
    // Version B refused v2 only. v1 and v3 must both reach the index and the
    // applied cut must land on v3, not on the rejected middle batch.
    let a1 = batch(1, &[([1, 0, 0, 0], b"a1")]);
    let a2 = batch(2, &[([2, 0, 0, 0], b"a2")]);
    let a3 = batch(3, &[([3, 0, 0, 0], b"a3")]);
    let pages = ScriptedPages::new().page(0, 1, 0, vec![(a1, 0), (a2, 128), (a3, 256)], None);
    let source = source(pages, |version| version.sequence != 2, &[front(0, 1, 0)]).expect("source");
    let (frames, outcome) = drain(source);
    assert_eq!(sequences(&frames), vec![1, 3]);
    assert_eq!(outcome.applied, Some(version(3)));
    assert_eq!(outcome.merged, 3);
    assert_eq!(outcome.discarded, 1);
}

#[test]
fn a_front_for_a_dead_generation_is_skipped() {
    // A trim-pending or manifest-absent generation is dead by construction. Its
    // page is not requested at all: the scripted owner would fail the read.
    let e2 = batch_of(2, 1, &[([1, 2, 0, 0], b"kept")]);
    let pages = ScriptedPages::new().page(0, 2, 0, vec![(e2, 0)], None);
    let fronts = [front(0, 1, 0), front(0, 2, 0)];
    let source =
        HybridReplaySource::new(pages, |_| true, &fronts, &[(0, 2)], 8, 64).expect("source");
    let (frames, outcome) = drain(source);
    assert_eq!(sequences(&frames), vec![1]);
    assert_eq!(outcome.skipped_dead, 1);
    assert_eq!(outcome.applied, Some(version_of(2, 1)));
}

#[test]
fn a_live_generation_without_a_front_is_refused() {
    // Replaying without the front of a generation the manifest still retains
    // would silently drop that stream's suffix.
    let pages = ScriptedPages::new();
    let error =
        HybridReplaySource::new(pages, |_| true, &[front(0, 1, 0)], &[(0, 1), (0, 2)], 8, 64)
            .err()
            .expect("a live generation without a front must fail");
    assert!(error.to_string().contains("has no stored replay front"));
}

#[test]
fn stored_front_is_the_replay_start() {
    let later = batch(2, &[([2, 0, 0, 0], b"a2")]);
    // The page service only knows the stored start; a request at 0 would be an
    // unexpected page and fail the test.
    let pages = ScriptedPages::new().page(0, 1, 128, vec![(later, 128)], None);
    let source = source(pages, |_| true, &[front(0, 1, 128)]).expect("source");
    let (frames, outcome) = drain(source);
    assert_eq!(sequences(&frames), vec![2]);
    assert_eq!(outcome.applied, Some(version(2)));
}

#[test]
fn over_wide_start_replays_the_same_state_again() {
    let earlier = batch(1, &[([1, 0, 0, 0], b"a1")]);
    let later = batch(2, &[([2, 0, 0, 0], b"a2")]);
    let pages = ScriptedPages::new().page(0, 1, 0, vec![(earlier, 0), (later, 128)], None);
    // One batch wider than the exact start at 128.
    let source = source(pages, |_| true, &[front(0, 1, 0)]).expect("source");
    let (frames, outcome) = drain(source);
    // The early batch is applied a second time; idempotent by version in the
    // index, so an over-wide front costs replay time and changes no state.
    assert_eq!(sequences(&frames), vec![1, 2]);
    assert_eq!(frames.len(), 4);
    assert_eq!(outcome.applied, Some(version(2)));
}

#[test]
fn unaligned_starts_are_floored_to_the_fragment() {
    // The stored front is aligned, so this only guards a corrupted or legacy
    // value; flooring widens the replay and never narrows it.
    assert_eq!(aligned_replay_start(0, 64), 0);
    assert_eq!(aligned_replay_start(128, 64), 128);
    assert_eq!(aligned_replay_start(130, 64), 128);
    assert_eq!(aligned_replay_start(130, 0), 130);
}

#[test]
fn uneven_streams_keep_the_lagging_suffix() {
    // Owner 0 is ahead of owner 1. One global offset cut taken from the lagging
    // stream would drop owner 0's v6; per-stream starts keep it.
    let a1 = batch(1, &[([1, 0, 0, 0], b"a1")]);
    let a6 = batch(6, &[([6, 0, 0, 0], b"a6")]);
    let b2 = batch(2, &[([2, 0, 0, 0], b"b2")]);
    let b3 = batch(3, &[([3, 0, 0, 0], b"b3")]);
    let pages = ScriptedPages::new()
        .page(0, 1, 0, vec![(a1, 0), (a6, 128)], None)
        .page(1, 1, 64, vec![(b2, 64), (b3, 192)], None);
    let source = source(pages, |_| true, &[front(0, 1, 0), front(1, 1, 64)]).expect("source");
    let (frames, outcome) = drain(source);
    assert_eq!(sequences(&frames), vec![1, 2, 3, 6]);
    assert_eq!(outcome.applied, Some(version(6)));
}

#[test]
fn a_skipping_page_does_not_end_the_stream() {
    // A page that holds only skipped groups carries no batch; the merge must
    // follow its cursor instead of treating the stream as exhausted.
    let a2 = batch(2, &[([2, 0, 0, 0], b"a2")]);
    let b3 = batch(3, &[([3, 0, 0, 0], b"b3")]);
    let pages = ScriptedPages::new()
        .page(0, 1, 0, Vec::new(), Some(64))
        .page(0, 1, 64, vec![(a2, 64)], None)
        .page(1, 1, 0, vec![(b3, 0)], None);
    let source = source(pages, |_| true, &[front(0, 1, 0), front(1, 1, 0)]).expect("source");
    let (frames, outcome) = drain(source);
    assert_eq!(sequences(&frames), vec![2, 3]);
    assert_eq!(outcome.merged, 2);
}

#[test]
fn allocation_floor_fronts_recover_dropped_extents() {
    // Two allocated-but-unpublished extents (v5 at offset 128, v7 at offset 320)
    // have no published batch, but they are acknowledged WAL data after a
    // crash. The stored fronts start at their offsets, so replay recovers both.
    let d1 = batch(5, &[([5, 0, 0, 0], b"d1")]);
    let d2 = batch(7, &[([7, 0, 0, 0], b"d2")]);
    let pages = ScriptedPages::new().page(0, 1, 128, vec![(d1, 128), (d2, 320)], None);
    let source = source(pages, |_| true, &[front(0, 1, 128)]).expect("source");
    let (frames, outcome) = drain(source);
    assert_eq!(sequences(&frames), vec![5, 7]);
    assert_eq!(outcome.applied, Some(version(7)));
}

#[test]
fn narrow_front_loses_the_dropped_extent() {
    // Negative control for `allocation_floor_fronts_recover_dropped_extents`:
    // a front above the first dropped extent starts the stream past it, so the
    // extent never reaches the index. Without the floor term, that is the loss.
    let d2 = batch(7, &[([7, 0, 0, 0], b"d2")]);
    let pages = ScriptedPages::new().page(0, 1, 320, vec![(d2, 320)], None);
    let source = source(pages, |_| true, &[front(0, 1, 320)]).expect("source");
    let (frames, outcome) = drain(source);
    assert_eq!(sequences(&frames), vec![7]);
    assert_eq!(outcome.applied, Some(version(7)));
}

#[test]
fn invalid_pages_are_rejected() {
    // A cursor that does not advance would spin on a live owner.
    let stale = ScriptedPages::new().page(0, 1, 0, Vec::new(), Some(0));
    let error = source(stale, |_| true, &[front(0, 1, 0)])
        .err()
        .expect("non-advancing cursor must fail");
    assert!(error.to_string().contains("does not advance"));

    // A page larger than the requested limit breaks the retention bound.
    let a1 = batch(1, &[([1, 0, 0, 0], b"a1")]);
    let a2 = batch(2, &[([2, 0, 0, 0], b"a2")]);
    let overfull = ScriptedPages::new().page(0, 1, 0, vec![(a1, 0), (a2, 128)], None);
    let error = HybridReplaySource::new(overfull, |_| true, &[front(0, 1, 0)], &[(0, 1)], 1, 64)
        .err()
        .expect("overfull page must fail");
    assert!(error.to_string().contains("exceeds its limit"));

    // A batch from another fencing generation cannot be replayed under this
    // authority.
    let foreign = batch(1, &[([1, 0, 0, 0], b"a1")]);
    let wrong_epoch = ScriptedPages::new().page(0, 2, 0, vec![(foreign, 0)], None);
    let error = source(wrong_epoch, |_| true, &[front(0, 2, 0)])
        .err()
        .expect("foreign generation must fail");
    assert!(error.to_string().contains("another fencing generation"));
}

#[test]
fn non_increasing_stream_version_is_rejected() {
    let a2 = batch(2, &[([2, 0, 0, 0], b"a2")]);
    let a1 = batch(1, &[([1, 0, 0, 0], b"a1")]);
    let pages = ScriptedPages::new().page(0, 1, 0, vec![(a2, 0), (a1, 128)], None);
    let error = source(pages, |_| true, &[front(0, 1, 0)])
        .err()
        .expect("descending versions must fail");
    assert!(error.to_string().contains("not strictly ordered"));
}

#[test]
fn duplicate_version_across_owners_is_rejected() {
    let a3 = batch(3, &[([3, 0, 0, 0], b"a3")]);
    let b1 = batch(1, &[([1, 1, 0, 0], b"b1")]);
    let again = batch(3, &[([3, 1, 0, 0], b"b3")]);
    let pages = ScriptedPages::new()
        .page(0, 1, 0, vec![(a3, 0)], None)
        .page(1, 1, 0, vec![(b1, 0)], Some(128))
        .page(1, 1, 128, vec![(again, 128)], None);
    let mut source = source(pages, |_| true, &[front(0, 1, 0), front(1, 1, 0)]).expect("source");
    let mut error = None;
    loop {
        match source.next() {
            Ok(Some(_)) => {}
            Ok(None) => break,
            Err(reason) => {
                error = Some(reason);
                break;
            }
        }
    }
    assert!(
        error.is_some_and(|error| error.to_string().contains("not strictly ordered")),
        "two owners claiming one version must fail the merge"
    );
}

#[test]
fn batch_anchor_drops_the_whole_batch_at_the_cutoff() {
    // Amendment 8: the anchor sorts at member 0's order key, so the native rule
    // "drop every buffered entry at or above the cutoff" discards member 0 and
    // every later member of a partial batch, while the previous complete batch
    // survives. A `u32::MAX` anchor would sort after its own members and keep
    // them; this test fails if the sentinel comes back.
    let address = PhysicalValueAddress {
        partition: 0,
        segment_generation: 1,
        offset: 128,
        len: 64,
    };
    let descriptor = RecordDescriptor {
        operation_index: 0,
        offset: 8,
        len: 24,
        keyspace: 0,
        kind: RecordKind::Put,
        key_len: 4,
        value_offset: 14,
        value_len: 2,
        checksum: MemberChecksum(0),
    };
    let earlier = VersionPosition::member(version(1), address, 0, descriptor, 7);
    let anchor = VersionPosition::batch_start(version(2), address);
    let member0 = VersionPosition::member(version(2), address, 0, descriptor, 7);
    let member1 = VersionPosition::member(version(2), address, 1, descriptor, 7);
    assert!(earlier < anchor, "the previous batch must survive");
    assert!(anchor <= member0, "the anchor must not sort after member 0");
    assert!(member0 < member1);
    assert!(member1 >= anchor);
    assert_eq!(anchor, member0, "the anchor is logically member 0");
    assert!(
        !anchor.same_physical(&member0),
        "the anchor names the batch frame, so it is physically distinct from member 0"
    );
}

#[test]
fn finish_rejects_a_partial_batch() {
    let a1 = batch(1, &[([1, 0, 0, 0], b"a1")]);
    let pages = ScriptedPages::new().page(0, 1, 0, vec![(a1, 0)], None);
    let source = source(pages, |_| true, &[front(0, 1, 0)]).expect("source");
    let cutoff = VersionPosition::batch_start(
        version(1),
        PhysicalValueAddress {
            partition: 0,
            segment_generation: 1,
            offset: 0,
            len: 8,
        },
    );
    let error = source
        .finish(Some(cutoff))
        .err()
        .expect("a cutoff means half a batch reached the index");
    assert!(error.to_string().contains("inside a batch"));
}

#[test]
fn a_range_clear_is_one_lone_entry_and_never_a_buffered_position() {
    // The replay driver's `DropCells` arm applies by cell range and never reads
    // the frame's position or buffers it; inside a `BatchStart` group it would
    // reject the entry. So the source must yield exactly one `Entry`, with no
    // `BatchStart` before it and nothing after it, carrying the batch anchor.
    let shape = shape();
    let keyspace = shape.iter_ks().next().expect("one keyspace").id();
    let clear = CompleteBatch::new(
        &shape,
        BatchId {
            database: [7; 16],
            operation: [9; 16],
        },
        version(2),
        vec![Mutation::DropCells {
            keyspace,
            first_cell: CellId::Integer(0),
            last_cell: CellId::Integer(0),
        }],
        limits(),
    )
    .expect("range clear")
    .into_canonical_owned();
    let pages = ScriptedPages::new().page(0, 1, 0, vec![(clear, 0)], None);
    let mut source = source(pages, |_| true, &[front(0, 1, 0)]).expect("source");
    let first = source
        .next()
        .expect("first frame")
        .expect("a range clear yields a frame");
    let ReplayFrame::Entry { position, entry } = first else {
        panic!("a range clear must be a lone Entry, not a BatchStart group");
    };
    assert!(
        matches!(
            entry,
            WalEntry::DropCells(ks, from, to)
                if ks == keyspace && from == CellId::Integer(0) && to == CellId::Integer(0)
        ),
        "the entry must be the range clear"
    );
    assert_eq!(position.batch, version(2));
    assert_eq!(position.operation_index, 0, "it carries the batch anchor");
    assert!(
        source.next().expect("end of stream").is_none(),
        "no member frames follow a range clear"
    );
    let outcome = source.finish(None).expect("clean end");
    assert_eq!(outcome.applied, Some(version(2)));
}

#[test]
fn fronts_at_a_cut_below_the_head_recover_the_published_but_unstored_extent() {
    // v3 is a dirty unflushed cell's put, v4 is published but in no stored blob,
    // and the head is v5. The snapshot heads the region and drains the owner at
    // the cut (v3), so the stored front is v3's extent and recovery re-reads v3,
    // v4 and v5.
    let v3 = batch(3, &[([3, 0, 0, 0], b"v3")]);
    let v4 = batch(4, &[([4, 0, 0, 0], b"v4")]);
    let v5 = batch(5, &[([5, 0, 0, 0], b"v5")]);
    let pages = ScriptedPages::new().page(0, 1, 128, vec![(v3, 128), (v4, 320), (v5, 512)], None);
    let source = source(pages, below(6), &[front(0, 1, 128)]).expect("source");
    let (frames, outcome) = drain(source);
    assert_eq!(sequences(&frames), vec![3, 4, 5]);
    assert_eq!(outcome.applied, Some(version(5)));
}

#[test]
fn fronts_at_the_head_lose_the_extent_below_it() {
    // Negative control for the test above: the unfixed pass drained at the head
    // (v5), so the stored front started past v3 and v4 and recovery never saw
    // them — the dirty cell's put and the unstored extent were lost.
    let v5 = batch(5, &[([5, 0, 0, 0], b"v5")]);
    let pages = ScriptedPages::new().page(0, 1, 512, vec![(v5, 512)], None);
    let source = source(pages, below(6), &[front(0, 1, 512)]).expect("source");
    let (frames, _) = drain(source);
    assert_eq!(sequences(&frames), vec![5]);
    assert!(
        !sequences(&frames)
            .iter()
            .any(|sequence| *sequence == 3 || *sequence == 4),
        "a front at the head cannot recover anything below it"
    );
}

/// A relocation copy: a durable owner batch that consumed an allocator version
/// and was never published.
fn relocated_batch(sequence: u64, keys: &[([u8; 4], &[u8])]) -> CompleteBatch {
    let shape = shape();
    let keyspace = shape.iter_ks().next().expect("one keyspace").id();
    let mutations = keys
        .iter()
        .map(|(key, value)| Mutation::Put {
            keyspace,
            key: key.to_vec().into(),
            value: value.to_vec().into(),
        })
        .collect();
    CompleteBatch::new_relocated(
        &shape,
        BatchId {
            database: [7; 16],
            operation: [sequence as u8; 16],
        },
        version(sequence),
        mutations,
        limits(),
    )
    .expect("valid relocated batch")
    .into_canonical_owned()
}

/// Folds the frames the index would receive into "which version wrote each key
/// last", the way the replay buffer resolves overwrites (ascending order, the
/// later position wins).
fn last_writers(
    source: impl ReplaySource<VersionPosition, Output = ReplayOutcome>,
) -> (HashMap<Vec<u8>, BatchVersion>, ReplayOutcome) {
    let mut source = source;
    let mut writers = HashMap::new();
    while let Some(frame) = source.next().expect("scripted source cannot fail") {
        if let ReplayFrame::Entry {
            position,
            entry: WalEntry::Record(_, key, _, _),
        } = frame
        {
            writers.insert(key.to_vec(), position.batch);
        }
    }
    (writers, source.finish(None).expect("clean end"))
}

const KEY: [u8; 4] = [1, 0, 0, 0];

#[test]
fn a_relocated_copy_never_overrides_a_newer_user_write() {
    // K is written at v1, overwritten by the user at vu = 3, and relocation
    // then copies the *old* value to a new place as a durable batch at
    // vR = 5. After a crash the index is rebuilt from the owner WALs: if the
    // copy were replayed it would out-order the user's write and resurrect the
    // stale value. It must be skipped.
    let original = batch(1, &[(KEY, b"old")]);
    let user = batch(3, &[(KEY, b"new")]);
    let copy = relocated_batch(5, &[(KEY, b"old")]);
    let pages =
        ScriptedPages::new().page(0, 1, 0, vec![(original, 0), (user, 128), (copy, 256)], None);
    let source = source(pages, |_| true, &[front(0, 1, 0)]).expect("source");
    let (writers, outcome) = last_writers(source);
    assert_eq!(
        writers.get(&KEY.to_vec()),
        Some(&version(3)),
        "the user's write at vu must stay the winner"
    );
    assert_eq!(outcome.relocated_skipped, 1);
    assert_eq!(outcome.merged, 3);
    // The copy's version is a processed no-op: the cut moves past it instead of
    // stalling at the last published batch.
    assert_eq!(outcome.applied, Some(version(5)));
}

#[test]
fn without_the_relocated_flag_the_stale_copy_resurrects() {
    // Negative control for the test above: the same content at the same version
    // as an ordinary batch is applied, and out-orders the user's write. The
    // `is_relocated` skip is the only thing that prevents it.
    let original = batch(1, &[(KEY, b"old")]);
    let user = batch(3, &[(KEY, b"new")]);
    let copy = batch(5, &[(KEY, b"old")]);
    let pages =
        ScriptedPages::new().page(0, 1, 0, vec![(original, 0), (user, 128), (copy, 256)], None);
    let source = source(pages, |_| true, &[front(0, 1, 0)]).expect("source");
    let (writers, outcome) = last_writers(source);
    assert_eq!(writers.get(&KEY.to_vec()), Some(&version(5)));
    assert_eq!(outcome.relocated_skipped, 0);
}

#[test]
fn a_relocated_copy_the_filter_refuses_is_discarded_not_skipped() {
    // Version B decides first: a relocated copy above the acknowledged prefix is
    // a discarded batch, so it neither counts as a skip nor advances `applied`.
    let user = batch(3, &[(KEY, b"new")]);
    let copy = relocated_batch(5, &[(KEY, b"old")]);
    let pages = ScriptedPages::new().page(0, 1, 0, vec![(user, 0), (copy, 128)], None);
    let source = source(pages, below(5), &[front(0, 1, 0)]).expect("source");
    let (writers, outcome) = last_writers(source);
    assert_eq!(writers.get(&KEY.to_vec()), Some(&version(3)));
    assert_eq!(outcome.discarded, 1);
    assert_eq!(outcome.relocated_skipped, 0);
    assert_eq!(outcome.applied, Some(version(3)));
}
