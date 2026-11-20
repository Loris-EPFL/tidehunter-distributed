//! Cut fronts, allocation tracking and the hybrid [`Loader`] implementation.
//!
//! Convention (A13): cuts are exclusive (`below(cut)` is `version < cut`),
//! starts are inclusive and ends are exclusive.
//!
//! Native Tidehunter derives its index frontier from local `WalGuard`s: every
//! position below `WalTracker::last_processed` is in the index. A remote
//! append never touches the coordinator's value WAL, so the hybrid tracks the
//! same fact with three independent cut fronts (plan invariant I2):
//!
//! * [`CutFront`] — the Version B durable cut: every batch below it is owner
//!   synced (or aborted).
//! * [`AppliedFront`] — the contiguous index-applied cut, advanced only after
//!   every member of the batch below it has been inserted into `LargeTable`.
//! * [`PublishedFront`] — the publication watermark (`current`).
//!
//! The flush target is `min(durable, applied, lowest held latch)`. The durable
//! cut alone is not sufficient: Version B's prefix advances on owner fsyncs,
//! possibly before the index has inserted every batch at or below it, so
//! flushing at the durable cut would persist a replay point above versions the
//! index never applied.
//!
//! The fronts are lock-free for readers: each is a seqlock over the 16 B cut
//! pair (two `AtomicU64`s plus a sequence marker and a writer flag). `Loader::last_processed` runs at
//! least once per index insert, so a mutex there would serialize the put path;
//! writers take a short spin lock and are per-batch, not per-put. The memory
//! ordering is the classic seqlock contract: the writer publishes an odd
//! sequence before touching the fields and an even one with `Release` after;
//! readers load the sequence with `Acquire` before and after the fields and
//! retry on a mismatch, so they never observe a torn `(epoch, sequence)`.

use super::position::VersionPosition;
use super::region::StreamFront;
use crate::context::{KsContextVec, WalEntryKind};
use crate::db::WalEntry;
use crate::distributed::durability::DurabilityTier;
use crate::distributed::storage::LocalDbError;
use crate::distributed::types::BatchVersion;
use crate::index::index_format::IndexFormat;
use crate::index::index_table::IndexTable;
use crate::key_shape::{KeySpace, KeySpaceDesc};
use crate::large_table::Loader;
use crate::wal::position::{IndexCut, WalPosition};
use crate::wal::{PreparedWalWrite, Wal, WalError, WalRandomRead, WalWriter};
use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};

/// A cut cell with lock-free reads.
///
/// The cut fields are the plan's 16 B `(epoch, sequence)` pair; the marker and
/// writer flag are control state. The marker is `u64`, so ABA needs 2^63
/// updates. `get` has no backoff beyond `spin_loop`: the writer owns the cell
/// for exactly two field stores, so yielding would only add put-path latency.
#[derive(Debug)]
struct CutCell {
    /// Even when stable, odd while a writer is publishing. Bumped by two on
    /// every store.
    sequence_marker: AtomicU64,
    epoch: AtomicU64,
    sequence: AtomicU64,
    /// Writer exclusion. Readers never take it.
    writer: AtomicBool,
}

/// Releases the writer lock even if the critical section panics.
struct WriterGuard<'a>(&'a CutCell);

impl Drop for WriterGuard<'_> {
    fn drop(&mut self) {
        // Release: the published fields are visible before the lock is free.
        self.0.writer.store(false, Ordering::Release);
    }
}

impl CutCell {
    const fn new(initial: BatchVersion) -> Self {
        Self {
            sequence_marker: AtomicU64::new(0),
            epoch: AtomicU64::new(initial.epoch),
            sequence: AtomicU64::new(initial.sequence),
            writer: AtomicBool::new(false),
        }
    }

    fn get(&self) -> BatchVersion {
        loop {
            let before = self.sequence_marker.load(Ordering::Acquire);
            if before & 1 == 0 {
                // Order the field loads after the marker load (seqlock
                // reader barrier); pairs with the writer's releasing fence
                // before its field stores.
                std::sync::atomic::fence(Ordering::Acquire);
                let epoch = self.epoch.load(Ordering::Relaxed);
                let sequence = self.sequence.load(Ordering::Relaxed);
                std::sync::atomic::fence(Ordering::Acquire);
                // `Acquire` pairs with the writer's releasing store: a stable
                // marker means both fields were loaded from one publication.
                if self.sequence_marker.load(Ordering::Acquire) == before {
                    return BatchVersion { epoch, sequence };
                }
            }
            std::hint::spin_loop();
        }
    }

    /// Publishes `cut`. The caller must hold the writer lock.
    fn write_locked(&self, cut: BatchVersion) {
        self.write_with_marker(cut, || {});
    }

    fn write_with_marker(&self, cut: BatchVersion, between: impl FnOnce()) {
        // Odd marker: readers retry while the fields change.
        self.sequence_marker.fetch_add(1, Ordering::Relaxed);
        // Release fence: the field stores below cannot become visible before
        // the odd marker (seqlock writer barrier).
        std::sync::atomic::fence(Ordering::Release);
        between();
        self.epoch.store(cut.epoch, Ordering::Relaxed);
        self.sequence.store(cut.sequence, Ordering::Relaxed);
        // Even, with release: readers that observe it also observe the fields.
        self.sequence_marker.fetch_add(1, Ordering::Release);
    }

    /// Test-only store that pauses between the odd marker and the fields.
    #[cfg(test)]
    fn store_with_pause(&self, cut: BatchVersion, pause: impl FnOnce()) {
        let _guard = self.lock_writer();
        self.write_with_marker(cut, pause);
    }

    /// Test-only marker value, to prove a skipped advance publishes nothing.
    #[cfg(test)]
    fn marker(&self) -> u64 {
        self.sequence_marker.load(Ordering::Acquire)
    }

    fn lock_writer(&self) -> WriterGuard<'_> {
        while self.writer.swap(true, Ordering::Acquire) {
            std::hint::spin_loop();
        }
        WriterGuard(self)
    }

    /// Monotone max update; returns whether the cut advanced.
    fn advance(&self, cut: BatchVersion) -> bool {
        if cut <= self.get() {
            return false;
        }
        let _guard = self.lock_writer();
        let current = BatchVersion {
            epoch: self.epoch.load(Ordering::Relaxed),
            sequence: self.sequence.load(Ordering::Relaxed),
        };
        let advanced = cut > current;
        if advanced {
            self.write_locked(cut);
        }
        advanced
    }

    /// Non-monotone replacement, used only by the latch mirror where releasing
    /// a checkpoint raises the effective minimum.
    fn store(&self, cut: BatchVersion) {
        let _guard = self.lock_writer();
        self.write_locked(cut);
    }
}

macro_rules! cut_front {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Debug)]
        pub(crate) struct $name(CutCell);

        impl $name {
            pub(crate) const fn new(initial: BatchVersion) -> Self {
                Self(CutCell::new(initial))
            }

            pub(crate) fn get(&self) -> BatchVersion {
                self.0.get()
            }

            pub(crate) fn advance(&self, cut: BatchVersion) -> bool {
                self.0.advance(cut)
            }
        }
    };
}

cut_front! {
    /// Version B durable cut: every batch below it is owner synced or aborted.
    CutFront
}
cut_front! {
    /// Contiguous index-applied cut: every published batch below it is fully
    /// inserted into `LargeTable`; gaps from discarded groups are skipped.
    AppliedFront
}
cut_front! {
    /// Publication watermark: every batch below it is visible.
    PublishedFront
}

/// Held checkpoint cuts (plan invariant I6).
///
/// A read served from a cut must keep that cut answerable, so promotion may
/// not pass it. `pin` is called when a checkpoint or iterator view is created
/// (cold path); `Loader::last_processed` reads the mirror lock-free on every
/// index insert.
#[derive(Debug)]
pub(crate) struct CutLatches {
    state: Mutex<LatchState>,
    /// Mirror of `state.held.values().min()`, or `NO_LATCH` when nothing is
    /// held; read lock-free through the seqlock (`CutCell::get`).
    min: CutCell,
}

/// "No latch is held", a value no real cut can take (a version's epoch is a
/// fencing generation, far below `u64::MAX`).
const NO_LATCH: BatchVersion = BatchVersion {
    epoch: u64::MAX,
    sequence: u64::MAX,
};

#[derive(Debug, Default)]
struct LatchState {
    next_id: u64,
    held: BTreeMap<u64, BatchVersion>,
}

impl CutLatches {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(LatchState::default()),
            min: CutCell::new(NO_LATCH),
        })
    }

    /// Pins `cut` until the returned latch is dropped.
    pub(crate) fn pin(self: &Arc<Self>, cut: BatchVersion) -> CutLatch {
        let mut state = self.state.lock();
        let id = state.next_id;
        state.next_id = state.next_id.wrapping_add(1);
        state.held.insert(id, cut);
        self.publish_min(&state);
        CutLatch {
            id,
            latches: Arc::clone(self),
        }
    }

    /// `None` when no latch is held; otherwise the lowest held cut.
    pub(crate) fn min_held(&self) -> Option<BatchVersion> {
        // The reader half of the pair documented at `publish_min`: it runs after
        // the caller's loads of `applied` and `durable`.
        std::sync::atomic::fence(Ordering::SeqCst);
        let min = self.min.get();
        (min != NO_LATCH).then_some(min)
    }

    fn release(&self, id: u64) {
        let mut state = self.state.lock();
        state.held.remove(&id);
        self.publish_min(&state);
    }

    fn publish_min(&self, state: &LatchState) {
        // Lock order: `state` then the cut-cell writer. Readers take neither.
        // "No latch" is a sentinel inside the same seqlocked cell as the minimum,
        // so a reader's single consistent read can never pair a stale count with
        // a new minimum (two separate loads could, in either store order). A
        // latch at NONE stays representable: NONE != NO_LATCH.
        let min = state.held.values().copied().min().unwrap_or(NO_LATCH);
        self.min.store(min);
        // SeqCst fence, the writer half of a store-then-load pair (Dekker): a
        // pin stores its latch here and then samples the publication watermark,
        // while a flusher reads `applied`/`durable` and then the latch mirror
        // (`min_held`, which fences the same way). With a SeqCst fence on both
        // sides at least one of them observes the other: either the flusher sees
        // the latch and clamps, or the pin's sample is at or above everything
        // the flusher read. Release/Acquire alone permits both to miss (store
        // buffering), which is the race the NONE latch exists to close. Main's
        // decision: not a documented architecture assumption.
        std::sync::atomic::fence(Ordering::SeqCst);
    }
}

/// A held cut. Dropping it releases the clamp.
#[derive(Debug)]
pub(crate) struct CutLatch {
    id: u64,
    latches: Arc<CutLatches>,
}

impl Drop for CutLatch {
    fn drop(&mut self) {
        self.latches.release(self.id);
    }
}

/// Monotone publication byte counter.
///
/// `current = Σ per-stream owner tails`; publication advances it with the
/// value bytes it just published. The byte cadence uses it only for pacing
/// (root trigger and staleness); it never decides what is durable or what the
/// index may flush.
#[derive(Debug)]
pub(crate) struct ValueBytes {
    current: AtomicU64,
}

impl ValueBytes {
    pub(crate) const fn new(initial: u64) -> Self {
        Self {
            current: AtomicU64::new(initial),
        }
    }

    pub(crate) fn get(&self) -> u64 {
        self.current.load(Ordering::Acquire)
    }

    /// Adds published value bytes; returns the previous value.
    pub(crate) fn add(&self, bytes: u64) -> u64 {
        self.current.fetch_add(bytes, Ordering::AcqRel)
    }
}

/// The cut sources one hybrid database shares between publication, the loader,
/// the flusher and the snapshot pass.
#[derive(Clone, Debug)]
pub(crate) struct CutSources {
    durable: Arc<CutFront>,
    applied: Arc<AppliedFront>,
    published: Arc<PublishedFront>,
    latches: Arc<CutLatches>,
    bytes: Arc<ValueBytes>,
    tier: DurabilityTier,
}

impl CutSources {
    /// A fresh serving epoch. All cuts start at `(epoch, 0)`, below every real
    /// batch of the epoch.
    pub(crate) fn new(epoch: u64, tier: DurabilityTier) -> Self {
        debug_assert!(epoch != 0, "epoch 0 is the reserved NONE cut");
        let start = BatchVersion { epoch, sequence: 0 };
        Self {
            durable: Arc::new(CutFront::new(start)),
            applied: Arc::new(AppliedFront::new(start)),
            published: Arc::new(PublishedFront::new(start)),
            latches: CutLatches::new(),
            bytes: Arc::new(ValueBytes::new(0)),
            tier,
        }
    }

    /// Version B durable cut (cold paths: the durable-cut actor and open).
    pub(crate) fn set_durable(&self, cut: BatchVersion) {
        self.durable.advance(cut);
    }

    /// Contiguous index-applied cut (cold path: publication completion).
    pub(crate) fn set_applied(&self, cut: BatchVersion) {
        self.applied.advance(cut);
    }

    /// Publication watermark (publication path).
    pub(crate) fn set_published(&self, cut: BatchVersion) {
        self.published.advance(cut);
    }

    /// Publication advances the value-byte counter by the bytes it published.
    pub(crate) fn add_bytes(&self, bytes: u64) {
        self.bytes.add(bytes);
    }

    /// Pins a cut for a read view (plan invariant I6).
    pub(crate) fn pin_cut(&self, cut: BatchVersion) -> CutLatch {
        self.latches.pin(cut)
    }

    pub(crate) fn durable(&self) -> BatchVersion {
        self.durable.get()
    }

    pub(crate) fn applied(&self) -> BatchVersion {
        self.applied.get()
    }

    pub(crate) fn published(&self) -> BatchVersion {
        self.published.get()
    }

    pub(crate) fn lowest_held(&self) -> Option<BatchVersion> {
        self.latches.min_held()
    }

    pub(crate) fn bytes(&self) -> u64 {
        self.bytes.get()
    }

    pub(crate) fn tier(&self) -> DurabilityTier {
        self.tier
    }
}

/// `min(durable, applied, lowest held latch)` (plan invariant I2/I6).
pub(crate) fn compose_last_processed(
    durable: BatchVersion,
    applied: BatchVersion,
    held: Option<BatchVersion>,
) -> BatchVersion {
    let mut cut = durable.min(applied);
    if let Some(held) = held {
        cut = cut.min(held);
    }
    cut
}

/// The monotone stored target (Amendment 9).
///
/// The stored target is `max(previous, min(durable, applied, lowest held
/// latch))`, where `previous` is the cut the stored control region names
/// (`HybridTable::stored_cut`), not the head of the live index: a held
/// checkpoint taken below the region cut, or a replayed durable cut that is
/// still behind, must never lower what the last snapshot already persisted. An
/// unchanged target makes the store a no-op (the previous region and
/// `replay_from` are kept).
pub(crate) fn next_stored_target(
    previous: BatchVersion,
    durable: BatchVersion,
    applied: BatchVersion,
    held: Option<BatchVersion>,
) -> BatchVersion {
    previous.max(compose_last_processed(durable, applied, held))
}

/// `WalError` implements `Debug` only; render it with the failing operation.
fn index_wal_error(operation: &str, error: WalError) -> LocalDbError {
    LocalDbError::new(format!("index WAL {operation}: {error:?}"))
}

/// The hybrid [`Loader`]: the coordinator's local index WAL plus the cut fronts.
pub(crate) struct HybridLoader {
    index_wal: Arc<Wal>,
    index_writer: Arc<WalWriter>,
    contexts: Arc<KsContextVec>,
    cuts: CutSources,
}

impl HybridLoader {
    pub(crate) fn new(
        index_wal: Arc<Wal>,
        index_writer: Arc<WalWriter>,
        contexts: Arc<KsContextVec>,
        cuts: CutSources,
    ) -> Self {
        Self {
            index_wal,
            index_writer,
            contexts,
            cuts,
        }
    }

    /// Release check: every batch below `cut` is owner synced.
    ///
    /// Version B establishes the durable cut before publication acknowledges
    /// the tier, and the snapshot performs no owner RPC. A cut above the
    /// durable front therefore means the caller asked to persist a version the
    /// owners have not synced; report it as an invalid request (the leased
    /// engine's receipt path records the outcome; this check never retries).
    pub(crate) fn ensure_durable_target(&self, cut: BatchVersion) -> Result<(), LocalDbError> {
        let durable = self.cuts.durable();
        if cut <= durable {
            return Ok(());
        }
        Err(LocalDbError::with_kind(
            std::io::ErrorKind::InvalidInput,
            format!(
                "flush target {cut:?} is above the durable cut {durable:?}: \
                 the owners have not synced every batch below the target"
            ),
        ))
    }

    pub(crate) fn index_wal(&self) -> &Arc<Wal> {
        &self.index_wal
    }

    pub(crate) fn index_writer(&self) -> &Arc<WalWriter> {
        &self.index_writer
    }
}

impl Loader for HybridLoader {
    type Position = VersionPosition;
    type Error = LocalDbError;

    /// The value-byte counter at a cut (I2b): the hybrid cadence compares byte
    /// distances, not version distances, so the native `cut.byte_position()`
    /// (always 0 for a version) must never be used here.
    fn flushed_at_bytes(&self, _cut: BatchVersion) -> u64 {
        self.cuts.bytes()
    }

    /// The insertion bound read on a cell's `Empty` -> dirty edge: the contiguous
    /// index-applied prefix (`applied`), not `last_processed`, which a `NONE`
    /// latch can pull to `NONE`. One seqlocked cut read, under the row lock and
    /// before the insert; the publisher advances `applied` under the lock that
    /// serialises all inserts of a pass, so it cannot pass an in-flight lower
    /// write between this read and the insert.
    fn insert_anchor_bound(&self) -> BatchVersion {
        self.cuts.applied()
    }

    fn load(
        &self,
        ks: &KeySpaceDesc,
        position: WalPosition,
    ) -> Result<IndexTable<VersionPosition>, LocalDbError> {
        let (read_type, frame) = self
            .index_wal
            .read(position)
            .map_err(|error| index_wal_error("read", error))?;
        let frame = frame.ok_or_else(|| {
            LocalDbError::new(format!(
                "index blob at index WAL offset {} was reclaimed",
                position.offset()
            ))
        })?;
        // The frame still carries the `[WAL_ENTRY_INDEX][keyspace]` prefix:
        // decode it exactly as native `Db::read_entry` does, so the index
        // deserializer sees only the blob (`index_reader` skips the same prefix).
        let frame_len = frame.len() as u64;
        match WalEntry::from_bytes(frame) {
            WalEntry::Index(id, blob) if id == ks.id() => {
                // Same per-keyspace read counters native `Db::report_read` bumps
                // for an index entry, so hybrid index loads are not invisible.
                let context = self.contexts.ks_context(id);
                context.inc_read(WalEntryKind::Index, read_type);
                context.inc_read_bytes(WalEntryKind::Index, read_type, frame_len);
                Ok(ks.index_format().deserialize_index(ks, blob))
            }
            _ => Err(LocalDbError::new(format!(
                "index WAL offset {} does not hold an index entry of keyspace {}",
                position.offset(),
                ks.name()
            ))),
        }
    }

    fn index_reader(&self, position: WalPosition) -> Result<WalRandomRead, LocalDbError> {
        self.index_wal
            .random_reader_at(position, WalEntry::INDEX_PREFIX_SIZE)
            .map_err(|error| index_wal_error("random reader", error))
    }

    fn flush_supported(&self) -> bool {
        true
    }

    fn flush(
        &self,
        ks: KeySpace,
        data: &IndexTable<VersionPosition>,
    ) -> Result<WalPosition, LocalDbError> {
        let context = self.contexts.ks_context(ks);
        let name = context.name();
        context.metrics.flush_count.with_label_values(&[name]).inc();
        context
            .metrics
            .flushed_keys
            .with_label_values(&[name])
            .inc_by(data.len() as u64);
        let index = context
            .ks_config
            .index_format()
            .serialize_index(data, &context.ks_config);
        context
            .metrics
            .flushed_bytes
            .with_label_values(&[name])
            .inc_by(index.len() as u64);
        let write = PreparedWalWrite::new(&WalEntry::Index(ks, index));
        context.inc_wal_written(WalEntryKind::Index, write.len() as u64);
        let guard = self
            .index_writer
            .write(&write)
            .map_err(|error| index_wal_error("write", error))?;
        Ok(*guard.wal_position())
    }

    fn last_processed(&self) -> BatchVersion {
        compose_last_processed(
            self.cuts.durable(),
            self.cuts.applied(),
            self.cuts.lowest_held(),
        )
    }

    fn current(&self) -> BatchVersion {
        self.cuts.published()
    }

    /// Native `min_wal_position`: entries whose position is below it name a
    /// reclaimed WAL byte and are dropped by the flusher's L1 clean-up
    /// (`retain_above_position`). The hybrid never drops an index entry by
    /// version: a value's owner file is retired only after relocation re-pointed
    /// every live entry, so nothing in the index is below any floor and this is
    /// `NONE`, for which `below` is false for every position. Any other value
    /// would make the flusher delete live entries (e.g. every earlier-epoch
    /// entry for a floor at the serving epoch's first version).
    fn min_position(&self) -> BatchVersion {
        <BatchVersion as IndexCut>::NONE
    }

    fn current_position(&self) -> VersionPosition {
        VersionPosition::tombstone(self.current())
    }
}

/// Sentinel for an unset offset: an empty stream's front, or a cleared
/// dropped floor.
pub(crate) const UNSET_OFFSET: u64 = u64::MAX;

/// Per-stream allocation queue capacity for one snapshot interval.
///
/// The queue spans the whole snapshot interval (there is no continuous
/// drain), so the capacity is `ceil(snapshot_written_bytes /
/// min_batch_frame_bytes) + margin`: at Sui's 8 GiB root cadence and a ~20 KB
/// batch frame that is ≈4.2×10^5 extents ≈ 9.6 MB per stream, ≈154 MB at 16
/// owners. If the coordinator memory budget does not allow it, lower the
/// hybrid's `snapshot_written_bytes`; never shrink the cap below one interval.
pub(crate) fn allocation_capacity(
    snapshot_written_bytes: u64,
    min_batch_frame_bytes: u64,
    margin: usize,
) -> NonZeroUsize {
    debug_assert!(min_batch_frame_bytes > 0, "a frame is at least one byte");
    let extents = snapshot_written_bytes.div_ceil(min_batch_frame_bytes.max(1));
    let capacity = extents.saturating_add(margin as u64);
    NonZeroUsize::new(capacity.min(usize::MAX as u64) as usize).unwrap_or(NonZeroUsize::MIN)
}

/// One stream's published allocation watermarks.
///
/// Only the owner worker writes these atomics; the snapshot reads them. The
/// extent queue, its `drain_to(target)` and the `dropped_max_version`
/// bookkeeping are worker-local in S7, not here. `front` is the minimum
/// outstanding offset, or [`UNSET_OFFSET`] when nothing is outstanding; `tail`
/// is the highest published end; `dropped_floor` is the minimum offset the
/// worker dropped at its queue cap; `outstanding` is metrics/evidence only and
/// is not part of the reader's gate.
#[derive(Debug)]
struct StreamState {
    /// Published partition id; initialised to the dense slot index and
    /// replaced by `publish_partition` with the real owner id.
    partition: AtomicU32,
    front: AtomicU64,
    tail: AtomicU64,
    dropped_floor: AtomicU64,
    outstanding: AtomicU32,
    /// Fencing generation of this stream (immutable while serving).
    generation: AtomicU64,
}

impl StreamState {
    fn new(partition: u32) -> Self {
        Self {
            partition: AtomicU32::new(partition),
            front: AtomicU64::new(UNSET_OFFSET),
            tail: AtomicU64::new(0),
            dropped_floor: AtomicU64::new(UNSET_OFFSET),
            outstanding: AtomicU32::new(0),
            generation: AtomicU64::new(0),
        }
    }
}

/// The publish-side allocation view shared by the owner workers and the
/// snapshot pass.
///
/// `fronts()` is `min(front, tail, dropped_floor)` per stream. Independent
/// atomic stores are safe: the reader takes `tail` first, so a tail load that
/// observes a new reservation is followed by loads that observe its front
/// store (the worker stores front before tail); an older/stale front or floor
/// only makes the result older, never newer. `outstanding` is not a gate: a
/// stale zero with a new tail would return the tail above an allocated but
/// unapplied extent.
#[derive(Debug)]
pub(crate) struct AllocationView {
    streams: Box<[StreamState]>,
}

impl AllocationView {
    /// `streams` is the number of dense partition streams at open.
    pub(crate) fn new(streams: usize) -> Self {
        assert!(
            streams <= u32::MAX as usize,
            "more allocation streams than partitions can represent",
        );
        Self {
            streams: (0..streams)
                .map(|index| StreamState::new(u32::try_from(index).unwrap_or(u32::MAX)))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        }
    }

    pub(crate) fn streams(&self) -> usize {
        self.streams.len()
    }

    /// Publishes the highest written end. The worker is the sole writer, so
    /// an absolute store is enough; `Release` orders it after the front store
    /// a reader relies on.
    pub(crate) fn publish_tail(&self, stream: u32, end: u64) {
        self.state(stream).tail.store(end, Ordering::Release);
    }

    /// Publishes the minimum outstanding offset, or `None` when empty.
    pub(crate) fn publish_front(&self, stream: u32, front: Option<u64>) {
        self.state(stream)
            .front
            .store(front.unwrap_or(UNSET_OFFSET), Ordering::Release);
    }

    /// Publishes the queue length for evidence (not a correctness gate).
    pub(crate) fn publish_outstanding(&self, stream: u32, outstanding: u32) {
        self.state(stream)
            .outstanding
            .store(outstanding, Ordering::Release);
    }

    /// Publishes the lowest offset dropped at the worker's queue cap; pass
    /// [`UNSET_OFFSET`] to clear it.
    pub(crate) fn publish_dropped_floor(&self, stream: u32, offset: u64) {
        self.state(stream)
            .dropped_floor
            .store(offset, Ordering::Release);
    }

    /// Publishes the owner fencing generation of this stream.
    pub(crate) fn publish_generation(&self, stream: u32, generation: u64) {
        self.state(stream)
            .generation
            .store(generation, Ordering::Release);
    }

    /// Publishes the real owner partition id of this stream (the dense slot
    /// index is only the initial value; partition ids are arbitrary `u32`s).
    pub(crate) fn publish_partition(&self, stream: u32, partition: u32) {
        self.state(stream)
            .partition
            .store(partition, Ordering::Release);
    }

    /// The per-stream replay table for a snapshot (plan invariant I3/I4).
    ///
    /// Caller contract: `front` is [`UNSET_OFFSET`] **iff** the worker's queue
    /// is empty, and `dropped_floor` is cleared only after every skipped
    /// extent is represented (the worker publishes a front at or below the
    /// skipped offset before clearing the floor). Under that contract
    /// `min(front, tail, dropped_floor) <=` every unrepresented offset. If the
    /// caller publishes an unset front while its queue is non-empty, the
    /// minimum degenerates to the tail and can skip an unapplied extent; the
    /// `ExtentQueue` must never do that.
    pub(crate) fn fronts(&self) -> Vec<StreamFront> {
        self.streams
            .iter()
            .map(|stream| {
                // Tail first: a new tail means the worker's preceding front
                // store is visible. An older front only widens replay.
                let tail = stream.tail.load(Ordering::Acquire);
                let front = stream.front.load(Ordering::Acquire);
                let dropped = stream.dropped_floor.load(Ordering::Acquire);
                let generation = stream.generation.load(Ordering::Acquire);
                let partition = stream.partition.load(Ordering::Acquire);
                StreamFront {
                    partition,
                    generation,
                    replay_from: front.min(tail).min(dropped),
                }
            })
            .collect()
    }

    fn state(&self, stream: u32) -> &StreamState {
        &self.streams[stream as usize]
    }

    /// Raw `(tail, front, dropped_floor, outstanding)` for tests that need to
    /// model a reader's interleaving.
    #[cfg(test)]
    pub(crate) fn raw(&self, stream: u32) -> (u64, u64, u64, u32) {
        let state = self.state(stream);
        (
            state.tail.load(Ordering::Acquire),
            state.front.load(Ordering::Acquire),
            state.dropped_floor.load(Ordering::Acquire),
            state.outstanding.load(Ordering::Acquire),
        )
    }
}

#[cfg(test)]
mod tests;
