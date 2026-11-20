//! Worker-local per-stream extent queue behind the native index's replay table.
//!
//! The coordinator's index loader needs one number per owner stream (plan
//! invariant I3/I4): the owner byte offset at which recovery must start
//! replaying that stream. That is the oldest *allocated* extent the index has not
//! represented yet, or the stream tail when none is outstanding, further clamped
//! by any extent the worker could not queue at its cap.
//!
//! Ownership: exactly one owner worker appends this stream's frames, so it alone
//! owns the queue, its cap accounting and the drain; the snapshot thread only
//! reads the atomics in [`AllocationView`]. There is no lock and no shared
//! collection.
//!
//! Ordering (`AllocationView::fronts` takes `tail` first, then `front` and
//! `dropped_floor`, all `Acquire`): a new `tail` is published only after the
//! `front`/`dropped_floor` store a reader needs, so a snapshot that observes the
//! new tail also observes a start at or below the extent just reserved — it can
//! therefore never step over an allocated batch. `outstanding` is evidence only
//! and never gates the result.
//!
//! Drain: a snapshot sends one `DrainTo(stored_target)` per real store (never on
//! the write path). Every batch strictly below the target is represented by the
//! region that store writes, so its extent no longer needs replay; the head left
//! behind is therefore `>= target` by construction. An extent dropped at the cap
//! keeps its floor until the target passes its version, because a min-version
//! release would expose a later dropped extent that is still unrepresented.

use crate::distributed::hybrid::loader::{AllocationView, UNSET_OFFSET};
use crate::distributed::types::BatchVersion;
use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::sync::Arc;

/// One published extent: the start of the member frame of one batch in this
/// stream's value WAL. The queue's tail carries the end.
#[derive(Clone, Copy, Debug)]
struct Extent {
    version: BatchVersion,
    offset: u64,
}

/// One owner worker's offset-ordered allocation queue.
pub(super) struct ExtentQueue {
    view: Arc<AllocationView>,
    stream: u32,
    /// Hard bound on `pending.len()`. Sized from the snapshot interval by
    /// [`crate::distributed::hybrid::loader::allocation_capacity`]: the queue
    /// spans exactly one store interval, so a genuine cap hit is evidence that
    /// the index is falling behind the owners, never a silent loss (the dropped
    /// floor keeps recovery correct).
    capacity: NonZeroUsize,
    /// Extents `>=` the last drained target, in offset order.
    pending: VecDeque<Extent>,
    /// Lowest offset dropped at the cap; worker-local until published.
    dropped_min_offset: u64,
    /// Highest version dropped at the cap. The floor may be released only once
    /// the stored target passes it.
    dropped_max_version: Option<BatchVersion>,
    cap_hits: u64,
}

impl ExtentQueue {
    /// Opens an empty queue for one dense partition stream.
    ///
    /// `partition` is the owner's real partition id (not this dense slot index)
    /// and `generation` its fencing generation: both are immutable while the
    /// epoch serves, so they are published once here and every later `fronts()`
    /// carries them. A stored replay table must name the owner, not a slot.
    ///
    /// The queue grows to its cap only when the index falls a whole snapshot
    /// interval behind, so an idle owner holds no capacity at all.
    pub(super) fn new(
        view: Arc<AllocationView>,
        stream: u32,
        partition: u32,
        generation: u64,
        capacity: NonZeroUsize,
    ) -> Self {
        debug_assert!(
            (stream as usize) < view.streams(),
            "stream {stream} is not in the allocation view",
        );
        view.publish_partition(stream, partition);
        view.publish_generation(stream, generation);
        Self {
            view,
            stream,
            capacity,
            pending: VecDeque::new(),
            dropped_min_offset: UNSET_OFFSET,
            dropped_max_version: None,
            cap_hits: 0,
        }
    }

    /// Records one allocated batch extent, in the order the frames were written.
    ///
    /// `version` is the batch's logical version and `[offset, end)` its member
    /// frame in the owner's value WAL. The reservation is published even when
    /// the queue is at its cap: the tail must describe the bytes that exist.
    pub(super) fn reserve(&mut self, version: BatchVersion, offset: u64, end: u64) {
        if self.pending.len() < self.capacity.get() {
            let was_empty = self.pending.is_empty();
            self.pending.push_back(Extent { version, offset });
            if was_empty {
                // Publish the start before the end: a reader that sees the new
                // tail must also see this store. Unconditional on the floor: the
                // front is the queue's head, and an over-wide front (the floor
                // below it) only costs replay time, while a missing front would
                // let `fronts()` narrow to the tail above this extent.
                self.publish_head();
            }
            debug_assert_eq!(
                self.pending.is_empty(),
                self.pending.front().is_none(),
                "queue emptiness and head disagree",
            );
            self.view
                .publish_outstanding(self.stream, len_u32(self.pending.len()));
            self.view.publish_tail(self.stream, end);
            return;
        }
        // At the cap: keep the oldest head, remember the lowest dropped offset
        // and the highest dropped version, then move the tail. The snapshot
        // replays from the floor, which is idempotent by version, so the extent
        // is recovered even though it was not queued.
        self.dropped_min_offset = self.dropped_min_offset.min(offset);
        self.dropped_max_version = Some(match self.dropped_max_version {
            Some(current) => current.max(version),
            None => version,
        });
        self.view
            .publish_dropped_floor(self.stream, self.dropped_min_offset);
        self.view.publish_tail(self.stream, end);
        self.cap_hits += 1;
    }

    /// Drops every queued extent strictly below the snapshot's stored target.
    ///
    /// The caller has already reserved every extent the writer allocated, and the
    /// target is the cut the region being written records, so each dropped extent
    /// is represented by that region.
    pub(super) fn drain_to(&mut self, target: BatchVersion) {
        let mut drained = false;
        while self
            .pending
            .front()
            .is_some_and(|head| head.version < target)
        {
            self.pending.pop_front();
            drained = true;
        }
        if drained {
            self.view
                .publish_outstanding(self.stream, len_u32(self.pending.len()));
        }
        // The front is republished on every drain, not only when something
        // popped: releasing the floor below without restoring the head would
        // leave `fronts()` = `tail`, above an extent the stored cut does not
        // cover. Publishing first means a reader between these two stores sees
        // `min(head, floor)` — correct or lower, never a cleared floor with an
        // unset head.
        self.publish_head();
        // Release the floor only when the target has passed the highest dropped
        // version: `target` is exclusive, so `target > dropped` means every
        // dropped extent is below the stored cut.
        if self
            .dropped_max_version
            .is_some_and(|dropped| target > dropped)
        {
            self.dropped_min_offset = UNSET_OFFSET;
            self.dropped_max_version = None;
            self.view.publish_dropped_floor(self.stream, UNSET_OFFSET);
        }
        debug_assert!(
            self.pending
                .front()
                .is_none_or(|head| head.version >= target),
            "drain_to left a head below the stored target",
        );
    }

    /// Republishes the start from the queue itself, so "`front` is UNSET if and
    /// only if the queue is empty" cannot drift from `pending`. With that
    /// invariant `fronts()` = `min(front, tail, dropped_floor)` is always at or
    /// below every extent the published cut does not represent.
    fn publish_head(&self) {
        self.view
            .publish_front(self.stream, self.pending.front().map(|head| head.offset));
    }

    /// Extents this worker could not queue at the cap since open. Sustained
    /// growth is a finding for the capacity report, never a correctness issue.
    pub(super) fn cap_hits(&self) -> u64 {
        self.cap_hits
    }

    /// Queued extents; the metrics variant of `pending.len()`.
    #[cfg(test)]
    pub(super) fn queued(&self) -> usize {
        self.pending.len()
    }
}

/// Saturating `u32` count for the evidence atomic; a queue never holds more than
/// `usize::MAX` extents in practice, and the value is diagnostics only.
fn len_u32(len: usize) -> u32 {
    u32::try_from(len).unwrap_or(u32::MAX)
}

#[cfg(test)]
mod tests;
