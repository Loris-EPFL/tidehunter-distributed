//! Frame budget and allocation accounting for the leased writer.
//!
//! Both answer "what may one leased frame carry, and what did it allocate".
//! Keeping them out of `live_client.rs` holds that file under its length ratchet
//! without changing the writer's surface: the methods stay inherent on `Client`
//! and keep their `pub(crate)` visibility.

use super::super::*;

/// How many queued append groups one leased frame can carry. One frame has one
/// extent allocation, one NVMe write and one adoption record, so merging queued
/// groups divides all three by the merge factor.
#[derive(Clone, Copy, Debug)]
pub(crate) struct LiveGroupBudget {
    max_batches: usize,
    max_payload: usize,
    max_frame: usize,
}

impl LiveGroupBudget {
    /// Size of an empty group: header only.
    pub(crate) const EMPTY_PAYLOAD: usize = crate::distributed::group::GROUP_HEADER_LEN;

    /// Counters of a group after adding `extra`, or `None` if it would exceed the
    /// owner's reservation limits (batch count, payload, aligned frame size).
    pub(crate) fn extend(
        &self,
        count: usize,
        payload: usize,
        extra: &[CompleteBatch],
    ) -> Option<(usize, usize)> {
        use crate::distributed::group::CompleteGroup;
        let count = count.checked_add(extra.len())?;
        let mut payload = payload;
        for batch in extra {
            payload = payload
                .checked_add(CompleteGroup::encoded_member_len(batch.encoded().len()).ok()?)?;
        }
        let frame = payload.checked_add(8 + 4095)? & !4095;
        (count <= self.max_batches && payload <= self.max_payload && frame <= self.max_frame)
            .then_some((count, payload))
    }
}

impl Client {
    pub(crate) fn live_group_budget(&self) -> LiveGroupBudget {
        LiveGroupBudget {
            max_batches: self.expected.group_limits.max_batches as usize,
            max_payload: self.expected.group_limits.max_encoded_bytes,
            max_frame: (self.expected.fragment_bytes as usize).min(64 * 1024 * 1024),
        }
    }

    /// Start recording value-WAL allocations for the hybrid index writer.
    ///
    /// Off by default so no other route pays for or retains an entry. Idempotent.
    /// After this, the caller must drain with [`Self::take_lease_reservations`]
    /// right after every successful submission; that drain is the queue's bound.
    pub(crate) fn enable_lease_reservations(&mut self) {
        self.live_leased
            .reservations
            .get_or_insert_with(std::collections::VecDeque::new);
    }

    /// Hand every value-WAL allocation of the submissions since the last call to
    /// `sink`, oldest first.
    ///
    /// The hybrid coordinator allocates the owner offsets, so the index writer is
    /// the only component that can turn a submitted batch into an index position
    /// and into the next snapshot's per-stream replay start. This drains the
    /// per-batch queue the leased writer fills when a frame is accepted, if
    /// recording was enabled; the owner worker calls it after each successful
    /// submission, before it accepts the next command, so the queue never holds
    /// more than one group.
    ///
    /// The sink runs while the client mutex is held and must not block: the
    /// index side only records an allocation (`AllocationView` / pending
    /// publication), it never waits for owner I/O here.
    pub(crate) fn take_lease_reservations(
        &mut self,
        mut sink: impl FnMut(BatchVersion, PhysicalValueAddress),
    ) {
        let Some(queue) = self.live_leased.reservations.as_mut() else {
            return;
        };
        for (version, address) in queue.drain(..) {
            sink(version, address);
        }
    }
}

pub(super) fn record_write_io(
    counters: &crate::distributed::nvmeof::live_metrics::LiveNvmeCounters,
    before: &crate::distributed::nvmeof::live_io::LiveIoStats,
    after: &crate::distributed::nvmeof::live_io::LiveIoStats,
) {
    use crate::distributed::nvmeof::live_metrics::LiveTimingStage;
    let ns = |n: u128| n.min(u64::MAX as u128) as u64;
    counters.record_write(
        after
            .completed_write_ops
            .saturating_sub(before.completed_write_ops),
        after
            .completed_write_bytes
            .saturating_sub(before.completed_write_bytes),
    );
    counters.record_write_submit_calls(after.submit_calls.saturating_sub(before.submit_calls));
    let submissions = after.submissions.saturating_sub(before.submissions);
    if submissions != 0 {
        counters.record_timing(
            LiveTimingStage::Queue,
            ns(after.queue_ns.saturating_sub(before.queue_ns)),
            submissions,
        );
        counters.record_timing(
            LiveTimingStage::Submit,
            ns(after.submit_ns.saturating_sub(before.submit_ns)),
            submissions,
        );
    }
    let completions = after.completions.saturating_sub(before.completions);
    if completions != 0 {
        counters.record_timing(
            LiveTimingStage::Completion,
            ns(after.submit_to_cq_ns.saturating_sub(before.submit_to_cq_ns)),
            completions,
        );
    }
}
