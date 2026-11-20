//! Owner-side durable progress of one log, for the coordinator's durable cut.
//!
//! The coordinator's `NativeRecovery::DurablePrefix` policy needs, per owner, the highest
//! global batch version whose whole owner prefix is synced. Versions strictly increase in
//! the WAL order of one owner stream, so the version of the last batch below the synced
//! byte end is that bound. This module keeps the two tails (appended and synced) next to
//! the byte frontiers in the log's `State`; every field changes only under that lock.
//!
//! Report rule: the synced tail moves only in the persistence publication step, after the
//! file, namespace and live-root syncs of a capture all returned success. The captured
//! version is read under the same lock acquisition as the captured byte end, so a report
//! never names a batch that the sync did not cover. A failed sync leaves it unchanged.
//!
//! Strict-root rule: on a live stream, recovery rejects a corrupt frame only below its
//! strict live root; beyond it, the scan stops at the first defect as at a torn tail. A
//! sync that defers the root therefore publishes nothing. The synced tail moves only with
//! a root install (or a seal), so every reported version lies below a root recovery
//! enforces. The durable-progress nudge installs the root of a lagging stream.

use super::BatchVersion;

#[cfg(test)]
mod tests;

/// Appended and synced tails of one log. Lives inside the log `State` mutex.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct ProgressTail {
    /// Version of the last batch appended (owner RPC) or adopted (leased NVMe-oF).
    appended: Option<BatchVersion>,
    /// Version of the last batch of the synced prefix.
    synced: Option<BatchVersion>,
    /// WAL end of the synced prefix: the captured end of the latest publication. On a
    /// live stream it is also covered by the strict live root.
    synced_end: u64,
    /// Number of times the synced point advanced on this handle.
    generation: u64,
}

impl ProgressTail {
    /// A recovered handle: everything below the recovered end is synced and, on a live
    /// stream, covered by the root that recovery installed. It tracks no versions.
    pub(super) fn recovered(end: u64) -> Self {
        Self {
            synced_end: end,
            ..Self::default()
        }
    }

    /// Record the last batch of a group that reached the appended frontier.
    pub(super) fn appended(&mut self, last: BatchVersion) {
        self.appended = Some(last);
    }

    /// The appended tail at a sync capture. Read under the same lock as the captured end.
    pub(super) fn capture(&self) -> Option<BatchVersion> {
        self.appended
    }

    /// Publish a successful sync of `captured` through the captured WAL `end`. The
    /// generation counts only advances of the end, so a long-poll is not woken by a
    /// no-op barrier.
    pub(super) fn synced(&mut self, captured: Option<BatchVersion>, end: u64) {
        // Persistence publications are serialized and their captures grow, so the merge is
        // monotonic; `max` keeps it so even if a caller published an older capture.
        self.synced = self.synced.max(captured);
        if end > self.synced_end {
            self.synced_end = end;
            self.generation += 1;
        }
    }

    pub(super) fn snapshot(&self, appended_end: u64) -> LogDurableProgress {
        LogDurableProgress {
            appended: self.appended,
            synced: self.synced,
            appended_end,
            synced_end: self.synced_end,
            generation: self.generation,
        }
    }
}

/// A consistent snapshot of one log's durable progress.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LogDurableProgress {
    /// Version of the last appended or adopted batch, `None` before the first one. A
    /// recovered (sealed) handle does not track versions and reports `None`.
    pub appended: Option<BatchVersion>,
    /// Every batch of this stream with a version at or below this one is synced.
    pub synced: Option<BatchVersion>,
    /// Contiguous appended WAL end, and the WAL end of the synced prefix. On a live
    /// stream the synced end is the end of its strict live root: data synced beyond it
    /// is not reported until a root covers it.
    pub appended_end: u64,
    pub synced_end: u64,
    /// Advances of the synced point on this log handle; restarts with the handle.
    pub generation: u64,
}

impl LogDurableProgress {
    /// Nothing appended to this log is waiting for a reportable sync.
    #[must_use]
    pub fn idle(&self) -> bool {
        self.appended_end == self.synced_end
    }
}
