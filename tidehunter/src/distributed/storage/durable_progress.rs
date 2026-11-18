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

use super::BatchVersion;

/// Appended and synced tails of one log. Lives inside the log `State` mutex.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct ProgressTail {
    /// Version of the last batch appended (owner RPC) or adopted (leased NVMe-oF).
    appended: Option<BatchVersion>,
    /// Version of the last batch of the synced prefix.
    synced: Option<BatchVersion>,
    /// Number of times the synced point advanced on this handle.
    generation: u64,
}

impl ProgressTail {
    /// Record the last batch of a group that reached the appended frontier.
    pub(super) fn appended(&mut self, last: BatchVersion) {
        self.appended = Some(last);
    }

    /// The appended tail at a sync capture. Read under the same lock as the captured end.
    pub(super) fn capture(&self) -> Option<BatchVersion> {
        self.appended
    }

    /// Publish a successful sync of `captured`. `advanced` is whether the byte frontier
    /// moved; the generation counts only real advances so a long-poll is not woken by a
    /// no-op barrier.
    pub(super) fn synced(&mut self, captured: Option<BatchVersion>, advanced: bool) {
        // Persistence publications are serialized and their captures grow, so the merge is
        // monotonic; `max` keeps it so even if a caller published an older capture.
        self.synced = self.synced.max(captured);
        if advanced {
            self.generation += 1;
        }
    }

    pub(super) fn snapshot(&self, appended_end: u64, synced_end: u64) -> LogDurableProgress {
        LogDurableProgress {
            appended: self.appended,
            synced: self.synced,
            appended_end,
            synced_end,
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
    /// WAL byte frontiers: contiguous appended end and synced (durable) end.
    pub appended_end: u64,
    pub synced_end: u64,
    /// Advances of the synced point on this log handle; restarts with the handle.
    pub generation: u64,
}

impl LogDurableProgress {
    /// Nothing appended to this log is waiting for a sync.
    #[must_use]
    pub fn idle(&self) -> bool {
        self.appended_end == self.synced_end
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn version(sequence: u64) -> BatchVersion {
        BatchVersion { epoch: 3, sequence }
    }

    #[test]
    fn synced_tail_moves_only_on_publication_and_never_backwards() {
        let mut tail = ProgressTail::default();
        tail.appended(version(1));
        let first = tail.capture();
        tail.appended(version(4));
        assert_eq!(tail.snapshot(10, 0).synced, None);
        tail.synced(first, true);
        let report = tail.snapshot(20, 10);
        assert_eq!(
            (report.synced, report.appended),
            (Some(version(1)), Some(version(4)))
        );
        assert_eq!(report.generation, 1);
        assert!(!report.idle());
        tail.synced(Some(version(0)), false);
        assert_eq!(tail.snapshot(20, 20).synced, Some(version(1)));
        assert_eq!(tail.snapshot(20, 20).generation, 1);
        let second = tail.capture();
        tail.synced(second, true);
        let report = tail.snapshot(20, 20);
        assert_eq!(report.synced, Some(version(4)));
        assert!(report.idle());
        assert_eq!(report.generation, 2);
    }
}
