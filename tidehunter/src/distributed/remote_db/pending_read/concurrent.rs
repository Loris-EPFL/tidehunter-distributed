//! Read diagnostics on the concurrent coordinator handle.
//!
//! The scalar, window and record read methods that used exact reference
//! descriptors, index pins and read cuts are gone: the hybrid resolves a key to
//! a native index position and fetches it through [`ValueReader`]. What remains
//! here is the instrumentation the benchmark harness, the Sui adapter and the
//! write-path tests read without taking a database borrow.
//!
//! [`ValueReader`]: crate::distributed::hybrid::read::ValueReader

#[allow(clippy::wildcard_imports)]
// Private read delegate shares its parent's bounded transport types.
use super::*;

impl ConcurrentRemoteDatabase {
    /// Arm the finite read-profile sample. Called once before serving.
    pub fn set_read_profile_limit(&self, limit: u64) {
        self.reads
            .state
            .diagnostics
            .limit
            .store(limit, Ordering::Relaxed);
        self.reads
            .state
            .diagnostics
            .remaining
            .store(limit, Ordering::Relaxed);
        self.reads.state.coordinator_diagnostics.arm(limit);
        self.reads.state.trace.arm(limit);
    }

    /// True while a finite read-profile sample is armed.
    pub(crate) fn read_profile_enabled(&self) -> bool {
        self.reads.state.diagnostics.limit.load(Ordering::Relaxed) != 0
    }

    /// Cumulative first-N sampled counters; never per-request means.
    pub fn read_profile_monitor(&self) -> ReadProfileMonitor {
        ReadProfileMonitor {
            state: self.reads.state.clone(),
        }
    }

    /// Snapshot of the bounded read trace, or `None` when it is not armed.
    pub fn read_trace_snapshot(&self) -> ReadTraceSnapshot {
        self.reads.state.trace.snapshot()
    }

    /// Number of remote value requests handed to a live lane so far.
    pub fn live_read_submission_count(&self) -> u64 {
        self.reads
            .state
            .live_read_submissions
            .load(Ordering::Relaxed)
    }
}
