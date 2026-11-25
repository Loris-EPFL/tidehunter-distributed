//! Read-profile counters and their lock-free snapshot handle.
//!
//! Split out of `pending_read.rs` to hold that file under its length ratchet.
//! The counters are cumulative atomics; `select` gates only the finite latency
//! sample, so operator counters keep counting after the sample is exhausted.

use super::*;

// A finite opt-in sample of real actor requests. No connection-control RPCs or
// extra locks are introduced on either the read owner or metrics scrape path.
macro_rules! read_diagnostics {
    ($($name:ident),+ $(,)?) => {
        #[derive(Default)]
        pub(super) struct ReadDiagnostics {
            pub(super) limit: AtomicU64,
            pub(super) remaining: AtomicU64,
            $(pub(super) $name: AtomicU64,)+
        }
        impl ReadDiagnostics {
            fn visit(&self, set: &mut impl FnMut(&'static str, u64)) {
                set("read_profile_limit", self.limit.load(Ordering::Relaxed));
                set("read_profile_enabled", u64::from(self.limit.load(Ordering::Relaxed) != 0));
                $(set(concat!("read_profile_", stringify!($name)),
                    self.$name.load(Ordering::Relaxed));)+
            }
        }
    };
}
read_diagnostics!(
    selected,
    completed,
    errors,
    failed_owner_profile_not_applicable,
    inline_completed,
    inline_ns,
    actor_completed,
    nvme_completed,
    nvme_flight_ns,
    nvme_flight_count,
    nvme_flight_max_ns,
    // Why admission returned backpressure; credit shortage is real load, the
    // others are structural.
    admission_retry_credit,
    admission_retry_flight,
    admission_retry_lane,
    admission_retry_paused,
    // Live lane inbox pressure: inbox-full events, deepest inbox (an upper
    // bound on occupancy; see `LiveLane` in `drive_live_reads`), deepest
    // actor-side overflow queue, and time requests spent in that queue.
    live_lane_full,
    live_lane_depth_max,
    live_lane_pending_max,
    live_pending_wait_ns,
    // Lane-start live export prefetches: completed, and failed. A failed one
    // leaves the lane usable and the first read retries the exchange; the
    // first error text is exported by
    // `ReadProfileMonitor::live_exports_prefetch_error`.
    live_exports_prefetched,
    live_exports_prefetch_failures,
    live_inline,
    live_inline_fallback_drain,
    live_inline_fallback_busy,
    live_inline_fallback_grant,
    live_inline_fallback_credit,
    live_inline_fallback_paused,
    live_inline_fallback_coalesced,
    // Operator counters for the read deadline path: a final caller-visible
    // timeout, and a submission-time retry of an idempotent member read.
    read_timeouts,
    read_retries,
);
impl ReadDiagnostics {
    pub(super) fn inline_complete(&self, elapsed_ns: u64) {
        self.completed.fetch_add(1, Ordering::Relaxed);
        self.inline_completed.fetch_add(1, Ordering::Relaxed);
        self.inline_ns.fetch_add(elapsed_ns, Ordering::Relaxed);
    }
    pub(super) fn select(&self) -> bool {
        // Once exhausted (and by default), no RMW or timestamp is performed.
        if self.remaining.load(Ordering::Relaxed) == 0 {
            return false;
        }
        let selected = self
            .remaining
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |left| {
                left.checked_sub(1)
            })
            .is_ok();
        if selected {
            self.selected.fetch_add(1, Ordering::Relaxed);
        }
        selected
    }
    pub(super) fn nvme_complete(&self, elapsed_ns: Option<u64>, failed: bool) {
        self.nvme_completed.fetch_add(1, Ordering::Relaxed);
        if let Some(ns) = elapsed_ns {
            self.nvme_flight_ns.fetch_add(ns, Ordering::Relaxed);
            self.nvme_flight_count.fetch_add(1, Ordering::Relaxed);
            self.nvme_flight_max_ns.fetch_max(ns, Ordering::Relaxed);
        }
        self.completed.fetch_add(1, Ordering::Relaxed);
        self.actor_completed.fetch_add(1, Ordering::Relaxed);
        if failed {
            self.errors.fetch_add(1, Ordering::Relaxed);
            self.failed_owner_profile_not_applicable
                .fetch_add(1, Ordering::Relaxed);
        }
    }
}
/// Lock-free local snapshot handle; keeps no connection or database owner alive.
#[derive(Clone)]
pub struct ReadProfileMonitor {
    pub(super) state: Arc<State>,
}
impl ReadProfileMonitor {
    pub fn visit(&self, set: &mut impl FnMut(&'static str, u64)) {
        self.state.diagnostics.visit(set);
        set(
            "read_singleflight_coalesced_members",
            self.state.coalesced_members.load(Ordering::Relaxed),
        );
        set(
            "read_singleflight_metadata_bytes",
            self.state.flight_metadata_bytes as u64,
        );
    }
    /// Sequential, lock-free snapshots of configured partitions; not a global cut.
    pub fn visit_partitions(&self, set: &mut impl FnMut(u32, &'static str, u64)) {
        for (partition, diagnostics) in self.state.owner_diagnostics.iter() {
            diagnostics.visit(&mut |name, value| {
                if name.starts_with("read_profile_live_")
                    || matches!(
                        name,
                        "read_profile_completed"
                            | "read_profile_inline_completed"
                            | "read_profile_actor_completed"
                            | "read_profile_errors"
                            | "read_profile_failed_owner_profile_not_applicable"
                            | "read_profile_read_timeouts"
                            | "read_profile_read_retries"
                    )
                {
                    set(*partition, name, value);
                }
            });
        }
    }
}

/// A deadline the caller observes: typed, so the value path can tell a
/// transient deadline from a broken authority and may retry the read.
pub(super) fn deadline_error(message: &'static str) -> LocalDbError {
    LocalDbError::with_kind(std::io::ErrorKind::TimedOut, message)
}

impl State {
    /// One caller-visible read timeout, per service and per owner partition.
    pub(super) fn note_read_timeout(&self, node: usize) {
        self.diagnostics
            .read_timeouts
            .fetch_add(1, Ordering::Relaxed);
        if let Some((_, owner)) = self.owner_diagnostics.get(node) {
            owner.read_timeouts.fetch_add(1, Ordering::Relaxed);
        }
    }
    /// Submission-time retries of one idempotent member request.
    pub(super) fn note_read_retries(&self, node: usize, retries: u8) {
        if retries == 0 {
            return;
        }
        let retries = u64::from(retries);
        self.diagnostics
            .read_retries
            .fetch_add(retries, Ordering::Relaxed);
        if let Some((_, owner)) = self.owner_diagnostics.get(node) {
            owner.read_retries.fetch_add(retries, Ordering::Relaxed);
        }
    }
}
