//! Owner-side append frontier and group-commit syncer, shared by both durability tiers.
//!
//! One [`Frontier`] tracks one owner WAL. It mirrors the native Tidehunter tracker
//! (paper §3.1/§5): writers finish in any order, and the *appended frontier* is the
//! highest WAL offset below which every position is complete. One [`Syncer`] thread
//! per owner WAL fsyncs up to the current appended frontier in a single call and then
//! publishes the *synced frontier*. The tiers differ only in what an acknowledgement
//! waits for (see `native.rs` and `synchronous.rs`); nothing here depends on the tier.
//!
//! Positions are WAL byte offsets. A range is `[start, end)`; frontiers are exclusive ends.
//!
//! # Concurrency
//!
//! * One `parking_lot::Mutex<State>` per frontier (one per owner WAL, so the lock is
//!   sharded by owner). Critical sections hold no I/O, no callbacks, and no allocation
//!   (the pending buffer is preallocated to its bound). Lock order: this is the only lock
//!   in the module; the sync callback runs with it released.
//! * Readers of the frontiers use two atomics and never take the lock. Both are stored
//!   with `Release` while the lock is held and read with `Acquire`: a reader that sees
//!   `synced() >= x` also sees everything the syncer did before publishing, including the
//!   fsync that made `x` durable.
//! * Queue: the out-of-order completion buffer. Owner: the frontier. Capacity:
//!   [`FrontierConfig::max_pending`] entries of 24 bytes. Full-queue policy: **wait**. A
//!   completion that does not start at the frontier blocks until the buffer drains.
//!   This cannot deadlock on a full buffer because the completion that starts at the
//!   frontier never needs a slot, so the writer that owns the lowest unfinished position
//!   always makes progress. The contract on callers: that writer must not wait on a
//!   writer blocked in [`Frontier::complete`]. Shutdown or failure wakes every blocked writer
//!   with an error.
//! * Waiters block on condition variables. No per-request queue exists. The number of
//!   waiters is bounded by the number of caller threads.
//!
//! # Failure
//!
//! A failed fsync is sticky (an fsync error can drop dirty pages, so a retry could report
//! success for lost data). The frontier fails, every waiter gets [`FrontierError::Failed`],
//! and the synced frontier never advances again. The owner must reopen and recover.

use parking_lot::{Condvar, Mutex};
use std::collections::VecDeque;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/// When the syncer runs without a waiter.
///
/// Native Tidehunter syncs a WAL map (fragment) when it is finalized and has no other
/// trigger (paper §3.1). The default policy is the same: the syncer runs when the appended
/// frontier crosses a fragment boundary. This is a sync trigger, not a lag bound: a stalled
/// or slow fsync cannot be interrupted, and writes accumulate meanwhile without backpressure.
/// Waiters (Synchronous acknowledgements, explicit flush, checkpoint capture) always
/// trigger an immediate sync in addition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SyncPolicy {
    /// Size of the owner WAL fragment. Crossing a multiple of it triggers a sync.
    /// Must be at least 1. Read it from the owner storage configuration.
    pub fragment_bytes: u64,
    /// Opt-in extra trigger, stronger than native, not a hard bound: also start a sync once
    /// the oldest unsynced appended byte is this old (milliseconds). `None` (the default) matches native Tidehunter.
    pub max_unsynced_age_ms: Option<u64>,
}

impl SyncPolicy {
    /// The native cadence for an owner WAL with fragments of `fragment_bytes`.
    #[must_use]
    pub const fn native(fragment_bytes: u64) -> Self {
        Self {
            fragment_bytes,
            max_unsynced_age_ms: None,
        }
    }

    #[must_use]
    pub fn max_unsynced_age(self) -> Option<Duration> {
        self.max_unsynced_age_ms.map(Duration::from_millis)
    }
}

/// Construction parameters of one [`Frontier`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FrontierConfig {
    /// First position of the tracked WAL range: the recovered durable end of the owner WAL.
    /// Both frontiers start here.
    pub start: u64,
    /// Capacity of the out-of-order completion buffer. Must be at least 1.
    pub max_pending: usize,
    pub policy: SyncPolicy,
}

impl FrontierConfig {
    /// Default capacity of the out-of-order completion buffer (about 96 KiB).
    pub const DEFAULT_MAX_PENDING: usize = 4096;

    #[must_use]
    pub fn new(start: u64, fragment_bytes: u64) -> Self {
        Self {
            start,
            max_pending: Self::DEFAULT_MAX_PENDING,
            policy: SyncPolicy::native(fragment_bytes),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FrontierError {
    /// The fsync failed, the syncer panicked, or the owner called [`Frontier::fail`].
    Failed,
    /// The deadline passed before the condition held. The condition may still become true.
    Timeout,
    /// The frontier or its syncer stopped; the condition can no longer become true.
    Stopped,
    /// The completed range overlaps a completed range or lies below the appended frontier.
    Overlap,
    /// `start >= end`.
    EmptyRange,
    /// Zero `max_pending` or zero `fragment_bytes`.
    InvalidConfig,
}

impl std::fmt::Display for FrontierError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Failed => "durability frontier failed",
            Self::Timeout => "durability wait timed out",
            Self::Stopped => "durability frontier stopped",
            Self::Overlap => "completed range overlaps a completed range",
            Self::EmptyRange => "completed range is empty",
            Self::InvalidConfig => "invalid durability frontier configuration",
        })
    }
}

impl std::error::Error for FrontierError {}

/// A WAL position with the number of batches covered up to it since the frontier started.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FrontierPoint {
    pub end: u64,
    pub batches: u64,
}

/// Snapshot of the frontier counters. Lag is appended minus synced.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct FrontierStats {
    pub appended: FrontierPoint,
    pub synced: FrontierPoint,
    pub lag_bytes: u64,
    pub lag_batches: u64,
    /// Age of the oldest appended-but-unsynced position, zero when synced. An upper-bound
    /// estimate during an fsync: positions appended while the fsync runs count from the
    /// fsync start, which can only overestimate their age.
    pub lag_age: Duration,
    /// Largest lag observed at the start of any fsync.
    pub peak_lag_bytes: u64,
    pub peak_lag_batches: u64,
    pub peak_lag_age: Duration,
    pub pending_completions: usize,
    pub peak_pending_completions: usize,
    /// Completions that blocked on the full out-of-order buffer.
    pub completion_full_waits: u64,
    pub fsync_calls: u64,
    pub fsync_failures: u64,
    /// Bytes and batches made durable by successful fsyncs.
    pub fsync_bytes: u64,
    pub fsync_batches: u64,
    pub max_batches_per_fsync: u64,
    /// Acknowledgement waits that actually blocked, with total blocked time.
    pub appended_waits: u64,
    pub appended_wait_ns: u64,
    pub synced_waits: u64,
    pub synced_wait_ns: u64,
    /// Explicit flush requests, the subset that blocked, and their blocked time.
    pub flush_requests: u64,
    pub flush_waits: u64,
    pub flush_wait_ns: u64,
    pub flush_wait_max_ns: u64,
}

impl FrontierStats {
    /// Mean batches made durable per fsync; zero before the first fsync.
    #[must_use]
    pub fn batches_per_fsync(&self) -> f64 {
        if self.fsync_calls == 0 {
            0.0
        } else {
            self.fsync_batches as f64 / self.fsync_calls as f64
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct Completion {
    start: u64,
    end: u64,
    batches: u64,
}

#[derive(Default)]
struct Counters {
    peak_lag_bytes: u64,
    peak_lag_batches: u64,
    peak_lag_age: Duration,
    peak_pending: usize,
    completion_full_waits: u64,
    fsync_calls: u64,
    fsync_failures: u64,
    fsync_bytes: u64,
    fsync_batches: u64,
    max_batches_per_fsync: u64,
    appended_waits: u64,
    appended_wait_ns: u64,
    synced_waits: u64,
    synced_wait_ns: u64,
    flush_requests: u64,
    flush_waits: u64,
    flush_wait_ns: u64,
    flush_wait_max_ns: u64,
}

struct State {
    appended_end: u64,
    appended_batches: u64,
    synced_end: u64,
    synced_batches: u64,
    /// Completed ranges above the appended frontier, sorted by `start`, disjoint, and
    /// never touching the frontier (a range touching it is absorbed immediately).
    pending: VecDeque<Completion>,
    /// Highest position a waiter needs synced. The syncer syncs without waiting for the
    /// cadence while `demand_end > synced_end`.
    demand_end: u64,
    /// When the oldest unsynced appended position was appended.
    unsynced_since: Option<Instant>,
    stopping: bool,
    syncer_running: bool,
    syncer_exited: bool,
    failure: Option<String>,
    counters: Counters,
}

/// The appended frontier at the instant the syncer picked its fsync target.
#[derive(Clone, Copy, Debug)]
pub(super) struct Capture {
    pub(super) end: u64,
    batches: u64,
    taken: Instant,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum WaitKind {
    Appended,
    Synced,
    Flush,
}

/// See the module documentation.
pub struct Frontier {
    max_pending: usize,
    policy: SyncPolicy,
    /// Mirror of `State::appended_end`; `Release` store under the lock, `Acquire` load.
    appended: AtomicU64,
    /// Mirror of `State::synced_end`; same ordering.
    synced: AtomicU64,
    state: Mutex<State>,
    /// Notified when the appended frontier advances, on stop and on failure.
    appended_cv: Condvar,
    /// Notified when the synced frontier advances, on syncer exit and on failure.
    synced_cv: Condvar,
    /// Wakes the single syncer: appended advanced, demand rose, stop, or failure.
    syncer_cv: Condvar,
    /// Notified when the pending buffer shrinks, on stop and on failure.
    space_cv: Condvar,
}

impl Frontier {
    /// A frontier with the default capacity and the native sync cadence for owner WAL
    /// fragments of `fragment_bytes`, starting at `start`.
    pub fn new(start: u64, fragment_bytes: u64) -> Result<Arc<Self>, FrontierError> {
        Self::with_config(FrontierConfig::new(start, fragment_bytes))
    }

    pub fn with_config(config: FrontierConfig) -> Result<Arc<Self>, FrontierError> {
        if config.max_pending == 0 || config.policy.fragment_bytes == 0 {
            return Err(FrontierError::InvalidConfig);
        }
        Ok(Arc::new(Self {
            max_pending: config.max_pending,
            policy: config.policy,
            appended: AtomicU64::new(config.start),
            synced: AtomicU64::new(config.start),
            state: Mutex::new(State {
                appended_end: config.start,
                appended_batches: 0,
                synced_end: config.start,
                synced_batches: 0,
                pending: VecDeque::with_capacity(config.max_pending),
                demand_end: config.start,
                unsynced_since: None,
                stopping: false,
                syncer_running: false,
                syncer_exited: false,
                failure: None,
                counters: Counters::default(),
            }),
            appended_cv: Condvar::new(),
            synced_cv: Condvar::new(),
            syncer_cv: Condvar::new(),
            space_cv: Condvar::new(),
        }))
    }

    /// Highest position below which every position is appended. Lock-free.
    #[must_use]
    pub fn appended(&self) -> u64 {
        self.appended.load(Ordering::Acquire)
    }

    /// Highest position below which every position is durable (fsynced). Lock-free.
    ///
    /// Receipt-checkpoint capture bounds its persisted index cut by this frontier.
    /// WAL-position checkpoint capture instead uses the installed position root
    /// and its proof reply; that root can be ahead without publishing receipts.
    #[must_use]
    pub fn synced(&self) -> u64 {
        self.synced.load(Ordering::Acquire)
    }

    /// Both frontiers with their batch counts, read under one lock acquisition.
    #[must_use]
    pub fn points(&self) -> (FrontierPoint, FrontierPoint) {
        let st = self.state.lock();
        (
            FrontierPoint {
                end: st.appended_end,
                batches: st.appended_batches,
            },
            FrontierPoint {
                end: st.synced_end,
                batches: st.synced_batches,
            },
        )
    }

    /// Why the frontier failed, if it did.
    #[must_use]
    pub fn failure(&self) -> Option<String> {
        self.state.lock().failure.clone()
    }

    /// Record that `batches` batches occupying `[start, end)` are completely written,
    /// in any order relative to other completions. Advances the appended frontier over
    /// every now-contiguous completion. Blocks while the out-of-order buffer is full
    /// (see the module documentation).
    pub fn complete(&self, start: u64, end: u64, batches: u64) -> Result<(), FrontierError> {
        let mut st = self.state.lock();
        self.complete_locked(&mut st, Some(start), end, batches)
    }

    /// Record that every position below `end` is complete, for callers that already
    /// serialize their appends (the range is `[appended, end)`). Not idempotent in the
    /// batch count: an `end` at or below the frontier is [`FrontierError::Overlap`].
    pub fn publish_appended(&self, end: u64, batches: u64) -> Result<(), FrontierError> {
        let mut st = self.state.lock();
        self.complete_locked(&mut st, None, end, batches)
    }

    fn complete_locked(
        &self,
        st: &mut parking_lot::MutexGuard<'_, State>,
        start: Option<u64>,
        end: u64,
        batches: u64,
    ) -> Result<(), FrontierError> {
        let mut counted_full_wait = false;
        loop {
            if st.failure.is_some() {
                return Err(FrontierError::Failed);
            }
            if st.stopping {
                return Err(FrontierError::Stopped);
            }
            let Some(start) = start else {
                // `publish_appended`: the range is `[appended, end)`.
                if end <= st.appended_end {
                    return Err(FrontierError::Overlap);
                }
                let appended = st.appended_end;
                return self.complete_locked(st, Some(appended), end, batches);
            };
            if start >= end {
                return Err(FrontierError::EmptyRange);
            }
            if start < st.appended_end {
                return Err(FrontierError::Overlap);
            }
            let completion = Completion {
                start,
                end,
                batches,
            };
            if start == st.appended_end {
                if st.pending.front().is_some_and(|next| next.start < end) {
                    return Err(FrontierError::Overlap);
                }
                let drained = self.advance(st, completion);
                if drained {
                    self.space_cv.notify_all();
                }
                self.appended_cv.notify_all();
                self.syncer_cv.notify_one();
                return Ok(());
            }
            let at = st.pending.partition_point(|queued| queued.start < start);
            if at > 0 && st.pending[at - 1].end > start {
                return Err(FrontierError::Overlap);
            }
            if st.pending.get(at).is_some_and(|next| next.start < end) {
                return Err(FrontierError::Overlap);
            }
            if st.pending.len() == self.max_pending {
                if !counted_full_wait {
                    counted_full_wait = true;
                    st.counters.completion_full_waits += 1;
                }
                self.space_cv.wait(st);
                continue;
            }
            st.pending.insert(at, completion);
            let depth = st.pending.len();
            st.counters.peak_pending = st.counters.peak_pending.max(depth);
            return Ok(());
        }
    }

    /// Apply a completion that starts at the frontier, then absorb every pending
    /// completion made contiguous by it. Returns whether the pending buffer shrank.
    /// Loop bound: at most `pending.len()` pops.
    fn advance(&self, st: &mut State, first: Completion) -> bool {
        let was_synced = st.appended_end == st.synced_end;
        st.appended_end = first.end;
        st.appended_batches += first.batches;
        let mut drained = false;
        while st
            .pending
            .front()
            .is_some_and(|next| next.start == st.appended_end)
        {
            if let Some(next) = st.pending.pop_front() {
                st.appended_end = next.end;
                st.appended_batches += next.batches;
                drained = true;
            }
        }
        if was_synced {
            st.unsynced_since = Some(Instant::now());
        }
        // Release: pairs with the Acquire in `appended()`.
        self.appended.store(st.appended_end, Ordering::Release);
        drained
    }

    /// Wait until the appended frontier reaches `end`.
    pub fn wait_appended(&self, end: u64, deadline: Option<Instant>) -> Result<(), FrontierError> {
        if self.appended() >= end {
            return Ok(());
        }
        self.wait(end, deadline, WaitKind::Appended)
    }

    /// Wait until the synced frontier reaches `end`. The wait raises the demand, so the
    /// syncer starts without waiting for its cadence. `end` should not exceed what will
    /// ever be appended; otherwise the wait lasts until the deadline.
    pub fn wait_synced(&self, end: u64, deadline: Option<Instant>) -> Result<(), FrontierError> {
        if self.synced() >= end {
            return Ok(());
        }
        self.wait(end, deadline, WaitKind::Synced)
    }

    /// Ask the syncer to cover everything appended now, without waiting. Returns the
    /// position to pass to [`Frontier::wait_flush`].
    pub fn request_sync(&self) -> Result<u64, FrontierError> {
        let mut st = self.state.lock();
        if st.failure.is_some() {
            return Err(FrontierError::Failed);
        }
        st.counters.flush_requests += 1;
        let target = st.appended_end;
        if target > st.synced_end && st.demand_end < target {
            st.demand_end = target;
            self.syncer_cv.notify_one();
        }
        Ok(target)
    }

    /// Wait for the position returned by [`Frontier::request_sync`] to be synced and
    /// return the synced frontier. Counted as a flush wait.
    pub fn wait_flush(&self, target: u64, deadline: Option<Instant>) -> Result<u64, FrontierError> {
        if self.synced() < target {
            self.wait(target, deadline, WaitKind::Flush)?;
        }
        Ok(self.synced())
    }

    fn wait(
        &self,
        target: u64,
        deadline: Option<Instant>,
        kind: WaitKind,
    ) -> Result<(), FrontierError> {
        let began = Instant::now();
        let mut st = self.state.lock();
        if kind != WaitKind::Appended && st.demand_end < target {
            st.demand_end = target;
            self.syncer_cv.notify_one();
        }
        let outcome = loop {
            let reached = match kind {
                WaitKind::Appended => st.appended_end >= target,
                WaitKind::Synced | WaitKind::Flush => st.synced_end >= target,
            };
            if reached {
                break Ok(());
            }
            if st.failure.is_some() {
                break Err(FrontierError::Failed);
            }
            let (stopped, condvar) = match kind {
                WaitKind::Appended => (st.stopping, &self.appended_cv),
                WaitKind::Synced | WaitKind::Flush => (st.syncer_exited, &self.synced_cv),
            };
            if stopped {
                break Err(FrontierError::Stopped);
            }
            match deadline {
                None => condvar.wait(&mut st),
                Some(deadline) => {
                    if condvar.wait_until(&mut st, deadline).timed_out() {
                        // One last look: the condition may have held at the deadline.
                        let reached = match kind {
                            WaitKind::Appended => st.appended_end >= target,
                            WaitKind::Synced | WaitKind::Flush => st.synced_end >= target,
                        };
                        break if reached {
                            Ok(())
                        } else {
                            Err(FrontierError::Timeout)
                        };
                    }
                }
            }
        };
        let blocked = u64::try_from(began.elapsed().as_nanos()).unwrap_or(u64::MAX);
        let counters = &mut st.counters;
        match kind {
            WaitKind::Appended => {
                counters.appended_waits += 1;
                counters.appended_wait_ns = counters.appended_wait_ns.saturating_add(blocked);
            }
            WaitKind::Synced => {
                counters.synced_waits += 1;
                counters.synced_wait_ns = counters.synced_wait_ns.saturating_add(blocked);
            }
            WaitKind::Flush => {
                counters.flush_waits += 1;
                counters.flush_wait_ns = counters.flush_wait_ns.saturating_add(blocked);
                counters.flush_wait_max_ns = counters.flush_wait_max_ns.max(blocked);
            }
        }
        outcome
    }

    /// Poison the frontier. Every waiter and later completion gets [`FrontierError::Failed`].
    /// The first reason is kept.
    pub fn fail(&self, reason: &str) {
        let mut st = self.state.lock();
        st.failure.get_or_insert_with(|| reason.to_owned());
        drop(st);
        self.wake_all();
    }

    /// Refuse new completions and let the syncer drain: one final fsync of the appended
    /// frontier, then exit. Waiters whose condition cannot become true get
    /// [`FrontierError::Stopped`].
    pub fn stop(&self) {
        self.state.lock().stopping = true;
        self.wake_all();
    }

    fn wake_all(&self) {
        self.appended_cv.notify_all();
        self.synced_cv.notify_all();
        self.syncer_cv.notify_all();
        self.space_cv.notify_all();
    }

    #[must_use]
    pub fn stats(&self) -> FrontierStats {
        let st = self.state.lock();
        let c = &st.counters;
        FrontierStats {
            appended: FrontierPoint {
                end: st.appended_end,
                batches: st.appended_batches,
            },
            synced: FrontierPoint {
                end: st.synced_end,
                batches: st.synced_batches,
            },
            lag_bytes: st.appended_end - st.synced_end,
            lag_batches: st.appended_batches - st.synced_batches,
            lag_age: st
                .unsynced_since
                .map_or(Duration::ZERO, |since| since.elapsed()),
            peak_lag_bytes: c.peak_lag_bytes,
            peak_lag_batches: c.peak_lag_batches,
            peak_lag_age: c.peak_lag_age,
            pending_completions: st.pending.len(),
            peak_pending_completions: c.peak_pending,
            completion_full_waits: c.completion_full_waits,
            fsync_calls: c.fsync_calls,
            fsync_failures: c.fsync_failures,
            fsync_bytes: c.fsync_bytes,
            fsync_batches: c.fsync_batches,
            max_batches_per_fsync: c.max_batches_per_fsync,
            appended_waits: c.appended_waits,
            appended_wait_ns: c.appended_wait_ns,
            synced_waits: c.synced_waits,
            synced_wait_ns: c.synced_wait_ns,
            flush_requests: c.flush_requests,
            flush_waits: c.flush_waits,
            flush_wait_ns: c.flush_wait_ns,
            flush_wait_max_ns: c.flush_wait_max_ns,
        }
    }

    /// Block until an fsync is due and capture its target, the whole appended frontier.
    /// Returns `None` when the frontier failed, or stopped with nothing left to sync.
    /// One call to the sync callback therefore covers every position appended so far:
    /// group commit.
    pub(super) fn next_capture(&self) -> Option<Capture> {
        let mut st = self.state.lock();
        loop {
            if st.failure.is_some() {
                return None;
            }
            if st.appended_end > st.synced_end {
                let now = Instant::now();
                let since = st.unsynced_since.unwrap_or(now);
                let age = now.saturating_duration_since(since);
                let lag_bytes = st.appended_end - st.synced_end;
                // A fragment finalized since the last sync: the appended frontier is past
                // a fragment boundary that lies above the synced frontier.
                let finalized = st.appended_end - st.appended_end % self.policy.fragment_bytes;
                let due = st.stopping
                    || st.demand_end > st.synced_end
                    || finalized > st.synced_end
                    || self
                        .policy
                        .max_unsynced_age()
                        .is_some_and(|bound| age >= bound);
                if due {
                    let lag_batches = st.appended_batches - st.synced_batches;
                    let c = &mut st.counters;
                    c.peak_lag_bytes = c.peak_lag_bytes.max(lag_bytes);
                    c.peak_lag_batches = c.peak_lag_batches.max(lag_batches);
                    c.peak_lag_age = c.peak_lag_age.max(age);
                    return Some(Capture {
                        end: st.appended_end,
                        batches: st.appended_batches,
                        taken: now,
                    });
                }
                // Fragment, demand, stop and failure changes notify the syncer; only the
                // opt-in age bound needs a timed wait.
                match self
                    .policy
                    .max_unsynced_age()
                    .and_then(|bound| since.checked_add(bound))
                {
                    Some(wake) => {
                        self.syncer_cv.wait_until(&mut st, wake);
                    }
                    None => self.syncer_cv.wait(&mut st),
                }
            } else if st.stopping {
                return None;
            } else {
                self.syncer_cv.wait(&mut st);
            }
        }
    }

    /// Publish a completed fsync of `capture`.
    pub(super) fn finish_sync(&self, capture: &Capture) {
        let mut st = self.state.lock();
        let bytes = capture.end - st.synced_end;
        let batches = capture.batches - st.synced_batches;
        st.synced_end = capture.end;
        st.synced_batches = capture.batches;
        let c = &mut st.counters;
        c.fsync_calls += 1;
        c.fsync_bytes += bytes;
        c.fsync_batches += batches;
        c.max_batches_per_fsync = c.max_batches_per_fsync.max(batches);
        // Positions appended during the fsync are older than "now"; the capture instant
        // is an upper bound of their age.
        st.unsynced_since = (st.appended_end > capture.end).then_some(capture.taken);
        // Release: pairs with the Acquire in `synced()`. The fsync returned before this
        // store, so a reader that sees the new value sees durable data.
        self.synced.store(capture.end, Ordering::Release);
        drop(st);
        self.synced_cv.notify_all();
    }

    fn fail_sync(&self, reason: String) {
        let mut st = self.state.lock();
        st.counters.fsync_failures += 1;
        st.failure.get_or_insert(reason);
        drop(st);
        self.wake_all();
    }
}

/// Clears the syncer state when the thread ends, including by panic.
struct SyncerExit<'a>(&'a Frontier);

impl Drop for SyncerExit<'_> {
    fn drop(&mut self) {
        let mut st = self.0.state.lock();
        st.syncer_exited = true;
        st.syncer_running = false;
        if std::thread::panicking() {
            st.failure
                .get_or_insert_with(|| "syncer thread panicked".to_owned());
        }
        drop(st);
        self.0.wake_all();
    }
}

/// The one background syncer thread of an owner WAL.
///
/// Thread role: flush (slow disk work; never runs on network or request threads).
/// Shutdown: [`Syncer::shutdown`] or drop stops the frontier, drains with one final
/// fsync of the appended frontier, and joins the thread.
pub struct Syncer {
    frontier: Arc<Frontier>,
    thread: Option<JoinHandle<()>>,
}

impl Syncer {
    /// Start the syncer of `frontier`. `sync(end)` must make every WAL byte below `end`
    /// durable (fsync of the data and of any namespace change that locates it) before it
    /// returns `Ok`. It may make more durable. It runs without any frontier lock.
    /// An `Err` fails the frontier permanently.
    pub fn spawn(
        name: &str,
        frontier: Arc<Frontier>,
        mut sync: impl FnMut(u64) -> io::Result<()> + Send + 'static,
    ) -> io::Result<Self> {
        {
            let mut st = frontier.state.lock();
            if st.syncer_running || st.syncer_exited {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "frontier already has a syncer",
                ));
            }
            st.syncer_running = true;
        }
        let worker = Arc::clone(&frontier);
        let spawned = std::thread::Builder::new()
            .name(name.to_owned())
            .spawn(move || {
                let _exit = SyncerExit(&worker);
                // Terminates when `next_capture` returns None (stop or failure).
                while let Some(capture) = worker.next_capture() {
                    match sync(capture.end) {
                        Ok(()) => worker.finish_sync(&capture),
                        Err(error) => {
                            worker.fail_sync(format!("fsync through {}: {error}", capture.end));
                            return;
                        }
                    }
                }
            });
        match spawned {
            Ok(thread) => Ok(Self {
                frontier,
                thread: Some(thread),
            }),
            Err(error) => {
                frontier.state.lock().syncer_running = false;
                Err(error)
            }
        }
    }

    #[must_use]
    pub fn frontier(&self) -> &Arc<Frontier> {
        &self.frontier
    }

    /// Stop, drain, and join. `Err(Failed)` if the frontier failed.
    pub fn shutdown(mut self) -> Result<(), FrontierError> {
        self.stop_and_join();
        if self.frontier.state.lock().failure.is_some() {
            Err(FrontierError::Failed)
        } else {
            Ok(())
        }
    }

    fn stop_and_join(&mut self) {
        self.frontier.stop();
        if let Some(thread) = self.thread.take() {
            // A panicked syncer already failed the frontier through `SyncerExit`.
            let _ = thread.join();
        }
    }
}

impl Drop for Syncer {
    fn drop(&mut self) {
        self.stop_and_join();
    }
}

/// One owner that can be asked to make everything it has appended durable.
/// The two phases let [`flush_all`] start every owner before it waits for any.
pub trait FlushTarget {
    /// What `begin_flush` hands to `finish_flush`.
    type Ticket;

    /// Request a sync of everything appended now. Must not block on the sync itself.
    fn begin_flush(&self) -> Result<Self::Ticket, FrontierError>;

    /// Wait for the sync and return the owner's synced frontier.
    fn finish_flush(
        &self,
        ticket: Self::Ticket,
        deadline: Option<Instant>,
    ) -> Result<u64, FrontierError>;
}

impl FlushTarget for Frontier {
    type Ticket = u64;

    fn begin_flush(&self) -> Result<u64, FrontierError> {
        self.request_sync()
    }

    fn finish_flush(&self, ticket: u64, deadline: Option<Instant>) -> Result<u64, FrontierError> {
        self.wait_flush(ticket, deadline)
    }
}

impl<T: FlushTarget + ?Sized> FlushTarget for Arc<T> {
    type Ticket = T::Ticket;

    fn begin_flush(&self) -> Result<Self::Ticket, FrontierError> {
        (**self).begin_flush()
    }

    fn finish_flush(
        &self,
        ticket: Self::Ticket,
        deadline: Option<Instant>,
    ) -> Result<u64, FrontierError> {
        (**self).finish_flush(ticket, deadline)
    }
}

/// Per-owner outcome of [`flush_all`], in the order of the input slice.
#[derive(Debug)]
pub struct FlushReport {
    pub owners: Vec<Result<u64, FrontierError>>,
}

/// The first failing owner of a [`FlushReport`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FlushAllError {
    pub owner: usize,
    pub error: FrontierError,
}

impl std::fmt::Display for FlushAllError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "flush of owner {} failed: {}", self.owner, self.error)
    }
}

impl std::error::Error for FlushAllError {}

impl FlushReport {
    /// The synced frontier of every owner, or the first failure.
    pub fn into_synced(self) -> Result<Vec<u64>, FlushAllError> {
        self.owners
            .into_iter()
            .enumerate()
            .map(|(owner, result)| result.map_err(|error| FlushAllError { owner, error }))
            .collect()
    }
}

/// Request a sync on every owner, then wait for all of them against one shared deadline.
///
/// Every owner's syncer starts before the first wait begins, so the fsyncs run in parallel
/// and the total time is the slowest owner, not the sum. Each owner's flush covers every
/// batch that owner acknowledged before `begin_flush` was called, so the call covers
/// everything acknowledged before it started. A failing owner does not stop the others;
/// the report holds one result per owner.
pub fn flush_all<T: FlushTarget>(owners: &[T], deadline: Option<Instant>) -> FlushReport {
    let tickets: Vec<_> = owners.iter().map(FlushTarget::begin_flush).collect();
    let owners = owners
        .iter()
        .zip(tickets)
        .map(|(owner, ticket)| ticket.and_then(|ticket| owner.finish_flush(ticket, deadline)))
        .collect();
    FlushReport { owners }
}
