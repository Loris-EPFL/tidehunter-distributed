//! Option C cell flusher pool and its scheduler (checkpoint option C spec
//! §4.2, §6.1, §6.2, §9.3). Inactive: only tests start it until the commit
//! stage wires it under the per-cell checkpoint mode.
//!
//! # Roles
//!
//! - `P = cell_flushers(N)` flushers named `value-index-flush-<w>`. Worker `w`
//!   owns the index shards `[floor(wN/P), floor((w+1)N/P))`. Only it captures,
//!   encodes, appends and installs cells of that range, so a cell never has
//!   two flights and a worker's [`CellStore`] has one writer.
//! - One scheduler actor, `value-index-flush-scheduler`. It reads per-shard
//!   cell metadata one shard read lock at a time, selects cells and sends
//!   them to the owning worker. It never captures, encodes or waits on a
//!   worker.
//!
//! # Queues and wake
//!
//! - Job queue per worker: `sync_channel(FLUSH_QUEUE)` of cell identities
//!   with their send time (queue age, spec §11.4). Owner: the scheduler
//!   holds every sender and only uses `try_send`. Full: the cell's queued
//!   mark is cleared and its dirty state stays in the index, which is the
//!   source of truth, so a later pass sends it again. Shutdown: the
//!   scheduler drops the senders; a draining worker finishes the queued
//!   cells and its last group, a cancelled one unqueues them.
//! - Scheduler wake: one coalesced reason word under a mutex (capacity one;
//!   repeated wakes merge). The scheduler checks it before it parks and waits
//!   on a condvar with the policy deadline (1 s), never a sleep or poll.
//! - At most `FLUSH_QUEUE` queued cells plus `GROUP_CELLS` captured cells per
//!   worker, and at most one queued descriptor or flight per cell
//!   (`NativeIndex::try_queue_cell`).
//!
//! # Capture lifetime
//!
//! Every capture ends in an accepted `install_cell` of a flush or restore
//! result. Cancellation restores. A capture is dropped without install only
//! after the core failed: [`Flights`] records the failure first when it is
//! dropped non-empty.
//!
//! # Crash consistency
//!
//! A base is installed only after [`CellStore::seal`] made its segment and
//! the segment name durable (store before install). Nothing here adopts a
//! root, returns replay or identity credit, or deletes a file. A crash loses
//! RAM state and unadopted segments only; recovery uses the adopted root and
//! the retained Value WAL suffix.
//!
//! # Lock order
//!
//! `index.writer()` -> option-D publisher ([`FlushHooks::with_publisher`]) ->
//! one index shard -> retention `cold`, all inside the native index calls.
//! `wake` and `progress` are leaf locks: nothing else is taken under them and
//! no hook runs under them. Nothing waits (channel, condvar, store I/O) while
//! a serving guard is held.
#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "option C flusher pool, started by the per-cell commit stage"
    )
)]

use super::cell_metrics::{CellMetrics, PassGauges, SelectionReason, StoreSnapshot, thread_cpu_ns};
use crate::cell::CellId;
use crate::distributed::native_index::{
    CELL_FLIGHTS_PER_FLUSHER, CellCaptureOutcome, CellDirty, CellFlushResult, CellInstall,
    CellRetired, CellSkip, IndexStoreWriter, IndexStoreWriterStats, IndexWriter, NativeIndex,
    NativeIndexError, NewCellBase, PrefixCut, PreparedCell, cell_flushers,
};
use crate::distributed::types::BatchVersion;
use crate::key_shape::KeySpace;
use parking_lot::{Condvar, Mutex};
use std::fmt;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, TryRecvError, TrySendError, sync_channel};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

type CellKey = (KeySpace, CellId);
/// A queued cell and when the scheduler sent it.
type Job = (CellKey, Instant);

/// Capacity of each worker's job queue (spec §6.2).
pub(crate) const FLUSH_QUEUE: usize = 32;
/// Most captured cells in one worker group, equal to the per-worker flight
/// allowance of the native index.
pub(crate) const GROUP_CELLS: usize = CELL_FLIGHTS_PER_FLUSHER;
/// A group is sealed once its frames reach this many bytes (spec §4.3).
pub(crate) const GROUP_BYTES: u64 = 8 << 20;

const WAKE_DIRTY: u8 = 1;
const WAKE_PRESSURE: u8 = 1 << 1;
const WAKE_REQUEST: u8 = 1 << 2;
const WAKE_PROGRESS: u8 = 1 << 3;
const WAKE_STOP: u8 = 1 << 4;

/// One flusher's private index store. The index-store stage implements it:
/// the encoder runs inside `append`, because only native-index code can read
/// a [`PreparedCell`]'s sources and build a frame base.
pub(crate) trait CellStore: Send {
    /// Encode `cell` once and buffer its frame in this worker's open segment.
    /// No sync. Returns the frame bytes; 0 means no retained row and no frame.
    fn append(&mut self, cell: &PreparedCell) -> Result<u64, NativeIndexError>;

    /// Make every frame appended since the last seal durable (file and
    /// directory sync), map it read-only, and push exactly one base per
    /// append, in append order. No output after zero appends.
    fn seal(&mut self, bases: &mut Vec<NewCellBase>) -> Result<(), NativeIndexError>;

    /// Cumulative store and encoder counters (spec §11.4). Read by the
    /// owning worker after each append and seal.
    fn stats(&self) -> IndexStoreWriterStats {
        IndexStoreWriterStats::default()
    }

    /// Arm per-phase encoder timing for the next appends (profile-on only).
    fn set_profile(&mut self, _on: bool) {}
}

/// The production store: one worker-private segment stream per flusher. The
/// writer enforces the 32-cell / 8 MiB group bounds itself and keeps bases in
/// append order across the segments it seals early.
impl CellStore for IndexStoreWriter {
    fn append(&mut self, cell: &PreparedCell) -> Result<u64, NativeIndexError> {
        IndexStoreWriter::append(self, cell)
    }

    fn seal(&mut self, bases: &mut Vec<NewCellBase>) -> Result<(), NativeIndexError> {
        IndexStoreWriter::seal(self, bases)
    }

    fn stats(&self) -> IndexStoreWriterStats {
        IndexStoreWriter::stats(self)
    }

    fn set_profile(&mut self, on: bool) {
        IndexStoreWriter::set_profile(self, on);
    }
}

/// Encode, append and seal one prepared cell in its own segment, then bind
/// the base. For offline recovery flushing (spec §8.1 step 7), which has no
/// serving users and flushes one cell at a time. On error `prepared` is
/// dropped: allowed only on a fail-closed path.
pub(crate) fn flush_prepared<S: CellStore>(
    store: &mut S,
    prepared: PreparedCell,
) -> Result<CellFlushResult, NativeIndexError> {
    store.append(&prepared)?;
    let mut bases = Vec::with_capacity(1);
    store.seal(&mut bases)?;
    match (bases.pop(), bases.is_empty()) {
        (Some(base), true) => Ok(prepared.finish(base)),
        _ => Err(NativeIndexError::Invalid(
            "index store sealed a base count unlike its appends",
        )),
    }
}

/// Selection thresholds (spec §6.1). Initial tunables, not measured optima.
#[derive(Clone, Copy, Debug)]
pub(crate) struct FlushPolicy {
    /// Unrepresented published versions that select a cell.
    pub dirty_versions: usize,
    /// Encoded upper bound of unrepresented references that selects a cell.
    pub dirty_upper: usize,
    /// Age of the oldest unrepresented reference that selects a cell.
    pub age: Duration,
    /// Longest scheduler wait without a wake.
    pub deadline: Duration,
}

impl Default for FlushPolicy {
    fn default() -> Self {
        Self {
            dirty_versions: 1024,
            dirty_upper: 256 << 10,
            age: Duration::from_secs(1),
            deadline: Duration::from_secs(1),
        }
    }
}

/// Replay-tail state the scheduler reads once per pass, with no guard held.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ReplayPressure {
    /// Newest prefix whose suffix through `L` reaches a quarter of either
    /// replay-tail bound: a cell whose oldest unrepresented batch is in it
    /// lags `L` (spec §6.1). `BeforeAll`: no cell lags.
    pub lagging: PrefixCut,
    /// The granted replay tail reached half of either hard bound: drain to a
    /// fixed target.
    pub drain: bool,
}

impl Default for ReplayPressure {
    fn default() -> Self {
        Self {
            lagging: PrefixCut::BeforeAll,
            drain: false,
        }
    }
}

/// Integration points of the commit stage: option-D publisher, Admission and
/// core health. No method may wait on the flusher pool.
pub(crate) trait FlushHooks: Send + Sync + 'static {
    /// Run `f` with the option-D publisher held. Called with `writer` held,
    /// so the lock order is writer -> publisher.
    fn with_publisher<'w, R>(
        &self,
        writer: &mut IndexWriter<'w>,
        f: impl FnOnce(&mut IndexWriter<'w>) -> R,
    ) -> R;

    /// Current replay-tail pressure. Called with no guard held.
    fn replay_pressure(&self) -> ReplayPressure;

    /// A group install freed index memory (`Admission::regrant`). Called with
    /// no guard held.
    fn regrant(&self);

    /// The first flusher failure: fail-stop the core. Called once, with no
    /// guard held.
    fn fail(&self, error: &FlushError);

    /// Clock for age selection.
    fn now(&self) -> Instant {
        Instant::now()
    }
}

/// External reasons to run a scheduler pass.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WakeReason {
    /// Publication touched cells. Send after the serving guards are dropped.
    Dirty,
    /// An Admission grant is blocked by a checkpoint-bound dimension: drain
    /// to a fixed target.
    AdmissionBlocked,
}

/// How [`CellFlushPool::request_stop`] treats queued and captured cells.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StopMode {
    /// Flush every queued cell, seal and install the last groups.
    Drain,
    /// Restore every captured cell without sealing and unqueue the rest.
    Cancel,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FlushRole {
    Scheduler,
    Worker(usize),
}

/// The flusher step that failed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FlushStage {
    OpenStore,
    Route,
    Reserve,
    Capture,
    Prepare,
    Append,
    Seal,
    Install,
    Restore,
}

#[derive(Clone, Debug)]
pub(crate) enum FlushError {
    Step {
        worker: usize,
        stage: FlushStage,
        cause: Arc<NativeIndexError>,
    },
    /// A capture was dropped without an install outside a failure.
    DroppedCapture {
        worker: usize,
    },
    Panicked {
        role: FlushRole,
    },
    /// A worker's job queue closed while the scheduler ran.
    Disconnected {
        worker: usize,
    },
    Spawn {
        role: FlushRole,
        cause: Arc<std::io::Error>,
    },
    /// [`CellFlushPool::wait_covered`] passed its deadline. Not a failure.
    Timeout {
        target: PrefixCut,
    },
    /// The pool stopped before the target was covered. Not a failure.
    Stopped,
}

impl fmt::Display for FlushError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Step {
                worker,
                stage,
                cause,
            } => write!(f, "cell flusher {worker} failed at {stage:?}: {cause}"),
            Self::DroppedCapture { worker } => {
                write!(f, "cell flusher {worker} dropped a capture without install")
            }
            Self::Panicked { role } => write!(f, "cell flush {role:?} panicked"),
            Self::Disconnected { worker } => write!(f, "cell flusher {worker} queue closed"),
            Self::Spawn { role, cause } => write!(f, "cannot start cell flush {role:?}: {cause}"),
            Self::Timeout { target } => write!(f, "cell coverage of {target:?} timed out"),
            Self::Stopped => f.write_str("cell flusher pool stopped"),
        }
    }
}

impl std::error::Error for FlushError {}

/// Pool totals of the [`CellMetrics`] counters this pool records.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct FlushStats {
    pub passes: u64,
    pub dispatched: u64,
    /// Sends that found a worker queue full.
    pub queue_full: u64,
    /// Cells returned to the scheduler because no flush allowance was free.
    pub deferred: u64,
    pub captured: u64,
    pub clean: u64,
    pub installed: u64,
    pub stale: u64,
    pub restored: u64,
}

struct Progress {
    failure: Option<FlushError>,
    /// Newest target for which a full pass found no cell missing a mutation
    /// at or below it.
    covered: PrefixCut,
    /// Newest explicitly requested target.
    requested: PrefixCut,
    passes: u64,
    stopped: bool,
}

struct Shared<H> {
    index: Arc<NativeIndex>,
    hooks: H,
    policy: FlushPolicy,
    /// Coalesced scheduler wake reasons. Leaf lock.
    wake: Mutex<u8>,
    wake_ready: Condvar,
    /// Leaf lock.
    progress: Mutex<Progress>,
    progress_changed: Condvar,
    /// Set before the scheduler is told to stop. Release/Acquire: a worker
    /// that observes the closed queue or the reopened store gate sees it.
    cancel: AtomicBool,
    /// Claimed by the first failure (`swap`, AcqRel) before the core is failed
    /// and `progress.failure` is stored; read with Acquire at step boundaries.
    failed: AtomicBool,
    /// Option C counters: this pool writes the worker, store, range and
    /// scheduler blocks, each from its one owning thread.
    metrics: Arc<CellMetrics>,
}

impl<H: FlushHooks> Shared<H> {
    fn wake(&self, reasons: u8) {
        *self.wake.lock() |= reasons;
        self.wake_ready.notify_one();
    }

    /// Take the pending reasons; park up to the deadline only when none is
    /// pending. Checking under the lock before parking loses no wake.
    fn wait_wake(&self) -> u8 {
        let mut reasons = self.wake.lock();
        if *reasons == 0 {
            // A timeout simply runs the deadline pass.
            let _ = self.wake_ready.wait_for(&mut reasons, self.policy.deadline);
        }
        std::mem::take(&mut *reasons)
    }

    fn failed(&self) -> bool {
        self.failed.load(Ordering::Acquire)
    }

    fn cancelled(&self) -> bool {
        self.cancel.load(Ordering::Acquire)
    }

    /// Record the first failure, wake every waiter and stop the scheduler.
    ///
    /// The first caller fails the core (`hooks.fail`, no lock held) before it
    /// stores `progress.failure`, so a waiter that observes the failure for
    /// any reason (this notify, another notify, a timeout or a spurious wake)
    /// never sees a healthy core. Workers read `failed` only to stop, so it
    /// turning true before `progress.failure` is stored is harmless.
    fn fail(&self, error: FlushError) {
        // AcqRel: exactly one caller claims the failure; later callers only
        // re-notify.
        if !self.failed.swap(true, Ordering::AcqRel) {
            self.hooks.fail(&error);
            self.progress.lock().failure = Some(error);
        }
        self.progress_changed.notify_all();
        self.wake(WAKE_STOP);
    }
}

/// Fails the core when its thread unwinds. Declare it first in the thread
/// body so it drops last.
struct PanicGuard<H: FlushHooks> {
    shared: Arc<Shared<H>>,
    role: FlushRole,
}

impl<H: FlushHooks> Drop for PanicGuard<H> {
    fn drop(&mut self) {
        if thread::panicking() {
            self.shared.fail(FlushError::Panicked { role: self.role });
        }
    }
}

/// Worker `w`'s shard range `[floor(wN/P), floor((w+1)N/P))`.
fn shard_range(worker: usize, shards: usize, workers: usize) -> Range<usize> {
    worker * shards / workers..(worker + 1) * shards / workers
}

/// `P = cell_flushers(N)`, the same count behind the native index's flight
/// limit `CELL_FLIGHTS_PER_FLUSHER * P`.
fn flushers(index: &NativeIndex) -> usize {
    cell_flushers(index.shard_count())
}

/// Selection class; lower is served first (spec §6.1).
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum Class {
    /// Holds the oldest required replay prefix: inside the drain target or
    /// lagging `L`.
    Prefix,
    Aged,
    Size,
}

struct Candidate {
    class: Class,
    oldest: BatchVersion,
    since: Option<Instant>,
    key: CellKey,
}

impl Candidate {
    fn rank(&self) -> (Class, BatchVersion, Option<Instant>) {
        (self.class, self.oldest, self.since)
    }
}

fn classify(
    policy: &FlushPolicy,
    dirty: &CellDirty,
    oldest: BatchVersion,
    now: Instant,
    target: Option<PrefixCut>,
    lagging: PrefixCut,
) -> Option<Class> {
    if target.is_some_and(|target| target.contains(oldest)) || lagging.contains(oldest) {
        Some(Class::Prefix)
    } else if dirty
        .dirty_since
        .is_some_and(|since| now.saturating_duration_since(since) >= policy.age)
    {
        Some(Class::Aged)
    } else if dirty.versions >= policy.dirty_versions || dirty.upper >= policy.dirty_upper {
        Some(Class::Size)
    } else {
        None
    }
}

struct Scheduler<H: FlushHooks> {
    shared: Arc<Shared<H>>,
    senders: Vec<SyncSender<Job>>,
    ranges: Vec<Range<usize>>,
    /// Rotating first shard of each range: ties start at a different shard
    /// every pass.
    cursors: Vec<usize>,
    /// Fixed drain target, served until no cell misses a mutation at or
    /// below it.
    target: Option<PrefixCut>,
    cells: Vec<(CellKey, CellDirty)>,
    /// Best `FLUSH_QUEUE` candidates of one range, plus one shard's cells.
    candidates: Vec<Candidate>,
}

impl<H: FlushHooks> Scheduler<H> {
    /// Actor loop. Ends on stop, cancellation or failure, then drops the job
    /// senders.
    fn run(&mut self) {
        loop {
            let reasons = self.shared.wait_wake();
            if reasons & WAKE_STOP != 0 || self.shared.failed() || self.shared.cancelled() {
                return;
            }
            if !self.pass(reasons) {
                return;
            }
        }
    }

    /// One rotation over every shard. False after a fatal queue error.
    fn pass(&mut self, reasons: u8) -> bool {
        let shared = Arc::clone(&self.shared);
        let index = &*shared.index;
        // The hint only says publication happened; the pass rereads facts.
        index.take_cell_dirty_signal();
        let published = index.published_prefix();
        let pressure = shared.hooks.replay_pressure();
        let started = self.target.is_none();
        if started {
            let (requested, covered) = {
                let progress = shared.progress.lock();
                (progress.requested, progress.covered)
            };
            self.target = if requested > covered {
                Some(requested.min(published))
            } else if pressure.drain || reasons & WAKE_PRESSURE != 0 {
                Some(published)
            } else {
                None
            };
        }
        let target = self.target;
        let started = started && target.is_some();
        let mut oldest_uncovered: Option<BatchVersion> = None;
        let mut oldest_dirty_since: Option<Instant> = None;
        let mut dirty_cells = 0u64;
        let now = shared.hooks.now();
        // A cell, queued or in flight included, still misses a mutation at or
        // below the target. Mutations at or below a published prefix are all
        // promoted before it is published, and new cells only receive newer
        // ones, so one clean rotation proves the target covered.
        let mut missing = false;
        for worker in 0..self.senders.len() {
            let range = self.ranges[worker].clone();
            let shards = range.len();
            self.candidates.clear();
            let (mut scanned, mut dirty_in_range) = (0usize, 0usize);
            for step in 0..shards {
                let shard = range.start + (self.cursors[worker] + step) % shards;
                index.shard_dirty_cells(shard, &mut self.cells);
                scanned += self.cells.len();
                for (key, dirty) in self.cells.drain(..) {
                    let Some(oldest) = dirty.oldest_uncovered else {
                        continue;
                    };
                    dirty_in_range += 1;
                    oldest_uncovered = Some(oldest_uncovered.map_or(oldest, |o| o.min(oldest)));
                    if let Some(since) = dirty.dirty_since {
                        oldest_dirty_since =
                            Some(oldest_dirty_since.map_or(since, |o| o.min(since)));
                    }
                    missing |= target.is_some_and(|target| target.contains(oldest));
                    // Only a mutation inside L can be captured.
                    if dirty.queued || dirty.in_flight || !published.contains(oldest) {
                        continue;
                    }
                    if let Some(class) = classify(
                        &shared.policy,
                        &dirty,
                        oldest,
                        now,
                        target,
                        pressure.lagging,
                    ) {
                        self.candidates.push(Candidate {
                            class,
                            oldest,
                            since: dirty.dirty_since,
                            key,
                        });
                    }
                }
                if self.candidates.len() > FLUSH_QUEUE {
                    self.candidates
                        .select_nth_unstable_by_key(FLUSH_QUEUE - 1, Candidate::rank);
                    self.candidates.truncate(FLUSH_QUEUE);
                }
            }
            shared
                .metrics
                .range(worker)
                .scanned(scanned, dirty_in_range);
            dirty_cells += dirty_in_range as u64;
            self.cursors[worker] = (self.cursors[worker] + 1) % shards;
            self.candidates.sort_unstable_by_key(Candidate::rank);
            if !self.dispatch(worker, target) {
                return false;
            }
        }
        let mut progress = shared.progress.lock();
        let covered_now = target.is_some() && !missing;
        if let Some(target) = target
            && !missing
        {
            self.target = None;
            progress.covered = progress.covered.max(target);
        }
        progress.passes += 1;
        let covered = progress.covered;
        drop(progress);
        shared.progress_changed.notify_all();
        shared.metrics.scheduler_pass(
            PassGauges {
                target: self.target,
                covered: Some(covered),
                oldest_uncovered,
                oldest_dirty_since,
                dirty_cells,
            },
            started,
            covered_now,
        );
        true
    }

    /// Send the sorted candidates to `worker` until its queue is full.
    fn dispatch(&mut self, worker: usize, target: Option<PrefixCut>) -> bool {
        let index = &*self.shared.index;
        let range = self.shared.metrics.range(worker);
        for Candidate {
            key, class, oldest, ..
        } in self.candidates.drain(..)
        {
            if !index.try_queue_cell(key.0, &key.1) {
                continue;
            }
            match self.senders[worker].try_send((key, Instant::now())) {
                Ok(()) => range.selected(match class {
                    Class::Prefix if target.is_some_and(|target| target.contains(oldest)) => {
                        SelectionReason::Pressure
                    }
                    Class::Prefix => SelectionReason::Lagging,
                    Class::Aged => SelectionReason::Aged,
                    Class::Size => SelectionReason::Size,
                }),
                Err(TrySendError::Full((key, _))) => {
                    // The dirty state stays; a later pass sends it again.
                    index.unqueue_cell(key.0, &key.1);
                    range.queue_full();
                    break;
                }
                Err(TrySendError::Disconnected((key, _))) => {
                    index.unqueue_cell(key.0, &key.1);
                    self.shared.fail(FlushError::Disconnected { worker });
                    return false;
                }
            }
        }
        true
    }
}

/// Captured cells of one worker that still need an install. Dropping it
/// non-empty is the fail-stop path: it records the failure before the
/// captures drop.
struct Flights<H: FlushHooks> {
    worker: usize,
    shared: Arc<Shared<H>>,
    /// Appended to the open segment, awaiting its seal.
    prepared: Vec<PreparedCell>,
    /// Flush or restore results awaiting install.
    results: Vec<CellFlushResult>,
    /// Stale installs awaiting restore.
    stale: Vec<PreparedCell>,
}

impl<H: FlushHooks> Drop for Flights<H> {
    fn drop(&mut self) {
        if !(self.prepared.is_empty() && self.results.is_empty() && self.stale.is_empty()) {
            self.shared.fail(if thread::panicking() {
                FlushError::Panicked {
                    role: FlushRole::Worker(self.worker),
                }
            } else {
                FlushError::DroppedCapture {
                    worker: self.worker,
                }
            });
        }
    }
}

struct Worker<H: FlushHooks, S> {
    id: usize,
    range: Range<usize>,
    shared: Arc<Shared<H>>,
    jobs: Receiver<Job>,
    store: S,
    flights: Flights<H>,
    group_bytes: u64,
    // Reused, capacity `GROUP_CELLS`: pushes under the serving guards never
    // allocate.
    bases: Vec<NewCellBase>,
    retired: Vec<CellRetired>,
    discarded: Vec<NewCellBase>,
    /// Store counters present when this worker started; the store's own
    /// cumulative counters are published on top of them.
    store_base: StoreSnapshot,
}

impl<H: FlushHooks, S: CellStore> Worker<H, S> {
    fn new(
        id: usize,
        range: Range<usize>,
        shared: Arc<Shared<H>>,
        jobs: Receiver<Job>,
        store: S,
    ) -> Self {
        let store_base = shared.metrics.store(id).base();
        Self {
            id,
            range,
            flights: Flights {
                worker: id,
                shared: Arc::clone(&shared),
                prepared: Vec::with_capacity(GROUP_CELLS),
                results: Vec::with_capacity(GROUP_CELLS),
                stale: Vec::with_capacity(GROUP_CELLS),
            },
            shared,
            jobs,
            store,
            group_bytes: 0,
            bases: Vec::with_capacity(GROUP_CELLS),
            retired: Vec::with_capacity(GROUP_CELLS),
            discarded: Vec::with_capacity(GROUP_CELLS),
            store_base,
        }
    }

    fn error(&self, stage: FlushStage, cause: NativeIndexError) -> FlushError {
        FlushError::Step {
            worker: self.id,
            stage,
            cause: Arc::new(cause),
        }
    }

    /// Serve jobs until the scheduler drops the sender. Group cells while
    /// jobs are ready; seal on the group limit or an empty queue (no timer).
    fn run(&mut self) -> Result<(), FlushError> {
        loop {
            if self.shared.failed() {
                return Ok(());
            }
            if self.shared.cancelled() {
                return self.cancel();
            }
            let job = if self.flights.prepared.is_empty() {
                match self.jobs.recv() {
                    Ok(job) => job,
                    Err(_) => return Ok(()),
                }
            } else {
                match self.jobs.try_recv() {
                    Ok(job) => job,
                    Err(TryRecvError::Empty) => {
                        self.seal_install()?;
                        continue;
                    }
                    Err(TryRecvError::Disconnected) => {
                        return if self.shared.failed() {
                            Ok(())
                        } else if self.shared.cancelled() {
                            self.cancel()
                        } else {
                            self.seal_install()
                        };
                    }
                }
            };
            let (job, sent) = job;
            self.shared.metrics.worker(self.id).dequeued(sent.elapsed());
            if self.shared.failed() || self.shared.cancelled() {
                self.shared.index.unqueue_cell(job.0, &job.1);
                continue;
            }
            self.flush_cell(job)?;
            if self.flights.prepared.len() >= GROUP_CELLS || self.group_bytes >= GROUP_BYTES {
                self.seal_install()?;
            }
        }
    }

    /// Capture, prepare and append one cell. A cell that finds no free flush
    /// allowance seals this worker's group once (its permits may hold the
    /// allowance) and otherwise returns to the scheduler.
    fn flush_cell(&mut self, (keyspace, cell): CellKey) -> Result<(), FlushError> {
        let shared = Arc::clone(&self.shared);
        let index = &*shared.index;
        if !self.range.contains(&index.shard_of_cell(keyspace, &cell)) {
            index.unqueue_cell(keyspace, &cell);
            return Err(self.error(
                FlushStage::Route,
                NativeIndexError::Invalid("cell outside the flusher's shard range"),
            ));
        }
        for attempt in 0..2 {
            let can_seal = attempt == 0 && !self.flights.prepared.is_empty();
            let permit = match index.reserve_cell_flush(0) {
                Ok(permit) => permit,
                Err(NativeIndexError::Capacity) if can_seal => {
                    self.seal_install()?;
                    continue;
                }
                Err(NativeIndexError::Capacity) => break,
                Err(error) => return Err(self.error(FlushStage::Reserve, error)),
            };
            let requested = Instant::now();
            let mut held = requested;
            let outcome = {
                let mut writer = index.writer();
                let outcome = shared.hooks.with_publisher(&mut writer, |writer| {
                    held = Instant::now();
                    // `L` under publication serialization (spec §5.2).
                    let target = writer.published_prefix();
                    writer.capture_cell(keyspace, &cell, target, permit)
                });
                shared
                    .metrics
                    .worker(self.id)
                    .capture_hold(held - requested, held.elapsed());
                outcome
            };
            match outcome.map_err(|error| self.error(FlushStage::Capture, error))? {
                CellCaptureOutcome::Captured(capture) => {
                    let captured = capture.totals();
                    let prepared = capture
                        .prepare()
                        .map_err(|error| self.error(FlushStage::Prepare, error))?;
                    shared.metrics.worker(self.id).captured(
                        captured.versions,
                        captured.rows,
                        prepared.residual_totals().versions,
                    );
                    // Held in `Flights` before the store runs: an error or a
                    // panic in `append` fails the core before the capture
                    // drops.
                    self.flights.prepared.push(prepared);
                    // In bounds: the cell was pushed just above.
                    let last = self.flights.prepared.len() - 1;
                    self.store.set_profile(shared.metrics.profiling());
                    let cpu_start = thread_cpu_ns();
                    let append_start = Instant::now();
                    let bytes = self
                        .store
                        .append(&self.flights.prepared[last])
                        .map_err(|error| self.error(FlushStage::Append, error))?;
                    let wall = append_start.elapsed();
                    let cpu = cpu_start
                        .zip(thread_cpu_ns())
                        .map_or(0, |(start, end)| end.saturating_sub(start));
                    shared.metrics.worker(self.id).appended(wall, cpu);
                    self.publish_store_stats();
                    self.group_bytes = self.group_bytes.saturating_add(bytes);
                    return Ok(());
                }
                CellCaptureOutcome::Clean { .. } => {
                    // Defensive: the scheduler queues a cell only when its
                    // oldest uncovered version lies in the published prefix,
                    // which never shrinks, and only this queued capture can
                    // remove that version. The real-process crash matrix
                    // therefore has no cut here; the pure cell model
                    // (`cell_model.rs`) covers a clean advance after capture.
                    shared.metrics.worker(self.id).clean();
                    return Ok(());
                }
                CellCaptureOutcome::Skipped {
                    reason: CellSkip::NeedsPermit { .. },
                    permit,
                } => {
                    drop(permit);
                    if !can_seal {
                        break;
                    }
                    self.seal_install()?;
                }
                CellCaptureOutcome::Skipped { reason, .. } => {
                    return Err(self.error(
                        FlushStage::Capture,
                        NativeIndexError::Invalid(match reason {
                            CellSkip::InFlight => "a dispatched cell already has a flight",
                            CellSkip::ExactMode => "cell flusher on an exact-checkpoint index",
                            CellSkip::NeedsPermit { .. } => "unreachable permit skip",
                        }),
                    ));
                }
            }
        }
        // No allowance now: an install by any worker wakes the scheduler,
        // which sends the cell again.
        index.unqueue_cell(keyspace, &cell);
        shared.metrics.worker(self.id).deferred();
        Ok(())
    }

    /// Publish the store's cumulative counters (one writer per worker).
    fn publish_store_stats(&self) {
        self.shared
            .metrics
            .store(self.id)
            .publish(&self.store_base, &self.store.stats());
    }

    /// Seal the open group, install its results and restore stale ones.
    fn seal_install(&mut self) -> Result<(), FlushError> {
        if self.flights.prepared.is_empty() {
            return Ok(());
        }
        self.bases.clear();
        let seal_start = Instant::now();
        let sealed = self.store.seal(&mut self.bases);
        self.shared
            .metrics
            .worker(self.id)
            .sealed(seal_start.elapsed());
        self.publish_store_stats();
        sealed.map_err(|error| self.error(FlushStage::Seal, error))?;
        if self.bases.len() != self.flights.prepared.len() {
            return Err(self.error(
                FlushStage::Seal,
                NativeIndexError::Invalid("index store sealed a base count unlike its appends"),
            ));
        }
        self.group_bytes = 0;
        for (prepared, base) in self.flights.prepared.drain(..).zip(self.bases.drain(..)) {
            self.flights.results.push(prepared.finish(base));
        }
        self.install_results(FlushStage::Install)?;
        if !self.flights.stale.is_empty() {
            while let Some(prepared) = self.flights.stale.pop() {
                let result = prepared
                    .restore()
                    .map_err(|error| self.error(FlushStage::Restore, error))?;
                self.flights.results.push(result);
            }
            self.install_results(FlushStage::Restore)?;
        }
        Ok(())
    }

    /// Install every pending result in one writer -> publisher hold, then
    /// drop the replaced holders outside it and regrant Admission.
    fn install_results(&mut self, stage: FlushStage) -> Result<(), FlushError> {
        let count = self.flights.results.len();
        let stale_before = self.flights.stale.len();
        let Self {
            shared,
            flights,
            retired,
            discarded,
            ..
        } = self;
        let requested = Instant::now();
        let mut held = requested;
        let mut hold = Duration::ZERO;
        #[cfg(test)]
        crate::failpoints::write_model::boundary("cell_install_before");
        let rejected = {
            let mut writer = shared.index.writer();
            shared.hooks.with_publisher(&mut writer, |writer| {
                held = Instant::now();
                // `pop` leaves uninstalled results in place on a rejection.
                let rejected = loop {
                    let Some(result) = flights.results.pop() else {
                        break None;
                    };
                    match writer.install_cell(result) {
                        CellInstall::Installed { retired: old, .. } => retired.push(old),
                        CellInstall::Stale {
                            prepared,
                            discarded: base,
                        } => {
                            discarded.push(base);
                            flights.stale.push(prepared);
                        }
                        CellInstall::Rejected { error, result } => break Some((error, result)),
                    }
                };
                hold = held.elapsed();
                rejected
            })
        };
        let wait = held - requested;
        // The install hold (writer -> publisher) is released.
        #[cfg(test)]
        crate::failpoints::write_model::boundary("cell_install_after");
        if let Some((error, result)) = rejected {
            let failure = self.error(stage, error);
            self.shared.fail(failure.clone());
            drop(result);
            return Err(failure);
        }
        let stale = self.flights.stale.len() - stale_before;
        if stage == FlushStage::Restore && stale != 0 {
            return Err(self.error(
                stage,
                NativeIndexError::Invalid("a restore result was stale"),
            ));
        }
        // Freed outside the guards; then the memory is visible to Admission.
        self.retired.clear();
        self.discarded.clear();
        let (installed, stale, restored) = if stage == FlushStage::Restore {
            (0, 0, count)
        } else {
            (count - stale, stale, 0)
        };
        self.shared
            .metrics
            .worker(self.id)
            .installed(installed, stale, restored, wait, hold);
        self.shared.hooks.regrant();
        self.shared.wake(WAKE_PROGRESS);
        Ok(())
    }

    /// Restore the open group without sealing (its segment becomes an orphan
    /// the next open deletes), then unqueue the cells still queued until the
    /// stopping scheduler closes the queue.
    fn cancel(&mut self) -> Result<(), FlushError> {
        while let Some(prepared) = self.flights.prepared.pop() {
            let result = prepared
                .restore()
                .map_err(|error| self.error(FlushStage::Restore, error))?;
            self.flights.results.push(result);
        }
        self.group_bytes = 0;
        if !self.flights.results.is_empty() {
            self.install_results(FlushStage::Restore)?;
        }
        for ((keyspace, cell), sent) in self.jobs.iter() {
            self.shared.metrics.worker(self.id).dequeued(sent.elapsed());
            self.shared.index.unqueue_cell(keyspace, &cell);
        }
        Ok(())
    }
}

/// The running flusher pool: `P` workers and the scheduler, all joined by
/// [`Self::join`] or on drop.
pub(crate) struct CellFlushPool<H: FlushHooks> {
    shared: Arc<Shared<H>>,
    workers: usize,
    threads: Vec<JoinHandle<()>>,
}

impl<H: FlushHooks> CellFlushPool<H> {
    /// Start `cell_flushers(N)` workers, each with the store `stores(w)`
    /// (stream id `w`), and the scheduler. `metrics` must be sized for
    /// `cell_flushers(N)` workers; the pool records its worker, store, range
    /// and scheduler counters and its threads' CPU there.
    pub(crate) fn start<S: CellStore + 'static>(
        index: Arc<NativeIndex>,
        hooks: H,
        policy: FlushPolicy,
        metrics: Arc<CellMetrics>,
        stores: impl FnMut(usize) -> Result<S, NativeIndexError>,
    ) -> Result<Self, FlushError> {
        let workers = flushers(&index);
        Self::start_with(index, hooks, policy, workers, metrics, stores)
    }

    fn start_with<S: CellStore + 'static>(
        index: Arc<NativeIndex>,
        hooks: H,
        policy: FlushPolicy,
        workers: usize,
        metrics: Arc<CellMetrics>,
        mut stores: impl FnMut(usize) -> Result<S, NativeIndexError>,
    ) -> Result<Self, FlushError> {
        let shards = index.shard_count();
        debug_assert!((1..=shards).contains(&workers));
        if metrics.workers() != workers {
            return Err(FlushError::Step {
                worker: 0,
                stage: FlushStage::OpenStore,
                cause: Arc::new(NativeIndexError::Invalid(
                    "cell metrics sized for another flusher count",
                )),
            });
        }
        let mut opened = Vec::with_capacity(workers);
        for worker in 0..workers {
            opened.push(stores(worker).map_err(|cause| FlushError::Step {
                worker,
                stage: FlushStage::OpenStore,
                cause: Arc::new(cause),
            })?);
        }
        let shared = Arc::new(Shared {
            index,
            hooks,
            policy,
            wake: Mutex::new(0),
            wake_ready: Condvar::new(),
            progress: Mutex::new(Progress {
                failure: None,
                covered: PrefixCut::BeforeAll,
                requested: PrefixCut::BeforeAll,
                passes: 0,
                stopped: false,
            }),
            progress_changed: Condvar::new(),
            cancel: AtomicBool::new(false),
            failed: AtomicBool::new(false),
            metrics,
        });
        // On an early return the pool's drop cancels and joins what started.
        let mut pool = Self {
            shared: Arc::clone(&shared),
            workers,
            threads: Vec::with_capacity(workers + 1),
        };
        let mut senders = Vec::with_capacity(workers);
        let mut ranges = Vec::with_capacity(workers);
        for (id, store) in opened.into_iter().enumerate() {
            let (sender, jobs) = sync_channel(FLUSH_QUEUE);
            let range = shard_range(id, shards, workers);
            let worker_shared = Arc::clone(&shared);
            let worker_range = range.clone();
            let thread = thread::Builder::new()
                .name(format!("value-index-flush-{id}"))
                .spawn(move || {
                    let _panic = PanicGuard {
                        shared: Arc::clone(&worker_shared),
                        role: FlushRole::Worker(id),
                    };
                    let _cpu = worker_shared.metrics.enter_worker(id);
                    let mut worker = Worker::new(id, worker_range, worker_shared, jobs, store);
                    if let Err(error) = worker.run() {
                        worker.shared.fail(error);
                    }
                })
                .map_err(|cause| FlushError::Spawn {
                    role: FlushRole::Worker(id),
                    cause: Arc::new(cause),
                })?;
            pool.threads.push(thread);
            senders.push(sender);
            ranges.push(range);
        }
        let mut scheduler = Scheduler {
            shared: Arc::clone(&shared),
            senders,
            ranges,
            cursors: vec![0; workers],
            target: None,
            cells: Vec::new(),
            candidates: Vec::with_capacity(FLUSH_QUEUE),
        };
        let thread = thread::Builder::new()
            .name("value-index-flush-scheduler".to_owned())
            .spawn(move || {
                let _panic = PanicGuard {
                    shared: Arc::clone(&scheduler.shared),
                    role: FlushRole::Scheduler,
                };
                let _cpu = scheduler.shared.metrics.enter_scheduler();
                scheduler.run();
                // No pass runs after this: `covered` is final, so waiters
                // of an uncovered target return `Stopped`.
                scheduler.shared.progress.lock().stopped = true;
                scheduler.shared.progress_changed.notify_all();
            })
            .map_err(|cause| FlushError::Spawn {
                role: FlushRole::Scheduler,
                cause: Arc::new(cause),
            })?;
        pool.threads.push(thread);
        Ok(pool)
    }

    pub(crate) fn workers(&self) -> usize {
        self.workers
    }

    pub(crate) fn wake(&self, reason: WakeReason) {
        self.shared.wake(match reason {
            WakeReason::Dirty => WAKE_DIRTY,
            WakeReason::AdmissionBlocked => WAKE_PRESSURE,
        });
    }

    /// Ask the scheduler to drain to `target`, a complete prefix (an explicit
    /// index flush or checkpoint barrier). Does not wait.
    pub(crate) fn request_target(&self, target: PrefixCut) {
        {
            let mut progress = self.shared.progress.lock();
            if target <= progress.covered {
                return;
            }
            progress.requested = progress.requested.max(target);
        }
        self.shared.wake(WAKE_REQUEST);
    }

    /// Newest target with no cell missing a mutation at or below it.
    pub(crate) fn covered(&self) -> PrefixCut {
        self.shared.progress.lock().covered
    }

    /// Wait until `target` is covered, a failure, the pool stops, or
    /// `timeout` passes. Use [`Self::request_target`] (or pressure) to start
    /// the drain.
    pub(crate) fn wait_covered(
        &self,
        target: PrefixCut,
        timeout: Duration,
    ) -> Result<(), FlushError> {
        let started = Instant::now();
        let mut progress = self.shared.progress.lock();
        loop {
            if let Some(failure) = &progress.failure {
                return Err(failure.clone());
            }
            if progress.covered >= target {
                return Ok(());
            }
            if progress.stopped {
                return Err(FlushError::Stopped);
            }
            let Some(remaining) = timeout.checked_sub(started.elapsed()) else {
                return Err(FlushError::Timeout { target });
            };
            let _ = self
                .shared
                .progress_changed
                .wait_for(&mut progress, remaining);
        }
    }

    pub(crate) fn stats(&self) -> FlushStats {
        let metrics = &self.shared.metrics;
        let (workers, ranges) = (metrics.worker_totals(), metrics.range_totals());
        FlushStats {
            passes: self.shared.progress.lock().passes,
            dispatched: ranges.dispatched(),
            queue_full: ranges.queue_full,
            deferred: workers.deferred,
            captured: workers.captured,
            clean: workers.clean,
            installed: workers.installed,
            stale: workers.stale,
            restored: workers.restored,
        }
    }

    /// Stop the scheduler; workers then drain or cancel per `mode`. Does not
    /// wait: [`Self::join`] does.
    pub(crate) fn request_stop(&self, mode: StopMode) {
        if mode == StopMode::Cancel {
            self.shared.cancel.store(true, Ordering::Release);
        }
        self.shared.wake(WAKE_STOP);
    }

    /// Stop with `mode` and join every thread.
    pub(crate) fn shutdown(self, mode: StopMode) -> Result<(), FlushError> {
        self.request_stop(mode);
        self.join()
    }

    /// Join every thread after [`Self::request_stop`]. Returns the first
    /// failure.
    pub(crate) fn join(mut self) -> Result<(), FlushError> {
        self.join_threads()
    }

    fn join_threads(&mut self) -> Result<(), FlushError> {
        // The scheduler exits first and drops the job senders; then every
        // worker drains or cancels and exits.
        for thread in self.threads.drain(..) {
            // A panic was already recorded by the thread's guard.
            let _ = thread.join();
        }
        let failure = {
            let mut progress = self.shared.progress.lock();
            progress.stopped = true;
            progress.failure.clone()
        };
        self.shared.progress_changed.notify_all();
        failure.map_or(Ok(()), Err)
    }

    #[cfg(test)]
    fn wait_passes(&self, passes: u64, timeout: Duration) {
        let started = Instant::now();
        let mut progress = self.shared.progress.lock();
        while progress.passes < passes {
            let remaining = timeout
                .checked_sub(started.elapsed())
                .unwrap_or_else(|| panic!("scheduler pass {passes} did not finish"));
            let _ = self
                .shared
                .progress_changed
                .wait_for(&mut progress, remaining);
        }
    }
}

impl<H: FlushHooks> Drop for CellFlushPool<H> {
    fn drop(&mut self) {
        if !self.threads.is_empty() {
            self.request_stop(StopMode::Cancel);
            // The failure, if any, was already reported through the hooks.
            let _ = self.join_threads();
        }
    }
}

#[cfg(test)]
#[path = "cell_flush_tests.rs"]
mod tests;
