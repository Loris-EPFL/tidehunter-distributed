//! One bounded FIFO owner for this node's stream requests and controls.
//!
//! Accepted work owns its payload through completion, drains on shutdown and
//! never runs synchronous WAL I/O on network progress. Stream requests (live
//! extent leases and adoptions, record resolution, position proofs, durable
//! progress polls) overlap; every other control is an exclusive barrier that
//! first drains the ordered flush queue.

use super::super::durability::DurabilityConfig;
use super::super::durability::DurabilityTier;
use super::super::durability::frontier::{Frontier, Syncer, flush_all};
use super::super::storage::LogDurableProgress;
use super::durable_progress::{self, HubSender, PollIdentity, Wake};
use super::*;
use parking_lot::Condvar;
use std::collections::VecDeque;
use std::sync::mpsc;
use std::thread::JoinHandle;

#[cfg(test)]
#[path = "control_forced_pause_tests.rs"]
mod forced_pause_tests;
#[cfg(test)]
mod retired_syncer_tests;
#[cfg(test)]
#[path = "control_tests.rs"]
mod tests;

const MAX_COMMANDS: usize = 64;
const MAX_COMMAND_BYTES: usize = 256 * 1024 * 1024;
// See `Shared::flush` for the capacity bound.
const FLUSH_QUEUE: usize = 2 * MAX_COMMANDS + 16;
// Fixed worker set. Stream requests overlap up to this many at a time; the
// partition's state lock orders their state changes, not this pool.
const CONTROL_WORKERS: usize = 4;

type PublishedLiveLeases = (
    [u8; 16],
    Vec<(
        super::super::nvmeof::live_storage::LiveWalSegment,
        super::super::nvmeof::live_storage::LiveTargetBinding,
        u64,
        u64,
    )>,
);

pub(super) struct ControlContext {
    pub(super) partition: PartitionConfig,
    pub(super) inventory: Arc<Mutex<NodeInventory>>,
    /// Node lifecycle gate (see `inventory::LifecycleGate`): taken before
    /// `inventory`, held across lifecycle and live target syncs.
    pub(super) lifecycle: inventory::LifecycleGate,
    pub(super) readers: Arc<parking_lot::RwLock<inventory::ReaderAuthority>>,
    /// A lifecycle drop sets this under the live-control and lifecycle gates.
    /// Session sweeps acquire it; a queued fence rechecks under those gates.
    pub(super) closed: AtomicBool,
    pub(super) terminal: Arc<AtomicBool>,
    pub(super) poison_pending: Arc<AtomicBool>,
    /// Sticky for this owner incarnation: set by both poison funnels, never cleared,
    /// not even by a successful live recovery fence. Shared with the inventory, which
    /// reports `NodeStatus::recovery_clean = false` once it is set.
    pub(super) persistence_failed: Arc<AtomicBool>,
    pub(super) recoverable_live_failure: AtomicBool,
    pub(super) poison_gate: Mutex<()>,
    pub(super) wal_member_read_policy: super::super::storage::WalMemberReadPolicy,
    /// With async member reads, caps `NodeStatus::max_reads` and each
    /// `ResolveLiveRecords` request (see `effective_max_reads`).
    pub(super) max_device_reads_per_status: usize,
    /// Owner OFI transport counters, reported by `PartitionStats`. Sessions
    /// write their own cache-line shard (relaxed); a snapshot is not atomic.
    pub(super) transport: Arc<OwnerTransportCounters>,
    /// Data-free live-control gate. Lock order: this gate, then the lifecycle
    /// gate, then the inventory, then `readers`. Fence, seal, generation trim,
    /// older-stream retirement, live read publication, live pin release and
    /// target shutdown take it first. The scheduler orders serving retirement
    /// against exclusive controls; it does not take this gate. Terminal
    /// publication only `try_lock`s it and defers while it is held.
    pub(super) live_control_gate: Mutex<()>,
    #[cfg(test)]
    pub(super) before_fence: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}

pub(super) struct ControlWork {
    pub(super) id: u64,
    pub(super) request: Request,
    pub(super) limits: TcpLimits,
    pub(super) measured: bool,
    pub(super) queued: Option<Instant>,
    pub(super) submitted: Option<Instant>,
    pub(super) charge: usize,
    pub(super) completed: mpsc::SyncSender<reactor::WorkCompletion>,
    /// Durable-progress requests only: wakes the session once the reply is
    /// published. Drops after `complete`/`deliver` on every path of `run`, or
    /// moves into the parked hub waiter.
    pub(super) progress: Option<super::progress_wait::CompletionNotify>,
}

struct Queue {
    work: VecDeque<ControlWork>,
    // Includes the command executing off-lock, not just waiting commands.
    requests: usize,
    bytes: usize,
    stopped: bool,
    flush_failed: bool,
    // Stream operations between dequeue and completion/flush hand-off.
    active_appends: usize,
    // An exclusive control is executing; nothing else may start.
    control_active: bool,
}
struct Shared {
    queue: Mutex<Queue>,
    ready: Condvar,
    context: Arc<ControlContext>,
    // Owner: the flusher thread (sole receiver). Capacity: at most one
    // `Barrier` per control worker (each waits for its own before continuing);
    // the capacity below exceeds that, so a send made under the queue lock
    // never blocks. Full-queue policy: unreachable by this bound.
    // Shutdown: `Stop` after the workers joined; queued items are served first.
    flush: mpsc::SyncSender<FlushCommand>,
    // Lock order: `durability` before `syncers`. Both are cold on the append
    // path (first append of each log, configuration and live fencing). A live
    // extent request (lease or adoption) reads the tier under `durability`
    // once per request; no lock here is taken per put.
    durability: Mutex<DurabilityState>,
    // One syncer thread per log that received appends. Live fencing retires the
    // old log's worker before recovery; shutdown joins any remaining workers.
    syncers: Mutex<Vec<SyncerSlot>>,
    // Leaf lock: never hold with `durability` or `syncers`; join off-lock.
    preparers: Mutex<Vec<PreparerSlot>>,
    // Parked durable-progress polls. Log syncers notify it after each sync.
    progress: HubSender,
    // Test-only cap of a poll's wait (ms), default `DURABLE_PROGRESS_MAX_WAIT`.
    // A long cap lets a test tell a wake from a deadline reply without a clock.
    #[cfg(test)]
    progress_cap_ms: std::sync::atomic::AtomicU64,
    #[cfg(test)]
    session_barrier_waits: std::sync::atomic::AtomicU64,
    #[cfg(test)]
    grant_profiles: Mutex<(Vec<TestGrantProfile>, u64)>,
    #[cfg(test)]
    before_target_shutdown: Mutex<Option<(mpsc::SyncSender<()>, mpsc::Receiver<()>)>>,
}

struct DurabilityState {
    config: DurabilityConfig,
    // Remains fixed even after a live fence retires the last syncer slot.
    started: bool,
}

struct SyncerSlot {
    log: Arc<super::super::storage::PartitionLog>,
    // Dropping stops the syncer after one final sync of everything appended.
    syncer: Syncer,
}

struct PreparerSlot {
    log: Arc<super::super::storage::PartitionLog>,
    trigger: mpsc::SyncSender<()>,
    stopping: Arc<AtomicBool>,
    #[cfg(test)]
    waiting_for_gate: Arc<AtomicBool>,
    worker: JoinHandle<()>,
}

#[cfg(test)]
#[derive(Clone, serde::Serialize)]
struct TestGrantProfile {
    envelope_id: u64,
    file_index: u64,
    new_file: bool,
    profile: LeaseProfile,
    prepared: u64,
    published: u64,
    timing: super::super::storage::LiveTestLeaseTiming,
    prelog_inventory_ns: u64,
    prelog_frontier_ns: u64,
    prelog_tier_ns: u64,
}

impl Shared {
    /// One worker owns each log's preparation triggers. Capacity one: a full
    /// queue coalesces triggers because the worker recomputes the target.
    /// Shutdown sets stopping, wakes publication retries, drops the sender and joins.
    fn trigger_file_preparation(&self, log: &Arc<super::super::storage::PartitionLog>) {
        let mut slots = self.preparers.lock();
        let slot = if let Some(index) = slots.iter().position(|slot| Arc::ptr_eq(&slot.log, log)) {
            &slots[index]
        } else {
            let (trigger, receiver) = mpsc::sync_channel::<()>(1);
            let owner = log.clone();
            let context = self.context.clone();
            let stopping = Arc::new(AtomicBool::new(false));
            let worker_stopping = stopping.clone();
            #[cfg(test)]
            let waiting_for_gate = Arc::new(AtomicBool::new(false));
            #[cfg(test)]
            let worker_waiting = waiting_for_gate.clone();
            let worker = match thread::Builder::new()
                .name("value-wal-file-prep".into())
                .spawn(move || {
                    // Intended receive loop: sender closure terminates it.
                    while receiver.recv().is_ok() {
                        // Acquire observes shutdown before any new preparation.
                        if worker_stopping.load(Ordering::Acquire) {
                            break;
                        }
                        // Fatal I/O errors poison the log in this method.
                        // Allocation errors leave the grant path as the retry.
                        if let Ok(Some(file)) = owner.prepare_next_live_file() {
                            context.publish_prepared_file(
                                &owner,
                                file,
                                &worker_stopping,
                                #[cfg(test)]
                                &worker_waiting,
                            );
                        }
                        // Maintenance after preparation: the prepared
                        // successor always goes first. A seal is bounded,
                        // takes no lifecycle gate and changes no namespace.
                        if !worker_stopping.load(Ordering::Acquire) {
                            context.seal_ready_files(&owner);
                        }
                    }
                }) {
                Ok(worker) => worker,
                // Preparation is optional; the grant still performs the work.
                Err(_) => return,
            };
            slots.push(PreparerSlot {
                log: log.clone(),
                trigger,
                stopping,
                #[cfg(test)]
                waiting_for_gate,
                worker,
            });
            &slots[slots.len() - 1]
        };
        let _ = slot.trigger.try_send(());
    }

    fn join_preparer(&self, slot: PreparerSlot) -> Result<(), TcpError> {
        let PreparerSlot {
            log,
            trigger,
            stopping,
            worker,
            ..
        } = slot;
        // Release hands shutdown to the worker; unpark interrupts its retry wait.
        stopping.store(true, Ordering::Release);
        worker.thread().unpark();
        drop(trigger);
        let result = worker.join();
        drop(log);
        result.map_err(|_| {
            self.context.poison_readers();
            TcpError::new("log file preparer panicked; restart required")
        })
    }

    /// The log's frontier, creating it and its syncer on the first append.
    fn frontier_for(
        &self,
        log: &Arc<super::super::storage::PartitionLog>,
    ) -> Result<Arc<Frontier>, TcpError> {
        if let Some(frontier) = log.frontier() {
            return Ok(frontier.clone());
        }
        let mut durability = self.durability.lock();
        let mut slots = self.syncers.lock();
        if let Some(frontier) = log.frontier() {
            return Ok(frontier.clone());
        }
        let frontier = log
            .attach_frontier(&durability.config)
            .map_err(TcpError::component)?;
        let worker = log.clone();
        let progress = self.progress.clone();
        let syncer = Syncer::spawn("value-wal-syncer", frontier.clone(), move |_end| {
            let result = worker.sync_for_frontier();
            // After the publication, so a woken poll sees this sync. A WAL I/O failure
            // has already failed the frontier (`PartitionLog::persist_scope`); any other
            // failure fails it after this call and a parked nudge sees it at its deadline.
            progress.advanced();
            result
        })?;
        slots.push(SyncerSlot {
            log: log.clone(),
            syncer,
        });
        durability.started = true;
        Ok(frontier)
    }

    /// Wait until every log's synced frontier covers what it had appended when
    /// this began. The cold flush behind every non-append control.
    fn flush_logs(&self) -> Result<(), TcpError> {
        let logs: Vec<_> = self
            .syncers
            .lock()
            .iter()
            .map(|slot| slot.log.clone())
            .collect();
        for log in &logs {
            log.self_adopt_live_extent().map_err(TcpError::component)?;
        }
        let frontiers: Vec<_> = logs
            .iter()
            .filter_map(|log| log.frontier().cloned())
            .collect();
        // Start every flush before waiting for any: logs sync in parallel.
        let report = flush_all(&frontiers, None);
        report
            .into_synced()
            .map(|_| ())
            .map_err(|error| TcpError::new(format!("log frontier flush failed: {error}")))?;
        // An explicit flush also catches up a live root the syncer deferred. The
        // catch-up publishes durable progress, so parked polls re-check.
        {
            for log in &logs {
                log.persist_live_root()
                    .map_err(|error| TcpError::new(format!("live root flush failed: {error}")))?;
            }
            self.progress.advanced();
        }
        Ok(())
    }

    /// Called only by an exclusive, validated live fence after target revocation.
    /// Release the registry lock before joining: the worker owns the WAL I/O.
    fn retire_syncer(
        &self,
        log: &Arc<super::super::storage::PartitionLog>,
    ) -> Result<(), TcpError> {
        {
            let slot = {
                let mut slots = self.preparers.lock();
                slots
                    .iter()
                    .position(|slot| Arc::ptr_eq(&slot.log, log))
                    .map(|index| slots.swap_remove(index))
            };
            if let Some(slot) = slot {
                self.join_preparer(slot)?;
            }
        }
        let slot = {
            let mut slots = self.syncers.lock();
            slots
                .iter()
                .position(|slot| Arc::ptr_eq(&slot.log, log))
                .map(|index| slots.swap_remove(index))
        };
        if let Some(SyncerSlot { log, syncer }) = slot {
            // The log marks its retirement transition before the frontier stops.
            let result = log.retire_syncer(syncer).map_err(|error| error.to_string());
            drop(log);
            result.map_err(|error| {
                self.context.poison_readers();
                TcpError::new(format!(
                    "log syncer drain failed; restart required: {error}"
                ))
            })?;
        }
        Ok(())
    }
}

enum FlushCommand {
    /// A frontier flush ordered after every earlier command.
    Barrier(mpsc::SyncSender<()>),
    Stop,
}
pub(super) struct ControlOwner {
    shared: Arc<Shared>,
    threads: Vec<JoinHandle<()>>,
    flush_thread: Option<JoinHandle<()>>,
    // Declared last: drops after `Drop::drop` joined the workers, then answers every
    // parked poll and joins the hub thread.
    _progress: durable_progress::Hub,
}
impl ControlOwner {
    pub(super) fn start(context: Arc<ControlContext>) -> Result<Self, TcpError> {
        let progress = durable_progress::Hub::start()?;
        let (flush, receiver) = mpsc::sync_channel(FLUSH_QUEUE);
        let shared = Arc::new(Shared {
            queue: Mutex::new(Queue {
                work: VecDeque::new(),
                requests: 0,
                bytes: 0,
                stopped: false,
                flush_failed: false,
                active_appends: 0,
                control_active: false,
            }),
            ready: Condvar::new(),
            context,
            flush,
            progress: progress.sender(),
            #[cfg(test)]
            progress_cap_ms: std::sync::atomic::AtomicU64::new(
                super::DURABLE_PROGRESS_MAX_WAIT.as_millis() as u64,
            ),
            durability: Mutex::new(DurabilityState {
                config: DurabilityConfig::default(),
                started: false,
            }),
            syncers: Mutex::new(Vec::new()),
            preparers: Mutex::new(Vec::new()),
            #[cfg(test)]
            grant_profiles: Mutex::new((Vec::new(), 0)),
            #[cfg(test)]
            before_target_shutdown: Mutex::new(None),
            #[cfg(test)]
            session_barrier_waits: std::sync::atomic::AtomicU64::new(0),
        });
        let flush_shared = shared.clone();
        let flush_thread = thread::Builder::new()
            .name("value-wal-flusher".into())
            .spawn(move || {
                let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    flush_run(&flush_shared, &receiver)
                }))
                .is_err();
                if panicked {
                    flush_shared.context.poison_readers();
                    {
                        let mut queue = flush_shared.queue.lock();
                        queue.flush_failed = true;
                        queue.stopped = true;
                    }
                    fail_queued_flush(&receiver);
                }
            })?;
        let mut threads = Vec::with_capacity(CONTROL_WORKERS);
        for index in 0..CONTROL_WORKERS {
            let owner = shared.clone();
            match thread::Builder::new()
                .name(format!("value-wal-control-{index}"))
                .spawn(move || run(&owner))
            {
                Ok(thread) => threads.push(thread),
                Err(error) => {
                    shared.queue.lock().stopped = true;
                    shared.ready.notify_all();
                    for thread in threads {
                        let _ = thread.join();
                    }
                    let _ = shared.flush.send(FlushCommand::Stop);
                    let _ = flush_thread.join();
                    return Err(TcpError::from(error));
                }
            }
        }
        Ok(Self {
            shared,
            threads,
            flush_thread: Some(flush_thread),
            _progress: progress,
        })
    }

    /// Select policy before the first append or lease starts a stream frontier.
    /// A frontier and syncer freeze the policy so queued writes cannot mix two
    /// acknowledgement contracts.
    pub(super) fn set_durability(&self, config: DurabilityConfig) -> Result<(), TcpError> {
        let mut current = self.shared.durability.lock();
        if current.started {
            return if current.config == config {
                Ok(())
            } else {
                Err(TcpError::new(
                    "durability tier cannot change after a stream frontier starts",
                ))
            };
        }
        current.config = config;
        Ok(())
    }

    pub(super) fn durability(&self) -> DurabilityConfig {
        self.shared.durability.lock().config
    }

    #[cfg(test)]
    pub(super) fn note_session_barrier_wait(&self) {
        // Relaxed: test observation only, not a synchronization hand-off.
        self.shared
            .session_barrier_waits
            .fetch_add(1, Ordering::Relaxed);
    }

    #[cfg(test)]
    pub(super) fn session_barrier_waits(&self) -> u64 {
        self.shared.session_barrier_waits.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(super) fn pending_requests_for_test(&self) -> usize {
        self.shared.queue.lock().requests
    }

    #[cfg(test)]
    pub(super) fn exclusive_waits_for_stream(&self) -> bool {
        let queue = self.shared.queue.lock();
        queue.active_appends != 0
            && queue
                .work
                .front()
                .is_some_and(|work| !is_stream_request(&work.request))
    }

    /// Full admission returns ownership unchanged; the session retains its
    /// original deadline and credits and retries without blocking progress.
    // Returning the owned request avoids a heap allocation on every retry.
    #[allow(clippy::result_large_err)]
    pub(super) fn try_submit(&self, work: ControlWork) -> Result<(), ControlWork> {
        let mut queue = self.shared.queue.lock();
        if queue.stopped
            || queue.requests == MAX_COMMANDS
            || work.charge > MAX_COMMAND_BYTES.saturating_sub(queue.bytes)
        {
            return Err(work);
        }
        queue.requests += 1;
        queue.bytes += work.charge;
        queue.work.push_back(work);
        self.shared.ready.notify_one();
        Ok(())
    }
    /// Final process teardown: prevent new controls only after the session
    /// owner and all admitted work have quiesced.
    pub(super) fn finish_live_target_shutdown(
        &self,
    ) -> Result<super::super::nvmeof::LiveTargetStats, TcpError> {
        let context = &self.shared.context;
        let _gate = context.live_control_gate.lock();
        // Teardown owns the lifecycle gate from here on, so the preparer cannot
        // expose a capability after teardown starts. The inventory is taken
        // only to check and, at the end, to install the shutdown state.
        let mut lifecycle = context.lifecycle.lock();
        if !context.inventory.lock().live_pins.is_empty() {
            return Err(TcpError::new(
                "live NVMe-oF pins must drain before shutdown",
            ));
        }
        let (done, completed) = mpsc::sync_channel(0);
        {
            let mut queue = self.shared.queue.lock();
            if queue.requests != 0 || queue.stopped || queue.flush_failed {
                return Err(TcpError::new("log control owner is not quiescent"));
            }
            queue.stopped = true;
            self.shared.ready.notify_all();
            self.shared
                .flush
                .send(FlushCommand::Barrier(done))
                .map_err(|_| TcpError::new("log flusher stopped before shutdown"))?;
        }
        completed
            .recv()
            .map_err(|_| TcpError::new("log flusher stopped before shutdown"))?;
        #[cfg(test)]
        if let Some((entered, resume)) = self.shared.before_target_shutdown.lock().take() {
            let _ = entered.send(());
            let _ = resume.recv_timeout(Duration::from_secs(5));
        }
        let slots = std::mem::take(&mut *self.shared.preparers.lock());
        let mut join_error = None;
        for slot in slots {
            if let Err(error) = self.shared.join_preparer(slot) {
                join_error.get_or_insert(error);
            }
        }
        if let Some(error) = join_error {
            return Err(error);
        }
        NodeInventory::finish_live_target_shutdown(&mut context.inventory.lock(), &mut lifecycle)
    }
}

#[cfg(test)]
impl ControlOwner {
    pub(super) fn grant_profiles(&self) -> serde_json::Value {
        let profiles = self.shared.grant_profiles.lock();
        serde_json::json!({"records": profiles.0, "dropped": profiles.1})
    }

    pub(super) fn preparer_count(&self) -> usize {
        self.shared.preparers.lock().len()
    }
}

#[cfg(test)]
#[test]
fn preparation_triggers_coalesce_when_the_worker_is_busy() {
    use crate::distributed::storage::LivePausePoint;
    use crate::distributed::tcp::leased_tests::Fixture;
    let fixture = Fixture::new(DurabilityTier::Native, 16 * 1024);
    let _client = fixture.writer_without_ahead();
    let log = fixture.node.log_for_test(1);
    log.lease_live_extent(RequestAuthority { epoch: 1 })
        .unwrap();
    let (entered, release) = log.pause_live(LivePausePoint::PrepareBeforePublish);
    let shared = &fixture.node.control.shared;
    shared.trigger_file_preparation(&log);
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    for _ in 0..128 {
        shared.trigger_file_preparation(&log);
    }
    {
        let slots = shared.preparers.lock();
        assert_eq!(slots.len(), 1);
        assert!(matches!(
            slots[0].trigger.try_send(()),
            Err(mpsc::TrySendError::Full(()))
        ));
    }
    release.send(()).unwrap();
    shared.retire_syncer(&log).unwrap();
    assert!(shared.preparers.lock().is_empty());
    assert_eq!(log.live_file_preparation_stats().live_files_prepared, 1);
}
impl Drop for ControlOwner {
    fn drop(&mut self) {
        self.shared.queue.lock().stopped = true;
        self.shared.ready.notify_all();
        for thread in self.threads.drain(..) {
            if thread.join().is_err() {
                self.shared.context.poison_readers();
            }
        }
        {
            let slots = std::mem::take(&mut *self.shared.preparers.lock());
            for slot in slots {
                let _ = self.shared.join_preparer(slot);
            }
        }
        let _ = self.shared.flush.send(FlushCommand::Stop);
        if let Some(thread) = self.flush_thread.take()
            && thread.join().is_err()
        {
            self.shared.context.poison_readers();
        }
        // Shutdown drains each syncer, then catches up a deferred live root.
        // The registry lock is released before any join or file sync.
        let syncers = std::mem::take(&mut *self.shared.syncers.lock());
        for SyncerSlot { log, syncer } in syncers {
            // Retirement marks its transition before the frontier stops, so
            // its drain may adopt and cover a written frame.
            if log.retire_syncer(syncer).is_err() {
                self.shared.context.poison_readers();
            }
        }
    }
}
#[cfg(test)]
impl ControlOwner {
    /// Test-only: join every file preparer after its current step, so an
    /// in-process crash image is taken at a step boundary (journal and
    /// configfs agree) rather than across a running export.
    pub(super) fn stop_preparers_for_test(&self) {
        let slots = std::mem::take(&mut *self.shared.preparers.lock());
        for slot in slots {
            // A log poisoned by an injected cut still stops its preparer; the
            // join result only reports that poison.
            let _ = self.shared.join_preparer(slot);
        }
    }
}

/// Holds the scheduling state of one running command. Dropping it (including on
/// unwind) releases the slot and wakes every worker, so a waiting control or the
/// appends queued behind a control can start.
struct Slot<'a> {
    shared: &'a Shared,
    append: bool,
}
impl Drop for Slot<'_> {
    fn drop(&mut self) {
        let mut queue = self.shared.queue.lock();
        if self.append {
            queue.active_appends -= 1;
        } else {
            queue.control_active = false;
        }
        self.shared.ready.notify_all();
    }
}

impl StorageNode {
    /// Select the write durability tier. Call after `open` and before the first
    /// client request; it fails once an append has fixed the policy.
    pub fn set_durability(&self, config: DurabilityConfig) -> Result<(), TcpError> {
        self.control.set_durability(config)
    }

    /// The durability configuration the append workers apply now.
    pub fn durability(&self) -> DurabilityConfig {
        self.control.durability()
    }

    /// Test accessor: the owner's log of `epoch`, for frontier assertions.
    #[cfg(test)]
    pub(crate) fn log_for_test(&self, epoch: u64) -> Arc<super::super::storage::PartitionLog> {
        self.inventory.lock().active(epoch).unwrap()
    }

    /// Test hook: the log of `epoch` with its stream frontier and syncer
    /// attached, as the first lease (formerly the first append) does.
    #[cfg(test)]
    pub(crate) fn stream_log_for_test(
        &self,
        epoch: u64,
    ) -> Arc<super::super::storage::PartitionLog> {
        let log = self.log_for_test(epoch);
        self.control.shared.frontier_for(&log).unwrap();
        log
    }

    /// Test accessor: the owner's log of stream `source` while `authority`
    /// serves (the active log, or an older sealed one), to read records of
    /// either kind directly from the owner WAL.
    #[cfg(test)]
    pub(crate) fn source_log_for_test(
        &self,
        authority: u64,
        source: u64,
    ) -> Arc<super::super::storage::PartitionLog> {
        let inventory = self.inventory.lock();
        if source == authority {
            inventory.active(source).unwrap()
        } else {
            inventory.retirement_log(authority, source).unwrap()
        }
    }

    /// Test hook: cap of a durable-progress wait for requests submitted from now on.
    #[cfg(test)]
    pub(crate) fn set_durable_progress_cap_for_test(&self, cap: Duration) {
        // Relaxed: read by the next poll; tests order it before the submission.
        self.control.shared.progress_cap_ms.store(
            u64::try_from(cap.as_millis()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
    }
}

/// Requests that move data through one log's WAL. They overlap each other; the
/// log's own state lock orders them. Everything else is an exclusive control.
// Seal/fence are exclusive barriers: they wait for active stream operations,
// including an unlocked close scan. Direct storage callers use `sealing`.
// A durable-progress poll overlaps writes like a position proof: it takes no
// append ticket and no flush barrier, and leaves its slot before it parks.
pub(super) fn is_stream_request(request: &Request) -> bool {
    matches!(
        request,
        Request::CheckpointPosition { .. }
            | Request::DurableProgress { .. }
            | Request::LeaseLiveExtent { .. }
            | Request::PreleaseLiveExtent { .. }
            | Request::AdoptLiveExtent { .. }
            | Request::ResolveLiveRecords { .. }
    )
}

/// File maintenance of the serving stream and read-only file inventory. They
/// overlap stream requests like a position proof (no append ticket, no flush
/// barrier) and never take control's exclusive slot, so writes, grants and
/// prepared rollovers keep running while a seal or retirement syncs. Fence,
/// seal and shutdown still wait for them as active stream work. An older
/// sealed stream's retirement stays an exclusive lifecycle control.
pub(super) fn is_maintenance_request(request: &Request) -> bool {
    match request {
        Request::RetireWalFiles {
            authority_epoch,
            epoch,
            ..
        } => epoch == authority_epoch,
        Request::FileInventoryPage { .. } | Request::RelocationPage { .. } => true,
        _ => false,
    }
}

/// Stream requests overlap; the log orders their state changes. Exclusive
/// controls wait for all earlier stream operations, including position proofs,
/// and block later work until completion. Returns `None` once stopped and empty.
fn next_work(shared: &Shared) -> Option<(ControlWork, Slot<'_>)> {
    let mut queue = shared.queue.lock();
    loop {
        let runnable = match queue.work.front() {
            None if queue.stopped => return None,
            None => None,
            Some(_) if queue.control_active => None,
            Some(front)
                if is_stream_request(&front.request) || is_maintenance_request(&front.request) =>
            {
                Some(true)
            }
            Some(_) => (queue.active_appends == 0).then_some(false),
        };
        let Some(append) = runnable else {
            shared.ready.wait(&mut queue);
            continue;
        };
        let work = queue.work.pop_front()?;
        if append {
            queue.active_appends += 1;
        } else {
            queue.control_active = true;
        }
        return Some((work, Slot { shared, append }));
    }
}

fn run(shared: &Shared) {
    // CONTROL_WORKERS identical workers drain one FIFO. Stream requests run in
    // parallel; controls are exclusive barriers (see `next_work`).
    while let Some((work, _slot)) = next_work(shared) {
        let ControlWork {
            id,
            request,
            limits,
            measured,
            queued,
            submitted,
            charge,
            completed,
            progress,
        } = work;
        let mut profile = ConnectionProfile::default();
        let began = measured.then(Instant::now);
        if let (Some(queued), Some(began)) = (queued, began) {
            profile.control_queue_ns = queued.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
            if let Some(submitted) = submitted {
                profile.session_admission_wait_ns = submitted
                    .duration_since(queued)
                    .as_nanos()
                    .min(u128::from(u64::MAX))
                    as u64;
                profile.appender_queue_ns = began
                    .duration_since(submitted)
                    .as_nanos()
                    .min(u128::from(u64::MAX)) as u64;
            }
            profile.control_queue_samples = 1;
            profile.handler_ns = began.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
        }
        if shared.context.terminal.load(Ordering::Acquire)
            && !shared.context.permits_recovery_control(&request)
        {
            complete(
                shared,
                id,
                charge,
                completed,
                Err(TcpError::new(
                    "log control authority is poisoned; reopen required",
                )),
                profile,
                measured,
                began,
            );
            continue;
        }
        let request = match request {
            request @ (Request::LeaseLiveExtent { .. }
            | Request::PreleaseLiveExtent { .. }
            | Request::AdoptLiveExtent { .. }) => {
                let epoch = match &request {
                    Request::LeaseLiveExtent { epoch }
                    | Request::PreleaseLiveExtent { epoch, .. }
                    | Request::AdoptLiveExtent { epoch, .. } => *epoch,
                    _ => unreachable!("matched live extent request"),
                };
                let response = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    #[cfg(test)]
                    let prelog_started = Instant::now();
                    let log = shared.context.inventory.lock().active(epoch)?;
                    #[cfg(test)]
                    let prelog_inventory_ns = prelog_started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
                    #[cfg(test)]
                    let prelog_started = Instant::now();
                    let frontier = shared.frontier_for(&log)?;
                    #[cfg(test)]
                    let prelog_frontier_ns = prelog_started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
                    let adoption = matches!(&request, Request::AdoptLiveExtent { .. });
                    let closes_extent =
                        matches!(&request, Request::AdoptLiveExtent { close: true, .. });
                    // frontier_for freezes this authoritative policy before
                    // any extent can be returned to the coordinator.
                    #[cfg(test)]
                    let prelog_started = Instant::now();
                    let tier = shared.durability.lock().config.tier;
                    #[cfg(test)]
                    let prelog_tier_ns = prelog_started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
                    let reply = shared.context.handle_live_extent(request, tier)?;
                    #[cfg(test)]
                    {
                        let lease = match &reply {
                            Response::LiveLeased(leased) => Some(leased),
                            Response::LiveAdopted(adopted) => adopted.next.as_ref(),
                            _ => None,
                        };
                        if let Some(leased) = lease {
                            let stats = log.live_file_preparation_stats();
                            let mut profiles = shared.grant_profiles.lock();
                            if profiles.0.len() < 64 {
                                let file_index = leased.extent.segment.file_index;
                                let new_file = profiles.0.last().is_none_or(|last| last.file_index != file_index);
                                profiles.0.push(TestGrantProfile {
                                    envelope_id: id,
                                    file_index,
                                    new_file,
                                    profile: leased.profile,
                                    prepared: stats.live_files_prepared,
                                    published: stats.live_files_published,
                                    timing: log.live_test_lease_timing(),
                                    prelog_inventory_ns,
                                    prelog_frontier_ns,
                                    prelog_tier_ns,
                                });
                            } else {
                                profiles.1 += 1;
                            }
                        }
                    }
                    if matches!(&reply, Response::LiveLeased(_))
                        || matches!(&reply, Response::LiveAdopted(adopted) if adopted.next.is_some())
                    {
                        shared.trigger_file_preparation(&log);
                    }
                    // Neither tier requests a per-frame sync on adoption.
                    // Synchronous frames are already FUA- or flush-covered
                    // before acknowledgement; synchronous owner reads of a
                    // newer batch persist on demand. Extent close requests the
                    // fragment-boundary sync (and root install) in both tiers.
                    // The test-only baseline arm restores the pre-change
                    // per-adoption synchronous sync request for measurement.
                    let baseline = tier == DurabilityTier::Synchronous
                        && crate::distributed::storage::baseline_write_path();
                    if adoption && (closes_extent || baseline) {
                        log.note_live_frontier_request();
                        frontier.request_sync().map_err(TcpError::component)?;
                    }
                    Ok(reply)
                }))
                .unwrap_or_else(|_| {
                    shared.context.poison_readers();
                    Err(TcpError::new(
                        "live extent owner panicked; outcome unknown; reopen authority",
                    ))
                });
                complete(
                    shared, id, charge, completed, response, profile, measured, began,
                );
                continue;
            }
            Request::DurableProgress {
                authority_epoch,
                epoch,
                after,
                max_wait_ms,
                sync,
            } => {
                let poll = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    durable_progress_poll(shared, authority_epoch, epoch, after, max_wait_ms, sync)
                }))
                .unwrap_or_else(|_| {
                    shared.context.poison_readers();
                    Err(TcpError::new(
                        "durable progress poll panicked; outcome unknown; reopen authority",
                    ))
                });
                match poll {
                    Ok(DurablePoll::Park {
                        identity,
                        log,
                        wake,
                        deadline,
                    }) => {
                        // The hub bounds parked polls itself. The control admission
                        // ends here, so a parked poll never holds a control slot or
                        // delays the quiescence check of a live target shutdown.
                        release(shared, charge);
                        shared.progress.park(Box::new(durable_progress::Waiter {
                            id,
                            completed,
                            profile,
                            measured,
                            began,
                            identity,
                            log,
                            wake,
                            deadline,
                            progress,
                        }));
                    }
                    Ok(DurablePoll::Ready(report)) => complete(
                        shared,
                        id,
                        charge,
                        completed,
                        Ok(Response::DurableProgress(report)),
                        profile,
                        measured,
                        began,
                    ),
                    Err(error) => complete(
                        shared,
                        id,
                        charge,
                        completed,
                        Err(error),
                        profile,
                        measured,
                        began,
                    ),
                }
                continue;
            }
            request => request,
        };

        // Position proofs and cold record grants establish their own covering
        // data barriers. A grant must not join the flusher: that barrier may
        // self-adopt an unrelated predecessor or wait for durable metadata.
        // Lifecycle controls still drain the ordered flush queue.
        // File maintenance needs no flush: a sealed file's bytes are already
        // covered by the durable position root.
        let read_owned_barrier = match &request {
            Request::CheckpointPosition { .. } | Request::ResolveLiveRecords { .. } => true,
            request => is_maintenance_request(request),
        };
        let (barrier_ok, barrier_wait_ns) = if read_owned_barrier {
            (true, 0)
        } else {
            let (barrier_done, barrier_result) = mpsc::sync_channel(0);
            let barrier_started = measured.then(Instant::now);
            let barrier_sent = {
                let queue = shared.queue.lock();
                !queue.flush_failed
                    && shared
                        .flush
                        .send(FlushCommand::Barrier(barrier_done))
                        .is_ok()
            };
            let (barrier_ok, barrier_wait_ns) = if barrier_sent {
                let result = barrier_result.recv();
                let wait = barrier_started.map_or(0, elapsed_ns);
                if measured {
                    profile.control_queue_ns = profile.control_queue_ns.saturating_add(wait);
                }
                (result.is_ok(), wait)
            } else {
                (false, 0)
            };
            (barrier_ok, barrier_wait_ns)
        };
        if !barrier_ok {
            shared.context.poison_readers();
            {
                let mut queue = shared.queue.lock();
                queue.flush_failed = true;
                queue.stopped = true;
            }
            complete(
                shared,
                id,
                charge,
                completed,
                Err(TcpError::new("log persistence owner stopped")),
                profile,
                measured,
                began,
            );
            continue;
        }
        // Parked polls belong to the authority or stream this control may end. The
        // hub drops them (and their log references) before it runs: a live fence
        // requires the old log to have no other strong handle.
        if matches!(&request, Request::Fence { .. } | Request::Seal { .. }) {
            shared.progress.retire_parked();
        }
        let response = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            shared.context.handle_profiled_draining(
                request,
                limits,
                measured.then_some(&mut profile),
                |log| shared.retire_syncer(log),
            )
        }))
        .unwrap_or_else(|_| {
            shared.context.poison_readers();
            Err(TcpError::new(
                "log control owner panicked; outcome may be unknown; reopen authority",
            ))
        });
        if measured {
            profile.control_execution_ns =
                began.map_or(0, elapsed_ns).saturating_sub(barrier_wait_ns);
        }
        // The tier lives here, not in the inventory that builds the status.
        let response = response.map(|response| match response {
            Response::Status(StatusReply::Database(mut status)) => {
                status.durability_tier = shared.durability.lock().config.tier;
                Response::Status(StatusReply::Database(status))
            }
            response => response,
        });
        complete(
            shared, id, charge, completed, response, profile, measured, began,
        );
    }
}

/// The outcome of a validated durable-progress poll.
enum DurablePoll {
    Ready(durable_progress::DurableProgressReport),
    Park {
        identity: PollIdentity,
        log: std::sync::Weak<super::super::storage::PartitionLog>,
        wake: Wake,
        deadline: Instant,
    },
}

/// Validate a poll and answer it now when its condition holds or its wait is zero.
/// A nudge (`sync`) adopts leased frames. On a live stream it then runs one barrier
/// here that syncs everything appended and installs the strict live root over it:
/// the report covers only the root's prefix, and the bounded syncer may defer the
/// root for a whole fragment. On any other stream it asks the log syncer to cover
/// everything appended now and never syncs on this worker.
fn durable_progress_poll(
    shared: &Shared,
    authority_epoch: u64,
    epoch: u64,
    after: u64,
    max_wait_ms: u32,
    sync: bool,
) -> Result<DurablePoll, TcpError> {
    #[cfg(not(test))]
    let wait = durable_progress::capped_wait(max_wait_ms);
    #[cfg(test)]
    let wait = Duration::from_millis(u64::from(max_wait_ms)).min(Duration::from_millis(
        // Relaxed: see `progress_cap_ms`.
        shared.progress_cap_ms.load(Ordering::Relaxed),
    ));
    let deadline = Instant::now() + wait;
    let (identity, log) = {
        // A lookup only: the guard drops before any adoption or sync below.
        let inventory = shared.context.inventory.lock();
        let identity = PollIdentity {
            database: shared.context.partition.inventory.database,
            partition: shared.context.partition.partition,
            authority_epoch,
            epoch,
            incarnation: inventory.incarnation(),
        };
        (
            identity,
            inventory.durable_progress_log(authority_epoch, epoch)?,
        )
    };
    let Some(log) = log else {
        return Ok(DurablePoll::Ready(
            identity.report(LogDurableProgress::default()),
        ));
    };
    let wake = if sync {
        log.self_adopt_live_extent().map_err(TcpError::component)?;
        let live = log.persist_live_progress().map_err(TcpError::component)?;
        if live {
            // Other parked polls of this log see the barrier's publication.
            shared.progress.advanced();
        }
        let progress = log.durable_progress();
        // Lag implies an append, which starts the frontier and fixes the tier anyway;
        // an idle log must not start a frontier here.
        let target = if !live && progress.appended_end > progress.synced_end {
            shared
                .frontier_for(&log)?
                .request_sync()
                .map_err(TcpError::component)?
        } else {
            progress.synced_end
        };
        Wake::Synced(target)
    } else {
        Wake::Generation(after)
    };
    let progress = log.durable_progress();
    if wake.ready(&progress) || Instant::now() >= deadline {
        return Ok(DurablePoll::Ready(identity.report(progress)));
    }
    Ok(DurablePoll::Park {
        identity,
        log: Arc::downgrade(&log),
        wake,
        deadline,
    })
}

fn flush_run(shared: &Shared, receiver: &mpsc::Receiver<FlushCommand>) {
    while let Ok(command) = receiver.recv() {
        match command {
            FlushCommand::Barrier(done) => {
                // Every command before this one is replied; now make anything
                // acknowledged under the native tier durable too.
                if shared.flush_logs().is_err() {
                    drop(done);
                    shared.context.poison_readers();
                    {
                        let mut queue = shared.queue.lock();
                        queue.flush_failed = true;
                        queue.stopped = true;
                    }
                    fail_queued_flush(receiver);
                    return;
                }
                let _ = done.send(());
            }
            FlushCommand::Stop => return,
        }
    }
}

/// Drop every queued barrier after a persistence failure: its waiter sees the
/// disconnect and fails its control.
fn fail_queued_flush(receiver: &mpsc::Receiver<FlushCommand>) {
    while let Ok(FlushCommand::Barrier(done)) = receiver.try_recv() {
        drop(done);
    }
}

// Keep completion fields inline; a wrapper would only move this argument list.
#[allow(clippy::too_many_arguments)]
fn complete(
    shared: &Shared,
    id: u64,
    charge: usize,
    completed: mpsc::SyncSender<reactor::WorkCompletion>,
    response: Result<Response, TcpError>,
    profile: ConnectionProfile,
    measured: bool,
    began: Option<Instant>,
) {
    // Retire the admission before publishing the reply. The reply is the last
    // effect of an admitted command, so a thread that holds it (or is ordered
    // after its receiver, e.g. by a session join) must see the owner quiescent:
    // `finish_live_target_shutdown` refuses teardown while `requests != 0`, and
    // a session retrying a full `try_submit` after this reply must find the
    // slot free. The queue mutex release is sequenced before `try_send`, and the
    // channel orders it before the receiver's next queue lock. Crash
    // consistency is unchanged: every durable effect of the command (the WAL
    // write, and the covering sync for Synchronous) precedes this call; only
    // delivery of the already computed reply follows the release. Releasing
    // first admits at most one extra in-transit completion per worker and the
    // flusher beyond `MAX_COMMANDS`, so admission stays bounded. Parked
    // durable-progress polls add at most the progress hub's waiter bound.
    release(shared, charge);
    deliver(id, completed, response, profile, measured, began);
}

/// Retire one admitted command's request and byte credits.
fn release(shared: &Shared, charge: usize) {
    let mut queue = shared.queue.lock();
    queue.requests -= 1;
    queue.bytes -= charge;
}

/// Publish a command's reply to its session. The admission must already be retired.
pub(super) fn deliver(
    id: u64,
    completed: mpsc::SyncSender<reactor::WorkCompletion>,
    response: Result<Response, TcpError>,
    mut profile: ConnectionProfile,
    measured: bool,
    began: Option<Instant>,
) {
    profile.handler_ns = began.map_or(0, elapsed_ns);
    if measured {
        profile.control_execution_samples = 1;
    }
    let _ = completed.try_send(reactor::WorkCompletion {
        id,
        response,
        profile,
    });
}

impl ControlContext {
    fn status(&self) -> Result<NodeStatus, TcpError> {
        if self.poison_pending.load(Ordering::Acquire)
            && self.recoverable_live_failure.load(Ordering::Acquire)
        {
            return self.inventory.lock().recoverable_status();
        }
        self.inventory.lock().status()
    }
    pub(super) fn effective_max_reads(&self, limits: TcpLimits) -> usize {
        if self.wal_member_read_policy == super::super::storage::WalMemberReadPolicy::Async {
            limits.max_reads.min(self.max_device_reads_per_status)
        } else {
            limits.max_reads
        }
    }
    fn permits_recovery_control(&self, request: &Request) -> bool {
        // Terminal publication does not revoke idle exports. Their pool
        // must still be able to release pins after confirmed disconnects.
        matches!(request, Request::ReleaseLiveExportPin { .. })
            || (self.recoverable_live_failure.load(Ordering::Acquire)
                && matches!(request, Request::Status { .. } | Request::Fence { .. }))
    }

    fn poison_incomplete_live(&self) {
        // Release: before the flags below, so a fence that observes the recoverable
        // failure (Acquire) also observes the latch.
        self.persistence_failed.store(true, Ordering::Release);
        {
            let _gate = self.poison_gate.lock();
            if !self.poison_pending.load(Ordering::Acquire) {
                self.recoverable_live_failure.store(true, Ordering::Release);
                self.poison_pending.store(true, Ordering::Release);
            }
        }
        self.finish_poison_if_quiescent();
    }
    pub(super) fn poison_readers(&self) {
        let _gate = self.poison_gate.lock();
        self.recoverable_live_failure
            .store(false, Ordering::Release);
        // Release, before `poison_pending`: see `poison_incomplete_live`.
        self.persistence_failed.store(true, Ordering::Release);
        self.poison_pending.store(true, Ordering::Release);
        drop(_gate);
        self.finish_poison_if_quiescent();
    }
    pub(super) fn finish_poison_if_quiescent(&self) {
        if !self.poison_pending.load(Ordering::Acquire) {
            return;
        }
        // Record pins protect in-flight I/O. Idle controller pins protect
        // exports only and must not delay terminal authority publication.
        // A held live-control gate defers publication, as a held lifecycle
        // gate does below.
        let Some(_gate) = self.live_control_gate.try_lock() else {
            return;
        };
        // A lifecycle step holds the gate while it releases the inventory
        // across its syncs; its intermediate states (cleared pins, a fence
        // that resets `terminal` at its end) must not publish terminal
        // authority. Defer, as the held inventory did before: the next
        // release or completion retries. The export, release and lease paths
        // drop their gate guard before poisoning; a poison raised inside a
        // gated step (drain, history failure) defers exactly as it did when
        // that step held the inventory.
        {
            let Some(_lifecycle) = self.lifecycle.try_lock() else {
                return;
            };
            let Some(inventory) = self.inventory.try_lock() else {
                return;
            };
            if !inventory.live_pins.is_empty() {
                return;
            }
        }
        if !self.terminal.swap(true, Ordering::AcqRel) {
            self.readers.write().invalidate();
        }
    }
    /// `kind` names the request (resolve, recovery or replay) that counts an
    /// export created inline here.
    fn publish_live_leases(
        &self,
        epoch: u64,
        leases: Vec<super::super::nvmeof::live_storage::LiveRecordLease>,
        kind: super::inventory::InlineExport,
    ) -> Result<PublishedLiveLeases, TcpError> {
        let metadata: Vec<_> = leases
            .iter()
            .map(|lease| (lease.segment.clone(), lease.offset, u64::from(lease.len)))
            .collect();
        let gate = self.live_control_gate.lock();
        if !self.readers.read().active_valid(epoch, &self.terminal) {
            return Err(TcpError::new("stale live read authority"));
        }
        // Lock order: live-control gate, lifecycle gate, inventory. Each export below
        // releases the inventory across its journal syncs and configfs work.
        let mut lifecycle = self.lifecycle.lock();
        let mut inventory = self.inventory.lock();
        inventory.active(epoch)?;
        let target = lifecycle
            .as_mut()
            .ok_or_else(|| TcpError::new("live target not configured"))?;
        if inventory.live_pins.len() >= 1024 {
            return Err(TcpError::new("live pin capacity reached"));
        }
        let token = (0..16)
            .find_map(|_| {
                let candidate = rand::random::<[u8; 16]>();
                (candidate != [0; 16] && !inventory.live_pins.contains_key(&candidate))
                    .then_some(candidate)
            })
            .ok_or_else(|| TcpError::new("live pin token allocation failed"))?;
        inventory.live_pins.insert(token, (epoch, leases));
        if let Some(events) = inventory.r1_events.as_mut() {
            events.pin(
                "pin_held",
                "record",
                token,
                metadata.iter().map(|(segment, _, _)| {
                    (
                        segment.epoch,
                        segment.file_index,
                        Some((segment.device, segment.inode)),
                    )
                }),
                "resolve",
            );
        }
        let mut locations = Vec::with_capacity(metadata.len());
        for (segment, offset, len) in metadata {
            let binding = match inventory.live_segment_writable(epoch, &segment) {
                Ok(writable) => NodeInventory::publish_live_segment(
                    &mut inventory,
                    target,
                    &segment,
                    writable,
                    kind,
                ),
                Err(error) => Err(error),
            };
            match binding {
                Ok(binding) => locations.push((segment, binding, offset, len)),
                Err(error) => {
                    // Publication can fail after creating a kernel capability.
                    // Keep its in-memory epoch pin and file leases quarantined;
                    // the durable target registry is drained on restart.
                    drop(inventory);
                    drop(lifecycle);
                    drop(gate);
                    self.poison_readers();
                    return Err(TcpError::new(format!(
                        "live read export failed; outcome unknown: {error}"
                    )));
                }
            }
        }
        Ok((token, locations))
    }
    fn bounded_live_response(
        &self,
        epoch: u64,
        token: [u8; 16],
        response: Response,
        limit: usize,
    ) -> Result<Response, TcpError> {
        let size = wire_codec(limit).serialized_size(&reactor::Envelope {
            protocol: PROTOCOL,
            id: u64::MAX,
            body: &response,
        });
        if size.is_ok_and(|size| size <= limit as u64) {
            return Ok(response);
        }
        let released = {
            let mut lifecycle = self.lifecycle.lock();
            NodeInventory::release_live_pin(
                &mut self.inventory.lock(),
                lifecycle.as_mut(),
                epoch,
                token,
            )
        };
        if let Err(error) = released {
            self.poison_readers();
            return Err(error);
        }
        Err(TcpError::new(
            "live metadata response exceeds negotiated frame",
        ))
    }
    /// One bounded seal kick on the file preparer thread. A storage-detected
    /// corruption below the durable position has already poisoned the log;
    /// readers follow it. An I/O error only leaves the file unsealed until a
    /// later kick. Takes the inventory only to count, never the gate.
    fn seal_ready_files(&self, log: &Arc<super::super::storage::PartitionLog>) {
        let result = log.seal_ready_files();
        if matches!(result, Ok(0)) {
            return;
        }
        if let Err(error) = &result
            && !matches!(error, StorageError::Io(_))
        {
            self.poison_readers();
            return;
        }
        // Read the durable seals before the inventory lock (state lock only);
        // the events follow the root installs they name. A kick that ends in
        // an I/O error keeps the seals it installed before it, so they are
        // reported too.
        let seals = log.file_retirement_state().0;
        let generation = log.config().inventory.epoch;
        let mut inventory = self.inventory.lock();
        if let Ok(sealed) = result {
            let stats = &mut inventory.file_maintenance;
            stats.sealed_files = stats.sealed_files.saturating_add(sealed as u64);
        }
        if let Some(events) = inventory.r1_events.as_mut() {
            events.seals(generation, &seals);
        }
    }
    /// The preparer never waits for the lifecycle gate: fence, seal and final
    /// shutdown hold it while joining us. Retry at most 30,000 times, parking
    /// 1 ms between attempts. Once it holds the gate it may wait for the
    /// inventory, whose holders never wait for the gate or for this worker.
    fn publish_prepared_file(
        &self,
        log: &Arc<super::super::storage::PartitionLog>,
        file: u64,
        stopping: &AtomicBool,
        #[cfg(test)] waiting_for_gate: &AtomicBool,
    ) {
        let segment = match log.live_segment(file) {
            Ok(segment) => segment,
            Err(_) => {
                log.poison_writer();
                self.poison_readers();
                return;
            }
        };
        for _attempt in 0..30_000 {
            // Acquire observes retirement before trying the lifecycle gate.
            if stopping.load(Ordering::Acquire) {
                return;
            }
            let Some(mut lifecycle) = self.lifecycle.try_lock() else {
                #[cfg(test)]
                // Release lets the test observe a real failed lock attempt.
                waiting_for_gate.store(true, Ordering::Release);
                thread::park_timeout(Duration::from_millis(1));
                continue;
            };
            let Some(target) = lifecycle.as_mut() else {
                return;
            };
            let mut inventory = self.inventory.lock();
            // Acquire observes authority loss during unlocked file preparation.
            if self.poison_pending.load(Ordering::Acquire)
                || self.terminal.load(Ordering::Acquire)
                || !inventory
                    .active(segment.epoch)
                    .is_ok_and(|current| Arc::ptr_eq(&current, log))
            {
                return;
            }
            #[cfg(test)]
            let export_started = Instant::now();
            let result = NodeInventory::publish_permanent_segment(&mut inventory, target, &segment);
            #[cfg(test)]
            log.record_test_file_export(
                file,
                _attempt + 1,
                export_started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
                result.is_ok(),
            );
            drop(inventory);
            drop(lifecycle);
            if result.is_err() {
                log.poison_writer();
                self.poison_readers();
            } else if matches!(result, Ok((_, true))) {
                log.live_file_published();
            }
            return;
        }
        // Optional preparation timed out; a later grant publishes the file.
    }

    /// `kind` names the request (lease, prelease or rollover) that counts an
    /// export created inline here.
    fn lease_live_published(
        &self,
        epoch: u64,
        tier: DurabilityTier,
        after: Option<[u8; 16]>,
        kind: super::inventory::InlineExport,
    ) -> Result<LiveLeased, TcpError> {
        #[cfg(test)]
        let lookup_started = Instant::now();
        let log = self.inventory.lock().active(epoch)?;
        #[cfg(test)]
        let lookup_inventory_wait_ns =
            lookup_started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        let leased = match after {
            Some(after) => log.prelease_live_extent(RequestAuthority { epoch }, after),
            None => log.lease_live_extent(RequestAuthority { epoch }),
        };
        let (extent, mut profile) = leased.map_err(|error| {
            if matches!(error, StorageError::Unknown(_) | StorageError::Poisoned) {
                self.poison_readers();
            }
            TcpError::component(error)
        })?;
        let publish_started = Instant::now();
        // Prepared file: answer from its published binding with only the
        // inventory mutex. No reap, no target work, no lifecycle gate, so a
        // seal, retirement revoke or tombstone removal never delays it.
        let published = {
            let mut inventory = self.inventory.lock();
            // The grant is allocated: a first grant in a later file is a roll.
            if let Some(events) = inventory.r1_events.as_mut() {
                events.grant(&extent.segment);
            }
            let ready = (|| {
                // Acquire observes terminal authority published during unlocked I/O.
                if self.poison_pending.load(Ordering::Acquire)
                    || self.terminal.load(Ordering::Acquire)
                {
                    return Err(TcpError::new(
                        "live lease authority poisoned during pregrant",
                    ));
                }
                inventory.ready_permanent_binding(epoch, &log, &extent.segment)
            })();
            match ready {
                Ok(Some(binding)) => {
                    drop(inventory);
                    #[cfg(test)]
                    log.record_test_lease_publication(0, 0, lookup_inventory_wait_ns);
                    profile.publish_ns =
                        publish_started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
                    return Ok(LiveLeased {
                        extent,
                        binding,
                        tier,
                        profile,
                    });
                }
                Ok(None) => inventory.has_live_binding(epoch, extent.segment.file_index),
                Err(error) => {
                    // Same contract as the gated path: the granted extent
                    // has no usable binding, so its outcome is unknown.
                    drop(inventory);
                    log.poison_writer();
                    self.poison_readers();
                    return Err(TcpError::new(format!(
                        "live target publication failed; outcome unknown: {error}"
                    )));
                }
            }
        };
        // Missing binding: the only grant path that consults the lifecycle
        // gate. Lock order: lifecycle gate, then inventory. The reap and the
        // export release the inventory across their syncs, so appends and live
        // completions on the other workers never wait for them.
        let gate_started = Instant::now();
        let mut lifecycle = self.lifecycle.lock();
        let gate_wait_ns = gate_started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        let mut inventory = self.inventory.lock();
        inventory.record_grant_gate_wait(published, gate_wait_ns);
        #[cfg(test)]
        let inventory_wait_ns = publish_started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        #[cfg(test)]
        let publication_started = Instant::now();
        let binding = (|| {
            let current = inventory.active(epoch)?;
            // Acquire observes terminal authority published during unlocked I/O.
            if self.poison_pending.load(Ordering::Acquire) || self.terminal.load(Ordering::Acquire)
            {
                return Err(TcpError::new(
                    "live lease authority poisoned during pregrant",
                ));
            }
            if !Arc::ptr_eq(&current, &log) {
                return Err(TcpError::new("live lease authority changed"));
            }
            drop(current);
            let target = lifecycle
                .as_mut()
                .ok_or_else(|| TcpError::new("live target not configured"))?;
            NodeInventory::reap_idle_live_segments(&mut inventory, target, epoch)?;
            let (binding, created) =
                NodeInventory::publish_permanent_segment(&mut inventory, target, &extent.segment)?;
            if created {
                // Background preparation had not published this file yet.
                inventory.record_inline_export(kind);
            }
            Ok(binding)
        })();
        #[cfg(test)]
        let publication_call_ns = publication_started
            .elapsed()
            .as_nanos()
            .min(u64::MAX as u128) as u64;
        drop(inventory);
        drop(lifecycle);
        profile.publish_ns = publish_started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        #[cfg(test)]
        log.record_test_lease_publication(
            inventory_wait_ns,
            publication_call_ns,
            lookup_inventory_wait_ns,
        );
        let binding = binding.map_err(|error| {
            log.poison_writer();
            self.poison_readers();
            TcpError::new(format!(
                "live target publication failed; outcome unknown: {error}"
            ))
        })?;
        Ok(LiveLeased {
            extent,
            binding,
            tier,
            profile,
        })
    }

    fn handle_live_extent(
        &self,
        request: Request,
        tier: DurabilityTier,
    ) -> Result<Response, TcpError> {
        // Acquire observes a terminal authority published after queue admission.
        if self.poison_pending.load(Ordering::Acquire) || self.terminal.load(Ordering::Acquire) {
            return Err(TcpError::new(
                "log control authority is poisoned; reopen required",
            ));
        }
        match request {
            Request::LeaseLiveExtent { epoch } => self
                .lease_live_published(epoch, tier, None, super::inventory::InlineExport::Lease)
                .map(Response::LiveLeased),
            Request::PreleaseLiveExtent { epoch, after } => self
                .lease_live_published(
                    epoch,
                    tier,
                    Some(after),
                    super::inventory::InlineExport::Prelease,
                )
                .map(Response::LiveLeased),
            Request::AdoptLiveExtent {
                epoch,
                token,
                through,
                close,
                lease_next,
            } => {
                if lease_next && !close {
                    return Err(TcpError::new("next live extent requires close"));
                }
                let log = self.inventory.lock().active(epoch)?;
                let (adoption, profile) = log
                    .adopt_live_extent(RequestAuthority { epoch }, token, through, close)
                    .map_err(|error| {
                        if matches!(error, StorageError::IncompleteLiveFrame) {
                            self.poison_incomplete_live();
                        } else if matches!(error, StorageError::Unknown(_) | StorageError::Poisoned)
                        {
                            self.poison_readers();
                        }
                        TcpError::component(error)
                    })?;
                let next = if lease_next {
                    Some(self.lease_live_published(
                        epoch,
                        tier,
                        None,
                        super::inventory::InlineExport::Rollover,
                    )?)
                } else {
                    None
                };
                Ok(Response::LiveAdopted(LiveAdopted {
                    adoption,
                    next,
                    profile,
                }))
            }
            _ => Err(TcpError::new("not a live extent control request")),
        }
    }

    #[cfg(test)]
    pub(super) fn handle_profiled(
        &self,
        request: Request,
        limits: TcpLimits,
        profile: Option<&mut ConnectionProfile>,
    ) -> Result<Response, TcpError> {
        self.handle_profiled_draining(request, limits, profile, |_| Ok(()))
    }

    fn handle_profiled_draining(
        &self,
        request: Request,
        limits: TcpLimits,
        profile: Option<&mut ConnectionProfile>,
        _drain_log: impl FnOnce(&Arc<super::super::storage::PartitionLog>) -> Result<(), TcpError>,
    ) -> Result<Response, TcpError> {
        if let Request::ReleaseLiveEpochPin { epoch, token } = &request {
            let (result, failed) = {
                let _gate = self.live_control_gate.lock();
                let mut lifecycle = self.lifecycle.lock();
                let mut inventory = self.inventory.lock();
                let result = if inventory.live_pins.contains_key(token) {
                    NodeInventory::release_live_pin(
                        &mut inventory,
                        lifecycle.as_mut(),
                        *epoch,
                        *token,
                    )
                } else {
                    Err(TcpError::new(
                        "unknown live epoch pin; outcome may be unknown",
                    ))
                };
                (result, !inventory.healthy)
            };
            if failed {
                self.poison_readers();
            }
            result?;
            self.finish_poison_if_quiescent();
            return Ok(Response::NvmeofEpochReleased);
        }
        if let Request::ReleaseLiveExportPin { epoch, token } = &request {
            let (result, failed) = {
                let mut lifecycle = self.lifecycle.lock();
                let mut inventory = self.inventory.lock();
                let result = NodeInventory::release_live_export_pin(
                    &mut inventory,
                    lifecycle.as_mut(),
                    *epoch,
                    *token,
                );
                let failed = result.is_err() && !inventory.healthy;
                (result, failed)
            };
            if failed {
                self.poison_readers();
            }
            result?;
            self.finish_poison_if_quiescent();
            return Ok(Response::NvmeofEpochReleased);
        }
        // Admission can precede an earlier command's panic or uncertain I/O.
        // Queued work must recheck terminal health before decoding or mutation.
        if (self.poison_pending.load(Ordering::Acquire) || self.terminal.load(Ordering::Acquire))
            && !self.permits_recovery_control(&request)
        {
            return Err(TcpError::new(
                "log control authority is poisoned; reopen required",
            ));
        }
        match request {
            Request::OpenLiveExports { authority_epoch } => {
                let mut lifecycle = self.lifecycle.lock();
                let mut inventory = self.inventory.lock();
                let log = inventory.active(authority_epoch)?;
                let target = lifecycle
                    .as_mut()
                    .ok_or_else(|| TcpError::new("live target not configured"))?;
                let (address, service, transport) = target.endpoint();
                let count = log.live_file_count().map_err(TcpError::component)?;
                for file in 0..count {
                    let segment = match log.live_segment(file) {
                        Ok(segment) => segment,
                        Err(StorageError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
                            continue;
                        }
                        Err(error) => return Err(TcpError::component(error)),
                    };
                    if let Err(error) =
                        NodeInventory::publish_permanent_segment(&mut inventory, target, &segment)
                    {
                        drop(inventory);
                        drop(lifecycle);
                        log.poison_writer();
                        self.poison_readers();
                        return Err(TcpError::new(format!(
                            "live target publication failed; outcome unknown: {error}"
                        )));
                    }
                }
                Ok(Response::LiveExports(LiveExportInfo {
                    address,
                    service,
                    transport,
                    wal_file_bytes: log.config().wal_file_bytes,
                    block_size: 4096,
                    exported_files: count,
                }))
            }
            Request::LeaseLiveExtent { .. }
            | Request::PreleaseLiveExtent { .. }
            | Request::AdoptLiveExtent { .. } => Err(TcpError::new(
                "live extent requires the stream durability dispatch",
            )),
            Request::ResolveLiveRecords {
                authority_epoch,
                members,
            } => {
                if members
                    .iter()
                    .any(|member| member.segment_generation == authority_epoch)
                    && self.inventory.lock().live_target_configured
                {
                    return Err(TcpError::new(
                        "live record resolution is disabled for the active epoch in permanent export mode",
                    ));
                }
                if members.is_empty() || members.len() > self.effective_max_reads(limits).min(256) {
                    return Err(TcpError::new("live record count exceeds negotiated bound"));
                }
                let snapshot =
                    self.readers
                        .read()
                        .snapshot(authority_epoch, &members, &self.terminal)?;
                let mut plans = Vec::with_capacity(snapshot.members.len());
                for (log, member) in &snapshot.members {
                    // The storage error's kind is the wire kind: flattening a
                    // retired member's `NotFound` would disable the retry.
                    plans.push(log.resolve_live_member(*member).map_err(|error| {
                        TcpError::with_kind(error.wire_kind(), error.to_string())
                    })?);
                }
                let (token, locations) = self.publish_live_leases(
                    authority_epoch,
                    plans,
                    super::inventory::InlineExport::Resolve,
                )?;
                let entries = members
                    .into_iter()
                    .zip(locations)
                    .map(
                        |(member, (segment, binding, offset, len))| LiveRecordLocation {
                            member,
                            segment,
                            binding,
                            offset,
                            len,
                        },
                    )
                    .collect();
                self.bounded_live_response(
                    authority_epoch,
                    token,
                    Response::LiveRecords(LiveRecordLocations {
                        entries,
                        epoch_pin_token: token,
                    }),
                    limits.max_frame_bytes,
                )
            }
            Request::ReplayLiveWal {
                authority_epoch,
                source_epoch,
                from,
                limit,
                require_end,
            } => {
                let log = self
                    .inventory
                    .lock()
                    .live_recovery_log(authority_epoch, source_epoch)?;
                if limit == 0 || limit > limits.recovery_page_batches || limit > 256 {
                    return Err(TcpError::new("live recovery page count exceeds bound"));
                }
                let (receipts, plans, next) = if let Some(log) = log {
                    let page = log
                        .replay_page(
                            from,
                            limit,
                            limits.max_frame_bytes.saturating_sub(32),
                            require_end,
                        )
                        .map_err(TcpError::component)?;
                    let mut receipts = Vec::new();
                    let mut plans = Vec::new();
                    for decoded in page.groups {
                        for receipt in decoded.receipts {
                            plans.push(
                                log.resolve_live_batch(receipt)
                                    .map_err(TcpError::component)?,
                            );
                            receipts.push(Receipt {
                                id: receipt.id(),
                                version: receipt.version(),
                                digest: receipt.digest(),
                                address: receipt.address(),
                            });
                        }
                    }
                    let next = page
                        .next
                        .map(usize::try_from)
                        .transpose()
                        .map_err(|_| TcpError::new("replay offset exceeds address space"))?;
                    (receipts, plans, next)
                } else {
                    if from != 0 {
                        return Err(TcpError::new("invalid abandoned epoch cursor"));
                    }
                    (Vec::new(), Vec::new(), None)
                };
                let (token, locations) = self.publish_live_leases(
                    authority_epoch,
                    plans,
                    super::inventory::InlineExport::Replay,
                )?;
                let entries = receipts
                    .into_iter()
                    .zip(locations)
                    .map(|(receipt, (segment, binding, offset, len))| {
                        live_wire::LiveRecoveryEntryWire {
                            receipt,
                            segment,
                            binding,
                            offset,
                            len,
                        }
                    })
                    .collect();
                self.bounded_live_response(
                    authority_epoch,
                    token,
                    Response::LiveRecovery(live_wire::LiveRecoveryWire {
                        entries,
                        next,
                        epoch_pin_token: token,
                    }),
                    limits.max_frame_bytes,
                )
            }
            Request::AcquireLiveExportPin {
                epoch,
                segment,
                binding,
            } => {
                // Bindings and pins change only under the lifecycle gate, so
                // a pin never attaches to a binding a revocation has claimed.
                let _lifecycle = self.lifecycle.lock();
                self.inventory
                    .lock()
                    .acquire_live_export_pin(epoch, &segment, &binding)
                    .map(Response::NvmeofEpochPinned)
            }
            Request::LiveTargetStats => {
                // Stats verify configfs objects: file I/O, gate only.
                let mut lifecycle = self.lifecycle.lock();
                let target = lifecycle
                    .as_mut()
                    .ok_or_else(|| TcpError::new("live target not configured"))?;
                Ok(Response::LiveTargetStats(
                    target.stats().map_err(TcpError::component)?,
                ))
            }
            Request::ShutdownLiveTarget { epoch } => {
                let _gate = self.live_control_gate.lock();
                let mut lifecycle = self.lifecycle.lock();
                let target = lifecycle
                    .as_mut()
                    .ok_or_else(|| TcpError::new("live target not configured"))?;
                NodeInventory::shutdown_live_target(&mut self.inventory.lock(), target, epoch)
                    .map(Response::LiveTargetStats)
            }
            Request::ReleaseLiveEpochPin { .. } | Request::ReleaseLiveExportPin { .. } => {
                unreachable!("release handled before health check")
            }
            Request::CheckpointPosition {
                authority_epoch,
                epoch,
                through,
            } => {
                let mut measured = profile;
                // The active stream slot excludes fence/seal/retirement. Retain
                // the validated log, not inventory, across the slow file barrier.
                let log = self
                    .inventory
                    .lock()
                    .checkpoint_log(authority_epoch, epoch)?;
                let durable_end = match log.as_ref() {
                    Some(log) => log
                        .persist_wal_through_profiled(
                            through,
                            measured
                                .as_deref_mut()
                                .map(|profile| &mut profile.storage_write),
                        )
                        .map_err(TcpError::component)?,
                    None => 0,
                };
                // Both schedulers count this as active stream work. Check that
                // invariant before replying instead of accepting a stale handle.
                match (
                    log.as_ref(),
                    self.inventory
                        .lock()
                        .checkpoint_log(authority_epoch, epoch)?,
                ) {
                    (Some(before), Some(after)) if Arc::ptr_eq(before, &after) => {}
                    (None, None) => {}
                    _ => {
                        return Err(TcpError::new(
                            "position proof authority changed during sync",
                        ));
                    }
                }
                if let Some(profile) = measured {
                    profile.append_persist_ns = profile.storage_write.persist_total_ns;
                }
                Ok(Response::CheckpointPosition {
                    database: self.partition.inventory.database,
                    partition: self.partition.partition,
                    epoch,
                    durable_end,
                })
            }
            Request::FileInventoryPage {
                authority_generation,
                source_generation,
                after,
                limit,
            } => NodeInventory::file_inventory_page(
                &self.inventory,
                authority_generation,
                source_generation,
                after,
                limit,
            )
            .map(|reply| Response::FileInventoryPage(Box::new(reply))),
            Request::RelocationPage {
                authority_generation,
                source_generation,
                from,
                end,
                limit,
            } => NodeInventory::relocation_page(
                &self.inventory,
                authority_generation,
                source_generation,
                from,
                end,
                limit,
                limits,
            )
            .map(|page| Response::RelocationPage(Box::new(page))),
            Request::RetirementFiles {
                authority_epoch,
                epoch,
                after,
                limit,
            } => self
                .inventory
                .lock()
                .retirement_files(authority_epoch, epoch, after, limit)
                .map(Response::RetirementFiles),
            Request::RetireWalFiles {
                authority_epoch,
                epoch,
                files,
                witness,
            } => {
                if epoch > authority_epoch {
                    return Err(TcpError::new("retirement of a future stream"));
                }
                if epoch == authority_epoch {
                    // The scheduler counts serving retirement as active stream
                    // work. Fence, Seal and target shutdown wait for that work
                    // to finish. Do not block live read publication here: the
                    // coordinator has drained the victims, whose pins are
                    // rechecked in retire_active_files.
                    let witness = witness.ok_or_else(|| {
                        TcpError::new("serving stream retirement requires an Exact witness")
                    })?;
                    return NodeInventory::retire_active_files(
                        &self.inventory,
                        &self.lifecycle,
                        authority_epoch,
                        &files,
                        &witness,
                    )
                    .map(Response::Retired);
                }
                if witness.is_some() {
                    return Err(TcpError::new(
                        "older sealed stream retirement takes no Exact witness",
                    ));
                }
                let _gate = self.live_control_gate.lock();
                let mut lifecycle = self.lifecycle.lock();
                NodeInventory::retire_wal_files(
                    &mut self.inventory.lock(),
                    &mut lifecycle,
                    authority_epoch,
                    epoch,
                    &files,
                )
                .map(Response::Retired)
            }
            Request::Status { protocol, database } => {
                if protocol != PROTOCOL {
                    return Err(TcpError::new("unsupported wire protocol"));
                }
                // A database session is bound to exactly this database.
                if database != Some(self.partition.inventory.database) {
                    return Err(TcpError::with_kind(
                        std::io::ErrorKind::InvalidInput,
                        "status scope differs from the bound database",
                    ));
                }
                let mut status = self.status()?;
                status.max_frame_bytes = limits.max_frame_bytes;
                status.max_reads = self.effective_max_reads(limits);
                status.recovery_page_batches = limits.recovery_page_batches;
                status.polling = limits.polling;
                Ok(Response::Status(StatusReply::Database(status)))
            }
            Request::Fence { epoch } => {
                #[cfg(test)]
                {
                    let before_fence = self.before_fence.lock().take();
                    if let Some(before_fence) = before_fence {
                        before_fence();
                    }
                }
                let _gate = self.live_control_gate.lock();
                // Lock order: live-control gate, lifecycle gate, inventory. The
                // fence releases the inventory across every revocation and sync.
                let mut lifecycle = self.lifecycle.lock();
                if self.closed.load(Ordering::Acquire) {
                    return Err(TcpError::new("database closed for drop"));
                }
                let mut status = if self.recoverable_live_failure.load(Ordering::Acquire) {
                    let status = NodeInventory::fence_recoverable_live(
                        &mut self.inventory.lock(),
                        &mut lifecycle,
                        epoch,
                        _drain_log,
                    )?;
                    drop(lifecycle);
                    let _gate = self.poison_gate.lock();
                    if !self.recoverable_live_failure.swap(false, Ordering::AcqRel) {
                        return Err(TcpError::new(
                            "owner failed during live fence; restart required",
                        ));
                    }
                    self.poison_pending.store(false, Ordering::Release);
                    status
                } else {
                    NodeInventory::fence(
                        &mut self.inventory.lock(),
                        &mut lifecycle,
                        epoch,
                        false,
                        _drain_log,
                    )?
                };
                status.max_frame_bytes = limits.max_frame_bytes;
                status.max_reads = self.effective_max_reads(limits);
                status.recovery_page_batches = limits.recovery_page_batches;
                status.polling = limits.polling;
                Ok(Response::Status(StatusReply::Database(status)))
            }
            // The long-poll needs the control owner's progress hub.
            Request::DurableProgress { .. } => Err(TcpError::new(
                "durable progress poll bypassed the log control owner",
            )),
            Request::Seal { epoch } => {
                let _gate = self.live_control_gate.lock();
                let mut lifecycle = self.lifecycle.lock();
                NodeInventory::seal_epoch(
                    &mut self.inventory.lock(),
                    &mut lifecycle,
                    epoch,
                    _drain_log,
                )?;
                Ok(Response::Sealed)
            }
            Request::StatsDetailed => {
                let stats = self.inventory.lock().stats()?;
                Ok(Response::StatsDetailed {
                    groups: stats.appended_groups,
                    barriers: stats.persistence_barriers,
                    namespace_syncs: stats.namespace_syncs,
                    inventory_witnesses: stats.inventory_witnesses,
                    live_frontier_file_syncs: stats.live_frontier_file_syncs,
                    live_frontier_namespace_syncs: stats.live_frontier_namespace_syncs,
                    records: stats.registered_records,
                    metadata: stats.reference_metadata_bytes,
                })
            }
            Request::PartitionStats => {
                Ok(Response::PartitionStats(Box::new(PartitionStatsReply {
                    partition: self.inventory.lock().stats()?,
                    transport: self.transport.snapshot(),
                })))
            }
            Request::Stats => {
                let stats = self.inventory.lock().stats()?;
                Ok(Response::Stats {
                    groups: stats.appended_groups,
                    barriers: stats.persistence_barriers,
                    inventory_witnesses: stats.inventory_witnesses,
                    records: stats.registered_records,
                    metadata: stats.reference_metadata_bytes,
                })
            }
            Request::ConnectionProfile { .. }
            | Request::OwnerTimelineArm { .. }
            | Request::OwnerTimelineFetch { .. } => Err(TcpError::new(
                "connection diagnostics require an authenticated session",
            )),
            #[cfg(not(all(target_os = "linux", feature = "test-utils")))]
            Request::ReadEvidence { .. } | Request::ProcessResources { .. } => {
                Err(TcpError::new(super::EVIDENCE_UNAVAILABLE))
            }
            #[cfg(all(target_os = "linux", feature = "test-utils"))]
            Request::ReadEvidence { epoch, evict } => {
                let inventory = self.inventory.lock();
                inventory.active(epoch)?;
                inventory
                    .read_evidence_for_benchmark(epoch, evict)
                    .map(Response::ReadEvidence)
            }
            #[cfg(all(target_os = "linux", feature = "test-utils"))]
            Request::ProcessResources { epoch } => self
                .inventory
                .lock()
                .process_resources_for_benchmark(epoch)
                .map(Response::ProcessResources),
            Request::InventoryPage(page) => self
                .inventory
                .lock()
                .inventory_page(&page, limits.max_frame_bytes)
                .map(Response::InventoryPage),
            Request::CheckTrimGeneration(check) => {
                let _gate = self.live_control_gate.lock();
                let _lifecycle = self.lifecycle.lock();
                let (result, _) = self
                    .inventory
                    .lock()
                    .trim_readiness(check.authority_generation, check.generation)?;
                Ok(Response::CheckTrimGeneration(
                    super::generation_control::CheckTrimReply {
                        generation: check.generation,
                        result,
                    },
                ))
            }
            Request::TrimGeneration(trim) => {
                let _gate = self.live_control_gate.lock();
                let mut lifecycle = self.lifecycle.lock();
                NodeInventory::trim_generation(
                    &mut self.inventory.lock(),
                    &mut lifecycle,
                    trim.authority_generation,
                    trim.generation,
                )?;
                Ok(Response::TrimGeneration(
                    super::generation_control::TrimReply {
                        generation: trim.generation,
                    },
                ))
            }
            Request::ProvisionDatabase { .. } | Request::DropDatabase { .. } => {
                Err(TcpError::with_kind(
                    std::io::ErrorKind::InvalidInput,
                    "provision and drop require a host-scope session",
                ))
            }
        }
    }
}
