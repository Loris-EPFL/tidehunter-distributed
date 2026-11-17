//! One bounded FIFO owner for this node's physical append stream and controls.
//!
//! Like `GroupCommitWorker`, accepted work owns its payload through completion,
//! drains on shutdown and never runs synchronous WAL I/O on network progress.
//! TiKV WriteBarrier::enter services an empty queue immediately, then takes only
//! callers already behind active work; no timer is needed here. Each Append RPC
//! remains a complete recovery group, while multiple groups may append during an
//! earlier captured prefix's file/namespace barrier. The serial flusher completes
//! receipts only after durability.
//! Source: tikv/raft-engine 097c499a19fbb38754c73aa2f31532329df7c0c6,
//! src/write_barrier.rs:170-225 and src/engine.rs:147-235.

use super::super::durability::frontier::{Frontier, Syncer, flush_all};
use super::super::durability::{DurabilityConfig, DurabilityTier};
use super::*;
use parking_lot::Condvar;
use std::collections::VecDeque;
use std::sync::mpsc;
use std::thread::JoinHandle;

#[cfg(test)]
#[path = "control_tests.rs"]
mod tests;

const MAX_COMMANDS: usize = 64;
const MAX_COMMAND_BYTES: usize = 256 * 1024 * 1024;
// See `Shared::flush` for the capacity bound.
const FLUSH_QUEUE: usize = 2 * MAX_COMMANDS + 16;
// Fixed worker set. Appends overlap up to this many at a time; the partition's
// state lock orders the WAL allocation, not this pool.
const CONTROL_WORKERS: usize = 4;
// Admission retries after a persistence drain. Another worker's appended group
// may still be on its way to the flush queue when a drain runs, so one drain does
// not always free capacity; each retry's drain then covers it.
const ADMISSION_RETRIES: usize = 8;

#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
type PublishedLiveLeases = (
    [u8; 16],
    Vec<(
        super::super::nvmeof::live_storage::LiveWalSegment,
        super::super::nvmeof::live_target::LiveTargetBinding,
        u64,
        u64,
    )>,
);

pub(super) struct ControlContext {
    pub(super) partition: PartitionConfig,
    pub(super) inventory: Arc<Mutex<NodeInventory>>,
    pub(super) readers: Arc<parking_lot::RwLock<inventory::ReaderAuthority>>,
    pub(super) terminal: Arc<AtomicBool>,
    pub(super) poison_pending: Arc<AtomicBool>,
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    pub(super) recoverable_live_failure: AtomicBool,
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    pub(super) poison_gate: Mutex<()>,
    pub(super) append_shape: KeyShape,
    pub(super) append_batch_limits: BatchLimits,
    pub(super) append_group_limits: GroupLimits,
    pub(super) wal_member_read: super::super::storage::WalMemberReadPolicy,
    pub(super) read_config: read_executor::ReadExecutorConfig,
    pub(super) read_admission: Arc<read_executor::ReadAdmission>,
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    pub(super) nvmeof_leases: Arc<Mutex<super::super::nvmeof::lease::ExportLeaseRegistry>>,
    #[cfg(test)]
    pub(super) panic_after_append: Arc<AtomicBool>,
}

pub(super) struct ControlWork {
    pub(super) id: u64,
    pub(super) request: Request,
    pub(super) limits: TcpLimits,
    pub(super) measured: bool,
    pub(super) trace_shape: Option<(u64, usize, usize)>,
    pub(super) queued: Option<Instant>,
    pub(super) submitted: Option<Instant>,
    pub(super) charge: usize,
    pub(super) completed: mpsc::SyncSender<reactor::WorkCompletion>,
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
    // Next append ticket to hand out, in queue (submission) order.
    next_ticket: u64,
    // An exclusive control is executing; nothing else may start.
    control_active: bool,
}
struct Shared {
    queue: Mutex<Queue>,
    ready: Condvar,
    context: Arc<ControlContext>,
    // Owner: the flusher thread (sole receiver). Capacity: every queued request
    // owns at most one `Append` item (<= MAX_COMMANDS), plus at most one
    // `Barrier` per append worker and per exclusive control; the capacity below
    // exceeds that sum, so a send made under the queue lock never blocks.
    // Full-queue policy: unreachable by this bound.
    // Shutdown: `Stop` after the workers joined; queued items are served first.
    flush: mpsc::SyncSender<FlushCommand>,
    // Tier policy. Synchronous: an append waits for the covering sync before
    // its reply. Native: the reply follows the WAL write and the node's syncer
    // makes it durable in the background; exclusive controls flush first.
    synchronous: AtomicBool,
    // Lock order: `durability` before `syncers`. Both are cold on the append
    // path (first append of each log, configuration and live fencing). A live
    // extent request (lease or adoption) reads the tier under `durability`
    // once per request; no lock here is taken per put.
    durability: Mutex<DurabilityState>,
    // One syncer thread per log that received appends. Live fencing retires the
    // old log's worker before recovery; shutdown joins any remaining workers.
    syncers: Mutex<Vec<SyncerSlot>>,
    // Leaf lock: never hold with `durability` or `syncers`; join off-lock.
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    preparers: Mutex<Vec<PreparerSlot>>,
    // Next append ticket allowed to reach the WAL. Never held with `queue`.
    // Waiting is bounded by the tickets ahead, which always progress.
    order: Mutex<u64>,
    order_cv: Condvar,
    #[cfg(test)]
    before_ack: Mutex<Option<(mpsc::SyncSender<()>, mpsc::Receiver<()>)>>,
    #[cfg(test)]
    session_barrier_waits: std::sync::atomic::AtomicU64,
    #[cfg(all(test, feature = "experimental-nvmeof", target_os = "linux"))]
    grant_profiles: Mutex<(Vec<TestGrantProfile>, u64)>,
    #[cfg(all(test, feature = "experimental-nvmeof", target_os = "linux"))]
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

#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
struct PreparerSlot {
    log: Arc<super::super::storage::PartitionLog>,
    trigger: mpsc::SyncSender<()>,
    stopping: Arc<AtomicBool>,
    #[cfg(test)]
    waiting_for_inventory: Arc<AtomicBool>,
    worker: JoinHandle<()>,
}

#[cfg(all(test, feature = "experimental-nvmeof", target_os = "linux"))]
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
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
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
            let waiting_for_inventory = Arc::new(AtomicBool::new(false));
            #[cfg(test)]
            let worker_waiting = waiting_for_inventory.clone();
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
                waiting_for_inventory,
                worker,
            });
            &slots[slots.len() - 1]
        };
        let _ = slot.trigger.try_send(());
    }

    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
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
        let syncer = Syncer::spawn("value-wal-syncer", frontier.clone(), move |_end| {
            worker.sync_for_frontier()
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
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
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
            .map_err(|error| TcpError::new(format!("log frontier flush failed: {error}")))
    }

    /// Called only by an exclusive, validated live fence after target revocation.
    /// Release the registry lock before joining: the worker owns the WAL I/O.
    fn retire_syncer(
        &self,
        log: &Arc<super::super::storage::PartitionLog>,
    ) -> Result<(), TcpError> {
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
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
            let result = syncer.shutdown();
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

/// What the owner replies once the covering barrier has completed.
enum FlushReply {
    Appended,
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    LiveCompleted {
        validation_ns: u64,
        validation_samples: u64,
    },
}

struct PendingFlush {
    id: u64,
    charge: usize,
    completed: mpsc::SyncSender<reactor::WorkCompletion>,
    log: Arc<super::super::storage::PartitionLog>,
    receipts: Vec<AppendedBatch>,
    reply: FlushReply,
    profile: ConnectionProfile,
    measured: bool,
    began: Option<Instant>,
    enqueued: Option<Instant>,
    // Appended frontier captured at hand-off; covers this command's groups.
    end: u64,
}

// Keep append commands inline: the fixed-capacity channel bounds resident work,
// and boxing would add a per-append allocation.
#[allow(clippy::large_enum_variant)]
enum FlushCommand {
    /// Synchronous tier: reply after the barrier covering `receipts`.
    Append(PendingFlush),
    /// A frontier flush ordered after every earlier command.
    Barrier(mpsc::SyncSender<()>),
    Stop,
}
pub(super) struct ControlOwner {
    shared: Arc<Shared>,
    threads: Vec<JoinHandle<()>>,
    flush_thread: Option<JoinHandle<()>>,
}
impl ControlOwner {
    pub(super) fn start(context: Arc<ControlContext>) -> Result<Self, TcpError> {
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
                next_ticket: 0,
            }),
            ready: Condvar::new(),
            context,
            flush,
            synchronous: AtomicBool::new(
                DurabilityConfig::default().tier == DurabilityTier::Synchronous,
            ),
            durability: Mutex::new(DurabilityState {
                config: DurabilityConfig::default(),
                started: false,
            }),
            syncers: Mutex::new(Vec::new()),
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            preparers: Mutex::new(Vec::new()),
            #[cfg(all(test, feature = "experimental-nvmeof", target_os = "linux"))]
            grant_profiles: Mutex::new((Vec::new(), 0)),
            #[cfg(all(test, feature = "experimental-nvmeof", target_os = "linux"))]
            before_target_shutdown: Mutex::new(None),
            order: Mutex::new(0),
            order_cv: Condvar::new(),
            #[cfg(test)]
            before_ack: Mutex::new(None),
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
                    fail_queued_flush(
                        &flush_shared,
                        &receiver,
                        "log persistence owner panicked; append outcome may be unknown",
                    );
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
        // Release pairs with the Acquire load in `hand_off`.
        self.shared.synchronous.store(
            config.tier == DurabilityTier::Synchronous,
            Ordering::Release,
        );
        Ok(())
    }

    pub(super) fn durability(&self) -> DurabilityConfig {
        self.shared.durability.lock().config
    }

    /// Test hook: hold the next appended group just before its acknowledgement.
    #[cfg(test)]
    pub(super) fn pause_before_next_ack(&self) -> (mpsc::Receiver<()>, mpsc::SyncSender<()>) {
        let (entered, entered_rx) = mpsc::sync_channel(1);
        let (resume_tx, resume) = mpsc::sync_channel(0);
        *self.shared.before_ack.lock() = Some((entered, resume));
        (entered_rx, resume_tx)
    }

    #[cfg(test)]
    pub(super) fn note_session_barrier_wait(&self) {
        // Relaxed: test observation only, not a synchronization hand-off.
        self.shared
            .session_barrier_waits
            .fetch_add(1, Ordering::Relaxed);
    }

    #[cfg(all(test, feature = "experimental-nvmeof", target_os = "linux"))]
    pub(super) fn session_barrier_waits(&self) -> u64 {
        self.shared.session_barrier_waits.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(super) fn pending_requests_for_test(&self) -> usize {
        self.shared.queue.lock().requests
    }

    #[cfg(all(test, feature = "experimental-nvmeof", target_os = "linux"))]
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
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    pub(super) fn finish_live_target_shutdown(
        &self,
    ) -> Result<super::super::nvmeof::LiveTargetStats, TcpError> {
        let context = &self.shared.context;
        let leases = context.nvmeof_leases.lock();
        if leases.has_live_or_quarantined() {
            return Err(TcpError::new("NVMe-oF leases must drain before shutdown"));
        }
        let mut inventory = context.inventory.lock();
        if !inventory.live_pins.is_empty() {
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
        inventory.finish_live_target_shutdown()
    }
}

#[cfg(all(test, feature = "experimental-nvmeof", target_os = "linux"))]
impl ControlOwner {
    pub(super) fn grant_profiles(&self) -> serde_json::Value {
        let profiles = self.shared.grant_profiles.lock();
        serde_json::json!({"records": profiles.0, "dropped": profiles.1})
    }

    pub(super) fn preparer_count(&self) -> usize {
        self.shared.preparers.lock().len()
    }
}

#[cfg(all(test, feature = "experimental-nvmeof", target_os = "linux"))]
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
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
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
    }
}

/// Holds the scheduling state of one running command. Dropping it (including on
/// unwind) releases the slot and wakes every worker, so a waiting control or the
/// appends queued behind a control can start.
struct Slot<'a> {
    shared: &'a Shared,
    append: bool,
    // Append ticket (queue order), passed on when the slot drops.
    ticket: Option<u64>,
}
impl Drop for Slot<'_> {
    fn drop(&mut self) {
        // Pass the append turn first, outside the queue lock, so later tickets
        // proceed even when this append failed before reaching its turn.
        if let Some(ticket) = self.ticket {
            pass_turn(self.shared, ticket);
        }
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
}

/// Requests that move data through one log's WAL. They overlap each other; the
/// log's own state lock orders them. Everything else is an exclusive control.
// Seal/fence are exclusive barriers: they wait for active stream operations,
// including an unlocked close scan. Direct storage callers use `sealing`.
pub(super) fn is_stream_request(request: &Request) -> bool {
    match request {
        Request::Append { .. } | Request::CheckpointPosition { .. } => true,
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        Request::ReserveLiveGroup { .. }
        | Request::CompleteLiveGroup { .. }
        | Request::LeaseLiveExtent { .. }
        | Request::PreleaseLiveExtent { .. }
        | Request::AdoptLiveExtent { .. }
        | Request::ResolveLiveRecords { .. } => true,
        _ => false,
    }
}

/// Stream requests overlap; the log orders their state changes. Exclusive
/// controls wait for all earlier stream operations, including position proofs,
/// and block later work until completion. Returns `None` once stopped and empty.
fn next_work(shared: &Shared) -> Option<(ControlWork, Slot<'_>, u64)> {
    let mut queue = shared.queue.lock();
    loop {
        let runnable = match queue.work.front() {
            None if queue.stopped => return None,
            None => None,
            Some(_) if queue.control_active => None,
            Some(front) if is_stream_request(&front.request) => Some(true),
            Some(_) => (queue.active_appends == 0).then_some(false),
        };
        let Some(append) = runnable else {
            shared.ready.wait(&mut queue);
            continue;
        };
        let work = queue.work.pop_front()?;
        let ticket = (append && matches!(work.request, Request::Append { .. })).then(|| {
            let ticket = queue.next_ticket;
            queue.next_ticket += 1;
            ticket
        });
        if append {
            queue.active_appends += 1;
        } else {
            queue.control_active = true;
        }
        return Some((
            work,
            Slot {
                shared,
                append,
                ticket,
            },
            ticket.unwrap_or(0),
        ));
    }
}

/// Block until every append dequeued before this one has passed its turn.
fn wait_turn(shared: &Shared, ticket: u64) {
    let mut turn = shared.order.lock();
    while *turn != ticket {
        shared.order_cv.wait(&mut turn);
    }
}

/// Pass the turn to the next ticket once this one's turn has come.
fn pass_turn(shared: &Shared, ticket: u64) {
    let mut turn = shared.order.lock();
    while *turn != ticket {
        shared.order_cv.wait(&mut turn);
    }
    *turn += 1;
    shared.order_cv.notify_all();
}

fn run(shared: &Shared) {
    // CONTROL_WORKERS identical workers drain one FIFO. Appends run in parallel;
    // controls are exclusive barriers (see `next_work`). Each append hands its
    // complete group to the bounded flush queue, where the syncer batches the
    // WAL barrier for every ready group.
    while let Some((work, _slot, ticket)) = next_work(shared) {
        let ControlWork {
            id,
            request,
            limits,
            measured,
            trace_shape: _,
            queued,
            submitted,
            charge,
            completed,
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
            Request::Append { epoch, batches } => {
                let mut drain_wait_ns = 0;
                let mut drain = || {
                    let (done, completed) = mpsc::sync_channel(0);
                    let queued = Instant::now();
                    {
                        let queue = shared.queue.lock();
                        if queue.flush_failed {
                            return Err(TcpError::new(
                                "log flusher stopped before admission retry",
                            ));
                        }
                        shared
                            .flush
                            .send(FlushCommand::Barrier(done))
                            .map_err(|_| {
                                TcpError::new("log flusher stopped before admission retry")
                            })?;
                    }
                    let result = completed
                        .recv()
                        .map_err(|_| TcpError::new("log flusher stopped before admission retry"));
                    drain_wait_ns += elapsed_ns(queued);
                    result
                };
                // Decode and group construction run in parallel; only the WAL
                // append waits for its ticket (queue order). The ticket passes on
                // when the worker's slot drops, whatever the outcome.
                let mut take_turn = || wait_turn(shared, ticket);
                let append = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    shared.context.append_profiled(
                        epoch,
                        batches,
                        measured.then_some(&mut profile),
                        Some(&mut drain),
                        Some(&mut take_turn),
                    )
                }))
                .unwrap_or_else(|_| {
                    shared.context.poison_readers();
                    Err(TcpError::new(
                        "log control owner panicked after append; outcome may be unknown; reopen authority",
                    ))
                });
                if measured {
                    profile.control_queue_ns =
                        profile.control_queue_ns.saturating_add(drain_wait_ns);
                    profile.append_barrier_wait_ns = drain_wait_ns;
                    profile.control_execution_ns =
                        began.map_or(0, elapsed_ns).saturating_sub(drain_wait_ns);
                }
                match append {
                    Ok((log, receipts)) => hand_off(
                        shared,
                        PendingFlush {
                            id,
                            charge,
                            completed,
                            log,
                            receipts,
                            reply: FlushReply::Appended,
                            profile,
                            measured,
                            began,
                            enqueued: measured.then(Instant::now),
                            end: 0,
                        },
                        Some(epoch),
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
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
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
                    // Native keeps its fragment-boundary/on-demand policy;
                    // only synchronous adoption requests a per-prefix root sync.
                    if adoption && (tier == DurabilityTier::Synchronous || closes_extent) {
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
            // Validation and publication run here. Persistence is handed to the
            // flusher exactly like an append, so the WAL and frontier barriers of
            // several completions are shared and never run inside this worker.
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            Request::CompleteLiveGroup { epoch, token } => {
                let completion = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    shared.context.complete_live_published(epoch, token)
                }))
                .unwrap_or_else(|_| {
                    shared.context.poison_readers();
                    Err(TcpError::new(
                        "log control owner panicked during live completion; outcome may be unknown; reopen authority",
                    ))
                });
                match completion {
                    Ok((log, receipts, validation_ns, validation_samples)) => hand_off(
                        shared,
                        PendingFlush {
                            id,
                            charge,
                            completed,
                            log,
                            receipts,
                            reply: FlushReply::LiveCompleted {
                                validation_ns,
                                validation_samples,
                            },
                            profile,
                            measured,
                            began,
                            enqueued: measured.then(Instant::now),
                            end: 0,
                        },
                        None,
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
        let read_owned_barrier = match &request {
            Request::CheckpointPosition { .. } => true,
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            Request::ResolveLiveRecords { .. } => true,
            _ => false,
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
        complete(
            shared, id, charge, completed, response, profile, measured, began,
        );
    }
}

fn wire_receipts(receipts: &[AppendedBatch]) -> Vec<Receipt> {
    receipts
        .iter()
        .map(|receipt| Receipt {
            id: receipt.id(),
            version: receipt.version(),
            digest: receipt.digest(),
            address: receipt.address(),
        })
        .collect()
}

fn reply_for(
    reply: &FlushReply,
    receipts: Vec<Receipt>,
    persistence: super::super::storage::StorageWriteProfile,
) -> Response {
    match reply {
        FlushReply::Appended => {
            let _ = persistence;
            Response::Appended(receipts)
        }
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        FlushReply::LiveCompleted {
            validation_ns,
            validation_samples,
        } => Response::LiveCompleted(Box::new(live_control::LiveCompletionWire {
            receipts,
            validation_ns: *validation_ns,
            validation_samples: *validation_samples,
            persistence,
        })),
    }
}

/// A persistence owner failure: nothing later may be acknowledged.
fn fail_open(shared: &Shared, pending: PendingFlush, error: TcpError) {
    shared.context.poison_readers();
    {
        let mut queue = shared.queue.lock();
        queue.flush_failed = true;
        queue.stopped = true;
    }
    complete(
        shared,
        pending.id,
        pending.charge,
        pending.completed,
        Err(error),
        pending.profile,
        pending.measured,
        pending.began,
    );
}

/// Acknowledge an appended (or live-completed) group according to the tier.
/// Native replies now: the WAL write is in the page cache and the log's syncer
/// makes it durable in the background, so a process crash keeps it and a kernel
/// crash can lose only the unsynced suffix. Synchronous queues the group for the
/// flusher, which replies once the synced frontier covers it.
fn hand_off(shared: &Shared, mut pending: PendingFlush, prefix_epoch: Option<u64>) {
    let frontier = match shared.frontier_for(&pending.log) {
        Ok(frontier) => frontier,
        Err(error) => return fail_open(shared, pending, error),
    };
    // Native tier: the reply follows validation (a live completion) or the WAL
    // write (an append), both of which publish into the contiguous appended
    // frontier. A live frame beyond the persisted frontier root is recovered by
    // the tolerant live replay, so a process crash loses nothing acknowledged.
    #[cfg(test)]
    if let Some((entered, resume)) = shared.before_ack.lock().take() {
        let _ = entered.send(());
        let _ = resume.recv();
    }
    if !shared.synchronous.load(Ordering::Acquire) {
        // Re-checked at the acknowledgement point: an append paused before this
        // hand-off must not succeed after any persistence failure or poison.
        let poisoned = shared.context.terminal.load(Ordering::Acquire)
            || shared.context.poison_pending.load(Ordering::Acquire);
        if let Some(reason) = frontier
            .failure()
            .or_else(|| poisoned.then(|| "log control authority is poisoned".to_owned()))
        {
            return fail_open(
                shared,
                pending,
                TcpError::new(format!(
                    "log syncer failed; append outcome unknown: {reason}"
                )),
            );
        }
        let response = reply_for(
            &pending.reply,
            wire_receipts(&pending.receipts),
            super::super::storage::StorageWriteProfile::default(),
        );
        complete(
            shared,
            pending.id,
            pending.charge,
            pending.completed,
            Ok(response),
            pending.profile,
            pending.measured,
            pending.began,
        );
        return;
    }
    // Acquire: this load observes every group published before it, including
    // this command's own, so waiting for it covers them.
    pending.end = frontier.appended();
    let rejected = {
        let queue = shared.queue.lock();
        if pending.measured
            && let Some(epoch) = prefix_epoch
        {
            let mut prefix = super::super::write_timeline::ReadyAppendPrefix {
                commands: 0,
                batches: 0,
                encoded_bytes: 0,
            };
            let mut available = true;
            for queued in &queue.work {
                match &queued.request {
                    Request::Append {
                        epoch: queued_epoch,
                        ..
                    } if *queued_epoch == epoch => {}
                    _ => break,
                }
                if let Some((_, batches, bytes)) = queued.trace_shape {
                    prefix.commands += 1;
                    prefix.batches = prefix.batches.saturating_add(batches);
                    prefix.encoded_bytes = prefix.encoded_bytes.saturating_add(bytes);
                } else {
                    available = false;
                    break;
                }
            }
            pending.profile.ready_append_prefix = available.then_some(prefix);
        }
        if queue.flush_failed {
            Some(pending)
        } else {
            match shared.flush.send(FlushCommand::Append(pending)) {
                Ok(()) => None,
                Err(error) => match error.0 {
                    FlushCommand::Append(pending) => Some(pending),
                    FlushCommand::Barrier(_) | FlushCommand::Stop => None,
                },
            }
        }
    };
    if let Some(pending) = rejected {
        fail_open(
            shared,
            pending,
            TcpError::new("log persistence owner stopped; append outcome unknown"),
        );
    }
}

/// Group commit for the synchronous tier. The flusher takes the first queued
/// command with a blocking receive, then every command that is ready now
/// (`try_recv`); it never waits for more, so an empty queue adds no delay and a
/// command's wait is bounded by one sync. One wait on the log's synced frontier
/// covers all of them, because the syncer's capture includes every group
/// appended before the wait began. A reply follows the sync that covers it.
/// Count and byte caps bound the batch and the memory it pins.
const MAX_FLUSH_BATCH_BYTES: usize = 64 * 1024 * 1024;

fn flush_run(shared: &Shared, receiver: &mpsc::Receiver<FlushCommand>) {
    // The batch never exceeds MAX_COMMANDS entries and is reused.
    let mut batch: Vec<PendingFlush> = Vec::with_capacity(MAX_COMMANDS);
    let mut carried = None;
    loop {
        let command = match carried.take() {
            Some(command) => command,
            None => match receiver.recv() {
                Ok(command) => command,
                Err(_) => return,
            },
        };
        match command {
            FlushCommand::Append(first) => {
                let mut bytes = first.charge;
                batch.push(first);
                while batch.len() < MAX_COMMANDS && bytes < MAX_FLUSH_BATCH_BYTES {
                    match receiver.try_recv() {
                        Ok(FlushCommand::Append(next)) => {
                            bytes = bytes.saturating_add(next.charge);
                            batch.push(next);
                        }
                        // A barrier or stop keeps its FIFO place after this batch.
                        Ok(other) => {
                            carried = Some(other);
                            break;
                        }
                        Err(_) => break,
                    }
                }
                if !flush_batch(shared, &mut batch) {
                    drop(carried.take());
                    fail_queued_flush(
                        shared,
                        receiver,
                        "log persistence failed; append outcome may be unknown",
                    );
                    return;
                }
            }
            FlushCommand::Barrier(done) => {
                // Every command before this one is replied; now make anything
                // acknowledged under the native tier durable too.
                if let Err(error) = shared.flush_logs() {
                    drop(done);
                    shared.context.poison_readers();
                    {
                        let mut queue = shared.queue.lock();
                        queue.flush_failed = true;
                        queue.stopped = true;
                    }
                    fail_queued_flush(shared, receiver, &error.to_string());
                    return;
                }
                let _ = done.send(());
            }
            FlushCommand::Stop => return,
        }
    }
}

/// Complete every command in `batch`, one frontier wait per distinct log (one
/// active log per epoch, so runs are adjacent). Returns false after a failure;
/// the caller then fails everything still queued.
fn flush_batch(shared: &Shared, batch: &mut Vec<PendingFlush>) -> bool {
    for pending in batch.iter_mut() {
        if let (Some(enqueued), true) = (pending.enqueued, pending.measured) {
            let wait = elapsed_ns(enqueued);
            pending.profile.flusher_queue_ns = wait;
            pending.profile.control_queue_ns =
                pending.profile.control_queue_ns.saturating_add(wait);
        }
    }
    let mut failed = false;
    while !batch.is_empty() {
        let run = batch
            .iter()
            .take_while(|pending| Arc::ptr_eq(&pending.log, &batch[0].log))
            .count();
        if failed {
            for pending in batch.drain(..run) {
                complete(
                    shared,
                    pending.id,
                    pending.charge,
                    pending.completed,
                    Err(TcpError::new(
                        "log persistence failed; append outcome may be unknown",
                    )),
                    pending.profile,
                    pending.measured,
                    pending.began,
                );
            }
            continue;
        }
        let end = batch[..run]
            .iter()
            .map(|pending| pending.end)
            .max()
            .unwrap_or(0);
        let leader = batch[..run]
            .iter()
            .position(|pending| pending.measured)
            .unwrap_or(0);
        let started = batch[leader].measured.then(Instant::now);
        let log = &batch[0].log;
        let synced = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let frontier = log
                .frontier()
                .ok_or_else(|| TcpError::new("log frontier missing"))?;
            DurabilityTier::Synchronous
                .acknowledge(frontier, end, None)
                .map(|_| ())
                .map_err(|error| {
                    TcpError::new(format!(
                        "log persistence failed; append outcome may be unknown: {error}"
                    ))
                })
        }))
        .map_err(|_| {
            log.poison_writer();
            TcpError::new("log persistence panicked; append outcome may be unknown")
        })
        .and_then(|result| result);
        // Stage timings of the sync(s) this wait covered go to the batch leader.
        let sync_profile = log.take_sync_profile();
        if let Some(started) = started {
            let elapsed = elapsed_ns(started);
            let storage = &mut batch[leader].profile.storage_write;
            storage.add(sync_profile);
            // `add` drops the single-invocation capture; the leader keeps it.
            storage.persistence_capture = sync_profile.persistence_capture;
            storage.flush_batches = storage.flush_batches.saturating_add(1);
            storage.flush_batch_commands = storage.flush_batch_commands.saturating_add(run as u64);
            for pending in batch[..run].iter_mut().filter(|pending| pending.measured) {
                pending.profile.append_persist_ns += elapsed;
                pending.profile.control_execution_ns =
                    pending.profile.control_execution_ns.saturating_add(elapsed);
            }
        }
        match synced {
            Ok(()) => {
                for (index, pending) in batch.drain(..run).enumerate() {
                    let persistence = if index == leader {
                        sync_profile
                    } else {
                        super::super::storage::StorageWriteProfile::default()
                    };
                    let response = reply_for(
                        &pending.reply,
                        wire_receipts(&pending.receipts),
                        persistence,
                    );
                    complete(
                        shared,
                        pending.id,
                        pending.charge,
                        pending.completed,
                        Ok(response),
                        pending.profile,
                        pending.measured,
                        pending.began,
                    );
                }
            }
            Err(error) => {
                failed = true;
                shared.context.poison_readers();
                {
                    let mut queue = shared.queue.lock();
                    queue.flush_failed = true;
                    queue.stopped = true;
                }
                let message = error.to_string();
                for pending in batch.drain(..run) {
                    complete(
                        shared,
                        pending.id,
                        pending.charge,
                        pending.completed,
                        Err(TcpError::new(message.clone())),
                        pending.profile,
                        pending.measured,
                        pending.began,
                    );
                }
            }
        }
    }
    !failed
}

fn fail_queued_flush(shared: &Shared, receiver: &mpsc::Receiver<FlushCommand>, error: &str) {
    loop {
        match receiver.try_recv() {
            Ok(FlushCommand::Append(pending)) => complete(
                shared,
                pending.id,
                pending.charge,
                pending.completed,
                Err(TcpError::new(error)),
                pending.profile,
                pending.measured,
                pending.began,
            ),
            Ok(FlushCommand::Barrier(done)) => drop(done),
            Ok(FlushCommand::Stop)
            | Err(mpsc::TryRecvError::Empty | mpsc::TryRecvError::Disconnected) => return,
        }
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
    mut profile: ConnectionProfile,
    measured: bool,
    began: Option<Instant>,
) {
    profile.handler_ns = began.map_or(0, elapsed_ns);
    if measured {
        profile.control_execution_samples = 1;
    }
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
    // flusher beyond `MAX_COMMANDS`, so admission stays bounded.
    {
        let mut queue = shared.queue.lock();
        queue.requests -= 1;
        queue.bytes -= charge;
    }
    let _ = completed.try_send(reactor::WorkCompletion {
        id,
        response,
        profile,
        is_control: true,
    });
}

impl ControlContext {
    fn status(&self) -> Result<NodeStatus, TcpError> {
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        if self.poison_pending.load(Ordering::Acquire)
            && self.recoverable_live_failure.load(Ordering::Acquire)
        {
            return self.inventory.lock().recoverable_status();
        }
        self.inventory.lock().status()
    }
    pub(super) fn effective_max_reads(&self, limits: TcpLimits) -> usize {
        if self.wal_member_read == super::super::storage::WalMemberReadPolicy::Async {
            limits.max_reads.min(self.read_config.max_device_reads)
        } else {
            limits.max_reads
        }
    }
    fn permits_recovery_control(&self, request: &Request) -> bool {
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        {
            // Terminal publication does not revoke idle exports. Their pool
            // must still be able to release pins after confirmed disconnects.
            matches!(request, Request::ReleaseLiveExportPin { .. })
                || (self.recoverable_live_failure.load(Ordering::Acquire)
                    && matches!(request, Request::Status { .. } | Request::Fence { .. }))
        }
        #[cfg(not(all(feature = "experimental-nvmeof", target_os = "linux")))]
        {
            let _ = request;
            false
        }
    }

    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    fn poison_incomplete_live(&self) {
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
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        let _gate = self.poison_gate.lock();
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        self.recoverable_live_failure
            .store(false, Ordering::Release);
        self.poison_pending.store(true, Ordering::Release);
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        drop(_gate);
        self.finish_poison_if_quiescent();
    }
    pub(super) fn finish_poison_if_quiescent(&self) {
        if !self.poison_pending.load(Ordering::Acquire) {
            return;
        }
        // Record/sealed pins protect in-flight I/O. Idle controller pins protect
        // exports only and must not delay terminal authority publication.
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        let Some(leases) = self.nvmeof_leases.try_lock() else {
            return;
        };
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        if leases.has_live_or_quarantined() {
            return;
        }
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        {
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
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    fn publish_live_leases(
        &self,
        epoch: u64,
        leases: Vec<super::super::nvmeof::live_storage::LiveRecordLease>,
    ) -> Result<PublishedLiveLeases, TcpError> {
        let metadata: Vec<_> = leases
            .iter()
            .map(|lease| (lease.segment.clone(), lease.offset, u64::from(lease.len)))
            .collect();
        let registry = self.nvmeof_leases.lock();
        if !self.readers.read().active_valid(epoch, &self.terminal) {
            return Err(TcpError::new("stale live read authority"));
        }
        let mut inventory = self.inventory.lock();
        inventory.active(epoch)?;
        if inventory.live_target.is_none() {
            return Err(TcpError::new("live target not configured"));
        }
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
        let mut locations = Vec::with_capacity(metadata.len());
        for (segment, offset, len) in metadata {
            let binding = inventory
                .live_segment_writable(epoch, &segment)
                .and_then(|writable| inventory.publish_live_segment(&segment, writable));
            match binding {
                Ok(binding) => locations.push((segment, binding, offset, len)),
                Err(error) => {
                    // Publication can fail after creating a kernel capability.
                    // Keep its in-memory epoch pin and file leases quarantined;
                    // the durable target registry is drained on restart.
                    drop(inventory);
                    drop(registry);
                    self.poison_readers();
                    return Err(TcpError::new(format!(
                        "live read export failed; outcome unknown: {error}"
                    )));
                }
            }
        }
        Ok((token, locations))
    }
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
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
        let released = self.inventory.lock().release_live_pin(epoch, token);
        if let Err(error) = released {
            self.poison_readers();
            return Err(error);
        }
        Err(TcpError::new(
            "live metadata response exceeds negotiated frame",
        ))
    }
    fn append_profiled(
        &self,
        epoch: u64,
        batches: Vec<WireBytes>,
        mut profile: Option<&mut ConnectionProfile>,
        drain: Option<&mut dyn FnMut() -> Result<(), TcpError>>,
        mut turn: Option<&mut dyn FnMut()>,
    ) -> Result<(Arc<super::super::storage::PartitionLog>, Vec<AppendedBatch>), TcpError> {
        if self.poison_pending.load(Ordering::Acquire) || self.terminal.load(Ordering::Acquire) {
            return Err(TcpError::new(
                "log control authority is poisoned; reopen required",
            ));
        }
        let shape = &self.append_shape;
        let batch_limits = self.append_batch_limits;
        let group_limits = self.append_group_limits;
        if batches.is_empty() || batches.len() > group_limits.max_batches as usize {
            return Err(TcpError::new("invalid append group count"));
        }
        let began = profile.as_ref().map(|_| Instant::now());
        let batches = batches
            .into_iter()
            .map(|bytes| {
                CompleteBatch::decode_owned(shape, bytes.into_vec(), batch_limits)
                    .map_err(TcpError::component)
            })
            .collect::<Result<Vec<_>, _>>();
        if let Some(p) = profile.as_deref_mut() {
            p.append_decode_ns += began.map_or(0, elapsed_ns);
        }
        let began = profile.as_ref().map(|_| Instant::now());
        let group = batches.and_then(|batches| {
            CompleteGroup::new(shape, batches, batch_limits, group_limits)
                .map_err(TcpError::component)
        });
        if let Some(p) = profile.as_deref_mut() {
            p.append_group_build_ns += began.map_or(0, elapsed_ns);
        }
        let group = group?;

        let began = profile.as_ref().map(|_| Instant::now());
        // The node inventory lock covers only the active-log lookup. Holding it
        // across the append would serialize every append worker on one node-wide
        // lock. Controls that change the active log run exclusively (`next_work`).
        let log = {
            let inventory = self.inventory.lock();
            let residence = profile.as_ref().map(|_| Instant::now());
            if let Some(p) = profile.as_deref_mut() {
                p.append_inventory_wait_ns +=
                    began.zip(residence).map_or(0, |(start, acquired)| {
                        acquired
                            .duration_since(start)
                            .as_nanos()
                            .min(u64::MAX as u128) as u64
                    });
            }
            let log = inventory.active(epoch)?;
            if let Some(p) = profile.as_deref_mut() {
                p.append_inventory_residence_ns += residence.map_or(0, elapsed_ns);
            }
            log
        };
        let began = profile.as_ref().map(|_| Instant::now());
        // Appends of one owner must reach the WAL in submission order: the
        // coordinator's receipt history and checkpoint prefix digests assume it.
        if let Some(turn) = turn.as_mut() {
            turn();
        }
        let mut appended = log.try_append_group(
            RequestAuthority { epoch },
            &group,
            profile.as_deref_mut().map(|p| &mut p.storage_write),
        );
        let elapsed = began.map_or(0, elapsed_ns);
        if let Some(p) = profile.as_deref_mut() {
            p.append_wal_ns += elapsed;
        }
        let mut drain = drain;
        for _ in 0..ADMISSION_RETRIES {
            if !matches!(&appended, Err(StorageError::NeedsPersistence)) {
                break;
            }
            if let Some(drain) = drain.as_mut()
                && drain().is_err()
            {
                self.poison_readers();
                return Err(TcpError::new("log flusher stopped before admission retry"));
            }
            if let Err(error) = log.wait_history() {
                self.poison_readers();
                return Err(TcpError::component(error));
            }
            let began = profile.as_ref().map(|_| Instant::now());
            appended = if let Some(p) = profile.as_deref_mut() {
                log.append_group_profiled(RequestAuthority { epoch }, &group, &mut p.storage_write)
            } else {
                log.append_group(RequestAuthority { epoch }, &group)
            };
            let elapsed = began.map_or(0, elapsed_ns);
            if let Some(p) = profile.as_deref_mut() {
                p.append_wal_ns += elapsed;
            }
        }
        let receipts = match appended {
            Ok(receipts) => receipts,
            Err(error) => {
                if !matches!(
                    &error,
                    StorageError::WrongDatabase
                        | StorageError::WrongPartition
                        | StorageError::Fenced
                        | StorageError::Sealed
                        | StorageError::IdentityConflict
                        | StorageError::VersionConflict
                        | StorageError::Capacity
                        | StorageError::NeedsPersistence
                        | StorageError::MixedRetry
                        | StorageError::Codec(_)
                        | StorageError::Group(_)
                ) {
                    self.poison_readers();
                }
                return Err(TcpError::component(error));
            }
        };
        #[cfg(test)]
        assert!(
            !self.panic_after_append.swap(false, Ordering::AcqRel),
            "injected panic after native append",
        );
        Ok((log, receipts))
    }
    /// Validate the written live frame and publish its receipts. Durability is
    /// not established here: the caller makes the covering sync follow.
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    fn complete_live_published(
        &self,
        epoch: u64,
        token: [u8; 16],
    ) -> Result<
        (
            Arc<super::super::storage::PartitionLog>,
            Vec<AppendedBatch>,
            u64,
            u64,
        ),
        TcpError,
    > {
        let log = self.inventory.lock().active(epoch)?;
        let (receipts, validation_ns, validation_samples) = log
            .complete_live_group_profiled(RequestAuthority { epoch }, token)
            .map_err(|error| {
                if matches!(&error, StorageError::IncompleteLiveFrame) {
                    self.poison_incomplete_live();
                } else if matches!(&error, StorageError::Unknown(_) | StorageError::Poisoned) {
                    self.poison_readers();
                }
                TcpError::component(error)
            })?;
        Ok((log, receipts, validation_ns, validation_samples))
    }
    /// The preparer never waits for inventory: fence/seal can hold it while
    /// joining us. Retry at most 30,000 times, parking 1 ms between attempts.
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    fn publish_prepared_file(
        &self,
        log: &Arc<super::super::storage::PartitionLog>,
        file: u64,
        stopping: &AtomicBool,
        #[cfg(test)] waiting_for_inventory: &AtomicBool,
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
            // Acquire observes retirement before trying the inventory lock.
            if stopping.load(Ordering::Acquire) {
                return;
            }
            let Some(mut inventory) = self.inventory.try_lock() else {
                #[cfg(test)]
                // Release lets the test observe a real failed lock attempt.
                waiting_for_inventory.store(true, Ordering::Release);
                thread::park_timeout(Duration::from_millis(1));
                continue;
            };
            if inventory
                .live_target
                .as_ref()
                .is_none_or(|target| target.export_mode().files_per_subsystem().is_none())
            {
                return;
            }
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
            let result = inventory.publish_permanent_segment(&segment);
            #[cfg(test)]
            log.record_test_file_export(
                file,
                _attempt + 1,
                export_started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
                result.is_ok(),
            );
            if result.is_err() {
                drop(inventory);
                log.poison_writer();
                self.poison_readers();
            } else if matches!(result, Ok((_, true))) {
                log.live_file_published();
            }
            return;
        }
        // Optional preparation timed out; a later grant publishes the file.
    }

    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    fn lease_live_published(
        &self,
        epoch: u64,
        tier: DurabilityTier,
        after: Option<[u8; 16]>,
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
        let mut inventory = self.inventory.lock();
        #[cfg(test)]
        let inventory_wait_ns = publish_started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        #[cfg(test)]
        let publication_started = Instant::now();
        let binding = inventory.active(epoch).and_then(|current| {
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
            inventory.reap_idle_live_segments(epoch)?;
            if inventory
                .live_target
                .as_ref()
                .is_some_and(|target| target.export_mode().files_per_subsystem().is_some())
            {
                inventory
                    .publish_permanent_segment(&extent.segment)
                    .map(|(binding, _created)| binding)
            } else {
                inventory.publish_live_segment(&extent.segment, true)
            }
        });
        #[cfg(test)]
        let publication_call_ns = publication_started
            .elapsed()
            .as_nanos()
            .min(u64::MAX as u128) as u64;
        drop(inventory);
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

    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
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
                .lease_live_published(epoch, tier, None)
                .map(Response::LiveLeased),
            Request::PreleaseLiveExtent { epoch, after } => self
                .lease_live_published(epoch, tier, Some(after))
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
                    Some(self.lease_live_published(epoch, tier, None)?)
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
        mut profile: Option<&mut ConnectionProfile>,
        _drain_log: impl FnOnce(&Arc<super::super::storage::PartitionLog>) -> Result<(), TcpError>,
    ) -> Result<Response, TcpError> {
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        if let Request::ReleaseNvmeofExport { epoch, token } = &request {
            self.nvmeof_leases
                .lock()
                .release(*epoch, *token)
                .map_err(TcpError::component)?;
            self.finish_poison_if_quiescent();
            return Ok(Response::NvmeofExportReleased);
        }
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        if let Request::ReleaseNvmeofEpochPin { epoch, token } = &request {
            self.nvmeof_leases
                .lock()
                .release_epoch_pin(*epoch, *token)
                .map_err(TcpError::component)?;
            self.finish_poison_if_quiescent();
            return Ok(Response::NvmeofEpochReleased);
        }
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        if let Request::ReleaseLiveEpochPin { epoch, token } = &request {
            let (result, failed) = {
                let _leases = self.nvmeof_leases.lock();
                let mut inventory = self.inventory.lock();
                let result = if inventory.live_pins.contains_key(token) {
                    inventory.release_live_pin(*epoch, *token)
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
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        if let Request::ReleaseLiveExportPin { epoch, token } = &request {
            let result = self
                .inventory
                .lock()
                .release_live_export_pin(*epoch, *token);
            if result.is_err() && !self.inventory.lock().healthy {
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
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            Request::OpenLiveExports { authority_epoch } => {
                let mut inventory = self.inventory.lock();
                let log = inventory.active(authority_epoch)?;
                let target = inventory
                    .live_target
                    .as_ref()
                    .ok_or_else(|| TcpError::new("live target not configured"))?;
                let mode = target.export_mode();
                let (address, service, transport) = target.endpoint();
                let count = if mode.files_per_subsystem().is_some() {
                    log.live_file_count().map_err(TcpError::component)?
                } else {
                    0
                };
                for file in 0..count {
                    let segment = match log.live_segment(file) {
                        Ok(segment) => segment,
                        Err(StorageError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
                            continue;
                        }
                        Err(error) => return Err(TcpError::component(error)),
                    };
                    if let Err(error) = inventory.publish_permanent_segment(&segment) {
                        drop(inventory);
                        log.poison_writer();
                        self.poison_readers();
                        return Err(TcpError::new(format!(
                            "live target publication failed; outcome unknown: {error}"
                        )));
                    }
                }
                Ok(Response::LiveExports(live_control::LiveExportInfo {
                    mode,
                    address,
                    service,
                    transport,
                    wal_file_bytes: log.config().wal_file_bytes,
                    block_size: 4096,
                    exported_files: count,
                }))
            }
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            Request::LeaseLiveExtent { .. }
            | Request::PreleaseLiveExtent { .. }
            | Request::AdoptLiveExtent { .. } => Err(TcpError::new(
                "live extent requires the stream durability dispatch",
            )),
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            Request::ReserveLiveGroup { epoch, descriptor } => {
                use super::super::nvmeof::live_storage::LiveReserveResult;
                let mut inventory = self.inventory.lock();
                if inventory
                    .live_target
                    .as_ref()
                    .is_some_and(|target| target.export_mode().files_per_subsystem().is_some())
                {
                    return Err(TcpError::new(
                        "permanent live export requires leased write mode",
                    ));
                }
                let log = inventory.active(epoch)?;
                let mut result =
                    log.reserve_live_group(RequestAuthority { epoch }, descriptor.clone());
                if matches!(result, Err(StorageError::NeedsPersistence)) {
                    log.wait_history().map_err(TcpError::component)?;
                    result = log.reserve_live_group(RequestAuthority { epoch }, descriptor);
                }
                let result = match result {
                    Ok(result) => result,
                    Err(error) => {
                        drop(inventory);
                        if matches!(&error, StorageError::Unknown(_) | StorageError::Poisoned) {
                            self.poison_readers();
                        }
                        return Err(TcpError::component(error));
                    }
                };
                match result {
                    LiveReserveResult::Existing(receipts) => {
                        drop(inventory);
                        let durable = log.persist_many(&receipts).map_err(|error| {
                            self.poison_readers();
                            TcpError::component(error)
                        })?;
                        Ok(Response::LiveReserved(
                            live_control::LiveReserveWire::Existing(
                                durable.into_iter().map(Receipt::from_durable).collect(),
                            ),
                        ))
                    }
                    LiveReserveResult::Reserved(reservation) => {
                        let binding = inventory.reap_idle_live_segments(epoch).and_then(|()| {
                            inventory.publish_live_segment(&reservation.segment, true)
                        });
                        drop(inventory);
                        let binding = binding.map_err(|error| {
                            log.poison_writer();
                            self.poison_readers();
                            TcpError::new(format!(
                                "live target publication failed; outcome unknown: {error}"
                            ))
                        })?;
                        Ok(Response::LiveReserved(
                            live_control::LiveReserveWire::Reserved(LiveReserved {
                                reservation,
                                binding,
                            }),
                        ))
                    }
                }
            }
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            Request::CompleteLiveGroup { epoch, token } => {
                let (log, receipts, validation_ns, validation_samples) =
                    self.complete_live_published(epoch, token)?;
                let mut persistence = super::super::storage::StorageWriteProfile::default();
                let durable = log
                    .persist_many_profiled(&receipts, &mut persistence)
                    .map_err(|error| {
                        self.poison_readers();
                        TcpError::component(error)
                    })?;
                Ok(Response::LiveCompleted(Box::new(
                    live_control::LiveCompletionWire {
                        receipts: durable.into_iter().map(Receipt::from_durable).collect(),
                        validation_ns,
                        validation_samples,
                        persistence,
                    },
                )))
            }
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            Request::ResolveLiveRecords {
                authority_epoch,
                references,
            } => {
                if references
                    .iter()
                    .any(|reference| reference.batch_version.epoch == authority_epoch)
                    && self
                        .inventory
                        .lock()
                        .live_target
                        .as_ref()
                        .is_some_and(|target| target.export_mode().files_per_subsystem().is_some())
                {
                    return Err(TcpError::new(
                        "live record resolution is disabled for the active epoch in permanent export mode",
                    ));
                }
                if references.is_empty()
                    || references.len() > self.effective_max_reads(limits).min(256)
                {
                    return Err(TcpError::new("live record count exceeds negotiated bound"));
                }
                let snapshot =
                    self.readers
                        .read()
                        .snapshot(authority_epoch, &references, &self.terminal)?;
                let mut plans = Vec::with_capacity(snapshot.members.len());
                for (log, reference) in &snapshot.members {
                    plans.push(
                        log.resolve_live_record(reference)
                            .map_err(TcpError::component)?,
                    );
                }
                let (token, locations) = self.publish_live_leases(authority_epoch, plans)?;
                let entries = references
                    .into_iter()
                    .zip(locations)
                    .map(
                        |(reference, (segment, binding, offset, len))| LiveRecordLocation {
                            reference,
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
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            Request::RecoverLiveMetadata {
                authority_epoch,
                source_epoch,
                cursor,
                limit,
            } => {
                self.inventory.lock().require_history(false)?;
                if limit == 0 || limit > limits.recovery_page_batches || limit > 256 {
                    return Err(TcpError::new("live recovery page count exceeds bound"));
                }
                let log = self
                    .inventory
                    .lock()
                    .live_recovery_log(authority_epoch, source_epoch)?;
                let (receipts, plans, next) = if let Some(log) = log {
                    let count = log.receipt_count();
                    if cursor > count {
                        return Err(TcpError::new("recovery cursor beyond retained history"));
                    }
                    let receipts = log
                        .receipt_page(cursor, limit)
                        .map_err(TcpError::component)?;
                    let mut durable = Vec::with_capacity(receipts.len());
                    let mut plans = Vec::with_capacity(receipts.len());
                    for receipt in receipts {
                        durable.push(Receipt::from_durable(
                            log.persist_through(receipt).map_err(TcpError::component)?,
                        ));
                        plans.push(
                            log.resolve_live_batch(receipt)
                                .map_err(TcpError::component)?,
                        );
                    }
                    let end = cursor + durable.len();
                    (durable, plans, (end < count).then_some(end))
                } else {
                    if cursor != 0 {
                        return Err(TcpError::new("invalid abandoned epoch cursor"));
                    }
                    (Vec::new(), Vec::new(), None)
                };
                let (token, locations) = self.publish_live_leases(authority_epoch, plans)?;
                let entries = receipts
                    .into_iter()
                    .zip(locations)
                    .map(|(receipt, (segment, binding, offset, len))| {
                        live_control::LiveRecoveryEntryWire {
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
                    Response::LiveRecovery(live_control::LiveRecoveryWire {
                        entries,
                        next,
                        epoch_pin_token: token,
                    }),
                    limits.max_frame_bytes,
                )
            }
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            Request::ReplayLiveWal {
                authority_epoch,
                source_epoch,
                from,
                limit,
                require_end,
            } => {
                let log = {
                    let inventory = self.inventory.lock();
                    inventory.require_history(true)?;
                    inventory.live_recovery_log(authority_epoch, source_epoch)?
                };
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
                let (token, locations) = self.publish_live_leases(authority_epoch, plans)?;
                let entries = receipts
                    .into_iter()
                    .zip(locations)
                    .map(|(receipt, (segment, binding, offset, len))| {
                        live_control::LiveRecoveryEntryWire {
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
                    Response::LiveRecovery(live_control::LiveRecoveryWire {
                        entries,
                        next,
                        epoch_pin_token: token,
                    }),
                    limits.max_frame_bytes,
                )
            }
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            Request::AcquireLiveExportPin {
                epoch,
                segment,
                binding,
            } => self
                .inventory
                .lock()
                .acquire_live_export_pin(epoch, &segment, &binding)
                .map(Response::NvmeofEpochPinned),
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            Request::LiveTargetStats => {
                let inventory = self.inventory.lock();
                let target = inventory
                    .live_target
                    .as_ref()
                    .ok_or_else(|| TcpError::new("live target not configured"))?;
                Ok(Response::LiveTargetStats(
                    target.stats().map_err(TcpError::component)?,
                ))
            }
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            Request::ShutdownLiveTarget { epoch } => {
                let leases = self.nvmeof_leases.lock();
                if leases.has_live_or_quarantined() {
                    return Err(TcpError::new("NVMe-oF leases must drain before shutdown"));
                }
                self.inventory
                    .lock()
                    .shutdown_live_target(epoch)
                    .map(Response::LiveTargetStats)
            }
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            Request::RegisterNvmeofEpochPin { epoch } => {
                let mut leases = self.nvmeof_leases.lock();
                if self.poison_pending.load(Ordering::Acquire)
                    || !self.readers.read().active_valid(epoch, &self.terminal)
                {
                    return Err(TcpError::new("NVMe-oF epoch pin authority mismatch"));
                }
                let token = leases
                    .register_epoch_pin(epoch)
                    .map_err(TcpError::component)?;
                if self.poison_pending.load(Ordering::Acquire)
                    || !self.readers.read().active_valid(epoch, &self.terminal)
                {
                    leases
                        .release_epoch_pin(epoch, token)
                        .map_err(TcpError::component)?;
                    return Err(TcpError::new("authority changed during NVMe-oF epoch pin"));
                }
                Ok(Response::NvmeofEpochPinned(token))
            }
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            Request::ReleaseNvmeofEpochPin { .. }
            | Request::ReleaseLiveEpochPin { .. }
            | Request::ReleaseLiveExportPin { .. } => {
                unreachable!("release handled before health check")
            }
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            Request::RegisterNvmeofExport {
                epoch,
                binding,
                reference,
            } => {
                let mut leases = self.nvmeof_leases.lock();
                if self.poison_pending.load(Ordering::Acquire)
                    || !self.readers.read().active_valid(epoch, &self.terminal)
                    || binding.database_uuid != self.partition.inventory.database
                    || binding.partition != self.partition.partition
                    || binding.epoch > epoch
                    || binding.generation != binding.epoch
                    || binding.wal_file_bytes != self.partition.wal_file_bytes
                {
                    return Err(TcpError::new("NVMe-oF export authority mismatch"));
                }
                // Retirement is serialized by this same lease gate. Recheck a
                // durable member of the exact source file at the admission
                // boundary, after any earlier paged catalog validation.
                let snapshot = self.readers.read().snapshot(
                    epoch,
                    std::slice::from_ref(&reference),
                    &self.terminal,
                )?;
                let (log, member) = snapshot
                    .members
                    .first()
                    .ok_or_else(|| TcpError::new("missing export authority member"))?;
                let location = log
                    .nvmeof_record_location(member)
                    .map_err(TcpError::component)?;
                if location.reference != reference
                    || reference.batch_version.epoch != binding.epoch
                    || reference.batch_address.segment_generation != binding.generation
                    || location.wal_file_bytes != binding.wal_file_bytes
                    || location.source_file_index != binding.source_file_index
                    || location.source_device != binding.source_device
                    || location.source_inode != binding.source_inode
                    || location.source_file_len != binding.image_len
                    || location.durable_frontier != binding.durable_frontier
                    || !self.readers.read().snapshot_valid(
                        epoch,
                        snapshot.generation,
                        &self.terminal,
                    )
                {
                    return Err(TcpError::new(
                        "NVMe-oF source no longer matches owner authority",
                    ));
                }
                let token = leases
                    .register(epoch, &binding)
                    .map_err(TcpError::component)?;
                if self.poison_pending.load(Ordering::Acquire)
                    || !self.readers.read().active_valid(epoch, &self.terminal)
                {
                    leases.release(epoch, token).map_err(TcpError::component)?;
                    return Err(TcpError::new(
                        "authority changed during NVMe-oF registration",
                    ));
                }
                Ok(Response::NvmeofExportRegistered(token))
            }
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            Request::ReleaseNvmeofExport { .. } => {
                unreachable!("release handled before health check")
            }
            #[cfg(feature = "experimental-nvmeof")]
            Request::ValidateRecordReferences { epoch, references } => {
                if references.is_empty() || references.len() > limits.max_reads.min(256) {
                    return Err(TcpError::new("record validation count exceeds bound"));
                }
                let snapshot = self
                    .readers
                    .read()
                    .snapshot(epoch, &references, &self.terminal)?;
                let generation = snapshot.generation;
                // Pageable descriptor I/O runs on this bounded control owner,
                // never under reader authority or on the network reactor.
                let locations = snapshot
                    .members
                    .iter()
                    .map(|(log, reference)| {
                        log.nvmeof_record_location(reference)
                            .map_err(TcpError::component)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                if !self
                    .readers
                    .read()
                    .snapshot_valid(epoch, generation, &self.terminal)
                {
                    return Err(TcpError::new(
                        "reader authority changed during export admission",
                    ));
                }
                Ok(Response::ValidatedRecordReferences(locations))
            }
            Request::Checkpoint {
                authority_epoch,
                epoch,
                prefix,
            } => self
                .inventory
                .lock()
                .checkpoint_proof(authority_epoch, epoch, prefix)
                .map(Response::Checkpoint),
            Request::CheckpointPosition {
                authority_epoch,
                epoch,
                through,
            } => {
                let mut measured = profile.as_deref_mut();
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
            Request::ReplayWal {
                authority_epoch,
                source_epoch,
                from,
                limit,
                require_end,
            } => self.inventory.lock().replay_wal_page(
                authority_epoch,
                source_epoch,
                from,
                limit,
                require_end,
                limits,
            ),
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
            Request::RetireFiles {
                authority_epoch,
                epoch,
                prefix,
                files,
            } => {
                #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
                let _leases = {
                    let leases = self.nvmeof_leases.lock();
                    if leases.has_live_or_quarantined() {
                        return Err(TcpError::new("NVMe-oF leases must drain before retirement"));
                    }
                    leases
                };
                let result =
                    self.inventory
                        .lock()
                        .retire_files(authority_epoch, epoch, prefix, &files);
                result.map(|()| Response::Retired)
            }
            Request::RetireWalFiles {
                authority_epoch,
                epoch,
                files,
            } => {
                self.inventory.lock().require_history(true)?;
                #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
                let _leases = {
                    let leases = self.nvmeof_leases.lock();
                    if leases.has_live_or_quarantined() {
                        return Err(TcpError::new("NVMe-oF leases must drain before retirement"));
                    }
                    leases
                };
                self.inventory
                    .lock()
                    .retire_wal_files(authority_epoch, epoch, &files)
                    .map(|()| Response::Retired)
            }
            Request::ReadIoStats => {
                let log = {
                    let inventory = self.inventory.lock();
                    let active = inventory.status()?.active_epoch;
                    active.map(|epoch| inventory.active(epoch)).transpose()?
                };
                let mut stats = self.read_admission.stats();
                stats.authority = log.map(|log| log.authority_cache_stats());
                Ok(Response::ReadIoStats(stats))
            }
            Request::OperationalReadIoStats => Ok(Response::OperationalReadIoStats(Box::new((
                self.partition.clone(),
                self.read_admission.try_stats(),
            )))),
            Request::Status { protocol } => {
                if protocol != PROTOCOL {
                    return Err(TcpError::new("unsupported wire protocol"));
                }
                let mut status = self.status()?;
                status.max_frame_bytes = limits.max_frame_bytes;
                status.max_reads = self.effective_max_reads(limits);
                status.max_read_bytes = self.read_config.max_bytes;
                status.recovery_page_batches = limits.recovery_page_batches;
                status.polling = limits.polling;
                Ok(Response::Status(status))
            }
            Request::Fence { epoch } => {
                #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
                let _leases = {
                    let leases = self.nvmeof_leases.lock();
                    if leases.has_live_or_quarantined() {
                        return Err(TcpError::new("NVMe-oF leases must drain before fencing"));
                    }
                    leases
                };
                #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
                let mut status = if self.recoverable_live_failure.load(Ordering::Acquire) {
                    let status = self
                        .inventory
                        .lock()
                        .fence_recoverable_live(epoch, _drain_log)?;
                    let _gate = self.poison_gate.lock();
                    if !self.recoverable_live_failure.swap(false, Ordering::AcqRel) {
                        return Err(TcpError::new(
                            "owner failed during live fence; restart required",
                        ));
                    }
                    self.poison_pending.store(false, Ordering::Release);
                    status
                } else {
                    self.inventory
                        .lock()
                        .fence_to_with_drain(epoch, _drain_log)?
                };
                #[cfg(not(all(feature = "experimental-nvmeof", target_os = "linux")))]
                let mut status = self.inventory.lock().fence_to(epoch)?;
                status.max_frame_bytes = limits.max_frame_bytes;
                status.max_reads = self.effective_max_reads(limits);
                status.max_read_bytes = self.read_config.max_bytes;
                status.recovery_page_batches = limits.recovery_page_batches;
                status.polling = limits.polling;
                Ok(Response::Status(status))
            }
            Request::Append { epoch, batches } => {
                let (log, receipts) =
                    self.append_profiled(epoch, batches, profile.as_deref_mut(), None, None)?;
                let began = profile.as_ref().map(|_| Instant::now());
                let durable = if let Some(p) = profile.as_deref_mut() {
                    log.persist_many_profiled(&receipts, &mut p.storage_write)
                } else {
                    log.persist_many(&receipts)
                };
                if let Some(p) = profile {
                    p.append_persist_ns += began.map_or(0, elapsed_ns);
                }
                let durable = durable.map_err(|error| {
                    self.poison_readers();
                    TcpError::component(error)
                })?;
                Ok(Response::Appended(
                    durable.into_iter().map(Receipt::from_durable).collect(),
                ))
            }
            Request::Read { .. } | Request::ReadProfiled { .. } => {
                Err(TcpError::new("read sent to the log control owner"))
            }
            Request::Recover {
                epoch,
                cursor,
                limit,
            } => self
                .inventory
                .lock()
                .recovery_page(epoch, cursor, limit, limits),
            Request::Seal { epoch } => {
                #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
                let _leases = {
                    let leases = self.nvmeof_leases.lock();
                    if leases.has_live_or_quarantined() {
                        return Err(TcpError::new("NVMe-oF leases must drain before sealing"));
                    }
                    leases
                };
                self.inventory.lock().seal_draining(epoch, _drain_log)?;
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
            #[cfg(feature = "test-utils")]
            Request::PrepareRamProbe { .. } | Request::ReadRamProbe => Err(TcpError::new(
                "RAM probes require the authenticated session owner",
            )),
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
        }
    }
}
