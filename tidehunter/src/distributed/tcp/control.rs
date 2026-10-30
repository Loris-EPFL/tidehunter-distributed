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

pub(super) struct ControlContext {
    pub(super) partition: PartitionConfig,
    pub(super) inventory: Arc<Mutex<NodeInventory>>,
    pub(super) readers: Arc<parking_lot::RwLock<inventory::ReaderAuthority>>,
    pub(super) terminal: Arc<AtomicBool>,
    pub(super) append_shape: KeyShape,
    pub(super) append_batch_limits: BatchLimits,
    pub(super) append_group_limits: GroupLimits,
    pub(super) wal_member_read: super::super::storage::WalMemberReadPolicy,
    pub(super) read_config: read_executor::ReadExecutorConfig,
    pub(super) read_admission: Arc<read_executor::ReadAdmission>,
    pub(super) resident_grants: Arc<resident::GrantRegistry>,
    #[cfg(test)]
    pub(super) panic_after_append: Arc<AtomicBool>,
}

pub(super) struct ControlWork {
    pub(super) id: u64,
    pub(super) request: Request,
    pub(super) limits: TcpLimits,
    pub(super) measured: bool,
    pub(super) queued: Option<Instant>,
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
}
struct Shared {
    queue: Mutex<Queue>,
    ready: Condvar,
    context: Arc<ControlContext>,
    flush: mpsc::SyncSender<FlushCommand>,
}

struct PendingFlush {
    id: u64,
    charge: usize,
    completed: mpsc::SyncSender<reactor::WorkCompletion>,
    log: Arc<super::super::storage::PartitionLog>,
    receipts: Vec<AppendedBatch>,
    profile: ConnectionProfile,
    measured: bool,
    began: Option<Instant>,
    enqueued: Option<Instant>,
}

// Keep append commands inline: the fixed-capacity channel bounds resident work,
// and boxing would add a per-append allocation.
#[allow(clippy::large_enum_variant)]
enum FlushCommand {
    Append(PendingFlush),
    Barrier(mpsc::SyncSender<()>),
    Stop,
}
pub(super) struct ControlOwner {
    shared: Arc<Shared>,
    thread: Option<JoinHandle<()>>,
    flush_thread: Option<JoinHandle<()>>,
}
impl ControlOwner {
    pub(super) fn start(context: Arc<ControlContext>) -> Result<Self, TcpError> {
        let (flush, receiver) = mpsc::sync_channel(MAX_COMMANDS);
        let shared = Arc::new(Shared {
            queue: Mutex::new(Queue {
                work: VecDeque::new(),
                requests: 0,
                bytes: 0,
                stopped: false,
                flush_failed: false,
            }),
            ready: Condvar::new(),
            context,
            flush,
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
        let owner = shared.clone();
        let thread = match thread::Builder::new()
            .name("value-wal-control".into())
            .spawn(move || run(&owner))
        {
            Ok(thread) => thread,
            Err(error) => {
                let _ = shared.flush.send(FlushCommand::Stop);
                let _ = flush_thread.join();
                return Err(TcpError::from(error));
            }
        };
        Ok(Self {
            shared,
            thread: Some(thread),
            flush_thread: Some(flush_thread),
        })
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
}
impl Drop for ControlOwner {
    fn drop(&mut self) {
        self.shared.queue.lock().stopped = true;
        self.shared.ready.notify_all();
        if let Some(thread) = self.thread.take()
            && thread.join().is_err()
        {
            self.shared.context.poison_readers();
        }
        let _ = self.shared.flush.send(FlushCommand::Stop);
        if let Some(thread) = self.flush_thread.take()
            && thread.join().is_err()
        {
            self.shared.context.poison_readers();
        }
    }
}

fn run(shared: &Shared) {
    // One FIFO appender preserves control ordering. Its bounded flush queue lets
    // complete appends proceed while the flusher is in a disk barrier.
    loop {
        let work = {
            let mut queue = shared.queue.lock();
            while queue.work.is_empty() && !queue.stopped {
                shared.ready.wait(&mut queue);
            }
            let Some(work) = queue.work.pop_front() else {
                return;
            };
            work
        };
        let ControlWork {
            id,
            request,
            limits,
            measured,
            queued,
            charge,
            completed,
        } = work;
        let mut profile = ConnectionProfile::default();
        let began = measured.then(Instant::now);
        if let (Some(queued), Some(began)) = (queued, began) {
            profile.control_queue_ns = queued.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
            profile.control_queue_samples = 1;
            profile.handler_ns = began.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
        }
        if shared.context.terminal.load(Ordering::Acquire) {
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
                let append = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    shared.context.append_profiled(
                        epoch,
                        batches,
                        measured.then_some(&mut profile),
                        Some(&mut drain),
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
                    profile.control_execution_ns =
                        began.map_or(0, elapsed_ns).saturating_sub(drain_wait_ns);
                }
                match append {
                    Ok((log, receipts)) => {
                        let pending = PendingFlush {
                            id,
                            charge,
                            completed,
                            log,
                            receipts,
                            profile,
                            measured,
                            began,
                            enqueued: measured.then(Instant::now),
                        };
                        let rejected = {
                            let queue = shared.queue.lock();
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
                                Err(TcpError::new(
                                    "log persistence owner stopped; append outcome unknown",
                                )),
                                pending.profile,
                                pending.measured,
                                pending.began,
                            );
                        }
                    }
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

        // Every non-append control is a FIFO barrier: no fence, seal, status or
        // recovery response can overtake accepted appends or their completions.
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
            shared
                .context
                .handle_profiled(request, limits, measured.then_some(&mut profile))
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

fn flush_run(shared: &Shared, receiver: &mpsc::Receiver<FlushCommand>) {
    while let Ok(command) = receiver.recv() {
        match command {
            FlushCommand::Append(mut pending) => {
                if let (Some(enqueued), true) = (pending.enqueued, pending.measured) {
                    pending.profile.control_queue_ns = pending
                        .profile
                        .control_queue_ns
                        .saturating_add(elapsed_ns(enqueued));
                }
                let started = pending.measured.then(Instant::now);
                let durable = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    if pending.measured {
                        pending.log.persist_many_profiled(
                            &pending.receipts,
                            &mut pending.profile.storage_write,
                        )
                    } else {
                        pending.log.persist_many(&pending.receipts)
                    }
                }))
                .map_err(|_| {
                    pending.log.poison_writer();
                    TcpError::new("log persistence panicked; append outcome may be unknown")
                })
                .and_then(|durable| durable.map_err(TcpError::component));
                if let Some(started) = started {
                    let elapsed = elapsed_ns(started);
                    pending.profile.append_persist_ns += elapsed;
                    pending.profile.control_execution_ns =
                        pending.profile.control_execution_ns.saturating_add(elapsed);
                }
                let failed = durable.is_err();
                let result = durable.map(|receipts| {
                    Response::Appended(receipts.into_iter().map(Receipt::from_durable).collect())
                });
                if failed {
                    shared.context.poison_readers();
                    let mut queue = shared.queue.lock();
                    queue.flush_failed = true;
                    queue.stopped = true;
                }
                complete(
                    shared,
                    pending.id,
                    pending.charge,
                    pending.completed,
                    result,
                    pending.profile,
                    pending.measured,
                    pending.began,
                );
                if failed {
                    fail_queued_flush(
                        shared,
                        receiver,
                        "log persistence failed; append outcome may be unknown",
                    );
                    return;
                }
            }
            FlushCommand::Barrier(done) => {
                let _ = done.send(());
            }
            FlushCommand::Stop => return,
        }
    }
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
    let _ = completed.try_send(reactor::WorkCompletion {
        id,
        response,
        profile,
        is_control: true,
    });
    let mut queue = shared.queue.lock();
    queue.requests -= 1;
    queue.bytes -= charge;
}

impl ControlContext {
    fn status(&self) -> Result<NodeStatus, TcpError> {
        self.inventory.lock().status()
    }
    pub(super) fn effective_max_reads(&self, limits: TcpLimits) -> usize {
        if self.wal_member_read == super::super::storage::WalMemberReadPolicy::Async {
            limits.max_reads.min(self.read_config.max_device_reads)
        } else {
            limits.max_reads
        }
    }
    pub(super) fn poison_readers(&self) {
        self.resident_grants.request_poison();
        self.finish_poison_if_quiescent();
    }
    pub(super) fn finish_poison_if_quiescent(&self) {
        if self.resident_grants.poison_quiescent() && !self.terminal.swap(true, Ordering::AcqRel) {
            // The terminal authority boundary is after every session's exact
            // drain acknowledgment. Readers remain valid while notifications
            // are in flight; no successful cached result can cross this point.
            // A lost or unknown session stays represented as a live/quarantined
            // owner: poison cannot publish terminal authority until remote DMA
            // is proven impossible (operationally, restart the affected process).
            self.readers.write().invalidate();
        }
    }
    fn append_profiled(
        &self,
        epoch: u64,
        batches: Vec<WireBytes>,
        mut profile: Option<&mut ConnectionProfile>,
        drain: Option<&mut dyn FnMut() -> Result<(), TcpError>>,
    ) -> Result<(Arc<super::super::storage::PartitionLog>, Vec<AppendedBatch>), TcpError> {
        if self.terminal.load(Ordering::Acquire) || self.resident_grants.poison_requested() {
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
        let inventory = self.inventory.lock();
        let residence = profile.as_ref().map(|_| Instant::now());
        if let Some(p) = profile.as_deref_mut() {
            p.append_inventory_wait_ns += began.zip(residence).map_or(0, |(start, acquired)| {
                acquired
                    .duration_since(start)
                    .as_nanos()
                    .min(u64::MAX as u128) as u64
            });
        }
        let log = inventory.active(epoch)?;
        let began = profile.as_ref().map(|_| Instant::now());
        let mut appended = log.try_append_group(
            RequestAuthority { epoch },
            &group,
            profile.as_deref_mut().map(|p| &mut p.storage_write),
        );
        let elapsed = began.map_or(0, elapsed_ns);
        if let Some(p) = profile.as_deref_mut() {
            p.append_wal_ns += elapsed;
            p.append_inventory_residence_ns += residence.map_or(0, elapsed_ns);
        }
        drop(inventory);
        if matches!(&appended, Err(StorageError::NeedsPersistence)) {
            if let Some(drain) = drain
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
            if let Some(p) = profile {
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
    pub(super) fn handle_profiled(
        &self,
        request: Request,
        limits: TcpLimits,
        mut profile: Option<&mut ConnectionProfile>,
    ) -> Result<Response, TcpError> {
        // Admission can precede an earlier command's panic or uncertain I/O.
        // Queued work must recheck terminal health before decoding or mutation.
        if self.terminal.load(Ordering::Acquire) || self.resident_grants.poison_requested() {
            return Err(TcpError::new(
                "log control authority is poisoned; reopen required",
            ));
        }
        match request {
            Request::Checkpoint {
                authority_epoch,
                epoch,
                prefix,
            } => self
                .inventory
                .lock()
                .checkpoint_proof(authority_epoch, epoch, prefix)
                .map(Response::Checkpoint),
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
                if !self.resident_grants.close_admission() {
                    return Err(TcpError::new(
                        "resident grants are live or being issued; release them before retirement",
                    ));
                }
                let result =
                    self.inventory
                        .lock()
                        .retire_files(authority_epoch, epoch, prefix, &files);
                if result.is_ok() {
                    self.resident_grants.reopen_admission();
                }
                result.map(|()| Response::Retired)
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
            Request::ResidentGrantStats => Ok(Response::ResidentGrantStats(Box::new((
                self.partition.clone(),
                self.resident_grants.try_promotion_stats(),
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
                if !self.resident_grants.close_admission() {
                    return Err(TcpError::new(
                        "resident grants are live or being issued; release them before fencing",
                    ));
                }
                let mut status = self.inventory.lock().fence_to(epoch)?;
                status.max_frame_bytes = limits.max_frame_bytes;
                status.max_reads = self.effective_max_reads(limits);
                status.max_read_bytes = self.read_config.max_bytes;
                status.recovery_page_batches = limits.recovery_page_batches;
                status.polling = limits.polling;
                self.resident_grants.reopen_admission();
                Ok(Response::Status(status))
            }
            Request::Append { epoch, batches } => {
                let (log, receipts) =
                    self.append_profiled(epoch, batches, profile.as_deref_mut(), None)?;
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
            Request::RevokeGrants { .. } => Err(TcpError::new(
                "resident grant revocation requires the session owner",
            )),
            Request::Recover {
                epoch,
                cursor,
                limit,
            } => self
                .inventory
                .lock()
                .recovery_page(epoch, cursor, limit, limits),
            Request::Seal { epoch } => {
                if !self.resident_grants.close_admission() {
                    return Err(TcpError::new(
                        "resident grants are live or being issued; release them before sealing",
                    ));
                }
                self.inventory.lock().seal(epoch)?;
                Ok(Response::Sealed)
            }
            Request::StatsDetailed => {
                let stats = self.inventory.lock().stats()?;
                Ok(Response::StatsDetailed {
                    groups: stats.appended_groups,
                    barriers: stats.persistence_barriers,
                    namespace_syncs: stats.namespace_syncs,
                    inventory_witnesses: stats.inventory_witnesses,
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
            Request::ConnectionProfile { .. } => Err(TcpError::new(
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
                if evict {
                    self.read_admission.clear_cache();
                }
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
