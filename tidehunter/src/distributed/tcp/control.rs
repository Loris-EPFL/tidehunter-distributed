//! One bounded FIFO owner for this node's physical append stream and controls.
//!
//! Like `GroupCommitWorker`, accepted work owns its payload through completion,
//! drains on shutdown and never runs synchronous WAL I/O on network progress.
//! TiKV WriteBarrier::enter services an empty queue immediately, then takes only
//! callers already behind active work; no timer is needed here. Each Append RPC
//! already contains one complete recovery group and one exact-file sync. Keep
//! those groups separate: there is never a second unpersisted group per stream.
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
    pub(super) inventory: Arc<Mutex<NodeInventory>>,
    pub(super) readers: Arc<parking_lot::RwLock<inventory::ReaderAuthority>>,
    pub(super) terminal: Arc<AtomicBool>,
    pub(super) append_shape: KeyShape,
    pub(super) append_batch_limits: BatchLimits,
    pub(super) append_group_limits: GroupLimits,
    pub(super) wal_member_read: super::super::storage::WalMemberReadPolicy,
    pub(super) read_config: read_executor::ReadExecutorConfig,
    pub(super) read_admission: Arc<read_executor::ReadAdmission>,
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
}
struct Shared {
    queue: Mutex<Queue>,
    ready: Condvar,
    context: Arc<ControlContext>,
}

pub(super) struct ControlOwner {
    shared: Arc<Shared>,
    thread: Option<JoinHandle<()>>,
}
impl ControlOwner {
    pub(super) fn start(context: Arc<ControlContext>) -> Result<Self, TcpError> {
        let shared = Arc::new(Shared {
            queue: Mutex::new(Queue {
                work: VecDeque::new(),
                requests: 0,
                bytes: 0,
                stopped: false,
            }),
            ready: Condvar::new(),
            context,
        });
        let owner = shared.clone();
        let thread = thread::Builder::new()
            .name("value-wal-control".into())
            .spawn(move || run(&owner))?;
        Ok(Self {
            shared,
            thread: Some(thread),
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
    }
}

fn run(shared: &Shared) {
    // Intentionally lives until its sole owner stops admission, then drains all
    // accepted commands. Neither a dropped caller nor a deadline cancels I/O.
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
            profile.control_queue_ns = began
                .duration_since(queued)
                .as_nanos()
                .min(u128::from(u64::MAX)) as u64;
            profile.control_queue_samples = 1;
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
        profile.handler_ns = began.map_or(0, elapsed_ns);
        if measured {
            profile.control_execution_ns = profile.handler_ns;
            profile.control_execution_samples = 1;
        }
        // Capacity is SESSION_REQUESTS, exactly the maximum accepted session
        // work. Delivery never waits for a disconnected or slow receiver.
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
        self.terminal.store(true, Ordering::Release);
        self.readers.write().invalidate();
    }
    pub(super) fn handle_profiled(
        &self,
        request: Request,
        limits: TcpLimits,
        mut profile: Option<&mut ConnectionProfile>,
    ) -> Result<Response, TcpError> {
        // Admission can precede an earlier command's panic or uncertain I/O.
        // Queued work must recheck terminal health before decoding or mutation.
        if self.terminal.load(Ordering::Acquire) {
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
                self.inventory
                    .lock()
                    .retire_files(authority_epoch, epoch, prefix, &files)?;
                Ok(Response::Retired)
            }
            Request::ReadIoStats => Ok(Response::ReadIoStats(self.read_admission.stats())),
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
                let mut status = self.inventory.lock().fence_to(epoch)?;
                status.max_frame_bytes = limits.max_frame_bytes;
                status.max_reads = self.effective_max_reads(limits);
                status.max_read_bytes = self.read_config.max_bytes;
                status.recovery_page_batches = limits.recovery_page_batches;
                status.polling = limits.polling;
                Ok(Response::Status(status))
            }
            Request::Append { epoch, batches } => {
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
                    p.append_inventory_wait_ns +=
                        began.zip(residence).map_or(0, |(start, acquired)| {
                            acquired
                                .duration_since(start)
                                .as_nanos()
                                .min(u64::MAX as u128) as u64
                        });
                }
                let result = (|| {
                    let log = inventory.active(epoch)?;
                    let began = profile.as_ref().map(|_| Instant::now());
                    let appended = if let Some(p) = profile.as_deref_mut() {
                        log.append_group_profiled(
                            RequestAuthority { epoch },
                            &group,
                            &mut p.storage_write,
                        )
                    } else {
                        log.append_group(RequestAuthority { epoch }, &group)
                    };
                    if let Some(p) = profile.as_deref_mut() {
                        p.append_wal_ns += began.map_or(0, elapsed_ns);
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
                    let began = profile.as_ref().map(|_| Instant::now());
                    let durable = if let Some(p) = profile.as_deref_mut() {
                        log.persist_many_profiled(&receipts, &mut p.storage_write)
                    } else {
                        log.persist_many(&receipts)
                    };
                    if let Some(p) = profile.as_deref_mut() {
                        p.append_persist_ns += began.map_or(0, elapsed_ns);
                    }
                    let durable = match durable {
                        Ok(durable) => durable,
                        Err(error) => {
                            self.poison_readers();
                            return Err(TcpError::component(error));
                        }
                    };
                    Ok(Response::Appended(
                        durable.into_iter().map(Receipt::from_durable).collect(),
                    ))
                })();
                drop(inventory);
                if let Some(p) = profile {
                    p.append_inventory_residence_ns += residence.map_or(0, elapsed_ns);
                }
                result
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
