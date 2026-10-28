//! Independent committed reads and one bounded, pipelined write owner.
//!
//! eRPC §§3.1–3.2 and `ReFlex` §§3–4 separate admission from completion; the
//! owner never waits for a single append before accepting another batch. Like
//! `FoundationDB` §§2.3–2.6, read cuts and durable ordered publication remain
//! separate from scheduling. This RF1 path still requires synced WAL receipts.
#[allow(clippy::wildcard_imports)]
// Private implementation shares its parent's internal types.
use super::*;
use parking_lot::Condvar;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

pub(super) struct FailureOnPanic(pub(super) Arc<AtomicBool>);
impl Drop for FailureOnPanic {
    fn drop(&mut self) {
        if thread::panicking() {
            self.0.store(true, Ordering::Release);
        }
    }
}

#[derive(Default)]
struct WriteBudget {
    requests: usize,
    bytes: usize,
    peak_requests: usize,
    peak_bytes: usize,
    closed: bool,
}
struct Admission {
    budget: Mutex<WriteBudget>,
    changed: Condvar,
    max_requests: usize,
    max_bytes: usize,
}
struct WriteCredit {
    admission: Arc<Admission>,
    bytes: usize,
}
impl Drop for WriteCredit {
    fn drop(&mut self) {
        let mut budget = self.admission.budget.lock();
        budget.requests -= 1;
        budget.bytes -= self.bytes;
        self.admission.changed.notify_all();
    }
}

/// The reply owns admission credit until the owner has resolved it. Dropping a
/// request during unwinding is an error acknowledgement, never silent success.
struct WriteReply {
    sender: Option<Reply<BatchVersion>>,
    version: Option<BatchVersion>,
    _credit: WriteCredit,
}
impl WriteReply {
    fn finish(mut self, result: Result<BatchVersion, LocalDbError>) {
        let _ = self.sender.take().unwrap().send(result);
    }
}
impl Drop for WriteReply {
    fn drop(&mut self) {
        if let Some(sender) = self.sender.take() {
            let error = match self.version {
                Some(version) => {
                    LocalDbError::unknown(version, "write authority stopped before acknowledgement")
                }
                None => LocalDbError::new("write authority stopped before admission"),
            };
            let _ = sender.send(Err(error));
        }
    }
}

enum OwnerCommand {
    Write {
        operation: [u8; 16],
        mutations: Vec<Mutation>,
        reply: WriteReply,
    },
    Flush(Reply<()>),
    ArmProfile {
        limit: usize,
        reply: Reply<()>,
    },
    FinishProfile(Reply<()>),
    Retire {
        floor: IndexReadCut,
        reply: Reply<()>,
    },
}

#[derive(Clone, Copy, Default)]
struct Snapshot {
    stats: LocalDbStats,
    pipeline: PipelineStats,
    authority: AuthorityWriteStats,
    metadata_history_bytes: usize,
    checkpoint_diagnostics: CheckpointDiagnosticSummary,
}

/// Consumes one opened authority. Reads never borrow or lock its write owner.
/// A write acknowledgement means synced complete-batch storage AND atomic
/// publication after its native-index predecessors, not a global storage prefix.
pub struct ConcurrentRemoteDatabase {
    sender: Option<mpsc::SyncSender<OwnerCommand>>,
    wake: mpsc::SyncSender<()>,
    thread: Option<JoinHandle<Result<(), LocalDbError>>>,
    admission: Arc<Admission>,
    snapshot: Arc<Mutex<Snapshot>>,
    pub(super) failure: Arc<AtomicBool>,
    pub(super) index: Arc<RwLock<NativeIndex>>,
    pub(super) reads: Arc<ReadService>,
    pub(super) cache: Arc<Mutex<ReadCache>>,
    write_profile: Arc<Mutex<WriteProfileSnapshot>>,
    pub(super) shape: KeyShape,
    pub(super) config: RemoteDatabaseConfig,
    pub(super) epoch: u64,
    pub(super) partition_indexes: HashMap<u32, usize>,
    pub(super) read_resolve_ns: AtomicU64,
    pub(super) value_read_ns: AtomicU64,
    io_clients: Vec<(u32, Arc<Mutex<Client>>)>,
    operational_monitor: OperationalReadIoMonitor,
    operational_lifecycle: std::sync::Weak<Mutex<OperationalReadIoLifecycle>>,
    #[cfg(test)]
    progress: Arc<Mutex<Option<mpsc::SyncSender<PipelineStats>>>>,
}

impl ConcurrentRemoteDatabase {
    /// Transfer a drained authority into the bounded serving owner.
    ///
    /// # Errors
    /// Rejects an unhealthy authority, failed drain, or failed thread creation.
    pub fn new(mut db: RemoteDatabase) -> Result<Self, LocalDbError> {
        db.ensure_healthy()?;
        // Transfer only a committed baseline into the new bounded admission
        // domain. This construction barrier also resolves any older submissions
        // made through the single-owner API before it was consumed.
        db.drain()?;
        db.config.pipelined_writes = true;
        let admission = Arc::new(Admission {
            budget: Mutex::new(WriteBudget::default()),
            changed: Condvar::new(),
            max_requests: db.config.max_pending_batches,
            max_bytes: db.config.max_pending_bytes,
        });
        let snapshot = Arc::new(Mutex::new(Snapshot {
            stats: db.stats(),
            pipeline: db.pipeline_stats(),
            authority: db.authority_write_stats(),
            metadata_history_bytes: db.metadata_history_bytes(),
            checkpoint_diagnostics: db.checkpoint_diagnostic_summary(),
        }));
        let (sender, receiver) = mpsc::sync_channel(db.config.max_pending_batches);
        let mut owner = Self {
            sender: Some(sender),
            wake: db.preparation_wake.clone(),
            thread: None,
            admission: admission.clone(),
            snapshot: snapshot.clone(),
            failure: db.concurrent_failure.clone(),
            index: db.index.clone(),
            reads: db.reads.clone(),
            cache: db.read_cache.clone(),
            write_profile: db.write_profile.clone(),
            shape: db.shape.clone(),
            config: db.config.clone(),
            epoch: db.epoch,
            partition_indexes: db.partition_indexes.clone(),
            read_resolve_ns: AtomicU64::new(0),
            value_read_ns: AtomicU64::new(0),
            io_clients: db.read_evidence_monitor().clients,
            operational_monitor: db.operational_sampler.monitor(),
            operational_lifecycle: db.operational_sampler.downgrade_lifecycle(),
            #[cfg(test)]
            progress: db.concurrent_progress.clone(),
        };
        owner.thread = Some(
            thread::Builder::new()
                .name("wal-write-owner".into())
                .spawn(move || {
                    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        drive_owner(&mut db, receiver, &admission, &snapshot)
                    }));
                    {
                        let mut budget = admission.budget.lock();
                        budget.closed = true;
                        admission.changed.notify_all();
                    }
                    let result = match outcome {
                        Ok(result) => result,
                        Err(_) => Err(LocalDbError::new(
                            "write authority panicked; recovery required",
                        )),
                    };
                    if result.is_err() {
                        db.concurrent_failure.store(true, Ordering::Release);
                        db.reads.poison();
                        db.healthy = false;
                    }
                    let shutdown = db.shutdown();
                    result.and(shutdown)
                })
                .map_err(LocalDbError::component)?,
        );
        Ok(owner)
    }

    pub(super) fn ensure_healthy(&self) -> Result<(), LocalDbError> {
        if self.failure.load(Ordering::Acquire) || self.reads.failed() {
            return Err(LocalDbError::new(
                "TCP authority requires recovery or is closed",
            ));
        }
        Ok(())
    }

    pub(super) fn poison(&self) {
        self.failure.store(true, Ordering::Release);
        self.reads.poison();
        let _budget = self.admission.budget.lock();
        self.admission.changed.notify_all();
        let _ = self.wake.try_send(());
    }

    /// # Errors
    /// Rejects reads after an observed authority, transport, or shutdown failure.
    pub fn validate_cached_read(&self) -> Result<(), LocalDbError> {
        self.ensure_healthy()
    }

    /// # Errors
    /// Rejects an unhealthy authority before or after capturing its committed cut.
    pub fn published_cut(&self) -> Result<IndexReadCut, LocalDbError> {
        self.ensure_healthy()?;
        let cut = self.index.read().latest_cut();
        self.ensure_healthy()?;
        Ok(cut)
    }

    /// Capture and protect one published cut without a retention race.
    pub fn protected_published_cut(
        &self,
    ) -> Result<(IndexReadCut, super::super::native_index::IndexReadPin), LocalDbError> {
        self.ensure_healthy()?;
        let index = self.index.read();
        let cut = index.latest_cut();
        let pin = index
            .pin_cut(cut.clone())
            .map_err(LocalDbError::component)?;
        self.ensure_healthy()?;
        Ok((cut, pin))
    }

    pub fn pin_cut(
        &self,
        cut: IndexReadCut,
    ) -> Result<super::super::native_index::IndexReadPin, LocalDbError> {
        self.ensure_healthy()?;
        let pin = self
            .index
            .read()
            .pin_cut(cut)
            .map_err(LocalDbError::component)?;
        self.ensure_healthy()?;
        Ok(pin)
    }

    pub fn retention_floor(&self) -> IndexReadCut {
        self.index.read().retention_floor()
    }

    pub fn index_stats(&self) -> super::super::native_index::NativeIndexStats {
        self.index.read().stats()
    }

    pub fn index_history_bytes(&self) -> usize {
        self.index.read().retained_metadata_bytes()
    }

    pub fn metadata_history_bytes(&self) -> usize {
        self.snapshot.lock().metadata_history_bytes
    }

    /// # Errors
    /// Returns key/schema validation errors or an observed authority failure.
    pub fn index_reference(
        &self,
        ks: KeySpace,
        key: &[u8],
        cut: IndexReadCut,
    ) -> Result<Option<ValueRecordReference>, LocalDbError> {
        self.ensure_healthy()?;
        let started = Instant::now();
        let result = self
            .index
            .read()
            .get(ks, key, cut)
            .map_err(LocalDbError::component);
        self.read_resolve_ns
            .fetch_add(elapsed(started), Ordering::Relaxed);
        self.ensure_healthy()?;
        result
    }

    /// # Errors
    /// Returns index-bound validation errors or an observed authority failure.
    pub fn index_page(
        &self,
        ks: KeySpace,
        after_reduced: Option<&[u8]>,
        reverse: bool,
        cut: IndexReadCut,
    ) -> Result<Vec<(Bytes, ValueRecordReference)>, LocalDbError> {
        self.ensure_healthy()?;
        let result = self
            .index
            .read()
            .scan_reduced(ks, after_reduced, reverse, 256, cut)
            .map_err(LocalDbError::component);
        self.ensure_healthy()?;
        result
    }

    pub(crate) fn relocation_history(
        &self,
        ks: KeySpace,
        cut: IndexReadCut,
    ) -> Result<super::super::native_index::NativeIndexHistory, LocalDbError> {
        self.ensure_healthy()?;
        self.index
            .read()
            .relocation_history(ks, cut)
            .map_err(LocalDbError::component)
    }

    pub(crate) fn next_relocation_record(
        &self,
        history: &mut super::super::native_index::NativeIndexHistory,
    ) -> Result<Option<(Bytes, ValueRecordReference)>, LocalDbError> {
        self.ensure_healthy()?;
        Ok(history.next(&self.index.read()))
    }

    /// Submit one complete batch and wait for synced, ordered publication.
    ///
    /// # Errors
    /// Returns validation, admission-budget, storage, publication, or owner errors.
    /// An uncertain version requires recovery, not an assumed failed append.
    pub fn write_batch(
        &self,
        operation: [u8; 16],
        mutations: Vec<Mutation>,
    ) -> Result<BatchVersion, LocalDbError> {
        self.ensure_healthy()?;
        let validated = CompleteBatch::validated_mutations(
            &self.shape,
            &mutations,
            self.config.storage.batch_limits,
        )
        .map_err(LocalDbError::component)?;
        // Include canonical encoding, detached input, routing, both actor and
        // authority metadata and per-request/reply ownership. Credit spans queue
        // residence through publication, not merely the channel send.
        let bytes = validated
            .encoded_len()
            .checked_mul(2)
            .and_then(|bytes| bytes.checked_add(validated.routing_heap_bound()))
            .and_then(|bytes| {
                bytes.checked_add(mutations.len().checked_mul(
                    2 * (std::mem::size_of::<Mutation>()
                        + std::mem::size_of::<super::super::types::RecordDescriptor>())
                        + std::mem::size_of::<crate::batch::PendingOp>(),
                )?)
            })
            .and_then(|bytes| bytes.checked_add(2048))
            .ok_or_else(|| LocalDbError::new("write admission byte overflow"))?;
        let credit = self.reserve(bytes)?;
        // Bytes may be tiny views of arbitrarily large caller allocations.
        // Detach exactly the admitted contents before retaining them in a queue.
        let mutations = mutations
            .into_iter()
            .map(|mutation| match mutation {
                Mutation::Put {
                    keyspace,
                    key,
                    value,
                } => Mutation::Put {
                    keyspace,
                    key: key.to_vec().into(),
                    value: value.to_vec().into(),
                },
                Mutation::Delete { keyspace, key } => Mutation::Delete {
                    keyspace,
                    key: key.to_vec().into(),
                },
            })
            .collect();
        let (sender, receiver) = mpsc::sync_channel(1);
        let reply = WriteReply {
            sender: Some(sender),
            version: None,
            _credit: credit,
        };
        self.sender
            .as_ref()
            .ok_or_else(|| LocalDbError::new("write authority closed"))?
            .send(OwnerCommand::Write {
                operation,
                mutations,
                reply,
            })
            .map_err(|_| LocalDbError::new("write authority stopped"))?;
        let _ = self.wake.try_send(());
        let version = receive(receiver)?;
        self.ensure_healthy()?;
        Ok(version)
    }

    fn reserve(&self, bytes: usize) -> Result<WriteCredit, LocalDbError> {
        if bytes > self.admission.max_bytes {
            return Err(LocalDbError::new("write exceeds admission byte budget"));
        }
        let mut budget = self.admission.budget.lock();
        loop {
            self.ensure_healthy()?;
            if budget.closed {
                return Err(LocalDbError::new("write authority closed"));
            }
            if budget.requests < self.admission.max_requests
                && bytes <= self.admission.max_bytes - budget.bytes
            {
                break;
            }
            self.admission.changed.wait(&mut budget);
        }
        budget.requests += 1;
        budget.bytes += bytes;
        budget.peak_requests = budget.peak_requests.max(budget.requests);
        budget.peak_bytes = budget.peak_bytes.max(budget.bytes);
        Ok(WriteCredit {
            admission: self.admission.clone(),
            bytes,
        })
    }

    /// # Errors
    /// Returns authority, owner, or checkpoint persistence errors.
    pub fn flush_index(&self) -> Result<(), LocalDbError> {
        self.ensure_healthy()?;
        let (sender, receiver) = mpsc::sync_channel(1);
        self.sender
            .as_ref()
            .ok_or_else(|| LocalDbError::new("write authority closed"))?
            .send(OwnerCommand::Flush(sender))
            .map_err(|_| LocalDbError::new("write authority stopped"))?;
        let _ = self.wake.try_send(());
        receive(receiver)?;
        self.ensure_healthy()
    }

    /// Persist a Stop-aware floor and reclaim wholly unreachable sealed files.
    pub(crate) fn retire_history_before(&self, floor: IndexReadCut) -> Result<(), LocalDbError> {
        self.ensure_healthy()?;
        let (reply, receiver) = mpsc::sync_channel(1);
        self.sender
            .as_ref()
            .ok_or_else(|| LocalDbError::new("write authority closed"))?
            .send(OwnerCommand::Retire { floor, reply })
            .map_err(|_| LocalDbError::new("write authority stopped"))?;
        let _ = self.wake.try_send(());
        receive(receiver)?;
        self.ensure_healthy()
    }

    pub fn read_cache_stats(&self) -> ReadCacheStats {
        self.cache.lock().stats()
    }
    pub fn per_keyspace_read_cache_stats(
        &self,
    ) -> [PerKeyspaceReadCacheStats; KEYSPACE_COUNTER_COUNT] {
        self.cache.lock().per_keyspace_stats()
    }
    pub fn stats(&self) -> LocalDbStats {
        let mut stats = self.snapshot.lock().stats;
        stats.read_resolve_ns += self.read_resolve_ns.load(Ordering::Relaxed);
        stats.read_resolve_ns_available = self.read_profile_enabled();
        stats.value_read_ns += self.value_read_ns.load(Ordering::Relaxed);
        stats
    }
    pub fn pipeline_stats(&self) -> PipelineStats {
        let mut stats = self.snapshot.lock().pipeline;
        let budget = self.admission.budget.lock();
        stats.concurrent_queued_peak = budget.peak_requests;
        stats.concurrent_queue_bytes_peak = budget.peak_bytes;
        stats
    }

    /// Visit one local-only snapshot of write owner, checkpoint, and cache metrics.
    /// This performs no diagnostic RPC and does not enable sampled profiling.
    pub fn visit_write_metrics(&self, visit: impl FnOnce(WriteMetricsSnapshot)) {
        let snapshot = *self.snapshot.lock();
        visit(WriteMetricsSnapshot {
            authority: snapshot.authority,
            pipeline: self.pipeline_stats(),
            read_cache: self.cache.lock().stats(),
            write_profile: *self.write_profile.lock(),
            checkpoint_diagnostics: snapshot.checkpoint_diagnostics,
        });
    }
    /// Bounded metrics snapshot; lock contention makes the whole snapshot unavailable.
    pub fn try_visit_write_metrics(&self, visit: impl FnOnce(WriteMetricsSnapshot)) -> Option<()> {
        let snapshot = {
            let stored = self.snapshot.try_lock()?;
            let budget = self.admission.budget.try_lock()?;
            let cache = self.cache.try_lock()?;
            let write_profile = self.write_profile.try_lock()?;
            let mut pipeline = stored.pipeline;
            pipeline.concurrent_queued_peak = budget.peak_requests;
            pipeline.concurrent_queue_bytes_peak = budget.peak_bytes;
            WriteMetricsSnapshot {
                authority: stored.authority,
                pipeline,
                read_cache: cache.stats(),
                write_profile: *write_profile,
                checkpoint_diagnostics: stored.checkpoint_diagnostics,
            }
        };
        visit(snapshot);
        Some(())
    }
    pub fn try_index_metrics(
        &self,
    ) -> Option<(
        super::super::native_index::NativeIndexStats,
        usize,
        IndexReadCut,
    )> {
        let index = self.index.try_read()?;
        Some((
            index.stats(),
            index.retained_metadata_bytes(),
            index.retention_floor(),
        ))
    }
    pub fn try_metadata_history_bytes(&self) -> Option<usize> {
        Some(self.snapshot.try_lock()?.metadata_history_bytes)
    }
    pub fn try_read_cache_stats(&self) -> Option<ReadCacheStats> {
        Some(self.cache.try_lock()?.stats())
    }
    pub fn try_stats(&self) -> Option<LocalDbStats> {
        let mut stats = self.snapshot.try_lock()?.stats;
        stats.read_resolve_ns += self.read_resolve_ns.load(Ordering::Relaxed);
        stats.read_resolve_ns_available = self.read_profile_enabled();
        stats.value_read_ns += self.value_read_ns.load(Ordering::Relaxed);
        Some(stats)
    }
    pub fn try_per_keyspace_read_cache_stats(
        &self,
    ) -> Option<[PerKeyspaceReadCacheStats; KEYSPACE_COUNTER_COUNT]> {
        Some(self.cache.try_lock()?.per_keyspace_stats())
    }
    pub fn read_evidence_monitor(&self) -> ReadEvidenceMonitor {
        ReadEvidenceMonitor {
            clients: self.io_clients.clone(),
            reads: self.reads.clone(),
        }
    }
    /// Passive snapshots do not keep the write owner or its connections alive.
    pub fn operational_read_io_monitor(&self) -> OperationalReadIoMonitor {
        if let Some(lifecycle) = self.operational_lifecycle.upgrade() {
            OperationalReadIoSampler::activate_lifecycle(&lifecycle, &self.reads);
        }
        self.operational_monitor.clone()
    }
    pub fn read_admission_generation(&self) -> u64 {
        self.reads.admission_generation()
    }
    /// # Errors
    /// Returns an absolute admission deadline, failure, or closed-owner error.
    pub fn wait_read_admission(
        &self,
        observed: u64,
        deadline: Instant,
    ) -> Result<(), LocalDbError> {
        self.ensure_healthy()?;
        self.reads.wait_admission(observed, deadline)?;
        self.ensure_healthy()
    }
    /// # Errors
    /// Returns write-owner, transport, or shutdown failures after joining progress.
    pub fn shutdown(mut self) -> Result<(), LocalDbError> {
        self.stop()
    }
    /// Arm a fresh bounded profile after warm-up has drained.
    pub fn arm_write_profile(&self, limit: usize) -> Result<(), LocalDbError> {
        self.ensure_healthy()?;
        if limit > 65_536 {
            return Err(LocalDbError::new("write profile limit exceeds 65536"));
        }
        *self.write_profile.lock() = WriteProfileSnapshot {
            limit_per_connection: limit,
            expected_connections: self.io_clients.len(),
            ..WriteProfileSnapshot::default()
        };
        let (sender, receiver) = mpsc::sync_channel(1);
        self.sender
            .as_ref()
            .ok_or_else(|| LocalDbError::new("write authority closed"))?
            .send(OwnerCommand::ArmProfile {
                limit,
                reply: sender,
            })
            .map_err(|_| LocalDbError::new("write authority stopped"))?;
        let _ = self.wake.try_send(());
        receive(receiver)?;
        self.ensure_healthy()
    }
    /// Drain admitted writes and finalize any partial opt-in storage write profiles.
    /// This is the only call apart from the configured sample boundary that may
    /// perform profile-control RPCs; the metrics visitor remains local-only.
    pub fn finish_write_profile(&self) -> Result<(), LocalDbError> {
        self.ensure_healthy()?;
        if self.write_profile.lock().limit_per_connection == 0 {
            return Ok(());
        }
        let (sender, receiver) = mpsc::sync_channel(1);
        self.sender
            .as_ref()
            .ok_or_else(|| LocalDbError::new("write authority closed"))?
            .send(OwnerCommand::FinishProfile(sender))
            .map_err(|_| LocalDbError::new("write authority stopped"))?;
        let _ = self.wake.try_send(());
        receive(receiver)?;
        self.ensure_healthy()
    }
    fn stop(&mut self) -> Result<(), LocalDbError> {
        self.sender.take();
        let _ = self.wake.try_send(());
        let result = self.thread.take().map_or(Ok(()), |thread| {
            thread
                .join()
                .unwrap_or_else(|_| Err(LocalDbError::new("write authority panicked")))
        });
        self.failure.store(true, Ordering::Release);
        result
    }
}
impl Drop for ConcurrentRemoteDatabase {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

#[allow(clippy::needless_pass_by_value)] // Receiver ownership resolves queued replies on every exit.
fn refresh_snapshot(db: &RemoteDatabase, snapshot: &Mutex<Snapshot>) {
    *snapshot.lock() = Snapshot {
        stats: db.stats(),
        pipeline: db.pipeline_stats(),
        authority: db.authority_write_stats(),
        metadata_history_bytes: db.metadata_history_bytes(),
        checkpoint_diagnostics: db.checkpoint_diagnostic_summary(),
    };
}

fn drive_owner(
    db: &mut RemoteDatabase,
    receiver: mpsc::Receiver<OwnerCommand>,
    admission: &Admission,
    snapshot: &Mutex<Snapshot>,
) -> Result<(), LocalDbError> {
    let mut pending: VecDeque<WriteReply> = VecDeque::new();
    let mut barrier: Option<Reply<()>> = None;
    let mut disconnected = false;
    let mut finish_profiles = false;
    let mut retirement: Option<IndexReadCut> = None;
    let mut arm_profile_limit = None;
    // Declared after pending replies: unwind fences readers before resolving
    // those acknowledgements and releasing their admission credits.
    let _poison = FailureOnPanic(db.concurrent_failure.clone());
    // This serving loop intentionally lives until all senders close or a fatal
    // error occurs. Each pass admits at most the configured request bound.
    loop {
        db.ensure_healthy()?;
        // Harvest completions first, without dispatching short tails. Drain only
        // commands already ready below, then send the resulting groups at once.
        // There is no timer or wait for a second request: singletons stay prompt.
        db.progress_pipeline(false)?;
        for _ in 0..pending.len() {
            let reply = pending.pop_front().unwrap();
            let version = reply.version.unwrap();
            if version.epoch < db.epoch || db.publication.is_published(version) {
                db.ensure_healthy()?;
                db.pipeline_stats.concurrent_completed += 1;
                if db.config.write_profile_limit != 0 {
                    // Diagnostic snapshots must be visible before client completion.
                    refresh_snapshot(db, snapshot);
                }
                reply.finish(Ok(version));
            } else {
                pending.push_back(reply);
            }
        }
        if barrier.is_some() && db.submissions.is_empty() {
            let finishing_profiles = finish_profiles;
            let arming_profile = arm_profile_limit.take();
            let result = if let Some(limit) = arming_profile {
                db.arm_write_profile(limit)
            } else if finishing_profiles {
                db.finish_write_profile()
            } else if let Some(floor) = retirement.take() {
                db.retire_history_before(floor)
            } else {
                db.commit_checkpoint()
            };
            finish_profiles = false;
            let failed = result.is_err();
            refresh_snapshot(db, snapshot);
            let _ = barrier.take().unwrap().send(result);
            if failed && !finishing_profiles {
                return Err(LocalDbError::new("checkpoint failed; recovery required"));
            }
        }
        refresh_snapshot(db, snapshot);
        #[cfg(test)]
        if let Some(events) = db.concurrent_progress.lock().as_ref() {
            let _ = events.try_send(db.pipeline_stats());
        }
        if disconnected && db.submissions.is_empty() {
            return Ok(());
        }
        let mut admitted = false;
        if barrier.is_none() && !disconnected {
            for _ in 0..admission.max_requests {
                match receiver.try_recv() {
                    Ok(OwnerCommand::Write {
                        operation,
                        mutations,
                        mut reply,
                    }) => {
                        match db.submit(operation, mutations) {
                            Ok(version) => {
                                reply.version = Some(version);
                                pending.push_back(reply);
                                db.pipeline_stats.concurrent_admitted += 1;
                                db.pipeline_stats.concurrent_pending_peak =
                                    db.pipeline_stats.concurrent_pending_peak.max(pending.len());
                                #[cfg(test)]
                                if db.panic_after_concurrent_admissions
                                    == Some(db.pipeline_stats.concurrent_admitted)
                                {
                                    panic!("injected concurrent authority panic after admission");
                                }
                            }
                            Err(error) => {
                                let poisoned = !db.healthy || db.ensure_healthy().is_err();
                                if poisoned {
                                    db.concurrent_failure.store(true, Ordering::Release);
                                    db.reads.poison();
                                }
                                reply.finish(Err(error));
                                if poisoned {
                                    return Err(LocalDbError::new(
                                        "write failed; recovery required",
                                    ));
                                }
                            }
                        }
                        admitted = true;
                    }
                    Ok(OwnerCommand::Flush(reply)) => {
                        barrier = Some(reply);
                        admitted = true;
                        break;
                    }
                    Ok(OwnerCommand::ArmProfile { limit, reply }) => {
                        barrier = Some(reply);
                        arm_profile_limit = Some(limit);
                        admitted = true;
                        break;
                    }
                    Ok(OwnerCommand::FinishProfile(reply)) => {
                        barrier = Some(reply);
                        finish_profiles = true;
                        admitted = true;
                        break;
                    }
                    Ok(OwnerCommand::Retire { floor, reply }) => {
                        barrier = Some(reply);
                        retirement = Some(floor);
                        admitted = true;
                        break;
                    }
                    Err(mpsc::TryRecvError::Empty) => break,
                    Err(mpsc::TryRecvError::Disconnected) => {
                        disconnected = true;
                        break;
                    }
                }
            }
        }
        db.progress_pipeline(true)?;
        if !admitted {
            // Notifications share the append completion channel, so either new
            // input or ANY node completion wakes the owner. The timeout covers
            // a panicked worker whose disconnected reply has no wake hint.
            let started = Instant::now();
            let _ = db.completion_wake.recv_timeout(Duration::from_millis(100));
            if !pending.is_empty() {
                db.stats.storage_wait_ns += elapsed(started);
            }
        }
    }
}

#[cfg(test)]
mod tests;
