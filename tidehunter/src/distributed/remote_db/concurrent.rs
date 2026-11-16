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

/// One authority thread writes these counters. Relaxed atomics let metric
/// visitors sample them without a new lock; they publish no write authority.
#[derive(Default)]
#[repr(align(64))]
pub(super) struct AuthorityCounters {
    prepare_ns: AtomicU64,
    admit_ns: AtomicU64,
    windows: AtomicU64,
    batches: AtomicU64,
}

/// Exclusive wall time: nested preparation/admission spans subtract from their
/// parent, so pool-join progress and stale re-preparation are counted only once.
pub(super) struct AuthoritySpan<'a> {
    counters: &'a AuthorityCounters,
    target: &'a AtomicU64,
    started: Instant,
    prepare_before: u64,
    admit_before: u64,
}

impl AuthorityCounters {
    fn span<'a>(&'a self, target: &'a AtomicU64) -> AuthoritySpan<'a> {
        AuthoritySpan {
            counters: self,
            target,
            started: Instant::now(),
            // Relaxed: this authority thread is the sole counter writer.
            prepare_before: self.prepare_ns.load(Ordering::Relaxed),
            admit_before: self.admit_ns.load(Ordering::Relaxed),
        }
    }
    pub(super) fn preparing(&self) -> AuthoritySpan<'_> {
        self.span(&self.prepare_ns)
    }
    pub(super) fn admitting(&self) -> AuthoritySpan<'_> {
        self.span(&self.admit_ns)
    }
    fn record_window(&self, batches: usize) {
        // Relaxed: independent workload counts, not admission publication.
        self.windows.fetch_add(1, Ordering::Relaxed);
        self.batches.fetch_add(batches as u64, Ordering::Relaxed);
    }
    fn snapshot(&self, mut stats: AuthorityWriteStats) -> AuthorityWriteStats {
        // Relaxed: weakly consistent diagnostic sums; sample after quiescence.
        stats.authority_prepare_ns = self.prepare_ns.load(Ordering::Relaxed);
        stats.authority_admit_ns = self.admit_ns.load(Ordering::Relaxed);
        stats.authority_windows = self.windows.load(Ordering::Relaxed);
        stats.authority_batches = self.batches.load(Ordering::Relaxed);
        stats
    }
}

impl Drop for AuthoritySpan<'_> {
    fn drop(&mut self) {
        // Relaxed: only the authority thread records nested spans.
        let nested = self
            .counters
            .prepare_ns
            .load(Ordering::Relaxed)
            .saturating_sub(self.prepare_before)
            .saturating_add(
                self.counters
                    .admit_ns
                    .load(Ordering::Relaxed)
                    .saturating_sub(self.admit_before),
            );
        self.target.fetch_add(
            elapsed(self.started).saturating_sub(nested),
            Ordering::Relaxed,
        );
    }
}

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
    first_failure: Arc<std::sync::OnceLock<String>>,
    authority: AuthorityCounters,
    timeline: Arc<WriteTimeline>,
}
struct WriteCredit {
    admission: Arc<Admission>,
    bytes: usize,
}
impl Drop for WriteCredit {
    fn drop(&mut self) {
        let started = self.admission.timeline.block_started();
        let mut budget = self.admission.budget.lock();
        self.admission.timeline.block_finished(
            super::super::write_timeline::WriteBlockStage::CreditReleaseLock,
            started,
            budget.requests,
        );
        budget.requests -= 1;
        budget.bytes -= self.bytes;
        let started = self.admission.timeline.block_started();
        self.admission.changed.notify_all();
        self.admission.timeline.block_finished(
            super::super::write_timeline::WriteBlockStage::CreditNotify,
            started,
            budget.requests,
        );
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
            let message = failure_context(
                &self._credit.admission.first_failure,
                match self.version {
                    Some(_) => "write authority stopped before acknowledgement",
                    None => "write authority stopped before admission",
                },
            );
            let error = match self.version {
                Some(version) => LocalDbError::unknown(version, message),
                None => LocalDbError::new(message),
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

struct DeferredWrite {
    prepared: PreparedAdmission,
    reply: WriteReply,
    since: Option<Instant>,
    drained_generation: Option<u64>,
    fresh_generation: Option<u64>,
}

type QueuedWrite = ([u8; 16], Vec<Mutation>, WriteReply);

fn prepare_window(
    db: &mut RemoteDatabase,
    writes: &[QueuedWrite],
    counters: &AuthorityCounters,
) -> Result<Vec<Result<PreparedAdmission, LocalDbError>>, LocalDbError> {
    counters.record_window(writes.len());
    let _admit = counters.admitting();
    let started = Instant::now();
    let before = db.authority_write_stats();
    let mut planned = HashMap::with_capacity(writes.len());
    let mut next = db.submission_next();
    let mut encoded_bytes = 0usize;
    let mut plans = Vec::with_capacity(writes.len());
    let mut slots = Vec::with_capacity(writes.len());
    let mut results: Vec<_> = (0..writes.len()).map(|_| None).collect();
    for (index, (operation, mutations, _)) in writes.iter().enumerate() {
        let plan = (|| {
            let id = BatchId {
                database: db.config.storage.database,
                operation: *operation,
            };
            let existing = db.existing_identity(id)?;
            let validated = CompleteBatch::validated_mutations(
                &db.shape,
                mutations,
                db.config.storage.batch_limits,
            )
            .map_err(LocalDbError::component)?;
            let version = if let Some((version, _)) = existing {
                version
            } else if let Some(version) = planned.get(&id) {
                *version
            } else {
                let version = BatchVersion {
                    epoch: db.epoch,
                    sequence: next,
                };
                next = next
                    .checked_add(1)
                    .ok_or_else(|| LocalDbError::new("version overflow"))?;
                planned.insert(id, version);
                version
            };
            encoded_bytes = encoded_bytes.saturating_add(validated.encoded_len());
            Ok((id, version, validated))
        })();
        match plan {
            Ok(plan) => {
                slots.push(index);
                plans.push(plan);
            }
            Err(error) => results[index] = Some(Err(error)),
        }
    }
    let prepared = db.encode_preparation_plans(
        &plans,
        encoded_bytes >= super::preparation::PARALLEL_PREPARATION_MIN_BYTES,
        Some(counters),
    )?;
    for (index, result) in slots.into_iter().zip(prepared) {
        results[index] = Some(result.and_then(|(batch, _)| {
            let prepared = db.prepare_admission(batch)?;
            let batch = &prepared.batch;
            db.write_timeline
                .batch(batch.id().operation, batch.version());
            // The timeline has no timestamp insertion API. Prepared is recorded
            // on the owner after join; prepare_task_ns sums per-put encode spans.
            db.write_timeline.batch_event(
                batch.version(),
                WriteTimelineStage::Prepared,
                None,
                None,
            );
            Ok(prepared)
        }));
    }
    let after = db.authority_write_stats();
    let nested = (after.storage_wait_ns - before.storage_wait_ns)
        .saturating_add(after.publication_ns - before.publication_ns)
        .saturating_add(after.dispatch_ns - before.dispatch_ns);
    db.stats.validation_ns += elapsed(started).saturating_sub(nested);
    db.record_admission_span(started, before);
    Ok(results.into_iter().map(Option::unwrap).collect())
}

fn reject_preparation(
    db: &RemoteDatabase,
    reply: WriteReply,
    error: LocalDbError,
) -> Result<(), LocalDbError> {
    let poisoned = !db.healthy || db.ensure_healthy().is_err();
    if poisoned {
        record_first_failure(
            &db.first_failure,
            None,
            error.uncertain_version,
            "prepare_submit",
            &error,
        );
    }
    reply.finish(Err(error));
    if poisoned {
        return Err(LocalDbError::new("write failed; recovery required"));
    }
    Ok(())
}

#[derive(Clone, Copy, Default)]
struct Snapshot {
    stats: LocalDbStats,
    pipeline: PipelineStats,
    authority: AuthorityWriteStats,
    metadata_history_bytes: usize,
    checkpoint_diagnostics: CheckpointDiagnosticSummary,
}

/// Setup evidence from one owner-monitor connection.
#[cfg(feature = "experimental-fabric")]
pub struct FabricProviderEvidence {
    pub partition: u32,
    pub provider: Option<tidehunter_fabric::ProviderInfo>,
    pub open_profile: Option<tidehunter_fabric::OpenProfile>,
}

#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
type LiveTargetReports = Vec<(u32, super::super::nvmeof::LiveTargetStats)>;

/// Evidence collected only after the final sealed authority and initiator drain.
#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
pub struct LiveFinalShutdownEvidence {
    pub metrics: super::super::nvmeof::live_metrics::LiveNvmeStats,
    pub targets: LiveTargetReports,
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
    first_failure: Arc<std::sync::OnceLock<String>>,
    pub(super) index: Arc<NativeIndex>,
    pub(super) reads: Arc<ReadService>,
    pub(super) cache: Arc<ReadCache>,
    write_profile: Arc<Mutex<WriteProfileSnapshot>>,
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    live_metrics: Option<Arc<super::super::nvmeof::live_metrics::LiveNvmeMetrics>>,
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    live_final_requested: Arc<AtomicBool>,
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    live_final_targets: Arc<Mutex<Option<LiveTargetReports>>>,
    pub(crate) write_timeline: Arc<WriteTimeline>,
    pub(super) shape: KeyShape,
    pub(super) config: RemoteDatabaseConfig,
    pub(super) epoch: u64,
    pub(super) partition_indexes: HashMap<u32, usize>,
    pub(super) read_resolve_ns: AtomicU64,
    pub(super) value_read_ns: AtomicU64,
    io_clients: Vec<(u32, ControlHandle)>,
    operational_monitor: OperationalReadIoMonitor,
    operational_lifecycle: std::sync::Weak<Mutex<OperationalReadIoLifecycle>>,
    #[cfg(test)]
    progress: Arc<Mutex<Option<mpsc::SyncSender<PipelineStats>>>>,
}

fn owner_timeline_error(
    owner: u32,
    phase: &'static str,
    error: LocalDbError,
) -> super::super::write_timeline::OwnerTimelineError {
    let mut message = error.to_string();
    let truncated = message.len() > 128;
    if truncated {
        let mut end = 128;
        while !message.is_char_boundary(end) {
            end -= 1;
        }
        message.truncate(end);
    }
    super::super::write_timeline::OwnerTimelineError {
        owner,
        phase,
        message,
        truncated,
    }
}

impl ConcurrentRemoteDatabase {
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    pub fn live_nvmeof_stats(&self) -> Option<super::super::nvmeof::live_metrics::LiveNvmeStats> {
        self.live_metrics.as_ref().map(|metrics| metrics.snapshot())
    }

    pub(crate) fn arm_write_timeline(
        &self,
        limit: usize,
        sample_every: usize,
    ) -> Result<(), LocalDbError> {
        self.write_timeline
            .arm(limit, sample_every)
            .map_err(LocalDbError::new)?;
        if limit == 0 {
            return Ok(());
        }
        for (_, client) in &self.io_clients {
            if let Err(error) = client.owner_timeline_arm(limit) {
                let mut failures = Vec::new();
                for (partition, client) in &self.io_clients {
                    if let Err(cleanup) = client.control(|client| client.owner_timeline_arm(0)) {
                        failures.push(format!("owner {partition}: {cleanup}"));
                    }
                }
                let _ = self.write_timeline.finish();
                return Err(LocalDbError::new(format!(
                    "owner timeline arm failed: {error}; disarm failures: {}",
                    failures.join(", ")
                )));
            }
        }
        Ok(())
    }

    pub(crate) fn finish_write_timeline(
        &self,
    ) -> Result<super::super::write_timeline::WriteTimelineExport, LocalDbError> {
        if !self.write_timeline.enabled() {
            return Err(LocalDbError::new("write timeline is not armed"));
        }
        let mut records = Vec::new();
        let mut dropped = 0u64;
        let mut incomplete = 0u64;
        let mut captured = 0;
        let mut errors = Vec::new();
        for (partition, client) in &self.io_clients {
            match client.control(|client| Ok(client.owner_timeline_fetch_partial())) {
                Ok((mut page, owner_dropped, owner_incomplete, fetch_error)) => {
                    for record in &mut page {
                        record.owner = *partition;
                    }
                    records.extend(page);
                    dropped = dropped.saturating_add(owner_dropped);
                    incomplete = incomplete.saturating_add(owner_incomplete);
                    if let Some(error) = fetch_error {
                        errors.push(owner_timeline_error(
                            *partition,
                            "fetch",
                            LocalDbError::component(error),
                        ));
                    } else {
                        captured += 1;
                    }
                }
                Err(error) => errors.push(owner_timeline_error(*partition, "fetch", error)),
            }
        }
        for (partition, client) in &self.io_clients {
            if let Err(error) = client.control(|client| client.owner_timeline_arm(0)) {
                errors.push(owner_timeline_error(*partition, "disarm", error));
            }
        }
        self.write_timeline.set_owner_records(
            records,
            dropped,
            incomplete,
            self.io_clients.len(),
            captured,
            errors,
        );
        self.write_timeline.finish().map_err(LocalDbError::new)
    }
    pub(crate) fn finish_write_timeline_until(
        &self,
        deadline: Instant,
    ) -> Result<super::super::write_timeline::WriteTimelineExport, LocalDbError> {
        if !self.write_timeline.enabled() {
            return Err(LocalDbError::new("write timeline is not armed"));
        }
        let mut records = Vec::new();
        let mut dropped = 0u64;
        let mut incomplete = 0u64;
        let mut captured = 0;
        let mut errors = Vec::new();
        for (partition, client) in &self.io_clients {
            let mut offset = 0usize;
            let mut owner_dropped = 0u64;
            let mut owner_incomplete = 0u64;
            loop {
                match client.control_until(deadline, move |client| {
                    client.owner_timeline_page_until(offset, deadline)
                }) {
                    Ok((mut page, page_dropped, page_incomplete, more)) => {
                        for record in &mut page {
                            record.owner = *partition;
                        }
                        offset += page.len();
                        records.extend(page);
                        owner_dropped = page_dropped;
                        owner_incomplete = page_incomplete;
                        if !more {
                            captured += 1;
                            break;
                        }
                    }
                    Err(error) => {
                        errors.push(owner_timeline_error(*partition, "fetch", error));
                        break;
                    }
                }
            }
            dropped = dropped.saturating_add(owner_dropped);
            incomplete = incomplete.saturating_add(owner_incomplete);
        }
        for (partition, client) in &self.io_clients {
            if let Err(error) = client.control_until(deadline, move |client| {
                client.owner_timeline_arm_until(0, deadline)
            }) {
                errors.push(owner_timeline_error(*partition, "disarm", error));
            }
        }
        self.write_timeline
            .finish_with_owner_until(
                deadline,
                super::super::write_timeline::OwnerTimelineCollection {
                    records,
                    dropped,
                    incomplete,
                    expected: self.io_clients.len(),
                    captured,
                    errors,
                },
            )
            .map_err(LocalDbError::new)
    }
    /// Setup-time evidence for the existing owner-monitor connections only.
    #[cfg(feature = "experimental-fabric")]
    pub fn fabric_provider_evidence(&self) -> Vec<FabricProviderEvidence> {
        self.io_clients
            .iter()
            .map(|(partition, client)| {
                let client = client.client.lock();
                FabricProviderEvidence {
                    partition: *partition,
                    provider: client.provider_info().cloned(),
                    open_profile: client.connect_profile().fabric_open,
                }
            })
            .collect()
    }

    #[cfg(all(test, feature = "experimental-nvmeof", target_os = "linux"))]
    pub(crate) fn install_nvmeof_file_fixture(
        &self,
        config: super::super::nvmeof::NvmeofReaderConfig,
        path: &std::path::Path,
    ) -> Result<(), LocalDbError> {
        self.ensure_healthy()?;
        self.reads
            .install_nvmeof_file_fixture(config, &self.config, &self.shape, path)
    }

    /// Install a verified sealed-WAL reader while all reads are idle.
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    pub fn install_nvmeof_reader(
        &self,
        config: super::super::nvmeof::NvmeofReaderConfig,
    ) -> Result<(), LocalDbError> {
        self.ensure_healthy()?;
        self.reads
            .install_nvmeof_reader(config, &self.config, &self.shape)
    }

    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    pub fn clear_nvmeof_reader(&self) -> Result<(), LocalDbError> {
        self.reads.clear_nvmeof_reader()
    }

    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    pub fn nvmeof_stats(&self) -> Vec<super::super::nvmeof::NvmeofStats> {
        self.reads.nvmeof_stats()
    }

    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    pub fn register_nvmeof_metrics(
        &self,
        registry: &prometheus::Registry,
    ) -> Result<(), LocalDbError> {
        self.reads.register_nvmeof_metrics(registry)?;
        if let Some(metrics) = &self.live_metrics {
            super::super::nvmeof::live_prometheus::register(registry, metrics.clone())
                .map_err(LocalDbError::component)?;
        }
        Ok(())
    }

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
            first_failure: db.first_failure.clone(),
            authority: AuthorityCounters::default(),
            timeline: db.write_timeline.clone(),
        });
        let snapshot = Arc::new(Mutex::new(Snapshot {
            stats: db.stats(),
            pipeline: db.pipeline_stats(),
            authority: db.authority_write_stats(),
            metadata_history_bytes: db.metadata_history_bytes(),
            checkpoint_diagnostics: db.checkpoint_diagnostic_summary(),
        }));
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        let live_final_requested = Arc::new(AtomicBool::new(false));
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        let live_final_targets = Arc::new(Mutex::new(None));
        let (sender, receiver) = mpsc::sync_channel(db.config.max_pending_batches);
        let mut owner = Self {
            sender: Some(sender),
            wake: db.preparation_wake.clone(),
            thread: None,
            admission: admission.clone(),
            snapshot: snapshot.clone(),
            failure: db.concurrent_failure.clone(),
            first_failure: db.first_failure.clone(),
            index: db.index.clone(),
            reads: db.reads.clone(),
            cache: db.read_cache.clone(),
            write_profile: db.write_profile.clone(),
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            live_metrics: db.live_metrics.clone(),
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            live_final_requested: live_final_requested.clone(),
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            live_final_targets: live_final_targets.clone(),
            write_timeline: db.write_timeline.clone(),
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
                    let result = match outcome {
                        Ok(result) => result,
                        Err(_) => Err(LocalDbError::new(
                            "write authority panicked; recovery required",
                        )),
                    };
                    if let Err(error) = &result {
                        record_first_failure(&db.first_failure, None, None, "write_owner", error);
                        db.concurrent_failure.store(true, Ordering::Release);
                        db.reads.poison();
                        db.healthy = false;
                    }
                    {
                        let mut budget = admission.budget.lock();
                        budget.closed = true;
                        admission.changed.notify_all();
                    }
                    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
                    let shutdown = if result.is_ok() && live_final_requested.load(Ordering::Acquire)
                    {
                        db.shutdown_live_target_final().map(|targets| {
                            *live_final_targets.lock() = Some(targets);
                        })
                    } else {
                        db.shutdown()
                    };
                    #[cfg(not(all(feature = "experimental-nvmeof", target_os = "linux")))]
                    let shutdown = db.shutdown();
                    result.and(shutdown)
                })
                .map_err(LocalDbError::component)?,
        );
        Ok(owner)
    }

    pub(super) fn ensure_healthy(&self) -> Result<(), LocalDbError> {
        if self.failure.load(Ordering::Acquire) || self.reads.failed() {
            return Err(LocalDbError::new(failure_context(
                &self.first_failure,
                "TCP authority requires recovery or is closed",
            )));
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
        let cut = self.index.latest_cut();
        self.ensure_healthy()?;
        Ok(cut)
    }

    /// Capture and protect one published cut without a retention race.
    pub fn protected_published_cut(
        &self,
    ) -> Result<(IndexReadCut, super::super::native_index::IndexReadPin), LocalDbError> {
        self.ensure_healthy()?;
        let index = &self.index;
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
        let pin = self.index.pin_cut(cut).map_err(LocalDbError::component)?;
        self.ensure_healthy()?;
        Ok(pin)
    }

    pub fn retention_floor(&self) -> IndexReadCut {
        self.index.retention_floor()
    }

    pub fn index_stats(&self) -> super::super::native_index::NativeIndexStats {
        self.index.stats()
    }

    pub fn index_history_bytes(&self) -> usize {
        self.index.retained_metadata_bytes()
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
            .relocation_history(ks, cut)
            .map_err(LocalDbError::component)
    }

    pub(crate) fn next_relocation_record(
        &self,
        history: &mut super::super::native_index::NativeIndexHistory,
    ) -> Result<Option<(Bytes, ValueRecordReference)>, LocalDbError> {
        self.ensure_healthy()?;
        Ok(history.next(&self.index))
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
        self.write_batch_inner(operation, mutations, None)
    }

    pub(crate) fn write_batch_traced(
        &self,
        operation: [u8; 16],
        mutations: Vec<Mutation>,
        sample: Option<WriteTimelineSample>,
    ) -> Result<BatchVersion, LocalDbError> {
        self.write_batch_inner(operation, mutations, sample)
    }

    fn write_batch_inner(
        &self,
        operation: [u8; 16],
        mutations: Vec<Mutation>,
        sample: Option<WriteTimelineSample>,
    ) -> Result<BatchVersion, LocalDbError> {
        if let Some(sample) = sample {
            self.write_timeline
                .event(sample, WriteTimelineStage::ValidationStart);
        }
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
        if let Some(sample) = sample {
            self.write_timeline
                .event(sample, WriteTimelineStage::AdmissionWaitStarted);
        }
        let block_started = self.write_timeline.block_started();
        let credit = self.reserve(bytes)?;
        self.write_timeline.block_finished(
            super::super::write_timeline::WriteBlockStage::AdmissionCredit,
            block_started,
            0,
        );
        if let Some(sample) = sample {
            self.write_timeline
                .event(sample, WriteTimelineStage::AdmissionAcquired);
        }
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
        if let Some(sample) = sample {
            self.write_timeline
                .event(sample, WriteTimelineStage::OwnerEnqueueStarted);
        }
        let sender = self.sender.as_ref().ok_or_else(|| {
            LocalDbError::new(failure_context(
                &self.first_failure,
                "write authority closed",
            ))
        })?;
        self.write_timeline
            .wait_enter(WriteWait::OwnerEnqueueToDequeue, 1);
        sender
            .send(OwnerCommand::Write {
                operation,
                mutations,
                reply,
            })
            .map_err(|_| {
                self.write_timeline
                    .wait_leave(WriteWait::OwnerEnqueueToDequeue, 1);
                LocalDbError::new(failure_context(
                    &self.first_failure,
                    "write authority stopped",
                ))
            })?;
        let wake_result = self.wake.try_send(());
        self.write_timeline.wait_wake(
            WriteWait::OwnerEnqueueToDequeue,
            matches!(wake_result, Err(mpsc::TrySendError::Full(()))),
        );
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
                return Err(LocalDbError::new(failure_context(
                    &self.first_failure,
                    "write authority closed",
                )));
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
    /// Live retirement can return a non-fatal busy error while a read is held;
    /// finish or drop the read and retry without reopening the authority.
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
        self.cache.stats()
    }
    pub fn index_shard_stats(&self) -> super::super::native_index::IndexShardStats {
        self.index.shard_stats()
    }
    pub fn read_cache_shard_stats(&self) -> super::ReadCacheShardStats {
        self.cache.shard_stats()
    }
    pub fn per_keyspace_read_cache_stats(
        &self,
    ) -> [PerKeyspaceReadCacheStats; KEYSPACE_COUNTER_COUNT] {
        self.cache.per_keyspace_stats()
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
            authority: self.admission.authority.snapshot(snapshot.authority),
            pipeline: self.pipeline_stats(),
            read_cache: self.cache.stats(),
            write_profile: *self.write_profile.lock(),
            checkpoint_diagnostics: snapshot.checkpoint_diagnostics,
        });
    }
    /// Bounded metrics snapshot; lock contention makes the whole snapshot unavailable.
    pub fn try_visit_write_metrics(&self, visit: impl FnOnce(WriteMetricsSnapshot)) -> Option<()> {
        let snapshot = {
            let stored = self.snapshot.try_lock()?;
            let budget = self.admission.budget.try_lock()?;
            let cache = &self.cache;
            let write_profile = self.write_profile.try_lock()?;
            let mut pipeline = stored.pipeline;
            pipeline.concurrent_queued_peak = budget.peak_requests;
            pipeline.concurrent_queue_bytes_peak = budget.peak_bytes;
            WriteMetricsSnapshot {
                authority: self.admission.authority.snapshot(stored.authority),
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
        let index = &self.index;
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
        Some(self.cache.stats())
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
        Some(self.cache.per_keyspace_stats())
    }
    pub fn read_evidence_monitor(&self) -> ReadEvidenceMonitor {
        ReadEvidenceMonitor {
            clients: self.io_clients.clone(),
            reads: self.reads.clone(),
        }
    }
    /// Explicit flush: return only when every owner's synced frontier covers all
    /// writes it acknowledged before this call. Owners flush in parallel (one
    /// scoped thread per owner, bounded by the configured owner count); each
    /// request is queued behind that owner's earlier appends and runs the owner's
    /// control barrier, which flushes every log's frontier before replying. A
    /// `Synchronous` owner has nothing unsynced, so this costs one round trip.
    ///
    /// # Errors
    /// Returns the first owner failure; owners that already flushed stay flushed.
    pub fn flush_durable(&self) -> Result<(), LocalDbError> {
        self.ensure_healthy()?;
        let outcomes: Vec<Result<(), LocalDbError>> = std::thread::scope(|scope| {
            let flushes: Vec<_> = self
                .io_clients
                .iter()
                .map(|(_, handle)| {
                    scope.spawn(move || handle.control(|client| client.status().map(|_| ())))
                })
                .collect();
            flushes
                .into_iter()
                .map(|flush| {
                    flush
                        .join()
                        .unwrap_or_else(|_| Err(LocalDbError::new("owner flush panicked")))
                })
                .collect()
        });
        outcomes.into_iter().collect::<Result<(), _>>()?;
        self.ensure_healthy()
    }
    /// Request advisory mapped-WAL eviction on every owner at a quiescent boundary.
    /// The owner control handler visits all retained source epochs, not just the
    /// current writer epoch. Callers must exclude concurrent reads and writes.
    #[cfg(all(target_os = "linux", feature = "test-utils"))]
    pub fn evict_mapped_wal_for_benchmark(
        &self,
    ) -> Result<Vec<(u32, crate::read_probe::ReadEvidence)>, LocalDbError> {
        self.ensure_healthy()?;
        self.reads.ensure_idle()?;
        // The reply can wake its caller just before the owner drops its credit.
        // Wait for that bounded retirement rather than spuriously failing.
        let deadline = Instant::now() + self.config.timeout;
        let mut budget = self.admission.budget.lock();
        while budget.requests != 0 {
            let now = Instant::now();
            if now >= deadline {
                return Err(LocalDbError::new(
                    "writes did not drain before mapped-WAL eviction",
                ));
            }
            self.admission.changed.wait_for(&mut budget, deadline - now);
        }
        drop(budget);
        if self.config.read_cache_bytes != 0 {
            return Err(LocalDbError::new(
                "mapped-WAL eviction requires disabled coordinator read cache",
            ));
        }
        let epoch = self.epoch;
        let result = self
            .io_clients
            .iter()
            .map(|(partition, client)| {
                client
                    .control(move |client| client.read_evidence_for_benchmark(epoch, true))
                    .map(|evidence| (*partition, evidence))
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.ensure_healthy()?;
        Ok(result)
    }

    /// Passive snapshots do not keep the write owner or its connections alive.
    pub fn operational_read_io_monitor(&self) -> OperationalReadIoMonitor {
        if let Some(lifecycle) = self.operational_lifecycle.upgrade() {
            OperationalReadIoSampler::activate_lifecycle(&lifecycle);
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

    /// Final campaign teardown, distinct from ordinary close/reopen.
    /// Returns only owner-reported target evidence after successful drain and seal.
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    pub fn shutdown_live_target_final(mut self) -> Result<LiveFinalShutdownEvidence, LocalDbError> {
        let metrics = self
            .live_metrics
            .take()
            .ok_or_else(|| LocalDbError::new("live NVMe is not configured"))?;
        self.live_final_requested.store(true, Ordering::Release);
        self.stop()?;
        let targets = self
            .live_final_targets
            .lock()
            .take()
            .ok_or_else(|| LocalDbError::new("final live target evidence unavailable"))?;
        Ok(LiveFinalShutdownEvidence {
            metrics: metrics.snapshot(),
            targets,
        })
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

fn refresh_snapshot(db: &RemoteDatabase, snapshot: &Mutex<Snapshot>) {
    let block_started = db.write_timeline.block_started();
    *snapshot.lock() = Snapshot {
        stats: db.stats(),
        pipeline: db.pipeline_stats(),
        authority: db.authority_write_stats(),
        metadata_history_bytes: db.metadata_history_bytes(),
        checkpoint_diagnostics: db.checkpoint_diagnostic_summary(),
    };
    db.write_timeline.block_finished(
        super::super::write_timeline::WriteBlockStage::Snapshot,
        block_started,
        db.submissions.len(),
    );
}

#[allow(clippy::needless_pass_by_value)] // Receiver ownership resolves queued replies on every exit.
fn drive_owner(
    db: &mut RemoteDatabase,
    receiver: mpsc::Receiver<OwnerCommand>,
    admission: &Admission,
    snapshot: &Mutex<Snapshot>,
) -> Result<(), LocalDbError> {
    #[cfg(test)]
    if let Some(resume) = db.owner_start_pause.take() {
        let _ = resume.recv();
    }
    let mut pending: VecDeque<WriteReply> = VecDeque::new();
    // At most one ready-input window is retained. Reply credits cover its
    // detached inputs and encodings; a deferred front blocks all later writes.
    let window_bound = admission.max_requests.min(db.config.max_pending_batches);
    let mut prepared_writes = VecDeque::with_capacity(window_bound);
    let mut held_command = None;
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
        db.ensure_healthy().inspect_err(|error| {
            record_first_failure(
                &db.first_failure,
                None,
                error.uncertain_version,
                "owner_health",
                error,
            );
        })?;
        // Harvest completions first, without dispatching short tails. Drain only
        // commands already ready below, then send the resulting groups at once.
        // There is no timer or wait for a second request: singletons stay prompt.
        {
            let _admit = admission.authority.admitting();
            db.progress_pipeline(false).inspect_err(|error| {
                record_first_failure(
                    &db.first_failure,
                    None,
                    error.uncertain_version,
                    "progress_pipeline",
                    error,
                );
            })?;
            for _ in 0..pending.len() {
                let reply = pending.pop_front().unwrap();
                let version = reply.version.unwrap();
                if version.epoch < db.epoch || db.publication.is_published(version) {
                    db.ensure_healthy().inspect_err(|error| {
                        record_first_failure(
                            &db.first_failure,
                            None,
                            error.uncertain_version,
                            "publication_health",
                            error,
                        );
                    })?;
                    db.pipeline_stats.concurrent_completed += 1;
                    if db.config.write_profile_limit != 0 {
                        // Diagnostic snapshots must be visible before client completion.
                        refresh_snapshot(db, snapshot);
                    }
                    db.write_timeline.batch_event(
                        version,
                        WriteTimelineStage::CallerReplyQueued,
                        None,
                        None,
                    );
                    let block_started = db.write_timeline.block_started();
                    reply.finish(Ok(version));
                    db.write_timeline.block_finished(
                        super::super::write_timeline::WriteBlockStage::ReplyCreditRelease,
                        block_started,
                        pending.len(),
                    );
                } else {
                    pending.push_back(reply);
                }
            }
        }
        if barrier.is_some() && db.submissions.is_empty() {
            let finishing_profiles = finish_profiles;
            let arming_profile = arm_profile_limit.take();
            let retiring = retirement.is_some();
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
            let fatal = result.is_err()
                && !finishing_profiles
                && (!retiring || db.ensure_healthy().is_err());
            if fatal && let Err(error) = &result {
                record_first_failure(
                    &db.first_failure,
                    None,
                    error.uncertain_version,
                    "checkpoint",
                    error,
                );
            }
            refresh_snapshot(db, snapshot);
            let _ = barrier.take().unwrap().send(result);
            if fatal {
                return Err(LocalDbError::new(failure_context(
                    &db.first_failure,
                    "checkpoint failed; recovery required",
                )));
            }
        }
        refresh_snapshot(db, snapshot);
        #[cfg(test)]
        if let Some(events) = db.concurrent_progress.lock().as_ref() {
            let _ = events.try_send(db.pipeline_stats());
        }
        if disconnected && prepared_writes.is_empty() && db.submissions.is_empty() {
            return Ok(());
        }
        let mut admitted = false;
        // Retained prepared writes consume this pass's budget too.
        let mut receive_budget = admission.max_requests - prepared_writes.len();
        if barrier.is_none() && !disconnected {
            for pass in 0..admission.max_requests {
                if prepared_writes.is_empty() && held_command.is_none() {
                    if receive_budget == 0 {
                        break;
                    }
                    let collect_span = admission.authority.admitting();
                    let mut writes = Vec::new();
                    let mut bytes = 0usize;
                    for _ in 0..window_bound
                        .min(admission.max_requests - pass)
                        .min(receive_budget)
                    {
                        match receiver.try_recv() {
                            Ok(OwnerCommand::Write {
                                operation,
                                mutations,
                                reply,
                            }) => {
                                db.write_timeline
                                    .wait_leave(WriteWait::OwnerEnqueueToDequeue, 1);
                                db.write_timeline
                                    .operation_event(operation, WriteTimelineStage::OwnerDequeued);
                                bytes += reply._credit.bytes;
                                receive_budget -= 1;
                                writes.push((operation, mutations, reply));
                                // All these requests already own byte credits;
                                // their sum cannot exceed the admission bound.
                                if bytes >= admission.max_bytes {
                                    break;
                                }
                            }
                            other => {
                                held_command = Some(other);
                                break;
                            }
                        }
                    }
                    drop(collect_span);
                    if !writes.is_empty() {
                        let prepared = prepare_window(db, &writes, &admission.authority)?;
                        let _admit = admission.authority.admitting();
                        for ((_, _, reply), result) in writes.into_iter().zip(prepared) {
                            match result {
                                Ok(prepared) => prepared_writes.push_back(DeferredWrite {
                                    prepared,
                                    reply,
                                    since: None,
                                    drained_generation: None,
                                    fresh_generation: None,
                                }),
                                Err(error) => reject_preparation(db, reply, error)?,
                            }
                        }
                        admitted = true;
                    }
                }
                if let Some(mut write) = prepared_writes.pop_front() {
                    let _admit = admission.authority.admitting();
                    let id = write.prepared.batch.id();
                    let version = write.prepared.batch.version();
                    let next = BatchVersion {
                        epoch: db.epoch,
                        sequence: db.submission_next(),
                    };
                    let expected = if version == next {
                        next
                    } else {
                        db.existing_identity(id)?
                            .map(|(version, _)| version)
                            .unwrap_or(next)
                    };
                    if version != expected {
                        // An earlier prepare/admission failed, or a checkpoint
                        // changed the epoch. No slot was allocated for this
                        // batch: re-encode with the next reachable version.
                        db.write_timeline.forget_version(id.operation, version);
                        let started = Instant::now();
                        let before = db.authority_write_stats();
                        let result = db.prepare_submit(
                            id.operation,
                            write.prepared.batch.mutations().to_vec(),
                            Some(&admission.authority),
                        );
                        db.record_admission_span(started, before);
                        match result {
                            Ok(prepared) => write.prepared = prepared,
                            Err(error) => {
                                reject_preparation(db, write.reply, error)?;
                                admitted = true;
                                continue;
                            }
                        }
                    }
                    let trace_identity = db.write_timeline.enabled().then(|| {
                        (
                            write.prepared.batch.id().operation,
                            write.prepared.batch.version(),
                        )
                    });
                    let pressure = db.prepared_pressure(&write.prepared);
                    let full = db.submissions.len() >= db.config.max_admitted_batches
                        || db.submissions.len() >= db.config.max_pending_batches;
                    let result = match pressure {
                        Err(error) => Some(Err(error)),
                        Ok(pressure)
                            if pressure.is_none()
                                && (!full
                                    || write.prepared.batch.version().sequence
                                        != db.submission_next()
                                    || write.prepared.batch.version().epoch != db.epoch) =>
                        {
                            let started = Instant::now();
                            let before = db.authority_write_stats();
                            let block_started = db.write_timeline.block_started();
                            let result = db.admit_prepared_with_progress(write.prepared, false);
                            db.write_timeline.block_finished(
                                super::super::write_timeline::WriteBlockStage::OrderedAdmission,
                                block_started,
                                db.submissions.len(),
                            );
                            db.record_admission_span(started, before);
                            Some(result)
                        }
                        Ok(pressure) => {
                            if write.since.is_none() {
                                db.trace_admission_pressure(pressure.as_ref());
                                write.since = Some(Instant::now());
                                db.write_stats.concurrent_checkpoint_deferrals += 1;
                            }
                            if db.submissions.is_empty() {
                                let generation = *write
                                    .drained_generation
                                    .get_or_insert(db.snapshot_generation);
                                if write.fresh_generation.is_none()
                                    && db.snapshot_generation > generation
                                {
                                    write.fresh_generation = Some(db.snapshot_generation);
                                }
                                if let Some(fresh) = write.fresh_generation {
                                    if db.snapshot_generation > fresh
                                        || db.background_checkpoint.is_none()
                                    {
                                        let error = match pressure {
                                            Some(pressure) => db.admission_capacity_error(pressure),
                                            None => Err(LocalDbError::new(
                                                "TCP authority admission capacity reached",
                                            )),
                                        };
                                        Some(error.and_then(|()| {
                                            Err(LocalDbError::new("admission capacity reached"))
                                        }))
                                    } else {
                                        prepared_writes.push_front(write);
                                        break;
                                    }
                                } else if db.background_checkpoint.is_none() {
                                    db.start_checkpoint().inspect_err(|error| {
                                        record_first_failure(
                                            &db.first_failure,
                                            None,
                                            error.uncertain_version,
                                            "start_checkpoint",
                                            error,
                                        );
                                    })?;
                                    if db.background_checkpoint.is_some() {
                                        write.fresh_generation = Some(db.snapshot_generation);
                                        prepared_writes.push_front(write);
                                        break;
                                    }
                                    let error = match pressure {
                                        Some(pressure) => db.admission_capacity_error(pressure),
                                        None => Err(LocalDbError::new(
                                            "TCP authority admission capacity reached",
                                        )),
                                    };
                                    Some(error.and_then(|()| {
                                        Err(LocalDbError::new("admission capacity reached"))
                                    }))
                                } else {
                                    prepared_writes.push_front(write);
                                    break;
                                }
                            } else {
                                prepared_writes.push_front(write);
                                break;
                            }
                        }
                    };
                    if let Some(since) = write.since {
                        db.write_stats.concurrent_checkpoint_wait_ns += elapsed(since);
                    }
                    match result.unwrap() {
                        Ok(version) => {
                            write.reply.version = Some(version);
                            pending.push_back(write.reply);
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
                            if let Some((operation, version)) = trace_identity {
                                db.write_timeline.forget_version(operation, version);
                            }
                            let poisoned = !db.healthy || db.ensure_healthy().is_err();
                            if poisoned {
                                record_first_failure(
                                    &db.first_failure,
                                    None,
                                    error.uncertain_version,
                                    "admit_prepared",
                                    &error,
                                );
                                db.concurrent_failure.store(true, Ordering::Release);
                                db.reads.poison();
                            }
                            write.reply.finish(Err(error));
                            if poisoned {
                                return Err(LocalDbError::new("write failed; recovery required"));
                            }
                        }
                    }
                    admitted = true;
                    continue;
                }
                let Some(command) = held_command.take() else {
                    // Every member of a full window may fail validation.
                    continue;
                };
                match command {
                    Ok(OwnerCommand::Write { .. }) => {
                        unreachable!("write commands are prepared above")
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
        {
            let _admit = admission.authority.admitting();
            db.progress_pipeline(true).inspect_err(|error| {
                record_first_failure(
                    &db.first_failure,
                    None,
                    error.uncertain_version,
                    "progress_pipeline",
                    error,
                );
            })?;
        }
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
