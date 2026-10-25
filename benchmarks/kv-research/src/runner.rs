use crate::backend::{BackendResult, BatchAtomicity, KvBackend};
use crate::config::{ExecutionMode, RunConfig};
use crate::duration_ns;
use crate::report::{Reporter, RunSummary, Sample};
use crate::workload::{
    OperationKind, WorkItem, WorkloadGenerator, fresh_namespace, key_bytes, value_bytes,
};
use blake2::{Blake2b512, Digest};
use std::borrow::Cow;
use std::future::Future;
use std::io::{Read, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, mpsc as std_mpsc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tidehunter::minibytes::Bytes;
use tokio::sync::mpsc;
use tokio::time::{Instant as TokioInstant, sleep_until, timeout_at};

// One owner enforces qualification, phase order, shutdown and terminal artifact publication.
#[allow(clippy::too_many_lines)]
pub async fn run<B: KvBackend>(
    backend: B,
    config: RunConfig,
    output: &Path,
    registry: prometheus::Registry,
) -> BackendResult<()> {
    config.validate()?;
    let info = backend.info();
    if info.name.is_empty() {
        return Err(invalid("backend returned empty identity"));
    }
    if config.workload.mix.put_batch > 0 && info.batch_atomicity == BatchAtomicity::Unsupported {
        return Err(invalid(
            "put_batch requested but backend does not provide supported batch semantics",
        ));
    }
    let namespace = if let Some(encoded) = &config.key_namespace_hex {
        let mut namespace = [0; 16];
        for (byte, pair) in namespace.iter_mut().zip(encoded.as_bytes().chunks_exact(2)) {
            *byte = u8::from_str_radix(std::str::from_utf8(pair)?, 16)?;
        }
        namespace
    } else {
        fresh_namespace()?
    };
    // Qualification remains isolated even when an owned cloud dataset is reused.
    let validation_namespace = derive_namespace(&fresh_namespace()?, b"qualification");
    let config_bytes = serde_json::to_vec(&config)?;
    let config_hash = hex_digest(&config_bytes);
    let started_wall = unix_seconds();
    let started = Instant::now();
    let deadline = started + Duration::from_secs(config.limits.deadline_seconds);
    let mut reporter = Reporter::create(output, registry, info.name.clone())?;
    reporter.set_phase("qualification");
    reporter.snapshot()?;

    let backend = Arc::new(backend);
    let mut failed = false;
    let mut fatal_reason = None;
    match qualify_on_worker(
        Arc::clone(&backend),
        validation_namespace,
        config.seed,
        deadline,
        config.execution.request_timeout_ms,
        config.limits.shutdown_grace_seconds,
        &mut reporter,
    )
    .await
    {
        Ok(()) => reporter.snapshot()?,
        Err(error) => {
            failed = true;
            fatal_reason = Some(format!("pre-workload qualification failed: {error}"));
        }
    }

    let mut generator = WorkloadGenerator::new(
        config.seed ^ 0x6b76_7265_7365_6172,
        config.dataset.keys,
        config.workload.mix.clone(),
        config.workload.batch_size,
        config.workload.distribution.clone(),
    );
    if fatal_reason.is_none() && !config.qualification_only && config.phases.preload {
        reporter.set_phase("preload");
        match preload(
            Arc::clone(&backend),
            &config,
            &namespace,
            started,
            deadline,
            &mut reporter,
        )
        .await
        {
            Ok(()) => reporter.snapshot()?,
            Err(error) => {
                failed = true;
                fatal_reason = Some(format!("preload failed: {error}"));
            }
        }
    }
    for (phase, count) in [
        ("warmup", config.phases.warmup_operations),
        ("aging", config.phases.aging_operations),
    ] {
        if config.qualification_only || fatal_reason.is_some() || count == 0 {
            continue;
        }
        reporter.set_phase(phase);
        if let Err(error) = run_phase(
            Arc::clone(&backend),
            &config,
            &namespace,
            &mut generator,
            phase,
            count,
            started,
            deadline,
            &mut reporter,
        )
        .await
        {
            failed = true;
            fatal_reason = Some(format!("{phase} phase stopped: {error}"));
        }
        reporter.snapshot()?;
    }
    if !config.qualification_only
        && fatal_reason.is_none()
        && config.phases.measurement_operations > 0
    {
        if let Some(gate) = &config.measurement_gate
            && let Err(error) = wait_for_measurement_gate(gate, deadline).await
        {
            failed = true;
            fatal_reason = Some(format!("measurement gate failed: {error}"));
            reporter.snapshot()?;
        }
        if fatal_reason.is_none() {
            reporter.set_phase("measurement");
            if let Err(error) = run_phase(
                Arc::clone(&backend),
                &config,
                &namespace,
                &mut generator,
                "measurement",
                config.phases.measurement_operations,
                started,
                deadline,
                &mut reporter,
            )
            .await
            {
                failed = true;
                fatal_reason = Some(format!("measurement phase stopped: {error}"));
            }
            reporter.snapshot()?;
        }
    }

    reporter.set_phase("qualification");
    if fatal_reason.is_none() && Instant::now() < deadline {
        if let Err(error) = qualify_on_worker(
            Arc::clone(&backend),
            derive_namespace(&namespace, b"qualification-post"),
            config.seed,
            deadline,
            config.execution.request_timeout_ms,
            config.limits.shutdown_grace_seconds,
            &mut reporter,
        )
        .await
        {
            failed = true;
            fatal_reason = Some(format!("post-workload qualification failed: {error}"));
        }
    } else if fatal_reason.is_none() {
        failed = true;
        fatal_reason =
            Some("global deadline expired before post-workload qualification".to_owned());
    }
    if let Err(error) = shutdown_on_worker(
        backend,
        deadline,
        config.limits.shutdown_grace_seconds,
        &mut reporter,
    )
    .await
    {
        failed = true;
        fatal_reason = Some(fatal_reason.map_or_else(
            || format!("backend shutdown failed: {error}"),
            |previous| format!("{previous}; backend shutdown failed: {error}"),
        ));
    }

    let finished_wall = unix_seconds();
    let totals = reporter.totals();
    if totals.3 > 0 || totals.4 > 0 {
        failed = true;
        let detail = format!(
            "workload recorded {} timeout/unknown and {} error/wrong-value outcomes",
            totals.3, totals.4
        );
        fatal_reason =
            Some(fatal_reason.map_or(detail.clone(), |previous| format!("{previous}; {detail}")));
    }
    let summary = RunSummary {
        status: if failed { "failed".to_owned() } else { "completed".to_owned() },
        started_unix_seconds: started_wall,
        finished_unix_seconds: finished_wall,
        backend: info,
        phase_counts: serde_json::json!({
            "offered": totals.0, "completed_terminal": totals.1, "succeeded": totals.6, "rejected": totals.2,
            "timeouts_or_unknown_mutations": totals.3, "errors_or_wrong_values": totals.4,
            "logical_keys_completed_terminal": totals.5, "logical_keys_offered": totals.7, "logical_keys_rejected": totals.8,
            "outstanding_at_finalization": totals.0.saturating_sub(totals.1).saturating_sub(totals.2),
        }),
        distributions: reporter.distribution_summaries(),
        limitations: vec![
            "Finite local workload phases are not evidence of sustainable steady-state capacity.".to_owned(),
            "Owner physical-device independence is not inferred from configured topology; see backend topology evidence.".to_owned(),
            "Measured API latency ends when response bytes are materialized; byte verification is outside the API timer.".to_owned(),
            "Batch latency is one complete backend API call; logical key counts are reported separately and no per-key latency is inferred.".to_owned(),
            "Concurrent workload traces do not assert a completion-order final-state oracle or linearizability.".to_owned(),
            "Synchronous adapters run on fixed dedicated workers; Tokio timeouts cannot preempt blocking calls, so overruns are retained and the process supervisor supplies the hard wall-clock bound.".to_owned(),
        ],
    };
    let manifest_extra = serde_json::json!({
        "config_hash_blake2b512": config_hash,
        "binary": binary_identity(),
        "config": config,
        "run_namespace_hex": hex(&namespace),
        "seed": config.seed,
        "source_revision": config.source_revision.as_deref().or(option_env!("GIT_COMMIT")),
        "hardware": hardware_identity(),
        "timing": {"clock": "std::time::Instant monotonic", "timer_start": "backend API dispatch after bounded admission", "timer_end": "response materialized"},
        "request_timeout_ms": config.execution.request_timeout_ms,
        "data_policy": {"key_bytes": 32, "key_derivation": "BLAKE2(namespace,seed,logical_key_id), all bits hashed",
            "value_identity_header_bytes": 16, "value_entropy_fraction_of_remainder": config.dataset.entropy},
        "phase_semantics": {"preload": config.phases.preload, "warmup_operations": config.phases.warmup_operations,
            "aging_operations": config.phases.aging_operations, "measurement_operations": config.phases.measurement_operations,
            "sustainability_claim": false},
        "qualification_failure": fatal_reason,
    });
    reporter.finish(&summary, failed, &manifest_extra)?;
    if failed {
        return Err(invalid(
            &fatal_reason.unwrap_or_else(|| "run failed".to_owned()),
        ));
    }
    Ok(())
}

async fn preload<B: KvBackend>(
    backend: Arc<B>,
    config: &RunConfig,
    namespace: &[u8; 16],
    origin: Instant,
    deadline: Instant,
    reporter: &mut Reporter,
) -> BackendResult<()> {
    let mut pool = WorkerPool::new(&backend, config, namespace, origin)?;
    let drain_deadline = deadline + Duration::from_secs(config.limits.shutdown_grace_seconds);
    let mut next_snapshot = Instant::now() + Duration::from_secs(1);
    let mut expired = false;
    for id in 0..config.dataset.keys {
        if Instant::now() >= deadline {
            expired = true;
            break;
        }
        if pool.pending.len() >= config.execution.concurrency {
            match receive_with_snapshots(&mut pool, reporter, drain_deadline, &mut next_snapshot)
                .await
            {
                Ok(completion) => pool.record_completion(completion, reporter)?,
                Err(error) => {
                    let _ = pool.record_unknown_pending(reporter);
                    pool.shutdown(false);
                    return Err(error);
                }
            }
        }
        let work = WorkItem {
            operation: OperationKind::Put,
            keys: vec![id],
        };
        let scheduled = duration_ns(origin.elapsed());
        reporter.offered("preload", "put", 1);
        if let Err(work) = pool.try_dispatch(work, "preload", scheduled, deadline) {
            reporter.rejected(rejected_sample(
                "preload",
                &work,
                scheduled,
                origin,
                "worker_queue_full",
            ))?;
        }
        if Instant::now() >= next_snapshot {
            reporter.snapshot()?;
            next_snapshot = Instant::now() + Duration::from_secs(1);
        }
    }
    while !pool.pending.is_empty() {
        match receive_with_snapshots(&mut pool, reporter, drain_deadline, &mut next_snapshot).await
        {
            Ok(completion) => pool.record_completion(completion, reporter)?,
            Err(error) => {
                let _ = pool.record_unknown_pending(reporter);
                pool.shutdown(false);
                return Err(error);
            }
        }
    }
    pool.shutdown(true);
    if expired {
        return Err(invalid("global deadline expired before preload completed"));
    }
    Ok(())
}

async fn receive_with_snapshots(
    pool: &mut WorkerPool,
    reporter: &mut Reporter,
    deadline: Instant,
    next_snapshot: &mut Instant,
) -> BackendResult<WorkerCompletion> {
    loop {
        tokio::select! {
            result = timeout_at(TokioInstant::from_std(deadline), pool.receive()) => return result?,
            () = sleep_until(TokioInstant::from_std(*next_snapshot)) => {
                reporter.snapshot()?;
                *next_snapshot = Instant::now() + Duration::from_secs(1);
            }
        }
    }
}

// Keep the ordered behavioral checks and their cleanup together; no timed workload is mixed in.
#[allow(clippy::too_many_lines)]
async fn qualify<B: KvBackend>(
    backend: &B,
    namespace: &[u8; 16],
    seed: u64,
    deadline: Instant,
    request_timeout_ms: u64,
) -> BackendResult<()> {
    if Instant::now() >= deadline {
        return Err(invalid("deadline expired before qualification"));
    }
    let keys = (0..6)
        .map(|id| key_bytes(namespace, seed, id))
        .collect::<Vec<_>>();
    let first = Bytes::from(vec![0x31; 256]);
    let overwrite = Bytes::from(vec![0x72; 256]);
    let second = Bytes::from(vec![0xb4; 256]);
    call(
        deadline,
        request_timeout_ms,
        backend.put(keys[0].clone(), first.clone()),
        "qualification put",
    )
    .await?;
    check_one(
        deadline,
        request_timeout_ms,
        backend.get(keys[0].as_ref()),
        Some(&first),
        "qualification initial read",
    )
    .await?;
    call(
        deadline,
        request_timeout_ms,
        backend.put(keys[0].clone(), overwrite.clone()),
        "qualification overwrite",
    )
    .await?;
    call(
        deadline,
        request_timeout_ms,
        backend.put(keys[1].clone(), second.clone()),
        "qualification distinct scalar put",
    )
    .await?;
    check_one(
        deadline,
        request_timeout_ms,
        backend.get(keys[0].as_ref()),
        Some(&overwrite),
        "qualification overwritten read",
    )
    .await?;
    check_many(
        deadline,
        request_timeout_ms,
        backend.get_many(&[
            keys[1].clone(),
            keys[4].clone(),
            keys[0].clone(),
            keys[1].clone(),
        ]),
        &[Some(&second), None, Some(&overwrite), Some(&second)],
        "qualification scalar ordered get_many",
    )
    .await?;
    let info = backend.info();
    if info.batch_atomicity != BatchAtomicity::Unsupported {
        call(
            deadline,
            request_timeout_ms,
            backend.put_batch(&[
                (keys[2].clone(), first.clone()),
                (keys[3].clone(), second.clone()),
            ]),
            "qualification put_batch",
        )
        .await?;
        check_many(
            deadline,
            request_timeout_ms,
            backend.get_many(&[
                keys[3].clone(),
                keys[5].clone(),
                keys[2].clone(),
                keys[3].clone(),
            ]),
            &[Some(&second), None, Some(&first), Some(&second)],
            "qualification ordered batch read",
        )
        .await?;
    }
    for key in &keys[..4] {
        call(
            deadline,
            request_timeout_ms,
            backend.delete(key.clone()),
            "qualification delete",
        )
        .await?;
    }
    check_many(
        deadline,
        request_timeout_ms,
        backend.get_many(&[
            keys[3].clone(),
            keys[1].clone(),
            keys[0].clone(),
            keys[4].clone(),
        ]),
        &[None, None, None, None],
        "qualification delete verification",
    )
    .await?;
    Ok(())
}
async fn qualify_on_worker<B: KvBackend>(
    backend: Arc<B>,
    namespace: [u8; 16],
    seed: u64,
    deadline: Instant,
    request_timeout_ms: u64,
    shutdown_grace_seconds: u64,
    reporter: &mut Reporter,
) -> BackendResult<()> {
    let (sender, mut receiver) = tokio::sync::oneshot::channel();
    let handle = tokio::runtime::Handle::current();
    let worker = thread::Builder::new()
        .name("kv-research-qualification".to_owned())
        .spawn(move || {
            let result = handle.block_on(qualify(
                backend.as_ref(),
                &namespace,
                seed,
                deadline,
                request_timeout_ms,
            ));
            let _ = sender.send(result);
        })?;
    let shutdown_deadline = deadline + Duration::from_secs(shutdown_grace_seconds);
    let mut next_snapshot = Instant::now() + Duration::from_secs(1);
    let result = loop {
        tokio::select! {
            result = &mut receiver => break result.map_err(|_| invalid("qualification worker exited without a result"))?,
            () = sleep_until(TokioInstant::from_std(next_snapshot)) => {
                reporter.snapshot()?;
                next_snapshot = Instant::now() + Duration::from_secs(1);
            }
            () = sleep_until(TokioInstant::from_std(shutdown_deadline)) => {
                break Err(invalid("qualification worker exceeded global deadline and shutdown grace"));
            }
        }
    };
    if result.is_ok() || Instant::now() < shutdown_deadline {
        let _ = worker.join();
    }
    result
}
async fn shutdown_on_worker<B: KvBackend>(
    backend: Arc<B>,
    deadline: Instant,
    shutdown_grace_seconds: u64,
    reporter: &mut Reporter,
) -> BackendResult<()> {
    let backend = Arc::try_unwrap(backend)
        .map_err(|_| invalid("backend still has outstanding worker references at shutdown"))?;
    let handle = tokio::runtime::Handle::current();
    let (sender, mut receiver) = tokio::sync::oneshot::channel();
    let worker = thread::Builder::new()
        .name("kv-research-shutdown".to_owned())
        .spawn(move || {
            let _ = sender.send(handle.block_on(backend.shutdown()));
        })?;
    let shutdown_deadline = deadline + Duration::from_secs(shutdown_grace_seconds);
    let mut next_snapshot = Instant::now() + Duration::from_secs(1);
    let result = loop {
        tokio::select! {
            result = &mut receiver => break result.map_err(|_| invalid("backend shutdown worker exited without a result"))?,
            () = sleep_until(TokioInstant::from_std(next_snapshot)) => {
                reporter.snapshot()?;
                next_snapshot = Instant::now() + Duration::from_secs(1);
            }
            () = sleep_until(TokioInstant::from_std(shutdown_deadline)) => {
                break Err(invalid("backend shutdown exceeded the global deadline and grace period"));
            }
        }
    };
    if Instant::now() < shutdown_deadline {
        let _ = worker.join();
    }
    result
}

async fn call<F: Future<Output = BackendResult<()>>>(
    deadline: Instant,
    timeout_ms: u64,
    future: F,
    context: &str,
) -> BackendResult<()> {
    let request_deadline = request_deadline(deadline, timeout_ms);
    timeout_at(TokioInstant::from_std(request_deadline), future)
        .await
        .map_err(|_| {
            invalid(&format!(
                "{context} timed out; mutation outcome may be unknown"
            ))
        })?
        .map_err(|error| invalid(&format!("{context}: {error}")))
}

async fn check_one<F: Future<Output = BackendResult<Option<Bytes>>>>(
    deadline: Instant,
    timeout_ms: u64,
    future: F,
    expected: Option<&Bytes>,
    context: &str,
) -> BackendResult<()> {
    let request_deadline = request_deadline(deadline, timeout_ms);
    let actual = timeout_at(TokioInstant::from_std(request_deadline), future)
        .await
        .map_err(|_| invalid(&format!("{context} timed out")))??;
    if actual.as_ref() != expected {
        return Err(invalid(&format!(
            "{context}: returned bytes differ from expected value"
        )));
    }
    Ok(())
}

async fn check_many<F: Future<Output = BackendResult<Vec<Option<Bytes>>>>>(
    deadline: Instant,
    timeout_ms: u64,
    future: F,
    expected: &[Option<&Bytes>],
    context: &str,
) -> BackendResult<()> {
    let request_deadline = request_deadline(deadline, timeout_ms);
    let actual = timeout_at(TokioInstant::from_std(request_deadline), future)
        .await
        .map_err(|_| invalid(&format!("{context} timed out")))??;
    if actual.len() != expected.len()
        || actual
            .iter()
            .zip(expected)
            .any(|(actual, expected)| actual.as_ref() != *expected)
    {
        return Err(invalid(&format!(
            "{context}: result count, order, missing status, or bytes differ"
        )));
    }
    Ok(())
}

fn request_deadline(global: Instant, timeout_ms: u64) -> Instant {
    global.min(Instant::now() + Duration::from_millis(timeout_ms))
}

struct WorkerRequest {
    id: u64,
    work: WorkItem,
    phase: &'static str,
    scheduled_ns: u64,
    origin: Instant,
    global_deadline: Instant,
    dispatch_timestamp: Arc<AtomicU64>,
}

struct WorkerCompletion {
    id: u64,
    sample: Sample,
}

struct PendingOperation {
    operation: OperationKind,
    logical_keys: u64,
    bytes: u64,
    phase: &'static str,
    scheduled_ns: u64,
    dispatch_timestamp: Arc<AtomicU64>,
}

struct WorkerPool {
    senders: Vec<std_mpsc::SyncSender<WorkerRequest>>,
    next_worker: usize,
    completions: mpsc::Receiver<WorkerCompletion>,
    workers: Vec<JoinHandle<()>>,
    pending: std::collections::HashMap<u64, PendingOperation>,
    next_id: u64,
    origin: Instant,
    value_size: usize,
}

impl WorkerPool {
    fn new<B: KvBackend>(
        backend: &Arc<B>,
        config: &RunConfig,
        namespace: &[u8; 16],
        origin: Instant,
    ) -> BackendResult<Self> {
        let (completion_tx, completions) = mpsc::channel(config.execution.queue_capacity.max(1));
        let mut senders = Vec::with_capacity(config.execution.concurrency);
        let mut workers = Vec::with_capacity(config.execution.concurrency);
        let per_worker_capacity = config
            .execution
            .queue_capacity
            .div_ceil(config.execution.concurrency)
            .max(1);
        let config = Arc::new(config.clone());
        let namespace = *namespace;
        let handle = tokio::runtime::Handle::current();
        for worker_index in 0..config.execution.concurrency {
            let (sender, receiver) = std_mpsc::sync_channel::<WorkerRequest>(per_worker_capacity);
            let backend = Arc::clone(backend);
            let config = Arc::clone(&config);
            let tx = completion_tx.clone();
            let handle = handle.clone();
            workers.push(
                thread::Builder::new()
                    .name(format!("kv-research-worker-{worker_index}"))
                    .spawn(move || {
                        while let Ok(request) = receiver.recv() {
                            let WorkerRequest {
                                id,
                                work,
                                phase,
                                scheduled_ns,
                                origin,
                                global_deadline,
                                dispatch_timestamp,
                            } = request;
                            let sample = handle.block_on(execute_item(
                                backend.as_ref(),
                                &config,
                                &namespace,
                                work,
                                phase,
                                scheduled_ns,
                                origin,
                                global_deadline,
                                dispatch_timestamp,
                            ));
                            if tx.blocking_send(WorkerCompletion { id, sample }).is_err() {
                                break;
                            }
                        }
                    })?,
            );
            senders.push(sender);
        }
        drop(completion_tx);
        Ok(Self {
            senders,
            next_worker: 0,
            completions,
            workers,
            pending: std::collections::HashMap::default(),
            next_id: 0,
            origin,
            value_size: config.dataset.value_size,
        })
    }

    fn try_dispatch(
        &mut self,
        work: WorkItem,
        phase: &'static str,
        scheduled_ns: u64,
        deadline: Instant,
    ) -> Result<(), WorkItem> {
        let id = self.next_id;
        self.next_id = self.next_id.saturating_add(1);
        let dispatch_timestamp = Arc::new(AtomicU64::new(0));
        let operation = work.operation;
        let logical_keys = work.keys.len() as u64;
        let bytes = if matches!(operation, OperationKind::Put | OperationKind::PutBatch) {
            logical_keys.saturating_mul(self.value_size as u64)
        } else {
            0
        };
        let mut request = WorkerRequest {
            id,
            work,
            phase,
            scheduled_ns,
            origin: self.origin,
            global_deadline: deadline,
            dispatch_timestamp: Arc::clone(&dispatch_timestamp),
        };
        for offset in 0..self.senders.len() {
            let index = (self.next_worker + offset) % self.senders.len();
            match self.senders[index].try_send(request) {
                Ok(()) => {
                    self.next_worker = (index + 1) % self.senders.len();
                    self.pending.insert(
                        id,
                        PendingOperation {
                            operation,
                            logical_keys,
                            bytes,
                            phase,
                            scheduled_ns,
                            dispatch_timestamp,
                        },
                    );
                    return Ok(());
                }
                Err(std_mpsc::TrySendError::Full(returned)) => request = returned,
                Err(std_mpsc::TrySendError::Disconnected(returned)) => return Err(returned.work),
            }
        }
        Err(request.work)
    }

    fn record_completion(
        &mut self,
        completion: WorkerCompletion,
        reporter: &mut Reporter,
    ) -> BackendResult<()> {
        self.pending.remove(&completion.id);
        if let Err(error) = reporter.record(completion.sample) {
            let _ = self.record_unknown_pending(reporter);
            self.shutdown(false);
            return Err(error);
        }
        Ok(())
    }

    async fn receive(&mut self) -> BackendResult<WorkerCompletion> {
        self.completions
            .recv()
            .await
            .ok_or_else(|| invalid("worker completion channel closed with requests outstanding"))
    }

    fn record_unknown_pending(&mut self, reporter: &mut Reporter) -> BackendResult<()> {
        let now = duration_ns(self.origin.elapsed());
        let mut first_error = None;
        for (_, pending) in self.pending.drain() {
            let operation = pending.operation;
            let dispatch_marker = pending.dispatch_timestamp.load(Ordering::Acquire);
            let dispatch_observed = dispatch_marker > 0;
            let dispatched = dispatch_marker.saturating_sub(1);
            let outcome = if dispatch_observed {
                timeout_outcome(operation)
            } else {
                "timeout"
            };
            let sample = Sample {
                phase: pending.phase,
                operation: operation.as_str(),
                outcome,
                scheduled_ns: pending.scheduled_ns,
                dispatched_ns: dispatched,
                dispatch_observed,
                completed_ns: now,
                api_ns: if dispatch_observed {
                    now.saturating_sub(dispatched)
                } else {
                    0
                },
                response_ns: now.saturating_sub(pending.scheduled_ns),
                scheduling_ns: if dispatch_observed {
                    dispatched.saturating_sub(pending.scheduled_ns)
                } else {
                    0
                },
                logical_keys: pending.logical_keys,
                bytes: pending.bytes,
                error_kind: Cow::Borrowed("external_shutdown_deadline_unknown_outcome"),
            };
            if let Err(error) = reporter.record(sample)
                && first_error.is_none()
            {
                first_error = Some(error);
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    fn shutdown(&mut self, join_workers: bool) {
        self.senders.clear();
        if join_workers {
            for worker in self.workers.drain(..) {
                let _ = worker.join();
            }
        } else {
            self.workers.clear();
        }
    }
}

async fn wait_for_measurement_gate(
    gate: &crate::config::MeasurementGateConfig,
    global_deadline: Instant,
) -> BackendResult<()> {
    let invalid =
        |message: &str| std::io::Error::new(std::io::ErrorKind::InvalidInput, message.to_owned());
    let normalize_marker = |marker: &Path| -> BackendResult<std::path::PathBuf> {
        let parent = marker
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let file_name = marker
            .file_name()
            .ok_or_else(|| invalid("measurement gate marker path must name a file"))?;
        if !parent.is_dir() {
            return Err(
                invalid("measurement gate marker parent directory must already exist").into(),
            );
        }
        Ok(std::fs::canonicalize(parent)?.join(file_name))
    };
    let ready_identity = normalize_marker(&gate.ready_file)?;
    let release_identity = normalize_marker(&gate.release_file)?;
    if ready_identity == release_identity {
        return Err(invalid("measurement gate marker paths must resolve to distinct files").into());
    }
    for marker in [&gate.ready_file, &gate.release_file] {
        if marker.try_exists()? {
            return Err(invalid("measurement gate markers must not already exist").into());
        }
    }
    if Instant::now() >= global_deadline {
        return Err(invalid("global deadline expired before measurement gate").into());
    }
    let mut ready = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&gate.ready_file)?;
    ready.write_all(b"ready\n")?;
    ready.sync_all()?;

    let gate_deadline =
        (Instant::now() + Duration::from_secs(gate.timeout_seconds)).min(global_deadline);
    loop {
        if Instant::now() >= gate_deadline {
            let message = if gate_deadline == global_deadline {
                "global deadline expired while waiting at measurement gate"
            } else {
                "measurement gate timed out waiting for release marker"
            };
            return Err(invalid(message).into());
        }
        if gate.release_file.try_exists()? {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

// Keep phase state, independent clocks and reporter borrowing explicit at this private boundary.
#[allow(clippy::too_many_arguments)]
// Keep open/closed-loop admission beside their common completion/drain accounting.
#[allow(clippy::too_many_lines)]
// Phase counts are admitted below 100 million, exactly representable in f64.
#[allow(clippy::cast_precision_loss)]
// Match makes the received-event and disconnected-channel paths explicit in select branches.
#[allow(clippy::single_match_else)]
async fn run_phase<B: KvBackend>(
    backend: Arc<B>,
    config: &RunConfig,
    namespace: &[u8; 16],
    generator: &mut WorkloadGenerator,
    phase: &'static str,
    count: u64,
    origin: Instant,
    deadline: Instant,
    reporter: &mut Reporter,
) -> BackendResult<()> {
    let mut pool = WorkerPool::new(&backend, config, namespace, origin)?;
    let phase_start = Instant::now();
    let mut scheduled_index = 0_u64;
    let mut phase_expired = false;
    let mut next_snapshot = Instant::now() + Duration::from_secs(1);
    match config.execution.mode {
        ExecutionMode::ClosedLoop => {
            while scheduled_index < count {
                if Instant::now() >= deadline {
                    phase_expired = true;
                    break;
                }
                if pool.pending.len() >= config.execution.concurrency {
                    tokio::select! {
                        () = sleep_until(TokioInstant::from_std(next_snapshot)) => {
                            reporter.snapshot()?;
                            next_snapshot = Instant::now() + Duration::from_secs(1);
                        }
                        completion = pool.completions.recv() => {
                            match completion {
                                Some(completion) => {
                                    if let Err(error) = pool.record_completion(completion, reporter) {
                                        let _ = pool.record_unknown_pending(reporter);
                                        pool.shutdown(false);
                                        return Err(error);
                                    }
                                }
                                None => {
                                    let _ = pool.record_unknown_pending(reporter);
                                    pool.shutdown(false);
                                    return Err(invalid("worker completion channel closed"));
                                }
                            }
                        }
                    }
                    continue;
                }
                let work = generator.next_item()?;
                let scheduled = duration_ns(origin.elapsed());
                reporter.offered(phase, work.operation.as_str(), work.keys.len() as u64);
                if Instant::now() >= next_snapshot {
                    reporter.snapshot()?;
                    next_snapshot = Instant::now() + Duration::from_secs(1);
                }
                if let Err(work) = pool.try_dispatch(work, phase, scheduled, deadline) {
                    reporter.rejected(rejected_sample(
                        phase,
                        &work,
                        scheduled,
                        origin,
                        "worker_queue_full",
                    ))?;
                }
                scheduled_index += 1;
            }
        }
        ExecutionMode::OpenLoop => {
            let rate = config
                .execution
                .offered_ops_per_second
                .ok_or_else(|| invalid("open-loop rate missing"))?;
            while scheduled_index < count {
                if Instant::now() >= deadline {
                    phase_expired = true;
                    break;
                }
                let due = phase_start + Duration::from_secs_f64(scheduled_index as f64 / rate);
                if due >= deadline {
                    phase_expired = true;
                    break;
                }
                tokio::select! {
                    () = sleep_until(TokioInstant::from_std(due)) => {},
                    () = sleep_until(TokioInstant::from_std(next_snapshot)) => {
                        reporter.snapshot()?;
                        next_snapshot = Instant::now() + Duration::from_secs(1);
                        continue;
                    }
                    completion = pool.completions.recv(), if !pool.pending.is_empty() => {
                        match completion {
                            Some(completion) => {
                                if let Err(error) = pool.record_completion(completion, reporter) {
                                    let _ = pool.record_unknown_pending(reporter);
                                    pool.shutdown(false);
                                    return Err(error);
                                }
                            }
                            None => {
                                let _ = pool.record_unknown_pending(reporter);
                                pool.shutdown(false);
                                return Err(invalid("worker completion channel closed"));
                            }
                        }
                        // Completion is not a new arrival; wait again for this same due time.
                        continue;
                    }
                }
                let work = generator.next_item()?;
                let scheduled = duration_ns(due.saturating_duration_since(origin));
                reporter.offered(phase, work.operation.as_str(), work.keys.len() as u64);
                let dispatched = if pool.pending.len() >= config.execution.queue_capacity {
                    Err(work)
                } else {
                    pool.try_dispatch(work, phase, scheduled, deadline)
                };
                if let Err(work) = dispatched {
                    reporter.rejected(rejected_sample(
                        phase,
                        &work,
                        scheduled,
                        origin,
                        "admission_capacity",
                    ))?;
                }
                if Instant::now() >= next_snapshot {
                    reporter.snapshot()?;
                    next_snapshot = Instant::now() + Duration::from_secs(1);
                }
                scheduled_index += 1;
            }
        }
    }
    let drain_deadline = deadline + Duration::from_secs(config.limits.shutdown_grace_seconds);
    reporter.set_phase("drain");
    while !pool.pending.is_empty() {
        tokio::select! {
            () = sleep_until(TokioInstant::from_std(next_snapshot)) => {
                reporter.snapshot()?;
                next_snapshot = Instant::now() + Duration::from_secs(1);
            }
            result = timeout_at(TokioInstant::from_std(drain_deadline), pool.receive()) => {
                match result {
                    Ok(Ok(completion)) => pool.record_completion(completion, reporter)?,
                    Ok(Err(error)) => {
                        let _ = pool.record_unknown_pending(reporter);
                        pool.shutdown(false);
                        return Err(error);
                    }
                    Err(_) => {
                        let _ = pool.record_unknown_pending(reporter);
                        pool.shutdown(false);
                        return Err(invalid("bounded worker shutdown grace expired; unresolved mutations retained as unknown outcomes"));
                    }
                }
            }
        }
    }
    pool.shutdown(true);
    if phase_expired {
        return Err(invalid(
            "global deadline expired before all phase arrivals were scheduled",
        ));
    }
    Ok(())
}

fn rejected_sample(
    phase: &'static str,
    work: &WorkItem,
    scheduled: u64,
    origin: Instant,
    reason: &'static str,
) -> Sample {
    let completed = duration_ns(origin.elapsed());
    Sample {
        phase,
        operation: work.operation.as_str(),
        outcome: "rejected",
        scheduled_ns: scheduled,
        dispatched_ns: 0,
        dispatch_observed: false,
        completed_ns: completed,
        api_ns: 0,
        response_ns: completed.saturating_sub(scheduled),
        scheduling_ns: 0,
        logical_keys: work.keys.len() as u64,
        bytes: 0,
        error_kind: Cow::Borrowed(reason),
    }
}

// Dispatch publication, API timing and the oracle need distinct inputs across the await boundary.
#[allow(clippy::too_many_arguments)]
// Keep one matched API boundary and its post-clock oracle for all operation kinds.
#[allow(clippy::too_many_lines)]
// This response type belongs only to the local API/oracle boundary, not the adapter interface.
#[allow(clippy::items_after_statements)]
async fn execute_item<B: KvBackend>(
    backend: &B,
    config: &RunConfig,
    namespace: &[u8; 16],
    work: WorkItem,
    phase: &'static str,
    scheduled: u64,
    origin: Instant,
    deadline: Instant,
    dispatch_timestamp: Arc<AtomicU64>,
) -> Sample {
    let operation = work.operation;
    let op = operation.as_str();
    let key_bytes_vec = work
        .keys
        .iter()
        .map(|id| key_bytes(namespace, config.seed, *id))
        .collect::<Vec<_>>();
    let entries = if matches!(
        operation,
        OperationKind::Read | OperationKind::Put | OperationKind::GetMany | OperationKind::PutBatch
    ) {
        work.keys
            .iter()
            .zip(&key_bytes_vec)
            .map(|(id, key)| {
                (
                    key.clone(),
                    value_bytes(
                        namespace,
                        config.seed,
                        *id,
                        config.dataset.value_size,
                        config.dataset.entropy,
                    ),
                )
            })
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    let key_count = work.keys.len() as u64;
    let bytes = match operation {
        OperationKind::Put | OperationKind::PutBatch => {
            (config.dataset.value_size * work.keys.len()) as u64
        }
        _ => 0,
    };
    let request_deadline = deadline.min(
        origin
            + Duration::from_nanos(scheduled)
                .saturating_add(Duration::from_millis(config.execution.request_timeout_ms)),
    );
    if Instant::now() >= request_deadline {
        return make_sample(
            phase,
            op,
            "timeout",
            scheduled,
            0,
            false,
            origin,
            0,
            key_count,
            bytes,
            Cow::Borrowed("request_timeout_before_backend_dispatch"),
        );
    }
    let api_start = Instant::now();
    let dispatched = duration_ns(api_start.duration_since(origin));
    dispatch_timestamp.store(dispatched.saturating_add(1), Ordering::Release);
    enum BackendResponse {
        Read(Option<Bytes>),
        Many(Vec<Option<Bytes>>),
        Mutation(u64),
    }
    let result = timeout_at(TokioInstant::from_std(request_deadline), async {
        match operation {
            OperationKind::Read | OperationKind::MissingRead => backend
                .get(key_bytes_vec[0].as_ref())
                .await
                .map(BackendResponse::Read),
            OperationKind::Put => backend
                .put(key_bytes_vec[0].clone(), entries[0].1.clone())
                .await
                .map(|()| BackendResponse::Mutation(bytes)),
            OperationKind::Delete => backend
                .delete(key_bytes_vec[0].clone())
                .await
                .map(|()| BackendResponse::Mutation(0)),
            OperationKind::GetMany => backend
                .get_many(&key_bytes_vec)
                .await
                .map(BackendResponse::Many),
            OperationKind::PutBatch => backend
                .put_batch(&entries)
                .await
                .map(|()| BackendResponse::Mutation(bytes)),
        }
    })
    .await;
    let api_end = Instant::now();
    let completed_ns = duration_ns(api_end.duration_since(origin));
    let api_ns = completed_ns.saturating_sub(dispatched);
    let timeout_ns = config
        .execution
        .request_timeout_ms
        .saturating_mul(1_000_000);
    match result {
        Ok(Ok(response)) => {
            let (returned_bytes, outcome, error_kind) = match response {
                BackendResponse::Read(value) => {
                    let absent = value.is_none();
                    let valid = operation == OperationKind::MissingRead
                        || value
                            .as_ref()
                            .is_none_or(|actual| actual.as_ref() == entries[0].1.as_ref());
                    let returned_bytes = value.as_ref().map_or(0, |v| v.len() as u64);
                    let outcome = if operation == OperationKind::MissingRead {
                        if absent {
                            "success"
                        } else {
                            "unexpected_present"
                        }
                    } else if valid {
                        "success"
                    } else {
                        "wrong_value"
                    };
                    let error = if outcome == "success" {
                        String::new()
                    } else if operation == OperationKind::MissingRead {
                        "missing-key read returned a value".to_owned()
                    } else {
                        "read bytes differ from deterministic payload".to_owned()
                    };
                    (returned_bytes, outcome, error)
                }
                BackendResponse::Many(values) => {
                    let valid_count = values.len() == work.keys.len();
                    let valid_values = values.iter().zip(&entries).all(|(actual, expected)| {
                        actual
                            .as_ref()
                            .is_none_or(|value| value.as_ref() == expected.1.as_ref())
                    });
                    let valid = valid_count && valid_values;
                    (
                        values
                            .iter()
                            .flatten()
                            .map(|value| value.len() as u64)
                            .sum(),
                        if valid { "success" } else { "wrong_value" },
                        if valid {
                            String::new()
                        } else {
                            "get_many result count or ordered value bytes differ".to_owned()
                        },
                    )
                }
                BackendResponse::Mutation(returned_bytes) => {
                    (returned_bytes, "success", String::new())
                }
            };
            let note = if api_ns > timeout_ns {
                format!("{error_kind}; completed_after_request_timeout")
            } else {
                error_kind
            };
            make_sample_at(
                phase,
                op,
                outcome,
                scheduled,
                dispatched,
                true,
                completed_ns,
                api_ns,
                key_count,
                returned_bytes.max(bytes),
                Cow::Owned(note),
            )
        }
        Ok(Err(error)) => {
            let timed_out = is_timed_out(error.as_ref());
            let outcome = if timed_out {
                timeout_outcome(operation)
            } else {
                "error"
            };
            let note = if timed_out {
                format!("backend_io_timeout: {error}")
            } else if api_ns > timeout_ns {
                format!("{error}; completed_after_request_timeout")
            } else {
                error.to_string()
            };
            make_sample_at(
                phase,
                op,
                outcome,
                scheduled,
                dispatched,
                true,
                completed_ns,
                api_ns,
                key_count,
                bytes,
                Cow::Owned(note),
            )
        }
        Err(_) => make_sample_at(
            phase,
            op,
            timeout_outcome(operation),
            scheduled,
            dispatched,
            true,
            completed_ns,
            api_ns,
            key_count,
            bytes,
            Cow::Borrowed("request_deadline_timeout"),
        ),
    }
}

fn timeout_outcome(operation: OperationKind) -> &'static str {
    if matches!(
        operation,
        OperationKind::Put | OperationKind::Delete | OperationKind::PutBatch
    ) {
        "unknown_mutation"
    } else {
        "timeout"
    }
}

fn is_timed_out(error: &(dyn std::error::Error + 'static)) -> bool {
    let mut current = Some(error);
    while let Some(source) = current {
        if source
            .downcast_ref::<std::io::Error>()
            .is_some_and(|error| error.kind() == std::io::ErrorKind::TimedOut)
        {
            return true;
        }
        current = source.source();
    }
    false
}

// Raw event fields stay explicit so this helper owns the single completion-clock observation.
#[allow(clippy::too_many_arguments)]
fn make_sample(
    phase: &'static str,
    operation: &'static str,
    outcome: &'static str,
    scheduled: u64,
    dispatched: u64,
    dispatch_observed: bool,
    origin: Instant,
    api_ns: u64,
    logical_keys: u64,
    bytes: u64,
    error_kind: Cow<'static, str>,
) -> Sample {
    let completed_ns = duration_ns(origin.elapsed());
    make_sample_at(
        phase,
        operation,
        outcome,
        scheduled,
        dispatched,
        dispatch_observed,
        completed_ns,
        api_ns,
        logical_keys,
        bytes,
        error_kind,
    )
}

// Centralize clock decomposition for the complete raw event, including undispatched outcomes.
#[allow(clippy::too_many_arguments)]
fn make_sample_at(
    phase: &'static str,
    operation: &'static str,
    outcome: &'static str,
    scheduled: u64,
    dispatched: u64,
    dispatch_observed: bool,
    completed_ns: u64,
    api_ns: u64,
    logical_keys: u64,
    bytes: u64,
    error_kind: Cow<'static, str>,
) -> Sample {
    let error_kind = if error_kind.len() <= 256 {
        error_kind
    } else {
        Cow::Owned(error_kind.chars().take(256).collect())
    };
    Sample {
        phase,
        operation,
        outcome,
        scheduled_ns: scheduled,
        dispatched_ns: dispatched,
        dispatch_observed,
        completed_ns,
        api_ns,
        response_ns: completed_ns.saturating_sub(scheduled),
        scheduling_ns: if dispatch_observed {
            dispatched.saturating_sub(scheduled)
        } else {
            0
        },
        logical_keys,
        bytes,
        error_kind,
    }
}

fn derive_namespace(namespace: &[u8; 16], purpose: &[u8]) -> [u8; 16] {
    let mut hasher = Blake2b512::new();
    hasher.update(namespace);
    hasher.update(purpose);
    let digest = hasher.finalize();
    std::array::from_fn(|index| digest[index])
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(DIGITS[usize::from(byte >> 4)]));
        output.push(char::from(DIGITS[usize::from(byte & 15)]));
    }
    output
}
fn hex_digest(bytes: &[u8]) -> String {
    let digest = Blake2b512::digest(bytes);
    hex(&digest)
}

// One reusable 64 KiB setup buffer fits the non-recursive 2 MiB worker stack; avoid a heap copy.
#[allow(clippy::large_stack_arrays)]
fn binary_identity() -> serde_json::Value {
    let result = std::env::current_exe()
        .and_then(|path| std::fs::File::open(&path).map(|file| (path, file)));
    match result {
        Ok((path, mut file)) => {
            let mut hasher = Blake2b512::new();
            let mut buffer = [0_u8; 64 * 1024];
            loop {
                match file.read(&mut buffer) {
                    Ok(0) => {
                        return serde_json::json!({"path": path, "blake2b512": hex(&hasher.finalize())});
                    }
                    Ok(read) => hasher.update(&buffer[..read]),
                    Err(error) => {
                        return serde_json::json!({"path": path, "error": error.to_string()});
                    }
                }
            }
        }
        Err(error) => serde_json::json!({"error": error.to_string()}),
    }
}
fn unix_seconds() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0.0, |duration| duration.as_secs_f64())
}
fn hardware_identity() -> serde_json::Value {
    let cpu = std::fs::read_to_string("/proc/cpuinfo")
        .ok()
        .and_then(|content| {
            content
                .lines()
                .find(|line| line.starts_with("model name"))
                .map(str::to_owned)
        });
    let memory = std::fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|content| {
            content
                .lines()
                .find(|line| line.starts_with("MemTotal:"))
                .map(str::to_owned)
        });
    let hostname = std::fs::read_to_string("/etc/hostname")
        .ok()
        .map(|value| value.trim().to_owned());
    serde_json::json!({"os": std::env::consts::OS, "architecture": std::env::consts::ARCH, "cpu": cpu, "memory": memory, "hostname": hostname})
}
fn invalid(message: &str) -> Box<dyn std::error::Error + Send + Sync> {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message.to_owned()).into()
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::BackendInfo;
    use std::collections::HashMap;
    use std::sync::Mutex;

    struct MemoryBackend {
        data: Mutex<HashMap<Vec<u8>, Bytes>>,
        corrupt_reads: bool,
        reverse_groups: bool,
    }

    impl MemoryBackend {
        fn new(corrupt_reads: bool, reverse_groups: bool) -> Self {
            Self {
                data: Mutex::new(HashMap::new()),
                corrupt_reads,
                reverse_groups,
            }
        }

        fn actual_value(&self, value: Bytes) -> Bytes {
            if self.corrupt_reads {
                Bytes::from(vec![0xff; value.len()])
            } else {
                value
            }
        }
    }

    impl KvBackend for MemoryBackend {
        fn info(&self) -> BackendInfo {
            BackendInfo {
                name: "memory-test".to_owned(),
                server_version: "test".to_owned(),
                client_version: "test".to_owned(),
                acknowledgement: "test acknowledgement".to_owned(),
                consistency: "test sequential".to_owned(),
                replication_factor: 1,
                measurement_boundary: "in process".to_owned(),
                get_many_strategy: "ordered".to_owned(),
                batch_atomicity: BatchAtomicity::SerializableTransaction,
                topology: serde_json::json!({"owners": 1}),
                configuration: serde_json::json!({}),
            }
        }

        async fn get(&self, key: &[u8]) -> BackendResult<Option<Bytes>> {
            Ok(self
                .data
                .lock()
                .map_err(|_| invalid("poisoned test store"))?
                .get(key)
                .cloned()
                .map(|value| self.actual_value(value)))
        }

        async fn put(&self, key: Bytes, value: Bytes) -> BackendResult<()> {
            self.data
                .lock()
                .map_err(|_| invalid("poisoned test store"))?
                .insert(key.as_ref().to_vec(), value);
            Ok(())
        }

        async fn delete(&self, key: Bytes) -> BackendResult<()> {
            self.data
                .lock()
                .map_err(|_| invalid("poisoned test store"))?
                .remove(key.as_ref());
            Ok(())
        }

        async fn get_many(&self, keys: &[Bytes]) -> BackendResult<Vec<Option<Bytes>>> {
            let data = self
                .data
                .lock()
                .map_err(|_| invalid("poisoned test store"))?;
            let mut values = keys
                .iter()
                .map(|key| {
                    data.get(key.as_ref())
                        .cloned()
                        .map(|value| self.actual_value(value))
                })
                .collect::<Vec<_>>();
            if self.reverse_groups {
                values.reverse();
            }
            Ok(values)
        }

        async fn put_batch(&self, entries: &[(Bytes, Bytes)]) -> BackendResult<()> {
            let mut data = self
                .data
                .lock()
                .map_err(|_| invalid("poisoned test store"))?;
            for (key, value) in entries {
                data.insert(key.as_ref().to_vec(), value.clone());
            }
            Ok(())
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn open_loop_completions_do_not_release_future_arrivals() {
        let directory = std::env::temp_dir().join(format!(
            "kv-research-arrivals-{}-{}",
            std::process::id(),
            hex(&fresh_namespace().unwrap())
        ));
        std::fs::create_dir(&directory).unwrap();
        let output = directory.join("result");
        let mut config: RunConfig = serde_json::from_str(include_str!("../example.json")).unwrap();
        config.dataset.keys = 8;
        config.dataset.value_size = 256;
        config.workload.batch_size = 2;
        config.workload.mix = crate::config::OperationMix {
            read: 1,
            ..crate::config::OperationMix::default()
        };
        config.execution.mode = ExecutionMode::OpenLoop;
        config.execution.concurrency = 1;
        config.execution.queue_capacity = 2;
        config.execution.offered_ops_per_second = Some(100.0);
        config.execution.request_timeout_ms = 1000;
        config.phases.warmup_operations = 0;
        config.phases.aging_operations = 0;
        config.phases.measurement_operations = 8;
        config.limits.deadline_seconds = 30;
        let result = run(
            MemoryBackend::new(false, false),
            config,
            &output,
            prometheus::Registry::new(),
        )
        .await;
        let raw = std::fs::read_to_string(output.join("samples.csv"));
        std::fs::remove_dir_all(&directory).unwrap();
        result.unwrap();
        let raw = raw.unwrap();
        let rows: Vec<_> = raw
            .lines()
            .filter(|line| line.starts_with("measurement,"))
            .collect();
        assert_eq!(
            rows.len(),
            8,
            "every declared arrival must have a terminal row"
        );
        for row in rows {
            let columns: Vec<_> = row.split(',').collect();
            let scheduled: u64 = columns[3].parse().unwrap();
            let dispatched: u64 = columns[4].parse().unwrap();
            let completed: u64 = columns[6].parse().unwrap();
            assert!(
                scheduled <= dispatched && dispatched <= completed,
                "early or inconsistent dispatch: {row}"
            );
        }
    }

    #[tokio::test]
    async fn qualification_rejects_wrong_returned_bytes() {
        let backend = MemoryBackend::new(true, false);
        assert!(
            qualify(
                &backend,
                &[3; 16],
                42,
                Instant::now() + Duration::from_secs(3),
                1000
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn qualification_rejects_reordered_get_many_results() {
        let backend = MemoryBackend::new(false, true);
        assert!(
            qualify(
                &backend,
                &[3; 16],
                42,
                Instant::now() + Duration::from_secs(3),
                1000
            )
            .await
            .is_err()
        );
    }

    #[test]
    fn sample_latency_uses_shared_dispatch_and_completion_instants() {
        let sample = make_sample_at(
            "measurement",
            "read",
            "success",
            3,
            7,
            true,
            17,
            10,
            1,
            8,
            Cow::Borrowed(""),
        );
        assert_eq!(sample.scheduling_ns, 4);
        assert_eq!(sample.response_ns, 14);
    }

    #[test]
    fn backend_timed_out_errors_are_timeout_outcomes() {
        let error = std::io::Error::new(std::io::ErrorKind::TimedOut, "backend deadline");
        assert!(is_timed_out(&error));
        assert_eq!(timeout_outcome(OperationKind::PutBatch), "unknown_mutation");
        assert_eq!(timeout_outcome(OperationKind::GetMany), "timeout");
    }
    #[test]
    fn rejected_arrivals_keep_nonzero_response_and_zero_dispatch() {
        let origin = Instant::now()
            .checked_sub(Duration::from_nanos(100))
            .unwrap();
        let work = WorkItem {
            operation: OperationKind::GetMany,
            keys: vec![1, 2, 3],
        };
        let sample = rejected_sample("measurement", &work, 1, origin, "capacity");
        assert_eq!(sample.dispatched_ns, 0);
        assert!(sample.response_ns >= 99);
        assert_eq!(sample.logical_keys, 3);
        assert_eq!(sample.outcome, "rejected");
    }
}
