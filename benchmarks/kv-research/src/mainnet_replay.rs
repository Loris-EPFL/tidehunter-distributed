//! W=1 checkpoint-commit replay with Sui checkpoint pipelining depth K
//! (`workload.checkpoint_pipeline_depth`, default 1 = strictly sequential).
//! The scheduler alone owns the DAG, the dirty overlay and the pipeline state.
use crate::backend::{BackendInfo, BackendResult, BatchAtomicity};
use crate::backends::native::replay::NativeReplayBackend;
use crate::backends::replay::{
    FinalRows, ReplayBackend, UnknownReplayMutation, UnsupportedReplayBackend, physical_key,
};
use crate::backends::tidehunter::replay::TidehunterReplayBackend;
use crate::config::{
    BackendSpec, DiagnosticsConfig, ExecutionConfig, ExecutionMode, LimitsConfig,
    MeasurementGateConfig,
};
use crate::duration_ns;
use crate::mainnet_trace::{Batch, Read, Row, Trace, expanded_keys};
use crate::report::{ReplayIdentity, Reporter, RunSummary, Sample};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tokio::time::{Instant as TokioInstant, timeout_at};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplayKind {
    SuiMainnetReplay,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReplayWorkload {
    pub kind: ReplayKind,
    pub manifest_path: PathBuf,
    pub manifest_sha256: String,
    pub repetitions: u64,
    pub minimum_measurement_seconds: f64,
    #[serde(default)]
    pub node_databases: Option<Vec<u8>>,
    /// Checkpoints in flight (K), the analogue of Sui's
    /// `checkpoint_execution_max_concurrency` (`.buffered(K)` in the checkpoint executor).
    /// Absent means K=1, the sequential scheduler. Absent stays absent when the config is
    /// echoed, so K=1 configurations keep their serialized identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint_pipeline_depth: Option<u32>,
}
/// Replay has no synthetic dataset, seed, mix, phases or key namespace.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReplayConfig {
    pub schema_version: u32,
    #[serde(default)]
    pub case_id: Option<String>,
    #[serde(default)]
    pub source_revision: Option<String>,
    pub backend: BackendSpec,
    pub workload: ReplayWorkload,
    pub execution: ExecutionConfig,
    pub limits: LimitsConfig,
    #[serde(default)]
    pub measurement_gate: Option<MeasurementGateConfig>,
    #[serde(default)]
    pub measurement_completion_gate: Option<MeasurementGateConfig>,
    #[serde(default)]
    pub diagnostics: DiagnosticsConfig,
}
/// Largest admitted checkpoint pipeline depth; Sui's default is 4 (sui-config node.rs).
const MAX_PIPELINE_DEPTH: u32 = 64;
fn ensure(ok: bool, message: &str) -> BackendResult<()> {
    if ok {
        Ok(())
    } else {
        Err(message.to_owned().into())
    }
}
impl ReplayConfig {
    /// Checkpoint pipeline depth K; absent means the sequential scheduler.
    pub fn pipeline_depth(&self) -> usize {
        self.workload
            .checkpoint_pipeline_depth
            .map_or(1, |depth| depth as usize)
    }
    pub fn validate(&self) -> BackendResult<Trace> {
        ensure(self.schema_version == 1, "replay schema_version must be 1")?;
        if !matches!(
            self.backend.kind.as_str(),
            "native"
                | "tidehunter_tcp"
                | "valkey_tcp"
                | "valkey_rdma"
                | "garnet"
                | "kvrocks"
                | "foundationdb"
                | "tikv"
                | "cassandra"
        ) {
            return Err(Box::new(UnsupportedReplayBackend(
                self.backend.kind.clone(),
            )));
        }
        if self.backend.kind == "native" {
            NativeReplayBackend::validate(&self.backend.options)?;
        }
        if matches!(
            self.backend.kind.as_str(),
            "valkey_tcp"
                | "valkey_rdma"
                | "garnet"
                | "kvrocks"
                | "foundationdb"
                | "tikv"
                | "cassandra"
        ) {
            crate::backends::validate_options(&self.backend.kind, &self.backend.options)?;
        }
        ensure(
            self.execution.mode == ExecutionMode::ClosedLoop
                && matches!(self.execution.client_shards, None | Some(1))
                && self.execution.offered_ops_per_second.is_none(),
            "replay requires closed_loop, client_shards=1, no offered rate",
        )?;
        ensure(
            (1..=4096).contains(&self.execution.concurrency)
                && (self.execution.concurrency..=65536).contains(&self.execution.queue_capacity)
                && (1..=300_000).contains(&self.execution.request_timeout_ms),
            "invalid replay caller/queue/deadline bounds",
        )?;
        ensure(
            self.workload.repetitions > 0
                && self.workload.minimum_measurement_seconds.is_finite()
                && self.workload.minimum_measurement_seconds >= 0.0,
            "replay requires finite repetitions and nonnegative minimum duration",
        )?;
        ensure(
            self.workload
                .checkpoint_pipeline_depth
                .is_none_or(|depth| (1..=MAX_PIPELINE_DEPTH).contains(&depth)),
            "checkpoint_pipeline_depth must be within 1..=64",
        )?;
        ensure(
            (1..=86400).contains(&self.limits.deadline_seconds)
                && (1..=600).contains(&self.limits.shutdown_grace_seconds)
                && self.workload.minimum_measurement_seconds < self.limits.deadline_seconds as f64,
            "invalid replay global deadline",
        )?;
        ensure(
            self.diagnostics.write_profile_limit == 0
                && self.diagnostics.write_timeline_limit == 0
                && self.diagnostics.read_profile_limit == 0
                && self.diagnostics.valkey_profile_limit == 0,
            "replay does not support scalar-only diagnostics",
        )?;
        for gate in [&self.measurement_gate, &self.measurement_completion_gate]
            .into_iter()
            .flatten()
        {
            ensure(
                gate.ready_file.is_absolute()
                    && gate.release_file.is_absolute()
                    && gate.ready_file != gate.release_file
                    && (1..=300).contains(&gate.timeout_seconds),
                "invalid replay measurement gate",
            )?;
        }
        if let (Some(a), Some(b)) = (&self.measurement_gate, &self.measurement_completion_gate) {
            ensure(
                a.ready_file != b.ready_file
                    && a.ready_file != b.release_file
                    && a.release_file != b.ready_file
                    && a.release_file != b.release_file,
                "replay gates require distinct markers",
            )?;
        }
        let mut trace = Trace::load(&self.workload.manifest_path, &self.workload.manifest_sha256)?;
        if let Some(selected) = &self.workload.node_databases {
            trace.select_node_databases(selected)?;
        }
        if self.backend.kind == "tidehunter_tcp" {
            TidehunterReplayBackend::validate(&self.backend.options, &trace)?;
        }
        let reads = trace
            .reads
            .iter()
            .flatten()
            .filter(|r| r.producer.is_none())
            .count() as u64;
        let members = trace
            .batches
            .iter()
            .map(|b| b.entries.len() as u64)
            .sum::<u64>();
        let operations = reads
            .checked_add(members)
            .and_then(|n| n.checked_add(trace.preload.len() as u64))
            .and_then(|n| n.checked_mul(self.workload.repetitions))
            .ok_or("replay operation arithmetic overflow")?;
        let bytes = trace
            .preload
            .iter()
            .chain(trace.batches.iter().flat_map(|b| &b.entries))
            .map(|r| (r.key.len() + r.value.len()) as u64)
            .sum::<u64>()
            .checked_mul(self.workload.repetitions)
            .ok_or("replay byte arithmetic overflow")?;
        let keys = (trace.preload.len() as u64)
            .checked_add(members)
            .and_then(|n| n.checked_mul(self.workload.repetitions))
            .ok_or("replay key arithmetic overflow")?;
        ensure(
            operations <= self.limits.max_operations
                && bytes <= self.limits.max_bytes
                && keys <= self.limits.max_keys,
            "replay exceeds operation/key/byte limits",
        )?;
        Ok(trace)
    }

    /// Admit execution, including disk and memory checks on the executing host.
    pub fn admit(&self) -> BackendResult<(Trace, Vec<Prepared>, serde_json::Value)> {
        self.admit_with(resource_estimate, true)
    }

    /// Validate configuration and estimate resources without checking the validating host.
    pub fn admit_configuration(&self) -> BackendResult<(Trace, Vec<Prepared>, serde_json::Value)> {
        self.admit_with(resource_estimate, false)
    }

    fn admit_with(
        &self,
        resources: impl FnOnce(&Self, &Trace) -> BackendResult<serde_json::Value>,
        enforce_host_resources: bool,
    ) -> BackendResult<(Trace, Vec<Prepared>, serde_json::Value)> {
        let trace = self.validate()?;
        let mut resources = resources(self, &trace)?;
        if enforce_host_resources {
            check_host_resources(self, &mut resources)?;
        }
        let mut prepared = prepare(&trace, self.workload.repetitions)?;
        if matches!(
            self.backend.kind.as_str(),
            "valkey_tcp" | "valkey_rdma" | "garnet" | "kvrocks"
        ) {
            prepare_physical_keys(&mut prepared, self.backend.options["cluster"] == true);
            #[cfg(any(
                feature = "valkey-tcp",
                feature = "garnet",
                feature = "kvrocks",
                feature = "valkey-rdma"
            ))]
            {
                resources["resp_admission"] =
                    crate::backends::resp_replay::admit(&prepared, &self.backend.kind)?;
            }
        }
        #[cfg(feature = "foundationdb")]
        if self.backend.kind == "foundationdb" {
            resources["foundationdb_admission"] =
                crate::backends::foundationdb::replay::admit(&mut prepared)?;
        }
        if self.backend.kind == "tikv" {
            prepare_physical_keys(&mut prepared, false);
            #[cfg(feature = "tikv")]
            {
                resources["tikv_admission"] = crate::backends::tikv::replay::admit(&prepared)?;
            }
        }
        #[cfg(feature = "cassandra")]
        if self.backend.kind == "cassandra" {
            crate::backends::cassandra::replay::prepare_keys(&mut prepared);
            resources["cassandra_admission"] =
                crate::backends::cassandra::replay::admit(&prepared)?;
        }
        if self.backend.kind == "native" {
            resources["native_index_geometry"] = NativeReplayBackend::validate_geometry(
                &self.backend.options,
                &trace.tables,
                prepared.iter().flat_map(|p| {
                    p.preload
                        .iter()
                        .chain(p.batches.iter().flat_map(|b| &b.entries))
                }),
            )?;
        }
        Ok((trace, prepared, resources))
    }
}
#[derive(Debug)]
pub struct Prepared {
    pub preload: Vec<Row>,
    pub batches: Vec<Batch>,
    pub reads: Vec<Vec<Read>>,
    overlay: Vec<Vec<Option<tidehunter::minibytes::Bytes>>>,
}
pub fn prepare(trace: &Trace, repetitions: u64) -> BackendResult<Vec<Prepared>> {
    let keys = expanded_keys(trace, repetitions)?;
    let mut result = Vec::with_capacity(keys.len());
    for keys in keys {
        let row = |r: &Row| -> Row {
            Row {
                key: keys[&(r.table, r.key.clone())].clone(),
                backend_key: keys[&(r.table, r.key.clone())].clone(),
                ..r.clone()
            }
        };
        result.push(Prepared {
            preload: trace.preload.iter().map(row).collect(),
            batches: trace
                .batches
                .iter()
                .map(|b| Batch {
                    entries: b.entries.iter().map(row).collect(),
                    ..b.clone()
                })
                .collect(),
            reads: trace
                .reads
                .iter()
                .map(|rs| {
                    rs.iter()
                        .map(|r| Read {
                            row: row(&r.row),
                            producer: r.producer,
                        })
                        .collect()
                })
                .collect(),
            overlay: trace
                .reads
                .iter()
                .map(|reads| {
                    reads
                        .iter()
                        .map(|read| {
                            read.producer.and_then(|_| {
                                trace
                                    .batches
                                    .iter()
                                    .filter(|batch| batch.phase == 7)
                                    .flat_map(|batch| &batch.entries)
                                    .find(|r| r.table == read.row.table && r.key == read.row.key)
                                    .map(|r| r.value.clone())
                            })
                        })
                        .collect()
                })
                .collect(),
        });
    }
    Ok(result)
}

fn prepare_physical_keys(prepared: &mut [Prepared], cluster: bool) {
    let mut keys = BTreeMap::new();
    for p in prepared {
        for row in p
            .preload
            .iter_mut()
            .chain(p.batches.iter_mut().flat_map(|b| &mut b.entries))
            .chain(p.reads.iter_mut().flatten().map(|r| &mut r.row))
        {
            row.backend_key = keys
                .entry((row.table, row.key.clone()))
                .or_insert_with(|| physical_key(row.table, &row.key, cluster))
                .clone();
        }
    }
}
#[derive(Clone, Copy)]
enum Operation {
    Read { tx: usize, index: usize },
    Commit(usize),
    Preload,
}
struct Work {
    repetition: usize,
    operation: Operation,
    scheduled: Instant,
    deadline: Instant,
    samples: Vec<Sample>,
}
struct Completion {
    worker: usize,
    work: Work,
    result: BackendResult<()>,
}
#[derive(Clone, Copy)]
struct Pending {
    repetition: usize,
    operation: Operation,
    scheduled: Instant,
    deadline: Instant,
}
/// Each dedicated caller owns one capacity-1 inbox; scheduler rejects a full inbox.
/// Completion capacity is caller count, one completion per outstanding job. Workers wait
/// for completion capacity; closing inboxes and draining/joining workers shuts the pool down.
/// Sample buffers are allocated once per caller and recycled, never once per get.
struct Pool {
    senders: Vec<mpsc::Sender<Work>>,
    receiver: mpsc::Receiver<Completion>,
    handles: Vec<JoinHandle<()>>,
    buffers: Vec<Option<Vec<Sample>>>,
    pending: Vec<Option<Pending>>,
    timeout_ms: u64,
    origin: Instant,
    markers: Vec<Arc<AtomicU64>>,
}
impl Pool {
    fn new<B: ReplayBackend + ?Sized>(
        backend: Arc<B>,
        prepared: Arc<Vec<Prepared>>,
        n: usize,
        origin: Instant,
        timeout_ms: u64,
    ) -> BackendResult<Self> {
        let (sender, receiver) = mpsc::channel(n);
        let mut pool = Self {
            senders: Vec::with_capacity(n),
            receiver,
            handles: Vec::with_capacity(n),
            buffers: Vec::with_capacity(n),
            pending: vec![None; n],
            timeout_ms,
            origin,
            markers: Vec::with_capacity(n),
        };
        for worker in 0..n {
            let (inbox, mut jobs) = mpsc::channel::<Work>(1);
            let results = sender.clone();
            let backend = Arc::clone(&backend);
            let prepared = Arc::clone(&prepared);
            let marker = Arc::new(AtomicU64::new(0));
            pool.markers.push(Arc::clone(&marker));
            let handle = std::thread::Builder::new()
                .name(format!("mainnet-read-{worker}"))
                .spawn(move || {
                    while let Some(mut work) = jobs.blocking_recv() {
                        let result = execute_work(
                            &*backend,
                            &prepared[work.repetition],
                            &mut work,
                            origin,
                            &marker,
                        );
                        if results
                            .blocking_send(Completion {
                                worker,
                                work,
                                result,
                            })
                            .is_err()
                        {
                            break;
                        }
                    }
                });
            let handle = match handle {
                Ok(handle) => handle,
                Err(error) => {
                    pool.senders.clear();
                    for handle in pool.handles.drain(..) {
                        handle
                            .join()
                            .map_err(|_| "replay startup worker panicked")?;
                    }
                    return Err(error.into());
                }
            };
            pool.senders.push(inbox);
            pool.handles.push(handle);
            pool.buffers.push(Some(Vec::with_capacity(1)));
        }
        Ok(pool)
    }
    fn dispatch(
        &mut self,
        worker: usize,
        repetition: usize,
        operation: Operation,
        scheduled: Instant,
        deadline: Instant,
    ) -> BackendResult<()> {
        let samples = self.buffers[worker].take().ok_or("caller already busy")?;
        let deadline = if matches!(operation, Operation::Preload) {
            deadline
        } else {
            crate::runner::request_deadline(deadline, self.timeout_ms)
        };
        let pending = Pending {
            repetition,
            operation,
            scheduled,
            deadline,
        };
        self.markers[worker].store(0, Ordering::Release);
        if let Err(error) = self.senders[worker].try_send(Work {
            repetition,
            operation,
            scheduled,
            deadline,
            samples,
        }) {
            self.buffers[worker] = Some(error.into_inner().samples);
            return Err("bounded replay caller rejected".into());
        }
        self.pending[worker] = Some(pending);
        Ok(())
    }
    async fn receive(&mut self, global_deadline: Instant) -> BackendResult<Completion> {
        let deadline = self
            .pending
            .iter()
            .flatten()
            .map(|p| p.deadline)
            .min()
            .unwrap_or(global_deadline)
            .min(global_deadline);
        self.receive_until(deadline).await
    }
    async fn receive_until(&mut self, deadline: Instant) -> BackendResult<Completion> {
        let completion = timeout_at(TokioInstant::from_std(deadline), self.receiver.recv())
            .await
            .map_err(|_| "replay deadline expired; outstanding mutation outcome unknown")?
            .ok_or("replay callers stopped")?;
        self.pending[completion.worker] = None;
        Ok(completion)
    }
    async fn drain(
        &mut self,
        trace: &Trace,
        prepared: &[Prepared],
        measurements: &mut Measurements,
        deadline: Instant,
    ) -> BackendResult<()> {
        while self.pending.iter().any(Option::is_some) {
            match self.receive_until(deadline).await {
                Ok(completion) => {
                    observe_completion(
                        trace,
                        &prepared[completion.work.repetition],
                        &completion,
                        &mut measurements.calls,
                        &mut measurements.counts,
                    )?;
                    // Admission has stopped. Preserve the original run error.
                    let _ = self.recycle(completion);
                }
                Err(_) => break,
            }
        }
        for (worker, pending) in self.pending.iter_mut().enumerate() {
            let Some(pending) = pending.take() else {
                continue;
            };
            if matches!(pending.operation, Operation::Preload) {
                continue;
            }
            // Acquire pairs with the worker's Release publication of its dispatch clock.
            let marker = self.markers[worker].load(Ordering::Acquire);
            let dispatched_ns = marker.saturating_sub(1);
            let dispatched = if marker > 0 {
                self.origin + Duration::from_nanos(dispatched_ns)
            } else {
                pending.scheduled
            };
            let (operation, outcome, members, bytes) = match pending.operation {
                Operation::Read { tx, index } => (
                    "get",
                    "timeout",
                    1,
                    prepared[pending.repetition].reads[tx][index]
                        .row
                        .value
                        .len() as u64,
                ),
                Operation::Commit(phase) => {
                    let batch = &prepared[pending.repetition].batches[phase];
                    (
                        "commit",
                        "unknown_mutation",
                        batch.entries.len() as u64,
                        batch.entries.iter().map(|r| r.value.len() as u64).sum(),
                    )
                }
                Operation::Preload => unreachable!("preload excluded above"),
            };
            let now = Instant::now();
            let mut row = sample(
                operation,
                outcome,
                pending.scheduled,
                dispatched,
                now,
                now,
                self.origin,
                members,
                bytes,
            );
            row.dispatch_observed = marker > 0;
            if marker == 0 {
                row.dispatched_ns = 0;
                row.api_ns = 0;
                row.scheduling_ns = 0;
            }
            row.error_kind = Cow::Borrowed("external_shutdown_deadline_unknown_outcome");
            measurements.calls.push((
                row,
                identity(
                    trace,
                    &prepared[pending.repetition],
                    pending.repetition,
                    pending.operation,
                    members,
                )?,
            ));
        }
        Ok(())
    }
    fn recycle(&mut self, mut completion: Completion) -> BackendResult<()> {
        completion.work.samples.clear();
        self.buffers[completion.worker] = Some(completion.work.samples);
        completion.result
    }
    async fn close(self) -> BackendResult<usize> {
        drop(self.senders);
        drop(self.receiver);
        let mut joined = Vec::with_capacity(self.handles.len());
        let mut unresolved = 0;
        for (handle, buffer) in self.handles.into_iter().zip(self.buffers) {
            if buffer.is_some() || handle.is_finished() {
                joined.push(handle);
            } else {
                // Match runner.rs: only an unresolved call after grace is detached.
                // The failed case requires external process supervision, not cancellation.
                drop(handle);
                unresolved += 1;
            }
        }
        tokio::task::spawn_blocking(move || {
            for handle in joined {
                handle.join().map_err(|_| "replay caller panicked")?;
            }
            Ok(unresolved)
        })
        .await?
    }
}
fn execute_work<B: ReplayBackend + ?Sized>(
    backend: &B,
    prepared: &Prepared,
    work: &mut Work,
    origin: Instant,
    marker: &AtomicU64,
) -> BackendResult<()> {
    match work.operation {
        Operation::Read { tx, index } => {
            let read = &prepared.reads[tx][index];
            let dispatched = Instant::now();
            ensure(
                dispatched < work.deadline,
                "replay get dispatch deadline expired",
            )?;
            // Release publishes the API dispatch clock to the deadline/drain owner.
            marker.store(
                duration_ns(dispatched.saturating_duration_since(origin)) + 1,
                Ordering::Release,
            );
            let actual = backend.get(read.row.table, &read.row.backend_key);
            let completed = Instant::now();
            let outcome = if completed >= work.deadline {
                "timeout"
            } else {
                match &actual {
                    Ok(Some(value)) if *value == read.row.value => "success",
                    Ok(_) => "wrong_value",
                    Err(_) => "error",
                }
            };
            // Compare the full bytes after the API completion clock.
            let released = Instant::now();
            work.samples.push(sample(
                "get",
                outcome,
                work.scheduled,
                dispatched,
                completed,
                released,
                origin,
                1,
                read.row.value.len() as u64,
            ));
            if outcome != "success" {
                return Err(actual
                    .err()
                    .unwrap_or_else(|| format!("replay get {outcome}: tx={tx}").into()));
            }
        }
        Operation::Commit(phase) => {
            let batch = &prepared.batches[phase];
            let dispatched = Instant::now();
            ensure(
                dispatched < work.deadline,
                "replay write dispatch deadline expired",
            )?;
            marker.store(
                duration_ns(dispatched.saturating_duration_since(origin)) + 1,
                Ordering::Release,
            );
            let result = backend.commit(batch.db, &batch.entries);
            let completed = Instant::now();
            let outcome = if completed >= work.deadline
                || result
                    .as_ref()
                    .err()
                    .is_some_and(|e| e.is::<UnknownReplayMutation>())
            {
                "unknown_mutation"
            } else if result.is_ok() {
                "success"
            } else {
                "error"
            };
            work.samples.push(sample(
                "commit",
                outcome,
                work.scheduled,
                dispatched,
                completed,
                completed,
                origin,
                batch.entries.len() as u64,
                batch.entries.iter().map(|r| r.value.len() as u64).sum(),
            ));
            result?;
            ensure(
                outcome == "success",
                "replay write deadline expired; mutation outcome unknown",
            )?;
        }
        Operation::Preload => {
            for chunk in prepared.preload.chunks(256) {
                ensure(
                    Instant::now() < work.deadline,
                    "replay preload deadline expired",
                )?;
                backend.commit(0, chunk)?;
            }
        }
    }
    Ok(())
}

// Match the existing raw-sample clock columns without allocating a per-call context.
#[allow(clippy::too_many_arguments)]
fn sample(
    operation: &'static str,
    outcome: &'static str,
    scheduled: Instant,
    dispatched: Instant,
    completed: Instant,
    released: Instant,
    origin: Instant,
    members: u64,
    bytes: u64,
) -> Sample {
    Sample {
        stream: None,
        phase: "measurement",
        operation,
        outcome,
        caller_sample_id: None,
        scheduled_ns: duration_ns(scheduled.saturating_duration_since(origin)),
        dispatched_ns: duration_ns(dispatched.saturating_duration_since(origin)),
        dispatch_observed: true,
        completed_ns: duration_ns(completed.saturating_duration_since(origin)),
        api_ns: duration_ns(completed.saturating_duration_since(dispatched)),
        response_ns: duration_ns(completed.saturating_duration_since(scheduled)),
        scheduling_ns: duration_ns(dispatched.saturating_duration_since(scheduled)),
        released_ns: duration_ns(released.saturating_duration_since(origin)),
        logical_keys: members,
        bytes,
        error_kind: Cow::Borrowed(if outcome == "success" { "" } else { outcome }),
    }
}
#[derive(Default, Debug, Serialize)]
pub struct Counts {
    pub store_gets: u64,
    pub overlay_demands: u64,
    pub put_members: u64,
    pub write_calls: u64,
    pub checkpoints: u64,
    pub transactions: u64,
}
struct Measurements {
    calls: Vec<(Sample, ReplayIdentity)>,
    counts: Counts,
    dependency_wait_ns: u64,
    max_in_flight: usize,
    cross_overlay_reads: u64,
    cross_store_reads: u64,
}
impl Measurements {
    fn new(trace: &Trace, repetitions: usize) -> BackendResult<Self> {
        let per_checkpoint = trace
            .reads
            .iter()
            .flatten()
            .filter(|r| r.producer.is_none())
            .count()
            .checked_add(trace.batches.len())
            .ok_or("sample count overflow")?;
        let capacity = per_checkpoint
            .checked_mul(repetitions)
            .ok_or("sample count overflow")?;
        Ok(Self {
            calls: Vec::with_capacity(capacity),
            counts: Counts::default(),
            dependency_wait_ns: 0,
            max_in_flight: 0,
            cross_overlay_reads: 0,
            cross_store_reads: 0,
        })
    }

    fn pipeline(&self, depth: usize, readers: usize) -> serde_json::Value {
        serde_json::json!({
            "checkpoint_pipeline_depth": depth,
            "max_in_flight_checkpoints": self.max_in_flight,
            "cross_checkpoint_overlay_reads": self.cross_overlay_reads,
            "cross_checkpoint_store_reads": self.cross_store_reads,
            "read_callers": readers,
            "commit_caller": if depth == 1 { "read caller 0; commits never overlap reads" } else { "one dedicated caller; W=1 serializes every write call" },
            "ordering": PIPELINE_ORDERING,
        })
    }
}

/// Report text of the ordering rules the scheduler enforces.
const PIPELINE_ORDERING: &str = "Sui PipelineStages: stage s of checkpoint r begins after stage s of r-1 and stage s-1 of r end; stages sync(phases 0-4), execute(reads), finalize(phases 5-6), commit_outputs(phase 7), bump_executed(phase 8); at most K checkpoints admitted (buffered(K)), retired in order; reads see earlier in-flight checkpoints' outputs through the dirty overlay until committed; one W=1 commit caller picks the oldest eligible batch";

/// Sui checkpoint-executor pipeline stage that owns a replay batch phase
/// (`checkpoint_executor/utils.rs` `PipelineStage`). Ordered as executed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Stage {
    /// Phases 0-4: state sync stores the certified checkpoint, its contents and the
    /// verified/synced watermarks before `stream_synced_checkpoints` yields it.
    Sync,
    /// ExecuteTransactions + WaitForTransactions: the store reads. Commits no batch.
    Execute,
    /// Phases 5-6: `insert_finalized_transactions` (FinalizeTransactions).
    Finalize,
    /// Phase 7: `commit_transaction_outputs` with the committed watermark (CommitTransactionOutputs).
    CommitOutputs,
    /// Phase 8: highest-executed watermark (BumpHighestExecutedCheckpoint).
    BumpExecuted,
}
fn batch_stage(phase: u16) -> Stage {
    match phase {
        0..=4 => Stage::Sync,
        5 | 6 => Stage::Finalize,
        7 => Stage::CommitOutputs,
        _ => Stage::BumpExecuted,
    }
}

/// Event after which a cross-checkpoint read's value exists.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Gate {
    /// The earlier checkpoint's producing transaction executed: its phase-7 outputs are in
    /// Sui's writeback dirty set before `commit_transaction_outputs`.
    Publish { repetition: usize, tx: usize },
    /// Any other post-execution write (finalize rows, checkpoint-level rows) is visible
    /// only once its batch commits.
    Commit { repetition: usize, batch: usize },
}
/// A store read whose key an earlier checkpoint inside the pipeline window writes.
#[derive(Debug)]
struct CrossRead {
    /// Window writer batches `(repetition, batch index)`; all commit in one stage, in order.
    writers: Vec<(usize, usize)>,
    /// Latest writer's value: Sui reads the newest version, dirty or committed.
    value: tidehunter::minibytes::Bytes,
}
/// Cross-checkpoint data dependencies for K>1, computed before measurement. Sparse:
/// per-repetition keys are distinct for every non-control table, so the v1 fixture has none.
#[derive(Debug, Default)]
struct CrossLinks {
    reads: BTreeMap<(usize, usize, usize), CrossRead>,
    gates: BTreeMap<(usize, usize), Vec<Gate>>,
    waiters: BTreeMap<Gate, Vec<(usize, usize)>>,
}
struct Writer<'a> {
    repetition: usize,
    batch: usize,
    stage: Stage,
    row: &'a Row,
}
impl CrossLinks {
    /// Link every store read to the writers of its key among the previous K-1 checkpoints.
    /// Checkpoints at least K apart never overlap: checkpoint r is admitted only after r-K retired.
    fn new(trace: &Trace, prepared: &[Prepared], depth: usize) -> BackendResult<Self> {
        let mut links = Self::default();
        if depth <= 1 {
            return Ok(links);
        }
        let mut window: HashMap<(u16, &[u8]), Vec<Writer<'_>>> = HashMap::new();
        for (repetition, p) in prepared.iter().enumerate() {
            if let Some(old) = repetition.checked_sub(depth) {
                for row in prepared[old].batches.iter().flat_map(|b| &b.entries) {
                    if let std::collections::hash_map::Entry::Occupied(mut writers) =
                        window.entry((row.table, &row.key[..]))
                    {
                        writers.get_mut().retain(|w| w.repetition != old);
                        if writers.get().is_empty() {
                            writers.remove();
                        }
                    }
                }
            }
            for (batch, b) in p.batches.iter().enumerate() {
                let stage = batch_stage(b.phase);
                for row in &b.entries {
                    let writers = window.entry((row.table, &row.key[..])).or_default();
                    // Same-stage writes commit in checkpoint order; writes of one key in two
                    // stages by overlapping checkpoints could commit out of sequential order.
                    ensure(
                        writers
                            .iter()
                            .all(|w| w.repetition == repetition || w.stage == stage),
                        "cross-checkpoint write-write conflict across pipeline stages",
                    )?;
                    writers.push(Writer {
                        repetition,
                        batch,
                        stage,
                        row,
                    });
                }
            }
            for (tx, reads) in p.reads.iter().enumerate() {
                let mut gates = Vec::new();
                for (index, read) in reads.iter().enumerate() {
                    if read.producer.is_some() {
                        continue;
                    }
                    let Some(writers) = window.get(&(read.row.table, &read.row.key[..])) else {
                        continue;
                    };
                    // Own sync writes and earlier sync writes commit before this execution.
                    if writers
                        .iter()
                        .any(|w| w.repetition == repetition && w.stage == Stage::Sync)
                    {
                        continue;
                    }
                    let Some(latest) = writers.iter().rev().find(|w| w.repetition < repetition)
                    else {
                        continue;
                    };
                    if latest.stage == Stage::Sync {
                        continue;
                    }
                    // Only transaction outputs (phase 7 rows of a transaction) sit in Sui's
                    // writeback dirty set after execution; every other write is visible
                    // once its batch commits.
                    let producer = latest.row.tx as usize;
                    gates.push(
                        if latest.stage == Stage::CommitOutputs && producer < trace.transactions {
                            Gate::Publish {
                                repetition: latest.repetition,
                                tx: producer,
                            }
                        } else {
                            Gate::Commit {
                                repetition: latest.repetition,
                                batch: latest.batch,
                            }
                        },
                    );
                    links.reads.insert(
                        (repetition, tx, index),
                        CrossRead {
                            writers: writers
                                .iter()
                                .filter(|w| w.repetition < repetition)
                                .map(|w| (w.repetition, w.batch))
                                .collect(),
                            value: latest.row.value.clone(),
                        },
                    );
                }
                gates.sort_unstable();
                gates.dedup();
                for &gate in &gates {
                    links
                        .waiters
                        .entry(gate)
                        .or_default()
                        .push((repetition, tx));
                }
                if !gates.is_empty() {
                    links.gates.insert((repetition, tx), gates);
                }
            }
        }
        Ok(links)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Execution {
    /// Admitted; sync commits or the previous checkpoint's execution start pending.
    Waiting,
    /// Transactions enqueued at this clock.
    Running(Instant),
    /// Every transaction executed and the previous checkpoint's execution ended.
    Ended,
}
/// Per in-flight checkpoint state; slot `r % K` belongs to repetition r.
struct Slot {
    repetition: usize,
    next_batch: usize,
    execution: Execution,
    published: usize,
    completed: Vec<bool>,
    remaining: Vec<usize>,
    ready_at: Vec<Option<Instant>>,
    read_index: Vec<usize>,
}
/// Owns all pipeline state on the scheduler task; workers only see dispatched `Work`.
/// Read callers `0..readers`; commits use caller 0 when K=1 (reads are idle by
/// construction) and the extra caller `readers` when K>1, so read parallelism stays N.
/// The ready heap is preallocated for K checkpoints' transactions, one entry each.
struct Scheduler<'a> {
    trace: &'a Trace,
    prepared: &'a [Prepared],
    links: &'a CrossLinks,
    depth: usize,
    readers: usize,
    commit_worker: usize,
    transactions: usize,
    deadline: Instant,
    dependents: Vec<Vec<usize>>,
    slots: Vec<Slot>,
    ready: BinaryHeap<Reverse<(usize, usize)>>,
    idle: Vec<usize>,
    active_reads: usize,
    committing: bool,
    admitted: usize,
    retired: usize,
}
/// Pool callers for N readers at depth K.
fn callers(readers: usize, depth: usize) -> usize {
    readers + usize::from(depth > 1)
}
impl<'a> Scheduler<'a> {
    fn new(
        trace: &'a Trace,
        prepared: &'a [Prepared],
        links: &'a CrossLinks,
        readers: usize,
        depth: usize,
        deadline: Instant,
    ) -> BackendResult<Self> {
        ensure(
            depth >= 1 && readers >= 1,
            "replay needs one checkpoint and caller",
        )?;
        let mut dependents = vec![Vec::new(); trace.transactions];
        for (consumer, producers) in trace.dependencies.iter().enumerate() {
            for &producer in producers {
                dependents[producer].push(consumer);
            }
        }
        let n = trace.transactions;
        let slots = (0..depth)
            .map(|_| Slot {
                repetition: usize::MAX,
                next_batch: 0,
                execution: Execution::Waiting,
                published: 0,
                completed: vec![false; n],
                remaining: vec![0; n],
                ready_at: vec![None; n],
                read_index: vec![0; n],
            })
            .collect();
        Ok(Self {
            trace,
            prepared,
            links,
            depth,
            readers,
            commit_worker: if depth == 1 { 0 } else { readers },
            // Without selected reads there is no execution stage (no transactions published).
            transactions: if trace.reads.iter().any(|reads| !reads.is_empty()) {
                n
            } else {
                0
            },
            deadline,
            dependents,
            slots,
            ready: BinaryHeap::with_capacity(n.saturating_mul(depth)),
            idle: Vec::with_capacity(readers),
            active_reads: 0,
            committing: false,
            admitted: 0,
            retired: 0,
        })
    }

    fn slot(&self, repetition: usize) -> &Slot {
        &self.slots[repetition % self.depth]
    }

    /// The slot of an admitted, unretired repetition.
    fn live(&self, repetition: usize) -> Option<&Slot> {
        let slot = self.slot(repetition);
        (repetition >= self.retired && repetition < self.admitted && slot.repetition == repetition)
            .then_some(slot)
    }

    fn stage_ended(&self, repetition: usize, stage: Stage) -> bool {
        let Some(slot) = self.live(repetition) else {
            return repetition < self.retired;
        };
        (stage < Stage::Execute || slot.execution == Execution::Ended)
            && self.prepared[repetition]
                .batches
                .get(slot.next_batch)
                .is_none_or(|batch| batch_stage(batch.phase) > stage)
    }

    fn committed(&self, repetition: usize, batch: usize) -> bool {
        repetition < self.retired || self.live(repetition).is_some_and(|s| s.next_batch > batch)
    }

    fn gate_open(&self, gate: Gate) -> bool {
        match gate {
            Gate::Publish { repetition, tx } => {
                repetition < self.retired
                    || self
                        .live(repetition)
                        .is_some_and(|s| s.execution != Execution::Waiting && s.completed[tx])
            }
            Gate::Commit { repetition, batch } => self.committed(repetition, batch),
        }
    }

    /// Make `tx` of `repetition` ready if it was waiting only on this decrement. Each
    /// producer and gate completes once and edges are unique, so each indegree unit is
    /// decremented once; the preallocated heap holds at most one entry per transaction.
    fn release_one(
        slot: &mut Slot,
        ready: &mut BinaryHeap<Reverse<(usize, usize)>>,
        repetition: usize,
        tx: usize,
        now: Instant,
    ) {
        slot.remaining[tx] -= 1;
        if slot.remaining[tx] == 0 {
            slot.ready_at[tx] = Some(now);
            ready.push(Reverse((repetition, tx)));
        }
    }

    /// Readers counted this gate as closed only if their execution began before it opened.
    fn release(&mut self, gate: Gate, now: Instant) {
        let links = self.links;
        let Some(waiters) = links.waiters.get(&gate) else {
            return;
        };
        for &(repetition, tx) in waiters {
            if self
                .live(repetition)
                .is_some_and(|s| matches!(s.execution, Execution::Running(_)))
            {
                Self::release_one(
                    &mut self.slots[repetition % self.depth],
                    &mut self.ready,
                    repetition,
                    tx,
                    now,
                );
            }
        }
    }

    fn publish(&mut self, repetition: usize, tx: usize, now: Instant, m: &mut Measurements) {
        let index = repetition % self.depth;
        self.slots[index].completed[tx] = true;
        self.slots[index].published += 1;
        m.counts.transactions += 1;
        for &consumer in &self.dependents[tx] {
            Self::release_one(
                &mut self.slots[index],
                &mut self.ready,
                repetition,
                consumer,
                now,
            );
        }
        if !self.links.waiters.is_empty() {
            self.release(Gate::Publish { repetition, tx }, now);
        }
    }

    /// Sui ExecuteTransactions: enqueue every transaction; roots are ready now.
    fn begin_execution(&mut self, repetition: usize) {
        if self.active_reads == 0 {
            self.idle.clear();
            self.idle.extend((0..self.readers).rev());
        }
        let index = repetition % self.depth;
        let slot = &mut self.slots[index];
        slot.completed.fill(false);
        slot.ready_at.fill(None);
        slot.read_index.fill(0);
        slot.published = 0;
        let start = Instant::now();
        slot.execution = Execution::Running(start);
        if self.transactions == 0 {
            return;
        }
        let links = self.links;
        let trace = self.trace;
        for (tx, producers) in trace.dependencies.iter().enumerate() {
            let closed = links.gates.get(&(repetition, tx)).map_or(0, |gates| {
                gates.iter().filter(|gate| !self.gate_open(**gate)).count()
            });
            let slot = &mut self.slots[index];
            slot.remaining[tx] = producers.len() + closed;
            if slot.remaining[tx] == 0 {
                slot.ready_at[tx] = Some(start);
                self.ready.push(Reverse((repetition, tx)));
            }
        }
    }

    /// Retire, admit and move execution stages until nothing changes.
    fn advance(&mut self, m: &mut Measurements) -> bool {
        let mut progressed = false;
        loop {
            let mut changed = false;
            // Sui yields `.buffered(K)` outputs in order; bump_executed already orders them.
            while self.retired < self.admitted {
                let slot = self.slot(self.retired);
                if slot.execution != Execution::Ended
                    || slot.next_batch < self.prepared[self.retired].batches.len()
                {
                    break;
                }
                m.counts.checkpoints += 1;
                self.retired += 1;
                changed = true;
            }
            while self.admitted < self.prepared.len() && self.admitted < self.retired + self.depth {
                let slot = &mut self.slots[self.admitted % self.depth];
                slot.repetition = self.admitted;
                slot.next_batch = 0;
                slot.execution = Execution::Waiting;
                slot.published = 0;
                self.admitted += 1;
                m.max_in_flight = m.max_in_flight.max(self.admitted - self.retired);
                changed = true;
            }
            for repetition in self.retired..self.admitted {
                let previous = (repetition > self.retired).then(|| self.slot(repetition - 1));
                let previous_started = previous.is_none_or(|s| s.execution != Execution::Waiting);
                let previous_ended = previous.is_none_or(|s| s.execution == Execution::Ended);
                let (execution, published) = {
                    let slot = self.slot(repetition);
                    (slot.execution, slot.published)
                };
                match execution {
                    Execution::Waiting
                        if previous_started && self.stage_ended(repetition, Stage::Sync) =>
                    {
                        self.begin_execution(repetition);
                        changed = true;
                    }
                    Execution::Running(_) if previous_ended && published == self.transactions => {
                        self.slots[repetition % self.depth].execution = Execution::Ended;
                        changed = true;
                    }
                    _ => {}
                }
            }
            if !changed {
                return progressed;
            }
            progressed = true;
        }
    }

    /// Dispatch the oldest eligible batch on the single commit caller.
    fn dispatch_commit(&mut self, pool: &mut Pool) -> BackendResult<bool> {
        if self.committing {
            return Ok(false);
        }
        for repetition in self.retired..self.admitted {
            let slot = self.slot(repetition);
            let Some(batch) = self.prepared[repetition].batches.get(slot.next_batch) else {
                continue;
            };
            let stage = batch_stage(batch.phase);
            if (stage > Stage::Execute && slot.execution != Execution::Ended)
                || (repetition > self.retired && !self.stage_ended(repetition - 1, stage))
            {
                continue;
            }
            ensure(
                self.depth > 1 || self.active_reads == 0,
                "sequential replay commit overlaps reads",
            )?;
            let operation = Operation::Commit(slot.next_batch);
            pool.dispatch(
                self.commit_worker,
                repetition,
                operation,
                Instant::now(),
                self.deadline,
            )?;
            self.committing = true;
            return Ok(true);
        }
        Ok(false)
    }

    /// First store read at or after `from`. A cross-checkpoint read with an uncommitted
    /// window writer is served from that checkpoint's dirty output instead of the store.
    fn next_store_read(
        &self,
        repetition: usize,
        tx: usize,
        from: usize,
        m: &mut Measurements,
    ) -> BackendResult<Option<usize>> {
        let reads = &self.prepared[repetition].reads[tx];
        for (index, read) in reads.iter().enumerate().skip(from) {
            if read.producer.is_some() {
                continue;
            }
            if !self.links.reads.is_empty()
                && let Some(cross) = self.links.reads.get(&(repetition, tx, index))
            {
                if !cross.writers.iter().all(|&(r, b)| self.committed(r, b)) {
                    ensure(
                        cross.value == read.row.value,
                        "cross-checkpoint overlay missing/wrong value",
                    )?;
                    m.counts.overlay_demands += 1;
                    m.cross_overlay_reads += 1;
                    continue;
                }
                m.cross_store_reads += 1;
            }
            return Ok(Some(index));
        }
        Ok(None)
    }

    /// Hand ready transactions to idle read callers, oldest checkpoint first.
    fn dispatch_reads(&mut self, pool: &mut Pool, m: &mut Measurements) -> BackendResult<bool> {
        let mut progressed = false;
        while let Some(worker) = self.idle.pop() {
            let Some(Reverse((repetition, tx))) = self.ready.pop() else {
                self.idle.push(worker);
                break;
            };
            progressed = true;
            let index = repetition % self.depth;
            let Execution::Running(start) = self.slots[index].execution else {
                return Err("ready transaction outside its execution stage".into());
            };
            let ready =
                self.slots[index].ready_at[tx].ok_or("ready transaction lacks arrival clock")?;
            let prepared = &self.prepared[repetition];
            for (position, read) in prepared.reads[tx].iter().enumerate() {
                if let Some(producer) = read.producer {
                    ensure(
                        self.slots[index].completed[producer],
                        "dirty overlay producer not published",
                    )?;
                    ensure(
                        prepared.overlay[tx][position].as_ref() == Some(&read.row.value),
                        "dirty overlay missing/wrong value",
                    )?;
                    m.counts.overlay_demands += 1;
                }
            }
            m.dependency_wait_ns += duration_ns(ready.saturating_duration_since(start));
            if let Some(position) = self.next_store_read(repetition, tx, 0, m)? {
                self.slots[index].read_index[tx] = position;
                pool.dispatch(
                    worker,
                    repetition,
                    Operation::Read {
                        tx,
                        index: position,
                    },
                    ready,
                    self.deadline,
                )?;
                self.active_reads += 1;
            } else {
                self.publish(repetition, tx, Instant::now(), m);
                self.idle.push(worker);
            }
        }
        Ok(progressed)
    }

    fn schedule(&mut self, pool: &mut Pool, m: &mut Measurements) -> BackendResult<()> {
        // Each pass retires, admits, dispatches or publishes; finite work bounds the loop.
        loop {
            let mut progressed = self.advance(m);
            progressed |= self.dispatch_commit(pool)?;
            progressed |= self.dispatch_reads(pool, m)?;
            if !progressed {
                return Ok(());
            }
        }
    }

    async fn run(mut self, pool: &mut Pool, m: &mut Measurements) -> BackendResult<()> {
        loop {
            self.schedule(pool, m)?;
            if self.retired == self.prepared.len() {
                return Ok(());
            }
            ensure(
                self.active_reads > 0 || self.committing,
                "replay dependency cycle",
            )?;
            let completion = pool.receive(self.deadline).await?;
            let repetition = completion.work.repetition;
            observe_completion(
                self.trace,
                &self.prepared[repetition],
                &completion,
                &mut m.calls,
                &mut m.counts,
            )?;
            let worker = completion.worker;
            let operation = completion.work.operation;
            pool.recycle(completion)?;
            let index = repetition % self.depth;
            match operation {
                Operation::Read { tx, .. } => {
                    let from = self.slots[index].read_index[tx] + 1;
                    if let Some(position) = self.next_store_read(repetition, tx, from, m)? {
                        self.slots[index].read_index[tx] = position;
                        pool.dispatch(
                            worker,
                            repetition,
                            Operation::Read {
                                tx,
                                index: position,
                            },
                            Instant::now(),
                            self.deadline,
                        )?;
                    } else {
                        self.publish(repetition, tx, Instant::now(), m);
                        self.active_reads -= 1;
                        self.idle.push(worker);
                    }
                }
                Operation::Commit(batch) => {
                    ensure(
                        self.slots[index].next_batch == batch,
                        "replay commit completed out of order",
                    )?;
                    self.slots[index].next_batch += 1;
                    self.committing = false;
                    if !self.links.waiters.is_empty() {
                        self.release(Gate::Commit { repetition, batch }, Instant::now());
                    }
                }
                Operation::Preload => return Err("unexpected replay completion".into()),
            }
        }
    }
}
fn identity(
    trace: &Trace,
    prepared: &Prepared,
    repetition: usize,
    operation: Operation,
    members: u64,
) -> BackendResult<ReplayIdentity> {
    let (source_tx, db, table, batch_phase, parent_batch) = match operation {
        Operation::Read { tx, .. } => (Some(u32::try_from(tx)?), 0, Some(13), None, None),
        Operation::Commit(phase) => (
            None,
            prepared.batches[phase].db,
            None,
            Some(prepared.batches[phase].phase),
            Some(repetition as u64 * 9 + u64::from(prepared.batches[phase].phase)),
        ),
        Operation::Preload => return Err("preload has no measurement identity".into()),
    };
    Ok(ReplayIdentity {
        source_checkpoint: trace.sequence,
        source_tx,
        repetition: repetition as u64,
        db,
        table,
        batch_phase,
        parent_batch,
        members,
    })
}

fn observe_completion(
    trace: &Trace,
    prepared: &Prepared,
    completion: &Completion,
    calls: &mut Vec<(Sample, ReplayIdentity)>,
    counts: &mut Counts,
) -> BackendResult<()> {
    for row in &completion.work.samples {
        calls.push((
            row.clone(),
            identity(
                trace,
                prepared,
                completion.work.repetition,
                completion.work.operation,
                row.logical_keys,
            )?,
        ));
        if row.outcome == "success" {
            match completion.work.operation {
                Operation::Read { .. } => counts.store_gets += 1,
                Operation::Commit(_) => {
                    counts.write_calls += 1;
                    counts.put_members += row.logical_keys;
                }
                Operation::Preload => {}
            }
        }
    }
    Ok(())
}

/// Native fixture evidence and tests use this same scheduler, not a mock execution path.
pub async fn execute<B: ReplayBackend + ?Sized>(
    backend: Arc<B>,
    trace: &Trace,
    prepared: Arc<Vec<Prepared>>,
    concurrency: usize,
    deadline: Instant,
    timeout_ms: u64,
) -> BackendResult<Counts> {
    Ok(replay(
        backend,
        trace,
        prepared,
        concurrency,
        1,
        deadline,
        timeout_ms,
    )
    .await?
    .counts)
}

/// Preload, then replay every repetition at pipeline depth `depth`.
async fn replay<B: ReplayBackend + ?Sized>(
    backend: Arc<B>,
    trace: &Trace,
    prepared: Arc<Vec<Prepared>>,
    concurrency: usize,
    depth: usize,
    deadline: Instant,
    timeout_ms: u64,
) -> BackendResult<Measurements> {
    let links = CrossLinks::new(trace, &prepared, depth)?;
    let origin = Instant::now();
    let mut pool = Pool::new(
        Arc::clone(&backend),
        Arc::clone(&prepared),
        callers(concurrency, depth),
        origin,
        timeout_ms,
    )?;
    let mut m = Measurements::new(trace, prepared.len())?;
    let result: BackendResult<()> = async {
        for rep in 0..prepared.len() {
            pool.dispatch(0, rep, Operation::Preload, Instant::now(), deadline)?;
            let c = pool.receive(deadline).await?;
            pool.recycle(c)?;
        }
        backend.check_background_health()?;
        Scheduler::new(trace, &prepared, &links, concurrency, depth, deadline)?
            .run(&mut pool, &mut m)
            .await?;
        backend.check_background_health()?;
        Ok(())
    }
    .await;
    if result.is_err() {
        pool.drain(
            trace,
            &prepared,
            &mut m,
            Instant::now() + Duration::from_secs(1),
        )
        .await?;
    }
    pool.close().await?;
    backend.check_background_health()?;
    result?;
    Ok(m)
}

/// Independent final-state oracle: every preload (all precede measurement), then every
/// prepared batch in sequential checkpoint-then-phase order, regardless of K or of the
/// observed commit order. Exact under pipelining because `CrossLinks` rejects keys that
/// overlapping checkpoints write in different stages, and same-stage writes commit in
/// checkpoint order; a skipped, lost or reordered same-key commit fails verification.
fn expected_final_rows(prepared: &[Prepared]) -> FinalRows {
    let mut expected = FinalRows::new();
    for row in prepared.iter().flat_map(|p| &p.preload) {
        expected.insert((row.table, row.key.clone()), row.clone());
    }
    for row in prepared
        .iter()
        .flat_map(|p| p.batches.iter().flat_map(|b| &b.entries))
    {
        expected.insert((row.table, row.key.clone()), row.clone());
    }
    expected
}
fn now_unix() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0.0, |d| d.as_secs_f64())
}

/// Native WALs grow by fragment (`wal/mod.rs::extend_to_map_id`), not by
/// preallocating whole 10 GiB files. The R=5456 native_default evidence used
/// 4,876,845,056 physical bytes for 4,780,683,600 key/value payload bytes:
/// 1.0201146x. Reserve 2x payload (96.1% above that observed amplification)
/// for framing/index growth, plus one whole fragment per open value/index WAL
/// in each of the three node domains. This is a conservative empirical model,
/// not a measured physical-I/O count or a universal engine amplification bound.
fn native_disk_reservation(payload: u64, fragment_size: u64) -> BackendResult<(u64, u64)> {
    let fragments = fragment_size
        .checked_mul(6)
        .ok_or("replay fragment reservation overflow")?;
    let reserve = payload
        .checked_mul(2)
        .and_then(|n| n.checked_add(fragments))
        .ok_or("replay disk reservation overflow")?;
    Ok((reserve, fragments))
}

fn resource_estimate(config: &ReplayConfig, trace: &Trace) -> BackendResult<serde_json::Value> {
    let geometry = if config.backend.kind == "native" {
        NativeReplayBackend::geometry(&config.backend.options)?
    } else if config.backend.kind == "tidehunter_tcp" {
        TidehunterReplayBackend::geometry(&config.backend.options)?
    } else {
        serde_json::json!({"wal_file_size":0,"admission_path":"."})
    };
    let payload = trace
        .preload
        .iter()
        .chain(trace.batches.iter().flat_map(|b| &b.entries))
        .map(|r| (r.key.len() + r.value.len()) as u64)
        .sum::<u64>()
        .checked_mul(config.workload.repetitions)
        .ok_or("replay disk bound overflow")?;
    let rows =
        (trace.preload.len() + trace.batches.iter().map(|b| b.entries.len()).sum::<usize>()) as u64;
    let index = rows
        .checked_mul(config.workload.repetitions)
        .and_then(|n| n.checked_mul(128))
        .ok_or("replay index bound overflow")?;
    let network_store = !matches!(config.backend.kind.as_str(), "native" | "tidehunter_tcp");
    let coordinator = if config.backend.kind == "tidehunter_tcp" {
        TidehunterReplayBackend::coordinator_reservation(
            &config.backend.options,
            trace,
            config.workload.repetitions,
        )?
    } else {
        serde_json::Value::Null
    };
    let (reserve, wal) = if network_store {
        (0, 0)
    } else if config.backend.kind == "native" {
        let fragment_size = geometry["fragment_size"]
            .as_u64()
            .ok_or("missing native fragment geometry")?;
        native_disk_reservation(payload, fragment_size)?
    } else {
        // Leased replay owners are external to the coordinator disk budget.
        (
            coordinator["reservation_bytes"]
                .as_u64()
                .ok_or("missing distributed coordinator disk reservation")?,
            0,
        )
    };
    // Include immutable row views, retained raw samples, key-map nodes and key owners
    // at peak expansion. Values stay shared with the two input buffers.
    let demands = trace.reads.iter().map(Vec::len).sum::<usize>() as u64;
    let calls = trace
        .reads
        .iter()
        .flatten()
        .filter(|r| r.producer.is_none())
        .count() as u64
        + trace.batches.len() as u64;
    let source: std::collections::BTreeSet<_> = trace
        .preload
        .iter()
        .chain(trace.batches.iter().flat_map(|b| &b.entries))
        .chain(trace.reads.iter().flatten().map(|r| &r.row))
        .map(|r| (r.table, &r.key))
        .collect();
    let per_repeat = rows * std::mem::size_of::<Row>() as u64
        + demands
            * (std::mem::size_of::<Read>()
                + std::mem::size_of::<Option<tidehunter::minibytes::Bytes>>()) as u64
        + calls * (std::mem::size_of::<Sample>() + std::mem::size_of::<ReplayIdentity>()) as u64
        + source.len() as u64 * 512;
    let memory = per_repeat
        .checked_mul(config.workload.repetitions)
        .and_then(|n| n.checked_add(index))
        .ok_or("replay memory bound overflow")?;
    Ok(
        serde_json::json!({"estimated_disk_reservation_bytes":reserve,
        "estimated_memory_reservation_bytes":memory,
        "admission_path":geometry["admission_path"].as_str()
            .or_else(|| config.backend.options["path"].as_str())
            .ok_or("replay admission path is required")?,
        "host_resources":"not_checked; enforced at run time on the executing host",
        "payload_key_value_bytes":payload,"index_reservation_bytes":index,"wal_fragment_reservation_bytes":wal,
        "coordinator_disk_reservation":coordinator,
        "disk_payload_amplification_factor":if config.backend.kind == "native" {2} else {0},
        "reservation_model":if network_store {"client memory admission only; remote engine/disk budgets require the exact-owned lifecycle receipt"} else if config.backend.kind == "native" {"2x key/value payload for framing/index amplification + six open WAL fragments; empirical conservative native estimate, not physical I/O measurement; memory additionally reserves 128 bytes per row for indexes"} else {coordinator["model"].as_str().ok_or("missing coordinator disk reservation model")?}}),
    )
}

fn check_host_resources(
    config: &ReplayConfig,
    resources: &mut serde_json::Value,
) -> BackendResult<()> {
    let reserve = resources["estimated_disk_reservation_bytes"]
        .as_u64()
        .ok_or("missing replay disk estimate")?;
    let memory = resources["estimated_memory_reservation_bytes"]
        .as_u64()
        .ok_or("missing replay memory estimate")?;
    if config.backend.kind == "tidehunter_tcp" {
        TidehunterReplayBackend::validate_disk_paths(&config.backend.options, reserve)?;
    }
    let mut path = PathBuf::from(
        resources["admission_path"]
            .as_str()
            .ok_or("replay admission path is required")?,
    );
    while !path.exists() {
        if !path.pop() {
            path = PathBuf::from(".");
            break;
        }
    }
    let output = std::process::Command::new("df")
        .args(["-B1", "--output=avail", "--"])
        .arg(path)
        .output()?;
    ensure(output.status.success(), "replay free-space check failed")?;
    let available = String::from_utf8(output.stdout)?
        .lines()
        .last()
        .ok_or("missing free-space result")?
        .trim()
        .parse::<u64>()?;
    ensure(
        reserve <= available,
        "insufficient native replay disk space, including payload amplification and open WAL fragments",
    )?;
    let meminfo = std::fs::read_to_string("/proc/meminfo")?;
    let memory_available = meminfo
        .lines()
        .find_map(|line| line.strip_prefix("MemAvailable:"))
        .and_then(|line| line.split_whitespace().next())
        .ok_or("MemAvailable not reported")?
        .parse::<u64>()?
        .checked_mul(1024)
        .ok_or("available-memory arithmetic overflow")?;
    ensure(
        memory <= memory_available,
        "insufficient memory for bounded replay expansion and retained exact samples",
    )?;
    resources["available_bytes"] = available.into();
    resources["memory_available_bytes"] = memory_available.into();
    resources["host_resources"] = "checked on the executing host".into();
    Ok(())
}

pub async fn run(config: ReplayConfig, output: &Path) -> BackendResult<()> {
    // Hash the executable before starting the bounded replay, never after a timed-out call.
    let binary = crate::runner::binary_identity();
    let started_unix = now_unix();
    let started = Instant::now();
    let (trace, mut prepared, mut resources) = config.admit()?;
    resources["binary"] = binary;
    let (backend, info) = open_backend(&config, &trace).await?;
    resources["prepared_placement"] = backend.prepare_case(&mut prepared)?;
    let prepared = Arc::new(prepared);
    run_prepared(
        config,
        output,
        trace,
        prepared,
        backend,
        resources,
        (started, started_unix, Some(info)),
    )
    .await
}

pub(crate) async fn run_prepared<B: ReplayBackend + ?Sized>(
    config: ReplayConfig,
    output: &Path,
    trace: Trace,
    prepared: Arc<Vec<Prepared>>,
    backend: Arc<B>,
    resources: serde_json::Value,
    clock: (Instant, f64, Option<BackendInfo>),
) -> BackendResult<()> {
    let (started, started_unix, info) = clock;
    let info = match info {
        Some(info) => info,
        None => native_info(&config.backend.options)?,
    };
    let deadline = started + Duration::from_secs(config.limits.deadline_seconds);
    let capability = backend.commit_capability().labels(&info)?;
    let depth = config.pipeline_depth();
    // Cold, before preload and measurement; empty when K=1.
    let links = CrossLinks::new(&trace, &prepared, depth)?;
    let mut reporter = Reporter::create(
        output,
        prometheus::Registry::new(),
        config.backend.kind.clone(),
    )?;
    let mut pool = Pool::new(
        Arc::clone(&backend),
        Arc::clone(&prepared),
        callers(config.execution.concurrency, depth),
        started,
        config.execution.request_timeout_ms,
    )?;
    let mut m = Measurements::new(&trace, prepared.len())?;
    let preload_start = Instant::now();
    let mut preload_ns = 0;
    let mut window_start = None;
    let mut elapsed = Duration::ZERO;
    let mut physical_before = serde_json::Value::Null;
    let mut physical_after = serde_json::Value::Null;
    let mut result: BackendResult<()> = async {
        for rep in 0..prepared.len() {
            pool.dispatch(0, rep, Operation::Preload, Instant::now(), deadline)?;
            let c = pool.receive(deadline).await?;
            pool.recycle(c)?;
        }
        preload_ns = duration_ns(preload_start.elapsed());
        if let Some(gate) = &config.measurement_gate {
            crate::runner::wait_for_measurement_gate(gate, deadline).await?;
        }
        backend.check_background_health()?;
        physical_before = backend.physical_counts();
        reporter.set_phase("measurement");
        let begin = Instant::now();
        window_start = Some(begin);
        Scheduler::new(
            &trace,
            &prepared,
            &links,
            config.execution.concurrency,
            depth,
            deadline,
        )?
        .run(&mut pool, &mut m)
        .await?;
        elapsed = begin.elapsed();
        physical_after = backend.physical_counts();
        backend.check_background_health()?;
        if let Some(gate) = &config.measurement_completion_gate {
            crate::runner::wait_for_measurement_gate(gate, deadline).await?;
        }
        ensure(
            elapsed.as_secs_f64() >= config.workload.minimum_measurement_seconds,
            "replay measurement shorter than required minimum; finite keys are not reused",
        )
    }
    .await;
    if let Some(begin) = window_start
        && elapsed.is_zero()
    {
        elapsed = begin.elapsed();
    }
    if result.is_err() {
        pool.drain(
            &trace,
            &prepared,
            &mut m,
            Instant::now() + Duration::from_secs(config.limits.shutdown_grace_seconds),
        )
        .await?;
    }
    let unresolved_workers = pool.close().await?;
    let mut final_verification = serde_json::Value::Null;
    if result.is_ok() {
        let expected = expected_final_rows(&prepared);
        let verifier = Arc::clone(&backend);
        match tokio::task::spawn_blocking(move || verifier.verify_final(&expected, deadline))
            .await?
        {
            Ok(receipt) => final_verification = receipt,
            Err(error) => result = Err(error),
        }
    }
    for (s, id) in &m.calls {
        reporter.offered(s.phase, s.operation, s.logical_keys);
        reporter.record_replay(s.clone(), id.clone()).await?;
    }
    if let Err(error) = backend.check_background_health() {
        result = match result {
            Ok(()) => Err(error),
            Err(previous) => {
                Err(format!("{previous}; final background health failed: {error}").into())
            }
        };
    }
    let metrics = profile(
        &m,
        &trace,
        &prepared,
        window_start.map_or(0, |s| duration_ns(s.saturating_duration_since(started))),
        elapsed,
    );
    let mut metrics = metrics;
    metrics["replay_contract"] = capability;
    metrics["physical"] = serde_json::json!({"before_measurement":physical_before,"after_measurement":physical_after,"including_preload":backend.physical_counts()});
    metrics["write_amplification"] = write_amplification(
        &m,
        &prepared,
        &physical_before,
        &physical_after,
        result.is_ok(),
    );
    metrics["final_verification"] = final_verification;
    metrics["functional_qualification"] =
        serde_json::json!(result.is_ok() && unresolved_workers == 0);
    metrics["minimum_measurement_seconds"] =
        serde_json::json!(config.workload.minimum_measurement_seconds);
    metrics["logical_key_bytes"] = serde_json::json!(
        prepared
            .iter()
            .flat_map(|p| p.batches.iter().flat_map(|b| &b.entries))
            .map(|r| r.key.len() as u64)
            .sum::<u64>()
    );
    metrics["client_physical_key_bytes"] = serde_json::json!(
        prepared
            .iter()
            .flat_map(|p| p.batches.iter().flat_map(|b| &b.entries))
            .map(|r| r.backend_key.len() as u64)
            .sum::<u64>()
    );
    if config.backend.kind == "tidehunter_tcp"
        && config.workload.node_databases.as_deref() == Some(&[0])
    {
        let evidence = crate::runner::measurement_read_evidence(
            &physical_before,
            &physical_after,
            window_start.is_some(),
            result.is_ok(),
        );
        std::fs::write(
            output.join("measurement-read-evidence.json"),
            serde_json::to_vec(&evidence)?,
        )?;
    }
    metrics["topology"] = info.topology.clone();
    metrics["case_identity"] = serde_json::json!({"scope":trace.scope(),"fixture_sha256":config.workload.manifest_sha256,"source":trace.manifest["source"],"repeat_domain":std::str::from_utf8(crate::mainnet_trace::DOMAIN)?,"repeat_version":1,"repetitions":prepared.len(),"N":config.execution.concurrency,"K":depth,"W":1,"table_mapping":trace.tables,"device_counters":device_counters_scope()});
    metrics["pipeline"] = m.pipeline(depth, config.execution.concurrency);
    let unknown_outcomes = m
        .calls
        .iter()
        .filter(|(sample, _)| matches!(sample.outcome, "timeout" | "unknown_mutation"))
        .count()
        .max(unresolved_workers);
    let run_status = if result.is_ok() {
        "completed"
    } else if unknown_outcomes > 0 || unresolved_workers > 0 {
        "timed_out"
    } else {
        "failed"
    };
    let summary=RunSummary {status:if result.is_ok(){"completed"}else{"failed"}.into(),durability:replay_durability(&info),started_unix_seconds:started_unix,finished_unix_seconds:now_unix(),backend:info,
        execution:serde_json::json!({"effective_client_shards":1}),
        phase_counts:serde_json::json!({"scope":trace.scope(),"profile":metrics,"preload_ns":preload_ns,"counts":m.counts,"error":result.as_ref().err().map(ToString::to_string),"resources":resources,"sustained_30s_qualified":result.is_ok() && unresolved_workers==0 && elapsed.as_secs_f64()>=30.0,"run_status":run_status,"unknown_outcomes":unknown_outcomes,"unresolved_workers":unresolved_workers,"shutdown_contract":"join completed callers; after deadline plus grace detach only unresolved calls, under external process supervision; no cancellation or rollback"}),
        distributions:reporter.distribution_summaries(),limitations:vec![trace.scope(),"Package reads, accumulator materialization, JSON-RPC indexes, source node caches, pruning and epoch transitions are not represented; device read counters unmeasured".into()]};
    reporter.finish(&summary,result.is_err(),&serde_json::json!({"scope":trace.scope(),"config":config,"binary":resources["binary"],"trace_manifest":trace.manifest,"profile":metrics}))?;
    result
}

/// Where replay device bytes come from. Owner block devices are outside this client
/// process; the campaign controller snapshots them at the measurement gates when the job
/// requests `device_counters`, exactly as for the put cases.
fn device_counters_scope() -> serde_json::Value {
    serde_json::json!({"collector":"campaign controller","artifact":"device_counters.json","window":"block-device snapshots at the measurement.ready and measurement.complete gates; whole device, including background writes","payload":"write_amplification.payload"})
}

/// Payload of the successful commits inside the measurement window. `value_bytes` is the
/// denominator of the put cases' `wal_bytes_per_payload` (successful puts times value
/// size); `key_value_bytes` adds the logical key bytes the store encodes with each value.
#[derive(Default, Serialize)]
struct CommittedPayload {
    commits: u64,
    rows: u64,
    value_bytes: u64,
    key_bytes: u64,
    key_value_bytes: u64,
}

fn committed_payload(m: &Measurements, prepared: &[Prepared]) -> BackendResult<CommittedPayload> {
    let mut payload = CommittedPayload::default();
    for (sample, id) in &m.calls {
        if sample.operation != "commit" || sample.outcome != "success" {
            continue;
        }
        let phase = id.batch_phase.ok_or("commit sample lacks a batch phase")?;
        let batch = prepared
            .get(usize::try_from(id.repetition)?)
            .and_then(|p| p.batches.iter().find(|batch| batch.phase == phase))
            .ok_or("commit sample has no prepared batch")?;
        ensure(
            batch.entries.len() as u64 == sample.logical_keys,
            "commit sample members differ from its prepared batch",
        )?;
        let key_bytes = batch
            .entries
            .iter()
            .map(|r| r.key.len() as u64)
            .sum::<u64>();
        payload.commits += 1;
        payload.rows += sample.logical_keys;
        payload.value_bytes += sample.bytes;
        payload.key_bytes += key_bytes;
        payload.key_value_bytes += key_bytes + sample.bytes;
    }
    Ok(payload)
}

/// Measurement-boundary delta of the coordinator's live leased-write counters, summed over
/// node databases: the counter the put cases divide by payload (`completed_write_bytes`,
/// whole aligned NVMe-oF frames), plus write operations and leased frames.
fn live_write_delta(
    before: &serde_json::Value,
    after: &serde_json::Value,
) -> Result<[u64; 3], String> {
    const FIELDS: [&str; 3] = [
        "completed_write_bytes",
        "completed_write_ops",
        "leased_frames",
    ];
    // `physical_counts` returns one observation, or `{"databases": {id: observation}}`.
    fn snapshots(value: &serde_json::Value) -> Vec<(&str, &serde_json::Value)> {
        match value
            .get("databases")
            .and_then(serde_json::Value::as_object)
        {
            Some(databases) => databases
                .iter()
                .map(|(id, db)| (id.as_str(), &db["live_nvmeof"]))
                .collect(),
            None => vec![("", &value["live_nvmeof"])],
        }
    }
    let (start, end) = (snapshots(before), snapshots(after));
    if start.len() != end.len() || start.iter().zip(&end).any(|(a, b)| a.0 != b.0) {
        return Err("node database population differs between measurement boundaries".into());
    }
    let mut total = [0u64; 3];
    for ((_, a), (_, b)) in start.iter().zip(&end) {
        for (slot, field) in total.iter_mut().zip(FIELDS) {
            let (Some(first), Some(last)) = (
                a.get(field).and_then(serde_json::Value::as_u64),
                b.get(field).and_then(serde_json::Value::as_u64),
            ) else {
                return Err(format!(
                    "live NVMe-oF counter {field} unavailable; owner-RPC and non-TideHunter routes expose no leased-write bytes"
                ));
            };
            // u64::MAX is the saturated-counter sentinel, as in the measurement read evidence.
            if first == u64::MAX || last == u64::MAX {
                return Err(format!("live NVMe-oF counter {field} saturated"));
            }
            *slot = last
                .checked_sub(first)
                .and_then(|delta| slot.checked_add(delta))
                .ok_or_else(|| format!("live NVMe-oF counter {field} decreased or overflowed"))?;
        }
    }
    Ok(total)
}

fn per_payload_byte(bytes: u64, payload: u64) -> Option<f64> {
    (payload > 0).then(|| bytes as f64 / payload as f64)
}

/// Replay counterpart of the put cases' write amplification: payload of the successful
/// measurement-window commits and logical WAL bytes over the same boundaries. Device
/// bytes per payload use the controller's `device_counters.json` with this payload.
fn write_amplification(
    m: &Measurements,
    prepared: &[Prepared],
    before: &serde_json::Value,
    after: &serde_json::Value,
    drained: bool,
) -> serde_json::Value {
    let payload = match committed_payload(m, prepared) {
        Ok(payload) => payload,
        Err(error) => {
            return serde_json::json!({"available":false,"error":error.to_string(),"device":device_counters_scope()});
        }
    };
    let logical_wal = if !drained {
        serde_json::json!({"available":false,"error":"measurement did not complete and drain successfully"})
    } else {
        match live_write_delta(before, after) {
            Ok([bytes, write_ops, leased_frames]) => serde_json::json!({
                "available":true,"counter":"live_nvmeof.completed_write_bytes","bytes":bytes,
                "write_ops":write_ops,"leased_frames":leased_frames,
                "bytes_per_value_byte":per_payload_byte(bytes, payload.value_bytes),
                "bytes_per_key_value_byte":per_payload_byte(bytes, payload.key_value_bytes)}),
            Err(error) => serde_json::json!({"available":false,"error":error}),
        }
    };
    serde_json::json!({"available":true,"scope":"successful measurement-window commits; logical WAL is the gate-boundary delta of the coordinator live leased-write counters, not physical device bytes",
        "payload":payload,"logical_wal":logical_wal,"device":device_counters_scope()})
}
/// Backend durability evidence plus the tier the backend confirmed from owner receipts,
/// in the field the campaign admits (`confirmed_by_backend_receipt`), as the runner records it.
fn replay_durability(info: &BackendInfo) -> serde_json::Value {
    let mut durability = info
        .configuration
        .get("durability")
        .unwrap_or(&info.configuration)
        .clone();
    if let (Some(tier), Some(fields)) = (
        info.configuration.get("durability_tier"),
        durability.as_object_mut(),
    ) {
        fields.insert("tier".into(), tier.clone());
        fields.insert("confirmed_by_backend_receipt".into(), tier.clone());
    }
    durability
}

async fn open_backend(
    config: &ReplayConfig,
    trace: &Trace,
) -> BackendResult<(Arc<dyn ReplayBackend>, BackendInfo)> {
    match config.backend.kind.as_str() {
        "native" => Ok((
            Arc::new(NativeReplayBackend::open(
                &config.backend.options,
                &trace.tables,
            )?),
            native_info(&config.backend.options)?,
        )),
        "tidehunter_tcp" => {
            let backend = TidehunterReplayBackend::open(&config.backend.options, trace).await?;
            let info = backend.info();
            Ok((Arc::new(backend), info))
        }
        #[cfg(feature = "foundationdb")]
        "foundationdb" => {
            let backend = crate::backends::foundationdb::replay::FoundationDbReplayBackend::open(
                &config.backend.options,
            )
            .await?;
            let info = backend.info();
            Ok((Arc::new(backend), info))
        }
        #[cfg(any(
            feature = "valkey-tcp",
            feature = "garnet",
            feature = "kvrocks",
            feature = "valkey-rdma"
        ))]
        "valkey_tcp" | "valkey_rdma" | "garnet" | "kvrocks" => {
            let backend = crate::backends::resp_replay::RespReplayBackend::open(
                &config.backend.kind,
                &config.backend.options,
            )
            .await?;
            let info = backend.info();
            Ok((Arc::new(backend), info))
        }
        #[cfg(feature = "tikv")]
        "tikv" => {
            let backend =
                crate::backends::tikv::replay::TikvReplayBackend::open(&config.backend.options)
                    .await?;
            let info = backend.info();
            Ok((Arc::new(backend), info))
        }
        #[cfg(feature = "cassandra")]
        "cassandra" => {
            let backend = crate::backends::cassandra::replay::CassandraReplayBackend::open(
                &config.backend.options,
            )
            .await?;
            let info = backend.info();
            Ok((Arc::new(backend), info))
        }
        _ => Err(Box::new(UnsupportedReplayBackend(
            config.backend.kind.clone(),
        ))),
    }
}

fn native_info(options: &serde_json::Value) -> BackendResult<BackendInfo> {
    let geometry = NativeReplayBackend::geometry(options)?;
    let synced = geometry["durability"] == "value_wal_barrier";
    Ok(BackendInfo {
        name: "native".into(),
        server_version: "Tidehunter 0.1.0".into(),
        client_version: env!("CARGO_PKG_VERSION").into(),
        acknowledgement: if synced {
            "native atomic batch followed by Value-WAL file and directory persistence barrier"
        } else {
            "native write acknowledgement; no foreground Value-WAL fsync"
        }
        .into(),
        consistency: "native Tidehunter Db keyspace semantics".into(),
        replication_factor: 1,
        measurement_boundary:
            "foreground native operation on dedicated bounded caller; verification outside API time"
                .into(),
        get_many_strategy: "not used".into(),
        batch_atomicity: BatchAtomicity::CrashRecovery,
        topology: serde_json::json!({"processes":1,"node_databases":3,"native_shards":1,"physical_ssds":"unverified","network":"none"}),
        configuration: geometry,
    })
}
fn quantiles(mut values: Vec<u64>) -> serde_json::Value {
    values.sort_unstable();
    let q = |n: usize, d: usize| {
        values
            .get((values.len() * n).div_ceil(d).saturating_sub(1))
            .copied()
    };
    serde_json::json!({"count":values.len(),"p50_ns":q(50,100),"p90_ns":q(90,100),"p99_ns":q(99,100),"p999_ns":q(999,1000),"method":"exact nearest rank"})
}
fn profile(
    m: &Measurements,
    trace: &Trace,
    prepared: &[Prepared],
    start_ns: u64,
    elapsed: Duration,
) -> serde_json::Value {
    let mut populations: BTreeMap<String, Vec<&Sample>> = BTreeMap::new();
    let mut tables: BTreeMap<String, (u64, u64, Vec<u64>)> = BTreeMap::new();
    let mut buckets = vec![0u64; usize::try_from(elapsed.as_secs()).unwrap_or(0)];
    let mut api_sum = 0u64;
    let mut verification = 0u64;
    for (s, id) in &m.calls {
        populations
            .entry(format!("operation:{}:{}", s.operation, s.outcome))
            .or_default()
            .push(s);
        populations
            .entry(format!(
                "{}:{}:db{}:phase{:?}",
                s.operation, s.outcome, id.db, id.batch_phase
            ))
            .or_default()
            .push(s);
        api_sum += s.api_ns;
        verification += s.released_ns.saturating_sub(s.completed_ns);
        if s.outcome != "success" {
            continue;
        }
        if let Some(bucket) = buckets.get_mut(
            usize::try_from(s.completed_ns.saturating_sub(start_ns) / 1_000_000_000)
                .unwrap_or(usize::MAX),
        ) {
            *bucket += 1;
        }
        if let Some(table) = id.table {
            let name = format!("get:{}", trace.tables[usize::from(table)].name);
            let t = tables.entry(name).or_default();
            t.0 += 1;
            t.1 += s.bytes;
            t.2.push(s.api_ns);
        }
        if let Some(phase) = id.batch_phase {
            let Some(batch) = prepared[usize::try_from(id.repetition).unwrap_or(0)]
                .batches
                .iter()
                .find(|batch| batch.phase == phase)
            else {
                continue;
            };
            let mut seen = [false; 17];
            for row in &batch.entries {
                let t = tables
                    .entry(format!("put:{}", trace.tables[usize::from(row.table)].name))
                    .or_default();
                t.0 += 1;
                t.1 += row.value.len() as u64;
                if !seen[usize::from(row.table)] {
                    t.2.push(s.api_ns);
                    seen[usize::from(row.table)] = true;
                }
            }
        }
    }
    let latency:BTreeMap<_,_>=populations.into_iter().map(|(name,s)| (name,serde_json::json!({"api":quantiles(s.iter().map(|s|s.api_ns).collect()),"scheduling":quantiles(s.iter().map(|s|s.scheduling_ns).collect()),"response":quantiles(s.iter().map(|s|s.response_ns).collect())}))).collect();
    let seconds = elapsed.as_secs_f64();
    let tables:BTreeMap<_,_>=tables.into_iter().map(|(name,(count,bytes,s))| {let attribution=name.starts_with("put:");(name,serde_json::json!({"members":count,"members_per_second":count as f64/seconds,"value_bytes":bytes,"value_bytes_per_second":bytes as f64/seconds,"latency":quantiles(s),"shared_parent_batch_latency_attribution":attribution}))}).collect();
    let success = m
        .calls
        .iter()
        .filter(|(s, _)| s.outcome == "success")
        .count();
    serde_json::json!({"scope":trace.scope(),"measurement_elapsed_seconds":seconds,"api_calls_per_second":success as f64/seconds,"offered_calls":m.calls.len(),"successful_calls":success,"rejected_calls":0,
        "offered_calls_per_second":m.calls.len() as f64/seconds,"rejected_calls_per_second":0.0,
        "timed_out_calls_per_second":m.calls.iter().filter(|(s,_)|matches!(s.outcome,"timeout"|"unknown_mutation")).count() as f64/seconds,
        "timed_out_calls":m.calls.iter().filter(|(s,_)|matches!(s.outcome,"timeout"|"unknown_mutation")).count(),"logical_ops_per_second":(m.counts.store_gets+m.counts.put_members) as f64/seconds,
        "put_members_per_second":m.counts.put_members as f64/seconds,"transactions_per_second":m.counts.transactions as f64/seconds,"checkpoints_per_second":m.counts.checkpoints as f64/seconds,
        "value_bytes_per_second":m.calls.iter().filter(|(s,_)|s.outcome=="success").map(|(s,_)|s.bytes).sum::<u64>() as f64/seconds,
        "mean_in_flight_api_calls":api_sum as f64/(seconds*1e9),"verification_ns":verification,"dependency_wait_ns":m.dependency_wait_ns,"complete_one_second_api_rate_buckets":buckets,
        "latency":latency,"tables":tables,"topology":"one process; three node DBs on one local filesystem; no network; source caches not modeled","device_counters":device_counters_scope()})
}

#[cfg(test)]
mod tests;
