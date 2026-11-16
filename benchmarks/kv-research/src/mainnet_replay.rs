//! W=1 checkpoint-commit replay. The scheduler alone owns the DAG and dirty overlay.
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
use crate::mainnet_trace::{Batch, Read, Row, SCOPE, Trace, expanded_keys};
use crate::report::{ReplayIdentity, Reporter, RunSummary, Sample};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap};
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
fn ensure(ok: bool, message: &str) -> BackendResult<()> {
    if ok {
        Ok(())
    } else {
        Err(message.to_owned().into())
    }
}
impl ReplayConfig {
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
                && self.execution.client_shards == 1
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
        let trace = Trace::load(&self.workload.manifest_path, &self.workload.manifest_sha256)?;
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

    pub fn admit(&self) -> BackendResult<(Trace, Vec<Prepared>, serde_json::Value)> {
        let trace = self.validate()?;
        let mut resources = resource_admission(self, &trace)?;
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
                                trace.batches[7]
                                    .entries
                                    .iter()
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
    completed: Vec<bool>,
    remaining: Vec<usize>,
    dependents: Vec<Vec<usize>>,
    ready: BinaryHeap<Reverse<usize>>,
    ready_at: Vec<Option<Instant>>,
    read_index: Vec<usize>,
    idle: Vec<usize>,
}
impl Measurements {
    fn new(trace: &Trace, repetitions: usize, callers: usize) -> BackendResult<Self> {
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
        let mut dependents = vec![Vec::new(); trace.transactions];
        for (consumer, producers) in trace.dependencies.iter().enumerate() {
            for &producer in producers {
                dependents[producer].push(consumer);
            }
        }
        Ok(Self {
            calls: Vec::with_capacity(capacity),
            counts: Counts::default(),
            dependency_wait_ns: 0,
            completed: vec![false; trace.transactions],
            remaining: vec![0; trace.transactions],
            dependents,
            ready: BinaryHeap::with_capacity(trace.transactions),
            ready_at: vec![None; trace.transactions],
            read_index: vec![0; trace.transactions],
            idle: Vec::with_capacity(callers),
        })
    }

    fn publish(&mut self, tx: usize, now: Instant) {
        // Each producer completes once; unique edges decrement each indegree once.
        // The preallocated heap holds at most one entry per transaction.
        self.completed[tx] = true;
        self.counts.transactions += 1;
        for &consumer in &self.dependents[tx] {
            self.remaining[consumer] -= 1;
            if self.remaining[consumer] == 0 {
                self.ready_at[consumer] = Some(now);
                self.ready.push(Reverse(consumer));
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
            Some(u16::try_from(phase)?),
            Some(repetition as u64 * 9 + phase as u64),
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

async fn checkpoint(
    pool: &mut Pool,
    trace: &Trace,
    prepared: &Prepared,
    rep: usize,
    deadline: Instant,
    measurements: &mut Measurements,
) -> BackendResult<()> {
    for phase in 0..9 {
        if phase == 5 {
            measurements.completed.fill(false);
            measurements.ready_at.fill(None);
            measurements.read_index.fill(0);
            measurements.idle.clear();
            measurements.idle.extend((0..pool.senders.len()).rev());
            measurements.ready.clear();
            let execution_start = Instant::now();
            for (tx, producers) in trace.dependencies.iter().enumerate() {
                measurements.remaining[tx] = producers.len();
                if producers.is_empty() {
                    measurements.ready_at[tx] = Some(execution_start);
                    measurements.ready.push(Reverse(tx));
                }
            }
            let mut finished = 0usize;
            let mut active = 0usize;
            // Publication bits gate immutable staged output views, owned only here.
            while finished < trace.transactions {
                while let Some(worker) = measurements.idle.pop() {
                    let Some(Reverse(tx)) = measurements.ready.pop() else {
                        measurements.idle.push(worker);
                        break;
                    };
                    let ready =
                        measurements.ready_at[tx].ok_or("ready transaction lacks arrival clock")?;
                    for (index, read) in prepared.reads[tx].iter().enumerate() {
                        if let Some(producer) = read.producer {
                            ensure(
                                measurements.completed[producer],
                                "dirty overlay producer not published",
                            )?;
                            ensure(
                                prepared.overlay[tx][index].as_ref() == Some(&read.row.value),
                                "dirty overlay missing/wrong value",
                            )?;
                            measurements.counts.overlay_demands += 1;
                        }
                    }
                    measurements.dependency_wait_ns +=
                        duration_ns(ready.saturating_duration_since(execution_start));
                    if let Some(index) =
                        prepared.reads[tx].iter().position(|r| r.producer.is_none())
                    {
                        measurements.read_index[tx] = index;
                        pool.dispatch(worker, rep, Operation::Read { tx, index }, ready, deadline)?;
                        active += 1;
                    } else {
                        measurements.publish(tx, Instant::now());
                        finished += 1;
                        measurements.idle.push(worker);
                    }
                }
                if finished == trace.transactions {
                    break;
                }
                ensure(active > 0, "replay dependency cycle")?;
                let completion = pool.receive(deadline).await?;
                let Operation::Read { tx, .. } = completion.work.operation else {
                    return Err("unexpected replay completion".into());
                };
                observe_completion(
                    trace,
                    prepared,
                    &completion,
                    &mut measurements.calls,
                    &mut measurements.counts,
                )?;
                let worker = completion.worker;
                pool.recycle(completion)?;
                let next = prepared.reads[tx]
                    .iter()
                    .enumerate()
                    .skip(measurements.read_index[tx] + 1)
                    .find(|(_, r)| r.producer.is_none())
                    .map(|(index, _)| index);
                if let Some(index) = next {
                    measurements.read_index[tx] = index;
                    pool.dispatch(
                        worker,
                        rep,
                        Operation::Read { tx, index },
                        Instant::now(),
                        deadline,
                    )?;
                } else {
                    measurements.publish(tx, Instant::now());
                    finished += 1;
                    active -= 1;
                    measurements.idle.push(worker);
                }
            }
        }
        pool.dispatch(0, rep, Operation::Commit(phase), Instant::now(), deadline)?;
        let completion = pool.receive(deadline).await?;
        observe_completion(
            trace,
            prepared,
            &completion,
            &mut measurements.calls,
            &mut measurements.counts,
        )?;
        pool.recycle(completion)?;
    }
    measurements.counts.checkpoints += 1;
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
    let origin = Instant::now();
    let mut pool = Pool::new(
        Arc::clone(&backend),
        Arc::clone(&prepared),
        concurrency,
        origin,
        timeout_ms,
    )?;
    let mut m = Measurements::new(trace, prepared.len(), concurrency)?;
    let result: BackendResult<()> = async {
        for rep in 0..prepared.len() {
            pool.dispatch(0, rep, Operation::Preload, Instant::now(), deadline)?;
            let c = pool.receive(deadline).await?;
            pool.recycle(c)?;
        }
        backend.check_background_health()?;
        for (rep, p) in prepared.iter().enumerate() {
            checkpoint(&mut pool, trace, p, rep, deadline, &mut m).await?;
        }
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
    Ok(m.counts)
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

fn resource_admission(config: &ReplayConfig, trace: &Trace) -> BackendResult<serde_json::Value> {
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
    let (reserve, wal) = if network_store {
        (0, 0)
    } else if config.backend.kind == "native" {
        let fragment_size = geometry["fragment_size"]
            .as_u64()
            .ok_or("missing native fragment geometry")?;
        native_disk_reservation(payload, fragment_size)?
    } else {
        // Distributed owners reserve whole WAL files; keep the conservative whole-file bound.
        let wal = geometry["wal_file_size"]
            .as_u64()
            .ok_or("missing distributed WAL geometry")?
            .checked_mul(6)
            .ok_or("replay WAL reservation overflow")?;
        let reserve = payload
            .checked_mul(2)
            .and_then(|n| n.checked_add(index))
            .and_then(|n| n.checked_add(wal))
            .ok_or("replay disk reservation overflow")?;
        TidehunterReplayBackend::validate_disk_paths(&config.backend.options, reserve)?;
        (reserve, wal)
    };
    let mut path = PathBuf::from(
        geometry["admission_path"]
            .as_str()
            .or_else(|| config.backend.options["path"].as_str())
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
    // Include immutable row views, retained raw samples, key-map nodes and key owners
    // at peak expansion. Values stay shared with the two input buffers.
    let demands = trace.reads.iter().map(Vec::len).sum::<usize>() as u64;
    let calls = trace
        .reads
        .iter()
        .flatten()
        .filter(|r| r.producer.is_none())
        .count() as u64
        + 9;
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
    Ok(
        serde_json::json!({"estimated_disk_reservation_bytes":reserve,"available_bytes":available,
        "estimated_memory_reservation_bytes":memory,"memory_available_bytes":memory_available,
        "payload_key_value_bytes":payload,"index_reservation_bytes":index,"wal_fragment_reservation_bytes":wal,
        "disk_payload_amplification_factor":2,
        "reservation_model":if network_store {"client memory admission only; remote engine/disk budgets require the exact-owned lifecycle receipt"} else if config.backend.kind == "native" {"2x key/value payload for framing/index amplification + six open WAL fragments; empirical conservative native estimate, not physical I/O measurement; memory additionally reserves 128 bytes per row for indexes"} else {"2x key/value payload + 128 bytes per row + six whole WAL files, checked on every coordinator root; owner device capacity is not observable; memory additionally reserves 128 bytes per row for indexes"}}),
    )
}

pub async fn run(config: ReplayConfig, output: &Path) -> BackendResult<()> {
    let started_unix = now_unix();
    let started = Instant::now();
    let (trace, mut prepared, mut resources) = config.admit()?;
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

async fn run_prepared<B: ReplayBackend + ?Sized>(
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
    let mut reporter = Reporter::create(
        output,
        prometheus::Registry::new(),
        config.backend.kind.clone(),
    )?;
    let mut pool = Pool::new(
        Arc::clone(&backend),
        Arc::clone(&prepared),
        config.execution.concurrency,
        started,
        config.execution.request_timeout_ms,
    )?;
    let mut m = Measurements::new(&trace, prepared.len(), config.execution.concurrency)?;
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
        for (rep, p) in prepared.iter().enumerate() {
            checkpoint(&mut pool, &trace, p, rep, deadline, &mut m).await?;
        }
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
        let mut expected = FinalRows::new();
        for p in prepared.iter() {
            for row in p
                .preload
                .iter()
                .chain(p.batches.iter().flat_map(|b| &b.entries))
            {
                expected.insert((row.table, row.key.clone()), row.clone());
            }
        }
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
    metrics["topology"] = info.topology.clone();
    metrics["case_identity"] = serde_json::json!({"scope":SCOPE,"fixture_sha256":config.workload.manifest_sha256,"source":trace.manifest["source"],"repeat_domain":std::str::from_utf8(crate::mainnet_trace::DOMAIN)?,"repeat_version":1,"repetitions":prepared.len(),"N":config.execution.concurrency,"W":1,"table_mapping":trace.tables,"device_counters":"unmeasured"});
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
    let summary=RunSummary {status:if result.is_ok(){"completed"}else{"failed"}.into(),durability:info.configuration.clone(),started_unix_seconds:started_unix,finished_unix_seconds:now_unix(),backend:info,
        phase_counts:serde_json::json!({"scope":SCOPE,"profile":metrics,"preload_ns":preload_ns,"counts":m.counts,"error":result.as_ref().err().map(ToString::to_string),"resources":resources,"sustained_30s_qualified":result.is_ok() && unresolved_workers==0 && elapsed.as_secs_f64()>=30.0,"run_status":run_status,"unknown_outcomes":unknown_outcomes,"unresolved_workers":unresolved_workers,"shutdown_contract":"join completed callers; after deadline plus grace detach only unresolved calls, under external process supervision; no cancellation or rollback"}),
        distributions:reporter.distribution_summaries(),limitations:vec![SCOPE.into(),"Package reads, accumulator materialization, JSON-RPC indexes, source node caches, pruning and epoch transitions are not represented; device read counters unmeasured".into()]};
    reporter.finish(&summary,result.is_err(),&serde_json::json!({"scope":SCOPE,"config":config,"trace_manifest":trace.manifest,"profile":metrics}))?;
    result
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
            let batch =
                &prepared[usize::try_from(id.repetition).unwrap_or(0)].batches[usize::from(phase)];
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
    serde_json::json!({"scope":SCOPE,"measurement_elapsed_seconds":seconds,"api_calls_per_second":success as f64/seconds,"offered_calls":m.calls.len(),"successful_calls":success,"rejected_calls":0,
        "offered_calls_per_second":m.calls.len() as f64/seconds,"rejected_calls_per_second":0.0,
        "timed_out_calls_per_second":m.calls.iter().filter(|(s,_)|matches!(s.outcome,"timeout"|"unknown_mutation")).count() as f64/seconds,
        "timed_out_calls":m.calls.iter().filter(|(s,_)|matches!(s.outcome,"timeout"|"unknown_mutation")).count(),"logical_ops_per_second":(m.counts.store_gets+m.counts.put_members) as f64/seconds,
        "put_members_per_second":m.counts.put_members as f64/seconds,"transactions_per_second":m.counts.transactions as f64/seconds,"checkpoints_per_second":m.counts.checkpoints as f64/seconds,
        "value_bytes_per_second":m.calls.iter().filter(|(s,_)|s.outcome=="success").map(|(s,_)|s.bytes).sum::<u64>() as f64/seconds,
        "mean_in_flight_api_calls":api_sum as f64/(seconds*1e9),"verification_ns":verification,"dependency_wait_ns":m.dependency_wait_ns,"complete_one_second_api_rate_buckets":buckets,
        "latency":latency,"tables":tables,"topology":"one process; three node DBs on one local filesystem; no network; source caches not modeled","device_counters":"unmeasured"})
}

#[cfg(test)]
mod tests;
