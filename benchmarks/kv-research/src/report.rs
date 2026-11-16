use prometheus::{
    Encoder, HistogramOpts, HistogramVec, IntCounterVec, IntGauge, Opts, Registry, TextEncoder,
};
use serde::Serialize;
use std::borrow::Cow;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;

use crate::backend::{BackendInfo, BackendResult};
use crate::duration_ns;
/// Raw sample queue: owner is the single `kv-research-sample-writer` thread; producers are the
/// scheduling task (completions, rejections) and snapshot flush requests.
/// Capacity: 4096 messages (about 0.5 MiB of generic samples; replay adds a fixed identity).
/// Up to 65536 admitted requests can drain one row each, so the queue can fill before recording ends.
/// Full-queue policy: old single streams wait and count the stall. Interference rows use
/// `try_send`; saturation fails the case and marks raw samples incomplete. Admission never
/// waits for telemetry in interference. Flush requests use `try_send` and are skipped when full.
/// Shutdown: `finish` drops the sender, the writer drains every queued row, flushes and syncs.
const RAW_SAMPLE_QUEUE_CAPACITY: usize = 4096;

const LATENCY_BUCKETS_SECONDS: &[f64] = &[
    0.000_001, 0.000_002, 0.000_005, 0.000_010, 0.000_020, 0.000_050, 0.000_100, 0.000_200,
    0.000_500, 0.001, 0.002, 0.005, 0.010, 0.020, 0.050, 0.100, 0.250, 0.500, 1.0, 2.5, 5.0, 10.0,
    30.0,
];

/// Independent measurement populations. Old single-stream rows leave this column empty.
#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Stream {
    Read,
    Put,
}

impl Stream {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Put => "put",
        }
    }
}

#[derive(Default, Debug, Serialize)]
pub struct StreamCounts {
    pub offered: u64,
    pub admitted: u64,
    pub completed: u64,
    pub successful: u64,
    pub rejected: u64,
    /// Accepted calls without a terminal worker response at the drain boundary.
    pub failed: u64,
    pub first_dispatch_ns: Option<u64>,
    pub last_completion_ns: Option<u64>,
}

#[derive(Serialize)]
pub struct InterferenceResult {
    pub schema_version: u32,
    pub case_id: String,
    pub read: StreamCounts,
    pub put: StreamCounts,
    pub measurement_drained: bool,
    pub read_offered_per_second: u64,
    pub measurement_elapsed_ns: u64,
    pub window_start_ns: u64,
    pub window_end_ns: u64,
}

#[derive(Clone, Debug)]
pub struct Sample {
    pub stream: Option<Stream>,
    pub phase: &'static str,
    pub operation: &'static str,
    pub outcome: &'static str,
    pub caller_sample_id: Option<u64>,
    pub scheduled_ns: u64,
    pub dispatched_ns: u64,
    pub dispatch_observed: bool,
    pub completed_ns: u64,
    pub api_ns: u64,
    pub response_ns: u64,
    pub scheduling_ns: u64,
    /// Benchmark worker release: end of oracle verification, on the same origin clock as
    /// `completed_ns`. `completed_ns` stays the API boundary; the difference is oracle cost.
    pub released_ns: u64,
    pub logical_keys: u64,
    pub bytes: u64,
    pub error_kind: Cow<'static, str>,
}

#[derive(Serialize)]
pub struct DistributionSummary {
    pub count: u64,
    pub min_ns: Option<u64>,
    pub max_ns: Option<u64>,
    pub p50_upper_bound_ns: Option<u64>,
    pub p90_upper_bound_ns: Option<u64>,
    pub p99_upper_bound_ns: Option<u64>,
    pub p999_upper_bound_ns: Option<u64>,
    pub quantile_method: &'static str,
}

struct LatencyAccumulator {
    count: u64,
    min_ns: u64,
    max_ns: u64,
    bins: [u64; 64],
}

impl Default for LatencyAccumulator {
    fn default() -> Self {
        Self {
            count: 0,
            min_ns: 0,
            max_ns: 0,
            bins: [0; 64],
        }
    }
}

impl LatencyAccumulator {
    fn merge(&mut self, other: Self) {
        if other.count == 0 {
            return;
        }
        if self.count == 0 {
            self.min_ns = other.min_ns;
            self.max_ns = other.max_ns;
        } else {
            self.min_ns = self.min_ns.min(other.min_ns);
            self.max_ns = self.max_ns.max(other.max_ns);
        }
        self.count += other.count;
        for (bin, count) in self.bins.iter_mut().zip(other.bins) {
            *bin += count;
        }
    }

    fn observe(&mut self, value: u64) {
        if self.count == 0 {
            self.min_ns = value;
            self.max_ns = value;
        } else {
            self.min_ns = self.min_ns.min(value);
            self.max_ns = self.max_ns.max(value);
        }
        self.count += 1;
        let index = (u64::BITS - value.max(1).leading_zeros() - 1) as usize;
        self.bins[index.min(63)] += 1;
    }

    fn quantile_upper_bound(&self, numerator: u64, denominator: u64) -> Option<u64> {
        if self.count == 0 {
            return None;
        }
        // Count is bounded by admitted run operations; integer ranks avoid floating rounding.
        let target = (self.count * numerator).div_ceil(denominator);
        let mut cumulative = 0;
        for (index, count) in self.bins.iter().enumerate() {
            cumulative += count;
            if cumulative >= target.max(1) {
                let upper = if index == 63 {
                    u64::MAX
                } else {
                    (1_u64 << (index + 1)) - 1
                };
                return Some(upper);
            }
        }
        Some(self.max_ns)
    }

    fn summary(&self) -> DistributionSummary {
        DistributionSummary {
            count: self.count,
            min_ns: (self.count > 0).then_some(self.min_ns),
            max_ns: (self.count > 0).then_some(self.max_ns),
            p50_upper_bound_ns: self.quantile_upper_bound(50, 100),
            p90_upper_bound_ns: self.quantile_upper_bound(90, 100),
            p99_upper_bound_ns: self.quantile_upper_bound(99, 100),
            p999_upper_bound_ns: self.quantile_upper_bound(999, 1000),
            quantile_method: "upper bound from fixed base-2 nanosecond histogram; samples.csv is authoritative",
        }
    }
}

#[derive(Serialize)]
pub struct RunSummary {
    pub status: String,
    /// Durability contract and matched comparison cohort; tiers are never mixed in a comparison.
    pub durability: serde_json::Value,
    pub started_unix_seconds: f64,
    pub finished_unix_seconds: f64,
    pub backend: BackendInfo,
    pub phase_counts: serde_json::Value,
    pub distributions: serde_json::Value,
    pub limitations: Vec<String>,
}

/// Replay-only identity, attached to a real get or the parent atomic batch call.
#[derive(Clone, Debug, Serialize)]
pub struct ReplayIdentity {
    pub source_checkpoint: u64,
    pub source_tx: Option<u32>,
    pub repetition: u64,
    pub db: u16,
    pub table: Option<u16>,
    pub batch_phase: Option<u16>,
    pub parent_batch: Option<u64>,
    pub members: u64,
}

/// Raw rows and flush requests share one bounded queue so the writer needs no timer.
enum WriterMessage {
    Row(Sample),
    ReplayRow(Sample, ReplayIdentity),
    Flush,
}

/// Result of one bounded admission. `stalled_ns` is `Some` when the queue was full and the
/// sender had to wait for capacity.
struct Admission {
    stalled_ns: Option<u64>,
    delivered: bool,
}

/// Never blocks the calling thread: a full queue suspends only this task until the writer
/// frees capacity. A message is never dropped; a stopped writer is reported as not delivered.
async fn send_bounded(sender: &mpsc::Sender<WriterMessage>, message: WriterMessage) -> Admission {
    match sender.try_send(message) {
        Ok(()) => Admission {
            stalled_ns: None,
            delivered: true,
        },
        Err(mpsc::error::TrySendError::Full(message)) => {
            let started = Instant::now();
            let delivered = sender.send(message).await.is_ok();
            Admission {
                stalled_ns: Some(duration_ns(started.elapsed())),
                delivered,
            }
        }
        Err(mpsc::error::TrySendError::Closed(_)) => Admission {
            stalled_ns: None,
            delivered: false,
        },
    }
}

pub struct Reporter {
    output: PathBuf,
    samples: Option<mpsc::Sender<WriterMessage>>,
    sample_writer: Option<JoinHandle<std::io::Result<()>>>,
    client_shard: bool,
    registry: Registry,
    pub backend_label: String,
    status: IntGauge,
    snapshot_timestamp: prometheus::Gauge,
    current_phase: IntGauge,
    offered: IntCounterVec,
    completed: IntCounterVec,
    rejected: IntCounterVec,
    timeout: IntCounterVec,
    error: IntCounterVec,
    keys_completed: IntCounterVec,
    keys_offered: IntCounterVec,
    keys_rejected: IntCounterVec,
    succeeded: IntCounterVec,
    api_latency: HistogramVec,
    response_latency: HistogramVec,
    scheduling_delay: HistogramVec,
    offered_total: u64,
    offered_operations: std::collections::BTreeMap<(&'static str, &'static str), u64>,
    completed_total: u64,
    rejected_total: u64,
    timeout_total: u64,
    error_total: u64,
    keys_completed_total: u64,
    keys_offered_total: u64,
    keys_rejected_total: u64,
    succeeded_total: u64,
    sample_backpressure_ns: u64,
    sample_backpressure_events: u64,
    sample_submit_errors: u64,
    release_samples: u64,
    release_oracle_ns_sum: u64,
    release_oracle_ns_max: u64,
    distributions: std::collections::BTreeMap<
        (
            &'static str,
            &'static str,
            &'static str,
            &'static str,
            &'static str,
        ),
        LatencyAccumulator,
    >,
}

impl Reporter {
    // Register the complete bounded metric schema and its writer lifecycle in one place.
    #[allow(clippy::too_many_lines)]
    pub fn create(output: &Path, registry: Registry, backend_label: String) -> BackendResult<Self> {
        fs::create_dir_all(output)?;
        if fs::read_dir(output)?.next().is_some() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "output directory must be fresh",
            )
            .into());
        }
        let samples_path = output.join("samples.csv.tmp");
        let samples_file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(samples_path)?;
        let (sample_sender, mut sample_receiver) =
            mpsc::channel::<WriterMessage>(RAW_SAMPLE_QUEUE_CAPACITY);
        let sample_writer = thread::Builder::new().name("kv-research-sample-writer".to_owned()).spawn(move || {
            let mut samples = BufWriter::new(samples_file);
            writeln!(samples, "phase,operation,outcome,scheduled_ns,dispatched_ns,dispatch_observed,completed_ns,api_ns,response_ns,scheduling_ns,logical_keys,bytes,error_kind,caller_sample_id,released_ns,stream,source_checkpoint,source_tx,repetition,db,table,batch_phase,parent_batch,members")?;
            let mut next_flush = Instant::now() + Duration::from_secs(1);
            // This dedicated OS thread owns the receiver; blocking here never touches a runtime thread.
            while let Some(message) = sample_receiver.blocking_recv() {
                match message {
                    WriterMessage::Row(sample) => write_sample_row(&mut samples, &sample)?,
                    WriterMessage::ReplayRow(sample, identity) => write_replay_sample_row(&mut samples, &sample, Some(&identity))?,
                    WriterMessage::Flush => samples.flush()?,
                }
                if Instant::now() >= next_flush {
                    samples.flush()?;
                    next_flush = Instant::now() + Duration::from_secs(1);
                }
            }
            samples.flush()?;
            samples.get_ref().sync_all()
        })?;
        let labels = &["backend", "operation", "outcome", "phase"];
        let status = IntGauge::with_opts(Opts::new(
            "kv_comparison_run_status",
            "Run state: 0=running, 1=completed, 2=failed",
        ))?;
        let snapshot_timestamp = prometheus::Gauge::with_opts(Opts::new(
            "kv_comparison_snapshot_timestamp_seconds",
            "Unix time of the last complete metric snapshot",
        ))?;
        let current_phase = IntGauge::with_opts(Opts::new(
            "kv_comparison_current_phase",
            "Active phase code: qualification=1, preload=2, warmup=3, aging=4, measurement=5, drain=6",
        ))?;
        let offered = IntCounterVec::new(
            Opts::new(
                "kv_comparison_offered_total",
                "Scheduled operation arrivals",
            ),
            labels,
        )?;
        let completed = IntCounterVec::new(
            Opts::new(
                "kv_comparison_completed_total",
                "Terminal accepted operations, including timeout and error outcomes",
            ),
            labels,
        )?;
        let rejected = IntCounterVec::new(
            Opts::new(
                "kv_comparison_rejected_total",
                "Open-loop arrivals rejected at bounded admission",
            ),
            labels,
        )?;
        let timeout = IntCounterVec::new(
            Opts::new(
                "kv_comparison_timeout_total",
                "Timed-out or unresolved operations",
            ),
            labels,
        )?;
        let error = IntCounterVec::new(
            Opts::new(
                "kv_comparison_error_total",
                "Failed or correctness-invalid operations",
            ),
            labels,
        )?;
        let keys_completed = IntCounterVec::new(
            Opts::new(
                "kv_comparison_keys_completed_total",
                "Logical keys terminally completed",
            ),
            labels,
        )?;
        let keys_offered = IntCounterVec::new(
            Opts::new(
                "kv_comparison_keys_offered_total",
                "Logical keys in scheduled operations",
            ),
            labels,
        )?;
        let keys_rejected = IntCounterVec::new(
            Opts::new(
                "kv_comparison_keys_rejected_total",
                "Logical keys in rejected arrivals",
            ),
            labels,
        )?;
        let succeeded = IntCounterVec::new(
            Opts::new(
                "kv_comparison_succeeded_total",
                "Operations with successful KV outcomes",
            ),
            labels,
        )?;
        let histogram = |name: &str, help: &str| {
            HistogramVec::new(
                HistogramOpts::new(name, help).buckets(LATENCY_BUCKETS_SECONDS.to_vec()),
                labels,
            )
        };
        let api_latency = histogram(
            "kv_comparison_api_latency_seconds",
            "Backend API-call latency through materialized response",
        )?;
        let response_latency = histogram(
            "kv_comparison_response_latency_seconds",
            "Scheduled-arrival to completed-response latency",
        )?;
        let scheduling_delay = histogram(
            "kv_comparison_scheduling_delay_seconds",
            "Scheduled-arrival to backend dispatch delay",
        )?;
        for collector in [
            Box::new(status.clone()) as Box<dyn prometheus::core::Collector>,
            Box::new(snapshot_timestamp.clone()),
            Box::new(current_phase.clone()),
            Box::new(offered.clone()),
            Box::new(completed.clone()),
            Box::new(rejected.clone()),
            Box::new(timeout.clone()),
            Box::new(error.clone()),
            Box::new(keys_completed.clone()),
            Box::new(keys_offered.clone()),
            Box::new(keys_rejected.clone()),
            Box::new(succeeded.clone()),
            Box::new(api_latency.clone()),
            Box::new(response_latency.clone()),
            Box::new(scheduling_delay.clone()),
        ] {
            registry.register(collector)?;
        }
        status.set(0);
        snapshot_timestamp.set(unix_seconds());
        Ok(Self {
            output: output.to_owned(),
            samples: Some(sample_sender),
            sample_writer: Some(sample_writer),
            client_shard: false,
            registry,
            backend_label,
            status,
            snapshot_timestamp,
            current_phase,
            offered,
            completed,
            rejected,
            timeout,
            error,
            keys_completed,
            keys_offered,
            keys_rejected,
            succeeded,
            api_latency,
            response_latency,
            scheduling_delay,
            offered_total: 0,
            offered_operations: std::collections::BTreeMap::default(),
            completed_total: 0,
            rejected_total: 0,
            timeout_total: 0,
            error_total: 0,
            keys_completed_total: 0,
            keys_offered_total: 0,
            keys_rejected_total: 0,
            succeeded_total: 0,
            sample_backpressure_ns: 0,
            sample_backpressure_events: 0,
            sample_submit_errors: 0,
            release_samples: 0,
            release_oracle_ns_sum: 0,
            release_oracle_ns_max: 0,
            distributions: std::collections::BTreeMap::default(),
        })
    }

    /// A client owns its counters and distributions. Only metric handles and the bounded
    /// raw-row sender are shared; the original reporter owns snapshots and writer shutdown.
    pub(crate) fn fork_client(&self) -> Self {
        Self {
            output: self.output.clone(),
            samples: self.samples.clone(),
            sample_writer: None,
            client_shard: true,
            registry: self.registry.clone(),
            backend_label: self.backend_label.clone(),
            status: self.status.clone(),
            snapshot_timestamp: self.snapshot_timestamp.clone(),
            current_phase: self.current_phase.clone(),
            offered: self.offered.clone(),
            completed: self.completed.clone(),
            rejected: self.rejected.clone(),
            timeout: self.timeout.clone(),
            error: self.error.clone(),
            keys_completed: self.keys_completed.clone(),
            keys_offered: self.keys_offered.clone(),
            keys_rejected: self.keys_rejected.clone(),
            succeeded: self.succeeded.clone(),
            api_latency: self.api_latency.clone(),
            response_latency: self.response_latency.clone(),
            scheduling_delay: self.scheduling_delay.clone(),
            offered_total: 0,
            offered_operations: std::collections::BTreeMap::default(),
            completed_total: 0,
            rejected_total: 0,
            timeout_total: 0,
            error_total: 0,
            keys_completed_total: 0,
            keys_offered_total: 0,
            keys_rejected_total: 0,
            succeeded_total: 0,
            sample_backpressure_ns: 0,
            sample_backpressure_events: 0,
            sample_submit_errors: 0,
            release_samples: 0,
            release_oracle_ns_sum: 0,
            release_oracle_ns_max: 0,
            distributions: std::collections::BTreeMap::default(),
        }
    }

    pub(crate) fn merge_client(&mut self, client: Self) {
        self.offered_total += client.offered_total;
        for (population, count) in client.offered_operations {
            *self.offered_operations.entry(population).or_default() += count;
        }
        self.completed_total += client.completed_total;
        self.rejected_total += client.rejected_total;
        self.timeout_total += client.timeout_total;
        self.error_total += client.error_total;
        self.keys_completed_total += client.keys_completed_total;
        self.keys_offered_total += client.keys_offered_total;
        self.keys_rejected_total += client.keys_rejected_total;
        self.succeeded_total += client.succeeded_total;
        self.sample_backpressure_ns += client.sample_backpressure_ns;
        self.sample_backpressure_events += client.sample_backpressure_events;
        self.sample_submit_errors += client.sample_submit_errors;
        self.release_samples += client.release_samples;
        self.release_oracle_ns_sum += client.release_oracle_ns_sum;
        self.release_oracle_ns_max = self.release_oracle_ns_max.max(client.release_oracle_ns_max);
        for (population, values) in client.distributions {
            self.distributions
                .entry(population)
                .or_default()
                .merge(values);
        }
    }

    pub fn set_phase(&self, phase: &str) {
        if self.client_shard {
            return;
        }
        self.current_phase.set(match phase {
            "qualification" => 1,
            "preload" => 2,
            "warmup" => 3,
            "aging" => 4,
            "measurement" => 5,
            "drain" => 6,
            _ => 0,
        });
    }

    #[cfg(test)]
    pub(crate) fn stall_sample_writer_for_test(&mut self) -> impl Send + use<> {
        self.samples.take();
        if let Some(writer) = self.sample_writer.take() {
            writer.join().unwrap().unwrap();
        }
        let (sender, receiver) = mpsc::channel::<WriterMessage>(1);
        self.samples = Some(sender);
        receiver
    }

    pub fn offered(&mut self, phase: &'static str, operation: &'static str, logical_keys: u64) {
        let labels = [self.backend_label.as_str(), operation, "scheduled", phase];
        self.offered.with_label_values(&labels).inc();
        self.keys_offered
            .with_label_values(&labels)
            .inc_by(logical_keys);
        self.offered_total += 1;
        *self
            .offered_operations
            .entry((phase, operation))
            .or_default() += 1;
        self.keys_offered_total += logical_keys;
    }

    pub async fn rejected(&mut self, sample: Sample) -> BackendResult<()> {
        let labels = [
            self.backend_label.as_str(),
            sample.operation,
            "rejected",
            sample.phase,
        ];
        self.rejected.with_label_values(&labels).inc();
        self.keys_rejected
            .with_label_values(&labels)
            .inc_by(sample.logical_keys);
        self.rejected_total += 1;
        self.keys_rejected_total += sample.logical_keys;
        self.response_latency
            .with_label_values(&labels)
            .observe(Duration::from_nanos(sample.response_ns).as_secs_f64());
        self.distributions
            .entry((
                sample.phase,
                sample.stream.map_or("", Stream::as_str),
                sample.operation,
                "response",
                "rejected",
            ))
            .or_default()
            .observe(sample.response_ns);
        self.write_sample(sample).await
    }

    pub async fn record(&mut self, sample: Sample) -> BackendResult<()> {
        self.record_with_identity(sample, None).await
    }

    pub async fn record_replay(
        &mut self,
        sample: Sample,
        identity: ReplayIdentity,
    ) -> BackendResult<()> {
        self.record_with_identity(sample, Some(identity)).await
    }

    async fn record_with_identity(
        &mut self,
        sample: Sample,
        identity: Option<ReplayIdentity>,
    ) -> BackendResult<()> {
        let labels = [
            self.backend_label.as_str(),
            sample.operation,
            sample.outcome,
            sample.phase,
        ];
        self.completed.with_label_values(&labels).inc();
        self.completed_total += 1;
        if sample.outcome == "success" {
            self.succeeded.with_label_values(&labels).inc();
            self.succeeded_total += 1;
        }
        self.keys_completed
            .with_label_values(&labels)
            .inc_by(sample.logical_keys);
        self.keys_completed_total += sample.logical_keys;
        if sample.outcome == "timeout" || sample.outcome == "unknown_mutation" {
            self.timeout.with_label_values(&labels).inc();
            self.timeout_total += 1;
        }
        if sample.outcome == "error"
            || sample.outcome == "wrong_value"
            || sample.outcome == "unexpected_present"
        {
            self.error.with_label_values(&labels).inc();
            self.error_total += 1;
        }
        self.response_latency
            .with_label_values(&labels)
            .observe(Duration::from_nanos(sample.response_ns).as_secs_f64());
        self.distributions
            .entry((
                sample.phase,
                sample.stream.map_or("", Stream::as_str),
                sample.operation,
                "response",
                sample.outcome,
            ))
            .or_default()
            .observe(sample.response_ns);
        if sample.dispatch_observed {
            self.api_latency
                .with_label_values(&labels)
                .observe(Duration::from_nanos(sample.api_ns).as_secs_f64());
            self.scheduling_delay
                .with_label_values(&labels)
                .observe(Duration::from_nanos(sample.scheduling_ns).as_secs_f64());
            self.distributions
                .entry((
                    sample.phase,
                    sample.stream.map_or("", Stream::as_str),
                    sample.operation,
                    "api",
                    sample.outcome,
                ))
                .or_default()
                .observe(sample.api_ns);
            self.distributions
                .entry((
                    sample.phase,
                    sample.stream.map_or("", Stream::as_str),
                    sample.operation,
                    "scheduling",
                    sample.outcome,
                ))
                .or_default()
                .observe(sample.scheduling_ns);
        }
        let oracle_ns = sample.released_ns.saturating_sub(sample.completed_ns);
        self.release_samples = self.release_samples.saturating_add(1);
        self.release_oracle_ns_sum = self.release_oracle_ns_sum.saturating_add(oracle_ns);
        self.release_oracle_ns_max = self.release_oracle_ns_max.max(oracle_ns);
        if sample.dispatch_observed {
            self.distributions
                .entry((
                    sample.phase,
                    sample.stream.map_or("", Stream::as_str),
                    sample.operation,
                    "oracle",
                    sample.outcome,
                ))
                .or_default()
                .observe(oracle_ns);
        }
        self.write_sample_with_identity(sample, identity).await
    }

    async fn write_sample(&mut self, sample: Sample) -> BackendResult<()> {
        self.write_sample_with_identity(sample, None).await
    }

    async fn write_sample_with_identity(
        &mut self,
        sample: Sample,
        identity: Option<ReplayIdentity>,
    ) -> BackendResult<()> {
        let sender = self.samples.as_ref().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "sample writer already stopped",
            )
        })?;
        let interference = sample.stream.is_some();
        let message = match identity {
            Some(identity) => WriterMessage::ReplayRow(sample, identity),
            None => WriterMessage::Row(sample),
        };
        // The arrival clock must not wait for telemetry. A full writer queue invalidates
        // interference instead of delaying admission or losing a row without an error.
        if interference {
            return match sender.try_send(message) {
                Ok(()) => Ok(()),
                Err(error) => {
                    self.sample_submit_errors = self.sample_submit_errors.saturating_add(1);
                    Err(std::io::Error::other(format!(
                        "interference telemetry admission failed: {error}"
                    ))
                    .into())
                }
            };
        }
        let admission = send_bounded(sender, message).await;
        if let Some(stalled_ns) = admission.stalled_ns {
            self.sample_backpressure_events = self.sample_backpressure_events.saturating_add(1);
            self.sample_backpressure_ns = self.sample_backpressure_ns.saturating_add(stalled_ns);
        }
        if admission.delivered {
            Ok(())
        } else {
            self.sample_submit_errors = self.sample_submit_errors.saturating_add(1);
            Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "raw sample writer stopped; current sample was not persisted",
            )
            .into())
        }
    }

    pub fn snapshot(&mut self) -> BackendResult<()> {
        if self.client_shard {
            return Ok(());
        }
        // A full queue means rows are flowing and the writer flushes by elapsed time.
        if let Some(sender) = &self.samples {
            let _ = sender.try_send(WriterMessage::Flush);
        }
        self.snapshot_timestamp.set(unix_seconds());
        let mut encoded = Vec::new();
        TextEncoder::new().encode(&self.registry.gather(), &mut encoded)?;
        atomic_write(&self.output.join("live.prom"), &encoded)?;
        Ok(())
    }

    pub fn finish(
        mut self,
        summary: &RunSummary,
        failed: bool,
        manifest_extra: &serde_json::Value,
    ) -> BackendResult<()> {
        self.samples.take();
        let writer_error = match self.sample_writer.take().map(JoinHandle::join) {
            Some(Ok(Ok(()))) | None => None,
            Some(Ok(Err(error))) => Some(error.to_string()),
            Some(Err(_)) => Some("sample writer thread panicked".to_owned()),
        };
        let mut sample_error = writer_error;
        if let Err(error) = fs::rename(
            self.output.join("samples.csv.tmp"),
            self.output.join("samples.csv"),
        ) {
            sample_error = Some(sample_error.map_or_else(
                || error.to_string(),
                |previous| format!("{previous}; samples.csv rename failed: {error}"),
            ));
        }
        if self.sample_submit_errors > 0 {
            sample_error = Some(sample_error.map_or_else(
                || format!("{} sample submissions failed", self.sample_submit_errors),
                |previous| {
                    format!(
                        "{previous}; {} sample submissions failed",
                        self.sample_submit_errors
                    )
                },
            ));
        }
        let raw_samples_complete = sample_error.is_none();
        let failed = failed || !raw_samples_complete;
        self.status.set(if failed { 2 } else { 1 });
        self.current_phase.set(0);
        self.snapshot_timestamp.set(unix_seconds());
        let mut encoded = Vec::new();
        TextEncoder::new().encode(&self.registry.gather(), &mut encoded)?;
        let mut summary_value = serde_json::to_value(summary)?;
        summary_value["status"] = serde_json::json!(if failed { "failed" } else { "completed" });
        atomic_write(
            &self.output.join("summary.json"),
            &serde_json::to_vec_pretty(&summary_value)?,
        )?;
        let manifest = serde_json::json!({
            "schema_version": 1,
            "summary": "summary.json",
            "samples": "samples.csv",
            "metrics": "metrics.prom",
            "live_metrics": "live.prom",
            "status": if failed { "failed" } else { "completed" },
            "backend_info": summary.backend,
            "durability": summary.durability,
            "offered_operations": self.offered_total,
            "completed_operations": self.completed_total,
            "succeeded_operations": self.succeeded_total,
            "rejected_arrivals": self.rejected_total,
            "sample_writer_queue_capacity": RAW_SAMPLE_QUEUE_CAPACITY,
            "timeouts_or_unknown_mutations": self.timeout_total,
            "errors_or_wrong_values": self.error_total,
            "logical_keys_offered": self.keys_offered_total,
            "logical_keys_completed": self.keys_completed_total,
            "logical_keys_rejected": self.keys_rejected_total,
            "raw_samples_complete": raw_samples_complete,
            "raw_sample_writer_error": sample_error,
            "sample_writer_backpressure_ns": self.sample_backpressure_ns,
            "sample_writer_backpressure_events": self.sample_backpressure_events,
            "sample_writer_admission": {
                "mode": if manifest_extra.get("interference").is_some() {
                    "interference rows use bounded try_send; saturation fails the case and marks raw samples incomplete; other rows wait"
                } else {
                    "tokio bounded mpsc; a full queue suspends only the sending task, no runtime thread blocks, no sample is dropped"
                },
                "queue_capacity": RAW_SAMPLE_QUEUE_CAPACITY,
                "stall_total_ns": self.sample_backpressure_ns,
                "stall_events": self.sample_backpressure_events,
                "submit_errors": self.sample_submit_errors,
            },
            "worker_release": {
                "clock": "released_ns column: benchmark worker release after oracle verification; completed_ns stays the API response boundary",
                "samples": self.release_samples,
                "oracle_ns_sum": self.release_oracle_ns_sum,
                "oracle_ns_max": self.release_oracle_ns_max,
            },
            "run": manifest_extra,
        });
        atomic_write(
            &self.output.join("manifest.json"),
            &serde_json::to_vec(&manifest)?,
        )?;
        atomic_write(&self.output.join("metrics.prom"), &encoded)?;
        atomic_write(&self.output.join("live.prom"), &encoded)?;
        if !raw_samples_complete {
            return Err(std::io::Error::other("raw sample artifact incomplete").into());
        }
        Ok(())
    }

    /// Actual offered mix includes rejected arrivals. Probabilities, not a realized
    /// random sequence, are preserved when the number of independent clients changes.
    #[allow(clippy::cast_precision_loss)]
    pub(crate) fn realized_mix(&self) -> serde_json::Value {
        let mut phases = std::collections::BTreeMap::new();
        for ((phase, _), count) in &self.offered_operations {
            *phases.entry(*phase).or_insert(0_u64) += count;
        }
        serde_json::Value::Object(
            self.offered_operations
                .iter()
                .map(|((phase, operation), count)| {
                    (
                        format!("{phase}.{operation}"),
                        serde_json::json!({
                            "offered": count,
                            "fraction": *count as f64 / phases[phase] as f64,
                        }),
                    )
                })
                .collect(),
        )
    }

    #[must_use]
    pub fn totals(&self) -> (u64, u64, u64, u64, u64, u64, u64, u64, u64) {
        (
            self.offered_total,
            self.completed_total,
            self.rejected_total,
            self.timeout_total,
            self.error_total,
            self.keys_completed_total,
            self.succeeded_total,
            self.keys_offered_total,
            self.keys_rejected_total,
        )
    }

    #[must_use]
    pub fn distribution_summaries(&self) -> serde_json::Value {
        serde_json::Value::Object(
            self.distributions
                .iter()
                .map(|((phase, stream, operation, metric, outcome), values)| {
                    (
                        if stream.is_empty() {
                            format!("{phase}.{operation}.{metric}.{outcome}")
                        } else {
                            format!("{phase}.{stream}.{operation}.{metric}.{outcome}")
                        },
                        serde_json::json!(values.summary()),
                    )
                })
                .collect(),
        )
    }
}
fn write_sample_row<W: Write>(samples: &mut W, sample: &Sample) -> std::io::Result<()> {
    write_replay_sample_row(samples, sample, None)
}

fn write_replay_sample_row<W: Write>(
    samples: &mut W,
    sample: &Sample,
    identity: Option<&ReplayIdentity>,
) -> std::io::Result<()> {
    write!(
        samples,
        "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
        csv(sample.phase),
        csv(sample.operation),
        csv(sample.outcome),
        sample.scheduled_ns,
        sample.dispatched_ns,
        sample.dispatch_observed,
        sample.completed_ns,
        sample.api_ns,
        sample.response_ns,
        sample.scheduling_ns,
        sample.logical_keys,
        sample.bytes,
        csv(sample.error_kind.as_ref()),
        sample
            .caller_sample_id
            .map_or_else(String::new, |id| id.to_string()),
        sample.released_ns,
        sample.stream.map_or("", Stream::as_str)
    )?;
    if let Some(identity) = identity {
        write!(samples, ",{},", identity.source_checkpoint)?;
        if let Some(tx) = identity.source_tx {
            write!(samples, "{tx}")?;
        }
        write!(samples, ",{},{}", identity.repetition, identity.db)?;
        for value in [
            identity.table.map(u64::from),
            identity.batch_phase.map(u64::from),
            identity.parent_batch,
            Some(identity.members),
        ] {
            write!(samples, ",")?;
            if let Some(value) = value {
                write!(samples, "{value}")?;
            }
        }
        writeln!(samples)
    } else {
        writeln!(samples, ",,,,,,,,")
    }
}

fn csv(value: &str) -> Cow<'_, str> {
    if value
        .bytes()
        .any(|b| b == b',' || b == b'"' || b == b'\n' || b == b'\r')
    {
        Cow::Owned(format!("\"{}\"", value.replace('"', "\"\"")))
    } else {
        Cow::Borrowed(value)
    }
}

fn unix_seconds() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0.0, |duration| duration.as_secs_f64())
}

fn atomic_write(path: &Path, contents: &[u8]) -> BackendResult<()> {
    let temporary = path.with_extension(format!(
        "{}.tmp",
        path.extension()
            .and_then(|ext| ext.to_str())
            .unwrap_or("file")
    ));
    {
        let mut file = File::create(&temporary)?;
        file.write_all(contents)?;
        file.sync_all()?;
    }
    fs::rename(temporary, path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    /// Auxiliary primitive-cost check, not a store-throughput benchmark. All five
    /// successful-call counter updates and three labelled histogram observations
    /// match offered()/record(); raw output and private accumulators are excluded.
    #[test]
    #[ignore = "explicit local release-mode accounting measurement"]
    fn client_metric_handle_cost() {
        for clients in [1, 2, 4] {
            for shared in [false, true] {
                let root = std::env::temp_dir().join(format!(
                    "kv-client-metrics-{}-{}-{clients}-{shared}",
                    std::process::id(),
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_nanos()
                ));
                let mut parents = (0..if shared { 1 } else { clients })
                    .map(|index| {
                        Reporter::create(
                            &root.join(index.to_string()),
                            Registry::new(),
                            "test".into(),
                        )
                        .unwrap()
                    })
                    .collect::<Vec<_>>();
                let clients = (0..clients)
                    .map(|index| parents[if shared { 0 } else { index }].fork_client())
                    .collect::<Vec<_>>();
                let count = clients.len();
                let barrier = std::sync::Barrier::new(count + 1);
                let elapsed = std::thread::scope(|scope| {
                    let mut threads = Vec::with_capacity(count);
                    for mut client in clients {
                        let barrier = &barrier;
                        threads.push(scope.spawn(move || {
                            barrier.wait();
                            for _ in 0..200_000 / count {
                                client.offered("measurement", "read", 1);
                                let labels = ["test", "read", "success", "measurement"];
                                client.completed.with_label_values(&labels).inc();
                                client.succeeded.with_label_values(&labels).inc();
                                client.keys_completed.with_label_values(&labels).inc();
                                client
                                    .response_latency
                                    .with_label_values(&labels)
                                    .observe(0.000_002);
                                client
                                    .api_latency
                                    .with_label_values(&labels)
                                    .observe(0.000_001);
                                client
                                    .scheduling_delay
                                    .with_label_values(&labels)
                                    .observe(0.000_001);
                            }
                        }));
                    }
                    let started = Instant::now();
                    barrier.wait();
                    for thread in threads {
                        thread.join().unwrap();
                    }
                    started.elapsed()
                });
                println!(
                    "client_metric_handle_cost clients={count} shared={shared} aggregate_ns_per_call={:.3}",
                    elapsed.as_nanos() as f64 / 200_000.0
                );
                for parent in &mut parents {
                    parent.samples.take();
                    parent
                        .sample_writer
                        .take()
                        .unwrap()
                        .join()
                        .unwrap()
                        .unwrap();
                }
                std::fs::remove_dir_all(root).unwrap();
            }
        }
    }

    #[tokio::test]
    async fn client_reports_merge_counts_distributions_and_all_raw_rows_once() {
        let path = std::env::temp_dir().join(format!(
            "kv-research-client-report-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let mut parent = Reporter::create(&path, Registry::new(), "test".into()).unwrap();
        for (index, latency) in [3, 255].into_iter().enumerate() {
            let mut client = parent.fork_client();
            let WriterMessage::Row(mut sample) = row("success") else {
                unreachable!()
            };
            sample.api_ns = latency;
            sample.response_ns = latency + 1;
            client.offered("measurement", "read", 1);
            client.record(sample).await.unwrap();
            assert_eq!(parent.totals().0, index as u64);
            parent.merge_client(client);
        }
        assert_eq!(parent.totals().0, 2);
        assert_eq!(parent.totals().1, 2);
        assert_eq!(parent.totals().6, 2);
        let summary = parent.distribution_summaries();
        assert_eq!(summary["measurement.read.api.success"]["count"], 2);
        assert_eq!(summary["measurement.read.api.success"]["min_ns"], 3);
        assert_eq!(summary["measurement.read.api.success"]["max_ns"], 255);
        assert_eq!(parent.realized_mix()["measurement.read"]["offered"], 2);
        assert_eq!(parent.realized_mix()["measurement.read"]["fraction"], 1.0);
        let receiver = parent.stall_sample_writer_for_test();
        assert_eq!(
            std::fs::read_to_string(path.join("samples.csv.tmp"))
                .unwrap()
                .lines()
                .count(),
            3
        );
        drop(receiver);
        drop(parent);
        std::fs::remove_dir_all(path).unwrap();
    }

    use super::*;

    #[test]
    fn latency_summary_is_bounded_and_uses_bucket_upper_bounds() {
        let mut summary = LatencyAccumulator::default();
        for value in [100, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10] {
            summary.observe(value);
        }
        let summary = summary.summary();
        assert_eq!(summary.count, 11);
        assert_eq!(summary.p50_upper_bound_ns, Some(7));
        assert_eq!(summary.max_ns, Some(100));
        let mut exact = LatencyAccumulator::default();
        exact.observe(3);
        assert_eq!(exact.quantile_upper_bound(99, 100), Some(3));
    }

    #[tokio::test]
    async fn stream_tails_and_outcomes_are_not_pooled_and_rejections_have_no_api_tail() {
        let path = std::env::temp_dir().join(format!(
            "kv-research-tail-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let mut reporter = Reporter::create(&path, Registry::new(), "test".into()).unwrap();
        for (stream, operation, outcome, latency) in [
            (Stream::Read, "read", "success", 3),
            (Stream::Put, "put", "success", 255),
            (Stream::Read, "read", "timeout", 1023),
        ] {
            let WriterMessage::Row(mut sample) = row(outcome) else {
                unreachable!()
            };
            sample.stream = Some(stream);
            sample.operation = operation;
            sample.api_ns = latency;
            sample.completed_ns = sample.dispatched_ns + latency;
            sample.response_ns = sample.completed_ns;
            sample.released_ns = sample.completed_ns;
            reporter.offered("measurement", operation, 1);
            reporter.record(sample).await.unwrap();
        }
        let WriterMessage::Row(mut rejected) = row("rejected") else {
            unreachable!()
        };
        rejected.stream = Some(Stream::Read);
        rejected.dispatch_observed = false;
        rejected.dispatched_ns = 0;
        rejected.api_ns = 0;
        rejected.scheduling_ns = 0;
        reporter.offered("measurement", "read", 1);
        reporter.rejected(rejected).await.unwrap();
        let summary = reporter.distribution_summaries();
        for (key, expected) in [
            ("measurement.read.read.api.success", 3),
            ("measurement.put.put.api.success", 255),
            ("measurement.read.read.api.timeout", 1023),
        ] {
            assert_eq!(summary[key]["count"], 1);
            assert_eq!(summary[key]["p99_upper_bound_ns"], expected);
            assert_eq!(summary[key]["p999_upper_bound_ns"], expected);
        }
        assert!(summary.get("measurement.read.read.api.rejected").is_none());
        assert!(summary.get("measurement.read.api.success").is_none());
        // Finish the writer here so no queue or thread outlives the fixture.
        reporter.samples.take();
        reporter
            .sample_writer
            .take()
            .unwrap()
            .join()
            .unwrap()
            .unwrap();
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn csv_fields_escape_commas_and_quotes() {
        assert_eq!(csv("a,\"b"), "\"a,\"\"b\"");
        assert_eq!(csv("plain"), "plain");
        assert_eq!(csv("a\rb"), "\"a\rb\"");
        assert_eq!(csv("a\nb"), "\"a\nb\"");
        assert!(matches!(csv("plain"), Cow::Borrowed("plain")));
        assert!(matches!(csv("a,b"), Cow::Owned(_)));
    }

    fn row(outcome: &'static str) -> WriterMessage {
        WriterMessage::Row(Sample {
            stream: None,
            phase: "measurement",
            operation: "read",
            outcome,
            caller_sample_id: None,
            scheduled_ns: 0,
            dispatched_ns: 1,
            dispatch_observed: true,
            completed_ns: 5,
            api_ns: 4,
            response_ns: 5,
            scheduling_ns: 1,
            released_ns: 9,
            logical_keys: 1,
            bytes: 0,
            error_kind: Cow::Borrowed(""),
        })
    }

    // The runtime has one thread. A blocking send would park it, the drain task below could
    // never run, and this test would hang; an awaiting send lets the drain task free capacity.
    #[tokio::test(flavor = "current_thread")]
    async fn full_queue_suspends_only_the_sender_and_loses_no_sample() {
        let (sender, mut receiver) = mpsc::channel::<WriterMessage>(1);
        assert!(send_bounded(&sender, row("first")).await.delivered);
        let drain = async {
            tokio::task::yield_now().await;
            let mut outcomes = Vec::new();
            for _ in 0..2 {
                match receiver.recv().await {
                    Some(WriterMessage::Row(sample)) => outcomes.push(sample.outcome),
                    _ => break,
                }
            }
            outcomes
        };
        let (admission, outcomes) = tokio::join!(send_bounded(&sender, row("second")), drain);
        assert!(admission.delivered);
        assert!(admission.stalled_ns.is_some());
        assert_eq!(outcomes, ["first", "second"]);
    }

    #[tokio::test]
    async fn stopped_writer_is_reported_not_delivered() {
        let (sender, receiver) = mpsc::channel::<WriterMessage>(1);
        drop(receiver);
        let admission = send_bounded(&sender, row("lost")).await;
        assert!(!admission.delivered);
        assert!(admission.stalled_ns.is_none());
    }

    #[test]
    fn sample_row_keeps_release_clock_and_empty_replay_identity() {
        let WriterMessage::Row(sample) = row("success") else {
            unreachable!("row builds a sample message");
        };
        let mut line = Vec::new();
        write_sample_row(&mut line, &sample).unwrap();
        assert_eq!(
            String::from_utf8(line).unwrap(),
            "measurement,read,success,0,1,true,5,4,5,1,1,0,,,9,,,,,,,,,\n"
        );
    }
}
