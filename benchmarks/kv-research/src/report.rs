use prometheus::{
    Encoder, HistogramOpts, HistogramVec, IntCounterVec, IntGauge, Opts, Registry, TextEncoder,
};
use serde::Serialize;
use std::borrow::Cow;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, SyncSender};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::backend::{BackendInfo, BackendResult};
use crate::duration_ns;
const RAW_SAMPLE_QUEUE_CAPACITY: usize = 4096;

const LATENCY_BUCKETS_SECONDS: &[f64] = &[
    0.000_001, 0.000_002, 0.000_005, 0.000_010, 0.000_020, 0.000_050, 0.000_100, 0.000_200,
    0.000_500, 0.001, 0.002, 0.005, 0.010, 0.020, 0.050, 0.100, 0.250, 0.500, 1.0, 2.5, 5.0, 10.0,
    30.0,
];

#[derive(Clone, Debug)]
pub struct Sample {
    pub phase: &'static str,
    pub operation: &'static str,
    pub outcome: &'static str,
    pub scheduled_ns: u64,
    pub dispatched_ns: u64,
    pub dispatch_observed: bool,
    pub completed_ns: u64,
    pub api_ns: u64,
    pub response_ns: u64,
    pub scheduling_ns: u64,
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
    pub started_unix_seconds: f64,
    pub finished_unix_seconds: f64,
    pub backend: BackendInfo,
    pub phase_counts: serde_json::Value,
    pub distributions: serde_json::Value,
    pub limitations: Vec<String>,
}

pub struct Reporter {
    output: PathBuf,
    samples: Option<SyncSender<Sample>>,
    sample_writer: Option<JoinHandle<std::io::Result<()>>>,
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
    distributions: std::collections::BTreeMap<
        (&'static str, &'static str, &'static str, &'static str),
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
        let (sample_sender, sample_receiver) =
            mpsc::sync_channel::<Sample>(RAW_SAMPLE_QUEUE_CAPACITY);
        let sample_writer = thread::Builder::new().name("kv-research-sample-writer".to_owned()).spawn(move || {
            let mut samples = BufWriter::new(samples_file);
            writeln!(samples, "phase,operation,outcome,scheduled_ns,dispatched_ns,dispatch_observed,completed_ns,api_ns,response_ns,scheduling_ns,logical_keys,bytes,error_kind")?;
            let mut next_flush = Instant::now() + Duration::from_secs(1);
            loop {
                match sample_receiver.recv_timeout(next_flush.saturating_duration_since(Instant::now())) {
                    Ok(sample) => write_sample_row(&mut samples, &sample)?,
                    Err(mpsc::RecvTimeoutError::Timeout) => {},
                    Err(mpsc::RecvTimeoutError::Disconnected) => break,
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
            distributions: std::collections::BTreeMap::default(),
        })
    }

    pub fn set_phase(&self, phase: &str) {
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

    pub fn offered(&mut self, phase: &str, operation: &str, logical_keys: u64) {
        let labels = [self.backend_label.as_str(), operation, "scheduled", phase];
        self.offered.with_label_values(&labels).inc();
        self.keys_offered
            .with_label_values(&labels)
            .inc_by(logical_keys);
        self.offered_total += 1;
        self.keys_offered_total += logical_keys;
    }

    pub fn rejected(&mut self, sample: Sample) -> BackendResult<()> {
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
            .entry((sample.phase, sample.operation, "response", "rejected"))
            .or_default()
            .observe(sample.response_ns);
        self.write_sample(sample)
    }

    pub fn record(&mut self, sample: Sample) -> BackendResult<()> {
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
            .entry((sample.phase, sample.operation, "response", sample.outcome))
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
                .entry((sample.phase, sample.operation, "api", sample.outcome))
                .or_default()
                .observe(sample.api_ns);
            self.distributions
                .entry((sample.phase, sample.operation, "scheduling", sample.outcome))
                .or_default()
                .observe(sample.scheduling_ns);
        }
        self.write_sample(sample)
    }

    fn write_sample(&mut self, sample: Sample) -> BackendResult<()> {
        let sender = self.samples.as_ref().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "sample writer already stopped",
            )
        })?;
        match sender.try_send(sample) {
            Ok(()) => Ok(()),
            Err(mpsc::TrySendError::Full(sample)) => {
                self.sample_backpressure_events = self.sample_backpressure_events.saturating_add(1);
                let started = Instant::now();
                let result = sender.send(sample);
                self.sample_backpressure_ns = self
                    .sample_backpressure_ns
                    .saturating_add(duration_ns(started.elapsed()));
                if result.is_err() {
                    self.sample_submit_errors = self.sample_submit_errors.saturating_add(1);
                    Err(std::io::Error::new(
                        std::io::ErrorKind::BrokenPipe,
                        "raw sample writer stopped; current sample was not persisted",
                    )
                    .into())
                } else {
                    Ok(())
                }
            }
            Err(mpsc::TrySendError::Disconnected(_sample)) => {
                self.sample_submit_errors = self.sample_submit_errors.saturating_add(1);
                Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "raw sample writer stopped; current sample was not persisted",
                )
                .into())
            }
        }
    }

    pub fn snapshot(&mut self) -> BackendResult<()> {
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
        let sample_writer_error = sample_error.clone();
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
            "raw_sample_writer_error": sample_writer_error,
            "sample_writer_backpressure_ns": self.sample_backpressure_ns,
            "sample_writer_backpressure_events": self.sample_backpressure_events,
            "run": manifest_extra,
        });
        atomic_write(
            &self.output.join("manifest.json"),
            &serde_json::to_vec_pretty(&manifest)?,
        )?;
        atomic_write(&self.output.join("metrics.prom"), &encoded)?;
        atomic_write(&self.output.join("live.prom"), &encoded)?;
        if !raw_samples_complete {
            return Err(std::io::Error::other("raw sample artifact incomplete").into());
        }
        Ok(())
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
                .map(|((phase, operation, metric, outcome), values)| {
                    (
                        format!("{phase}.{operation}.{metric}.{outcome}"),
                        serde_json::json!(values.summary()),
                    )
                })
                .collect(),
        )
    }
}
fn write_sample_row<W: Write>(samples: &mut W, sample: &Sample) -> std::io::Result<()> {
    writeln!(
        samples,
        "{},{},{},{},{},{},{},{},{},{},{},{},{}",
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
        csv(sample.error_kind.as_ref())
    )
}

fn csv(value: &str) -> String {
    if value
        .bytes()
        .any(|b| b == b',' || b == b'"' || b == b'\n' || b == b'\r')
    {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_owned()
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

    #[test]
    fn csv_fields_escape_commas_and_quotes() {
        assert_eq!(csv("a,\"b"), "\"a,\"\"b\"");
        assert_eq!(csv("plain"), "plain");
        assert_eq!(csv("a\rb"), "\"a\rb\"");
    }
}
