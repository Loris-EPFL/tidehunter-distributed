use prometheus::core::{Collector, Desc};
use prometheus::proto::{Counter, Gauge, Metric, MetricFamily, MetricType};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct StageStats {
    pub count: u64,
    pub sum_ns: u64,
    pub max_ns: u64,
}
impl StageStats {
    pub(crate) fn add(&mut self, elapsed: Duration) {
        let n = elapsed.as_nanos().min(u64::MAX as u128) as u64;
        self.count = self.count.saturating_add(1);
        self.sum_ns = self.sum_ns.saturating_add(n);
        self.max_ns = self.max_ns.max(n);
    }
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct NvmeofStats {
    pub database_uuid: [u8; 16],
    pub epoch: u64,
    pub partition: u32,
    pub export_uuid: [u8; 16],
    pub logical_requests: u64,
    pub logical_members: u64,
    pub eligible_hits: u64,
    pub ineligible_fallbacks: u64,
    pub aligned_requested_bytes: u64,
    pub aligned_read_bytes: u64,
    pub io_submissions: u64,
    pub io_completions: u64,
    pub io_errors: u64,
    pub io_timeouts: u64,
    pub alignment: StageStats,
    pub admission: StageStats,
    pub queue: StageStats,
    /// Host syscall submission to CQ observation: includes kernel, network,
    /// target and device, and is not a device or network-only latency.
    pub submit_to_cq: StageStats,
    pub extraction: StageStats,
    pub verification: StageStats,
}
pub(crate) type SharedStats = Arc<Mutex<NvmeofStats>>;

fn fields(s: &NvmeofStats) -> Vec<(&'static str, u64, bool)> {
    let mut v = vec![
        ("logical_requests_total", s.logical_requests, false),
        ("logical_members_total", s.logical_members, false),
        ("eligible_hits_total", s.eligible_hits, false),
        ("ineligible_fallbacks_total", s.ineligible_fallbacks, false),
        (
            "aligned_requested_bytes_total",
            s.aligned_requested_bytes,
            false,
        ),
        ("aligned_read_bytes_total", s.aligned_read_bytes, false),
        ("io_submissions_total", s.io_submissions, false),
        ("io_completions_total", s.io_completions, false),
        ("io_errors_total", s.io_errors, false),
        ("io_timeouts_total", s.io_timeouts, false),
    ];
    for (name, stage) in [
        ("alignment", s.alignment),
        ("admission", s.admission),
        ("queue", s.queue),
        ("submit_to_cq", s.submit_to_cq),
        ("extraction", s.extraction),
        ("verification", s.verification),
    ] {
        let names = match name {
            "alignment" => [
                "alignment_observations_total",
                "alignment_seconds_total",
                "alignment_max_seconds",
            ],
            "admission" => [
                "admission_observations_total",
                "admission_seconds_total",
                "admission_max_seconds",
            ],
            "queue" => [
                "queue_observations_total",
                "queue_seconds_total",
                "queue_max_seconds",
            ],
            "submit_to_cq" => [
                "submit_to_cq_observations_total",
                "submit_to_cq_seconds_total",
                "submit_to_cq_max_seconds",
            ],
            "extraction" => [
                "extraction_observations_total",
                "extraction_seconds_total",
                "extraction_max_seconds",
            ],
            _ => [
                "verification_observations_total",
                "verification_seconds_total",
                "verification_max_seconds",
            ],
        };
        v.push((names[0], stage.count, false));
        v.push((names[1], stage.sum_ns, false));
        v.push((names[2], stage.max_ns, true));
    }
    v
}
struct SourceSet {
    active: Vec<SharedStats>,
    retired: Vec<(&'static str, u64, bool)>,
}
#[derive(Clone)]
pub struct NvmeofMetrics {
    sources: Arc<RwLock<SourceSet>>,
}
struct StatsCollector {
    descs: Vec<Desc>,
    sources: Arc<RwLock<SourceSet>>,
}
impl Collector for StatsCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }
    fn collect(&self) -> Vec<MetricFamily> {
        let sources = self.sources.read().unwrap_or_else(|e| e.into_inner());
        let mut values = sources.retired.clone();
        for source in &sources.active {
            let snapshot = source.lock().unwrap_or_else(|e| e.into_inner()).clone();
            for (target, (_, value, max)) in values.iter_mut().zip(fields(&snapshot)) {
                target.1 = if max {
                    target.1.max(value)
                } else {
                    target.1.saturating_add(value)
                };
            }
        }
        self.descs
            .iter()
            .zip(values)
            .map(|(desc, (name, value, max))| {
                let value = if name.ends_with("_seconds_total") || name.ends_with("_max_seconds") {
                    value as f64 / 1_000_000_000.0
                } else {
                    value as f64
                };
                let mut metric = Metric::default();
                let mut family = MetricFamily::default();
                family.set_name(desc.fq_name.clone());
                family.set_help(desc.help.clone());
                if max {
                    let mut gauge = Gauge::default();
                    gauge.set_value(value);
                    metric.set_gauge(gauge);
                    family.set_field_type(MetricType::GAUGE);
                } else {
                    let mut counter = Counter::default();
                    counter.set_value(value);
                    metric.set_counter(counter);
                    family.set_field_type(MetricType::COUNTER);
                }
                family.set_metric(vec![metric]);
                family
            })
            .collect()
    }
}
impl NvmeofMetrics {
    pub fn register(registry: &prometheus::Registry) -> Result<Self, prometheus::Error> {
        let descs = fields(&NvmeofStats::default())
            .into_iter()
            .map(|(name, _, _)| {
                Desc::new(
                    format!("tidehunter_nvmeof_{name}"),
                    format!("NVMe-oF reader {name}"),
                    vec![],
                    HashMap::new(),
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let sources = Arc::new(RwLock::new(SourceSet {
            active: Vec::new(),
            retired: fields(&NvmeofStats::default()),
        }));
        registry.register(Box::new(StatsCollector {
            descs,
            sources: sources.clone(),
        }))?;
        Ok(Self { sources })
    }
    /// Replace the live reader set after installation or removal.
    pub fn replace_sources(&self, readers: &[&super::NvmeofReader]) {
        let next: Vec<_> = readers.iter().map(|reader| reader.stats.clone()).collect();
        let mut sources = self.sources.write().unwrap_or_else(|e| e.into_inner());
        let removed: Vec<_> = sources
            .active
            .iter()
            .filter(|old| !next.iter().any(|new| Arc::ptr_eq(old, new)))
            .cloned()
            .collect();
        for source in removed {
            let snapshot = source.lock().unwrap_or_else(|e| e.into_inner()).clone();
            for (target, (_, value, max)) in sources.retired.iter_mut().zip(fields(&snapshot)) {
                target.1 = if max {
                    target.1.max(value)
                } else {
                    target.1.saturating_add(value)
                };
            }
        }
        sources.active = next;
    }
}
