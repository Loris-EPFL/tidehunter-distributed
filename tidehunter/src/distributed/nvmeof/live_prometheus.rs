//! Pull-only aggregate live NVMe metrics. Namespace identities are evidence,
//! never metric labels. CQ time includes the kernel, fabric, target and storage.
use super::live_metrics::{LiveNvmeMetrics, LiveNvmeStats};
use prometheus::core::{Collector, Desc};
use prometheus::proto::{Counter, Gauge, Metric, MetricFamily, MetricType};
use std::collections::HashMap;
use std::sync::Arc;

fn fields(s: &LiveNvmeStats) -> Vec<(&'static str, u64, bool, &'static str)> {
    let c = s.counts;
    let mut values = vec![
        (
            "completed_read_ops_total",
            c.completed_read_ops,
            false,
            "Completed live read operations",
        ),
        (
            "completed_read_bytes_total",
            c.completed_read_bytes,
            false,
            "Bytes completed by live read operations",
        ),
        (
            "completed_write_ops_total",
            c.completed_write_ops,
            false,
            "Completed live write operations",
        ),
        (
            "completed_write_bytes_total",
            c.completed_write_bytes,
            false,
            "Bytes completed by live write operations",
        ),
        (
            "recovery_read_ops_total",
            c.recovery_read_ops,
            false,
            "Completed live recovery read operations",
        ),
        (
            "recovery_read_bytes_total",
            c.recovery_read_bytes,
            false,
            "Bytes completed by live recovery reads",
        ),
        (
            "metadata_requests_total",
            c.metadata_requests,
            false,
            "Live metadata control requests",
        ),
        (
            "namespace_connects_total",
            c.namespace_connects,
            false,
            "New verified live controller connections",
        ),
        (
            "unknown_writes_total",
            c.unknown_writes,
            false,
            "Writes with unknown outcome",
        ),
        ("errors_total", c.errors, false, "Live data plane errors"),
        (
            "payload_rpc_requests_total",
            c.payload_rpc_requests,
            false,
            "Live payload RPC requests",
        ),
    ];
    macro_rules! stage {
        ($name:literal, $ns:ident, $samples:ident, $help:literal) => {
            values.push((concat!($name, "_seconds_total"), c.$ns, false, $help));
            values.push((
                concat!($name, "_samples_total"),
                c.$samples,
                false,
                "Timing samples; zero means unavailable",
            ));
        };
    }
    stage!(
        "queue",
        queue_ns,
        queue_samples,
        "Aggregate queue time in seconds"
    );
    stage!(
        "submit",
        submit_ns,
        submit_samples,
        "Aggregate submission time in seconds"
    );
    stage!(
        "cq",
        cq_ns,
        cq_samples,
        "Submission to observed CQ seconds; includes kernel, fabric, target and storage"
    );
    stage!(
        "control",
        control_ns,
        control_samples,
        "Aggregate metadata control time in seconds"
    );
    stage!(
        "validation",
        validation_ns,
        validation_samples,
        "Aggregate validation time in seconds"
    );
    stage!(
        "persistence",
        persistence_ns,
        persistence_samples,
        "Aggregate persistence time in seconds; nests WAL and frontier stages"
    );
    stage!(
        "allocation",
        allocation_ns,
        allocation_samples,
        "Aggregate allocation time in seconds"
    );
    stage!(
        "publication",
        publication_ns,
        publication_samples,
        "Aggregate publication time in seconds"
    );
    stage!(
        "namespace_setup",
        namespace_setup_ns,
        namespace_setup_samples,
        "Aggregate namespace setup time in seconds"
    );
    stage!(
        "read_io",
        read_io_ns,
        read_io_samples,
        "Aggregate read I/O wait time in seconds"
    );
    stage!(
        "owner_completion",
        owner_completion_ns,
        owner_completion_samples,
        "Owner full-frame read and validation time in seconds, not client CPU"
    );
    stage!(
        "wal_persistence",
        wal_persistence_ns,
        wal_persistence_samples,
        "WAL persistence time in seconds; nested in persistence"
    );
    stage!(
        "wal_namespace",
        wal_namespace_ns,
        wal_namespace_samples,
        "WAL namespace persistence time in seconds; nested in persistence"
    );
    stage!(
        "frontier_file",
        frontier_file_ns,
        frontier_file_samples,
        "Frontier file persistence time in seconds; nested in persistence"
    );
    stage!(
        "frontier_namespace",
        frontier_namespace_ns,
        frontier_namespace_samples,
        "Frontier namespace persistence time in seconds; nested in persistence"
    );
    stage!(
        "delivery",
        delivery_ns,
        delivery_samples,
        "Observed CQE to delivered, validated read result in seconds"
    );
    stage!(
        "controller_connect",
        controller_connect_ns,
        controller_connect_samples,
        "Blocking kernel controller connect and verification in seconds, excluding pool waits"
    );
    stage!(
        "controller_disconnect",
        controller_disconnect_ns,
        controller_disconnect_samples,
        "Blocking idle-controller eviction delete in seconds"
    );
    values.extend([
        (
            "lane_inflight_samples_total",
            c.lane_inflight_samples,
            false,
            "Lane in-flight occupancy samples, one per flight submission",
        ),
        (
            "lane_inflight_requests_sum_total",
            c.lane_inflight_request_sum,
            false,
            "Sum of requests in flight at each sample; divide by samples for the mean",
        ),
        (
            "lane_inflight_operations_sum_total",
            c.lane_inflight_op_sum,
            false,
            "Sum of member operations in flight at each sample; divide by samples for the mean",
        ),
        (
            "lane_inflight_requests_max",
            c.lane_inflight_request_max,
            true,
            "Largest requests in flight on any lane",
        ),
        (
            "lane_inflight_operations_max",
            c.lane_inflight_op_max,
            true,
            "Largest member operations in flight on any lane",
        ),
        (
            "grant_hits_total",
            c.grant_hits,
            false,
            "Live reads that found their file grant cached",
        ),
        (
            "grant_misses_total",
            c.grant_misses,
            false,
            "Live reads that had to resolve and open a file grant",
        ),
        (
            "grant_evictions_total",
            c.grant_evictions,
            false,
            "Cached file grants retired to make room",
        ),
        (
            "copy_bytes_avoided_total",
            c.copy_bytes_avoided,
            false,
            "Member bytes delivered as slices of the I/O block instead of a copy",
        ),
    ]);
    // Each evidence entry contains cumulative per-owner counters. Use only the
    // latest snapshot per partition, not the sum of overlapping snapshots.
    let mut latest = HashMap::new();
    for fence in &s.fences {
        latest.insert(fence.partition, &fence.target);
    }
    let mut namespace = 0u64;
    let mut drains = 0u64;
    let mut fences = 0u64;
    let mut seconds = 0u64;
    let mut exposed = 0u64;
    let mut readonly = 0u64;
    let mut errors = 0u64;
    let mut fixtures = 0u64;
    for target in latest.values() {
        if target.fixture_only {
            fixtures += 1;
            continue;
        }
        namespace = namespace.saturating_add(target.owned_namespace_count);
        drains = drains.saturating_add(target.disable_drain_successes);
        fences = fences.saturating_add(target.fence_count);
        seconds = seconds.saturating_add(target.disable_drain_time_ns);
        exposed = exposed.saturating_add(target.namespaces_exposed);
        readonly = readonly.saturating_add(target.readonly_commission_successes);
        errors = errors.saturating_add(target.errors);
    }
    values.extend([
        (
            "target_observed_partitions",
            latest.len() as u64,
            true,
            "Partitions represented by recorded fence evidence",
        ),
        (
            "target_fixture_partitions",
            fixtures,
            true,
            "Fixture-only target evidence, excluded from kernel counters",
        ),
        (
            "target_owned_namespaces",
            namespace,
            true,
            "Namespaces at latest recorded target snapshot, not a live probe",
        ),
        (
            "target_exposed_total",
            exposed,
            false,
            "Namespaces exposed at latest recorded kernel target snapshots",
        ),
        (
            "target_disable_drain_total",
            drains,
            false,
            "Successful disable/drains at latest recorded kernel target snapshots",
        ),
        (
            "target_disable_drain_seconds_total",
            seconds,
            false,
            "Time spent in successful target disable/drains",
        ),
        (
            "target_readonly_commission_total",
            readonly,
            false,
            "Successful read-only commissions at latest target snapshots",
        ),
        (
            "target_fence_total",
            fences,
            false,
            "Target fence operations at latest recorded target snapshots",
        ),
        (
            "target_errors_total",
            errors,
            false,
            "Errors at latest recorded kernel target snapshots",
        ),
        (
            "route_evidence_dropped_total",
            s.route_evidence_dropped,
            false,
            "Route evidence entries dropped due to bounded capacity",
        ),
        (
            "fence_evidence_dropped_total",
            s.fence_evidence_dropped,
            false,
            "Fence evidence entries dropped due to bounded capacity",
        ),
    ]);
    values
}

struct LiveCollector {
    descs: Vec<Desc>,
    metrics: Arc<LiveNvmeMetrics>,
}
impl Collector for LiveCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }
    fn collect(&self) -> Vec<MetricFamily> {
        self.descs
            .iter()
            .zip(fields(&self.metrics.snapshot()))
            .map(|(desc, (name, raw, gauge, _))| {
                let value = if name.ends_with("_seconds_total") {
                    raw as f64 / 1_000_000_000.0
                } else {
                    raw as f64
                };
                let mut metric = Metric::default();
                let mut family = MetricFamily::default();
                family.set_name(desc.fq_name.clone());
                family.set_help(desc.help.clone());
                if gauge {
                    let mut data = Gauge::default();
                    data.set_value(value);
                    metric.set_gauge(data);
                    family.set_field_type(MetricType::GAUGE);
                } else {
                    let mut data = Counter::default();
                    data.set_value(value);
                    metric.set_counter(data);
                    family.set_field_type(MetricType::COUNTER);
                }
                family.set_metric(vec![metric]);
                family
            })
            .collect()
    }
}

pub(crate) fn register(
    registry: &prometheus::Registry,
    metrics: Arc<LiveNvmeMetrics>,
) -> Result<(), prometheus::Error> {
    let descs = fields(&LiveNvmeStats::default())
        .into_iter()
        .map(|(name, _, _, help)| {
            Desc::new(
                format!("tidehunter_nvmeof_live_{name}"),
                help.to_owned(),
                vec![],
                HashMap::new(),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    registry.register(Box::new(LiveCollector { descs, metrics }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed::nvmeof::LiveTargetStats;
    use crate::distributed::nvmeof::live_metrics::{LiveFenceEvidence, LiveTimingStage};
    #[test]
    fn scrape_aggregates_counters_and_latest_nonfixture_fence() {
        let metrics = LiveNvmeMetrics::new();
        let producer = metrics.register().unwrap();
        producer.record_timing(LiveTimingStage::Completion, 2_000_000_000, 2);
        for (partition, drains, fixture) in [(2, 1, false), (2, 3, false), (3, 9, true)] {
            metrics.record_fence(LiveFenceEvidence {
                partition,
                authority_epoch: 1,
                target: LiveTargetStats {
                    fixture_only: fixture,
                    disable_drain_successes: drains,
                    ..Default::default()
                },
            });
        }
        let registry = prometheus::Registry::new();
        register(&registry, metrics).unwrap();
        let families = registry.gather();
        let counter = |name: &str| {
            families.iter().find(|f| f.name() == name).unwrap().metric[0]
                .counter
                .as_ref()
                .unwrap()
                .value()
        };
        assert_eq!(counter("tidehunter_nvmeof_live_cq_seconds_total"), 2.0);
        assert_eq!(
            counter("tidehunter_nvmeof_live_target_disable_drain_total"),
            3.0
        );
    }
}
