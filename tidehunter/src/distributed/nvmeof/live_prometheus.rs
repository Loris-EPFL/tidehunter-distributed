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
            "lease_exchanges_total",
            c.lease_exchanges,
            false,
            "Initial extent lease control exchanges",
        ),
        (
            "leased_frames_total",
            c.leased_frames,
            false,
            "Completed leased frames",
        ),
        (
            "leased_worker_waits_total",
            c.leased_worker_waits,
            false,
            "Waits on the acknowledgement path: at least one leased frame in flight",
        ),
        (
            "leased_worker_wait_timeouts_total",
            c.leased_worker_wait_timeouts,
            false,
            "Timed-out waits on the acknowledgement path: at least one leased frame in flight",
        ),
        (
            "leased_flushes_total",
            c.leased_flushes,
            false,
            "Submitted namespace flushes",
        ),
        (
            "leased_flush_seconds_total",
            c.leased_flush_ns,
            false,
            "Flush submission to local CQE harvest, including worker wake delay",
        ),
        (
            "leased_flush_samples_total",
            c.leased_flush_samples,
            false,
            "Successful namespace flush observations",
        ),
        (
            "leased_flush_covered_frames_total",
            c.leased_flush_covered_frames,
            false,
            "Non-FUA frames captured by submitted namespace flushes",
        ),
        (
            "read_lane_pin_failures_total",
            c.read_lane_pin_failures,
            false,
            "Read lane affinity failures",
        ),
        (
            "leased_bytes_total",
            c.leased_bytes,
            false,
            "Completed leased frame bytes",
        ),
        (
            "leased_skip_markers_total",
            c.leased_skip_markers,
            false,
            "Completed leased skip marker blocks",
        ),
        (
            "extents_leased_total",
            c.extents_leased,
            false,
            "Leased WAL extents",
        ),
        (
            "extent_preleases_total",
            c.extent_preleases,
            false,
            "Preleased WAL extents",
        ),
        (
            "extent_switch_waits_total",
            c.extent_switch_waits,
            false,
            "Extent switches waiting for lease-ahead",
        ),
        (
            "lease_profile_samples_total",
            c.lease_profile_samples,
            false,
            "Validated owner lease profiles",
        ),
        (
            "adopt_profile_samples_total",
            c.adopt_profile_samples,
            false,
            "Validated owner adoption profiles",
        ),
        (
            "adopt_close_scan_bytes_total",
            c.adopt_close_scan_bytes,
            false,
            "Owner close suffix bytes scanned",
        ),
        (
            "adoption_async_exchanges_total",
            c.adoption_async_exchanges,
            false,
            "Submitted asynchronous adoptions",
        ),
        (
            "adoption_sync_exchanges_total",
            c.adoption_sync_exchanges,
            false,
            "Synchronous adoption exchanges",
        ),
        (
            "adoption_history_full_settles_total",
            c.adoption_history_full_settles,
            false,
            "Successful settles forced by full leased prefix history",
        ),
        (
            "leased_failed_writes_total",
            c.leased_failed_writes,
            false,
            "Leased write failures excluding short, timeout and full outcomes",
        ),
        (
            "leased_short_writes_total",
            c.leased_short_writes,
            false,
            "Leased writes with a short terminal CQE",
        ),
        (
            "leased_timed_out_writes_total",
            c.leased_timed_out_writes,
            false,
            "Leased writes failing their I/O deadline",
        ),
        (
            "leased_full_writes_total",
            c.leased_full_writes,
            false,
            "Leased writes rejected by queue credit",
        ),
        (
            "fua_writes_total",
            c.fua_writes,
            false,
            "Admitted Data FUA writes including skip markers; not a terminal success count",
        ),
        (
            "failed_write_ops_total",
            c.failed_write_ops,
            false,
            "Negative terminal write CQEs including retirement after a timeout",
        ),
        (
            "short_write_ops_total",
            c.short_write_ops,
            false,
            "Short terminal write CQEs including retirement after a timeout",
        ),
        (
            "short_write_bytes_total",
            c.short_write_bytes,
            false,
            "Bytes reported by short write CQEs; not acknowledged bytes",
        ),
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
    ];
    macro_rules! stage {
        ($name:literal, $ns:ident, $samples:ident, $help:literal $(,)?) => {
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
        "leased_write",
        leased_write_ns,
        leased_write_samples,
        "Successful leased block writes including skips: submission return to local CQE harvest; includes worker wake delay; not device time; excludes failed writes and drain",
    );
    stage!(
        "extent_switch",
        extent_switch_ns,
        extent_switch_samples,
        "Synchronous leased extent switch seconds including skip and adoption"
    );
    values.extend([
        (
            "lease_reserve_seconds_total",
            c.lease_reserve_ns,
            false,
            "Owner extent allocation seconds",
        ),
        (
            "lease_namespace_sync_seconds_total",
            c.lease_namespace_sync_ns,
            false,
            "Owner lease namespace persistence seconds",
        ),
        (
            "lease_publish_seconds_total",
            c.lease_publish_ns,
            false,
            "Owner lease export publication seconds",
        ),
        (
            "adopt_owner_seconds_total",
            c.adopt_owner_ns,
            false,
            "Owner adoption seconds including close scan",
        ),
        (
            "adopt_close_scan_seconds_total",
            c.adopt_close_scan_ns,
            false,
            "Owner close suffix scan seconds",
        ),
    ]);
    stage!(
        "adoption_async",
        adoption_async_ns,
        adoption_async_samples,
        "Successful asynchronous adoption post-submit to validated reply harvest seconds; includes harvest delay"
    );
    stage!(
        "adoption_settle",
        adoption_settle_ns,
        adoption_settle_samples,
        "Successful open-extent settle seconds including pending asynchronous adoption"
    );
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
        (
            "leased_frame_batches_total",
            c.leased_frame_batches,
            false,
            "Complete batches in released leased frames",
        ),
        (
            "write_submit_calls_total",
            c.write_submit_calls,
            false,
            "Write-queue io_uring enter calls that consumed an SQE",
        ),
        (
            "owner_drains_total",
            c.owner_drains,
            false,
            "Owner-worker drains of the READY owner queue",
        ),
        (
            "owner_drain_batches_total",
            c.owner_drain_batches,
            false,
            "Batches taken by owner-worker drains",
        ),
        (
            "owner_drain_stop_empty_total",
            c.owner_drain_stop_empty,
            false,
            "Drains stopped at an unallocated owner-queue entry",
        ),
        (
            "owner_drain_stop_allocated_total",
            c.owner_drain_stop_allocated,
            false,
            "Drains stopped at an ALLOCATED owner-queue head",
        ),
        (
            "owner_drain_stop_fence_total",
            c.owner_drain_stop_fence,
            false,
            "Drains stopped at an ordered control or snapshot bound",
        ),
        (
            "owner_drain_stop_limit_total",
            c.owner_drain_stop_limit,
            false,
            "Drains stopped by group, reference or frame geometry",
        ),
        (
            "owner_drain_stop_credit_total",
            c.owner_drain_stop_credit,
            false,
            "Drains stopped by transport byte credit or append occupancy",
        ),
        (
            "leased_submit_ready_left_total",
            c.leased_submit_ready_left,
            false,
            "Leased frame submissions that left READY batches unsubmitted",
        ),
        (
            "leased_submit_ready_left_batches_total",
            c.leased_submit_ready_left_batches,
            false,
            "READY batches left unsubmitted at leased frame submissions",
        ),
        (
            "leased_submit_ready_left_full_total",
            c.leased_submit_ready_left_full,
            false,
            "Full leased frames submitted while READY batches remained",
        ),
        (
            "leased_submit_ready_left_full_batches_total",
            c.leased_submit_ready_left_full_batches,
            false,
            "READY batches left behind full leased frames",
        ),
        (
            "leased_deferred_submits_total",
            c.leased_deferred_submits,
            false,
            "Submissions of a leased group deferred earlier for credit",
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
        producer.record_leased_frame(4096, 2);
        producer.record_leased_worker_wait(false);
        producer.record_leased_worker_wait(true);
        producer.record_leased_flush(3);
        producer.record_leased_flush_completion(2_000_000_000);
        producer.record_read_lane_pin_failure();
        producer.record_leased_skip();
        producer.record_extent_lease();
        producer.record_adoption(false);
        producer.record_adoption(true);
        producer.record_fua_write();
        producer.record_terminal_write_outcomes(1, 2, 2048);
        producer.record_timing(LiveTimingStage::LeasedWrite, 1_000_000_000, 1);
        producer.record_timing(LiveTimingStage::ExtentSwitch, 3_000_000_000, 1);
        producer.record_timing(LiveTimingStage::AdoptionAsync, 4_000_000_000, 2);
        producer.record_timing(LiveTimingStage::AdoptionSettle, 6_000_000_000, 3);
        producer.record_adoption_history_full_settle();
        producer.record_lease_profile(crate::distributed::tcp::LeaseProfile {
            reserve_ns: 1_000_000_000,
            namespace_sync_ns: 2_000_000_000,
            publish_ns: 3_000_000_000,
        });
        producer.record_adopt_profile(crate::distributed::tcp::AdoptProfile {
            frames: 2,
            adopt_ns: 5_000_000_000,
            close_scan_ns: 4_000_000_000,
            close_scan_bytes: 4096,
        });
        use crate::distributed::nvmeof::live_metrics::LiveWriteFailure;
        for outcome in [
            LiveWriteFailure::Failed,
            LiveWriteFailure::Short,
            LiveWriteFailure::TimedOut,
            LiveWriteFailure::Full,
        ] {
            producer.record_leased_write_failure(outcome);
        }
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
        for (name, expected) in [
            ("leased_worker_waits_total", 2.0),
            ("leased_worker_wait_timeouts_total", 1.0),
            ("leased_flushes_total", 1.0),
            ("leased_flush_seconds_total", 2.0),
            ("leased_flush_samples_total", 1.0),
            ("leased_flush_covered_frames_total", 3.0),
            ("read_lane_pin_failures_total", 1.0),
            ("adoption_async_seconds_total", 4.0),
            ("adoption_async_samples_total", 2.0),
            ("adoption_settle_seconds_total", 6.0),
            ("adoption_settle_samples_total", 3.0),
            ("adoption_history_full_settles_total", 1.0),
            ("lease_reserve_seconds_total", 1.0),
            ("lease_namespace_sync_seconds_total", 2.0),
            ("lease_publish_seconds_total", 3.0),
            ("lease_profile_samples_total", 1.0),
            ("adopt_owner_seconds_total", 5.0),
            ("adopt_close_scan_seconds_total", 4.0),
            ("adopt_close_scan_bytes_total", 4096.0),
            ("adopt_profile_samples_total", 1.0),
            ("extent_preleases_total", 0.0),
            ("extent_switch_waits_total", 0.0),
        ] {
            assert_eq!(counter(&format!("tidehunter_nvmeof_live_{name}")), expected);
        }
        assert_eq!(counter("tidehunter_nvmeof_live_leased_frames_total"), 1.0);
        assert_eq!(counter("tidehunter_nvmeof_live_leased_bytes_total"), 4096.0);
        assert_eq!(
            counter("tidehunter_nvmeof_live_leased_frame_batches_total"),
            2.0
        );
        assert_eq!(
            counter("tidehunter_nvmeof_live_leased_skip_markers_total"),
            1.0
        );
        assert_eq!(counter("tidehunter_nvmeof_live_extents_leased_total"), 1.0);
        assert_eq!(
            counter("tidehunter_nvmeof_live_adoption_async_exchanges_total"),
            1.0
        );
        assert_eq!(
            counter("tidehunter_nvmeof_live_adoption_sync_exchanges_total"),
            1.0
        );
        assert_eq!(counter("tidehunter_nvmeof_live_fua_writes_total"), 1.0);
        assert_eq!(
            counter("tidehunter_nvmeof_live_failed_write_ops_total"),
            1.0
        );
        assert_eq!(counter("tidehunter_nvmeof_live_short_write_ops_total"), 2.0);
        assert_eq!(
            counter("tidehunter_nvmeof_live_short_write_bytes_total"),
            2048.0
        );
        assert_eq!(
            counter("tidehunter_nvmeof_live_leased_write_seconds_total"),
            1.0
        );
        assert_eq!(
            counter("tidehunter_nvmeof_live_leased_write_samples_total"),
            1.0
        );
        for name in ["failed", "short", "timed_out", "full"] {
            assert_eq!(
                counter(&format!(
                    "tidehunter_nvmeof_live_leased_{name}_writes_total"
                )),
                1.0
            );
        }
        assert_eq!(
            counter("tidehunter_nvmeof_live_extent_switch_seconds_total"),
            3.0
        );
        assert_eq!(
            counter("tidehunter_nvmeof_live_target_disable_drain_total"),
            3.0
        );
    }
}
