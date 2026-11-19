use serde_json::{Map, Value, json};
use tidehunter::distributed::nvmeof::live_metrics::LiveNvmeStats;

const COUNTERS: &[&str] = &[
    "completed_read_ops",
    "completed_read_bytes",
    "completed_write_ops",
    "completed_write_bytes",
    "recovery_read_ops",
    "recovery_read_bytes",
    "metadata_requests",
    "namespace_connects",
    "unknown_writes",
    "errors",
    "payload_rpc_requests",
    "queue_ns",
    "queue_samples",
    "submit_ns",
    "submit_samples",
    "cq_ns",
    "cq_samples",
    "control_ns",
    "control_samples",
    "validation_ns",
    "validation_samples",
    "persistence_ns",
    "persistence_samples",
    "allocation_ns",
    "allocation_samples",
    "publication_ns",
    "publication_samples",
    "namespace_setup_ns",
    "namespace_setup_samples",
    "read_io_ns",
    "read_io_samples",
    "owner_completion_ns",
    "owner_completion_samples",
    "wal_persistence_ns",
    "wal_persistence_samples",
    "wal_namespace_ns",
    "wal_namespace_samples",
    "frontier_file_ns",
    "frontier_file_samples",
    "frontier_namespace_ns",
    "frontier_namespace_samples",
    "lane_inflight_samples",
    "lane_inflight_request_sum",
    "lane_inflight_op_sum",
    "delivery_ns",
    "delivery_samples",
    "controller_connect_ns",
    "controller_connect_samples",
    "controller_disconnect_ns",
    "controller_disconnect_samples",
    "grant_hits",
    "grant_misses",
    "grant_evictions",
    "copy_bytes_avoided",
    "reserve_exchanges",
    "complete_exchanges",
    "lease_exchanges",
    "leased_frames",
    "leased_eventfd_unavailable",
    "leased_worker_waits",
    "leased_worker_wait_timeouts",
    "leased_flushes",
    "leased_flush_ns",
    "leased_flush_samples",
    "leased_flush_covered_frames",
    "read_lane_pin_failures",
    "leased_bytes",
    "leased_skip_markers",
    "extents_leased",
    "adoption_async_exchanges",
    "adoption_sync_exchanges",
    "adoption_async_ns",
    "adoption_async_samples",
    "adoption_settle_ns",
    "adoption_settle_samples",
    "adoption_history_full_settles",
    "fua_writes",
    "leased_write_ns",
    "leased_write_samples",
    "extent_switch_ns",
    "extent_switch_samples",
    "lease_reserve_ns",
    "lease_namespace_sync_ns",
    "lease_publish_ns",
    "lease_profile_samples",
    "adopt_owner_ns",
    "adopt_close_scan_ns",
    "adopt_close_scan_bytes",
    "adopt_profile_samples",
    "extent_preleases",
    "extent_switch_waits",
    "leased_failed_writes",
    "leased_short_writes",
    "leased_timed_out_writes",
    "leased_full_writes",
    "failed_write_ops",
    "short_write_ops",
    "short_write_bytes",
];

/// Lifetime maxima kept across lanes. A gauge peak is not additive and a delta of it is
/// meaningless, so both window boundaries are recorded and never subtracted.
const PEAKS: &[&str] = &[
    "lane_inflight_request_max",
    "lane_inflight_op_max",
    "max_leased_in_flight",
];

fn counts(stats: Option<LiveNvmeStats>) -> Result<Value, String> {
    let stats = stats.ok_or("live NVMe counter snapshot unavailable")?;
    serde_json::to_value(stats.counts).map_err(|error| error.to_string())
}

/// Call only after API callers drain. The owner worker continues to harvest
/// adoption replies while idle. Reserved and non-live routes never wait here.
/// An adoption not yet submitted at the boundary falls outside the window.
/// This excludes at most one adoption per boundary; the sum/count pair stays
/// consistent. Return the second snapshot with equal submitted, validated and
/// nanosecond values, not the first snapshot.
pub(super) fn boundary_snapshot(
    leased: bool,
    mut snapshot: impl FnMut() -> Option<LiveNvmeStats>,
    timeout: std::time::Duration,
) -> Result<Option<LiveNvmeStats>, crate::backend::LiveAdoptionBoundaryError> {
    use crate::backend::LiveAdoptionBoundaryError;
    if !leased {
        return Ok(snapshot());
    }
    let deadline = std::time::Instant::now() + timeout;
    let mut previous = None;
    loop {
        let stats = snapshot().ok_or(LiveAdoptionBoundaryError(
            "leased adoption boundary counter snapshot unavailable",
        ))?;
        let counts = stats.counts;
        if counts.errors != 0 || counts.unknown_writes != 0 {
            return Err(LiveAdoptionBoundaryError(
                "leased adoption boundary observed a failed or poisoned live client",
            ));
        }
        let observed = (
            counts.adoption_async_exchanges,
            counts.adoption_async_samples,
            counts.adoption_async_ns,
        );
        if counts.adoption_async_exchanges == counts.adoption_async_samples
            && previous == Some(observed)
        {
            return Ok(Some(stats));
        }
        if std::time::Instant::now() >= deadline {
            return Err(LiveAdoptionBoundaryError(
                "leased adoption boundary did not quiesce before the bounded wait expired",
            ));
        }
        previous =
            (counts.adoption_async_exchanges == counts.adoption_async_samples).then_some(observed);
        std::thread::sleep(
            std::time::Duration::from_millis(1)
                .min(deadline.saturating_duration_since(std::time::Instant::now())),
        );
    }
}

pub(super) struct Window {
    limit: usize,
    baseline: Value,
    end: Option<Value>,
}

impl Window {
    pub(super) fn arm(limit: usize, stats: Option<LiveNvmeStats>) -> Result<Self, String> {
        if limit == 0 {
            return Err("live NVMe write profile limit must be nonzero".to_owned());
        }
        Ok(Self {
            limit,
            baseline: counts(stats)?,
            end: None,
        })
    }

    pub(super) fn freeze(&mut self, stats: Option<LiveNvmeStats>) -> Result<(), String> {
        if self.end.is_some() {
            return Err("live NVMe write profile end already frozen".to_owned());
        }
        self.end = Some(counts(stats)?);
        Ok(())
    }

    pub(super) fn finish(self) -> Result<Value, String> {
        let end = self.end.ok_or("live NVMe write profile end not frozen")?;
        let mut deltas = Map::new();
        for &name in COUNTERS {
            let start = self
                .baseline
                .get(name)
                .and_then(Value::as_u64)
                .ok_or_else(|| format!("live NVMe baseline missing counter {name}"))?;
            let finish = end
                .get(name)
                .and_then(Value::as_u64)
                .ok_or_else(|| format!("live NVMe end missing counter {name}"))?;
            let delta = finish
                .checked_sub(start)
                .ok_or_else(|| format!("live NVMe counter decreased: {name}"))?;
            deltas.insert(name.to_owned(), json!(delta));
        }
        for name in ["errors", "unknown_writes"] {
            if deltas[name].as_u64() != Some(0) {
                return Err(format!("live NVMe measurement recorded {name}"));
            }
        }
        let mut peaks = Map::new();
        for &name in PEAKS {
            let boundary = |snapshot: &Value, which: &str| {
                snapshot
                    .get(name)
                    .and_then(Value::as_u64)
                    .ok_or_else(|| format!("live NVMe {which} missing peak {name}"))
            };
            peaks.insert(
                name.to_owned(),
                json!({
                    "at_start": boundary(&self.baseline, "baseline")?,
                    "at_end": boundary(&end, "end")?,
                    "scope": "lifetime maximum, not a measurement-window maximum",
                }),
            );
        }
        let selected = &deltas["completed_write_ops"];
        Ok(json!({
            "runtime_observation_mode": "live_nvmeof_phase_counters",
            "scope": "measurement_nvme_io_cumulative_delta",
            "requested_limit": self.limit,
            "selection_policy": "all completed NVMe I/O in the armed measurement window; nonzero requested limit enables fixed-space counters and does not select the first N operations; selected operations count completed NVMe writes, not logical keys or API calls",
            "selected_operations": selected,
            "stages": {"live_nvmeof": deltas},
            "live_nvmeof_peaks": peaks,
            "clock_scope": "existing client-side cumulative elapsed clock sums and sample counts, baseline at arm and end frozen before the completion gate; submission-to-CQ and read I/O include scheduling, kernel, fabric, target and possible storage; owner timings come from completion responses, not cross-host timestamp subtraction",
            "overlap_semantics": "independent clock populations may overlap or nest and are not additive latency fractions; zero samples mean unavailable timing, not zero latency; no per-stage percentiles",
            "unsupported_stages": ["physical_device_io", "wire_latency", "per_stage_percentiles"],
            "finalization_error": null
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn leased_boundary_waits_for_validated_reply_before_freezing() {
        use std::sync::{Arc, mpsc};
        use std::time::Duration;
        use tidehunter::distributed::nvmeof::live_metrics::{LiveNvmeMetrics, LiveTimingStage};
        let metrics = LiveNvmeMetrics::new();
        let producer = metrics.register().unwrap();
        producer.record_adoption(false);
        let (release, reply) = mpsc::sync_channel(1);
        let owner = std::thread::spawn(move || {
            reply.recv().unwrap();
            producer.record_timing(LiveTimingStage::AdoptionAsync, 7_000, 1);
        });
        let (entered, first_snapshot) = mpsc::sync_channel(1);
        let (finished, result) = mpsc::sync_channel(1);
        let source = Arc::clone(&metrics);
        let boundary = std::thread::spawn(move || {
            let mut entered = Some(entered);
            let snapshot = boundary_snapshot(
                true,
                || {
                    let snapshot = source.snapshot();
                    if let Some(entered) = entered.take() {
                        entered.send(()).unwrap();
                    }
                    Some(snapshot)
                },
                Duration::from_secs(1),
            );
            finished.send(snapshot).unwrap();
        });
        first_snapshot.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(matches!(result.try_recv(), Err(mpsc::TryRecvError::Empty)));
        release.send(()).unwrap();
        let stats = result
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .unwrap()
            .unwrap();
        owner.join().unwrap();
        boundary.join().unwrap();
        assert_eq!(stats.counts.adoption_async_exchanges, 1);
        assert_eq!(stats.counts.adoption_async_samples, 1);
        assert_eq!(stats.counts.adoption_async_ns, 7_000);
        let window = Window::arm(1, Some(stats)).unwrap();
        assert_eq!(window.baseline["adoption_async_ns"], 7_000);
        assert_eq!(window.baseline["adoption_async_samples"], 1);
    }

    #[test]
    fn leased_boundary_times_out_or_reports_poison() {
        use std::time::{Duration, Instant};
        let mut stats = LiveNvmeStats::default();
        stats.counts.adoption_async_exchanges = 1;
        let began = Instant::now();
        let error =
            boundary_snapshot(true, || Some(stats.clone()), Duration::from_millis(5)).unwrap_err();
        assert!(error.to_string().contains("did not quiesce"));
        assert!(began.elapsed() >= Duration::from_millis(5));
        assert!(began.elapsed() < Duration::from_secs(1));
        stats.counts.errors = 1;
        let error = boundary_snapshot(true, || Some(stats.clone()), Duration::ZERO).unwrap_err();
        assert!(error.to_string().contains("failed or poisoned"));
    }

    #[test]
    fn reserved_and_nonlive_boundaries_do_not_wait_for_adoption() {
        let mut stats = LiveNvmeStats::default();
        stats.counts.adoption_async_exchanges = 1;
        let mut snapshots = 0;
        let result = boundary_snapshot(
            false,
            || {
                snapshots += 1;
                Some(stats.clone())
            },
            std::time::Duration::ZERO,
        )
        .unwrap()
        .unwrap();
        assert_eq!(snapshots, 1);
        assert_eq!(result.counts.adoption_async_samples, 0);
        assert!(
            boundary_snapshot(false, || None, std::time::Duration::ZERO)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn window_freezes_actual_write_and_read_deltas() {
        let mut start = LiveNvmeStats::default();
        start.counts.completed_write_ops = 12;
        start.counts.completed_read_ops = 8;
        let mut window = Window::arm(1, Some(start.clone())).unwrap();
        let mut end = start;
        end.counts.completed_write_ops = 15;
        end.counts.completed_read_ops = 10;
        end.counts.cq_ns = 90;
        end.counts.cq_samples = 3;
        end.counts.adoption_async_ns = 4_000;
        end.counts.adoption_async_samples = 2;
        end.counts.adoption_settle_ns = 9_000;
        end.counts.adoption_settle_samples = 3;
        end.counts.adoption_history_full_settles = 1;
        window.freeze(Some(end.clone())).unwrap();
        end.counts.completed_write_ops = 100;
        assert!(window.freeze(Some(end)).is_err());
        let result = window.finish().unwrap();
        assert_eq!(result["selected_operations"], 3);
        assert_eq!(result["stages"]["live_nvmeof"]["completed_read_ops"], 2);
        assert_eq!(result["stages"]["live_nvmeof"]["cq_samples"], 3);
        for (name, expected) in [
            ("adoption_async_ns", 4_000),
            ("adoption_async_samples", 2),
            ("adoption_settle_ns", 9_000),
            ("adoption_settle_samples", 3),
            ("adoption_history_full_settles", 1),
        ] {
            assert_eq!(result["stages"]["live_nvmeof"][name], expected);
        }
        assert!(result.get("sampled_requests").is_none());
    }

    #[test]
    fn window_carries_lane_delivery_connection_and_grant_counters_and_gauge_boundaries() {
        let mut start = LiveNvmeStats::default();
        start.counts.lane_inflight_samples = 4;
        start.counts.lane_inflight_request_sum = 6;
        start.counts.lane_inflight_op_sum = 9;
        start.counts.delivery_ns = 1_000;
        start.counts.delivery_samples = 4;
        start.counts.controller_connect_ns = 50;
        start.counts.controller_connect_samples = 1;
        start.counts.grant_hits = 3;
        start.counts.copy_bytes_avoided = 4_096;
        start.counts.lane_inflight_request_max = 5;
        start.counts.lane_inflight_op_max = 8;
        let mut window = Window::arm(1, Some(start.clone())).unwrap();
        let mut end = start;
        end.counts.lane_inflight_samples = 10;
        end.counts.lane_inflight_request_sum = 18;
        end.counts.lane_inflight_op_sum = 30;
        end.counts.delivery_ns = 2_500;
        end.counts.delivery_samples = 9;
        end.counts.controller_connect_ns = 350;
        end.counts.controller_connect_samples = 3;
        end.counts.controller_disconnect_ns = 70;
        end.counts.controller_disconnect_samples = 2;
        end.counts.grant_hits = 13;
        end.counts.grant_misses = 2;
        end.counts.grant_evictions = 1;
        end.counts.copy_bytes_avoided = 20_480;
        // A lower end maximum cannot be differenced; it is reported as recorded.
        end.counts.lane_inflight_request_max = 4;
        end.counts.lane_inflight_op_max = 11;
        window.freeze(Some(end)).unwrap();
        let result = window.finish().unwrap();
        let counters = &result["stages"]["live_nvmeof"];
        for (name, expected) in [
            ("lane_inflight_samples", 6),
            ("lane_inflight_request_sum", 12),
            ("lane_inflight_op_sum", 21),
            ("delivery_ns", 1_500),
            ("delivery_samples", 5),
            ("controller_connect_ns", 300),
            ("controller_connect_samples", 2),
            ("controller_disconnect_ns", 70),
            ("controller_disconnect_samples", 2),
            ("grant_hits", 10),
            ("grant_misses", 2),
            ("grant_evictions", 1),
            ("copy_bytes_avoided", 16_384),
        ] {
            assert_eq!(counters[name], expected, "{name}");
        }
        assert!(counters.get("lane_inflight_request_max").is_none());
        let peaks = &result["live_nvmeof_peaks"];
        assert_eq!(peaks["lane_inflight_request_max"]["at_start"], 5);
        assert_eq!(peaks["lane_inflight_request_max"]["at_end"], 4);
        assert_eq!(peaks["lane_inflight_op_max"]["at_start"], 8);
        assert_eq!(peaks["lane_inflight_op_max"]["at_end"], 11);
    }

    #[test]
    fn missing_reset_and_error_evidence_fails_closed() {
        assert!(Window::arm(1, None).is_err());
        assert!(Window::arm(0, Some(LiveNvmeStats::default())).is_err());
        let mut window = Window::arm(1, Some(LiveNvmeStats::default())).unwrap();
        assert!(window.freeze(None).is_err());
        assert!(window.finish().is_err());
        let mut start = LiveNvmeStats::default();
        start.counts.completed_write_ops = 1;
        let mut window = Window::arm(1, Some(start)).unwrap();
        window.freeze(Some(LiveNvmeStats::default())).unwrap();
        assert!(window.finish().is_err());
        for name in ["errors", "unknown_writes"] {
            let mut window = Window::arm(1, Some(LiveNvmeStats::default())).unwrap();
            let mut end = LiveNvmeStats::default();
            if name == "errors" {
                end.counts.errors = 1;
            } else {
                end.counts.unknown_writes = 1;
            }
            window.freeze(Some(end)).unwrap();
            assert!(window.finish().is_err());
        }
        let mut window = Window::arm(1, Some(LiveNvmeStats::default())).unwrap();
        window.freeze(Some(LiveNvmeStats::default())).unwrap();
        let result = window.finish().unwrap();
        assert_eq!(result["selected_operations"], 0);
        // Zero-sample timings remain present as unavailable observations, not
        // fabricated nonzero samples or fields dropped by the phase exporter.
        for name in [
            "reserve_exchanges",
            "complete_exchanges",
            "lease_exchanges",
            "leased_frames",
            "leased_eventfd_unavailable",
            "leased_worker_waits",
            "leased_worker_wait_timeouts",
            "leased_flushes",
            "leased_flush_ns",
            "leased_flush_samples",
            "leased_flush_covered_frames",
            "read_lane_pin_failures",
            "leased_bytes",
            "leased_skip_markers",
            "extents_leased",
            "adoption_async_exchanges",
            "adoption_sync_exchanges",
            "adoption_async_ns",
            "adoption_async_samples",
            "adoption_settle_ns",
            "adoption_settle_samples",
            "adoption_history_full_settles",
            "fua_writes",
            "leased_write_ns",
            "leased_write_samples",
            "extent_switch_ns",
            "extent_switch_samples",
            "lease_reserve_ns",
            "lease_namespace_sync_ns",
            "lease_publish_ns",
            "lease_profile_samples",
            "adopt_owner_ns",
            "adopt_close_scan_ns",
            "adopt_close_scan_bytes",
            "adopt_profile_samples",
            "extent_preleases",
            "extent_switch_waits",
            "leased_failed_writes",
            "leased_short_writes",
            "leased_timed_out_writes",
            "leased_full_writes",
            "failed_write_ops",
            "short_write_ops",
            "short_write_bytes",
        ] {
            assert_eq!(result["stages"]["live_nvmeof"][name], 0, "{name}");
        }
    }

    #[test]
    fn live_write_profile_fields_match_the_observer_fixture() {
        let mut window = Window::arm(2, Some(LiveNvmeStats::default())).unwrap();
        let mut end = LiveNvmeStats::default();
        end.counts.completed_write_ops = 3;
        window.freeze(Some(end)).unwrap();
        let output = window.finish().unwrap();
        let deltas = output["stages"]["live_nvmeof"].as_object().unwrap();
        let mut fields: Vec<&str> = deltas.keys().map(String::as_str).collect();
        fields.sort_unstable();
        let expected: Vec<&str> = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../experiments/kv-comparison/observability/live_nvmeof_write_profile_fields.json"
        )))
        .unwrap();
        assert_eq!(fields, expected);
    }

    #[test]
    fn missing_scalar_is_an_error() {
        let mut window = Window::arm(1, Some(LiveNvmeStats::default())).unwrap();
        window.freeze(Some(LiveNvmeStats::default())).unwrap();
        window
            .end
            .as_mut()
            .unwrap()
            .as_object_mut()
            .unwrap()
            .remove("cq_samples");
        assert!(window.finish().is_err());
    }
}
