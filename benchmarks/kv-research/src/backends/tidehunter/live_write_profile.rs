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
];

/// Lifetime maxima kept across lanes. A gauge peak is not additive and a delta of it is
/// meaningless, so both window boundaries are recorded and never subtracted.
const PEAKS: &[&str] = &["lane_inflight_request_max", "lane_inflight_op_max"];

fn counts(stats: Option<LiveNvmeStats>) -> Result<Value, String> {
    let stats = stats.ok_or("live NVMe counter snapshot unavailable")?;
    serde_json::to_value(stats.counts).map_err(|error| error.to_string())
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
        let selected = deltas["completed_write_ops"].clone();
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
        window.freeze(Some(end.clone())).unwrap();
        end.counts.completed_write_ops = 100;
        assert!(window.freeze(Some(end)).is_err());
        let result = window.finish().unwrap();
        assert_eq!(result["selected_operations"], 3);
        assert_eq!(result["stages"]["live_nvmeof"]["completed_read_ops"], 2);
        assert_eq!(result["stages"]["live_nvmeof"]["cq_samples"], 3);
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
        assert_eq!(window.finish().unwrap()["selected_operations"], 0);
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
