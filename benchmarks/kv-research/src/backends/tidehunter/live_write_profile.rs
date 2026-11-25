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
    "publication_ns",
    "publication_samples",
    "namespace_setup_ns",
    "namespace_setup_samples",
    "read_io_ns",
    "read_io_samples",
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
/// adoption replies while idle. Non-live routes never wait here.
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
mod tests;
