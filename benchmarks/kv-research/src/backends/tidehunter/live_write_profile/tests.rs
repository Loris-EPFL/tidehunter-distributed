use super::*;

/// Hang guard for a wait that follows another thread's or the boundary's
/// progress. It is never the expected wait: the boundary's own quiesce
/// window and the reply handoff take milliseconds, so only a dead peer or a
/// broken wakeup reaches this cap.
const WAIT_HANG_GUARD: std::time::Duration = std::time::Duration::from_secs(120);

#[test]
fn leased_boundary_waits_for_validated_reply_before_freezing() {
    use std::sync::{Arc, mpsc};
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
            WAIT_HANG_GUARD,
        );
        finished.send(snapshot).unwrap();
    });
    first_snapshot
        .recv_timeout(WAIT_HANG_GUARD)
        .expect("the boundary never took its snapshot");
    assert!(matches!(result.try_recv(), Err(mpsc::TryRecvError::Empty)));
    release.send(()).unwrap();
    let stats = result
        .recv_timeout(WAIT_HANG_GUARD)
        .expect("the boundary never finished")
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
    // The deadline is honored, not ignored: only the hang guard, never a
    // promptness premise that load alone can exceed.
    assert!(began.elapsed() < WAIT_HANG_GUARD);
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
