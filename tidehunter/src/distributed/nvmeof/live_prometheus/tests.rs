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
                write_protect_successes: drains,
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
    assert_eq!(
        counter("tidehunter_nvmeof_live_target_write_protect_total"),
        3.0
    );
}
