use super::*;

#[cfg(test)]
#[test]
fn live_read_lane_pin_failure_is_counted_without_failing_the_read() {
    let temp = tempdir::TempDir::new("read-lane-pin-failure").unwrap();
    let metrics = crate::distributed::nvmeof::live_metrics::LiveNvmeMetrics::new();
    let counters = metrics.register().unwrap();
    pin_live_read_lane(
        Some(ReadLanePlacement {
            device: Ok(temp.path().join("missing-device")),
            counters: Some(counters),
        }),
        0,
    );
    assert_eq!(metrics.snapshot().counts.read_lane_pin_failures, 1);
}
