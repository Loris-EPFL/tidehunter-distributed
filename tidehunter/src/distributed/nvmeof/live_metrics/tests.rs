use super::*;

#[test]
fn leased_worker_waits_sum_across_producers() {
    let metrics = LiveNvmeMetrics::new();
    let first = metrics.register().unwrap();
    let second = metrics.register().unwrap();
    first.record_leased_worker_wait(false);
    first.record_leased_worker_wait(true);
    second.record_leased_worker_wait(true);
    let counts = metrics.snapshot().counts;
    assert_eq!(counts.leased_worker_waits, 3);
    assert_eq!(counts.leased_worker_wait_timeouts, 2);
}
