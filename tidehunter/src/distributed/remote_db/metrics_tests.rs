//! The coordinator's metrics are the caller's (D40, native `Db::open`).
//!
//! `Db::open` takes the caller's `Arc<Metrics>` and every index, read and
//! flusher counter is registered in the caller's registry. The hybrid open did
//! the opposite: it built `Metrics::new()` inside the open (a registry private
//! to that call) and ignored the caller, so Sui's registry never saw the
//! coordinator's index counters. This test opens a fixture authority with a
//! registry of its own and observes an index counter in it.
//!
//! Negative control: put `Metrics::new()` back at the `HybridTable::open`
//! call site and the observation count stays 0 in the caller's registry.

use super::pipeline_tests::Fixture;
use super::*;
use crate::distributed::codec::Mutation;
use crate::key_shape::KeySpace;

/// Observation count of one histogram family in `registry`, 0 when absent.
fn histogram_count(registry: &prometheus::Registry, name: &str) -> u64 {
    registry
        .gather()
        .iter()
        .find(|family| family.name() == name)
        .map_or(0, |family| {
            family
                .get_metric()
                .iter()
                .map(|metric| metric.get_histogram().get_sample_count())
                .sum()
        })
}

#[test]
fn the_coordinator_index_metrics_go_to_the_callers_registry() {
    let fixture = Fixture::new();
    let registry = prometheus::Registry::new();
    let metrics = Metrics::new_in(&registry);
    let db = RemoteDatabase::open_inner_with(
        &fixture.temp.path().join("authority"),
        &fixture.shape,
        fixture.config.clone(),
        metrics,
        None,
        None,
        None,
        Some(false),
    )
    .expect("open with the caller's metrics");
    assert_eq!(
        histogram_count(&registry, "index_size"),
        0,
        "an open observes no index size before the first write",
    );

    let operation = fixture.operation(0, 7);
    db.write_batch(
        operation,
        vec![Mutation::Put {
            keyspace: KeySpace(0),
            key: 0u32.to_be_bytes().to_vec().into(),
            value: vec![7; 32].into(),
        }],
    )
    .expect("write one key");
    assert!(
        histogram_count(&registry, "index_size") > 0,
        "the coordinator index must report into the caller's registry",
    );
}
