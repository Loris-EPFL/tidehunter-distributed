#![cfg(feature = "experimental-fabric")]

use super::*;
use std::sync::Arc;

fn stats(base: u64) -> OwnerTransportStats {
    OwnerTransportStats::from_array(std::array::from_fn(|index| base + index as u64))
}

#[test]
fn flush_publishes_only_the_growth_of_each_session() {
    let counters = Arc::new(OwnerTransportCounters::default());
    let mut first = counters.attach();
    let mut second = counters.attach();
    first.flush(stats(10));
    first.flush(stats(10));
    second.flush(stats(1));
    first.flush(stats(15));
    let total = counters.snapshot().to_array();
    for (index, value) in total.into_iter().enumerate() {
        assert_eq!(value, 15 + 1 + 2 * index as u64);
    }
}

#[test]
fn reset_and_closed_sessions_keep_the_lifetime_total() {
    let counters = Arc::new(OwnerTransportCounters::default());
    let mut sink = counters.attach();
    sink.flush(stats(100));
    sink.rebase();
    sink.flush(stats(5));
    drop(sink);
    let total = counters.snapshot();
    assert_eq!(total.send_size_le_4k, 105);
    assert_eq!(total.cq_idle_waits, 106 + 11);
    // A counter that went down without a rebase is not subtracted.
    let mut sink = counters.attach();
    sink.flush(stats(50));
    sink.flush(stats(20));
    sink.flush(stats(21));
    assert_eq!(counters.snapshot().send_size_le_4k, 105 + 50 + 1);
}

#[test]
fn sessions_on_many_threads_sum_exactly() {
    const THREADS: usize = 24;
    const ROUNDS: u64 = 1000;
    let counters = Arc::new(OwnerTransportCounters::default());
    std::thread::scope(|scope| {
        for _ in 0..THREADS {
            let counters = &counters;
            scope.spawn(move || {
                let mut sink = counters.attach();
                for round in 1..=ROUNDS {
                    sink.flush(stats(round));
                }
            });
        }
    });
    let total = counters.snapshot();
    assert_eq!(total.send_size_le_4k, THREADS as u64 * ROUNDS);
    assert_eq!(total.cq_idle_waits, THREADS as u64 * (ROUNDS + 6));
}
