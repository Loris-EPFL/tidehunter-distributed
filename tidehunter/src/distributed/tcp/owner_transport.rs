//! Owner-wide send-path counters summed across every session of one storage node.
//!
//! The provider already counts these per connection (`tidehunter_fabric::ProgressStats`)
//! and the connection state belongs to one session thread. Each session therefore
//! keeps a private baseline and adds only the change since its last flush to one
//! cache-line-sized shard chosen at attach time. Sessions share no lock and, with
//! more sessions than shards, share at most a shard's atomics once per sweep that
//! changed a counter. A snapshot sums the shards.

use serde::{Deserialize, Serialize};
#[cfg(feature = "experimental-fabric")]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicU64, Ordering};

const FIELDS: usize = 7;
const SHARDS: usize = 16;

/// Sends, EAGAIN retries and idle CQ parking summed across all sessions of the
/// owner. Always-on counters; no clock is read. Totals include closed sessions.
/// Without an OFI transport every field stays zero.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct OwnerTransportStats {
    /// Successful provider sends by posted message length: at most 4 KiB,
    /// 16 KiB, 64 KiB, and above.
    pub send_size_le_4k: u64,
    pub send_size_le_16k: u64,
    pub send_size_le_64k: u64,
    pub send_size_gt_64k: u64,
    /// Provider sends that returned EAGAIN, and posts that reused the filled slot
    /// of an earlier EAGAIN without copying the bytes again.
    pub send_post_eagain: u64,
    pub send_post_retries: u64,
    /// Idle sweeps that parked on the provider CQ wait object.
    pub cq_idle_waits: u64,
}

impl OwnerTransportStats {
    #[cfg(feature = "experimental-fabric")]
    fn to_array(self) -> [u64; FIELDS] {
        [
            self.send_size_le_4k,
            self.send_size_le_16k,
            self.send_size_le_64k,
            self.send_size_gt_64k,
            self.send_post_eagain,
            self.send_post_retries,
            self.cq_idle_waits,
        ]
    }

    fn from_array(values: [u64; FIELDS]) -> Self {
        let [
            send_size_le_4k,
            send_size_le_16k,
            send_size_le_64k,
            send_size_gt_64k,
            send_post_eagain,
            send_post_retries,
            cq_idle_waits,
        ] = values;
        Self {
            send_size_le_4k,
            send_size_le_16k,
            send_size_le_64k,
            send_size_gt_64k,
            send_post_eagain,
            send_post_retries,
            cq_idle_waits,
        }
    }

    #[cfg(feature = "experimental-fabric")]
    pub(super) fn from_progress(progress: &tidehunter_fabric::ProgressStats) -> Self {
        Self {
            send_size_le_4k: progress.send_size_le_4k,
            send_size_le_16k: progress.send_size_le_16k,
            send_size_le_64k: progress.send_size_le_64k,
            send_size_gt_64k: progress.send_size_gt_64k,
            send_post_eagain: progress.send_post_eagain,
            send_post_retries: progress.send_post_retries,
            cq_idle_waits: progress.cq_idle_waits,
        }
    }
}

/// One cache line per shard so two sessions on different shards never write the
/// same line.
#[repr(align(64))]
#[derive(Default)]
struct Shard([AtomicU64; FIELDS]);

#[derive(Default)]
pub(crate) struct OwnerTransportCounters {
    shards: [Shard; SHARDS],
    /// Hands out shards round-robin; any assignment is correct, this only
    /// spreads sessions.
    #[cfg(feature = "experimental-fabric")]
    next: AtomicUsize,
}

impl OwnerTransportCounters {
    /// Sequential relaxed loads: independent statistics, not a globally atomic cut.
    pub(crate) fn snapshot(&self) -> OwnerTransportStats {
        let mut totals = [0_u64; FIELDS];
        for shard in &self.shards {
            for (total, counter) in totals.iter_mut().zip(&shard.0) {
                *total = total.saturating_add(counter.load(Ordering::Relaxed));
            }
        }
        OwnerTransportStats::from_array(totals)
    }

    #[cfg(feature = "experimental-fabric")]
    // Relaxed assignment needs no ordering: every shard is valid for every session.
    pub(super) fn attach(self: &std::sync::Arc<Self>) -> OwnerTransportSink {
        OwnerTransportSink {
            counters: self.clone(),
            shard: self.next.fetch_add(1, Ordering::Relaxed) % SHARDS,
            flushed: [0; FIELDS],
        }
    }
}

/// A session's handle to its shard plus the totals it already published.
#[cfg(feature = "experimental-fabric")]
pub(super) struct OwnerTransportSink {
    counters: std::sync::Arc<OwnerTransportCounters>,
    shard: usize,
    flushed: [u64; FIELDS],
}

#[cfg(feature = "experimental-fabric")]
impl OwnerTransportSink {
    /// Publishes the growth of `current` since the previous flush.
    pub(super) fn flush(&mut self, current: OwnerTransportStats) {
        let shard = &self.counters.shards[self.shard].0;
        for ((counter, flushed), now) in shard.iter().zip(&mut self.flushed).zip(current.to_array())
        {
            // A smaller value means the session counters were reset without a
            // rebase; publishing nothing is safe and the baseline follows.
            let delta = now.saturating_sub(*flushed);
            if delta != 0 {
                // Independent statistics do not publish or guard session state.
                counter.fetch_add(delta, Ordering::Relaxed);
            }
            *flushed = now;
        }
    }

    /// The session's own counters were reset to zero; publish nothing for the
    /// reset itself and count from zero again.
    pub(super) fn rebase(&mut self) {
        self.flushed = [0; FIELDS];
    }
}

#[cfg(all(test, feature = "experimental-fabric"))]
mod tests {
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
}
