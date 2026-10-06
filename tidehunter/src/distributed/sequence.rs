//! Shared logical positions for one already-fenced database authority.
//!
//! Tidehunter's local allocation (§3.1) motivates keeping ordering beside the
//! index authority. CORFU separates sequence allocation from data placement;
//! this initial single-authority design does not need a network sequencer hop.
//! This counter is neither distributed consensus nor a durability frontier.

use super::types::BatchVersion;
use std::sync::atomic::{AtomicU64, Ordering};

/// An epoch-scoped counter shared by the authority's local caller threads.
///
/// Construct this only after external epoch fencing and complete inventory
/// recovery. An exclusive native file lock is not a distributed writer lease.
/// Reconstructing a counter with the same epoch can reuse versions: this type
/// intentionally does not offer persistent failover or automatic restart.
/// Admission must bound outstanding batches before calling `allocate`.
pub struct LogicalClock {
    epoch: u64,
    next: AtomicU64,
}

impl LogicalClock {
    pub fn new_fenced_epoch(epoch: u64) -> Self {
        Self {
            epoch,
            next: AtomicU64::new(0),
        }
    }

    /// Reserve a logical batch version without assigning any physical address.
    ///
    /// Relaxed ordering supplies uniqueness only. Storage persistence, index
    /// publication and prefix tracking require their own synchronization.
    /// `None` requires a new fenced epoch; the sequence never wraps.
    pub fn allocate(&self) -> Option<BatchVersion> {
        self.next
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| v.checked_add(1))
            .ok()
            .map(|sequence| BatchVersion {
                epoch: self.epoch,
                sequence,
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn concurrent_callers_share_one_logical_order() {
        let clock = LogicalClock::new_fenced_epoch(7);
        let versions = std::thread::scope(|scope| {
            let workers: Vec<_> = (0..4)
                .map(|_| {
                    let clock = &clock;
                    scope.spawn(move || {
                        (0..1024)
                            .map(|_| clock.allocate().unwrap())
                            .collect::<Vec<_>>()
                    })
                })
                .collect();
            workers
                .into_iter()
                .flat_map(|w| w.join().unwrap())
                .collect::<BTreeSet<_>>()
        });
        assert_eq!(versions.len(), 4096);
        assert_eq!(
            versions.first(),
            Some(&BatchVersion {
                epoch: 7,
                sequence: 0
            })
        );
        assert_eq!(
            versions.last(),
            Some(&BatchVersion {
                epoch: 7,
                sequence: 4095
            })
        );
    }

    #[test]
    fn exhausted_epoch_never_reuses_a_position() {
        let clock = LogicalClock {
            epoch: 9,
            next: AtomicU64::new(u64::MAX - 1),
        };
        assert_eq!(
            clock.allocate(),
            Some(BatchVersion {
                epoch: 9,
                sequence: u64::MAX - 1
            })
        );
        assert_eq!(clock.allocate(), None);
        assert_eq!(clock.allocate(), None);
    }
}
