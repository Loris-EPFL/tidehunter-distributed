//! Shared logical positions for one already-fenced database authority.
//!
//! Tidehunter's local allocation (§3.1) motivates keeping ordering beside the
//! index authority. CORFU separates sequence allocation from data placement;
//! this initial single-authority design does not need a network sequencer hop.
//! This counter is neither distributed consensus nor a durability frontier.

use super::types::BatchVersion;
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(test)]
mod tests;

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
