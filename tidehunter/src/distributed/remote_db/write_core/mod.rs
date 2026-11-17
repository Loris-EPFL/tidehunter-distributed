//! Bounded caller allocation, per-owner handoff and publication state.
//!
//! Lock order: identity shard, key shards in ascending index, owner order.
//! Release all three before acquiring index writer, publisher, then slot data.
//! Admission is never held with any of these locks or across encoding or I/O.

pub(super) mod completion;
mod control;
pub(super) mod core;
mod dependencies;
mod identity;
pub(super) mod owner_queue;
pub(super) mod owner_sender;
mod publisher;
#[cfg(test)]
mod scenario_tests;
mod sequencer;
#[cfg(all(test, feature = "experimental-nvmeof", target_os = "linux"))]
pub(super) mod test_completion;
pub(super) mod transport_budget;
mod worker;
pub(super) mod worker_group;
pub(super) mod write;

use super::DependencyKey;
use crate::key_shape::KeySpace;

type Dependency = (KeySpace, DependencyKey);

fn shard_count(parallelism: usize) -> usize {
    parallelism
        .saturating_mul(8)
        .clamp(64, 4096)
        .next_power_of_two()
}
