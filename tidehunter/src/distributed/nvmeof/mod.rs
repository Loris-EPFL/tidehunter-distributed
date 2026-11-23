//! Live native WAL data paths over Linux NVMe-oF (kernel target, buffered
//! file backing, one permanent export subsystem per owner).

pub mod live_connect;
pub mod live_io;
pub mod live_metrics;
pub mod live_namespace;
pub mod live_pool;
pub(crate) mod live_prometheus;
/// Live WAL metadata types. Compiled in every feature set: owner-protocol
/// payloads carry them (see `tcp::live_wire`).
pub mod live_storage;
pub mod live_target;

#[cfg(test)]
#[path = "kernel_write_protect_tests.rs"]
mod kernel_write_protect_tests;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NvmeofTransport {
    Tcp,
    Rdma,
}

/// Bounded live controller queues; defaults use four 128-entry I/O queues.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LiveQueueGeometry {
    pub io_queues: u32,
    pub queue_size: u32,
    pub poll_queues: u32,
}

impl Default for LiveQueueGeometry {
    fn default() -> Self {
        Self {
            io_queues: 4,
            queue_size: 128,
            poll_queues: 0,
        }
    }
}

impl LiveQueueGeometry {
    pub fn validate(self) -> Result<(), &'static str> {
        if !(1..=16).contains(&self.io_queues)
            || !(16..=128).contains(&self.queue_size)
            || self.poll_queues > 4
        {
            return Err("invalid live queue geometry");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LiveTargetConfig {
    pub address: String,
    pub service: u16,
    pub transport: NvmeofTransport,
    pub host_nqn: String,
    pub port_id: u16,
    pub state_dir: std::path::PathBuf,
    pub commissioning_port_id: u16,
    pub commissioning_service: u16,
}

/// Per-process target evidence. Fixture events are not kernel qualifications.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct LiveTargetStats {
    pub fixture_only: bool,
    pub namespaces_exposed: u64,
    pub disable_drain_successes: u64,
    pub readonly_commission_successes: u64,
    pub errors: u64,
    pub owned_namespace_count: u64,
    pub fence_count: u64,
    pub disable_drain_time_ns: u64,
    pub write_protect_successes: u64,
    pub write_protect_time_ns: u64,
}
