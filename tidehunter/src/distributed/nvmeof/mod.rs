//! Live native WAL data paths over Linux NVMe-oF (kernel target, buffered
//! file backing, one permanent export subsystem per owner).

#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
pub mod live_connect;
#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
pub mod live_io;
#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
pub mod live_metrics;
#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
pub mod live_namespace;
#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
pub mod live_pool;
#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
pub(crate) mod live_prometheus;
#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
pub mod live_storage;
#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
pub mod live_target;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NvmeofTransport {
    Tcp,
    Rdma,
}

pub mod legacy_wire;
pub use legacy_wire::SpdkTarget;

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
    /// Wire placeholder, always `Kernel` (see `legacy_wire`).
    #[serde(default)]
    pub target_implementation: legacy_wire::LiveTargetImplementation,
    pub namespaces_exposed: u64,
    pub disable_drain_successes: u64,
    pub readonly_commission_successes: u64,
    pub errors: u64,
    pub owned_namespace_count: u64,
    pub fence_count: u64,
    pub disable_drain_time_ns: u64,
}
