//! Wire types of the removed sealed-export, SPDK and on-demand export routes.
//!
//! They keep the bincode layout of the owner protocol unchanged until the
//! protocol bump removes them together with their request variants. No code
//! path produces a non-default value: producers send `None`, `PerOwner` or no
//! message at all, and receivers reject anything else with a named error.

use serde::{Deserialize, Serialize};

/// Removed SPDK target proof; always `None` where it appears.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpdkTarget {
    pub rpc_socket: std::path::PathBuf,
    pub pid: u32,
    pub process_start_ticks: u64,
    pub bdev_name: String,
    pub image_path: std::path::PathBuf,
}

/// Removed sealed-export target policy.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExportTarget {
    #[default]
    Kernel,
    Spdk(SpdkTarget),
    KernelDirect,
}

/// Removed sealed-export registration body of `Request::RegisterNvmeofExport`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExportBinding {
    pub database_uuid: [u8; 16],
    pub epoch: u64,
    pub partition: u32,
    pub generation: u64,
    pub export_uuid: [u8; 16],
    pub nqn: String,
    pub nsid: u32,
    pub wal_file_bytes: u64,
    pub source_file_index: u64,
    pub image_len: u64,
    pub block_size: u32,
    pub source_device: u64,
    pub source_inode: u64,
    pub durable_frontier: u64,
    #[serde(default)]
    pub target: ExportTarget,
}

/// Export scope reported by `Response::LiveExports`. The owner always reports
/// `PerOwner`; the coordinator rejects any other value.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LiveExportMode {
    OnDemand,
    PerFile,
    PerOwner,
}

/// Target kind field of `LiveTargetStats` (`Response::LiveTargetStats`); the
/// owner always reports `Kernel`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LiveTargetImplementation {
    #[default]
    Kernel,
    Spdk,
}
