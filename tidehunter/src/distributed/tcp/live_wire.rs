//! Owner-protocol payloads of the live NVMe-oF requests. They are compiled in
//! every feature set so the wire tags of `Request`/`Response` never depend on
//! features; only the live route itself is gated.

use super::Receipt;
use crate::distributed::nvmeof::NvmeofTransport;
use crate::distributed::nvmeof::live_storage::{
    LiveAdoption, LiveTargetBinding, LiveWalSegment, LiveWriteExtent,
};
use crate::distributed::types::ValueRecordReference;
use serde::{Deserialize, Serialize};

/// The owner's permanent per-owner export (`OpenLiveExports`).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LiveExportInfo {
    pub address: String,
    pub service: u16,
    pub transport: NvmeofTransport,
    pub wal_file_bytes: u64,
    pub block_size: u32,
    pub exported_files: u64,
}

/// Writable binding for one fragment remainder; never a durability proof.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LiveLeased {
    pub extent: LiveWriteExtent,
    pub binding: LiveTargetBinding,
    /// Authoritative, immutable policy of the stream whose frontier is started.
    pub tier: crate::distributed::durability::DurabilityTier,
    pub profile: LeaseProfile,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LiveAdopted {
    pub adoption: LiveAdoption,
    pub next: Option<LiveLeased>,
    pub profile: AdoptProfile,
}

/// Owner-local intervals; they must not be subtracted from coordinator clocks.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct LeaseProfile {
    pub reserve_ns: u64,
    pub namespace_sync_ns: u64,
    pub publish_ns: u64,
}

/// Adoption time includes close scanning. Frames counts only newly adopted frames.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct AdoptProfile {
    pub frames: u64,
    pub adopt_ns: u64,
    pub close_scan_ns: u64,
    pub close_scan_bytes: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LiveRecordLocation {
    pub reference: ValueRecordReference,
    pub segment: LiveWalSegment,
    pub binding: LiveTargetBinding,
    pub offset: u64,
    pub len: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LiveRecordLocations {
    pub entries: Vec<LiveRecordLocation>,
    pub epoch_pin_token: [u8; 16],
}

#[derive(Serialize, Deserialize)]
pub(super) struct LiveRecoveryEntryWire {
    pub(super) receipt: Receipt,
    pub(super) segment: LiveWalSegment,
    pub(super) binding: LiveTargetBinding,
    pub(super) offset: u64,
    pub(super) len: u64,
}

/// One `ReplayLiveWal` page.
#[derive(Serialize, Deserialize)]
pub(super) struct LiveRecoveryWire {
    pub(super) entries: Vec<LiveRecoveryEntryWire>,
    pub(super) next: Option<usize>,
    pub(super) epoch_pin_token: [u8; 16],
}
