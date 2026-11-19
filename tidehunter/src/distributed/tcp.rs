//! Static RF1 storage transport with authenticated TCP/OFI and explicit trusted
//! (no-TLS) TCP and OFI modes.
//!
//! The Tidehunter paper's complete-batch replay and values-in-WAL rules remain
//! the authority boundary. Transport is not a global log or consensus service.
//! TLS modes require a dedicated CA and validate both peers with rustls.
//! Trusted TCP and trusted OFI modes omit application authentication and
//! encryption; deployment isolation is required. WAL integrity, fencing and
//! durability checks remain. A trusted mode is always an explicit opt-in
//! (`Transport::TcpTrusted`, `FabricSecurity::Trusted`), never a default,
//! fallback or downgrade, and receipts record `Transport::security_label()`.
//!
//! Fair experiments pair like with like: `Transport::Tcp` against OFI with
//! `FabricSecurity::MutualTls` (both mutual TLS), and `Transport::TcpTrusted`
//! against OFI with `FabricSecurity::Trusted` (both trusted-network/no-TLS).

mod capacity;
pub mod checkpoint;
mod diagnostics;
mod fabric;
mod reactor;
pub use fabric::{FabricBackend, FabricConfig, FabricSecurity, Transport, TransportProgressStats};
mod owner_transport;
pub(crate) use owner_transport::OwnerTransportCounters;
pub use owner_transport::OwnerTransportStats;
#[cfg(feature = "experimental-fabric")]
pub use tidehunter_fabric::ProviderInfo;
mod control;
mod durable_progress;
pub use durable_progress::{DURABLE_PROGRESS_MAX_WAIT, DurableProgressReport};
mod generation_control;
pub use generation_control::{InventoryGeneration, OwnerGenerationState, TrimReadiness};
mod inventory;
#[cfg(test)]
pub(crate) use generation_control::OwnerTrimCrash;
mod live_client;
pub use super::nvmeof::LiveQueueGeometry;
#[cfg(test)]
pub(crate) use live_client::leased_tests;
pub(super) use live_client::{LeasedSubmit, LiveGroupBudget};
pub use live_client::{LeasedSync, LiveClientConfig, LiveInitiatorIdentity};
#[cfg(test)]
pub(crate) mod export_pin_tests;
mod live_control;
pub(super) mod live_read;
mod live_recovery;
mod relocation_scan;
pub use diagnostics::{ConnectionProfile, PollingProfile, SocketProfile, WorkProfile};
pub use live_control::{LiveRecoveryEntry, LiveRecoveryPage};
mod live_wire;
pub(crate) use live_control::{
    LiveExportPin, LiveExportPinControl, LiveExportPinRequest, LivePinRecipe,
};
pub use live_wire::{
    AdoptProfile, LeaseProfile, LiveAdopted, LiveExportInfo, LiveLeased, LiveRecordLocation,
    LiveRecordLocations,
};
#[cfg(test)]
mod probe;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod transport_security_tests;

#[cfg(all(test, feature = "experimental-fabric"))]
pub(super) fn software_transport(security: FabricSecurity) -> Transport {
    Transport::ofi(FabricBackend::SoftwareTcp, security)
}
use super::codec::{CompleteBatch, Mutation};
use super::storage::StorageError;
use super::storage::{DurableBatch, PartitionConfig, PartitionStats};
use super::types::{
    BatchId, BatchVersion, PayloadDigest, PhysicalValueAddress, RequestAuthority,
    ValueRecordReference,
};
use crate::key_shape::KeyShape;
use crate::metrics::Metrics;
use bincode::Options;
use parking_lot::Mutex;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName};
use rustls::{
    ClientConfig, ClientConnection, RootCertStore, ServerConfig, ServerConnection, StreamOwned,
};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fs;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

pub use inventory::{FileMaintenanceStats, NodeInventory};
/// Wire protocol version. 22 adds leased live WAL extents and adoption proofs.
/// 23 adds OpenLiveExports for permanent live export.
/// 24 adds lease profiles, PreleaseLiveExtent, CheckpointPosition, ReplayWal, ReplayLiveWal and RetireWalFiles.
/// 25 gives production requests and replies feature-stable tags before NVMe-oF and test-only variants.
/// 26 adds controller-lifetime pins for non-permanent live exports.
/// 27 adds DurableProgress, the owner durability tier in NodeStatus, and PartitionStats
/// (feature-stable tags after DurableProgress in both Request and Response).
/// 28 replaces the kernel boot id in NodeStatus and DurableProgressReport with the owner
/// incarnation, and makes `NodeStatus::recovery_clean` false after any persistence failure.
/// 29 removes the owner-RPC payload, receipt-history, sealed-export, Reserved,
/// RAM-probe and read-executor requests, compiles every variant in every
/// feature set, documents each tag (frozen by `wire_tag_tests`), drops
/// `NodeStatus::{history_persistence, max_read_bytes}` and the SPDK, sealed-export
/// and export-mode placeholders, and adds owner transport counters to
/// `PartitionStats`.
/// 30 (L1) replaces the retained generation vector and `max_epochs` in
/// `NodeStatus` with the owner high-water and inventory revision, and appends
/// InventoryPage, CheckTrimGeneration and TrimGeneration.
/// 31 (R0) appends FileInventoryPage (request tag 29, response tag 26).
/// 32 (R1 owner) gives RetireWalFiles inode-bound proofs and an Exact witness, and `Retired`
/// a Completed/Busy outcome (payloads change in place; tags are unchanged).
/// 33 (R1 logical) appends RelocationPage (request tag 30, response tag 27).
const PROTOCOL: u32 = 33;
pub(crate) const HARD_FRAME_LIMIT: usize = 64 * 1024 * 1024;
/// Leave one protocol request slot available for ordered checkpoint control.
pub(super) const APPEND_COMMAND_WINDOW: usize = reactor::SESSION_REQUESTS - 1;
pub(super) const APPEND_SESSION_BYTES: usize = reactor::SESSION_BYTES;
const MODE_MAGIC: [u8; 4] = *b"THDP";

/// Mode ids 2-5 were the removed EFA and EFA-direct backends (MutualTls and
/// Trusted each). They stay retired and are never reassigned, so a preface from
/// a binary that still had them can never match a current mode.
fn transport_mode_id(transport: &Transport) -> u8 {
    match transport {
        Transport::Tcp => 1,
        Transport::TcpTrusted => 10,
        Transport::Ofi { fabric, security } => match (&fabric.backend, security) {
            (FabricBackend::Verbs, FabricSecurity::MutualTls) => 6,
            (FabricBackend::Verbs, FabricSecurity::Trusted) => 7,
            (FabricBackend::SoftwareTcp, FabricSecurity::MutualTls) => 8,
            (FabricBackend::SoftwareTcp, FabricSecurity::Trusted) => 9,
        },
    }
}

fn write_transport_mode(stream: &mut (impl Read + Write), transport: &Transport) -> io::Result<()> {
    let mut preface = [0; 8];
    preface[..4].copy_from_slice(&MODE_MAGIC);
    preface[4..6].copy_from_slice(&(PROTOCOL as u16).to_be_bytes());
    preface[6] = transport_mode_id(transport);
    stream.write_all(&preface)?;
    stream.flush()
}

fn read_transport_mode(stream: &mut impl Read, expected: &Transport) -> Result<(), TcpError> {
    let mut preface = [0; 8];
    stream.read_exact(&mut preface)?;
    if preface[..4] != MODE_MAGIC
        || u16::from_be_bytes([preface[4], preface[5]]) != PROTOCOL as u16
        || preface[6] != transport_mode_id(expected)
        || preface[7] != 0
    {
        return Err(TcpError::new("transport mode/protocol mismatch"));
    }
    Ok(())
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeConfig {
    pub root: PathBuf,
    /// Full static cluster inventory, this node's partition; template epoch = 1.
    /// Retained fencing generations are bounded by the ones still referenced,
    /// not by a configured count.
    pub partition: PartitionConfig,
    pub shape_yaml: String,
    /// Member read policy of owner WAL reads; with `Async`, also bounds each
    /// live record resolution (see `read_max_device_reads`).
    #[serde(default = "default_async_read_policy")]
    pub wal_member_read: super::storage::WalMemberReadPolicy,
    /// Write and sync zeros across each external WAL file before it is usable,
    /// so live writes never convert unwritten extents. On by default and always
    /// serialized: an absent key means on, and opting out requires `false`.
    #[serde(default = "default_wal_prewrite")]
    pub wal_prewrite: bool,
    /// With `wal_member_read: async`, caps `NodeStatus::max_reads` and the
    /// references of one `ResolveLiveRecords` request.
    #[serde(default = "default_device_reads")]
    pub read_max_device_reads: usize,
    /// Optional live target. Once enabled, its persisted configuration is
    /// required on every restart so outstanding namespaces can be revoked.
    #[serde(default)]
    pub live_target: Option<super::nvmeof::LiveTargetConfig>,
}

impl NodeConfig {
    /// Validate static owner settings without opening files, sockets or nvmet.
    pub fn validate(&self) -> Result<(), TcpError> {
        self.validated_shape()?;
        self.validate_reads()?;
        if let Some(target) = &self.live_target {
            super::nvmeof::live_target::validate_config(target).map_err(TcpError::component)?;
        }
        Ok(())
    }

    fn validated_shape(&self) -> Result<KeyShape, TcpError> {
        self.partition.validate().map_err(TcpError::component)?;
        if self.partition.inventory.epoch != 1 || self.partition.inventory.segment_generation != 1 {
            return Err(TcpError::new("invalid node epoch/configuration template"));
        }
        KeyShape::from_yaml(&self.shape_yaml).map_err(TcpError::component)
    }

    fn validate_reads(&self) -> Result<(), TcpError> {
        // Same bound the removed read executor applied to max_device_reads.
        if !(1..=4096).contains(&self.read_max_device_reads) {
            return Err(TcpError::new("read_max_device_reads must be in 1..=4096"));
        }
        Ok(())
    }
}

fn default_wal_prewrite() -> bool {
    true
}
fn default_async_read_policy() -> super::storage::WalMemberReadPolicy {
    super::storage::WalMemberReadPolicy::Async
}
fn default_device_reads() -> usize {
    128
}
/// TLS credentials and transport selection. Trusted TCP and trusted OFI require
/// empty credential paths; mutual-TLS modes require a dedicated CA.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsFiles {
    #[serde(default)]
    pub transport: Transport,
    #[serde(default)]
    pub ca: PathBuf,
    #[serde(default)]
    pub cert: PathBuf,
    #[serde(default)]
    pub key: PathBuf,
    /// Expected server DNS name; ignored by the listener and trusted modes.
    #[serde(default)]
    pub server_name: String,
}

/// Network-owner scheduling only, not a whole-system lock-free guarantee.
/// Adaptive waits for socket readiness only when a TCP owner is fully drained;
/// arrivals wake it without an idle-sleep floor. Active TCP yields. An idle
/// fabric owner parks on the provider CQ wait object when the provider grants
/// one (software RxM; `verbs` through a wait fd) and otherwise keeps bounded
/// spin/yield CQ progress (no TCP readiness dependency).
/// Busy keeps bounded sweeps and all shutdown, deadline and peer checks, but
/// never explicitly sleeps/yields while driving the established connection.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PollingPolicy {
    #[default]
    Adaptive,
    Busy,
}

/// Budget for the bounded initial-fence metadata chain: 11 synchronous fsyncs
/// (Preparing manifest 2, epoch directory 1, WAL file/directory 2, registration 2,
/// WAL root 2, Active manifest 2) at 3 s each, plus 1.5 s for the initial mmap.
/// The 3 s allowance adds about 30% to the measured 2.308668 s contended fsync.
/// This is not a completion bound for sealing arbitrarily many retained files
/// or revoking many exports. Larger explicitly configured timeouts still apply.
/// A live export creation (13 target-registry saves, 26 fsyncs, plus the
/// commissioning connection) is a lifecycle operation, so every request that
/// may create one inline also uses it: a group reservation, a lease, live
/// record resolution, prelease, and an adoption that leases the next extent.
/// `StorageNode::live_inline_export_stats` counts how often each one did.
/// Other data requests, including position proofs, do not use it.
pub const LIFECYCLE_TIMEOUT: Duration = Duration::from_millis(34_500);

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct TcpLimits {
    pub max_frame_bytes: usize,
    pub max_connections: usize,
    pub max_reads: usize,
    pub recovery_page_batches: usize,
    /// Handshake, partial-frame, and accepted-data-request timeout; lifecycle
    /// requests use at least [`LIFECYCLE_TIMEOUT`]. Established authenticated
    /// sessions may remain idle without expiring.
    pub io_timeout: Duration,
    #[serde(default)]
    pub polling: PollingPolicy,
}
impl Default for TcpLimits {
    fn default() -> Self {
        Self {
            max_frame_bytes: 16 * 1024 * 1024,
            // Two automatic coordinator session sets (MAX_AUTO_READ_LANES read lanes plus the
            // append/control and operational connections each), so a coordinator that omits
            // `read_lanes` can open, and reopen while old sessions drain, against default limits.
            max_connections: 2
                * (super::remote_db::RemoteDatabaseConfig::MAX_AUTO_READ_LANES
                    + super::remote_db::RemoteDatabaseConfig::NON_READ_CONNECTIONS),
            max_reads: 256,
            recovery_page_batches: 128,
            io_timeout: Duration::from_secs(10),
            polling: PollingPolicy::Adaptive,
        }
    }
}
impl TcpLimits {
    pub fn validate(self) -> Result<(), TcpError> {
        if !(1024..=HARD_FRAME_LIMIT).contains(&self.max_frame_bytes)
            || !(1..=256).contains(&self.max_connections)
            || !(1..=256).contains(&self.max_reads)
            || !(1..=1024).contains(&self.recovery_page_batches)
            || self.io_timeout.is_zero()
            || self.io_timeout > Duration::from_secs(300)
        {
            return Err(TcpError::new("invalid transport limits"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeStatus {
    pub partition: PartitionConfig,
    pub shape_yaml: String,
    /// Highest fencing generation the owner ever accepted. Never decreases;
    /// retained generations are read with `Client::inventory_sweep`.
    pub highest_fenced_generation: u64,
    /// Owner-incarnation-local ledger revision that freezes one page sweep.
    pub inventory_revision: u64,
    pub active_epoch: Option<u64>,
    /// False means only a strictly higher live fence may recover this owner.
    pub healthy: bool,
    /// Negotiated bound for each encoded request/response frame.
    pub max_frame_bytes: usize,
    /// References per live record resolution.
    pub max_reads: usize,
    pub recovery_page_batches: usize,
    pub polling: PollingPolicy,
    /// The owner is configured for direct live NVMe-oF WAL access.
    pub live_nvmeof: bool,
    /// The owner's write durability tier now; the default tier before configuration.
    pub durability_tier: super::durability::DurabilityTier,
    /// Owner incarnation: random, created when the owner opened, never persisted.
    pub incarnation: [u8; 16],
    /// False when a WAL recovery this owner incarnation ran found a lost or damaged
    /// frame with a valid frame after it (a hole below the recovered tail), or once
    /// this incarnation saw a persistence failure or a reader poison.
    pub recovery_clean: bool,
}
pub struct RecoveryPage {
    pub batches: Vec<(CompleteBatch, DurableBatch)>,
    pub next: Option<usize>,
}

/// Explicit diagnostic timings of control exchanges; ordinary requests take no
/// extra stage clocks. Exchange starts after queueing and also waits for local
/// request-output retirement; encode/decode intervals are nested inside it.
/// Legacy send/header/body clocks are zero: partial multiplexed I/O cannot be
/// attributed to one request. Socket clocks are collected at connection scope only.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct TcpReadProfile {
    pub request_encode_ns: u64,
    pub request_encode_count: u64,
    pub exchange_ns: u64,
    pub exchange_count: u64,
    /// Not attributable per request under multiplexing; zero in protocol v2.
    pub request_send_ns: u64,
    pub response_header_ns: u64,
    pub response_body_ns: u64,
    pub socket: SocketProfile,
    pub response_decode_ns: u64,
    pub response_decode_count: u64,
    pub record_verify_ns: u64,
    pub record_verify_count: u64,
    pub requests: u64,
}

impl TcpReadProfile {
    pub(crate) fn add(&mut self, next: Self) {
        self.request_encode_ns += next.request_encode_ns;
        self.request_encode_count += next.request_encode_count;
        self.exchange_ns += next.exchange_ns;
        self.exchange_count += next.exchange_count;
        self.request_send_ns += next.request_send_ns;
        self.response_header_ns += next.response_header_ns;
        self.response_body_ns += next.response_body_ns;
        self.socket.add(next.socket);
        self.response_decode_ns += next.response_decode_ns;
        self.response_decode_count += next.response_decode_count;
        self.record_verify_ns += next.record_verify_ns;
        self.record_verify_count += next.record_verify_count;
        self.requests += next.requests;
    }
}

#[derive(Debug)]
pub struct TcpError {
    message: String,
}
impl TcpError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
    fn component(error: impl fmt::Display) -> Self {
        Self::new(error.to_string())
    }
}
impl fmt::Display for TcpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.message.fmt(f)
    }
}
impl std::error::Error for TcpError {}
impl From<io::Error> for TcpError {
    fn from(error: io::Error) -> Self {
        Self::component(error)
    }
}

// Wire tags are the bincode variant indices, i.e. declaration order. Every
// variant is compiled in every feature set (no `#[cfg]`), so a tag never
// depends on features; builds without a route reject its request with a named
// error. Each variant documents its tag, and `wire_tag_tests` freezes the
// order: append new variants at the end, never reorder or remove without a
// protocol bump.
#[derive(Serialize, Deserialize)]
enum Request {
    /// Tag 0.
    RetirementFiles {
        authority_epoch: u64,
        epoch: u64,
        after: Option<u64>,
        limit: usize,
    },
    /// Tag 1.
    Status { protocol: u32 },
    /// Tag 2.
    Fence { epoch: u64 },
    /// Tag 3.
    Seal { epoch: u64 },
    /// Tag 4.
    Stats,
    /// Tag 5.
    StatsDetailed,
    /// Tag 6. Connection-local diagnostics, explicit and disabled by default.
    ConnectionProfile { enabled: bool, reset: bool },
    /// Tag 7.
    OwnerTimelineArm { limit: usize },
    /// Tag 8.
    OwnerTimelineFetch { offset: usize },
    /// Tag 9.
    CheckpointPosition {
        authority_epoch: u64,
        epoch: u64,
        through: u64,
    },
    /// Tag 10. Whole-file retirement. `epoch` older than the authority: older
    /// sealed stream (no seals, no witness). `epoch` equal to it: durably
    /// sealed files of the serving stream, with the adopted Exact witness.
    RetireWalFiles {
        authority_epoch: u64,
        epoch: u64,
        files: Vec<super::storage::RetireProof>,
        witness: Option<super::storage::RetirementWitness>,
    },
    /// Tag 11. Owner durable progress of stream `epoch` for the coordinator's
    /// durable cut. A long-poll: see `DurableProgressReport` for the reply
    /// contract.
    DurableProgress {
        authority_epoch: u64,
        epoch: u64,
        after: u64,
        max_wait_ms: u32,
        sync: bool,
    },
    /// Tag 12. Complete owner persistence counters and owner transport counters.
    PartitionStats,
    /// Tag 13.
    ReleaseLiveEpochPin { epoch: u64, token: [u8; 16] },
    /// Tag 14.
    ResolveLiveRecords {
        authority_epoch: u64,
        references: Vec<ValueRecordReference>,
    },
    /// Tag 15.
    LiveTargetStats,
    /// Tag 16.
    ShutdownLiveTarget { epoch: u64 },
    /// Tag 17.
    LeaseLiveExtent { epoch: u64 },
    /// Tag 18.
    AdoptLiveExtent {
        epoch: u64,
        token: [u8; 16],
        through: u64,
        close: bool,
        lease_next: bool,
    },
    /// Tag 19.
    OpenLiveExports { authority_epoch: u64 },
    /// Tag 20.
    PreleaseLiveExtent { epoch: u64, after: [u8; 16] },
    /// Tag 21.
    ReplayLiveWal {
        authority_epoch: u64,
        source_epoch: u64,
        from: u64,
        limit: usize,
        require_end: u64,
    },
    /// Tag 22.
    AcquireLiveExportPin {
        epoch: u64,
        segment: super::nvmeof::live_storage::LiveWalSegment,
        binding: super::nvmeof::live_storage::LiveTargetBinding,
    },
    /// Tag 23.
    ReleaseLiveExportPin { epoch: u64, token: [u8; 16] },
    /// Tag 24. Benchmark evidence; owners built without Linux `test-utils`
    /// reply with an error.
    ReadEvidence { epoch: u64, evict: bool },
    /// Tag 25. Benchmark evidence; owners built without Linux `test-utils`
    /// reply with an error.
    ProcessResources { epoch: u64 },
    /// Tag 26.
    InventoryPage(generation_control::InventoryPageRequest),
    /// Tag 27.
    CheckTrimGeneration(generation_control::CheckTrimRequest),
    /// Tag 28.
    TrimGeneration(generation_control::TrimRequest),
    /// Tag 29. Read-only owner WAL file inventory for capacity accounting.
    FileInventoryPage {
        authority_generation: u64,
        source_generation: u64,
        after: Option<u64>,
        limit: u16,
    },
    /// Tag 30. R1 relocation scan: metadata of the complete batches of one
    /// stream in `[from, end)`, active or older sealed
    /// (`storage::RelocationPage`).
    RelocationPage {
        authority_generation: u64,
        source_generation: u64,
        from: u64,
        end: u64,
        limit: u16,
    },
}
impl Request {
    /// Startup/recovery negotiation, durable epoch/retirement maintenance and
    /// live export creation have a different contract from data-path control
    /// messages. A position proof must not extend its caller's data SLA. Client
    /// and owner both derive their deadline from this method.
    fn timeout(&self, data_timeout: Duration) -> Duration {
        let lifecycle = match self {
            Self::RetirementFiles { .. }
            | Self::Status { .. }
            | Self::Fence { .. }
            | Self::Seal { .. }
            | Self::RetireWalFiles { .. }
            // A directory scan with `lstat` per file, like retirement pages.
            | Self::FileInventoryPage { .. }
            // Reads and decodes up to one page of WAL groups.
            | Self::RelocationPage { .. } => true,
            Self::Stats
            | Self::StatsDetailed
            | Self::PartitionStats
            | Self::ConnectionProfile { .. }
            | Self::OwnerTimelineArm { .. }
            | Self::OwnerTimelineFetch { .. }
            | Self::CheckpointPosition { .. } => false,
            // A long-poll parks for its capped wait on top of the data-path SLA.
            Self::DurableProgress { max_wait_ms, .. } => {
                return data_timeout.saturating_add(durable_progress::capped_wait(*max_wait_ms));
            }
            Self::ShutdownLiveTarget { .. }
            | Self::OpenLiveExports { .. }
            | Self::ReplayLiveWal { .. }
            | Self::ResolveLiveRecords { .. }
            | Self::PreleaseLiveExtent { .. }
            // Write grants that create an export inline when no preparation
            // published their file: the first lease after open or a fence (a
            // fence revokes every export). The grant itself is memory work and
            // stays short.
            | Self::LeaseLiveExtent { .. } => true,
            Self::AdoptLiveExtent { lease_next, .. } => *lease_next,
            Self::ReleaseLiveEpochPin { .. }
            | Self::AcquireLiveExportPin { .. }
            | Self::ReleaseLiveExportPin { .. }
            | Self::LiveTargetStats
            | Self::ProcessResources { .. } => false,
            Self::ReadEvidence { evict, .. } => *evict,
            // Cold control lane with normal lifecycle deadlines.
            Self::InventoryPage(_) | Self::CheckTrimGeneration(_) | Self::TrimGeneration(_) => {
                true
            }
        };
        if lifecycle {
            data_timeout.max(LIFECYCLE_TIMEOUT)
        } else {
            data_timeout
        }
    }

    fn is_measurement_control(&self) -> bool {
        matches!(
            self,
            Self::Stats
                | Self::StatsDetailed
                | Self::PartitionStats
                | Self::ConnectionProfile { .. }
                | Self::OwnerTimelineArm { .. }
                | Self::OwnerTimelineFetch { .. }
                | Self::ReadEvidence { .. }
                | Self::ProcessResources { .. }
        )
    }
}
#[derive(Clone, Copy, Serialize, Deserialize)]
struct Receipt {
    id: BatchId,
    version: BatchVersion,
    digest: PayloadDigest,
    address: PhysicalValueAddress,
}
impl Receipt {
    fn from_durable(batch: DurableBatch) -> Self {
        let batch = batch.appended();
        Self {
            id: batch.id(),
            version: batch.version(),
            digest: batch.digest(),
            address: batch.address(),
        }
    }
    fn durable(self) -> DurableBatch {
        DurableBatch::from_validated_parts(self.id, self.version, self.digest, self.address)
    }
}
/// Owner persistence counters plus owner transport counters (`PartitionStats`).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PartitionStatsReply {
    pub partition: PartitionStats,
    /// Owner-side OFI transport counters summed over open and closed sessions;
    /// zero without an OFI transport.
    pub transport: OwnerTransportStats,
}
/// See `Request` for the wire-tag rules.
#[derive(Serialize, Deserialize, strum::AsRefStr)]
enum Response {
    /// Tag 0.
    CheckpointPosition {
        database: [u8; 16],
        partition: u32,
        epoch: u64,
        durable_end: u64,
    },
    /// Tag 1.
    RetirementFiles(Vec<u64>),
    /// Tag 2.
    Retired(super::storage::RetirementOutcome),
    /// Tag 3.
    Status(NodeStatus),
    /// Tag 4.
    Sealed,
    /// Tag 5.
    Stats {
        groups: u64,
        barriers: u64,
        inventory_witnesses: u64,
        records: usize,
        metadata: usize,
    },
    /// Tag 6.
    Error(String),
    /// Tag 7.
    StatsDetailed {
        groups: u64,
        barriers: u64,
        namespace_syncs: u64,
        inventory_witnesses: u64,
        live_frontier_file_syncs: u64,
        live_frontier_namespace_syncs: u64,
        records: usize,
        metadata: usize,
    },
    /// Tag 8. Diagnostics must not enlarge every ordinary response value.
    ConnectionProfile(Box<ConnectionProfile>),
    /// Tag 9.
    OwnerTimelineArmed,
    /// Tag 10.
    OwnerTimelinePage {
        records: Vec<super::write_timeline::OwnerTimelineRecord>,
        dropped: u64,
        incomplete: u64,
        more: bool,
    },
    /// Tag 11.
    DurableProgress(DurableProgressReport),
    /// Tag 12. Boxed so ordinary responses do not grow.
    PartitionStats(Box<PartitionStatsReply>),
    /// Tag 13. Live export pin token (`AcquireLiveExportPin`).
    NvmeofEpochPinned([u8; 16]),
    /// Tag 14. Live epoch or export pin released.
    NvmeofEpochReleased,
    /// Tag 15.
    LiveRecords(LiveRecordLocations),
    /// Tag 16. `ReplayLiveWal` page.
    LiveRecovery(live_wire::LiveRecoveryWire),
    /// Tag 17.
    LiveTargetStats(super::nvmeof::LiveTargetStats),
    /// Tag 18.
    LiveLeased(LiveLeased),
    /// Tag 19.
    LiveAdopted(LiveAdopted),
    /// Tag 20.
    LiveExports(LiveExportInfo),
    /// Tag 21.
    ReadEvidence(crate::read_probe::ReadEvidence),
    /// Tag 22.
    ProcessResources(crate::read_probe::ProcessResources),
    /// Tag 23.
    InventoryPage(generation_control::InventoryPageReply),
    /// Tag 24.
    CheckTrimGeneration(generation_control::CheckTrimReply),
    /// Tag 25.
    TrimGeneration(generation_control::TrimReply),
    /// Tag 26. Boxed so ordinary responses do not grow.
    FileInventoryPage(Box<super::capacity::FileInventoryReply>),
    /// Tag 27. Boxed like FileInventoryPage.
    RelocationPage(Box<super::storage::RelocationPage>),
}

/// Named error of a benchmark evidence request this owner build cannot serve.
#[cfg(not(all(target_os = "linux", feature = "test-utils")))]
const EVIDENCE_UNAVAILABLE: &str = "benchmark evidence requests require test-utils on Linux";

#[cfg(test)]
mod wire_tag_tests;

/// Live exports created inline by an owner request, while its caller waited,
/// by request kind. A lease pays for one only when no
/// preparation published its file first. Each count is one export creation
/// with its target-registry syncs (see `LIFECYCLE_TIMEOUT`). Exports of
/// background preparation and of `OpenLiveExports` are not counted.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
pub struct LiveInlineExportStats {
    /// `LeaseLiveExtent` (kind 33): a lease without a current extent, the first
    /// after open or after a fence.
    pub lease: u64,
    /// `AdoptLiveExtent { lease_next: true }` (kind 34): rollover preparation.
    pub rollover: u64,
    /// `PreleaseLiveExtent` (kind 36): background lease-ahead.
    pub prelease: u64,
    /// `ResolveLiveRecords` (kind 29): the first resolution of a file without
    /// a binding.
    pub resolve: u64,
    /// `ReplayLiveWal`: a history page over a file without a binding.
    pub replay: u64,
}

/// One partition's durable epoch authority. Writes/fences serialize at the node;
/// read requests retain Arc log handles and perform disk I/O outside this mutex.
pub struct StorageNode {
    inventory: Arc<Mutex<NodeInventory>>,
    control: control::ControlOwner,
    control_context: Arc<control::ControlContext>,
    pub(super) live_nvme_selected: bool,
}
impl StorageNode {
    pub fn open(config: NodeConfig, metrics: Arc<Metrics>) -> Result<Arc<Self>, TcpError> {
        if let Some(error) = super::sys::route_unavailable() {
            return Err(TcpError::new(error));
        }
        config.validate_reads()?;
        let wal_member_read = config.wal_member_read;
        let read_max_device_reads = config.read_max_device_reads;
        let inventory = NodeInventory::open(config, metrics)?;
        let live_nvme_selected = inventory.live_target_configured;
        let lifecycle = inventory.lifecycle.clone();
        let readers = inventory.readers.clone();
        let terminal = inventory.terminal.clone();
        let persistence_failed = inventory.persistence_failed.clone();
        let partition = inventory.config.partition.clone();
        let inventory = Arc::new(Mutex::new(inventory));
        let control_context = Arc::new(control::ControlContext {
            partition,
            inventory: inventory.clone(),
            lifecycle,
            readers,
            terminal,
            poison_pending: Arc::new(AtomicBool::new(false)),
            persistence_failed,
            recoverable_live_failure: AtomicBool::new(false),
            poison_gate: Mutex::new(()),
            wal_member_read,
            read_max_device_reads,
            transport: Arc::default(),
            live_control_gate: Mutex::new(()),
        });
        let control = control::ControlOwner::start(control_context.clone())?;
        Ok(Arc::new(Self {
            inventory,
            control,
            control_context,
            live_nvme_selected,
        }))
    }
    pub fn status(&self) -> Result<NodeStatus, TcpError> {
        if self.control_context.poison_pending.load(Ordering::Acquire) {
            return Err(TcpError::new("reader authority is poison-pending"));
        }
        // The gate orders status after any lifecycle step in progress: those
        // steps release the inventory across their syncs.
        let _lifecycle = self.control_context.lifecycle.lock();
        self.inventory.lock().status()
    }
    /// Final owner-process shutdown, after serving sessions have stopped.
    /// Returns actual target teardown evidence; ordinary database close must
    /// not call this because the owner cannot accept another epoch afterward.
    pub fn finish_live_target_shutdown(
        &self,
    ) -> Result<Option<super::nvmeof::LiveTargetStats>, TcpError> {
        if !self.live_nvme_selected {
            return Ok(None);
        }
        self.control.finish_live_target_shutdown().map(Some)
    }

    /// Owner-process preparation totals (bytes, nanoseconds, completed files),
    /// including streams replaced by fencing. Not part of the wire protocol.
    pub fn wal_prewrite_stats(&self) -> (u64, u64, u64) {
        self.inventory.lock().wal_prewrite_stats()
    }

    /// Owner-process count of live exports that a request created inline, by
    /// request kind, including epochs replaced by fencing. Not part of the
    /// wire protocol.
    pub fn live_inline_export_stats(&self) -> LiveInlineExportStats {
        self.inventory.lock().inline_exports
    }

    /// Owner-process R1 seal/retirement counters and the grant-path gate
    /// waits they must not cause. Not part of the wire protocol.
    pub fn file_maintenance_stats(&self) -> FileMaintenanceStats {
        self.inventory.lock().file_maintenance
    }

    /// Test-only: stop the file preparers after their current step (see
    /// `ControlOwner::stop_preparers_for_test`). No grant may follow.
    #[cfg(test)]
    pub(crate) fn stop_file_preparers_for_test(&self) {
        self.control.stop_preparers_for_test();
    }

    #[cfg(test)]
    pub(crate) fn bench_live_grant_profiles(&self) -> serde_json::Value {
        self.control.grant_profiles()
    }
    /// Install the ordinary-file live target fixture before serving any requests.
    /// This exercises the protocol, not a kernel NVMe target.
    #[cfg(feature = "test-utils")]
    pub fn set_live_target_fixture(
        &self,
        config: super::nvmeof::LiveTargetConfig,
        configfs: std::path::PathBuf,
    ) -> Result<(), TcpError> {
        self.install_live_target(
            super::nvmeof::live_target::LiveTargetManager::open_fixture(config, configfs)
                .map_err(TcpError::component)?,
        )
    }
    /// Install a live target manager after open: the gate owns the manager and
    /// the inventory records that one is installed.
    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn install_live_target(
        &self,
        target: super::nvmeof::live_target::LiveTargetManager,
    ) -> Result<(), TcpError> {
        let mut slot = self.control_context.lifecycle.lock();
        let mut inventory = self.inventory.lock();
        if slot.is_some() || inventory.live_target_configured {
            return Err(TcpError::new("live target fixture already installed"));
        }
        inventory.live_target_configured = true;
        *slot = Some(target);
        Ok(())
    }
    /// Test access to the installed live target, taken through the gate.
    #[cfg(test)]
    pub(crate) fn with_live_target<R>(
        &self,
        f: impl FnOnce(&mut super::nvmeof::live_target::LiveTargetManager) -> R,
    ) -> R {
        let mut slot = self.control_context.lifecycle.lock();
        f(slot.as_mut().expect("live target installed"))
    }
    /// Test access to a gated lifecycle step: the gate, then the inventory.
    #[cfg(test)]
    pub(crate) fn with_lifecycle<R>(
        &self,
        f: impl FnOnce(
            &mut parking_lot::MutexGuard<'_, NodeInventory>,
            &mut inventory::LifecycleSlot,
        ) -> R,
    ) -> R {
        let mut slot = self.control_context.lifecycle.lock();
        let mut inventory = self.inventory.lock();
        f(&mut inventory, &mut slot)
    }
    #[cfg(test)]
    pub(crate) fn pause_next_persist(
        &self,
        epoch: u64,
    ) -> Result<
        (
            std::sync::mpsc::Receiver<()>,
            std::sync::mpsc::SyncSender<()>,
        ),
        TcpError,
    > {
        let log = self
            .inventory
            .lock()
            .active(epoch)
            .map_err(TcpError::component)?;
        Ok(log.pause_next_persist())
    }
    #[cfg(test)]
    pub(crate) fn pause_inventory(
        &self,
    ) -> (
        std::sync::mpsc::Receiver<()>,
        std::sync::mpsc::SyncSender<()>,
    ) {
        let inventory = self.inventory.clone();
        let (entered, waiting) = std::sync::mpsc::sync_channel(1);
        let (release, resumed) = std::sync::mpsc::sync_channel(1);
        std::thread::spawn(move || {
            let _guard = inventory.lock();
            let _ = entered.send(());
            let _ = resumed.recv();
        });
        (waiting, release)
    }
    #[cfg(test)]
    pub(crate) fn contains_batch(&self, epoch: u64, id: BatchId) -> Result<bool, TcpError> {
        let log = self
            .inventory
            .lock()
            .active(epoch)
            .map_err(TcpError::component)?;
        log.lookup(id)
            .map(|batch| batch.is_some())
            .map_err(TcpError::component)
    }

    /// Bounded reusable session workers. Shutdown stops acceptance, drains the
    /// bounded socket queue and joins workers; frame I/O has absolute deadlines.
    pub fn serve(
        self: &Arc<Self>,
        listener: TcpListener,
        tls: TlsFiles,
        limits: TcpLimits,
        shutdown: Arc<AtomicBool>,
    ) -> Result<(), TcpError> {
        limits.validate()?;
        if self
            .inventory
            .lock()
            .config
            .partition
            .group_limits
            .max_encoded_bytes
            > limits.max_frame_bytes.saturating_sub(512)
        {
            return Err(TcpError::new(
                "transport frame cannot carry configured complete groups",
            ));
        }
        // The status no longer grows with retained history; the largest reply
        // carrying generation data is a full inventory page.
        let mut largest_status = self.status()?;
        largest_status.active_epoch = Some(u64::MAX);
        largest_status.highest_fenced_generation = u64::MAX;
        largest_status.inventory_revision = u64::MAX;
        wire_codec(limits.max_frame_bytes)
            .serialized_size(&reactor::Envelope {
                protocol: PROTOCOL,
                id: u64::MAX,
                body: Response::Status(largest_status),
            })
            .map_err(TcpError::component)?;
        wire_codec(limits.max_frame_bytes)
            .serialized_size(&reactor::Envelope {
                protocol: PROTOCOL,
                id: u64::MAX,
                body: Response::InventoryPage(generation_control::InventoryPageReply {
                    incarnation: [u8::MAX; 16],
                    revision: u64::MAX,
                    entries: vec![
                        InventoryGeneration {
                            generation: u64::MAX,
                            state: OwnerGenerationState::Abandoned,
                        };
                        generation_control::page_capacity(limits.max_frame_bytes)
                    ],
                    next: Some(u64::MAX),
                }),
            })
            .map_err(TcpError::component)?;
        tls.transport.validate()?;
        let server = tls.server_config()?;
        let transport = tls.transport;
        listener.set_nonblocking(true)?;
        struct ConnectionPermit(Arc<AtomicUsize>);
        impl Drop for ConnectionPermit {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::AcqRel);
            }
        }
        let connections = Arc::new(AtomicUsize::new(0));
        let (accepted, sockets) =
            std::sync::mpsc::sync_channel::<(TcpStream, ConnectionPermit)>(limits.max_connections);
        let sockets = Arc::new(Mutex::new(sockets));
        let mut threads: Vec<thread::JoinHandle<()>> = Vec::new();
        let mut accept_error = None;
        // This service loop intentionally lives until shutdown. Workers grow
        // only to the admitted peak, never beyond max_connections. Do not join
        // individual sessions here: is_finished does not mean OS thread exit
        // has finished, including io_uring's kernel-task cleanup.
        while !shutdown.load(Ordering::Acquire) {
            if threads.iter().any(thread::JoinHandle::is_finished) {
                accept_error = Some(io::Error::other("session worker exited unexpectedly"));
                break;
            }
            match listener.accept() {
                Ok((socket, _)) => {
                    let active = connections.load(Ordering::Acquire);
                    if active >= limits.max_connections {
                        drop(socket);
                        continue;
                    }
                    if active >= threads.len() {
                        let node = self.clone();
                        let server = server.clone();
                        let shutdown = shutdown.clone();
                        let sockets = sockets.clone();
                        let transport = transport.clone();
                        match thread::Builder::new()
                            .name("wal-mtls-session".into())
                            .spawn(move || {
                                // The listener closes this bounded channel at
                                // shutdown. A worker retains one session owner
                                // at a time and is reused across disconnects.
                                loop {
                                    let work = sockets.lock().recv();
                                    let Ok((socket, _permit)) = work else {
                                        break;
                                    };
                                    if shutdown.load(Ordering::Acquire) {
                                        continue;
                                    }
                                    if let Err(error) = node.connection(
                                        socket,
                                        server.clone(),
                                        limits,
                                        &shutdown,
                                        &transport,
                                    ) {
                                        eprintln!("storage control connection closed: {error}");
                                    }
                                }
                            }) {
                            Ok(handle) => threads.push(handle),
                            Err(error) => {
                                accept_error = Some(error);
                                break;
                            }
                        }
                    }
                    connections.fetch_add(1, Ordering::AcqRel);
                    if let Err(error) =
                        accepted.try_send((socket, ConnectionPermit(connections.clone())))
                    {
                        accept_error = Some(io::Error::other(format!(
                            "bounded session queue failed: {error}"
                        )));
                        break;
                    }
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    // Connection setup is outside steady-state progress. Do
                    // not spend another CPU polling an idle listener.
                    thread::sleep(Duration::from_millis(5));
                }
                Err(error) => {
                    accept_error = Some(error);
                    break;
                }
            }
        }
        shutdown.store(true, Ordering::Release);
        drop(accepted);
        drop(sockets);
        for handle in threads {
            if handle.join().is_err() && accept_error.is_none() {
                accept_error = Some(io::Error::other("session worker panicked"));
            }
        }
        match accept_error {
            Some(error) => Err(error.into()),
            None => Ok(()),
        }
    }

    fn connection(
        self: &Arc<Self>,
        socket: TcpStream,
        config: ServerTlsConfig,
        limits: TcpLimits,
        shutdown: &AtomicBool,
        transport: &Transport,
    ) -> Result<(), TcpError> {
        let mut io = DeadlineIo::new(socket, limits.io_timeout)?;
        if transport.is_native() {
            read_transport_mode(&mut io, transport)?;
            if !transport.is_fabric() {
                return reactor::serve_session(
                    self,
                    reactor::Session::new_plain(io)?,
                    limits,
                    shutdown,
                    None,
                );
            }
            #[cfg(feature = "experimental-fabric")]
            {
                let endpoint = fabric::bootstrap_idle_parking(&mut io, transport)?;
                io.fabric = Some(endpoint);
                return reactor::serve_session(
                    self,
                    reactor::Session::new_native(io)?,
                    limits,
                    shutdown,
                    None,
                );
            }
            #[cfg(not(feature = "experimental-fabric"))]
            return Err(TcpError::new("trusted OFI transport was not compiled"));
        }
        let server_config = config
            .config
            .ok_or_else(|| TcpError::new("TLS transport has no server credentials"))?;
        let verifier = config
            .verifier
            .ok_or_else(|| TcpError::new("TLS transport has no peer verifier"))?;
        let mut conn = ServerConnection::new(server_config).map_err(TcpError::component)?;
        conn.set_buffer_limit(Some(reactor::TLS_BUFFER_BYTES));
        while conn.is_handshaking() {
            conn.complete_io(&mut io)?;
        }
        if conn.alpn_protocol() != Some(transport.alpn().as_slice()) {
            return Err(TcpError::new("unsupported authenticated protocol"));
        }
        let stream = StreamOwned::new(conn, io);
        #[cfg(feature = "experimental-fabric")]
        let mut stream = stream;
        if transport.is_fabric() {
            #[cfg(feature = "experimental-fabric")]
            {
                let endpoint = fabric::bootstrap_idle_parking(&mut stream, transport)?;
                stream.sock.fabric = Some(endpoint);
            }
            #[cfg(not(feature = "experimental-fabric"))]
            return Err(TcpError::new("OFI transport was not compiled"));
        }
        reactor::serve_session(
            self,
            reactor::Session::new(stream.conn.into(), stream.sock)?,
            limits,
            shutdown,
            Some(verifier.as_ref()),
        )
    }

    #[cfg(test)]
    fn handle(&self, request: Request, limits: TcpLimits) -> Result<Response, TcpError> {
        self.control_context.handle_profiled(request, limits, None)
    }
}

/// Primary setup spans exclude shared TLS parsing; fabric_open is nested.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ClientConnectProfile {
    pub connect_ns: u64,
    pub tls_ns: u64,
    /// None for TCP; Some measures fabric bootstrap, including native mode setup.
    pub fabric_ns: Option<u64>,
    /// Fresh status RPC; reuse also includes certificate/control revalidation
    /// and retirement of already-owned transport fences.
    pub status_ns: u64,
    /// Fine-grained local endpoint creation spans; None for TCP/reused lanes.
    #[cfg(feature = "experimental-fabric")]
    pub fabric_open: Option<tidehunter_fabric::OpenProfile>,
}

/// Transport config and optional TLS identity built from one credential snapshot.
#[derive(Clone)]
pub(crate) struct ClientTlsConfig {
    pub config: Option<Arc<ClientConfig>>,
    /// Live-target connection key component (the credential digest).
    pub identity: [u8; 32],
}

#[derive(Clone)]
struct ServerTlsConfig {
    config: Option<Arc<ServerConfig>>,
    verifier: Option<Arc<dyn rustls::server::danger::ClientCertVerifier>>,
}

/// Persistent multiplexed connection with explicit TLS or trusted-fabric mode.
/// Synchronous control APIs drive the same correlated protocol as the read owner.
/// Lost replies do not imply rejected writes; failed connections are not retried in place.
pub struct Client {
    stream: reactor::Session,
    expected: PartitionConfig,
    shape: KeyShape,
    timeout: Duration,
    broken: bool,
    frame_limit: usize,
    max_reads: usize,
    page_limit: usize,
    diagnostics: Option<TcpReadProfile>,
    reactor: reactor::ClientState,
    connected_status: Option<NodeStatus>,
    connect_profile: ClientConnectProfile,
    polling: PollingPolicy,
    live_config: Option<LiveClientConfig>,
    live_pin_recipe: Arc<LivePinRecipe>,
    live_exports: Option<(u64, LiveExportInfo, u64)>,
    live_pool: Option<Arc<super::nvmeof::live_pool::LiveControllerPool>>,
    live_writer: Option<live_client::LiveWriteQueue>,
    live_writer_closing: bool,
    live_leased: live_client::LeasedWriter,
    live_write_failed: bool,
    #[cfg(any(test, feature = "test-utils"))]
    live_write_local_fixture: bool,
    live_counters: Option<Arc<super::nvmeof::live_metrics::LiveNvmeCounters>>,
    live_metrics: Option<Arc<super::nvmeof::live_metrics::LiveNvmeMetrics>>,
    live_unreleased_tokens: Vec<(u64, [u8; 16])>,
}
impl Client {
    /// The node's authenticated policy, adopted by ordinary read/control drivers.
    pub fn polling_policy(&self) -> PollingPolicy {
        self.polling
    }

    /// Outstanding leased transport groups, distinct from worker-local queued
    /// groups.
    #[cfg(test)]
    pub(crate) fn append_flights_in_flight(&self) -> usize {
        self.leased_flights_in_flight()
    }
    pub fn connect(
        endpoint: &str,
        tls: &TlsFiles,
        timeout: Duration,
        expected: PartitionConfig,
        shape: &KeyShape,
    ) -> Result<Self, TcpError> {
        let shared = tls.client_config_with_identity()?;
        Self::connect_shared(endpoint, tls, shared, timeout, expected, shape)
    }
    pub(crate) fn connect_shared(
        endpoint: &str,
        tls: &TlsFiles,
        client_config: ClientTlsConfig,
        timeout: Duration,
        expected: PartitionConfig,
        shape: &KeyShape,
    ) -> Result<Self, TcpError> {
        Self::connect_shared_with_status(
            endpoint,
            tls,
            client_config,
            timeout,
            expected,
            shape,
            true,
        )
    }
    fn connect_shared_with_status(
        endpoint: &str,
        tls: &TlsFiles,
        client_config: ClientTlsConfig,
        timeout: Duration,
        expected: PartitionConfig,
        shape: &KeyShape,
        query_status: bool,
    ) -> Result<Self, TcpError> {
        let live_pin_recipe = Arc::new(LivePinRecipe::new(
            endpoint,
            tls,
            &client_config,
            timeout,
            &expected,
            shape,
        ));
        expected.validate().map_err(TcpError::component)?;
        tls.transport.validate()?;
        if timeout.is_zero() || timeout > Duration::from_secs(300) {
            return Err(TcpError::new("invalid client timeout"));
        }
        let address: SocketAddr = endpoint.parse().map_err(|_| {
            TcpError::new("endpoint must be numeric IP:port; set TLS server_name separately")
        })?;
        let began = Instant::now();
        let socket = TcpStream::connect_timeout(&address, timeout)?;
        let connect_ns = elapsed_ns(began);
        let mut io = DeadlineIo::new(socket, timeout)?;
        let setup_started = Instant::now();
        if tls.transport.is_native() {
            write_transport_mode(&mut io, &tls.transport)?;
        }
        let (session, tls_ns, fabric_ns) = if tls.transport.is_native() {
            if !tls.transport.is_fabric() {
                (reactor::Session::new_plain(io)?, 0, None)
            } else {
                #[cfg(feature = "experimental-fabric")]
                {
                    let endpoint = fabric::bootstrap(&mut io, &tls.transport)?;
                    io.fabric = Some(endpoint);
                    let fabric_ns = Some(elapsed_ns(setup_started));
                    (reactor::Session::new_native(io)?, 0, fabric_ns)
                }
                #[cfg(not(feature = "experimental-fabric"))]
                {
                    return Err(TcpError::new("trusted OFI transport was not compiled"));
                }
            }
        } else {
            let name =
                ServerName::try_from(tls.server_name.clone()).map_err(TcpError::component)?;
            let client_tls_config = client_config
                .config
                .ok_or_else(|| TcpError::new("TLS transport has no client credentials"))?;
            let mut conn =
                ClientConnection::new(client_tls_config, name).map_err(TcpError::component)?;
            conn.set_buffer_limit(Some(reactor::TLS_BUFFER_BYTES));
            while conn.is_handshaking() {
                conn.complete_io(&mut io)?;
            }
            let tls_ns = elapsed_ns(setup_started);
            if conn.alpn_protocol() != Some(tls.transport.alpn().as_slice()) {
                return Err(TcpError::new(
                    "required authenticated transport was not negotiated",
                ));
            }
            let stream = StreamOwned::new(conn, io);
            #[cfg(feature = "experimental-fabric")]
            let mut stream = stream;
            #[cfg(feature = "experimental-fabric")]
            let fabric_ns = if tls.transport.is_fabric() {
                let began = Instant::now();
                stream.sock.fabric = Some(fabric::bootstrap(&mut stream, &tls.transport)?);
                Some(elapsed_ns(began))
            } else {
                None
            };
            #[cfg(not(feature = "experimental-fabric"))]
            let fabric_ns = None;
            (
                reactor::Session::new(stream.conn.into(), stream.sock)?,
                tls_ns,
                fabric_ns,
            )
        };
        let shape = shape.clone();
        let mut client = Self {
            stream: session,
            expected,
            shape,
            timeout,
            broken: false,
            frame_limit: HARD_FRAME_LIMIT,
            max_reads: 256,
            page_limit: 1024,
            diagnostics: None,
            reactor: reactor::ClientState::default(),
            connected_status: None,
            connect_profile: ClientConnectProfile {
                connect_ns,
                tls_ns,
                fabric_ns,
                status_ns: 0,
                #[cfg(feature = "experimental-fabric")]
                fabric_open: None,
            },
            polling: PollingPolicy::Adaptive,
            live_config: None,
            live_pin_recipe,
            live_exports: None,
            live_pool: None,
            live_writer: None,
            live_writer_closing: false,
            live_leased: live_client::LeasedWriter::default(),
            live_write_failed: false,
            #[cfg(any(test, feature = "test-utils"))]
            live_write_local_fixture: false,
            live_counters: None,
            live_metrics: None,
            live_unreleased_tokens: Vec::new(),
        };
        if query_status {
            let began = Instant::now();
            client.connected_status = Some(client.status()?);
            client.connect_profile.status_ns = elapsed_ns(began);
        }
        #[cfg(feature = "experimental-fabric")]
        {
            // Provider setup may defer bounded resources until its first CQ
            // progress, inside status rather than endpoint creation.
            client.connect_profile.fabric_open = client
                .stream
                .sock
                .fabric
                .as_ref()
                .map(tidehunter_fabric::Endpoint::open_profile);
        }
        Ok(client)
    }
    /// Authenticated status captured during connect; not a fresh health probe.
    pub(crate) fn connected_status(&self) -> &NodeStatus {
        self.connected_status
            .as_ref()
            .expect("connected client status")
    }
    /// Whether this client retains a locally usable session, not a live peer
    /// health probe. A broken client returns `None`: its underlying session may
    /// have been aborted or merely marked unusable, so absence is not proven.
    pub fn session_present(&self) -> Option<bool> {
        (!self.broken).then_some(true)
    }

    pub(crate) fn connect_profile(&self) -> ClientConnectProfile {
        self.connect_profile
    }
    /// Returns setup-time provider evidence for the selected OFI endpoint.
    /// TCP sessions have no provider metadata; no provider query occurs per request.
    #[cfg(feature = "experimental-fabric")]
    pub fn provider_info(&self) -> Option<&tidehunter_fabric::ProviderInfo> {
        self.stream
            .sock
            .fabric
            .as_ref()
            .map(tidehunter_fabric::Endpoint::provider_info)
    }
    fn request(&mut self, request: Request) -> Result<Response, TcpError> {
        self.request_inner(request, None)
    }
    fn request_inner(
        &mut self,
        request: Request,
        profile: Option<&mut TcpReadProfile>,
    ) -> Result<Response, TcpError> {
        if self.diagnostics.is_none() || request.is_measurement_control() {
            return self.exchange(&request, profile);
        }
        let mut measured = TcpReadProfile::default();
        let response = self.exchange(&request, Some(&mut measured));
        if let Some(profile) = profile {
            profile.add(measured);
        }
        if response.is_ok()
            && let Some(total) = &mut self.diagnostics
        {
            total.add(measured);
        }
        response
    }
    fn exchange(
        &mut self,
        request: &Request,
        profile: Option<&mut TcpReadProfile>,
    ) -> Result<Response, TcpError> {
        self.exchange_correlated(request, profile)
    }
    /// Capture/reset this authenticated connection's server clocks. The caller
    /// keeps this extra control exchange outside measured data operations.
    /// # Errors
    /// Returns transport, protocol or remote errors; failed connections cannot be reused.
    pub fn connection_profile(
        &mut self,
        enabled: bool,
        reset: bool,
    ) -> Result<ConnectionProfile, TcpError> {
        // Exclude this diagnostic control exchange from the client transport
        // snapshot, including the encrypted profile response itself.
        let client_transport = self.transport_progress_stats();
        let mut previous = self.diagnostics.take().unwrap_or_default();
        previous.socket = self.stream.sock.profile.take().unwrap_or_default();
        // The control exchange is deliberately outside both client timing
        // windows and the returned transport snapshot.
        self.stream.configure_transport_stats(false, false);
        let response = self.request(Request::ConnectionProfile { enabled, reset })?;
        match response {
            Response::ConnectionProfile(mut profile) => {
                self.stream.configure_transport_stats(enabled, reset);
                self.stream.sock.profile = enabled.then(|| {
                    if reset {
                        SocketProfile::default()
                    } else {
                        previous.socket
                    }
                });
                self.diagnostics = enabled.then(|| {
                    if reset {
                        TcpReadProfile::default()
                    } else {
                        previous
                    }
                });
                profile.client = previous;
                profile.client_transport = client_transport;
                Ok(*profile)
            }
            response => self.wrong_variant("ConnectionProfile", &response, "ConnectionProfile"),
        }
    }
    pub(crate) fn owner_timeline_arm(&mut self, limit: usize) -> Result<(), TcpError> {
        match self.request(Request::OwnerTimelineArm { limit })? {
            Response::OwnerTimelineArmed => Ok(()),
            response => self.wrong_variant("OwnerTimelineArm", &response, "OwnerTimelineArmed"),
        }
    }

    pub(crate) fn owner_timeline_fetch_partial(
        &mut self,
    ) -> (
        Vec<super::write_timeline::OwnerTimelineRecord>,
        u64,
        u64,
        Option<TcpError>,
    ) {
        let mut records = Vec::new();
        let mut dropped = 0;
        let mut incomplete = 0;
        loop {
            match self.request(Request::OwnerTimelineFetch {
                offset: records.len(),
            }) {
                Ok(Response::OwnerTimelinePage {
                    records: page,
                    dropped: d,
                    incomplete: i,
                    more,
                }) => {
                    if more && page.is_empty() {
                        return (
                            records,
                            dropped,
                            incomplete,
                            Some(TcpError::new("empty owner timeline page")),
                        );
                    }
                    records.extend(page);
                    dropped = d;
                    incomplete = i;
                    if !more {
                        return (records, dropped, incomplete, None);
                    }
                }
                Ok(_) => {
                    return (
                        records,
                        dropped,
                        incomplete,
                        Some(TcpError::new("unexpected owner timeline response")),
                    );
                }
                Err(error) => return (records, dropped, incomplete, Some(error)),
            }
        }
    }
    pub(crate) fn owner_timeline_arm_until(
        &mut self,
        limit: usize,
        deadline: Instant,
    ) -> Result<(), TcpError> {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(TcpError::new("owner timeline deadline expired"));
        }
        let previous = self.timeout;
        self.timeout = previous.min(remaining);
        let result = self.owner_timeline_arm(limit);
        self.timeout = previous;
        result
    }

    pub(crate) fn owner_timeline_page_until(
        &mut self,
        offset: usize,
        deadline: Instant,
    ) -> Result<
        (
            Vec<super::write_timeline::OwnerTimelineRecord>,
            u64,
            u64,
            bool,
        ),
        TcpError,
    > {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(TcpError::new("owner timeline deadline expired"));
        }
        let previous = self.timeout;
        self.timeout = previous.min(remaining);
        let response = self.request(Request::OwnerTimelineFetch { offset });
        self.timeout = previous;
        match response? {
            Response::OwnerTimelinePage {
                records,
                dropped,
                incomplete,
                more,
            } => {
                if more && records.is_empty() {
                    return Err(TcpError::new("empty owner timeline page"));
                }
                Ok((records, dropped, incomplete, more))
            }
            response => self.wrong_variant("OwnerTimelineFetch", &response, "OwnerTimelinePage"),
        }
    }

    #[cfg(all(target_os = "linux", feature = "test-utils"))]
    /// # Errors
    /// Returns transport, epoch validation or remote Linux evidence errors.
    pub fn read_evidence_for_benchmark(
        &mut self,
        epoch: u64,
        evict: bool,
    ) -> Result<crate::read_probe::ReadEvidence, TcpError> {
        match self.request(Request::ReadEvidence { epoch, evict })? {
            Response::ReadEvidence(evidence) => Ok(evidence),
            response => self.wrong_variant("ReadEvidence", &response, "ReadEvidence"),
        }
    }
    #[cfg(all(target_os = "linux", feature = "test-utils"))]
    /// # Errors
    /// Returns authenticated transport, epoch or process accounting errors.
    pub fn process_resources_for_benchmark(
        &mut self,
        epoch: u64,
    ) -> Result<crate::read_probe::ProcessResources, TcpError> {
        match self.request(Request::ProcessResources { epoch })? {
            Response::ProcessResources(resources) => Ok(resources),
            response => self.wrong_variant("ProcessResources", &response, "ProcessResources"),
        }
    }
    fn wrong_response<T>(
        &mut self,
        request: &str,
        detail: impl std::fmt::Display,
    ) -> Result<T, TcpError> {
        self.broken = true;
        Err(TcpError::new(format!(
            "{request} reply failed check: {detail}"
        )))
    }
    fn wrong_variant<T>(
        &mut self,
        request: &str,
        response: &Response,
        expected: &str,
    ) -> Result<T, TcpError> {
        self.wrong_response(
            request,
            format_args!("expected {expected}, received {}", response.as_ref()),
        )
    }
    fn shape_sha256(yaml: &str) -> String {
        // This named TLS 1.3 suite always supplies SHA-256. Rustls/ring is
        // enabled by experimental-distributed, including no-test-utils clients.
        let hash = rustls::crypto::ring::cipher_suite::TLS13_AES_128_GCM_SHA256
            .tls13()
            .expect("TLS13_AES_128_GCM_SHA256 is a TLS 1.3 suite")
            .common
            .hash_provider
            .hash(yaml.as_bytes());
        let mut hex = String::with_capacity(64);
        const DIGITS: &[u8; 16] = b"0123456789abcdef";
        for byte in hash.as_ref() {
            hex.push(char::from(DIGITS[(byte >> 4) as usize]));
            hex.push(char::from(DIGITS[(byte & 15) as usize]));
        }
        hex
    }
    fn check_status(&mut self, request: &str, status: NodeStatus) -> Result<NodeStatus, TcpError> {
        if status.partition != self.expected {
            return self.wrong_response(
                request,
                "partition differs from the client partition configuration",
            );
        }
        let expected_shape = self.shape.to_yaml().map_err(TcpError::component)?;
        if status.shape_yaml != expected_shape {
            return self.wrong_response(
                request,
                format_args!(
                    "shape_yaml SHA-256 owner={} client={}",
                    Self::shape_sha256(&status.shape_yaml),
                    Self::shape_sha256(&expected_shape)
                ),
            );
        }
        if !(1024..=HARD_FRAME_LIMIT).contains(&status.max_frame_bytes) {
            return self.wrong_response(
                request,
                format_args!(
                    "max_frame_bytes={}; expected 1024..={HARD_FRAME_LIMIT}",
                    status.max_frame_bytes
                ),
            );
        }
        if !(1..=256).contains(&status.max_reads) {
            return self.wrong_response(
                request,
                format_args!("max_reads={}; expected 1..=256", status.max_reads),
            );
        }
        if !(1..=1024).contains(&status.recovery_page_batches) {
            return self.wrong_response(
                request,
                format_args!(
                    "recovery_page_batches={}; expected 1..=1024",
                    status.recovery_page_batches
                ),
            );
        }
        // An active generation is always the owner high-water.
        if status
            .active_epoch
            .is_some_and(|epoch| epoch == 0 || epoch != status.highest_fenced_generation)
        {
            return self.wrong_response(
                request,
                format_args!(
                    "active_epoch={:?} is not the owner high-water {}",
                    status.active_epoch, status.highest_fenced_generation
                ),
            );
        }
        self.frame_limit = status.max_frame_bytes;
        self.max_reads = status.max_reads;
        self.page_limit = status.recovery_page_batches;
        self.polling = status.polling;
        Ok(status)
    }
    pub fn status(&mut self) -> Result<NodeStatus, TcpError> {
        match self.request(Request::Status { protocol: PROTOCOL })? {
            Response::Status(status) => self.check_status("Status", status),
            response => self.wrong_variant("Status", &response, "Status"),
        }
    }
    pub fn fence_to(&mut self, epoch: u64) -> Result<NodeStatus, TcpError> {
        match self.request(Request::Fence { epoch })? {
            Response::Status(status) if status.active_epoch == Some(epoch) => {
                self.check_status("Fence", status)
            }
            Response::Status(status) => self.wrong_response(
                "Fence",
                format_args!("active_epoch={:?}, expected {epoch}", status.active_epoch),
            ),
            response => self.wrong_variant("Fence", &response, "Status"),
        }
    }
    fn check_receipt(
        &self,
        batch: &CompleteBatch,
        receipt: Receipt,
    ) -> Result<DurableBatch, TcpError> {
        let address = receipt.address;
        if receipt.id != batch.id()
            || receipt.version != batch.version()
            || receipt.digest != batch.digest()
            || receipt.id.database != self.expected.inventory.database
            || self
                .expected
                .inventory
                .partition_for(receipt.id)
                .map_err(TcpError::component)?
                != self.expected.partition
            || address.partition != self.expected.partition
            || address.segment_generation != receipt.version.epoch
            || usize::try_from(address.len).ok() != batch.encoded().len().checked_add(8)
            || address.offset.checked_add(u64::from(address.len)).is_none()
            || address.offset % self.expected.fragment_bytes + u64::from(address.len)
                > self.expected.fragment_bytes
        {
            return Err(TcpError::new(
                "receipt differs from requested complete batch",
            ));
        }
        Ok(receipt.durable())
    }
    pub fn read_record(
        &mut self,
        epoch: u64,
        reference: &ValueRecordReference,
    ) -> Result<Mutation, TcpError> {
        self.read_records(epoch, std::slice::from_ref(reference))?
            .pop()
            .ok_or_else(|| TcpError::new("empty record response"))
    }
    /// Direct live reads with a fresh published-index cache. The read-service
    /// lanes retain their own bounded, trusted-index grants across requests.
    pub fn read_records(
        &mut self,
        epoch: u64,
        references: &[ValueRecordReference],
    ) -> Result<Vec<Mutation>, TcpError> {
        if self.live_config.is_none() {
            return Err(TcpError::new("reads require live NVMe-oF configuration"));
        }
        if !self.live_unreleased_tokens.is_empty() {
            return Err(TcpError::new("live read has quarantined owner grants"));
        }
        let mut cache = live_read::LiveReadCache::default();
        let result = self.live_read_records(
            &mut cache,
            epoch,
            references,
            Instant::now() + self.timeout,
            None,
        );
        let cleanup = cache.close(self, epoch);
        self.live_unreleased_tokens.extend(
            cache
                .take_unreleased()
                .into_iter()
                .map(|token| (epoch, token)),
        );
        // Live I/O and validation clocks come from live_nvmeof_stats.
        match (result, cleanup) {
            (Ok(values), Ok(())) => Ok(values),
            (Err(error), Ok(())) => Err(error),
            (result, Err(cleanup)) => {
                self.broken = true;
                Err(TcpError::new(format!(
                    "live read result {:?}; owner grant cleanup remains quarantined: {cleanup}",
                    result.err(),
                )))
            }
        }
    }
    pub fn seal(&mut self, epoch: u64) -> Result<(), TcpError> {
        match self.request(Request::Seal { epoch })? {
            Response::Sealed => Ok(()),
            response => self.wrong_variant("Seal", &response, "Sealed"),
        }
    }
    /// Complete owner persistence counters.
    pub fn stats(&mut self) -> Result<PartitionStats, TcpError> {
        self.partition_stats().map(|reply| reply.partition)
    }
    /// Owner persistence counters with the owner's OFI transport counters.
    pub fn partition_stats(&mut self) -> Result<PartitionStatsReply, TcpError> {
        match self.request(Request::PartitionStats)? {
            Response::PartitionStats(reply) => Ok(*reply),
            response => self.wrong_variant("PartitionStats", &response, "PartitionStats"),
        }
    }
}

impl TlsFiles {
    fn trusted_mode(&self) -> Result<bool, TcpError> {
        self.transport.validate()?;
        let trusted = self.transport.is_native();
        if trusted
            && [&self.ca, &self.cert, &self.key]
                .iter()
                .any(|path| !path.as_os_str().is_empty())
        {
            return Err(TcpError::new(
                "trusted-network transport requires empty TLS credential paths",
            ));
        }
        Ok(trusted)
    }
    fn material(
        &self,
    ) -> Result<
        (
            RootCertStore,
            Vec<CertificateDer<'static>>,
            PrivateKeyDer<'static>,
            [u8; 32],
        ),
        TcpError,
    > {
        fn bounded(path: &std::path::Path) -> Result<Vec<u8>, TcpError> {
            let file = fs::File::open(path)?;
            if file.metadata()?.len() > 1024 * 1024 {
                return Err(TcpError::new("TLS material exceeds size bound"));
            }
            let mut bytes = Vec::new();
            file.take(1024 * 1024 + 1).read_to_end(&mut bytes)?;
            if bytes.len() > 1024 * 1024 {
                return Err(TcpError::new("TLS material exceeds size bound"));
            }
            Ok(bytes)
        }
        let ca = bounded(&self.ca)?;
        let cert = bounded(&self.cert)?;
        let key = bounded(&self.key)?;
        let mut hash = blake2b_simd::Params::new().hash_length(32).to_state();
        for bytes in [&ca, &cert, &key] {
            hash.update(&(bytes.len() as u64).to_le_bytes());
            hash.update(bytes);
        }
        let identity = hash.finalize().as_bytes().try_into().expect("32-byte hash");
        let mut roots = RootCertStore::empty();
        roots
            .add(CertificateDer::from(ca))
            .map_err(TcpError::component)?;
        Ok((
            roots,
            vec![CertificateDer::from(cert)],
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key)),
            identity,
        ))
    }
    fn server_config(&self) -> Result<ServerTlsConfig, TcpError> {
        if self.trusted_mode()? {
            return Ok(ServerTlsConfig {
                config: None,
                verifier: None,
            });
        }
        let (roots, cert, key, _) = self.material()?;
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
            Arc::new(roots),
            provider.clone(),
        )
        .build()
        .map_err(TcpError::component)?;
        let mut config = ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(TcpError::component)?
            .with_client_cert_verifier(verifier.clone())
            .with_single_cert(cert, key)
            .map_err(TcpError::component)?;
        config.alpn_protocols = self.transport.server_alpn();
        Ok(ServerTlsConfig {
            config: Some(Arc::new(config)),
            verifier: Some(verifier),
        })
    }
    #[cfg(test)]
    pub(crate) fn client_config(&self) -> Result<Arc<ClientConfig>, TcpError> {
        self.client_config_with_identity()?
            .config
            .ok_or_else(|| TcpError::new("trusted-network transport has no TLS client config"))
    }
    pub(crate) fn client_config_with_identity(&self) -> Result<ClientTlsConfig, TcpError> {
        if self.trusted_mode()? {
            return Ok(ClientTlsConfig {
                config: None,
                identity: [0; 32],
            });
        }
        let (roots, cert, key, identity) = self.material()?;
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let verifier = rustls::client::WebPkiServerVerifier::builder_with_provider(
            Arc::new(roots),
            provider.clone(),
        )
        .build()
        .map_err(TcpError::component)?;
        let mut config = ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(TcpError::component)?
            .with_webpki_verifier(verifier)
            .with_client_auth_cert(cert, key)
            .map_err(TcpError::component)?;
        config.alpn_protocols = vec![self.transport.alpn()];
        Ok(ClientTlsConfig {
            config: Some(Arc::new(config)),
            identity,
        })
    }
}

fn wire_codec(limit: usize) -> impl Options {
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_limit(limit as u64)
        .reject_trailing_bytes()
}
#[cfg(test)]
fn read_frame<T: serde::de::DeserializeOwned>(
    stream: &mut impl Read,
    limit: usize,
) -> Result<T, TcpError> {
    let bytes = read_frame_bytes(stream, limit)?;
    wire_codec(limit)
        .deserialize(&bytes)
        .map_err(TcpError::component)
}
#[cfg(test)]
fn read_frame_bytes(stream: &mut impl Read, limit: usize) -> Result<Vec<u8>, TcpError> {
    let mut bytes = Vec::new();
    read_frame_into(stream, limit, &mut bytes)?;
    Ok(bytes)
}
#[cfg(test)]
fn read_frame_length(stream: &mut impl Read, limit: usize) -> Result<usize, TcpError> {
    let mut header = [0; 4];
    stream.read_exact(&mut header)?;
    let len = u32::from_be_bytes(header) as usize;
    if len == 0 || len > limit {
        return Err(TcpError::new("frame length exceeds limit"));
    }
    Ok(len)
}
#[cfg(test)]
fn read_frame_into(
    stream: &mut impl Read,
    limit: usize,
    bytes: &mut Vec<u8>,
) -> Result<(), TcpError> {
    let len = read_frame_length(stream, limit)?;
    bytes.resize(len, 0);
    stream.read_exact(bytes)?;
    Ok(())
}
#[cfg(test)]
fn write_frame<T: Serialize>(
    stream: &mut impl Write,
    message: &T,
    limit: usize,
) -> Result<(), TcpError> {
    let frame = encode_frame(message, limit)?;
    stream.write_all(&frame)?;
    stream.flush()?;
    Ok(())
}
#[cfg(test)]
fn encode_frame<T: Serialize>(message: &T, limit: usize) -> Result<Vec<u8>, TcpError> {
    let mut frame = Vec::new();
    encode_frame_into(message, limit, &mut frame)?;
    Ok(frame)
}
#[cfg(test)]
fn encode_frame_into<T: Serialize>(
    message: &T,
    limit: usize,
    frame: &mut Vec<u8>,
) -> Result<(), TcpError> {
    let size = wire_codec(limit)
        .serialized_size(message)
        .map_err(TcpError::component)? as usize;
    encode_sized_frame_into(message, limit, size, frame)
}
fn encode_sized_frame_into<T: Serialize>(
    message: &T,
    limit: usize,
    size: usize,
    frame: &mut Vec<u8>,
) -> Result<(), TcpError> {
    if size == 0 || size > limit {
        return Err(TcpError::new("frame length exceeds limit"));
    }
    frame.clear();
    frame.reserve_exact(size + 4);
    frame.extend_from_slice(&(size as u32).to_be_bytes());
    // Like bincode::serialize, reserve after bounded sizing and serialize once
    // without repeating that sizing walk. No bytes escape before size validation.
    wire_codec(limit)
        .with_no_limit()
        .serialize_into(&mut *frame, message)
        .map_err(TcpError::component)?;
    if frame.len() != size + 4 {
        return Err(TcpError::new("serialized frame size changed"));
    }
    // One application write prevents a separate tiny TLS record for the header.
    Ok(())
}
fn elapsed_ns(started: Instant) -> u64 {
    started.elapsed().as_nanos().min(u64::MAX as u128) as u64
}

/// Absolute deadline checked for every underlying TLS socket operation, so a
/// trickling peer cannot renew the timeout for each byte of one frame.
struct DeadlineIo {
    #[cfg(feature = "experimental-fabric")]
    fabric: Option<tidehunter_fabric::Endpoint>,
    socket: TcpStream,
    deadline: Instant,
    read_timeout: Option<Duration>,
    write_timeout: Option<Duration>,
    profile: Option<SocketProfile>,
}
impl DeadlineIo {
    fn new(socket: TcpStream, timeout: Duration) -> io::Result<Self> {
        socket.set_nodelay(true)?;
        Ok(Self {
            socket,
            #[cfg(feature = "experimental-fabric")]
            fabric: None,
            deadline: Instant::now() + timeout,
            read_timeout: None,
            write_timeout: None,
            profile: None,
        })
    }
    fn remaining(&self) -> io::Result<Duration> {
        self.deadline
            .checked_duration_since(Instant::now())
            .filter(|duration| !duration.is_zero())
            .ok_or_else(|| io::Error::new(io::ErrorKind::TimedOut, "TLS frame deadline expired"))
    }
    fn prepare(&mut self, read: bool) -> io::Result<()> {
        let began = self.profile.map(|_| Instant::now());
        let remaining = self.remaining()?;
        #[cfg(feature = "experimental-fabric")]
        if self.fabric.is_some() {
            return Err(io::Error::other(
                "blocking I/O forbidden after fabric switch",
            ));
        }
        // Never round a timeout upwards: keep the absolute deadline and use
        // sub-millisecond precision for its final fraction. Reusing the same
        // millisecond avoids a setsockopt for every small TLS read/write.
        let timeout = if remaining >= Duration::from_millis(1) {
            Duration::from_millis(remaining.as_millis() as u64)
        } else {
            remaining
        };
        let cached = if read {
            &mut self.read_timeout
        } else {
            &mut self.write_timeout
        };
        let changed = *cached != Some(timeout);
        if changed {
            if read {
                self.socket.set_read_timeout(Some(timeout))?;
            } else {
                self.socket.set_write_timeout(Some(timeout))?;
            }
            *cached = Some(timeout);
        }
        if let Some(p) = self.profile.as_mut() {
            p.timeout_setup_ns += began.map_or(0, elapsed_ns);
            p.timeout_sets += u64::from(changed);
        }
        Ok(())
    }
}
impl Read for DeadlineIo {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        self.prepare(true)?;
        let began = self.profile.map(|_| Instant::now());
        let result = self.socket.read(bytes);
        if let Some(p) = self.profile.as_mut() {
            p.read_ns += began.map_or(0, elapsed_ns);
            p.read_calls += 1;
            p.read_bytes += result.as_ref().copied().unwrap_or(0) as u64;
        }
        result
    }
}
impl Write for DeadlineIo {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.prepare(false)?;
        let began = self.profile.map(|_| Instant::now());
        let result = self.socket.write(bytes);
        if let Some(p) = self.profile.as_mut() {
            p.write_ns += began.map_or(0, elapsed_ns);
            p.write_calls += 1;
            p.write_bytes += result.as_ref().copied().unwrap_or(0) as u64;
        }
        result
    }
    fn flush(&mut self) -> io::Result<()> {
        self.socket.flush()
    }
    fn write_vectored(&mut self, buffers: &[io::IoSlice<'_>]) -> io::Result<usize> {
        // rustls offers multiple encrypted records together. The default Write
        // implementation only uses the first slice and defeats that batching.
        self.prepare(false)?;
        let began = self.profile.map(|_| Instant::now());
        let result = self.socket.write_vectored(buffers);
        if let Some(p) = self.profile.as_mut() {
            p.write_ns += began.map_or(0, elapsed_ns);
            p.write_calls += 1;
            p.write_bytes += result.as_ref().copied().unwrap_or(0) as u64;
        }
        result
    }
}
