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
pub(crate) use client_profile::ClientTlsConfig;
pub use client_profile::{ClientConnectProfile, LiveInlineExportStats};
pub use fabric::{FabricBackend, FabricConfig, FabricSecurity, Transport, TransportProgressStats};
mod owner_transport;
pub(crate) use owner_transport::OwnerTransportCounters;
pub use owner_transport::OwnerTransportStats;
#[cfg(feature = "experimental-fabric")]
pub use tidehunter_fabric::ProviderInfo;
mod client_profile;
mod control;
mod coordinator_lease;
mod durable_progress;
pub use durable_progress::{DURABLE_PROGRESS_MAX_WAIT, DurableProgressReport};
mod generation_control;
pub use generation_control::{InventoryGeneration, OwnerGenerationState, TrimReadiness};
pub mod host;
mod inventory;
#[cfg(test)]
pub(crate) use generation_control::OwnerTrimCrash;
mod live_client;
pub(crate) use live_client::{BreakKind, ResumeReaper};
mod progress_wait;
pub(crate) use progress_wait::WakeFd;
#[cfg(test)]
mod progress_wait_tests;
#[cfg(any(test, feature = "test-utils"))]
pub use host::DatabaseOpenHook;
pub(crate) use host::identity as host_identity;
pub use host::{
    DatabaseLiveTarget, DatabaseProvisionSpec, HostDurabilityDefaults, HostStatus, LiveTargetPool,
    OwnerHost, OwnerHostConfig,
};
mod connect_shared;
mod deadline_io;
mod listener;
pub use super::nvmeof::LiveQueueGeometry;
#[cfg(test)]
pub(crate) use live_client::leased_tests;
pub(super) use live_client::{LeasedSubmit, LiveGroupBudget};
pub use live_client::{LeasedSync, LiveClientConfig, LiveInitiatorIdentity};
pub(crate) use reactor::HostClient;
mod live_control;
pub(super) mod live_read;
mod live_recovery;
mod relocation_scan;
pub use diagnostics::{ConnectionProfile, PollingProfile, SocketProfile, WorkProfile};
pub use live_control::{LiveRecoveryEntry, LiveRecoveryPage};
mod live_wire;

pub use live_wire::{
    AdoptProfile, LeaseProfile, LiveAdopted, LiveExportInfo, LiveLeased, LiveRecordLocation,
    LiveRecordLocations,
};
#[cfg(test)]
mod connection_tests;
#[cfg(test)]
#[path = "tcp/probe_tests.rs"]
mod probe;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod transport_security_tests;

use super::codec::CompleteBatch;
#[cfg(test)]
use super::codec::Mutation;
use super::storage::StorageError;
use super::storage::{DurableBatch, PartitionConfig, PartitionStats};
use super::types::{BatchId, BatchVersion, PayloadDigest, PhysicalValueAddress, RequestAuthority};
use crate::key_shape::KeyShape;
use crate::metrics::Metrics;
use bincode::Options;
use deadline_io::DeadlineIo;
use parking_lot::Mutex;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName};
use rustls::{
    ClientConfig, ClientConnection, RootCertStore, ServerConfig, ServerConnection, StreamOwned,
};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fs;
use std::io::{self, Read, Write};
use std::net::TcpListener;
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
/// 34 (L2) scopes Status and types Error in place, and appends ProvisionDatabase
/// and DropDatabase (request tags 31–32, response tags 28–29).
/// 35 (hybrid index) keys ResolveLiveRecords/LiveRecords by `PhysicalValueAddress`
/// (payloads change in place; tags are unchanged), and adds `durable_end`, the
/// end of the highest durable frame of the generation's log, to every
/// `InventoryGeneration` of the inventory sweep.
/// 36 (lifetime exports) removes AcquireLiveExportPin/ReleaseLiveExportPin (tags 22-23) and
/// `NvmeofEpochPinned` (tag 13); later tags shift. Exports live as long as their files,
/// `export_pins` leaves `WalFileObservation`, and write-protect counters join `LiveTargetStats`.
/// 37 (coordinator leases) appends RenewCoordinatorLease (request 31) and its
/// granted-duration reply (response 29).
const PROTOCOL: u32 = 37;
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
    /// live record resolution (see `max_device_reads_per_status`).
    #[serde(default = "default_async_read_policy")]
    pub wal_member_read_policy: super::storage::WalMemberReadPolicy,
    /// Write and sync zeros across each external WAL file before it is usable,
    /// so live writes never convert unwritten extents. On by default and always
    /// serialized: an absent key means on, and opting out requires `false`.
    #[serde(default = "default_wal_prewrite")]
    pub wal_prewrite: bool,
    /// With `wal_member_read_policy: async`, caps `NodeStatus::max_reads` and
    /// the members of one `ResolveLiveRecords` request.
    #[serde(default = "default_device_reads")]
    pub max_device_reads_per_status: usize,
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
        validate_device_read_limit(self.max_device_reads_per_status)
    }
}

fn validate_device_read_limit(limit: usize) -> Result<(), TcpError> {
    if !(1..=4096).contains(&limit) {
        return Err(TcpError::new(
            "max_device_reads_per_status must be in 1..=4096",
        ));
    }
    Ok(())
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

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
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
    kind: io::ErrorKind,
    wait_unavailable: bool,
}
impl TcpError {
    fn new(message: impl Into<String>) -> Self {
        Self::with_kind(io::ErrorKind::Other, message)
    }
    /// An error with a typed class that survives the wire (`WireError`).
    pub(crate) fn with_kind(kind: io::ErrorKind, message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            kind,
            wait_unavailable: false,
        }
    }
    fn component(error: impl fmt::Display) -> Self {
        Self::new(error.to_string())
    }
    /// Typed class: local I/O kind, or the remote error code of the owner.
    pub fn kind(&self) -> io::ErrorKind {
        self.kind
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
        Self::with_kind(error.kind(), error.to_string())
    }
}
impl From<TcpError> for io::Error {
    fn from(error: TcpError) -> Self {
        io::Error::new(error.kind, error.message)
    }
}
/// Whether `error` is a transport break that a write resume may heal (W1): a
/// lost session or data queue whose writes can be re-issued as identical bytes.
///
/// Only transport classes qualify. A protocol error, a permission error and a
/// typed remote verdict are the owner's answer and must still fail-stop, as
/// must an error whose kind says the bytes may be on the device but unreadable
/// (`InvalidData`).
pub(crate) fn is_transient_break(error: &TcpError) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionRefused
            | io::ErrorKind::BrokenPipe
            | io::ErrorKind::NotConnected
            | io::ErrorKind::UnexpectedEof
            | io::ErrorKind::TimedOut
            | io::ErrorKind::HostUnreachable
            | io::ErrorKind::NetworkDown
    )
}
/// Whether an errno reported by the NVMe-oF queue is a lost queue or link
/// rather than a rejected command: the write may be retried at the same offset.
pub(crate) fn is_transient_errno(errno: i32) -> bool {
    [
        libc::ENODEV,
        libc::ENXIO,
        libc::ENOTCONN,
        libc::ECONNRESET,
        libc::EPIPE,
        libc::ETIMEDOUT,
        libc::EHOSTUNREACH,
        libc::ENETDOWN,
        libc::ENETRESET,
    ]
    .contains(&errno)
}
impl From<&str> for WireError {
    fn from(message: &str) -> Self {
        Self::other(message)
    }
}
impl From<String> for WireError {
    fn from(message: String) -> Self {
        Self::other(message)
    }
}
impl fmt::Display for WireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.message.fmt(f)
    }
}

/// `Response::Error` body: an explicit `u8` class and the message. Codes:
/// 0 Other, 1 InvalidInput, 2 InvalidData, 3 PermissionDenied, 4 NotFound,
/// 5 AlreadyExists, 6 WouldBlock, 7 TimedOut. Unknown codes decode as Other.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct WireError {
    pub code: u8,
    pub message: String,
}
impl WireError {
    pub(crate) fn other(message: impl Into<String>) -> Self {
        Self {
            code: 0,
            message: message.into(),
        }
    }
    fn code_of(kind: io::ErrorKind) -> u8 {
        match kind {
            io::ErrorKind::InvalidInput => 1,
            io::ErrorKind::InvalidData => 2,
            io::ErrorKind::PermissionDenied => 3,
            io::ErrorKind::NotFound => 4,
            io::ErrorKind::AlreadyExists => 5,
            io::ErrorKind::WouldBlock => 6,
            io::ErrorKind::TimedOut => 7,
            _ => 0,
        }
    }
    fn kind(&self) -> io::ErrorKind {
        match self.code {
            1 => io::ErrorKind::InvalidInput,
            2 => io::ErrorKind::InvalidData,
            3 => io::ErrorKind::PermissionDenied,
            4 => io::ErrorKind::NotFound,
            5 => io::ErrorKind::AlreadyExists,
            6 => io::ErrorKind::WouldBlock,
            7 => io::ErrorKind::TimedOut,
            _ => io::ErrorKind::Other,
        }
    }
}
impl From<&TcpError> for WireError {
    fn from(error: &TcpError) -> Self {
        Self {
            code: Self::code_of(error.kind),
            message: error.message.clone(),
        }
    }
}
impl From<TcpError> for WireError {
    fn from(error: TcpError) -> Self {
        Self {
            code: Self::code_of(error.kind),
            message: error.message,
        }
    }
}
impl From<WireError> for TcpError {
    fn from(error: WireError) -> Self {
        Self {
            wait_unavailable: error.message.starts_with(progress_wait::WAIT_UNAVAILABLE),
            ..Self::with_kind(error.kind(), error.message)
        }
    }
}

/// Status reply of either session scope. Encoded as an explicit `u8` scope
/// (`Host = 0`, `Database = 1`) followed by the scope's body, inside the
/// existing `Response::Status` tag. A Fence reply is always `Database`.
#[derive(Clone, Debug)]
pub(crate) enum StatusReply {
    Host(HostStatus),
    Database(NodeStatus),
}
impl Serialize for StatusReply {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeTuple;
        let mut tuple = serializer.serialize_tuple(2)?;
        match self {
            Self::Host(status) => {
                tuple.serialize_element(&0u8)?;
                tuple.serialize_element(status)?;
            }
            Self::Database(status) => {
                tuple.serialize_element(&1u8)?;
                tuple.serialize_element(status)?;
            }
        }
        tuple.end()
    }
}
impl<'de> Deserialize<'de> for StatusReply {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = StatusReply;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a u8 status scope followed by its status body")
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> Result<StatusReply, A::Error> {
                let scope: u8 = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(0, &self))?;
                match scope {
                    0 => seq
                        .next_element()?
                        .map(StatusReply::Host)
                        .ok_or_else(|| serde::de::Error::invalid_length(1, &self)),
                    1 => seq
                        .next_element()?
                        .map(StatusReply::Database)
                        .ok_or_else(|| serde::de::Error::invalid_length(1, &self)),
                    other => Err(serde::de::Error::custom(format!(
                        "unknown status scope {other}"
                    ))),
                }
            }
        }
        deserializer.deserialize_tuple(2, Visitor)
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
    /// Tag 1. The first Status binds a session to the host (`None`) or one database.
    Status {
        protocol: u32,
        database: Option<[u8; 16]>,
    },
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
    /// Tag 14. Resolve member frame addresses (a hybrid position's `address`)
    /// to owner-pinned file ranges. Keyed by address alone: the serving
    /// generation is `segment_generation`, never a batch epoch, because a
    /// relocated position keeps its logical epoch but names the generation
    /// that now holds its bytes. A retired member answers `NotFound`.
    ResolveLiveRecords {
        authority_epoch: u64,
        members: Vec<PhysicalValueAddress>,
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
    /// Tag 22. Benchmark evidence; owners built without Linux `test-utils`
    /// reply with an error.
    ReadEvidence { epoch: u64, evict: bool },
    /// Tag 23. Benchmark evidence; owners built without Linux `test-utils`
    /// reply with an error.
    ProcessResources { epoch: u64 },
    /// Tag 24.
    InventoryPage(generation_control::InventoryPageRequest),
    /// Tag 25.
    CheckTrimGeneration(generation_control::CheckTrimRequest),
    /// Tag 26.
    TrimGeneration(generation_control::TrimRequest),
    /// Tag 27. Read-only owner WAL file inventory for capacity accounting.
    FileInventoryPage {
        authority_generation: u64,
        source_generation: u64,
        after: Option<u64>,
        limit: u16,
    },
    /// Tag 28. R1 relocation scan: metadata of the complete batches of one
    /// stream in `[from, end)`, active or older sealed
    /// (`storage::RelocationPage`).
    RelocationPage {
        authority_generation: u64,
        source_generation: u64,
        from: u64,
        end: u64,
        limit: u16,
    },
    /// Tag 29. Host scope only; idempotent for an identical request.
    /// The authority may retry after an unknown outcome.
    ProvisionDatabase {
        database: [u8; 16],
        partition: PartitionConfig,
        shape_yaml: String,
        live_target: DatabaseLiveTarget,
    },
    /// Tag 30. Host scope only. `authority` is the bound trust principal and
    /// `generation` the coordinator's highest fencing reservation (0 if none).
    DropDatabase {
        database: [u8; 16],
        authority: [u8; 32],
        generation: u64,
    },
    /// Tag 31. Renew the coordinator lease of `authority_epoch` for `holder`.
    /// The reactor answers it inline, ahead of the control queue, so an
    /// exclusive fence, seal or trim cannot delay a renewal. `duration_ms` is
    /// clamped by the owner; the reply carries the granted value.
    RenewCoordinatorLease {
        authority_epoch: u64,
        holder: [u8; 16],
        duration_ms: u32,
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
            | Self::RelocationPage { .. }
            | Self::ProvisionDatabase { .. }
            | Self::DropDatabase { .. } => true,
            Self::Stats
            | Self::StatsDetailed
            | Self::PartitionStats
            | Self::ConnectionProfile { .. }
            | Self::OwnerTimelineArm { .. }
            | Self::OwnerTimelineFetch { .. }
            | Self::CheckpointPosition { .. }
            // A lease renewal is memory work on a lease thread: it must not
            // wait for a lifecycle-class budget, and the data timeout bounds
            // the request's own flight.
            | Self::RenewCoordinatorLease { .. } => false,
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
    Status(StatusReply),
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
    Error(WireError),
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
    /// Tag 13. Live epoch pin released.
    NvmeofEpochReleased,
    /// Tag 14.
    LiveRecords(LiveRecordLocations),
    /// Tag 15. `ReplayLiveWal` page.
    LiveRecovery(live_wire::LiveRecoveryWire),
    /// Tag 16.
    LiveTargetStats(super::nvmeof::LiveTargetStats),
    /// Tag 17.
    LiveLeased(LiveLeased),
    /// Tag 18.
    LiveAdopted(LiveAdopted),
    /// Tag 19.
    LiveExports(LiveExportInfo),
    /// Tag 20.
    ReadEvidence(crate::read_probe::ReadEvidence),
    /// Tag 21.
    ProcessResources(crate::read_probe::ProcessResources),
    /// Tag 22.
    InventoryPage(generation_control::InventoryPageReply),
    /// Tag 23.
    CheckTrimGeneration(generation_control::CheckTrimReply),
    /// Tag 24.
    TrimGeneration(generation_control::TrimReply),
    /// Tag 25. Boxed so ordinary responses do not grow.
    FileInventoryPage(Box<super::remote_db::file_inventory::FileInventoryReply>),
    /// Tag 26. Boxed like FileInventoryPage.
    RelocationPage(Box<super::storage::RelocationPage>),
    /// Tag 27.
    ProvisionDatabase {
        database: [u8; 16],
        partition: u32,
        durability_tier: super::durability::DurabilityTier,
    },
    /// Tag 28.
    DropDatabase { database: [u8; 16], generation: u64 },
    /// Tag 29. The clamped lease duration this owner granted.
    CoordinatorLease { granted_ms: u32 },
}

/// Named error of a benchmark evidence request this owner build cannot serve.
#[cfg(not(all(target_os = "linux", feature = "test-utils")))]
const EVIDENCE_UNAVAILABLE: &str = "benchmark evidence requests require test-utils on Linux";

#[cfg(test)]
mod wire_tag_tests;

/// One database's durable fencing authority within an [`OwnerHost`].
pub struct StorageNode {
    inventory: Arc<Mutex<NodeInventory>>,
    control: control::ControlOwner,
    control_context: Arc<control::ControlContext>,
    close_wake: WakeFd,
    #[cfg(test)]
    progress_wait_hook: Mutex<Option<Arc<progress_wait::WaitHook>>>,
    pub(super) live_nvme_selected: bool,
}
impl StorageNode {
    pub fn open(config: NodeConfig, metrics: Arc<Metrics>) -> Result<Arc<Self>, TcpError> {
        Self::open_inner(config, metrics, inventory::OpenMode::Prepare)
    }

    fn open_inner(
        config: NodeConfig,
        metrics: Arc<Metrics>,
        mode: inventory::OpenMode,
    ) -> Result<Arc<Self>, TcpError> {
        if let Some(error) = super::sys::route_unavailable() {
            return Err(TcpError::new(error));
        }
        config.validate_reads()?;
        let close_wake = WakeFd::for_progress("database close wake")?;
        let wal_member_read_policy = config.wal_member_read_policy;
        let max_device_reads_per_status = config.max_device_reads_per_status;
        let inventory = NodeInventory::open_with_mode(config, metrics, mode)?;
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
            closed: AtomicBool::new(false),
            terminal,
            poison_pending: Arc::new(AtomicBool::new(false)),
            persistence_failed,
            recoverable_live_failure: AtomicBool::new(false),
            poison_gate: Mutex::new(()),
            wal_member_read_policy,
            max_device_reads_per_status,
            transport: Arc::default(),
            live_control_gate: Mutex::new(()),
            coordinator_lease: Arc::new(coordinator_lease::CoordinatorLeaseCell::new()),
            #[cfg(test)]
            before_fence: Mutex::new(None),
            #[cfg(test)]
            before_open_live_exports: Mutex::new(None),
            #[cfg(test)]
            live_exports_served: AtomicUsize::new(0),
        });
        let control = control::ControlOwner::start(control_context.clone())?;
        Ok(Arc::new(Self {
            inventory,
            control,
            control_context,
            close_wake,
            #[cfg(test)]
            progress_wait_hook: Mutex::new(None),
            live_nvme_selected,
        }))
    }

    /// Rootless unit tests for one directly attached inventory (no host RPCs).
    #[cfg(test)]
    pub(crate) fn bench_open_live_fixture(
        config: NodeConfig,
        metrics: Arc<Metrics>,
        configfs: PathBuf,
    ) -> Result<Arc<Self>, TcpError> {
        inventory::with_live_target_fixture(configfs, || Self::open(config, metrics))
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

    /// The database this node serves (session binding in protocol tests).
    #[cfg(test)]
    pub(crate) fn database(&self) -> [u8; 16] {
        self.control_context.partition.inventory.database
    }

    /// Database drop: stop routing, close every session and invalidate read
    /// authority. Irreversible; the host drops the node once sessions exit.
    pub(crate) fn close_for_drop(&self, generation: u64) -> Result<(), TcpError> {
        let _gate = self.control_context.live_control_gate.lock();
        let _lifecycle = self.control_context.lifecycle.lock();
        let high_water = self.inventory.lock().highest_fenced_generation();
        if generation < high_water {
            return Err(TcpError::with_kind(
                io::ErrorKind::InvalidInput,
                format!("DropDatabase: stale fencing generation {generation} < {high_water}"),
            ));
        }
        self.control_context.closed.store(true, Ordering::Release);
        self.control_context.readers.write().invalidate();
        // Interrupt a parked DurableProgress request of this database.
        self.close_wake.signal().map_err(TcpError::component)
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.control_context.closed.load(Ordering::Acquire)
    }

    /// Highest fencing generation this owner ever recorded for the database
    /// (its fence high-water). Zero when never fenced.
    #[cfg(test)]
    pub(crate) fn fence_high_water(&self) -> Result<u64, TcpError> {
        let _lifecycle = self.control_context.lifecycle.lock();
        Ok(self.inventory.lock().highest_fenced_generation())
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

    /// Start this owner incarnation's ordered R1 evidence stream at
    /// `<case_output>/capacity/r1-events/owner_O<partition>/<incarnation>.jsonl`.
    /// Events cover live grants that roll files, durable seals, retirement
    /// steps and record/connection pins taken from now on. Not wire-visible.
    pub fn enable_r1_events(
        &self,
        config: &crate::distributed::r1_events::R1EventConfig,
    ) -> Result<(), TcpError> {
        NodeInventory::enable_r1_events(&self.inventory, config)
    }

    #[cfg(test)]
    pub(crate) fn config_for_test(&self) -> NodeConfig {
        self.inventory.lock().config.clone()
    }

    /// Write the terminal line and close this owner's stream. A process that
    /// never calls this leaves an incomplete (`.partial`) stream.
    pub fn finish_r1_events(&self) -> Result<(), TcpError> {
        let events = self
            .inventory
            .lock()
            .r1_events
            .take()
            .ok_or_else(|| TcpError::new("r1 event stream is not enabled"))?;
        events.log().finish().map_err(TcpError::component)
    }

    /// Test-only: make the retirement revoke step report one victim file as
    /// not revoked, so its R1 file carries no `target_io_drained` /
    /// `target_removed` (the victim without a binding case). The revoke itself
    /// still runs. Returns whether the file had a live binding.
    #[cfg(test)]
    pub(crate) fn skip_retirement_revoke_report_for_test(
        &self,
        generation: u64,
        file: u64,
    ) -> bool {
        self.inventory
            .lock()
            .skip_retirement_revoke_report_for_test(generation, file)
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

    /// Test-only: live export exchanges this owner began serving.
    #[cfg(test)]
    pub(crate) fn live_exports_served(&self) -> usize {
        // Relaxed, like the counter it reads: test evidence, not a barrier.
        self.control_context
            .live_exports_served
            .load(Ordering::Relaxed)
    }

    /// Test-only: owner-wide flush barriers this owner enqueued so far.
    #[cfg(test)]
    pub(crate) fn flush_barriers_sent(&self) -> u64 {
        self.control.flush_barriers_sent()
    }

    /// Test-only: park the next serving retirement between its revoke and its
    /// root. Returns `(entered, resume)`.
    #[cfg(test)]
    pub(crate) fn pause_retirement_after_revoke(&self) -> inventory::RevokeParkTest {
        self.inventory.lock().pause_after_revoke()
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

    /// Test-only: file indexes with an installed live binding for `epoch`.
    #[cfg(test)]
    pub(crate) fn live_binding_files(&self, epoch: u64) -> Vec<u64> {
        self.inventory.lock().live_binding_files(epoch)
    }

    /// Serve this one database directly: protocol tests only. Production
    /// owners serve through [`OwnerHost::serve`], which routes by database.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn serve(
        self: &Arc<Self>,
        listener: TcpListener,
        tls: TlsFiles,
        limits: TcpLimits,
        shutdown: Arc<AtomicBool>,
    ) -> Result<(), TcpError> {
        limits.validate()?;
        self.validate_limits(limits)?;
        let node = self.clone();
        listener::serve_listener(
            listener,
            tls,
            limits,
            shutdown,
            Arc::new(move |session, verifier, shutdown, wake| {
                reactor::serve_session(&node, session, limits, shutdown, wake, verifier, None)
            }),
        )
    }

    /// This database's geometry must fit the transport's frame bound and its
    /// largest Status reply.
    pub(crate) fn validate_limits(&self, limits: TcpLimits) -> Result<(), TcpError> {
        if self
            .inventory
            .lock()
            .config
            .partition
            .group_limits
            .max_encoded_bytes
            > limits.max_frame_bytes.saturating_sub(512)
        {
            return Err(TcpError::with_kind(
                io::ErrorKind::InvalidInput,
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
                body: Response::Status(StatusReply::Database(largest_status)),
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
                            durable_end: u64::MAX,
                        };
                        generation_control::page_capacity(limits.max_frame_bytes)
                    ],
                    next: Some(u64::MAX),
                }),
            })
            .map_err(TcpError::component)?;
        Ok(())
    }

    #[cfg(test)]
    fn handle(&self, request: Request, limits: TcpLimits) -> Result<Response, TcpError> {
        self.control_context.handle_profiled(request, limits, None)
    }
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
    /// The authenticated route of this session, kept so the read lane can
    /// rebuild an aborted session on the same owner. It carries no pin state:
    /// the lifetime-export model removed the export pins.
    route: connect_shared::ReconnectRoute,
    live_config: Option<LiveClientConfig>,
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
    progress_wait: Option<progress_wait::ClientProgressWait>,
    /// The coordinator's lease on this owner, when this client is a write
    /// worker's client. Leased writes check it before every submission.
    lease_validity: Option<Arc<super::remote_db::coordinator_lease::LeaseValidity>>,
    /// W1: resume a leased write after a transient transport break. The
    /// database config decides; a client that is not a write worker leaves it
    /// false, so a read or control client never rebuilds itself in place.
    resume_writes: bool,
    /// The database's one DMA reaper. A resume that detaches a dead queue hands
    /// it here instead of draining it on the worker thread: `LiveIo::shutdown`
    /// is unbounded and the kernel keep-alive can hold a lost command.
    /// Capacity: one queue per owner worker; a full reaper fails the resume.
    resume_reaper: Option<std::sync::mpsc::SyncSender<live_client::LiveWriteQueue>>,
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
        connect_shared::connect(
            endpoint,
            tls,
            client_config,
            expected,
            shape,
            connect_shared::SessionSetup {
                deadline: Instant::now() + timeout,
                timeout,
                progress: None,
            },
        )
    }

    pub(crate) fn connect_progress_shared(
        endpoint: &str,
        tls: &TlsFiles,
        client_config: ClientTlsConfig,
        timeout: Duration,
        expected: PartitionConfig,
        shape: &KeyShape,
        stop: Arc<WakeFd>,
    ) -> Result<Self, TcpError> {
        connect_shared::connect(
            endpoint,
            tls,
            client_config,
            expected,
            shape,
            connect_shared::SessionSetup {
                deadline: Instant::now() + timeout,
                timeout,
                progress: Some(stop),
            },
        )
    }

    /// Authenticated status captured during connect; not a fresh health probe.
    #[allow(
        clippy::expect_used,
        reason = "connect stores the authenticated status before the client is returned"
    )]
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
    #[allow(
        clippy::expect_used,
        reason = "the named suite is TLS 1.3 by definition"
    )]
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
        match self.request(Request::Status {
            protocol: PROTOCOL,
            database: Some(self.expected.inventory.database),
        })? {
            Response::Status(StatusReply::Database(status)) => self.check_status("Status", status),
            response => self.wrong_variant("Status", &response, "Status(Database)"),
        }
    }
    pub fn fence_to(&mut self, epoch: u64) -> Result<NodeStatus, TcpError> {
        match self.request(Request::Fence { epoch })? {
            Response::Status(StatusReply::Database(status))
                if status.active_epoch == Some(epoch) =>
            {
                self.check_status("Fence", status)
            }
            Response::Status(StatusReply::Database(status)) => self.wrong_response(
                "Fence",
                format_args!("active_epoch={:?}, expected {epoch}", status.active_epoch),
            ),
            response => self.wrong_variant("Fence", &response, "Status(Database)"),
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
    #[allow(
        clippy::expect_used,
        reason = "the hasher is configured for a 32-byte digest just above"
    )]
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
            return Ok(ClientTlsConfig { config: None });
        }
        let (roots, cert, key, _identity) = self.material()?;
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
