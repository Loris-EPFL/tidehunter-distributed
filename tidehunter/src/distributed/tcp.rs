//! Static RF1 storage transport with authenticated TLS and explicit trusted fabric modes.
//!
//! The Tidehunter paper's complete-batch replay and values-in-WAL rules remain
//! the authority boundary. Transport is not a global log or consensus service.
//! TLS modes require a dedicated CA and validate both peers with rustls.
//! Trusted native modes omit application authentication and encryption; deployment
//! isolation is required. WAL integrity, fencing and durability checks remain.

pub mod checkpoint;
mod reactor;
mod read_executor;
pub use read_executor::ReadExecutorStats;
mod diagnostics;
mod fabric;
pub use fabric::{Transport, TransportProgressStats};
mod control;
mod inventory;
mod resident;
#[cfg(feature = "experimental-fabric")]
use resident::encode_envelope;
use resident::{RemoteRegion, ResidentGrant};
pub use resident::{ResidentReadConfig, ResidentReadStats};
mod payload;
pub use diagnostics::{ConnectionProfile, PollingProfile, SocketProfile, WorkProfile};
use payload::WireBytes;
#[cfg(test)]
mod probe;
#[cfg(test)]
mod tests;

use super::codec::{BatchLimits, CompleteBatch, Mutation};
use super::group::{CompleteGroup, GROUP_HEADER_LEN, GroupLimits};
use super::storage::{AppendedBatch, DurableBatch, PartitionConfig, PartitionStats, StorageError};
use super::types::{
    BatchId, BatchVersion, PayloadDigest, PhysicalValueAddress, RequestAuthority,
    ValueRecordReference,
};
use crate::key_shape::KeyShape;
use crate::metrics::Metrics;
use bincode::Options;
use parking_lot::Mutex;
use rayon::prelude::*;
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
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

pub use inventory::NodeInventory;
const PROTOCOL: u32 = 10;
pub(crate) const HARD_FRAME_LIMIT: usize = 64 * 1024 * 1024;
static NEXT_RESIDENT_SESSION: AtomicU64 = AtomicU64::new(1);
#[cfg(feature = "experimental-fabric")]
static NEXT_RESIDENT_GRANT: AtomicU64 = AtomicU64::new(1);
const MODE_MAGIC: [u8; 4] = *b"THDP";

fn transport_mode_id(transport: Transport) -> u8 {
    match transport {
        Transport::Tcp => 1,
        Transport::Efa => 2,
        Transport::EfaDirect => 3,
        Transport::FabricTcp => 4,
        Transport::EfaNative => 5,
        Transport::FabricTcpNative => 6,
    }
}

fn write_transport_mode(stream: &mut (impl Read + Write), transport: Transport) -> io::Result<()> {
    let mut preface = [0; 8];
    preface[..4].copy_from_slice(&MODE_MAGIC);
    preface[4..6].copy_from_slice(&(PROTOCOL as u16).to_be_bytes());
    preface[6] = transport_mode_id(transport);
    stream.write_all(&preface)?;
    stream.flush()
}

fn read_transport_mode(stream: &mut impl Read, expected: Transport) -> Result<(), TcpError> {
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
pub struct NodeConfig {
    pub root: PathBuf,
    /// Full static cluster inventory, this node's partition; template epoch = 1.
    pub partition: PartitionConfig,
    pub shape_yaml: String,
    pub max_epochs: usize,
    /// Bounded workers for cold authority planning and explicit mapped/positional
    /// diagnostics. Append/control persistence has its own per-log owner.
    #[serde(default = "default_read_workers")]
    pub read_workers: usize,
    #[serde(default = "default_async_read_policy")]
    pub wal_member_read: super::storage::WalMemberReadPolicy,
    /// Opt-in NOWAIT prefix reads; compare with forced-async on the target host.
    #[serde(default)]
    pub read_resident_first: bool,
    /// Node-wide evictable encoded-member cache; clients still verify integrity.
    #[serde(default)]
    pub read_cache_bytes: usize,
    /// Host memory reserved for this node's cache plus bounded inflight reads.
    /// Excludes index, application, transport/kernel and page-cache reserves.
    #[serde(default = "default_read_memory_allowance_bytes")]
    pub read_memory_allowance_bytes: usize,
    #[serde(default = "default_read_requests")]
    pub read_max_inflight_requests: usize,
    #[serde(default = "default_read_bytes")]
    pub read_max_inflight_bytes: usize,
    #[serde(default = "default_device_reads")]
    pub read_max_device_reads: usize,
    /// Process-wide source/destination registration budgets for resident RMA.
    /// Disabled by default; ordinary RPC behavior is unchanged.
    #[serde(default)]
    pub resident_reads: ResidentReadConfig,
}
impl NodeConfig {
    /// Requested fixed slot-table bytes before allocation; included in cache budget.
    pub fn read_cache_metadata_bytes(&self) -> usize {
        read_executor::ReadExecutorConfig {
            cache_bytes: self.read_cache_bytes,
            ..read_executor::ReadExecutorConfig::default()
        }
        .cache_metadata_bytes()
    }
}

fn default_read_workers() -> usize {
    2
}
fn default_async_read_policy() -> super::storage::WalMemberReadPolicy {
    super::storage::WalMemberReadPolicy::Async
}
fn default_read_requests() -> usize {
    32
}
fn default_read_bytes() -> usize {
    64 * 1024 * 1024
}
fn default_device_reads() -> usize {
    128
}
pub(crate) fn default_read_memory_allowance_bytes() -> usize {
    512 * 1024 * 1024
}
/// TLS credentials and transport selection. Trusted native modes require empty
/// credential paths and do not authenticate peers; TLS modes require a dedicated CA.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TlsFiles {
    #[serde(default)]
    pub transport: Transport,
    #[serde(default)]
    pub ca: PathBuf,
    #[serde(default)]
    pub cert: PathBuf,
    #[serde(default)]
    pub key: PathBuf,
    /// Expected server DNS name; ignored by the listener and trusted native modes.
    #[serde(default)]
    pub server_name: String,
    /// Explicit opt-in for one-sided reads on this client/session transport.
    #[serde(default)]
    pub resident_reads: ResidentReadConfig,
}

/// Network-owner scheduling only, not a whole-system lock-free guarantee.
/// Adaptive waits for socket readiness only when a TCP owner is fully drained;
/// arrivals wake it without an idle-sleep floor. Active TCP yields; fabric keeps
/// bounded spin/yield CQ progress even while idle (no TCP readiness dependency).
/// Busy keeps bounded sweeps and all shutdown, deadline and peer checks, but
/// never explicitly sleeps/yields while driving the established connection.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PollingPolicy {
    #[default]
    Adaptive,
    Busy,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct TcpLimits {
    pub max_frame_bytes: usize,
    pub max_connections: usize,
    pub max_reads: usize,
    pub recovery_page_batches: usize,
    /// Handshake, partial-frame, and accepted-request timeout; established
    /// authenticated sessions may remain idle without expiring.
    pub io_timeout: Duration,
    #[serde(default)]
    pub polling: PollingPolicy,
}
impl Default for TcpLimits {
    fn default() -> Self {
        Self {
            max_frame_bytes: 16 * 1024 * 1024,
            max_connections: 16,
            max_reads: 256,
            recovery_page_batches: 128,
            io_timeout: Duration::from_secs(10),
            polling: PollingPolicy::Adaptive,
        }
    }
}
impl TcpLimits {
    fn validate(self) -> Result<(), TcpError> {
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

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NodeEpoch {
    pub epoch: u64,
    pub sealed: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeStatus {
    pub partition: PartitionConfig,
    pub shape_yaml: String,
    pub max_epochs: usize,
    pub epochs: Vec<NodeEpoch>,
    pub active_epoch: Option<u64>,
    /// Negotiated bound for each encoded request/response frame.
    pub max_frame_bytes: usize,
    pub max_reads: usize,
    /// Node-wide retained-read budget, used to split aggregate RPCs.
    pub max_read_bytes: usize,
    pub recovery_page_batches: usize,
    pub polling: PollingPolicy,
}
pub struct RecoveryPage {
    pub batches: Vec<(CompleteBatch, DurableBatch)>,
    pub next: Option<usize>,
}

/// Explicit diagnostic timings; ordinary requests take no extra stage clocks.
/// WAL-read exchange spans encoding through authenticated, verified completion
/// (including interleaved progress), not wire-only RTT. Encode/decode/verification
/// and server intervals are nested and must not be added to exchange. Legacy
/// send/header/body clocks are zero: partial multiplexed I/O cannot be attributed
/// to one request. Socket clocks are collected at connection scope only.
/// Unlike WAL reads, control/RAM exchange starts after queueing and also waits for
/// local request-output retirement. RAM payload verification follows that exchange.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct TcpReadProfile {
    pub request_encode_ns: u64,
    pub exchange_ns: u64,
    /// Not attributable per request under multiplexing; zero in protocol v2.
    pub request_send_ns: u64,
    pub response_header_ns: u64,
    pub response_body_ns: u64,
    pub socket: SocketProfile,
    pub response_decode_ns: u64,
    pub record_verify_ns: u64,
    /// RAM probe payload verification after exchange, not nested inside it.
    pub ram_payload_verify_ns: u64,
    pub ram_payload_verify_calls: u64,
    /// Bytes whose full deterministic payload check succeeded.
    pub ram_payload_verified_bytes: u64,
    pub requests: u64,
    /// True when the response carried server handler clocks.
    pub server_handler_available: bool,
    /// True when at least one sample used RMA and had no server handler.
    pub server_handler_not_applicable: bool,
    /// Read-handler residence through completed records, excluding the final recheck.
    pub server_handler_ns: u64,
    /// No append/control inventory wait; retained as a zero-valued compatibility
    /// field after reader authority moved to its separate short-lived lock.
    pub server_inventory_wait_ns: u64,
    /// Active-epoch validation and Arc log-handle pinning under the reader lock.
    pub server_authority_lookup_ns: u64,
    /// Exact member validation through completed range data/response construction,
    /// outside reader authority. Async includes ring-admission and kernel I/O
    /// residence; mapped/positional measures their diagnostic read/copy path.
    pub server_record_read_ns: u64,
    /// Completed response to reader-authority recheck, including scheduler delay.
    pub server_response_recheck_wait_ns: u64,
    /// Active-authority recheck before TLS enqueueing.
    pub server_response_recheck_ns: u64,
    /// Successful profiled read replies carrying the above spans.
    pub server_response_recheck_samples: u64,
    /// Retained as a zero-valued compatibility field; authority rechecks do not defer.
    pub server_response_recheck_deferrals: u64,
}

impl TcpReadProfile {
    pub(crate) fn add(&mut self, next: Self) {
        self.request_encode_ns += next.request_encode_ns;
        self.exchange_ns += next.exchange_ns;
        self.request_send_ns += next.request_send_ns;
        self.response_header_ns += next.response_header_ns;
        self.response_body_ns += next.response_body_ns;
        self.socket.add(next.socket);
        self.response_decode_ns += next.response_decode_ns;
        self.record_verify_ns += next.record_verify_ns;
        self.ram_payload_verify_ns += next.ram_payload_verify_ns;
        self.ram_payload_verify_calls += next.ram_payload_verify_calls;
        self.ram_payload_verified_bytes += next.ram_payload_verified_bytes;
        self.requests += next.requests;
        self.server_handler_available |= next.server_handler_available;
        self.server_handler_not_applicable |= next.server_handler_not_applicable;
        self.server_handler_ns += next.server_handler_ns;
        self.server_inventory_wait_ns += next.server_inventory_wait_ns;
        self.server_authority_lookup_ns += next.server_authority_lookup_ns;
        self.server_record_read_ns += next.server_record_read_ns;
        self.server_response_recheck_wait_ns += next.server_response_recheck_wait_ns;
        self.server_response_recheck_ns += next.server_response_recheck_ns;
        self.server_response_recheck_samples += next.server_response_recheck_samples;
        self.server_response_recheck_deferrals += next.server_response_recheck_deferrals;
    }
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
struct ServerReadProfile {
    handler_ns: u64,
    inventory_wait_ns: u64,
    authority_lookup_ns: u64,
    record_read_ns: u64,
    response_recheck_wait_ns: u64,
    response_recheck_ns: u64,
    response_recheck_samples: u64,
    response_recheck_deferrals: u64,
}
// Fixed-integer bincode encodes eight u64 values. Reserve this additional space
// in both server response accounting and client profiled-read chunking.
const SERVER_READ_PROFILE_BYTES: usize = 8 * 8;

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

/// Advisory cache admission only; both classes validate and return identical data.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ReadClass {
    Point,
    ScanNoFill,
}

#[derive(Serialize, Deserialize)]
enum Request {
    Checkpoint {
        authority_epoch: u64,
        epoch: u64,
        prefix: usize,
    },
    RetirementFiles {
        authority_epoch: u64,
        epoch: u64,
        after: Option<u64>,
        limit: usize,
    },
    RetireFiles {
        authority_epoch: u64,
        epoch: u64,
        prefix: usize,
        files: Vec<u64>,
    },
    ReadIoStats,
    RevokeGrants {
        generations: Vec<u64>,
    },
    Status {
        protocol: u32,
    },
    Fence {
        epoch: u64,
    },
    Append {
        epoch: u64,
        batches: Vec<WireBytes>,
    },
    Read {
        epoch: u64,
        references: Vec<ValueRecordReference>,
        class: ReadClass,
        grant_limit: usize,
        grant_max_bytes: usize,
    },
    Recover {
        epoch: u64,
        cursor: usize,
        limit: usize,
    },
    Seal {
        epoch: u64,
    },
    Stats,
    ReadProfiled {
        epoch: u64,
        references: Vec<ValueRecordReference>,
        class: ReadClass,
        grant_limit: usize,
        grant_max_bytes: usize,
    },
    StatsDetailed,
    /// Connection-local diagnostics, explicit and disabled by default.
    ConnectionProfile {
        enabled: bool,
        reset: bool,
    },
    /// Nonblocking operational snapshot; not a durability barrier or evidence cut.
    OperationalReadIoStats,
    #[cfg(all(target_os = "linux", feature = "test-utils"))]
    ReadEvidence {
        epoch: u64,
        evict: bool,
    },
    #[cfg(all(target_os = "linux", feature = "test-utils"))]
    ProcessResources {
        epoch: u64,
    },
    #[cfg(feature = "test-utils")]
    PrepareRamProbe {
        value_bytes: usize,
    },
    #[cfg(feature = "test-utils")]
    ReadRamProbe,
}
impl Request {
    fn is_measurement_control(&self) -> bool {
        match self {
            Self::Stats
            | Self::StatsDetailed
            | Self::ReadIoStats
            | Self::OperationalReadIoStats
            | Self::ConnectionProfile { .. } => true,
            #[cfg(all(target_os = "linux", feature = "test-utils"))]
            Self::ReadEvidence { .. } => true,
            #[cfg(all(target_os = "linux", feature = "test-utils"))]
            Self::ProcessResources { .. } => true,
            _ => false,
        }
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
#[derive(Serialize, Deserialize)]
enum Response {
    Checkpoint(checkpoint::CheckpointProof),
    RetirementFiles(Vec<u64>),
    Retired,
    ReadIoStats(ReadExecutorStats),
    Status(NodeStatus),
    Appended(Vec<Receipt>),
    Records(Vec<WireBytes>),
    ResidentRecords {
        grants: Vec<Option<ResidentGrant>>,
        records: Vec<WireBytes>,
    },
    ResidentRecordsProfiled {
        grants: Vec<Option<ResidentGrant>>,
        records: Vec<WireBytes>,
        profile: ServerReadProfile,
    },
    GrantsRevoked(Vec<u64>),
    ResidentPoison {
        owner_boot: [u8; 16],
        session_id: u64,
        generations: Vec<u64>,
    },
    Recovery {
        batches: Vec<(WireBytes, Receipt)>,
        next: Option<usize>,
    },
    Sealed,
    Stats {
        groups: u64,
        barriers: u64,
        inventory_witnesses: u64,
        records: usize,
        metadata: usize,
    },
    Error(String),
    RecordsProfiled {
        records: Vec<WireBytes>,
        profile: ServerReadProfile,
    },
    StatsDetailed {
        groups: u64,
        barriers: u64,
        namespace_syncs: u64,
        inventory_witnesses: u64,
        records: usize,
        metadata: usize,
    },
    // Diagnostics must not enlarge every ordinary read/write response value.
    ConnectionProfile(Box<ConnectionProfile>),
    OperationalReadIoStats(Box<(PartitionConfig, Option<ReadExecutorStats>)>),
    #[cfg(all(target_os = "linux", feature = "test-utils"))]
    ReadEvidence(crate::read_probe::ReadEvidence),
    #[cfg(all(target_os = "linux", feature = "test-utils"))]
    ProcessResources(crate::read_probe::ProcessResources),
    #[cfg(feature = "test-utils")]
    RamProbePrepared,
    #[cfg(feature = "test-utils")]
    RamProbe(WireBytes),
}

#[cfg(test)]
type ReadHook = Arc<dyn Fn(&ValueRecordReference) + Send + Sync>;

/// One partition's durable epoch authority. Writes/fences serialize at the node;
/// read requests retain Arc log handles and perform disk I/O outside this mutex.
pub struct StorageNode {
    inventory: Arc<Mutex<NodeInventory>>,
    readers: Arc<parking_lot::RwLock<inventory::ReaderAuthority>>,
    read_pool: rayon::ThreadPool,
    control: control::ControlOwner,
    control_context: Arc<control::ControlContext>,
    wal_member_read: super::storage::WalMemberReadPolicy,
    read_config: read_executor::ReadExecutorConfig,
    read_admission: Arc<read_executor::ReadAdmission>,
    resident_grants: Arc<resident::GrantRegistry>,
    owner_boot: [u8; 16],
    resident_reads: ResidentReadConfig,
    terminal: Arc<AtomicBool>,
    #[cfg(any(test, feature = "test-utils"))]
    drop_append_response: AtomicBool,
    #[cfg(test)]
    panic_after_append: Arc<AtomicBool>,
    #[cfg(test)]
    read_hook: Mutex<Option<ReadHook>>,
}
impl StorageNode {
    pub fn open(config: NodeConfig, metrics: Arc<Metrics>) -> Result<Arc<Self>, TcpError> {
        if !(1..=8).contains(&config.read_workers) {
            return Err(TcpError::new("read_workers must be in 1..=8"));
        }
        if (config.read_cache_bytes != 0
            || config.read_resident_first
            || config.resident_reads.source_enabled())
            && config.wal_member_read != super::storage::WalMemberReadPolicy::Async
        {
            return Err(TcpError::new(
                "resident grants, shard cache and resident-first reads require async reads",
            ));
        }
        let read_pool = rayon::ThreadPoolBuilder::new()
            .num_threads(config.read_workers)
            .thread_name(|i| format!("wal-record-read-{i}"))
            .build()
            .map_err(TcpError::component)?;
        let wal_member_read = config.wal_member_read;
        let read_config = read_executor::ReadExecutorConfig {
            cache_bytes: config.read_cache_bytes,
            memory_allowance_bytes: config.read_memory_allowance_bytes,
            max_requests: config.read_max_inflight_requests,
            max_bytes: config.read_max_inflight_bytes,
            max_device_reads: config.read_max_device_reads,
            resident_first: config.read_resident_first,
        };
        let read_admission =
            Arc::new(read_executor::ReadAdmission::new(read_config).map_err(TcpError::component)?);
        let resident_reads = config.resident_reads;
        let inventory = NodeInventory::open(config, metrics)?;
        let readers = inventory.readers.clone();
        let terminal = inventory.terminal.clone();
        let append_shape = inventory.shape.clone();
        let append_batch_limits = inventory.config.partition.batch_limits;
        let append_group_limits = inventory.config.partition.group_limits;
        let partition = inventory.config.partition.clone();
        let inventory = Arc::new(Mutex::new(inventory));
        let resident_grants = Arc::new(resident::GrantRegistry::new(
            resident_reads.source_bytes,
            resident_reads.source_regions,
        ));
        let owner_boot = rand::random::<[u8; 16]>();
        #[cfg(test)]
        let panic_after_append = Arc::new(AtomicBool::new(false));
        let control_context = Arc::new(control::ControlContext {
            partition,
            inventory: inventory.clone(),
            readers: readers.clone(),
            terminal: terminal.clone(),
            append_shape,
            append_batch_limits,
            append_group_limits,
            wal_member_read,
            read_config,
            read_admission: read_admission.clone(),
            resident_grants: resident_grants.clone(),
            #[cfg(test)]
            panic_after_append: panic_after_append.clone(),
        });
        let control = control::ControlOwner::start(control_context.clone())?;
        Ok(Arc::new(Self {
            inventory,
            readers,
            control,
            control_context,
            terminal,
            read_pool,
            wal_member_read,
            read_config,
            read_admission,
            #[cfg(any(test, feature = "test-utils"))]
            drop_append_response: AtomicBool::new(false),
            #[cfg(test)]
            panic_after_append,
            #[cfg(test)]
            read_hook: Mutex::new(None),
            resident_reads,
            resident_grants,
            owner_boot,
        }))
    }
    pub fn status(&self) -> Result<NodeStatus, TcpError> {
        self.inventory.lock().status()
    }
    fn reader_snapshot(
        &self,
        epoch: u64,
        references: &[ValueRecordReference],
    ) -> Result<inventory::ReaderSnapshot, TcpError> {
        if self.resident_grants.poison_requested() {
            return Err(TcpError::new("reader authority is poison-pending"));
        }
        self.readers
            .read()
            .snapshot(epoch, references, &self.terminal)
    }
    pub(super) fn reader_authority_valid(&self, epoch: u64, generation: Option<u64>) -> bool {
        if self.resident_grants.poison_requested() {
            return false;
        }
        let readers = self.readers.read();
        if let Some(generation) = generation {
            readers.snapshot_valid(epoch, generation, &self.terminal)
        } else {
            readers.active_valid(epoch, &self.terminal)
        }
    }
    pub(super) fn finish_poison_if_quiescent(&self) {
        self.control_context.finish_poison_if_quiescent();
    }
    pub(super) fn poison_readers(&self) {
        self.control_context.poison_readers();
    }
    /// Explicit correctness-harness hook, unavailable in ordinary builds.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn inject_drop_next_append_response(&self) {
        self.drop_append_response.store(true, Ordering::Release);
    }
    #[cfg(test)]
    pub fn inject_panic_after_next_append(&self) {
        self.panic_after_append.store(true, Ordering::Release);
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
        let mut largest_status = self.status()?;
        largest_status.active_epoch = Some(u64::MAX);
        largest_status.epochs.resize(
            largest_status.max_epochs,
            NodeEpoch {
                epoch: 1,
                sealed: true,
            },
        );
        wire_codec(limits.max_frame_bytes)
            .serialized_size(&reactor::Envelope {
                protocol: PROTOCOL,
                id: u64::MAX,
                body: Response::Status(largest_status),
            })
            .map_err(TcpError::component)?;
        tls.transport.validate()?;
        if self.inventory.lock().config.resident_reads.source_enabled()
            && !tls.transport.is_native()
        {
            return Err(TcpError::new(
                "resident source grants require the explicitly trusted native transport",
            ));
        }
        let transport = tls.transport;
        let server = tls.server_config()?;
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
                                    let _ = node.connection(
                                        socket,
                                        server.clone(),
                                        limits,
                                        &shutdown,
                                        transport,
                                    );
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
        transport: Transport,
    ) -> Result<(), TcpError> {
        #[cfg(feature = "experimental-fabric")]
        let source_resident_reads = {
            self.inventory
                .lock()
                .config
                .resident_reads
                .source_per_connection(limits.max_connections)
        };
        let mut io = DeadlineIo::new(socket, limits.io_timeout)?;
        if transport.is_native() {
            read_transport_mode(&mut io, transport)?;
            #[cfg(feature = "experimental-fabric")]
            {
                let endpoint = fabric::bootstrap(&mut io, transport, source_resident_reads)?;
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
            return Err(TcpError::new("native transport was not compiled"));
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
        if conn.alpn_protocol() != Some(transport.alpn().as_slice())
            && conn.alpn_protocol() != Some(Transport::Tcp.alpn().as_slice())
        {
            return Err(TcpError::new("unsupported authenticated protocol"));
        }
        let selected_fabric = transport != Transport::Tcp
            && conn.alpn_protocol() == Some(transport.alpn().as_slice());
        let stream = StreamOwned::new(conn, io);
        #[cfg(feature = "experimental-fabric")]
        let mut stream = stream;
        if selected_fabric {
            #[cfg(feature = "experimental-fabric")]
            {
                let endpoint = fabric::bootstrap(&mut stream, transport, source_resident_reads)?;
                stream.sock.fabric = Some(endpoint);
            }
            #[cfg(not(feature = "experimental-fabric"))]
            return Err(TcpError::new("fabric transport was not compiled"));
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
        self.handle_profiled(request, limits, None)
    }
    fn handle_caught(
        &self,
        request: Request,
        limits: TcpLimits,
        profile: Option<&mut ConnectionProfile>,
    ) -> Result<Response, TcpError> {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.handle_profiled(request, limits, profile)
        }))
        .unwrap_or_else(|_| {
            self.poison_readers();
            Err(TcpError::new(
                "request worker panicked; authority must be reopened",
            ))
        })
    }

    fn handle_profiled(
        &self,
        request: Request,
        limits: TcpLimits,
        profile: Option<&mut ConnectionProfile>,
    ) -> Result<Response, TcpError> {
        match request {
            Request::Read {
                epoch, references, ..
            } => self.read_response::<false>(epoch, references, limits),
            Request::ReadProfiled {
                epoch, references, ..
            } => self.read_response::<true>(epoch, references, limits),
            request => self
                .control_context
                .handle_profiled(request, limits, profile),
        }
    }

    fn effective_max_reads(&self, limits: TcpLimits) -> usize {
        self.control_context.effective_max_reads(limits)
    }

    /// Both wire variants share every authority, membership and bounds check.
    /// Const specialization keeps ordinary reads free of diagnostic clocks.
    fn read_response<const PROFILE: bool>(
        &self,
        epoch: u64,
        references: Vec<ValueRecordReference>,
        limits: TcpLimits,
    ) -> Result<Response, TcpError> {
        let started = PROFILE.then(Instant::now);
        if references.is_empty() || references.len() > limits.max_reads {
            return Err(TcpError::new("read request count exceeds bound"));
        }
        let mut profile = ServerReadProfile::default();
        let began = PROFILE.then(Instant::now);
        let snapshot = self.reader_snapshot(epoch, &references)?;
        let generation = snapshot.generation;
        // This diagnostic handler runs off the network owner. Snapshot capture
        // itself only pins epoch/log identity under the short authority lock.
        for (log, reference) in &snapshot.members {
            log.validate_durable_record_reference(reference)
                .map_err(TcpError::component)?;
        }
        let logs = snapshot
            .members
            .into_iter()
            .map(|(log, _)| log)
            .collect::<Vec<_>>();
        if let Some(began) = began {
            profile.authority_lookup_ns = elapsed_ns(began);
        }
        let began = PROFILE.then(Instant::now);
        let mut size = 16usize
            + if PROFILE {
                SERVER_READ_PROFILE_BYTES
            } else {
                0
            };
        for reference in &references {
            size = size
                .checked_add(reference.descriptor.len as usize + 8)
                .ok_or_else(|| TcpError::new("read response overflow"))?;
            if size > limits.max_frame_bytes {
                return Err(TcpError::new("read response exceeds frame bound"));
            }
        }
        // Explicit mapped/positional diagnostic controls only. Session count
        // and byte credits bound queued work; indexed collection preserves order.
        let records = self.read_pool.install(|| {
            logs.par_iter()
                .zip(references.par_iter())
                .map(|(log, reference)| {
                    #[cfg(test)]
                    {
                        let hook = self.read_hook.lock().clone();
                        if let Some(hook) = hook {
                            hook(reference);
                        }
                    }
                    let result =
                        log.read_record_encoded_with_policy(reference, self.wal_member_read);
                    self.read_admission
                        .record_sync_read(reference.descriptor.len as usize, result.is_ok());
                    result.map(Into::into).map_err(TcpError::component)
                })
                .collect::<Result<Vec<WireBytes>, TcpError>>()
        })?;
        if !self.reader_authority_valid(epoch, Some(generation)) {
            return Err(TcpError::new("read authority was fenced or poisoned"));
        }
        if let (Some(began), Some(started)) = (began, started) {
            profile.record_read_ns = elapsed_ns(began);
            profile.handler_ns = elapsed_ns(started);
            Ok(Response::RecordsProfiled { records, profile })
        } else {
            Ok(Response::Records(records))
        }
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
    pub verifier: Option<Arc<rustls::client::WebPkiServerVerifier>>,
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
    max_read_bytes: usize,
    page_limit: usize,
    diagnostics: Option<TcpReadProfile>,
    reactor: reactor::ClientState,
    connected_status: Option<NodeStatus>,
    connect_profile: ClientConnectProfile,
    resident_reads: ResidentReadConfig,
    reuse_verifier: Option<Arc<rustls::client::WebPkiServerVerifier>>,
    server_name: Option<ServerName<'static>>,
    polling: PollingPolicy,
    #[cfg(feature = "test-utils")]
    ram_probe_bytes: Option<usize>,
}
impl Client {
    /// The node's authenticated policy, adopted by ordinary read/control drivers.
    pub fn polling_policy(&self) -> PollingPolicy {
        self.polling
    }

    /// Prime/replace one session-local deterministic diagnostic buffer. No WAL
    /// state is read or changed. Bytes are `i % 251`, length 0..=64 KiB, also
    /// bounded by the negotiated response frame. Not available in normal builds.
    #[cfg(feature = "test-utils")]
    pub fn prepare_ram_probe(&mut self, value_bytes: usize) -> Result<(), TcpError> {
        validate_ram_probe_size(value_bytes, self.frame_limit)?;
        match self.request(Request::PrepareRamProbe { value_bytes })? {
            Response::RamProbePrepared => {
                self.ram_probe_bytes = Some(value_bytes);
                Ok(())
            }
            _ => self.wrong_response(),
        }
    }

    /// Read through real authenticated framing/TLS/transport progress and verify
    /// every returned byte before transferring the owned response to the caller.
    #[cfg(feature = "test-utils")]
    pub fn read_ram_probe(&mut self) -> Result<minibytes::Bytes, TcpError> {
        let response = self.request(Request::ReadRamProbe)?;
        let began = self.diagnostics.as_ref().map(|_| Instant::now());
        let valid = match &response {
            Response::RamProbe(bytes) => {
                Some(bytes.as_slice().len()) == self.ram_probe_bytes
                    && bytes
                        .as_slice()
                        .iter()
                        .enumerate()
                        .all(|(i, &b)| b == (i % 251) as u8)
            }
            _ => false,
        };
        if let Some(profile) = &mut self.diagnostics {
            profile.ram_payload_verify_ns += began.map_or(0, elapsed_ns);
            profile.ram_payload_verify_calls += 1;
        }
        match response {
            Response::RamProbe(bytes) if valid => {
                if let Some(profile) = &mut self.diagnostics {
                    profile.ram_payload_verified_bytes += bytes.as_slice().len() as u64;
                }
                Ok(bytes.into_bytes())
            }
            _ => self.wrong_response(),
        }
    }

    /// Node-wide bounded cache and actual WAL submission evidence.
    pub fn read_io_stats(&mut self) -> Result<ReadExecutorStats, TcpError> {
        match self.request(Request::ReadIoStats)? {
            Response::ReadIoStats(stats) => Ok(stats),
            _ => self.wrong_response(),
        }
    }
    /// Nonblocking read-only owner counters for operational metrics.
    pub fn operational_read_io_stats(&mut self) -> Result<Option<ReadExecutorStats>, TcpError> {
        match self.request(Request::OperationalReadIoStats)? {
            Response::OperationalReadIoStats(snapshot) => {
                let (partition, stats) = *snapshot;
                if partition != self.expected {
                    return self.wrong_response();
                }
                Ok(stats)
            }
            _ => self.wrong_response(),
        }
    }
    /// Local cumulative grant/RMA counters for this partition connection.
    /// Snapshot performs no RPC or provider progress.
    #[must_use]
    pub fn resident_read_stats(&self) -> ResidentReadStats {
        let mut stats = self.reactor.resident_read_stats();
        stats.destination_slots_live = self.resident_reads.destination_slots as u64;
        stats
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
    pub(crate) fn connect_operational_shared(
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
            false,
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
        expected.validate().map_err(TcpError::component)?;
        tls.transport.validate()?;
        if timeout.is_zero() || timeout > Duration::from_secs(300) {
            return Err(TcpError::new("invalid client timeout"));
        }
        let address: SocketAddr = endpoint.parse().map_err(|_| {
            TcpError::new("endpoint must be numeric IP:port; set TLS server_name separately")
        })?;
        let destination_resident_reads = tls.resident_reads.destination_per_connection(1)?;
        let began = Instant::now();
        let socket = TcpStream::connect_timeout(&address, timeout)?;
        let connect_ns = elapsed_ns(began);
        let mut io = DeadlineIo::new(socket, timeout)?;
        let setup_started = Instant::now();
        if tls.transport.is_native() {
            write_transport_mode(&mut io, tls.transport)?;
        }
        let (session, reuse_verifier, server_name, tls_ns, fabric_ns) = if tls.transport.is_native()
        {
            #[cfg(feature = "experimental-fabric")]
            {
                let endpoint =
                    fabric::bootstrap(&mut io, tls.transport, destination_resident_reads)?;
                io.fabric = Some(endpoint);
                let fabric_ns = Some(elapsed_ns(setup_started));
                (reactor::Session::new_native(io)?, None, None, 0, fabric_ns)
            }
            #[cfg(not(feature = "experimental-fabric"))]
            {
                return Err(TcpError::new("native transport was not compiled"));
            }
        } else {
            let name =
                ServerName::try_from(tls.server_name.clone()).map_err(TcpError::component)?;
            let client_tls_config = client_config
                .config
                .ok_or_else(|| TcpError::new("TLS transport has no client credentials"))?;
            let verifier = client_config
                .verifier
                .ok_or_else(|| TcpError::new("TLS transport has no peer verifier"))?;
            let mut conn = ClientConnection::new(client_tls_config, name.clone())
                .map_err(TcpError::component)?;
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
            let fabric_ns = if tls.transport == Transport::Tcp {
                None
            } else {
                let began = Instant::now();
                stream.sock.fabric = Some(fabric::bootstrap(
                    &mut stream,
                    tls.transport,
                    destination_resident_reads,
                )?);
                Some(elapsed_ns(began))
            };
            #[cfg(not(feature = "experimental-fabric"))]
            let fabric_ns = None;
            (
                reactor::Session::new(stream.conn.into(), stream.sock)?,
                Some(verifier),
                Some(name),
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
            max_read_bytes: 256 * 1024 * 1024,
            page_limit: 1024,
            diagnostics: None,
            resident_reads: destination_resident_reads,
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
            reuse_verifier,
            server_name,
            polling: PollingPolicy::Adaptive,
            #[cfg(feature = "test-utils")]
            ram_probe_bytes: None,
        };
        if query_status {
            let began = Instant::now();
            client.connected_status = Some(client.status()?);
            client.connect_profile.status_ns = elapsed_ns(began);
        }
        #[cfg(feature = "experimental-fabric")]
        {
            // Normal EFA can defer RX pool creation/registration until its
            // first CQ progress, inside status rather than fi_endpoint.
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
    pub(crate) fn connect_profile(&self) -> ClientConnectProfile {
        self.connect_profile
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
            _ => self.wrong_response(),
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
            _ => self.wrong_response(),
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
            _ => self.wrong_response(),
        }
    }
    fn wrong_response<T>(&mut self) -> Result<T, TcpError> {
        self.broken = true;
        Err(TcpError::new("unexpected authenticated response"))
    }
    fn check_status(&mut self, status: NodeStatus) -> Result<NodeStatus, TcpError> {
        if status.partition != self.expected
            || status.shape_yaml != self.shape.to_yaml().map_err(TcpError::component)?
            || status.max_epochs == 0
            || status.max_epochs > 1024
            || !(1024..=HARD_FRAME_LIMIT).contains(&status.max_frame_bytes)
            || !(1..=256).contains(&status.max_reads)
            || !(1..=256 * 1024 * 1024).contains(&status.max_read_bytes)
            || !(1..=1024).contains(&status.recovery_page_batches)
            || status.epochs.len() > status.max_epochs
            || status.epochs.iter().any(|e| e.epoch == 0)
            || status.epochs.windows(2).any(|w| w[0].epoch >= w[1].epoch)
            || status.epochs.iter().filter(|e| !e.sealed).count()
                != usize::from(status.active_epoch.is_some())
            || status.active_epoch.is_some_and(|epoch| {
                status.epochs.last()
                    != Some(&NodeEpoch {
                        epoch,
                        sealed: false,
                    })
            })
        {
            return self.wrong_response();
        }
        self.frame_limit = status.max_frame_bytes;
        self.max_reads = status.max_reads;
        self.max_read_bytes = status.max_read_bytes;
        self.page_limit = status.recovery_page_batches;
        self.polling = status.polling;
        Ok(status)
    }
    pub fn status(&mut self) -> Result<NodeStatus, TcpError> {
        match self.request(Request::Status { protocol: PROTOCOL })? {
            Response::Status(status) => self.check_status(status),
            _ => self.wrong_response(),
        }
    }
    pub fn fence_to(&mut self, epoch: u64) -> Result<NodeStatus, TcpError> {
        self.revoke_resident_grants()?;
        match self.request(Request::Fence { epoch })? {
            Response::Status(status) if status.active_epoch == Some(epoch) => {
                self.check_status(status)
            }
            _ => self.wrong_response(),
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
    pub fn append_many(
        &mut self,
        epoch: u64,
        batches: &[CompleteBatch],
    ) -> Result<Vec<DurableBatch>, TcpError> {
        let id = self
            .submit_append_many(epoch, batches)?
            .ok_or_else(|| TcpError::new("session append admission is full"))?;
        let mut polling = reactor::ActivePoll::default();
        loop {
            if let Some(completion) = self.take_append_completion(id)? {
                return completion.result;
            }
            self.pause_append_poll(&mut polling);
        }
    }
    pub(crate) fn submit_append_many(
        &mut self,
        epoch: u64,
        batches: &[CompleteBatch],
    ) -> Result<Option<u64>, TcpError> {
        if batches.is_empty() || batches.len() > self.expected.group_limits.max_batches as usize {
            return Err(TcpError::new("invalid append group count"));
        }
        let mut encoded_bytes = GROUP_HEADER_LEN;
        let mut charge = 64usize;
        for batch in batches {
            batch
                .validate_for(&self.shape, self.expected.batch_limits)
                .map_err(TcpError::component)?;
            if batch.version().epoch != epoch
                || batch.id().database != self.expected.inventory.database
                || self
                    .expected
                    .inventory
                    .partition_for(batch.id())
                    .map_err(TcpError::component)?
                    != self.expected.partition
            {
                return Err(TcpError::new("batch belongs to another epoch/partition"));
            }
            encoded_bytes = encoded_bytes
                .checked_add(
                    CompleteGroup::encoded_member_len(batch.encoded().len())
                        .map_err(TcpError::component)?,
                )
                .ok_or_else(|| TcpError::new("append group size overflow"))?;
            charge = charge
                .checked_add(batch.encoded().len().saturating_add(64))
                .ok_or_else(|| TcpError::new("append session credit overflow"))?;
        }
        if encoded_bytes > self.expected.group_limits.max_encoded_bytes {
            return Err(TcpError::new("append group exceeds configured byte limit"));
        }
        self.submit_append(epoch, batches, charge)
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
    pub fn read_records(
        &mut self,
        epoch: u64,
        references: &[ValueRecordReference],
    ) -> Result<Vec<Mutation>, TcpError> {
        self.read_records_inner(epoch, references, None)
    }
    pub fn read_record_profiled(
        &mut self,
        epoch: u64,
        reference: &ValueRecordReference,
    ) -> Result<(Mutation, TcpReadProfile), TcpError> {
        let (mut values, profile) =
            self.read_records_profiled(epoch, std::slice::from_ref(reference))?;
        Ok((
            values
                .pop()
                .ok_or_else(|| TcpError::new("empty record response"))?,
            profile,
        ))
    }
    pub fn read_records_profiled(
        &mut self,
        epoch: u64,
        references: &[ValueRecordReference],
    ) -> Result<(Vec<Mutation>, TcpReadProfile), TcpError> {
        let mut profile = TcpReadProfile::default();
        let values = self.read_records_inner(epoch, references, Some(&mut profile))?;
        Ok((values, profile))
    }
    fn read_records_inner(
        &mut self,
        epoch: u64,
        references: &[ValueRecordReference],
        profile: Option<&mut TcpReadProfile>,
    ) -> Result<Vec<Mutation>, TcpError> {
        self.read_records_correlated(epoch, references, ReadClass::Point, profile)
    }
    pub fn recovery_page(
        &mut self,
        epoch: u64,
        cursor: usize,
        limit: usize,
    ) -> Result<RecoveryPage, TcpError> {
        if limit == 0 || limit > 1024 {
            return Err(TcpError::new("invalid recovery page limit"));
        }
        match self.request(Request::Recover {
            epoch,
            cursor,
            limit: limit.min(self.page_limit),
        })? {
            Response::Recovery { batches, next }
                if batches.len() <= limit
                    && next.is_none_or(|next| {
                        cursor.checked_add(batches.len()) == Some(next) && !batches.is_empty()
                    }) =>
            {
                self.broken = true;
                let mut result = Vec::with_capacity(batches.len());
                for (encoded, receipt) in batches {
                    let batch = CompleteBatch::decode_owned(
                        &self.shape,
                        encoded.into_vec(),
                        self.expected.batch_limits,
                    )
                    .map_err(TcpError::component)?;
                    if batch.version().epoch != epoch {
                        return self.wrong_response();
                    }
                    let durable = self.check_receipt(&batch, receipt)?;
                    result.push((batch, durable));
                }
                self.broken = false;
                Ok(RecoveryPage {
                    batches: result,
                    next,
                })
            }
            _ => self.wrong_response(),
        }
    }
    pub fn seal(&mut self, epoch: u64) -> Result<(), TcpError> {
        self.revoke_resident_grants()?;
        match self.request(Request::Seal { epoch })? {
            Response::Sealed => Ok(()),
            _ => self.wrong_response(),
        }
    }
    pub fn stats(&mut self) -> Result<PartitionStats, TcpError> {
        match self.request(Request::StatsDetailed)? {
            Response::StatsDetailed {
                groups,
                barriers,
                namespace_syncs,
                inventory_witnesses,
                records,
                metadata,
            } => Ok(PartitionStats {
                appended_groups: groups,
                persistence_barriers: barriers,
                namespace_syncs,
                inventory_witnesses,
                registered_records: records,
                reference_metadata_bytes: metadata,
            }),
            _ => self.wrong_response(),
        }
    }
}

impl TlsFiles {
    fn native_mode(&self) -> Result<bool, TcpError> {
        self.transport.validate()?;
        self.resident_reads.validate()?;
        if self.resident_reads.enabled() && !self.transport.is_native() {
            return Err(TcpError::new(
                "one-sided reads require the explicitly trusted native transport",
            ));
        }
        let native = self.transport.is_native();
        if native
            && [&self.ca, &self.cert, &self.key]
                .iter()
                .any(|path| !path.as_os_str().is_empty())
        {
            return Err(TcpError::new(
                "trusted native transport requires empty TLS credential paths",
            ));
        }
        Ok(native)
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
        if self.native_mode()? {
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
            .ok_or_else(|| TcpError::new("native transport has no TLS client config"))
    }
    pub(crate) fn client_config_with_identity(&self) -> Result<ClientTlsConfig, TcpError> {
        if self.native_mode()? {
            return Ok(ClientTlsConfig {
                config: None,
                verifier: None,
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
            .with_webpki_verifier(verifier.clone())
            .with_client_auth_cert(cert, key)
            .map_err(TcpError::component)?;
        config.alpn_protocols = vec![self.transport.alpn()];
        Ok(ClientTlsConfig {
            config: Some(Arc::new(config)),
            verifier: Some(verifier),
            identity,
        })
    }
}

#[cfg(feature = "test-utils")]
fn validate_ram_probe_size(value_bytes: usize, frame_limit: usize) -> Result<(), TcpError> {
    // Envelope u32 protocol + u64 ID + u32 response tag + u64 byte length.
    if value_bytes > 64 * 1024 || value_bytes > frame_limit.saturating_sub(24) {
        return Err(TcpError::new("RAM probe exceeds payload/frame bound"));
    }
    Ok(())
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
