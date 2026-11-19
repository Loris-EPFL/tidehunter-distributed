//! Static RF1 database authority over authenticated TCP Value WAL partitions.
//!
//! One coordinator host holds the native directory lock and durable authority
//! manifest. Every restart durably reserves a new epoch, fences ALL registered nodes,
//! then validates a committed checkpoint and replays its complete sealed tails.
//! Copying the authority directory to another live host is unsupported; failover
//! requires a separately established lease/consensus protocol.
//!
//! Paper §§3.1/3.3: logical allocation, complete WAL authority and publication are
//! separate. An RPC timeout never authorizes skipping a batch or reusing its ID.
//! Values remain on the selected node's native WAL. The coordinator retains only
//! native-routed index references and bounded in-flight complete batches.

use super::codec::{CompleteBatch, Mutation};
use super::durability::{DurabilityTier, DurableCutConfig};
use super::native_index::{IndexReadCut, NativeIndex};
use super::native_publication::{NativePublicationRouting, PreparedNativePublication};
use super::reference_directory::ReferenceDirectory;
use super::storage::{
    AppendedBatch, DurableBatch, PartitionConfig, PartitionStats, StaticInventory,
    WAL_REPLAY_WINDOW_BATCHES, encode_metadata, install_metadata, read_metadata,
};
use super::storage::{LocalCheckpoint, LocalDbError, LocalDbStats, LocalInventoryConfig};
use super::tcp::checkpoint::CheckpointPositionProof;
use super::tcp::{Client, TlsFiles};
use super::types::{BatchId, BatchVersion, MutationVersion, ValueRecordReference};
use super::write_timeline::{
    CheckpointPressure, WriteTimeline, WriteTimelineSample, WriteTimelineStage, WriteWait,
};
use crate::key_shape::{KeyShape, KeySpace};
use crate::lock::DbLock;
use crate::metrics::Metrics;
use bincode::Options;
use blake2::{Blake2b, Digest};
use minibytes::Bytes;
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, HashMap, VecDeque};
use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::{Arc, mpsc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use typenum::U32;

mod checkpoint_lifecycle;
mod concurrent;
mod coordinator_profile;
use checkpoint_lifecycle::BackgroundCheckpoint;
pub use concurrent::ConcurrentRemoteDatabase;
#[cfg(feature = "experimental-fabric")]
pub use concurrent::FabricProviderEvidence;
pub use concurrent::LiveFinalShutdownEvidence;
use coordinator_profile::{
    CoordinatorResolutionDiagnostics, MemberOutcome, ResolutionSample, ResolutionStage,
};
pub use coordinator_profile::{CoordinatorResolutionSnapshot, CoordinatorResolutionStage};
mod durable_cut;
pub use durable_cut::{DurableCutStats, RecoveredCut};
mod fencing_generations;
use fencing_generations::AuthorityGeneration;
#[cfg(test)]
use fencing_generations::GenerationState;
mod pending_read;
pub(crate) use pending_read::ReadTraceChild;
pub use pending_read::ReadTraceSnapshot;
mod replay_limits;
mod write_core;
pub use replay_limits::ReplayLimits;
use write_core::completion::{AppendCompletion, GroupCompletion, WorkerAccounting};
use write_core::owner_queue::OwnerQueue;
use write_core::owner_sender::{OrderedCommand, OwnerSender};
use write_core::transport_budget::{
    AppendByteBudget, DEFAULT_OWNER_APPEND_OCCUPANCY, validate_owner_occupancy,
};
use write_core::worker_group::{GroupDrain, WorkerPolicy, take_ready_group};
mod read_cache;
mod read_pipeline;
mod transports;
use pending_read::ReadService;
pub use pending_read::{
    CompletedGet, CompletedRecord, PendingGet, PendingRecord, ReadProfileMonitor,
};
pub use read_cache::{
    KEYSPACE_COUNTER_COUNT, PerKeyspaceReadCacheStats, ReadCacheShardLoad, ReadCacheShardStats,
    ReadCacheStats,
};
use read_cache::{ReadCache, ReadCachePolicy};
pub(crate) use read_pipeline::PendingScanPage;

pub const BACKEND_ID: &str = "tidehunter-tcp-rf1";
const MANIFEST: &str = "tcp-database-authority-v1";
const MAX_CHECKPOINT_METADATA_BYTES: u64 = 128 * 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RemoteEndpoint {
    pub partition: u32,
    pub address: String,
    pub server_name: String,
}

/// Unknown keys fail at parse: a key of a removed mode (for example
/// `history_persistence` or `checkpoint_mode`) is never silently ignored.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteDatabaseConfig {
    pub storage: LocalInventoryConfig,
    pub endpoints: Vec<RemoteEndpoint>,
    pub tls: TlsFiles,
    pub timeout: Duration,
    /// Resident suffix identities and ordering slots, not a lifetime write limit.
    /// Checkpoints move completed identities into the bounded WAL-replay retry window.
    pub max_admitted_batches: usize,
    pub max_index_bytes: usize,
    /// Lock shards of the coordinator index; a power of two. In memory only, so
    /// it can change between opens.
    #[serde(default = "default_index_shards")]
    pub index_shards: usize,
    pub max_pending_batches: usize,
    pub max_pending_bytes: usize,
    /// Background checkpoint trigger.
    #[serde(default)]
    pub replay_limits: ReplayLimits,
    /// Queued and active append commands per owner; one transport slot is reserved for control.
    #[serde(default = "default_owner_append_occupancy")]
    pub owner_append_occupancy: usize,
    /// Dispatch full per-node groups during admission; drain remains the ack barrier.
    #[serde(default = "default_pipeline")]
    pub pipelined_writes: bool,
    /// Leased live writes only: before each frame submission, the owner
    /// worker polls the owner queue again and merges groups that became
    /// READY after the drain at the top of its turn. `false` keeps only that
    /// first drain; it is an A/B measurement control, valid only with leased
    /// live writes. Absent means `true`, so documents are unchanged.
    #[serde(
        default = "default_leased_submit_redrain",
        skip_serializing_if = "is_true"
    )]
    pub leased_submit_redrain: bool,
    /// Derivative verified-record cache. Zero disables it; never authoritative.
    #[serde(default = "default_read_cache_bytes")]
    pub read_cache_bytes: usize,
    /// Explicit host slice for cache ownership and bounded pending read buffers.
    #[serde(default = "default_read_memory_allowance_bytes")]
    pub read_memory_allowance_bytes: usize,
    /// Bounded scan-page lookahead; one is the serial control.
    #[serde(default = "default_scan_prefetch_pages")]
    pub scan_prefetch_pages: usize,
    /// Absent resolves to the coordinator host's available CPU parallelism at open.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_lanes: Option<usize>,
    /// Pin live read workers to hardware-queue CPUs once a namespace is known.
    /// Best effort: affinity failures are counted and do not fail reads.
    #[serde(default)]
    pub pin_read_lanes: bool,
    #[serde(default = "default_max_pending_reads")]
    pub max_pending_reads: usize,
    #[serde(default = "default_max_pending_read_bytes")]
    pub max_pending_read_bytes: usize,
    /// Per-append-connection bounded sample; zero keeps storage profiling disabled.
    #[serde(default = "default_write_profile_limit")]
    pub write_profile_limit: usize,
    /// The leased live owner-WAL NVMe-oF route; required. Remote databases have
    /// no other data path.
    pub live_nvme: super::tcp::LiveClientConfig,
    /// The durable cut of Native-tier recovery. Absent means the default lags.
    #[serde(default, skip_serializing_if = "DurableCutConfig::is_default")]
    pub durable_cut: DurableCutConfig,
}

fn default_max_pending_reads() -> usize {
    256
}
fn default_max_pending_read_bytes() -> usize {
    64 * 1024 * 1024
}

fn default_scan_prefetch_pages() -> usize {
    2
}

fn default_pipeline() -> bool {
    true
}
fn default_leased_submit_redrain() -> bool {
    true
}
fn is_true(value: &bool) -> bool {
    *value
}
fn default_read_cache_bytes() -> usize {
    8 * 1024 * 1024
}
fn default_read_memory_allowance_bytes() -> usize {
    512 * 1024 * 1024
}
fn default_write_profile_limit() -> usize {
    0
}

fn default_owner_append_occupancy() -> usize {
    DEFAULT_OWNER_APPEND_OCCUPANCY
}
fn default_index_shards() -> usize {
    super::native_index::DEFAULT_INDEX_SHARDS
}

impl RemoteDatabaseConfig {
    pub const MAX_OWNER_APPEND_OCCUPANCY: usize = super::tcp::APPEND_COMMAND_WINDOW;
    pub const DEFAULT_OWNER_APPEND_OCCUPANCY: usize = DEFAULT_OWNER_APPEND_OCCUPANCY;
    /// Each lane retains a connection to every owner. Owners must budget for
    /// an automatic coordinator without knowing that coordinator's CPU count.
    pub const MAX_AUTO_READ_LANES: usize = 64;
    /// One session from `transports::connect(..., 1)` is shared by append,
    /// checkpoint and lifecycle control; live pins reuse read/control sessions.
    const APPEND_CONTROL_CONNECTIONS: usize = 1;
    /// The durable-cut poller owns one plain session per owner.
    const DURABLE_PROGRESS_CONNECTIONS: usize = 1;
    pub const NON_READ_CONNECTIONS: usize =
        Self::APPEND_CONTROL_CONNECTIONS + Self::DURABLE_PROGRESS_CONNECTIONS;
    /// Budget two complete coordinator session sets so a reopen can overlap
    /// old sessions until their owner permits observe EOF. Auto configs use
    /// the cap, not the preparing host's CPU count.
    pub fn generated_owner_connection_limit(
        read_lanes: Option<usize>,
    ) -> Result<usize, LocalDbError> {
        let lanes = read_lanes.unwrap_or(Self::MAX_AUTO_READ_LANES);
        let required = lanes
            .checked_add(Self::NON_READ_CONNECTIONS)
            .and_then(|sessions| sessions.checked_mul(2))
            .ok_or_else(|| {
                LocalDbError::new(format!(
                    "read_lanes={lanes}: overlapping owner connection budget overflows"
                ))
            })?;
        if required > 256 {
            return Err(LocalDbError::new(format!(
                "read_lanes={lanes}: two session sets require {required} connections, exceeding owner max_connections ceiling 256"
            )));
        }
        Ok(required)
    }

    pub fn effective_read_lanes(&self) -> usize {
        crate::parallelism::resolve_count(
            self.read_lanes,
            crate::parallelism::available().min(Self::MAX_AUTO_READ_LANES),
        )
    }

    fn validate_read_lane_pinning(&self, read_lanes: usize) -> Result<(), LocalDbError> {
        if self.pin_read_lanes && read_lanes > self.live_nvme.queue_geometry.io_queues as usize {
            return Err(LocalDbError::new(
                "pin_read_lanes requires read_lanes <= live I/O queues",
            ));
        }
        Ok(())
    }

    fn effective_owner_append_occupancy(&self) -> usize {
        if self.live_nvme.leased_write_window.is_none() {
            Self::MAX_OWNER_APPEND_OCCUPANCY
        } else {
            self.owner_append_occupancy
        }
    }
    /// Fixed handle, tag and history arrays, already included in the cache budget.
    pub fn read_cache_metadata_bytes(&self) -> usize {
        ReadCache::metadata_bytes(self.read_cache_bytes)
    }

    fn inventory(&self, epoch: u64) -> StaticInventory {
        StaticInventory {
            database: self.storage.database,
            epoch,
            segment_generation: epoch,
            partitions: self.storage.partitions.clone(),
        }
    }

    fn partition_config(&self, epoch: u64, partition: u32) -> PartitionConfig {
        PartitionConfig {
            inventory: self.inventory(epoch),
            partition,
            batch_limits: self.storage.batch_limits,
            group_limits: self.storage.group_limits,
            fragment_bytes: self.storage.fragment_bytes,
            wal_file_bytes: self.storage.wal_file_bytes,
            max_batches: self.storage.max_batches_per_partition,
            max_reference_metadata_bytes: self.storage.max_reference_metadata_bytes,
        }
    }

    fn append_byte_budget(&self) -> Result<AppendByteBudget, LocalDbError> {
        let largest_charge = self
            .storage
            .group_limits
            .max_encoded_bytes
            .checked_add((self.storage.group_limits.max_batches as usize + 1) * 64)
            .ok_or_else(|| LocalDbError::new("owner append byte budget overflow"))?;
        AppendByteBudget::new(super::tcp::APPEND_SESSION_BYTES, 64 * 1024, largest_charge)
            .map_err(LocalDbError::new)
    }

    /// Validate bounded execution and recovery geometry without filesystem or
    /// network access, so preparation can reject invalid campaigns locally.
    ///
    /// # Errors
    /// Rejects unsupported live mode, incompatible limits or invalid inventories.
    pub fn validate(&self) -> Result<(), LocalDbError> {
        if !self.index_shards.is_power_of_two()
            || self.index_shards > super::native_index::MAX_INDEX_SHARDS
        {
            return Err(LocalDbError::new(
                "index_shards must be a power of two in 1..=MAX_INDEX_SHARDS",
            ));
        }
        self.durable_cut.validate().map_err(LocalDbError::new)?;
        {
            let live = &self.live_nvme;
            live.validate().map_err(LocalDbError::new)?;
            if let Some(read_lanes) = self.read_lanes {
                self.validate_read_lane_pinning(read_lanes)?;
            }
            if !live.owner_identities.is_empty()
                && (live.owner_identities.len() != self.storage.partitions.len()
                    || self
                        .storage
                        .partitions
                        .iter()
                        .any(|p| !live.owner_identities.contains_key(p)))
            {
                return Err(LocalDbError::new(
                    "live owner identities must cover every partition",
                ));
            }
            {
                if self.storage.fragment_bytes >= 1 << 30 {
                    return Err(LocalDbError::new(
                        "leased live extents must be smaller than 1 GiB",
                    ));
                }
                let write_frame = self
                    .storage
                    .group_limits
                    .max_encoded_bytes
                    .checked_add(8 + 4095)
                    .map(|bytes| bytes & !4095)
                    .ok_or_else(|| LocalDbError::new("live frame size overflow"))?;
                // An unaligned recovered member needs an aligned I/O buffer
                // plus its owned result. Neither logical batch limit is reduced.
                let recovery_bytes = self
                    .storage
                    .batch_limits
                    .max_encoded_bytes
                    .checked_add(8)
                    .and_then(|bytes| bytes.checked_mul(2))
                    .and_then(|bytes| bytes.checked_add(3 * 4095))
                    .ok_or_else(|| LocalDbError::new("live recovery size overflow"))?;
                let write_bytes = write_frame
                    .checked_add(4095)
                    .ok_or_else(|| LocalDbError::new("live write size overflow"))?;
                if live.max_inflight_bytes < write_bytes.max(recovery_bytes)
                    || !self.storage.fragment_bytes.is_multiple_of(4096)
                    || !self.storage.wal_file_bytes.is_multiple_of(4096)
                    || write_frame as u64 > self.storage.fragment_bytes
                {
                    return Err(LocalDbError::new(
                        "live queue or fragment cannot admit the configured write and recovery limits",
                    ));
                }
            }
        }
        self.replay_limits.validate().map_err(LocalDbError::new)?;
        validate_owner_occupancy(self.owner_append_occupancy).map_err(LocalDbError::new)?;
        if self
            .live_nvme
            .leased_write_window
            .is_some_and(|window| window > self.owner_append_occupancy)
        {
            return Err(LocalDbError::new(
                "leased write window exceeds owner append occupancy",
            ));
        }
        self.inventory(1)
            .validate()
            .map_err(LocalDbError::component)?;
        self.append_byte_budget()?;
        if self
            .endpoints
            .iter()
            .map(|e| e.partition)
            .collect::<Vec<_>>()
            != self.storage.partitions
            || self
                .endpoints
                .iter()
                .any(|e| e.address.is_empty() || e.server_name.is_empty())
            || self.max_admitted_batches == 0
            || self.max_admitted_batches > 1_000_000
            || self.max_pending_batches == 0
            || self.max_pending_batches > 4096
            || self.max_pending_bytes == 0
            || self.max_pending_bytes > 256 * 1024 * 1024
            || !(1..=2).contains(&self.scan_prefetch_pages)
            || self.read_memory_allowance_bytes == 0
            // Cache bytes include all fixed arrays and charged live entries.
            || self
                .read_cache_bytes
                .checked_add(self.max_pending_read_bytes)
                .is_none_or(|bytes| bytes > self.read_memory_allowance_bytes)
            || self.read_lanes.is_some_and(|lanes| !(1..=1024).contains(&lanes))
            || self.max_pending_reads == 0
            || self.write_profile_limit > 65_536
            || !(ReadService::MIN_FLIGHT_BYTES..=256 * 1024 * 1024)
                .contains(&self.max_pending_read_bytes)
            || self.timeout.is_zero()
            || self.timeout > Duration::from_secs(120)
        {
            return Err(LocalDbError::new(
                "invalid static TCP authority configuration",
            ));
        }
        for endpoint in &self.endpoints {
            self.partition_config(1, endpoint.partition)
                .validate()
                .map_err(LocalDbError::component)?;
        }
        if !checkpoint_metadata_fits(self.endpoints.len(), 0) {
            return Err(LocalDbError::new(
                "checkpoint epoch inventory exceeds metadata budget",
            ));
        }
        if !checkpoint_metadata_fits(
            self.endpoints.len(),
            self.replay_limits
                .max_batches
                .min(self.max_admitted_batches),
        ) {
            return Err(LocalDbError::new(
                "replay suffix exceeds checkpoint metadata capacity",
            ));
        }
        self.recovery_page_limit()?;
        Ok(())
    }

    fn recovery_page_limit(&self) -> Result<usize, LocalDbError> {
        if self
            .storage
            .group_limits
            .max_encoded_bytes
            .checked_mul(self.endpoints.len())
            .is_none_or(|bytes| bytes > self.max_pending_bytes)
        {
            return Err(LocalDbError::new(
                "pending byte budget is below one replay group per owner",
            ));
        }
        let metadata = (self.storage.batch_limits.max_operations as usize)
            .checked_mul(
                std::mem::size_of::<Mutation>()
                    + std::mem::size_of::<super::types::RecordDescriptor>(),
            )
            .ok_or_else(|| LocalDbError::new("recovery budget overflow"))?;
        let per_batch = self
            .storage
            .batch_limits
            .max_encoded_bytes
            .checked_add(metadata)
            .and_then(|n| n.checked_add(512))
            .ok_or_else(|| LocalDbError::new("recovery budget overflow"))?;
        let one_per_node = per_batch
            .checked_mul(self.endpoints.len())
            .filter(|n| *n > 0)
            .ok_or_else(|| LocalDbError::new("recovery budget overflow"))?;
        let limit = (self.max_pending_bytes / one_per_node).min(32);
        if limit == 0 {
            return Err(LocalDbError::new(
                "recovery budget cannot hold one bounded batch per node",
            ));
        }
        Ok(limit)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct AuthorityManifest {
    format: u32,
    storage: LocalInventoryConfig,
    #[serde(flatten)]
    mode: AuthorityMode,
    endpoints: Vec<RemoteEndpoint>,
    shape_yaml: String,
    #[serde(default = "ReplayLimits::legacy")]
    replay_limits: ReplayLimits,
    /// The data-plane mode and initiator identity survive authority reopen.
    #[serde(default)]
    live_nvme_identity: Option<(String, [u8; 16])>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    live_nvme_owner_identities: BTreeMap<u32, super::tcp::LiveInitiatorIdentity>,
    /// Next fencing generation to allocate; never reused (L1).
    next_generation: u64,
    /// Retained generations, strictly ascending; see `fencing_generations`.
    generations: Vec<AuthorityGeneration>,
    /// Excluded generation whose owner trims are not all acknowledged yet.
    trim_pending: Option<u64>,
    /// In-memory only: the trim the next checkpoint adopts with its root.
    #[serde(skip)]
    adopt_trim: Option<u64>,
    #[serde(default)]
    checkpoint: Option<CheckpointCommit>,
    /// Ownership is reserved durably before any checkpoint output is created.
    #[serde(default)]
    checkpoint_staging: Option<CheckpointFiles>,
    /// Previous committed files, removable only after the new root is durable.
    #[serde(default)]
    checkpoint_obsolete: Option<CheckpointCommit>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct AuthorityMode {
    /// Always `wal_replay`. Kept so that a legacy manifest (absent field means
    /// `on_disk`, or `memory_only`) fails open with a named mode.
    #[serde(default = "legacy_manifest_history")]
    history_persistence: String,
    /// Presence markers of the removed per-cell checkpoint mode. Exact never
    /// writes these keys; any present value, explicit null included, fails
    /// open as a legacy mode.
    #[serde(
        default,
        deserialize_with = "present_value",
        skip_serializing_if = "Option::is_none"
    )]
    checkpoint_mode: Option<serde_yaml::Value>,
    #[serde(
        default,
        deserialize_with = "present_value",
        skip_serializing_if = "Option::is_none"
    )]
    cell_checkpoint: Option<serde_yaml::Value>,
    #[serde(
        default,
        deserialize_with = "present_value",
        skip_serializing_if = "Option::is_none"
    )]
    cell_checkpoint_obsolete: Option<serde_yaml::Value>,
}

/// A present key is `Some`, even when its value is null; only an absent key
/// takes the `None` default. Plain `Option` would read null as absent.
fn present_value<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<serde_yaml::Value>, D::Error> {
    serde_yaml::Value::deserialize(deserializer).map(Some)
}

const MANIFEST_HISTORY: &str = "wal_replay";
/// Authority manifest format of the removed per-cell checkpoint mode.
const PER_CELL_MANIFEST_FORMAT: u32 = 5;

fn legacy_manifest_history() -> String {
    "on_disk".into()
}

fn legacy_mode_error(mode: &str) -> LocalDbError {
    LocalDbError::new(format!(
        "unsupported legacy distributed mode {mode}: use archive/multi-design"
    ))
}

impl AuthorityMode {
    /// Reject manifests of removed modes before any fence or cleanup.
    fn reject_legacy_mode(&self, format: u32) -> Result<(), LocalDbError> {
        if format == PER_CELL_MANIFEST_FORMAT
            || self.checkpoint_mode.is_some()
            || self.cell_checkpoint.is_some()
            || self.cell_checkpoint_obsolete.is_some()
        {
            return Err(legacy_mode_error("per_cell"));
        }
        if self.history_persistence != MANIFEST_HISTORY {
            return Err(legacy_mode_error(&self.history_persistence));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct CheckpointFiles {
    epoch: u64,
    generation: u64,
    index_files: Vec<String>,
}
impl CheckpointFiles {
    fn names(&self) -> Vec<String> {
        let mut names = vec![
            format!("index-{}-{}.snapshot", self.epoch, self.generation),
            format!("checkpoint-{}-{}.metadata", self.epoch, self.generation),
            format!(
                ".checkpoint-{}-{}.metadata.pending",
                self.epoch, self.generation
            ),
        ];
        names.extend(self.index_files.iter().cloned());
        names
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct CheckpointCommit {
    epoch: u64,
    generation: u64,
    metadata_bytes: u64,
    metadata_digest: [u8; 32],
    index_files: Vec<String>,
}
impl CheckpointCommit {
    fn metadata_name(&self) -> String {
        format!("checkpoint-{}-{}.metadata", self.epoch, self.generation)
    }
    fn index_name(&self) -> String {
        format!("index-{}-{}.snapshot", self.epoch, self.generation)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StreamPosition {
    partition: u32,
    epoch: u64,
    replay_from: u64,
    last_offset: Option<u64>,
    durable_end: u64,
    complete: bool,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
struct RecentReceipt {
    id: BatchId,
    version: BatchVersion,
    digest: super::types::PayloadDigest,
}

#[derive(Serialize, Deserialize)]
struct CheckpointImageV3 {
    format: u32,
    database: [u8; 16],
    shape_yaml: String,
    authority_epoch: u64,
    generation: u64,
    cut: Option<MutationVersion>,
    published_next: u64,
    index_bytes: u64,
    index_digest: [u8; 32],
    positions: Vec<StreamPosition>,
    recent: Vec<RecentReceipt>,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct RecoveryStats {
    pub checkpoint_loaded: bool,
    pub checkpoint_load_ns: u64,
    pub proof_validation_ns: u64,
    pub tail_replay_ns: u64,
    pub checkpoint_commit_ns: u64,
    pub connection_setup_ns: u64,
    pub fencing_ns: u64,
    pub tail_batches: u64,
    pub tail_bytes: u64,
    /// Per-open credential parsing/configuration, before parallel connections.
    pub tls_config_ns: u64,
    /// Read-connection worker wall time; overlaps control setup and recovery.
    pub read_lane_setup_ns: u64,
    /// Critical-path wait for prepared read connections after recovery.
    pub read_lane_wait_ns: u64,
    pub created_connections: u64,
    /// Nested elapsed worker sums across both connection sets, not additive.
    pub connect_worker_ns: u64,
    pub status_worker_ns: u64,
    pub tls_worker_ns: u64,
    pub fabric_worker_ns: Option<u64>,
    /// Nested fabric setup worker sums. Queue/MTU/version fields are maxima,
    /// not sums. Absent for TCP and for reused transports with no new endpoint.
    #[cfg(feature = "experimental-fabric")]
    pub fabric_open_worker: Option<tidehunter_fabric::OpenProfile>,
    /// Tail batches above the durable cut that recovery did not publish.
    pub cut_discarded_batches: u64,
    /// Tail batches above the persisted cut that recovery extended over.
    pub cut_extended_batches: u64,
    /// Missing sequences the cut extension skipped as never written, because
    /// they lay below the owner-continuity bound.
    pub cut_skipped_versions: u64,
    /// The owner-continuity bound of the cut extension (exclusive sequence);
    /// `u64::MAX` when every owner was continuous.
    pub cut_continuity_bound: Option<u64>,
    /// The bound this open decided for the previously tracked epoch.
    pub recovered_cut: Option<RecoveredCut>,
}
impl RecoveryStats {
    fn add_connection(&mut self, profile: super::tcp::ClientConnectProfile) {
        self.connect_worker_ns += profile.connect_ns;
        self.status_worker_ns += profile.status_ns;
        self.tls_worker_ns += profile.tls_ns;
        if let Some(ns) = profile.fabric_ns {
            *self.fabric_worker_ns.get_or_insert(0) += ns;
        }
        #[cfg(feature = "experimental-fabric")]
        if let Some(open) = profile.fabric_open {
            let total = self.fabric_open_worker.get_or_insert_default();
            for (counter, ns) in [
                (&mut total.allocation_ns, open.allocation_ns),
                (&mut total.discovery_ns, open.discovery_ns),
                (&mut total.fabric_ns, open.fabric_ns),
                (&mut total.domain_ns, open.domain_ns),
                (&mut total.cq_ns, open.cq_ns),
                (&mut total.av_ns, open.av_ns),
                (&mut total.endpoint_ns, open.endpoint_ns),
                (&mut total.bind_enable_ns, open.bind_enable_ns),
                (&mut total.register_ns, open.register_ns),
                (&mut total.address_ns, open.address_ns),
                (&mut total.peer_setup_ns, open.peer_setup_ns),
                (&mut total.first_progress_ns, open.first_progress_ns),
            ] {
                *counter += ns;
            }
            total.requested_tx = total.requested_tx.max(open.requested_tx);
            total.requested_rx = total.requested_rx.max(open.requested_rx);
            total.selected_tx = total.selected_tx.max(open.selected_tx);
            total.selected_rx = total.selected_rx.max(open.selected_rx);
            total.mtu = total.mtu.max(open.mtu);
            total.provider_version = total.provider_version.max(open.provider_version);
        }
    }
}

// Own only reduced key bytes: a dependency must not pin an old value's batch.
type DependencyKey = SmallVec<[u8; 32]>;

#[derive(Clone, Copy)]
pub(crate) enum OpenInterruption {
    #[cfg(any(test, feature = "test-utils"))]
    AfterFences(usize),
    #[cfg(test)]
    AfterReadSetup,
    #[cfg(test)]
    DuringReplay,
    /// After the durable-cut record is persisted, before the epoch activates.
    #[cfg(test)]
    AfterCutRecord,
    /// After the reservation is durable, before any owner is fenced.
    #[cfg(test)]
    AfterReservation,
    /// After `Served(g)` is durable, before offline trim maintenance.
    #[cfg(test)]
    AfterServed,
    /// Inside the offline trim transaction.
    #[cfg(test)]
    Trim(fencing_generations::TrimCrash),
}

/// Test-only open hook over a read-only config clone and the read-lane clients.
#[cfg(test)]
type ReadHook<'a> = Box<dyn FnOnce(&mut RemoteDatabaseConfig, &mut Vec<Client>) + 'a>;

#[derive(Clone, Copy, Debug, Default)]
pub struct PipelineStats {
    pub prepare_calls: u64,
    pub prepare_wall_ns: u64,
    /// Sum of worker elapsed durations: overlaps; not process CPU time.
    pub prepare_task_ns: u64,
    pub append_requests: u64,
    pub concurrent_admitted: u64,
    pub concurrent_pending_peak: usize,
    pub concurrent_queued_peak: usize,
    pub concurrent_queue_bytes_peak: usize,
    /// Owner-worker drains that took a batch or stopped at a nonempty head,
    /// the batches they took, and why each stopped.
    pub owner_drains: u64,
    pub owner_drain_batches: u64,
    pub owner_drain_stop_empty: u64,
    pub owner_drain_stop_allocated: u64,
    pub owner_drain_stop_fence: u64,
    pub owner_drain_stop_limit: u64,
    pub owner_drain_stop_credit: u64,
}

/// Monotonic diagnostic spans. Background checkpoint lifetimes overlap foreground
/// work; task sums and parent spans are not additive critical-path clocks.
#[derive(Clone, Copy, Debug, Default)]
pub struct AuthorityWriteStats {
    pub storage_wait_ns: u64,
    pub publication_ns: u64,
    pub dispatch_ns: u64,
    pub preparation_admission_ns: u64,
    /// Exclusive concurrent-authority preparation wall time, including pool join
    /// waits, but excluding nested admission/publication progress.
    pub authority_prepare_ns: u64,
    /// Concurrent-authority planning, admission and publication bookkeeping.
    pub authority_admit_ns: u64,
    /// Nonempty input windows, including windows with rejected members.
    pub authority_windows: u64,
    /// Input batches in those windows; stale re-preparation does not add a batch.
    pub authority_batches: u64,
    /// Owner time freezing/adopting checkpoints and explicitly waiting for their
    /// completion. Background I/O overlaps are excluded.
    pub checkpoint_ns: u64,
    pub checkpoint_count: u64,
    /// Captured checkpoint wall lifetimes, including concurrent suffix progress.
    pub background_checkpoint_ns: u64,
    /// Actual owner blocking in admission backpressure (drain plus completed
    /// durable cut), not the lifetime of an already running background worker.
    pub foreground_checkpoint_ns: u64,
    pub foreground_checkpoint_count: u64,
    /// Actor writes deferred at a capacity boundary, and their elapsed queue wait.
    pub concurrent_checkpoint_deferrals: u64,
    pub concurrent_checkpoint_wait_ns: u64,
    /// New logical admissions while immutable checkpoint persistence was active.
    pub background_checkpoint_suffix_batches: u64,
    /// Receipts observed by the owner while a prior sequence is unpublished.
    /// Elapsed time ends at this batch's publication; clocks can overlap.
    pub durable_receipt_publication_blocked_batches: u64,
    pub durable_receipt_publication_blocked_ns: u64,
    /// Current count of receipts in the above blocked state.
    pub durable_receipt_publication_waiting_batches: u64,
    pub durable_receipt_publication_waiting_peak: u64,
    /// Batches published while an earlier admission remained unresolved.
    pub out_of_prefix_published_batches: u64,
    /// Durable batches observed waiting for a native-index predecessor, once each.
    pub dependency_wait_batches: u64,
    /// Publication-view admission: live reservations examined and grown, and the
    /// time the exclusive index guard was held (sum and maximum per admission).
    pub admission_view_scanned: u64,
    pub admission_view_grown: u64,
    pub admission_exclusive_lock_ns: u64,
    pub admission_exclusive_lock_max_ns: u64,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct CheckpointDiagnostic {
    pub cursor: u64,
    pub epoch: u64,
    pub generation: Option<u64>,
    pub checkpoint_generation: Option<u64>,
    pub trigger: Option<CheckpointTrigger>,
    pub requested_cut: Option<MutationVersion>,
    pub captured_cut: Option<MutationVersion>,
    pub captured_batches: Option<usize>,
    pub captured_bytes: Option<usize>,
    pub reserved_index_bytes_before: usize,
    pub reserved_index_bytes_after: usize,
    pub pressure_wait_ns: Option<u64>,
    pub drain_ns: Option<u64>,
    pub existing_capture_wait_ns: Option<u64>,
    pub new_capture_wait_ns: Option<u64>,
    pub staging_manifest_ns: Option<u64>,
    pub node_prefix_wait_ns: Option<u64>,
    pub index_prepare_ns: Option<u64>,
    pub index_persist_ns: Option<u64>,
    pub metadata_install_ns: Option<u64>,
    pub root_adoption_ns: Option<u64>,
    pub owner_adoption_ns: Option<u64>,
    pub obsolete_cleanup_ns: Option<u64>,
    pub outcome: Option<CheckpointProgressOutcome>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum CheckpointTrigger {
    Tail,
    KnownIdentities,
    PartitionWindow,
    IndexReservation,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum CheckpointProgressOutcome {
    AlreadyAvailable,
    AfterDrain,
    AfterExistingCapture,
    AfterNewCapture,
    CapacityError,
    ProgressError,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CheckpointDiagnosticsSnapshot {
    pub next_cursor: u64,
    pub dropped_records: u64,
    pub records: Vec<CheckpointDiagnostic>,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct CheckpointDiagnosticSummary {
    pub available: bool,
    pub records: u64,
    pub dropped_records: u64,
    pub pressure_wait_ns: u64,
    pub pressure_wait_count: u64,
    pub drain_ns: u64,
    pub drain_count: u64,
    pub existing_capture_wait_ns: u64,
    pub existing_capture_wait_count: u64,
    pub new_capture_wait_ns: u64,
    pub new_capture_wait_count: u64,
    pub staging_manifest_ns: u64,
    pub staging_manifest_count: u64,
    pub node_prefix_wait_ns: u64,
    pub node_prefix_wait_count: u64,
    pub index_prepare_ns: u64,
    pub index_prepare_count: u64,
    pub index_persist_ns: u64,
    pub index_persist_count: u64,
    pub metadata_install_ns: u64,
    pub metadata_install_count: u64,
    pub root_adoption_ns: u64,
    pub root_adoption_count: u64,
    pub owner_adoption_ns: u64,
    pub owner_adoption_count: u64,
    pub obsolete_cleanup_ns: u64,
    pub obsolete_cleanup_count: u64,
    pub tail_pressure_count: u64,
    pub known_identity_pressure_count: u64,
    pub partition_window_pressure_count: u64,
    pub index_reservation_pressure_count: u64,
    pub already_available_count: u64,
    pub after_drain_relief_count: u64,
    pub after_existing_capture_relief_count: u64,
    pub after_new_capture_relief_count: u64,
    pub capacity_error_count: u64,
    pub progress_error_count: u64,
}

impl CheckpointDiagnosticSummary {
    fn record(&mut self, record: CheckpointDiagnostic) {
        self.records = self.records.saturating_add(1);
        match record.trigger {
            Some(CheckpointTrigger::Tail) => {
                self.tail_pressure_count = self.tail_pressure_count.saturating_add(1);
            }
            Some(CheckpointTrigger::KnownIdentities) => {
                self.known_identity_pressure_count =
                    self.known_identity_pressure_count.saturating_add(1);
            }
            Some(CheckpointTrigger::PartitionWindow) => {
                self.partition_window_pressure_count =
                    self.partition_window_pressure_count.saturating_add(1);
            }
            Some(CheckpointTrigger::IndexReservation) => {
                self.index_reservation_pressure_count =
                    self.index_reservation_pressure_count.saturating_add(1);
            }
            None => {}
        }
        match record.outcome {
            Some(CheckpointProgressOutcome::AlreadyAvailable) => {
                self.already_available_count = self.already_available_count.saturating_add(1);
            }
            Some(CheckpointProgressOutcome::AfterDrain) => {
                self.after_drain_relief_count = self.after_drain_relief_count.saturating_add(1);
            }
            Some(CheckpointProgressOutcome::AfterExistingCapture) => {
                self.after_existing_capture_relief_count =
                    self.after_existing_capture_relief_count.saturating_add(1);
            }
            Some(CheckpointProgressOutcome::AfterNewCapture) => {
                self.after_new_capture_relief_count =
                    self.after_new_capture_relief_count.saturating_add(1);
            }
            Some(CheckpointProgressOutcome::CapacityError) => {
                self.capacity_error_count = self.capacity_error_count.saturating_add(1);
            }
            Some(CheckpointProgressOutcome::ProgressError) => {
                self.progress_error_count = self.progress_error_count.saturating_add(1);
            }
            None => {}
        }
        macro_rules! add_span {
            ($source:ident, $sum:ident, $count:ident) => {
                if let Some(ns) = record.$source {
                    self.$sum = self.$sum.saturating_add(ns);
                    self.$count = self.$count.saturating_add(1);
                }
            };
        }
        add_span!(pressure_wait_ns, pressure_wait_ns, pressure_wait_count);
        add_span!(drain_ns, drain_ns, drain_count);
        add_span!(
            existing_capture_wait_ns,
            existing_capture_wait_ns,
            existing_capture_wait_count
        );
        add_span!(
            new_capture_wait_ns,
            new_capture_wait_ns,
            new_capture_wait_count
        );
        add_span!(
            staging_manifest_ns,
            staging_manifest_ns,
            staging_manifest_count
        );
        add_span!(
            node_prefix_wait_ns,
            node_prefix_wait_ns,
            node_prefix_wait_count
        );
        add_span!(index_prepare_ns, index_prepare_ns, index_prepare_count);
        add_span!(index_persist_ns, index_persist_ns, index_persist_count);
        add_span!(
            metadata_install_ns,
            metadata_install_ns,
            metadata_install_count
        );
        add_span!(root_adoption_ns, root_adoption_ns, root_adoption_count);
        add_span!(owner_adoption_ns, owner_adoption_ns, owner_adoption_count);
        add_span!(
            obsolete_cleanup_ns,
            obsolete_cleanup_ns,
            obsolete_cleanup_count
        );
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct WriteMetricsSnapshot {
    pub authority: AuthorityWriteStats,
    pub pipeline: PipelineStats,
    pub read_cache: ReadCacheStats,
    pub write_profile: WriteProfileSnapshot,
    pub checkpoint_diagnostics: CheckpointDiagnosticSummary,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct WriteProfileSnapshot {
    pub limit_per_connection: usize,
    pub expected_connections: usize,
    pub captured_connections: usize,
    pub sampled_requests: u64,
    pub profile_activation_errors: u64,
    pub profile_capture_errors: u64,
    /// Sampled checkpoint admissions (bounded by the per-connection limit).
    pub checkpoint_requests: u64,
    /// Terminal sampled controls, including errors; in-flight controls are excluded.
    pub checkpoint_completed: u64,
    /// Submit-to-retirement time for terminal samples, including server waits.
    pub checkpoint_wait_ns: u64,
    /// Append replies forwarded while terminal sampled controls were pending.
    pub checkpoint_append_completions: u64,
    /// Failed admission polls for sampled controls that were eventually admitted.
    /// Committed at admission; pre-admission failures are not counted.
    pub checkpoint_admission_stalls: u64,
    pub profile: super::tcp::ConnectionProfile,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ShutdownStats {
    pub drain_ns: u64,
    pub checkpoint_ns: u64,
    pub seal_ns: u64,
    pub join_ns: u64,
}

/// Diagnostic clocks only. Stages are nested and must not be summed with
/// dispatch/wait into a purported non-overlapping latency breakdown.
#[derive(Clone, Copy, Debug, Default)]
pub struct RemoteReadProfile {
    pub index_ns: u64,
    pub dispatch_ns: u64,
    pub wait_ns: u64,
    /// Caller-owned scalar live read, with no read-actor queue/return handoff.
    pub inline_read_ns: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
}

pub struct RemoteDatabase {
    core: Arc<write_core::core::WriteCore>,
    // The shared core joins every worker before this directory lock is dropped.
    _lock: DbLock,
}

impl RemoteDatabase {
    pub fn live_nvmeof_stats(&self) -> Option<super::nvmeof::live_metrics::LiveNvmeStats> {
        self.core
            .live_metrics
            .as_ref()
            .map(|metrics| metrics.snapshot())
    }

    pub fn index_stats(&self) -> super::native_index::NativeIndexStats {
        self.core.index.stats()
    }

    /// Logical retained index charge, including pageable historical references.
    pub fn index_history_bytes(&self) -> usize {
        self.core.index.retained_metadata_bytes()
    }

    /// Resident completed/in-flight authority bookkeeping, excluding pageable
    /// receipt files, allocator overhead and the separately reported index.
    pub fn metadata_history_bytes(&self) -> usize {
        self.core.metadata_history_bytes()
    }

    pub fn retention_floor(&self) -> IndexReadCut {
        self.core.index.retention_floor()
    }

    /// Capture and protect one published cut that the caller can keep: a
    /// standing-budget view takes its owner charge (`Capacity` when full).
    pub fn protected_published_cut(
        &self,
    ) -> Result<(IndexReadCut, super::native_index::IndexReadPin), LocalDbError> {
        self.ensure_healthy()?;
        let index = &self.core.index;
        let cut = index.latest_cut().map_err(LocalDbError::component)?;
        let pin = index
            .pin_cut(cut.clone())
            .map_err(LocalDbError::component)?;
        Ok((cut, pin))
    }

    pub fn pin_cut(
        &self,
        cut: IndexReadCut,
    ) -> Result<super::native_index::IndexReadPin, LocalDbError> {
        self.ensure_healthy()?;
        self.core
            .index
            .pin_cut(cut)
            .map_err(LocalDbError::component)
    }
    /// Resolve the authoritative owner of a key for benchmark evidence.
    ///
    /// The caller must have drained the database and prevent concurrent writes
    /// until this observation is complete. The returned owner comes from the
    /// exact pinned index reference; absence or a tombstone returns `None`.
    /// No transport or cache lock is held.
    #[cfg(all(target_os = "linux", feature = "test-utils"))]
    pub fn partition_for_key_for_benchmark(
        &self,
        keyspace: KeySpace,
        full_key: &[u8],
    ) -> Result<Option<u32>, LocalDbError> {
        let (cut, pin) = self.protected_published_cut()?;
        let reference = self
            .core
            .index
            .get(keyspace, full_key, cut)
            .map_err(LocalDbError::component)?;
        let partition = reference.map(|reference| reference.batch_address.partition);
        drop(pin);
        Ok(partition)
    }

    pub fn open(
        path: &Path,
        shape: &KeyShape,
        config: RemoteDatabaseConfig,
        _metrics: Arc<Metrics>,
    ) -> Result<Self, LocalDbError> {
        Self::open_inner(path, shape, config, None)
    }
    /// `open` over the rootless ordinary-file live I/O fixture (`buffered`
    /// selects buffered fixture I/O). Every client created during open selects
    /// the fixture before any live I/O, so WAL replay during open and later
    /// reads of earlier files use it too. No kernel controller or physical NVMe
    /// device is qualified by this route.
    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn open_with_local_live_fixture(
        path: &Path,
        shape: &KeyShape,
        config: RemoteDatabaseConfig,
        buffered: bool,
    ) -> Result<Self, LocalDbError> {
        Self::open_inner_with(
            path,
            shape,
            config,
            None,
            #[cfg(test)]
            None,
            Some(buffered),
        )
    }
    /// Exercise a coordinator failure after a durable Preparing epoch and a
    /// strict subset of node fences. No serving handle can escape this path.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn open_fail_after_fences(
        path: &Path,
        shape: &KeyShape,
        config: RemoteDatabaseConfig,
        _metrics: Arc<Metrics>,
        successful_fences: usize,
    ) -> Result<Self, LocalDbError> {
        if successful_fences == 0 || successful_fences >= config.endpoints.len() {
            return Err(LocalDbError::new(
                "fault injection needs a strict nonempty subset of fences",
            ));
        }
        Self::open_inner(
            path,
            shape,
            config,
            Some(OpenInterruption::AfterFences(successful_fences)),
        )
    }

    /// Open with a hook over the read-lane clients and a read-only config clone
    /// before `ReadService::from_clients`. The write route, worker clients and
    /// manifest identity use the unmodified `config`.
    #[cfg(test)]
    pub(in crate::distributed::remote_db) fn open_with_read_hook(
        path: &Path,
        shape: &KeyShape,
        config: RemoteDatabaseConfig,
        hook: impl FnOnce(&mut RemoteDatabaseConfig, &mut Vec<Client>),
    ) -> Result<Self, LocalDbError> {
        Self::open_inner_with(path, shape, config, None, Some(Box::new(hook)), Some(false))
    }

    fn open_inner(
        path: &Path,
        shape: &KeyShape,
        config: RemoteDatabaseConfig,
        interruption: Option<OpenInterruption>,
    ) -> Result<Self, LocalDbError> {
        Self::open_inner_with(
            path,
            shape,
            config,
            interruption,
            #[cfg(test)]
            None,
            #[cfg(any(test, feature = "test-utils"))]
            None,
        )
    }

    /// `open` over the unbuffered ordinary-file live I/O fixture (see
    /// `open_with_local_live_fixture`).
    #[cfg(test)]
    pub(crate) fn open_local_live_fixture(
        path: &Path,
        shape: &KeyShape,
        config: RemoteDatabaseConfig,
    ) -> Result<Self, LocalDbError> {
        Self::open_inner_local_fixture(path, shape, config, None)
    }

    /// `open_inner` over the ordinary-file live I/O fixture (see
    /// `open_local_live_fixture`), with an optional injected interruption.
    #[cfg(test)]
    pub(crate) fn open_inner_local_fixture(
        path: &Path,
        shape: &KeyShape,
        config: RemoteDatabaseConfig,
        interruption: Option<OpenInterruption>,
    ) -> Result<Self, LocalDbError> {
        Self::open_inner_with(path, shape, config, interruption, None, Some(false))
    }

    fn open_inner_with(
        path: &Path,
        shape: &KeyShape,
        config: RemoteDatabaseConfig,
        interruption: Option<OpenInterruption>,
        #[cfg(test)] read_hook: Option<ReadHook<'_>>,
        // `Some(buffered)` selects the ordinary-file live I/O fixture on every
        // client before any live I/O; `None` uses the real live route.
        #[cfg(any(test, feature = "test-utils"))] local_fixture: Option<bool>,
    ) -> Result<Self, LocalDbError> {
        if let Some(error) = super::sys::route_unavailable() {
            return Err(LocalDbError::new(error));
        }
        config.validate()?;
        config.validate_read_lane_pinning(config.effective_read_lanes())?;
        let mut index = NativeIndex::with_shards(
            shape,
            config.storage.database,
            config.max_index_bytes,
            config.index_shards,
        )
        .map_err(LocalDbError::component)?;
        let shape_yaml = shape.to_yaml().map_err(LocalDbError::component)?;
        // Keep the caller's shape: relocation filters and compactors are
        // closures, which the YAML form (manifest and owner check) drops.
        let shape = shape.clone();
        match fs::create_dir(path) {
            Ok(()) => File::open(
                path.parent()
                    .filter(|p| !p.as_os_str().is_empty())
                    .unwrap_or(Path::new(".")),
            )
            .and_then(|f| f.sync_all())
            .map_err(LocalDbError::component)?,
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(LocalDbError::component(e)),
        }
        let lock = DbLock::acquire(path, Duration::ZERO).map_err(LocalDbError::component)?;
        let path = path.canonicalize().map_err(LocalDbError::component)?;
        let mut manifest: AuthorityManifest = if path.join(MANIFEST).exists() {
            fencing_generations::read_authority_manifest(&path.join(MANIFEST))?
        } else {
            for entry in fs::read_dir(&path).map_err(LocalDbError::component)? {
                let name = entry.map_err(LocalDbError::component)?.file_name();
                if name != "LOCK" && name != format!(".{MANIFEST}.pending").as_str() {
                    return Err(LocalDbError::new(
                        "missing authority manifest beside retained data",
                    ));
                }
            }
            let manifest = AuthorityManifest {
                format: fencing_generations::AUTHORITY_FORMAT,
                storage: config.storage.clone(),
                mode: AuthorityMode {
                    history_persistence: MANIFEST_HISTORY.into(),
                    checkpoint_mode: None,
                    cell_checkpoint: None,
                    cell_checkpoint_obsolete: None,
                },
                endpoints: config.endpoints.clone(),
                shape_yaml: shape_yaml.clone(),
                replay_limits: config.replay_limits,
                live_nvme_identity: Some((
                    config.live_nvme.host_nqn.clone(),
                    config.live_nvme.host_id,
                )),
                live_nvme_owner_identities: config.live_nvme.owner_identities.clone(),
                next_generation: 1,
                generations: Vec::new(),
                trim_pending: None,
                adopt_trim: None,
                checkpoint: None,
                checkpoint_staging: None,
                checkpoint_obsolete: None,
            };
            persist_manifest(&path, &manifest)?;
            manifest
        };
        if manifest.storage != config.storage
            || manifest.endpoints != config.endpoints
            || manifest.shape_yaml != shape_yaml
            || manifest
                .live_nvme_identity
                .as_ref()
                .map(|(host_nqn, host_id)| (host_nqn.as_str(), *host_id))
                != Some((config.live_nvme.host_nqn.as_str(), config.live_nvme.host_id))
            || config.live_nvme.owner_identities != manifest.live_nvme_owner_identities
        {
            return Err(LocalDbError::new(
                "authority configuration differs from the recorded authority",
            ));
        }
        fencing_generations::validate(&manifest)?;
        manifest
            .replay_limits
            .validate()
            .map_err(LocalDbError::new)?;
        let live_pool = {
            let registry = super::nvmeof::live_connect::LiveConnectionRegistry::open(
                &path.join("live-nvme-connections"),
            )
            .map_err(LocalDbError::component)?;
            let clients = config
                .endpoints
                .len()
                .checked_mul(config.effective_read_lanes() + 1)
                .ok_or_else(|| LocalDbError::new("live client capacity overflow"))?;
            let controllers = clients
                .checked_mul(super::tcp::live_read::LIVE_READ_CACHE_CAPACITY + 1)
                .ok_or_else(|| LocalDbError::new("live controller capacity overflow"))?;
            Some(Arc::new(
                super::nvmeof::live_pool::LiveControllerPool::new(registry, controllers, clients)
                    .map_err(LocalDbError::component)?,
            ))
        };
        let mut live_open_guard = live_pool
            .as_ref()
            .map(|pool| transports::LivePoolOpenGuard::new(pool.clone(), config.timeout));
        let live_metrics = Some(super::nvmeof::live_metrics::LiveNvmeMetrics::new());
        let mut recovery_stats = RecoveryStats::default();
        let tls_started = Instant::now();
        let tls_config = config
            .tls
            .client_config_with_identity()
            .map_err(LocalDbError::component)?;
        recovery_stats.tls_config_ns = elapsed(tls_started);
        // Read transports prepare concurrently, but own no authority/index and
        // cannot issue reads until every node is fenced and recovery completes.
        let read_setup = transports::ReadSetup::start(
            config.clone(),
            shape.clone(),
            tls_config.clone(),
            live_pool.clone(),
            live_metrics.clone(),
            #[cfg(any(test, feature = "test-utils"))]
            local_fixture,
        )?;
        let connected = transports::connect(
            &config,
            &shape,
            &tls_config,
            1,
            live_pool.clone(),
            live_metrics.clone(),
            #[cfg(any(test, feature = "test-utils"))]
            local_fixture,
        )?;
        recovery_stats.connection_setup_ns = connected.elapsed_ns;
        recovery_stats.created_connections = connected.clients.len() as u64;
        let mut clients = connected.clients;
        for client in &clients {
            let status = client.connected_status();
            fencing_generations::check_owner_status(status.partition.partition, status, &manifest)?;
            recovery_stats.add_connection(client.connect_profile());
        }
        // One durable replacement folds a previous reservation into Aborted,
        // appends Reserved(g) and advances the never-reused counter before ANY
        // owner may activate g. A failed partial fence is aborted by the next open.
        let epoch = fencing_generations::reserve(&mut manifest, config.storage.database)?;
        persist_manifest(&path, &manifest)?;
        #[cfg(test)]
        if matches!(interruption, Some(OpenInterruption::AfterReservation)) {
            return Err(LocalDbError::new("injected interruption after reservation"));
        }
        let fencing_started = Instant::now();
        // Fault injection retains a strict partial-fence boundary. Normal opens
        // fence all endpoints concurrently, after the inactive durable reservation.
        let _ = interruption; // No fault-injection variants exist in production.
        #[cfg(any(test, feature = "test-utils"))]
        let fail_after_fences = match interruption {
            Some(OpenInterruption::AfterFences(count)) => Some(count),
            _ => None,
        };
        #[cfg(not(any(test, feature = "test-utils")))]
        let fence_count = clients.len();
        #[cfg(any(test, feature = "test-utils"))]
        let fence_count = fail_after_fences.unwrap_or(clients.len());
        let fenced = endpoint_map(&mut clients[..fence_count], |node, client| {
            let status = client.fence_to(epoch).map_err(LocalDbError::component)?;
            if !status.healthy
                || status.active_epoch != Some(epoch)
                || status.highest_fenced_generation != epoch
            {
                return Err(LocalDbError::new(
                    "node did not establish the complete fenced inventory",
                ));
            }
            let partition = config.endpoints[node].partition;
            // One frozen sweep of the complete ledger; no partial sweep is used.
            let entries = client
                .inventory_sweep(status.incarnation, status.inventory_revision)
                .map_err(LocalDbError::component)?;
            let history =
                fencing_generations::owner_history(partition, &entries, &manifest, epoch)?;
            if let Some(metrics) = &live_metrics {
                metrics.record_fence(super::nvmeof::live_metrics::LiveFenceEvidence {
                    partition: config.endpoints[node].partition,
                    authority_epoch: epoch,
                    target: client
                        .live_target_stats()
                        .map_err(LocalDbError::component)?,
                });
            }
            Ok((
                history,
                (
                    status.durability_tier,
                    durable_cut::OwnerFence {
                        partition: config.endpoints[node].partition,
                        incarnation: status.incarnation,
                        recovery_clean: status.recovery_clean,
                    },
                ),
            ))
        })?;
        let (histories, owners): (Vec<_>, Vec<_>) = fenced.into_iter().unzip();
        let (tiers, owner_fences): (Vec<_>, Vec<_>) = owners.into_iter().unzip();
        #[cfg(any(test, feature = "test-utils"))]
        if fail_after_fences.is_some() {
            return Err(LocalDbError::new(
                "injected coordinator interruption during partial fencing",
            ));
        }
        fencing_generations::check_stream_budget(&config, &histories)?;
        // Recovery is permitted only AFTER every registered node was fenced.
        // Unavailable RF1 nodes stop recovery; max observed sequence is not a
        // substitute for complete inventory or evidence of absent records.
        recovery_stats.fencing_ns = elapsed(fencing_started);
        let mut known = HashMap::new();
        let mut positions = BTreeMap::new();
        let mut recent = VecDeque::with_capacity(WAL_REPLAY_WINDOW_BATCHES);
        let mut recent_ids = HashMap::with_capacity(WAL_REPLAY_WINDOW_BATCHES);
        let mut latest = None;
        if let Some(checkpoint) = &manifest.checkpoint {
            load_checkpoint(
                &path,
                checkpoint,
                &manifest,
                &config,
                &shape,
                &histories,
                epoch,
                &mut index,
                &mut latest,
                &mut positions,
                &mut recent,
                &mut recent_ids,
                &mut recovery_stats,
            )?;
        }
        let committed_complete = positions
            .iter()
            .filter_map(|(key, position)| position.complete.then_some(*key))
            .collect();
        let committed_streams = positions.keys().copied().collect();
        let mut cut_plan = durable_cut::RecoveryPlan::new(
            durable_cut::CutRecord::load(&path, config.storage.database, epoch)?,
            config.storage.database,
            latest.map(|cut| cut.batch),
            &owner_fences,
        );
        let replay_started = Instant::now();
        replay_history(
            &mut clients,
            &histories,
            &fencing_generations::retained_below(&manifest, epoch),
            epoch,
            &config,
            &shape,
            &mut index,
            &mut known,
            &recent_ids,
            &mut latest,
            &mut positions,
            &mut cut_plan,
            &mut recovery_stats,
            #[cfg(test)]
            interruption,
        )?;
        recovery_stats.cut_discarded_batches = cut_plan.discarded;
        recovery_stats.cut_extended_batches = cut_plan.extended;
        recovery_stats.cut_skipped_versions = cut_plan.skipped;
        recovery_stats.cut_continuity_bound = cut_plan.continuity_bound;
        // Version B tracks this epoch only when every owner acknowledges at
        // the page cache (Native). Synchronous owners are per-batch durable
        // and never truncated.
        let tracked = tiers.iter().all(|tier| *tier == DurabilityTier::Native);
        let (cut_record, recovered_cut) = cut_plan.finish(epoch, tracked, &owner_fences);
        recovery_stats.recovered_cut = recovered_cut;
        recovery_stats.tail_replay_ns = elapsed(replay_started);
        // Current checkpoint/proofs and the complete tail have now validated.
        // Resolve only durably named cleanup intents before allowing more work.
        cleanup_checkpoint_artifacts(&path, &mut manifest, false)?;
        // One constant sparse-view quota for the serialized publisher: the
        // installed view and its replacement at max_pending_batches - 1
        // exclusions. No write reserves view quota, and no publication can then
        // fail on view capacity after its WAL write. A cut that leaves one read
        // call takes an owner charge from the view allowance instead (that
        // reader API fails with Capacity when full). Views held inside one read
        // stay uncharged, and no reader holds one while it waits for read
        // admission: extra memory <= max_pending_reads x
        // view_charge(max_pending_batches - 1), plus the transient and fixed
        // holders listed at `StandingViewBudget` (metric
        // `index_reader_held_view_bytes`).
        let view_budget = index
            .reserve_standing_view_budget(config.max_pending_batches - 1)
            .map_err(|error| {
                LocalDbError::new(format!(
                    "max_index_bytes cannot hold the standing sparse publication view \
                     budget for max_pending_batches: {error}"
                ))
            })?;
        let mut workers = Vec::new();
        let write_profile = Arc::new(Mutex::new(WriteProfileSnapshot {
            limit_per_connection: config.write_profile_limit,
            expected_connections: clients.len(),
            ..WriteProfileSnapshot::default()
        }));
        let write_timeline = Arc::new(WriteTimeline::with_owners(
            config.endpoints.iter().map(|endpoint| endpoint.partition),
        ));
        let concurrent_failure = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let first_failure = Arc::new(std::sync::OnceLock::new());
        let completion = Arc::new(write_core::completion::CoreCompletion::unbound());
        for client in clients {
            let partition = config.endpoints[workers.len()].partition;
            let worker = ClientWorker::start(
                client,
                config
                    .max_pending_batches
                    .max(config.effective_owner_append_occupancy()),
                concurrent_failure.clone(),
                WorkerDiagnostics {
                    timeout: config.timeout,
                    write_profile_limit: config.write_profile_limit,
                    write_profile: write_profile.clone(),
                    write_timeline: write_timeline.clone(),
                    first_failure: first_failure.clone(),
                    partition,
                    policy: WorkerPolicy::new(
                        config.partition_config(epoch, partition),
                        config.max_pending_batches,
                        config.effective_owner_append_occupancy(),
                        config.leased_submit_redrain,
                    )?,
                },
                completion.clone(),
            )
            .map_err(LocalDbError::component)?;
            workers.push(worker);
        }
        let connection_started = Instant::now();
        let prepared_reads = read_setup.finish()?;
        recovery_stats.read_lane_wait_ns = elapsed(connection_started);
        recovery_stats.read_lane_setup_ns = prepared_reads.elapsed_ns;
        recovery_stats.created_connections += prepared_reads.clients.len() as u64;
        #[cfg(test)]
        let mut prepared_reads = prepared_reads;
        #[cfg(test)]
        let read_config = read_hook.map(|hook| {
            let mut read_config = config.clone();
            hook(&mut read_config, &mut prepared_reads.clients);
            read_config
        });
        #[cfg(test)]
        let read_config = read_config.as_ref().unwrap_or(&config);
        #[cfg(not(test))]
        let read_config = &config;
        let reads = ReadService::from_clients(
            read_config,
            epoch,
            prepared_reads.clients,
            &mut recovery_stats,
            first_failure.clone(),
        )?;
        #[cfg(test)]
        if matches!(interruption, Some(OpenInterruption::AfterReadSetup)) {
            return Err(LocalDbError::new("injected interruption after read setup"));
        }
        // The truncation decided above and the tracked epoch are durable before
        // any caller can observe this epoch: a discarded batch never returns.
        if let Some(record) = &cut_record {
            durable_cut::persist_open_record(&path, record)?;
        }
        #[cfg(test)]
        if matches!(interruption, Some(OpenInterruption::AfterCutRecord)) {
            return Err(LocalDbError::new("injected interruption after cut record"));
        }
        fencing_generations::mark_served(&mut manifest, epoch)?;
        // Old suffixes validated against their original durable bound. Only a
        // fully recovered, not-yet-serving epoch may activate a larger bound.
        manifest.replay_limits = config.replay_limits;
        persist_manifest(&path, &manifest)?;
        #[cfg(test)]
        if matches!(interruption, Some(OpenInterruption::AfterServed)) {
            return Err(LocalDbError::new("injected interruption after served"));
        }
        let partition_indexes = config
            .endpoints
            .iter()
            .enumerate()
            .map(|(i, e)| (e.partition, i))
            .collect();
        let checkpoint_diagnostics =
            (config.write_profile_limit != 0).then(|| VecDeque::with_capacity(128));
        let reads = Arc::new(reads);
        // The durable-cut actor starts only after offline trim maintenance below:
        // trim may rewrite the open-time record, which the actor then owns.
        let durable_cut_config = config.durable_cut;
        let durable_cut_targets: Option<Vec<durable_cut::PollTarget>> = tracked.then(|| {
            config
                .endpoints
                .iter()
                .map(|endpoint| {
                    let mut tls = config.tls.clone();
                    tls.server_name = endpoint.server_name.clone();
                    let address = endpoint.address.clone();
                    let client_config = tls_config.clone();
                    let timeout = config.timeout;
                    // Owners register with the epoch-1 inventory, as in `transports`.
                    let expected = config.partition_config(1, endpoint.partition);
                    let shape = shape.clone();
                    durable_cut::PollTarget {
                        partition: endpoint.partition,
                        // The owner requires a Status handshake on every new
                        // session, and Status flushes the owner's logs. The
                        // first session therefore opens in `start`, before
                        // this epoch can append; a reconnect after a failed
                        // poll costs one extra owner sync.
                        connect: Box::new(move || {
                            Client::connect_shared(
                                &address,
                                &tls,
                                client_config.clone(),
                                timeout,
                                expected.clone(),
                                &shape,
                            )
                        }),
                    }
                })
                .collect()
        });
        let mut cut_record = cut_record;
        let core = write_core::core::WriteCore::new(write_core::core::OpenState {
            clients: workers,
            reads,
            read_cache: Arc::new(ReadCache::new(config.read_cache_bytes)),
            write_profile,
            write_timeline,
            live_pool,
            live_metrics,
            inventory: config.inventory(epoch),
            config,
            shape,
            path,
            epoch,
            index: Arc::new(index),
            partition_indexes,
            publisher: write_core::core::PublisherState {
                view_budget,
                known,
                stream_last: BTreeMap::new(),
                positions,
                committed_complete,
                committed_streams,
                recent,
                published_next: 0,
                upper: latest,
                dependents: HashMap::new(),
                latest,
                live_latest: latest,
                snapshot_generation: 0,
                manifest,
                retained_epochs: histories
                    .into_iter()
                    .map(|mut epochs| {
                        epochs.push(epoch);
                        epochs
                    })
                    .collect(),
                checkpoint_tail_batches: recovery_stats.tail_batches as usize,
                checkpoint_tail_bytes: recovery_stats.tail_bytes as usize,
                background_checkpoint: None,
                checkpoint_diagnostic_summary: CheckpointDiagnosticSummary {
                    available: checkpoint_diagnostics.is_some(),
                    ..CheckpointDiagnosticSummary::default()
                },
                checkpoint_diagnostics,
                next_checkpoint_diagnostic: 0,
                dropped_checkpoint_diagnostics: 0,
                last_checkpoint_diagnostic: None,
            },
            concurrent_failure,
            first_failure,
            stats: write_core::core::CoreStats {
                recovery: recovery_stats,
                ..write_core::core::CoreStats::default()
            },
        });
        completion.bind(&core);
        let database = Self { core, _lock: lock };
        // A successful recovery establishes a new baseline before admitting
        // another bounded tail, including after repeated crash/reopen cycles.
        if database.core.publisher.lock().checkpoint_tail_batches != 0 {
            let started = Instant::now();
            database.core.commit_checkpoint()?;
            database.core.stats.lock().recovery.checkpoint_commit_ns = elapsed(started);
        }
        // Offline fencing-generation maintenance (L1): no handle escaped, no
        // write was admitted and the durable-cut actor has not started.
        #[cfg(test)]
        let trim_crash = match interruption {
            Some(OpenInterruption::Trim(point)) => Some(point),
            _ => None,
        };
        fencing_generations::maintain(
            &database.core,
            &mut cut_record,
            #[cfg(test)]
            trim_crash,
        )?;
        if let (Some(record), Some(targets)) = (cut_record.filter(|_| tracked), durable_cut_targets)
        {
            let cut = durable_cut::DurableCut::start(
                database.core.path.clone(),
                epoch,
                durable_cut_config,
                record,
                targets,
            )?;
            database
                .core
                .durable_cut
                .set(cut)
                .map_err(|_| LocalDbError::new("durable cut actor already attached"))?;
        }
        if let Some(guard) = &mut live_open_guard {
            guard.disarm();
        }
        Ok(database)
    }

    pub fn backend_id(&self) -> &'static str {
        match &self.core.config.tls.transport {
            super::tcp::Transport::Tcp => BACKEND_ID,
            super::tcp::Transport::TcpTrusted => "tidehunter-tcp-trusted-rf1",
            super::tcp::Transport::Ofi { fabric, security } => match (&fabric.backend, security) {
                (super::tcp::FabricBackend::Verbs, super::tcp::FabricSecurity::MutualTls) => {
                    "tidehunter-ofi-verbs-mutual-tls-rf1"
                }
                (super::tcp::FabricBackend::Verbs, super::tcp::FabricSecurity::Trusted) => {
                    "tidehunter-ofi-verbs-trusted-rf1"
                }
                (super::tcp::FabricBackend::SoftwareTcp, super::tcp::FabricSecurity::MutualTls) => {
                    "tidehunter-ofi-software-tcp-mutual-tls-rf1"
                }
                (super::tcp::FabricBackend::SoftwareTcp, super::tcp::FabricSecurity::Trusted) => {
                    "tidehunter-ofi-software-tcp-trusted-rf1"
                }
            },
        }
    }
    pub fn epoch(&self) -> u64 {
        self.core.epoch
    }

    pub fn validate_cached_read(&self) -> Result<(), LocalDbError> {
        self.ensure_healthy()
    }

    /// The caller can keep the cut: a standing-budget view takes its owner
    /// charge (`Capacity` when full).
    pub fn published_cut(&self) -> Result<IndexReadCut, LocalDbError> {
        self.ensure_healthy()?;
        self.core
            .index
            .latest_cut()
            .map_err(LocalDbError::component)
    }

    /// Bounded index-only page. Values are fetched outside the authority lock
    /// using submit_record/wait/finish_record, retaining exact references.
    pub fn index_page(
        &self,
        ks: KeySpace,
        after_reduced: Option<&[u8]>,
        reverse: bool,
        cut: IndexReadCut,
    ) -> Result<Vec<(Bytes, ValueRecordReference)>, LocalDbError> {
        self.ensure_healthy()?;
        self.core
            .index
            .scan_reduced(ks, after_reduced, reverse, 256, cut)
            .map_err(LocalDbError::component)
    }

    pub fn index_reference(
        &self,
        ks: KeySpace,
        key: &[u8],
        cut: IndexReadCut,
    ) -> Result<Option<ValueRecordReference>, LocalDbError> {
        self.ensure_healthy()?;
        self.core
            .index
            .get(ks, key, cut)
            .map_err(LocalDbError::component)
    }

    fn ensure_healthy(&self) -> Result<(), LocalDbError> {
        self.core.ensure_healthy()
    }

    /// A failed or mismatched verified read is fail-stop for the whole engine.
    /// The read's exact error becomes the first recorded cause.
    pub(super) fn poison(&self, cause: &LocalDbError) {
        self.core.fail(write_core::write::copy_error(cause));
    }

    /// Enqueue without acknowledging durability. Capacity rejection has no effect.
    pub fn submit(
        &self,
        operation: [u8; 16],
        mutations: Vec<Mutation>,
    ) -> Result<BatchVersion, LocalDbError> {
        self.core.submit(operation, mutations)
    }

    /// Validate the entire window, including retry conflicts, before allocating
    /// any member. A window larger than one partition's metadata window spills
    /// into consecutive segments; each later segment waits for the earlier
    /// segments' publication. A capacity, deadline or closed error in a later
    /// segment can leave earlier segments admitted (an uncertain prefix).
    /// `drain` remains the durable acknowledgement.
    pub fn submit_many(
        &self,
        batches: &[([u8; 16], Vec<Mutation>)],
    ) -> Result<Vec<BatchVersion>, LocalDbError> {
        self.core.submit_many(batches)
    }

    pub fn drain(&self) -> Result<(), LocalDbError> {
        self.core.drain()
    }

    pub fn write_batch(
        &self,
        operation: [u8; 16],
        mutations: Vec<Mutation>,
    ) -> Result<BatchVersion, LocalDbError> {
        self.core.write(operation, mutations, None)
    }

    fn fetch(
        &mut self,
        references: &[ValueRecordReference],
        sample: &mut Option<ResolutionSample<'_>>,
    ) -> Result<Vec<Mutation>, LocalDbError> {
        self.ensure_healthy()?;
        if references.len() > 256 {
            return Err(LocalDbError::new("read window exceeds 256 records"));
        }
        let mut output = vec![None; references.len()];
        let mut missing = Vec::new();
        let mut slots = Vec::new();
        for (slot, reference) in references.iter().enumerate() {
            let clock = ResolutionSample::start(sample);
            let cached = self.core.read_cache.get(reference);
            if let Some(sample) = sample.as_ref() {
                sample.stage(ResolutionStage::CacheLookup, clock);
            }
            if let Some(value) = cached {
                if let Some(sample) = sample.as_ref() {
                    sample.member(MemberOutcome::CacheHit);
                }
                output[slot] = Some(value);
            } else {
                if let Some(sample) = sample.as_ref() {
                    sample.member(MemberOutcome::RemoteMiss);
                }
                slots.push(slot);
                missing.push(*reference);
            }
        }
        if !missing.is_empty() {
            let values = self.fetch_uncached(&missing, sample)?;
            if values.len() != missing.len() {
                let error = LocalDbError::new("read result count mismatch");
                self.poison(&error);
                return Err(error);
            }
            self.ensure_healthy()?;
            for ((slot, reference), value) in slots.into_iter().zip(missing).zip(values) {
                let prepared = self.core.read_cache.prepare_verified(reference, &value);
                self.core.read_cache.insert_prepared(prepared);
                output[slot] = Some(value);
            }
        } else if let Some(sample) = sample.as_mut() {
            sample.success();
        }
        self.ensure_healthy()?;
        Ok(output.into_iter().map(Option::unwrap).collect())
    }

    fn fetch_uncached(
        &mut self,
        references: &[ValueRecordReference],
        sample: &mut Option<ResolutionSample<'_>>,
    ) -> Result<Vec<Mutation>, LocalDbError> {
        if references.len() > 256 {
            return Err(LocalDbError::new("read window exceeds 256 records"));
        }
        let started = Instant::now();
        let mut groups: Vec<Vec<(usize, ValueRecordReference)>> =
            vec![Vec::new(); self.core.clients.len()];
        for (i, reference) in references.iter().enumerate() {
            let clock = ResolutionSample::start(sample);
            let node = self
                .core
                .partition_indexes
                .get(&reference.batch_address.partition)
                .copied()
                .ok_or_else(|| LocalDbError::new("reference names unregistered partition"));
            if let Ok(node) = node.as_ref() {
                groups[*node].push((i, *reference));
            }
            if let Some(sample) = sample.as_ref() {
                sample.stage(ResolutionStage::RoutePreparation, clock);
            }
            node?;
        }
        let values = vec![None; references.len()];
        let clock = ResolutionSample::start(sample);
        let batch = pending_read::ReadBatch::new(&self.core.reads, values, groups);
        if let Some(sample) = sample.as_ref() {
            sample.stage(ResolutionStage::BatchPreparation, clock);
        }
        let mut batch = batch?;
        // These private grouped fetch callers resolve the current committed cut
        // under the sole owner. Transfer its protection through physical read
        // completion, even if the aggregate caller exits early on an error.
        // The references are resolved, so only the retention floor is pinned.
        self.ensure_healthy()?;
        let clock = ResolutionSample::start(sample);
        let pin = self
            .core
            .index
            .pin_call_floor(IndexReadCut::Latest)
            .map_err(LocalDbError::component);
        if let Some(sample) = sample.as_ref() {
            sample.stage(ResolutionStage::PinPreparation, clock);
        }
        batch.pin = Some(pin?);
        if let Some(sample) = sample.as_mut() {
            sample.success();
        }
        while !batch.complete() {
            batch.dispatch(&self.core.reads)?;
            // If nothing belongs to this call, only external handles hold the
            // missing capacity; leave them to their owner and report pressure.
            batch.finish_one()?;
        }
        // Relaxed: diagnostic time only, never publication authority.
        self.core
            .value_read_ns
            .fetch_add(elapsed(started), std::sync::atomic::Ordering::Relaxed);
        self.ensure_healthy()?;
        batch
            .values
            .into_iter()
            .map(|v| v.ok_or_else(|| LocalDbError::new("missing remote read result")))
            .collect()
    }

    fn get_cut(
        &mut self,
        ks: KeySpace,
        key: &[u8],
        cut: IndexReadCut,
    ) -> Result<Option<Bytes>, LocalDbError> {
        let pending = self.prepare_get(ks, key, cut, None, true)?;
        self.finish_get(pending)
    }

    pub fn get(&mut self, ks: KeySpace, key: &[u8]) -> Result<Option<Bytes>, LocalDbError> {
        self.get_cut(ks, key, IndexReadCut::Latest)
    }

    /// Run the actual scalar reference/fetch path with extra stage clocks.
    /// Benchmark callers must keep these samples separate from ordinary gets.
    pub fn get_profiled(
        &mut self,
        ks: KeySpace,
        key: &[u8],
    ) -> Result<(Option<Bytes>, RemoteReadProfile), LocalDbError> {
        let mut profile = RemoteReadProfile::default();
        let pending = self.prepare_get(ks, key, IndexReadCut::Latest, Some(&mut profile), true)?;
        let value = self.finish_get_profiled(pending, Some(&mut profile))?;
        Ok((value, profile))
    }
    pub fn get_many(
        &mut self,
        ks: KeySpace,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<Bytes>>, LocalDbError> {
        let monitor = self.core.reads.resolution_monitor();
        let mut sample = monitor
            .as_ref()
            .and_then(|monitor| monitor.select_resolution(true, keys.len()));
        self.ensure_healthy()?;
        if keys.len() > 256 {
            return Err(LocalDbError::new("read batch exceeds 256 keys"));
        }
        let resolve_started = Instant::now();
        let clock = ResolutionSample::start(&sample);
        // One resolved cut for every key of the batch: no key sees a later publication.
        let cut = self.core.index.call_cut();
        if let Some(sample) = &sample {
            sample.stage(ResolutionStage::CutCapture, clock);
        }
        let mut slots = Vec::new();
        let mut references = Vec::new();
        for (slot, key) in keys.iter().enumerate() {
            let clock = ResolutionSample::start(&sample);
            let reference = self
                .core
                .index
                .get(ks, key, cut.clone())
                .map_err(LocalDbError::component);
            if let Some(sample) = &sample {
                sample.stage(ResolutionStage::IndexLookup, clock);
            }
            match reference {
                Ok(Some(reference)) => {
                    slots.push(slot);
                    references.push(reference);
                }
                Ok(None) => {
                    if let Some(sample) = &sample {
                        sample.member(MemberOutcome::IndexMiss);
                    }
                }
                Err(error) => {
                    if let Some(sample) = &sample {
                        sample.member(MemberOutcome::Error);
                    }
                    return Err(error);
                }
            }
        }
        self.core.read_resolve_ns.fetch_add(
            elapsed(resolve_started),
            std::sync::atomic::Ordering::Relaxed,
        );
        let mut output = vec![None; keys.len()];
        for (slot, mutation) in slots.into_iter().zip(self.fetch(&references, &mut sample)?) {
            output[slot] = Self::point_value(ks, &keys[slot], mutation)?;
        }
        if let Some(sample) = &mut sample {
            sample.success();
        }
        Ok(output)
    }
    pub fn exists(&mut self, ks: KeySpace, key: &[u8]) -> Result<bool, LocalDbError> {
        self.ensure_healthy()?;
        let started = Instant::now();
        let reference = self
            .core
            .index
            .get(ks, key, IndexReadCut::Latest)
            .map_err(LocalDbError::component)?;
        self.core
            .read_resolve_ns
            .fetch_add(elapsed(started), std::sync::atomic::Ordering::Relaxed);
        let Some(reference) = reference else {
            return Ok(false);
        };
        if !self.core.shape.ks(ks).need_check_index_key() {
            return Ok(true);
        }
        match self.fetch(&[reference], &mut None)?.remove(0) {
            Mutation::Put {
                keyspace,
                key: actual,
                ..
            } => Ok(keyspace == ks && actual.as_ref() == key),
            Mutation::Delete { .. } => Err(LocalDbError::new(
                "live index reference resolves to tombstone",
            )),
        }
    }
    pub fn checkpoint(&mut self) -> Result<LocalCheckpoint, LocalDbError> {
        self.drain()?;
        let latest = self.core.publisher.lock().latest;
        Ok(LocalCheckpoint {
            database: self.core.config.storage.database,
            cut: latest,
            _pin: Some(
                self.core
                    .index
                    .pin_cut(latest.map_or(IndexReadCut::BeforeAll, IndexReadCut::At))
                    .map_err(LocalDbError::component)?,
            ),
        })
    }
    pub fn get_at(
        &mut self,
        ks: KeySpace,
        key: &[u8],
        checkpoint: &LocalCheckpoint,
    ) -> Result<Option<Bytes>, LocalDbError> {
        if checkpoint.database != self.core.config.storage.database {
            return Err(LocalDbError::new("checkpoint belongs to another database"));
        }
        self.get_cut(
            ks,
            key,
            checkpoint
                .cut
                .map_or(IndexReadCut::BeforeAll, IndexReadCut::At),
        )
    }
    // Checkpoint freezing, background persistence and durable adoption live in
    // checkpoint_lifecycle; the authority owns every resulting root transition.

    pub fn flush_index(&self) -> Result<(), LocalDbError> {
        self.core.flush_index()
    }
    pub fn unload_index(&self) -> Result<(), LocalDbError> {
        self.flush_index()
    }
    pub fn pipeline_stats(&self) -> PipelineStats {
        self.core.pipeline_stats()
    }
    pub fn recovery_stats(&self) -> RecoveryStats {
        self.core.stats.lock().recovery
    }
    /// Durable-cut progress of this epoch; `enabled` is false when untracked.
    pub fn durable_cut_stats(&self) -> DurableCutStats {
        self.core.durable_cut_stats()
    }
    pub fn authority_write_stats(&self) -> AuthorityWriteStats {
        self.core.authority_write_stats()
    }
    pub fn checkpoint_diagnostic_summary(&self) -> CheckpointDiagnosticSummary {
        self.core.publisher.lock().checkpoint_diagnostic_summary
    }

    /// Cursor for bounded checkpoint diagnostics. A zero-profile database has
    /// no diagnostic ring and returns an empty snapshot.
    pub fn checkpoint_diagnostic_cursor(&self) -> u64 {
        self.core.publisher.lock().next_checkpoint_diagnostic
    }

    pub fn checkpoint_diagnostics_since(&self, cursor: u64) -> CheckpointDiagnosticsSnapshot {
        let publisher = self.core.publisher.lock();
        CheckpointDiagnosticsSnapshot {
            next_cursor: publisher.next_checkpoint_diagnostic,
            dropped_records: publisher.dropped_checkpoint_diagnostics,
            records: publisher
                .checkpoint_diagnostics
                .as_ref()
                .into_iter()
                .flat_map(|records| records.iter())
                .filter(|record| record.cursor >= cursor)
                .copied()
                .collect(),
        }
    }
    /// Arm fresh bounded append and checkpoint profiles after warm-up.
    /// Finish any pre-existing checkpoint before resetting the phase counters.
    pub fn arm_write_profile(&self, limit: usize) -> Result<(), LocalDbError> {
        self.core.arm_write_profile(limit)
    }
    /// Capture partial opt-in append profiles after all admitted writes have drained.
    /// With a zero limit, this returns without issuing any diagnostic RPC.
    pub fn finish_write_profile(&self) -> Result<(), LocalDbError> {
        self.core.finish_write_profile()
    }
    pub fn shutdown_stats(&self) -> ShutdownStats {
        self.core.stats.lock().shutdown
    }
    pub fn read_cache_stats(&self) -> ReadCacheStats {
        self.core.read_cache.stats()
    }
    /// Per-shard load of the coordinator index (lock acquisitions, contention, waits).
    pub fn index_shard_stats(&self) -> super::native_index::IndexShardStats {
        self.core.index.shard_stats()
    }
    /// Per-shard load of the coordinator read cache.
    pub fn read_cache_shard_stats(&self) -> ReadCacheShardStats {
        self.core.read_cache.shard_stats()
    }
    pub fn per_keyspace_read_cache_stats(
        &self,
    ) -> [PerKeyspaceReadCacheStats; KEYSPACE_COUNTER_COUNT] {
        self.core.read_cache.per_keyspace_stats()
    }
    /// Enable private Sui checkpoint cache policy using live shape names.
    pub(super) fn configure_sui_read_cache(&mut self) {
        let policy = ReadCachePolicy::for_sui_shape(&self.core.shape);
        self.core.read_cache.configure_policy(policy);
    }

    /// Discard derivative cache contents without changing index/WAL state.
    pub fn clear_read_cache(&mut self) {
        self.core.read_cache.clear();
    }

    /// Outside measured operations: drain first so all node clocks cover a
    /// complete interval. Node sums can overlap and are not elapsed wall time.
    /// # Errors
    /// Returns pending-write, connection or remote diagnostic errors.
    pub fn connection_profiles(
        &mut self,
        enabled: bool,
        reset: bool,
    ) -> Result<Vec<(u32, super::tcp::ConnectionProfile)>, LocalDbError> {
        self.ensure_healthy()?;
        self.core.reads.ensure_idle()?;
        self.drain()?;
        let read_profiles = self.core.reads.profiles(enabled, reset)?;
        self.core
            .clients
            .iter()
            .zip(&self.core.config.endpoints)
            .zip(read_profiles)
            .map(|((client, endpoint), reads)| {
                let mut profile =
                    client.control(move |client| client.connection_profile(enabled, reset))?;
                profile.add(reads);
                Ok((endpoint.partition, profile))
            })
            .collect()
    }

    #[cfg(all(target_os = "linux", feature = "test-utils"))]
    /// # Errors
    /// Returns pending-write, connection or remote Linux evidence errors.
    pub fn read_evidence_for_benchmark(
        &mut self,
        evict: bool,
    ) -> Result<Vec<(u32, crate::read_probe::ReadEvidence)>, LocalDbError> {
        self.ensure_healthy()?;
        self.core.reads.ensure_idle()?;
        self.drain()?;
        if evict {
            self.clear_read_cache();
        }
        let epoch = self.core.epoch;
        self.core
            .clients
            .iter()
            .zip(&self.core.config.endpoints)
            .map(|(client, endpoint)| {
                client
                    .control(move |client| client.read_evidence_for_benchmark(epoch, evict))
                    .map(|p| (endpoint.partition, p))
            })
            .collect()
    }

    #[cfg(all(target_os = "linux", feature = "test-utils"))]
    /// Collect each storage process outside timed operations, without eviction.
    /// # Errors
    /// Returns pending-read, write, authenticated transport or accounting errors.
    pub fn process_resources_for_benchmark(
        &mut self,
    ) -> Result<Vec<(u32, crate::read_probe::ProcessResources)>, LocalDbError> {
        self.ensure_healthy()?;
        self.core.reads.ensure_idle()?;
        self.drain()?;
        let epoch = self.core.epoch;
        self.core
            .clients
            .iter()
            .zip(&self.core.config.endpoints)
            .map(|(client, endpoint)| {
                client
                    .control(move |client| client.process_resources_for_benchmark(epoch))
                    .map(|resources| (endpoint.partition, resources))
            })
            .collect()
    }

    pub fn stats(&self) -> LocalDbStats {
        self.core.database_stats()
    }
    pub fn storage_stats(&self) -> Result<PartitionStats, LocalDbError> {
        let mut total = PartitionStats::default();
        for client in &self.core.clients {
            let stats = receive(client.stats()?)?;
            total.appended_groups += stats.appended_groups;
            total.persistence_barriers += stats.persistence_barriers;
            total.namespace_syncs += stats.namespace_syncs;
            total.inventory_witnesses += stats.inventory_witnesses;
            total.live_frontier_file_syncs += stats.live_frontier_file_syncs;
            total.live_frontier_namespace_syncs += stats.live_frontier_namespace_syncs;
            total.wal_file_syncs += stats.wal_file_syncs;
            total.live_frontier_requests += stats.live_frontier_requests;
            total.live_frontier_installs += stats.live_frontier_installs;
            total.live_frontier_bytes += stats.live_frontier_bytes;
            total.live_frontier_deferred += stats.live_frontier_deferred;
            total.read_persist_requests += stats.read_persist_requests;
            total.cold_grant_syncs += stats.cold_grant_syncs;
            total.registered_records += stats.registered_records;
            total.reference_metadata_bytes += stats.reference_metadata_bytes;
        }
        Ok(total)
    }
    pub fn shutdown(&self) -> Result<(), LocalDbError> {
        self.core.shutdown()
    }
}

impl Drop for RemoteDatabase {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

struct ReplayPage {
    batches: VecDeque<(CompleteBatch, DurableBatch)>,
    next: Option<usize>,
    last: Option<BatchVersion>,
    position: StreamPosition,
    replayed_end: u64,
    replay_last: Option<(u64, u32)>,
}

fn fill_replay_page(
    client: &mut Client,
    state: &mut ReplayPage,
    epoch: u64,
    authority_epoch: u64,
    cursor: usize,
    limit: usize,
) -> Result<(), LocalDbError> {
    let mut page = client
        .replay_live_wal_page(
            authority_epoch,
            epoch,
            cursor as u64,
            limit,
            state.position.durable_end,
        )
        .map_err(LocalDbError::component)?;
    if page.batches.len() > limit || page.next.is_some_and(|next| next <= cursor) {
        return Err(LocalDbError::new("invalid bounded recovery page"));
    }
    if let Some((_, receipt)) = page.batches.last() {
        state.replay_last = Some((
            receipt.appended().address().offset,
            receipt.appended().address().len,
        ));
        state.replayed_end = state
            .replayed_end
            .max(receipt.appended().address().offset + u64::from(receipt.appended().address().len));
    }
    let last_offset = state.position.last_offset;
    page.batches.retain(|(_, receipt)| {
        last_offset.is_none_or(|last| receipt.appended().address().offset > last)
    });
    for (batch, _) in &page.batches {
        if batch.version().epoch != epoch || state.last.is_some_and(|last| last >= batch.version())
        {
            return Err(LocalDbError::new("recovery stream is not strictly ordered"));
        }
        state.last = Some(batch.version());
    }
    state.next = page.next;
    state.batches.extend(page.batches);
    Ok(())
}

fn checkpoint_metadata_fits(streams: usize, receipts: usize) -> bool {
    // Fixed-width bincode: a receipt is 104 bytes; a stream including its
    // vector header is 84. Charge 128 for each and reserve 1 MiB for schema
    // and fixed fields (the authority's existing CRC metadata limit). Streams
    // are the actual retained (owner, generation) pairs, not a lifetime quota.
    streams
        .checked_add(receipts)
        .and_then(|entries| entries.checked_mul(128))
        .and_then(|bytes| bytes.checked_add(1024 * 1024))
        .is_some_and(|bytes| bytes as u64 <= MAX_CHECKPOINT_METADATA_BYTES)
}

/// K-way merge of bounded pages. Never retain the database's value history in
/// coordinator RAM merely to sort it; the index receives one batch at a time.
#[allow(clippy::too_many_arguments)] // Keep independently owned recovery state explicit.
fn replay_history(
    clients: &mut [Client],
    histories: &[Vec<u64>],
    retained: &[(u64, bool)],
    current_epoch: u64,
    config: &RemoteDatabaseConfig,
    shape: &KeyShape,
    index: &mut NativeIndex,
    known: &mut HashMap<BatchId, AppendedBatch>,
    recent_ids: &HashMap<BatchId, (BatchVersion, super::types::PayloadDigest)>,
    latest: &mut Option<MutationVersion>,
    positions: &mut BTreeMap<(u32, u64), StreamPosition>,
    cut: &mut durable_cut::RecoveryPlan,
    stats: &mut RecoveryStats,
    #[cfg(test)] interruption: Option<OpenInterruption>,
) -> Result<(), LocalDbError> {
    let mut index = index.writer();
    let page_limit = config
        .recovery_page_limit()?
        .max(config.storage.group_limits.max_batches as usize);
    let mut previous = latest.map(|cut| cut.batch);
    // Only the retained union, never `1..current_epoch`: trimmed and pending
    // generations get no default position. Aborted streams must be empty.
    for &(epoch, aborted) in retained {
        let mut pages = endpoint_map(clients, |node, client| {
            let mut page = ReplayPage {
                batches: VecDeque::new(),
                next: None,
                last: None,
                position: positions
                    .get(&(config.endpoints[node].partition, epoch))
                    .cloned()
                    .unwrap_or(StreamPosition {
                        partition: config.endpoints[node].partition,
                        epoch,
                        replay_from: 0,
                        last_offset: None,
                        durable_end: 0,
                        complete: false,
                    }),
                replayed_end: 0,
                replay_last: None,
            };
            if histories[node].contains(&epoch) && !page.position.complete {
                let mut cursor =
                    usize::try_from(page.position.replay_from).map_err(LocalDbError::component)?;
                loop {
                    fill_replay_page(client, &mut page, epoch, current_epoch, cursor, page_limit)?;
                    if !page.batches.is_empty() || page.next.is_none() {
                        break;
                    }
                    cursor = page.next.unwrap();
                }
            }
            Ok(page)
        })?;
        let mut ready = BinaryHeap::new();
        for (node, page) in pages.iter().enumerate() {
            if let Some((batch, _)) = page.batches.front() {
                ready.push(Reverse((batch.version(), node)));
            }
        }
        while let Some(Reverse((version, node))) = ready.pop() {
            let (batch, durable) = pages[node].batches.pop_front().unwrap();
            // A batch above the durable cut is not published: not indexed, not
            // a retry identity, not a version floor. It precedes the duplicate
            // check because a later epoch may hold its retried identity.
            if cut.keep(version) {
                if aborted {
                    return Err(LocalDbError::new(format!(
                        "aborted fencing generation {epoch} contains accepted WAL data"
                    )));
                }
                if previous.is_some_and(|old| old >= version)
                    || known.contains_key(&batch.id())
                    || recent_ids.contains_key(&batch.id())
                {
                    return Err(LocalDbError::new(
                        "duplicate authoritative identity or logical version",
                    ));
                }
                previous = Some(version);
                index
                    .check_capacity(
                        index
                            .estimated_batch_bytes(&batch)
                            .map_err(LocalDbError::component)?,
                    )
                    .map_err(LocalDbError::component)?;
                PreparedNativePublication::prepare_authenticated(
                    shape,
                    config.storage.batch_limits,
                    &batch,
                    durable,
                )
                .map_err(LocalDbError::component)?
                .publish(&mut index)
                .map_err(LocalDbError::component)?;
                index.promote_committed().map_err(LocalDbError::component)?;
                // Replay is the only publisher: make the replayed prefix visible.
                index.publish_latest();
                *latest = batch.mutation_version(batch.mutations().len() - 1);
                known.insert(batch.id(), durable.appended());
                stats.tail_batches += 1;
                stats.tail_bytes += batch.encoded().len() as u64;
                #[cfg(test)]
                if matches!(interruption, Some(OpenInterruption::DuringReplay)) {
                    return Err(LocalDbError::new("injected interruption during WAL replay"));
                }
            }
            drop(batch);
            if pages[node].batches.is_empty() && pages[node].next.is_some() {
                // Refill independently depleted/half-consumed streams together.
                // Available slots, not another page allocation, bound lookahead:
                // each node retains at most page_limit complete batches in total.
                let mut streams: Vec<_> = clients.iter_mut().zip(pages.iter_mut()).collect();
                endpoint_map(&mut streams, |_, (client, page)| {
                    if page.batches.is_empty()
                        && let Some(next) = page.next
                    {
                        let available = page_limit - page.batches.len();
                        fill_replay_page(client, page, epoch, current_epoch, next, available)?;
                    }
                    Ok(())
                })?;
            }
            if let Some((batch, _)) = pages[node].batches.front() {
                ready.push(Reverse((batch.version(), node)));
            }
        }
        for page in pages {
            let mut position = page.position;
            if let Some((offset, _)) = page.replay_last {
                position.replay_from = offset - offset % config.storage.fragment_bytes;
                position.last_offset = Some(offset);
            }
            position.durable_end = position.durable_end.max(page.replayed_end);
            position.complete = true;
            positions.insert((position.partition, epoch), position);
        }
    }
    Ok(())
}

fn encode_checkpoint(image: &impl Serialize) -> Result<Vec<u8>, LocalDbError> {
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_limit(MAX_CHECKPOINT_METADATA_BYTES)
        .serialize(image)
        .map_err(LocalDbError::component)
}

fn read_checkpoint<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T, LocalDbError> {
    let file = File::open(path).map_err(LocalDbError::component)?;
    if file.metadata().map_err(LocalDbError::component)?.len() > MAX_CHECKPOINT_METADATA_BYTES {
        return Err(LocalDbError::new(
            "checkpoint receipt metadata exceeds bound",
        ));
    }
    let mut bytes = Vec::new();
    file.take(MAX_CHECKPOINT_METADATA_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(LocalDbError::component)?;
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_limit(MAX_CHECKPOINT_METADATA_BYTES)
        .reject_trailing_bytes()
        .deserialize(&bytes)
        .map_err(LocalDbError::component)
}

fn checkpoint_file_digest(path: &Path) -> Result<(u64, [u8; 32]), LocalDbError> {
    let metadata = fs::symlink_metadata(path).map_err(LocalDbError::component)?;
    if !metadata.file_type().is_file() || metadata.len() > 1 << 30 {
        return Err(LocalDbError::new(
            "checkpoint file is not a bounded regular file",
        ));
    }
    let mut file = File::open(path).map_err(LocalDbError::component)?;
    let mut digest = Blake2b::<U32>::new();
    let mut buffer = [0u8; 64 * 1024];
    let mut length = 0;
    loop {
        let read = file.read(&mut buffer).map_err(LocalDbError::component)?;
        if read == 0 {
            break;
        }
        length += read as u64;
        if length > metadata.len() {
            return Err(LocalDbError::new("checkpoint file changed while reading"));
        }
        digest.update(&buffer[..read]);
    }
    if length != metadata.len() {
        return Err(LocalDbError::new("checkpoint file truncated"));
    }
    Ok((length, digest.finalize().into()))
}

#[allow(clippy::too_many_arguments)] // Validate against the same fenced state used by tail replay.
fn load_checkpoint(
    path: &Path,
    commit: &CheckpointCommit,
    manifest: &AuthorityManifest,
    config: &RemoteDatabaseConfig,
    shape: &KeyShape,
    histories: &[Vec<u64>],
    authority_epoch: u64,
    index: &mut NativeIndex,
    latest: &mut Option<MutationVersion>,
    positions: &mut BTreeMap<(u32, u64), StreamPosition>,
    recent: &mut VecDeque<RecentReceipt>,
    recent_ids: &mut HashMap<BatchId, (BatchVersion, super::types::PayloadDigest)>,
    stats: &mut RecoveryStats,
) -> Result<(), LocalDbError> {
    let started = Instant::now();
    if commit.generation == 0
        || !fencing_generations::is_served(manifest, commit.epoch)
        || commit.epoch >= authority_epoch
    {
        return Err(LocalDbError::new(
            "checkpoint has no committed authority generation",
        ));
    }
    let metadata_path = path.join(commit.metadata_name());
    if commit.metadata_bytes > MAX_CHECKPOINT_METADATA_BYTES
        || checkpoint_file_digest(&metadata_path)?
            != (commit.metadata_bytes, commit.metadata_digest)
    {
        return Err(LocalDbError::new(
            "committed checkpoint metadata digest differs",
        ));
    }
    let mut header = [0; 4];
    File::open(&metadata_path)
        .and_then(|mut file| std::io::Read::read_exact(&mut file, &mut header))
        .map_err(LocalDbError::component)?;
    let format: u32 = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .deserialize(&header)
        .map_err(LocalDbError::component)?;
    if format != 3 {
        return Err(LocalDbError::new(
            "checkpoint image format is not the WAL-replay format 3",
        ));
    }
    let mut image: CheckpointImageV3 = read_checkpoint(&metadata_path)?;
    for position in std::mem::take(&mut image.positions) {
        if !config.storage.partitions.contains(&position.partition)
            || !histories[config
                .endpoints
                .iter()
                .position(|endpoint| endpoint.partition == position.partition)
                .unwrap()]
            .contains(&position.epoch)
            || position.epoch > commit.epoch
            || position.replay_from % config.storage.fragment_bytes != 0
            || position.last_offset.is_some_and(|offset| {
                offset < position.replay_from || offset >= position.durable_end
            })
            || positions
                .insert((position.partition, position.epoch), position)
                .is_some()
        {
            return Err(LocalDbError::new("invalid checkpoint WAL position"));
        }
    }
    if image.recent.len() > WAL_REPLAY_WINDOW_BATCHES {
        return Err(LocalDbError::new("checkpoint retry window exceeds bound"));
    }
    for receipt in std::mem::take(&mut image.recent) {
        if receipt.id.database != config.storage.database
            || image.cut.is_none_or(|cut| receipt.version > cut.batch)
            || recent
                .back()
                .is_some_and(|last| last.version >= receipt.version)
            || recent_ids
                .insert(receipt.id, (receipt.version, receipt.digest))
                .is_some()
        {
            return Err(LocalDbError::new("invalid checkpoint retry identity"));
        }
        recent.push_back(receipt);
    }
    if image.format != format
        || image.database != config.storage.database
        || image.shape_yaml != manifest.shape_yaml
        || image.authority_epoch != commit.epoch
        || image.generation != commit.generation
        || image.cut.is_some_and(|cut| cut.batch.epoch > commit.epoch)
        || image.published_next
            != image
                .cut
                .filter(|cut| cut.batch.epoch == commit.epoch)
                .map_or(0, |cut| cut.batch.sequence.saturating_add(1))
    {
        return Err(LocalDbError::new(
            "checkpoint domain, generation or publication cut differs",
        ));
    }
    let index_path = path.join(commit.index_name());
    if checkpoint_file_digest(&index_path)? != (image.index_bytes, image.index_digest) {
        return Err(LocalDbError::new(
            "committed checkpoint index digest differs",
        ));
    }
    let restored = NativeIndex::open_snapshot_with_shards(
        &index_path,
        shape,
        config.storage.database,
        config.max_index_bytes,
        config.index_shards,
    )
    .map_err(LocalDbError::component)?;
    if restored.call_cut() != image.cut.map_or(IndexReadCut::BeforeAll, IndexReadCut::At) {
        return Err(LocalDbError::new(
            "checkpoint index does not match exact published cut",
        ));
    }
    let mut reachable = restored.reachable_files();
    reachable.sort_unstable();
    reachable.dedup();
    if reachable != commit.index_files {
        return Err(LocalDbError::new(
            "checkpoint cell-file inventory differs from rooted index",
        ));
    }
    stats.checkpoint_load_ns = elapsed(started);
    *index = restored;
    *latest = image.cut;
    stats.checkpoint_loaded = true;
    Ok(())
}

fn cleanup_checkpoint_artifacts(
    path: &Path,
    manifest: &mut AuthorityManifest,
    _interrupt_after_unlink: bool,
) -> Result<(), LocalDbError> {
    let current = manifest
        .checkpoint
        .as_ref()
        .map(|commit| (commit.epoch, commit.generation));
    let staged = manifest.checkpoint_staging.as_ref();
    let obsolete = manifest
        .checkpoint_obsolete
        .as_ref()
        .map(|commit| CheckpointFiles {
            epoch: commit.epoch,
            generation: commit.generation,
            index_files: commit.index_files.clone(),
        });
    if staged.is_none() && obsolete.is_none() {
        return Ok(());
    }
    // The CRC-protected ledger names only generations reserved by this authority.
    // Validate every intent before deleting anything, especially the live root.
    for (files, retiring) in [(staged, false), (obsolete.as_ref(), true)] {
        if let Some(files) = files {
            let identity = (files.epoch, files.generation);
            // A retiring commit is strictly older than the current root; its
            // producer may already be trimmed. A staged one must be served.
            if files.generation == 0
                || (!retiring && !fencing_generations::is_served(manifest, files.epoch))
                || if retiring {
                    current.is_none_or(|current| identity >= current)
                } else {
                    current.is_some_and(|current| identity <= current)
                }
            {
                return Err(LocalDbError::new(
                    "checkpoint cleanup intent conflicts with committed authority",
                ));
            }
            for name in files.names() {
                if manifest
                    .checkpoint
                    .as_ref()
                    .is_some_and(|current| current.index_files.binary_search(&name).is_ok())
                {
                    continue;
                }
                if Path::new(&name).components().count() != 1 {
                    return Err(LocalDbError::new(
                        "checkpoint cleanup filename is not relative",
                    ));
                }
                match fs::symlink_metadata(path.join(name)) {
                    Ok(metadata) if metadata.file_type().is_file() => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(LocalDbError::component(error)),
                    Ok(_) => {
                        return Err(LocalDbError::new(
                            "checkpoint cleanup target is not a regular file",
                        ));
                    }
                }
            }
        }
    }
    for files in [staged, obsolete.as_ref()].into_iter().flatten() {
        for name in files.names() {
            if manifest
                .checkpoint
                .as_ref()
                .is_some_and(|current| current.index_files.binary_search(&name).is_ok())
            {
                continue;
            }
            match fs::remove_file(path.join(name)) {
                Ok(()) => {}
                // An interrupted earlier cleanup may already have unlinked it.
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(LocalDbError::component(error)),
            }
        }
    }
    #[cfg(test)]
    if _interrupt_after_unlink {
        return Err(LocalDbError::new(
            "injected interruption after checkpoint artifact unlink",
        ));
    }
    // Unlink never modifies mapped immutable bytes. Linux keeps existing inode
    // mappings alive until their last owner drops; names are never reused.
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(LocalDbError::component)?;
    manifest.checkpoint_staging = None;
    manifest.checkpoint_obsolete = None;
    persist_manifest(path, manifest)
}

fn persist_manifest(path: &Path, manifest: &AuthorityManifest) -> Result<(), LocalDbError> {
    install_metadata(
        path,
        MANIFEST,
        &encode_metadata(manifest).map_err(LocalDbError::component)?,
    )
    .map_err(LocalDbError::component)
}
fn elapsed(start: Instant) -> u64 {
    start.elapsed().as_nanos().min(u64::MAX as u128) as u64
}

/// Bound independent endpoint operations by the available CPU parallelism.
/// Scoped ownership joins every started operation on errors/panics.
fn endpoint_map<T: Send, R: Send>(
    items: &mut [T],
    operation: impl Fn(usize, &mut T) -> Result<R, LocalDbError> + Sync,
) -> Result<Vec<R>, LocalDbError> {
    let mut output = Vec::with_capacity(items.len());
    let parallelism = crate::parallelism::available();
    for (wave, chunk) in items.chunks_mut(parallelism).enumerate() {
        let results = thread::scope(|scope| {
            let operation = &operation;
            let mut handles = Vec::with_capacity(chunk.len());
            let mut results = Vec::with_capacity(chunk.len());
            for (slot, item) in chunk.iter_mut().enumerate() {
                match thread::Builder::new()
                    .spawn_scoped(scope, move || operation(wave * parallelism + slot, item))
                {
                    Ok(handle) => {
                        handles.push(handle);
                        results.push(None);
                    }
                    Err(error) => {
                        results.push(Some(Err(LocalDbError::component(error))));
                        break;
                    }
                }
            }
            for (slot, handle) in handles.into_iter().enumerate() {
                results[slot] = Some(
                    handle
                        .join()
                        .unwrap_or_else(|_| Err(LocalDbError::new("endpoint worker panicked"))),
                );
            }
            results
        });
        // Consume errors only after all threads in this wave have joined.
        for result in results.into_iter().flatten() {
            output.push(result?);
        }
    }
    Ok(output)
}

type Reply<T> = mpsc::SyncSender<Result<T, LocalDbError>>;
fn capture_write_profile(
    shared: &Mutex<WriteProfileSnapshot>,
    profile: super::tcp::ConnectionProfile,
) {
    let mut snapshot = shared.lock();
    snapshot.profile.add(profile);
    snapshot.captured_connections += 1;
}
fn record_write_profile_error(shared: &Mutex<WriteProfileSnapshot>, activation: bool) {
    let mut snapshot = shared.lock();
    if activation {
        snapshot.profile_activation_errors += 1;
    } else {
        snapshot.profile_capture_errors += 1;
    }
}
/// First causal fail-stop record: the bounded message plus the uncertain
/// version it names. It is written once, before any failure flag is stored,
/// so a control wait that observes the flag can report the typed version.
#[derive(Debug)]
struct FirstFailure {
    message: String,
    version: Option<BatchVersion>,
}

impl FirstFailure {
    fn version(&self) -> Option<BatchVersion> {
        self.version
    }
}

impl std::ops::Deref for FirstFailure {
    type Target = str;
    fn deref(&self) -> &str {
        &self.message
    }
}

impl std::fmt::Display for FirstFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

// Failure-only evidence. No caller data is included, and formatting is capped
// while writing rather than allocating an unbounded intermediate error string.
fn record_first_failure(
    slot: &std::sync::OnceLock<FirstFailure>,
    partition: Option<u32>,
    version: Option<BatchVersion>,
    stage: &'static str,
    error: &dyn std::fmt::Display,
) {
    struct Bounded(String, bool);
    impl std::fmt::Write for Bounded {
        fn write_str(&mut self, text: &str) -> std::fmt::Result {
            let remaining = 4084usize.saturating_sub(self.0.len());
            let mut end = remaining.min(text.len());
            while !text.is_char_boundary(end) {
                end -= 1;
            }
            self.1 |= end < text.len();
            self.0.push_str(&text[..end]);
            Ok(())
        }
    }
    if slot.get().is_none() {
        use std::fmt::Write;
        let mut message = Bounded(String::new(), false);
        let _ = write!(
            message,
            "owner={partition:?} batch={version:?} stage={stage}: {error}"
        );
        if message.1 {
            message.0.push_str(" [truncated]");
        }
        let _ = slot.set(FirstFailure {
            message: message.0,
            version,
        });
    }
}

fn failure_context(slot: &std::sync::OnceLock<FirstFailure>, message: &str) -> String {
    match slot.get() {
        Some(cause) => format!("{message}; first causal failure: {cause}"),
        None => message.to_owned(),
    }
}

type Pending<T> = mpsc::Receiver<Result<T, LocalDbError>>;
enum Command {
    QueueReady,
    CheckpointPosition(u64, u64, u64, Reply<CheckpointPositionProof>),
    Stats(Reply<PartitionStats>),
    Seal(u64, Reply<()>),
    ArmProfile(usize, Reply<()>),
    OwnerTimelineArm(usize, Reply<()>),
    FinishProfile(Reply<()>),
    Control(Box<dyn FnOnce(&mut Client) + Send>),
}

/// Append groups exist only inside their owner worker. The control channel
/// carries Command, which cannot carry a batch or a per-write reply channel.
/// A group is nonempty and all its batches share one fencing generation.
enum WorkerItem {
    Append(Vec<CompleteBatch>, GroupCompletion),
    Control(Command),
}
fn trace_worker_dequeue(timeline: &WriteTimeline, partition: u32, command: &WorkerItem) {
    let WorkerItem::Append(batches, _) = command else {
        return;
    };
    timeline.wait_leave(WriteWait::OwnerEnqueueToDequeue, batches.len());
    if timeline.enabled() {
        for batch in batches {
            timeline.batch_event(
                batch.version(),
                WriteTimelineStage::WorkerDequeued,
                Some(partition),
                None,
            );
        }
    }
}
#[derive(Clone)]
struct ControlHandle {
    #[cfg(any(test, feature = "experimental-fabric"))]
    client: Arc<Mutex<Client>>,
    sender: std::sync::Weak<OwnerSender>,
    worker_wake: std::sync::Weak<std::os::fd::OwnedFd>,
}
impl ControlHandle {
    fn wake_worker(&self) -> Result<(), LocalDbError> {
        let fd = self
            .worker_wake
            .upgrade()
            .ok_or_else(|| LocalDbError::new("TCP worker stopped"))?;
        signal_worker_wake(&fd)
    }

    fn owner_timeline_arm(&self, limit: usize) -> Result<(), LocalDbError> {
        let (reply, pending) = mpsc::sync_channel(1);
        self.sender
            .upgrade()
            .ok_or_else(|| LocalDbError::new("TCP worker stopped"))?
            .send(Command::OwnerTimelineArm(limit, reply))?;
        self.wake_worker()?;
        receive(pending)
    }
    fn control<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&mut Client) -> Result<T, super::tcp::TcpError> + Send + 'static,
    ) -> Result<T, LocalDbError> {
        let (reply, pending) = mpsc::sync_channel(1);
        let command = Command::Control(Box::new(move |client| {
            let _ = reply.send(operation(client).map_err(LocalDbError::component));
        }));
        self.sender
            .upgrade()
            .ok_or_else(|| LocalDbError::new("TCP worker stopped"))?
            .send(command)?;
        self.wake_worker()?;
        receive(pending)
    }
    fn control_until<T: Send + 'static>(
        &self,
        deadline: Instant,
        operation: impl FnOnce(&mut Client) -> Result<T, super::tcp::TcpError> + Send + 'static,
    ) -> Result<T, LocalDbError> {
        let (reply, pending) = mpsc::sync_channel(1);
        let command = Command::Control(Box::new(move |client| {
            if Instant::now() < deadline {
                let _ = reply.send(operation(client).map_err(LocalDbError::component));
            }
        }));
        let sender = self
            .sender
            .upgrade()
            .ok_or_else(|| LocalDbError::new("TCP worker stopped"))?;
        sender.send_until(command, deadline)?;
        self.wake_worker()?;
        pending
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            .map_err(|_| LocalDbError::new("owner timeline control deadline expired"))?
    }
}
type LeasedMembers = Vec<(BatchVersion, usize, GroupCompletion)>;

/// Releases leased receipts to their callers in frame order. Each released
/// frame is one harvested completion: its receipts enter W3
/// (`HarvestToDurable`) here and leave it in `WriteCore::on_durable`, which
/// runs on this worker inside `reply.send`, so the handoff never needs a hint.
fn release_leased_replies(
    released: &mut Vec<(u64, Vec<DurableBatch>)>,
    replies: &mut VecDeque<(u64, LeasedMembers)>,
    write_timeline: &WriteTimeline,
) -> Result<(), super::tcp::TcpError> {
    for (id, receipts) in released.drain(..) {
        if replies.front().is_none_or(|entry| entry.0 != id) {
            return Err(std::io::Error::other("leased receipt release order differs").into());
        }
        let Some((_, mut members)) = replies.pop_front() else {
            unreachable!();
        };
        write_timeline.wait_enter(WriteWait::HarvestToDurable, receipts.len());
        write_timeline.wait_wake(WriteWait::HarvestToDurable, true);
        if members.len() == 1 {
            if let Some((_, _, reply)) = members.pop() {
                let _ = reply.send(Ok(receipts));
            }
        } else {
            let mut receipts = receipts.into_iter();
            for (_, len, reply) in members {
                let _ = reply.send(Ok(receipts.by_ref().take(len).collect()));
            }
        }
    }
    Ok(())
}

/// Settles the leased route before a worker-initiated control exchange (the
/// write-profile arm and capture): drains the frames in flight and the pending
/// adoption, then releases their receipts in frame order. Commands use
/// `prepare_leased_command` for the same step.
fn settle_leased_for_control(
    client: &mut Client,
    released: &mut Vec<(u64, Vec<DurableBatch>)>,
    replies: &mut VecDeque<(u64, LeasedMembers)>,
    write_timeline: &WriteTimeline,
) -> Result<(), super::tcp::TcpError> {
    if !client.live_nvme_configured() {
        return Ok(());
    }
    client.wait_leased_control(released, Some(write_timeline))?;
    release_leased_replies(released, replies, write_timeline)
}

/// The first unresolved leased version of one owner: the oldest frame in
/// flight, else the frame deferred before submission. A transport failure
/// leaves the outcome of every such frame unknown, so a failure record names
/// this version as its uncertain version.
fn first_unresolved_leased(
    replies: &VecDeque<(u64, LeasedMembers)>,
    deferred: Option<&LeasedMembers>,
) -> Option<BatchVersion> {
    replies
        .front()
        .map(|(_, members)| members)
        .or(deferred)
        .and_then(|members| members.first())
        .map(|(version, _, _)| *version)
}

fn signal_worker_wake(fd: &std::os::fd::OwnedFd) -> Result<(), LocalDbError> {
    use std::os::fd::AsRawFd;
    let value = 1u64;
    // SAFETY: value has eight readable bytes. The borrowed OwnedFd stays open.
    let result = unsafe { libc::write(fd.as_raw_fd(), (&value as *const u64).cast(), 8) };
    if result < 0 {
        let error = std::io::Error::last_os_error();
        if error.kind() != std::io::ErrorKind::WouldBlock {
            return Err(LocalDbError::new(format!(
                "worker wake failed after command admission: {error}"
            )));
        }
    }
    Ok(())
}

struct ClientWorker {
    client: Arc<Mutex<Client>>,
    sender: Option<Arc<OwnerSender>>,
    /// The worker owns this fd. Command senders and CQEs add to its counter.
    /// The worker reads it after poll; drop closes it after the queue stops.
    worker_wake: Arc<std::os::fd::OwnedFd>,
    thread: Mutex<Option<JoinHandle<()>>>,
    queue: Arc<OwnerQueue>,
    accounting: Arc<WorkerAccounting>,
    #[cfg(test)]
    failure: Arc<std::sync::atomic::AtomicBool>,
    #[cfg(test)]
    first_failure: Arc<std::sync::OnceLock<FirstFailure>>,
    #[cfg(test)]
    partition: u32,
    #[cfg(test)]
    test_completion: Option<Arc<write_core::test_completion::TestCompletion>>,
}
struct WorkerDiagnostics {
    timeout: Duration,
    write_profile_limit: usize,
    write_profile: Arc<Mutex<WriteProfileSnapshot>>,
    write_timeline: Arc<WriteTimeline>,
    first_failure: Arc<std::sync::OnceLock<FirstFailure>>,
    partition: u32,
    policy: WorkerPolicy,
}
impl ClientWorker {
    fn control_handle(&self) -> ControlHandle {
        ControlHandle {
            #[cfg(any(test, feature = "experimental-fabric"))]
            client: self.client.clone(),
            sender: self.sender.as_ref().map(Arc::downgrade).unwrap_or_default(),
            worker_wake: Arc::downgrade(&self.worker_wake),
        }
    }

    fn control<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&mut Client) -> Result<T, super::tcp::TcpError> + Send + 'static,
    ) -> Result<T, LocalDbError> {
        self.control_handle().control(operation)
    }

    fn send(&self, command: Command) -> Result<(), LocalDbError> {
        self.sender
            .as_ref()
            .ok_or_else(|| LocalDbError::new("TCP worker stopped"))?
            .send(command)?;
        signal_worker_wake(&self.worker_wake)?;
        Ok(())
    }
    #[cfg(test)]
    fn enqueue_batch(&self, batch: CompleteBatch) -> Result<(), LocalDbError> {
        if self.failure.load(std::sync::atomic::Ordering::Acquire) {
            return Err(LocalDbError::unknown(
                batch.version(),
                failure_context(
                    &self.first_failure,
                    "TCP worker stopped before queue admission",
                ),
            ));
        }
        let sequence = batch.version().sequence;
        let (owner_sequence, _) = self.queue.reserve(|| sequence);
        if self.queue.ready(owner_sequence, sequence, batch) {
            let sender = self
                .sender
                .as_ref()
                .ok_or_else(|| LocalDbError::new("TCP worker stopped"))?;
            match sender.try_send(Command::QueueReady) {
                Ok(()) | Err(mpsc::TrySendError::Full(_)) => {}
                Err(mpsc::TrySendError::Disconnected(_)) => {
                    return Err(LocalDbError::new("TCP worker lost queue wake"));
                }
            }
            signal_worker_wake(&self.worker_wake)?;
        }
        Ok(())
    }

    #[cfg(test)]
    fn enqueue(
        &self,
        epoch: u64,
        batches: Vec<CompleteBatch>,
    ) -> Result<Pending<Vec<DurableBatch>>, LocalDbError> {
        if self.failure.load(std::sync::atomic::Ordering::Acquire) {
            return Err(LocalDbError::new(failure_context(
                &self.first_failure,
                "TCP worker stopped",
            )));
        }
        let completion = self
            .test_completion
            .as_ref()
            .ok_or_else(|| LocalDbError::new("test completion sink is not installed"))?;
        if batches.iter().any(|batch| batch.version().epoch != epoch) {
            return Err(LocalDbError::new("test enqueue crossed writer epochs"));
        }
        let receiver = completion.register(&batches).map_err(|error| {
            LocalDbError::new(failure_context(&self.first_failure, error.message()))
        })?;
        for batch in batches {
            if let Err(error) = self.enqueue_batch(batch) {
                record_first_failure(
                    &self.first_failure,
                    Some(self.partition),
                    error.uncertain_version,
                    "test_enqueue",
                    &error,
                );
                self.failure
                    .store(true, std::sync::atomic::Ordering::Release);
                completion.failed(self.partition, &[], LocalDbError::new(error.message()));
                return Err(error);
            }
        }
        Ok(receiver)
    }
    fn stats(&self) -> Result<Pending<PartitionStats>, LocalDbError> {
        let (tx, rx) = mpsc::sync_channel(1);
        self.send(Command::Stats(tx))?;
        Ok(rx)
    }
    fn arm_profile(&self, limit: usize) -> Result<(), LocalDbError> {
        let (tx, rx) = mpsc::sync_channel(1);
        self.send(Command::ArmProfile(limit, tx))?;
        receive(rx)
    }
    fn finish_profile(&self) -> Result<(), LocalDbError> {
        let (tx, rx) = mpsc::sync_channel(1);
        self.send(Command::FinishProfile(tx))?;
        receive(rx)
    }
    fn seal(&self, epoch: u64) -> Result<Pending<()>, LocalDbError> {
        let (tx, rx) = mpsc::sync_channel(1);
        self.send(Command::Seal(epoch, tx))?;
        Ok(rx)
    }
}
impl ClientWorker {
    fn stop(&self) -> Result<(), LocalDbError> {
        if let Some(sender) = &self.sender {
            sender.close();
        }
        let worker = self.thread.lock().take();
        if let Some(worker) = worker {
            // The join handle is owned locally; no mutex or allocation-order
            // guard may remain held while the worker drains and exits.
            let result = worker
                .join()
                .map_err(|_| LocalDbError::new("TCP worker panicked during shutdown"));
            self.client.lock().clear_worker_wake();
            result?;
        }
        Ok(())
    }
}
impl Drop for ClientWorker {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}
fn receive<T>(pending: Pending<T>) -> Result<T, LocalDbError> {
    pending
        .recv()
        .map_err(|_| LocalDbError::new("TCP worker lost request outcome"))?
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod leased_tests;
#[cfg(test)]
mod live_bench_tests;
#[cfg(test)]
mod position_tests;

#[cfg(test)]
mod pipeline_tests;

#[cfg(test)]
mod durable_cut_tests;
#[cfg(test)]
mod legacy_fixture_tests;
#[cfg(test)]
mod lifecycle_tests;
#[cfg(test)]
mod model_check_tests;
#[cfg(test)]
mod wal_replay_tests;
