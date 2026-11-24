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
pub use super::hybrid::HybridIndexConfig;
use super::hybrid::StoredSnapshot;
use super::hybrid::loader::CutLatch;
use super::hybrid::loader::{CutSources, allocation_capacity};
use super::hybrid::position::VersionPosition;
use super::hybrid::read::ValueReader;
use super::hybrid::region::StreamFront;
use super::hybrid::replay::{ReplayTransport, replay_into};
use super::hybrid::table::{HybridTable, ResolvedValue};
use super::native_publication::NativePublicationRouting;
use super::reference_directory::ReferenceDirectory;
use super::storage::{
    DurableBatch, PartitionConfig, PartitionStats, StaticInventory, WAL_REPLAY_WINDOW_BATCHES,
    encode_metadata, install_metadata, read_metadata,
};
use super::storage::{LocalDbError, LocalDbStats, LocalInventoryConfig};
use super::tcp::{Client, OwnerGenerationState, TlsFiles};
use super::types::{BatchId, BatchVersion, MutationVersion};
use super::write_timeline::{WriteTimeline, WriteTimelineSample, WriteTimelineStage, WriteWait};
use crate::key_shape::{KeyShape, KeySpace};
use crate::lock::DbLock;
use crate::metrics::Metrics;
use minibytes::Bytes;
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::{Arc, mpsc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

mod callback_starts;
mod stream_positions;
use stream_positions::{OwnerTail, check_stream_bound, seed_positions};
mod coordinator_io;
pub(crate) mod coordinator_lease;
mod failure;
use coordinator_io::CoordinatorIoCategory;
pub(in crate::distributed::remote_db) use failure::{
    FirstFailure, failure_context, record_first_failure,
};
mod authority_lifecycle;
pub(crate) use authority_lifecycle::drop_db;
pub use authority_lifecycle::{AuthorityLifecycle, OwnerControlConfig};
mod concurrent;
mod dead_wal_files;
mod r1_events;
mod relocation;
pub use concurrent::ConcurrentRemoteDatabase;
#[cfg(feature = "experimental-fabric")]
pub use concurrent::FabricProviderEvidence;
pub use concurrent::LiveFinalShutdownEvidence;
pub(crate) use relocation::RelocationReport;
#[cfg(feature = "test_methods")]
pub(crate) use relocation::{CallbackDecision, RelocationGate};
mod durable_cut;
pub use coordinator_io::{
    CoordinatorIoSnapshot, FileIo, coordinator_io_snapshot, diagnostics_clock_ns,
    set_coordinator_allocation_accounting,
};
pub use durable_cut::{DurableCutStats, RecoveredCut};
mod fencing_generations;
pub(crate) mod file_inventory;
use fencing_generations::AuthorityGeneration;
#[cfg(test)]
use fencing_generations::GenerationState;
pub(crate) mod checkpoint_read;
pub(crate) mod pending_read;
pub use checkpoint_read::ReadCheckpoint;
pub(crate) use pending_read::ReadTraceChild;
pub use pending_read::ReadTraceSnapshot;
mod write_core;
use write_core::completion::{AppendCompletion, GroupCompletion, WorkerAccounting};
use write_core::owner_queue::OwnerQueue;
use write_core::owner_sender::{OrderedCommand, OwnerSender};
use write_core::transport_budget::{
    AppendByteBudget, DEFAULT_OWNER_APPEND_OCCUPANCY, validate_owner_occupancy,
};
use write_core::worker_group::{GroupDrain, WorkerPolicy, take_ready_group};
pub(crate) mod read_cache;
mod transports;
pub use pending_read::ReadProfileMonitor;
use pending_read::ReadService;
pub use read_cache::{
    KEYSPACE_COUNTER_COUNT, PerKeyspaceReadCacheStats, ReadCacheShardLoad, ReadCacheShardStats,
    ReadCacheStats,
};
use read_cache::{ReadCache, ReadCachePolicy};
mod snapshots;
pub use snapshots::{RemoteReadProfile, ShutdownStats};

pub const BACKEND_ID: &str = "tidehunter-tcp-rf1";
const MANIFEST: &str = "tcp-database-authority-v1";

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
    /// Storage geometry. The database identity is provisioned at first open
    /// and stored in the authority; it never appears in documents.
    #[serde(with = "authority_lifecycle::storage_geometry")]
    pub storage: LocalInventoryConfig,
    pub endpoints: Vec<RemoteEndpoint>,
    pub tls: TlsFiles,
    pub timeout: Duration,
    /// Resident suffix identities and ordering slots, not a lifetime write limit.
    /// Completed identities leave the bounded retry window as newer batches publish.
    pub max_admitted_batches: usize,
    pub max_pending_batches: usize,
    pub max_pending_bytes: usize,
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
    /// Coordinator lease duration in milliseconds, validated to
    /// 1000..=30000. The owner clamps and returns the granted value; the
    /// renewer renews at a fifth of it, so a lapse is detected within about one
    /// lease. The default 5000 assumes the renewal thread is scheduled within
    /// about 4.5 s; a host that starves it will fail-stop the database. Absent
    /// means 5000.
    #[serde(default = "coordinator_lease::default_ms")]
    pub coordinator_lease_ms: u32,
    /// Resume a leased write after a transient connection break to an owner
    /// that kept running, instead of failing the whole database (W1). The
    /// owner must have the same incarnation and a valid coordinator lease, so
    /// a restart or a lapse still fails the database and the application
    /// reopens it. Absent means `true`: the change adds no property and removes
    /// an accidental fail-stop, and `false` restores the old determinism.
    #[serde(default = "default_resume_transient_writes")]
    pub resume_transient_writes: bool,
    /// The leased live owner-WAL NVMe-oF route; required. Remote databases have
    /// no other data path.
    pub live_nvme: super::tcp::LiveClientConfig,
    /// The durable cut of Native-tier recovery. Absent means the default lags.
    #[serde(default, skip_serializing_if = "DurableCutConfig::is_default")]
    pub durable_cut: DurableCutConfig,
    /// The native index configuration of the coordinator's `LargeTable`,
    /// `IndexFlusher`, control region and relocation policy (including native
    /// `relocation_max_reclaim_pct`, the one source of that value): Sui's
    /// resolved native values reach the engine only through this field, built by
    /// [`RemoteDatabaseConfig::with_native_index`]. Absent means Sui's production
    /// defaults; an unknown key fails at parse.
    #[serde(default)]
    pub index: HybridIndexConfig,
}

fn default_resume_transient_writes() -> bool {
    true
}
fn default_max_pending_reads() -> usize {
    256
}
fn default_max_pending_read_bytes() -> usize {
    64 * 1024 * 1024
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

impl RemoteDatabaseConfig {
    /// Takes the index rows of the native [`crate::config::Config`] the caller
    /// resolved for a native database (Sui's `thdb_config`), so a remote
    /// database indexes with exactly the values a native one would.
    #[must_use]
    pub fn with_native_index(mut self, native: &crate::config::Config) -> Self {
        self.index = HybridIndexConfig::from_native(native);
        self
    }

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
    /// The coordinator lease renewer owns one dedicated session per owner, so
    /// a renewal never waits behind a leased frame or a long poll.
    const LEASE_RENEWAL_CONNECTIONS: usize = 1;
    pub const NON_READ_CONNECTIONS: usize = Self::APPEND_CONTROL_CONNECTIONS
        + Self::DURABLE_PROGRESS_CONNECTIONS
        + Self::LEASE_RENEWAL_CONNECTIONS;
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
        self.index.validate()?;
        self.durable_cut.validate().map_err(LocalDbError::new)?;
        if !(1_000..=30_000).contains(&self.coordinator_lease_ms) {
            return Err(LocalDbError::new(format!(
                "coordinator_lease_ms {} outside 1000..=30000",
                self.coordinator_lease_ms
            )));
        }
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
    /// Adopted relocation callback starts, ascending by `(partition,
    /// generation)`; they only rise (`callback_starts`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    callback_starts: Vec<CallbackStart>,
    lifecycle: Option<AuthorityLifecycle>,
    owner_control: Option<OwnerControlConfig>,
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

fn new_authority_manifest(config: &RemoteDatabaseConfig, shape_yaml: &str) -> AuthorityManifest {
    AuthorityManifest {
        format: fencing_generations::AUTHORITY_FORMAT,
        storage: config.storage.clone(),
        mode: AuthorityMode {
            history_persistence: MANIFEST_HISTORY.into(),
            checkpoint_mode: None,
            cell_checkpoint: None,
            cell_checkpoint_obsolete: None,
        },
        endpoints: config.endpoints.clone(),
        shape_yaml: shape_yaml.to_owned(),
        live_nvme_identity: Some((config.live_nvme.host_nqn.clone(), config.live_nvme.host_id)),
        live_nvme_owner_identities: config.live_nvme.owner_identities.clone(),
        next_generation: 1,
        generations: Vec::new(),
        trim_pending: None,
        callback_starts: Vec::new(),
        lifecycle: None,
        owner_control: None,
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StreamPosition {
    partition: u32,
    epoch: u64,
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

/// R1 durable callback cursor of one stream: the next relocation scan of
/// `(partition, fencing_generation)` starts at file-aligned offset `from`.
/// Monotone across adopted cursors; never an owner replay cursor.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct CallbackStart {
    partition: u32,
    fencing_generation: u64,
    from: u64,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct RecoveryStats {
    pub checkpoint_loaded: bool,
    pub checkpoint_load_ns: u64,
    /// Retained older generations the loaded control region did not name, so
    /// the open replayed them from offset 0. Zero in steady state: every store
    /// names every retained generation (`hybrid::region::older_replay_from`).
    /// Non-zero is legitimate only for a generation that opened and closed
    /// before any store, and is a defect otherwise.
    pub old_generation_replayed_from_zero: u64,
    /// Rows of the loaded control region that name a generation this open does
    /// not retain (trimmed after the store). The open skips them; the next
    /// store drops them.
    pub stale_region_rows_skipped: u64,
    pub tail_replay_ns: u64,
    pub connection_setup_ns: u64,
    pub fencing_ns: u64,
    pub tail_batches: u64,
    pub tail_bytes: u64,
    /// Batches below the loaded region's cut that replay read again and did not
    /// apply, because the stored blobs already hold them. Sources: the
    /// per-stream fragment floor, a retained generation with no stored row
    /// (replayed from offset 0), the dropped floor at the extent-queue cap, and
    /// an older-generation row carried forward. Not part of `tail_batches`.
    pub below_cut_skipped: u64,
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
    /// Owner-worker extent-queue cap hits: each one widens the snapshot's replay
    /// table instead of tracking an extent individually.
    pub allocation_cap_hits: u64,
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

#[derive(Clone, Copy, Debug, Default)]
pub struct WriteMetricsSnapshot {
    pub authority: AuthorityWriteStats,
    pub pipeline: PipelineStats,
    pub read_cache: ReadCacheStats,
    pub write_profile: WriteProfileSnapshot,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct WriteProfileSnapshot {
    pub limit_per_connection: usize,
    pub expected_connections: usize,
    pub captured_connections: usize,
    pub sampled_requests: u64,
    pub profile_activation_errors: u64,
    pub profile_capture_errors: u64,
    pub profile: super::tcp::ConnectionProfile,
}

pub struct RemoteDatabase {
    core: Arc<write_core::core::WriteCore>,
    /// The value path over the owners' read lanes; shared with the concurrent
    /// facade so a read is one fetch, one CRC check and one native decode.
    reader: Arc<ValueReader>,
    /// Coordinator lease renewers, one per owner. Dropped after
    /// `Drop::drop` stopped and joined them.
    lease: coordinator_lease::CoordinatorLease,
    /// The one DMA reaper of detached live write queues (W1), joined by
    /// `Drop::drop`.
    resume_reaper: super::tcp::ResumeReaper,
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

    /// Resident completed/in-flight authority bookkeeping, excluding pageable
    /// receipt files, allocator overhead and the separately reported index.
    pub fn metadata_history_bytes(&self) -> usize {
        self.core.metadata_history_bytes()
    }

    /// The monotone value-byte counter that paces the snapshot cadence.
    pub(crate) fn index_value_bytes(&self) -> u64 {
        self.core.table.index_value_bytes()
    }

    /// One native snapshot pass (the cadence thread's call).
    pub(crate) fn snapshot_index(&self) -> Result<StoredSnapshot, LocalDbError> {
        self.ensure_healthy()?;
        self.core.snapshot_table()
    }

    /// A failed snapshot pass is fail-stop, as native's `expect` on its pass
    /// is: the exact cause becomes the engine's first recorded failure.
    pub(crate) fn record_snapshot_failure(&self, error: &LocalDbError) {
        self.poison(error);
    }

    /// Resolve the authoritative owner of a key for benchmark evidence.
    ///
    /// The caller must have drained the database and prevent concurrent writes
    /// until this observation is complete. The owner comes from the key's index
    /// position at the publication watermark; absence or a tombstone returns
    /// `None`. No transport or cache lock is held.
    #[cfg(all(target_os = "linux", feature = "test-utils"))]
    pub fn partition_for_key_for_benchmark(
        &self,
        keyspace: KeySpace,
        full_key: &[u8],
    ) -> Result<Option<u32>, LocalDbError> {
        self.ensure_healthy()?;
        let table = &self.core.table;
        Ok(table
            .get(keyspace, full_key, table.current())?
            .map(|position| position.address.partition))
    }

    pub fn open(
        path: &Path,
        shape: &KeyShape,
        config: RemoteDatabaseConfig,
        metrics: Arc<Metrics>,
    ) -> Result<Self, LocalDbError> {
        Self::open_inner(path, shape, config, metrics, None)
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
            Metrics::new(),
            None,
            #[cfg(test)]
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
        metrics: Arc<Metrics>,
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
            metrics,
            Some(OpenInterruption::AfterFences(successful_fences)),
        )
    }

    fn open_inner(
        path: &Path,
        shape: &KeyShape,
        config: RemoteDatabaseConfig,
        metrics: Arc<Metrics>,
        interruption: Option<OpenInterruption>,
    ) -> Result<Self, LocalDbError> {
        Self::open_inner_with(
            path,
            shape,
            config,
            metrics,
            interruption,
            #[cfg(test)]
            None,
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
        Self::open_inner_with(
            path,
            shape,
            config,
            Metrics::new(),
            interruption,
            None,
            #[cfg(test)]
            None,
            Some(false),
        )
    }

    // The parameter set is the open's whole control surface: the caller's
    // metrics, the injected interruption, and the test-only read hook, lease
    // hook and I/O fixture that `#[cfg]` removes from a production build.
    #[allow(clippy::too_many_arguments)]
    fn open_inner_with(
        path: &Path,
        shape: &KeyShape,
        config: RemoteDatabaseConfig,
        metrics: Arc<Metrics>,
        interruption: Option<OpenInterruption>,
        #[cfg(test)] read_hook: Option<ReadHook<'_>>,
        // Test-only: the renewer's send decision per owner, so a test can stop
        // one owner's renewals without a stopped owner or a clock.
        #[cfg(test)] lease_hook: Option<coordinator_lease::SendHook>,
        // `Some(buffered)` selects the ordinary-file live I/O fixture on every
        // client before any live I/O; `None` uses the real live route.
        #[cfg(any(test, feature = "test-utils"))] local_fixture: Option<bool>,
    ) -> Result<Self, LocalDbError> {
        if let Some(error) = super::sys::route_unavailable() {
            return Err(LocalDbError::new(error));
        }
        config.validate()?;
        config.validate_read_lane_pinning(config.effective_read_lanes())?;
        // Reported once per open and never an error: native batch compression has
        // no meaning on the leased route, which carries whole owner frames.
        if let Some(report) = config.index.unsupported_report(&path.display().to_string()) {
            eprintln!("{report}");
        }
        let shape_yaml = shape.to_yaml().map_err(LocalDbError::component)?;
        // Preserve callback closures; YAML describes the shape, not its policy.
        let shape = shape.clone();
        // Namespace locks, native lock, pending-drop recovery and, for a new
        // path, identity provisioning on every owner. The resolved config
        // carries the provisioned database identity from here on.
        let mut config = config;
        let authority_lifecycle::OpenedAuthority {
            lock,
            path,
            mut manifest,
            namespace: _namespace,
        } = authority_lifecycle::open_authority(path, &mut config, &shape_yaml, |config| {
            new_authority_manifest(config, &shape_yaml)
        })?;
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
        // Use the configured identity for comparisons, and the provisioned
        // per-database identity for actual namespace grants and connections.
        authority_lifecycle::apply_effective_initiators(&mut config);
        let config = config;
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
        // The one place the stream bound is enforced before anything is
        // persisted: a reservation that would take `owners x (retained + 1)
        // past the control-region bound is refused here, with the manifest
        // untouched and no owner fenced. Trimming first is not possible: an
        // owner trim is fenced by the reserved generation itself. The soft
        // pressure signal after open is what keeps databases from getting here.
        check_stream_bound(config.endpoints.len(), manifest.generations.len())?;
        // One durable replacement folds a previous reservation into Aborted,
        // appends Reserved(g) and advances the never-reused counter before ANY
        // owner may activate g. A failed partial fence is aborted by the next open.
        let epoch = fencing_generations::reserve(&mut manifest, config.storage.database)?;
        persist_manifest(&path, &manifest, CoordinatorIoCategory::Open)?;
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
            // The tail of each retained older stream is what its owner reports;
            // nothing is derived from replay (see `seed_positions`).
            let tails: Vec<OwnerTail> = entries
                .iter()
                .filter(|entry| history.contains(&entry.generation))
                .map(|entry| OwnerTail {
                    partition,
                    generation: entry.generation,
                    durable_end: entry.durable_end,
                    // Every older generation was fenced before this sweep, so its
                    // reported tail is final: Sealed and Abandoned are complete.
                    complete: matches!(
                        entry.state,
                        OwnerGenerationState::Sealed | OwnerGenerationState::Abandoned
                    ),
                })
                .collect();
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
                tails,
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
        let mut histories = Vec::with_capacity(fenced.len());
        let mut owner_tails = Vec::new();
        let mut owners = Vec::with_capacity(fenced.len());
        for (history, tails, owner) in fenced {
            histories.push(history);
            owner_tails.extend(tails);
            owners.push(owner);
        }
        let (tiers, owner_fences): (Vec<_>, Vec<_>) = owners.into_iter().unzip();
        #[cfg(any(test, feature = "test-utils"))]
        if fail_after_fences.is_some() {
            return Err(LocalDbError::new(
                "injected coordinator interruption during partial fencing",
            ));
        }
        // Recovery is permitted only AFTER every registered node was fenced.
        // Unavailable RF1 nodes stop recovery; max observed sequence is not a
        // substitute for complete inventory or evidence of absent records.
        recovery_stats.fencing_ns = elapsed(fencing_started);
        // Version B tracks this epoch only when every owner acknowledges at the
        // page cache (Native). Synchronous owners are per-batch durable and
        // never truncated.
        // The hybrid durable front is fed from the persisted durable-cut record
        // (tracked, every owner Native) or from the published prefix (every owner
        // Synchronous, where an acknowledgement implies the owner synced). A mixed
        // set has neither a record that covers its Native owners nor an ack that
        // proves them synced, so it is refused rather than silently weakened.
        if tiers.windows(2).any(|pair| pair[0] != pair[1]) {
            return Err(LocalDbError::new(
                "owners acknowledge at different durability tiers; the hybrid index needs one \
                 tier across owners to prove what its durable front covers",
            ));
        }
        let tracked = tiers.iter().all(|tier| *tier == DurabilityTier::Native);
        let partitions: Vec<u32> = config
            .endpoints
            .iter()
            .map(|endpoint| endpoint.partition)
            .collect();
        // Every open is a new fencing generation whose owner streams start
        // empty, so the cut fronts start at `{epoch, 0}`: above every batch of an
        // earlier generation, below every batch of this one (A17).
        let cuts = CutSources::new(
            epoch,
            if tracked {
                DurabilityTier::Native
            } else {
                DurabilityTier::Synchronous
            },
        );
        let table = Arc::new(HybridTable::open_with_partitions(
            &path,
            Arc::new(shape.clone()),
            &config.index,
            cuts,
            metrics,
            &partitions,
        )?);
        table.validate_partitions(&partitions)?;
        recovery_stats.checkpoint_loaded = table.region_loaded();
        recovery_stats.checkpoint_load_ns = table.region_load_ns();
        // No checkpoint covers any batch for Version B: the durable-cut record is
        // the only claim about what the owners synced, which is the stricter
        // reading of a region cut that is exclusive.
        let mut cut_plan = durable_cut::RecoveryPlan::new(
            durable_cut::CutRecord::load(&path, config.storage.database, epoch)?,
            config.storage.database,
            None,
            &owner_fences,
        );
        // The streams to replay are the retained generations each owner holds,
        // all strictly older than this open's fencing generation (Sealed or
        // Abandoned). The serving generation is new (A17): its owner stream
        // starts empty and is never replayed, and asking the owner for its pages
        // fails ("recovery requires sealed immutable epoch"). A stream with a
        // stored front resumes there; one without (a generation opened and not
        // yet snapshotted) replays from its start. Batches below the loaded
        // region's cut are read again and dropped by the replay merge.
        let mut fronts = Vec::new();
        let mut live_streams = Vec::new();
        for (node, partition) in partitions.iter().enumerate() {
            for generation in histories[node].iter().copied() {
                live_streams.push((*partition, generation));
                let front =
                    match table.opened_fronts().iter().find(|front| {
                        front.partition == *partition && front.generation == generation
                    }) {
                        Some(front) => *front,
                        None => {
                            recovery_stats.old_generation_replayed_from_zero += 1;
                            StreamFront {
                                partition: *partition,
                                generation,
                                replay_from: 0,
                            }
                        }
                    };
                fronts.push(front);
            }
        }
        recovery_stats.stale_region_rows_skipped = u64::try_from(
            table
                .opened_fronts()
                .iter()
                .filter(|front| !live_streams.contains(&(front.partition, front.generation)))
                .count(),
        )
        .unwrap_or(u64::MAX);
        let replay_started = Instant::now();
        let page_limit = config
            .recovery_page_limit()?
            .max(config.storage.group_limits.max_batches as usize);
        let report = {
            let mut transport = ReplayTransport {
                clients: &mut clients,
                partitions: &partitions,
                page_limit,
                authority_epoch: epoch,
                fragment_bytes: config.storage.fragment_bytes,
            };
            replay_into(
                &table,
                &mut transport,
                |version| cut_plan.keep(version),
                &fronts,
                &live_streams,
            )?
        };
        recovery_stats.tail_batches = report.tail_batches;
        recovery_stats.tail_bytes = report.tail_bytes;
        recovery_stats.below_cut_skipped = report.below_cut_skipped;
        #[cfg(test)]
        if matches!(interruption, Some(OpenInterruption::DuringReplay)) {
            return Err(LocalDbError::new("injected interruption during WAL replay"));
        }
        recovery_stats.cut_discarded_batches = cut_plan.discarded;
        recovery_stats.cut_extended_batches = cut_plan.extended;
        recovery_stats.cut_skipped_versions = cut_plan.skipped;
        recovery_stats.cut_continuity_bound = cut_plan.continuity_bound;
        let (cut_record, recovered_cut) = cut_plan.finish(epoch, tracked, &owner_fences);
        recovery_stats.recovered_cut = recovered_cut;
        recovery_stats.tail_replay_ns = elapsed(replay_started);
        // W1: one thread per database retires a queue a resume detached. It
        // starts before the workers so no resume can find the channel closed.
        let resume_reaper = super::tcp::ResumeReaper::start(config.endpoints.len())
            .map_err(LocalDbError::component)?;
        let resume_sender = resume_reaper.sender();
        let resume_writes = config.resume_transient_writes;
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
        // The per-stream extent queue spans one snapshot interval (A12): the
        // drain runs once per real store, so the queue must hold every extent
        // the interval can allocate. 4 KiB is the smallest owner frame.
        let allocation_queue_capacity = allocation_capacity(
            table.config().snapshot_written_bytes,
            4096,
            config.max_pending_batches,
        );
        for mut client in clients {
            let partition = config.endpoints[workers.len()].partition;
            client.set_resume_writes(resume_writes, resume_sender.clone());
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
                table.allocation().clone(),
                u32::try_from(workers.len())
                    .map_err(|_| LocalDbError::new("more owner streams than a u32 can index"))?,
                epoch,
                allocation_queue_capacity,
            )
            .map_err(LocalDbError::component)?;
            workers.push(worker);
        }
        table.attach_stream_controls(StreamControls::new(
            config
                .endpoints
                .iter()
                .zip(&workers)
                .map(|(endpoint, worker)| (endpoint.partition, worker.control_handle()))
                .collect(),
        ));
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
        // Before serving, gate DropCells and the hybrid index: a binary of an
        // older format refuses this authority.
        manifest.format = fencing_generations::AUTHORITY_FORMAT;
        persist_manifest(&path, &manifest, CoordinatorIoCategory::Open)?;
        #[cfg(test)]
        if matches!(interruption, Some(OpenInterruption::AfterServed)) {
            return Err(LocalDbError::new("injected interruption after served"));
        }
        let partition_indexes: HashMap<u32, usize> = config
            .endpoints
            .iter()
            .enumerate()
            .map(|(i, e)| (e.partition, i))
            .collect();
        let reads = Arc::new(reads);
        let read_cache = Arc::new(ReadCache::new(config.read_cache_bytes));
        let reader = Arc::new(ValueReader::new(
            Arc::clone(&reads),
            Arc::clone(&read_cache),
            Arc::new(shape.clone()),
            Arc::new(partition_indexes.clone()),
            config.timeout,
        ));
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
                        // poll costs one extra owner sync. The lane waits on
                        // readiness and the poller's stop wake.
                        connect: Box::new(move |stop| {
                            Client::connect_progress_shared(
                                &address,
                                &tls,
                                client_config.clone(),
                                timeout,
                                expected.clone(),
                                &shape,
                                stop,
                            )
                        }),
                    }
                })
                .collect()
        });
        // Older retained streams are seeded from the tail their owner reported in
        // the fence sweep (`owner_tails`), never from what replay read: a lower
        // value would make a generation look covered sooner and stop the
        // relocation scan short. Every owner and retained generation gets an
        // entry (see `stream_positions`), so a missing entry always means "no
        // proof".
        let positions = seed_positions(
            &partitions,
            &fencing_generations::retained_below(&manifest, epoch),
            &owner_tails,
        )?;
        let callback_starts = callback_starts::load(
            &manifest.callback_starts,
            config.storage.wal_file_bytes,
            &partitions,
            &histories,
            epoch,
        )?;
        let mut cut_record = cut_record;
        // One dedicated renewal session per owner. It is opened like the
        // durable-cut poller's (a plain session over the epoch-1 inventory the
        // owner registered with), and its handshake is interruptible by the
        // renewer's stop wake.
        let lease_duration_ms = config.coordinator_lease_ms;
        let lease_epoch = epoch;
        let lease_holder = rand::random::<[u8; 16]>();
        let lease_targets = coordinator_lease::targets(&config, &tls_config, &shape);
        let core = write_core::core::WriteCore::new(write_core::core::OpenState {
            clients: workers,
            reads,
            read_cache,
            write_profile,
            write_timeline,
            live_pool,
            live_metrics,
            inventory: config.inventory(epoch),
            config,
            shape,
            path,
            epoch,
            table,
            partition_indexes,
            publisher: write_core::core::PublisherState {
                // Out-of-window retries are re-applied, as native has no operation
                // ids; the window fills at publication (owner-accepted weakening).
                recent: write_core::receipts::ReceiptWindow::new(),
                positions,
                callback_starts,
                published_next: 0,
                upper: None,
                dependents: HashMap::new(),
                range_barrier: None,
                latest: None,
                live_latest: None,
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
            },
            concurrent_failure,
            first_failure,
            stats: write_core::core::CoreStats {
                recovery: recovery_stats,
                ..write_core::core::CoreStats::default()
            },
        });
        completion.bind(&core);
        // The first grant of every owner must succeed before this database
        // accepts a write. Each write worker's client checks its owner's
        // validity stamp before it submits a frame, so no lease work runs on
        // the submission path.
        let lease = coordinator_lease::CoordinatorLease::start(
            lease_targets,
            lease_epoch,
            lease_holder,
            lease_duration_ms,
            core.clone(),
            #[cfg(test)]
            lease_hook,
        )?;
        for (index, worker) in core.clients.iter().enumerate() {
            worker
                .client
                .lock()
                .set_lease_validity(lease.validity(index));
        }
        let database = Self {
            core,
            reader,
            lease,
            resume_reaper,
            _lock: lock,
        };
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
                database.core.table.cuts().clone(),
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

    /// The database identity provisioned at this path's first open.
    pub fn database_id(&self) -> [u8; 16] {
        self.core.config.storage.database
    }

    pub fn validate_cached_read(&self) -> Result<(), LocalDbError> {
        self.ensure_healthy()
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

    /// The value of `key` at `cut`, given what the index names for it.
    ///
    /// The value path takes the *indexed* (reduced) key: it binds the member it
    /// decodes to that key. A `Reduction` keyspace indexes only a slice of the
    /// key, so another full key can share the index entry; native's `get` then
    /// compares the full key, and so does this (`need_check_index_key`), through
    /// the entry read that returns the member's own key and bypasses the
    /// position-keyed reader cache, which would answer for the wrong key.
    ///
    /// `refill` installs a fetched value into the native per-cell LRU, exactly
    /// what native `Db::get` does and native `DbCheckpoint::get` does not, so a
    /// latched or checkpoint read passes `false`. A resident answer needs no
    /// refill: it came from that LRU. A keyspace without a value cache has no
    /// LRU to fill, and the refill then costs nothing.
    pub(super) fn read_value(
        &self,
        ks: KeySpace,
        key: &[u8],
        cut: BatchVersion,
        resolved: ResolvedValue,
        refill: bool,
    ) -> Result<Option<Bytes>, LocalDbError> {
        let table = &self.core.table;
        let descriptor = table.ks_desc(ks);
        let reduced = descriptor.reduce_key(key);
        let position = match resolved {
            ResolvedValue::Position(position) => position,
            ResolvedValue::Resident { full_key, value } => {
                return Ok(
                    (!descriptor.need_check_index_key() || full_key.as_ref() == key)
                        .then_some(value),
                );
            }
        };
        let refill = refill && descriptor.value_cache_size().is_some();
        let at = reduced.as_ref();
        let reader = &self.reader;
        let resolve = || table.get_value(ks, key, cut);
        if descriptor.need_check_index_key() {
            let entry = reader.read_entry_with_resolve(ks, at, position, resolve, false)?;
            let Some((full_key, value)) = entry else {
                return Ok(None);
            };
            // A `Reduction` keyspace's index entry is only a slice of the key:
            // the member's own key is the one that binds the value.
            if full_key.as_ref() != key {
                return Ok(None);
            }
            if refill {
                // The member's own key is the key the value is stored under, and
                // it is the one the read just bound the value to.
                table.refill_value(ks, reduced.into_owned().into(), full_key, value.clone());
            }
            return Ok(Some(value));
        }
        let value = reader.read_with_resolve(ks, at, position, resolve, false)?;
        if refill && let Some(value) = &value {
            table.refill_value(
                ks,
                reduced.into_owned().into(),
                Bytes::copy_from_slice(key),
                value.clone(),
            );
        }
        Ok(value)
    }

    /// One key: an index lookup, then either the value the native per-cell LRU
    /// holds or the owner read of the member frame the position names. The owner
    /// may retire the file between the two, so the value path re-resolves exactly
    /// once (see `ValueReader`).
    ///
    /// `refill` is the caller's statement that this is a live read which may
    /// seed the LRU (see [`Self::read_value`]); a checkpoint view passes `false`.
    pub(super) fn get_at_cut(
        &self,
        ks: KeySpace,
        key: &[u8],
        cut: BatchVersion,
        refill: bool,
    ) -> Result<Option<Bytes>, LocalDbError> {
        self.ensure_healthy()?;
        let resolve_started = Instant::now();
        let resolved = self.core.table.get_value(ks, key, cut)?;
        // Relaxed: diagnostic time only, never publication authority.
        self.core.read_resolve_ns.fetch_add(
            elapsed(resolve_started),
            std::sync::atomic::Ordering::Relaxed,
        );
        let Some(resolved) = resolved else {
            return Ok(None);
        };
        let fetch_started = Instant::now();
        let value = self.read_value(ks, key, cut, resolved, refill);
        self.core
            .value_read_ns
            .fetch_add(elapsed(fetch_started), std::sync::atomic::Ordering::Relaxed);
        self.ensure_healthy()?;
        value
    }

    /// Presence at `cut`. A keyspace whose index key is a reduction of the full
    /// key needs the member's own key to decide; every other keyspace is answered
    /// by the index alone, with no owner round trip. A resident value is the
    /// answer without any round trip, exactly as native `exists` reads its LRU.
    pub(super) fn exists_at_cut(
        &self,
        ks: KeySpace,
        key: &[u8],
        cut: BatchVersion,
    ) -> Result<bool, LocalDbError> {
        self.ensure_healthy()?;
        let started = Instant::now();
        let resolved = self.core.table.get_value(ks, key, cut)?;
        self.core
            .read_resolve_ns
            .fetch_add(elapsed(started), std::sync::atomic::Ordering::Relaxed);
        let Some(resolved) = resolved else {
            return Ok(false);
        };
        if !self.core.table.ks_desc(ks).need_check_index_key() {
            return Ok(true);
        }
        // Native `exists` never refills the LRU: presence is not a value read.
        Ok(self.read_value(ks, key, cut, resolved, false)?.is_some())
    }

    /// Any number of keys at one cut, fetched in pages of the value reader's
    /// bound. An invalid key fails only its own slot; a failed owner page fails
    /// the call. A resident value is answered without any owner contact, and a
    /// fetched one refills the native per-cell LRU only when `refill` says this
    /// is a live read (see [`Self::read_value`]); a latched view passes `false`.
    ///
    /// Every key is resolved at the one sampled `cut`: a publication landing
    /// mid-call must not let different slots observe different publication
    /// points, and the batch has no per-key cut to fall back on.
    pub(super) fn get_many_at_cut<K: AsRef<[u8]>>(
        &self,
        ks: KeySpace,
        keys: &[K],
        cut: BatchVersion,
        trace: Option<&pending_read::ReadTraceCallGuard>,
        refill: bool,
    ) -> Result<Vec<Result<Option<Bytes>, LocalDbError>>, LocalDbError> {
        self.get_many_with(ks, keys, cut, trace, refill, false)
    }

    /// The batch read of the Sui adapter at the current publication cut: the
    /// keys are resolved at that one cut, a resident value answers inline, and
    /// every miss is fetched in one batched owner read per owner. Native
    /// `multi_get` runs one `Db::get` per key, so a retired member is retried
    /// per slot (see [`Self::get_many_with`]).
    pub(super) fn get_many_live<K: AsRef<[u8]>>(
        &self,
        ks: KeySpace,
        keys: &[K],
        trace: Option<&pending_read::ReadTraceCallGuard>,
    ) -> Result<Vec<Result<Option<Bytes>, LocalDbError>>, LocalDbError> {
        self.get_many_with(ks, keys, self.core.table.current(), trace, true, true)
    }

    /// Both batch reads. `repair_retired` selects the retired-member contract of
    /// a failed owner page: when set, every slot of the page is re-read through
    /// the scalar path, which re-resolves its key exactly once, because the
    /// batched fetch cannot attribute the failure to one slot. When clear, the
    /// failed page fails the call.
    fn get_many_with<K: AsRef<[u8]>>(
        &self,
        ks: KeySpace,
        keys: &[K],
        cut: BatchVersion,
        trace: Option<&pending_read::ReadTraceCallGuard>,
        refill: bool,
        repair_retired: bool,
    ) -> Result<Vec<Result<Option<Bytes>, LocalDbError>>, LocalDbError> {
        /// Rows one `ValueReader::read_many` call may hold.
        const PAGE_KEYS: usize = 256;
        if let Some(trace) = trace {
            trace.event("index_resolution_started");
        }
        self.ensure_healthy()?;
        let table = &self.core.table;
        let descriptor = table.ks_desc(ks);
        let mut results = Vec::with_capacity(keys.len());
        for page in keys.chunks(PAGE_KEYS) {
            let resolve_started = Instant::now();
            let mut rows: Vec<(Bytes, VersionPosition)> = Vec::with_capacity(page.len());
            let mut slots = Vec::with_capacity(page.len());
            for key in page {
                let key = key.as_ref();
                match table.get_value(ks, key, cut) {
                    Ok(Some(ResolvedValue::Resident { full_key, value })) => {
                        results.push(Ok((!descriptor.need_check_index_key()
                            || full_key.as_ref() == key)
                            .then_some(value)));
                    }
                    Ok(Some(ResolvedValue::Position(position)))
                        if descriptor.need_check_index_key() =>
                    {
                        results.push(self.read_value(
                            ks,
                            key,
                            cut,
                            ResolvedValue::Position(position),
                            refill,
                        ));
                    }
                    Ok(Some(ResolvedValue::Position(position))) => {
                        slots.push(results.len());
                        rows.push((
                            Bytes::copy_from_slice(descriptor.reduce_key(key).as_ref()),
                            position,
                        ));
                        results.push(Ok(None));
                    }
                    Ok(None) => results.push(Ok(None)),
                    Err(error) => results.push(Err(error)),
                }
            }
            self.core.read_resolve_ns.fetch_add(
                elapsed(resolve_started),
                std::sync::atomic::Ordering::Relaxed,
            );
            if rows.is_empty() {
                continue;
            }
            if let Some(trace) = trace {
                trace.event("value_fetch_started");
            }
            let fetch_started = Instant::now();
            let fetched = match self.reader.read_many(ks, &rows) {
                Ok(values) => {
                    let refill = refill && descriptor.value_cache_size().is_some();
                    for ((slot, (reduced, _)), value) in slots.into_iter().zip(&rows).zip(values) {
                        if refill && let Some(value) = &value {
                            table.refill_value(
                                ks,
                                reduced.clone(),
                                Bytes::copy_from_slice(keys[slot].as_ref()),
                                value.clone(),
                            );
                        }
                        results[slot] = Ok(value);
                    }
                    Ok(())
                }
                // A retired owner file is the one failure the index can
                // repair. The batched fetch cannot say which slot held the
                // retired member, so the per-slot batch falls back to the
                // scalar read for this page: that read re-resolves its key
                // exactly once and answers per slot, like native `Db::get`.
                // The strict batch keeps its contract (a failed page fails the
                // call).
                Err(error) if repair_retired && pending_read::member_read_is_retired(&error) => {
                    for (slot, (_, position)) in slots.into_iter().zip(rows) {
                        results[slot] = self.read_value(
                            ks,
                            keys[slot].as_ref(),
                            cut,
                            ResolvedValue::Position(position),
                            refill,
                        );
                    }
                    Ok(())
                }
                Err(error) => Err(error),
            };
            self.core
                .value_read_ns
                .fetch_add(elapsed(fetch_started), std::sync::atomic::Ordering::Relaxed);
            fetched?;
        }
        self.ensure_healthy()?;
        Ok(results)
    }

    /// The live read: a scalar lookup at the current cut, which may fill the
    /// native per-cell LRU from the member it fetched.
    pub fn get(&mut self, ks: KeySpace, key: &[u8]) -> Result<Option<Bytes>, LocalDbError> {
        self.get_at_cut(ks, key, self.core.table.current(), true)
    }

    /// The scalar read with its two stage clocks. Benchmark callers must keep
    /// these samples separate from ordinary gets. A resident value reports its
    /// index time only: it fetched nothing.
    pub fn get_profiled(
        &mut self,
        ks: KeySpace,
        key: &[u8],
    ) -> Result<(Option<Bytes>, RemoteReadProfile), LocalDbError> {
        self.ensure_healthy()?;
        let cut = self.core.table.current();
        let started = Instant::now();
        let resolved = self.core.table.get_value(ks, key, cut)?;
        let index_ns = elapsed(started);
        let hits = self.reader.cache_hits();
        let started = Instant::now();
        let remote = matches!(resolved, Some(ResolvedValue::Position(_)));
        let value = match resolved {
            Some(resolved) => self.read_value(ks, key, cut, resolved, true)?,
            None => None,
        };
        self.ensure_healthy()?;
        let profile = RemoteReadProfile {
            index_ns,
            wait_ns: elapsed(started),
            cache_hits: self.reader.cache_hits() - hits,
            cache_misses: u64::from(remote).saturating_sub(self.reader.cache_hits() - hits),
            ..RemoteReadProfile::default()
        };
        Ok((value, profile))
    }

    pub fn get_many(
        &mut self,
        ks: KeySpace,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<Bytes>>, LocalDbError> {
        self.get_many_at_cut(ks, keys, self.core.table.current(), None, true)?
            .into_iter()
            .collect()
    }

    pub fn exists(&mut self, ks: KeySpace, key: &[u8]) -> Result<bool, LocalDbError> {
        self.exists_at_cut(ks, key, self.core.table.current())
    }

    /// One native snapshot pass: flushes the cells that are stale by value-byte
    /// distance and stores one control region when the flush frontier advanced.
    /// The pass outcome ([`StoredSnapshot`]) is crate-internal; callers outside
    /// the crate only need to know the pass succeeded.
    pub fn flush_index(&self) -> Result<(), LocalDbError> {
        self.core.flush_index().map(|_| ())
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
        for owner in self.owner_storage_stats() {
            let (_, stats) = owner?;
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

    /// Owner persistence counters of each owner, in endpoint order.
    pub fn storage_stats_by_partition(&self) -> Result<Vec<(u32, PartitionStats)>, LocalDbError> {
        self.owner_storage_stats().collect()
    }

    fn owner_storage_stats(
        &self,
    ) -> impl Iterator<Item = Result<(u32, PartitionStats), LocalDbError>> + '_ {
        self.core
            .config
            .endpoints
            .iter()
            .zip(&self.core.clients)
            .map(|(endpoint, client)| Ok((endpoint.partition, receive(client.stats()?)?)))
    }
    pub fn shutdown(&self) -> Result<(), LocalDbError> {
        // Renewals run through the Seal; see `close_engine`.
        self.close_engine(|core| core.shutdown())
    }
}

impl Drop for RemoteDatabase {
    fn drop(&mut self) {
        let _ = self.shutdown();
        // The clients that hand queues over live in `core`, which drops after
        // this function, so the reaper stops on its flag rather than on a
        // closed channel. A queue already received is drained first.
        self.resume_reaper.finish();
    }
}

fn persist_manifest(
    path: &Path,
    manifest: &AuthorityManifest,
    category: CoordinatorIoCategory,
) -> Result<FileIo, LocalDbError> {
    coordinator_io::install_counted(
        path,
        MANIFEST,
        &encode_metadata(manifest).map_err(LocalDbError::component)?,
        category,
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
type Pending<T> = mpsc::Receiver<Result<T, LocalDbError>>;
/// The owner drain lanes of one database: one control handle per owner worker,
/// in dense stream order. The snapshot pass drains through it, once per real
/// store, so it never competes with the append path for a lock.
#[derive(Clone)]
pub(crate) struct StreamControls {
    handles: Vec<(u32, ControlHandle)>,
}

impl StreamControls {
    fn new(handles: Vec<(u32, ControlHandle)>) -> Self {
        Self { handles }
    }

    /// Drains every owner stream to `target` and waits for each acknowledgement.
    /// Owners are drained one after the other: a store is cold and the slowest
    /// owner bounds it either way.
    pub(crate) fn drain_to(&self, target: BatchVersion) -> Result<(), LocalDbError> {
        for (partition, handle) in &self.handles {
            handle.drain_to(target).map_err(|error| {
                LocalDbError::new(format!("drain owner {partition} to {target:?}: {error}"))
            })?;
        }
        Ok(())
    }
}

enum Command {
    QueueReady,
    Stats(Reply<PartitionStats>),
    Seal(u64, Reply<()>),
    ArmProfile(usize, Reply<()>),
    OwnerTimelineArm(usize, Reply<()>),
    FinishProfile(Reply<()>),
    Control(Box<dyn FnOnce(&mut Client) + Send>),
    DrainTo(BatchVersion, Reply<()>),
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
    #[cfg(feature = "experimental-fabric")]
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

    /// Freezes this owner worker's allocation view at `target` and waits for its
    /// acknowledgement. A worker that already failed drops the acknowledgement,
    /// which surfaces as an error instead of covering a queue that never drained.
    fn drain_to(&self, target: BatchVersion) -> Result<(), LocalDbError> {
        let (reply, pending) = mpsc::sync_channel(1);
        self.sender
            .upgrade()
            .ok_or_else(|| LocalDbError::new("TCP worker stopped"))?
            .send(Command::DrainTo(target, reply))?;
        self.wake_worker()?;
        receive(pending)
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
    #[cfg(test)]
    allocation: Arc<super::hybrid::loader::AllocationView>,
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
    /// Test handle on this worker's allocation view (the stream fronts the next
    /// snapshot would store).
    #[cfg(test)]
    pub(crate) fn allocation_fronts(&self) -> Vec<StreamFront> {
        self.allocation.fronts()
    }
    fn control_handle(&self) -> ControlHandle {
        ControlHandle {
            #[cfg(feature = "experimental-fabric")]
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
mod sui_adapter_tests;
#[cfg(test)]
mod tests;

#[cfg(test)]
mod dead_wal_files_tests;
#[cfg(test)]
mod leased_tests;
#[cfg(test)]
mod live_bench_tests;

#[cfg(test)]
mod copy_durable_pass_tests;

#[cfg(test)]
mod relocation_copy_cas_tests;

#[cfg(test)]
mod relocation_tests;

#[cfg(test)]
mod pipeline_tests;

#[cfg(test)]
mod drop_cells_tests;
#[cfg(test)]
mod durable_cut_tests;
#[cfg(test)]
mod format_gate_tests;

#[cfg(test)]
mod retained_generation_counter_tests;

#[cfg(test)]
mod retained_generation_tests;

#[cfg(test)]
mod lifecycle_tests;
#[cfg(test)]
mod model_check_tests;
#[cfg(test)]
mod wal_replay_tests;

#[cfg(test)]
mod open_tests;

#[cfg(test)]
mod metrics_tests;

#[cfg(test)]
mod stream_bound_tests;

#[cfg(test)]
mod value_cache_tests;
