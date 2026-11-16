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
use super::group::{CompleteGroup, GROUP_HEADER_LEN};
use super::local_db::{LocalCheckpoint, LocalDbError, LocalDbStats};
use super::local_inventory::LocalInventoryConfig;
use super::native_index::{IndexReadCut, NativeIndex, PublicationViewReservation};
use super::native_publication::{NativePublicationRouting, PreparedNativePublication};
use super::publication::PublicationTracker;
use super::reference_directory::ReferenceDirectory;
use super::sequence::LogicalClock;
use super::storage::{
    AppendedBatch, DurableBatch, HistoryPersistence, PartitionConfig, PartitionStats,
    StaticInventory, WAL_REPLAY_WINDOW_BATCHES, encode_metadata, install_metadata, read_metadata,
};
use super::tcp::checkpoint::{CheckpointPositionProof, CheckpointProof, CheckpointReceipt};
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
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, VecDeque};
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
#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
pub use concurrent::LiveFinalShutdownEvidence;
use coordinator_profile::{
    CoordinatorResolutionDiagnostics, MemberOutcome, ResolutionSample, ResolutionStage,
};
pub use coordinator_profile::{CoordinatorResolutionSnapshot, CoordinatorResolutionStage};
mod owner_dispatch;
mod pending_read;
#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
pub(crate) use pending_read::ReadTraceChild;
pub use pending_read::ReadTraceSnapshot;
mod preparation;
mod receipt_history;
mod replay_limits;
use owner_dispatch::{
    AppendByteBudget, DEFAULT_OWNER_APPEND_OCCUPANCY, DispatchMode, OwnerDispatchPolicy,
};
use receipt_history::{HistoryPointer, ReceiptHistory};
pub use replay_limits::ReplayLimits;
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
pub use transports::RemoteTransportPool;

const OPERATIONAL_SAMPLE_INTERVAL: Duration = Duration::from_secs(1);
const OPERATIONAL_REQUEST_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Default)]
pub struct OperationalReadIoSnapshot {
    pub generation: u64,
    pub last_attempt: Option<Instant>,
    pub available: bool,
    pub failed_samples: u64,
    pub owners: Vec<(u32, super::tcp::ReadExecutorStats)>,
}
struct OperationalReadIoState {
    snapshot: RwLock<OperationalReadIoSnapshot>,
    stopped: std::sync::atomic::AtomicBool,
}

/// Passive operational owner counters. This handle retains only an inert snapshot.
#[derive(Clone)]
pub struct OperationalReadIoMonitor {
    state: Arc<OperationalReadIoState>,
}
impl OperationalReadIoMonitor {
    pub fn visit(&self, visit: impl FnOnce(&OperationalReadIoSnapshot, bool)) {
        let snapshot = self.state.snapshot.read();
        visit(
            &snapshot,
            self.state
                .stopped
                .load(std::sync::atomic::Ordering::Acquire),
        );
    }
    pub fn try_visit(&self, visit: impl FnOnce(&OperationalReadIoSnapshot, bool)) -> Option<()> {
        let snapshot = self.state.snapshot.try_read()?;
        visit(
            &snapshot,
            self.state
                .stopped
                .load(std::sync::atomic::Ordering::Acquire),
        );
        Some(())
    }
}

struct PendingOperationalReadIoSampling {
    config: RemoteDatabaseConfig,
    shape: KeyShape,
    tls_config: super::tcp::ClientTlsConfig,
}

pub(super) struct OperationalReadIoLifecycle {
    state: Arc<OperationalReadIoState>,
    pending: Option<PendingOperationalReadIoSampling>,
    stop: Option<mpsc::SyncSender<()>>,
    worker: Option<JoinHandle<()>>,
    stopped: bool,
}

struct OperationalReadIoSampler {
    state: Arc<OperationalReadIoState>,
    lifecycle: Arc<Mutex<OperationalReadIoLifecycle>>,
}
impl OperationalReadIoSampler {
    fn dormant(
        config: RemoteDatabaseConfig,
        shape: KeyShape,
        tls_config: super::tcp::ClientTlsConfig,
    ) -> Self {
        let state = Arc::new(OperationalReadIoState {
            snapshot: RwLock::new(OperationalReadIoSnapshot::default()),
            stopped: std::sync::atomic::AtomicBool::new(false),
        });
        Self {
            state: state.clone(),
            lifecycle: Arc::new(Mutex::new(OperationalReadIoLifecycle {
                state,
                pending: Some(PendingOperationalReadIoSampling {
                    config,
                    shape,
                    tls_config,
                }),
                stop: None,
                worker: None,
                stopped: false,
            })),
        }
    }

    fn monitor(&self) -> OperationalReadIoMonitor {
        OperationalReadIoMonitor {
            state: self.state.clone(),
        }
    }

    fn downgrade_lifecycle(&self) -> std::sync::Weak<Mutex<OperationalReadIoLifecycle>> {
        Arc::downgrade(&self.lifecycle)
    }

    fn activate(&self) {
        Self::activate_lifecycle(&self.lifecycle);
    }

    pub(super) fn activate_lifecycle(lifecycle: &Arc<Mutex<OperationalReadIoLifecycle>>) {
        let mut lifecycle = lifecycle.lock();
        if lifecycle.stopped || lifecycle.worker.is_some() {
            return;
        }
        let Some(pending) = lifecycle.pending.take() else {
            return;
        };
        let state = lifecycle.state.clone();
        let thread_state = state.clone();
        let (stop, stopped) = mpsc::sync_channel(1);
        let spawn_result = thread::Builder::new()
            .name("wal-operational-metrics".to_owned())
            .spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    operational_sample_loop(
                        pending.config,
                        pending.shape,
                        pending.tls_config,
                        &thread_state,
                        stopped,
                    )
                }));
                if result.is_err() {
                    mark_operational_sample_failed(&thread_state);
                    eprintln!("operational read-I/O sampler panicked");
                }
            });
        match spawn_result {
            Ok(worker) => {
                lifecycle.stop = Some(stop);
                lifecycle.worker = Some(worker);
            }
            Err(error) => {
                lifecycle.stopped = true;
                mark_operational_sample_failed(&state);
                eprintln!("failed to start operational read-I/O sampler: {error}");
            }
        }
    }

    fn stop(&mut self) {
        let (state, stop, worker) = {
            let mut lifecycle = self.lifecycle.lock();
            lifecycle.stopped = true;
            lifecycle.pending = None;
            lifecycle
                .state
                .stopped
                .store(true, std::sync::atomic::Ordering::Release);
            (
                lifecycle.state.clone(),
                lifecycle.stop.take(),
                lifecycle.worker.take(),
            )
        };
        if let Some(stop) = stop {
            let _ = stop.try_send(());
        }
        if let Some(worker) = worker
            && worker.join().is_err()
        {
            mark_operational_sample_failed(&state);
            eprintln!("operational read-I/O sampler panicked during shutdown");
        }
        if let Some(mut snapshot) = state.snapshot.try_write() {
            snapshot.available = false;
            snapshot.owners.clear();
        }
    }
}

fn mark_operational_sample_failed(state: &OperationalReadIoState) {
    state
        .stopped
        .store(true, std::sync::atomic::Ordering::Release);
    if let Some(mut snapshot) = state.snapshot.try_write() {
        snapshot.generation = snapshot.generation.saturating_add(1);
        snapshot.last_attempt = Some(Instant::now());
        snapshot.failed_samples = snapshot.failed_samples.saturating_add(1);
        snapshot.available = false;
        snapshot.owners.clear();
    }
}

impl Drop for OperationalReadIoSampler {
    fn drop(&mut self) {
        self.stop();
    }
}

fn operational_sample_loop(
    config: RemoteDatabaseConfig,
    shape: KeyShape,
    tls_config: super::tcp::ClientTlsConfig,
    state: &OperationalReadIoState,
    stop: mpsc::Receiver<()>,
) {
    let mut clients: Vec<Option<Client>> = (0..config.endpoints.len()).map(|_| None).collect();
    loop {
        if stop.try_recv().is_ok() {
            break;
        }
        let attempt_started = Instant::now();
        let mut owners = Vec::with_capacity(config.endpoints.len());
        let mut available = true;
        for (index, endpoint) in config.endpoints.iter().enumerate() {
            if stop.try_recv().is_ok() {
                available = false;
                break;
            }
            if clients[index].is_none() {
                let mut tls = config.tls.clone();
                tls.server_name = endpoint.server_name.clone();
                clients[index] = Client::connect_operational_shared(
                    &endpoint.address,
                    &tls,
                    tls_config.clone(),
                    OPERATIONAL_REQUEST_TIMEOUT,
                    config.partition_config(1, endpoint.partition),
                    &shape,
                )
                .ok();
            }
            match clients[index]
                .as_mut()
                .map(Client::operational_read_io_stats)
            {
                Some(Ok(Some(stats))) => owners.push((endpoint.partition, stats)),
                Some(Ok(None)) | None => available = false,
                Some(Err(_)) => {
                    clients[index] = None;
                    available = false;
                }
            }
        }
        if !state.stopped.load(std::sync::atomic::Ordering::Acquire)
            && let Some(mut snapshot) = state.snapshot.try_write()
        {
            snapshot.generation = snapshot.generation.saturating_add(1);
            snapshot.last_attempt = Some(attempt_started);
            snapshot.available = available && owners.len() == config.endpoints.len();
            snapshot.failed_samples = snapshot
                .failed_samples
                .saturating_add(u64::from(!snapshot.available));
            snapshot.owners = if snapshot.available {
                owners
            } else {
                Vec::new()
            };
        }
        match stop.recv_timeout(OPERATIONAL_SAMPLE_INTERVAL) {
            Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
            Err(mpsc::RecvTimeoutError::Timeout) => {}
        }
    }
}
/// Explicit benchmark evidence handle for owner read I/O.
pub struct ReadEvidenceMonitor {
    clients: Vec<(u32, ControlHandle)>,
    reads: Arc<ReadService>,
}
impl ReadEvidenceMonitor {
    /// Sample owner read I/O counters outside the measured interval. Callers
    /// must finish all reads and writes first; sequential owner replies do not
    /// constitute a globally consistent cut.
    /// Counter sampling does not close current or preleased writer extents.
    pub fn sample_owner_read_io(
        &self,
    ) -> Result<Vec<(u32, super::tcp::ReadExecutorStats)>, LocalDbError> {
        self.reads.ensure_idle()?;
        self.clients
            .iter()
            .map(|(partition, client)| client.read_io_stats().map(|stats| (*partition, stats)))
            .collect()
    }
}

pub const BACKEND_ID: &str = "tidehunter-tcp-rf1";
const MANIFEST: &str = "tcp-database-authority-v1";
const MAX_CHECKPOINT_METADATA_BYTES: u64 = 128 * 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RemoteEndpoint {
    pub partition: u32,
    pub address: String,
    pub server_name: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RemoteDatabaseConfig {
    pub storage: LocalInventoryConfig,
    #[serde(default = "HistoryPersistence::legacy")]
    pub history_persistence: HistoryPersistence,
    pub endpoints: Vec<RemoteEndpoint>,
    pub tls: TlsFiles,
    pub timeout: Duration,
    /// Resident suffix identities and ordering slots, not a lifetime write limit.
    /// Checkpoints move completed identities into the selected retry history.
    pub max_admitted_batches: usize,
    pub max_index_bytes: usize,
    /// Lock shards of the coordinator index; a power of two. In memory only, so
    /// it can change between opens.
    #[serde(default = "default_index_shards")]
    pub index_shards: usize,
    pub max_pending_batches: usize,
    pub max_pending_bytes: usize,
    /// Background checkpoint trigger; hard suffix bounds apply only to legacy modes.
    #[serde(default)]
    pub replay_limits: ReplayLimits,
    /// Queued and active append commands per owner; one transport slot is reserved for control.
    #[serde(default = "default_owner_append_occupancy")]
    pub owner_append_occupancy: usize,
    /// Dispatch full per-node groups during admission; drain remains the ack barrier.
    #[serde(default = "default_pipeline")]
    pub pipelined_writes: bool,
    /// Derivative verified-record cache. Zero disables it; never authoritative.
    #[serde(default = "default_read_cache_bytes")]
    pub read_cache_bytes: usize,
    /// Explicit host slice for cache ownership and bounded pending read buffers.
    #[serde(default = "default_read_memory_allowance_bytes")]
    pub read_memory_allowance_bytes: usize,
    /// CPU workers for the bounded borrowed-window submit_many API.
    #[serde(default = "default_preparation_workers")]
    pub preparation_workers: usize,
    /// Bounded scan-page lookahead; one is the serial control.
    #[serde(default = "default_scan_prefetch_pages")]
    pub scan_prefetch_pages: usize,
    #[serde(default = "default_read_lanes")]
    pub read_lanes: usize,
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
    /// Select direct live owner-WAL NVMe-oF before opening or recovering.
    #[serde(default)]
    pub live_nvme: Option<super::tcp::LiveClientConfig>,
}

fn default_read_lanes() -> usize {
    2
}
fn default_max_pending_reads() -> usize {
    256
}
fn default_max_pending_read_bytes() -> usize {
    64 * 1024 * 1024
}

fn default_preparation_workers() -> usize {
    2
}
fn default_scan_prefetch_pages() -> usize {
    2
}

fn default_pipeline() -> bool {
    true
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

    fn effective_owner_append_occupancy(&self) -> usize {
        if self.live_nvme.as_ref().is_some_and(|live| {
            live.write_mode == super::tcp::LiveWriteMode::Leased
                && live.leased_write_window.is_none()
        }) {
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
        if let Some(live) = &self.live_nvme {
            live.validate().map_err(LocalDbError::new)?;
            if self.pin_read_lanes && self.read_lanes > live.queue_geometry.io_queues as usize {
                return Err(LocalDbError::new(
                    "pin_read_lanes requires read_lanes <= live I/O queues",
                ));
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
            #[cfg(not(all(feature = "experimental-nvmeof", target_os = "linux")))]
            return Err(LocalDbError::new(
                "live NVMe-oF requires the Linux experimental-nvmeof feature",
            ));
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            {
                if live.write_mode == super::tcp::LiveWriteMode::Leased
                    && self.storage.fragment_bytes >= 1 << 30
                {
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
        OwnerDispatchPolicy::new(self.owner_append_occupancy).map_err(LocalDbError::new)?;
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        if self.live_nvme.as_ref().is_some_and(|live| {
            live.write_mode == super::tcp::LiveWriteMode::Leased
                && live
                    .leased_write_window
                    .is_some_and(|window| window > self.owner_append_occupancy)
        }) {
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
            || self.storage.max_epochs == 0
            || self.storage.max_epochs > 1024
            || self.max_admitted_batches == 0
            || self.max_admitted_batches > 1_000_000
            || self.max_pending_batches == 0
            || self.max_pending_batches > 4096
            || self.max_pending_bytes == 0
            || self.max_pending_bytes > 256 * 1024 * 1024
            || !(1..=8).contains(&self.preparation_workers)
            || !(1..=2).contains(&self.scan_prefetch_pages)
            || self.read_memory_allowance_bytes == 0
            // Cache bytes include all fixed arrays and charged live entries.
            || self
                .read_cache_bytes
                .checked_add(self.max_pending_read_bytes)
                .is_none_or(|bytes| bytes > self.read_memory_allowance_bytes)
            || !(1..=8).contains(&self.read_lanes)
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
        if !checkpoint_metadata_fits(self, 0) {
            return Err(LocalDbError::new(
                "checkpoint epoch inventory exceeds metadata budget",
            ));
        }
        if !checkpoint_metadata_fits(
            self,
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
        if self.history_persistence == HistoryPersistence::WalReplay
            && self
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
struct AuthorityEpoch {
    epoch: u64,
    /// False means no caller could have acquired a serving database handle.
    active: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct AuthorityManifest {
    format: u32,
    storage: LocalInventoryConfig,
    #[serde(default = "HistoryPersistence::legacy")]
    history_persistence: HistoryPersistence,
    endpoints: Vec<RemoteEndpoint>,
    shape_yaml: String,
    #[serde(default = "ReplayLimits::legacy")]
    replay_limits: ReplayLimits,
    /// The data-plane mode and initiator identity survive authority reopen.
    #[serde(default)]
    live_nvme_identity: Option<(String, [u8; 16])>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    live_nvme_owner_identities: BTreeMap<u32, super::tcp::LiveInitiatorIdentity>,
    epochs: Vec<AuthorityEpoch>,
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

#[derive(Serialize, Deserialize)]
struct CheckpointImage {
    format: u32,
    database: [u8; 16],
    shape_yaml: String,
    authority_epoch: u64,
    generation: u64,
    cut: Option<MutationVersion>,
    published_next: u64,
    index_bytes: u64,
    index_digest: [u8; 32],
    streams: Vec<CheckpointStream>,
    history_root: Option<HistoryPointer>,
}

#[derive(Serialize, Deserialize)]
struct CheckpointStream {
    proof: CheckpointProof,
    state: [u8; 32],
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
    /// Pool identity/checkout and disposal of a mismatched idle bundle.
    pub transport_checkout_ns: u64,
    /// Read-connection worker wall time; overlaps control setup and recovery.
    pub read_lane_setup_ns: u64,
    /// Critical-path wait for prepared read connections after recovery.
    pub read_lane_wait_ns: u64,
    pub created_connections: u64,
    pub reused_connections: u64,
    /// Nested elapsed worker sums across both connection sets, not additive.
    pub connect_worker_ns: u64,
    pub status_worker_ns: u64,
    pub tls_worker_ns: u64,
    pub fabric_worker_ns: Option<u64>,
    /// Nested fabric setup worker sums. Queue/MTU/version fields are maxima,
    /// not sums. Absent for TCP and for reused transports with no new endpoint.
    #[cfg(feature = "experimental-fabric")]
    pub fabric_open_worker: Option<tidehunter_fabric::OpenProfile>,
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

struct Submission {
    batch: Option<CompleteBatch>,
    routing: Option<NativePublicationRouting>,
    publication_view: Option<PublicationViewReservation>,
    /// Immediate native-index predecessors; transitive order follows their batches.
    predecessors: SmallVec<[u64; 4]>,
    dependency_blocked: bool,
    last_mutation: MutationVersion,
    encoded_bytes: usize,
    metadata_bytes: usize,
    credit: usize,
}

struct PreparedAdmission {
    batch: CompleteBatch,
    routing: NativePublicationRouting,
    partition: usize,
    metadata_bytes: usize,
    credit: usize,
    group_charge: usize,
}

/// Rayon terminates its workers on drop but does not join their OS threads.
/// Retain handles so even failed construction releases no authority lock early.
struct PreparationPool {
    pool: Option<Arc<rayon::ThreadPool>>,
    threads: Vec<JoinHandle<()>>,
}

impl PreparationPool {
    fn build(count: usize) -> Result<Self, LocalDbError> {
        let threads = Arc::new(Mutex::new(Vec::with_capacity(count)));
        let handles = threads.clone();
        let result = rayon::ThreadPoolBuilder::new()
            .num_threads(count)
            .thread_name(|index| format!("value-wal-prepare-{index}"))
            .spawn_handler(move |worker| {
                let handle = thread::Builder::new()
                    .name(worker.name().unwrap_or("value-wal-prepare").to_owned())
                    .spawn(move || worker.run())?;
                handles.lock().push(handle);
                Ok(())
            })
            .build();
        let mut owner = Self {
            pool: None,
            threads: std::mem::take(&mut *threads.lock()),
        };
        owner.pool = Some(Arc::new(result.map_err(LocalDbError::component)?));
        Ok(owner)
    }
}

impl Drop for PreparationPool {
    fn drop(&mut self) {
        self.pool.take();
        for thread in self.threads.drain(..) {
            let _ = thread.join();
        }
    }
}

#[derive(Clone, Copy)]
enum OpenInterruption {
    #[cfg(any(test, feature = "test-utils"))]
    AfterFences(usize),
    #[cfg(test)]
    AfterReadSetup,
    #[cfg(test)]
    DuringReplay,
}

#[derive(Clone, Copy)]
enum AdmissionPressure {
    Tail {
        encoded_bytes: usize,
        identity: bool,
    },
    Partition {
        partition: usize,
        records: usize,
    },
    Index {
        additional_bytes: usize,
    },
}

struct PendingAppend {
    first: BatchVersion,
    reply: Pending<Vec<DurableBatch>>,
    charge: usize,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct PipelineStats {
    pub prepare_calls: u64,
    pub parallel_prepared_batches: u64,
    pub prepare_wall_ns: u64,
    /// Sum of worker elapsed durations: overlaps; not process CPU time.
    pub prepare_task_ns: u64,
    pub prepare_reserved_bytes_peak: usize,
    /// Completion replies consumed while the next preparation is running.
    pub prepare_progress_completions: u64,
    pub prepare_progress_passes: u64,
    pub append_requests: u64,
    pub durable_batches: u64,
    pub direct_appends: u64,
    pub early_dispatches: u64,
    pub completions_before_drain: u64,
    pub peak_inflight_nodes: usize,
    pub concurrent_admitted: u64,
    pub concurrent_completed: u64,
    pub concurrent_pending_peak: usize,
    pub concurrent_queued_peak: usize,
    pub concurrent_queue_bytes_peak: usize,
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
    pub receipt_history_ns: Option<u64>,
    pub receipt_history_sync_ns: Option<u64>,
    pub history_stream_opens: Option<u64>,
    pub history_previous_slot_reads: Option<u64>,
    pub history_receipt_write_ops: Option<u64>,
    pub history_receipt_write_bytes: Option<u64>,
    pub history_buffer_flushes: Option<u64>,
    pub history_node_write_ops: Option<u64>,
    pub history_node_write_bytes: Option<u64>,
    pub history_sync_operations: Option<u64>,
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
    pub receipt_history_ns: u64,
    pub receipt_history_count: u64,
    pub receipt_history_sync_ns: u64,
    pub receipt_history_sync_count: u64,
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
    pub history_io_checkpoint_count: u64,
    pub history_stream_opens: u64,
    pub history_previous_slot_reads: u64,
    pub history_receipt_write_ops: u64,
    pub history_receipt_write_bytes: u64,
    pub history_buffer_flushes: u64,
    pub history_node_write_ops: u64,
    pub history_node_write_bytes: u64,
    pub history_sync_operations: u64,
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
            receipt_history_ns,
            receipt_history_ns,
            receipt_history_count
        );
        add_span!(
            receipt_history_sync_ns,
            receipt_history_sync_ns,
            receipt_history_sync_count
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
        if let Some(stream_opens) = record.history_stream_opens {
            self.history_io_checkpoint_count = self.history_io_checkpoint_count.saturating_add(1);
            self.history_stream_opens = self.history_stream_opens.saturating_add(stream_opens);
            self.history_previous_slot_reads = self
                .history_previous_slot_reads
                .saturating_add(record.history_previous_slot_reads.unwrap_or_default());
            self.history_receipt_write_ops = self
                .history_receipt_write_ops
                .saturating_add(record.history_receipt_write_ops.unwrap_or_default());
            self.history_receipt_write_bytes = self
                .history_receipt_write_bytes
                .saturating_add(record.history_receipt_write_bytes.unwrap_or_default());
            self.history_buffer_flushes = self
                .history_buffer_flushes
                .saturating_add(record.history_buffer_flushes.unwrap_or_default());
            self.history_node_write_ops = self
                .history_node_write_ops
                .saturating_add(record.history_node_write_ops.unwrap_or_default());
            self.history_node_write_bytes = self
                .history_node_write_bytes
                .saturating_add(record.history_node_write_bytes.unwrap_or_default());
            self.history_sync_operations = self
                .history_sync_operations
                .saturating_add(record.history_sync_operations.unwrap_or_default());
        }
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

/// Diagnostic clocks only. Actor/TCP stages are nested and must not be summed
/// with dispatch/wait into a purported non-overlapping latency breakdown.
#[derive(Clone, Copy, Debug, Default)]
pub struct RemoteReadProfile {
    pub index_ns: u64,
    pub dispatch_ns: u64,
    pub wait_ns: u64,
    pub actor_queue_ns: u64,
    pub actor_service_ns: u64,
    pub actor_return_ns: u64,
    /// Caller-owned scalar RPC, with no read-actor queue/return handoff.
    pub inline_read_ns: u64,
    pub tcp: super::tcp::TcpReadProfile,
    pub cache_hits: u64,
    pub cache_misses: u64,
}

struct WorkerReadProfile {
    queue_ns: u64,
    service_ns: u64,
    finished: Instant,
    tcp: super::tcp::TcpReadProfile,
}

pub struct RemoteDatabase {
    clients: Vec<ClientWorker>,
    reads: Arc<ReadService>,
    preparation_pool: Option<PreparationPool>,
    read_cache: Arc<ReadCache>,
    write_profile: Arc<Mutex<WriteProfileSnapshot>>,
    write_timeline: Arc<WriteTimeline>,
    transport_pool: Option<Arc<RemoteTransportPool>>,
    transport_key: Option<[u8; 32]>,
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    live_pool: Option<Arc<super::nvmeof::live_pool::LiveControllerPool>>,
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    live_metrics: Option<Arc<super::nvmeof::live_metrics::LiveNvmeMetrics>>,
    config: RemoteDatabaseConfig,
    operational_sampler: OperationalReadIoSampler,
    shape: KeyShape,
    path: PathBuf,
    epoch: u64,
    index: Arc<NativeIndex>,
    partition_indexes: HashMap<u32, usize>,
    inventory: StaticInventory,
    known: HashMap<BatchId, AppendedBatch>,
    receipt_history: Option<ReceiptHistory>,
    stream_last: BTreeMap<(u32, u64), (u64, u32)>,
    positions: BTreeMap<(u32, u64), StreamPosition>,
    committed_complete: BTreeSet<(u32, u64)>,
    recent: VecDeque<RecentReceipt>,
    recent_ids: HashMap<BatchId, (BatchVersion, super::types::PayloadDigest)>,
    active_identities: HashMap<BatchId, usize>,
    pending_writers: HashMap<KeySpace, HashMap<DependencyKey, u64>>,
    predecessor_marks: Vec<u64>,
    clock: LogicalClock,
    publication: PublicationTracker,
    submission_base: u64,
    submissions: VecDeque<Submission>,
    durable_wait_started: VecDeque<Option<Instant>>,
    staged: Vec<VecDeque<usize>>,
    staged_bytes: Vec<usize>,
    inflight: Vec<VecDeque<PendingAppend>>,
    completed_nodes: Vec<bool>,
    unresolved_append: Option<BatchVersion>,
    completion_wake: mpsc::Receiver<()>,
    preparation_wake: mpsc::SyncSender<()>,
    pipeline_stats: PipelineStats,
    reserved_index_bytes: usize,
    pending_bytes: usize,
    partition_admissions: Vec<usize>,
    partition_records: Vec<usize>,
    /// Greatest complete contiguous publication, never the live sparse upper cut.
    latest: Option<MutationVersion>,
    live_latest: Option<MutationVersion>,
    snapshot_generation: u64,
    inflight_bytes: Vec<usize>,
    owner_dispatch: OwnerDispatchPolicy,
    append_byte_budget: AppendByteBudget,
    manifest: AuthorityManifest,
    retained_epochs: Vec<Vec<u64>>,
    checkpoint_tail_batches: usize,
    checkpoint_tail_bytes: usize,
    background_checkpoint: Option<BackgroundCheckpoint>,
    recovery_stats: RecoveryStats,
    write_stats: AuthorityWriteStats,
    checkpoint_diagnostics: Option<VecDeque<CheckpointDiagnostic>>,
    checkpoint_diagnostic_summary: CheckpointDiagnosticSummary,
    next_checkpoint_diagnostic: u64,
    dropped_checkpoint_diagnostics: u64,
    last_checkpoint_diagnostic: Option<CheckpointDiagnostic>,
    shutdown_stats: ShutdownStats,
    #[cfg(test)]
    checkpoint_interruption: Option<u8>,
    #[cfg(test)]
    checkpoint_receipt_limit: Option<usize>,
    #[cfg(test)]
    retire_preflight_busy_once: bool,
    healthy: bool,
    // Unlike `healthy`, this never marks a protected in-progress transition.
    // It is monotonic and shared with reads and append workers.
    concurrent_failure: Arc<std::sync::atomic::AtomicBool>,
    first_failure: Arc<std::sync::OnceLock<String>>,
    stopped: bool,
    stats: LocalDbStats,
    #[cfg(test)]
    before_preparation: Option<(mpsc::SyncSender<()>, mpsc::Receiver<()>)>,
    #[cfg(test)]
    owner_start_pause: Option<mpsc::Receiver<()>>,
    #[cfg(test)]
    fail_prepared_operation: Option<[u8; 16]>,
    #[cfg(test)]
    concurrent_progress: Arc<Mutex<Option<mpsc::SyncSender<PipelineStats>>>>,
    #[cfg(test)]
    before_checkpoint_io: Option<mpsc::SyncSender<()>>,
    #[cfg(test)]
    checkpoint_pause: Option<(mpsc::SyncSender<()>, mpsc::Receiver<()>)>,
    #[cfg(test)]
    checkpoint_prefix_pause: Option<(mpsc::SyncSender<()>, mpsc::Receiver<()>)>,
    #[cfg(test)]
    corrupt_checkpoint_proof: bool,
    #[cfg(test)]
    checkpoint_adoption_pause: Option<(mpsc::SyncSender<()>, mpsc::Receiver<()>)>,
    #[cfg(test)]
    checkpoint_cleanup_pause: Option<(mpsc::SyncSender<()>, mpsc::Receiver<()>)>,
    #[cfg(test)]
    panic_after_concurrent_admissions: Option<u64>,
    // Network workers are joined before this host's authority lock is released.
    _lock: DbLock,
}

impl RemoteDatabase {
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    pub fn live_nvmeof_stats(&self) -> Option<super::nvmeof::live_metrics::LiveNvmeStats> {
        self.live_metrics.as_ref().map(|metrics| metrics.snapshot())
    }

    /// Install a verified sealed-WAL reader while all reads are idle.
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    pub fn install_nvmeof_reader(
        &self,
        config: super::nvmeof::NvmeofReaderConfig,
    ) -> Result<(), LocalDbError> {
        self.ensure_healthy()?;
        self.reads
            .install_nvmeof_reader(config, &self.config, &self.shape)
    }

    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    pub fn clear_nvmeof_reader(&self) -> Result<(), LocalDbError> {
        self.reads.clear_nvmeof_reader()
    }

    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    pub fn nvmeof_stats(&self) -> Vec<super::nvmeof::NvmeofStats> {
        self.reads.nvmeof_stats()
    }

    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    pub fn register_nvmeof_metrics(
        &self,
        registry: &prometheus::Registry,
    ) -> Result<(), LocalDbError> {
        self.reads.register_nvmeof_metrics(registry)
    }

    fn submission_next(&self) -> u64 {
        self.submission_base + self.submissions.len() as u64
    }

    pub fn index_stats(&self) -> super::native_index::NativeIndexStats {
        self.index.stats()
    }

    /// Logical retained index charge, including pageable historical references.
    pub fn index_history_bytes(&self) -> usize {
        self.index.retained_metadata_bytes()
    }

    /// Resident completed/in-flight authority bookkeeping, excluding pageable
    /// receipt files, allocator overhead and the separately reported index.
    pub fn metadata_history_bytes(&self) -> usize {
        self.known.capacity()
            * (std::mem::size_of::<BatchId>() + std::mem::size_of::<AppendedBatch>())
            + self.submissions.capacity() * std::mem::size_of::<Submission>()
            + self.durable_wait_started.capacity() * std::mem::size_of::<Option<Instant>>()
            + self.active_identities.capacity()
                * (std::mem::size_of::<BatchId>() + std::mem::size_of::<usize>())
            + self.pending_writers.capacity()
                * std::mem::size_of::<(KeySpace, HashMap<DependencyKey, u64>)>()
            + self
                .pending_writers
                .values()
                .map(|writers| {
                    writers.capacity() * std::mem::size_of::<(DependencyKey, u64)>()
                        + writers
                            .keys()
                            .filter(|key| key.spilled())
                            .map(SmallVec::capacity)
                            .sum::<usize>()
                })
                .sum::<usize>()
            + self.predecessor_marks.capacity() * std::mem::size_of::<u64>()
            + self
                .submissions
                .iter()
                .map(|entry| {
                    if entry.predecessors.spilled() {
                        entry.predecessors.capacity() * std::mem::size_of::<u64>()
                    } else {
                        0
                    }
                })
                .sum::<usize>()
            + self.publication.resident_metadata_bytes()
            + self
                .receipt_history
                .as_ref()
                .map_or(0, ReceiptHistory::cached_metadata_bytes)
            + self.recent.capacity() * std::mem::size_of::<RecentReceipt>()
            + self.recent_ids.capacity()
                * std::mem::size_of::<(BatchId, (BatchVersion, super::types::PayloadDigest))>()
    }

    pub fn retention_floor(&self) -> IndexReadCut {
        self.index.retention_floor()
    }

    pub fn protected_published_cut(
        &self,
    ) -> Result<(IndexReadCut, super::native_index::IndexReadPin), LocalDbError> {
        self.ensure_healthy()?;
        let index = &self.index;
        let cut = index.latest_cut();
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
        self.index.pin_cut(cut).map_err(LocalDbError::component)
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
        Self::open_inner(path, shape, config, None, None)
    }

    /// Open a fresh authority epoch using an optional process-owned transport
    /// lifetime. Only clean, quiescent connections can be reused; credentials,
    /// peer status, complete fencing and checkpoint/tail recovery are rechecked.
    pub fn open_with_transport_pool(
        path: &Path,
        shape: &KeyShape,
        config: RemoteDatabaseConfig,
        _metrics: Arc<Metrics>,
        transports: Arc<RemoteTransportPool>,
    ) -> Result<Self, LocalDbError> {
        Self::open_inner(path, shape, config, Some(transports), None)
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
            None,
            Some(OpenInterruption::AfterFences(successful_fences)),
        )
    }

    fn open_inner(
        path: &Path,
        shape: &KeyShape,
        config: RemoteDatabaseConfig,
        transport_pool: Option<Arc<RemoteTransportPool>>,
        interruption: Option<OpenInterruption>,
    ) -> Result<Self, LocalDbError> {
        config.validate()?;
        // Live queues and namespace grants cannot be returned to the TCP pool.
        let transport_pool = if config.live_nvme.is_some() {
            None
        } else {
            transport_pool
        };
        let mut index = NativeIndex::with_shards(
            shape,
            config.storage.database,
            config.max_index_bytes,
            config.index_shards,
        )
        .map_err(LocalDbError::component)?;
        let shape_yaml = shape.to_yaml().map_err(LocalDbError::component)?;
        let shape = KeyShape::from_yaml(&shape_yaml).map_err(LocalDbError::component)?;
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
            read_metadata(&path.join(MANIFEST)).map_err(LocalDbError::component)?
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
                format: 4,
                storage: config.storage.clone(),
                history_persistence: config.history_persistence,
                endpoints: config.endpoints.clone(),
                shape_yaml: shape_yaml.clone(),
                replay_limits: config.replay_limits,
                live_nvme_identity: config
                    .live_nvme
                    .as_ref()
                    .map(|live| (live.host_nqn.clone(), live.host_id)),
                live_nvme_owner_identities: config
                    .live_nvme
                    .as_ref()
                    .map(|live| live.owner_identities.clone())
                    .unwrap_or_default(),
                epochs: Vec::new(),
                checkpoint: None,
                checkpoint_staging: None,
                checkpoint_obsolete: None,
            };
            persist_manifest(&path, &manifest)?;
            manifest
        };
        if manifest.history_persistence != config.history_persistence {
            return Err(LocalDbError::new(
                "authority manifest history persistence differs",
            ));
        }
        if !matches!(manifest.format, 2..=4)
            || manifest.storage != config.storage
            || manifest.endpoints != config.endpoints
            || manifest.shape_yaml != shape_yaml
            || manifest
                .live_nvme_identity
                .as_ref()
                .map(|(host_nqn, host_id)| (host_nqn.as_str(), *host_id))
                != config
                    .live_nvme
                    .as_ref()
                    .map(|live| (live.host_nqn.as_str(), live.host_id))
            || config
                .live_nvme
                .as_ref()
                .map(|live| &live.owner_identities)
                .is_some_and(|identities| identities != &manifest.live_nvme_owner_identities)
            || (config.live_nvme.is_none() && !manifest.live_nvme_owner_identities.is_empty())
            || manifest.epochs.len() >= config.storage.max_epochs
            || manifest
                .epochs
                .iter()
                .enumerate()
                .any(|(i, e)| e.epoch != i as u64 + 1)
        {
            return Err(LocalDbError::new(
                "authority configuration/history differs or epoch limit reached",
            ));
        }
        manifest
            .replay_limits
            .validate()
            .map_err(LocalDbError::new)?;
        if config.history_persistence != HistoryPersistence::WalReplay
            && !config.replay_limits.covers(manifest.replay_limits)
        {
            return Err(LocalDbError::new(
                "replay limits cannot shrink below the recorded recovery bound on open",
            ));
        }
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        let live_pool = if config.live_nvme.is_some() {
            let registry = super::nvmeof::live_connect::LiveConnectionRegistry::open(
                &path.join("live-nvme-connections"),
            )
            .map_err(LocalDbError::component)?;
            let clients = config
                .endpoints
                .len()
                .checked_mul(config.read_lanes + 1)
                .ok_or_else(|| LocalDbError::new("live client capacity overflow"))?;
            let controllers = clients
                .checked_mul(super::tcp::live_read::LIVE_READ_CACHE_CAPACITY + 1)
                .ok_or_else(|| LocalDbError::new("live controller capacity overflow"))?;
            Some(Arc::new(
                super::nvmeof::live_pool::LiveControllerPool::new(registry, controllers, clients)
                    .map_err(LocalDbError::component)?,
            ))
        } else {
            None
        };
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        let mut live_open_guard = live_pool
            .as_ref()
            .map(|pool| transports::LivePoolOpenGuard::new(pool.clone(), config.timeout));
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        let live_metrics = config
            .live_nvme
            .as_ref()
            .map(|_| super::nvmeof::live_metrics::LiveNvmeMetrics::new());
        let recorded_replay_limits = manifest.replay_limits;
        let mut recovery_stats = RecoveryStats::default();
        let tls_started = Instant::now();
        let tls_config = config
            .tls
            .client_config_with_identity()
            .map_err(LocalDbError::component)?;
        recovery_stats.tls_config_ns = elapsed(tls_started);
        let checkout_started = Instant::now();
        let transport_key = transport_pool
            .as_ref()
            .map(|_| transports::pool_key(&config, &shape_yaml, tls_config.identity))
            .transpose()?;
        let retained = transport_pool
            .as_ref()
            .zip(transport_key)
            .and_then(|(pool, key)| pool.take(key));
        let (retained_controls, retained_reads) = retained
            .map(|idle| (Some(idle.controls), Some(idle.reads)))
            .unwrap_or_default();
        recovery_stats.transport_checkout_ns = elapsed(checkout_started);
        // Read transports prepare concurrently, but own no authority/index and
        // cannot issue reads until every node is fenced and recovery completes.
        let read_setup = transports::ReadSetup::start(
            config.clone(),
            shape.clone(),
            tls_config.clone(),
            retained_reads,
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            live_pool.clone(),
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            live_metrics.clone(),
        )?;
        let connected = transports::connect(
            &config,
            &shape,
            &tls_config,
            1,
            retained_controls,
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            live_pool.clone(),
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            live_metrics.clone(),
        )?;
        recovery_stats.connection_setup_ns = connected.elapsed_ns;
        recovery_stats.created_connections = (connected.clients.len() - connected.reused) as u64;
        recovery_stats.reused_connections = connected.reused as u64;
        let mut clients = connected.clients;
        for client in &clients {
            let status = client.connected_status();
            if status.history_persistence != config.history_persistence {
                return Err(LocalDbError::new(
                    "owner history persistence differs from the coordinator",
                ));
            }
            if status.max_epochs != config.storage.max_epochs
                || status
                    .epochs
                    .iter()
                    .any(|n| !manifest.epochs.iter().any(|e| e.epoch == n.epoch))
                || manifest
                    .epochs
                    .iter()
                    .filter(|e| e.active)
                    .any(|e| !status.epochs.iter().any(|n| n.epoch == e.epoch))
            {
                return Err(LocalDbError::new(
                    "node history disagrees with complete authority inventory",
                ));
            }
            recovery_stats.add_connection(client.connect_profile());
        }
        let epoch = manifest.epochs.len() as u64 + 1;
        manifest.epochs.push(AuthorityEpoch {
            epoch,
            active: false,
        });
        // Reserve the epoch before ANY node is allowed to activate it. A failed
        // partial fence stays in this manifest; the next open advances again.
        persist_manifest(&path, &manifest)?;
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
        let histories = endpoint_map(&mut clients[..fence_count], |node, client| {
            #[cfg(not(all(feature = "experimental-nvmeof", target_os = "linux")))]
            let _ = node;
            let status = client.fence_to(epoch).map_err(LocalDbError::component)?;
            if !status.healthy
                || status.active_epoch != Some(epoch)
                || !status.epochs.iter().any(|e| e.epoch == epoch && !e.sealed)
                || status.epochs.iter().any(|e| e.epoch < epoch && !e.sealed)
            {
                return Err(LocalDbError::new(
                    "node did not establish the complete fenced inventory",
                ));
            }
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            if let Some(metrics) = &live_metrics {
                metrics.record_fence(super::nvmeof::live_metrics::LiveFenceEvidence {
                    partition: config.endpoints[node].partition,
                    authority_epoch: epoch,
                    target: client
                        .live_target_stats()
                        .map_err(LocalDbError::component)?,
                });
            }
            Ok(status
                .epochs
                .into_iter()
                .filter(|e| e.epoch < epoch)
                .map(|e| e.epoch)
                .collect::<Vec<_>>())
        })?;
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
        let mut known = HashMap::new();
        let mut receipt_history = if config.history_persistence == HistoryPersistence::WalReplay {
            None
        } else {
            Some(ReceiptHistory::open(&path, config.storage.database)?)
        };
        let mut positions = BTreeMap::new();
        let retry_window = if config.history_persistence == HistoryPersistence::WalReplay {
            WAL_REPLAY_WINDOW_BATCHES
        } else {
            0
        };
        let mut recent = VecDeque::with_capacity(retry_window);
        let mut recent_ids = HashMap::with_capacity(retry_window);
        let mut latest = None;
        let mut cursors = HashMap::new();
        if let Some(checkpoint) = &manifest.checkpoint {
            load_checkpoint(
                &path,
                checkpoint,
                &manifest,
                &config,
                &shape,
                &mut clients,
                &histories,
                epoch,
                &mut index,
                &mut receipt_history,
                &mut latest,
                &mut cursors,
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
        let replay_started = Instant::now();
        replay_history(
            &mut clients,
            &histories,
            epoch,
            &config,
            recorded_replay_limits,
            &shape,
            &mut index,
            &mut known,
            &receipt_history,
            &recent_ids,
            &mut latest,
            &cursors,
            &mut positions,
            &mut recovery_stats,
            #[cfg(test)]
            interruption,
        )?;
        recovery_stats.tail_replay_ns = elapsed(replay_started);
        // Current checkpoint/proofs and the complete tail have now validated.
        // Resolve only durably named cleanup intents before allowing more work.
        cleanup_checkpoint_artifacts(&path, &mut manifest, false)?;
        let preparation_pool = if config.preparation_workers > 1 {
            Some(PreparationPool::build(config.preparation_workers)?)
        } else {
            None
        };
        let mut workers = Vec::new();
        let write_profile = Arc::new(Mutex::new(WriteProfileSnapshot {
            limit_per_connection: config.write_profile_limit,
            expected_connections: clients.len(),
            ..WriteProfileSnapshot::default()
        }));
        let write_timeline = Arc::new(WriteTimeline::default());
        let concurrent_failure = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let first_failure = Arc::new(std::sync::OnceLock::new());
        let (wake_sender, completion_wake) = mpsc::sync_channel(clients.len());
        for client in clients {
            workers.push(
                ClientWorker::start(
                    client,
                    config
                        .max_pending_batches
                        .max(config.effective_owner_append_occupancy()),
                    wake_sender.clone(),
                    concurrent_failure.clone(),
                    WorkerDiagnostics {
                        write_profile_limit: config.write_profile_limit,
                        write_profile: write_profile.clone(),
                        write_timeline: write_timeline.clone(),
                        first_failure: first_failure.clone(),
                        partition: config.endpoints[workers.len()].partition,
                    },
                )
                .map_err(LocalDbError::component)?,
            );
        }
        let connection_started = Instant::now();
        let prepared_reads = read_setup.finish()?;
        recovery_stats.read_lane_wait_ns = elapsed(connection_started);
        recovery_stats.read_lane_setup_ns = prepared_reads.elapsed_ns;
        recovery_stats.created_connections +=
            (prepared_reads.clients.len() - prepared_reads.reused) as u64;
        recovery_stats.reused_connections += prepared_reads.reused as u64;
        let reads = ReadService::from_clients(
            &config,
            epoch,
            prepared_reads.clients,
            &mut recovery_stats,
            first_failure.clone(),
        )?;
        #[cfg(test)]
        if matches!(interruption, Some(OpenInterruption::AfterReadSetup)) {
            return Err(LocalDbError::new("injected interruption after read setup"));
        }
        manifest.epochs.last_mut().unwrap().active = true;
        // Old suffixes validated against their original durable bound. Only a
        // fully recovered, not-yet-serving epoch may activate a larger bound.
        manifest.format = 4;
        manifest.replay_limits = config.replay_limits;
        persist_manifest(&path, &manifest)?;
        let count = workers.len();
        let database = config.storage.database;
        let limit = config.max_admitted_batches;
        let partition_indexes = config
            .endpoints
            .iter()
            .enumerate()
            .map(|(i, e)| (e.partition, i))
            .collect();
        let checkpoint_diagnostics =
            (config.write_profile_limit != 0).then(|| VecDeque::with_capacity(128));
        let reads = Arc::new(reads);
        let operational_sampler =
            OperationalReadIoSampler::dormant(config.clone(), shape.clone(), tls_config);
        let mut database = Self {
            clients: workers,
            reads,
            preparation_pool,
            read_cache: Arc::new(ReadCache::new(config.read_cache_bytes)),
            write_profile,
            write_timeline,
            transport_pool,
            transport_key,
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            live_pool,
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            live_metrics,
            inventory: config.inventory(epoch),
            owner_dispatch: OwnerDispatchPolicy::new(config.effective_owner_append_occupancy())
                .map_err(LocalDbError::new)?,
            append_byte_budget: config.append_byte_budget()?,
            config,
            operational_sampler,
            shape,
            path,
            epoch,
            index: Arc::new(index),
            partition_indexes,
            known,
            receipt_history,
            stream_last: BTreeMap::new(),
            positions,
            committed_complete,
            recent,
            recent_ids,
            active_identities: HashMap::new(),
            pending_writers: HashMap::new(),
            predecessor_marks: Vec::new(),
            clock: LogicalClock::new_fenced_epoch(epoch),
            publication: PublicationTracker::new(database, epoch, limit)
                .map_err(LocalDbError::component)?,
            submission_base: 0,
            submissions: VecDeque::new(),
            durable_wait_started: VecDeque::new(),
            staged: (0..count).map(|_| VecDeque::new()).collect(),
            staged_bytes: vec![0; count],
            inflight: (0..count).map(|_| VecDeque::new()).collect(),
            inflight_bytes: vec![0; count],
            completed_nodes: vec![false; count],
            unresolved_append: None,
            completion_wake,
            preparation_wake: wake_sender,
            pipeline_stats: PipelineStats::default(),
            reserved_index_bytes: 0,
            pending_bytes: 0,
            partition_admissions: vec![0; count],
            partition_records: vec![0; count],
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
            recovery_stats,
            write_stats: AuthorityWriteStats::default(),
            checkpoint_diagnostic_summary: CheckpointDiagnosticSummary {
                available: checkpoint_diagnostics.is_some(),
                ..CheckpointDiagnosticSummary::default()
            },
            checkpoint_diagnostics,
            next_checkpoint_diagnostic: 0,
            dropped_checkpoint_diagnostics: 0,
            last_checkpoint_diagnostic: None,
            shutdown_stats: ShutdownStats::default(),
            #[cfg(test)]
            checkpoint_interruption: None,
            #[cfg(test)]
            checkpoint_receipt_limit: None,
            #[cfg(test)]
            retire_preflight_busy_once: false,
            healthy: true,
            concurrent_failure,
            first_failure,
            stopped: false,
            stats: LocalDbStats::default(),
            #[cfg(test)]
            before_preparation: None,
            #[cfg(test)]
            owner_start_pause: None,
            #[cfg(test)]
            fail_prepared_operation: None,
            #[cfg(test)]
            concurrent_progress: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            before_checkpoint_io: None,
            #[cfg(test)]
            checkpoint_pause: None,
            #[cfg(test)]
            checkpoint_prefix_pause: None,
            #[cfg(test)]
            corrupt_checkpoint_proof: false,
            #[cfg(test)]
            checkpoint_adoption_pause: None,
            #[cfg(test)]
            checkpoint_cleanup_pause: None,
            #[cfg(test)]
            panic_after_concurrent_admissions: None,
            _lock: lock,
        };
        // A successful recovery establishes a new baseline before admitting
        // another bounded tail, including after repeated crash/reopen cycles.
        if database.checkpoint_tail_batches != 0 {
            let started = Instant::now();
            database.commit_checkpoint()?;
            database.recovery_stats.checkpoint_commit_ns = elapsed(started);
        }
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        if let Some(guard) = &mut live_open_guard {
            guard.disarm();
        }
        Ok(database)
    }

    pub fn backend_id(&self) -> &'static str {
        match &self.config.tls.transport {
            super::tcp::Transport::Tcp => BACKEND_ID,
            super::tcp::Transport::TcpTrusted => "tidehunter-tcp-trusted-rf1",
            super::tcp::Transport::Ofi { fabric, security } => match (&fabric.backend, security) {
                (super::tcp::FabricBackend::Efa, super::tcp::FabricSecurity::MutualTls) => {
                    "tidehunter-ofi-efa-mutual-tls-rf1"
                }
                (super::tcp::FabricBackend::Efa, super::tcp::FabricSecurity::Trusted) => {
                    "tidehunter-ofi-efa-trusted-rf1"
                }
                (super::tcp::FabricBackend::EfaDirect, super::tcp::FabricSecurity::MutualTls) => {
                    "tidehunter-ofi-efa-direct-mutual-tls-rf1"
                }
                (super::tcp::FabricBackend::EfaDirect, super::tcp::FabricSecurity::Trusted) => {
                    "tidehunter-ofi-efa-direct-trusted-rf1"
                }
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
        self.epoch
    }

    pub fn read_evidence_monitor(&self) -> ReadEvidenceMonitor {
        ReadEvidenceMonitor {
            clients: self
                .config
                .endpoints
                .iter()
                .zip(&self.clients)
                .map(|(endpoint, worker)| (endpoint.partition, worker.control_handle()))
                .collect(),
            reads: self.reads.clone(),
        }
    }
    pub fn operational_read_io_monitor(&self) -> OperationalReadIoMonitor {
        self.operational_sampler.activate();
        self.operational_sampler.monitor()
    }

    pub fn validate_cached_read(&self) -> Result<(), LocalDbError> {
        self.ensure_healthy()
    }

    pub fn published_cut(&self) -> Result<IndexReadCut, LocalDbError> {
        self.ensure_healthy()?;
        Ok(self.index.latest_cut())
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
        self.index
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
        self.index
            .get(ks, key, cut)
            .map_err(LocalDbError::component)
    }

    fn ensure_healthy(&self) -> Result<(), LocalDbError> {
        if !self.healthy
            || self.stopped
            || self.reads.failed()
            || self
                .concurrent_failure
                .load(std::sync::atomic::Ordering::Acquire)
        {
            if let Some(version) = self.unresolved_append {
                return Err(LocalDbError::unknown(
                    version,
                    failure_context(
                        &self.first_failure,
                        "TCP authority requires recovery with unresolved append",
                    ),
                ));
            }
            if let Some(pending) = self.inflight.iter().find_map(|queue| queue.front()) {
                return Err(LocalDbError::unknown(
                    pending.first,
                    failure_context(
                        &self.first_failure,
                        "TCP authority requires recovery with unresolved append",
                    ),
                ));
            }
            return Err(LocalDbError::new(failure_context(
                &self.first_failure,
                "TCP authority requires recovery or is closed",
            )));
        }
        self.publication
            .ensure_publication_healthy()
            .map_err(LocalDbError::component)
    }

    /// Bounded admission, possibly starting storage I/O before returning.
    /// Success is NOT a durability/visibility acknowledgement: use drain.
    pub fn submit(
        &mut self,
        operation: [u8; 16],
        mutations: Vec<Mutation>,
    ) -> Result<BatchVersion, LocalDbError> {
        let started = Instant::now();
        let before = self.authority_write_stats();
        let result = self.submit_inner(operation, mutations);
        self.record_admission_span(started, before);
        result
    }

    fn record_admission_span(&mut self, started: Instant, before: AuthorityWriteStats) {
        let after = self.authority_write_stats();
        let nested = (after.checkpoint_ns - before.checkpoint_ns)
            .saturating_add(after.storage_wait_ns - before.storage_wait_ns)
            .saturating_add(after.publication_ns - before.publication_ns)
            .saturating_add(after.dispatch_ns - before.dispatch_ns);
        self.write_stats.preparation_admission_ns += elapsed(started).saturating_sub(nested);
    }

    fn submit_inner(
        &mut self,
        operation: [u8; 16],
        mutations: Vec<Mutation>,
    ) -> Result<BatchVersion, LocalDbError> {
        let prepared = self.prepare_submit(operation, mutations, None)?;
        self.admit_prepared(prepared)
    }

    fn prepare_submit(
        &mut self,
        operation: [u8; 16],
        mutations: Vec<Mutation>,
        authority: Option<&concurrent::AuthorityCounters>,
    ) -> Result<PreparedAdmission, LocalDbError> {
        self.ensure_healthy()?;
        let started = Instant::now();
        let id = BatchId {
            database: self.config.storage.database,
            operation,
        };
        let version = self
            .existing_identity(id)?
            .map(|(v, _)| v)
            .unwrap_or(BatchVersion {
                epoch: self.epoch,
                sequence: self.submission_next(),
            });
        let prepare_span = authority.map(concurrent::AuthorityCounters::preparing);
        let batch = CompleteBatch::new(
            &self.shape,
            id,
            version,
            mutations,
            self.config.storage.batch_limits,
        )
        .map_err(LocalDbError::component);
        drop(prepare_span);
        let batch = batch?;
        let prepared = self.prepare_admission(batch)?;
        self.write_timeline.batch(operation, version);
        self.stats.validation_ns += elapsed(started);
        Ok(prepared)
    }

    fn check_checkpoint_capacity(&self, additional_identities: usize) -> Result<(), LocalDbError> {
        let receipts = self
            .known
            .len()
            .checked_add(self.active_identities.len())
            .and_then(|count| count.checked_add(additional_identities))
            .ok_or_else(|| LocalDbError::new("checkpoint receipt admission count overflow"))?;
        let receipts = if self.config.history_persistence == HistoryPersistence::WalReplay {
            receipts.min(WAL_REPLAY_WINDOW_BATCHES)
        } else {
            receipts
        };
        let fits = checkpoint_metadata_fits(&self.config, receipts);
        // Exercise the same admission boundary without constructing a million
        // metadata entries merely to reach the production byte ceiling.
        #[cfg(test)]
        let fits = fits
            && self
                .checkpoint_receipt_limit
                .is_none_or(|limit| receipts <= limit);
        if !fits {
            return Err(LocalDbError::new(
                "checkpoint receipt metadata admission capacity reached",
            ));
        }
        Ok(())
    }

    fn existing_identity(
        &self,
        id: BatchId,
    ) -> Result<Option<(BatchVersion, super::types::PayloadDigest)>, LocalDbError> {
        if let Some(receipt) = self.known.get(&id).copied() {
            return Ok(Some((receipt.version(), receipt.digest())));
        }
        if let Some(identity) = self.recent_ids.get(&id) {
            return Ok(Some(*identity));
        }
        if let Some(receipt) = self
            .receipt_history
            .as_ref()
            .map(|history| history.lookup(id))
            .transpose()
            .inspect_err(|error| {
                record_first_failure(
                    &self.first_failure,
                    None,
                    None,
                    "receipt_history_lookup",
                    error,
                );
                self.concurrent_failure
                    .store(true, std::sync::atomic::Ordering::Release);
                self.reads.poison();
            })?
            .flatten()
        {
            return Ok(Some((receipt.version(), receipt.digest())));
        }
        Ok(self.active_identities.get(&id).map(|i| {
            let batch = self.submissions[*i - self.submission_base as usize]
                .batch
                .as_ref()
                .unwrap();
            (batch.version(), batch.digest())
        }))
    }

    fn prepare_admission(&self, batch: CompleteBatch) -> Result<PreparedAdmission, LocalDbError> {
        #[cfg(test)]
        if self.fail_prepared_operation == Some(batch.id().operation) {
            return Err(LocalDbError::new("injected admission preparation failure"));
        }
        let batch = batch.into_canonical_owned();
        let partition = self
            .inventory
            .partition_for_validated(batch.id())
            .map_err(LocalDbError::component)?;
        let partition = self.partition_indexes[&partition];
        let routing = NativePublicationRouting::prepare(&self.shape, &batch)
            .map_err(LocalDbError::component)?;
        let metadata_bytes = self
            .index
            .estimated_routed_bytes(routing.operations())
            .map_err(LocalDbError::component)?;
        let worker_metadata = batch
            .mutations()
            .len()
            .checked_mul(
                std::mem::size_of::<Mutation>()
                    + std::mem::size_of::<super::types::RecordDescriptor>()
                    // Include geometric dependency-table slack until it shrinks.
                    + 4 * std::mem::size_of::<(DependencyKey, u64)>()
                    + 2 * std::mem::size_of::<u64>(),
            )
            .ok_or_else(|| LocalDbError::new("worker metadata reservation overflow"))?;
        let dependency_key_bytes = routing
            .operations()
            .iter()
            .try_fold(0usize, |bytes, operation| {
                bytes.checked_add(if operation.reduced_key().len() > 32 {
                    operation.reduced_key().len().checked_mul(2)?
                } else {
                    0
                })
            })
            .ok_or_else(|| LocalDbError::new("dependency key reservation overflow"))?;
        let credit = batch
            .canonical_heap_bytes()
            .and_then(|bytes| bytes.checked_add(worker_metadata))
            .and_then(|bytes| bytes.checked_add(dependency_key_bytes))
            .and_then(|bytes| bytes.checked_add(routing.owned_bytes(&self.shape)?))
            .and_then(|bytes| bytes.checked_add(2 * std::mem::size_of::<CompleteBatch>() + 512))
            .ok_or_else(|| LocalDbError::new("pending byte overflow"))?;
        let group_charge = CompleteGroup::encoded_member_len(batch.encoded().len())
            .map_err(LocalDbError::component)?;
        Ok(PreparedAdmission {
            batch,
            routing,
            partition,
            metadata_bytes,
            credit,
            group_charge,
        })
    }

    fn check_storage_group_capacity(
        &self,
        batches: usize,
        records: usize,
    ) -> Result<(), LocalDbError> {
        ReferenceDirectory::check_fresh_total_capacity(
            self.config.storage.max_batches_per_partition.min(1024),
            self.config.storage.max_reference_metadata_bytes,
            batches,
            records,
        )
        .map_err(LocalDbError::component)
    }
    fn admission_pressure_fits(&self, pressure: AdmissionPressure) -> bool {
        match pressure {
            AdmissionPressure::Tail {
                encoded_bytes,
                identity,
            } => {
                (self.config.history_persistence == HistoryPersistence::WalReplay
                    || !self.config.replay_limits.requires_flush(
                        self.checkpoint_tail_batches,
                        self.checkpoint_tail_bytes,
                        encoded_bytes,
                    ))
                    && (!identity
                        || self
                            .known
                            .len()
                            .saturating_add(self.active_identities.len())
                            < self.config.max_admitted_batches)
            }
            AdmissionPressure::Partition { partition, records } => self
                .check_storage_group_capacity(
                    self.partition_admissions[partition].saturating_add(1),
                    self.partition_records[partition].saturating_add(records),
                )
                .is_ok(),
            AdmissionPressure::Index { additional_bytes } => self
                .reserved_index_bytes
                .checked_add(additional_bytes)
                .is_some_and(|reserved| self.index.check_capacity(reserved).is_ok()),
        }
    }

    fn ensure_admission_pressure(
        &mut self,
        pressure: AdmissionPressure,
    ) -> Result<(), LocalDbError> {
        let enabled = self.checkpoint_diagnostics.is_some();
        let started = Instant::now();
        let requested_cut = self.latest;
        let before_index = self.reserved_index_bytes;
        let identity_pressure = self
            .known
            .len()
            .saturating_add(self.active_identities.len())
            >= self.config.max_admitted_batches;
        let mut details = CheckpointDiagnostic::default();
        let result =
            self.ensure_admission_pressure_inner(pressure, enabled.then_some(&mut details));
        let pressure_wait_ns = elapsed(started);
        if result.is_err() && details.outcome.is_none() {
            details.outcome = Some(CheckpointProgressOutcome::ProgressError);
        }
        if enabled {
            let trigger = match pressure {
                AdmissionPressure::Tail { identity: true, .. } if identity_pressure => {
                    CheckpointTrigger::KnownIdentities
                }
                AdmissionPressure::Tail { .. } => CheckpointTrigger::Tail,
                AdmissionPressure::Partition { .. } => CheckpointTrigger::PartitionWindow,
                AdmissionPressure::Index { .. } => CheckpointTrigger::IndexReservation,
            };
            let record = CheckpointDiagnostic {
                epoch: self.epoch,
                generation: None,
                trigger: Some(trigger),
                requested_cut,
                reserved_index_bytes_before: before_index,
                reserved_index_bytes_after: self.reserved_index_bytes,
                pressure_wait_ns: Some(pressure_wait_ns),
                ..details
            };
            self.record_checkpoint_diagnostic(record);
        }
        self.write_stats.foreground_checkpoint_ns += pressure_wait_ns;
        self.write_stats.foreground_checkpoint_count += 1;
        result
    }

    fn ensure_admission_pressure_inner(
        &mut self,
        pressure: AdmissionPressure,
        mut diagnostic: Option<&mut CheckpointDiagnostic>,
    ) -> Result<(), LocalDbError> {
        match pressure {
            AdmissionPressure::Tail { encoded_bytes, .. }
                if self.config.history_persistence != HistoryPersistence::WalReplay
                    && encoded_bytes > self.config.replay_limits.max_encoded_bytes =>
            {
                return Err(LocalDbError::new(
                    "batch exceeds checkpoint tail byte bound",
                ));
            }
            AdmissionPressure::Partition { records, .. } => {
                self.check_storage_group_capacity(1, records)?;
            }
            AdmissionPressure::Tail { .. } => {}
            AdmissionPressure::Index { .. } => {}
        }
        if diagnostic.is_some() {
            self.last_checkpoint_diagnostic = None;
        }
        if self.admission_pressure_fits(pressure) {
            if let Some(diagnostic) = diagnostic.as_deref_mut() {
                diagnostic.outcome = Some(CheckpointProgressOutcome::AlreadyAvailable);
            }
            return Ok(());
        }

        let drain_started = diagnostic.as_ref().map(|_| Instant::now());
        self.drain()?;
        if let (Some(diagnostic), Some(started)) = (diagnostic.as_deref_mut(), drain_started) {
            diagnostic.drain_ns = Some(elapsed(started));
        }
        if self.admission_pressure_fits(pressure) {
            if let Some(diagnostic) = diagnostic.as_deref_mut() {
                diagnostic.outcome = Some(CheckpointProgressOutcome::AfterDrain);
            }
            return Ok(());
        }
        if self.background_checkpoint.is_some() {
            let started = diagnostic.as_ref().map(|_| Instant::now());
            self.progress_checkpoint(true)?;
            if let (Some(diagnostic), Some(started)) = (diagnostic.as_deref_mut(), started) {
                diagnostic.existing_capture_wait_ns = Some(elapsed(started));
            }
            if let Some(diagnostic) = diagnostic.as_deref_mut() {
                self.include_checkpoint_diagnostic(diagnostic);
            }
        }
        if self.admission_pressure_fits(pressure) {
            if let Some(diagnostic) = diagnostic.as_deref_mut() {
                diagnostic.outcome = Some(CheckpointProgressOutcome::AfterExistingCapture);
            }
            return Ok(());
        }

        let started = diagnostic.as_ref().map(|_| Instant::now());
        self.start_checkpoint()?;
        if self.background_checkpoint.is_some() {
            self.progress_checkpoint(true)?;
        }
        if let (Some(diagnostic), Some(started)) = (diagnostic.as_deref_mut(), started) {
            diagnostic.new_capture_wait_ns = Some(elapsed(started));
        }
        if let Some(diagnostic) = diagnostic.as_deref_mut() {
            self.include_checkpoint_diagnostic(diagnostic);
        }
        if self.admission_pressure_fits(pressure) {
            if let Some(diagnostic) = diagnostic.as_deref_mut() {
                diagnostic.outcome = Some(CheckpointProgressOutcome::AfterNewCapture);
            }
            return Ok(());
        }
        if let Some(diagnostic) = diagnostic {
            diagnostic.outcome = Some(CheckpointProgressOutcome::CapacityError);
        }
        self.admission_capacity_error(pressure)
    }

    fn admission_capacity_error(&self, pressure: AdmissionPressure) -> Result<(), LocalDbError> {
        match pressure {
            AdmissionPressure::Tail { identity: true, .. } => {
                Err(LocalDbError::new("checkpoint identity capacity reached"))
            }
            AdmissionPressure::Tail { .. } => {
                Err(LocalDbError::new("checkpoint tail capacity reached"))
            }
            AdmissionPressure::Partition { partition, records } => {
                let batches = self.partition_admissions[partition]
                    .checked_add(1)
                    .ok_or_else(|| LocalDbError::new("partition reservation overflow"))?;
                let records = self.partition_records[partition]
                    .checked_add(records)
                    .ok_or_else(|| LocalDbError::new("reference reservation overflow"))?;
                self.check_storage_group_capacity(batches, records)
            }
            AdmissionPressure::Index { additional_bytes } => {
                let reserved = self
                    .reserved_index_bytes
                    .checked_add(additional_bytes)
                    .ok_or_else(|| LocalDbError::new("index reservation overflow"))?;
                self.index
                    .check_capacity(reserved)
                    .map_err(LocalDbError::component)
            }
        }
    }
    fn trace_admission_pressure(&self, pressure: Option<&AdmissionPressure>) {
        if !self.write_timeline.enabled() {
            return;
        }
        let (reason, incoming_encoded_bytes) = match pressure {
            Some(AdmissionPressure::Tail { encoded_bytes, .. }) => {
                let reason =
                    if self.checkpoint_tail_batches >= self.config.replay_limits.max_batches {
                        "tail_batches"
                    } else if self
                        .checkpoint_tail_bytes
                        .checked_add(*encoded_bytes)
                        .is_none_or(|bytes| bytes > self.config.replay_limits.max_encoded_bytes)
                    {
                        "tail_bytes"
                    } else {
                        "identity"
                    };
                (reason, Some(*encoded_bytes))
            }
            Some(AdmissionPressure::Partition { .. }) => ("partition", None),
            Some(AdmissionPressure::Index { .. }) => ("index", None),
            None => ("pending", None),
        };
        self.write_timeline.checkpoint_pressure(
            self.epoch,
            self.background_checkpoint
                .as_ref()
                .map(|_| self.snapshot_generation),
            CheckpointPressure {
                reason,
                batches: self.checkpoint_tail_batches,
                encoded_bytes: self.checkpoint_tail_bytes,
                max_batches: self.config.replay_limits.max_batches,
                max_encoded_bytes: self.config.replay_limits.max_encoded_bytes,
                incoming_encoded_bytes,
            },
        );
    }

    fn include_checkpoint_diagnostic(&self, diagnostic: &mut CheckpointDiagnostic) {
        let Some(checkpoint) = self.last_checkpoint_diagnostic else {
            return;
        };
        diagnostic.checkpoint_generation = checkpoint.generation;
        diagnostic.captured_cut = checkpoint.captured_cut;
        diagnostic.captured_batches = checkpoint.captured_batches;
        diagnostic.captured_bytes = checkpoint.captured_bytes;
    }
    // Shared by synchronous admission and the actor's deferred FIFO head.
    // No version or reservation is allocated here.
    fn prepared_pressure(
        &self,
        prepared: &PreparedAdmission,
    ) -> Result<Option<AdmissionPressure>, LocalDbError> {
        let batch = &prepared.batch;
        if let Some((version, digest)) = self.existing_identity(batch.id())? {
            if batch.version() != version || batch.digest() != digest {
                return Err(LocalDbError::new("retry changed original batch"));
            }
            return Ok(None);
        }
        let encoded_bytes = batch.encoded().len();
        if encoded_bytes > self.config.replay_limits.max_encoded_bytes {
            return Err(LocalDbError::new(
                "batch exceeds checkpoint tail byte bound",
            ));
        }
        let records = batch.mutations().len();
        self.check_storage_group_capacity(1, records)?;
        self.partition_records[prepared.partition]
            .checked_add(records)
            .ok_or_else(|| LocalDbError::new("reference reservation overflow"))?;
        self.reserved_index_bytes
            .checked_add(prepared.metadata_bytes)
            .ok_or_else(|| LocalDbError::new("index reservation overflow"))?;
        for pressure in [
            AdmissionPressure::Tail {
                encoded_bytes,
                identity: true,
            },
            AdmissionPressure::Partition {
                partition: prepared.partition,
                records,
            },
            AdmissionPressure::Index {
                additional_bytes: prepared.metadata_bytes,
            },
        ] {
            if !self.admission_pressure_fits(pressure) {
                return Ok(Some(pressure));
            }
        }
        Ok(None)
    }

    // Only private, locally constructed CompleteBatch values reach this path.
    // Preparation does not allocate a logical slot or confer authority.
    fn admit_prepared(
        &mut self,
        prepared: PreparedAdmission,
    ) -> Result<BatchVersion, LocalDbError> {
        self.admit_prepared_with_progress(prepared, true)
    }

    fn admit_prepared_with_progress(
        &mut self,
        prepared: PreparedAdmission,
        progress: bool,
    ) -> Result<BatchVersion, LocalDbError> {
        self.ensure_healthy()?;
        if progress
            && self.config.pipelined_writes
            && self.inflight.iter().any(|queue| !queue.is_empty())
        {
            self.progress_pipeline(false)?;
        }
        let started = Instant::now();
        let mut validation_excluded_ns = 0;
        let id = prepared.batch.id();
        let version = prepared.batch.version();
        if let Some((previous, digest)) = self.existing_identity(id)? {
            if version != previous || prepared.batch.digest() != digest {
                return Err(LocalDbError::new("retry changed original batch"));
            }
            return Ok(previous);
        }
        if self.submissions.len() >= self.config.max_admitted_batches
            || self.submissions.len() >= self.config.max_pending_batches
        {
            return Err(LocalDbError::new(
                "TCP authority admission capacity reached",
            ));
        }
        // Reserve checkpoint receipt space BEFORE remote effects, including all
        // outstanding identities and worst-case future retained empty epochs.
        self.check_checkpoint_capacity(1)?;
        while let Some(pressure) = self.prepared_pressure(&prepared)? {
            self.ensure_admission_pressure(pressure)?;
            validation_excluded_ns = elapsed(started);
        }
        let PreparedAdmission {
            batch,
            routing,
            partition,
            metadata_bytes,
            credit,
            group_charge,
        } = prepared;
        let encoded_bytes = batch.encoded().len();
        if id.database != self.config.storage.database
            || version
                != (BatchVersion {
                    epoch: self.epoch,
                    sequence: self.submission_next(),
                })
        {
            return Err(LocalDbError::new(
                "prepared identity/version is not next admission",
            ));
        }
        let reserved = self
            .reserved_index_bytes
            .checked_add(metadata_bytes)
            .ok_or_else(|| LocalDbError::new("index reservation overflow"))?;
        self.index
            .check_capacity(reserved)
            .map_err(LocalDbError::component)?;
        let records = self.partition_records[partition]
            .checked_add(batch.mutations().len())
            .ok_or_else(|| LocalDbError::new("reference reservation overflow"))?;
        let pending_bytes = self
            .pending_bytes
            .checked_add(credit)
            .filter(|n| *n <= self.config.max_pending_bytes)
            .ok_or_else(|| LocalDbError::new("pending byte budget reached"))?;
        let staged_bytes = self.staged_bytes[partition]
            .checked_add(group_charge)
            .ok_or_else(|| LocalDbError::new("staging byte overflow"))?;
        // Marks are indexed by the bounded dense admission window. A unique
        // admission sequence deduplicates repeated keys without allocating a
        // temporary set or a vector sized to every operation in a large batch.
        self.predecessor_marks
            .resize(self.submissions.len(), u64::MAX);
        let mut predecessors = SmallVec::new();
        for operation in routing.operations() {
            if let Some(&prior) = self
                .pending_writers
                .get(&operation.ks())
                .and_then(|writers| writers.get(operation.reduced_key().as_ref()))
            {
                let mark = &mut self.predecessor_marks[(prior - self.submission_base) as usize];
                if *mark != version.sequence {
                    *mark = version.sequence;
                    predecessors.push(prior);
                }
            }
        }
        let last_mutation = batch
            .mutation_version(batch.mutations().len() - 1)
            .ok_or_else(|| LocalDbError::new("empty admitted batch"))?;
        // New admissions can become exclusions in a later frozen view. Grow all
        // outstanding reservations before exposing that possibility to WAL I/O.
        // A serial write has no exclusions and therefore needs no view quota.
        //
        // Every admission grows each older live reservation to at least its own
        // exclusion capacity and reserves exactly that capacity for itself, so
        // capacity never increases from the oldest live reservation to the newest
        // (published entries drop theirs; they never rejoin). The reservations
        // needing growth are thus a newest-first run, and one pass stops at the
        // first one that already fits. While the window grows every older one
        // needs growth, so the pass is bounded by the window, not by the batch.
        let publication_view = {
            let max_excluded = self.submissions.len();
            let mut additional = NativeIndex::publication_view_charge(max_excluded)
                .map_err(LocalDbError::component)?;
            let mut scanned = 0u64;
            let mut growing = 0usize;
            for reservation in self
                .submissions
                .iter()
                .rev()
                .filter_map(|entry| entry.publication_view.as_ref())
            {
                scanned += 1;
                let needed = reservation
                    .additional_charge(max_excluded)
                    .map_err(LocalDbError::component)?;
                if needed == 0 {
                    break;
                }
                additional = additional
                    .checked_add(needed)
                    .ok_or_else(|| LocalDbError::new("publication view reservation overflow"))?;
                growing += 1;
            }
            // The writer handle excludes every other index writer (publication,
            // checkpoint capture), never readers. A reader pin may take quota
            // between the preflight and the growth; the growth then reports
            // `Capacity` before any WAL effect, and an already grown reservation
            // only holds more headroom for later admissions.
            let index = self.index.writer();
            let exclusive = Instant::now();
            index
                .check_publication_view_capacity(additional)
                .map_err(LocalDbError::component)?;
            for reservation in self
                .submissions
                .iter_mut()
                .rev()
                .filter_map(|entry| entry.publication_view.as_mut())
                .take(growing)
            {
                index
                    .grow_publication_view(reservation, max_excluded)
                    .map_err(LocalDbError::component)?;
            }
            let reserved = index
                .reserve_publication_view(max_excluded)
                .map_err(LocalDbError::component)?;
            let held = elapsed(exclusive);
            self.write_stats.admission_view_scanned += scanned;
            self.write_stats.admission_view_grown += growing as u64;
            self.write_stats.admission_exclusive_lock_ns += held;
            self.write_stats.admission_exclusive_lock_max_ns =
                self.write_stats.admission_exclusive_lock_max_ns.max(held);
            reserved
        };
        self.stats.validation_ns += elapsed(started).saturating_sub(validation_excluded_ns);
        self.healthy = false;
        if self.clock.allocate() != Some(version) {
            return Err(LocalDbError::unknown(
                version,
                "logical allocation mismatch",
            ));
        }
        self.publication
            .admit(&batch)
            .map_err(|e| LocalDbError::unknown(version, e))?;
        self.publication
            .mark_in_flight(id)
            .map_err(|e| LocalDbError::unknown(version, e))?;
        self.active_identities.insert(id, version.sequence as usize);
        for operation in routing.operations() {
            let writers = self.pending_writers.entry(operation.ks()).or_default();
            if let Some(writer) = writers.get_mut(operation.reduced_key().as_ref()) {
                *writer = version.sequence;
            } else {
                writers.insert(
                    DependencyKey::from_slice(operation.reduced_key().as_ref()),
                    version.sequence,
                );
            }
        }
        self.submissions.push_back(Submission {
            batch: Some(batch),
            publication_view: Some(publication_view),
            predecessors,
            dependency_blocked: false,
            last_mutation,
            encoded_bytes,
            metadata_bytes,
            credit,
            routing: Some(routing),
        });
        self.durable_wait_started.push_back(None);
        self.staged[partition].push_back(version.sequence as usize);
        if self.background_checkpoint.is_some() {
            self.write_stats.background_checkpoint_suffix_batches += 1;
        }
        self.staged_bytes[partition] = staged_bytes;
        self.partition_admissions[partition] += 1;
        self.partition_records[partition] = records;
        self.pending_bytes = pending_bytes;
        self.reserved_index_bytes = reserved;
        self.stats.admitted_batches += 1;
        self.checkpoint_tail_batches += 1;
        self.checkpoint_tail_bytes += encoded_bytes;
        self.healthy = true;
        self.write_timeline
            .wait_enter(WriteWait::AdmittedToAppendEnqueue, 1);
        self.write_timeline
            .batch_event(version, WriteTimelineStage::Admitted, None, None);
        if self.config.pipelined_writes && self.ready_group(partition) {
            self.progress_pipeline(false)?;
        }
        Ok(version)
    }

    /// Finish all accepted work. Only this barrier (or write_batch) acknowledges
    /// durable ordered visibility. Earlier submit calls may already have appended.
    pub fn drain(&mut self) -> Result<(), LocalDbError> {
        if let Err(error) = self.ensure_healthy() {
            if self.unresolved_append.is_none() {
                self.unresolved_append = self
                    .inflight
                    .iter()
                    .find_map(|queue| queue.front().map(|pending| pending.first));
            }
            self.healthy = false;
            self.inflight.iter_mut().for_each(VecDeque::clear);
            self.inflight_bytes.fill(0);
            return Err(error);
        }
        if self.config.pipelined_writes
            && self.submissions.len() == 1
            && self.inflight.iter().all(VecDeque::is_empty)
        {
            return self.drain_single();
        }
        loop {
            self.progress_pipeline(true)?;
            if self.submissions.is_empty() {
                return Ok(());
            }
            if self.inflight.iter().all(VecDeque::is_empty) {
                self.healthy = false;
                return Err(LocalDbError::new(
                    "unresolved publication without storage work",
                ));
            }
            // Wake on ANY node completion so a slow prefix does not stop other
            // nodes receiving their next group. Replies remain authoritative;
            // stale/coalesced notifications are harmless, bounded hints.
            let started = Instant::now();
            let _ = self
                .completion_wake
                .recv_timeout(Duration::from_millis(100));
            self.stats.storage_wait_ns += elapsed(started);
            // A timeout only repolls the result receivers. It never fabricates
            // an aborted batch; disconnected receivers are handled above.
        }
    }

    /// A lone write has no independent storage work to overlap. Message mode
    /// may use the caller's protected stream; live mode and overlapping
    /// checkpoints dispatch through the owner worker. Publication is identical.
    fn drain_single(&mut self) -> Result<(), LocalDbError> {
        self.healthy = false;
        let node = self
            .staged
            .iter()
            .position(|queue| !queue.is_empty())
            .ok_or_else(|| LocalDbError::new("single admission has no staged request"))?;
        let slot = *self.staged[node].front().unwrap();
        if self.staged.iter().map(VecDeque::len).sum::<usize>() != 1
            || slot != self.publication.published_prefix().next_sequence() as usize
        {
            return Err(LocalDbError::new(
                "single write staging disagrees with publication prefix",
            ));
        }
        let batch = self.submissions[slot - self.submission_base as usize]
            .batch
            .as_ref()
            .unwrap();
        let version = batch.version();
        self.pipeline_stats.append_requests += 1;
        self.pipeline_stats.peak_inflight_nodes = self.pipeline_stats.peak_inflight_nodes.max(1);
        let started = Instant::now();
        let receipts = if self.background_checkpoint.is_some() || self.config.live_nvme.is_some() {
            self.clients[node]
                .append(self.epoch, vec![batch.clone()])
                .and_then(receive)
                .map_err(|error| {
                    self.unresolved_append = Some(version);
                    LocalDbError::unknown(version, error)
                })?
        } else {
            self.pipeline_stats.direct_appends += 1;
            match self.clients[node]
                .client
                .lock()
                .append_many(self.epoch, std::slice::from_ref(batch))
            {
                Ok(receipts) => receipts,
                Err(error) => {
                    self.unresolved_append = Some(version);
                    return Err(LocalDbError::unknown(version, error));
                }
            }
        };
        self.stats.storage_wait_ns += elapsed(started);
        self.staged[node].pop_front();
        self.staged_bytes[node] = 0;
        self.accept_receipts(receipts)?;
        self.publish_completed()?;
        if !self.submissions.is_empty() {
            return Err(LocalDbError::unknown(
                version,
                "single write did not publish",
            ));
        }
        self.healthy = true;
        self.progress_checkpoint(false)?;
        if self.checkpoint_due() {
            self.start_checkpoint()?;
        }
        Ok(())
    }

    fn accept_receipts(&mut self, receipts: Vec<DurableBatch>) -> Result<(), LocalDbError> {
        for receipt in receipts {
            let version = receipt.appended().version();
            self.write_timeline
                .wait_leave(WriteWait::HarvestToDurable, 1);
            self.write_timeline.batch_event(
                version,
                WriteTimelineStage::DurableObserved,
                None,
                None,
            );
            if version.epoch != self.epoch
                || version.sequence < self.submission_base
                || version.sequence >= self.submission_next()
            {
                return Err(LocalDbError::unknown(
                    version,
                    "receipt outside active admission window",
                ));
            }
            let durable_prefix = self.publication.durable_prefix().next_sequence();
            if version.sequence > durable_prefix
                && let Some(wait_started) = self
                    .durable_wait_started
                    .get_mut((version.sequence - self.submission_base) as usize)
                && wait_started.is_none()
            {
                *wait_started = Some(Instant::now());
                self.write_stats.durable_receipt_publication_waiting_batches += 1;
                self.write_stats.durable_receipt_publication_waiting_peak = self
                    .write_stats
                    .durable_receipt_publication_waiting_peak
                    .max(self.write_stats.durable_receipt_publication_waiting_batches);
            }
            self.publication
                .record_durable(receipt)
                .map_err(|e| LocalDbError::unknown(version, e))?;
            self.pipeline_stats.durable_batches += 1;
        }
        Ok(())
    }

    fn ready_group(&self, node: usize) -> bool {
        let limits = self.config.storage.group_limits;
        !self.staged[node].is_empty()
            && (self.staged[node].len() >= limits.max_batches as usize
                || self.staged_bytes[node] + GROUP_HEADER_LEN
                    >= limits.max_encoded_bytes - limits.max_encoded_bytes / 4)
    }

    fn progress_pipeline(&mut self, flush_tails: bool) -> Result<(), LocalDbError> {
        self.healthy = false;
        let result = (|| {
            self.completed_nodes.fill(false);
            for node in 0..self.inflight.len() {
                let mut position = 0;
                while position < self.inflight[node].len() {
                    let pending = &self.inflight[node][position];
                    let completed = match pending.reply.try_recv() {
                        Ok(result) => Some(
                            result.map_err(|error| LocalDbError::unknown(pending.first, error)),
                        ),
                        Err(mpsc::TryRecvError::Empty) => None,
                        Err(mpsc::TryRecvError::Disconnected) => Some(Err(LocalDbError::unknown(
                            pending.first,
                            "TCP worker lost request outcome",
                        ))),
                    };
                    if let Some(receipts) = completed {
                        let first = pending.first;
                        self.inflight_bytes[node] -= pending.charge;
                        self.inflight[node].remove(position);
                        match receipts {
                            Ok(receipts) => {
                                if let Err(error) = self.accept_receipts(receipts) {
                                    self.unresolved_append = Some(first);
                                    return Err(error);
                                }
                                self.completed_nodes[node] = true;
                                if !flush_tails {
                                    self.pipeline_stats.completions_before_drain += 1;
                                }
                            }
                            Err(error) => {
                                self.unresolved_append = Some(first);
                                return Err(error);
                            }
                        }
                    } else {
                        position += 1;
                    }
                }
            }

            let started = Instant::now();
            let mode = if flush_tails {
                DispatchMode::DrainReady
            } else {
                DispatchMode::Early
            };
            // Bounded fair rounds flush only work already admitted. Each owner
            // gets at most one group per round, including queued worker commands.
            for _ in 0..self.owner_dispatch.rounds(mode) {
                let mut dispatched = false;
                for node in 0..self.clients.len() {
                    let refill = self.completed_nodes[node];
                    if !self.owner_dispatch.should_form_group(
                        mode,
                        self.inflight[node].len(),
                        self.staged[node].len(),
                        self.staged_bytes[node],
                        GROUP_HEADER_LEN,
                        self.ready_group(node),
                        refill,
                    ) {
                        continue;
                    }
                    let (count, charge) = {
                        let limits = self.config.storage.group_limits;
                        let mut bytes = GROUP_HEADER_LEN;
                        let mut count = 0;
                        let mut records = 0usize;
                        let mut charge = 64usize;
                        let mut byte_limited = false;
                        for &slot in &self.staged[node] {
                            let batch = self.submissions[slot - self.submission_base as usize]
                                .batch
                                .as_ref()
                                .unwrap();
                            let next = CompleteGroup::encoded_member_len(batch.encoded().len())
                                .map_err(LocalDbError::component)?;
                            let next_records = records
                                .checked_add(batch.mutations().len())
                                .ok_or_else(|| {
                                    LocalDbError::new("group reference reservation overflow")
                                })?;
                            if count == limits.max_batches as usize
                                || bytes + next > limits.max_encoded_bytes
                                || self
                                    .check_storage_group_capacity(count + 1, next_records)
                                    .is_err()
                            {
                                break;
                            }
                            let next_charge =
                                AppendByteBudget::add_batch_charge(charge, batch.encoded().len())
                                    .ok_or_else(|| {
                                    LocalDbError::new("owner append byte charge overflow")
                                })?;
                            if !self
                                .append_byte_budget
                                .admits(self.inflight_bytes[node], next_charge)
                            {
                                byte_limited = true;
                                break;
                            }
                            charge = next_charge;
                            bytes += next;
                            records = next_records;
                            count += 1;
                        }
                        if count == 0 {
                            if byte_limited {
                                continue;
                            }
                            return Err(LocalDbError::new("batch cannot fit remote group"));
                        }
                        let full = count == limits.max_batches as usize
                            || count < self.staged[node].len()
                            || bytes >= limits.max_encoded_bytes - limits.max_encoded_bytes / 4;
                        if !self
                            .owner_dispatch
                            .should_dispatch_group(mode, refill, full)
                        {
                            continue;
                        }
                        (count, charge)
                    };
                    let submissions = &self.submissions;
                    let submission_base = self.submission_base as usize;
                    let batches: Vec<_> = self.staged[node]
                        .drain(..count)
                        .map(|slot| {
                            submissions[slot - submission_base]
                                .batch
                                .as_ref()
                                .unwrap()
                                .clone()
                        })
                        .collect();
                    for batch in &batches {
                        self.staged_bytes[node] -=
                            CompleteGroup::encoded_member_len(batch.encoded().len())
                                .map_err(LocalDbError::component)?;
                    }
                    self.write_timeline
                        .wait_leave(WriteWait::AdmittedToAppendEnqueue, batches.len());
                    if self.write_timeline.enabled() {
                        for batch in &batches {
                            self.write_timeline.batch_event(
                                batch.version(),
                                WriteTimelineStage::AppendEnqueueStarted,
                                Some(self.config.endpoints[node].partition),
                                None,
                            );
                        }
                    }
                    let first = batches[0].version();
                    let reply = match self.clients[node].append(self.epoch, batches) {
                        Ok(reply) => reply,
                        Err(error) => {
                            self.unresolved_append = Some(first);
                            return Err(LocalDbError::unknown(first, error));
                        }
                    };
                    self.inflight_bytes[node] += charge;
                    self.inflight[node].push_back(PendingAppend {
                        first,
                        reply,
                        charge,
                    });
                    dispatched = true;
                    self.pipeline_stats.append_requests += 1;
                    if !flush_tails {
                        self.pipeline_stats.early_dispatches += 1;
                    }
                }
                if !flush_tails || !dispatched {
                    break;
                }
            }
            self.stats.dispatch_ns += elapsed(started);
            self.pipeline_stats.peak_inflight_nodes = self.pipeline_stats.peak_inflight_nodes.max(
                self.inflight
                    .iter()
                    .filter(|queue| !queue.is_empty())
                    .count(),
            );
            self.publish_completed()?;
            Ok(())
        })();
        if let Err(error) = &result {
            record_first_failure(
                &self.first_failure,
                None,
                error.uncertain_version,
                "progress_pipeline",
                error,
            );
            self.concurrent_failure
                .store(true, std::sync::atomic::Ordering::Release);
            self.reads.poison();
            self.inflight.iter_mut().for_each(VecDeque::clear);
            self.inflight_bytes.fill(0);
        } else {
            self.healthy = true;
        }
        if result.is_ok() {
            self.progress_checkpoint(false)?;
            if self.checkpoint_due() {
                self.start_checkpoint()?;
            }
        }
        result
    }

    fn publish_completed(&mut self) -> Result<(), LocalDbError> {
        if self.publication.durable_unpublished() == 0 {
            return Ok(());
        }
        let started = Instant::now();
        let mut cache_candidates = SmallVec::<[_; 4]>::new();
        let mut publication_view = None;
        let submissions = &mut self.submissions;
        let submission_base = self.submission_base;
        let mut contiguous_next = self.publication.published_prefix().next_sequence();
        let mut index_guard = self.index.writer();
        let _poison = concurrent::FailureOnPanic(self.concurrent_failure.clone());
        let index = &mut index_guard;
        let known = &mut self.known;
        let active = &mut self.active_identities;
        let pending_writers = &mut self.pending_writers;
        let latest = &mut self.latest;
        let live_latest = &mut self.live_latest;
        let reserved = &mut self.reserved_index_bytes;
        let waits = &mut self.durable_wait_started;
        let write_stats = &mut self.write_stats;
        let progress = self
            .publication
            .publish_available(|durable| {
                let receipt = durable.appended();
                let sequence = receipt.version().sequence;
                let slot = (sequence - submission_base) as usize;
                let entry = submissions
                    .get(slot)
                    .ok_or_else(|| LocalDbError::new("receipt outside admission inventory"))?;
                if entry.predecessors.iter().any(|&prior| {
                    prior >= submission_base
                        && submissions[(prior - submission_base) as usize]
                            .batch
                            .is_some()
                }) {
                    let entry = &mut submissions[slot];
                    if !entry.dependency_blocked {
                        entry.dependency_blocked = true;
                        write_stats.dependency_wait_batches += 1;
                    }
                    return Ok(false);
                }
                let entry = &mut submissions[slot];
                let batch = entry
                    .batch
                    .as_ref()
                    .ok_or_else(|| LocalDbError::new("batch already published"))?;
                let routing = entry
                    .routing
                    .take()
                    .ok_or_else(|| LocalDbError::new("missing prepared publication routing"))?;
                // Only the last pending writer owns this dependency-map entry.
                // Colliding full keys intentionally share their native reduced slot.
                for operation in routing.operations() {
                    if let std::collections::hash_map::Entry::Occupied(mut space) =
                        pending_writers.entry(operation.ks())
                    {
                        let writers = space.get_mut();
                        if writers.get(operation.reduced_key().as_ref()) == Some(&sequence) {
                            writers.remove(operation.reduced_key().as_ref());
                        }
                        // Release the table before returning its admission
                        // credit; otherwise each keyspace retains a full window.
                        if writers.is_empty() {
                            space.remove();
                        } else if writers.capacity() > writers.len().saturating_mul(4) {
                            // A stalled writer can keep this keyspace nonempty.
                            // Geometric shrinking bounds its retained buckets
                            // by live dependencies, not historical batch sizes.
                            writers.shrink_to(writers.len());
                        }
                    }
                }
                #[cfg(test)]
                crate::failpoints::write_model::boundary("publication_before");
                routing
                    .publish_authenticated(batch, durable, index)
                    .map_err(LocalDbError::component)?;
                index.promote_committed().map_err(LocalDbError::component)?;
                #[cfg(test)]
                crate::failpoints::write_model::boundary("publication_after");
                *live_latest = Some(live_latest.map_or(entry.last_mutation, |previous| {
                    previous.max(entry.last_mutation)
                }));
                known.insert(receipt.id(), receipt);
                active.remove(&receipt.id());
                *reserved -= entry.metadata_bytes;
                if sequence != contiguous_next {
                    write_stats.out_of_prefix_published_batches += 1;
                }
                if let Some(wait_started) = waits[slot].take() {
                    write_stats.durable_receipt_publication_blocked_batches += 1;
                    write_stats.durable_receipt_publication_blocked_ns += elapsed(wait_started);
                    write_stats.durable_receipt_publication_waiting_batches -= 1;
                }
                let batch = entry
                    .batch
                    .take()
                    .ok_or_else(|| LocalDbError::new("batch disappeared during publication"))?;
                entry.predecessors = SmallVec::new();
                publication_view = entry.publication_view.take();
                cache_candidates.push((receipt.address(), batch, entry.credit));
                // The recovery frontier cannot include a still-unpublished slot.
                while let Some(entry) =
                    submissions.get((contiguous_next - submission_base) as usize)
                    && entry.batch.is_none()
                {
                    *latest = Some(entry.last_mutation);
                    contiguous_next += 1;
                }
                Ok::<_, LocalDbError>(true)
            })
            .inspect_err(|_| {
                self.concurrent_failure
                    .store(true, std::sync::atomic::Ordering::Release);
            })
            .map_err(LocalDbError::component)?;
        let published_prefix = self.publication.published_prefix().next_sequence();
        debug_assert_eq!(contiguous_next, published_prefix);
        if progress.callbacks_completed != 0 {
            let upper = self.live_latest.expect("published batch has a mutation");
            let excluded_count = self
                .publication
                .unpublished_versions()
                .take_while(|version| *version <= upper.batch)
                .count();
            let mut excluded = Vec::with_capacity(excluded_count);
            excluded.extend(
                self.publication
                    .unpublished_versions()
                    .take_while(|version| *version <= upper.batch),
            );
            let floor = self
                .latest
                .map_or(IndexReadCut::BeforeAll, IndexReadCut::At);
            index
                .install_reserved_live_view(
                    upper,
                    floor,
                    excluded,
                    publication_view.expect("published batch reserved a live view"),
                )
                .inspect_err(|_| {
                    self.concurrent_failure
                        .store(true, std::sync::atomic::Ordering::Release);
                })
                .map_err(LocalDbError::component)?;
        }
        drop(index_guard);
        if self.write_timeline.enabled() {
            for (_, batch, _) in &cache_candidates {
                self.write_timeline.batch_event(
                    batch.version(),
                    WriteTimelineStage::Published,
                    None,
                    None,
                );
            }
        }
        for _ in submission_base..published_prefix {
            self.submissions.pop_front();
            self.durable_wait_started.pop_front();
        }
        self.submission_base = published_prefix;
        self.publication
            .retire_published()
            .map_err(LocalDbError::component)?;
        for (address, batch, credit) in cache_candidates {
            let partition = self.partition_indexes[&address.partition];
            self.partition_admissions[partition] -= 1;
            self.partition_records[partition] -= batch.mutations().len();
            if self.config.read_cache_bytes != 0 {
                for operation in 0..batch.mutations().len() {
                    let reference = batch
                        .record_reference(address, operation)
                        .map_err(LocalDbError::component)?;
                    let prepared = self
                        .read_cache
                        .prepare_post_publish(reference, &batch.mutations()[operation]);
                    self.read_cache.insert_prepared(prepared);
                }
            }
            drop(batch);
            self.pending_bytes -= credit;
        }
        self.stats.publication_ns += elapsed(started);
        self.stats.published_batches += progress.callbacks_completed;
        Ok(())
    }

    pub fn write_batch(
        &mut self,
        operation: [u8; 16],
        mutations: Vec<Mutation>,
    ) -> Result<BatchVersion, LocalDbError> {
        let version = self.submit(operation, mutations)?;
        self.drain()?;
        Ok(version)
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
            let cached = self.read_cache.get(reference);
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
                self.healthy = false;
                return Err(LocalDbError::new("read result count mismatch"));
            }
            self.ensure_healthy()?;
            for ((slot, reference), value) in slots.into_iter().zip(missing).zip(values) {
                let prepared = self.read_cache.prepare_verified(reference, &value);
                self.read_cache.insert_prepared(prepared);
                output[slot] = Some(value);
            }
        } else if let Some(sample) = sample.as_mut() {
            sample.success();
        }
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
            vec![Vec::new(); self.clients.len()];
        for (i, reference) in references.iter().enumerate() {
            let clock = ResolutionSample::start(sample);
            let node = self
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
        let batch = pending_read::ReadBatch::new(&self.reads, values, groups);
        if let Some(sample) = sample.as_ref() {
            sample.stage(ResolutionStage::BatchPreparation, clock);
        }
        let mut batch = batch?;
        // These private grouped fetch callers resolve the current committed cut
        // under the sole owner. Transfer its protection through physical read
        // completion, even if the aggregate caller exits early on an error.
        self.ensure_healthy()?;
        let clock = ResolutionSample::start(sample);
        let pin = self
            .index
            .pin_cut(IndexReadCut::Latest)
            .map_err(LocalDbError::component);
        if let Some(sample) = sample.as_ref() {
            sample.stage(ResolutionStage::PinPreparation, clock);
        }
        batch.pin = Some(pin?);
        if let Some(sample) = sample.as_mut() {
            sample.success();
        }
        while !batch.complete() {
            batch.dispatch(&self.reads)?;
            // If nothing belongs to this call, only external handles hold the
            // missing capacity; leave them to their owner and report pressure.
            batch.finish_one()?;
        }
        self.stats.value_read_ns += elapsed(started);
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
        let monitor = self.reads.resolution_monitor();
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
        let cut = self.index.latest_cut();
        if let Some(sample) = &sample {
            sample.stage(ResolutionStage::CutCapture, clock);
        }
        let mut slots = Vec::new();
        let mut references = Vec::new();
        for (slot, key) in keys.iter().enumerate() {
            let clock = ResolutionSample::start(&sample);
            let reference = self
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
        self.stats.read_resolve_ns += elapsed(resolve_started);
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
            .index
            .get(ks, key, IndexReadCut::Latest)
            .map_err(LocalDbError::component)?;
        self.stats.read_resolve_ns += elapsed(started);
        let Some(reference) = reference else {
            return Ok(false);
        };
        if !self.shape.ks(ks).need_check_index_key() {
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
        Ok(LocalCheckpoint {
            database: self.config.storage.database,
            cut: self.latest,
            pin: Some(
                self.index
                    .pin_cut(
                        self.latest
                            .map_or(IndexReadCut::BeforeAll, IndexReadCut::At),
                    )
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
        if checkpoint.database != self.config.storage.database {
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

    pub fn flush_index(&mut self) -> Result<(), LocalDbError> {
        self.drain()?;
        self.commit_checkpoint()
    }
    pub fn unload_index(&mut self) -> Result<(), LocalDbError> {
        self.flush_index()
    }
    pub fn pipeline_stats(&self) -> PipelineStats {
        self.pipeline_stats
    }
    pub fn recovery_stats(&self) -> RecoveryStats {
        self.recovery_stats
    }
    pub fn authority_write_stats(&self) -> AuthorityWriteStats {
        AuthorityWriteStats {
            storage_wait_ns: self.stats.storage_wait_ns,
            publication_ns: self.stats.publication_ns,
            dispatch_ns: self.stats.dispatch_ns,
            ..self.write_stats
        }
    }
    pub fn checkpoint_diagnostic_summary(&self) -> CheckpointDiagnosticSummary {
        self.checkpoint_diagnostic_summary
    }

    fn record_checkpoint_diagnostic(
        &mut self,
        mut record: CheckpointDiagnostic,
    ) -> CheckpointDiagnostic {
        let Some(records) = self.checkpoint_diagnostics.as_mut() else {
            return record;
        };
        record.cursor = self.next_checkpoint_diagnostic;
        if records.len() == 128 {
            records.pop_front();
            self.dropped_checkpoint_diagnostics =
                self.dropped_checkpoint_diagnostics.saturating_add(1);
        }
        records.push_back(record);
        self.next_checkpoint_diagnostic = self.next_checkpoint_diagnostic.saturating_add(1);
        self.checkpoint_diagnostic_summary.record(record);
        self.checkpoint_diagnostic_summary.dropped_records = self.dropped_checkpoint_diagnostics;
        record
    }
    /// Cursor for bounded checkpoint diagnostics. A zero-profile database has
    /// no diagnostic ring and returns an empty snapshot.
    pub fn checkpoint_diagnostic_cursor(&self) -> u64 {
        self.next_checkpoint_diagnostic
    }

    pub fn checkpoint_diagnostics_since(&self, cursor: u64) -> CheckpointDiagnosticsSnapshot {
        CheckpointDiagnosticsSnapshot {
            next_cursor: self.next_checkpoint_diagnostic,
            dropped_records: self.dropped_checkpoint_diagnostics,
            records: self
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
    pub fn arm_write_profile(&mut self, limit: usize) -> Result<(), LocalDbError> {
        if limit > 65_536 {
            return Err(LocalDbError::new("write profile limit exceeds 65536"));
        }
        self.drain()?;
        self.progress_checkpoint(true)?;
        if limit == 0 {
            self.checkpoint_diagnostics = None;
        } else if let Some(records) = self.checkpoint_diagnostics.as_mut() {
            records.clear();
        } else {
            self.checkpoint_diagnostics = Some(VecDeque::with_capacity(128));
        }
        self.checkpoint_diagnostic_summary = CheckpointDiagnosticSummary {
            available: limit != 0,
            ..CheckpointDiagnosticSummary::default()
        };
        self.dropped_checkpoint_diagnostics = 0;
        self.last_checkpoint_diagnostic = None;
        self.config.write_profile_limit = limit;
        *self.write_profile.lock() = WriteProfileSnapshot {
            limit_per_connection: limit,
            expected_connections: self.clients.len(),
            ..WriteProfileSnapshot::default()
        };
        for client in &self.clients {
            client.arm_profile(limit)?;
        }
        Ok(())
    }
    /// Capture partial opt-in append profiles after all admitted writes have drained.
    /// With a zero limit, this returns without issuing any diagnostic RPC.
    pub fn finish_write_profile(&mut self) -> Result<(), LocalDbError> {
        self.drain()?;
        if self.config.write_profile_limit == 0 {
            return Ok(());
        }
        let mut first_error = None;
        for client in &self.clients {
            if let Err(error) = client.finish_profile() {
                first_error.get_or_insert(error);
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        Ok(())
    }
    pub fn shutdown_stats(&self) -> ShutdownStats {
        self.shutdown_stats
    }
    pub fn read_cache_stats(&self) -> ReadCacheStats {
        self.read_cache.stats()
    }
    /// Per-shard load of the coordinator index (lock acquisitions, contention, waits).
    pub fn index_shard_stats(&self) -> super::native_index::IndexShardStats {
        self.index.shard_stats()
    }
    /// Per-shard load of the coordinator read cache.
    pub fn read_cache_shard_stats(&self) -> ReadCacheShardStats {
        self.read_cache.shard_stats()
    }
    pub fn per_keyspace_read_cache_stats(
        &self,
    ) -> [PerKeyspaceReadCacheStats; KEYSPACE_COUNTER_COUNT] {
        self.read_cache.per_keyspace_stats()
    }
    /// Enable private Sui checkpoint cache policy using live shape names.
    pub(super) fn configure_sui_read_cache(&mut self) {
        let policy = ReadCachePolicy::for_sui_shape(&self.shape);
        self.read_cache.configure_policy(policy);
    }

    /// Discard derivative cache contents without changing index/WAL state.
    pub fn clear_read_cache(&mut self) {
        self.read_cache.clear();
    }

    /// Read-only node counters outside timed reads. Outstanding work makes a
    /// phase boundary ambiguous, so callers must drain writes and finish reads.
    /// Counter sampling does not close current or preleased writer extents.
    pub fn read_io_stats(&self) -> Result<Vec<(u32, super::tcp::ReadExecutorStats)>, LocalDbError> {
        self.ensure_healthy()?;
        self.reads.ensure_idle()?;
        if !self.submissions.is_empty() {
            return Err(LocalDbError::new(
                "read I/O statistics require drained writes",
            ));
        }
        self.clients
            .iter()
            .zip(&self.config.endpoints)
            .map(|(client, endpoint)| {
                client
                    .read_io_stats()
                    .map(|stats| (endpoint.partition, stats))
            })
            .collect()
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
        self.reads.ensure_idle()?;
        self.drain()?;
        let read_profiles = self.reads.profiles(enabled, reset)?;
        self.clients
            .iter()
            .zip(&self.config.endpoints)
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
        self.reads.ensure_idle()?;
        self.drain()?;
        if evict {
            self.clear_read_cache();
        }
        let epoch = self.epoch;
        self.clients
            .iter()
            .zip(&self.config.endpoints)
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
        self.reads.ensure_idle()?;
        self.drain()?;
        let epoch = self.epoch;
        self.clients
            .iter()
            .zip(&self.config.endpoints)
            .map(|(client, endpoint)| {
                client
                    .control(move |client| client.process_resources_for_benchmark(epoch))
                    .map(|resources| (endpoint.partition, resources))
            })
            .collect()
    }

    pub fn stats(&self) -> LocalDbStats {
        self.stats
    }
    pub fn storage_stats(&self) -> Result<PartitionStats, LocalDbError> {
        let mut total = PartitionStats::default();
        for client in &self.clients {
            let stats = receive(client.stats()?)?;
            total.appended_groups += stats.appended_groups;
            total.persistence_barriers += stats.persistence_barriers;
            total.namespace_syncs += stats.namespace_syncs;
            total.inventory_witnesses += stats.inventory_witnesses;
            total.live_frontier_file_syncs += stats.live_frontier_file_syncs;
            total.live_frontier_namespace_syncs += stats.live_frontier_namespace_syncs;
            total.registered_records += stats.registered_records;
            total.reference_metadata_bytes += stats.reference_metadata_bytes;
        }
        Ok(total)
    }
    pub fn shutdown(&mut self) -> Result<(), LocalDbError> {
        self.shutdown_with(|_| Ok(()))
    }

    /// Final owner-process teardown, distinct from an ordinary reopenable close.
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    fn shutdown_live_target_final(
        &mut self,
    ) -> Result<Vec<(u32, super::nvmeof::LiveTargetStats)>, LocalDbError> {
        if self.config.live_nvme.is_none() || self.stopped {
            return Err(LocalDbError::new(
                "final live shutdown requires an open live database",
            ));
        }
        let mut targets = Vec::with_capacity(self.clients.len());
        self.shutdown_with(|database| {
            for (endpoint, worker) in database.config.endpoints.iter().zip(&database.clients) {
                let target = worker
                    .client
                    .lock()
                    .shutdown_live_target(database.epoch)
                    .map_err(LocalDbError::component)?;
                targets.push((endpoint.partition, target));
            }
            Ok(())
        })?;
        Ok(targets)
    }

    fn shutdown_with(
        &mut self,
        after_seal: impl FnOnce(&mut Self) -> Result<(), LocalDbError>,
    ) -> Result<(), LocalDbError> {
        if self.stopped {
            return Ok(());
        }
        self.operational_sampler.stop();
        let started = Instant::now();
        let mut result = self.drain();
        self.shutdown_stats.drain_ns = elapsed(started);
        let started = Instant::now();
        if result.is_ok() {
            result = self.commit_checkpoint();
        }
        self.shutdown_stats.checkpoint_ns = elapsed(started);
        let started = Instant::now();
        let read_clients = if result.is_ok() && self.transport_pool.is_some() {
            self.reads.stop_with_clients()
        } else {
            self.reads.stop();
            Vec::new()
        };
        self.shutdown_stats.join_ns = elapsed(started);
        if self.reads.failed() && result.is_ok() {
            result = Err(LocalDbError::new(
                "asynchronous read failed before shutdown",
            ));
        }
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        if self.live_pool.is_some() {
            let mut drain_failed = false;
            for worker in &self.clients {
                // The thread observing a failed close owns its DMA retirement;
                // an idle worker may already be asleep in receiver.recv().
                let (closed, failed_writer) = {
                    let mut client = worker.client.lock();
                    // Acquire observes any earlier owner failure. Poisoned
                    // authority can retire DMA, but cannot close/adopt extents.
                    if self
                        .concurrent_failure
                        .load(std::sync::atomic::Ordering::Acquire)
                    {
                        client.record_live_write_error();
                    }
                    if !self
                        .concurrent_failure
                        .load(std::sync::atomic::Ordering::Acquire)
                    {
                        assert_eq!(
                            client.leased_flights_in_flight(),
                            0,
                            "shutdown has waiting leased replies"
                        );
                    }
                    let closed = client.close_live_writer(&mut Vec::new(), None);
                    let failed_writer = if closed.is_err() {
                        client.take_failed_live_writer()
                    } else {
                        None
                    };
                    (closed, failed_writer)
                };
                let closed = closed.map_err(LocalDbError::component);
                if closed.is_err() {
                    // Release publishes poison before the potentially slow drain.
                    self.concurrent_failure
                        .store(true, std::sync::atomic::Ordering::Release);
                }
                let closed = if let Some(mut writer) = failed_writer {
                    match writer.drain_failed() {
                        Ok(()) => closed,
                        Err(error) => Err(match closed {
                            Ok(()) => LocalDbError::component(error),
                            Err(primary) => LocalDbError::new(format!(
                                "{primary}; detached live writer drain failed: {error}",
                            )),
                        }),
                    }
                } else {
                    closed
                };
                if let Err(error) = closed {
                    drain_failed = true;
                    result = Err(match result {
                        Ok(()) => error,
                        Err(primary) => LocalDbError::new(format!(
                            "{primary}; live writer drain failed: {error}",
                        )),
                    });
                }
            }
            if drain_failed {
                // Retain poisoned clients and the pool for recovery. A failed
                // retirement remains owned by its queue's fail-stop Drop.
                self.healthy = false;
                self.concurrent_failure
                    .store(true, std::sync::atomic::Ordering::Release);
                self.reads.poison();
                return result;
            }
        }
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        if let Some(pool) = &self.live_pool
            && let Err(error) = pool.shutdown(Instant::now() + self.config.timeout)
        {
            result = Err(match result {
                Ok(()) => LocalDbError::component(error),
                Err(primary) => LocalDbError::new(format!(
                    "{primary}; live controller cleanup remains quarantined: {error}",
                )),
            });
        }
        let started = Instant::now();
        if result.is_ok() {
            // Issue all seals before waiting for any; drain every submitted reply.
            let seals: Vec<_> = self
                .clients
                .iter()
                .map(|client| client.seal(self.epoch))
                .collect();
            for seal in seals {
                let sealed = seal.and_then(receive);
                if result.is_ok() {
                    result = sealed;
                }
            }
        }
        self.shutdown_stats.seal_ns = elapsed(started);
        if result.is_ok() {
            result = after_seal(self);
        }
        self.stopped = true;
        let started = Instant::now();
        let control_clients = if result.is_ok() && self.transport_pool.is_some() {
            self.clients
                .iter()
                .map(|worker| worker.client.clone())
                .collect()
        } else {
            Vec::new()
        };
        // A failed shutdown can still have a checkpoint holding worker senders.
        // Join it while the workers can service or reject its pending control.
        self.background_checkpoint.take();
        self.clients.clear();
        self.preparation_pool.take();
        self.shutdown_stats.join_ns += elapsed(started);
        if result.is_ok()
            && let Some((pool, key)) = self.transport_pool.as_ref().zip(self.transport_key)
        {
            pool.retain(transports::IdleTransports {
                key,
                controls: control_clients,
                reads: read_clients,
            });
        }
        result
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
    position: Option<StreamPosition>,
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
    let replay = state.position.as_ref();
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    let live = client.live_nvme_configured();
    #[cfg(not(all(feature = "experimental-nvmeof", target_os = "linux")))]
    let live = false;
    let mut page = if let Some(position) = replay {
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        if live {
            client.replay_live_wal_page(
                authority_epoch,
                epoch,
                cursor as u64,
                limit,
                position.durable_end,
            )
        } else {
            client.replay_wal_page(
                authority_epoch,
                epoch,
                cursor as u64,
                limit,
                position.durable_end,
            )
        }
        #[cfg(not(all(feature = "experimental-nvmeof", target_os = "linux")))]
        client.replay_wal_page(
            authority_epoch,
            epoch,
            cursor as u64,
            limit,
            position.durable_end,
        )
    } else {
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        if live {
            client.recover_live_page(authority_epoch, epoch, cursor, limit)
        } else {
            client.recovery_page(epoch, cursor, limit)
        }
        #[cfg(not(all(feature = "experimental-nvmeof", target_os = "linux")))]
        {
            let _ = live;
            client.recovery_page(epoch, cursor, limit)
        }
    }
    .map_err(LocalDbError::component)?;
    let end = cursor
        .checked_add(page.batches.len())
        .ok_or_else(|| LocalDbError::new("recovery cursor overflow"))?;
    if page.batches.len() > limit
        || page
            .next
            .is_some_and(|next| next <= cursor || (replay.is_none() && next != end))
    {
        return Err(LocalDbError::new("invalid bounded recovery page"));
    }
    if let Some(position) = replay {
        if let Some((_, receipt)) = page.batches.last() {
            state.replay_last = Some((
                receipt.appended().address().offset,
                receipt.appended().address().len,
            ));
            state.replayed_end = state.replayed_end.max(
                receipt.appended().address().offset + u64::from(receipt.appended().address().len),
            );
        }
        page.batches.retain(|(_, receipt)| {
            position
                .last_offset
                .is_none_or(|last| receipt.appended().address().offset > last)
        });
    }
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

fn checkpoint_metadata_fits(config: &RemoteDatabaseConfig, receipts: usize) -> bool {
    // Fixed-width bincode: a receipt is 104 bytes; a stream including its
    // vector header is 84. Charge 128 for each and reserve 1 MiB for schema
    // and fixed fields (the authority's existing CRC metadata limit).
    config
        .endpoints
        .len()
        .checked_mul(config.storage.max_epochs)
        .and_then(|streams| streams.checked_add(receipts))
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
    current_epoch: u64,
    config: &RemoteDatabaseConfig,
    replay_limits: ReplayLimits,
    shape: &KeyShape,
    index: &mut NativeIndex,
    known: &mut HashMap<BatchId, AppendedBatch>,
    receipt_history: &Option<ReceiptHistory>,
    recent_ids: &HashMap<BatchId, (BatchVersion, super::types::PayloadDigest)>,
    latest: &mut Option<MutationVersion>,
    cursors: &HashMap<(usize, u64), (usize, usize)>,
    positions: &mut BTreeMap<(u32, u64), StreamPosition>,
    stats: &mut RecoveryStats,
    #[cfg(test)] interruption: Option<OpenInterruption>,
) -> Result<(), LocalDbError> {
    let mut index = index.writer();
    let replay = config.history_persistence == HistoryPersistence::WalReplay;
    let page_limit = if replay {
        config
            .recovery_page_limit()?
            .max(config.storage.group_limits.max_batches as usize)
    } else {
        config.recovery_page_limit()?
    };
    let mut previous = latest.map(|cut| cut.batch);
    for epoch in 1..current_epoch {
        let mut pages = endpoint_map(clients, |node, client| {
            let mut page = ReplayPage {
                batches: VecDeque::new(),
                next: None,
                last: None,
                position: replay.then(|| {
                    positions
                        .get(&(config.endpoints[node].partition, epoch))
                        .cloned()
                        .unwrap_or(StreamPosition {
                            partition: config.endpoints[node].partition,
                            epoch,
                            replay_from: 0,
                            last_offset: None,
                            durable_end: 0,
                            complete: false,
                        })
                }),
                replayed_end: 0,
                replay_last: None,
            };
            let (cursor, retained) = cursors
                .get(&(node, epoch))
                .copied()
                .unwrap_or((0, usize::MAX));
            if histories[node].contains(&epoch) {
                if let Some(position) = &page.position {
                    if !position.complete {
                        let mut cursor = usize::try_from(position.replay_from)
                            .map_err(LocalDbError::component)?;
                        loop {
                            fill_replay_page(
                                client,
                                &mut page,
                                epoch,
                                current_epoch,
                                cursor,
                                page_limit,
                            )?;
                            if !page.batches.is_empty() || page.next.is_none() {
                                break;
                            }
                            cursor = page.next.unwrap();
                        }
                    }
                } else if cursor != retained {
                    fill_replay_page(client, &mut page, epoch, current_epoch, cursor, page_limit)?;
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
            if previous.is_some_and(|old| old >= version)
                || known.contains_key(&batch.id())
                || recent_ids.contains_key(&batch.id())
                || receipt_history
                    .as_ref()
                    .map(|history| history.lookup(batch.id()))
                    .transpose()?
                    .flatten()
                    .is_some()
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
            if !replay
                && (stats.tail_batches > replay_limits.max_batches as u64
                    || stats.tail_bytes > replay_limits.max_encoded_bytes as u64)
            {
                return Err(LocalDbError::new(
                    "retained tail exceeds committed admission bound",
                ));
            }
            drop(batch);
            if pages[node].batches.is_empty() && pages[node].next.is_some() {
                // Refill independently depleted/half-consumed streams together.
                // Available slots, not another page allocation, bound lookahead:
                // each node retains at most page_limit complete batches in total.
                let mut streams: Vec<_> = clients.iter_mut().zip(pages.iter_mut()).collect();
                endpoint_map(&mut streams, |_, (client, page)| {
                    if (if replay {
                        page.batches.is_empty()
                    } else {
                        page.batches.len() <= page_limit / 2
                    }) && let Some(next) = page.next
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
        if replay {
            for page in pages {
                if let Some(mut position) = page.position {
                    if let Some((offset, _)) = page.replay_last {
                        position.replay_from = offset - offset % config.storage.fragment_bytes;
                        position.last_offset = Some(offset);
                    }
                    position.durable_end = position.durable_end.max(page.replayed_end);
                    position.complete = true;
                    positions.insert((position.partition, epoch), position);
                }
            }
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
    clients: &mut [Client],
    histories: &[Vec<u64>],
    authority_epoch: u64,
    index: &mut NativeIndex,
    receipt_history: &mut Option<ReceiptHistory>,
    latest: &mut Option<MutationVersion>,
    cursors: &mut HashMap<(usize, u64), (usize, usize)>,
    positions: &mut BTreeMap<(u32, u64), StreamPosition>,
    recent: &mut VecDeque<RecentReceipt>,
    recent_ids: &mut HashMap<BatchId, (BatchVersion, super::types::PayloadDigest)>,
    stats: &mut RecoveryStats,
) -> Result<(), LocalDbError> {
    let started = Instant::now();
    if commit.generation == 0
        || !manifest
            .epochs
            .iter()
            .any(|epoch| epoch.epoch == commit.epoch && epoch.active)
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
    let replay = config.history_persistence == HistoryPersistence::WalReplay;
    if format != if replay { 3 } else { 2 } {
        return Err(LocalDbError::new(
            "checkpoint image format differs from the history persistence",
        ));
    }
    let image: CheckpointImage = if replay {
        let image: CheckpointImageV3 = read_checkpoint(&metadata_path)?;
        for position in image.positions {
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
        for receipt in image.recent {
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
        CheckpointImage {
            format: 3,
            database: image.database,
            shape_yaml: image.shape_yaml,
            authority_epoch: image.authority_epoch,
            generation: image.generation,
            cut: image.cut,
            published_next: image.published_next,
            index_bytes: image.index_bytes,
            index_digest: image.index_digest,
            streams: Vec::new(),
            history_root: None,
        }
    } else {
        read_checkpoint(&metadata_path)?
    };
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
    if restored.latest_cut() != image.cut.map_or(IndexReadCut::BeforeAll, IndexReadCut::At) {
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
    let started = Instant::now();
    if replay {
        *index = restored;
        *latest = image.cut;
        stats.checkpoint_loaded = true;
        return Ok(());
    }
    let receipt_history = receipt_history
        .as_mut()
        .expect("legacy history selected at open");
    receipt_history.root = image.history_root;
    let mut streams: BTreeMap<_, _> = BTreeMap::new();
    for stream in image.streams {
        let proof = &stream.proof;
        if receipt_history
            .counts
            .insert((proof.partition, proof.epoch), proof.prefix)
            .is_some()
            || receipt_history
                .chains
                .insert((proof.partition, proof.epoch), stream.state)
                .is_some()
        {
            return Err(LocalDbError::new("duplicate checkpoint receipt stream"));
        }
        if proof.database != config.storage.database
            || proof.epoch > commit.epoch
            || proof.retained < proof.prefix
            || proof.digest != receipt_history.prefix(proof.partition, proof.epoch, proof.prefix)?
            || streams
                .insert((proof.partition, proof.epoch), stream)
                .is_some()
        {
            return Err(LocalDbError::new("invalid checkpoint receipt prefix"));
        }
    }
    let proofs = endpoint_map(clients, |node, client| {
        histories[node]
            .iter()
            .filter(|epoch| **epoch <= commit.epoch)
            .map(|&epoch| {
                let partition = config.endpoints[node].partition;
                let stream = streams
                    .get(&(partition, epoch))
                    .ok_or_else(|| LocalDbError::new("checkpoint missing retained shard prefix"))?;
                let proof = client
                    .checkpoint(authority_epoch, epoch, stream.proof.prefix)
                    .map_err(LocalDbError::component)?;
                if proof.digest != stream.proof.digest
                    || (epoch < commit.epoch && proof.retained != proof.prefix)
                {
                    return Err(LocalDbError::new("fenced shard checkpoint proof differs"));
                }
                Ok((epoch, proof))
            })
            .collect::<Result<Vec<_>, LocalDbError>>()
    })?;
    let mut last_version = None;
    for (node, proofs) in proofs.into_iter().enumerate() {
        for (epoch, proof) in proofs {
            let partition = config.endpoints[node].partition;
            let stream = streams
                .remove(&(partition, epoch))
                .ok_or_else(|| LocalDbError::new("checkpoint missing retained shard prefix"))?;
            cursors.insert((node, epoch), (proof.prefix, proof.retained));
            if stream.proof.prefix != 0 {
                let receipt = receipt_history.receipt(partition, epoch, stream.proof.prefix - 1)?;
                if receipt.address.segment_generation != epoch
                    || receipt.address.len < 8
                    || receipt
                        .address
                        .offset
                        .checked_add(u64::from(receipt.address.len))
                        .is_none()
                    || image.cut.is_none_or(|cut| receipt.version > cut.batch)
                    || config.inventory(epoch).partition_for(receipt.id).ok() != Some(partition)
                    || receipt_history.lookup(receipt.id)? != Some(receipt.authenticated())
                {
                    return Err(LocalDbError::new("invalid retained checkpoint receipt"));
                }
                last_version = Some(last_version.map_or(receipt.version, |old: BatchVersion| {
                    old.max(receipt.version)
                }));
            }
        }
    }
    if !streams.is_empty() || last_version != image.cut.map(|cut| cut.batch) {
        return Err(LocalDbError::new(
            "checkpoint prefix outside fenced inventory or cut",
        ));
    }
    *index = restored;
    *latest = image.cut;
    stats.proof_validation_ns = elapsed(started);
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
            if files.generation == 0
                || !manifest
                    .epochs
                    .iter()
                    .any(|epoch| epoch.epoch == files.epoch && epoch.active)
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

/// At most eight independent endpoint operations at once. Scoped ownership
/// joins every started operation on errors/panics before any caller lock drops.
fn endpoint_map<T: Send, R: Send>(
    items: &mut [T],
    operation: impl Fn(usize, &mut T) -> Result<R, LocalDbError> + Sync,
) -> Result<Vec<R>, LocalDbError> {
    let mut output = Vec::with_capacity(items.len());
    for (wave, chunk) in items.chunks_mut(8).enumerate() {
        let results = thread::scope(|scope| {
            let operation = &operation;
            let mut handles: [Option<thread::ScopedJoinHandle<'_, Result<R, LocalDbError>>>; 8] =
                std::array::from_fn(|_| None);
            let mut results = std::array::from_fn::<_, 8, _>(|_| None);
            for (slot, item) in chunk.iter_mut().enumerate() {
                match thread::Builder::new()
                    .spawn_scoped(scope, move || operation(wave * 8 + slot, item))
                {
                    Ok(handle) => handles[slot] = Some(handle),
                    Err(error) => {
                        results[slot] = Some(Err(LocalDbError::component(error)));
                        break;
                    }
                }
            }
            for (slot, handle) in handles.into_iter().enumerate() {
                if let Some(handle) = handle {
                    results[slot] =
                        Some(handle.join().unwrap_or_else(|_| {
                            Err(LocalDbError::new("endpoint worker panicked"))
                        }));
                }
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
// Failure-only evidence. No caller data is included, and formatting is capped
// while writing rather than allocating an unbounded intermediate error string.
fn record_first_failure(
    slot: &std::sync::OnceLock<String>,
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
        let _ = slot.set(message.0);
    }
}

fn failure_context(slot: &std::sync::OnceLock<String>, message: &str) -> String {
    match slot.get() {
        Some(cause) => format!("{message}; first causal failure: {cause}"),
        None => message.to_owned(),
    }
}

type Pending<T> = mpsc::Receiver<Result<T, LocalDbError>>;
enum Command {
    Append(u64, Vec<CompleteBatch>, Reply<Vec<DurableBatch>>),
    Checkpoint(
        u64,
        u64,
        usize,
        Reply<super::tcp::checkpoint::CheckpointProof>,
    ),
    CheckpointPosition(u64, u64, u64, Reply<CheckpointPositionProof>),
    Stats(Reply<PartitionStats>),
    // Counter observation shares the control slot but must not close a lease.
    ReadIoStats(Reply<super::tcp::ReadExecutorStats>),
    Seal(u64, Reply<()>),
    ArmProfile(usize, Reply<()>),
    OwnerTimelineArm(usize, Reply<()>),
    FinishProfile(Reply<()>),
    Control(Box<dyn FnOnce(&mut Client) + Send>),
}
fn trace_worker_dequeue(timeline: &WriteTimeline, partition: u32, command: &Command) {
    if timeline.enabled()
        && let Command::Append(_, batches, _) = command
    {
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
    sender: std::sync::Weak<mpsc::SyncSender<Command>>,
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    worker_wake: std::sync::Weak<std::os::fd::OwnedFd>,
}
impl ControlHandle {
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    fn wake_worker(&self) -> Result<(), LocalDbError> {
        let fd = self
            .worker_wake
            .upgrade()
            .ok_or_else(|| LocalDbError::new("TCP worker stopped"))?;
        signal_worker_wake(&fd)
    }

    fn read_io_stats(&self) -> Result<super::tcp::ReadExecutorStats, LocalDbError> {
        let (reply, pending) = mpsc::sync_channel(1);
        self.sender
            .upgrade()
            .ok_or_else(|| LocalDbError::new("TCP worker stopped"))?
            .send(Command::ReadIoStats(reply))
            .map_err(|_| LocalDbError::new("TCP worker lost request outcome"))?;
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        self.wake_worker()?;
        receive(pending)
    }
    fn owner_timeline_arm(&self, limit: usize) -> Result<(), LocalDbError> {
        let (reply, pending) = mpsc::sync_channel(1);
        self.sender
            .upgrade()
            .ok_or_else(|| LocalDbError::new("TCP worker stopped"))?
            .send(Command::OwnerTimelineArm(limit, reply))
            .map_err(|_| LocalDbError::new("TCP worker lost request outcome"))?;
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
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
            .send(command)
            .map_err(|_| LocalDbError::new("TCP worker lost request outcome"))?;
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        self.wake_worker()?;
        receive(pending)
    }
    fn control_until<T: Send + 'static>(
        &self,
        deadline: Instant,
        operation: impl FnOnce(&mut Client) -> Result<T, super::tcp::TcpError> + Send + 'static,
    ) -> Result<T, LocalDbError> {
        let (reply, pending) = mpsc::sync_channel(1);
        let mut command = Command::Control(Box::new(move |client| {
            if Instant::now() < deadline {
                let _ = reply.send(operation(client).map_err(LocalDbError::component));
            }
        }));
        let sender = self
            .sender
            .upgrade()
            .ok_or_else(|| LocalDbError::new("TCP worker stopped"))?;
        loop {
            if Instant::now() >= deadline {
                return Err(LocalDbError::new("owner timeline control deadline expired"));
            }
            match sender.try_send(command) {
                Ok(()) => {
                    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
                    self.wake_worker()?;
                    break;
                }
                Err(mpsc::TrySendError::Full(returned)) => {
                    command = returned;
                    thread::sleep(
                        Duration::from_millis(1)
                            .min(deadline.saturating_duration_since(Instant::now())),
                    );
                }
                Err(mpsc::TrySendError::Disconnected(_)) => {
                    return Err(LocalDbError::new("TCP worker stopped"));
                }
            }
        }
        pending
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            .map_err(|_| LocalDbError::new("owner timeline control deadline expired"))?
    }
}
#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
type LeasedMembers = Vec<(BatchVersion, usize, Reply<Vec<DurableBatch>>)>;

#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
fn release_leased_replies(
    released: &mut Vec<(u64, Vec<DurableBatch>)>,
    replies: &mut VecDeque<(u64, LeasedMembers)>,
) -> Result<(), super::tcp::TcpError> {
    for (id, receipts) in released.drain(..) {
        if replies.front().is_none_or(|entry| entry.0 != id) {
            return Err(std::io::Error::other("leased receipt release order differs").into());
        }
        let Some((_, mut members)) = replies.pop_front() else {
            unreachable!();
        };
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

#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
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
    sender: Option<Arc<mpsc::SyncSender<Command>>>,
    /// The worker owns this fd. Command senders and CQEs add to its counter.
    /// The worker reads it after poll; drop closes it after the queue stops.
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    worker_wake: Arc<std::os::fd::OwnedFd>,
    thread: Option<JoinHandle<()>>,
}
struct WorkerDiagnostics {
    write_profile_limit: usize,
    write_profile: Arc<Mutex<WriteProfileSnapshot>>,
    write_timeline: Arc<WriteTimeline>,
    first_failure: Arc<std::sync::OnceLock<String>>,
    partition: u32,
}
impl ClientWorker {
    fn start(
        mut client: Client,
        queue_capacity: usize,
        wake: mpsc::SyncSender<()>,
        failure: Arc<std::sync::atomic::AtomicBool>,
        diagnostics: WorkerDiagnostics,
    ) -> std::io::Result<Self> {
        let WorkerDiagnostics {
            write_profile_limit,
            write_profile,
            write_timeline,
            first_failure,
            partition,
        } = diagnostics;
        client.trace_controls(Arc::clone(&write_timeline), partition);
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        let worker_wake = {
            use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
            // SAFETY: eventfd has plain value arguments. Check its result before taking ownership.
            let fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
            if fd < 0 {
                return Err(std::io::Error::last_os_error());
            }
            // SAFETY: fd is a new, valid descriptor and has no other owner.
            let fd = Arc::new(unsafe { OwnedFd::from_raw_fd(fd) });
            client.set_worker_wake(fd.as_raw_fd());
            fd
        };
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        let thread_wake = Arc::clone(&worker_wake);
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        let leased_write_window = client.leased_write_window();
        let client = Arc::new(Mutex::new(client));
        let worker_client = Arc::clone(&client);
        let (sender, receiver) = mpsc::sync_channel(queue_capacity);
        let thread = thread::Builder::new()
            .name("value-wal-tcp".into())
            .spawn(move || {
                #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
                let _thread_wake = thread_wake;
                let mut write_profile_limit = write_profile_limit;
                let mut profile_attempted = false;
                let mut profile_enabled = false;
                let mut profile_requests = 0usize;
                let mut checkpoint_samples = 0usize;
                let mut checkpoint_stalls = 0u64;
                let mut checkpoint_started: Option<Instant> = None;
                let mut checkpoint_admission_deadline: Option<Instant> = None;
                let mut checkpoint_append_completions = 0u64;
                let mut queued = VecDeque::new();
                // One fixed-cut proof may wait outside the append FIFO. Later
                // non-append commands never pass it. Earlier covered appends
                // have already run; passing appends have later WAL positions.
                let mut parked_checkpoint: Option<Command> = None;
                let mut appends: HashMap<u64, (BatchVersion, Reply<Vec<DurableBatch>>)> =
                    HashMap::new();
                let mut checkpoint: Option<(
                    u64,
                    usize,
                    Reply<super::tcp::checkpoint::CheckpointProof>,
                )> = None;
                let mut position: Option<(u64, u64, Reply<CheckpointPositionProof>)> = None;
                let mut disconnected = false;
                #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
                let mut leased_replies: VecDeque<(u64, LeasedMembers)> =
                    VecDeque::with_capacity(leased_write_window);
                #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
                let mut deferred: Option<(u64, Vec<CompleteBatch>, LeasedMembers)> = None;
                #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
                let mut released = Vec::with_capacity(leased_write_window);
                #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
                let prepare_leased_command =
                    |client: &mut Client,
                     command: &Command,
                     released: &mut Vec<(u64, Vec<DurableBatch>)>,
                     replies: &mut VecDeque<(u64, LeasedMembers)>| {
                        if !client.live_leased_mode() {
                            return true;
                        }
                        // Acquire observes a fatal checkpoint or another owner's
                        // failure immediately before dispatching another command.
                        if failure.load(std::sync::atomic::Ordering::Acquire) {
                            client.record_live_write_error();
                            return false;
                        }
                        let result = match command {
                            Command::Checkpoint(..) => {
                                client.settle_leased_extent(released, Some(&write_timeline))
                            }
                            Command::CheckpointPosition(..) => Ok(()),
                            Command::ReadIoStats(_) => {
                                client.wait_leased_control(released, Some(&write_timeline))
                            }
                            Command::Seal(..) | Command::Control(_) => {
                                client.close_leased_extent(released, Some(&write_timeline))
                            }
                            Command::Stats(_)
                            | Command::ArmProfile(..)
                            | Command::OwnerTimelineArm(..)
                            | Command::FinishProfile(_) => {
                                client.wait_leased_control(released, Some(&write_timeline))
                            }
                            Command::Append(..) => Ok(()),
                        };
                        let result =
                            result.and_then(|()| release_leased_replies(released, replies));
                        if let Err(error) = result {
                            client.record_live_write_error();
                            record_first_failure(
                                &first_failure,
                                Some(partition),
                                None,
                                "leased_command",
                                &error,
                            );
                            // Release publishes the failure before any queued command runs.
                            failure.store(true, std::sync::atomic::Ordering::Release);
                            let _ = wake.try_send(());
                            return false;
                        }
                        true
                    };
                loop {
                    let mut client = worker_client.lock();
                    while !disconnected && queued.len() < queue_capacity {
                        match receiver.try_recv() {
                            Ok(command) => {
                                trace_worker_dequeue(&write_timeline, partition, &command);
                                queued.push_back(command);
                            }
                            Err(mpsc::TryRecvError::Empty) => break,
                            Err(mpsc::TryRecvError::Disconnected) => disconnected = true,
                        }
                    }
                    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
                    if client.live_leased_mode() {
                        // Acquire observes authority failure before another control
                        // exchange or write can be admitted by this worker.
                        if failure.load(std::sync::atomic::Ordering::Acquire) {
                            client.record_live_write_error();
                            break;
                        }
                        let mut leased_failure_stage = "live_group";
                        let result = (|| {
                            client.progress_leased_flights(&mut released, Some(&write_timeline))?;
                            let has_released = !released.is_empty();
                            if let Err(error) =
                                release_leased_replies(&mut released, &mut leased_replies)
                            {
                                record_first_failure(
                                    &first_failure,
                                    Some(partition),
                                    None,
                                    "leased_release_order",
                                    &error,
                                );
                                return Err(error);
                            }
                            if has_released {
                                let _ = wake.try_send(());
                            }
                            leased_failure_stage = "leased_adoption";
                            if client.leased_adoption_pending() {
                                client.progress_appends()?;
                                client.harvest_leased_adoption()?;
                            }
                            if parked_checkpoint.is_none()
                                && checkpoint.is_none()
                                && position.is_none()
                                && matches!(
                                    queued.front(),
                                    Some(Command::CheckpointPosition(..) | Command::Checkpoint(..))
                                )
                            {
                                parked_checkpoint = queued.pop_front();
                            }
                            if let Some(command) = parked_checkpoint.as_ref() {
                                let ready = if matches!(command, Command::CheckpointPosition(..)) {
                                    client.prepare_leased_position()?;
                                    true
                                } else {
                                    !client.leased_adoption_pending()
                                };
                                if ready {
                                    // Fixed through/prefix proofs tolerate later appends:
                                    // position persistence validates its captured offset;
                                    // on-disk receipt history retains the exact prefix.
                                    // Checkpoint still settles its extent before submission.
                                    queued.push_front(parked_checkpoint.take().unwrap());
                                }
                            }
                            if appends.is_empty()
                                && checkpoint.is_none()
                                && parked_checkpoint.is_none()
                                && matches!(queued.front(), Some(Command::Append(..)) | None)
                            {
                                client.submit_leased_adoption()?;
                            }
                            Ok::<(), super::tcp::TcpError>(())
                        })();
                        if let Err(error) = result {
                            client.record_live_write_error();
                            record_first_failure(
                                &first_failure,
                                Some(partition),
                                None,
                                leased_failure_stage,
                                &error,
                            );
                            // Release publishes the fatal outcome to the authority.
                            failure.store(true, std::sync::atomic::Ordering::Release);
                            let _ = wake.try_send(());
                            break;
                        }
                        if checkpoint.is_none()
                            && position.is_none()
                            && client.leased_adoption_pending()
                            && client.leased_flights_in_flight() == 0
                            && !matches!(
                                queued.front(),
                                Some(Command::Append(..) | Command::CheckpointPosition(..))
                            )
                        {
                            // The control slot progresses only on this thread. Do
                            // not park forever on the command channel or spin on
                            // an adoption: wake within 1 ms to drive its deadline.
                            #[cfg(test)]
                            if !queued.is_empty() {
                                client.leased_test_note_command_adoption_wait();
                            }
                            // A guard regression would wait here with a frame in flight.
                            #[cfg(test)]
                            if client.leased_flights_in_flight() != 0 {
                                client.leased_test_note_flight_adoption_wait();
                            }
                            drop(client);
                            if disconnected || queued.len() == queue_capacity {
                                // Keep the bounded FIFO intact while a command
                                // waits for the adoption slot, without a CQ spin.
                                thread::park_timeout(Duration::from_millis(1));
                                continue;
                            }
                            match receiver.recv_timeout(Duration::from_millis(1)) {
                                Ok(command) => {
                                    trace_worker_dequeue(&write_timeline, partition, &command);
                                    queued.push_back(command);
                                }
                                Err(mpsc::RecvTimeoutError::Timeout) => {}
                                Err(mpsc::RecvTimeoutError::Disconnected) => disconnected = true,
                            }
                            continue;
                        }
                    }

                    if !disconnected
                        && queued.is_empty()
                        && parked_checkpoint.is_none()
                        && appends.is_empty()
                        && !client.has_pending_io()
                        && {
                            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
                            {
                                client.leased_flights_in_flight() == 0 && deferred.is_none()
                            }
                            #[cfg(not(all(feature = "experimental-nvmeof", target_os = "linux")))]
                            {
                                true
                            }
                        }
                    {
                        drop(client);
                        match receiver.recv() {
                            Ok(command) => {
                                trace_worker_dequeue(&write_timeline, partition, &command);
                                queued.push_back(command);
                            }
                            Err(_) => disconnected = true,
                        }
                        continue;
                    }
                    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
                    if let Some((epoch, batches, members)) = deferred.take() {
                        match client.submit_leased_group(
                            epoch,
                            batches,
                            &write_timeline,
                            partition,
                            queued.is_empty(),
                        ) {
                            Ok(super::tcp::LeasedSubmit::Submitted(id)) => {
                                leased_replies.push_back((id, members));
                                if position.is_none() {
                                    continue;
                                }
                            }
                            Ok(super::tcp::LeasedSubmit::Deferred(batches)) => {
                                deferred = Some((epoch, batches, members));
                            }
                            Err(error) => {
                                deferred = Some((epoch, Vec::new(), members));
                                client.record_live_write_error();
                                record_first_failure(
                                    &first_failure,
                                    Some(partition),
                                    None,
                                    "live_group",
                                    &error,
                                );
                                failure.store(true, std::sync::atomic::Ordering::Release);
                                let _ = wake.try_send(());
                                break;
                            }
                        }
                    }
                    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
                    if client.leased_flights_in_flight() != 0
                        && (deferred.is_some()
                            || !matches!(queued.front(), Some(Command::Append(..)))
                            || client.leased_flights_in_flight() >= client.leased_write_window())
                        && !matches!(queued.front(), Some(Command::CheckpointPosition(..)))
                    {
                        if let Err(error) = client.wait_leased_flights(true) {
                            client.record_live_write_error();
                            record_first_failure(
                                &first_failure,
                                Some(partition),
                                None,
                                "live_group",
                                &error,
                            );
                            failure.store(true, std::sync::atomic::Ordering::Release);
                            let _ = wake.try_send(());
                            break;
                        }
                        if position.is_none() {
                            continue;
                        }
                    }
                    // Merge ready same-epoch appends without crossing a control
                    // barrier. Reserved writes keep one reservation in flight.
                    // Leased frames use the configured completion window.
                    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
                    if client.live_nvme_configured()
                        && appends.is_empty()
                        && checkpoint.is_none()
                        && (position.is_none() || client.live_leased_mode())
                        && (!client.live_leased_mode()
                            || (deferred.is_none()
                                && client.leased_flights_in_flight()
                                    < client.leased_write_window()))
                        && let Some(Command::Append(epoch, _, _)) = queued.front()
                    {
                        let epoch = *epoch;
                        let budget = client.live_group_budget();
                        let mut take = 0usize;
                        let mut count = 0usize;
                        let mut payload = super::tcp::LiveGroupBudget::EMPTY_PAYLOAD;
                        for command in &queued {
                            let Command::Append(command_epoch, batches, _) = command else {
                                break;
                            };
                            if *command_epoch != epoch {
                                break;
                            }
                            match budget.extend(count, payload, batches) {
                                Some((next_count, next_payload)) => {
                                    let frame = (next_payload + 8 + 4095) & !4095;
                                    // Harvest a pending prelease in the worker before
                                    // a boundary write consumes it. This gives the
                                    // parked proof its first free control turn.
                                    if (position.is_some() || parked_checkpoint.is_some())
                                        && client.leased_append_needs_control(Some(frame))
                                        && client.leased_adoption_pending()
                                    {
                                        break;
                                    }
                                    count = next_count;
                                    payload = next_payload;
                                    take += 1;
                                }
                                // The first group is sent alone even if it exceeds the
                                // budget, so the owner reports the real limit error.
                                None if take == 0 => {
                                    if (position.is_none() && parked_checkpoint.is_none())
                                        || !client.leased_append_needs_control(None)
                                        || !client.leased_adoption_pending()
                                    {
                                        take = 1;
                                    }
                                    break;
                                }
                                None => break,
                            }
                        }
                        if take != 0 {
                            let leased = client.live_leased_mode();
                            let mut merged = if leased {
                                Vec::new()
                            } else {
                                Vec::with_capacity(count.max(1))
                            };
                            let mut members: LeasedMembers = Vec::with_capacity(take);
                            for _ in 0..take {
                                let Some(Command::Append(_, batches, reply)) = queued.pop_front()
                                else {
                                    unreachable!("append queue front changed");
                                };
                                members.push((batches[0].version(), batches.len(), reply));
                                if leased && merged.is_empty() {
                                    // Transfer the first command's allocation with its batches.
                                    merged = batches;
                                } else {
                                    merged.extend(batches);
                                }
                            }
                            let first = members[0].0;
                            if leased {
                                match client.submit_leased_group(
                                    epoch,
                                    merged,
                                    &write_timeline,
                                    partition,
                                    queued.is_empty(),
                                ) {
                                    Ok(super::tcp::LeasedSubmit::Submitted(id)) => {
                                        leased_replies.push_back((id, members));
                                    }
                                    Ok(super::tcp::LeasedSubmit::Deferred(batches)) => {
                                        deferred = Some((epoch, batches, members));
                                    }
                                    Err(error) => {
                                        deferred = Some((epoch, Vec::new(), members));
                                        client.record_live_write_error();
                                        record_first_failure(
                                            &first_failure,
                                            Some(partition),
                                            Some(first),
                                            "live_group",
                                            &error,
                                        );
                                        failure.store(true, std::sync::atomic::Ordering::Release);
                                        let _ = wake.try_send(());
                                        break;
                                    }
                                }
                                if position.is_none() {
                                    continue;
                                }
                            } else {
                                match client.write_live_group(
                                    epoch,
                                    &merged,
                                    &write_timeline,
                                    partition,
                                ) {
                                    Ok(receipts) => {
                                        let mut receipts = receipts.into_iter();
                                        for (_, len, reply) in members.drain(..) {
                                            let _ = reply
                                                .send(Ok(receipts.by_ref().take(len).collect()));
                                        }
                                    }
                                    Err(error) => {
                                        client.record_live_write_error();
                                        record_first_failure(
                                            &first_failure,
                                            Some(partition),
                                            Some(first),
                                            "live_group",
                                            &error,
                                        );
                                        failure.store(true, std::sync::atomic::Ordering::Release);
                                        for (version, _, reply) in members.drain(..) {
                                            let _ = reply
                                                .send(Err(LocalDbError::unknown(version, &error)));
                                        }
                                        let _ = wake.try_send(());
                                        break;
                                    }
                                }
                                let _ = wake.try_send(());
                            }
                            // While a proof is pending, also drive its reply/deadline
                            // after each direct frame. A full append FIFO cannot starve it.
                            if position.is_none() {
                                continue;
                            }
                        }
                    }

                    let mut progressed = false;
                    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
                    let live_append = client.live_nvme_configured();
                    #[cfg(not(all(feature = "experimental-nvmeof", target_os = "linux")))]
                    let live_append = false;
                    if let Some(Command::Append(epoch, batches, _)) = queued.front()
                        && !live_append
                        && (!profile_enabled || profile_requests < write_profile_limit)
                    {
                        if write_profile_limit > 0
                            && !profile_attempted
                            && appends.is_empty()
                            && checkpoint.is_none()
                            && position.is_none()
                        {
                            profile_attempted = true;
                            match client.connection_profile(true, false) {
                                Ok(_) => profile_enabled = true,
                                Err(_) => record_write_profile_error(&write_profile, true),
                            }
                        }
                        let first = batches[0].version();
                        match client.submit_append_many(*epoch, batches) {
                            Ok(Some(id)) => {
                                if write_timeline.enabled() {
                                    for batch in batches {
                                        write_timeline.batch_event(
                                            batch.version(),
                                            WriteTimelineStage::TxQueued,
                                            Some(partition),
                                            Some(id),
                                        );
                                    }
                                }
                                let Command::Append(_, _, reply) =
                                    queued.pop_front().expect("front append exists")
                                else {
                                    unreachable!("append queue front changed");
                                };
                                appends.insert(id, (first, reply));
                                if profile_enabled {
                                    profile_requests += 1;
                                    write_profile.lock().sampled_requests += 1;
                                }
                                progressed = true;
                            }
                            Ok(None) => {}
                            Err(error) => {
                                let Command::Append(_, _, reply) =
                                    queued.pop_front().expect("front append exists")
                                else {
                                    unreachable!("append queue front changed");
                                };
                                record_first_failure(
                                    &first_failure,
                                    Some(partition),
                                    Some(first),
                                    "append_submit",
                                    &error,
                                );
                                failure.store(true, std::sync::atomic::Ordering::Release);
                                let _ = reply.send(Err(LocalDbError::unknown(first, error)));
                                let _ = wake.try_send(());
                                progressed = true;
                            }
                        }
                    }
                    if checkpoint.is_none()
                        && parked_checkpoint.is_none()
                        && position.is_none()
                        && let Some(Command::Checkpoint(authority, epoch, prefix, _)) =
                            queued.front()
                    {
                        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
                        if let Some(command) = queued.front()
                            && !prepare_leased_command(
                                &mut client,
                                command,
                                &mut released,
                                &mut leased_replies,
                            )
                        {
                            break;
                        }
                        let deadline = *checkpoint_admission_deadline
                            .get_or_insert_with(|| client.checkpoint_deadline());
                        match client.submit_checkpoint(*authority, *epoch, *prefix, deadline) {
                            Ok(true) => {
                                let Command::Checkpoint(_, epoch, prefix, reply) =
                                    queued.pop_front().expect("front checkpoint exists")
                                else {
                                    unreachable!("checkpoint queue front changed");
                                };
                                write_timeline.checkpoint(
                                    "control_submit",
                                    Some(partition),
                                    epoch,
                                    Some(prefix),
                                );
                                checkpoint_admission_deadline = None;
                                if checkpoint_samples < write_profile_limit {
                                    checkpoint_samples += 1;
                                    checkpoint_started = Some(Instant::now());
                                    let mut profile = write_profile.lock();
                                    profile.checkpoint_requests += 1;
                                    profile.checkpoint_admission_stalls = profile
                                        .checkpoint_admission_stalls
                                        .saturating_add(checkpoint_stalls);
                                }
                                checkpoint_stalls = 0;
                                checkpoint = Some((epoch, prefix, reply));
                                progressed = true;
                            }
                            Ok(false) => {
                                if checkpoint_samples < write_profile_limit {
                                    checkpoint_stalls = checkpoint_stalls.saturating_add(1);
                                }
                            }
                            Err(error) => {
                                let Command::Checkpoint(_, _, _, reply) =
                                    queued.pop_front().expect("front checkpoint exists")
                                else {
                                    unreachable!("checkpoint queue front changed");
                                };
                                checkpoint_admission_deadline = None;
                                checkpoint_stalls = 0;
                                record_first_failure(
                                    &first_failure,
                                    Some(partition),
                                    None,
                                    "checkpoint_submit",
                                    &error,
                                );
                                failure.store(true, std::sync::atomic::Ordering::Release);
                                let _ = reply.send(Err(LocalDbError::component(error)));
                                let _ = wake.try_send(());
                                progressed = true;
                            }
                        }
                    }

                    if checkpoint.is_none()
                        && parked_checkpoint.is_none()
                        && position.is_none()
                        && let Some(Command::CheckpointPosition(authority, epoch, through, _)) =
                            queued.front()
                    {
                        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
                        if let Some(command) = queued.front()
                            && !prepare_leased_command(
                                &mut client,
                                command,
                                &mut released,
                                &mut leased_replies,
                            )
                        {
                            break;
                        }
                        let deadline = *checkpoint_admission_deadline
                            .get_or_insert_with(|| client.position_deadline());
                        match client
                            .submit_checkpoint_position(*authority, *epoch, *through, deadline)
                        {
                            Ok(true) => {
                                let Some(Command::CheckpointPosition(_, epoch, through, reply)) =
                                    queued.pop_front()
                                else {
                                    unreachable!()
                                };
                                write_timeline.checkpoint(
                                    "control_submit",
                                    Some(partition),
                                    epoch,
                                    None,
                                );
                                checkpoint_admission_deadline = None;
                                if checkpoint_samples < write_profile_limit {
                                    checkpoint_samples += 1;
                                    checkpoint_started = Some(Instant::now());
                                    let mut profile = write_profile.lock();
                                    profile.checkpoint_requests += 1;
                                    profile.checkpoint_admission_stalls = profile
                                        .checkpoint_admission_stalls
                                        .saturating_add(checkpoint_stalls);
                                }
                                checkpoint_stalls = 0;
                                position = Some((epoch, through, reply));
                                progressed = true;
                            }
                            Ok(false) => {
                                if checkpoint_samples < write_profile_limit {
                                    checkpoint_stalls = checkpoint_stalls.saturating_add(1);
                                }
                            }
                            Err(error) => {
                                let Some(Command::CheckpointPosition(_, _, _, reply)) =
                                    queued.pop_front()
                                else {
                                    unreachable!()
                                };
                                checkpoint_admission_deadline = None;
                                record_first_failure(
                                    &first_failure,
                                    Some(partition),
                                    None,
                                    "checkpoint_position_submit",
                                    &error,
                                );
                                failure.store(true, std::sync::atomic::Ordering::Release);
                                let _ = reply.send(Err(LocalDbError::component(error)));
                                let _ = wake.try_send(());
                                progressed = true;
                            }
                        }
                    }
                    if !appends.is_empty() || client.has_pending_io() {
                        match client.progress_appends() {
                            Ok(completions) => {
                                if !completions.is_empty() {
                                    progressed = true;
                                }
                                for completion in completions {
                                    if checkpoint_started.is_some()
                                        && appends.contains_key(&completion.id)
                                    {
                                        checkpoint_append_completions =
                                            checkpoint_append_completions.saturating_add(1);
                                    }
                                    write_timeline.response(partition, completion.id);
                                    if let Some((first, reply)) = appends.remove(&completion.id) {
                                        let result = completion
                                            .result
                                            .map_err(|error| LocalDbError::unknown(first, error));
                                        if let Ok(receipts) = &result {
                                            write_timeline.wait_enter(
                                                WriteWait::HarvestToDurable,
                                                receipts.len(),
                                            );
                                        }
                                        if let Err(error) = &result {
                                            record_first_failure(
                                                &first_failure,
                                                Some(partition),
                                                Some(first),
                                                "append_completion",
                                                error,
                                            );
                                            failure
                                                .store(true, std::sync::atomic::Ordering::Release);
                                        }
                                        let wake_count = write_timeline.completion_wake_started();
                                        let _ = reply.send(result);
                                        let wake_result = wake.try_send(());
                                        if let Some(wake_count) = wake_count {
                                            wake_count.complete(matches!(
                                                wake_result,
                                                Err(mpsc::TrySendError::Full(()))
                                            ));
                                        }
                                    }
                                }
                                if let Some((epoch, through, _)) = position.as_ref()
                                    && let Some(result) =
                                        client.take_checkpoint_position(*epoch, *through)
                                {
                                    write_timeline.checkpoint(
                                        "control_retire",
                                        Some(partition),
                                        *epoch,
                                        None,
                                    );
                                    if let Some(started) = checkpoint_started.take() {
                                        let mut profile = write_profile.lock();
                                        profile.checkpoint_completed += 1;
                                        profile.checkpoint_wait_ns = profile
                                            .checkpoint_wait_ns
                                            .saturating_add(elapsed(started));
                                        profile.checkpoint_append_completions = profile
                                            .checkpoint_append_completions
                                            .saturating_add(checkpoint_append_completions);
                                        checkpoint_append_completions = 0;
                                    }
                                    let (_, _, reply) = position.take().expect("position exists");
                                    let result = result.map_err(LocalDbError::component);
                                    if let Err(error) = &result {
                                        record_first_failure(
                                            &first_failure,
                                            Some(partition),
                                            None,
                                            "checkpoint_position_completion",
                                            error,
                                        );
                                        failure.store(true, std::sync::atomic::Ordering::Release);
                                    }
                                    let _ = reply.send(result);
                                    let _ = wake.try_send(());
                                    progressed = true;
                                }
                                if let Some((epoch, prefix, _)) = checkpoint.as_ref()
                                    && let Some(result) = client.take_checkpoint(*epoch, *prefix)
                                {
                                    write_timeline.checkpoint(
                                        "control_retire",
                                        Some(partition),
                                        *epoch,
                                        Some(*prefix),
                                    );
                                    if let Some(started) = checkpoint_started.take() {
                                        let mut profile = write_profile.lock();
                                        profile.checkpoint_completed += 1;
                                        profile.checkpoint_wait_ns = profile
                                            .checkpoint_wait_ns
                                            .saturating_add(elapsed(started));
                                        profile.checkpoint_append_completions = profile
                                            .checkpoint_append_completions
                                            .saturating_add(checkpoint_append_completions);
                                        checkpoint_append_completions = 0;
                                    }
                                    let (_, _, reply) =
                                        checkpoint.take().expect("checkpoint exists");
                                    let result = result.map_err(LocalDbError::component);
                                    if let Err(error) = &result {
                                        record_first_failure(
                                            &first_failure,
                                            Some(partition),
                                            None,
                                            "checkpoint_completion",
                                            error,
                                        );
                                        failure.store(true, std::sync::atomic::Ordering::Release);
                                    }
                                    let _ = reply.send(result);
                                    let _ = wake.try_send(());
                                    progressed = true;
                                }
                            }
                            Err(error) => {
                                let first = appends.values().next().map(|(first, _)| *first);
                                record_first_failure(
                                    &first_failure,
                                    Some(partition),
                                    first,
                                    "transport_progress",
                                    &error,
                                );
                                failure.store(true, std::sync::atomic::Ordering::Release);
                                for (_, (first, reply)) in appends.drain() {
                                    let _ = reply.send(Err(LocalDbError::unknown(first, &error)));
                                    let _ = wake.try_send(());
                                }
                                if let Some(started) = checkpoint_started.take() {
                                    let mut profile = write_profile.lock();
                                    profile.checkpoint_completed += 1;
                                    profile.checkpoint_wait_ns =
                                        profile.checkpoint_wait_ns.saturating_add(elapsed(started));
                                    profile.checkpoint_append_completions = profile
                                        .checkpoint_append_completions
                                        .saturating_add(checkpoint_append_completions);
                                }
                                if let Some((_, _, reply)) = checkpoint.take() {
                                    let _ = reply.send(Err(LocalDbError::new(error.to_string())));
                                    let _ = wake.try_send(());
                                }
                                if let Some((_, _, reply)) = position.take() {
                                    let _ = reply.send(Err(LocalDbError::new(error.to_string())));
                                    let _ = wake.try_send(());
                                }
                                let message = error.to_string();
                                for command in queued.drain(..) {
                                    match command {
                                        Command::Append(_, batches, reply) => {
                                            let first = batches[0].version();
                                            let _ = reply
                                                .send(Err(LocalDbError::unknown(first, &message)));
                                            let _ = wake.try_send(());
                                        }
                                        Command::Stats(reply) => {
                                            let _ = reply.send(Err(LocalDbError::new(&message)));
                                        }
                                        Command::ReadIoStats(reply) => {
                                            let _ = reply.send(Err(LocalDbError::new(&message)));
                                        }
                                        Command::CheckpointPosition(_, _, _, reply) => {
                                            let _ = reply.send(Err(LocalDbError::new(&message)));
                                        }
                                        Command::Checkpoint(_, _, _, reply) => {
                                            let _ = reply.send(Err(LocalDbError::new(&message)));
                                        }
                                        Command::ArmProfile(_, reply)
                                        | Command::OwnerTimelineArm(_, reply)
                                        | Command::Seal(_, reply)
                                        | Command::FinishProfile(reply) => {
                                            let _ = reply.send(Err(LocalDbError::new(&message)));
                                        }
                                        Command::Control(_) => {}
                                    }
                                }
                                break;
                            }
                        }
                    }
                    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
                    // Acquire observes checkpoint submission/completion failure in
                    // this pass before profile control, queued commands or close.
                    if client.live_leased_mode()
                        && failure.load(std::sync::atomic::Ordering::Acquire)
                    {
                        client.record_live_write_error();
                        break;
                    }

                    if profile_enabled
                        && profile_requests >= write_profile_limit
                        && appends.is_empty()
                        && checkpoint.is_none()
                        && position.is_none()
                    {
                        match client.connection_profile(false, false) {
                            Ok(profile) => capture_write_profile(&write_profile, profile),
                            Err(_) => record_write_profile_error(&write_profile, false),
                        }
                        profile_enabled = false;
                        progressed = true;
                    }

                    if appends.is_empty()
                        && checkpoint.is_none()
                        && position.is_none()
                        && parked_checkpoint.is_none()
                    {
                        match queued.front() {
                            Some(Command::Stats(_))
                            | Some(Command::ReadIoStats(_))
                            | Some(Command::OwnerTimelineArm(_, _))
                            | Some(Command::Seal(_, _))
                            | Some(Command::ArmProfile(_, _))
                            | Some(Command::FinishProfile(_))
                            | Some(Command::Control(_)) => {
                                #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
                                if let Some(command) = queued.front()
                                    && !prepare_leased_command(
                                        &mut client,
                                        command,
                                        &mut released,
                                        &mut leased_replies,
                                    )
                                {
                                    break;
                                }
                                match queued.pop_front().expect("front control exists") {
                                    Command::Control(operation) => operation(&mut client),
                                    Command::Stats(reply) => {
                                        let _ = reply
                                            .send(client.stats().map_err(LocalDbError::component));
                                    }
                                    Command::OwnerTimelineArm(limit, reply) => {
                                        let _ = reply.send(
                                            client
                                                .owner_timeline_arm(limit)
                                                .map_err(LocalDbError::component),
                                        );
                                    }
                                    Command::ReadIoStats(reply) => {
                                        let _ = reply.send(
                                            client.read_io_stats().map_err(LocalDbError::component),
                                        );
                                    }
                                    Command::Seal(epoch, reply) => {
                                        let _ = reply.send(
                                            client.seal(epoch).map_err(LocalDbError::component),
                                        );
                                    }
                                    Command::ArmProfile(limit, reply) => {
                                        write_profile_limit = limit;
                                        profile_requests = 0;
                                        checkpoint_samples = 0;
                                        checkpoint_stalls = 0;
                                        profile_attempted = limit != 0;
                                        let enabled = limit != 0;
                                        let result = client
                                            .connection_profile(enabled, true)
                                            .map(|_| profile_enabled = enabled)
                                            .map_err(LocalDbError::component);
                                        if result.is_err() {
                                            record_write_profile_error(&write_profile, true);
                                            profile_enabled = false;
                                        }
                                        let _ = reply.send(result);
                                    }
                                    Command::FinishProfile(reply) => {
                                        checkpoint_samples = write_profile_limit;
                                        let result = if profile_enabled {
                                            match client.connection_profile(false, false) {
                                                Ok(profile) => {
                                                    capture_write_profile(&write_profile, profile);
                                                    profile_enabled = false;
                                                    Ok(())
                                                }
                                                Err(error) => {
                                                    record_write_profile_error(
                                                        &write_profile,
                                                        false,
                                                    );
                                                    profile_enabled = false;
                                                    Err(LocalDbError::component(error))
                                                }
                                            }
                                        } else {
                                            Ok(())
                                        };
                                        let _ = reply.send(result);
                                    }
                                    Command::Append(..)
                                    | Command::Checkpoint(..)
                                    | Command::CheckpointPosition(..) => unreachable!(),
                                }
                                progressed = true;
                            }
                            _ => {}
                        }
                    }
                    if disconnected
                        && queued.is_empty()
                        && appends.is_empty()
                        && checkpoint.is_none()
                        && position.is_none()
                        && parked_checkpoint.is_none()
                    {
                        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
                        if client.live_leased_mode()
                            && let Err(error) =
                                client.close_leased_extent(&mut released, Some(&write_timeline))
                        {
                            client.record_live_write_error();
                            record_first_failure(
                                &first_failure,
                                Some(partition),
                                None,
                                "leased_close",
                                &error,
                            );
                            // Release publishes the fatal outcome to the authority.
                            failure.store(true, std::sync::atomic::Ordering::Release);
                            let _ = wake.try_send(());
                        }
                        break;
                    }
                    drop(client);
                    if !progressed {
                        thread::yield_now();
                    }
                }
                if let Some((_, _, reply)) = position.take() {
                    let message =
                        failure_context(&first_failure, "TCP worker stopped during position proof");
                    let _ = reply.send(Err(LocalDbError::new(message)));
                }
                if let Some((_, _, reply)) = checkpoint.take() {
                    let message = failure_context(
                        &first_failure,
                        "TCP worker stopped during checkpoint proof",
                    );
                    let _ = reply.send(Err(LocalDbError::new(message)));
                }
                if let Some(command) = parked_checkpoint.take() {
                    let message = failure_context(
                        &first_failure,
                        "TCP worker stopped before proof submission",
                    );
                    match command {
                        Command::CheckpointPosition(_, _, _, reply) => {
                            let _ = reply.send(Err(LocalDbError::new(message)));
                        }
                        Command::Checkpoint(_, _, _, reply) => {
                            let _ = reply.send(Err(LocalDbError::new(message)));
                        }
                        _ => unreachable!("only fixed-cut proofs may be parked"),
                    }
                }
                #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
                {
                    let message = failure_context(
                        &first_failure,
                        "leased worker stopped with unknown write outcome",
                    );
                    for (_, members) in leased_replies.drain(..) {
                        for (version, _, reply) in members {
                            let _ = reply.send(Err(LocalDbError::unknown(version, &message)));
                        }
                    }
                    if let Some((_, _, members)) = deferred.take() {
                        for (version, _, reply) in members {
                            let _ = reply.send(Err(LocalDbError::unknown(version, &message)));
                        }
                    }
                    let _ = wake.try_send(());
                    // Take only ownership under the lock. Terminal DMA drain
                    // stays on this worker, outside the mutex needed by shutdown.
                    let failed_writer = worker_client.lock().take_failed_live_writer();
                    if let Some(mut writer) = failed_writer
                        && let Err(error) = writer.drain_failed()
                    {
                        record_first_failure(
                            &first_failure,
                            Some(partition),
                            None,
                            "leased_drain",
                            &error,
                        );
                        // Release publishes drain failure without returning DMA.
                        failure.store(true, std::sync::atomic::Ordering::Release);
                        let _ = wake.try_send(());
                    }
                }
                // Acquire keeps profile control off a failed authority.
                if profile_enabled && !failure.load(std::sync::atomic::Ordering::Acquire) {
                    let mut client = worker_client.lock();
                    match client.connection_profile(false, false) {
                        Ok(profile) => capture_write_profile(&write_profile, profile),
                        Err(_) => record_write_profile_error(&write_profile, false),
                    }
                }
            })?;
        Ok(Self {
            client,
            sender: Some(Arc::new(sender)),
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            worker_wake,
            thread: Some(thread),
        })
    }
    fn control_handle(&self) -> ControlHandle {
        ControlHandle {
            #[cfg(any(test, feature = "experimental-fabric"))]
            client: self.client.clone(),
            sender: self.sender.as_ref().map(Arc::downgrade).unwrap_or_default(),
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            worker_wake: Arc::downgrade(&self.worker_wake),
        }
    }

    fn control<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&mut Client) -> Result<T, super::tcp::TcpError> + Send + 'static,
    ) -> Result<T, LocalDbError> {
        self.control_handle().control(operation)
    }
    fn read_io_stats(&self) -> Result<super::tcp::ReadExecutorStats, LocalDbError> {
        self.control_handle().read_io_stats()
    }

    fn send(&self, command: Command) -> Result<(), LocalDbError> {
        self.sender
            .as_ref()
            .ok_or_else(|| LocalDbError::new("TCP worker stopped"))?
            .send(command)
            .map_err(|_| LocalDbError::new("TCP worker lost request outcome"))?;
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        signal_worker_wake(&self.worker_wake)?;
        Ok(())
    }
    fn append(
        &self,
        epoch: u64,
        batches: Vec<CompleteBatch>,
    ) -> Result<Pending<Vec<DurableBatch>>, LocalDbError> {
        let (tx, rx) = mpsc::sync_channel(1);
        self.send(Command::Append(epoch, batches, tx))?;
        Ok(rx)
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
impl Drop for ClientWorker {
    fn drop(&mut self) {
        self.sender.take();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        self.client.lock().clear_worker_wake();
    }
}
fn receive<T>(pending: Pending<T>) -> Result<T, LocalDbError> {
    pending
        .recv()
        .map_err(|_| LocalDbError::new("TCP worker lost request outcome"))?
}

fn add_tcp_profile(total: &mut super::tcp::TcpReadProfile, next: super::tcp::TcpReadProfile) {
    total.add(next);
}

#[cfg(test)]
mod tests;

#[cfg(all(test, feature = "experimental-nvmeof", target_os = "linux"))]
mod leased_tests;
#[cfg(all(test, feature = "experimental-nvmeof", target_os = "linux"))]
mod live_bench_tests;
#[cfg(all(test, feature = "experimental-nvmeof", target_os = "linux"))]
mod position_tests;

#[cfg(test)]
mod pipeline_tests;

#[cfg(test)]
mod lifecycle_tests;
#[cfg(test)]
mod model_check_tests;
#[cfg(test)]
mod wal_replay_tests;
