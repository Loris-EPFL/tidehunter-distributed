//! Static RF1 database authority over authenticated TCP Value WAL partitions.
//!
//! This is the controlled single-coordinator-host deployment from NEXT_STEPS
//! M4/M5. A native directory lock and durable authority manifest stay on that
//! host. Every restart durably reserves a new epoch, fences ALL registered nodes,
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
use super::native_index::{IndexReadCut, NativeIndex};
use super::native_publication::{NativePublicationRouting, PreparedNativePublication};
use super::publication::PublicationTracker;
use super::reference_directory::ReferenceDirectory;
use super::sequence::LogicalClock;
use super::storage::{
    AppendedBatch, DurableBatch, PartitionConfig, PartitionStats, StaticInventory, encode_metadata,
    install_metadata, read_metadata,
};
use super::tcp::checkpoint::{CheckpointProof, CheckpointReceipt};
use super::tcp::{Client, TlsFiles};
use super::types::{BatchId, BatchVersion, MutationVersion, ValueRecordReference};
use crate::key_shape::{KeyShape, KeySpace};
use crate::lock::DbLock;
use crate::metrics::Metrics;
use bincode::Options;
use blake2::{Blake2b, Digest};
use minibytes::Bytes;
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
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
use checkpoint_lifecycle::BackgroundCheckpoint;
pub use concurrent::ConcurrentRemoteDatabase;
mod pending_read;
mod preparation;
mod receipt_history;
use receipt_history::{HistoryPointer, ReceiptHistory};
mod read_cache;
mod read_pipeline;
mod transports;
use pending_read::ReadService;
pub use pending_read::{
    CompletedGet, CompletedRecord, PendingGet, PendingRecord, ReadProfileMonitor,
};
pub use read_cache::{KEYSPACE_COUNTER_COUNT, PerKeyspaceReadCacheStats, ReadCacheStats};
use read_cache::{ReadCache, ReadCachePolicy};
pub(crate) use read_pipeline::PendingScanPage;
pub use transports::RemoteTransportPool;

/// Diagnostic handles do not borrow the coordinator/index owner. Each control
/// connection retains its own authenticated stream lock; no global Db lock is
/// held over these network waits. Counters are cumulative, not a quiescent cut.
pub struct ReadIoMonitor {
    clients: Vec<(u32, Arc<Mutex<Client>>)>,
}
impl ReadIoMonitor {
    pub fn sample(&self) -> Result<Vec<(u32, super::tcp::ReadExecutorStats)>, LocalDbError> {
        self.clients
            .iter()
            .map(|(partition, client)| {
                client
                    .lock()
                    .read_io_stats()
                    .map(|stats| (*partition, stats))
                    .map_err(LocalDbError::component)
            })
            .collect()
    }
}

pub const BACKEND_ID: &str = "experimental-native-cell-sharded-value-wal-tcp-rf1";
const MANIFEST: &str = "tcp-database-authority-v1";
/// Admission, not a timer: crashes retain at most this many post-checkpoint
/// complete batches, including unacknowledged in-flight work.
pub const CHECKPOINT_MAX_TAIL_BATCHES: usize = 1024;
pub const CHECKPOINT_MAX_TAIL_BYTES: usize = 64 * 1024 * 1024;
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
    pub endpoints: Vec<RemoteEndpoint>,
    pub tls: TlsFiles,
    pub timeout: Duration,
    /// Resident ordering/outcome slots, not a lifetime per-epoch write limit.
    /// Completed outcomes spill to immutable pages before this budget is full.
    pub max_admitted_batches: usize,
    pub max_index_bytes: usize,
    pub max_pending_batches: usize,
    pub max_pending_bytes: usize,
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
    #[serde(default = "default_max_pending_reads")]
    pub max_pending_reads: usize,
    #[serde(default = "default_max_pending_read_bytes")]
    pub max_pending_read_bytes: usize,
    /// Per-append-connection bounded sample; zero keeps storage profiling disabled.
    #[serde(default = "default_write_profile_limit")]
    pub write_profile_limit: usize,
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

impl RemoteDatabaseConfig {
    /// Fixed cache-table allocation estimate, available before opening the DB.
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

    fn validate(&self) -> Result<(), LocalDbError> {
        self.inventory(1)
            .validate()
            .map_err(LocalDbError::component)?;
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
        self.recovery_page_limit()?;
        Ok(())
    }

    fn recovery_page_limit(&self) -> Result<usize, LocalDbError> {
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
    endpoints: Vec<RemoteEndpoint>,
    shape_yaml: String,
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

struct Submission {
    batch: Option<CompleteBatch>,
    routing: Option<NativePublicationRouting>,
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
    /// New logical admissions while immutable checkpoint persistence was active.
    pub background_checkpoint_suffix_batches: u64,
    /// Receipts observed by the owner while a prior sequence is unpublished.
    /// Elapsed time ends at contiguous publication; it is not a storage/fsync clock.
    pub durable_receipt_publication_blocked_batches: u64,
    pub durable_receipt_publication_blocked_ns: u64,
    /// Current count of receipts in the above blocked state.
    pub durable_receipt_publication_waiting_batches: u64,
    pub durable_receipt_publication_waiting_peak: u64,
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
    pub sampled_requests: u64,
    pub captured_connections: usize,
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
    read_cache: Arc<Mutex<ReadCache>>,
    cache_policy: ReadCachePolicy,
    write_profile: Arc<Mutex<WriteProfileSnapshot>>,
    transport_pool: Option<Arc<RemoteTransportPool>>,
    transport_key: Option<[u8; 32]>,
    config: RemoteDatabaseConfig,
    shape: KeyShape,
    path: PathBuf,
    epoch: u64,
    index: Arc<RwLock<NativeIndex>>,
    partition_indexes: HashMap<u32, usize>,
    inventory: StaticInventory,
    known: HashMap<BatchId, AppendedBatch>,
    receipt_history: ReceiptHistory,
    active_identities: HashMap<BatchId, usize>,
    clock: LogicalClock,
    publication: PublicationTracker,
    submission_base: u64,
    submissions: VecDeque<Submission>,
    durable_wait_started: VecDeque<Option<Instant>>,
    staged: Vec<VecDeque<usize>>,
    staged_bytes: Vec<usize>,
    inflight: Vec<VecDeque<PendingAppend>>,
    unresolved_append: Option<BatchVersion>,
    completion_wake: mpsc::Receiver<()>,
    preparation_wake: mpsc::SyncSender<()>,
    pipeline_stats: PipelineStats,
    reserved_index_bytes: usize,
    pending_bytes: usize,
    partition_admissions: Vec<usize>,
    partition_records: Vec<usize>,
    latest: Option<MutationVersion>,
    snapshot_generation: u64,
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
    healthy: bool,
    // Unlike `healthy`, this never marks a protected in-progress transition.
    // It is monotonic and shared with reads and append workers.
    concurrent_failure: Arc<std::sync::atomic::AtomicBool>,
    stopped: bool,
    stats: LocalDbStats,
    #[cfg(test)]
    before_preparation: Option<(mpsc::SyncSender<()>, mpsc::Receiver<()>)>,
    #[cfg(test)]
    concurrent_progress: Arc<Mutex<Option<mpsc::SyncSender<PipelineStats>>>>,
    #[cfg(test)]
    before_checkpoint_io: Option<mpsc::SyncSender<()>>,
    #[cfg(test)]
    checkpoint_pause: Option<(mpsc::SyncSender<()>, mpsc::Receiver<()>)>,
    #[cfg(test)]
    panic_after_concurrent_admissions: Option<u64>,
    // Network workers are joined before this host's authority lock is released.
    _lock: DbLock,
}

impl RemoteDatabase {
    fn submission_next(&self) -> u64 {
        self.submission_base + self.submissions.len() as u64
    }

    pub fn index_stats(&self) -> super::native_index::NativeIndexStats {
        self.index.read().stats()
    }

    /// Logical retained index charge, including pageable historical references.
    pub fn index_history_bytes(&self) -> usize {
        self.index.read().retained_metadata_bytes()
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
            + self.publication.resident_metadata_bytes()
            + self.receipt_history.cached_metadata_bytes()
    }

    pub fn retention_floor(&self) -> IndexReadCut {
        self.index.read().retention_floor()
    }

    pub fn protected_published_cut(
        &self,
    ) -> Result<(IndexReadCut, super::native_index::IndexReadPin), LocalDbError> {
        self.ensure_healthy()?;
        let index = self.index.read();
        let cut = index.latest_cut();
        let pin = index.pin_cut(cut).map_err(LocalDbError::component)?;
        Ok((cut, pin))
    }

    pub fn pin_cut(
        &self,
        cut: IndexReadCut,
    ) -> Result<super::native_index::IndexReadPin, LocalDbError> {
        self.ensure_healthy()?;
        self.index
            .read()
            .pin_cut(cut)
            .map_err(LocalDbError::component)
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
        let mut index = NativeIndex::new(shape, config.storage.database, config.max_index_bytes)
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
                format: 2,
                storage: config.storage.clone(),
                endpoints: config.endpoints.clone(),
                shape_yaml: shape_yaml.clone(),
                epochs: Vec::new(),
                checkpoint: None,
                checkpoint_staging: None,
                checkpoint_obsolete: None,
            };
            persist_manifest(&path, &manifest)?;
            manifest
        };
        if manifest.format != 2
            || manifest.storage != config.storage
            || manifest.endpoints != config.endpoints
            || manifest.shape_yaml != shape_yaml
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
        )?;
        let connected = transports::connect(&config, &shape, tls_config, 1, retained_controls)?;
        recovery_stats.connection_setup_ns = connected.elapsed_ns;
        recovery_stats.created_connections = (connected.clients.len() - connected.reused) as u64;
        recovery_stats.reused_connections = connected.reused as u64;
        let mut clients = connected.clients;
        for client in &clients {
            let status = client.connected_status();
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
        let histories = endpoint_map(&mut clients[..fence_count], |_, client| {
            let status = client.fence_to(epoch).map_err(LocalDbError::component)?;
            if status.active_epoch != Some(epoch)
                || !status.epochs.iter().any(|e| e.epoch == epoch && !e.sealed)
                || status.epochs.iter().any(|e| e.epoch < epoch && !e.sealed)
            {
                return Err(LocalDbError::new(
                    "node did not establish the complete fenced inventory",
                ));
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
        let mut receipt_history = ReceiptHistory::open(&path, config.storage.database)?;
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
                &mut recovery_stats,
            )?;
        }
        let replay_started = Instant::now();
        replay_history(
            &mut clients,
            &histories,
            epoch,
            &config,
            &shape,
            &mut index,
            &mut known,
            &receipt_history,
            &mut latest,
            &cursors,
            &mut recovery_stats,
        )?;
        recovery_stats.tail_replay_ns = elapsed(replay_started);
        // Current checkpoint/proofs and the complete tail have now validated.
        // Resolve only durably named cleanup intents before allowing more work.
        cleanup_checkpoint_artifacts(&path, &mut manifest)?;
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
        let concurrent_failure = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (wake_sender, completion_wake) = mpsc::sync_channel(clients.len());
        for client in clients {
            workers.push(
                ClientWorker::start(
                    client,
                    config.max_pending_batches,
                    wake_sender.clone(),
                    concurrent_failure.clone(),
                    config.write_profile_limit,
                    write_profile.clone(),
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
        let reads =
            ReadService::from_clients(&config, epoch, prepared_reads.clients, &mut recovery_stats)?;
        #[cfg(test)]
        if matches!(interruption, Some(OpenInterruption::AfterReadSetup)) {
            return Err(LocalDbError::new("injected interruption after read setup"));
        }
        manifest.epochs.last_mut().unwrap().active = true;
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
        let mut database = Self {
            clients: workers,
            reads: Arc::new(reads),
            preparation_pool,
            read_cache: Arc::new(Mutex::new(ReadCache::new(config.read_cache_bytes))),
            cache_policy: ReadCachePolicy::default(),
            write_profile: write_profile.clone(),
            transport_pool,
            transport_key,
            inventory: config.inventory(epoch),
            config,
            shape,
            path,
            epoch,
            index: Arc::new(RwLock::new(index)),
            partition_indexes,
            known,
            receipt_history,
            active_identities: HashMap::new(),
            clock: LogicalClock::new_fenced_epoch(epoch),
            publication: PublicationTracker::new(database, epoch, limit)
                .map_err(LocalDbError::component)?,
            submission_base: 0,
            submissions: VecDeque::new(),
            durable_wait_started: VecDeque::new(),
            staged: (0..count).map(|_| VecDeque::new()).collect(),
            staged_bytes: vec![0; count],
            inflight: (0..count).map(|_| VecDeque::new()).collect(),
            unresolved_append: None,
            completion_wake,
            preparation_wake: wake_sender,
            pipeline_stats: PipelineStats::default(),
            reserved_index_bytes: 0,
            pending_bytes: 0,
            partition_admissions: vec![0; count],
            partition_records: vec![0; count],
            latest,
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
            healthy: true,
            concurrent_failure,
            stopped: false,
            stats: LocalDbStats::default(),
            #[cfg(test)]
            before_preparation: None,
            #[cfg(test)]
            concurrent_progress: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            before_checkpoint_io: None,
            #[cfg(test)]
            checkpoint_pause: None,
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
        Ok(database)
    }

    pub fn backend_id(&self) -> &'static str {
        match self.config.tls.transport {
            super::tcp::Transport::Tcp => BACKEND_ID,
            super::tcp::Transport::Efa => "experimental-native-cell-sharded-value-wal-efa-rf1",
            super::tcp::Transport::FabricTcp => {
                "experimental-native-cell-sharded-value-wal-software-fabric-rf1"
            }
            super::tcp::Transport::EfaDirect => {
                "experimental-native-cell-sharded-value-wal-efa-direct-rf1"
            }
            super::tcp::Transport::EfaNative => {
                "trusted-native-cell-sharded-value-wal-efa-native-rf1"
            }
            super::tcp::Transport::FabricTcpNative => {
                "native-cell-sharded-value-wal-software-fabric-rf1"
            }
        }
    }
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    pub fn read_io_monitor(&self) -> ReadIoMonitor {
        ReadIoMonitor {
            clients: self
                .config
                .endpoints
                .iter()
                .zip(&self.clients)
                .map(|(endpoint, worker)| (endpoint.partition, worker.client.clone()))
                .collect(),
        }
    }

    pub fn validate_cached_read(&self) -> Result<(), LocalDbError> {
        self.ensure_healthy()
    }

    pub fn published_cut(&self) -> Result<IndexReadCut, LocalDbError> {
        self.ensure_healthy()?;
        Ok(self.index.read().latest_cut())
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
            .read()
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
            .read()
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
                    "TCP authority requires recovery with unresolved append",
                ));
            }
            if let Some(pending) = self.inflight.iter().find_map(|queue| queue.front()) {
                return Err(LocalDbError::unknown(
                    pending.first,
                    "TCP authority requires recovery with unresolved append",
                ));
            }
            return Err(LocalDbError::new(
                "TCP authority requires recovery or is closed",
            ));
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
        let batch = CompleteBatch::new(
            &self.shape,
            id,
            version,
            mutations,
            self.config.storage.batch_limits,
        )
        .map_err(LocalDbError::component)?;
        let prepared = self.prepare_admission(batch)?;
        self.stats.validation_ns += elapsed(started);
        self.admit_prepared(prepared)
    }

    fn check_checkpoint_capacity(&self, additional_identities: usize) -> Result<(), LocalDbError> {
        let receipts = self
            .known
            .len()
            .checked_add(self.active_identities.len())
            .and_then(|count| count.checked_add(additional_identities))
            .ok_or_else(|| LocalDbError::new("checkpoint receipt admission count overflow"))?;
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
        if let Some(receipt) = self.receipt_history.lookup(id).inspect_err(|_| {
            self.concurrent_failure
                .store(true, std::sync::atomic::Ordering::Release);
            self.reads.poison();
        })? {
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
            .read()
            .estimated_routed_bytes(routing.operations())
            .map_err(LocalDbError::component)?;
        let worker_metadata = batch
            .mutations()
            .len()
            .checked_mul(
                std::mem::size_of::<Mutation>()
                    + std::mem::size_of::<super::types::RecordDescriptor>(),
            )
            .ok_or_else(|| LocalDbError::new("worker metadata reservation overflow"))?;
        let credit = batch
            .canonical_heap_bytes()
            .and_then(|bytes| bytes.checked_add(worker_metadata))
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
                !checkpoint_admission_requires_flush(
                    self.checkpoint_tail_batches,
                    self.checkpoint_tail_bytes,
                    encoded_bytes,
                ) && (!identity
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
                .is_some_and(|reserved| self.index.read().check_capacity(reserved).is_ok()),
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
                if encoded_bytes > CHECKPOINT_MAX_TAIL_BYTES =>
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
                    .read()
                    .check_capacity(reserved)
                    .map_err(LocalDbError::component)
            }
        }
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

    // Only private, locally constructed CompleteBatch values reach this path.
    // Preparation does not allocate a logical slot or confer authority.
    fn admit_prepared(
        &mut self,
        prepared: PreparedAdmission,
    ) -> Result<BatchVersion, LocalDbError> {
        let PreparedAdmission {
            batch,
            routing,
            partition,
            metadata_bytes,
            credit,
            group_charge,
        } = prepared;
        self.ensure_healthy()?;
        if self.config.pipelined_writes && self.inflight.iter().any(|queue| !queue.is_empty()) {
            self.progress_pipeline(false)?;
        }
        let started = Instant::now();
        let mut validation_excluded_ns = 0;
        let id = batch.id();
        let version = batch.version();
        if let Some((previous, digest)) = self.existing_identity(id)? {
            if version != previous || batch.digest() != digest {
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
        let encoded_bytes = batch.encoded().len();
        if checkpoint_admission_requires_flush(
            self.checkpoint_tail_batches,
            self.checkpoint_tail_bytes,
            encoded_bytes,
        ) || self
            .known
            .len()
            .saturating_add(self.active_identities.len())
            >= self.config.max_admitted_batches
        {
            self.stats.validation_ns += elapsed(started);
            self.ensure_admission_pressure(AdmissionPressure::Tail {
                encoded_bytes,
                identity: true,
            })?;
            validation_excluded_ns = elapsed(started);
        }
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
        // Whole batches must fit a fresh node window before logical allocation.
        // Successive groups may spill; cumulative reservations cause bounded
        // progress/backpressure, not a retained-history capacity rejection.
        let records = batch.mutations().len();
        let total_records = self.partition_records[partition]
            .checked_add(records)
            .ok_or_else(|| LocalDbError::new("reference reservation overflow"))?;
        if self
            .check_storage_group_capacity(
                self.partition_admissions[partition].saturating_add(1),
                total_records,
            )
            .is_err()
        {
            self.ensure_admission_pressure(AdmissionPressure::Partition { partition, records })?;
            validation_excluded_ns = elapsed(started);
        }
        let reserved = self
            .reserved_index_bytes
            .checked_add(metadata_bytes)
            .ok_or_else(|| LocalDbError::new("index reservation overflow"))?;
        if self.index.read().check_capacity(reserved).is_err() {
            self.ensure_admission_pressure(AdmissionPressure::Index {
                additional_bytes: metadata_bytes,
            })?;
            validation_excluded_ns = elapsed(started);
        }
        let reserved = self
            .reserved_index_bytes
            .checked_add(metadata_bytes)
            .ok_or_else(|| LocalDbError::new("index reservation overflow"))?;
        self.index
            .read()
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
        self.submissions.push_back(Submission {
            batch: Some(batch),
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

    /// A lone write has no independent storage work to overlap. Use the same
    /// protected TLS stream on the caller, avoiding request/completion handoffs.
    /// The receipt, persistence and publication rules are identical to groups.
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
        self.pipeline_stats.direct_appends += 1;
        self.pipeline_stats.peak_inflight_nodes = self.pipeline_stats.peak_inflight_nodes.max(1);
        let started = Instant::now();
        let receipts = match self.clients[node]
            .client
            .lock()
            .append_many(self.epoch, std::slice::from_ref(batch))
        {
            Ok(receipts) => receipts,
            Err(error) => {
                self.unresolved_append = Some(version);
                return Err(LocalDbError::unknown(version, error));
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
                        self.inflight[node].remove(position);
                        match receipts {
                            Ok(receipts) => {
                                if let Err(error) = self.accept_receipts(receipts) {
                                    self.unresolved_append = Some(first);
                                    return Err(error);
                                }
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
            for node in 0..self.clients.len() {
                if self.staged[node].is_empty() || (!flush_tails && !self.ready_group(node)) {
                    continue;
                }
                let count = {
                    let limits = self.config.storage.group_limits;
                    let mut bytes = GROUP_HEADER_LEN;
                    let mut count = 0;
                    let mut records = 0usize;
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
                        bytes += next;
                        records = next_records;
                        count += 1;
                    }
                    if count == 0 {
                        return Err(LocalDbError::new("batch cannot fit remote group"));
                    }
                    let full = count == limits.max_batches as usize
                        || count < self.staged[node].len()
                        || bytes >= limits.max_encoded_bytes - limits.max_encoded_bytes / 4;
                    if !flush_tails && !full {
                        continue;
                    }
                    count
                };
                let slots: Vec<_> = self.staged[node].drain(..count).collect();
                let batches: Vec<_> = slots
                    .iter()
                    .map(|&slot| {
                        self.submissions[slot - self.submission_base as usize]
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
                let first = batches[0].version();
                let reply = match self.clients[node].append(self.epoch, batches) {
                    Ok(reply) => reply,
                    Err(error) => {
                        self.unresolved_append = Some(first);
                        return Err(LocalDbError::unknown(first, error));
                    }
                };
                self.inflight[node].push_back(PendingAppend { first, reply });
                self.pipeline_stats.append_requests += 1;
                if !flush_tails {
                    self.pipeline_stats.early_dispatches += 1;
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
        if result.is_ok() {
            self.healthy = true;
        } else {
            self.concurrent_failure
                .store(true, std::sync::atomic::Ordering::Release);
            self.reads.poison();
            self.inflight.iter_mut().for_each(VecDeque::clear);
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
        if self.publication.durable_prefix().next_sequence()
            == self.publication.published_prefix().next_sequence()
        {
            return Ok(());
        }
        let started = Instant::now();
        let old_published_prefix = self.publication.published_prefix().next_sequence();
        let durable_prefix = self.publication.durable_prefix().next_sequence();
        let candidate_capacity = durable_prefix.saturating_sub(old_published_prefix) as usize;
        let mut cache_candidates = Vec::with_capacity(candidate_capacity);
        let submissions = &mut self.submissions;
        let submission_base = self.submission_base;
        let mut index_guard = self.index.write();
        let _poison = concurrent::FailureOnPanic(self.concurrent_failure.clone());
        let index = &mut *index_guard;
        let known = &mut self.known;
        let active = &mut self.active_identities;
        let latest = &mut self.latest;
        let reserved = &mut self.reserved_index_bytes;
        let progress = self
            .publication
            .publish_ready(|durable| {
                let receipt = durable.appended();
                let address = receipt.address();
                let entry = submissions
                    .get_mut((receipt.version().sequence - submission_base) as usize)
                    .ok_or_else(|| LocalDbError::new("receipt outside admission inventory"))?;
                let batch = entry
                    .batch
                    .as_ref()
                    .ok_or_else(|| LocalDbError::new("batch already published"))?;
                entry
                    .routing
                    .take()
                    .ok_or_else(|| LocalDbError::new("missing prepared publication routing"))?
                    .publish_authenticated(batch, durable, index)
                    .map_err(LocalDbError::component)?;
                index.promote_committed().map_err(LocalDbError::component)?;
                let mutation_version = batch.mutation_version(batch.mutations().len() - 1);
                *latest = mutation_version;
                known.insert(receipt.id(), receipt);
                active.remove(&receipt.id());
                *reserved -= entry.metadata_bytes;
                let batch = entry
                    .batch
                    .take()
                    .ok_or_else(|| LocalDbError::new("batch disappeared during publication"))?;
                cache_candidates.push((address, batch, entry.credit));
                Ok::<_, LocalDbError>(())
            })
            .inspect_err(|_| {
                self.concurrent_failure
                    .store(true, std::sync::atomic::Ordering::Release)
            })
            .map_err(LocalDbError::component)?;
        let published_prefix = self.publication.published_prefix().next_sequence();
        for sequence in old_published_prefix..published_prefix {
            if let Some(wait_started) = self
                .durable_wait_started
                .get_mut((sequence - submission_base) as usize)
                && let Some(wait_started) = wait_started.take()
            {
                self.write_stats.durable_receipt_publication_blocked_batches += 1;
                self.write_stats.durable_receipt_publication_blocked_ns += elapsed(wait_started);
                self.write_stats.durable_receipt_publication_waiting_batches -= 1;
            }
        }
        drop(index_guard);
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
                    let prepared = ReadCache::prepare_post_publish(
                        &self.read_cache,
                        self.config.read_cache_bytes,
                        reference,
                        &batch.mutations()[operation],
                    );
                    self.read_cache.lock().insert_prepared(prepared);
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
    ) -> Result<Vec<Mutation>, LocalDbError> {
        self.ensure_healthy()?;
        if references.len() > 256 {
            return Err(LocalDbError::new("read window exceeds 256 records"));
        }
        let mut output = vec![None; references.len()];
        let mut missing = Vec::new();
        let mut slots = Vec::new();
        for (slot, reference) in references.iter().enumerate() {
            if let Some(value) = self.read_cache.lock().get(reference) {
                output[slot] = Some(value);
            } else {
                slots.push(slot);
                missing.push(*reference);
            }
        }
        if !missing.is_empty() {
            let values = self.fetch_uncached(&missing)?;
            if values.len() != missing.len() {
                self.healthy = false;
                return Err(LocalDbError::new("read result count mismatch"));
            }
            self.ensure_healthy()?;
            for ((slot, reference), value) in slots.into_iter().zip(missing).zip(values) {
                let prepared = ReadCache::prepare_verified(
                    &self.read_cache,
                    self.config.read_cache_bytes,
                    reference,
                    &value,
                );
                self.read_cache.lock().insert_prepared(prepared);
                output[slot] = Some(value);
            }
        }
        Ok(output.into_iter().map(Option::unwrap).collect())
    }

    fn fetch_uncached(
        &mut self,
        references: &[ValueRecordReference],
    ) -> Result<Vec<Mutation>, LocalDbError> {
        if references.len() > 256 {
            return Err(LocalDbError::new("read window exceeds 256 records"));
        }
        let started = Instant::now();
        let mut groups: Vec<Vec<(usize, ValueRecordReference)>> =
            vec![Vec::new(); self.clients.len()];
        for (i, reference) in references.iter().enumerate() {
            let node = *self
                .partition_indexes
                .get(&reference.batch_address.partition)
                .ok_or_else(|| LocalDbError::new("reference names unregistered partition"))?;
            groups[node].push((i, *reference));
        }
        let mut batch =
            pending_read::ReadBatch::new(&self.reads, vec![None; references.len()], groups)?;
        // These private grouped fetch callers resolve the current committed cut
        // under the sole owner. Transfer its protection through physical read
        // completion, even if the aggregate caller exits early on an error.
        batch.pin = Some(self.pin_cut(IndexReadCut::Latest)?);
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
        self.ensure_healthy()?;
        if keys.len() > 256 {
            return Err(LocalDbError::new("read batch exceeds 256 keys"));
        }
        let resolve_started = Instant::now();
        let cut = self
            .latest
            .map_or(IndexReadCut::BeforeAll, IndexReadCut::At);
        let mut slots = Vec::new();
        let mut references = Vec::new();
        for (slot, key) in keys.iter().enumerate() {
            if let Some(reference) = self
                .index
                .read()
                .get(ks, key, cut)
                .map_err(LocalDbError::component)?
            {
                slots.push(slot);
                references.push(reference);
            }
        }
        self.stats.read_resolve_ns += elapsed(resolve_started);
        // Preserve per-shard RPC batching; independent scalar windows use the
        // split-phase API instead, so these remain distinct benchmark paths.
        let mut output = vec![None; keys.len()];
        for (slot, mutation) in slots.into_iter().zip(self.fetch(&references)?) {
            output[slot] = Self::point_value(ks, &keys[slot], mutation)?;
        }
        Ok(output)
    }
    pub fn exists(&mut self, ks: KeySpace, key: &[u8]) -> Result<bool, LocalDbError> {
        self.ensure_healthy()?;
        let started = Instant::now();
        let reference = self
            .index
            .read()
            .get(ks, key, IndexReadCut::Latest)
            .map_err(LocalDbError::component)?;
        self.stats.read_resolve_ns += elapsed(started);
        let Some(reference) = reference else {
            return Ok(false);
        };
        if !self.shape.ks(ks).need_check_index_key() {
            return Ok(true);
        }
        match self.fetch(&[reference])?.remove(0) {
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
                    .read()
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
    /// Capture partial opt-in append profiles after all admitted writes have drained.
    /// With a zero limit, this returns without issuing any diagnostic RPC.
    pub fn finish_write_profile(&mut self) -> Result<(), LocalDbError> {
        self.drain()?;
        if self.config.write_profile_limit == 0 {
            return Ok(());
        }
        for client in &self.clients {
            client.finish_profile()?;
        }
        Ok(())
    }
    pub fn shutdown_stats(&self) -> ShutdownStats {
        self.shutdown_stats
    }
    pub fn read_cache_stats(&self) -> ReadCacheStats {
        self.read_cache.lock().stats()
    }
    pub fn per_keyspace_read_cache_stats(
        &self,
    ) -> [PerKeyspaceReadCacheStats; KEYSPACE_COUNTER_COUNT] {
        self.read_cache.lock().per_keyspace_stats()
    }
    /// Enable private Sui checkpoint cache policy using live shape names.
    pub(super) fn configure_sui_read_cache(&mut self) {
        let policy = ReadCachePolicy::for_sui_shape(&self.shape);
        self.cache_policy = policy;
        self.read_cache.lock().configure_policy(policy);
    }

    /// Discard derivative cache contents without changing index/WAL state.
    pub fn clear_read_cache(&mut self) {
        *self.read_cache.lock() =
            ReadCache::new_with_policy(self.config.read_cache_bytes, self.cache_policy);
    }

    /// Read-only node counters outside timed reads. Outstanding work makes a
    /// phase boundary ambiguous, so callers must drain writes and finish reads.
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
                    .client
                    .lock()
                    .read_io_stats()
                    .map(|stats| (endpoint.partition, stats))
                    .map_err(LocalDbError::component)
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
                let mut profile = client
                    .client
                    .lock()
                    .connection_profile(enabled, reset)
                    .map_err(LocalDbError::component)?;
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
        self.clients
            .iter()
            .zip(&self.config.endpoints)
            .map(|(client, endpoint)| {
                client
                    .client
                    .lock()
                    .read_evidence_for_benchmark(self.epoch, evict)
                    .map(|p| (endpoint.partition, p))
                    .map_err(LocalDbError::component)
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
        self.clients
            .iter()
            .zip(&self.config.endpoints)
            .map(|(client, endpoint)| {
                client
                    .client
                    .lock()
                    .process_resources_for_benchmark(self.epoch)
                    .map(|resources| (endpoint.partition, resources))
                    .map_err(LocalDbError::component)
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
            total.registered_records += stats.registered_records;
            total.reference_metadata_bytes += stats.reference_metadata_bytes;
        }
        Ok(total)
    }
    pub fn shutdown(&mut self) -> Result<(), LocalDbError> {
        if self.stopped {
            return Ok(());
        }
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
        let started = Instant::now();
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
        self.shutdown_stats.seal_ns = elapsed(started);
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
}

fn fill_replay_page(
    client: &mut Client,
    state: &mut ReplayPage,
    epoch: u64,
    cursor: usize,
    limit: usize,
) -> Result<(), LocalDbError> {
    let page = client
        .recovery_page(epoch, cursor, limit)
        .map_err(LocalDbError::component)?;
    let end = cursor
        .checked_add(page.batches.len())
        .ok_or_else(|| LocalDbError::new("recovery cursor overflow"))?;
    if page.batches.len() > limit || page.next.is_some_and(|next| next != end || next <= cursor) {
        return Err(LocalDbError::new("invalid bounded recovery page"));
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

fn checkpoint_admission_requires_flush(batches: usize, bytes: usize, incoming: usize) -> bool {
    batches >= CHECKPOINT_MAX_TAIL_BATCHES
        || bytes
            .checked_add(incoming)
            .is_none_or(|total| total > CHECKPOINT_MAX_TAIL_BYTES)
}

/// K-way merge of bounded pages. Never retain the database's value history in
/// coordinator RAM merely to sort it; the index receives one batch at a time.
#[allow(clippy::too_many_arguments)]
fn replay_history(
    clients: &mut [Client],
    histories: &[Vec<u64>],
    current_epoch: u64,
    config: &RemoteDatabaseConfig,
    shape: &KeyShape,
    index: &mut NativeIndex,
    known: &mut HashMap<BatchId, AppendedBatch>,
    receipt_history: &ReceiptHistory,
    latest: &mut Option<MutationVersion>,
    cursors: &HashMap<(usize, u64), (usize, usize)>,
    stats: &mut RecoveryStats,
) -> Result<(), LocalDbError> {
    let page_limit = config.recovery_page_limit()?;
    let mut previous = latest.map(|cut| cut.batch);
    for epoch in 1..current_epoch {
        let mut pages = endpoint_map(clients, |node, client| {
            let mut page = ReplayPage {
                batches: VecDeque::new(),
                next: None,
                last: None,
            };
            let (cursor, retained) = cursors
                .get(&(node, epoch))
                .copied()
                .unwrap_or((0, usize::MAX));
            if histories[node].contains(&epoch) && cursor != retained {
                fill_replay_page(client, &mut page, epoch, cursor, page_limit)?;
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
                || receipt_history.lookup(batch.id())?.is_some()
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
            .publish(index)
            .map_err(LocalDbError::component)?;
            index.promote_committed().map_err(LocalDbError::component)?;
            *latest = batch.mutation_version(batch.mutations().len() - 1);
            known.insert(batch.id(), durable.appended());
            stats.tail_batches += 1;
            stats.tail_bytes += batch.encoded().len() as u64;
            if stats.tail_batches > CHECKPOINT_MAX_TAIL_BATCHES as u64
                || stats.tail_bytes > CHECKPOINT_MAX_TAIL_BYTES as u64
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
                    if page.batches.len() <= page_limit / 2
                        && let Some(next) = page.next
                    {
                        let available = page_limit - page.batches.len();
                        fill_replay_page(client, page, epoch, next, available)?;
                    }
                    Ok(())
                })?;
            }
            if let Some((batch, _)) = pages[node].batches.front() {
                ready.push(Reverse((batch.version(), node)));
            }
        }
    }
    Ok(())
}

fn encode_checkpoint(image: &CheckpointImage) -> Result<Vec<u8>, LocalDbError> {
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_limit(MAX_CHECKPOINT_METADATA_BYTES)
        .serialize(image)
        .map_err(LocalDbError::component)
}

fn read_checkpoint(path: &Path) -> Result<CheckpointImage, LocalDbError> {
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

#[allow(clippy::too_many_arguments)]
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
    receipt_history: &mut ReceiptHistory,
    latest: &mut Option<MutationVersion>,
    cursors: &mut HashMap<(usize, u64), (usize, usize)>,
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
    let image = read_checkpoint(&metadata_path)?;
    if image.format != 2
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
    let restored = NativeIndex::open_snapshot(
        &index_path,
        shape,
        config.storage.database,
        config.max_index_bytes,
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
) -> Result<(), LocalDbError> {
    let current = manifest
        .checkpoint
        .as_ref()
        .map(|commit| (commit.epoch, commit.generation));
    let staged = manifest.checkpoint_staging.clone();
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
    for (files, retiring) in [(staged.as_ref(), false), (obsolete.as_ref(), true)] {
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
    for files in [staged, obsolete].into_iter().flatten() {
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

type Pending<T> = mpsc::Receiver<Result<T, LocalDbError>>;
enum Command {
    Append(u64, Vec<CompleteBatch>, Reply<Vec<DurableBatch>>),
    Stats(Reply<PartitionStats>),
    Seal(u64, Reply<()>),
    FinishProfile(Reply<()>),
}
struct ClientWorker {
    client: Arc<Mutex<Client>>,
    sender: Option<mpsc::SyncSender<Command>>,
    thread: Option<JoinHandle<()>>,
}
impl ClientWorker {
    fn start(
        client: Client,
        queue_capacity: usize,
        wake: mpsc::SyncSender<()>,
        failure: Arc<std::sync::atomic::AtomicBool>,
        write_profile_limit: usize,
        write_profile: Arc<Mutex<WriteProfileSnapshot>>,
    ) -> std::io::Result<Self> {
        let client = Arc::new(Mutex::new(client));
        let worker_client = Arc::clone(&client);
        let (sender, receiver) = mpsc::sync_channel(queue_capacity);
        let thread = thread::Builder::new()
            .name("value-wal-tcp".into())
            .spawn(move || {
                let mut profile_attempted = false;
                let mut profile_enabled = false;
                let mut profile_requests = 0usize;
                let mut queued = VecDeque::new();
                let mut appends: HashMap<u64, (BatchVersion, Reply<Vec<DurableBatch>>)> =
                    HashMap::new();
                let mut disconnected = false;
                loop {
                    let mut client = worker_client.lock();
                    while !disconnected {
                        match receiver.try_recv() {
                            Ok(command) => queued.push_back(command),
                            Err(mpsc::TryRecvError::Empty) => break,
                            Err(mpsc::TryRecvError::Disconnected) => disconnected = true,
                        }
                    }

                    if !disconnected
                        && queued.is_empty()
                        && appends.is_empty()
                        && !client.has_pending_io()
                    {
                        drop(client);
                        match receiver.recv() {
                            Ok(command) => queued.push_back(command),
                            Err(_) => disconnected = true,
                        }
                        continue;
                    }

                    let mut progressed = false;
                    if let Some(Command::Append(epoch, batches, _)) = queued.front()
                        && (!profile_enabled || profile_requests < write_profile_limit)
                    {
                        if write_profile_limit > 0 && !profile_attempted && appends.is_empty() {
                            profile_attempted = true;
                            profile_enabled = client.connection_profile(true, false).is_ok();
                        }
                        let first = batches[0].version();
                        match client.submit_append_many(*epoch, batches) {
                            Ok(Some(id)) => {
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
                                failure.store(true, std::sync::atomic::Ordering::Release);
                                let _ = reply.send(Err(LocalDbError::unknown(first, error)));
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
                                    if let Some((first, reply)) = appends.remove(&completion.id) {
                                        let result = completion
                                            .result
                                            .map_err(|error| LocalDbError::unknown(first, error));
                                        if result.is_err() {
                                            failure
                                                .store(true, std::sync::atomic::Ordering::Release);
                                        }
                                        let _ = reply.send(result);
                                        let _ = wake.try_send(());
                                    }
                                }
                            }
                            Err(error) => {
                                failure.store(true, std::sync::atomic::Ordering::Release);
                                for (_, (first, reply)) in appends.drain() {
                                    let _ = reply
                                        .send(Err(LocalDbError::unknown(first, error.to_string())));
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
                                        Command::Seal(_, reply) | Command::FinishProfile(reply) => {
                                            let _ = reply.send(Err(LocalDbError::new(&message)));
                                        }
                                    }
                                }
                                break;
                            }
                        }
                    }

                    if profile_enabled
                        && profile_requests >= write_profile_limit
                        && appends.is_empty()
                    {
                        if let Ok(profile) = client.connection_profile(false, false) {
                            capture_write_profile(&write_profile, profile);
                        }
                        profile_enabled = false;
                        progressed = true;
                    }

                    if appends.is_empty() {
                        match queued.front() {
                            Some(Command::Stats(_))
                            | Some(Command::Seal(_, _))
                            | Some(Command::FinishProfile(_)) => {
                                match queued.pop_front().expect("front control exists") {
                                    Command::Stats(reply) => {
                                        let _ = reply
                                            .send(client.stats().map_err(LocalDbError::component));
                                    }
                                    Command::Seal(epoch, reply) => {
                                        let _ = reply.send(
                                            client.seal(epoch).map_err(LocalDbError::component),
                                        );
                                    }
                                    Command::FinishProfile(reply) => {
                                        let result = if profile_enabled {
                                            client
                                                .connection_profile(false, false)
                                                .map(|profile| {
                                                    capture_write_profile(&write_profile, profile);
                                                    profile_enabled = false;
                                                })
                                                .map_err(LocalDbError::component)
                                        } else {
                                            Ok(())
                                        };
                                        let _ = reply.send(result);
                                    }
                                    Command::Append(..) => unreachable!(),
                                }
                                progressed = true;
                            }
                            _ => {}
                        }
                    }
                    if disconnected && queued.is_empty() && appends.is_empty() {
                        break;
                    }
                    drop(client);
                    if !progressed {
                        thread::yield_now();
                    }
                }
                if profile_enabled {
                    let mut client = worker_client.lock();
                    if let Ok(profile) = client.connection_profile(false, false) {
                        capture_write_profile(&write_profile, profile);
                    }
                }
            })?;
        Ok(Self {
            client,
            sender: Some(sender),
            thread: Some(thread),
        })
    }
    fn send(&self, command: Command) -> Result<(), LocalDbError> {
        self.sender
            .as_ref()
            .ok_or_else(|| LocalDbError::new("TCP worker stopped"))?
            .send(command)
            .map_err(|_| LocalDbError::new("TCP worker lost request outcome"))
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

#[cfg(test)]
mod pipeline_tests;

#[cfg(test)]
mod lifecycle_tests;
