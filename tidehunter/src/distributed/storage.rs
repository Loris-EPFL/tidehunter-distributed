//! Native WAL-backed partition authority with bounded resident pending metadata.
//!
//! Completed receipts and descriptors use CRC-protected pageable disk history.
//! Recovery permanently seals an old epoch and replays only its unrooted suffix.
//! Whole-file retirement requires an explicit durable recovery/reachability
//! decision; no live values move and physical addresses are never reused.

use super::codec::{BatchLimits, CodecError, CompleteBatch, Mutation};
use super::group::{CompleteGroup, GROUP_HEADER_LEN, GroupError, GroupLimits};
use super::reference_directory::{
    ConflictLookup, DirectoryError, LookupWork, ReferenceDirectory, ReferenceDirectoryWriter,
    RegisteredBatch,
};
use super::types::{
    BatchId, BatchVersion, PayloadDigest, PhysicalValueAddress, RequestAuthority,
    ValueRecordReference,
};
use crate::crc::CrcFrame;
use crate::key_shape::KeyShape;
use crate::lock::DbLock;
use crate::metrics::Metrics;
use crate::wal::{PreparedWalWrite, Wal, WalError, WalWriter};
use crate::{WalKind, WalLayout, WalPosition};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
#[cfg(feature = "experimental-nvmeof")]
use std::os::unix::fs::FileExt;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::AtomicU8;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

const REGISTRATION: &str = "partition-registration-v5";
const OLD_REGISTRATION: &str = "partition-registration-v4";
const SEAL: &str = "partition-seal-v5";
const MAX_METADATA_BYTES: u64 = 1024 * 1024;
const LIVE_REGISTRATION: &str = "partition-live-nvmeof-v1";
const LIVE_FRONTIER: &str = "partition-live-frontier-v1";

pub(crate) mod file_inventory;
pub(crate) mod history;

/// Full static inventory for one database epoch, copied durably to every node.
/// The caller must hold the unique database authority; this is not a consensus
/// service. Placement is deterministic by original operation identity, so a retry
/// cannot silently choose a different partition in this inventory.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StaticInventory {
    pub database: [u8; 16],
    pub epoch: u64,
    pub segment_generation: u64,
    pub partitions: Vec<u32>,
}

impl StaticInventory {
    pub fn validate(&self) -> Result<(), StorageError> {
        if self.partitions.is_empty()
            || self.partitions.len() > 1024
            || self.partitions.windows(2).any(|p| p[0] >= p[1])
        {
            return Err(StorageError::Invalid(
                "inventory must contain 1..=1024 sorted unique partitions",
            ));
        }
        Ok(())
    }

    pub fn partition_for(&self, id: BatchId) -> Result<u32, StorageError> {
        self.validate()?;
        self.partition_for_validated(id)
    }

    /// Reuse an inventory whose immutable partition ordering was validated on
    /// open; foreign identities remain rejected on every placement.
    pub(crate) fn partition_for_validated(&self, id: BatchId) -> Result<u32, StorageError> {
        if id.database != self.database {
            return Err(StorageError::WrongDatabase);
        }
        // Stable across hosts/restarts; not std::hash's process-specific seed.
        let hash = id.operation.iter().fold(0xcbf29ce484222325u64, |h, b| {
            (h ^ u64::from(*b)).wrapping_mul(0x100000001b3)
        });
        Ok(self.partitions[(hash % self.partitions.len() as u64) as usize])
    }
}

pub use super::reference_directory::{AuthorityCacheStats, HistoryMaintenanceStats};
pub use crate::wal::WalMemberReadPolicy;

/// Explicit limits for this experimental log, separate from native Db limits.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PartitionConfig {
    pub inventory: StaticInventory,
    pub partition: u32,
    pub batch_limits: BatchLimits,
    pub group_limits: GroupLimits,
    pub fragment_bytes: u64,
    pub wal_file_bytes: u64,
    /// Bounds the current receipt/descriptor window (also capped at 1024), not
    /// lifetime durable history. A complete pending group still gates append.
    pub max_batches: usize,
    /// Resident completed/pending descriptors, cold-authority cache and immutable
    /// read/CQ pins share this budget. Cold disk history is not lifetime-resident.
    pub max_reference_metadata_bytes: usize,
}

impl PartitionConfig {
    pub(crate) fn validate(&self) -> Result<(), StorageError> {
        self.inventory.validate()?;
        self.batch_limits.validate()?;
        self.group_limits.validate()?;
        if !self.inventory.partitions.contains(&self.partition)
            || self.group_limits.max_encoded_bytes
                < GROUP_HEADER_LEN
                    + CompleteGroup::encoded_member_len(self.batch_limits.max_encoded_bytes)?
            || self.fragment_bytes < self.group_limits.max_encoded_bytes as u64 + 8
            || self.fragment_bytes > u32::MAX as u64
            || !self.fragment_bytes.is_multiple_of(8)
            || self.wal_file_bytes == 0
            || !self.wal_file_bytes.is_multiple_of(self.fragment_bytes)
            || self.max_batches == 0
            || self.max_batches > 1_000_000
        {
            return Err(StorageError::Invalid(
                "invalid partition capacity or WAL layout",
            ));
        }
        // Establish a feasible immutable-directory budget before filesystem effects.
        ReferenceDirectory::check_fresh_total_capacity(
            self.max_batches,
            self.max_reference_metadata_bytes,
            0,
            0,
        )?;
        Ok(())
    }

    fn layout(&self) -> WalLayout {
        WalLayout {
            frag_size: self.fragment_bytes,
            wal_file_size: self.wal_file_bytes,
            max_maps: 3,
            direct_io: false,
            kind: WalKind::Replay,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct Registration {
    format: u32,
    config: PartitionConfig,
    shape_yaml: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct SealedEnd {
    format: u32,
    epoch: u64,
    generation: u64,
    frame_count: usize,
    physical_end: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct LiveFrontier {
    database: [u8; 16],
    partition: u32,
    epoch: u64,
    generation: u64,
    end: u64,
    tail: Option<LiveFrontierTail>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct LiveFrontierTail {
    offset: u64,
    crc: u32,
    id: BatchId,
    version: BatchVersion,
    digest: PayloadDigest,
}

impl LiveFrontier {
    fn new(config: &PartitionConfig) -> Self {
        Self {
            database: config.inventory.database,
            partition: config.partition,
            epoch: config.inventory.epoch,
            generation: config.inventory.segment_generation,
            end: 0,
            tail: None,
        }
    }
}

/// Append success is not a durable acknowledgement or index publication.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AppendedBatch {
    id: BatchId,
    version: BatchVersion,
    digest: PayloadDigest,
    address: PhysicalValueAddress,
}

impl AppendedBatch {
    pub(crate) fn from_parts(
        id: BatchId,
        version: BatchVersion,
        digest: PayloadDigest,
        address: PhysicalValueAddress,
    ) -> Self {
        Self {
            id,
            version,
            digest,
            address,
        }
    }
    pub fn id(self) -> BatchId {
        self.id
    }
    pub fn version(self) -> BatchVersion {
        self.version
    }
    pub fn digest(self) -> PayloadDigest {
        self.digest
    }
    pub fn address(self) -> PhysicalValueAddress {
        self.address
    }
}

/// Local disk durability only. This is not replica durability, an index commit,
/// a completed logical prefix, or survival of permanent loss of this node's SSD.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DurableBatch(AppendedBatch);

impl DurableBatch {
    pub fn appended(self) -> AppendedBatch {
        self.0
    }

    /// Only the validated transport boundary may reconstruct this proof.
    /// It must first bind the reply to the requested database, version, digest,
    /// registered partition/generation and physical bounds. Deserializing an
    /// arbitrary peer receipt is not evidence of persistence.
    pub(crate) fn from_validated_parts(
        id: BatchId,
        version: BatchVersion,
        digest: PayloadDigest,
        address: PhysicalValueAddress,
    ) -> Self {
        Self(AppendedBatch {
            id,
            version,
            digest,
            address,
        })
    }
}

#[cfg(feature = "experimental-nvmeof")]
struct PendingLive {
    descriptor: super::nvmeof::live_storage::LiveGroupDescriptor,
    reservation: super::nvmeof::live_storage::LiveWriteReservation,
    allocation: crate::wal::ExternalWalAllocation,
    credit: super::reference_directory::MetadataCredit,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum WriterPoison {
    // Validation failed before publication. Only the earlier appended prefix
    // may still be synced; the unresolved external allocation stays pinned.
    #[cfg(feature = "experimental-nvmeof")]
    IncompleteLiveFrame,
    Fatal,
}

struct State {
    // This must drop before writer: the tracker can be waiting for this pin.
    #[cfg(feature = "experimental-nvmeof")]
    pending_live: Option<PendingLive>,
    #[cfg(feature = "experimental-nvmeof")]
    live: bool,
    #[cfg(feature = "experimental-nvmeof")]
    last_live_complete: Option<([u8; 16], usize, usize)>,
    #[cfg(feature = "experimental-nvmeof")]
    live_tail: Option<LiveFrontierTail>,
    // Writer drops before the directory lock. The experimental feature strictly
    // joins its native workers; shutdown can wait but cannot release a live path.
    writer: Option<WalWriter>,
    references: ReferenceDirectoryWriter,
    appended_end: u64,
    durable_end: u64,
    // Proof owned by this append-only handle, never a highest allocated file.
    // A new file requires a new namespace barrier. Rooted history permits
    // sealed whole-file retirement, never physical address reuse.
    namespace_synced_file: Option<crate::wal::position::WalFileId>,
    poison: Option<WriterPoison>,
    sealed: bool,
    stats: PartitionStats,
}

/// Counters for this open handle, not recovered durability or publication proof.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PartitionStats {
    pub appended_groups: u64,
    /// Completed explicit per-group file persistence calls. Creation and sealing
    /// also perform I/O, and background native syncs are not counted here.
    pub persistence_barriers: u64,
    /// Successful explicit WAL-directory syncs on first use of each file.
    /// Creation, sealing and recovery namespace I/O are excluded.
    pub namespace_syncs: u64,
    /// Completed immutable per-used-file inventory witness file+namespace pairs.
    /// Separate from native WAL barriers; first-file ACKs pay this extra cost.
    pub inventory_witnesses: u64,
    /// Successful live-frontier file and directory syncs, including attempts
    /// whose subsequent publication failed; creation/recovery are excluded.
    pub live_frontier_file_syncs: u64,
    pub live_frontier_namespace_syncs: u64,
    /// Currently charged resident/cache/read-CQ descriptor records, not disk total.
    pub registered_records: usize,
    pub reference_metadata_bytes: usize,
}

/// Opt-in diagnostic timings. Ordinary reads do not take these timestamps.
#[derive(Clone, Copy, Debug, Default)]
pub struct RecordReadProfile {
    pub membership_ns: u64,
    pub wal_range_ns: u64,
    pub verification_decode_ns: u64,
}

/// Snapshot under the existing serialized persistence state lock, not an ACK.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PersistenceCapture {
    pub durable_end: u64,
    pub captured_appended_end: u64,
    pub cumulative_appended_groups: u64,
}

/// Opt-in local monotonic elapsed sums, not CPU, device-only or network time.
/// Reset by assigning `Default::default()`; `add` saturates every counter.
/// Stage sums are disjoint subsets of their operation's total, including errors
/// and unwinding. Totals nest inside caller service times and may overlap work on
/// other partitions. Ordinary writes take no diagnostic timestamps.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct StorageWriteProfile {
    /// Append invocations, including retries, rejected requests and unwinding.
    pub append_calls: u64,
    /// Append invocations that did not return success (including unwinding).
    pub append_errors: u64,
    /// Successful all-existing append invocations; no native append occurred.
    pub append_retries: u64,
    /// Newly appended and reference-published complete groups.
    pub appended_groups: u64,
    /// Receipts returned by successful appends, including exact retries.
    pub appended_receipts: u64,
    /// Entire append implementation, including lock waits and failed stages.
    pub append_total_ns: u64,
    /// Time acquiring the append state mutex, excluding validation.
    pub append_state_lock_wait_ns: u64,
    /// Inclusive sum of the five exclusive validation/preparation stages below.
    pub append_validation_ns: u64,
    /// Epoch, state, group/schema and per-batch admission validation.
    #[serde(default)]
    pub append_basic_validation_ns: u64,
    /// Receipt and version lookups, conflict checks and exact-retry handling.
    #[serde(default)]
    pub append_conflict_lookup_ns: u64,
    /// Identity/version lookups, one per batch checked by an append.
    #[serde(default)]
    pub append_conflict_lookups: u64,
    /// Pending-table slots examined by those lookups. Before the table existed
    /// each lookup scanned every pending entry; this is the replacement length.
    #[serde(default)]
    pub append_conflict_pending_probes: u64,
    /// Rooted on-disk index reads performed by those lookups.
    #[serde(default)]
    pub append_conflict_history_lookups: u64,
    /// Rooted index reads avoided by the exact never-forgets key filter.
    #[serde(default)]
    pub append_conflict_filter_skips: u64,
    /// Receipt/descriptor counts, fresh capacity and WAL frame geometry.
    #[serde(default)]
    pub append_geometry_capacity_ns: u64,
    /// Resident metadata capacity checks and credit reservation, including
    /// maintenance initiation on a reservation failure.
    #[serde(default)]
    pub append_metadata_reservation_ns: u64,
    /// Descriptor copies and prepared native-write construction.
    #[serde(default)]
    pub append_descriptor_preparation_ns: u64,
    /// Bounded completed-authority page/index/root spill before a new WAL group.
    pub append_history_ns: u64,
    /// Native writer call through guard drop/storage copy; includes native locks,
    /// allocation and rollover, not just memory copying or device time.
    pub append_wal_ns: u64,
    /// Receipt/map/reference-directory publication after native guard drop.
    pub append_publication_ns: u64,
    /// Persistence invocations, including empty input and durable retries.
    pub persist_calls: u64,
    /// Persistence invocations not returning success (including unwinding).
    pub persist_errors: u64,
    /// Successful invocations requiring no barrier, including empty input.
    pub persist_noops: u64,
    /// Input receipts on successful persistence, including duplicate retries.
    pub persisted_receipts: u64,
    /// Persistence implementation, excluding returned DurableBatch vector creation.
    pub persist_total_ns: u64,
    /// Acquisition of the serialization mutex shared with sealing.
    pub persist_serialization_lock_wait_ns: u64,
    /// All persistence state-mutex acquisitions: initial check, recheck and
    /// post-I/O publication. Excluded from validation/publication spans below.
    pub persist_state_lock_wait_ns: u64,
    /// Receipt/state validation, summed across initial and serialized recheck.
    pub persist_validation_ns: u64,
    /// Actual native WAL file-persistence call, including its internal lookup,
    /// locking and file sync. Not an isolated device-latency measurement.
    pub persist_wal_file_ns: u64,
    /// Actual WAL-directory persistence call (or injected namespace failure).
    pub persist_namespace_ns: u64,
    /// New-file witness creation/write/file-sync span, not a per-Append mean.
    pub persist_inventory_file_ns: u64,
    /// Witness directory-sync span, separate from the native WAL namespace.
    pub persist_inventory_namespace_ns: u64,
    pub inventory_file_persist_calls: u64,
    pub inventory_file_persist_successes: u64,
    pub inventory_namespace_persist_calls: u64,
    pub inventory_namespace_persist_successes: u64,
    /// Live durable-frontier root file and directory barriers, separate from WAL.
    #[serde(default)]
    pub live_frontier_file_ns: u64,
    #[serde(default)]
    pub live_frontier_namespace_ns: u64,
    #[serde(default)]
    pub live_frontier_file_calls: u64,
    #[serde(default)]
    pub live_frontier_file_successes: u64,
    #[serde(default)]
    pub live_frontier_namespace_calls: u64,
    #[serde(default)]
    pub live_frontier_namespace_successes: u64,
    /// Post-I/O error/poison handling, counters and durable-frontier publication.
    pub persist_publication_ns: u64,
    /// Actual native WAL file-persistence calls attempted.
    pub file_persist_calls: u64,
    /// File calls returning success, even if later namespace/publication fails.
    pub file_persist_successes: u64,
    /// Required namespace barriers attempted (including injected failures).
    pub namespace_persist_calls: u64,
    /// Namespace calls returning success; does not alone prove publication.
    pub namespace_persist_successes: u64,
    /// Successful durable-frontier publications after all required barriers.
    pub durable_publications: u64,
    /// Persistence invocations that ran a WAL barrier, and the receipts those
    /// invocations covered together (receipts per sync = second / first).
    #[serde(default)]
    pub persist_barriers: u64,
    #[serde(default)]
    pub persist_barrier_receipts: u64,
    /// Flusher batches that ran one barrier, and the queued append commands they
    /// completed under it (queued commands per barrier = second / first). Set
    /// only by the owner flusher; direct storage calls leave both zero.
    #[serde(default)]
    pub flush_batches: u64,
    #[serde(default)]
    pub flush_batch_commands: u64,
    /// Present only for a non-noop single persist invocation; aggregates omit it.
    pub persistence_capture: Option<PersistenceCapture>,
}

impl StorageWriteProfile {
    pub fn add(&mut self, other: Self) {
        macro_rules! add_fields {
            ($($field:ident),+ $(,)?) => {
                $(self.$field = self.$field.saturating_add(other.$field);)+
            };
        }
        add_fields!(
            append_calls,
            append_errors,
            append_retries,
            appended_groups,
            appended_receipts,
            append_total_ns,
            append_state_lock_wait_ns,
            append_validation_ns,
            append_basic_validation_ns,
            append_conflict_lookup_ns,
            append_conflict_lookups,
            append_conflict_pending_probes,
            append_conflict_history_lookups,
            append_conflict_filter_skips,
            append_geometry_capacity_ns,
            append_metadata_reservation_ns,
            append_descriptor_preparation_ns,
            append_history_ns,
            append_wal_ns,
            append_publication_ns,
            persist_calls,
            persist_errors,
            persist_noops,
            persisted_receipts,
            persist_total_ns,
            persist_serialization_lock_wait_ns,
            persist_state_lock_wait_ns,
            persist_validation_ns,
            persist_wal_file_ns,
            persist_namespace_ns,
            persist_inventory_file_ns,
            persist_inventory_namespace_ns,
            inventory_file_persist_calls,
            inventory_file_persist_successes,
            inventory_namespace_persist_calls,
            inventory_namespace_persist_successes,
            live_frontier_file_ns,
            live_frontier_namespace_ns,
            live_frontier_file_calls,
            live_frontier_file_successes,
            live_frontier_namespace_calls,
            live_frontier_namespace_successes,
            persist_publication_ns,
            file_persist_calls,
            file_persist_successes,
            namespace_persist_calls,
            namespace_persist_successes,
            durable_publications,
            persist_barriers,
            persist_barrier_receipts,
            flush_batches,
            flush_batch_commands,
        );
        self.persistence_capture = None;
    }

    fn add_lookup(&mut self, work: LookupWork) {
        self.append_conflict_lookups = self.append_conflict_lookups.saturating_add(1);
        self.append_conflict_pending_probes = self
            .append_conflict_pending_probes
            .saturating_add(work.pending_probes);
        self.append_conflict_history_lookups = self
            .append_conflict_history_lookups
            .saturating_add(work.history_lookups);
        self.append_conflict_filter_skips = self
            .append_conflict_filter_skips
            .saturating_add(work.filter_skips);
    }
}

/// A stage selector avoids borrowing individual profile fields across storage
/// operations. Drop closes the last stage on every return and during unwinding.
#[derive(Clone, Copy)]
enum WriteStage {
    AppendLock,
    AppendBasicValidation,
    AppendConflictLookup,
    AppendGeometryCapacity,
    AppendMetadataReservation,
    AppendDescriptorPreparation,
    AppendHistory,
    AppendWal,
    AppendPublication,
    PersistSerializationLock,
    PersistStateLock,
    PersistValidation,
    PersistFile,
    PersistNamespace,
    PersistInventoryFile,
    PersistInventoryNamespace,
    #[cfg(feature = "experimental-nvmeof")]
    LiveFrontierFile,
    #[cfg(feature = "experimental-nvmeof")]
    LiveFrontierNamespace,
    PersistPublication,
}

impl WriteStage {
    fn elapsed(self, profile: &mut StorageWriteProfile) -> &mut u64 {
        match self {
            Self::AppendLock => &mut profile.append_state_lock_wait_ns,
            Self::AppendBasicValidation => &mut profile.append_basic_validation_ns,
            Self::AppendConflictLookup => &mut profile.append_conflict_lookup_ns,
            Self::AppendGeometryCapacity => &mut profile.append_geometry_capacity_ns,
            Self::AppendMetadataReservation => &mut profile.append_metadata_reservation_ns,
            Self::AppendDescriptorPreparation => &mut profile.append_descriptor_preparation_ns,
            Self::AppendHistory => &mut profile.append_history_ns,
            Self::AppendWal => &mut profile.append_wal_ns,
            Self::AppendPublication => &mut profile.append_publication_ns,
            Self::PersistSerializationLock => &mut profile.persist_serialization_lock_wait_ns,
            Self::PersistStateLock => &mut profile.persist_state_lock_wait_ns,
            Self::PersistValidation => &mut profile.persist_validation_ns,
            Self::PersistFile => &mut profile.persist_wal_file_ns,
            Self::PersistNamespace => &mut profile.persist_namespace_ns,
            Self::PersistInventoryFile => &mut profile.persist_inventory_file_ns,
            Self::PersistInventoryNamespace => &mut profile.persist_inventory_namespace_ns,
            #[cfg(feature = "experimental-nvmeof")]
            Self::LiveFrontierFile => &mut profile.live_frontier_file_ns,
            #[cfg(feature = "experimental-nvmeof")]
            Self::LiveFrontierNamespace => &mut profile.live_frontier_namespace_ns,
            Self::PersistPublication => &mut profile.persist_publication_ns,
        }
    }
}

struct WriteTiming<'a> {
    profile: Option<&'a mut StorageWriteProfile>,
    started: Option<Instant>,
    stage_started: Option<Instant>,
    stage: Option<WriteStage>,
    append: bool,
    success: bool,
}

impl<'a> WriteTiming<'a> {
    fn new(profile: Option<&'a mut StorageWriteProfile>, append: bool) -> Self {
        let started = profile.as_ref().map(|_| Instant::now());
        let mut timing = Self {
            profile,
            started,
            stage_started: started,
            stage: None,
            append,
            success: false,
        };
        timing.record(|profile| {
            let calls = if append {
                &mut profile.append_calls
            } else {
                &mut profile.persist_calls
            };
            *calls = calls.saturating_add(1);
        });
        timing
    }

    fn record(&mut self, update: impl FnOnce(&mut StorageWriteProfile)) {
        if let Some(profile) = self.profile.as_deref_mut() {
            update(profile);
        }
    }

    fn stage(&mut self, next: Option<WriteStage>) {
        if let Some(profile) = self.profile.as_deref_mut() {
            let now = Instant::now();
            if let (Some(stage), Some(started)) = (self.stage, self.stage_started) {
                let ns = duration_ns(now.duration_since(started));
                let elapsed = stage.elapsed(profile);
                *elapsed = elapsed.saturating_add(ns);
                if matches!(
                    stage,
                    WriteStage::AppendBasicValidation
                        | WriteStage::AppendConflictLookup
                        | WriteStage::AppendGeometryCapacity
                        | WriteStage::AppendMetadataReservation
                        | WriteStage::AppendDescriptorPreparation
                ) {
                    profile.append_validation_ns = profile.append_validation_ns.saturating_add(ns);
                }
            }
            self.stage_started = Some(now);
            self.stage = next;
        }
    }

    fn success(&mut self, receipts: usize) {
        self.success = true;
        let append = self.append;
        self.record(|profile| {
            let count = if append {
                &mut profile.appended_receipts
            } else {
                &mut profile.persisted_receipts
            };
            *count = count.saturating_add(receipts as u64);
        });
    }
}

impl Drop for WriteTiming<'_> {
    fn drop(&mut self) {
        self.stage(None);
        if let (Some(profile), Some(started)) = (self.profile.as_deref_mut(), self.started) {
            let (total, errors) = if self.append {
                (&mut profile.append_total_ns, &mut profile.append_errors)
            } else {
                (&mut profile.persist_total_ns, &mut profile.persist_errors)
            };
            *total = total.saturating_add(duration_ns(started.elapsed()));
            *errors = errors.saturating_add(u64::from(!self.success));
        }
    }
}

fn duration_ns(duration: Duration) -> u64 {
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
}

/// A local storage node component; caller threads serialize appends per partition.
/// Different instances/partitions may append concurrently.
pub struct PartitionLog {
    state: Mutex<State>,
    references: Arc<ReferenceDirectory>,
    durable_frontier: AtomicU64,
    // Appended/synced frontier of the durability layer. Set once by the node's
    // control owner, which also owns the single syncer thread. Appends publish
    // into it under `state`, so the frontier follows WAL order exactly.
    frontier: std::sync::OnceLock<Arc<super::durability::frontier::Frontier>>,
    // Stage timings of background syncs, taken by whoever waits for them.
    sync_profile: Mutex<StorageWriteProfile>,
    // Native tier: readers may use groups that are appended but not yet synced.
    serve_unsynced: AtomicBool,
    worker_claimed: AtomicBool,
    // Serialize disk barriers/sealing without blocking immutable reference reads.
    persistence: Mutex<()>,
    // New file pins serialize only with retirement, never ordinary WAL fsync.
    // False permanently closes read admission after an interrupted retirement.
    read_admission: parking_lot::RwLock<bool>,
    #[cfg(feature = "experimental-nvmeof")]
    live_read_pins: std::sync::atomic::AtomicUsize,
    #[cfg(feature = "experimental-nvmeof")]
    live_stream: bool,
    #[cfg(test)]
    before_authority_load: Mutex<
        Option<(
            std::sync::mpsc::SyncSender<()>,
            std::sync::mpsc::Receiver<()>,
        )>,
    >,
    #[cfg(test)]
    fail_inventory_witness: AtomicU8,
    #[cfg(test)]
    before_persist: Mutex<
        Option<(
            std::sync::mpsc::SyncSender<()>,
            std::sync::mpsc::Receiver<()>,
        )>,
    >,
    #[cfg(test)]
    fail_persist: AtomicBool,
    #[cfg(test)]
    fail_namespace_persist: AtomicBool,
    #[cfg(all(test, feature = "experimental-nvmeof"))]
    fail_live_frontier: AtomicU8,
    wal: Arc<Wal>,
    registration: Registration,
    shape: KeyShape,
    path: PathBuf,
    device: u64,
    _lock: DbLock,
    // A local database root cannot reopen while any partition/worker handle
    // survives. Drop this after the partition lock and all native WAL workers.
    _local_authority: Option<Arc<DbLock>>,
}

#[derive(Clone)]
pub(crate) struct RecordReadPlan {
    pub(crate) member: crate::wal::WalMemberReadPlan,
    pub(crate) device: u64,
    // Descriptor authority survives cache eviction through completion retirement.
    _authority: Arc<RegisteredBatch>,
    // Holds both immutable segment ownership and the directory authority lock.
    pub(crate) _log: Arc<PartitionLog>,
}

impl RecordReadPlan {
    #[cfg(test)]
    pub(crate) fn authority_bytes(&self) -> usize {
        self._authority.authority_bytes()
    }
}

impl PartitionLog {
    /// Check the durable live-stream selection without opening any WAL file.
    pub fn is_live_path(path: &Path) -> Result<bool, StorageError> {
        let marker = path.join(LIVE_REGISTRATION);
        let metadata = match fs::symlink_metadata(&marker) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return match fs::symlink_metadata(path.join(LIVE_FRONTIER)) {
                    Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
                    Err(error) => Err(error.into()),
                    Ok(_) => Err(StorageError::Invalid(
                        "live frontier lacks its stream registration",
                    )),
                };
            }
            Err(error) => return Err(error.into()),
        };
        if !metadata.file_type().is_file()
            || metadata.len() != b"live-nvmeof-v1\n".len() as u64
            || fs::read(marker)? != b"live-nvmeof-v1\n"
        {
            return Err(StorageError::Invalid("invalid live WAL registration"));
        }
        Ok(true)
    }

    /// Create a new, explicitly named epoch directory. Never opens/replaces an
    /// existing directory. Parent must already exist. Registration is durable
    /// before this method returns and before any append is possible.
    pub fn create(
        path: &Path,
        config: PartitionConfig,
        shape: &KeyShape,
        metrics: Arc<Metrics>,
    ) -> Result<Self, StorageError> {
        config.validate()?;
        let shape_yaml = shape
            .to_yaml()
            .map_err(|_| StorageError::Invalid("invalid key shape"))?;
        let registration = Registration {
            format: 5,
            config,
            shape_yaml,
        };
        // Deep copy canonical schema so callers cannot change a shared destroyed
        // flag after storage admission or mutate authority-eligible semantics.
        let shape = KeyShape::from_yaml(&registration.shape_yaml)
            .map_err(|_| StorageError::Invalid("invalid key shape"))?;
        let metadata = encode_metadata(&registration)?;
        fs::create_dir(path)?;
        let path = path.canonicalize()?;
        File::open(
            path.parent()
                .ok_or(StorageError::Invalid("missing parent"))?,
        )?
        .sync_all()?;
        let lock = DbLock::acquire(&path, Duration::ZERO)?;
        let (references, reference_writer) =
            ReferenceDirectory::open(&path, &registration.config, true)?;
        let wal = Wal::open(&path, registration.config.layout(), metrics)?;
        wal.persist_registered_files()?;
        install_metadata(&path, REGISTRATION, &metadata)?;
        let writer = wal.writer_after(None).map_err(StorageError::Wal)?;
        Ok(Self {
            state: Mutex::new(State {
                #[cfg(feature = "experimental-nvmeof")]
                pending_live: None,
                #[cfg(feature = "experimental-nvmeof")]
                live: false,
                #[cfg(feature = "experimental-nvmeof")]
                last_live_complete: None,
                #[cfg(feature = "experimental-nvmeof")]
                live_tail: None,
                writer: Some(writer),
                references: reference_writer,
                appended_end: 0,
                durable_end: 0,
                namespace_synced_file: None,
                poison: None,
                sealed: false,
                stats: PartitionStats::default(),
            }),
            references,
            durable_frontier: AtomicU64::new(0),
            frontier: std::sync::OnceLock::new(),
            sync_profile: Mutex::new(StorageWriteProfile::default()),
            serve_unsynced: AtomicBool::new(false),
            worker_claimed: AtomicBool::new(false),
            persistence: Mutex::new(()),
            read_admission: parking_lot::RwLock::new(true),
            #[cfg(feature = "experimental-nvmeof")]
            live_read_pins: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(feature = "experimental-nvmeof")]
            live_stream: false,
            #[cfg(test)]
            before_authority_load: Mutex::new(None),
            #[cfg(test)]
            fail_inventory_witness: AtomicU8::new(0),
            #[cfg(test)]
            before_persist: Mutex::new(None),
            #[cfg(test)]
            fail_persist: AtomicBool::new(false),
            #[cfg(test)]
            fail_namespace_persist: AtomicBool::new(false),
            #[cfg(all(test, feature = "experimental-nvmeof"))]
            fail_live_frontier: AtomicU8::new(0),
            wal,
            registration,
            shape,
            device: fs::metadata(&path)?.dev(),
            path,
            _lock: lock,
            _local_authority: None,
        })
    }

    /// Create an exclusively block-aligned external-write stream. Selection is
    /// persisted before the handle can be exposed to a target manager.
    #[cfg(feature = "experimental-nvmeof")]
    pub fn create_live(
        path: &Path,
        config: PartitionConfig,
        shape: &KeyShape,
        metrics: Arc<Metrics>,
    ) -> Result<Self, StorageError> {
        let max_frame = config
            .group_limits
            .max_encoded_bytes
            .checked_add(8 + 4095)
            .map(|n| n & !4095)
            .ok_or(StorageError::Capacity)?;
        if !config.fragment_bytes.is_multiple_of(4096)
            || !config.wal_file_bytes.is_multiple_of(4096)
            || config.wal_file_bytes > i64::MAX as u64
            || max_frame >= (1 << 30)
            || max_frame as u64 > config.fragment_bytes
        {
            return Err(StorageError::Invalid("invalid live WAL geometry"));
        }
        let mut log = Self::create(path, config, shape, metrics)?;
        install_metadata(
            &log.path,
            LIVE_FRONTIER,
            &encode_metadata(&LiveFrontier::new(log.config()))?,
        )?;
        install_metadata(&log.path, LIVE_REGISTRATION, b"live-nvmeof-v1\n")?;
        log.state.lock().live = true;
        log.live_stream = true;
        Ok(log)
    }

    /// The callback must synchronously revoke and drain all writable targets
    /// for this stream before recovery touches any WAL inode.
    #[cfg(feature = "experimental-nvmeof")]
    pub fn recover_live_sealed(
        path: &Path,
        expected: &PartitionConfig,
        metrics: Arc<Metrics>,
        revoke_and_drain: impl FnOnce() -> Result<(), StorageError>,
    ) -> Result<Self, StorageError> {
        revoke_and_drain()?;
        Self::recover_sealed_impl(path, expected, metrics, true)
    }

    /// Recover surviving complete frames and permanently seal this old epoch.
    /// The directory lock excludes local writers only; callers must not equate
    /// it with cluster fencing. An unavailable partition cannot be omitted from
    /// database recovery; use `recover_inventory` for the complete static set.
    pub fn recover_sealed(
        path: &Path,
        expected: &PartitionConfig,
        metrics: Arc<Metrics>,
    ) -> Result<Self, StorageError> {
        Self::recover_sealed_impl(path, expected, metrics, false)
    }

    fn recover_sealed_impl(
        path: &Path,
        expected: &PartitionConfig,
        metrics: Arc<Metrics>,
        live_revoked: bool,
    ) -> Result<Self, StorageError> {
        let live = Self::is_live_path(path)?;
        if live && !live_revoked {
            return Err(StorageError::Invalid(
                "live WAL recovery requires target revocation",
            ));
        }
        if live_revoked && !live {
            return Err(StorageError::Invalid("not a live WAL stream"));
        }
        expected.validate()?;
        let path = path.canonicalize()?;
        let lock = DbLock::acquire(&path, Duration::ZERO)?;
        let registration_path = path.join(REGISTRATION);
        if !registration_path.exists() && path.join(OLD_REGISTRATION).exists() {
            return Err(StorageError::Invalid(
                "obsolete partition registration format; recreate the experimental database",
            ));
        }
        let registration: Registration = read_metadata(&registration_path)?;
        if registration.format != 5 || registration.config != *expected {
            return Err(StorageError::Invalid(
                "registration differs from expected inventory/configuration",
            ));
        }
        let shape = KeyShape::from_yaml(&registration.shape_yaml)
            .map_err(|_| StorageError::Invalid("invalid stored key shape"))?;
        let (references, mut reference_writer) = ReferenceDirectory::open(&path, expected, false)?;
        let root = references.root();
        let scan_end = validate_file_inventory(&path, expected, &references)?.max(root.end);
        let frontier = if live {
            let frontier: LiveFrontier = read_metadata(&path.join(LIVE_FRONTIER))?;
            let identity = LiveFrontier::new(expected);
            if frontier.database != identity.database
                || frontier.partition != identity.partition
                || frontier.epoch != identity.epoch
                || frontier.generation != identity.generation
                || frontier.end > scan_end
                || frontier.end < root.end
                || (frontier.end == 0) != frontier.tail.is_none()
                || !frontier.end.is_multiple_of(4096)
            {
                return Err(StorageError::Invalid("invalid live durable frontier"));
            }
            if let Some(tail) = &frontier.tail
                && (!tail.offset.is_multiple_of(4096)
                    || tail.offset >= frontier.end
                    || tail.id.database != expected.inventory.database
                    || tail.version.epoch != expected.inventory.epoch)
            {
                return Err(StorageError::Invalid("invalid live durable frontier tail"));
            }
            Some(frontier)
        } else {
            None
        };
        file_inventory::validate(&path, expected, root.file_count)?;
        let wal = Wal::open_retained(&path, expected.layout(), metrics)?;
        // Complete a root-authorized unlink interrupted by a previous process.
        // These bounded pages contain only file IDs, never retained value data.
        for _ in 0..=root.retired_count.div_ceil(256) {
            let mut retired = Vec::with_capacity(256);
            for entry in fs::read_dir(&path)? {
                let entry = entry?;
                let Some((id, _)) = wal_file_entry(&entry, expected)? else {
                    continue;
                };
                if references.is_retired(id * expected.wal_file_bytes)? {
                    retired.push(crate::wal::position::WalFileId::from_u64(id));
                    if retired.len() == 256 {
                        break;
                    }
                }
            }
            if retired.is_empty() {
                break;
            }
            retired.sort_unstable();
            wal.retire_files(&retired)?;
        }
        let mut physical_end = root.end;
        // Scan each known fragment independently. A crash can persist a complete
        // frame in the next fragment without persisting the preceding skip marker.
        // A complete frame never crosses a fragment boundary; this does not scan
        // arbitrary user bytes for magic. The pending-durability gate rules out
        // later authority records behind a torn batch within the same fragment.
        let mut start = if live {
            0
        } else {
            root.end / expected.fragment_bytes * expected.fragment_bytes
        };
        // Below `strict_end` every frame was acknowledged durable: any defect is
        // fatal. Live frames beyond it were appended (and, under the native tier,
        // acknowledged) but not yet synced: they are adopted while complete and
        // valid, and the first missing or invalid frame ends the scan quietly.
        let strict_end = frontier.as_ref().map_or(scan_end, |frontier| frontier.end);
        let recovery_end = if live { scan_end } else { strict_end };
        let mut tail_seen = false;
        let mut stopped = false;
        while start < recovery_end && !stopped {
            if references.is_retired(start / expected.wal_file_bytes * expected.wal_file_bytes)? {
                start = (start / expected.wal_file_bytes + 1) * expected.wal_file_bytes;
                continue;
            }
            let scan_position =
                if !live && start == root.end / expected.fragment_bytes * expected.fragment_bytes {
                    root.end
                        .checked_add(7)
                        .map(|end| end & !7)
                        .ok_or(StorageError::Capacity)?
                } else {
                    start
                };
            let mut scan = match wal.wal_iterator_for_scan(scan_position) {
                Ok(scan) => scan,
                Err(WalError::EndOfWal) if live && scan_position < strict_end => {
                    return Err(StorageError::Invalid("missing durable live WAL fragment"));
                }
                // Beyond the synced frontier a missing fragment is the end of the WAL.
                Err(WalError::EndOfWal) if live => break,
                Err(WalError::EndOfWal) => {
                    start = start
                        .checked_add(expected.fragment_bytes)
                        .ok_or(StorageError::Capacity)?;
                    continue;
                }
                Err(error) => return Err(StorageError::Wal(error)),
            };
            #[cfg(feature = "experimental-nvmeof")]
            let live_file = if live {
                let file_id =
                    crate::wal::position::WalFileId::from_u64(start / expected.wal_file_bytes);
                Some(File::open(expected.layout().wal_file_name(&path, file_id))?)
            } else {
                None
            };
            let mut cursor = scan_position;
            'frames: loop {
                if live && cursor >= recovery_end.min(start + expected.fragment_bytes) {
                    break;
                }
                #[cfg(feature = "experimental-nvmeof")]
                if let Some(file) = &live_file {
                    let mut header = [0; 8];
                    file.read_exact_at(&mut header, cursor % expected.wal_file_bytes)?;
                    if header == [0xff; 8] {
                        // A damaged acknowledged frame header can itself look
                        // like a native skip marker. Real live skips occupy an
                        // unused, zero-filled fragment suffix; check it before
                        // accepting the jump.
                        let mut at = cursor + 8;
                        let end = start + expected.fragment_bytes;
                        let mut buffer = [0u8; 65536];
                        while at < end {
                            let len = (end - at).min(buffer.len() as u64) as usize;
                            file.read_exact_at(&mut buffer[..len], at % expected.wal_file_bytes)?;
                            if buffer[..len].iter().any(|byte| *byte != 0) {
                                if cursor < strict_end {
                                    return Err(StorageError::Invalid(
                                        "corrupt live skip-marker suffix",
                                    ));
                                }
                                stopped = true;
                                break 'frames;
                            }
                            at += len as u64;
                        }
                        break;
                    }
                }
                let (position, bytes) = match scan.next() {
                    Ok(record) => record,
                    // Below the synced frontier every frame was acknowledged durable:
                    // a defect is corruption. Beyond it the first missing or invalid
                    // frame is the end of the WAL, exactly as in native replay.
                    Err(WalError::Crc(_) | WalError::EndOfWal) if live && cursor < strict_end => {
                        return Err(StorageError::Invalid("corrupt durable live WAL frame"));
                    }
                    Err(WalError::Crc(_) | WalError::EndOfWal) if live => {
                        stopped = true;
                        break 'frames;
                    }
                    Err(WalError::Crc(_) | WalError::EndOfWal) => break,
                    Err(error) => return Err(StorageError::Wal(error)),
                };
                if position.offset() >= start + expected.fragment_bytes {
                    break;
                }
                let frame_end = position
                    .offset()
                    .checked_add(position.frame_len() as u64)
                    .ok_or(StorageError::Capacity)?;
                if live && position.offset() < strict_end && frame_end > strict_end {
                    return Err(StorageError::Invalid("live frame crosses durable frontier"));
                }
                cursor = frame_end;
                if live
                    && (position.offset() % 4096 != 0
                        || position.frame_len() % 4096 != 0
                        || bytes.len() < 12
                        || bytes[8..12] != [0, 3, 0, 12])
                {
                    if position.offset() >= strict_end {
                        stopped = true;
                        break 'frames;
                    }
                    return Err(StorageError::Invalid(
                        "invalid live recovery frame geometry",
                    ));
                }
                let group = match CompleteGroup::decode(
                    &shape,
                    bytes.clone(),
                    expected.batch_limits,
                    expected.group_limits,
                ) {
                    Ok(group) => group,
                    Err(_) if live && position.offset() >= strict_end => {
                        stopped = true;
                        break 'frames;
                    }
                    Err(error) => return Err(error.into()),
                };
                if let Some(frontier) = &frontier
                    && frame_end == frontier.end
                {
                    let tail = frontier
                        .tail
                        .as_ref()
                        .ok_or(StorageError::Invalid("missing frontier tail"))?;
                    let last = group
                        .batches()
                        .last()
                        .ok_or(StorageError::Invalid("empty live group"))?;
                    if tail.offset != position.offset()
                        || tail.crc != crc32fast::hash(&bytes)
                        || tail.id != last.id()
                        || tail.version != last.version()
                        || tail.digest != last.digest()
                    {
                        return Err(StorageError::Invalid("live frontier tail mismatch"));
                    }
                    tail_seen = true;
                }
                if position.offset() < root.end {
                    if frame_end > root.end {
                        return Err(StorageError::Invalid("history root splits WAL group"));
                    }
                    continue;
                }
                if live
                    && position.offset() >= strict_end
                    && group.batches().iter().any(|batch| {
                        validate_batch(expected, batch).is_err()
                            || references
                                .lookup_id(batch.id())
                                .map_or(true, |found| found.is_some())
                            || references
                                .lookup_version(batch.version())
                                .map_or(true, |found| found.is_some())
                    })
                {
                    // An invalid or duplicate frame beyond the synced frontier ends
                    // the replay; it was never acknowledged durable.
                    stopped = true;
                    break 'frames;
                }
                physical_end = position.offset() + position.frame_len() as u64;
                let mut group_metadata = Vec::with_capacity(group.batches().len());
                for (batch, offset) in group.batches().iter().zip(group.batch_offsets()) {
                    validate_batch(expected, batch)?;
                    let receipt =
                        receipt_for(expected, batch, member_position(position, *offset, batch));
                    group_metadata.push(RegisteredBatch::new(
                        batch.id(),
                        batch.version(),
                        batch.digest(),
                        receipt.address,
                        batch.record_descriptors().into(),
                        physical_end,
                    ));
                    if references.lookup_id(batch.id())?.is_some()
                        || references.lookup_version(batch.version())?.is_some()
                    {
                        return Err(StorageError::Invalid(
                            "duplicate identity or version in replay suffix",
                        ));
                    }
                }
                reference_writer.publish_group(group_metadata)?;
                // Recovery persists each complete group before evicting its
                // descriptor authority. A crash resumes from the last root.
                wal.persist_registered_files()?;
                // A frame adopted beyond the old frontier root must be covered by a
                // new root (WAL barrier first) BEFORE the history root advances past
                // it: a reopen requires `frontier.end >= history root.end`, and a
                // crash between the two publications must leave exactly that.
                if live && position.offset() >= strict_end {
                    let last = group
                        .batches()
                        .last()
                        .ok_or(StorageError::Invalid("empty live group"))?;
                    let mut adopted = LiveFrontier::new(expected);
                    adopted.end = physical_end;
                    adopted.tail = Some(LiveFrontierTail {
                        offset: position.offset(),
                        crc: crc32fast::hash(&bytes),
                        id: last.id(),
                        version: last.version(),
                        digest: last.digest(),
                    });
                    install_metadata(&path, LIVE_FRONTIER, &encode_metadata(&adopted)?)?;
                }
                reference_writer.persist_pending(scan_end.div_ceil(expected.wal_file_bytes))?;
            }
            start = start
                .checked_add(expected.fragment_bytes)
                .ok_or(StorageError::Invalid("WAL scan offset overflow"))?;
        }
        if let Some(frontier) = &frontier
            && let Some(tail) = &frontier.tail
            && !tail_seen
            && !references
                .is_retired(tail.offset / expected.wal_file_bytes * expected.wal_file_bytes)?
        {
            return Err(StorageError::Invalid("missing live durable frontier tail"));
        }
        wal.persist_registered_files()?;
        reference_writer.persist_pending(scan_end.div_ceil(expected.wal_file_bytes))?;
        file_inventory::remove_covered(&path, references.root().file_count)?;
        let end = SealedEnd {
            format: 5,
            epoch: expected.inventory.epoch,
            generation: expected.inventory.segment_generation,
            frame_count: references.len(),
            physical_end,
        };
        if path.join(SEAL).exists() {
            let recorded: SealedEnd = read_metadata(&path.join(SEAL))?;
            if recorded != end {
                return Err(StorageError::Invalid(
                    "sealed inventory no longer matches surviving frames",
                ));
            }
        }
        // Persist surviving, possibly unacknowledged frames before declaring
        // their authority irrevocable in the sealed inventory.
        wal.persist_registered_files()?;
        install_metadata(&path, SEAL, &encode_metadata(&end)?)?;
        Ok(Self {
            state: Mutex::new(State {
                #[cfg(feature = "experimental-nvmeof")]
                pending_live: None,
                #[cfg(feature = "experimental-nvmeof")]
                live,
                #[cfg(feature = "experimental-nvmeof")]
                last_live_complete: None,
                #[cfg(feature = "experimental-nvmeof")]
                live_tail: None,
                writer: None,
                references: reference_writer,
                appended_end: physical_end,
                durable_end: physical_end,
                namespace_synced_file: None,
                poison: None,
                sealed: true,
                stats: PartitionStats::default(),
            }),
            references,
            durable_frontier: AtomicU64::new(physical_end),
            frontier: std::sync::OnceLock::new(),
            sync_profile: Mutex::new(StorageWriteProfile::default()),
            serve_unsynced: AtomicBool::new(false),
            worker_claimed: AtomicBool::new(false),
            persistence: Mutex::new(()),
            read_admission: parking_lot::RwLock::new(true),
            #[cfg(feature = "experimental-nvmeof")]
            live_read_pins: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(feature = "experimental-nvmeof")]
            live_stream: live,
            #[cfg(test)]
            before_authority_load: Mutex::new(None),
            #[cfg(test)]
            fail_inventory_witness: AtomicU8::new(0),
            #[cfg(test)]
            before_persist: Mutex::new(None),
            #[cfg(test)]
            fail_persist: AtomicBool::new(false),
            #[cfg(test)]
            fail_namespace_persist: AtomicBool::new(false),
            #[cfg(all(test, feature = "experimental-nvmeof"))]
            fail_live_frontier: AtomicU8::new(0),
            wal,
            registration,
            shape,
            device: fs::metadata(&path)?.dev(),
            path,
            _lock: lock,
            _local_authority: None,
        })
    }

    pub fn config(&self) -> &PartitionConfig {
        &self.registration.config
    }

    #[cfg(all(target_os = "linux", feature = "test-utils"))]
    pub(crate) fn read_evidence_for_benchmark(
        &self,
        evict: bool,
    ) -> std::io::Result<crate::read_probe::ReadEvidence> {
        #[cfg(feature = "experimental-nvmeof")]
        if self.live_stream && !self.state.lock().sealed {
            return Err(io::Error::other(
                "mapped residency probe unavailable during live writes",
            ));
        }
        self.wal.read_evidence_for_benchmark(evict)
    }

    /// Aggregate credits held by a sole database submitter, including queued,
    /// active and already registered batches. The submitter must own every
    /// producer for this log; this check alone is not a concurrent reservation.
    pub(crate) fn check_reference_admission(
        &self,
        total_batches: usize,
        total_records: usize,
    ) -> Result<(), StorageError> {
        let state = self.state.lock();
        if state.poison.is_some() || state.references.background_failed() {
            return Err(StorageError::Poisoned);
        }
        if state.sealed {
            return Err(StorageError::Sealed);
        }
        ReferenceDirectory::check_fresh_total_capacity(
            self.config().max_batches.min(1024),
            self.config().max_reference_metadata_bytes,
            total_batches,
            total_records,
        )?;
        Ok(())
    }

    pub(crate) fn retain_local_authority(mut self, lock: Arc<DbLock>) -> Self {
        self._local_authority = Some(lock);
        self
    }

    pub(crate) fn key_shape(&self) -> &KeyShape {
        &self.shape
    }

    /// Check local evidence for this exact immutable receipt without initiating
    /// I/O. A token from a different copy of the same configured stream must not
    /// promote this log's still-unpersisted group.
    pub(crate) fn validate_durable_receipt(
        &self,
        durable: DurableBatch,
    ) -> Result<(), StorageError> {
        if pending_persistence(&self.state.lock(), durable.appended())?.is_some() {
            return Err(StorageError::NeedsPersistence);
        }
        Ok(())
    }

    /// Whether monotonic allocation has permanently passed a live WAL file.
    /// The target owner must separately wait for its namespace/read I/O pins
    /// and synchronously revoke/drain before removing a binding.
    #[cfg(feature = "experimental-nvmeof")]
    pub fn can_release_live_segment(&self, file_index: u64) -> Result<bool, StorageError> {
        let state = self.state.lock();
        if !state.live {
            return Err(StorageError::Invalid("not a live WAL stream"));
        }
        if state.poison.is_some() || state.references.background_failed() {
            return Err(StorageError::Poisoned);
        }
        if state
            .pending_live
            .as_ref()
            .is_some_and(|pending| pending.reservation.segment.file_index == file_index)
        {
            return Ok(false);
        }
        Ok(match state.writer.as_ref() {
            Some(writer) => file_index < writer.position() / self.config().wal_file_bytes,
            None => state.sealed,
        })
    }

    /// Reserve the final native extent, or return the exact previously
    /// registered receipts. No payload bytes are sent to the owner here.
    #[cfg(feature = "experimental-nvmeof")]
    pub fn reserve_live_group(
        &self,
        authority: RequestAuthority,
        descriptor: super::nvmeof::live_storage::LiveGroupDescriptor,
    ) -> Result<super::nvmeof::live_storage::LiveReserveResult, StorageError> {
        use super::nvmeof::live_storage::{
            LiveReserveResult, LiveWalSegment, LiveWriteReservation,
        };
        let config = self.config();
        if authority.epoch != config.inventory.epoch {
            return Err(StorageError::Fenced);
        }
        let mut state = self.state.lock();
        if state.poison.is_some() || state.references.background_failed() {
            return Err(StorageError::Poisoned);
        }
        if state.sealed {
            return Err(StorageError::Sealed);
        }
        if !state.live {
            return Err(StorageError::Invalid("not a live WAL stream"));
        }
        if let Some(pending) = &state.pending_live {
            return if pending.descriptor == descriptor {
                Ok(LiveReserveResult::Reserved(pending.reservation.clone()))
            } else {
                Err(StorageError::Invalid(
                    "unknown live reservation blocks append",
                ))
            };
        }
        let count = descriptor.batches.len();
        if count == 0 || count > config.group_limits.max_batches as usize {
            return Err(StorageError::Capacity);
        }
        let mut ids = std::collections::BTreeSet::new();
        let mut versions = std::collections::BTreeSet::new();
        let mut previous = Vec::with_capacity(count);
        let mut payload_len = GROUP_HEADER_LEN;
        let mut descriptor_count = 0usize;
        for batch in &descriptor.batches {
            if batch.version.epoch != authority.epoch {
                return Err(StorageError::Fenced);
            }
            if config.inventory.partition_for(batch.id)? != config.partition {
                return Err(StorageError::WrongPartition);
            }
            if !ids.insert(batch.id) {
                return Err(StorageError::IdentityConflict);
            }
            if !versions.insert(batch.version) {
                return Err(StorageError::VersionConflict);
            }
            if batch.encoded_len == 0
                || batch.encoded_len as usize > config.batch_limits.max_encoded_bytes
                || batch.descriptor_count == 0
                || batch.descriptor_count > config.batch_limits.max_operations
            {
                return Err(StorageError::Capacity);
            }
            payload_len = payload_len
                .checked_add(CompleteGroup::encoded_member_len(
                    batch.encoded_len as usize,
                )?)
                .ok_or(StorageError::Capacity)?;
            descriptor_count = descriptor_count
                .checked_add(batch.descriptor_count as usize)
                .ok_or(StorageError::Capacity)?;
            if let Some(receipt) = self.references.lookup_id(batch.id)? {
                if receipt.version != batch.version || receipt.digest != batch.digest {
                    return Err(StorageError::IdentityConflict);
                }
                let registered = self
                    .references
                    .lookup(receipt.address)?
                    .ok_or(StorageError::Invalid("missing existing batch authority"))?;
                if receipt.address.len != batch.encoded_len + 8
                    || registered.descriptors().len() != batch.descriptor_count as usize
                {
                    return Err(StorageError::IdentityConflict);
                }
                previous.push(receipt);
            } else if self.references.lookup_version(batch.version)?.is_some() {
                return Err(StorageError::VersionConflict);
            }
        }
        let frame_len = payload_len
            .checked_add(8 + 4095)
            .map(|n| n & !4095)
            .ok_or(StorageError::Capacity)?;
        if frame_len != descriptor.frame_len as usize
            || frame_len > config.fragment_bytes as usize
            || frame_len > 64 * 1024 * 1024
            || payload_len > config.group_limits.max_encoded_bytes
        {
            return Err(StorageError::Capacity);
        }
        if previous.len() == count {
            // A retry must describe the same physical group, not merely a set
            // of individually existing batches. Bind order, offsets, length
            // and the original outer frame CRC before returning receipts.
            let start = previous[0]
                .address
                .offset
                .checked_sub((8 + GROUP_HEADER_LEN) as u64)
                .ok_or(StorageError::IdentityConflict)?;
            let end = start
                .checked_add(frame_len as u64)
                .ok_or(StorageError::Capacity)?;
            let mut member_offset = start + (8 + GROUP_HEADER_LEN) as u64;
            for (receipt, summary) in previous.iter().zip(&descriptor.batches) {
                let registered = self
                    .references
                    .lookup(receipt.address)?
                    .ok_or(StorageError::Invalid("missing existing batch authority"))?;
                if receipt.address.offset != member_offset || registered.group_end() != end {
                    return Err(StorageError::IdentityConflict);
                }
                member_offset = member_offset
                    .checked_add(
                        CompleteGroup::encoded_member_len(summary.encoded_len as usize)? as u64,
                    )
                    .ok_or(StorageError::Capacity)?;
            }
            let file_id = crate::wal::position::WalFileId::from_u64(start / config.wal_file_bytes);
            let file = File::open(config.layout().wal_file_name(&self.path, file_id))?;
            let mut header = [0; 8];
            file.read_exact_at(&mut header, start % config.wal_file_bytes)?;
            if u32::from_be_bytes(header[..4].try_into().unwrap()) as usize + 8 != frame_len
                || u32::from_be_bytes(header[4..].try_into().unwrap()) != descriptor.frame_crc
            {
                return Err(StorageError::IdentityConflict);
            }
            return Ok(LiveReserveResult::Existing(previous));
        }
        if !previous.is_empty() {
            return Err(StorageError::MixedRetry);
        }
        self.receipt_count()
            .checked_add(count)
            .ok_or(StorageError::Capacity)?;
        ReferenceDirectory::check_fresh_total_capacity(
            config.max_batches.min(1024),
            config.max_reference_metadata_bytes,
            count,
            descriptor_count,
        )?;
        let next = state
            .writer
            .as_ref()
            .ok_or(StorageError::Sealed)?
            .position();
        let remaining = config.fragment_bytes - next % config.fragment_bytes;
        let predicted_end = next
            .checked_add(if frame_len as u64 > remaining {
                remaining
            } else {
                0
            })
            .and_then(|n| n.checked_add(frame_len as u64))
            .ok_or(StorageError::Capacity)?;
        if state
            .references
            .check_capacity(count, descriptor_count)
            .is_err()
            || predicted_end.saturating_sub(self.references.root().end) > 64 * 1024 * 1024
        {
            if physical_end(&state) > state.durable_end {
                return Err(StorageError::NeedsPersistence);
            }
            let old_root = self.references.root();
            let durable_end = state.durable_end;
            let files = durable_end.saturating_sub(1) / config.wal_file_bytes + 1;
            if let Err(error) = state
                .references
                .start_background(durable_end, files, &self.path)
            {
                state.poison = Some(WriterPoison::Fatal);
                return Err(error);
            }
            drop(state);
            self.wait_history()?;
            let new_root = self.references.root();
            if new_root.count == old_root.count && new_root.end == old_root.end {
                return Err(StorageError::Capacity);
            }
            return Err(StorageError::NeedsPersistence);
        }
        let credit = state.references.reserve_group(count, descriptor_count)?;
        // From this point an allocation failure is an unknown effect.
        state.poison = Some(WriterPoison::Fatal);
        let allocation = state
            .writer
            .as_ref()
            .ok_or(StorageError::Sealed)?
            .reserve_external(descriptor.frame_len)
            .map_err(|e| StorageError::Unknown(WalError::Io(e)))?;
        let metadata = allocation
            .file
            .metadata()
            .map_err(|e| StorageError::Unknown(WalError::Io(e)))?;
        if metadata.len() != config.wal_file_bytes {
            return Err(StorageError::Unknown(WalError::Io(io::Error::other(
                "WAL file size changed",
            ))));
        }
        let reservation = LiveWriteReservation {
            token: rand::random(),
            segment: LiveWalSegment {
                database: config.inventory.database,
                partition: config.partition,
                epoch: config.inventory.epoch,
                generation: config.inventory.segment_generation,
                file_index: allocation.position.offset() / config.wal_file_bytes,
                path: allocation.path.clone(),
                bytes: metadata.len(),
                device: metadata.dev(),
                inode: metadata.ino(),
                block_size: 4096,
            },
            frame_offset: allocation.position.offset(),
            frame_len: descriptor.frame_len,
        };
        state.pending_live = Some(PendingLive {
            descriptor,
            reservation: reservation.clone(),
            allocation,
            credit,
        });
        state.poison = None;
        Ok(LiveReserveResult::Reserved(reservation))
    }

    /// Validate actual bytes in the granted inode, then publish their existing
    /// native positions. An unsuccessful completion remains unknown and seals
    /// this writer; persistence is established separately by persist_many.
    /// The caller must have retired the successful whole-frame write CQ and
    /// must not issue further writes to this reservation after completion.
    #[cfg(feature = "experimental-nvmeof")]
    pub fn complete_live_group(
        &self,
        authority: RequestAuthority,
        token: [u8; 16],
    ) -> Result<Vec<AppendedBatch>, StorageError> {
        self.complete_live_group_impl(authority, token, false)
            .map(|(receipts, _, _)| receipts)
    }

    /// Successful full-frame read and validation time, excluding state-lock
    /// acquisition and metadata publication. Exact retries return zero samples.
    #[cfg(feature = "experimental-nvmeof")]
    pub fn complete_live_group_profiled(
        &self,
        authority: RequestAuthority,
        token: [u8; 16],
    ) -> Result<(Vec<AppendedBatch>, u64, u64), StorageError> {
        self.complete_live_group_impl(authority, token, true)
    }

    #[cfg(feature = "experimental-nvmeof")]
    fn complete_live_group_impl(
        &self,
        authority: RequestAuthority,
        token: [u8; 16],
        profile: bool,
    ) -> Result<(Vec<AppendedBatch>, u64, u64), StorageError> {
        if authority.epoch != self.config().inventory.epoch {
            return Err(StorageError::Fenced);
        }
        let mut state = self.state.lock();
        if state.poison.is_some() || state.references.background_failed() {
            return Err(StorageError::Poisoned);
        }
        if let Some((completed_token, start, count)) = state.last_live_complete
            && completed_token == token
        {
            let mut receipts = Vec::with_capacity(count);
            while receipts.len() < count {
                let page = self
                    .references
                    .receipt_page(start + receipts.len(), (count - receipts.len()).min(256))?;
                if page.is_empty() {
                    return Err(StorageError::Invalid("completed live receipt disappeared"));
                }
                receipts.extend(page);
            }
            return Ok((receipts, 0, 0));
        }
        let pending = state
            .pending_live
            .as_ref()
            .ok_or(StorageError::Invalid("no live reservation"))?;
        if pending.reservation.token != token {
            return Err(StorageError::Invalid("foreign live reservation"));
        }
        state.poison = Some(WriterPoison::Fatal);
        let mut pending = state.pending_live.take().ok_or(StorageError::Poisoned)?;
        let first_ordinal = self.receipt_count();
        let result = (|| -> Result<(Vec<AppendedBatch>, u64), StorageError> {
            let started = profile.then(Instant::now);
            let mut frame = vec![0; pending.reservation.frame_len as usize];
            let offset = pending.reservation.frame_offset % pending.reservation.segment.bytes;
            pending.allocation.file.read_exact_at(&mut frame, offset)?;
            if u32::from_be_bytes(frame[0..4].try_into().unwrap()) as usize + 8 != frame.len()
                || u32::from_be_bytes(frame[4..8].try_into().unwrap())
                    != pending.descriptor.frame_crc
            {
                return Err(StorageError::IncompleteLiveFrame);
            }
            let payload = CrcFrame::read_from_slice(&frame, 0)
                .map_err(|_| StorageError::IncompleteLiveFrame)?;
            if payload.len() < 12 || payload[8..12] != [0, 3, 0, 12] {
                return Err(StorageError::IncompleteLiveFrame);
            }
            let group = CompleteGroup::decode(
                &self.shape,
                payload.to_vec().into(),
                self.config().batch_limits,
                self.config().group_limits,
            )
            .map_err(|_| StorageError::IncompleteLiveFrame)?;
            if group.batches().len() != pending.descriptor.batches.len() {
                return Err(StorageError::IncompleteLiveFrame);
            }
            for (batch, summary) in group.batches().iter().zip(&pending.descriptor.batches) {
                validate_batch(self.config(), batch)
                    .map_err(|_| StorageError::IncompleteLiveFrame)?;
                if batch.id() != summary.id
                    || batch.version() != summary.version
                    || batch.digest() != summary.digest
                    || batch.encoded().len() != summary.encoded_len as usize
                    || batch.record_descriptors().len() != summary.descriptor_count as usize
                {
                    return Err(StorageError::IncompleteLiveFrame);
                }
            }
            let validation_ns = started.map_or(0, |started| duration_ns(started.elapsed()));
            let position = pending.allocation.position;
            let group_end = position.offset() + position.frame_len() as u64;
            let mut receipts = Vec::with_capacity(group.batches().len());
            let mut metadata = Vec::with_capacity(group.batches().len());
            for (batch, offset) in group.batches().iter().zip(group.batch_offsets()) {
                let receipt = receipt_for(
                    self.config(),
                    batch,
                    member_position(position, *offset, batch),
                );
                let descriptors: Box<[_]> = batch.record_descriptors().into();
                let credit = pending.credit.take_batch(descriptors.len())?;
                metadata.push(
                    RegisteredBatch::new(
                        batch.id(),
                        batch.version(),
                        batch.digest(),
                        receipt.address,
                        descriptors,
                        group_end,
                    )
                    .with_credit(credit),
                );
                receipts.push(receipt);
            }
            state.references.publish_group(metadata)?;
            state.appended_end = group_end;
            if let Some(frontier) = self.frontier.get() {
                frontier
                    .publish_appended(group_end, group.batches().len() as u64)
                    .map_err(|_| {
                        StorageError::Invalid("durability frontier rejected a live group")
                    })?;
            }
            let tail = pending
                .descriptor
                .batches
                .last()
                .ok_or(StorageError::Invalid("empty live group"))?;
            state.live_tail = Some(LiveFrontierTail {
                offset: position.offset(),
                crc: pending.descriptor.frame_crc,
                id: tail.id,
                version: tail.version,
                digest: tail.digest,
            });
            state.stats.appended_groups += 1;
            if state.references.background_failed() {
                return Err(StorageError::Poisoned);
            }
            Ok((receipts, validation_ns))
        })();
        match result {
            Ok((receipts, validation_ns)) => {
                pending
                    .allocation
                    .guard
                    .release()
                    .map_err(|e| StorageError::Unknown(WalError::Io(io::Error::other(e))))?;
                state.last_live_complete = Some((token, first_ordinal, receipts.len()));
                state.poison = None;
                Ok((receipts, validation_ns, 1))
            }
            Err(error) => {
                // Keep the unresolved allocation and inode pinned until target
                // revocation and recovery decide its outcome.
                state.pending_live = Some(pending);
                match error {
                    StorageError::IncompleteLiveFrame if !state.references.background_failed() => {
                        // No authority or appended frontier was published for
                        // this frame. Keep writes fenced, but let the syncer
                        // persist the validated prefix that precedes the hole.
                        state.poison = Some(WriterPoison::IncompleteLiveFrame);
                        Err(StorageError::IncompleteLiveFrame)
                    }
                    StorageError::IncompleteLiveFrame => Err(StorageError::Poisoned),
                    error => Err(StorageError::Unknown(WalError::Io(io::Error::other(error)))),
                }
            }
        }
    }

    /// Construct a one-batch group. Concurrent callers should use the bounded
    /// group-commit worker to amortize persistence across independent batches.
    pub fn append_complete_batch(
        &self,
        authority: RequestAuthority,
        batch: &CompleteBatch,
    ) -> Result<AppendedBatch, StorageError> {
        let group = CompleteGroup::new(
            &self.shape,
            vec![batch.clone()],
            self.config().batch_limits,
            self.config().group_limits,
        )?;
        Ok(self.append_group(authority, &group)?.remove(0))
    }

    /// Append whole application batches under one recoverable group frame.
    /// Every batch becomes recovery-eligible together, never via an inner CRC
    /// frame alone. A bounded suffix of complete groups may await persistence;
    /// the durable frontier advances only after a serial flusher syncs its
    /// captured prefix. All-existing retries return their original receipts.
    /// Mixed old/new groups are rejected before effects.
    pub fn append_group(
        &self,
        authority: RequestAuthority,
        group: &CompleteGroup,
    ) -> Result<Vec<AppendedBatch>, StorageError> {
        self.append_group_impl(authority, group, None, true)
    }

    /// Identical append semantics with opt-in, accumulating local stage timings.
    pub fn append_group_profiled(
        &self,
        authority: RequestAuthority,
        group: &CompleteGroup,
        profile: &mut StorageWriteProfile,
    ) -> Result<Vec<AppendedBatch>, StorageError> {
        self.append_group_impl(authority, group, Some(profile), true)
    }

    pub(crate) fn try_append_group(
        &self,
        authority: RequestAuthority,
        group: &CompleteGroup,
        profile: Option<&mut StorageWriteProfile>,
    ) -> Result<Vec<AppendedBatch>, StorageError> {
        self.append_group_impl(authority, group, profile, false)
    }

    pub(crate) fn wait_history(&self) -> Result<(), StorageError> {
        let (background, failed) = self.state.lock().references.background_handle();
        let mut started = None;
        let mut slot = match background.try_lock() {
            Some(slot) => slot,
            None => {
                started = self.references.wait_started();
                background.lock()
            }
        };
        if started.is_none() && slot.is_some() {
            started = self.references.wait_started();
        }
        if let Some(handle) = slot.take()
            && handle.join().is_err()
        {
            self.references.mark_failed();
        }
        self.references.wait_finished(started);
        if failed.load(Ordering::Acquire) {
            return Err(StorageError::Poisoned);
        }
        Ok(())
    }

    fn append_group_impl(
        &self,
        authority: RequestAuthority,
        group: &CompleteGroup,
        profile: Option<&mut StorageWriteProfile>,
        wait_history: bool,
    ) -> Result<Vec<AppendedBatch>, StorageError> {
        let mut timing = WriteTiming::new(profile, true);
        timing.stage(Some(WriteStage::AppendBasicValidation));
        let config = self.config();
        if authority.epoch != config.inventory.epoch {
            return Err(StorageError::Fenced);
        }
        // Bound admission retries if other appenders repeatedly consume the
        // space released by maintenance. No WAL effects precede these retries.
        for _ in 0..=config.max_batches.min(1024) {
            timing.stage(Some(WriteStage::AppendLock));
            let mut state = self.state.lock();
            timing.stage(Some(WriteStage::AppendBasicValidation));
            if state.poison.is_some() || state.references.background_failed() {
                return Err(StorageError::Poisoned);
            }
            if state.sealed {
                return Err(StorageError::Sealed);
            }
            #[cfg(feature = "experimental-nvmeof")]
            if state.live || state.pending_live.is_some() {
                return Err(StorageError::Invalid(
                    "ordinary append forbidden in live WAL stream",
                ));
            }
            // The typed group's immutable bytes were verified by its private
            // constructor/decoder. Recheck schema and admission limits here without
            // hashing/copying all values again under the partition's admission lock.
            // Raw network/disk bytes must still use the complete group decoder.
            group.validate_for(&self.shape, config.batch_limits, config.group_limits)?;
            let mut previous = Vec::new();
            for batch in group.batches() {
                timing.stage(Some(WriteStage::AppendBasicValidation));
                validate_batch(config, batch)?;
                timing.stage(Some(WriteStage::AppendConflictLookup));
                let mut work = LookupWork::default();
                let found = self
                    .references
                    .lookup_conflict(batch.id(), batch.version(), &mut work);
                timing.record(|profile| profile.add_lookup(work));
                match found? {
                    ConflictLookup::Existing(receipt) => {
                        if receipt.version != batch.version() || receipt.digest != batch.digest() {
                            return Err(StorageError::IdentityConflict);
                        }
                        previous.push(receipt);
                    }
                    ConflictLookup::VersionTaken => return Err(StorageError::VersionConflict),
                    ConflictLookup::Free => {}
                }
            }
            if previous.len() == group.batches().len() {
                timing.record(|profile| {
                    profile.append_retries = profile.append_retries.saturating_add(1);
                });
                timing.success(previous.len());
                return Ok(previous);
            }
            if !previous.is_empty() {
                return Err(StorageError::MixedRetry);
            }
            timing.stage(Some(WriteStage::AppendGeometryCapacity));
            self.receipt_count()
                .checked_add(group.batches().len())
                .ok_or(StorageError::Capacity)?;
            let descriptor_count = group.batches().iter().try_fold(0usize, |count, batch| {
                count
                    .checked_add(batch.record_descriptors().len())
                    .ok_or(StorageError::Capacity)
            })?;
            ReferenceDirectory::check_fresh_total_capacity(
                config.max_batches.min(1024),
                config.max_reference_metadata_bytes,
                group.batches().len(),
                descriptor_count,
            )?;
            let rooted_end = self.references.root().end;
            let frame_bytes = group.encoded().len() as u64 + 8;
            if frame_bytes > 64 * 1024 * 1024 {
                return Err(StorageError::Capacity);
            }
            let next = state
                .appended_end
                .checked_add(7)
                .map(|end| end & !7)
                .ok_or(StorageError::Capacity)?;
            let remaining = config.fragment_bytes - next % config.fragment_bytes;
            let predicted_end = next
                .saturating_add(if frame_bytes > remaining {
                    remaining
                } else {
                    0
                })
                .saturating_add(frame_bytes);
            timing.stage(Some(WriteStage::AppendMetadataReservation));
            if state
                .references
                .check_capacity(group.batches().len(), descriptor_count)
                .is_err()
                || predicted_end.saturating_sub(rooted_end) > 64 * 1024 * 1024
            {
                if physical_end(&state) > state.durable_end {
                    return Err(StorageError::NeedsPersistence);
                }
                timing.stage(Some(WriteStage::AppendHistory));
                let durable_end = state.durable_end;
                let files = durable_end.saturating_sub(1) / config.wal_file_bytes + 1;
                let old_root_count = self.references.root().count;
                if let Err(error) =
                    state
                        .references
                        .start_background(durable_end, files, &self.path)
                {
                    state.poison = Some(WriterPoison::Fatal);
                    return Err(error);
                }
                drop(state);
                if !wait_history {
                    return Err(StorageError::NeedsPersistence);
                }
                self.wait_history()?;
                if self.references.root().count == old_root_count
                    && self.references.root().end == rooted_end
                {
                    return Err(StorageError::Capacity);
                }
                timing.stage(Some(WriteStage::AppendLock));
                continue;
            }
            state
                .references
                .check_capacity(group.batches().len(), descriptor_count)?;
            let mut authority_credit = match state
                .references
                .reserve_group(group.batches().len(), descriptor_count)
            {
                Ok(credit) => credit,
                Err(StorageError::Capacity) => {
                    let durable_end = state.durable_end;
                    let files = durable_end.saturating_sub(1) / config.wal_file_bytes + 1;
                    if let Err(error) =
                        state
                            .references
                            .start_background(durable_end, files, &self.path)
                    {
                        state.poison = Some(WriterPoison::Fatal);
                        return Err(error);
                    }
                    if !state.references.background_active() {
                        state
                            .references
                            .reserve_group(group.batches().len(), descriptor_count)?
                    } else {
                        drop(state);
                        if !wait_history {
                            return Err(StorageError::NeedsPersistence);
                        }
                        timing.stage(Some(WriteStage::AppendHistory));
                        self.wait_history()?;
                        timing.stage(Some(WriteStage::AppendLock));
                        continue;
                    }
                }
                Err(error) => return Err(error),
            };
            timing.stage(Some(WriteStage::AppendDescriptorPreparation));
            // Allocate descriptors before emitting authority bytes.
            let descriptors: Vec<Box<[_]>> = group
                .batches()
                .iter()
                .map(|batch| batch.record_descriptors().into())
                .collect();
            let prepared = PreparedWalWrite::new(group);
            // Native allocation can advance before a write panics. parking_lot does
            // not poison on unwind: retain this explicit poison until all storage
            // and metadata effects finish. Otherwise a caller catching a panic could
            // append an acknowledged group behind an unrecoverable allocation hole.
            // The held state lock keeps this provisional poison private on success.
            state.poison = Some(WriterPoison::Fatal);
            timing.stage(Some(WriteStage::AppendWal));
            let guard = match state
                .writer
                .as_ref()
                .ok_or(StorageError::Sealed)?
                .write(&prepared)
            {
                Ok(guard) => guard,
                Err(error) => {
                    state.poison = Some(WriterPoison::Fatal);
                    return Err(StorageError::Unknown(error));
                }
            };
            let position = *guard.wal_position();
            let group_end = position.offset() + position.frame_len() as u64;
            drop(guard); // Native storage copy only, never native index publication.
            timing.stage(Some(WriteStage::AppendPublication));
            let mut receipts = Vec::with_capacity(group.batches().len());
            let mut group_metadata = Vec::with_capacity(group.batches().len());
            for ((batch, offset), descriptors) in group
                .batches()
                .iter()
                .zip(group.batch_offsets())
                .zip(descriptors)
            {
                let receipt = receipt_for(config, batch, member_position(position, *offset, batch));
                let credit = authority_credit
                    .take_batch(descriptors.len())
                    .map_err(|error| {
                        StorageError::Unknown(WalError::Io(io::Error::other(error)))
                    })?;
                group_metadata.push(
                    RegisteredBatch::new(
                        batch.id(),
                        batch.version(),
                        batch.digest(),
                        receipt.address,
                        descriptors,
                        group_end,
                    )
                    .with_credit(credit),
                );
                receipts.push(receipt);
            }
            // The directory validates all members before publishing one group prefix.
            // Any unexpected post-append error is UNKNOWN and forces sealed recovery.
            if let Err(error) = state.references.publish_group(group_metadata) {
                state.poison = Some(WriterPoison::Fatal);
                return Err(StorageError::Unknown(WalError::Io(io::Error::other(error))));
            }
            state.appended_end = group_end;
            if let Some(frontier) = self.frontier.get()
                && frontier
                    .publish_appended(group_end, group.batches().len() as u64)
                    .is_err()
            {
                // The WAL bytes exist but the frontier cannot cover them.
                return Err(StorageError::Unknown(WalError::Io(io::Error::other(
                    "durability frontier rejected an appended group",
                ))));
            }
            state.stats.appended_groups += 1;
            if state.references.background_failed() {
                return Err(StorageError::Unknown(WalError::Io(io::Error::other(
                    "authority history failed after WAL append",
                ))));
            }
            state.poison = None;
            timing.record(|profile| {
                profile.appended_groups = profile.appended_groups.saturating_add(1);
            });
            timing.success(receipts.len());
            return Ok(receipts);
        }
        Err(StorageError::Capacity)
    }

    /// Persist the fully appended prefix captured by the serialized flusher.
    /// Later concurrent appends are not covered by this publication.
    /// Allocation alone never promotes durability.
    pub fn persist_through(&self, appended: AppendedBatch) -> Result<DurableBatch, StorageError> {
        self.persist_receipts(std::slice::from_ref(&appended), None)?;
        Ok(DurableBatch(appended))
    }

    /// Validate and persist a set of exact receipts together, preserving input
    /// order (including duplicate retries). All receipts are checked before I/O.
    /// Already-durable receipts may accompany any pending prefix members.
    /// Several complete groups may be appended before the flusher captures its
    /// contiguous prefix; later appends are never inferred durable.
    ///
    /// Like a write barrier, receipts complete after their shared sync; unlike
    /// group-local barriers, this one may cover multiple captured groups.
    pub fn persist_many(
        &self,
        appended: &[AppendedBatch],
    ) -> Result<Vec<DurableBatch>, StorageError> {
        self.persist_receipts(appended, None)?;
        Ok(appended.iter().copied().map(DurableBatch).collect())
    }

    /// Identical persistence semantics with opt-in, accumulating local stage timings.
    /// `persist_total_ns` excludes allocation of the returned receipt vector.
    pub fn persist_many_profiled(
        &self,
        appended: &[AppendedBatch],
        profile: &mut StorageWriteProfile,
    ) -> Result<Vec<DurableBatch>, StorageError> {
        self.persist_receipts(appended, Some(profile))?;
        Ok(appended.iter().copied().map(DurableBatch).collect())
    }

    /// Barrier covering everything appended so far, not only named receipts.
    /// This is the explicit flush and the background syncer's group commit: one
    /// call makes every group appended before it captured the frontier durable.
    /// Returns the durable frontier after the call.
    pub fn persist_appended(&self) -> Result<u64, StorageError> {
        self.persist_scope(PersistScope::Appended, None)?;
        Ok(self.durable_frontier.load(Ordering::Acquire))
    }

    /// `persist_appended` with opt-in stage timings.
    pub fn persist_appended_profiled(
        &self,
        profile: &mut StorageWriteProfile,
    ) -> Result<u64, StorageError> {
        self.persist_scope(PersistScope::Appended, Some(profile))?;
        Ok(self.durable_frontier.load(Ordering::Acquire))
    }

    /// End of the contiguous appended (acknowledged-in-memory) WAL prefix.
    pub fn appended_frontier(&self) -> u64 {
        self.state.lock().appended_end
    }

    /// End of the prefix a WAL barrier has made durable.
    pub fn synced_frontier(&self) -> u64 {
        self.durable_frontier.load(Ordering::Acquire)
    }

    /// The durability layer's frontier, once the node attached it.
    pub fn frontier(&self) -> Option<&Arc<super::durability::frontier::Frontier>> {
        self.frontier.get()
    }

    /// Create (once) this log's frontier, starting at its durable end and already
    /// covering anything appended before the call. Appends publish into it under
    /// the state lock, so attaching can neither miss nor duplicate a group.
    pub fn attach_frontier(
        &self,
        config: &super::durability::DurabilityConfig,
    ) -> Result<Arc<super::durability::frontier::Frontier>, StorageError> {
        let state = self.state.lock();
        if let Some(existing) = self.frontier.get() {
            return Ok(existing.clone());
        }
        let frontier = super::durability::frontier::Frontier::with_config(
            config.frontier_config(state.durable_end, self.config().fragment_bytes),
        )
        .map_err(|_| StorageError::Invalid("invalid durability frontier configuration"))?;
        if state.appended_end > state.durable_end {
            frontier
                .publish_appended(state.appended_end, 0)
                .map_err(|_| StorageError::Invalid("durability frontier rejected backlog"))?;
        }
        // Release pairs with the Acquire load in `readable_frontier`.
        self.serve_unsynced.store(
            config.tier == super::durability::DurabilityTier::Native,
            Ordering::Release,
        );
        let _ = self.frontier.set(frontier.clone());
        Ok(frontier)
    }

    /// Syncer body: make everything appended durable and keep its stage timings
    /// for the waiter that triggered it. Runs on the single syncer thread.
    pub fn sync_for_frontier(&self) -> io::Result<()> {
        let mut profile = StorageWriteProfile::default();
        let result = self.persist_appended_profiled(&mut profile);
        let capture = profile.persistence_capture;
        let mut accumulated = self.sync_profile.lock();
        accumulated.add(profile);
        // The newest non-noop capture identifies the sync that covered the waiter.
        if capture.is_some() {
            accumulated.persistence_capture = capture;
        }
        drop(accumulated);
        result.map(|_| ()).map_err(io::Error::other)
    }

    /// Stage timings of background syncs since the last call.
    pub fn take_sync_profile(&self) -> StorageWriteProfile {
        std::mem::take(&mut *self.sync_profile.lock())
    }

    fn persist_receipts(
        &self,
        receipts: &[AppendedBatch],
        profile: Option<&mut StorageWriteProfile>,
    ) -> Result<(), StorageError> {
        self.persist_scope(PersistScope::Receipts(receipts), profile)
    }

    fn persist_scope(
        &self,
        scope: PersistScope<'_>,
        profile: Option<&mut StorageWriteProfile>,
    ) -> Result<(), StorageError> {
        let result = self.persist_scope_inner(scope, profile);
        if let Err(error) = &result
            && matches!(error, StorageError::Unknown(_) | StorageError::Poisoned)
        {
            self.fail_frontier(&format!("persistence failed: {error}"));
        }
        result
    }

    fn persist_scope_inner(
        &self,
        scope: PersistScope<'_>,
        profile: Option<&mut StorageWriteProfile>,
    ) -> Result<(), StorageError> {
        let receipt_count = scope.len();
        let mut timing = WriteTiming::new(profile, false);
        timing.record(|profile| profile.persistence_capture = None);
        if receipt_count == Some(0) {
            timing.record(|profile| {
                profile.persist_noops = profile.persist_noops.saturating_add(1);
            });
            timing.success(0);
            return Ok(());
        }

        timing.stage(Some(WriteStage::PersistStateLock));
        let state = self.state.lock();
        timing.stage(Some(WriteStage::PersistValidation));
        let pending = pending_scope(&state, scope)?;
        drop(state);
        if pending.is_none() {
            timing.record(|profile| {
                profile.persist_noops = profile.persist_noops.saturating_add(1);
            });
            timing.success(receipt_count.unwrap_or(0));
            return Ok(());
        }

        timing.stage(Some(WriteStage::PersistSerializationLock));
        let _persistence = self.persistence.lock();
        timing.stage(Some(WriteStage::PersistStateLock));
        #[cfg(feature = "experimental-nvmeof")]
        let mut live_frontier = None;
        let (start, end, namespace_synced_file) = {
            let mut state = self.state.lock();
            timing.stage(Some(WriteStage::PersistValidation));
            let Some(requested_end) = pending_scope(&state, scope)? else {
                timing.record(|profile| {
                    profile.persist_noops = profile.persist_noops.saturating_add(1);
                });
                timing.success(receipt_count.unwrap_or(0));
                return Ok(());
            };
            #[cfg(test)]
            if self.fail_persist.swap(false, Ordering::AcqRel) {
                state.poison = Some(WriterPoison::Fatal);
                return Err(StorageError::Unknown(WalError::Io(io::Error::other(
                    "injected persistence failure after append",
                ))));
            }
            if requested_end > state.appended_end {
                state.poison = Some(WriterPoison::Fatal);
                return Err(StorageError::Unknown(WalError::Io(io::Error::other(
                    "receipt exceeds appended frontier",
                ))));
            }
            timing.record(|profile| {
                profile.persistence_capture = Some(PersistenceCapture {
                    durable_end: state.durable_end,
                    captured_appended_end: state.appended_end,
                    cumulative_appended_groups: state.stats.appended_groups,
                });
            });
            #[cfg(feature = "experimental-nvmeof")]
            if state.live {
                let tail = state
                    .live_tail
                    .clone()
                    .ok_or(StorageError::Invalid("missing live frontier tail"))?;
                let mut frontier = LiveFrontier::new(self.config());
                frontier.end = state.appended_end;
                frontier.tail = Some(tail);
                live_frontier = Some(frontier);
            }
            (
                state.durable_end,
                state.appended_end,
                state.namespace_synced_file,
            )
        };
        timing.stage(None);
        #[cfg(test)]
        if let Some((entered, resume)) = self.before_persist.lock().take() {
            let _ = entered.send(());
            let _ = resume.recv();
        }

        let first_file = self.config().layout().locate_file(start).as_u64();
        let last_file = self
            .config()
            .layout()
            .locate_file(end.saturating_sub(1))
            .as_u64();
        let previous_namespace = namespace_synced_file.map(|file| file.as_u64());
        let namespace_sync = previous_namespace.is_none_or(|file| last_file > file);
        let mut witness_persisted = 0u64;
        #[cfg(feature = "experimental-nvmeof")]
        let mut frontier_syncs = (0u64, 0u64);
        let result = (|| -> io::Result<()> {
            for file_id in first_file..=last_file {
                timing.record(|profile| {
                    profile.file_persist_calls = profile.file_persist_calls.saturating_add(1);
                });
                timing.stage(Some(WriteStage::PersistFile));
                let offset = file_id
                    .checked_mul(self.config().wal_file_bytes)
                    .ok_or_else(|| io::Error::other("WAL file offset overflow"))?;
                self.wal.persist_frame_file(WalPosition::new(offset, 1))?;
                timing.record(|profile| {
                    profile.file_persist_successes =
                        profile.file_persist_successes.saturating_add(1);
                });
            }
            if namespace_sync {
                timing.record(|profile| {
                    profile.namespace_persist_calls =
                        profile.namespace_persist_calls.saturating_add(1);
                });
                timing.stage(Some(WriteStage::PersistNamespace));
                #[cfg(test)]
                if self.fail_namespace_persist.swap(false, Ordering::AcqRel) {
                    return Err(io::Error::other(
                        "injected namespace persistence failure after file sync",
                    ));
                }
                self.wal.persist_namespace()?;
                timing.record(|profile| {
                    profile.namespace_persist_successes =
                        profile.namespace_persist_successes.saturating_add(1);
                });
                let witness_start = previous_namespace.map_or(0, |file| file + 1);
                for file in witness_start..=last_file {
                    #[cfg(test)]
                    let fail_witness = self.fail_inventory_witness.swap(0, Ordering::AcqRel);
                    #[cfg(test)]
                    if fail_witness == 1 {
                        return Err(io::Error::other("injected crash before inventory witness"));
                    }
                    if file_inventory::persist(&self.path, self.config(), file, &mut timing)? {
                        witness_persisted += 1;
                    }
                    #[cfg(test)]
                    if fail_witness == 2 {
                        return Err(io::Error::other("injected crash after inventory witness"));
                    }
                }
            }
            #[cfg(feature = "experimental-nvmeof")]
            if let Some(frontier) = &live_frontier {
                self.persist_live_frontier(frontier, &mut timing, &mut frontier_syncs)?;
            }
            Ok(())
        })();

        timing.stage(Some(WriteStage::PersistStateLock));
        let mut state = self.state.lock();
        timing.stage(Some(WriteStage::PersistPublication));
        state.stats.inventory_witnesses += witness_persisted;
        #[cfg(feature = "experimental-nvmeof")]
        {
            state.stats.live_frontier_file_syncs += frontier_syncs.0;
            state.stats.live_frontier_namespace_syncs += frontier_syncs.1;
        }
        result.map_err(|error| {
            state.poison = Some(WriterPoison::Fatal);
            StorageError::Unknown(WalError::Io(error))
        })?;
        if persistence_poisoned(&state, scope) {
            return Err(StorageError::Poisoned);
        }
        debug_assert!(state.durable_end <= end);
        debug_assert!(end <= state.appended_end);
        state.stats.persistence_barriers += 1;
        state.stats.namespace_syncs += u64::from(namespace_sync);
        state.durable_end = end;
        self.durable_frontier.store(end, Ordering::Release);
        if namespace_sync {
            state.namespace_synced_file =
                Some(crate::wal::position::WalFileId::from_u64(last_file));
        }
        if state.references.should_start_background(end)
            && let Err(error) = state
                .references
                .start_background(end, last_file + 1, &self.path)
        {
            state.poison = Some(WriterPoison::Fatal);
            return Err(error);
        }
        if state.references.background_failed() {
            state.poison = Some(WriterPoison::Fatal);
            return Err(StorageError::Poisoned);
        }
        timing.record(|profile| {
            profile.durable_publications = profile.durable_publications.saturating_add(1);
            profile.persist_barriers = profile.persist_barriers.saturating_add(1);
            profile.persist_barrier_receipts = profile
                .persist_barrier_receipts
                .saturating_add(receipt_count.unwrap_or(0) as u64);
        });
        timing.success(receipt_count.unwrap_or(0));
        Ok(())
    }

    pub fn read_batch(&self, appended: AppendedBatch) -> Result<CompleteBatch, StorageError> {
        let _authority = self
            .load_authority(appended.address)?
            .filter(|record| {
                record.matches(
                    appended.id,
                    appended.version,
                    appended.digest,
                    appended.address,
                )
            })
            .ok_or(StorageError::Invalid("unknown or foreign address"))?;
        let _admission = self.read_admission()?;
        let position = WalPosition::new(appended.address.offset, appended.address.len);
        #[cfg(feature = "experimental-nvmeof")]
        let bytes = if self.live_stream {
            let payload_len = position
                .frame_len()
                .checked_sub(8)
                .ok_or(StorageError::Invalid("invalid batch frame"))?;
            let plan = self
                .wal
                .plan_record_payload_range(position, 0..payload_len)
                .map_err(StorageError::Wal)?
                .ok_or(StorageError::Invalid("authoritative value disappeared"))?;
            let offset = plan
                .offset
                .checked_sub(8)
                .ok_or(StorageError::Invalid("invalid frame offset"))?;
            let mut frame = vec![0; position.frame_len()];
            plan.file.read_exact_at(&mut frame, offset)?;
            let bytes: minibytes::Bytes = frame.into();
            Some(
                CrcFrame::read_from_bytes(&bytes, 0)
                    .map_err(|_| StorageError::Invalid("invalid batch frame CRC"))?,
            )
        } else {
            self.wal.read(position).map_err(StorageError::Wal)?.1
        };
        #[cfg(not(feature = "experimental-nvmeof"))]
        let bytes = self.wal.read(position).map_err(StorageError::Wal)?.1;
        let batch = CompleteBatch::decode(
            &self.shape,
            bytes.ok_or(StorageError::Invalid("authoritative value disappeared"))?,
            self.config().batch_limits,
        )?;
        if batch.id() != appended.id
            || batch.version() != appended.version
            || batch.digest() != appended.digest
        {
            return Err(StorageError::IdentityConflict);
        }
        Ok(batch)
    }

    /// Produce a scalar reference only from already-validated batch membership.
    pub fn record_reference(
        &self,
        appended: AppendedBatch,
        operation_index: u32,
    ) -> Result<ValueRecordReference, StorageError> {
        let registered = self
            .load_authority(appended.address)?
            .ok_or(StorageError::Invalid("unknown record reference"))?;
        let reference = registered.reference(operation_index)?;
        if reference.batch_id != appended.id
            || reference.batch_version != appended.version
            || reference.batch_digest != appended.digest
        {
            return Err(StorageError::Invalid("unknown or foreign address"));
        }
        Ok(reference)
    }

    fn load_authority(
        &self,
        address: PhysicalValueAddress,
    ) -> Result<Option<Arc<RegisteredBatch>>, StorageError> {
        match self.references.lookup(address) {
            Err(StorageError::Capacity) => {}
            result => return result,
        }
        let mut state = self.state.lock();
        if state.poison.is_some() || state.references.background_failed() {
            return Err(StorageError::Poisoned);
        }
        let durable_end = state.durable_end;
        let files = durable_end.saturating_sub(1) / self.config().wal_file_bytes + 1;
        if let Err(error) = state
            .references
            .start_background(durable_end, files, &self.path)
        {
            state.poison = Some(WriterPoison::Fatal);
            return Err(error);
        }
        drop(state);
        if let Err(error) = self.wait_history() {
            *self.read_admission.write() = false;
            return Err(error);
        }
        self.references.lookup(address)
    }

    /// End of the WAL prefix a reader may use: the synced frontier, or under the
    /// native tier the appended frontier. An appended group is in the page cache
    /// and already indexed, so serving it is correct for a live process; only a
    /// kernel crash can lose it, which is the native contract.
    fn readable_frontier(&self) -> u64 {
        let durable = self.durable_frontier.load(Ordering::Acquire);
        if !self.serve_unsynced.load(Ordering::Acquire) {
            return durable;
        }
        self.frontier
            .get()
            .map_or(durable, |frontier| durable.max(frontier.appended()))
    }

    fn validate_record_authority(
        &self,
        reference: &ValueRecordReference,
        registered: &RegisteredBatch,
    ) -> Result<(), StorageError> {
        if self.references.failed() {
            return Err(StorageError::Poisoned);
        }
        registered.validate_reference(reference)?;
        if registered.group_end() > self.readable_frontier() {
            return Err(StorageError::NeedsPersistence);
        }
        Ok(())
    }

    fn load_durable_record_authority(
        &self,
        reference: &ValueRecordReference,
    ) -> Result<Arc<RegisteredBatch>, StorageError> {
        let registered = self
            .load_authority(reference.batch_address)?
            .ok_or(StorageError::Invalid("unknown record reference"))?;
        self.validate_record_authority(reference, &registered)?;
        Ok(registered)
    }

    /// Read one independently verified record, never a whole value batch.
    /// Cold CRC authority is paged under the metadata budget; native file/mapping
    /// owners survive retirement. Retired addresses are never reused.
    pub fn read_record(&self, reference: &ValueRecordReference) -> Result<Mutation, StorageError> {
        Ok(CompleteBatch::decode_record(
            &self.shape,
            reference,
            self.read_record_encoded(reference)?,
        )?)
    }

    /// Transfer a member with exact authority membership and bounds checked.
    /// The returned bytes still need `CompleteBatch::decode_record` at their
    /// consuming boundary. This lets an authenticated TCP client verify once,
    /// instead of forcing both storage and client to hash the same payload.
    pub(crate) fn read_record_encoded(
        &self,
        reference: &ValueRecordReference,
    ) -> Result<minibytes::Bytes, StorageError> {
        self.read_record_encoded_with_policy(reference, WalMemberReadPolicy::Mapped)
    }

    /// Reference-directory membership can precede persistence. Expose a record
    /// only after its physical group lies within the release-published frontier.
    pub(crate) fn validate_durable_record_reference(
        &self,
        reference: &ValueRecordReference,
    ) -> Result<(), StorageError> {
        self.load_durable_record_authority(reference)?;
        Ok(())
    }

    /// Disk-free resident fast path. Contention or cold authority returns None;
    /// a bounded read worker must call `plan_record_read` for that miss.
    pub(crate) fn try_plan_record_read(
        self: &Arc<Self>,
        reference: &ValueRecordReference,
    ) -> Result<Option<RecordReadPlan>, StorageError> {
        if self.references.failed() {
            return Err(StorageError::Poisoned);
        }
        let Some(admission) = self.read_admission.try_read() else {
            return Ok(None);
        };
        if !*admission {
            return Err(StorageError::Poisoned);
        }
        let Some(registered) = self.references.lookup_resident(reference.batch_address) else {
            return Ok(None);
        };
        self.plan_registered_record_read(reference, registered)
            .map(Some)
    }

    /// Cold authority lookup is permitted only on a bounded blocking read worker.
    /// The returned plan owns both descriptor authority and the native file.
    pub(crate) fn plan_record_read(
        self: &Arc<Self>,
        reference: &ValueRecordReference,
    ) -> Result<RecordReadPlan, StorageError> {
        #[cfg(test)]
        if let Some((entered, resume)) = self.before_authority_load.lock().take() {
            let _ = entered.send(());
            let _ = resume.recv();
        }
        let registered = self
            .load_authority(reference.batch_address)?
            .ok_or(StorageError::Invalid("unknown record reference"))?;
        let _admission = self.read_admission()?;
        self.plan_registered_record_read(reference, registered)
    }

    fn plan_registered_record_read(
        self: &Arc<Self>,
        reference: &ValueRecordReference,
        registered: Arc<RegisteredBatch>,
    ) -> Result<RecordReadPlan, StorageError> {
        self.validate_record_authority(reference, &registered)?;
        let address = reference.batch_address;
        let start = reference.descriptor.offset as usize;
        let end = start
            .checked_add(reference.descriptor.len as usize)
            .ok_or(StorageError::Invalid("record bounds overflow"))?;
        let member = self
            .wal
            .plan_record_payload_range(WalPosition::new(address.offset, address.len), start..end)
            .map_err(StorageError::Wal)?
            .ok_or(StorageError::Invalid("authoritative record disappeared"))?;
        Ok(RecordReadPlan {
            member,
            device: self.device,
            _authority: registered,
            _log: self.clone(),
        })
    }

    /// Resolve a durable exact record reference to an owner-pinned file range.
    #[cfg(feature = "experimental-nvmeof")]
    pub fn resolve_live_record(
        self: &Arc<Self>,
        reference: &ValueRecordReference,
    ) -> Result<super::nvmeof::live_storage::LiveRecordLease, StorageError> {
        let registered = self.load_durable_record_authority(reference)?;
        let _admission = self.read_admission()?;
        let plan = self.plan_registered_record_read(reference, registered)?;
        self.live_lease(plan, reference.batch_address.offset)
    }

    /// Resolve a durable receipt to its complete inner native CRC frame for
    /// recovery. Metadata only; the caller reads and validates the frame.
    #[cfg(feature = "experimental-nvmeof")]
    pub fn resolve_live_batch(
        self: &Arc<Self>,
        receipt: AppendedBatch,
    ) -> Result<super::nvmeof::live_storage::LiveRecordLease, StorageError> {
        let registered = self
            .load_authority(receipt.address)?
            .ok_or(StorageError::Invalid("unknown batch receipt"))?;
        if registered.receipt() != receipt {
            return Err(StorageError::Invalid("foreign batch receipt"));
        }
        if registered.group_end() > self.readable_frontier() {
            return Err(StorageError::NeedsPersistence);
        }
        let _admission = self.read_admission()?;
        let position = WalPosition::new(receipt.address.offset, receipt.address.len);
        let payload_len = position
            .frame_len()
            .checked_sub(8)
            .ok_or(StorageError::Invalid("invalid batch frame"))?;
        let mut member = self
            .wal
            .plan_record_payload_range(position, 0..payload_len)
            .map_err(StorageError::Wal)?
            .ok_or(StorageError::Invalid("authoritative batch disappeared"))?;
        member.offset = member
            .offset
            .checked_sub(8)
            .ok_or(StorageError::Invalid("invalid frame offset"))?;
        member.len = member.len.checked_add(8).ok_or(StorageError::Capacity)?;
        let plan = RecordReadPlan {
            member,
            device: self.device,
            _authority: registered,
            _log: self.clone(),
        };
        self.live_lease(plan, receipt.address.offset)
    }

    #[cfg(feature = "experimental-nvmeof")]
    fn live_lease(
        &self,
        plan: RecordReadPlan,
        global_offset: u64,
    ) -> Result<super::nvmeof::live_storage::LiveRecordLease, StorageError> {
        use super::nvmeof::live_storage::{LiveRecordLease, LiveWalSegment};
        let config = self.config();
        let file_index = global_offset / config.wal_file_bytes;
        let metadata = plan.member.file.metadata()?;
        if metadata.len() != config.wal_file_bytes || !metadata.len().is_multiple_of(4096) {
            return Err(StorageError::Invalid("invalid live namespace geometry"));
        }
        let offset = plan.member.offset;
        let len = u32::try_from(plan.member.len).map_err(|_| StorageError::Capacity)?;
        let segment = LiveWalSegment {
            database: config.inventory.database,
            partition: config.partition,
            epoch: config.inventory.epoch,
            generation: config.inventory.segment_generation,
            file_index,
            path: config.layout().wal_file_name(
                &self.path,
                crate::wal::position::WalFileId::from_u64(file_index),
            ),
            bytes: metadata.len(),
            device: metadata.dev(),
            inode: metadata.ino(),
            block_size: 4096,
        };
        let named = fs::metadata(&segment.path)?;
        if named.dev() != segment.device
            || named.ino() != segment.inode
            || named.len() != segment.bytes
        {
            return Err(StorageError::Invalid(
                "WAL pathname no longer names pinned inode",
            ));
        }
        self.live_read_pins
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |pins| {
                pins.checked_add(1)
            })
            .map_err(|_| StorageError::Capacity)?;
        Ok(LiveRecordLease {
            segment,
            offset,
            len,
            _plan: plan,
        })
    }

    #[cfg(feature = "experimental-nvmeof")]
    pub(crate) fn release_live_read_pin(&self) {
        self.live_read_pins.fetch_sub(1, Ordering::AcqRel);
    }

    fn read_admission(&self) -> Result<parking_lot::RwLockReadGuard<'_, bool>, StorageError> {
        let admission = self.read_admission.read();
        if !*admission || self.references.failed() {
            return Err(StorageError::Poisoned);
        }
        Ok(admission)
    }

    #[cfg(test)]
    pub(crate) fn evict_authority_cache(&self) {
        self.references.evict_cache();
    }

    #[cfg(test)]
    pub(crate) fn pause_next_authority_load(
        &self,
    ) -> (
        std::sync::mpsc::Receiver<()>,
        std::sync::mpsc::SyncSender<()>,
    ) {
        let (entered, started) = std::sync::mpsc::sync_channel(1);
        let (resume, released) = std::sync::mpsc::sync_channel(1);
        *self.before_authority_load.lock() = Some((entered, released));
        (started, resume)
    }

    pub(crate) fn read_record_encoded_with_policy(
        &self,
        reference: &ValueRecordReference,
        policy: WalMemberReadPolicy,
    ) -> Result<minibytes::Bytes, StorageError> {
        let _authority = self.load_durable_record_authority(reference)?;
        let _admission = self.read_admission()?;
        self.read_member_range(reference, policy)
    }

    fn read_member_range(
        &self,
        reference: &ValueRecordReference,
        policy: WalMemberReadPolicy,
    ) -> Result<minibytes::Bytes, StorageError> {
        #[cfg(feature = "experimental-nvmeof")]
        let policy = if self.live_stream && policy == WalMemberReadPolicy::Mapped {
            WalMemberReadPolicy::Positional
        } else {
            policy
        };
        let address = reference.batch_address;
        let start = reference.descriptor.offset as usize;
        let end = start
            .checked_add(reference.descriptor.len as usize)
            .ok_or(StorageError::Invalid("record bounds overflow"))?;
        let (_, bytes) = self
            .wal
            .read_record_payload_range(
                WalPosition::new(address.offset, address.len),
                start..end,
                policy,
            )
            .map_err(StorageError::Wal)?;
        bytes.ok_or(StorageError::Invalid("authoritative record disappeared"))
    }

    pub fn read_record_profiled(
        &self,
        reference: &ValueRecordReference,
    ) -> Result<(Mutation, RecordReadProfile), StorageError> {
        use std::time::Instant;
        let started = Instant::now();
        let _authority = self.load_durable_record_authority(reference)?;
        let _admission = self.read_admission()?;
        let membership_ns = started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        let started = Instant::now();
        let bytes = self.read_member_range(reference, WalMemberReadPolicy::Mapped)?;
        let wal_range_ns = started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        let started = Instant::now();
        let mutation = CompleteBatch::decode_record(&self.shape, reference, bytes)?;
        let verification_decode_ns = started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        Ok((
            mutation,
            RecordReadProfile {
                membership_ns,
                wal_range_ns,
                verification_decode_ns,
            },
        ))
    }

    pub fn lookup(&self, id: BatchId) -> Result<Option<AppendedBatch>, StorageError> {
        self.references.lookup_id(id)
    }

    pub fn lookup_version(
        &self,
        version: BatchVersion,
    ) -> Result<Option<AppendedBatch>, StorageError> {
        self.references.lookup_version(version)
    }

    pub fn stats(&self) -> PartitionStats {
        let state = self.state.lock();
        let mut stats = state.stats;
        let references = state.references.stats();
        stats.registered_records = references.retained_descriptors;
        stats.reference_metadata_bytes = references.charged_metadata_bytes;
        stats
    }

    /// Metadata residency counters, not value-cache hits or physical read IOPS.
    pub fn authority_cache_stats(&self) -> AuthorityCacheStats {
        self.references.read_stats()
    }

    /// Enable or disable per-maintenance clocks and counters for this handle.
    pub fn set_history_maintenance_diagnostics(&self, enabled: bool) {
        self.references.set_history_diagnostics(enabled);
    }

    /// Snapshot cumulative local observations; counters are not an atomic set.
    pub fn history_maintenance_stats(&self) -> HistoryMaintenanceStats {
        self.references.history_stats()
    }

    /// The callback must not acquire this log's state or inventory locks.
    pub(crate) fn set_history_failure_notifier(&self, notifier: Arc<dyn Fn() + Send + Sync>) {
        self.references.set_history_failure_notifier(notifier);
    }

    #[cfg(test)]
    pub(crate) fn pause_next_history(
        &self,
    ) -> (
        std::sync::mpsc::Receiver<()>,
        std::sync::mpsc::SyncSender<()>,
    ) {
        self.references.pause_next_history()
    }

    #[cfg(test)]
    pub(crate) fn fail_next_history_after_root(&self) {
        self.references.fail_next_history_after_root();
    }

    pub(crate) fn claim_worker(&self) -> Result<(), StorageError> {
        if self.references.failed() || self.state.lock().poison.is_some() {
            return Err(StorageError::Poisoned);
        }
        self.worker_claimed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| StorageError::Invalid("partition already has a group worker"))?;
        Ok(())
    }

    pub(crate) fn release_worker(&self) {
        self.worker_claimed.store(false, Ordering::Release);
    }

    /// A stopped worker may have emitted an unreported group. Preserve it for
    /// sealed recovery instead of allowing another writer to guess its outcome.
    pub(crate) fn poison_writer(&self) {
        self.state.lock().poison = Some(WriterPoison::Fatal);
        self.fail_frontier("writer poisoned");
    }

    /// A failed or uncertain persistence fails the frontier, so no waiter and no
    /// native-tier acknowledgement can succeed after it.
    fn fail_frontier(&self, reason: &str) {
        if let Some(frontier) = self.frontier.get() {
            frontier.fail(reason);
        }
    }

    #[cfg(test)]
    pub(crate) fn pause_next_persist(
        &self,
    ) -> (
        std::sync::mpsc::Receiver<()>,
        std::sync::mpsc::SyncSender<()>,
    ) {
        let (entered, started) = std::sync::mpsc::sync_channel(1);
        let (resume, released) = std::sync::mpsc::sync_channel(1);
        *self.before_persist.lock() = Some((entered, released));
        (started, resume)
    }

    #[cfg(test)]
    pub(crate) fn fail_next_persist(&self) {
        self.fail_persist.store(true, Ordering::Release);
    }
    #[cfg(test)]
    pub(crate) fn fail_next_namespace_persist(&self) {
        // Force the next persistence through the required namespace-sync path.
        self.state.lock().namespace_synced_file = None;
        self.fail_namespace_persist.store(true, Ordering::Release);
    }

    #[cfg(test)]
    pub(crate) fn fail_next_inventory_witness(&self, after: bool) {
        self.fail_inventory_witness
            .store(if after { 2 } else { 1 }, Ordering::Release);
    }

    /// Stable physical append ordinal; includes the sole pending group.
    pub fn receipt_count(&self) -> usize {
        self.references.len()
    }

    /// Bounded pageable authority. No lifetime vector is materialized.
    pub fn receipt_page(
        &self,
        start: usize,
        limit: usize,
    ) -> Result<Vec<AppendedBatch>, StorageError> {
        self.references.receipt_page(start, limit)
    }

    pub fn checkpoint_prefix(&self, prefix: usize) -> Result<[u8; 32], StorageError> {
        self.references.checkpoint_prefix(
            prefix,
            self.config().inventory.database,
            self.config().inventory.epoch,
        )
    }

    /// Existing file offsets in bounded sorted pages, not an ownership proof.
    pub fn wal_file_page(
        &self,
        start_after: Option<u64>,
        limit: usize,
    ) -> Result<Vec<u64>, StorageError> {
        if limit == 0 || limit > 256 {
            return Err(StorageError::Invalid("invalid WAL file page limit"));
        }
        let mut result = Vec::with_capacity(limit);
        for entry in fs::read_dir(&self.path)? {
            let entry = entry?;
            let Some((id, _)) = wal_file_entry(&entry, self.config())? else {
                continue;
            };
            let offset = id
                .checked_mul(self.config().wal_file_bytes)
                .ok_or(StorageError::Invalid("WAL offset overflow"))?;
            if start_after.is_some_and(|after| offset <= after)
                || self.references.is_retired(offset)?
            {
                continue;
            }
            match result.binary_search(&offset) {
                Ok(_) => return Err(StorageError::Invalid("duplicate WAL file")),
                Err(index) if index < limit => {
                    result.insert(index, offset);
                    if result.len() > limit {
                        result.pop();
                    }
                }
                Err(_) => {}
            }
        }
        Ok(result)
    }

    /// Authenticated authority has proved these sealed files wholly unreachable
    /// from its installed index, retained cuts and pins. No values are relocated.
    pub fn retire_files(&self, recovery_prefix: usize, files: &[u64]) -> Result<(), StorageError> {
        let _persistence = self.persistence.lock();
        let mut state = self.state.lock();
        if !state.sealed
            || state.writer.is_some()
            || state.poison.is_some()
            || state.references.background_failed()
        {
            return Err(StorageError::Invalid(
                "retirement requires healthy sealed storage",
            ));
        }
        if files.len() > 256
            || files.windows(2).any(|pair| pair[0] >= pair[1])
            || recovery_prefix != self.references.root().count
            || recovery_prefix != self.receipt_count()
        {
            return Err(StorageError::Invalid(
                "retirement differs from complete durable prefix",
            ));
        }
        if files.is_empty() {
            return Ok(());
        }
        let size = self.config().wal_file_bytes;
        let root = self.references.root();
        let mut ids = Vec::with_capacity(files.len());
        for &offset in files {
            if !offset.is_multiple_of(size) || offset / size >= root.file_count {
                return Err(StorageError::Invalid(
                    "retirement outside registered WAL inventory",
                ));
            }
            if !self
                .path
                .join(format!("wal_{:016x}", offset / size))
                .is_file()
                && !self.references.is_retired(offset)?
            {
                return Err(StorageError::Invalid("unexplained missing WAL file"));
            }
            ids.push(crate::wal::position::WalFileId::from_u64(offset / size));
        }
        let mut admission = self.read_admission.write();
        #[cfg(feature = "experimental-nvmeof")]
        if self.live_read_pins.load(Ordering::Acquire) != 0 {
            return Err(StorageError::Invalid(
                "live namespace or read pin prevents retirement",
            ));
        }
        *admission = false;
        state.references.retire(files)?;
        self.wal.retire_files(&ids)?;
        *admission = true;
        Ok(())
    }

    /// Validate owner authority and return the exact pinned source-file mapping.
    /// This is control-plane metadata, not a per-value data read.
    #[cfg(feature = "experimental-nvmeof")]
    pub(crate) fn nvmeof_record_location(
        self: &Arc<Self>,
        reference: &ValueRecordReference,
    ) -> Result<super::tcp::ValidatedRecordLocation, StorageError> {
        {
            let state = self.state.lock();
            if state.poison.is_some() || state.references.background_failed() {
                return Err(StorageError::Poisoned);
            }
            if !state.sealed || state.writer.is_some() {
                return Err(StorageError::Invalid(
                    "export requires durably sealed storage",
                ));
            }
        }
        let plan = self.plan_record_read(reference)?;
        let source = plan.member.file.metadata()?;
        let wal_file_bytes = self.config().wal_file_bytes;
        Ok(super::tcp::ValidatedRecordLocation {
            reference: *reference,
            wal_file_bytes,
            source_file_index: reference.batch_address.offset / wal_file_bytes,
            source_device: source.dev(),
            source_inode: source.ino(),
            source_file_len: source.len(),
            durable_frontier: self.durable_frontier.load(Ordering::Acquire),
            offset: plan.member.offset,
            len: plan.member.len as u64,
        })
    }

    /// Export exactly the supplied durable references from one sealed WAL file.
    /// The copied file is independent of subsequent native segment retirement.
    #[cfg(feature = "experimental-nvmeof")]
    pub fn export_sealed_readset(
        self: &Arc<Self>,
        directory: &Path,
        references: &[ValueRecordReference],
        config: &super::nvmeof::ExportConfig,
    ) -> Result<super::nvmeof::ExportManifest, StorageError> {
        {
            let state = self.state.lock();
            if state.poison.is_some() || state.references.background_failed() {
                return Err(StorageError::Poisoned);
            }
            if !state.sealed || state.writer.is_some() {
                return Err(StorageError::Invalid(
                    "export requires durably sealed storage",
                ));
            }
        }
        // Charge plans, hash-set spare capacity, catalog and serialized JSON
        // before allocating any of them. This intentionally overestimates.
        let catalog_bytes = references
            .len()
            .checked_mul(4096)
            .and_then(|n| n.checked_add(config.nqn.len()))
            .and_then(|n| n.checked_add(4096))
            .ok_or(StorageError::Capacity)?;
        if references.is_empty() || catalog_bytes > config.max_catalog_bytes {
            return Err(StorageError::Capacity);
        }
        let mut plans = Vec::with_capacity(references.len());
        let mut unique = std::collections::HashSet::with_capacity(references.len());
        for reference in references {
            if !unique.insert(*reference) {
                return Err(StorageError::Invalid("duplicate export reference"));
            }
            plans.push((*reference, self.plan_record_read(reference)?));
        }
        super::nvmeof::publish_export(
            directory,
            config,
            &self.config().inventory,
            self.config().partition,
            self.durable_frontier.load(Ordering::Acquire),
            self.config().wal_file_bytes,
            &plans,
        )
    }

    pub fn seal_epoch(&self, authority: RequestAuthority) -> Result<(), StorageError> {
        self.seal_epoch_impl(authority, || {
            Err(StorageError::Invalid(
                "live WAL seal requires target revocation",
            ))
        })
    }

    #[cfg(feature = "experimental-nvmeof")]
    pub fn seal_live_epoch(
        &self,
        authority: RequestAuthority,
        revoke_and_drain: impl FnOnce() -> Result<(), StorageError>,
    ) -> Result<(), StorageError> {
        self.seal_epoch_impl(authority, revoke_and_drain)
    }

    fn seal_epoch_impl(
        &self,
        authority: RequestAuthority,
        revoke_and_drain: impl FnOnce() -> Result<(), StorageError>,
    ) -> Result<(), StorageError> {
        if authority.epoch != self.config().inventory.epoch {
            return Err(StorageError::Fenced);
        }
        let _persistence = self.persistence.lock();
        let mut state = self.state.lock();
        if state.sealed {
            return if state.poison.is_some() || state.references.background_failed() {
                Err(StorageError::Poisoned)
            } else {
                Ok(())
            };
        }
        #[cfg(feature = "experimental-nvmeof")]
        if state.live {
            revoke_and_drain()?;
            if state.pending_live.is_some() {
                state.poison = Some(WriterPoison::Fatal);
                return Err(StorageError::Invalid(
                    "unknown live reservation requires recovery",
                ));
            }
            if state.appended_end != state.durable_end {
                state.poison = Some(WriterPoison::Fatal);
                return Err(StorageError::Invalid(
                    "unacknowledged live suffix requires recovery",
                ));
            }
        }
        #[cfg(not(feature = "experimental-nvmeof"))]
        let _ = revoke_and_drain;
        if state.poison.is_some() || state.references.background_failed() {
            return Err(StorageError::Poisoned);
        }
        // Stop writer workers while retaining exclusive ownership of the path.
        drop(state.writer.take());
        state.sealed = true; // I/O errors must never reopen this writer.
        if let Err(error) = self.wal.persist_registered_files() {
            state.poison = Some(WriterPoison::Fatal);
            return Err(StorageError::Unknown(WalError::Io(error)));
        }
        let result = (|| -> Result<SealedEnd, StorageError> {
            let file_count =
                highest_wal_end(&self.path, self.config())?.div_ceil(self.config().wal_file_bytes);
            state.references.persist_pending(file_count)?;
            file_inventory::remove_covered(&self.path, self.references.root().file_count)?;
            let end = SealedEnd {
                format: 5,
                epoch: authority.epoch,
                generation: self.config().inventory.segment_generation,
                frame_count: self.references.len(),
                physical_end: physical_end(&state),
            };
            install_metadata(&self.path, SEAL, &encode_metadata(&end)?)?;
            Ok(end)
        })();
        let end = result.map_err(|error| {
            state.poison = Some(WriterPoison::Fatal);
            StorageError::Unknown(WalError::Io(io::Error::other(error)))
        })?;
        state.durable_end = end.physical_end;
        self.durable_frontier
            .store(end.physical_end, Ordering::Release);
        Ok(())
    }
}

/// Fail closed unless every registered partition is supplied and recovered.
/// Returns per-node sealed logs, not a published DB or resolved logical prefix.
/// A failed call may already have sealed reachable nodes; retrying is safe.
pub fn recover_inventory(
    inventory: &StaticInventory,
    entries: &[(PathBuf, PartitionConfig)],
    metrics: Arc<Metrics>,
) -> Result<Vec<PartitionLog>, StorageError> {
    inventory.validate()?;
    let mut supplied: Vec<_> = entries.iter().map(|(_, c)| c.partition).collect();
    supplied.sort_unstable();
    if supplied != inventory.partitions || entries.iter().any(|(_, c)| c.inventory != *inventory) {
        return Err(StorageError::Invalid(
            "incomplete or mismatched recovery inventory",
        ));
    }
    let logs: Vec<_> = entries
        .iter()
        .map(|(path, config)| PartitionLog::recover_sealed(path, config, metrics.clone()))
        .collect::<Result<_, _>>()?;
    for log in &logs {
        if log.registration.shape_yaml != logs[0].registration.shape_yaml {
            return Err(StorageError::Invalid(
                "inconsistent database schema in registered inventory",
            ));
        }
        let mut start = 0;
        while start < log.receipt_count() {
            let page = log.receipt_page(start, 256)?;
            for receipt in &page {
                for other in &logs {
                    if other.config().partition != log.config().partition
                        && other.lookup_version(receipt.version())?.is_some()
                    {
                        return Err(StorageError::VersionConflict);
                    }
                }
            }
            start += page.len();
        }
    }
    Ok(logs)
}

fn validate_batch(config: &PartitionConfig, batch: &CompleteBatch) -> Result<(), StorageError> {
    if batch.version().epoch != config.inventory.epoch {
        return Err(StorageError::Fenced);
    }
    if config.inventory.partition_for(batch.id())? != config.partition {
        return Err(StorageError::WrongPartition);
    }
    Ok(())
}

fn member_position(group: WalPosition, member_offset: u32, batch: &CompleteBatch) -> WalPosition {
    // CompleteGroup has validated bounds and alignment; native allocation fits
    // the outer frame inside one fragment, so every child shares that WAL file.
    WalPosition::new(
        group.offset() + 8 + u64::from(member_offset),
        (batch.encoded().len() + 8) as u32,
    )
}

fn receipt_for(config: &PartitionConfig, batch: &CompleteBatch, pos: WalPosition) -> AppendedBatch {
    AppendedBatch {
        id: batch.id(),
        version: batch.version(),
        digest: batch.digest(),
        address: PhysicalValueAddress {
            partition: config.partition,
            segment_generation: config.inventory.segment_generation,
            offset: pos.offset(),
            len: pos.frame_len_u32(),
        },
    }
}

fn pending_persistence(state: &State, receipt: AppendedBatch) -> Result<Option<u64>, StorageError> {
    let end = state.references.receipt_group_end(receipt)?;
    if state.poison.is_some() || state.references.background_failed() {
        return Err(StorageError::Poisoned);
    }
    Ok((end > state.durable_end).then_some(end))
}

fn pending_receipts(
    state: &State,
    receipts: &[AppendedBatch],
) -> Result<Option<u64>, StorageError> {
    let mut pending_end = None;
    for &receipt in receipts {
        if let Some(end) = pending_persistence(state, receipt)? {
            pending_end = Some(pending_end.map_or(end, |previous: u64| previous.max(end)));
        }
    }
    Ok(pending_end)
}

/// What one persistence call must make durable.
#[derive(Clone, Copy)]
enum PersistScope<'a> {
    /// Exactly these receipts' groups (and every earlier appended group).
    Receipts(&'a [AppendedBatch]),
    /// Everything appended when the barrier captures the frontier.
    Appended,
}
impl PersistScope<'_> {
    fn len(&self) -> Option<usize> {
        match self {
            Self::Receipts(receipts) => Some(receipts.len()),
            Self::Appended => None,
        }
    }
}

fn persistence_poisoned(state: &State, _scope: PersistScope<'_>) -> bool {
    let poisoned = match state.poison {
        None => false,
        #[cfg(feature = "experimental-nvmeof")]
        Some(WriterPoison::IncompleteLiveFrame) => !matches!(_scope, PersistScope::Appended),
        Some(WriterPoison::Fatal) => true,
    };
    poisoned || state.references.background_failed()
}

fn pending_scope(state: &State, scope: PersistScope<'_>) -> Result<Option<u64>, StorageError> {
    match scope {
        PersistScope::Receipts(receipts) => pending_receipts(state, receipts),
        PersistScope::Appended => {
            if persistence_poisoned(state, scope) {
                return Err(StorageError::Poisoned);
            }
            Ok((state.appended_end > state.durable_end).then_some(state.appended_end))
        }
    }
}

fn physical_end(state: &State) -> u64 {
    state.appended_end
}

pub(crate) fn encode_metadata(value: &impl Serialize) -> Result<Vec<u8>, StorageError> {
    let bytes = serde_yaml::to_string(value)
        .map_err(|_| StorageError::Invalid("metadata serialization failed"))?
        .into_bytes();
    if bytes.len() as u64 + 8 > MAX_METADATA_BYTES {
        return Err(StorageError::Invalid("metadata exceeds limit"));
    }
    Ok(CrcFrame::new(&bytes).as_ref().to_vec())
}

/// Commit the CRC-protected live durability frontier after the WAL file and
/// its namespace witnesses, before returning any durable receipt.
#[cfg(feature = "experimental-nvmeof")]
impl PartitionLog {
    fn persist_live_frontier(
        &self,
        frontier: &LiveFrontier,
        timing: &mut WriteTiming<'_>,
        syncs: &mut (u64, u64),
    ) -> io::Result<()> {
        let path = &self.path;
        let bytes = encode_metadata(frontier).map_err(io::Error::other)?;
        let temporary = path.join(format!(".{LIVE_FRONTIER}.pending"));
        timing.stage(Some(WriteStage::LiveFrontierFile));
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temporary)?;
        file.write_all(&bytes)?;
        timing.record(|profile| {
            profile.live_frontier_file_calls = profile.live_frontier_file_calls.saturating_add(1)
        });
        #[cfg(test)]
        if self.fail_live_frontier.load(Ordering::Acquire) == 1 {
            return Err(io::Error::other("injected live frontier file sync failure"));
        }
        file.sync_all()?;
        syncs.0 += 1;
        timing.record(|profile| {
            profile.live_frontier_file_successes =
                profile.live_frontier_file_successes.saturating_add(1)
        });
        timing.stage(None);
        #[cfg(test)]
        if self.fail_live_frontier.load(Ordering::Acquire) == 2 {
            return Err(io::Error::other("injected live frontier rename failure"));
        }
        fs::rename(temporary, path.join(LIVE_FRONTIER))?;
        timing.stage(Some(WriteStage::LiveFrontierNamespace));
        timing.record(|profile| {
            profile.live_frontier_namespace_calls =
                profile.live_frontier_namespace_calls.saturating_add(1)
        });
        #[cfg(test)]
        if self.fail_live_frontier.load(Ordering::Acquire) == 3 {
            return Err(io::Error::other(
                "injected live frontier directory sync failure",
            ));
        }
        File::open(path)?.sync_all()?;
        syncs.1 += 1;
        timing.record(|profile| {
            profile.live_frontier_namespace_successes =
                profile.live_frontier_namespace_successes.saturating_add(1)
        });
        timing.stage(None);
        Ok(())
    }
}

pub(crate) fn read_metadata<T: serde::de::DeserializeOwned>(
    path: &Path,
) -> Result<T, StorageError> {
    let file = File::open(path)?;
    if file.metadata()?.len() > MAX_METADATA_BYTES {
        return Err(StorageError::Invalid("metadata exceeds limit"));
    }
    let mut bytes = Vec::new();
    file.take(MAX_METADATA_BYTES + 1).read_to_end(&mut bytes)?;
    let payload = CrcFrame::read_from_slice(&bytes, 0)
        .map_err(|_| StorageError::Invalid("metadata CRC invalid"))?;
    if payload.len() + 8 != bytes.len() {
        return Err(StorageError::Invalid("metadata trailing bytes"));
    }
    serde_yaml::from_slice(payload).map_err(|_| StorageError::Invalid("metadata format invalid"))
}

pub(crate) fn install_metadata(path: &Path, name: &str, bytes: &[u8]) -> io::Result<()> {
    let temporary = path.join(format!(".{name}.pending"));
    // Exclusive directory ownership makes replacing an interrupted temporary
    // safe; the authoritative file is only changed by a synced rename.
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    fs::rename(temporary, path.join(name))?;
    File::open(path)?.sync_all()
}

fn wal_file_entry(
    entry: &fs::DirEntry,
    config: &PartitionConfig,
) -> Result<Option<(u64, u64)>, StorageError> {
    let name = entry.file_name();
    let name = name
        .to_str()
        .ok_or(StorageError::Invalid("non-UTF8 log filename"))?;
    if !name.starts_with("wal") {
        return Ok(None);
    }
    let name = name
        .strip_prefix("wal_")
        .ok_or(StorageError::Invalid("invalid WAL filename"))?;
    if name.len() != 16
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(StorageError::Invalid("noncanonical WAL filename"));
    }
    let id =
        u64::from_str_radix(name, 16).map_err(|_| StorageError::Invalid("invalid WAL filename"))?;
    let metadata = entry.metadata()?;
    if !entry.file_type()?.is_file() || metadata.len() > config.wal_file_bytes {
        return Err(StorageError::Invalid("invalid WAL file"));
    }
    id.checked_mul(config.wal_file_bytes)
        .ok_or(StorageError::Invalid("WAL file offset overflow"))?;
    Ok(Some((id, metadata.len())))
}

fn highest_wal_end(path: &Path, config: &PartitionConfig) -> Result<u64, StorageError> {
    let mut end = 0;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let Some((id, length)) = wal_file_entry(&entry, config)? else {
            continue;
        };
        let length = length
            .max(config.fragment_bytes)
            .div_ceil(config.fragment_bytes)
            .checked_mul(config.fragment_bytes)
            .ok_or(StorageError::Invalid("WAL length overflow"))?;
        end = end.max(
            id.checked_mul(config.wal_file_bytes)
                .and_then(|base| base.checked_add(length))
                .ok_or(StorageError::Invalid("WAL file offset overflow"))?,
        );
    }
    Ok(end)
}

fn validate_file_inventory(
    path: &Path,
    config: &PartitionConfig,
    references: &ReferenceDirectory,
) -> Result<u64, StorageError> {
    let root = references.root();
    let retired_count = u64::try_from(root.retired_count).map_err(|_| StorageError::Capacity)?;
    let expected_retained =
        root.file_count
            .checked_sub(retired_count)
            .ok_or(StorageError::Invalid(
                "retired file count exceeds rooted inventory",
            ))?;
    if root.file_count == 0 {
        return Err(StorageError::Invalid("missing original WAL inventory"));
    }
    let mut retained = 0u64;
    let mut suffix_count = 0u64;
    let mut suffix_last = None;
    let mut end = 0;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let Some((id, length)) = wal_file_entry(&entry, config)? else {
            continue;
        };
        let offset = id * config.wal_file_bytes;
        if references.is_retired(offset)? {
            if id >= root.file_count {
                return Err(StorageError::Invalid(
                    "retirement exceeds rooted file inventory",
                ));
            }
            // A pending retired name is not evidence of another retained file.
            continue;
        }
        if id < root.file_count {
            retained = retained.checked_add(1).ok_or(StorageError::Capacity)?;
        } else {
            suffix_count = suffix_count.checked_add(1).ok_or(StorageError::Capacity)?;
            suffix_last = Some(suffix_last.map_or(id, |last: u64| last.max(id)));
        }
        let length = length
            .max(config.fragment_bytes)
            .div_ceil(config.fragment_bytes)
            .checked_mul(config.fragment_bytes)
            .ok_or(StorageError::Capacity)?;
        end = end.max(offset.checked_add(length).ok_or(StorageError::Capacity)?);
    }
    // Canonical filenames are unique. Below the root, exactly its nonretired
    // IDs must exist; an extra suffix file cannot compensate for a root-range
    // loss. Above it, count+maximum prove a contiguous independently bounded run.
    if retained != expected_retained {
        return Err(StorageError::Invalid(
            "missing registered WAL file; cannot prove absence",
        ));
    }
    if suffix_count > config.max_batches.min(1024) as u64 + 16
        || suffix_last
            .is_some_and(|last| last.checked_add(1) != root.file_count.checked_add(suffix_count))
    {
        return Err(StorageError::Invalid(
            "unrooted WAL suffix has gaps or exceeds recovery bound",
        ));
    }
    Ok(end)
}

#[derive(Debug)]
pub enum StorageError {
    Io(io::Error),
    Wal(WalError),
    Codec(CodecError),
    Group(GroupError),
    Invalid(&'static str),
    WrongDatabase,
    WrongPartition,
    Fenced,
    Sealed,
    IdentityConflict,
    VersionConflict,
    Capacity,
    NeedsPersistence,
    MixedRetry,
    Poisoned,
    /// Unresolved external bytes failed frame/identity validation; no no-effect
    /// result is implied. Revocation and frontier-based recovery are required.
    IncompleteLiveFrame,
    /// Effects may exist. Resolve the original identity after recovery.
    Unknown(WalError),
}

impl From<io::Error> for StorageError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}
impl From<CodecError> for StorageError {
    fn from(e: CodecError) -> Self {
        Self::Codec(e)
    }
}
impl From<GroupError> for StorageError {
    fn from(e: GroupError) -> Self {
        Self::Group(e)
    }
}
impl From<DirectoryError> for StorageError {
    fn from(error: DirectoryError) -> Self {
        match error {
            DirectoryError::Capacity => Self::Capacity,
            DirectoryError::InvalidLimits => Self::Invalid("invalid reference metadata budget"),
            DirectoryError::InvalidMetadata => {
                Self::Invalid("invalid authority reference metadata")
            }
        }
    }
}
impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if matches!(self, Self::IncompleteLiveFrame) {
            return f.write_str("incomplete live frame: outcome unknown; recovery required");
        }
        write!(f, "{self:?}")
    }
}
impl std::error::Error for StorageError {}

#[cfg(test)]
mod crash_tests;
#[cfg(test)]
mod tests;
