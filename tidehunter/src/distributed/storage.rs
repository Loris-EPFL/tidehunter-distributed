//! Native WAL-backed partition authority with bounded resident pending metadata.
//!
//! WAL replay keeps a bounded retry window; legacy mode pages receipt history.
//! Recovery permanently seals an old epoch and scans from its owner position.
//! Whole-file retirement requires a durable recovery/reachability decision;
//! no live values move and physical addresses are never reused.

use super::codec::{BatchLimits, CodecError, CompleteBatch, Mutation};
use super::durability::frontier::FrontierError;
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
pub use history::WAL_REPLAY_WINDOW_BATCHES;
mod wal_replay;
use wal_replay::WalRoot;
pub(crate) use wal_replay::aligned_replay_start;
pub use wal_replay::{DecodedGroup, ReplayPage};
mod durable_progress;
pub use durable_progress::LogDurableProgress;
use durable_progress::ProgressTail;
mod file_inventory_page;
mod local_types;
pub use local_types::{LocalDbError, LocalDbStats, LocalInventoryConfig};
mod retired_files;
pub use retired_files::{FileSeal, PendingRetirement};
use retired_files::{RetiredFiles, validate_root_sets};
mod file_retirement;
pub(crate) use file_retirement::Cleared;
#[cfg(test)]
pub(crate) use file_retirement::CrashCut;
pub use file_retirement::{
    RetireProof, RetirementOutcome, RetirementWitness, UnlinkOutcome, WalInode,
};
mod relocation_scan;
pub use relocation_scan::{RelocationBatch, RelocationPage, RelocationPut};
mod file_seal;
mod leased_member;
mod lifecycle;
pub(crate) use leased_member::{RecordReadPlan, member_address_of_reference};
use lifecycle::Lifecycle;
mod write_profile;
pub use write_profile::{PersistenceCapture, RecordReadProfile, StorageWriteProfile};
use write_profile::{WriteStage, WriteTiming, duration_ns};

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

pub use super::reference_directory::AuthorityCacheStats;
pub use crate::wal::WalMemberReadPolicy;

/// Original engine defaults (`Config::default()` in `config.rs`).
pub const NATIVE_FRAGMENT_BYTES: u64 = 128 * 1024 * 1024;
pub const NATIVE_WAL_FILE_BYTES: u64 = 10 * (1 << 30);
const NATIVE_MAX_MAPS: usize = 16;
const RESEARCH_MAX_MAPS: usize = 3;

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
            max_maps: if self.fragment_bytes == NATIVE_FRAGMENT_BYTES {
                NATIVE_MAX_MAPS
            } else {
                RESEARCH_MAX_MAPS
            },
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

/// Read the durable sealed end used by generation trim after the log is closed.
pub(crate) fn trim_sealed_end(path: &Path, generation: u64) -> Result<Option<u64>, StorageError> {
    let seal_path = path.join(SEAL);
    match fs::symlink_metadata(&seal_path) {
        Ok(metadata) if metadata.file_type().is_file() => {}
        Ok(_) => return Err(StorageError::Invalid("invalid sealed WAL metadata")),
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    }
    let seal: SealedEnd = read_metadata(&seal_path)?;
    if seal.format != 5 || seal.epoch != generation || seal.generation != generation {
        return Err(StorageError::Invalid("sealed WAL identity mismatch"));
    }
    Ok(Some(seal.physical_end))
}

/// Persist the zero end for a never-activated generation before trim intent.
pub(crate) fn install_empty_trim_seal(path: &Path, generation: u64) -> Result<(), StorageError> {
    if let Some(end) = trim_sealed_end(path, generation)? {
        return if end == 0 {
            // A prior rename may have succeeded before its directory sync failed.
            File::open(path)?.sync_all()?;
            Ok(())
        } else {
            Err(StorageError::Invalid(
                "abandoned generation has nonzero sealed end",
            ))
        };
    }
    let seal = SealedEnd {
        format: 5,
        epoch: generation,
        generation,
        frame_count: 0,
        physical_end: 0,
    };
    install_metadata(path, SEAL, &encode_metadata(&seal)?)?;
    Ok(())
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

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
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

struct OpenLiveExtent {
    extent: super::nvmeof::live_storage::LiveWriteExtent,
    allocation: crate::wal::ExternalWalAllocation,
    adoption: super::nvmeof::live_storage::LiveAdoption,
    closing: bool,
    // Independent scratch for close and the revoked-tail seal scan.
    buffer: Vec<u8>,
    round: Arc<Mutex<AdoptionRound>>,
}

// Lock order: round gate -> state. Clone the gate under state, then release
// state BEFORE taking it. Each hold spans one bounded background round, never
// pressure work or persistence. Grants, close and position proofs do not take
// it; read/maintenance/seal scans enter without state or persistence held.
// Lifecycle FIFO ordering stays unchanged: a queued fence can wait for the
// active request, bounded by the extent's frames and retry count.
#[derive(Default)]
struct AdoptionRound {
    buffers: [Vec<u8>; 2],
    #[cfg(test)]
    active: Arc<std::sync::atomic::AtomicUsize>,
    #[cfg(test)]
    maximum_active: Arc<std::sync::atomic::AtomicUsize>,
}

const ADOPTION_MAX_FRAME_BYTES: usize = 64 * 1024 * 1024;
const ADOPTION_ROUND_FRAMES: usize = 32;
const ADOPTION_ROUND_BYTES: u64 = 8 * 1024 * 1024;
const ADOPTION_PARALLEL_BYTES: u64 = 256 * 1024;

struct PlannedLiveFrame {
    start: u64,
    len: u64,
    probe: Result<u32, StorageError>,
}

struct ValidatedLiveFrame {
    group: CompleteGroup,
    crc: u32,
}

struct PreparedLiveFrame {
    start: u64,
    len: u64,
    validated: Result<ValidatedLiveFrame, StorageError>,
}

#[derive(Clone, Copy)]
struct LiveFrameDemand {
    batches: usize,
    descriptors: usize,
    bytes: u64,
}

impl LiveFrameDemand {
    fn fits(self, state: &State, root_end: u64) -> bool {
        state
            .references
            .check_capacity(self.batches, self.descriptors)
            .is_ok()
            && state.appended_end.saturating_sub(root_end) + self.bytes <= 64 * 1024 * 1024
    }
}

enum LiveFrameStep {
    Adopted,
    End,
    Pressure(LiveFrameDemand),
}

#[cfg(test)]
#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) enum LivePausePoint {
    ScanSnapshot,
    FrameProbe,
    ValidateUnlocked,
    ValidatedBeforePublish,
    CloseSuffix,
    AfterFrame,
    PositionBeforeData,
    PositionBeforeRoot,
    PositionRootLocked,
    PositionAfterRoot,
    LeaseWaitingForPersistence,
    PrepareBeforePublish,
    ColdReadBeforeSync,
    ColdReadAfterSync,
    ColdReadBeforePin,
    ColdReadAfterPin,
    ReadPinBeforeIncrement,
    ProgressBeforePersist,
    SyncerBeforePersist,
    RetirementBeforePersistence,
    AppendedPersistenceLocked,
    RetirementPersistenceLocked,
}

#[cfg(test)]
type LivePauseHook = (
    LivePausePoint,
    std::sync::mpsc::SyncSender<()>,
    std::sync::mpsc::Receiver<()>,
);

#[derive(Clone, Copy, Eq, PartialEq)]
enum WriterPoison {
    // Validation failed before publication. Only the earlier appended prefix
    // may still be synced; the unresolved external allocation stays pinned.
    IncompleteLiveFrame,
    Fatal,
}

struct State {
    // This must drop before writer: the tracker can be waiting for this pin.
    open_live_extent: Option<OpenLiveExtent>,
    next_live_extent: Option<OpenLiveExtent>,
    last_live_adoption: Option<super::nvmeof::live_storage::LiveAdoption>,
    live: bool,
    lifecycle: Lifecycle,
    live_tail: Option<LiveFrontierTail>,
    /// End below which this handle owes no live-frontier root. Zero after
    /// create, so the first barrier or fragment crossing installs a root. After
    /// recovery it is the recovered end: recovery leaves that end described by
    /// its own durable roots and seal, and a sealed handle has no live tail.
    live_root_end: u64,
    // Writer drops before the directory lock. The experimental feature strictly
    // joins its native workers; shutdown can wait but cannot release a live path.
    writer: Option<WalWriter>,
    // Shared immutable snapshot: lock holders clone the `Arc`, never the seal
    // set. Writers copy it outside `state` under `persistence`, then swap it.
    wal_root: Option<Arc<WalRoot>>,
    references: ReferenceDirectoryWriter,
    appended_end: u64,
    durable_end: u64,
    // Versions at the appended and durable ends; moves with them under this lock.
    progress: ProgressTail,
    // Proof owned by this append-only handle, never a highest allocated file.
    // A new file requires a new namespace barrier. Rooted history permits
    // sealed whole-file retirement, never physical address reuse.
    namespace_synced_file: Option<crate::wal::position::WalFileId>,
    poison: Option<WriterPoison>,
    sealed: bool,
    stats: PartitionStats,
    live_file_preparation: LiveFilePreparationStats,
    #[cfg(test)]
    live_test_diagnostics: LiveTestDiagnostics,
}

/// Counters for this open handle, not recovered durability or publication proof.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct PartitionStats {
    pub appended_groups: u64,
    /// Completed explicit per-group file persistence calls. Creation and sealing
    /// also perform I/O, and background native syncs are not counted here.
    pub persistence_barriers: u64,
    /// Successful explicit WAL-directory syncs for used or prepared files.
    /// Creation, sealing and recovery namespace I/O are excluded.
    pub namespace_syncs: u64,
    /// Completed immutable used/prepared-file inventory witness file+namespace pairs.
    /// Separate from native WAL barriers; first-file ACKs pay this extra cost.
    pub inventory_witnesses: u64,
    /// Successful live-frontier file and directory syncs, including attempts
    /// whose subsequent publication failed; creation/recovery are excluded.
    pub live_frontier_file_syncs: u64,
    pub live_frontier_namespace_syncs: u64,
    /// WAL file data syncs (fdatasync) of completed persistence barriers; a
    /// failed barrier does not add to it. Cold-read scoped proofs, creation and
    /// recovery are excluded.
    pub wal_file_syncs: u64,
    /// Live-frontier persistence requests made by owner adoption control.
    pub live_frontier_requests: u64,
    /// Completed live-frontier root installs (temporary file write and sync,
    /// rename, directory sync) and the root file bytes they wrote.
    pub live_frontier_installs: u64,
    pub live_frontier_bytes: u64,
    /// Syncer barriers that synced the WAL and deferred the live root within a
    /// fragment (bounded policy; WAL-replay history only).
    pub live_frontier_deferred: u64,
    /// Completed on-demand data barriers requested by owner reads beyond the
    /// durable prefix (a barrier finding nothing pending also counts).
    pub read_persist_requests: u64,
    /// Scoped cold-grant fdatasyncs of a pinned WAL inode; they never advance
    /// the durable prefix, so they can repeat until a barrier does.
    pub cold_grant_syncs: u64,
    /// Currently charged resident/cache/read-CQ descriptor records, not disk total.
    pub registered_records: usize,
    pub reference_metadata_bytes: usize,
}

/// Owner-local preparation counters; not carried by the statistics protocol.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LiveFilePreparationStats {
    pub live_files_prepared: u64,
    pub live_files_published: u64,
    pub live_file_prepare_failures: u64,
    pub live_file_prewrite_bytes: u64,
    pub live_file_prewrite_ns: u64,
    pub live_file_prewrite_files: u64,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
pub(crate) struct LiveTestLeaseTiming {
    pub persistence_wait_ns: u64,
    pub state_wait_ns: u64,
    pub inventory_wait_ns: u64,
    pub publication_call_ns: u64,
    pub lookup_inventory_wait_ns: u64,
}

#[cfg(test)]
#[derive(Clone, Debug, serde::Serialize)]
pub(crate) struct LiveTestExport {
    file_index: u64,
    inventory_attempts: u64,
    export_ns: u64,
    success: bool,
}

#[cfg(test)]
#[derive(Clone, Debug, Default, serde::Serialize)]
pub(crate) struct LiveTestDiagnostics {
    lease: LiveTestLeaseTiming,
    exports: Vec<LiveTestExport>,
    dropped_exports: u64,
    pub adoption: LiveTestAdoption,
    cold_read_syncs: u64,
}

#[cfg(test)]
#[derive(Clone, Debug, Default, serde::Serialize)]
pub(crate) struct LiveTestAdoption {
    pub bytes: u64,
    pub frames: u64,
    pub rounds: u64,
    pub parallel_rounds: u64,
    pub validate_worker_ns: u64,
    pub validate_wall_ns: u64,
    pub publish_state_ns: u64,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BusyCause {
    RecordPin,
    ExportPin,
    PendingCapacity,
    Other(&'static str),
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BusyEvent {
    pub epoch: u64,
    pub file: u64,
    pub cause: BusyCause,
}

#[cfg(test)]
#[derive(Default)]
struct BusyEvents {
    events: std::collections::VecDeque<BusyEvent>,
    dropped: u64,
}

#[cfg(test)]
impl BusyEvents {
    fn record(&mut self, event: BusyEvent) {
        if self.events.len() == 64 {
            self.events.pop_front();
            self.dropped = self.dropped.saturating_add(1);
        }
        self.events.push_back(event);
    }
}

#[cfg(test)]
mod busy_events_tests;

/// A local storage node component; caller threads serialize appends per partition.
/// Different instances/partitions may append concurrently.
pub struct PartitionLog {
    #[cfg(test)]
    busy_events: Mutex<BusyEvents>,
    state: Mutex<State>,
    references: Arc<ReferenceDirectory>,
    durable_frontier: AtomicU64,
    // A successful sealed-stream page proves its next cursor is a frame boundary.
    replay_next: AtomicU64,
    #[cfg(test)]
    replay_decodes: AtomicU64,
    // Appended/synced frontier of the durability layer. Set once by the node's
    // control owner, which also owns the single syncer thread. Appends publish
    // into it under `state`, so the frontier follows WAL order exactly.
    frontier: std::sync::OnceLock<Arc<super::durability::frontier::Frontier>>,
    // Stage timings of background syncs, taken by whoever waits for them.
    sync_profile: Mutex<StorageWriteProfile>,
    // Native tier: readers may use groups that are appended but not yet synced.
    serve_unsynced: AtomicBool,
    // Serialize disk barriers/sealing without blocking immutable reference reads.
    persistence: Mutex<()>,
    // New file pins serialize only with retirement, never ordinary WAL fsync.
    // False permanently closes read admission after an interrupted retirement.
    read_admission: parking_lot::RwLock<bool>,
    live_read_pins: std::sync::atomic::AtomicUsize,
    live_stream: bool,
    // Recovery of this handle stopped at a lost, torn or invalid frame and a valid
    // frame followed later: a hole below the recovered tail. Owner continuity input.
    recovery_hole: bool,
    #[cfg(test)]
    fail_inventory_witness: AtomicU8,
    #[cfg(test)]
    fail_wal_root: AtomicU8,
    #[cfg(test)]
    fail_wal_retire: AtomicBool,
    // Test-only crash cut of seal/retirement (`file_retirement::CrashCut` code).
    #[cfg(test)]
    r1_crash: AtomicU8,
    // Pending cleanup entries whose unlink this process's recovery repeated,
    // with what it found; taken once by the R1 event stream (else unused).
    recovered_cleanup: Mutex<Vec<(PendingRetirement, UnlinkOutcome)>>,
    // Test-only pause after the retirement detach, before the unlinks (no
    // lock held): writes must proceed while a retirement is mid-flight.
    #[cfg(test)]
    before_unlink: file_seal::TestPause,
    // Test-only pause before a seal's file sync (no lock held).
    #[cfg(test)]
    before_seal_sync: file_seal::TestPause,
    #[cfg(test)]
    before_persist: file_seal::TestPause,
    #[cfg(test)]
    fail_persist: AtomicBool,
    #[cfg(test)]
    fail_live_progress: AtomicBool,
    #[cfg(test)]
    failed_live_progress: AtomicBool,
    #[cfg(test)]
    syncer_persist_hold_reached: AtomicBool,
    #[cfg(test)]
    fail_sealed_before_root: AtomicBool,
    #[cfg(test)]
    fail_appended_persist: AtomicBool,
    #[cfg(test)]
    failed_appended_persist: AtomicBool,
    #[cfg(test)]
    fail_namespace_persist: AtomicBool,
    #[cfg(test)]
    fail_live_frontier: AtomicU8,
    // Test handshake owner: the selected live operation; capacity one in each
    // direction, blocking send/recv, dropping either peer unblocks shutdown.
    #[cfg(test)]
    live_pause: Mutex<Option<LivePauseHook>>,
    #[cfg(test)]
    fail_after_live_publish: AtomicBool,
    wal: Arc<Wal>,
    registration: Registration,
    shape: KeyShape,
    path: PathBuf,
    _lock: DbLock,
    // A local database root cannot reopen while any partition/worker handle
    // survives. Drop this after the partition lock and all native WAL workers.
    _local_authority: Option<Arc<DbLock>>,
}

impl PartitionLog {
    #[cfg(test)]
    pub(crate) fn record_busy(&self, file: u64, cause: BusyCause) {
        self.busy_events.lock().record(BusyEvent {
            epoch: self.config().inventory.epoch,
            file,
            cause,
        });
    }

    /// Drain this log's bounded ring and reset its drop count. Events are
    /// recorded only where retirement returns `Busy`; a pin walk records the
    /// first blocker. The idle cleanup deferral is not a `Busy` and not recorded.
    #[cfg(test)]
    pub(crate) fn take_busy_events(&self) -> (Vec<BusyEvent>, u64) {
        let mut events = self.busy_events.lock();
        (
            events.events.drain(..).collect(),
            std::mem::take(&mut events.dropped),
        )
    }

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
        Self::create_inner(path, config, shape, metrics, false)
    }

    fn create_inner(
        path: &Path,
        config: PartitionConfig,
        shape: &KeyShape,
        metrics: Arc<Metrics>,
        prewrite: bool,
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
        let (references, reference_writer) = ReferenceDirectory::open(&registration.config)?;
        let wal = Wal::open_owner(&path, registration.config.layout(), metrics, prewrite)?;
        wal.persist_registered_files()?;
        install_metadata(&path, REGISTRATION, &metadata)?;
        let wal_root = WalRoot::new(&registration.config);
        wal_root.install(&path)?;
        let writer = wal.writer_after(None).map_err(StorageError::Wal)?;
        Ok(Self {
            #[cfg(test)]
            busy_events: Mutex::new(BusyEvents::default()),
            state: Mutex::new(State {
                open_live_extent: None,
                next_live_extent: None,
                last_live_adoption: None,
                live: false,
                lifecycle: Lifecycle::Running,
                live_tail: None,
                live_root_end: 0,
                writer: Some(writer),
                wal_root: Some(Arc::new(wal_root)),
                references: reference_writer,
                appended_end: 0,
                durable_end: 0,
                progress: ProgressTail::default(),
                namespace_synced_file: None,
                poison: None,
                sealed: false,
                stats: PartitionStats::default(),
                live_file_preparation: LiveFilePreparationStats::default(),
                #[cfg(test)]
                live_test_diagnostics: LiveTestDiagnostics::default(),
            }),
            references,
            durable_frontier: AtomicU64::new(0),
            replay_next: AtomicU64::new(u64::MAX),
            #[cfg(test)]
            replay_decodes: AtomicU64::new(0),
            frontier: std::sync::OnceLock::new(),
            sync_profile: Mutex::new(StorageWriteProfile::default()),
            serve_unsynced: AtomicBool::new(false),
            persistence: Mutex::new(()),
            read_admission: parking_lot::RwLock::new(true),
            live_read_pins: std::sync::atomic::AtomicUsize::new(0),
            live_stream: false,
            recovery_hole: false,
            #[cfg(test)]
            fail_inventory_witness: AtomicU8::new(0),
            #[cfg(test)]
            fail_wal_root: AtomicU8::new(0),
            #[cfg(test)]
            fail_wal_retire: AtomicBool::new(false),
            #[cfg(test)]
            r1_crash: AtomicU8::new(0),
            recovered_cleanup: Mutex::new(Vec::new()),
            #[cfg(test)]
            before_unlink: Mutex::new(None),
            #[cfg(test)]
            before_seal_sync: Mutex::new(None),
            #[cfg(test)]
            before_persist: Mutex::new(None),
            #[cfg(test)]
            fail_persist: AtomicBool::new(false),
            #[cfg(test)]
            fail_live_progress: AtomicBool::new(false),
            #[cfg(test)]
            failed_live_progress: AtomicBool::new(false),
            #[cfg(test)]
            syncer_persist_hold_reached: AtomicBool::new(false),
            #[cfg(test)]
            fail_sealed_before_root: AtomicBool::new(false),
            #[cfg(test)]
            fail_appended_persist: AtomicBool::new(false),
            #[cfg(test)]
            failed_appended_persist: AtomicBool::new(false),
            #[cfg(test)]
            fail_namespace_persist: AtomicBool::new(false),
            #[cfg(test)]
            fail_live_frontier: AtomicU8::new(0),
            #[cfg(test)]
            live_pause: Mutex::new(None),
            #[cfg(test)]
            fail_after_live_publish: AtomicBool::new(false),
            wal,
            registration,
            shape,
            path,
            _lock: lock,
            _local_authority: None,
        })
    }

    /// Create an exclusively block-aligned external-write stream. Selection is
    /// persisted before the handle can be exposed to a target manager.
    pub fn create_live(
        path: &Path,
        config: PartitionConfig,
        shape: &KeyShape,
        metrics: Arc<Metrics>,
    ) -> Result<Self, StorageError> {
        Self::create_live_with_prewrite(path, config, shape, metrics, false)
    }

    pub(crate) fn create_live_with_prewrite(
        path: &Path,
        config: PartitionConfig,
        shape: &KeyShape,
        metrics: Arc<Metrics>,
        prewrite: bool,
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
        let mut log = Self::create_inner(path, config, shape, metrics, prewrite)?;
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
        // WAL replay is the only history; a stream without its root used the
        // removed per-batch on-disk history.
        if !path.join(wal_replay::ROOT).exists() {
            return Err(StorageError::Invalid(
                "unsupported legacy distributed mode on_disk: use archive/multi-design",
            ));
        }
        Self::recover_wal_replay(path, registration, shape, lock, metrics, live)
    }

    pub fn config(&self) -> &PartitionConfig {
        &self.registration.config
    }

    #[cfg(all(target_os = "linux", feature = "test-utils"))]
    pub(crate) fn read_evidence_for_benchmark(
        &self,
        evict: bool,
    ) -> std::io::Result<crate::read_probe::ReadEvidence> {
        if self.live_stream {
            let state = self.state.lock();
            if !state.sealed
                && (state.open_live_extent.is_some() || state.next_live_extent.is_some())
            {
                return Err(io::Error::other(
                    "mapped residency probe unavailable during live writes",
                ));
            }
        }
        self.wal.read_evidence_for_benchmark(evict)
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
    #[cfg(test)]
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
    pub fn can_release_live_segment(&self, file_index: u64) -> Result<bool, StorageError> {
        let state = self.state.lock();
        if !state.live {
            return Err(StorageError::Invalid("not a live WAL stream"));
        }
        if state.poison.is_some() {
            return Err(StorageError::Poisoned);
        }
        if state
            .open_live_extent
            .as_ref()
            .is_some_and(|open| open.extent.segment.file_index == file_index)
            || state
                .next_live_extent
                .as_ref()
                .is_some_and(|open| open.extent.segment.file_index == file_index)
        {
            return Ok(false);
        }
        Ok(match state.writer.as_ref() {
            Some(writer) => file_index < writer.position() / self.config().wal_file_bytes,
            None => state.sealed,
        })
    }

    /// Grant exactly one native fragment remainder to the single live writer.
    pub fn lease_live_extent(
        &self,
        authority: RequestAuthority,
    ) -> Result<
        (
            super::nvmeof::live_storage::LiveWriteExtent,
            super::tcp::LeaseProfile,
        ),
        StorageError,
    > {
        self.lease_live_extent_impl(authority, None)
    }

    pub fn prelease_live_extent(
        &self,
        authority: RequestAuthority,
        after: [u8; 16],
    ) -> Result<
        (
            super::nvmeof::live_storage::LiveWriteExtent,
            super::tcp::LeaseProfile,
        ),
        StorageError,
    > {
        self.lease_live_extent_impl(authority, Some(after))
    }

    /// Prepare after the durable namespace highwater: at most one file beyond the
    /// allocator-position file (two beyond the last grant with two-fragment files).
    /// Allocation/file sync run unlocked; publication uses persistence -> state.
    pub fn prepare_next_live_file(&self) -> Result<Option<u64>, StorageError> {
        use crate::wal::position::WalFileId;
        let config = self.config();
        let live = |state: &State| {
            state.live
                && !state.sealed
                && !state.is_sealing()
                && state.poison.is_none()
                && state.writer.is_some()
        };
        let eligible_target = |state: &State| {
            if !live(state) {
                return None;
            }
            let target = state.namespace_synced_file?.as_u64() + 1;
            let position = state.writer.as_ref()?.position();
            let file = position / config.wal_file_bytes;
            let near_end = file == target - 1
                && target * config.wal_file_bytes - position <= 2 * config.fragment_bytes;
            (near_end || file == target).then_some(target)
        };
        // State alone; never wait for the mapper while holding either owner lock.
        let (target, preparer) = {
            let state = self.state.lock();
            let Some(target) = eligible_target(&state) else {
                return Ok(None);
            };
            let Some(writer) = state.writer.as_ref() else {
                return Ok(None);
            };
            (target, writer.external_file_preparer())
        };
        let prepare_ensure_span = super::owner_events::OwnerEventSpan::new(
            super::owner_events::OwnerEventKind::PrepareEnsureStart,
            super::owner_events::OwnerEventKind::PrepareEnsureEnd,
            config.partition,
            target,
            0,
        );
        let allocated = preparer.ensure(WalFileId::from_u64(target));
        prepare_ensure_span.end_at(target);
        // Release the sender before seal drops the writer and joins the mapper.
        drop(preparer);
        if allocated.is_err() {
            self.state
                .lock()
                .live_file_preparation
                .live_file_prepare_failures += 1;
            return Ok(None);
        }
        let prepare_file_sync_span = super::owner_events::OwnerEventSpan::new(
            super::owner_events::OwnerEventKind::PrepareFileSyncStart,
            super::owner_events::OwnerEventKind::PrepareFileSyncEnd,
            config.partition,
            target,
            0,
        );
        if let Err(error) = self
            .wal
            .persist_frame_file(WalPosition::new(target * config.wal_file_bytes, 1))
        {
            self.state.lock().poison = Some(WriterPoison::Fatal);
            return Err(StorageError::Unknown(WalError::Io(error)));
        }
        prepare_file_sync_span.end_at(target);
        #[cfg(test)]
        self.pause_live_at(LivePausePoint::PrepareBeforePublish);
        let _persistence = self.persistence.lock();
        {
            let state = self.state.lock();
            // A prelease can move position into target while its name remains
            // unpublished. Only a newer durable namespace publication obsoletes us.
            if !live(&state) || state.namespace_synced_file != Some(WalFileId::from_u64(target - 1))
            {
                return Ok(None);
            }
        }
        let mut timing = WriteTiming::new(None, false);
        let mut witnesses = 0;
        let prepare_witness_span = super::owner_events::OwnerEventSpan::new(
            super::owner_events::OwnerEventKind::PrepareWitnessStart,
            super::owner_events::OwnerEventKind::PrepareWitnessEnd,
            config.partition,
            target,
            0,
        );
        if let Err(error) =
            self.persist_namespace_witnesses(target, Some(target - 1), &mut timing, &mut witnesses)
        {
            self.state.lock().poison = Some(WriterPoison::Fatal);
            return Err(StorageError::Unknown(WalError::Io(error)));
        }
        prepare_witness_span.end_at(target);
        let mut state = self.state.lock();
        if !live(&state) || state.namespace_synced_file != Some(WalFileId::from_u64(target - 1)) {
            return Ok(None);
        }
        state.namespace_synced_file = Some(WalFileId::from_u64(target));
        state.stats.namespace_syncs += 1;
        state.stats.inventory_witnesses += witnesses;
        state.live_file_preparation.live_files_prepared += 1;
        super::owner_events::record(
            super::owner_events::OwnerEventKind::PreparePublished,
            config.partition,
            target,
            0,
        );
        Ok(Some(target))
    }

    pub(crate) fn live_file_published(&self) {
        self.state.lock().live_file_preparation.live_files_published += 1;
    }

    fn lease_live_extent_impl(
        &self,
        authority: RequestAuthority,
        after: Option<[u8; 16]>,
    ) -> Result<
        (
            super::nvmeof::live_storage::LiveWriteExtent,
            super::tcp::LeaseProfile,
        ),
        StorageError,
    > {
        #[cfg(test)]
        let state_started = Instant::now();
        let mut state = self.state.lock();
        #[cfg(test)]
        let initial_state_wait_ns = duration_ns(state_started.elapsed());
        let (range, ready) = self.validate_live_extent_grant(&state, authority, after)?;
        let lease_grant_span = super::owner_events::OwnerEventSpan::new(
            super::owner_events::OwnerEventKind::LeaseGrantStart,
            super::owner_events::OwnerEventKind::LeaseGrantEnd,
            self.config().partition,
            range.start,
            0,
        );
        if ready {
            #[cfg(test)]
            {
                state.live_test_diagnostics.lease = LiveTestLeaseTiming {
                    persistence_wait_ns: 0,
                    state_wait_ns: initial_state_wait_ns,
                    ..Default::default()
                };
            }
            // Readiness proves reserve_external needs no mapper request.
            // The pinned allocator range cannot retire while this state owns it.
            let granted = self.reserve_live_extent_grant(&mut state, range, after.is_some())?;
            lease_grant_span.end_at(granted.0.start);
            return Ok(granted);
        }
        drop(state);
        #[cfg(test)]
        if self.persistence.is_locked() {
            self.pause_live_at(LivePausePoint::LeaseWaitingForPersistence);
        }
        #[cfg(test)]
        let persistence_started = Instant::now();
        let _persistence = self.persistence.lock();
        #[cfg(test)]
        let persistence_wait_ns = duration_ns(persistence_started.elapsed());
        #[cfg(test)]
        let state_started = Instant::now();
        let mut state = self.state.lock();
        #[cfg(test)]
        {
            state.live_test_diagnostics.lease = LiveTestLeaseTiming {
                persistence_wait_ns,
                state_wait_ns: initial_state_wait_ns + duration_ns(state_started.elapsed()),
                ..Default::default()
            };
        }
        // State may have changed while waiting for persistence. Recheck authority,
        // lifecycle, token, geometry and readiness before any allocator mutation.
        let (range, _ready) = self.validate_live_extent_grant(&state, authority, after)?;
        let start = range.start;
        let previous_namespace = state.namespace_synced_file.map(|file| file.as_u64());
        let (extent, mut profile) =
            self.reserve_live_extent_grant(&mut state, range, after.is_some())?;
        let last_file = extent.segment.file_index;
        drop(state);
        let mut timing = WriteTiming::new(None, false);
        let mut witnesses = 0;
        let namespace_sync = previous_namespace.is_none_or(|file| last_file > file);
        let namespace_started = Instant::now();
        let result = (|| {
            if namespace_sync {
                let lease_namespace_sync_span = super::owner_events::OwnerEventSpan::new(
                    super::owner_events::OwnerEventKind::LeaseNamespaceSyncStart,
                    super::owner_events::OwnerEventKind::LeaseNamespaceSyncEnd,
                    self.config().partition,
                    last_file,
                    0,
                );
                #[cfg(test)]
                if let Some((entered, resume)) = self.before_persist.lock().take() {
                    let _ = entered.send(());
                    let _ = resume.recv();
                }
                // No bytes have been granted yet. Persist preallocation and the
                // final inode size before making its directory witness durable.
                self.wal.persist_frame_file(WalPosition::new(start, 1))?;
                self.persist_namespace_witnesses(
                    last_file,
                    previous_namespace,
                    &mut timing,
                    &mut witnesses,
                )?;
                lease_namespace_sync_span.end_at(last_file);
            }
            Ok::<(), io::Error>(())
        })();
        profile.namespace_sync_ns = if namespace_sync {
            duration_ns(namespace_started.elapsed())
        } else {
            0
        };
        let mut state = self.state.lock();
        state.stats.inventory_witnesses += witnesses;
        result.map_err(|error| {
            state.poison = Some(WriterPoison::Fatal);
            StorageError::Unknown(WalError::Io(error))
        })?;
        if state.is_sealing() || state.sealed {
            return Err(StorageError::Invalid("live stream is sealing"));
        }
        if state.poison.is_some() {
            return Err(StorageError::Poisoned);
        }
        if namespace_sync {
            state.namespace_synced_file =
                Some(crate::wal::position::WalFileId::from_u64(last_file));
            state.stats.namespace_syncs += 1;
        }
        state.poison = None;
        lease_grant_span.end_at(extent.start);
        Ok((extent, profile))
    }

    /// Validate without changing the allocator; both grant paths use this check.
    fn validate_live_extent_grant(
        &self,
        state: &State,
        authority: RequestAuthority,
        after: Option<[u8; 16]>,
    ) -> Result<(std::ops::Range<u64>, bool), StorageError> {
        let config = self.config();
        if authority.epoch != config.inventory.epoch {
            return Err(StorageError::Fenced);
        }
        if state.poison.is_some() {
            return Err(StorageError::Poisoned);
        }
        if state.sealed {
            return Err(StorageError::Sealed);
        }
        if state.is_sealing() {
            return Err(StorageError::Invalid("live stream is sealing"));
        }
        if !state.live {
            return Err(StorageError::Invalid("not a live WAL stream"));
        }
        if let Some(token) = after {
            if !state
                .open_live_extent
                .as_ref()
                .is_some_and(|open| open.extent.token == token && !open.closing)
            {
                return Err(StorageError::Invalid(
                    "prelease requires the open live extent",
                ));
            }
            if state.next_live_extent.is_some() {
                return Err(StorageError::Invalid("live extent already preleased"));
            }
        } else if state.open_live_extent.is_some() || state.next_live_extent.is_some() {
            // A lost reply requires fencing/recovery, never a second grant.
            return Err(StorageError::Invalid("live extent already open"));
        }
        let start = state
            .writer
            .as_ref()
            .ok_or(StorageError::Sealed)?
            .position();
        // A sealed or retired file is closed to every later grant, even if a
        // defect moved the allocator back: its seal certified its bytes.
        if state
            .wal_root
            .as_ref()
            .is_some_and(|root| start < root.seal_cursor())
        {
            return Err(StorageError::Invalid(
                "live grant inside a sealed or retired WAL file",
            ));
        }
        let len = config.fragment_bytes - start % config.fragment_bytes;
        if len >= 1 << 30 || !start.is_multiple_of(4096) || !len.is_multiple_of(4096) {
            return Err(StorageError::Invalid("invalid live extent geometry"));
        }
        let end = start.checked_add(len).ok_or(StorageError::Capacity)?;
        let file = crate::wal::position::WalFileId::from_u64(start / config.wal_file_bytes);
        let ready = state.namespace_synced_file.is_some_and(|last| last >= file)
            && state
                .writer
                .as_ref()
                .ok_or(StorageError::Sealed)?
                .external_file_preparer()
                .is_ready(file);
        Ok((start..end, ready))
    }

    /// Reserve/install atomically under state. Any uncertain reservation leaves
    /// fatal poison; successful transient poison never escapes this lock hold.
    fn reserve_live_extent_grant(
        &self,
        state: &mut State,
        range: std::ops::Range<u64>,
        prelease: bool,
    ) -> Result<
        (
            super::nvmeof::live_storage::LiveWriteExtent,
            super::tcp::LeaseProfile,
        ),
        StorageError,
    > {
        use super::nvmeof::live_storage::{LiveAdoption, LiveWalSegment, LiveWriteExtent};
        let config = self.config();
        let start = range.start;
        let end = range.end;
        state.poison = Some(WriterPoison::Fatal);
        let reserve_started = Instant::now();
        let allocation = state
            .writer
            .as_ref()
            .ok_or(StorageError::Sealed)?
            .reserve_external((end - start) as u32)
            .map_err(|error| StorageError::Unknown(WalError::Io(error)))?;
        let reserve_ns = duration_ns(reserve_started.elapsed());
        let metadata = allocation
            .file
            .metadata()
            .map_err(|error| StorageError::Unknown(WalError::Io(error)))?;
        if metadata.len() != config.wal_file_bytes {
            return Err(StorageError::Unknown(WalError::Io(io::Error::other(
                "WAL file size changed",
            ))));
        }
        let extent = LiveWriteExtent {
            token: rand::random(),
            segment: LiveWalSegment {
                database: config.inventory.database,
                partition: config.partition,
                epoch: config.inventory.epoch,
                generation: config.inventory.segment_generation,
                file_index: start / config.wal_file_bytes,
                path: allocation.path.clone(),
                bytes: metadata.len(),
                device: metadata.dev(),
                inode: metadata.ino(),
                block_size: 4096,
            },
            start,
            end,
        };
        let open = OpenLiveExtent {
            adoption: LiveAdoption {
                token: extent.token,
                adopted_end: start,
                receipt_count: 0,
                receipt_digest: [0; 32],
                closed: false,
            },
            extent: extent.clone(),
            closing: false,
            allocation,
            buffer: Vec::new(),
            round: Arc::new(Mutex::new(AdoptionRound::default())),
        };
        if prelease {
            state.next_live_extent = Some(open);
        } else {
            state.open_live_extent = Some(open);
        }
        state.poison = None;
        Ok((
            extent,
            super::tcp::LeaseProfile {
                reserve_ns,
                namespace_sync_ns: 0,
                publish_ns: 0,
            },
        ))
    }

    /// A failed declared frame fences writes while its earlier validated prefix
    /// remains persistable. An opportunistic scan never declares torn bytes.
    pub fn adopt_live_extent(
        &self,
        authority: RequestAuthority,
        token: [u8; 16],
        through: u64,
        close: bool,
    ) -> Result<
        (
            super::nvmeof::live_storage::LiveAdoption,
            super::tcp::AdoptProfile,
        ),
        StorageError,
    > {
        self.adopt_live_extent_impl(authority, token, through, close, None)
    }

    pub fn self_adopt_live_extent(&self) -> Result<(), StorageError> {
        if !self.live_stream {
            return Ok(());
        }
        let pending = {
            let state = self.state.lock();
            if state.poison == Some(WriterPoison::IncompleteLiveFrame) {
                return Ok(());
            }
            state
                .open_live_extent
                .as_ref()
                .map(|open| (open.extent.token, open.extent.end, open.adoption.clone()))
        };
        #[cfg(test)]
        self.pause_live_at(LivePausePoint::ScanSnapshot);
        if let Some((token, end, observed)) = pending {
            self.adopt_live_extent_impl(
                RequestAuthority {
                    epoch: self.config().inventory.epoch,
                },
                token,
                end,
                false,
                Some(observed),
            )?;
        }
        Ok(())
    }

    fn adopt_live_extent_impl(
        &self,
        authority: RequestAuthority,
        token: [u8; 16],
        through: u64,
        close: bool,
        opportunistic: Option<super::nvmeof::live_storage::LiveAdoption>,
    ) -> Result<
        (
            super::nvmeof::live_storage::LiveAdoption,
            super::tcp::AdoptProfile,
        ),
        StorageError,
    > {
        let started = Instant::now();
        let mut profile = super::tcp::AdoptProfile::default();
        let result = (|| {
            if authority.epoch != self.config().inventory.epoch {
                return Err(StorageError::Fenced);
            }
            // A round publishes up to 32 frames or drains pressure. Larger
            // rounds reduce attempts; each successful frame still advances by
            // at least 4 KiB. Retain three attempts per minimum-size frame for
            // drains/rechecks, including stale rounds, and the fatal loop bound.
            let steps = self.config().fragment_bytes / 4096 * 3 + 3;
            'rounds: for _ in 0..steps {
                let gate = {
                    let state = self.state.lock();
                    if let Some(summary) = Self::adoption_status(
                        &state,
                        token,
                        through,
                        close,
                        opportunistic.as_ref(),
                    )? {
                        return Ok(summary);
                    }
                    Arc::clone(
                        &state
                            .open_live_extent
                            .as_ref()
                            .ok_or(StorageError::Poisoned)?
                            .round,
                    )
                };
                // Callers enter without state or persistence. The extent gate
                // preserves complete-prefix scans for reads and maintenance.
                let mut round = gate.lock();
                let (file, cursor, end, file_bytes) = {
                    let state = self.state.lock();
                    if let Some(summary) = Self::adoption_status(
                        &state,
                        token,
                        through,
                        close,
                        opportunistic.as_ref(),
                    )? {
                        return Ok(summary);
                    }
                    let open = state
                        .open_live_extent
                        .as_ref()
                        .ok_or(StorageError::Poisoned)?;
                    if through <= open.adoption.adopted_end {
                        let summary = open.adoption.clone();
                        drop(state);
                        drop(round);
                        return if close {
                            self.close_live_extent(token, through, &mut profile)
                        } else {
                            Ok(summary)
                        };
                    }
                    (
                        Arc::clone(&open.allocation.file),
                        open.adoption.adopted_end,
                        open.extent.end,
                        open.extent.segment.bytes,
                    )
                };
                // Two reusable read buffers: 2 x 64 MiB maximum capacity.
                // At most 32 results are held, whose encoded-frame bytes total
                // <= 8 MiB, or one oversized frame <= 64 MiB. Existing detached
                // group/batch decode storage is bounded by that same input.
                #[cfg(test)]
                let wall_started = Instant::now();
                #[cfg(test)]
                {
                    let active = round.active.fetch_add(1, Ordering::Relaxed) + 1;
                    round.maximum_active.fetch_max(active, Ordering::Relaxed);
                }
                let mut adoption_round_span = super::owner_events::OwnerEventSpan::new(
                    super::owner_events::OwnerEventKind::AdoptionRoundStart,
                    super::owner_events::OwnerEventKind::AdoptionRoundEnd,
                    self.config().partition,
                    cursor,
                    0,
                );
                let mut plans = self.plan_adoption_round(
                    &file,
                    &mut round.buffers[0],
                    cursor,
                    through,
                    end,
                    file_bytes,
                );
                #[cfg(test)]
                let probe_ns = duration_ns(wall_started.elapsed());
                let (count, bytes) = plans
                    .iter()
                    .flatten()
                    .fold((0usize, 0u64), |(count, bytes), plan| {
                        (count + 1, bytes + plan.len)
                    });
                let parallel = plans[1].as_ref().is_some_and(|frame| frame.probe.is_ok())
                    && bytes >= ADOPTION_PARALLEL_BYTES;
                let split = if parallel {
                    let mut prefix_bytes = 0;
                    plans[..count - 1]
                        .iter()
                        .position(|plan| {
                            prefix_bytes += plan.as_ref().map_or(0, |frame| frame.len);
                            prefix_bytes >= bytes.div_ceil(2)
                        })
                        .map_or(count - 1, |index| index + 1)
                } else {
                    count
                };
                let mut prepared: [Option<PreparedLiveFrame>; ADOPTION_ROUND_FRAMES] =
                    std::array::from_fn(|_| None);
                #[cfg(test)]
                self.pause_live_at(LivePausePoint::ValidateUnlocked);
                let [first_buffer, second_buffer] = &mut round.buffers;
                let (first_plans, second_plans) = plans[..count].split_at_mut(split);
                let (first_results, second_results) = prepared[..count].split_at_mut(split);
                let validate = |plans: &mut [Option<PlannedLiveFrame>],
                                results: &mut [Option<PreparedLiveFrame>],
                                buffer: &mut Vec<u8>| {
                    #[cfg(test)]
                    let started = Instant::now();
                    for (plan, result) in plans.iter_mut().zip(results) {
                        let Some(plan) = plan.take() else {
                            break;
                        };
                        let validated = plan.probe.and_then(|probe_crc| {
                            let validated = self.validate_planned_frame(
                                &file, buffer, plan.start, plan.len, file_bytes, 0,
                            )?;
                            // Re-read the full frame, but never attach authority
                            // to a different CRC than the boundary probe saw.
                            if validated.crc != probe_crc {
                                return Err(StorageError::IncompleteLiveFrame);
                            }
                            Ok(validated)
                        });
                        let failed = validated.is_err();
                        *result = Some(PreparedLiveFrame {
                            start: plan.start,
                            len: plan.len,
                            validated,
                        });
                        if failed {
                            break;
                        }
                    }
                    #[cfg(test)]
                    {
                        duration_ns(started.elapsed())
                    }
                    #[cfg(not(test))]
                    {
                        0u64
                    }
                };
                let (first_ns, second_ns) = if parallel {
                    std::thread::scope(|scope| {
                        let worker =
                            scope.spawn(|| validate(second_plans, second_results, second_buffer));
                        let first = validate(first_plans, first_results, first_buffer);
                        let second = match worker.join() {
                            Ok(result) => result,
                            Err(panic) => std::panic::resume_unwind(panic),
                        };
                        (first, second)
                    })
                } else {
                    (validate(first_plans, first_results, first_buffer), 0)
                };
                #[cfg(test)]
                {
                    round.active.fetch_sub(1, Ordering::Relaxed);
                    let mut state = self.state.lock();
                    let diagnostics = &mut state.live_test_diagnostics.adoption;
                    diagnostics.rounds += 1;
                    diagnostics.parallel_rounds += u64::from(parallel);
                    diagnostics.validate_wall_ns += duration_ns(wall_started.elapsed());
                    diagnostics.validate_worker_ns += probe_ns + first_ns + second_ns;
                }
                #[cfg(not(test))]
                let _ = (first_ns, second_ns);
                #[cfg(test)]
                self.pause_live_at(LivePausePoint::ValidatedBeforePublish);
                let mut pressure = None;
                let mut incomplete_summary = None;
                #[cfg(test)]
                let frames_before = profile.frames;
                let mut adopted_end = cursor;
                for PreparedLiveFrame {
                    start,
                    len,
                    validated,
                } in prepared.into_iter().flatten()
                {
                    let mut state = self.state.lock();
                    #[cfg(test)]
                    let publish_started = Instant::now();
                    if let Some(summary) = Self::adoption_status(
                        &state,
                        token,
                        through,
                        close,
                        opportunistic.as_ref(),
                    )? {
                        adoption_round_span.end_at(summary.adopted_end);
                        return Ok(summary);
                    }
                    let open = state
                        .open_live_extent
                        .as_ref()
                        .ok_or(StorageError::Poisoned)?;
                    if open.adoption.adopted_end != start {
                        adoption_round_span.end_at(open.adoption.adopted_end);
                        continue 'rounds;
                    }
                    let mut open = state
                        .open_live_extent
                        .take()
                        .ok_or(StorageError::Poisoned)?;
                    let result = validated.and_then(|validated| {
                        self.publish_adoption_frame(&mut state, &mut open, start, len, validated)
                    });
                    let cursor = open.adoption.adopted_end;
                    adopted_end = cursor;
                    adoption_round_span.set_end_a(cursor);
                    let summary = open.adoption.clone();
                    state.open_live_extent = Some(open);
                    #[cfg(test)]
                    {
                        let diagnostics = &mut state.live_test_diagnostics.adoption;
                        diagnostics.publish_state_ns += duration_ns(publish_started.elapsed());
                        if matches!(result, Ok(LiveFrameStep::Adopted)) {
                            diagnostics.frames += 1;
                            diagnostics.bytes += len;
                        }
                    }
                    match result {
                        Ok(LiveFrameStep::Adopted) => {
                            profile.frames += 1;
                            drop(state);
                        }
                        Ok(LiveFrameStep::Pressure(demand)) => {
                            pressure = Some((state, demand));
                            break;
                        }
                        Err(StorageError::IncompleteLiveFrame) if opportunistic.is_some() => {
                            incomplete_summary = Some(summary);
                            break;
                        }
                        Err(error) => {
                            state.poison =
                                Some(if matches!(error, StorageError::IncompleteLiveFrame) {
                                    WriterPoison::IncompleteLiveFrame
                                } else {
                                    WriterPoison::Fatal
                                });
                            adoption_round_span.end_at(adopted_end);
                            return Err(error);
                        }
                        Ok(LiveFrameStep::End) => {
                            adoption_round_span.end_at(summary.adopted_end);
                            return Ok(summary);
                        }
                    }
                }
                adoption_round_span.end_at(adopted_end);
                drop(round);
                if let Some((state, demand)) = pressure {
                    self.drain_adoption_pressure(state, demand)?;
                }
                #[cfg(test)]
                if profile.frames > frames_before {
                    self.pause_live_at(LivePausePoint::AfterFrame);
                }
                if let Some(summary) = incomplete_summary {
                    return Ok(summary);
                }
            }
            self.state.lock().poison = Some(WriterPoison::Fatal);
            Err(StorageError::Invalid(
                "live adoption exceeded bounded progress",
            ))
        })();
        profile.adopt_ns = duration_ns(started.elapsed());
        result.map(|adoption| (adoption, profile))
    }

    fn adoption_status(
        state: &State,
        token: [u8; 16],
        through: u64,
        close: bool,
        opportunistic: Option<&super::nvmeof::live_storage::LiveAdoption>,
    ) -> Result<Option<super::nvmeof::live_storage::LiveAdoption>, StorageError> {
        if state.poison.is_some() {
            return Err(StorageError::Poisoned);
        }
        if state.is_sealing() && opportunistic.is_none() {
            return Err(StorageError::Invalid("live stream is sealing"));
        }
        if let Some(observed) = opportunistic {
            match state.open_live_extent.as_ref() {
                Some(open) if open.extent.token == token => {
                    if open.closing {
                        return Ok(Some(open.adoption.clone()));
                    }
                }
                _ => return Ok(Some(observed.clone())),
            }
        }
        if let Some(previous) = &state.last_live_adoption
            && previous.token == token
        {
            return if through <= previous.adopted_end {
                Ok(Some(previous.clone()))
            } else {
                Err(StorageError::Invalid("closed live extent"))
            };
        }
        let open = state
            .open_live_extent
            .as_ref()
            .ok_or(StorageError::Invalid("no live extent"))?;
        if open.extent.token != token {
            return Err(StorageError::Invalid("foreign live extent"));
        }
        if open.closing {
            return if close || through > open.adoption.adopted_end {
                Err(StorageError::Invalid("live extent is closing"))
            } else {
                Ok(Some(open.adoption.clone()))
            };
        }
        if through < open.extent.start || through > open.extent.end {
            return Err(StorageError::Invalid("adoption outside live extent"));
        }
        Ok(None)
    }

    fn drain_adoption_pressure(
        &self,
        mut state: parking_lot::MutexGuard<'_, State>,
        demand: LiveFrameDemand,
    ) -> Result<(), StorageError> {
        let files = state.appended_end.saturating_sub(1) / self.config().wal_file_bytes + 1;
        state.references.persist_pending(files)?;
        if demand.fits(&state, self.references.root().end) {
            Ok(())
        } else {
            Err(StorageError::Capacity)
        }
    }

    fn plan_adoption_round(
        &self,
        file: &File,
        buffer: &mut Vec<u8>,
        mut cursor: u64,
        through: u64,
        end: u64,
        file_bytes: u64,
    ) -> [Option<PlannedLiveFrame>; ADOPTION_ROUND_FRAMES] {
        let mut plans = std::array::from_fn(|_| None);
        let mut bytes = 0;
        for slot in &mut plans {
            if cursor >= through || bytes >= ADOPTION_ROUND_BYTES {
                break;
            }
            let start = cursor;
            let probe = self.probe_adoption_frame(file, buffer, start, through, end, file_bytes);
            let len = probe.as_ref().copied().unwrap_or(0);
            let failed = probe.is_err();
            if bytes != 0 && len > ADOPTION_ROUND_BYTES - bytes {
                break;
            }
            *slot = Some(PlannedLiveFrame {
                start,
                len,
                probe: probe
                    .map(|_| u32::from_be_bytes([buffer[4], buffer[5], buffer[6], buffer[7]])),
            });
            if failed {
                break;
            }
            cursor += len;
            bytes += len;
        }
        plans
    }

    fn probe_adoption_frame(
        &self,
        file: &File,
        buffer: &mut Vec<u8>,
        start: u64,
        through: u64,
        end: u64,
        file_bytes: u64,
    ) -> Result<u64, StorageError> {
        if through - start < 4096 || end - start < 4096 {
            return Err(StorageError::IncompleteLiveFrame);
        }
        if buffer.len() < 4096 {
            buffer.resize(4096, 0);
        }
        file.read_exact_at(&mut buffer[..4096], start % file_bytes)
            .map_err(Self::adoption_read_error)?;
        let len = u64::from(u32::from_be_bytes([
            buffer[0], buffer[1], buffer[2], buffer[3],
        ])) + 8;
        let max_frame =
            (self.config().group_limits.max_encoded_bytes as u64 + 8).div_ceil(4096) * 4096;
        if !len.is_multiple_of(4096)
            || len > max_frame.min(ADOPTION_MAX_FRAME_BYTES as u64)
            || len > through - start
            || len > end - start
        {
            return Err(StorageError::IncompleteLiveFrame);
        }
        #[cfg(test)]
        self.pause_live_at(LivePausePoint::FrameProbe);
        Ok(len)
    }

    fn validate_planned_frame(
        &self,
        file: &File,
        buffer: &mut Vec<u8>,
        start: u64,
        len: u64,
        file_bytes: u64,
        prefix_bytes: usize,
    ) -> Result<ValidatedLiveFrame, StorageError> {
        // Retain initialized high-water length. Requests re-read the whole frame;
        // the locked seal fallback retains its already-probed first block.
        if buffer.capacity() < len as usize {
            // Keep geometric reuse without allocating past the hard frame cap.
            let capacity = (len as usize)
                .max(buffer.capacity() * 2)
                .min(ADOPTION_MAX_FRAME_BYTES);
            buffer.reserve_exact(capacity - buffer.len());
        }
        if buffer.len() < len as usize {
            buffer.resize(len as usize, 0);
        }
        if len as usize > prefix_bytes {
            file.read_exact_at(
                &mut buffer[prefix_bytes..len as usize],
                start % file_bytes + prefix_bytes as u64,
            )
            .map_err(Self::adoption_read_error)?;
        }
        let group = self.decode_live_frame(&buffer[..len as usize])?;
        let crc = u32::from_be_bytes([buffer[4], buffer[5], buffer[6], buffer[7]]);
        Ok(ValidatedLiveFrame { group, crc })
    }

    fn adoption_read_error(error: io::Error) -> StorageError {
        if error.kind() == io::ErrorKind::UnexpectedEof {
            StorageError::IncompleteLiveFrame
        } else {
            StorageError::Io(error)
        }
    }

    // Request adoption reads and validates immutable bytes below declared through
    // without state. Ordered publication rechecks token, lifecycle and cursor;
    // concurrent adopters cannot publish the same frame twice. The revoked-tail
    // seal scan below retains its existing single-frame locked read semantics.
    fn adopt_live_frame(
        &self,
        state: &mut State,
        open: &mut OpenLiveExtent,
        through: u64,
    ) -> Result<LiveFrameStep, StorageError> {
        let start = open.adoption.adopted_end;
        if through <= start {
            return Ok(LiveFrameStep::End);
        }
        let len = self.probe_adoption_frame(
            &open.allocation.file,
            &mut open.buffer,
            start,
            through,
            open.extent.end,
            open.extent.segment.bytes,
        )?;
        let validated = self.validate_planned_frame(
            &open.allocation.file,
            &mut open.buffer,
            start,
            len,
            open.extent.segment.bytes,
            4096,
        )?;
        self.publish_adoption_frame(state, open, start, len, validated)
    }

    fn publish_adoption_frame(
        &self,
        state: &mut State,
        open: &mut OpenLiveExtent,
        start: u64,
        len: u64,
        validated: ValidatedLiveFrame,
    ) -> Result<LiveFrameStep, StorageError> {
        use super::nvmeof::live_storage::extend_live_receipt_digest;
        let ValidatedLiveFrame { group, crc } = validated;
        for batch in group.batches() {
            if !matches!(
                self.references.lookup_conflict(
                    batch.id(),
                    batch.version(),
                    &mut LookupWork::default(),
                )?,
                ConflictLookup::Free
            ) {
                return Err(StorageError::IncompleteLiveFrame);
            }
        }
        let count = group.batches().len();
        let descriptors = group
            .batches()
            .iter()
            .map(|batch| batch.record_descriptors().len())
            .sum();
        ReferenceDirectory::check_fresh_total_capacity(
            self.config().max_batches.min(1024),
            self.config().max_reference_metadata_bytes,
            count,
            descriptors,
        )?;
        // Only an already adopted backlog can be drained by persistence. The
        // next frame may follow a valid skip region larger than this window;
        // charging that physical gap before adopting its bridging frame would
        // demand an impossible history-root advance and poison a legal layout.
        let demand = LiveFrameDemand {
            batches: count,
            descriptors,
            bytes: len,
        };
        if !demand.fits(state, self.references.root().end) {
            return Ok(LiveFrameStep::Pressure(demand));
        }
        let mut credit = state.references.reserve_group(count, descriptors)?;
        state.poison = Some(WriterPoison::Fatal);
        let receipts = self.publish_live_group(
            state,
            &group,
            WalPosition::new(start, len as u32),
            crc,
            &mut credit,
        )?;
        for receipt in receipts {
            open.adoption.receipt_digest =
                extend_live_receipt_digest(open.adoption.receipt_digest, receipt);
            open.adoption.receipt_count += 1;
        }
        open.adoption.adopted_end = start + len;
        state.poison = None;
        Ok(LiveFrameStep::Adopted)
    }

    /// State owns the allocation guard throughout close. Lock order remains
    /// persistence -> state where both are needed; close takes state only to
    /// mark/revalidate ownership, never during suffix I/O. `closing` prevents
    /// mutation or regrant while the local file pin and buffer are borrowed.
    fn close_live_extent(
        &self,
        token: [u8; 16],
        through: u64,
        profile: &mut super::tcp::AdoptProfile,
    ) -> Result<super::nvmeof::live_storage::LiveAdoption, StorageError> {
        let (file, end, bytes, mut buffer) = {
            let mut state = self.state.lock();
            if state.poison.is_some() {
                return Err(StorageError::Poisoned);
            }
            if state.is_sealing() {
                return Err(StorageError::Invalid("live stream is sealing"));
            }
            let open = state
                .open_live_extent
                .as_mut()
                .ok_or(StorageError::Invalid("no live extent"))?;
            if open.extent.token != token {
                return Err(StorageError::Invalid("foreign live extent"));
            }
            if open.closing {
                return Err(StorageError::Invalid("live extent is closing"));
            }
            if through != open.adoption.adopted_end {
                state.poison = Some(WriterPoison::IncompleteLiveFrame);
                return Err(StorageError::IncompleteLiveFrame);
            }
            open.closing = true;
            // Shared ownership pins the final inode during unlocked close I/O;
            // the state keeps its guard and cannot regrant this physical range.
            (
                Arc::clone(&open.allocation.file),
                open.extent.end,
                open.extent.segment.bytes,
                std::mem::take(&mut open.buffer),
            )
        };
        #[cfg(test)]
        self.pause_live_at(LivePausePoint::CloseSuffix);
        let scan_started = Instant::now();
        let result = self.validate_live_extent_close(&file, &mut buffer, through, end, bytes);
        profile.close_scan_ns = duration_ns(scan_started.elapsed());
        profile.close_scan_bytes = end - through;
        let mut state = self.state.lock();
        let open = state
            .open_live_extent
            .as_mut()
            .ok_or(StorageError::Poisoned)?;
        if open.extent.token != token || !open.closing || open.adoption.adopted_end != through {
            state.poison = Some(WriterPoison::Fatal);
            return Err(StorageError::Poisoned);
        }
        open.closing = false;
        open.buffer = buffer;
        if let Err(error) = result {
            if state.poison.is_none() {
                state.poison = Some(if matches!(error, StorageError::IncompleteLiveFrame) {
                    WriterPoison::IncompleteLiveFrame
                } else {
                    WriterPoison::Fatal
                });
            }
            return Err(error);
        }
        if state.poison.is_some() {
            return Err(StorageError::Poisoned);
        }
        let mut open = state
            .open_live_extent
            .take()
            .ok_or(StorageError::Poisoned)?;
        open.adoption.closed = true;
        let summary = open.adoption;
        state.poison = Some(WriterPoison::Fatal);
        open.allocation
            .guard
            .release()
            .map_err(|error| StorageError::Unknown(WalError::Io(io::Error::other(error))))?;
        state.last_live_adoption = Some(summary.clone());
        state.open_live_extent = state.next_live_extent.take();
        state.poison = None;
        Ok(summary)
    }

    fn validate_live_extent_close(
        &self,
        file: &File,
        buffer: &mut Vec<u8>,
        through: u64,
        end: u64,
        bytes: u64,
    ) -> Result<(), StorageError> {
        let mut cursor = through;
        // At most ceil(extent remainder / 1 MiB) positional reads, with no
        // partition lock held. An extent is strictly smaller than 1 GiB.
        while cursor < end {
            let len = (end - cursor).min(1024 * 1024) as usize;
            buffer.resize(len, 0);
            file.read_exact_at(buffer, cursor % bytes)?;
            let first = cursor == through;
            if (first && buffer[..8] != [255; 8])
                || buffer[if first { 8 } else { 0 }..]
                    .iter()
                    .any(|byte| *byte != 0)
            {
                return Err(StorageError::IncompleteLiveFrame);
            }
            cursor += len as u64;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn pause_live(
        &self,
        point: LivePausePoint,
    ) -> (
        std::sync::mpsc::Receiver<()>,
        std::sync::mpsc::SyncSender<()>,
    ) {
        let (entered, started) = std::sync::mpsc::sync_channel(1);
        let (resume, released) = std::sync::mpsc::sync_channel(1);
        *self.live_pause.lock() = Some((point, entered, released));
        (started, resume)
    }

    #[cfg(test)]
    fn pause_live_at(&self, point: LivePausePoint) {
        if let Some(hook) = self.take_live_pause(point) {
            Self::run_live_pause(hook);
        }
    }

    /// Take the armed hook of `point`, if any. The caller runs it, so it can
    /// record that the armed point engaged before it blocks.
    #[cfg(test)]
    fn take_live_pause(&self, point: LivePausePoint) -> Option<LivePauseHook> {
        let mut slot = self.live_pause.lock();
        if slot.as_ref().is_some_and(|hook| hook.0 == point) {
            slot.take()
        } else {
            None
        }
    }

    #[cfg(test)]
    fn run_live_pause(hook: LivePauseHook) {
        let (_, entered, resume) = hook;
        let _ = entered.send(());
        let _ = resume.recv();
    }

    fn decode_live_frame(&self, frame: &[u8]) -> Result<CompleteGroup, StorageError> {
        let payload =
            CrcFrame::read_from_slice(frame, 0).map_err(|_| StorageError::IncompleteLiveFrame)?;
        if payload.len() + 8 != frame.len() || payload.len() < 12 || payload[8..12] != [0, 3, 0, 12]
        {
            return Err(StorageError::IncompleteLiveFrame);
        }
        let group = CompleteGroup::decode_slice(
            &self.shape,
            payload,
            self.config().batch_limits,
            self.config().group_limits,
        )
        .map_err(|_| StorageError::IncompleteLiveFrame)?;
        for batch in group.batches() {
            validate_batch(self.config(), batch).map_err(|_| StorageError::IncompleteLiveFrame)?;
        }
        Ok(group)
    }

    fn publish_live_group(
        &self,
        state: &mut State,
        group: &CompleteGroup,
        position: WalPosition,
        crc: u32,
        credit: &mut super::reference_directory::MetadataCredit,
    ) -> Result<Vec<AppendedBatch>, StorageError> {
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
            let batch_credit = credit.take_batch(descriptors.len())?;
            metadata.push(
                RegisteredBatch::new(
                    batch.id(),
                    batch.version(),
                    batch.digest(),
                    receipt.address,
                    descriptors,
                    group_end,
                )
                .with_credit(batch_credit),
            );
            receipts.push(receipt);
        }
        state.references.publish_group(metadata)?;
        #[cfg(test)]
        // AcqRel consumes the one-shot test fault published by its Release store.
        if self.fail_after_live_publish.swap(false, Ordering::AcqRel) {
            return Err(StorageError::Unknown(WalError::Io(io::Error::other(
                "injected crash after live reference publication",
            ))));
        }
        state.appended_end = group_end;
        if let Some(last) = group.batches().last() {
            state.progress.appended(last.version());
        }
        // Adoption proves APPENDED bytes only. Synchronous reads and checkpoint
        // proofs still require the owner's separate covering persistence barrier.
        if let Some(frontier) = self.frontier.get() {
            match frontier.publish_appended(group_end, group.batches().len() as u64) {
                Ok(()) => {}
                // A lifecycle transition owns adoption while its frontier is
                // stopped. The syncer drain at retirement adopts frames that
                // external writers left below the extent end; the seal adopts
                // the same way. Each transition persists the appended prefix
                // itself, so the adopted frame is durable before that
                // transition reports success (paper 3.1: the finalizing sync
                // publishes the written bytes). Every other stopped frontier,
                // and every other frontier error, still fails.
                Err(FrontierError::Stopped) if state.lifecycle.owns_stopped_frontier() => {}
                Err(_) => {
                    return Err(StorageError::Invalid(
                        "durability frontier rejected a live group",
                    ));
                }
            }
        }
        let tail = group
            .batches()
            .last()
            .ok_or(StorageError::Invalid("empty live group"))?;
        state.live_tail = Some(LiveFrontierTail {
            offset: position.offset(),
            crc,
            id: tail.id(),
            version: tail.version(),
            digest: tail.digest(),
        });
        state.stats.appended_groups += 1;
        Ok(receipts)
    }

    /// Construct a one-batch group. Callers that need throughput group several
    /// batches into one `append_group` call to amortize persistence.
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
        self.append_group_impl(authority, group, None)
    }

    /// Identical append semantics with opt-in, accumulating local stage timings.
    pub fn append_group_profiled(
        &self,
        authority: RequestAuthority,
        group: &CompleteGroup,
        profile: &mut StorageWriteProfile,
    ) -> Result<Vec<AppendedBatch>, StorageError> {
        self.append_group_impl(authority, group, Some(profile))
    }

    #[cfg(test)]
    pub(crate) fn try_append_group(
        &self,
        authority: RequestAuthority,
        group: &CompleteGroup,
        profile: Option<&mut StorageWriteProfile>,
    ) -> Result<Vec<AppendedBatch>, StorageError> {
        self.append_group_impl(authority, group, profile)
    }

    fn append_group_impl(
        &self,
        authority: RequestAuthority,
        group: &CompleteGroup,
        profile: Option<&mut StorageWriteProfile>,
    ) -> Result<Vec<AppendedBatch>, StorageError> {
        let mut timing = WriteTiming::new(profile, true);
        timing.stage(Some(WriteStage::AppendBasicValidation));
        let config = self.config();
        if authority.epoch != config.inventory.epoch {
            return Err(StorageError::Fenced);
        }
        timing.stage(Some(WriteStage::AppendLock));
        let mut state = self.state.lock();
        timing.stage(Some(WriteStage::AppendBasicValidation));
        if state.poison.is_some() {
            return Err(StorageError::Poisoned);
        }
        if state.sealed {
            return Err(StorageError::Sealed);
        }
        if state.live {
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
            let files = state.appended_end.saturating_sub(1) / config.wal_file_bytes + 1;
            state.references.persist_pending(files)?;
            state
                .references
                .check_capacity(group.batches().len(), descriptor_count)?;
            if predicted_end.saturating_sub(self.references.root().end) > 64 * 1024 * 1024 {
                return Err(StorageError::Capacity);
            }
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
                let files = state.appended_end.saturating_sub(1) / config.wal_file_bytes + 1;
                state.references.persist_pending(files)?;
                state
                    .references
                    .reserve_group(group.batches().len(), descriptor_count)?
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
        #[cfg(test)]
        crate::failpoints::write_model::boundary("append_before");
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
        #[cfg(test)]
        crate::failpoints::write_model::boundary("append_after");
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
                .map_err(|error| StorageError::Unknown(WalError::Io(io::Error::other(error))))?;
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
        if let Some(last) = group.batches().last() {
            state.progress.appended(last.version());
        }
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
        state.poison = None;
        timing.record(|profile| {
            profile.appended_groups = profile.appended_groups.saturating_add(1);
        });
        timing.success(receipts.len());
        Ok(receipts)
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
        self.self_adopt_live_extent()?;
        self.persist_scope(PersistScope::Appended, None)?;
        Ok(self.durable_frontier.load(Ordering::Acquire))
    }

    /// `persist_appended` with opt-in stage timings.
    pub fn persist_appended_profiled(
        &self,
        profile: &mut StorageWriteProfile,
    ) -> Result<u64, StorageError> {
        self.self_adopt_live_extent()?;
        self.persist_scope(PersistScope::Appended, Some(profile))?;
        Ok(self.durable_frontier.load(Ordering::Acquire))
    }

    /// End of the contiguous appended (acknowledged-in-memory) WAL prefix.
    pub fn appended_frontier(&self) -> u64 {
        self.state.lock().appended_end
    }

    /// End of the adopted-receipt prefix published by its covering barrier.
    /// A position-root proof can be ahead without publishing receipt bookkeeping.
    pub fn synced_frontier(&self) -> u64 {
        self.durable_frontier.load(Ordering::Acquire)
    }

    /// The recovery that opened this handle left a hole below its recovered tail: it
    /// ended early at a lost, torn or invalid frame and a valid frame followed. Always
    /// false for a handle created empty.
    pub fn recovery_found_hole(&self) -> bool {
        self.recovery_hole
    }

    /// Appended and synced tails with their byte frontiers, from one lock hold. On a
    /// live stream the synced tail covers only the prefix of the installed strict root.
    pub fn durable_progress(&self) -> LogDurableProgress {
        let state = self.state.lock();
        state.progress.snapshot(state.appended_end)
    }

    /// Published position-root cut, distinct from the adopted receipt frontier.
    #[cfg(test)]
    pub(crate) fn position_frontier_for_test(&self) -> u64 {
        self.state.lock().wal_root.as_ref().unwrap().synced_end
    }

    /// Generation, scan origin and fixed cut for root/lease interleaving tests.
    #[cfg(test)]
    pub(crate) fn position_root_for_test(&self) -> (u64, u64, u64) {
        let state = self.state.lock();
        let root = state.wal_root.as_ref().unwrap();
        (root.generation, root.scan_from, root.synced_end)
    }

    /// Count one adoption-triggered live-frontier persistence request.
    pub(crate) fn note_live_frontier_request(&self) {
        self.state.lock().stats.live_frontier_requests += 1;
    }

    /// Explicit barrier for the live root: after a bounded syncer deferred it,
    /// install a root covering the durable prefix. A no-op when the root is
    /// current, the log is not live, or persistence already failed (a failed
    /// log never writes another root; recovery scans its valid suffix).
    /// Flush, fence/retire and shutdown call it.
    pub fn persist_live_root(&self) -> Result<(), StorageError> {
        {
            let state = self.state.lock();
            if !live_root_lags(&state) || persistence_poisoned(&state, PersistScope::Appended) {
                return Ok(());
            }
        }
        self.persist_scope(PersistScope::Appended, None)
    }

    /// Durable-progress nudge on a live stream: make everything appended durable
    /// and install the strict live root over it, so the next report covers it (see
    /// [`Self::durable_progress`]). A no-op when the report already covers the
    /// appended end, as on a recovered handle. Returns false when the stream is not
    /// live: its reports follow every sync, so the syncer serves the nudge.
    pub fn persist_live_progress(&self) -> Result<bool, StorageError> {
        // Test hooks sit before the idle early return, so a nudge that finds
        // everything already persisted still reaches them. They run with no
        // lock held and only on a live stream, like the returns below.
        #[cfg(test)]
        if self.state.lock().live {
            self.pause_live_at(LivePausePoint::ProgressBeforePersist);
            if self.fail_live_progress.swap(false, Ordering::AcqRel) {
                self.failed_live_progress.store(true, Ordering::Release);
                return Err(StorageError::Io(io::Error::other(
                    "injected live progress failure",
                )));
            }
        }
        {
            let state = self.state.lock();
            if !state.live {
                return Ok(false);
            }
            if state.progress.snapshot(state.appended_end).idle() {
                return Ok(true);
            }
        }
        self.persist_scope(PersistScope::Appended, None)?;
        Ok(true)
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
    /// for the waiter that triggered it. Runs on the single syncer thread. It
    /// uses the bounded live-root policy; explicit barriers catch the root up.
    pub fn sync_for_frontier(&self) -> io::Result<()> {
        let mut profile = StorageWriteProfile::default();
        let adopted = self.self_adopt_live_extent();
        let result = adopted.and_then(|()| {
            // Hold point of the adoption-triggered persist: the copies are
            // adopted, nothing is persisted yet. No lock is held. A failure of
            // the persist below is armed with `fail_next_appended_persist`
            // (its consumed flag proves it fired), which fails the frontier
            // for good, as a real sync failure does. `stopping` is read after
            // this point, so a stop that starts while parked still selects the
            // barrier policy. The flag is set only when the armed hook is
            // actually taken, so it proves this pass engaged the hold.
            #[cfg(test)]
            if let Some(hook) = self.take_live_pause(LivePausePoint::SyncerBeforePersist) {
                self.syncer_persist_hold_reached
                    .store(true, Ordering::Release);
                Self::run_live_pause(hook);
            }
            // The final drain at syncer shutdown (fence, retire, owner stop)
            // is an explicit barrier: it leaves the root covering the prefix.
            let stopping = self
                .frontier
                .get()
                .is_some_and(|frontier| frontier.stopping());
            self.persist_scope_with(
                PersistScope::Appended,
                Some(&mut profile),
                syncer_root_policy(stopping),
            )
        });
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
        self.persist_scope_with(scope, profile, RootPolicy::Barrier)
    }

    fn persist_scope_with(
        &self,
        scope: PersistScope<'_>,
        profile: Option<&mut StorageWriteProfile>,
        root: RootPolicy,
    ) -> Result<(), StorageError> {
        let result = self.persist_scope_inner(scope, profile, root);
        if let Err(error) = &result
            && matches!(error, StorageError::Unknown(_) | StorageError::Poisoned)
        {
            self.fail_frontier(&format!("persistence failed: {error}"));
        }
        result
    }

    /// Persist the final names and inventory witnesses after their inodes have
    /// been synced. Preparation, leases and covering barriers share this sequence.
    fn persist_namespace_witnesses(
        &self,
        last_file: u64,
        previous_namespace: Option<u64>,
        timing: &mut WriteTiming<'_>,
        witness_persisted: &mut u64,
    ) -> io::Result<()> {
        if previous_namespace.is_some_and(|file| file >= last_file) {
            return Ok(());
        }
        timing.record(|profile| {
            profile.namespace_persist_calls = profile.namespace_persist_calls.saturating_add(1);
        });
        timing.stage(Some(WriteStage::PersistNamespace));
        #[cfg(test)]
        if self.fail_namespace_persist.swap(false, Ordering::AcqRel) {
            return Err(io::Error::other(
                "injected namespace persistence failure after file sync",
            ));
        }
        #[cfg(test)]
        crate::failpoints::write_model::boundary("namespace_sync_before");
        self.wal.persist_namespace()?;
        #[cfg(test)]
        crate::failpoints::write_model::boundary("namespace_sync_after");
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
            if file_inventory::persist(&self.path, self.config(), file, timing)? {
                *witness_persisted += 1;
            }
            #[cfg(test)]
            if fail_witness == 2 {
                return Err(io::Error::other("injected crash after inventory witness"));
            }
        }
        Ok(())
    }

    fn persist_scope_inner(
        &self,
        scope: PersistScope<'_>,
        profile: Option<&mut StorageWriteProfile>,
        root: RootPolicy,
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
        let pending = pending_with_root(&state, scope, root)?;
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
        #[cfg(test)]
        if matches!(scope, PersistScope::Appended) {
            self.pause_live_at(LivePausePoint::AppendedPersistenceLocked);
        }
        timing.stage(Some(WriteStage::PersistStateLock));
        let mut live_frontier = None;
        let (start, end, namespace_synced_file, captured, wal_root) = {
            let mut state = self.state.lock();
            timing.stage(Some(WriteStage::PersistValidation));
            let Some(requested_end) = pending_with_root(&state, scope, root)? else {
                timing.record(|profile| {
                    profile.persist_noops = profile.persist_noops.saturating_add(1);
                });
                timing.success(receipt_count.unwrap_or(0));
                return Ok(());
            };
            #[cfg(test)]
            let fail_appended = matches!(scope, PersistScope::Appended)
                && self.fail_appended_persist.swap(false, Ordering::AcqRel);
            #[cfg(test)]
            if fail_appended {
                self.failed_appended_persist.store(true, Ordering::Release);
            }
            #[cfg(test)]
            if self.fail_persist.swap(false, Ordering::AcqRel) || fail_appended {
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
            // The live root only sets the strict recovery prefix; recovery scans
            // every valid frame beyond it. A barrier always installs it. The
            // syncer defers it within a fragment, and only with windowed
            // (WAL-replay) history: on-disk history requires the root at or past
            // the history root that this barrier may advance.
            if state.live
                && (root == RootPolicy::Barrier
                    || state.appended_end / self.config().fragment_bytes
                        > state.live_root_end / self.config().fragment_bytes)
            {
                let tail = state
                    .live_tail
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
                // The version of the last batch below `end`, read under the same lock.
                state.progress.capture(),
                // The retirement record at capture time: a file it names was
                // durable when it retired and is unlinked, so it is not synced.
                state.wal_root.clone(),
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
        let mut wal_file_syncs = 0u64;
        let mut frontier_io = LiveFrontierIo::default();
        let result = (|| -> io::Result<()> {
            // A root catch-up after a deferred root has no new data to sync.
            for file_id in (first_file..=last_file).filter(|_| start < end) {
                let offset = file_id
                    .checked_mul(self.config().wal_file_bytes)
                    .ok_or_else(|| io::Error::other("WAL file offset overflow"))?;
                // Skip a file the WAL root names as retired, as
                // `persist_wal_through` does.
                // (a) `state.durable_end` (the `start` of this loop) is assigned
                // only in this function, in the graceful seal (after
                // `persist_appended`) and in recovery (`wal_replay.rs`).
                // `persist_wal_through` and the file-seal install move only the
                // root's `synced_end`. So a sealed file of the RUNNING
                // generation can retire while `durable_end` is still below it.
                // (b) A retired file was durable when it retired: its exact
                // seal is installed after `sync_sealed_file`, with
                // `synced_end >= seal.end`. Skipping its sync is correct, and
                // its path is gone, so a sync would fail and poison the log.
                // (c) `persist_namespace_witnesses` after this loop still
                // writes witness metadata for EVERY file from
                // `previous_namespace + 1` to `last_file` (from 0 when there is
                // none), retired files included. That is harmless: it is
                // metadata only and never opens the WAL file, and recovery
                // explains a retired file by `retired.skip_end`. The cost is
                // one create, fsync and directory sync per retired file at the
                // first persist.
                // (d) Lock order: `persistence`, then `state`. Retirement holds
                // `persistence` across root, unlink and directory sync, so the
                // retired set captured here cannot change during the loop.
                if wal_root
                    .as_deref()
                    .is_some_and(|root| root.retired.contains(offset))
                {
                    continue;
                }
                timing.record(|profile| {
                    profile.file_persist_calls = profile.file_persist_calls.saturating_add(1);
                });
                timing.stage(Some(WriteStage::PersistFile));
                #[cfg(test)]
                crate::failpoints::write_model::boundary("file_sync_before");
                self.wal.persist_frame_file(WalPosition::new(offset, 1))?;
                wal_file_syncs += 1;
                #[cfg(test)]
                crate::failpoints::write_model::boundary("file_sync_after");
                timing.record(|profile| {
                    profile.file_persist_successes =
                        profile.file_persist_successes.saturating_add(1);
                });
            }
            self.persist_namespace_witnesses(
                last_file,
                previous_namespace,
                &mut timing,
                &mut witness_persisted,
            )?;
            if let Some(frontier) = &live_frontier {
                self.persist_live_frontier(frontier, &mut timing, &mut frontier_io)?;
            }
            Ok(())
        })();

        timing.stage(Some(WriteStage::PersistStateLock));
        let mut state = self.state.lock();
        timing.stage(Some(WriteStage::PersistPublication));
        state.stats.inventory_witnesses += witness_persisted;
        if result.is_ok() {
            state.stats.wal_file_syncs += wal_file_syncs;
        }
        {
            state.stats.live_frontier_file_syncs += frontier_io.file_syncs;
            state.stats.live_frontier_namespace_syncs += frontier_io.namespace_syncs;
            state.stats.live_frontier_installs += frontier_io.installs;
            state.stats.live_frontier_bytes += frontier_io.bytes;
            if result.is_ok() {
                if live_frontier.is_some() {
                    // Persistence serializes installs; captures only grow.
                    state.live_root_end = end;
                } else if state.live {
                    state.stats.live_frontier_deferred += 1;
                }
            }
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
        // A live sync that deferred the strict root publishes no progress: recovery
        // enforces no frame beyond the root, so a report must not cover one.
        let reportable = live_frontier.is_some() || !state.live;
        if reportable {
            state.progress.synced(captured, end);
        }
        self.durable_frontier.store(end, Ordering::Release);
        if namespace_sync {
            state.namespace_synced_file =
                Some(crate::wal::position::WalFileId::from_u64(last_file));
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
        if self.live_stream && _authority.group_end() > self.readable_frontier() {
            // A position proof need not publish the adopted receipt frontier.
            // Serving a batch obtains its own covering receipt barrier.
            if !self.position_covers_batch(appended.address) {
                return Err(StorageError::NeedsPersistence);
            }
            self.persist_for_read()?;
            if _authority.group_end() > self.readable_frontier() {
                return Err(StorageError::NeedsPersistence);
            }
        }
        let _admission = self.read_admission()?;
        let position = WalPosition::new(appended.address.offset, appended.address.len);
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
            Ok(None) => {
                if self.live_stream {
                    let adopt_current =
                        self.state
                            .lock()
                            .open_live_extent
                            .as_ref()
                            .is_some_and(|open| {
                                !open.closing
                                    && address.offset >= open.extent.start
                                    && address
                                        .offset
                                        .checked_add(u64::from(address.len))
                                        .is_some_and(|end| end <= open.extent.end)
                            });
                    if adopt_current {
                        self.self_adopt_live_extent()?;
                        if let Some(registered) = self.references.lookup(address)? {
                            return Ok(Some(registered));
                        }
                    }
                }
                let fallback = {
                    let state = self.state.lock();
                    let replay = state.wal_root.is_some();
                    let leased = Self::leased_address(&state, address);
                    if replay || leased {
                        let allocated = state
                            .writer
                            .as_ref()
                            .map_or(state.appended_end, WalWriter::position);
                        if address
                            .offset
                            .checked_add(u64::from(address.len))
                            .is_none_or(|end| end > allocated)
                            || state.wal_root.as_ref().is_some_and(|root| {
                                root.retired.contains(
                                    address.offset / self.config().wal_file_bytes
                                        * self.config().wal_file_bytes,
                                )
                            })
                        {
                            return Err(StorageError::Invalid(
                                "WAL member differs from the reference",
                            ));
                        }
                        true
                    } else {
                        false
                    }
                };
                if fallback {
                    let _admission = self.read_admission()?;
                    return self.member_authority(address).map(Some);
                }
                if self.live_stream {
                    self.self_adopt_live_extent()?;
                    return self.references.lookup(address);
                }
                Ok(None)
            }
            result => result,
        }
    }

    fn position_covers_batch(&self, address: PhysicalValueAddress) -> bool {
        address
            .offset
            .checked_add(u64::from(address.len))
            .is_some_and(|end| {
                self.state
                    .lock()
                    .wal_root
                    .as_ref()
                    .is_some_and(|root| end <= root.synced_end)
            })
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
        registered.validate_reference(reference)?;
        if registered.group_end() > self.readable_frontier() {
            // A native-tier caller may already have written a pre-leased frame
            // while close of the old extent is pending. Its checked WAL member
            // is readable without adoption, but never without a synchronous
            // tier's durable frontier.
            if !self.serve_unsynced.load(Ordering::Acquire)
                || !Self::leased_address(&self.state.lock(), reference.batch_address)
            {
                return Err(StorageError::NeedsPersistence);
            }
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
        match self.validate_record_authority(reference, &registered) {
            Err(StorageError::NeedsPersistence)
                if self.position_covers_batch(reference.batch_address) =>
            {
                // A completed position proof is not a receipt publication.
                // Consume it through this read's own covering receipt barrier.
                self.persist_for_read()?;
                self.validate_record_authority(reference, &registered)?;
            }
            result => result?,
        }
        Ok(registered)
    }

    /// Blocking owner-serving boundary. A position proof can cover bytes without
    /// publishing the receipt frontier. The read obtains its own covering barrier
    /// before record bytes or a namespace grant leave the owner.
    fn load_serving_record_authority(
        &self,
        reference: &ValueRecordReference,
    ) -> Result<Arc<RegisteredBatch>, StorageError> {
        match self.load_durable_record_authority(reference) {
            Err(StorageError::NeedsPersistence) if self.live_stream => {
                self.persist_for_read()?;
                self.load_durable_record_authority(reference)
            }
            result => result,
        }
    }

    /// On-demand data barrier for a read beyond the durable prefix. A read is
    /// not a lifecycle barrier: a live root keeps the syncer's bounded policy.
    fn persist_for_read(&self) -> Result<(), StorageError> {
        self.self_adopt_live_extent()?;
        self.persist_scope_with(PersistScope::Appended, None, syncer_root_policy(false))?;
        self.state.lock().stats.read_persist_requests += 1;
        Ok(())
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
    #[cfg(test)]
    pub(crate) fn validate_durable_record_reference(
        &self,
        reference: &ValueRecordReference,
    ) -> Result<(), StorageError> {
        self.load_serving_record_authority(reference)?;
        Ok(())
    }
    /// Cold authority lookup for tests of the owner serving boundary. The
    /// returned plan owns both descriptor authority and the native file.
    #[cfg(test)]
    pub(crate) fn plan_record_read(
        self: &Arc<Self>,
        reference: &ValueRecordReference,
    ) -> Result<RecordReadPlan, StorageError> {
        let registered = self.load_serving_record_authority(reference)?;
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
            _authority: Some(registered),
            _log: self.clone(),
        })
    }
    /// Cold leased authority is scoped to this exact member. The captured inode
    /// and charged descriptors outlive unlocked I/O; neither adoption's round
    /// gate nor the durable-metadata gate participates in this read barrier.
    ///
    /// `address` names the member frame itself (offset including its native
    /// 8-byte header, length the header plus entry), so no descriptor is needed:
    /// the coordinator index holds the position and the payload CRC.
    ///
    /// `group_end` is the receipt group end of the batch that owns the member,
    /// passed only by reference callers: their sparse sync proof requires the
    /// whole group to be released. A hybrid address caller has no group, so the
    /// member's own end is the only bound and `None` states that explicitly.
    fn resolve_leased_member(
        self: &Arc<Self>,
        address: PhysicalValueAddress,
        group_end: Option<u64>,
    ) -> Result<Option<super::nvmeof::live_storage::LiveRecordLease>, StorageError> {
        let captured = {
            let state = self.state.lock();
            self.check_cold_read_state(&state, address)?;
            let Some(open) = Self::record_extent(&state, address) else {
                return Ok(None);
            };
            (
                open.extent.clone(),
                Arc::clone(&open.allocation.file),
                state.wal_root.as_ref().map_or(0, |root| root.synced_end),
            )
        };
        let (extent, file, synced_end) = captured;
        #[cfg(test)]
        self.pause_live_at(LivePausePoint::ColdReadBeforeSync);
        if !self.serve_unsynced.load(Ordering::Acquire)
            && group_end.is_none_or(|end| end > self.readable_frontier())
            && address.offset + u64::from(address.len) > synced_end
        {
            // The lease made the inode and its name durable before export.
            // fdatasync covers this checked member independently of the owner's
            // receipt prefix. Never install this sparse proof in any frontier.
            #[cfg(test)]
            let result = if self.fail_persist.swap(false, Ordering::AcqRel) {
                Err(io::Error::other("injected cold read persistence failure"))
            } else {
                file.sync_data()
            };
            #[cfg(not(test))]
            let result = file.sync_data();
            if let Err(error) = result {
                self.poison_writer();
                return Err(StorageError::Unknown(WalError::Io(error)));
            }
            let mut state = self.state.lock();
            state.stats.cold_grant_syncs += 1;
            #[cfg(test)]
            {
                state.live_test_diagnostics.cold_read_syncs += 1;
            }
        }
        #[cfg(test)]
        self.pause_live_at(LivePausePoint::ColdReadAfterSync);
        {
            let state = self.state.lock();
            if !self.check_cold_read_extent(&state, address, &extent)? {
                return Ok(None);
            }
        }
        let plan = RecordReadPlan {
            member: crate::wal::WalMemberReadPlan {
                file,
                // One source of geometry: the WAL layout owns the file-offset
                // arithmetic, so a config/layout disagreement cannot make this
                // path read a different offset than `Wal::plan_frame`.
                offset: self
                    .wal
                    .offset_in_file(WalPosition::new(address.offset, address.len)),
                len: address.len as usize,
            },
            _authority: None,
            _log: self.clone(),
        };
        #[cfg(test)]
        self.pause_live_at(LivePausePoint::ColdReadBeforePin);
        let lease = {
            let _admission = self.read_admission()?;
            self.live_lease(plan, address.offset)?
        };
        #[cfg(test)]
        self.pause_live_at(LivePausePoint::ColdReadAfterPin);
        // Do not take state while holding read_admission: retirement takes them
        // in the reverse order. The installed live pin now excludes retirement.
        let state = self.state.lock();
        if !self.check_cold_read_extent(&state, address, &extent)? {
            return Ok(None);
        }
        Ok(Some(lease))
    }

    /// Resolve a durable record reference to an owner-pinned file range.
    ///
    /// Kept for the reference-directory callers that still exist until S11
    /// deletes them. The descriptor is validated before it is reduced to the
    /// member address [`Self::resolve_live_member`] serves, so a forged or
    /// foreign reference fails here exactly as before.
    pub fn resolve_live_record(
        self: &Arc<Self>,
        reference: &ValueRecordReference,
    ) -> Result<super::nvmeof::live_storage::LiveRecordLease, StorageError> {
        if self.live_stream {
            let captured = {
                let state = self.state.lock();
                self.check_cold_read_state(&state, reference.batch_address)?;
                Self::record_extent(&state, reference.batch_address)
                    .map(|open| Arc::clone(&open.allocation.file))
            };
            if let Some(file) = captured {
                let registered = match self.references.lookup(reference.batch_address)? {
                    Some(registered) => registered,
                    None => self.member_authority_from_file(reference.batch_address, &file)?,
                };
                registered.validate_reference(reference)?;
                let address = member_address_of_reference(reference)?;
                if let Some(lease) =
                    self.resolve_leased_member(address, Some(registered.group_end()))?
                {
                    return Ok(lease);
                }
            }
        }
        let registered = self.load_serving_record_authority(reference)?;
        let _admission = self.read_admission()?;
        let plan = self.plan_registered_record_read(reference, registered)?;
        self.live_lease(plan, reference.batch_address.offset)
    }

    /// Resolve a durable receipt to its complete inner native CRC frame for
    /// recovery. Metadata only; the caller reads and validates the frame.
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
            if !self.position_covers_batch(receipt.address) {
                return Err(StorageError::NeedsPersistence);
            }
            self.persist_for_read()?;
            if registered.group_end() > self.readable_frontier() {
                return Err(StorageError::NeedsPersistence);
            }
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
            _authority: Some(registered),
            _log: self.clone(),
        };
        self.live_lease(plan, receipt.address.offset)
    }

    pub fn live_file_count(&self) -> Result<u64, StorageError> {
        let state = self.state.lock();
        if !state.live {
            return Err(StorageError::Invalid("not a live WAL stream"));
        }
        Ok(state
            .writer
            .as_ref()
            .ok_or(StorageError::Sealed)?
            .position()
            .div_ceil(self.config().wal_file_bytes))
    }

    pub fn live_segment(
        &self,
        file_index: u64,
    ) -> Result<super::nvmeof::live_storage::LiveWalSegment, StorageError> {
        let config = self.config();
        let path = config.layout().wal_file_name(
            &self.path,
            crate::wal::position::WalFileId::from_u64(file_index),
        );
        let metadata = fs::metadata(&path)?;
        if metadata.len() != config.wal_file_bytes || !metadata.len().is_multiple_of(4096) {
            return Err(StorageError::Invalid("invalid live namespace geometry"));
        }
        Ok(super::nvmeof::live_storage::LiveWalSegment {
            database: config.inventory.database,
            partition: config.partition,
            epoch: config.inventory.epoch,
            generation: config.inventory.segment_generation,
            file_index,
            path,
            bytes: metadata.len(),
            device: metadata.dev(),
            inode: metadata.ino(),
            block_size: 4096,
        })
    }

    fn live_lease(
        &self,
        plan: RecordReadPlan,
        global_offset: u64,
    ) -> Result<super::nvmeof::live_storage::LiveRecordLease, StorageError> {
        use super::nvmeof::live_storage::LiveRecordLease;
        let config = self.config();
        let file_index = global_offset / config.wal_file_bytes;
        let metadata = plan.member.file.metadata()?;
        let offset = plan.member.offset;
        let len = u32::try_from(plan.member.len).map_err(|_| StorageError::Capacity)?;
        let segment = self.live_segment(file_index)?;
        if metadata.dev() != segment.device
            || metadata.ino() != segment.inode
            || metadata.len() != segment.bytes
        {
            return Err(StorageError::Invalid(
                "WAL pathname no longer names pinned inode",
            ));
        }
        let file_pin = self
            .wal
            .read_pin_counter(global_offset, &metadata)?
            .ok_or_else(|| {
                StorageError::Io(io::Error::new(io::ErrorKind::NotFound, "WAL file retired"))
            })?;
        #[cfg(test)]
        self.pause_live_at(LivePausePoint::ReadPinBeforeIncrement);
        // Admission excludes closure until the lease owns this count.
        file_pin
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |pins| {
                (pins < Wal::READ_PIN_CLOSED - 1).then(|| pins + 1)
            })
            .map_err(|pins| {
                if pins & Wal::READ_PIN_CLOSED != 0 {
                    StorageError::Io(io::Error::new(io::ErrorKind::NotFound, "WAL file retired"))
                } else {
                    StorageError::Capacity
                }
            })?;
        if self
            .live_read_pins
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |pins| {
                pins.checked_add(1)
            })
            .is_err()
        {
            file_pin.fetch_sub(1, Ordering::Release);
            return Err(StorageError::Capacity);
        }
        Ok(LiveRecordLease {
            segment,
            offset,
            len,
            _plan: plan,
            _file_pin: file_pin,
        })
    }

    pub(crate) fn release_live_read_pin(&self) {
        self.live_read_pins.fetch_sub(1, Ordering::AcqRel);
    }

    fn read_admission(&self) -> Result<parking_lot::RwLockReadGuard<'_, bool>, StorageError> {
        let admission = self.read_admission.read();
        if !*admission {
            return Err(StorageError::Poisoned);
        }
        Ok(admission)
    }

    #[cfg(test)]
    pub(crate) fn evict_authority_cache(&self) {
        self.references.evict_cache();
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

    pub fn live_file_preparation_stats(&self) -> LiveFilePreparationStats {
        let mut stats = self.state.lock().live_file_preparation;
        (
            stats.live_file_prewrite_bytes,
            stats.live_file_prewrite_ns,
            stats.live_file_prewrite_files,
        ) = self.wal.external_prewrite_stats();
        stats
    }

    #[cfg(test)]
    pub(crate) fn live_test_lease_timing(&self) -> LiveTestLeaseTiming {
        self.state.lock().live_test_diagnostics.lease
    }

    #[cfg(test)]
    pub(crate) fn record_test_lease_publication(
        &self,
        inventory_wait_ns: u64,
        publication_call_ns: u64,
        lookup_inventory_wait_ns: u64,
    ) {
        let mut state = self.state.lock();
        state.live_test_diagnostics.lease.inventory_wait_ns = inventory_wait_ns;
        state.live_test_diagnostics.lease.publication_call_ns = publication_call_ns;
        state.live_test_diagnostics.lease.lookup_inventory_wait_ns = lookup_inventory_wait_ns;
    }

    #[cfg(test)]
    pub(crate) fn record_test_file_export(
        &self,
        file_index: u64,
        inventory_attempts: u64,
        export_ns: u64,
        success: bool,
    ) {
        let mut state = self.state.lock();
        let diagnostics = &mut state.live_test_diagnostics;
        if diagnostics.exports.len() < 64 {
            diagnostics.exports.push(LiveTestExport {
                file_index,
                inventory_attempts,
                export_ns,
                success,
            });
        } else {
            diagnostics.dropped_exports += 1;
        }
    }

    #[cfg(test)]
    pub(crate) fn live_test_diagnostics(&self) -> LiveTestDiagnostics {
        self.state.lock().live_test_diagnostics.clone()
    }

    /// Metadata residency counters, not value-cache hits or physical read IOPS.
    pub fn authority_cache_stats(&self) -> AuthorityCacheStats {
        self.references.read_stats()
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

    /// Fail the next live-progress nudge on a live stream, including an idle
    /// one that production would answer without persisting. Arm it only in a
    /// quiesced fixture: a background nudge can consume it first.
    #[cfg(test)]
    pub(crate) fn fail_next_live_progress(&self) {
        self.failed_live_progress.store(false, Ordering::Release);
        self.fail_live_progress.store(true, Ordering::Release);
    }

    /// Whether the injected progress failure fired. It proves the nudge
    /// reached the hook, not that an appended persist was attempted.
    #[cfg(test)]
    pub(crate) fn live_progress_failure_consumed(&self) -> bool {
        self.failed_live_progress.load(Ordering::Acquire)
    }

    /// Whether the syncer reached its hold point (after the self-adoption of
    /// the live extent, before the appended persist) since the last take.
    /// It proves the setup happened, not that the persist ran or failed.
    #[cfg(test)]
    pub(crate) fn syncer_persist_hold_reached(&self) -> bool {
        self.syncer_persist_hold_reached.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn take_syncer_persist_hold_reached(&self) -> bool {
        self.syncer_persist_hold_reached
            .swap(false, Ordering::AcqRel)
    }

    /// Fail a sealed retirement after target revoke and before root install.
    #[cfg(test)]
    pub(crate) fn fail_next_sealed_before_root(&self) {
        self.fail_sealed_before_root.store(true, Ordering::Release);
    }

    #[cfg(test)]
    pub(crate) fn take_sealed_before_root_failure(&self) -> bool {
        self.fail_sealed_before_root.swap(false, Ordering::AcqRel)
    }

    /// Fail one appended persist after its serialization lock is held.
    #[cfg(test)]
    pub(crate) fn fail_next_appended_persist(&self) {
        self.failed_appended_persist.store(false, Ordering::Release);
        self.fail_appended_persist.store(true, Ordering::Release);
    }

    /// Whether the dedicated appended failure was consumed.
    #[cfg(test)]
    pub(crate) fn appended_persist_failure_consumed(&self) -> bool {
        self.failed_appended_persist.load(Ordering::Acquire)
    }

    /// Fail the next `persist_wal_through` at `phase`: 4 before the data sync,
    /// 5 after the data and namespace syncs before the root install, 6 after
    /// the root install (`wal_replay.rs` `fail_position_at`). Unlike
    /// `fail_next_persist`, a phase names where in the persist it fails, so a
    /// persist that runs earlier does not consume it by accident (arm it
    /// right before the persist under test).
    #[cfg(test)]
    pub(crate) fn fail_next_position_persist(&self, phase: u8) {
        self.fail_wal_root.store(phase, Ordering::Release);
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

    /// Handle-local append count, including evicted replay-window receipts.
    /// WAL replay restarts this count at the persisted scan position.
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
            if start_after.is_some_and(|after| offset <= after) || self.is_retired(offset)? {
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

    pub fn seal_epoch(&self, authority: RequestAuthority) -> Result<(), StorageError> {
        self.seal_epoch_impl(authority, || {
            Err(StorageError::Invalid(
                "live WAL seal requires target revocation",
            ))
        })
    }

    /// The control service's exclusive FIFO barrier waits for stream requests,
    /// so its seal cannot overlap close. Direct API callers get the same
    /// exclusion from the state-owned sealing flag; closing is refused.
    pub fn seal_live_epoch(
        &self,
        authority: RequestAuthority,
        revoke_and_drain: impl FnOnce() -> Result<(), StorageError>,
    ) -> Result<(), StorageError> {
        self.seal_epoch_impl(authority, revoke_and_drain)
    }

    fn finish_live_extent_for_seal(&self) -> Result<(), StorageError> {
        // Revocation has drained external writes. Each retry either discovers
        // another frame or drains pressure; the finite extent bounds progress.
        for _ in 0..2 * (self.config().fragment_bytes / 4096 + 2) {
            self.self_adopt_live_extent()?;
            let adopted = self.state.lock().appended_end;
            self.persist_scope(PersistScope::Appended, None)?;
            // Invariant: an appended persist that returns Ok covers every frame
            // that was appended when it started. Persistence is monotone, and
            // `PersistScope::Appended` names the whole appended prefix, so
            // `durable_end` reaches `adopted`; a short persist is a defect, not
            // a seal state to recover from. The syncer is already retired: this
            // persist is the only barrier that makes the frames the seal just
            // adopted durable (paper 3.1: the finalizing sync publishes them).
            let durable = self.state.lock().durable_end;
            debug_assert!(
                durable >= adopted,
                "the seal persist must cover {adopted} adopted bytes but covers {durable}"
            );
            let _persistence = self.persistence.lock();
            let mut state = self.state.lock();
            if state.poison.is_some() {
                return Err(StorageError::Poisoned);
            }
            let Some(mut open) = state.open_live_extent.take() else {
                return Ok(());
            };
            if open.closing {
                state.open_live_extent = Some(open);
                return Err(StorageError::Invalid("live extent is closing"));
            }
            let end = open.extent.end;
            match self.adopt_live_frame(&mut state, &mut open, end) {
                Ok(LiveFrameStep::End) => {
                    // Revocation has drained writes; a valid skip can bridge
                    // the current extent into the already granted next one.
                    drop(open);
                    state.open_live_extent = state.next_live_extent.take();
                    if state.open_live_extent.is_none() {
                        return Ok(());
                    }
                }
                Err(StorageError::IncompleteLiveFrame) => {
                    if state.next_live_extent.is_some() {
                        // Sealing excludes new writes/adoptions. The local
                        // allocation pins the inode during unlocked suffix I/O.
                        drop(state);
                        let suffix = self.validate_live_extent_close(
                            &open.allocation.file,
                            &mut open.buffer,
                            open.adoption.adopted_end,
                            end,
                            open.extent.segment.bytes,
                        );
                        let mut state = self.state.lock();
                        match suffix {
                            Ok(()) => {
                                drop(open);
                                state.open_live_extent = state.next_live_extent.take();
                                continue;
                            }
                            Err(StorageError::IncompleteLiveFrame) => {
                                let next = state
                                    .next_live_extent
                                    .as_ref()
                                    .ok_or(StorageError::Poisoned)?;
                                open.buffer.resize(4096, 0);
                                if let Err(error) = next.allocation.file.read_exact_at(
                                    &mut open.buffer,
                                    next.extent.start % next.extent.segment.bytes,
                                ) {
                                    state.open_live_extent = Some(open);
                                    state.poison = Some(WriterPoison::Fatal);
                                    return Err(StorageError::Io(error));
                                }
                                if open.buffer.iter().any(|byte| *byte != 0) {
                                    state.open_live_extent = Some(open);
                                    state.poison = Some(WriterPoison::Fatal);
                                    return Err(StorageError::Invalid(
                                        "live frames beyond a torn extent tail",
                                    ));
                                }
                            }
                            Err(error) => {
                                state.open_live_extent = Some(open);
                                state.poison = Some(WriterPoison::Fatal);
                                return Err(error);
                            }
                        }
                        state.next_live_extent = None;
                    }
                    // An unwritten/torn tail with no later bytes is discarded.
                    drop(open);
                    return Ok(());
                }
                Ok(LiveFrameStep::Adopted | LiveFrameStep::Pressure(_)) => {
                    state.open_live_extent = Some(open);
                }
                Err(error) => {
                    state.open_live_extent = Some(open);
                    state.poison = Some(WriterPoison::Fatal);
                    return Err(error);
                }
            }
        }
        self.state.lock().poison = Some(WriterPoison::Fatal);
        Err(StorageError::Invalid("live seal exceeded bounded progress"))
    }

    fn seal_epoch_impl(
        &self,
        authority: RequestAuthority,
        revoke_and_drain: impl FnOnce() -> Result<(), StorageError>,
    ) -> Result<(), StorageError> {
        if authority.epoch != self.config().inventory.epoch {
            return Err(StorageError::Fenced);
        }
        let exclusive = {
            let mut state = self.state.lock();
            if state.live && !state.sealed {
                if state.is_sealing() {
                    return Err(StorageError::Invalid("live stream is sealing"));
                }
                if state
                    .open_live_extent
                    .as_ref()
                    .is_some_and(|open| open.closing)
                    || state
                        .next_live_extent
                        .as_ref()
                        .is_some_and(|open| open.closing)
                {
                    return Err(StorageError::Invalid("live extent is closing"));
                }
                state.lifecycle = Lifecycle::Sealing;
                true
            } else {
                false
            }
        };
        let result = self.seal_epoch_inner(authority, revoke_and_drain);
        if exclusive {
            self.state.lock().lifecycle = Lifecycle::Running;
        }
        result
    }

    fn seal_epoch_inner(
        &self,
        authority: RequestAuthority,
        revoke_and_drain: impl FnOnce() -> Result<(), StorageError>,
    ) -> Result<(), StorageError> {
        let mut revoke_and_drain = Some(revoke_and_drain);
        let has_extents = {
            let state = self.state.lock();
            state.open_live_extent.is_some() || state.next_live_extent.is_some()
        };
        if has_extents {
            revoke_and_drain
                .take()
                .ok_or(StorageError::Invalid("missing live revocation"))?()?;
            self.finish_live_extent_for_seal()?;
        }
        let _persistence = self.persistence.lock();
        let mut state = self.state.lock();
        if state.sealed {
            return if state.poison.is_some() {
                Err(StorageError::Poisoned)
            } else {
                Ok(())
            };
        }
        if state.live {
            if let Some(revoke_and_drain) = revoke_and_drain.take() {
                revoke_and_drain()?;
            }
            if state.appended_end != state.durable_end {
                state.poison = Some(WriterPoison::Fatal);
                return Err(StorageError::Invalid(
                    "unacknowledged live suffix requires recovery",
                ));
            }
        }
        if state.poison.is_some() {
            return Err(StorageError::Poisoned);
        }
        // Stop writer workers while retaining exclusive ownership of the path.
        drop(state.open_live_extent.take());
        drop(state.next_live_extent.take());
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
            if let Some(root) = state.wal_root.as_mut().map(Arc::make_mut) {
                root.generation = root
                    .generation
                    .checked_add(1)
                    .ok_or(StorageError::Capacity)?;
                root.install(&self.path)?;
            }
            install_metadata(&self.path, SEAL, &encode_metadata(&end)?)?;
            Ok(end)
        })();
        let end = result.map_err(|error| {
            state.poison = Some(WriterPoison::Fatal);
            StorageError::Unknown(WalError::Io(io::Error::other(error)))
        })?;
        // The seal synced every registered file under this lock hold, so it covers the
        // whole appended tail. Recovery rejects a scan that ends below the sealed end,
        // so the seal bounds the report like a live root.
        let captured = state.progress.capture();
        state.durable_end = end.physical_end;
        state.progress.synced(captured, end.physical_end);
        self.durable_frontier
            .store(end.physical_end, Ordering::Release);
        Ok(())
    }
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

pub(crate) fn member_position(
    group: WalPosition,
    member_offset: u32,
    batch: &CompleteBatch,
) -> WalPosition {
    // CompleteGroup has validated bounds and alignment; native allocation fits
    // the outer frame inside one fragment, so every child shares that WAL file.
    WalPosition::new(
        group.offset() + 8 + u64::from(member_offset),
        (batch.encoded().len() + 8) as u32,
    )
}

pub(crate) fn receipt_for(
    config: &PartitionConfig,
    batch: &CompleteBatch,
    pos: WalPosition,
) -> AppendedBatch {
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
    let end = state
        .references
        .receipt_group_end(receipt)
        .map_err(|error| {
            if state.wal_root.is_some()
                && matches!(error, StorageError::Invalid("receipt not from this log"))
            {
                StorageError::Invalid("receipt outside the replay window")
            } else {
                error
            }
        })?;
    if state.poison.is_some() {
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

/// Test-only measurement arm: restore the pre-change write path (root on
/// every syncer barrier, no last-moment merge) in this process.
#[cfg(test)]
pub(crate) static BASELINE_WRITE_PATH: AtomicBool = AtomicBool::new(false);

/// True when the test-only baseline arm is selected; always false otherwise.
#[cfg(test)]
pub(crate) fn baseline_write_path() -> bool {
    // Relaxed: a measurement arm selected before the fixture opens.
    BASELINE_WRITE_PATH.load(Ordering::Relaxed)
}

#[cfg(not(test))]
pub(crate) fn baseline_write_path() -> bool {
    false
}

fn syncer_root_policy(stopping: bool) -> RootPolicy {
    if stopping || baseline_write_path() {
        RootPolicy::Barrier
    } else {
        RootPolicy::Bounded
    }
}

/// Live-frontier root policy of one persistence barrier.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RootPolicy {
    /// Install the root covering the captured prefix, also when only the root
    /// lags. Every caller except the background syncer.
    Barrier,
    /// Background syncer: install the root only on a fragment crossing or with
    /// on-disk history; otherwise sync the WAL and defer the root.
    Bounded,
}

fn live_root_lags(state: &State) -> bool {
    state.live && state.live_root_end < state.durable_end
}

/// Pending WAL data, or for a barrier a lagging live root covered at the
/// current appended end (the data below it is already durable).
fn pending_with_root(
    state: &State,
    scope: PersistScope<'_>,
    root: RootPolicy,
) -> Result<Option<u64>, StorageError> {
    let pending = pending_scope(state, scope)?;
    if pending.is_none() && root == RootPolicy::Barrier && live_root_lags(state) {
        if persistence_poisoned(state, scope) {
            return Err(StorageError::Poisoned);
        }
        return Ok(Some(state.durable_end));
    }
    Ok(pending)
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
    match state.poison {
        None => false,
        Some(WriterPoison::IncompleteLiveFrame) => !matches!(_scope, PersistScope::Appended),
        Some(WriterPoison::Fatal) => true,
    }
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

/// Live-frontier root I/O done by one persistence barrier.
#[derive(Default)]
struct LiveFrontierIo {
    file_syncs: u64,
    namespace_syncs: u64,
    installs: u64,
    bytes: u64,
}

/// Commit the CRC-protected live durability frontier after the WAL file and
/// its namespace witnesses, before returning any durable receipt.
impl PartitionLog {
    fn persist_live_frontier(
        &self,
        frontier: &LiveFrontier,
        timing: &mut WriteTiming<'_>,
        counts: &mut LiveFrontierIo,
    ) -> io::Result<()> {
        let frontier_sync_span = super::owner_events::OwnerEventSpan::new(
            super::owner_events::OwnerEventKind::FrontierSyncStart,
            super::owner_events::OwnerEventKind::FrontierSyncEnd,
            self.config().partition,
            frontier.end,
            0,
        );
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
        counts.bytes += bytes.len() as u64;
        timing.record(|profile| {
            profile.live_frontier_file_calls = profile.live_frontier_file_calls.saturating_add(1)
        });
        #[cfg(test)]
        if self.fail_live_frontier.load(Ordering::Acquire) == 1 {
            return Err(io::Error::other("injected live frontier file sync failure"));
        }
        file.sync_all()?;
        counts.file_syncs += 1;
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
        counts.namespace_syncs += 1;
        counts.installs += 1;
        timing.record(|profile| {
            profile.live_frontier_namespace_successes =
                profile.live_frontier_namespace_successes.saturating_add(1)
        });
        timing.stage(None);
        frontier_sync_span.end_at(frontier.end);
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
mod native_layout_tests;
#[cfg(test)]
mod tests;
