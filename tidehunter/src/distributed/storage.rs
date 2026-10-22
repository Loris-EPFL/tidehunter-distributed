//! Native WAL-backed partition authority with bounded resident pending metadata.
//!
//! Completed receipts and descriptors use CRC-protected pageable disk history.
//! Recovery permanently seals an old epoch and replays only its unrooted suffix.
//! Whole-file retirement requires an explicit durable recovery/reachability
//! decision; no live values move and physical addresses are never reused.

use super::codec::{BatchLimits, CodecError, CompleteBatch, Mutation};
use super::group::{CompleteGroup, GROUP_HEADER_LEN, GroupError, GroupLimits};
use super::reference_directory::{
    DirectoryError, ReferenceDirectory, ReferenceDirectoryWriter, RegisteredBatch,
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

mod file_inventory;
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

    /// Only the authenticated transport boundary may reconstruct this proof.
    /// It must first bind the reply to the requested database, version, digest,
    /// registered partition/generation and physical bounds. Deserializing an
    /// arbitrary peer receipt is not evidence of persistence.
    pub(crate) fn from_authenticated_parts(
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

struct State {
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
    poisoned: bool,
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
    /// Authority/admission/retry checks and descriptor/native-write preparation.
    pub append_validation_ns: u64,
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
            persist_publication_ns,
            file_persist_calls,
            file_persist_successes,
            namespace_persist_calls,
            namespace_persist_successes,
            durable_publications,
        );
    }
}

/// A stage selector avoids borrowing individual profile fields across storage
/// operations. Drop closes the last stage on every return and during unwinding.
#[derive(Clone, Copy)]
enum WriteStage {
    AppendLock,
    AppendValidation,
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
    PersistPublication,
}

impl WriteStage {
    fn elapsed(self, profile: &mut StorageWriteProfile) -> &mut u64 {
        match self {
            Self::AppendLock => &mut profile.append_state_lock_wait_ns,
            Self::AppendValidation => &mut profile.append_validation_ns,
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
                let elapsed = stage.elapsed(profile);
                *elapsed = elapsed.saturating_add(duration_ns(now.duration_since(started)));
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
    worker_claimed: AtomicBool,
    // Serialize disk barriers/sealing without blocking immutable reference reads.
    persistence: Mutex<()>,
    // New file pins serialize only with retirement, never ordinary WAL fsync.
    // False permanently closes read admission after an interrupted retirement.
    read_admission: parking_lot::RwLock<bool>,
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
                writer: Some(writer),
                references: reference_writer,
                appended_end: 0,
                durable_end: 0,
                namespace_synced_file: None,
                poisoned: false,
                sealed: false,
                stats: PartitionStats::default(),
            }),
            references,
            durable_frontier: AtomicU64::new(0),
            worker_claimed: AtomicBool::new(false),
            persistence: Mutex::new(()),
            read_admission: parking_lot::RwLock::new(true),
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
            wal,
            registration,
            shape,
            device: fs::metadata(&path)?.dev(),
            path,
            _lock: lock,
            _local_authority: None,
        })
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
        let mut start = root.end / expected.fragment_bytes * expected.fragment_bytes;
        while start < scan_end {
            if references.is_retired(start / expected.wal_file_bytes * expected.wal_file_bytes)? {
                start = (start / expected.wal_file_bytes + 1) * expected.wal_file_bytes;
                continue;
            }
            let scan_position =
                if start == root.end / expected.fragment_bytes * expected.fragment_bytes {
                    root.end
                        .checked_add(7)
                        .map(|end| end & !7)
                        .ok_or(StorageError::Capacity)?
                } else {
                    start
                };
            let mut scan = match wal.wal_iterator_for_scan(scan_position) {
                Ok(scan) => scan,
                Err(WalError::EndOfWal) => {
                    start = start
                        .checked_add(expected.fragment_bytes)
                        .ok_or(StorageError::Capacity)?;
                    continue;
                }
                Err(error) => return Err(StorageError::Wal(error)),
            };
            loop {
                let (position, bytes) = match scan.next() {
                    Ok(record) => record,
                    // Serialized append + fail-stop after any unknown error ensure
                    // there can be no later complete frame beyond this torn tail.
                    // This fault model excludes arbitrary corruption of durable data.
                    Err(WalError::Crc(_) | WalError::EndOfWal) => break,
                    Err(error) => return Err(StorageError::Wal(error)),
                };
                if position.offset() < root.end {
                    continue;
                }
                if position.offset() >= start + expected.fragment_bytes {
                    break;
                }
                let group = CompleteGroup::decode(
                    &shape,
                    bytes,
                    expected.batch_limits,
                    expected.group_limits,
                )?;
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
                reference_writer.persist_pending(scan_end.div_ceil(expected.wal_file_bytes))?;
            }
            start = start
                .checked_add(expected.fragment_bytes)
                .ok_or(StorageError::Invalid("WAL scan offset overflow"))?;
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
                writer: None,
                references: reference_writer,
                appended_end: physical_end,
                durable_end: physical_end,
                namespace_synced_file: None,
                poisoned: false,
                sealed: true,
                stats: PartitionStats::default(),
            }),
            references,
            durable_frontier: AtomicU64::new(physical_end),
            worker_claimed: AtomicBool::new(false),
            persistence: Mutex::new(()),
            read_admission: parking_lot::RwLock::new(true),
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
        if state.poisoned {
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

    fn append_group_impl(
        &self,
        authority: RequestAuthority,
        group: &CompleteGroup,
        profile: Option<&mut StorageWriteProfile>,
    ) -> Result<Vec<AppendedBatch>, StorageError> {
        let mut timing = WriteTiming::new(profile, true);
        timing.stage(Some(WriteStage::AppendValidation));
        let config = self.config();
        if authority.epoch != config.inventory.epoch {
            return Err(StorageError::Fenced);
        }
        timing.stage(Some(WriteStage::AppendLock));
        let mut state = self.state.lock();
        timing.stage(Some(WriteStage::AppendValidation));
        if state.poisoned {
            return Err(StorageError::Poisoned);
        }
        if state.sealed {
            return Err(StorageError::Sealed);
        }
        // The typed group's immutable bytes were verified by its private
        // constructor/decoder. Recheck schema and admission limits here without
        // hashing/copying all values again under the partition's admission lock.
        // Raw network/disk bytes must still use the complete group decoder.
        group.validate_for(&self.shape, config.batch_limits, config.group_limits)?;
        let mut previous = Vec::new();
        for batch in group.batches() {
            validate_batch(config, batch)?;
            if let Some(receipt) = self.references.lookup_id(batch.id())? {
                if receipt.version != batch.version() || receipt.digest != batch.digest() {
                    return Err(StorageError::IdentityConflict);
                }
                previous.push(receipt);
            } else if self.references.lookup_version(batch.version())?.is_some() {
                return Err(StorageError::VersionConflict);
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
        if state
            .references
            .check_capacity(group.batches().len(), descriptor_count)
            .is_err()
            || predicted_end.saturating_sub(rooted_end) > 64 * 1024 * 1024
        {
            // History pages may root only data at or below durable_end. Do not
            // spill while an appended suffix is awaiting the flusher.
            if physical_end(&state) > state.durable_end {
                return Err(StorageError::NeedsPersistence);
            }
            // Every buffered completed group already has its exact-file WAL
            // barrier. Root cold metadata before admitting another group.
            timing.stage(Some(WriteStage::AppendHistory));
            let files = state.durable_end.saturating_sub(1) / config.wal_file_bytes + 1;
            if let Err(error) = state.references.persist_pending(files).and_then(|()| {
                file_inventory::remove_covered(&self.path, self.references.root().file_count)
            }) {
                state.poisoned = true;
                return Err(error);
            }
            timing.stage(Some(WriteStage::AppendValidation));
        }
        state
            .references
            .check_capacity(group.batches().len(), descriptor_count)?;
        let mut authority_credit = state
            .references
            .reserve_group(group.batches().len(), descriptor_count)?;
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
        state.poisoned = true;
        timing.stage(Some(WriteStage::AppendWal));
        let guard = match state
            .writer
            .as_ref()
            .ok_or(StorageError::Sealed)?
            .write(&prepared)
        {
            Ok(guard) => guard,
            Err(error) => {
                state.poisoned = true;
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
            state.poisoned = true;
            return Err(StorageError::Unknown(WalError::Io(io::Error::other(error))));
        }
        state.appended_end = group_end;
        state.stats.appended_groups += 1;
        state.poisoned = false;
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

    fn persist_receipts(
        &self,
        receipts: &[AppendedBatch],
        profile: Option<&mut StorageWriteProfile>,
    ) -> Result<(), StorageError> {
        let mut timing = WriteTiming::new(profile, false);
        if receipts.is_empty() {
            timing.record(|profile| {
                profile.persist_noops = profile.persist_noops.saturating_add(1);
            });
            timing.success(0);
            return Ok(());
        }

        timing.stage(Some(WriteStage::PersistStateLock));
        let state = self.state.lock();
        timing.stage(Some(WriteStage::PersistValidation));
        let pending = pending_receipts(&state, receipts)?;
        drop(state);
        if pending.is_none() {
            timing.record(|profile| {
                profile.persist_noops = profile.persist_noops.saturating_add(1);
            });
            timing.success(receipts.len());
            return Ok(());
        }

        timing.stage(Some(WriteStage::PersistSerializationLock));
        let _persistence = self.persistence.lock();
        timing.stage(Some(WriteStage::PersistStateLock));
        let (start, end, namespace_synced_file) = {
            let mut state = self.state.lock();
            timing.stage(Some(WriteStage::PersistValidation));
            let Some(requested_end) = pending_receipts(&state, receipts)? else {
                timing.record(|profile| {
                    profile.persist_noops = profile.persist_noops.saturating_add(1);
                });
                timing.success(receipts.len());
                return Ok(());
            };
            #[cfg(test)]
            if self.fail_persist.swap(false, Ordering::AcqRel) {
                state.poisoned = true;
                return Err(StorageError::Unknown(WalError::Io(io::Error::other(
                    "injected persistence failure after append",
                ))));
            }
            if requested_end > state.appended_end {
                state.poisoned = true;
                return Err(StorageError::Unknown(WalError::Io(io::Error::other(
                    "receipt exceeds appended frontier",
                ))));
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
            Ok(())
        })();

        timing.stage(Some(WriteStage::PersistStateLock));
        let mut state = self.state.lock();
        timing.stage(Some(WriteStage::PersistPublication));
        state.stats.inventory_witnesses += witness_persisted;
        result.map_err(|error| {
            state.poisoned = true;
            StorageError::Unknown(WalError::Io(error))
        })?;
        if state.poisoned {
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
        timing.record(|profile| {
            profile.durable_publications = profile.durable_publications.saturating_add(1);
        });
        timing.success(receipts.len());
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
        let (_, bytes) = self
            .wal
            .read(WalPosition::new(
                appended.address.offset,
                appended.address.len,
            ))
            .map_err(StorageError::Wal)?;
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
        // The first lookup released every authority/cache/admission guard.
        // A cold worker may release completed residency once; no reactor caller
        // reaches this path. Never persist the outstanding unacknowledged group.
        let persistence = self.persistence.lock();
        let mut state = self.state.lock();
        if state.poisoned {
            return Err(StorageError::Poisoned);
        }
        let durable_end = state.durable_end;
        let files = durable_end.saturating_sub(1) / self.config().wal_file_bytes + 1;
        let spill = state
            .references
            .persist_completed(durable_end, files)
            .and_then(|changed| {
                if changed {
                    file_inventory::remove_covered(&self.path, self.references.root().file_count)?;
                }
                Ok(changed)
            });
        let changed = spill.map_err(|error| {
            state.poisoned = true;
            *self.read_admission.write() = false;
            StorageError::Unknown(WalError::Io(io::Error::other(error)))
        })?;
        drop(state);
        drop(persistence);
        if changed {
            self.references.lookup(address)
        } else {
            Err(StorageError::Capacity)
        }
    }

    fn validate_record_authority(
        &self,
        reference: &ValueRecordReference,
        registered: &RegisteredBatch,
    ) -> Result<(), StorageError> {
        registered.validate_reference(reference)?;
        if registered.group_end() > self.durable_frontier.load(Ordering::Acquire) {
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

    pub(crate) fn claim_worker(&self) -> Result<(), StorageError> {
        if self.state.lock().poisoned {
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
        self.state.lock().poisoned = true;
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
        if !state.sealed || state.writer.is_some() || state.poisoned {
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
        *admission = false;
        state.references.retire(files)?;
        self.wal.retire_files(&ids)?;
        *admission = true;
        Ok(())
    }

    pub fn seal_epoch(&self, authority: RequestAuthority) -> Result<(), StorageError> {
        if authority.epoch != self.config().inventory.epoch {
            return Err(StorageError::Fenced);
        }
        let _persistence = self.persistence.lock();
        let mut state = self.state.lock();
        if state.poisoned {
            return Err(StorageError::Poisoned);
        }
        if state.sealed {
            return Ok(());
        }
        // Stop writer workers while retaining exclusive ownership of the path.
        drop(state.writer.take());
        state.sealed = true; // I/O errors must never reopen this writer.
        if let Err(error) = self.wal.persist_registered_files() {
            state.poisoned = true;
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
            state.poisoned = true;
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
    if state.poisoned {
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
        write!(f, "{self:?}")
    }
}
impl std::error::Error for StorageError {}

#[cfg(test)]
mod crash_tests;
#[cfg(test)]
mod tests;
