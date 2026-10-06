//! Native WAL-backed, append-only partition substrate for the static prototype.
//!
//! Reuses Tidehunter's allocator, mmap, CRC, readers and WAL worker lifecycle.
//! There is no second value database or per-batch coordinator decision log.
//! Recovery permanently seals the original epoch; opening an old directory never
//! restarts its writer. There is deliberately no GC, remote lease, network server,
//! automatic failover, or claim of integration with the native index yet.

use super::codec::{BatchLimits, CodecError, CompleteBatch};
use super::types::{BatchId, BatchVersion, PayloadDigest, PhysicalValueAddress, RequestAuthority};
use crate::crc::CrcFrame;
use crate::key_shape::KeyShape;
use crate::lock::DbLock;
use crate::metrics::Metrics;
use crate::wal::{PreparedWalWrite, Wal, WalError, WalWriter};
use crate::{WalKind, WalLayout, WalPosition};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

const REGISTRATION: &str = "partition-registration-v1";
const SEAL: &str = "partition-seal-v1";
const MAX_METADATA_BYTES: u64 = 1024 * 1024;

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

/// Explicit limits for this experimental log, separate from native Db limits.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PartitionConfig {
    pub inventory: StaticInventory,
    pub partition: u32,
    pub batch_limits: BatchLimits,
    pub fragment_bytes: u64,
    pub wal_file_bytes: u64,
    /// Bounds the resident retry/reference index. Reaching it rejects new writes
    /// before effects; entries are never evicted and re-executed as new requests.
    pub max_batches: usize,
}

impl PartitionConfig {
    fn validate(&self) -> Result<(), StorageError> {
        self.inventory.validate()?;
        self.batch_limits.validate()?;
        if !self.inventory.partitions.contains(&self.partition)
            || self.fragment_bytes < self.batch_limits.max_encoded_bytes as u64 + 8
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
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AppendedBatch {
    id: BatchId,
    version: BatchVersion,
    digest: PayloadDigest,
    address: PhysicalValueAddress,
}

impl AppendedBatch {
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
}

struct State {
    // Writer drops before the directory lock. Native worker joins have a
    // timeout; production stop/join hardening remains a separate gate.
    writer: Option<WalWriter>,
    records: BTreeMap<BatchId, AppendedBatch>,
    versions: BTreeMap<BatchVersion, BatchId>,
    appended_end: u64,
    durable_end: u64,
    poisoned: bool,
    sealed: bool,
}

/// A local storage node component; caller threads serialize on each partition.
/// Different instances/partitions may append concurrently. It queues no payloads.
pub struct PartitionLog {
    state: Mutex<State>,
    wal: Arc<Wal>,
    registration: Registration,
    shape: KeyShape,
    path: PathBuf,
    _lock: DbLock,
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
            format: 1,
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
        let wal = Wal::open(&path, registration.config.layout(), metrics)?;
        wal.persist_registered_files()?;
        install_metadata(&path, REGISTRATION, &metadata)?;
        let writer = wal.writer_after(None).map_err(StorageError::Wal)?;
        Ok(Self {
            state: Mutex::new(State {
                writer: Some(writer),
                records: BTreeMap::new(),
                versions: BTreeMap::new(),
                appended_end: 0,
                durable_end: 0,
                poisoned: false,
                sealed: false,
            }),
            wal,
            registration,
            shape,
            path,
            _lock: lock,
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
        let registration: Registration = read_metadata(&path.join(REGISTRATION))?;
        if registration.format != 1 || registration.config != *expected {
            return Err(StorageError::Invalid(
                "registration differs from expected inventory/configuration",
            ));
        }
        let fragments = validate_file_inventory(&path, expected)?;
        let shape = KeyShape::from_yaml(&registration.shape_yaml)
            .map_err(|_| StorageError::Invalid("invalid stored key shape"))?;
        let wal = Wal::open(&path, expected.layout(), metrics)?;
        let mut records = BTreeMap::new();
        let mut versions = BTreeMap::new();
        let mut physical_end = 0;
        // Scan each known fragment independently. A crash can persist a complete
        // frame in the next fragment without persisting the preceding skip marker.
        // A complete frame never crosses a fragment boundary; this does not scan
        // arbitrary user bytes for magic. The pending-durability gate rules out
        // later authority records behind a torn batch within the same fragment.
        for start in fragments {
            let mut scan = wal
                .wal_iterator_for_scan(start)
                .map_err(StorageError::Wal)?;
            loop {
                let (position, bytes) = match scan.next() {
                    Ok(record) => record,
                    // Serialized append + fail-stop after any unknown error ensure
                    // there can be no later complete frame beyond this torn tail.
                    // This fault model excludes arbitrary corruption of durable data.
                    Err(WalError::Crc(_) | WalError::EndOfWal) => break,
                    Err(error) => return Err(StorageError::Wal(error)),
                };
                if position.offset() >= start + expected.fragment_bytes {
                    break;
                }
                let batch = CompleteBatch::decode(&shape, bytes, expected.batch_limits)?;
                validate_batch(expected, &batch)?;
                if records.len() == expected.max_batches {
                    return Err(StorageError::Capacity);
                }
                let receipt = receipt_for(expected, &batch, position);
                if records.insert(batch.id(), receipt).is_some()
                    || versions.insert(batch.version(), batch.id()).is_some()
                {
                    return Err(StorageError::Invalid(
                        "duplicate identity or version in authority log",
                    ));
                }
                physical_end = position.offset() + position.frame_len() as u64;
            }
        }
        let end = SealedEnd {
            format: 1,
            epoch: expected.inventory.epoch,
            generation: expected.inventory.segment_generation,
            frame_count: records.len(),
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
                records,
                versions,
                appended_end: physical_end,
                durable_end: physical_end,
                poisoned: false,
                sealed: true,
            }),
            wal,
            registration,
            shape,
            path,
            _lock: lock,
        })
    }

    pub fn config(&self) -> &PartitionConfig {
        &self.registration.config
    }

    pub fn append_complete_batch(
        &self,
        authority: RequestAuthority,
        batch: &CompleteBatch,
    ) -> Result<AppendedBatch, StorageError> {
        let config = self.config();
        if authority.epoch != config.inventory.epoch {
            return Err(StorageError::Fenced);
        }
        let mut state = self.state.lock();
        if state.poisoned {
            return Err(StorageError::Poisoned);
        }
        if state.sealed {
            return Err(StorageError::Sealed);
        }
        // Revalidate against this partition's persisted schema and limits; a
        // CompleteBatch made against a different schema is not trusted.
        // Hold the admission lock while decoding so waiting caller threads do
        // not each allocate another decoded vector inside the storage service.
        let batch =
            CompleteBatch::decode(&self.shape, batch.encoded().clone(), config.batch_limits)?;
        validate_batch(config, &batch)?;
        if let Some(previous) = state.records.get(&batch.id()) {
            return if previous.version == batch.version() && previous.digest == batch.digest() {
                Ok(*previous)
            } else {
                Err(StorageError::IdentityConflict)
            };
        }
        if state.versions.contains_key(&batch.version()) {
            return Err(StorageError::VersionConflict);
        }
        if state.records.len() == config.max_batches {
            return Err(StorageError::Capacity);
        }
        // Native replay stops at the first torn frame. Merely serializing mmap
        // copies does not serialize kernel writeback: a later unacknowledged
        // complete frame could survive behind an earlier torn frame. For this
        // first storage refinement allow only one unpersisted batch per stream.
        // Group commit needs a recoverable group container/allocation protocol.
        if physical_end(&state) > state.durable_end {
            return Err(StorageError::NeedsPersistence);
        }
        let prepared = PreparedWalWrite::new(&batch);
        // Every semantic/capacity check precedes the first authoritative bytes.
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
        let receipt = receipt_for(config, &batch, *guard.wal_position());
        drop(guard); // Storage copy completed; NOT native index publication.
        state.versions.insert(batch.version(), batch.id());
        state.records.insert(batch.id(), receipt);
        state.appended_end = receipt.address.offset + u64::from(receipt.address.len);
        Ok(receipt)
    }

    /// Conservatively persist the outstanding complete batch. Never promotes
    /// durability based on allocation or the maximum logical sequence.
    pub fn persist_through(&self, appended: AppendedBatch) -> Result<DurableBatch, StorageError> {
        let mut state = self.state.lock();
        if state.records.get(&appended.id) != Some(&appended) {
            return Err(StorageError::Invalid("receipt not from this log"));
        }
        if state.poisoned {
            return Err(StorageError::Poisoned);
        }
        let end = appended.address.offset + u64::from(appended.address.len);
        if end > state.durable_end {
            if let Err(error) = self.wal.persist_frame_file(WalPosition::new(
                appended.address.offset,
                appended.address.len,
            )) {
                state.poisoned = true;
                return Err(StorageError::Unknown(WalError::Io(error)));
            }
            state.durable_end = physical_end(&state);
        }
        Ok(DurableBatch(appended))
    }

    pub fn read_batch(&self, appended: AppendedBatch) -> Result<CompleteBatch, StorageError> {
        if self.state.lock().records.get(&appended.id) != Some(&appended) {
            return Err(StorageError::Invalid("unknown or foreign address"));
        }
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

    pub fn lookup(&self, id: BatchId) -> Option<AppendedBatch> {
        self.state.lock().records.get(&id).copied()
    }

    /// Read-only retries can resolve existing identities in sealed epochs.
    /// Absent identities remain absent; this API never reappends an old version.
    /// On an active log this enumerates appended records, including its one
    /// possibly unpersisted batch. It does not certify a logical completed prefix
    /// or durable/publicly visible history; use durability receipts separately.
    pub fn recovered_batches(&self) -> Vec<AppendedBatch> {
        let state = self.state.lock();
        state
            .versions
            .values()
            .map(|id| state.records[id])
            .collect()
    }

    pub fn seal_epoch(&self, authority: RequestAuthority) -> Result<(), StorageError> {
        if authority.epoch != self.config().inventory.epoch {
            return Err(StorageError::Fenced);
        }
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
        let end = SealedEnd {
            format: 1,
            epoch: authority.epoch,
            generation: self.config().inventory.segment_generation,
            frame_count: state.records.len(),
            physical_end: physical_end(&state),
        };
        let result = install_metadata(&self.path, SEAL, &encode_metadata(&end)?);
        if let Err(error) = result {
            state.poisoned = true;
            return Err(StorageError::Unknown(WalError::Io(error)));
        }
        state.durable_end = end.physical_end;
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
    let mut versions = BTreeMap::new();
    for log in &logs {
        if log.registration.shape_yaml != logs[0].registration.shape_yaml {
            return Err(StorageError::Invalid(
                "inconsistent database schema in registered inventory",
            ));
        }
        for receipt in log.recovered_batches() {
            if versions.insert(receipt.version, receipt.id).is_some() {
                return Err(StorageError::VersionConflict);
            }
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

fn physical_end(state: &State) -> u64 {
    state.appended_end
}

fn encode_metadata(value: &impl Serialize) -> Result<Vec<u8>, StorageError> {
    let bytes = serde_yaml::to_string(value)
        .map_err(|_| StorageError::Invalid("metadata serialization failed"))?
        .into_bytes();
    if bytes.len() as u64 + 8 > MAX_METADATA_BYTES {
        return Err(StorageError::Invalid("metadata exceeds limit"));
    }
    Ok(CrcFrame::new(&bytes).as_ref().to_vec())
}

fn read_metadata<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T, StorageError> {
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

fn install_metadata(path: &Path, name: &str, bytes: &[u8]) -> io::Result<()> {
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

fn validate_file_inventory(
    path: &Path,
    config: &PartitionConfig,
) -> Result<Vec<u64>, StorageError> {
    let mut ids = Vec::new();
    let mut fragments = Vec::new();
    // One full fragment per admitted batch is the worst case, plus bounded
    // native mapper lookahead. Reject corrupt length/ID fields before building
    // a huge recovery work list. No GC or segment reuse exists in this version.
    let max_fragments = config.max_batches + 16;
    let max_end = (max_fragments as u64) * config.fragment_bytes;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name
            .to_str()
            .ok_or(StorageError::Invalid("non-UTF8 log filename"))?;
        if name.starts_with("wal") {
            let id = name
                .strip_prefix("wal_")
                .ok_or(StorageError::Invalid("invalid WAL filename"))?;
            if id.len() != 16 {
                return Err(StorageError::Invalid("invalid WAL filename"));
            }
            let id = u64::from_str_radix(id, 16)
                .map_err(|_| StorageError::Invalid("invalid WAL filename"))?;
            let meta = entry.metadata()?;
            if !entry.file_type()?.is_file()
                || !meta.is_file()
                || meta.len() > config.wal_file_bytes
            {
                return Err(StorageError::Invalid("invalid WAL file"));
            }
            let base = id
                .checked_mul(config.wal_file_bytes)
                .ok_or(StorageError::Invalid("WAL file offset overflow"))?;
            let length = meta
                .len()
                .max(config.fragment_bytes)
                .div_ceil(config.fragment_bytes)
                * config.fragment_bytes;
            if base.checked_add(length).is_none_or(|end| end > max_end)
                || ids.len() >= max_fragments
            {
                return Err(StorageError::Invalid(
                    "WAL inventory exceeds admitted storage bound",
                ));
            }
            for i in 0..length / config.fragment_bytes {
                if fragments.len() >= max_fragments {
                    return Err(StorageError::Invalid(
                        "WAL inventory exceeds recovery bound",
                    ));
                }
                fragments.push(
                    base.checked_add(i * config.fragment_bytes)
                        .ok_or(StorageError::Invalid("WAL fragment offset overflow"))?,
                );
            }
            ids.push(id);
        }
    }
    ids.sort_unstable();
    if ids.is_empty() || ids.iter().enumerate().any(|(i, id)| *id != i as u64) {
        return Err(StorageError::Invalid(
            "missing registered WAL file; cannot prove absence",
        ));
    }
    fragments.sort_unstable();
    Ok(fragments)
}

#[derive(Debug)]
pub enum StorageError {
    Io(io::Error),
    Wal(WalError),
    Codec(CodecError),
    Invalid(&'static str),
    WrongDatabase,
    WrongPartition,
    Fenced,
    Sealed,
    IdentityConflict,
    VersionConflict,
    Capacity,
    NeedsPersistence,
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
