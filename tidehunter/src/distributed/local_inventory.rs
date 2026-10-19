//! Durable local authority inventory and monotonically fenced epoch lifecycle.
//!
//! Tidehunter paper §§3.1/3.3 requires complete batch replay before serving;
//! CORFU's recovery principle requires fencing before interpreting missing
//! positions. Here fencing means exclusive LOCAL root and partition file locks,
//! strict worker joins and permanently sealing old streams. This is explicitly
//! not a distributed lease, consensus service, remote failover or a GC protocol.
//! All registered write-eligible streams must be present, including empty ones.

use super::codec::BatchLimits;
use super::group::GroupLimits;
use super::storage::{
    PartitionConfig, PartitionLog, StaticInventory, StorageError, encode_metadata,
    install_metadata, read_metadata, recover_inventory,
};
use crate::key_shape::KeyShape;
use crate::lock::DbLock;
use crate::metrics::Metrics;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

const MANIFEST: &str = "local-database-inventory-v1";
const MANIFEST_TEMPORARY: &str = ".local-database-inventory-v1.pending";
const FORMAT: u32 = 1;

/// Static placement, bounded resident authority windows and fenced epoch limits.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LocalInventoryConfig {
    pub database: [u8; 16],
    pub partitions: Vec<u32>,
    pub batch_limits: BatchLimits,
    pub group_limits: GroupLimits,
    pub fragment_bytes: u64,
    pub wal_file_bytes: u64,
    /// Per-log resident receipt/descriptor window, never a lifetime batch ceiling.
    pub max_batches_per_partition: usize,
    /// Includes cold-authority cache and metadata pinned through read completion.
    pub max_reference_metadata_bytes: usize,
    /// Includes interrupted preparation epochs. No epoch/identity history is
    /// evicted to make room: reaching this limit requires an explicit lifecycle
    /// extension, not silently recycling a directory or logical version.
    pub max_epochs: usize,
}

impl LocalInventoryConfig {
    fn validate(&self) -> Result<(), LocalInventoryError> {
        if self.max_epochs == 0 || self.max_epochs > 1024 {
            return Err(LocalInventoryError::Invalid("invalid retained epoch bound"));
        }
        let inventory = self.inventory(1);
        inventory.validate()?;
        self.partition_config(1, self.partitions[0]).validate()?;
        Ok(())
    }

    fn inventory(&self, epoch: u64) -> StaticInventory {
        StaticInventory {
            database: self.database,
            epoch,
            segment_generation: epoch,
            partitions: self.partitions.clone(),
        }
    }

    fn partition_config(&self, epoch: u64, partition: u32) -> PartitionConfig {
        PartitionConfig {
            inventory: self.inventory(epoch),
            partition,
            batch_limits: self.batch_limits,
            group_limits: self.group_limits,
            fragment_bytes: self.fragment_bytes,
            wal_file_bytes: self.wal_file_bytes,
            max_batches: self.max_batches_per_partition,
            max_reference_metadata_bytes: self.max_reference_metadata_bytes,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
enum EpochState {
    /// Persisted before any partition directory is created. No user can receive
    /// a writer until the same epoch's Active manifest has been synced.
    Preparing,
    /// Every partition was durably registered and writers could have escaped.
    Active,
    /// An interrupted preparation that was never exposed to application writes.
    /// Its directories remain untouched; its epoch number is never reused.
    Abandoned,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct EpochRecord {
    epoch: u64,
    state: EpochState,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Manifest {
    format: u32,
    config: LocalInventoryConfig,
    shape_yaml: String,
    epochs: Vec<EpochRecord>,
}

/// An exclusively owned local database lifetime with fresh active WAL streams.
///
/// Open seals and recovers every older Active epoch before creating a fresh
/// epoch. The caller must rebuild its logical index from recovered batches before
/// serving and preserve original IDs/versions for old retries. This object does
/// not sort/replay mutations or infer a published database frontier.
///
/// Every returned partition retains the root lock. Cloning one into a worker or
/// retaining a historical reader therefore prevents root reopen even if this
/// owner is dropped. None of these handles is a remote fencing capability.
pub struct LocalInventory {
    path: PathBuf,
    manifest: Manifest,
    shape: KeyShape,
    active_inventory: StaticInventory,
    active: Vec<Arc<PartitionLog>>,
    recovered: Vec<Arc<PartitionLog>>,
    // Declared last: keep ownership until all directly held partitions close.
    lock: Arc<DbLock>,
}

impl LocalInventory {
    pub fn create(
        path: &Path,
        config: LocalInventoryConfig,
        shape: &KeyShape,
        metrics: Arc<Metrics>,
    ) -> Result<Self, LocalInventoryError> {
        config.validate()?;
        let shape_yaml = canonical_shape(shape)?;
        fs::create_dir(path)?;
        sync_parent(path)?;
        Self::open_inner(path, config, shape_yaml, metrics, true, |_| Ok(false))
    }

    pub fn open(
        path: &Path,
        expected: &LocalInventoryConfig,
        shape: &KeyShape,
        metrics: Arc<Metrics>,
    ) -> Result<Self, LocalInventoryError> {
        expected.validate()?;
        Self::open_inner(
            path,
            expected.clone(),
            canonical_shape(shape)?,
            metrics,
            false,
            |_| Ok(false),
        )
    }

    /// Initialize only an empty root, or reopen its exact persisted configuration.
    /// A missing manifest beside any data/epoch directory is an error, never an
    /// invitation to infer an empty database. An interrupted temporary manifest
    /// alone is safe because no writers could have been exposed before activation.
    pub fn open_or_create(
        path: &Path,
        config: LocalInventoryConfig,
        shape: &KeyShape,
        metrics: Arc<Metrics>,
    ) -> Result<Self, LocalInventoryError> {
        Self::open_or_create_checkpoint(path, config, shape, metrics, |_| Ok(false))
    }

    /// Only LocalDatabase supplies this validator: it decodes/verifies the
    /// installed index root under our exclusive authority lock. Prefix proofs
    /// are then checked against every recovered stream before serving/replay.
    pub(crate) fn open_or_create_checkpoint(
        path: &Path,
        config: LocalInventoryConfig,
        shape: &KeyShape,
        metrics: Arc<Metrics>,
        validate_checkpoint: impl FnOnce(&Path) -> Result<bool, LocalInventoryError>,
    ) -> Result<Self, LocalInventoryError> {
        config.validate()?;
        let shape_yaml = canonical_shape(shape)?;
        match fs::create_dir(path) {
            Ok(()) => sync_parent(path)?,
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
        Self::open_inner(path, config, shape_yaml, metrics, true, validate_checkpoint)
    }

    fn open_inner(
        path: &Path,
        expected: LocalInventoryConfig,
        shape_yaml: String,
        metrics: Arc<Metrics>,
        allow_empty: bool,
        validate_checkpoint: impl FnOnce(&Path) -> Result<bool, LocalInventoryError>,
    ) -> Result<Self, LocalInventoryError> {
        require_directory(path)?;
        let path = path.canonicalize()?;
        let lock = Arc::new(DbLock::acquire(&path, Duration::ZERO)?);
        let manifest_path = path.join(MANIFEST);
        let manifest = match fs::symlink_metadata(&manifest_path) {
            Ok(metadata) if metadata.file_type().is_file() => {
                let manifest: Manifest = read_metadata(&manifest_path)?;
                validate_manifest(&manifest, &expected, &shape_yaml)?;
                manifest
            }
            Ok(_) => {
                return Err(LocalInventoryError::Invalid(
                    "manifest is not a regular file",
                ));
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound && allow_empty => {
                require_empty_bootstrap(&path)?;
                let manifest = Manifest {
                    format: FORMAT,
                    config: expected,
                    shape_yaml,
                    epochs: Vec::new(),
                };
                persist_manifest(&path, &manifest)?;
                manifest
            }
            Err(error) => return Err(error.into()),
        };
        validate_epoch_directories(&path, &manifest)?;
        let shape = KeyShape::from_yaml(&manifest.shape_yaml)
            .map_err(|_| LocalInventoryError::Invalid("invalid persisted key shape"))?;
        let checkpoint_recovery = validate_checkpoint(&path)?;
        let mut recovered = Vec::new();
        // Acquire each partition's exclusive lock and permanently seal it. The
        // root lock excludes every compliant older authority, including its
        // surviving partition handles. A missing/busy stream blocks ALL recovery.
        for record in &manifest.epochs {
            if record.state != EpochState::Active {
                continue;
            }
            let inventory = manifest.config.inventory(record.epoch);
            let entries: Vec<_> = inventory
                .partitions
                .iter()
                .map(|&partition| {
                    (
                        partition_path(&path, record.epoch, partition),
                        manifest.config.partition_config(record.epoch, partition),
                    )
                })
                .collect();
            for (partition, _) in &entries {
                require_directory(partition)?;
            }
            let logs = if checkpoint_recovery {
                entries
                    .iter()
                    .map(|(path, config)| {
                        PartitionLog::recover_sealed(path, config, metrics.clone())
                    })
                    .collect::<Result<Vec<_>, _>>()?
            } else {
                recover_inventory(&inventory, &entries, metrics.clone())?
            };
            // Individual registrations must agree with the durable root schema,
            // not merely agree with one another inside this epoch.
            for log in logs {
                if canonical_shape(log.key_shape())? != manifest.shape_yaml {
                    return Err(LocalInventoryError::Invalid(
                        "partition differs from root schema",
                    ));
                }
                recovered.push(Arc::new(log.retain_local_authority(lock.clone())));
            }
        }
        let mut owner = Self {
            active_inventory: manifest.config.inventory(0),
            path,
            manifest,
            shape,
            active: Vec::new(),
            recovered,
            lock,
        };
        // Preparing means no Active manifest was made durable before the prior
        // process lost authority. Preserve its files but never reuse its number.
        let mut abandoned = false;
        for record in &mut owner.manifest.epochs {
            if record.state == EpochState::Preparing {
                record.state = EpochState::Abandoned;
                abandoned = true;
            }
        }
        if abandoned {
            persist_manifest(&owner.path, &owner.manifest)?;
        }
        owner.start_epoch(metrics)?;
        Ok(owner)
    }

    fn start_epoch(&mut self, metrics: Arc<Metrics>) -> Result<(), LocalInventoryError> {
        if self.manifest.epochs.len() == self.manifest.config.max_epochs {
            return Err(LocalInventoryError::EpochCapacity);
        }
        let epoch = self.manifest.epochs.last().map_or(Ok(1), |record| {
            record
                .epoch
                .checked_add(1)
                .ok_or(LocalInventoryError::EpochCapacity)
        })?;
        self.manifest.epochs.push(EpochRecord {
            epoch,
            state: EpochState::Preparing,
        });
        // Reservation must survive before any epoch directory can exist.
        persist_manifest(&self.path, &self.manifest)?;
        let epoch_path = epoch_path(&self.path, epoch);
        fs::create_dir(&epoch_path)?;
        File::open(&self.path)?.sync_all()?;
        let mut active = Vec::with_capacity(self.manifest.config.partitions.len());
        for &partition in &self.manifest.config.partitions {
            let log = PartitionLog::create(
                &partition_path(&self.path, epoch, partition),
                self.manifest.config.partition_config(epoch, partition),
                &self.shape,
                metrics.clone(),
            )?;
            active.push(Arc::new(log.retain_local_authority(self.lock.clone())));
        }
        self.manifest
            .epochs
            .last_mut()
            .expect("reserved epoch")
            .state = EpochState::Active;
        // No partition handle escapes until this rename AND directory barrier
        // succeed. If the outcome is uncertain, reopen resolves the disk state.
        persist_manifest(&self.path, &self.manifest)?;
        self.active_inventory = self.manifest.config.inventory(epoch);
        self.active = active;
        Ok(())
    }

    pub fn active_inventory(&self) -> &StaticInventory {
        &self.active_inventory
    }
    pub fn active_partitions(&self) -> &[Arc<PartitionLog>] {
        &self.active
    }
    pub fn recovered_partitions(&self) -> &[Arc<PartitionLog>] {
        &self.recovered
    }
    pub fn config(&self) -> &LocalInventoryConfig {
        &self.manifest.config
    }
    pub fn shape(&self) -> &KeyShape {
        &self.shape
    }
    pub fn path(&self) -> &Path {
        &self.path
    }
}

fn canonical_shape(shape: &KeyShape) -> Result<String, LocalInventoryError> {
    let yaml = shape
        .to_yaml()
        .map_err(|_| LocalInventoryError::Invalid("invalid key shape"))?;
    KeyShape::from_yaml(&yaml).map_err(|_| LocalInventoryError::Invalid("invalid key shape"))?;
    Ok(yaml)
}

fn persist_manifest(path: &Path, manifest: &Manifest) -> Result<(), LocalInventoryError> {
    install_metadata(path, MANIFEST, &encode_metadata(manifest)?)?;
    Ok(())
}

fn validate_manifest(
    manifest: &Manifest,
    expected: &LocalInventoryConfig,
    shape_yaml: &str,
) -> Result<(), LocalInventoryError> {
    if manifest.format != FORMAT
        || manifest.config != *expected
        || manifest.shape_yaml != shape_yaml
        || manifest.epochs.len() > manifest.config.max_epochs
    {
        return Err(LocalInventoryError::Invalid(
            "database configuration/schema/format differs",
        ));
    }
    for (index, epoch) in manifest.epochs.iter().enumerate() {
        if epoch.epoch != index as u64 + 1
            || (epoch.state == EpochState::Preparing && index + 1 != manifest.epochs.len())
        {
            return Err(LocalInventoryError::Invalid("invalid epoch history"));
        }
    }
    Ok(())
}

fn require_empty_bootstrap(path: &Path) -> Result<(), LocalInventoryError> {
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        if !entry.file_type()?.is_file()
            || (entry.file_name() != "LOCK" && entry.file_name() != MANIFEST_TEMPORARY)
        {
            return Err(LocalInventoryError::Invalid(
                "missing inventory beside existing data",
            ));
        }
    }
    Ok(())
}

fn validate_epoch_directories(path: &Path, manifest: &Manifest) -> Result<(), LocalInventoryError> {
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if let Some(number) = name.strip_prefix("epoch_") {
            let epoch = u64::from_str_radix(number, 16)
                .map_err(|_| LocalInventoryError::Invalid("unknown epoch directory"))?;
            if number.len() != 16
                || name != format!("epoch_{epoch:016x}")
                || !entry.file_type()?.is_dir()
                || !manifest.epochs.iter().any(|record| record.epoch == epoch)
            {
                return Err(LocalInventoryError::Invalid("unregistered epoch directory"));
            }
            // An additional stream cannot be ignored merely because all known
            // streams are present. The static root manifest is the complete
            // registered inventory; unexpected paths require operator recovery.
            for partition in fs::read_dir(entry.path())? {
                let partition = partition?;
                let expected = manifest
                    .config
                    .partitions
                    .iter()
                    .any(|id| partition.file_name() == format!("partition_{id:08x}").as_str());
                if !expected || !partition.file_type()?.is_dir() {
                    return Err(LocalInventoryError::Invalid(
                        "unregistered partition directory",
                    ));
                }
            }
        }
    }
    Ok(())
}

fn require_directory(path: &Path) -> Result<(), LocalInventoryError> {
    if !fs::symlink_metadata(path)?.file_type().is_dir() {
        return Err(LocalInventoryError::Invalid(
            "inventory path is not a directory",
        ));
    }
    Ok(())
}

fn sync_parent(path: &Path) -> io::Result<()> {
    File::open(
        path.parent()
            .ok_or_else(|| io::Error::other("missing database parent"))?,
    )?
    .sync_all()
}

fn epoch_path(path: &Path, epoch: u64) -> PathBuf {
    path.join(format!("epoch_{epoch:016x}"))
}
fn partition_path(path: &Path, epoch: u64, partition: u32) -> PathBuf {
    epoch_path(path, epoch).join(format!("partition_{partition:08x}"))
}

#[derive(Debug)]
pub enum LocalInventoryError {
    Io(io::Error),
    Storage(StorageError),
    Invalid(&'static str),
    EpochCapacity,
}

impl From<io::Error> for LocalInventoryError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}
impl From<StorageError> for LocalInventoryError {
    fn from(error: StorageError) -> Self {
        Self::Storage(error)
    }
}
impl fmt::Display for LocalInventoryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "local database inventory I/O: {error}"),
            Self::Storage(error) => write!(f, "local database inventory storage: {error}"),
            Self::Invalid(reason) => write!(f, "invalid local database inventory: {reason}"),
            Self::EpochCapacity => f.write_str("local database retained epoch capacity reached"),
        }
    }
}
impl std::error::Error for LocalInventoryError {}

#[cfg(test)]
mod tests;
