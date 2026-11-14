//! One node's durable epoch fence. Mirrors LocalInventory's preparing-before-
//! files and active-after-registration ordering, with one physical partition.
//! This is a static RF1 deployment: local filesystem locks exclude node restarts,
//! while the service rejects stale epochs. It does not elect a coordinator.

use super::*;
use crate::distributed::storage::{PartitionLog, encode_metadata, install_metadata, read_metadata};
use crate::lock::DbLock;
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

const MANIFEST: &str = "tcp-node-inventory-v1";
const LIVE_TARGET_CONFIG: &str = "tcp-node-live-target-v1";
// Bound idle history namespaces, subsystem objects and pinned target FDs
// independently of WAL history size. In-flight pages keep their separate pins.
#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
pub(super) const MAX_IDLE_HISTORY_EXPORTS: usize = 16;
#[derive(Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
enum EpochState {
    Preparing,
    Active,
    Sealed,
    Abandoned,
}
#[derive(Clone, Copy, Serialize, Deserialize)]
struct Epoch {
    epoch: u64,
    state: EpochState,
}
#[derive(Serialize, Deserialize)]
struct Manifest {
    format: u32,
    partition: PartitionConfig,
    shape_yaml: String,
    max_epochs: usize,
    epochs: Vec<Epoch>,
}
#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
#[derive(Serialize, Deserialize)]
struct LiveTargetMarker {
    config: super::super::nvmeof::LiveTargetConfig,
    identity: super::super::nvmeof::live_target::LiveTargetIdentity,
}

pub struct NodeInventory {
    pub(super) config: NodeConfig,
    pub(super) shape: KeyShape,
    manifest: Manifest,
    logs: BTreeMap<u64, Arc<PartitionLog>>,
    pub(super) healthy: bool,
    pub(super) terminal: Arc<AtomicBool>,
    pub(super) readers: Arc<RwLock<ReaderAuthority>>,
    metrics: Arc<Metrics>,
    history_diagnostics: bool,
    history_persistence: crate::distributed::storage::HistoryPersistence,
    history_failure_notifier: Option<Arc<dyn Fn() + Send + Sync>>,
    // Partition handles each retain this root lock as well.
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    pub(super) nvmeof_leases: Arc<Mutex<super::super::nvmeof::lease::ExportLeaseRegistry>>,
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    pub(super) live_target: Option<super::super::nvmeof::live_target::LiveTargetManager>,
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    live_shutdown: bool,
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    live_bindings: BTreeMap<
        (u64, u64),
        (
            super::super::nvmeof::live_storage::LiveWalSegment,
            super::super::nvmeof::live_target::LiveTargetBinding,
            bool,
        ),
    >,
    // Least recently released first; only unpinned readonly bindings enter.
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    idle_history_exports: std::collections::VecDeque<(u64, u64)>,
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    pub(super) live_pins: BTreeMap<
        [u8; 16],
        (
            u64,
            Vec<super::super::nvmeof::live_storage::LiveRecordLease>,
        ),
    >,
    lock: Arc<DbLock>,
}

/// Offline access to one explicitly sealed owner epoch, retaining the owner lock.
#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
pub struct SealedOwnerExport {
    log: Arc<PartitionLog>,
    _lock: DbLock,
}

#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
impl SealedOwnerExport {
    pub fn open(config: &NodeConfig, epoch: u64, metrics: Arc<Metrics>) -> Result<Self, TcpError> {
        config.partition.validate().map_err(TcpError::component)?;
        if epoch == 0
            || config.partition.inventory.epoch != 1
            || config.partition.inventory.segment_generation != 1
            || !(1..=1024).contains(&config.max_epochs)
        {
            return Err(TcpError::new("invalid owner export configuration"));
        }
        let root = config.root.canonicalize()?;
        let lock = DbLock::acquire(&root, Duration::ZERO)?;
        let path = root.join(MANIFEST);
        if !fs::symlink_metadata(&path)?.file_type().is_file() {
            return Err(TcpError::new("node manifest is not a regular file"));
        }
        let manifest: Manifest = read_metadata(&path).map_err(TcpError::component)?;
        let shape = KeyShape::from_yaml(&config.shape_yaml).map_err(TcpError::component)?;
        if manifest.format != 1
            || manifest.partition != config.partition
            || manifest.shape_yaml != shape.to_yaml().map_err(TcpError::component)?
            || manifest.max_epochs != config.max_epochs
            || manifest.epochs.len() > config.max_epochs
            || manifest.epochs.iter().any(|e| e.epoch == 0)
            || manifest.epochs.windows(2).any(|w| w[0].epoch >= w[1].epoch)
            || !manifest
                .epochs
                .iter()
                .any(|e| e.epoch == epoch && e.state == EpochState::Sealed)
        {
            return Err(TcpError::new(
                "owner inventory does not contain the requested sealed epoch",
            ));
        }
        let leases = super::super::nvmeof::lease::ExportLeaseRegistry::open(
            &root,
            1024,
            super::super::nvmeof::MAX_EXPORT_MANIFEST_BYTES,
        )
        .map_err(TcpError::component)?;
        if leases.has_live_or_quarantined() {
            return Err(TcpError::new(
                "owner export blocked by live or quarantined NVMe-oF leases",
            ));
        }
        let path = epoch_path(&root, epoch);
        directory(&path)?;
        let log =
            PartitionLog::recover_sealed(&path, &partition_at(&config.partition, epoch), metrics)
                .map_err(TcpError::component)?;
        if log.key_shape().to_yaml().map_err(TcpError::component)? != manifest.shape_yaml {
            return Err(TcpError::new(
                "owner export key shape differs from inventory",
            ));
        }
        Ok(Self {
            log: Arc::new(log),
            _lock: lock,
        })
    }

    pub fn log(&self) -> &PartitionLog {
        &self.log
    }

    /// Export through the retained owner lock without exposing a clonable log handle.
    pub fn export_sealed_readset(
        &self,
        directory: &Path,
        references: &[ValueRecordReference],
        config: &super::super::nvmeof::ExportConfig,
    ) -> Result<super::super::nvmeof::ExportManifest, super::super::storage::StorageError> {
        self.log
            .export_sealed_readset(directory, references, config)
    }
}

#[derive(Default)]
pub(super) struct ReaderAuthority {
    generation: u64,
    active_epoch: Option<u64>,
    logs: BTreeMap<u64, Arc<PartitionLog>>,
    database: [u8; 16],
    partition: u32,
}

pub(super) struct ReaderSnapshot {
    pub(super) generation: u64,
    pub(super) members: Vec<(Arc<PartitionLog>, ValueRecordReference)>,
}

impl ReaderAuthority {
    pub(super) fn invalidate(&mut self) {
        self.generation = self.generation.wrapping_add(1);
        self.active_epoch = None;
    }

    fn publish(&mut self, epoch: u64, logs: &BTreeMap<u64, Arc<PartitionLog>>) {
        self.generation = self.generation.wrapping_add(1);
        self.logs.clone_from(logs);
        self.active_epoch = Some(epoch);
    }
    pub(super) fn snapshot(
        &self,
        epoch: u64,
        references: &[ValueRecordReference],
        terminal: &AtomicBool,
    ) -> Result<ReaderSnapshot, TcpError> {
        if terminal.load(Ordering::Acquire) {
            return Err(TcpError::new("node requires restart and sealed recovery"));
        }
        if self.active_epoch != Some(epoch) {
            return Err(TcpError::new("stale or sealed request authority"));
        }
        let mut members = Vec::with_capacity(references.len());
        for reference in references {
            if reference.batch_id.database != self.database
                || reference.batch_address.partition != self.partition
                || reference.batch_address.segment_generation != reference.batch_version.epoch
            {
                return Err(TcpError::new(
                    "value reference belongs to another node/domain",
                ));
            }
            let log = self
                .logs
                .get(&reference.batch_version.epoch)
                .cloned()
                .ok_or_else(|| TcpError::new("value reference names absent epoch"))?;
            members.push((log, *reference));
        }
        Ok(ReaderSnapshot {
            generation: self.generation,
            members,
        })
    }
    pub(super) fn snapshot_valid(
        &self,
        epoch: u64,
        generation: u64,
        terminal: &AtomicBool,
    ) -> bool {
        !terminal.load(Ordering::Acquire)
            && self.generation == generation
            && self.active_epoch == Some(epoch)
            && !terminal.load(Ordering::Acquire)
    }
    pub(super) fn active_valid(&self, epoch: u64, terminal: &AtomicBool) -> bool {
        !terminal.load(Ordering::Acquire) && self.active_epoch == Some(epoch)
    }
}
impl NodeInventory {
    #[cfg(all(target_os = "linux", feature = "test-utils"))]
    pub(super) fn process_resources_for_benchmark(
        &self,
        epoch: u64,
    ) -> Result<crate::read_probe::ProcessResources, TcpError> {
        self.active(epoch)?;
        crate::read_probe::ProcessResources::process().map_err(TcpError::component)
    }

    #[cfg(all(target_os = "linux", feature = "test-utils"))]
    pub(super) fn read_evidence_for_benchmark(
        &self,
        epoch: u64,
        evict: bool,
    ) -> Result<crate::read_probe::ReadEvidence, TcpError> {
        self.active(epoch)?;
        let mut evidence =
            crate::read_probe::ReadEvidence::process().map_err(TcpError::component)?;
        for log in self.logs.values() {
            let current = log
                .read_evidence_for_benchmark(evict)
                .map_err(TcpError::component)?;
            evidence.file_bytes += current.file_bytes;
            evidence.resident_bytes += current.resident_bytes;
        }
        Ok(evidence)
    }
    pub fn open(mut config: NodeConfig, metrics: Arc<Metrics>) -> Result<Self, TcpError> {
        let shape = config.validated_shape()?;
        config.shape_yaml = shape.to_yaml().map_err(TcpError::component)?;
        match fs::create_dir(&config.root) {
            Ok(()) => {
                File::open(
                    config
                        .root
                        .parent()
                        .filter(|p| !p.as_os_str().is_empty())
                        .unwrap_or(Path::new(".")),
                )?
                .sync_all()?;
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
        directory(&config.root)?;
        config.root = config.root.canonicalize()?;
        let lock = Arc::new(DbLock::acquire(&config.root, Duration::ZERO)?);
        // A build without the NVMe-oF revoker must never silently open an
        // owner whose prior kernel namespace capabilities may still exist.
        #[cfg(not(all(feature = "experimental-nvmeof", target_os = "linux")))]
        match fs::symlink_metadata(config.root.join("nvmeof-export-leases-v1")) {
            Ok(_) => {
                return Err(TcpError::new(
                    "NVMe-oF lease registry requires recovery with the Linux NVMe-oF feature",
                ));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        #[cfg(not(all(feature = "experimental-nvmeof", target_os = "linux")))]
        match fs::symlink_metadata(config.root.join(LIVE_TARGET_CONFIG)) {
            Ok(_) => {
                return Err(TcpError::new(
                    "live NVMe-oF target requires recovery with the Linux NVMe-oF feature",
                ));
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        let manifest_path = config.root.join(MANIFEST);
        let mut manifest = match fs::symlink_metadata(&manifest_path) {
            Ok(metadata) if metadata.file_type().is_file() => {
                read_metadata::<Manifest>(&manifest_path).map_err(TcpError::component)?
            }
            Ok(_) => return Err(TcpError::new("node manifest is not a regular file")),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                for entry in fs::read_dir(&config.root)? {
                    let entry = entry?;
                    if !entry.file_type()?.is_file()
                        || (entry.file_name() != "LOCK"
                            && entry.file_name() != format!(".{MANIFEST}.pending").as_str())
                    {
                        return Err(TcpError::new("missing node inventory beside existing data"));
                    }
                }
                let manifest = Manifest {
                    format: 1,
                    partition: config.partition.clone(),
                    shape_yaml: config.shape_yaml.clone(),
                    max_epochs: config.max_epochs,
                    epochs: Vec::new(),
                };
                persist(&config.root, &manifest)?;
                manifest
            }
            Err(error) => return Err(error.into()),
        };
        if manifest.format != 1
            || manifest.partition != config.partition
            || manifest.shape_yaml != config.shape_yaml
            || manifest.max_epochs != config.max_epochs
            || manifest.epochs.len() > config.max_epochs
            || manifest.epochs.iter().any(|e| e.epoch == 0)
            || manifest.epochs.windows(2).any(|w| w[0].epoch >= w[1].epoch)
            || manifest.epochs.iter().enumerate().any(|(index, e)| {
                matches!(e.state, EpochState::Active | EpochState::Preparing)
                    && index + 1 != manifest.epochs.len()
            })
        {
            return Err(TcpError::new("node inventory/configuration differs"));
        }
        // Hold the node lock before revoking capabilities: another process
        // must never revoke a currently running owner's namespaces.
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        let live_target = {
            let marker = config.root.join(LIVE_TARGET_CONFIG);
            use super::super::nvmeof::live_target::LiveTargetManager;
            match fs::symlink_metadata(&marker) {
                Ok(metadata) if metadata.file_type().is_file() => {
                    let saved: LiveTargetMarker =
                        read_metadata(&marker).map_err(TcpError::component)?;
                    if config.live_target.as_ref() != Some(&saved.config) {
                        return Err(TcpError::new(
                            "live target configuration required and must match persisted configuration",
                        ));
                    }
                    Some(
                        LiveTargetManager::open_existing(saved.config, saved.identity)
                            .map_err(TcpError::component)?,
                    )
                }
                Ok(_) => {
                    return Err(TcpError::new(
                        "live target configuration is not a regular file",
                    ));
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    for epoch in &manifest.epochs {
                        if matches!(epoch.state, EpochState::Active | EpochState::Sealed)
                            && PartitionLog::is_live_path(&epoch_path(&config.root, epoch.epoch))
                                .map_err(TcpError::component)?
                        {
                            return Err(TcpError::new(
                                "live WAL target identity marker is missing",
                            ));
                        }
                    }
                    if let Some(target_config) = config.live_target.clone() {
                        let manager = LiveTargetManager::open(target_config.clone())
                            .map_err(TcpError::component)?;
                        let saved = LiveTargetMarker {
                            config: target_config,
                            identity: manager.identity().map_err(TcpError::component)?,
                        };
                        install_metadata(
                            &config.root,
                            LIVE_TARGET_CONFIG,
                            &encode_metadata(&saved).map_err(TcpError::component)?,
                        )
                        .map_err(TcpError::component)?;
                        Some(manager)
                    } else {
                        None
                    }
                }
                Err(error) => return Err(error.into()),
            }
        };
        // Registry recovery occurs under the node lock, before any old epoch is
        // recovered or sealed. Orphaned admissions stay quarantined.
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        let nvmeof_leases = Arc::new(Mutex::new(
            super::super::nvmeof::lease::ExportLeaseRegistry::open(
                &config.root,
                1024,
                super::super::nvmeof::MAX_EXPORT_MANIFEST_BYTES,
            )
            .map_err(TcpError::component)?,
        ));
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        if nvmeof_leases.lock().has_live_or_quarantined() {
            return Err(TcpError::new(
                "unreleased NVMe-oF leases are quarantined; owner recovery requires proven initiator fencing",
            ));
        }
        for entry in fs::read_dir(&config.root)? {
            let entry = entry?;
            let name = entry.file_name();
            if let Some(name) = name.to_str().filter(|name| name.starts_with("epoch_")) {
                let matching = manifest
                    .epochs
                    .iter()
                    .any(|e| name == format!("epoch_{:016x}", e.epoch));
                if !matching || !entry.file_type()?.is_dir() {
                    return Err(TcpError::new("unregistered node epoch directory"));
                }
            }
        }
        let mut logs = BTreeMap::new();
        for epoch in &mut manifest.epochs {
            match epoch.state {
                EpochState::Preparing => epoch.state = EpochState::Abandoned,
                EpochState::Active | EpochState::Sealed => {
                    let path = epoch_path(&config.root, epoch.epoch);
                    directory(&path)?;
                    let expected = partition_at(&config.partition, epoch.epoch);
                    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
                    let log = if PartitionLog::is_live_path(&path).map_err(TcpError::component)? {
                        if live_target.is_none() {
                            return Err(TcpError::new(
                                "live WAL recovery requires its configured target manager",
                            ));
                        }
                        // Manager::open already synchronously revoked and drained
                        // the persisted targets while the root lock was held.
                        PartitionLog::recover_live_sealed(&path, &expected, metrics.clone(), || {
                            Ok(())
                        })
                    } else {
                        PartitionLog::recover_sealed(&path, &expected, metrics.clone())
                    };
                    #[cfg(not(all(feature = "experimental-nvmeof", target_os = "linux")))]
                    let log = PartitionLog::recover_sealed(&path, &expected, metrics.clone());
                    let log = log.map_err(TcpError::component)?;
                    if log.key_shape().to_yaml().map_err(TcpError::component)? != config.shape_yaml
                    {
                        return Err(TcpError::new("node stream schema differs"));
                    }
                    logs.insert(
                        epoch.epoch,
                        Arc::new(log.retain_local_authority(lock.clone())),
                    );
                    epoch.state = EpochState::Sealed;
                }
                EpochState::Abandoned => {}
            }
        }
        let history_persistence = logs.values().next().map_or_else(
            crate::distributed::storage::HistoryPersistence::default,
            |log| log.history_persistence(),
        );
        if logs
            .values()
            .any(|log| log.history_persistence() != history_persistence)
        {
            return Err(TcpError::new(
                "owner epochs have different history persistence",
            ));
        }
        // Restart always seals the old authority, even if it previously shut
        // down cleanly. It never resumes an old logical counter/writer epoch.
        persist(&config.root, &manifest)?;
        let readers = Arc::new(RwLock::new(ReaderAuthority {
            database: config.partition.inventory.database,
            partition: config.partition.partition,
            ..ReaderAuthority::default()
        }));
        Ok(Self {
            config,
            shape,
            manifest,
            logs,
            healthy: true,
            terminal: Arc::new(AtomicBool::new(false)),
            readers,
            metrics,
            history_diagnostics: false,
            history_persistence,
            history_failure_notifier: None,
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            nvmeof_leases,
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            live_target,
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            live_shutdown: false,
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            live_bindings: BTreeMap::new(),
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            idle_history_exports: std::collections::VecDeque::new(),
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            live_pins: BTreeMap::new(),
            lock,
        })
    }
    pub(super) fn set_history_maintenance_diagnostics(&mut self, enabled: bool) {
        self.history_diagnostics = enabled;
        for log in self.logs.values() {
            log.set_history_maintenance_diagnostics(enabled);
        }
    }

    pub(super) fn set_history_persistence(
        &mut self,
        mode: crate::distributed::storage::HistoryPersistence,
    ) -> Result<(), TcpError> {
        for log in self.logs.values() {
            log.set_history_persistence(mode)
                .map_err(TcpError::component)?;
        }
        self.history_persistence = mode;
        Ok(())
    }

    pub(super) fn history_maintenance_stats(
        &self,
    ) -> Vec<(u64, crate::distributed::storage::HistoryMaintenanceStats)> {
        self.logs
            .iter()
            .map(|(&epoch, log)| (epoch, log.history_maintenance_stats()))
            .collect()
    }

    pub(super) fn set_history_failure_notifier(&mut self, notifier: Arc<dyn Fn() + Send + Sync>) {
        for log in self.logs.values() {
            log.set_history_failure_notifier(notifier.clone());
        }
        self.history_failure_notifier = Some(notifier);
    }

    fn ensure_healthy(&self) -> Result<(), TcpError> {
        if self.healthy && !self.terminal.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(TcpError::new(
                "node requires restart and sealed recovery; outcome may be unknown",
            ))
        }
    }
    pub fn status(&self) -> Result<NodeStatus, TcpError> {
        self.ensure_healthy()?;
        self.status_snapshot(true)
    }
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    pub(super) fn recoverable_status(&self) -> Result<NodeStatus, TcpError> {
        if !self.healthy || self.live_target.is_none() || self.live_shutdown {
            return Err(TcpError::new("live owner requires restart"));
        }
        self.status_snapshot(false)
    }
    fn status_snapshot(&self, healthy: bool) -> Result<NodeStatus, TcpError> {
        Ok(NodeStatus {
            partition: self.config.partition.clone(),
            history_persistence: self.history_persistence,
            shape_yaml: self.config.shape_yaml.clone(),
            max_epochs: self.config.max_epochs,
            epochs: self
                .manifest
                .epochs
                .iter()
                .map(|e| NodeEpoch {
                    epoch: e.epoch,
                    sealed: e.state != EpochState::Active,
                })
                .collect(),
            active_epoch: self
                .manifest
                .epochs
                .last()
                .filter(|e| e.state == EpochState::Active)
                .map(|e| e.epoch),
            healthy,
            max_frame_bytes: HARD_FRAME_LIMIT,
            max_reads: 256,
            max_read_bytes: self.config.read_max_inflight_bytes,
            recovery_page_batches: 1024,
            polling: PollingPolicy::Adaptive,
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            live_nvmeof: self.live_target.is_some() && !self.live_shutdown,
            #[cfg(not(all(feature = "experimental-nvmeof", target_os = "linux")))]
            live_nvmeof: false,
        })
    }
    pub fn fence_to(&mut self, epoch: u64) -> Result<NodeStatus, TcpError> {
        self.fence_to_inner(epoch, false, |_| Ok(()))
    }
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    pub(super) fn fence_to_with_drain(
        &mut self,
        epoch: u64,
        drain_log: impl FnOnce(&Arc<PartitionLog>) -> Result<(), TcpError>,
    ) -> Result<NodeStatus, TcpError> {
        self.fence_to_inner(epoch, false, drain_log)
    }
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    pub(super) fn fence_recoverable_live(
        &mut self,
        epoch: u64,
        drain_log: impl FnOnce(&Arc<PartitionLog>) -> Result<(), TcpError>,
    ) -> Result<NodeStatus, TcpError> {
        if !self.healthy
            || self.live_target.is_none()
            || !self
                .manifest
                .epochs
                .last()
                .is_some_and(|entry| entry.state == EpochState::Active && epoch > entry.epoch)
        {
            return Err(TcpError::new(
                "live recovery requires a strictly higher authority epoch",
            ));
        }
        self.fence_to_inner(epoch, true, drain_log)
    }
    fn fence_to_inner(
        &mut self,
        epoch: u64,
        recovering: bool,
        _drain_log: impl FnOnce(&Arc<PartitionLog>) -> Result<(), TcpError>,
    ) -> Result<NodeStatus, TcpError> {
        if !recovering {
            self.ensure_healthy()?;
        }
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        if self.live_shutdown {
            return Err(TcpError::new("live target has shut down; reopen required"));
        }
        if self
            .manifest
            .epochs
            .last()
            .is_some_and(|e| e.epoch == epoch && e.state == EpochState::Active)
        {
            return self.status();
        }
        if epoch == 0
            || self
                .manifest
                .epochs
                .last()
                .is_some_and(|e| epoch <= e.epoch)
        {
            return Err(TcpError::new(
                "epoch already fenced/sealed; cannot reactivate",
            ));
        }
        if self.manifest.epochs.len() == self.config.max_epochs {
            return Err(TcpError::new("retained node epoch capacity reached"));
        }
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        if self.live_target.is_some() {
            // The new authority fences the prior coordinator before any old
            // allocation is scanned or its namespace-backed pins are dropped.
            self.healthy = false;
            {
                let mut readers = self.readers.write();
                readers.invalidate();
                readers.logs.clear();
            }
            self.revoke_live_targets()?;
            self.live_pins.clear();
        }
        if let Some(old) = self
            .manifest
            .epochs
            .last()
            .filter(|e| e.state == EpochState::Active)
            .copied()
        {
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            if self.live_target.is_some() {
                // Revocation and pin release above fence external writers/readers.
                // Join the control owner's syncer too: both its slot and callback
                // retain this log and its partition lock until the worker exits.
                _drain_log(
                    self.logs
                        .get(&old.epoch)
                        .ok_or_else(|| TcpError::new("active live stream missing"))?,
                )?;
                let previous = self
                    .logs
                    .remove(&old.epoch)
                    .ok_or_else(|| TcpError::new("active live stream missing"))?;
                if Arc::strong_count(&previous) != 1 {
                    self.logs.insert(old.epoch, previous);
                    return Err(TcpError::new(
                        "old live stream still has outstanding handles; reopen required",
                    ));
                }
                drop(previous);
                let path = epoch_path(&self.config.root, old.epoch);
                let expected = partition_at(&self.config.partition, old.epoch);
                // All target capabilities were synchronously drained above.
                // The core opens its own partition DbLock; no lock is bypassed.
                let log = PartitionLog::recover_live_sealed(
                    &path,
                    &expected,
                    self.metrics.clone(),
                    || Ok(()),
                )
                .map_err(TcpError::component)?;
                let log = Arc::new(log.retain_local_authority(self.lock.clone()));
                log.set_history_maintenance_diagnostics(self.history_diagnostics);
                log.set_history_persistence(self.history_persistence)
                    .map_err(TcpError::component)?;
                if let Some(notifier) = &self.history_failure_notifier {
                    log.set_history_failure_notifier(notifier.clone());
                }
                self.logs.insert(old.epoch, log);
                self.manifest.epochs.last_mut().unwrap().state = EpochState::Sealed;
                persist(&self.config.root, &self.manifest)?;
                self.healthy = true;
            } else {
                self.seal(old.epoch)?;
            }
            #[cfg(not(all(feature = "experimental-nvmeof", target_os = "linux")))]
            self.seal(old.epoch)?;
        }
        self.healthy = false;
        self.readers.write().invalidate();
        self.manifest.epochs.push(Epoch {
            epoch,
            state: EpochState::Preparing,
        });
        persist(&self.config.root, &self.manifest)?;
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        let log = if self.live_target.is_some() {
            PartitionLog::create_live(
                &epoch_path(&self.config.root, epoch),
                partition_at(&self.config.partition, epoch),
                &self.shape,
                self.metrics.clone(),
            )
        } else {
            PartitionLog::create(
                &epoch_path(&self.config.root, epoch),
                partition_at(&self.config.partition, epoch),
                &self.shape,
                self.metrics.clone(),
            )
        };
        #[cfg(not(all(feature = "experimental-nvmeof", target_os = "linux")))]
        let log = PartitionLog::create(
            &epoch_path(&self.config.root, epoch),
            partition_at(&self.config.partition, epoch),
            &self.shape,
            self.metrics.clone(),
        );
        let log = log.map_err(TcpError::component)?;
        let log = Arc::new(log.retain_local_authority(self.lock.clone()));
        log.set_history_maintenance_diagnostics(self.history_diagnostics);
        log.set_history_persistence(self.history_persistence)
            .map_err(TcpError::component)?;
        if let Some(notifier) = &self.history_failure_notifier {
            log.set_history_failure_notifier(notifier.clone());
        }
        self.manifest.epochs.last_mut().unwrap().state = EpochState::Active;
        persist(&self.config.root, &self.manifest)?;
        self.logs.insert(epoch, log);
        self.readers.write().publish(epoch, &self.logs);
        self.healthy = true;
        if recovering {
            self.terminal.store(false, Ordering::Release);
        }
        self.status()
    }
    pub(super) fn active(&self, epoch: u64) -> Result<Arc<PartitionLog>, TcpError> {
        self.ensure_healthy()?;
        if !self
            .manifest
            .epochs
            .last()
            .is_some_and(|e| e.epoch == epoch && e.state == EpochState::Active)
        {
            return Err(TcpError::new("stale or sealed request authority"));
        }
        self.logs
            .get(&epoch)
            .cloned()
            .ok_or_else(|| TcpError::new("active stream missing"))
    }
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    pub(super) fn live_segment_writable(
        &self,
        authority_epoch: u64,
        segment: &super::super::nvmeof::live_storage::LiveWalSegment,
    ) -> Result<bool, TcpError> {
        if segment.epoch != authority_epoch {
            return Ok(false);
        }
        let log = self.active(authority_epoch)?;
        Ok(!log
            .can_release_live_segment(segment.file_index)
            .map_err(TcpError::component)?)
    }

    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    pub(super) fn reap_idle_live_segments(&mut self, epoch: u64) -> Result<(), TcpError> {
        let log = self.active(epoch)?;
        let mut revoke = Vec::new();
        for (&key, (_, binding, writable)) in &self.live_bindings {
            if key.0 == epoch
                && *writable
                && !binding.permanent
                && !self.live_pins.values().any(|(_, leases)| {
                    leases
                        .iter()
                        .any(|lease| (lease.segment.epoch, lease.segment.file_index) == key)
                })
                && log
                    .can_release_live_segment(key.1)
                    .map_err(TcpError::component)?
            {
                revoke.push(key);
            }
        }
        for key in revoke {
            let (_, binding, _) = self
                .live_bindings
                .get(&key)
                .ok_or_else(|| TcpError::new("live binding disappeared"))?;
            self.live_target
                .as_mut()
                .ok_or_else(|| TcpError::new("live target disappeared"))?
                .revoke(binding)
                .map_err(TcpError::component)?;
            self.live_bindings.remove(&key);
        }
        Ok(())
    }

    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    pub(super) fn publish_live_segment(
        &mut self,
        segment: &super::super::nvmeof::live_storage::LiveWalSegment,
        writable: bool,
    ) -> Result<super::super::nvmeof::live_target::LiveTargetBinding, TcpError> {
        use super::super::nvmeof::live_target::LiveFileIdentity;
        let key = (segment.epoch, segment.file_index);
        if let Some((existing, binding, was_writable)) = self.live_bindings.get(&key) {
            if existing != segment || (writable && !was_writable) {
                return Err(TcpError::new("live segment identity or access changed"));
            }
            self.idle_history_exports.retain(|idle| *idle != key);
            return Ok(binding.clone());
        }
        let target = self
            .live_target
            .as_mut()
            .ok_or_else(|| TcpError::new("live target not configured"))?;
        let file = LiveFileIdentity {
            path: segment.path.clone(),
            device: segment.device,
            inode: segment.inode,
            bytes: segment.bytes,
            block_size: segment.block_size,
        };
        let binding = if writable {
            target.export_writable(file)
        } else {
            target.export_readonly(file)
        }
        .map_err(TcpError::component)?;
        self.live_bindings
            .insert(key, (segment.clone(), binding.clone(), writable));
        Ok(binding)
    }

    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    pub(super) fn publish_permanent_segment(
        &mut self,
        segment: &super::super::nvmeof::live_storage::LiveWalSegment,
    ) -> Result<(super::super::nvmeof::live_target::LiveTargetBinding, bool), TcpError> {
        use super::super::nvmeof::{live_storage::wal_export_name, live_target::LiveFileIdentity};
        let key = (segment.epoch, segment.file_index);
        if let Some((existing, binding, writable)) = self.live_bindings.get(&key) {
            if existing != segment || !writable {
                return Err(TcpError::new("live segment identity or access changed"));
            }
            return Ok((binding.clone(), false));
        }
        let target = self
            .live_target
            .as_mut()
            .ok_or_else(|| TcpError::new("live target not configured"))?;
        let files = target
            .export_mode()
            .files_per_subsystem()
            .ok_or_else(|| TcpError::new("permanent live export is not configured"))?;
        let name = wal_export_name(
            files,
            segment.database,
            segment.partition,
            segment.epoch,
            segment.file_index,
        );
        let binding = target
            .export_permanent(
                &name,
                LiveFileIdentity {
                    path: segment.path.clone(),
                    device: segment.device,
                    inode: segment.inode,
                    bytes: segment.bytes,
                    block_size: segment.block_size,
                },
            )
            .map_err(TcpError::component)?;
        self.live_bindings
            .insert(key, (segment.clone(), binding.clone(), true));
        Ok((binding, true))
    }

    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    fn revoke_live_targets(&mut self) -> Result<(), TcpError> {
        let Some(target) = self.live_target.as_mut() else {
            return Ok(());
        };
        for (_, binding, _) in self.live_bindings.values() {
            target.revoke(binding).map_err(TcpError::component)?;
        }
        self.live_bindings.clear();
        self.idle_history_exports.clear();
        Ok(())
    }
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    fn revoke_live_epoch(&mut self, epoch: u64) -> Result<(), TcpError> {
        let Some(target) = self.live_target.as_mut() else {
            return Ok(());
        };
        for ((binding_epoch, _), (_, binding, _)) in &self.live_bindings {
            if *binding_epoch == epoch {
                target.revoke(binding).map_err(TcpError::component)?;
            }
        }
        self.live_bindings
            .retain(|(binding_epoch, _), _| *binding_epoch != epoch);
        self.idle_history_exports
            .retain(|(binding_epoch, _)| *binding_epoch != epoch);
        Ok(())
    }

    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    pub(super) fn release_live_pin(&mut self, epoch: u64, token: [u8; 16]) -> Result<(), TcpError> {
        let Some((owner, leases)) = self.live_pins.get(&token) else {
            return Ok(());
        };
        if *owner != epoch {
            return Err(TcpError::new("live pin belongs to another authority epoch"));
        }
        let mut revoke = std::collections::BTreeSet::new();
        let mut retain = std::collections::BTreeSet::new();
        for lease in leases {
            let key = (lease.segment.epoch, lease.segment.file_index);
            let releasable = match self.live_bindings.get(&key) {
                Some((_, binding, _)) if binding.permanent => false,
                Some((_, _, false)) => self
                    .live_target
                    .as_ref()
                    .is_none_or(|target| target.export_mode().files_per_subsystem().is_none()),
                Some((_, _, true)) => self
                    .logs
                    .get(&key.0)
                    .ok_or_else(|| TcpError::new("live segment stream absent"))?
                    .can_release_live_segment(key.1)
                    .map_err(TcpError::component)?,
                None => false,
            };
            if !self.live_pins.iter().any(|(other, (_, leases))| {
                *other != token
                    && leases
                        .iter()
                        .any(|lease| (lease.segment.epoch, lease.segment.file_index) == key)
            }) {
                if releasable {
                    revoke.insert(key);
                } else if self
                    .live_bindings
                    .get(&key)
                    .is_some_and(|(_, binding, writable)| !binding.permanent && !writable)
                {
                    retain.insert(key);
                }
            }
        }
        for key in revoke {
            let (_, binding, _) = self
                .live_bindings
                .get(&key)
                .ok_or_else(|| TcpError::new("live binding disappeared"))?;
            let result = self
                .live_target
                .as_mut()
                .ok_or_else(|| TcpError::new("live target disappeared"))?
                .revoke(binding);
            if let Err(error) = result {
                self.healthy = false;
                return Err(TcpError::component(error));
            }
            self.live_bindings.remove(&key);
        }
        for key in retain {
            self.retain_idle_history_export(key)?;
        }
        self.live_pins.remove(&token);
        Ok(())
    }

    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    fn retain_idle_history_export(&mut self, key: (u64, u64)) -> Result<(), TcpError> {
        self.idle_history_exports.retain(|idle| *idle != key);
        if self.idle_history_exports.len() == MAX_IDLE_HISTORY_EXPORTS {
            let oldest = *self
                .idle_history_exports
                .front()
                .ok_or_else(|| TcpError::new("idle history queue disappeared"))?;
            let (_, binding, _) = self
                .live_bindings
                .get(&oldest)
                .ok_or_else(|| TcpError::new("idle history binding disappeared"))?;
            if let Err(error) = self
                .live_target
                .as_mut()
                .ok_or_else(|| TcpError::new("live target disappeared"))?
                .revoke(binding)
            {
                self.healthy = false;
                return Err(TcpError::component(error));
            }
            self.live_bindings.remove(&oldest);
            self.idle_history_exports.pop_front();
        }
        self.idle_history_exports.push_back(key);
        Ok(())
    }

    #[cfg(all(feature = "experimental-nvmeof", any(test, feature = "test-utils")))]
    pub(super) fn sealed_nvmeof_log(&self, epoch: u64) -> Result<Arc<PartitionLog>, TcpError> {
        self.ensure_healthy()?;
        if !self
            .manifest
            .epochs
            .iter()
            .any(|entry| entry.epoch == epoch && entry.state == EpochState::Sealed)
        {
            return Err(TcpError::new(
                "NVMe-oF export requires a sealed source epoch",
            ));
        }
        self.logs
            .get(&epoch)
            .cloned()
            .ok_or_else(|| TcpError::new("sealed export source is absent"))
    }
    pub fn seal(&mut self, epoch: u64) -> Result<(), TcpError> {
        self.seal_draining(epoch, |_| Ok(()))
    }

    pub(super) fn seal_draining(
        &mut self,
        epoch: u64,
        _drain_log: impl FnOnce(&Arc<PartitionLog>) -> Result<(), TcpError>,
    ) -> Result<(), TcpError> {
        self.ensure_healthy()?;
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        if !self.live_pins.is_empty() {
            return Err(TcpError::new("live NVMe-oF pins must drain before sealing"));
        }
        if self.manifest.epochs.iter().any(|e| {
            e.epoch == epoch && matches!(e.state, EpochState::Sealed | EpochState::Abandoned)
        }) {
            return Ok(());
        }
        let log = self.active(epoch)?;
        self.healthy = false;
        self.readers.write().invalidate();
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        if self.live_target.is_some() {
            self.revoke_live_targets()?;
            _drain_log(&log)?;
            log.seal_live_epoch(RequestAuthority { epoch }, || Ok(()))
                .map_err(TcpError::component)?;
        } else {
            log.seal_epoch(RequestAuthority { epoch })
                .map_err(TcpError::component)?;
        }
        #[cfg(not(all(feature = "experimental-nvmeof", target_os = "linux")))]
        log.seal_epoch(RequestAuthority { epoch })
            .map_err(TcpError::component)?;
        self.manifest.epochs.last_mut().unwrap().state = EpochState::Sealed;
        persist(&self.config.root, &self.manifest)?;
        self.healthy = true;
        Ok(())
    }
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    pub(super) fn shutdown_live_target(
        &mut self,
        epoch: u64,
    ) -> Result<super::super::nvmeof::LiveTargetStats, TcpError> {
        self.ensure_healthy()?;
        if !self.live_pins.is_empty() {
            return Err(TcpError::new(
                "live NVMe-oF pins must drain before shutdown",
            ));
        }
        if !self
            .manifest
            .epochs
            .last()
            .is_some_and(|entry| entry.epoch == epoch && entry.state == EpochState::Sealed)
        {
            return Err(TcpError::new(
                "live target shutdown requires latest sealed authority",
            ));
        }
        self.finish_target_shutdown()
    }

    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    pub(super) fn finish_live_target_shutdown(
        &mut self,
    ) -> Result<super::super::nvmeof::LiveTargetStats, TcpError> {
        self.ensure_healthy()?;
        if !self.live_pins.is_empty() {
            return Err(TcpError::new(
                "live NVMe-oF pins must drain before shutdown",
            ));
        }
        if let Some(epoch) = self
            .manifest
            .epochs
            .last()
            .filter(|entry| entry.state == EpochState::Active)
            .map(|entry| entry.epoch)
        {
            self.seal(epoch)?;
        }
        self.finish_target_shutdown()
    }

    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    fn finish_target_shutdown(
        &mut self,
    ) -> Result<super::super::nvmeof::LiveTargetStats, TcpError> {
        let target = self
            .live_target
            .as_mut()
            .ok_or_else(|| TcpError::new("live target not configured"))?;
        if !self.live_shutdown
            && let Err(error) = target.shutdown()
        {
            self.healthy = false;
            return Err(TcpError::component(error));
        }
        self.live_shutdown = true;
        let stats = target.stats().map_err(|error| {
            self.healthy = false;
            TcpError::component(error)
        })?;
        if stats.owned_namespace_count != 0 {
            self.healthy = false;
            return Err(TcpError::new("live target shutdown left owned namespaces"));
        }
        self.live_bindings.clear();
        self.idle_history_exports.clear();
        Ok(stats)
    }
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    pub(super) fn live_recovery_log(
        &self,
        authority_epoch: u64,
        epoch: u64,
    ) -> Result<Option<Arc<PartitionLog>>, TcpError> {
        self.active(authority_epoch)?;
        let record = self
            .manifest
            .epochs
            .iter()
            .find(|entry| entry.epoch == epoch)
            .ok_or_else(|| TcpError::new("recovery epoch absent"))?;
        match record.state {
            EpochState::Abandoned => Ok(None),
            EpochState::Sealed => self
                .logs
                .get(&epoch)
                .cloned()
                .map(Some)
                .ok_or_else(|| TcpError::new("recovery stream absent")),
            _ => Err(TcpError::new("recovery requires sealed immutable epoch")),
        }
    }
    fn retirement_log(
        &self,
        authority_epoch: u64,
        epoch: u64,
    ) -> Result<Arc<PartitionLog>, TcpError> {
        self.active(authority_epoch)?;
        if epoch >= authority_epoch
            || !self
                .manifest
                .epochs
                .iter()
                .any(|entry| entry.epoch == epoch && entry.state == EpochState::Sealed)
        {
            return Err(TcpError::new("retirement requires an older sealed epoch"));
        }
        self.logs
            .get(&epoch)
            .cloned()
            .ok_or_else(|| TcpError::new("retirement stream absent"))
    }

    pub(super) fn retirement_files(
        &self,
        authority_epoch: u64,
        epoch: u64,
        after: Option<u64>,
        limit: usize,
    ) -> Result<Vec<u64>, TcpError> {
        if !(1..=256).contains(&limit) {
            return Err(TcpError::new("retirement page exceeds bound"));
        }
        self.retirement_log(authority_epoch, epoch)?
            .wal_file_page(after, limit)
            .map_err(TcpError::component)
    }

    pub(super) fn retire_files(
        &mut self,
        authority_epoch: u64,
        epoch: u64,
        prefix: usize,
        files: &[u64],
    ) -> Result<(), TcpError> {
        self.require_history(false)?;
        self.retire_files_inner(authority_epoch, epoch, Some(prefix), files)
    }

    pub(super) fn retire_wal_files(
        &mut self,
        authority_epoch: u64,
        epoch: u64,
        files: &[u64],
    ) -> Result<(), TcpError> {
        self.require_history(true)?;
        self.retire_files_inner(authority_epoch, epoch, None, files)
    }

    fn retire_files_inner(
        &mut self,
        authority_epoch: u64,
        epoch: u64,
        prefix: Option<usize>,
        files: &[u64],
    ) -> Result<(), TcpError> {
        if files.len() > 256 {
            return Err(TcpError::new("retirement file count exceeds bound"));
        }
        let log = self.retirement_log(authority_epoch, epoch)?;
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        if !self.live_pins.is_empty() {
            return Err(TcpError::new(
                "live NVMe-oF pins must drain before retirement",
            ));
        }
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        if let Err(error) = self.revoke_live_epoch(epoch) {
            self.healthy = false;
            return Err(error);
        }
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        let retired_identities = if let Some(target) = self.live_target.as_ref() {
            let root = epoch_path(&self.config.root, epoch);
            let mut identities = Vec::new();
            for offset in files {
                let path = root.join(format!(
                    "wal_{:016x}",
                    offset / self.config.partition.wal_file_bytes
                ));
                if let Some(identity) = target.sealed_file(&path).map_err(TcpError::component)? {
                    identities.push(identity);
                }
            }
            identities
        } else {
            Vec::new()
        };
        match prefix {
            Some(prefix) => log.retire_files(prefix, files),
            None => log.retire_wal_files(files),
        }
        .map_err(TcpError::component)?;
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        if let Some(target) = self.live_target.as_mut() {
            for identity in retired_identities {
                target
                    .retire_deleted_file(&identity)
                    .map_err(TcpError::component)?;
            }
        }
        Ok(())
    }

    pub(super) fn checkpoint_proof(
        &self,
        authority_epoch: u64,
        epoch: u64,
        prefix: usize,
    ) -> Result<checkpoint::CheckpointProof, TcpError> {
        self.require_history(false)?;
        self.active(authority_epoch)?;
        let record = self
            .manifest
            .epochs
            .iter()
            .find(|entry| entry.epoch == epoch)
            .ok_or_else(|| TcpError::new("checkpoint epoch absent"))?;
        let log = match record.state {
            EpochState::Active | EpochState::Sealed => Some(
                self.logs
                    .get(&epoch)
                    .ok_or_else(|| TcpError::new("checkpoint stream absent"))?,
            ),
            EpochState::Abandoned => None,
            EpochState::Preparing => return Err(TcpError::new("checkpoint epoch incomplete")),
        };
        let retained = log.map_or(0, |log| log.receipt_count());
        if prefix > retained {
            return Err(TcpError::new("checkpoint prefix beyond retained history"));
        }
        if prefix != 0 {
            let log = log.ok_or_else(|| TcpError::new("checkpoint stream absent"))?;
            let last = log
                .receipt_page(prefix - 1, 1)
                .map_err(TcpError::component)?
                .into_iter()
                .next()
                .ok_or_else(|| TcpError::new("checkpoint receipt absent"))?;
            // Capturing a receipt is not durability evidence. Persist precisely
            // the captured prefix's last complete group before issuing proof.
            log.persist_through(last).map_err(TcpError::component)?;
        }
        let database = self.config.partition.inventory.database;
        let partition = self.config.partition.partition;
        Ok(checkpoint::CheckpointProof {
            database,
            partition,
            epoch,
            prefix,
            retained,
            digest: match log {
                Some(log) => log.checkpoint_prefix(prefix).map_err(TcpError::component)?,
                None => checkpoint::prefix_digest(database, partition, epoch, std::iter::empty()),
            },
        })
    }

    pub(super) fn recovery_page(
        &self,
        epoch: u64,
        cursor: usize,
        limit: usize,
        limits: TcpLimits,
    ) -> Result<Response, TcpError> {
        self.require_history(false)?;
        self.ensure_healthy()?;
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        if self.live_target.is_some() {
            return Err(TcpError::new(
                "live NVMe-oF recovery requires metadata-only control",
            ));
        }
        if limit == 0 || limit > limits.recovery_page_batches {
            return Err(TcpError::new("recovery page count exceeds bound"));
        }
        let record = self
            .manifest
            .epochs
            .iter()
            .find(|e| e.epoch == epoch)
            .ok_or_else(|| TcpError::new("recovery epoch absent"))?;
        if record.state == EpochState::Abandoned {
            return if cursor == 0 {
                Ok(Response::Recovery {
                    batches: Vec::new(),
                    next: None,
                })
            } else {
                Err(TcpError::new("invalid abandoned epoch cursor"))
            };
        }
        if record.state != EpochState::Sealed {
            return Err(TcpError::new("recovery requires sealed immutable epoch"));
        }
        let log = &self.logs[&epoch];
        let count = log.receipt_count();
        if cursor > count {
            return Err(TcpError::new("recovery cursor beyond retained history"));
        }
        let mut size = 32usize;
        let mut batches = Vec::new();
        // The wire limit is an upper bound. A bounded storage receipt page can
        // return fewer entries; `next` continues at the exact physical offset.
        for receipt in log
            .receipt_page(cursor, limit.min(256))
            .map_err(TcpError::component)?
        {
            let next = size
                .checked_add(receipt.address().len as usize + 256)
                .ok_or_else(|| TcpError::new("recovery response overflow"))?;
            if next > limits.max_frame_bytes {
                if batches.is_empty() {
                    return Err(TcpError::new("one recovery batch exceeds transport frame"));
                }
                break;
            }
            let batch = log.read_batch(receipt).map_err(TcpError::component)?;
            let durable = log.persist_through(receipt).map_err(TcpError::component)?;
            batches.push((
                batch.encoded().clone().into(),
                Receipt::from_durable(durable),
            ));
            size = next;
        }
        let end = cursor + batches.len();
        let next = (end < count).then_some(end);
        Ok(Response::Recovery { batches, next })
    }

    pub(super) fn require_history(&self, replay: bool) -> Result<(), TcpError> {
        use crate::distributed::storage::HistoryPersistence;
        if replay {
            if self.history_persistence != HistoryPersistence::WalReplay {
                return Err(TcpError::new("request requires WAL replay history"));
            }
        } else if self.history_persistence == HistoryPersistence::WalReplay {
            return Err(TcpError::new("request requires on-disk history"));
        }
        Ok(())
    }

    pub(super) fn checkpoint_log(
        &self,
        authority_epoch: u64,
        epoch: u64,
    ) -> Result<Option<Arc<super::super::storage::PartitionLog>>, TcpError> {
        self.require_history(true)?;
        self.active(authority_epoch)?;
        let record = self
            .manifest
            .epochs
            .iter()
            .find(|entry| entry.epoch == epoch)
            .ok_or_else(|| TcpError::new("checkpoint epoch absent"))?;
        match record.state {
            EpochState::Active | EpochState::Sealed => self
                .logs
                .get(&epoch)
                .cloned()
                .map(Some)
                .ok_or_else(|| TcpError::new("checkpoint stream absent")),
            EpochState::Abandoned => Ok(None),
            EpochState::Preparing => Err(TcpError::new("checkpoint epoch incomplete")),
        }
    }

    pub(super) fn replay_wal_page(
        &self,
        authority_epoch: u64,
        epoch: u64,
        from: u64,
        limit: usize,
        require_end: u64,
        limits: TcpLimits,
    ) -> Result<Response, TcpError> {
        self.require_history(true)?;
        self.active(authority_epoch)?;
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        if self.live_target.is_some() {
            return Err(TcpError::new(
                "live NVMe-oF recovery requires metadata-only control",
            ));
        }
        if limit == 0 || limit > limits.recovery_page_batches {
            return Err(TcpError::new("recovery page count exceeds bound"));
        }
        let record = self
            .manifest
            .epochs
            .iter()
            .find(|entry| entry.epoch == epoch)
            .ok_or_else(|| TcpError::new("recovery epoch absent"))?;
        if record.state == EpochState::Abandoned {
            return if from == 0 {
                Ok(Response::Recovery {
                    batches: Vec::new(),
                    next: None,
                })
            } else {
                Err(TcpError::new("invalid abandoned epoch cursor"))
            };
        }
        if record.state != EpochState::Sealed {
            return Err(TcpError::new("recovery requires sealed immutable epoch"));
        }
        let log = self
            .logs
            .get(&epoch)
            .ok_or_else(|| TcpError::new("recovery stream absent"))?;
        let page = log
            .replay_page(
                from,
                limit.min(256),
                limits.max_frame_bytes.saturating_sub(32),
                require_end,
            )
            .map_err(TcpError::component)?;
        let mut size = 32usize;
        let mut batches = Vec::new();
        let next = page.next;
        for decoded in page.groups {
            let group_size = decoded.receipts.iter().try_fold(size, |size, receipt| {
                size.checked_add(receipt.address().len as usize + 256)
                    .ok_or_else(|| TcpError::new("recovery response overflow"))
            })?;
            if group_size > limits.max_frame_bytes {
                return Err(TcpError::new("one recovery batch exceeds transport frame"));
            }
            size = group_size;
            for (batch, receipt) in decoded.group.batches().iter().zip(decoded.receipts) {
                batches.push((
                    batch.encoded().clone().into(),
                    Receipt {
                        id: receipt.id(),
                        version: receipt.version(),
                        digest: receipt.digest(),
                        address: receipt.address(),
                    },
                ));
            }
        }
        let next = next
            .map(usize::try_from)
            .transpose()
            .map_err(|_| TcpError::new("replay offset exceeds address space"))?;
        Ok(Response::Recovery { batches, next })
    }
    pub(super) fn stats(&self) -> Result<PartitionStats, TcpError> {
        self.ensure_healthy()?;
        Ok(self
            .manifest
            .epochs
            .last()
            .and_then(|e| self.logs.get(&e.epoch))
            .map_or_else(PartitionStats::default, |log| log.stats()))
    }
}
fn partition_at(template: &PartitionConfig, epoch: u64) -> PartitionConfig {
    let mut config = template.clone();
    config.inventory.epoch = epoch;
    config.inventory.segment_generation = epoch;
    config
}
fn epoch_path(root: &Path, epoch: u64) -> PathBuf {
    root.join(format!("epoch_{epoch:016x}"))
}
fn directory(path: &Path) -> Result<(), TcpError> {
    if !fs::symlink_metadata(path)?.file_type().is_dir() {
        return Err(TcpError::new("node inventory path is not a directory"));
    }
    Ok(())
}
fn persist(root: &Path, manifest: &Manifest) -> Result<(), TcpError> {
    install_metadata(
        root,
        MANIFEST,
        &encode_metadata(manifest).map_err(TcpError::component)?,
    )
    .map_err(TcpError::component)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed::tcp::tests::{config, metrics};

    #[cfg(all(target_os = "linux", feature = "test-utils"))]
    #[test]
    fn benchmark_eviction_includes_sealed_source_epochs() {
        let temporary = tempdir::TempDir::new("node-evict-retained").unwrap();
        let config = config(&temporary.path().join("node"));
        let mut inventory = NodeInventory::open(config.clone(), metrics()).unwrap();
        inventory.fence_to(1).unwrap();
        let batch = crate::distributed::tcp::tests::batch(&config, 1, 0);
        let source = inventory.active(1).unwrap();
        let receipt = source
            .append_complete_batch(RequestAuthority { epoch: 1 }, &batch)
            .unwrap();
        source.persist_through(receipt).unwrap();
        drop(source);
        inventory.seal(1).unwrap();
        inventory.fence_to(2).unwrap();
        let source_bytes = inventory.logs[&1]
            .read_evidence_for_benchmark(false)
            .unwrap()
            .file_bytes;
        let expected: u64 = inventory
            .logs
            .values()
            .map(|log| log.read_evidence_for_benchmark(false).unwrap().file_bytes)
            .sum();
        assert!(source_bytes > 0);
        let evidence = inventory.read_evidence_for_benchmark(2, true).unwrap();
        assert_eq!(evidence.file_bytes, expected);
        assert!(inventory.read_evidence_for_benchmark(1, true).is_err());
    }

    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    #[test]
    fn offline_export_requires_sealed_epoch_and_retains_owner_lock() {
        for mode in [
            crate::distributed::storage::HistoryPersistence::OnDisk,
            crate::distributed::storage::HistoryPersistence::WalReplay,
        ] {
            offline_export_requires_sealed_epoch_mode(mode);
        }
    }

    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    fn offline_export_requires_sealed_epoch_mode(
        mode: crate::distributed::storage::HistoryPersistence,
    ) {
        let temporary = tempdir::TempDir::new("node-sealed-export").unwrap();
        let config = config(&temporary.path().join("node"));
        let mut inventory = NodeInventory::open(config.clone(), metrics()).unwrap();
        inventory.set_history_persistence(mode).unwrap();
        inventory.fence_to(1).unwrap();
        assert!(SealedOwnerExport::open(&config, 1, metrics()).is_err());
        drop(inventory);
        assert!(SealedOwnerExport::open(&config, 1, metrics()).is_err());
        let mut inventory = NodeInventory::open(config.clone(), metrics()).unwrap();
        inventory.fence_to(2).unwrap();
        inventory.seal(2).unwrap();
        drop(inventory);
        let export = SealedOwnerExport::open(&config, 2, metrics()).unwrap();
        assert!(NodeInventory::open(config.clone(), metrics()).is_err());
        drop(export);
        assert!(NodeInventory::open(config, metrics()).is_ok());
    }
    #[cfg(not(all(feature = "experimental-nvmeof", target_os = "linux")))]
    #[test]
    fn feature_disabled_reopen_rejects_unreleased_nvme_capability() {
        let temporary = tempdir::TempDir::new("node-nvme-feature-downgrade").unwrap();
        let config = config(&temporary.path().join("node"));
        let mut inventory = NodeInventory::open(config.clone(), metrics()).unwrap();
        inventory.fence_to(1).unwrap();
        drop(inventory);
        let before = fs::read(config.root.join(MANIFEST)).unwrap();
        let marker = config.root.join("nvmeof-export-leases-v1");
        fs::write(&marker, b"unreleased or corrupt capability state").unwrap();
        assert!(NodeInventory::open(config.clone(), metrics()).is_err());
        assert_eq!(fs::read(config.root.join(MANIFEST)).unwrap(), before);
        fs::remove_file(marker).unwrap();
        let reopened = NodeInventory::open(config, metrics()).unwrap();
        assert_eq!(reopened.status().unwrap().active_epoch, None);
    }

    #[test]
    fn interrupted_preparing_epoch_is_retained_and_never_activated() {
        let temporary = tempdir::TempDir::new("node-preparing-fence").unwrap();
        let config = config(&temporary.path().join("node"));
        let mut inventory = NodeInventory::open(config.clone(), metrics()).unwrap();
        inventory
            .set_history_persistence(crate::distributed::storage::HistoryPersistence::OnDisk)
            .unwrap();
        inventory.fence_to(1).unwrap();
        inventory.seal(1).unwrap();
        inventory.manifest.epochs.push(Epoch {
            epoch: 2,
            state: EpochState::Preparing,
        });
        persist(&config.root, &inventory.manifest).unwrap();
        // Simulate interrupted creation after the durable reservation, before
        // any registered stream or active reply could become eligible.
        fs::create_dir(epoch_path(&config.root, 2)).unwrap();
        drop(inventory);
        let mut inventory = NodeInventory::open(config.clone(), metrics()).unwrap();
        assert_eq!(
            inventory.status().unwrap().epochs,
            vec![
                NodeEpoch {
                    epoch: 1,
                    sealed: true
                },
                NodeEpoch {
                    epoch: 2,
                    sealed: true
                }
            ]
        );
        assert!(inventory.fence_to(2).is_err());
        assert!(
            matches!(inventory.recovery_page(2,0,32,TcpLimits::default()).unwrap(),Response::Recovery{batches,next:None} if batches.is_empty())
        );
        inventory.fence_to(3).unwrap();
        assert!(epoch_path(&config.root, 2).is_dir());
    }

    #[test]
    fn surviving_log_retains_node_root_lock() {
        let temporary = tempdir::TempDir::new("node-root-lock").unwrap();
        let config = config(&temporary.path().join("node"));
        let mut inventory = NodeInventory::open(config.clone(), metrics()).unwrap();
        inventory.fence_to(1).unwrap();
        let log = inventory.active(1).unwrap();
        drop(inventory);
        assert!(NodeInventory::open(config.clone(), metrics()).is_err());
        drop(log);
        let inventory = NodeInventory::open(config, metrics()).unwrap();
        assert_eq!(inventory.status().unwrap().active_epoch, None);
    }

    #[test]
    fn checkpoint_proof_binds_exact_retained_prefix_and_fenced_authority() {
        let temporary = tempdir::TempDir::new("node-checkpoint-proof").unwrap();
        let config = config(&temporary.path().join("node"));
        let mut inventory = NodeInventory::open(config.clone(), metrics()).unwrap();
        inventory
            .set_history_persistence(crate::distributed::storage::HistoryPersistence::OnDisk)
            .unwrap();
        inventory.fence_to(1).unwrap();
        let mut receipts = Vec::new();
        for sequence in 0..2 {
            let id = (sequence * 1024..)
                .map(|n: u64| {
                    let mut operation = [0; 16];
                    operation[..8].copy_from_slice(&n.to_be_bytes());
                    BatchId {
                        database: config.partition.inventory.database,
                        operation,
                    }
                })
                .find(|id| {
                    config.partition.inventory.partition_for(*id).unwrap()
                        == config.partition.partition
                })
                .unwrap();
            let batch = CompleteBatch::new(
                &inventory.shape,
                id,
                BatchVersion { epoch: 1, sequence },
                vec![Mutation::Put {
                    keyspace: crate::key_shape::KeySpace(0),
                    key: b"key1"[..].into(),
                    value: vec![sequence as u8; 32].into(),
                }],
                config.partition.batch_limits,
            )
            .unwrap();
            let receipt = inventory
                .active(1)
                .unwrap()
                .append_complete_batch(RequestAuthority { epoch: 1 }, &batch)
                .unwrap();
            receipts.push(checkpoint::CheckpointReceipt::from(receipt));
            // Each singleton append is a distinct group. Make room for the
            // second append, then let checkpoint_proof persist that final group.
            if sequence == 0 {
                inventory
                    .active(1)
                    .unwrap()
                    .persist_through(receipt)
                    .unwrap();
            }
        }
        let prefix = inventory.checkpoint_proof(1, 1, 1).unwrap();
        assert_eq!(prefix.retained, 2);
        assert_eq!(
            prefix.digest,
            checkpoint::prefix_digest(
                prefix.database,
                prefix.partition,
                1,
                receipts[..1].iter().copied()
            )
        );
        assert_ne!(
            prefix.digest,
            inventory.checkpoint_proof(1, 1, 2).unwrap().digest
        );
        assert!(inventory.checkpoint_proof(1, 1, 3).is_err());
        assert!(inventory.checkpoint_proof(1, 99, 0).is_err());
        inventory.fence_to(2).unwrap();
        assert!(inventory.checkpoint_proof(1, 1, 1).is_err());
        assert_eq!(inventory.checkpoint_proof(2, 1, 1).unwrap(), prefix);
        drop(inventory);
        let mut inventory = NodeInventory::open(config, metrics()).unwrap();
        inventory.fence_to(3).unwrap();
        assert_eq!(inventory.checkpoint_proof(3, 1, 1).unwrap(), prefix);
    }
}
