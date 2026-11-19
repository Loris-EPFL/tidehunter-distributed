//! One node's durable epoch fence: an epoch is recorded preparing before its
//! files exist and active after registration, with one physical partition.
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
/// Registry of the removed sealed-export route; see `reject_legacy_owner_state`.
const LEGACY_EXPORT_LEASES: &str = "nvmeof-export-leases-v1";
// Bound idle history namespaces, subsystem objects and pinned target FDs
// independently of WAL history size. In-flight pages keep their separate pins.
#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
pub(super) const MAX_IDLE_HISTORY_EXPORTS: usize = 16;
/// The owner request that created a live export inline, while its caller
/// waited; selects the `LiveInlineExportStats` field it counts in.
#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum InlineExport {
    Lease,
    Rollover,
    Prelease,
    Resolve,
    Recover,
    Replay,
}
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

#[cfg(all(test, feature = "experimental-nvmeof", target_os = "linux"))]
std::thread_local! {
    static LIVE_FIXTURE: std::cell::RefCell<Option<PathBuf>> = const { std::cell::RefCell::new(None) };
}

/// Select only configfs emulation; `open` still commissions and validates the
/// normal durable marker while holding the node lock, before recovering WALs.
#[cfg(all(test, feature = "experimental-nvmeof", target_os = "linux"))]
pub(super) fn with_live_target_fixture<R>(configfs: PathBuf, open: impl FnOnce() -> R) -> R {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            LIVE_FIXTURE.with(|fixture| fixture.borrow_mut().take());
        }
    }
    LIVE_FIXTURE.with(|fixture| assert!(fixture.borrow_mut().replace(configfs).is_none()));
    let _reset = Reset;
    open()
}

#[cfg(all(test, feature = "experimental-nvmeof", target_os = "linux"))]
fn open_live_target_fixture(
    config: super::super::nvmeof::LiveTargetConfig,
    expected: Option<super::super::nvmeof::live_target::LiveTargetIdentity>,
) -> io::Result<super::super::nvmeof::live_target::LiveTargetManager> {
    use super::super::nvmeof::live_target::LiveTargetManager;
    if let Some(configfs) = LIVE_FIXTURE.with(|fixture| fixture.borrow().clone()) {
        return match expected {
            Some(identity) => LiveTargetManager::open_existing_fixture(config, configfs, identity),
            None => LiveTargetManager::open_fixture(config, configfs),
        };
    }
    match expected {
        Some(identity) => LiveTargetManager::open_existing(config, identity),
        None => LiveTargetManager::open(config),
    }
}

/// What the lifecycle gate owns: the live target registry when one is
/// configured. Builds without the Linux NVMe-oF feature own nothing; the gate
/// still serializes their epoch lifecycle.
#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
pub(super) type LifecycleSlot = Option<super::super::nvmeof::live_target::LiveTargetManager>;
#[cfg(not(all(feature = "experimental-nvmeof", target_os = "linux")))]
pub(super) type LifecycleSlot = ();

/// The node lifecycle gate. It is the single writer of the node's durable
/// lifecycle metadata: the epoch manifest, the set of logs, live bindings and
/// pins, and the live target registry (state journal, configfs objects and
/// sealed markers). It is held across their file and directory syncs, so
/// their records reach disk in the order they were decided, and each effect
/// that needs a durable record (a configfs object, an export, a revocation)
/// runs only after that record's sync returned.
///
/// Lock order: the control context's live-control gate → lifecycle gate →
/// `NodeInventory` →
/// `readers`. The inventory mutex is never held across a sync: holders of the
/// gate release it for every I/O step (`InventoryAccess::released`) and
/// retake it only to read or install state. The gate is never taken while the
/// inventory is held. Foreground work (appends, live completions, position
/// proofs, grant lookups) takes only the inventory, so it never waits for a
/// sync. The file preparer only `try_lock`s the gate, because fence, seal and
/// final shutdown join it while they hold the gate.
pub(super) type LifecycleGate = Arc<Mutex<LifecycleSlot>>;

/// Access to an inventory that a lifecycle step may release during I/O.
pub(super) trait InventoryAccess: std::ops::DerefMut<Target = NodeInventory> {
    /// Run `io` without the inventory mutex, then take it again.
    fn released<R>(&mut self, io: impl FnOnce() -> R) -> R;
}

/// A shared inventory: release the mutex across the I/O. The caller holds the
/// lifecycle gate, so between the two halves other threads can only read.
impl InventoryAccess for parking_lot::MutexGuard<'_, NodeInventory> {
    fn released<R>(&mut self, io: impl FnOnce() -> R) -> R {
        parking_lot::MutexGuard::unlocked(self, io)
    }
}

/// An exclusively owned inventory (opening, offline tools and tests): no
/// other thread can observe it, so there is no lock to release.
impl InventoryAccess for &mut NodeInventory {
    fn released<R>(&mut self, io: impl FnOnce() -> R) -> R {
        io()
    }
}

/// The configured live target of a gated lifecycle step: `None` for a node
/// without one. A live inventory reached without its gate content (an owned
/// inventory, or a caller that skipped the gate) is refused, never skipped.
#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
fn live_target<'a>(
    inventory: &NodeInventory,
    slot: &'a mut LifecycleSlot,
) -> Result<Option<&'a mut super::super::nvmeof::live_target::LiveTargetManager>, TcpError> {
    match (inventory.live_target_configured, slot.as_mut()) {
        (false, None) => Ok(None),
        (true, Some(target)) => Ok(Some(target)),
        _ => Err(TcpError::new(
            "live target lifecycle requires the node lifecycle gate",
        )),
    }
}

/// History settings that every new or recovered log of this node receives.
/// Captured under the inventory and applied to a log that is not yet
/// published, so the inventory can be released while the log is opened.
struct LogSettings {
    diagnostics: bool,
    persistence: crate::distributed::storage::HistoryPersistence,
    notifier: Option<Arc<dyn Fn() + Send + Sync>>,
}

impl LogSettings {
    fn apply(&self, log: &PartitionLog) -> Result<(), TcpError> {
        log.set_history_maintenance_diagnostics(self.diagnostics);
        log.set_history_persistence(self.persistence)
            .map_err(TcpError::component)?;
        if let Some(notifier) = &self.notifier {
            log.set_history_failure_notifier(notifier.clone());
        }
        Ok(())
    }
}

#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
type LiveTarget = super::super::nvmeof::live_target::LiveTargetManager;
#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
type LiveBinding = (
    super::super::nvmeof::live_storage::LiveWalSegment,
    super::super::nvmeof::live_target::LiveTargetBinding,
    bool,
);

#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
fn live_file(
    segment: &super::super::nvmeof::live_storage::LiveWalSegment,
) -> super::super::nvmeof::live_target::LiveFileIdentity {
    super::super::nvmeof::live_target::LiveFileIdentity {
        path: segment.path.clone(),
        device: segment.device,
        inode: segment.inode,
        bytes: segment.bytes,
        block_size: segment.block_size,
    }
}

#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
fn export_span(
    segment: &super::super::nvmeof::live_storage::LiveWalSegment,
) -> super::super::owner_events::OwnerEventSpan {
    super::super::owner_events::OwnerEventSpan::new(
        super::super::owner_events::OwnerEventKind::ExportStart,
        super::super::owner_events::OwnerEventKind::ExportEnd,
        segment.partition,
        segment.file_index,
        segment.inode,
    )
}

#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
fn revoke_span(
    segment: &super::super::nvmeof::live_storage::LiveWalSegment,
) -> super::super::owner_events::OwnerEventSpan {
    super::super::owner_events::OwnerEventSpan::new(
        super::super::owner_events::OwnerEventKind::RevokeStart,
        super::super::owner_events::OwnerEventKind::RevokeEnd,
        segment.partition,
        segment.file_index,
        segment.inode,
    )
}

/// Revoke claimed bindings with one fence and one cleanup pass. Runs with the
/// inventory released and the lifecycle gate held.
#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
fn revoke_bindings<'a>(
    target: &mut LiveTarget,
    claimed: impl Iterator<Item = &'a LiveBinding>,
) -> Result<(), TcpError> {
    let (spans, bindings): (Vec<_>, Vec<_>) = claimed
        .map(|(segment, binding, _)| (revoke_span(segment), binding))
        .unzip();
    target.revoke_all(&bindings).map_err(TcpError::component)?;
    drop(spans);
    Ok(())
}

pub struct NodeInventory {
    pub(super) config: NodeConfig,
    pub(super) shape: KeyShape,
    manifest: Manifest,
    logs: BTreeMap<u64, Arc<PartitionLog>>,
    pub(super) healthy: bool,
    pub(super) terminal: Arc<AtomicBool>,
    /// Sticky persistence-failure latch of this incarnation (see
    /// `ControlContext::persistence_failed`). Never cleared.
    pub(super) persistence_failed: Arc<AtomicBool>,
    pub(super) readers: Arc<RwLock<ReaderAuthority>>,
    metrics: Arc<Metrics>,
    history_diagnostics: bool,
    history_persistence: crate::distributed::storage::HistoryPersistence,
    history_failure_notifier: Option<Arc<dyn Fn() + Send + Sync>>,
    // Owner incarnation, created once at open (see `durable_progress::new_incarnation`).
    incarnation: [u8; 16],
    /// Whether a live target is installed. Immutable after installation, so
    /// reading it needs no gate.
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    pub(super) live_target_configured: bool,
    /// Cloned out at node open. Never locked by inventory methods: callers
    /// take it before the inventory (see `LifecycleGate`).
    pub(super) lifecycle: LifecycleGate,
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    live_shutdown: bool,
    // Keep preparation costs when fencing replaces an active log with recovery.
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    retired_prewrite_bytes: u64,
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    retired_prewrite_ns: u64,
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    retired_prewrite_files: u64,
    // Changed only under this inventory's lock, when an export's binding is
    // installed.
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    pub(super) inline_exports: LiveInlineExportStats,
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
    // Connection pins are distinct from record/page pins. One bounded pool
    // entry owns one token until its controller is confirmed disconnected.
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    pub(super) live_export_pins: BTreeMap<[u8; 16], (u64, (u64, u64))>,
    // At most 1024 outstanding tokens are fenced per retained epoch. Keep
    // tombstones so only tokens actually cleared by a fence are idempotent.
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    fenced_export_pins: BTreeMap<[u8; 16], u64>,
    lock: Arc<DbLock>,
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
        reject_legacy_owner_state(&config.root)?;
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
            #[cfg(not(test))]
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
                    #[cfg(test)]
                    let manager = open_live_target_fixture(saved.config, Some(saved.identity))
                        .map_err(TcpError::component)?;
                    #[cfg(not(test))]
                    let manager = LiveTargetManager::open_existing(saved.config, saved.identity)
                        .map_err(TcpError::component)?;
                    Some(manager)
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
                        #[cfg(test)]
                        let manager = open_live_target_fixture(target_config.clone(), None)
                            .map_err(TcpError::component)?;
                        #[cfg(not(test))]
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
            persistence_failed: Arc::new(AtomicBool::new(false)),
            readers,
            metrics,
            history_diagnostics: false,
            history_persistence,
            history_failure_notifier: None,
            incarnation: super::durable_progress::new_incarnation()?,
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            live_target_configured: live_target.is_some(),
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            lifecycle: Arc::new(Mutex::new(live_target)),
            #[cfg(not(all(feature = "experimental-nvmeof", target_os = "linux")))]
            lifecycle: Arc::new(Mutex::new(())),
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            live_shutdown: false,
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            retired_prewrite_bytes: 0,
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            retired_prewrite_ns: 0,
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            retired_prewrite_files: 0,
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            inline_exports: LiveInlineExportStats::default(),
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            live_bindings: BTreeMap::new(),
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            idle_history_exports: std::collections::VecDeque::new(),
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            live_pins: BTreeMap::new(),
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            live_export_pins: BTreeMap::new(),
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            fenced_export_pins: BTreeMap::new(),
            lock,
        })
    }
    pub(super) fn set_history_maintenance_diagnostics(&mut self, enabled: bool) {
        self.history_diagnostics = enabled;
        for log in self.logs.values() {
            log.set_history_maintenance_diagnostics(enabled);
        }
    }

    #[cfg(test)]
    pub(super) fn set_history_persistence(
        &mut self,
        mode: crate::distributed::storage::HistoryPersistence,
    ) -> Result<(), TcpError> {
        let mut inventory = self;
        Self::switch_history_persistence(&mut inventory, mode)
    }

    /// Switch every retained log, then record the mode for later logs. The
    /// switch can install or remove a WAL root and sync its directory, so it
    /// runs with the inventory released. The caller holds the lifecycle gate
    /// (or owns the inventory), which also orders it against log creation.
    pub(super) fn switch_history_persistence<H: InventoryAccess>(
        inventory: &mut H,
        mode: crate::distributed::storage::HistoryPersistence,
    ) -> Result<(), TcpError> {
        let logs: Vec<_> = inventory.logs.values().cloned().collect();
        inventory.released(|| {
            for log in &logs {
                log.set_history_persistence(mode)
                    .map_err(TcpError::component)?;
            }
            Ok::<_, TcpError>(())
        })?;
        inventory.history_persistence = mode;
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
        if !self.healthy || !self.live_target_configured || self.live_shutdown {
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
            live_nvmeof: self.live_target_configured && !self.live_shutdown,
            #[cfg(not(all(feature = "experimental-nvmeof", target_os = "linux")))]
            live_nvmeof: false,
            // The control owner holds the tier; it sets this field in its replies.
            durability_tier: super::super::durability::DurabilityTier::default(),
            incarnation: self.incarnation,
            // Every log this incarnation recovered stays in `logs` (a live fence
            // replaces the old handle with its recovery, and fails unless the old
            // frontier drained), so this covers every recovery since open. A failed
            // fsync may have dropped acknowledged dirty pages without any hole, so a
            // persistence failure or poison (latch, Acquire: pairs with the funnels'
            // Release) or a failed frontier makes every later recovery unprovable.
            recovery_clean: !self.persistence_failed.load(Ordering::Acquire)
                && !self.logs.values().any(|log| {
                    log.recovery_found_hole()
                        || log
                            .frontier()
                            .is_some_and(|frontier| frontier.failure().is_some())
                }),
        })
    }
    /// Fence an exclusively owned inventory. A live inventory needs its target
    /// registry: use `fence` with the lifecycle gate instead.
    pub fn fence_to(&mut self, epoch: u64) -> Result<NodeStatus, TcpError> {
        let mut inventory = self;
        Self::fence(
            &mut inventory,
            &mut LifecycleSlot::default(),
            epoch,
            false,
            |_| Ok(()),
        )
    }
    /// Live recovery fence after a recoverable live failure. Same gate contract
    /// as `fence`.
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    pub(super) fn fence_recoverable_live<H: InventoryAccess>(
        inventory: &mut H,
        slot: &mut LifecycleSlot,
        epoch: u64,
        drain_log: impl FnOnce(&Arc<PartitionLog>) -> Result<(), TcpError>,
    ) -> Result<NodeStatus, TcpError> {
        if !inventory.healthy
            || !inventory.live_target_configured
            || !inventory
                .manifest
                .epochs
                .last()
                .is_some_and(|entry| entry.state == EpochState::Active && epoch > entry.epoch)
        {
            return Err(TcpError::new(
                "live recovery requires a strictly higher authority epoch",
            ));
        }
        Self::fence(inventory, slot, epoch, true, drain_log)
    }
    /// Fence to `epoch`. The caller holds the lifecycle gate (`slot` is its
    /// content) or owns the inventory exclusively. Every sync runs with the
    /// inventory released; the gate keeps the manifest and log set single
    /// writer, and `healthy` stays false until the new epoch is installed.
    /// `drain_log` joins the control owner's workers of the old live log after
    /// target revocation and before the log is scanned.
    pub(super) fn fence<H: InventoryAccess>(
        inventory: &mut H,
        slot: &mut LifecycleSlot,
        epoch: u64,
        recovering: bool,
        drain_log: impl FnOnce(&Arc<PartitionLog>) -> Result<(), TcpError>,
    ) -> Result<NodeStatus, TcpError> {
        if !recovering {
            inventory.ensure_healthy()?;
        }
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        if inventory.live_shutdown {
            return Err(TcpError::new("live target has shut down; reopen required"));
        }
        if inventory
            .manifest
            .epochs
            .last()
            .is_some_and(|e| e.epoch == epoch && e.state == EpochState::Active)
        {
            return inventory.status();
        }
        if epoch == 0
            || inventory
                .manifest
                .epochs
                .last()
                .is_some_and(|e| epoch <= e.epoch)
        {
            return Err(TcpError::new(
                "epoch already fenced/sealed; cannot reactivate",
            ));
        }
        if inventory.manifest.epochs.len() == inventory.config.max_epochs {
            return Err(TcpError::new("retained node epoch capacity reached"));
        }
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        let target = live_target(inventory, slot)?;
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        let live = target.is_some();
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        if let Some(target) = target {
            // The new authority fences the prior coordinator before any old
            // allocation is scanned or its namespace-backed pins are dropped.
            inventory.healthy = false;
            {
                let mut readers = inventory.readers.write();
                readers.invalidate();
                readers.logs.clear();
            }
            Self::revoke_live_targets(inventory, target)?;
            let inventory = &mut **inventory;
            inventory.fenced_export_pins.extend(
                inventory
                    .live_export_pins
                    .iter()
                    .map(|(token, (epoch, _))| (*token, *epoch)),
            );
            inventory.live_pins.clear();
            inventory.live_export_pins.clear();
        }
        if let Some(old) = inventory
            .manifest
            .epochs
            .last()
            .filter(|e| e.state == EpochState::Active)
            .copied()
        {
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            if live {
                // Revocation and pin release above fence external writers/readers.
                // Join the control owner's syncer too: both its slot and callback
                // retain this log and its partition lock until the worker exits.
                let draining = inventory
                    .logs
                    .get(&old.epoch)
                    .cloned()
                    .ok_or_else(|| TcpError::new("active live stream missing"))?;
                inventory.released(|| drain_log(&draining))?;
                drop(draining);
                let previous = inventory
                    .logs
                    .remove(&old.epoch)
                    .ok_or_else(|| TcpError::new("active live stream missing"))?;
                if Arc::strong_count(&previous) != 1 {
                    inventory.logs.insert(old.epoch, previous);
                    return Err(TcpError::new(
                        "old live stream still has outstanding handles; reopen required",
                    ));
                }
                let preparation = previous.live_file_preparation_stats();
                inventory.retired_prewrite_bytes = inventory
                    .retired_prewrite_bytes
                    .saturating_add(preparation.live_file_prewrite_bytes);
                inventory.retired_prewrite_ns = inventory
                    .retired_prewrite_ns
                    .saturating_add(preparation.live_file_prewrite_ns);
                inventory.retired_prewrite_files = inventory
                    .retired_prewrite_files
                    .saturating_add(preparation.live_file_prewrite_files);
                drop(previous);
                let path = epoch_path(&inventory.config.root, old.epoch);
                let expected = partition_at(&inventory.config.partition, old.epoch);
                let metrics = inventory.metrics.clone();
                let authority = inventory.lock.clone();
                let settings = inventory.log_settings();
                // All target capabilities were synchronously drained above.
                // The core opens its own partition DbLock; no lock is bypassed.
                let log = inventory.released(|| {
                    let log =
                        PartitionLog::recover_live_sealed(&path, &expected, metrics, || Ok(()))
                            .map_err(TcpError::component)?;
                    let log = Arc::new(log.retain_local_authority(authority));
                    settings.apply(&log)?;
                    Ok::<_, TcpError>(log)
                })?;
                inventory.logs.insert(old.epoch, log);
                inventory.manifest.epochs.last_mut().unwrap().state = EpochState::Sealed;
                Self::persist_manifest(inventory)?;
                inventory.healthy = true;
            } else {
                Self::seal_epoch(inventory, slot, old.epoch, |_| Ok(()))?;
            }
            #[cfg(not(all(feature = "experimental-nvmeof", target_os = "linux")))]
            Self::seal_epoch(inventory, slot, old.epoch, |_| Ok(()))?;
        }
        #[cfg(not(all(feature = "experimental-nvmeof", target_os = "linux")))]
        let _ = &drain_log;
        inventory.healthy = false;
        inventory.readers.write().invalidate();
        inventory.manifest.epochs.push(Epoch {
            epoch,
            state: EpochState::Preparing,
        });
        Self::persist_manifest(inventory)?;
        let path = epoch_path(&inventory.config.root, epoch);
        let partition = partition_at(&inventory.config.partition, epoch);
        let shape = inventory.shape.clone();
        let metrics = inventory.metrics.clone();
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        let prewrite = inventory.config.wal_prewrite;
        let authority = inventory.lock.clone();
        let settings = inventory.log_settings();
        let log = inventory.released(|| {
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            let log = if live {
                PartitionLog::create_live_with_prewrite(&path, partition, &shape, metrics, prewrite)
            } else {
                PartitionLog::create(&path, partition, &shape, metrics)
            };
            #[cfg(not(all(feature = "experimental-nvmeof", target_os = "linux")))]
            let log = PartitionLog::create(&path, partition, &shape, metrics);
            let log = Arc::new(
                log.map_err(TcpError::component)?
                    .retain_local_authority(authority),
            );
            settings.apply(&log)?;
            Ok::<_, TcpError>(log)
        })?;
        inventory.manifest.epochs.last_mut().unwrap().state = EpochState::Active;
        Self::persist_manifest(inventory)?;
        let inventory = &mut **inventory;
        inventory.logs.insert(epoch, log);
        inventory.readers.write().publish(epoch, &inventory.logs);
        inventory.healthy = true;
        if recovering {
            inventory.terminal.store(false, Ordering::Release);
        }
        inventory.status()
    }
    /// Install the manifest with the inventory released. Only lifecycle gate
    /// holders change the manifest, so installs happen in decision order.
    fn persist_manifest<H: InventoryAccess>(inventory: &mut H) -> Result<(), TcpError> {
        let bytes = encode_metadata(&inventory.manifest).map_err(TcpError::component)?;
        let root = inventory.config.root.clone();
        inventory
            .released(|| install_metadata(&root, MANIFEST, &bytes))
            .map_err(TcpError::component)
    }
    fn log_settings(&self) -> LogSettings {
        LogSettings {
            diagnostics: self.history_diagnostics,
            persistence: self.history_persistence,
            notifier: self.history_failure_notifier.clone(),
        }
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

    /// Revoke idle writable on-demand exports of `epoch`. The caller holds the
    /// lifecycle gate; the claimed bindings leave `live_bindings` before the
    /// inventory is released, so no pin can attach to a namespace that is
    /// being revoked, and they return if the revocation fails.
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    pub(super) fn reap_idle_live_segments<H: InventoryAccess>(
        inventory: &mut H,
        target: &mut LiveTarget,
        epoch: u64,
    ) -> Result<(), TcpError> {
        let mut revoke = Vec::new();
        {
            let inventory: &NodeInventory = inventory;
            let log = inventory.active(epoch)?;
            for (&key, (_, binding, writable)) in &inventory.live_bindings {
                if key.0 == epoch
                    && *writable
                    && !binding.permanent
                    && !inventory
                        .live_export_pins
                        .values()
                        .any(|(_, file)| *file == key)
                    && !inventory.live_pins.values().any(|(_, leases)| {
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
        }
        if revoke.is_empty() {
            return Ok(());
        }
        let claimed: Vec<_> = revoke
            .iter()
            .filter_map(|key| inventory.live_bindings.remove_entry(key))
            .collect();
        let result = inventory.released(|| revoke_bindings(target, claimed.iter().map(|(_, b)| b)));
        if result.is_err() {
            inventory.live_bindings.extend(claimed);
        }
        result
    }

    /// Count one export that `kind` created inline.
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    pub(super) fn record_inline_export(&mut self, kind: InlineExport) {
        let stats = &mut self.inline_exports;
        let count = match kind {
            InlineExport::Lease => &mut stats.lease,
            InlineExport::Rollover => &mut stats.rollover,
            InlineExport::Prelease => &mut stats.prelease,
            InlineExport::Resolve => &mut stats.resolve,
            InlineExport::Recover => &mut stats.recover,
            InlineExport::Replay => &mut stats.replay,
        };
        *count = count.saturating_add(1);
    }

    /// Return the binding of `segment`, exporting it if needed and counting
    /// the export for the request `kind`. The caller holds the lifecycle gate;
    /// the export's journal syncs and configfs work run with the inventory
    /// released, and the binding is installed after.
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    pub(super) fn publish_live_segment<H: InventoryAccess>(
        inventory: &mut H,
        target: &mut LiveTarget,
        segment: &super::super::nvmeof::live_storage::LiveWalSegment,
        writable: bool,
        kind: InlineExport,
    ) -> Result<super::super::nvmeof::live_target::LiveTargetBinding, TcpError> {
        let key = (segment.epoch, segment.file_index);
        if let Some((existing, binding, was_writable)) = inventory.live_bindings.get(&key) {
            if existing != segment || (writable && !was_writable) {
                return Err(TcpError::new("live segment identity or access changed"));
            }
            let binding = binding.clone();
            inventory.idle_history_exports.retain(|idle| *idle != key);
            return Ok(binding);
        }
        let file = live_file(segment);
        let export_span = export_span(segment);
        let binding = inventory
            .released(|| {
                if writable {
                    target.export_writable(file)
                } else {
                    target.export_readonly(file)
                }
            })
            .map_err(TcpError::component)?;
        export_span.end_at(segment.file_index);
        inventory.record_inline_export(kind);
        inventory
            .live_bindings
            .insert(key, (segment.clone(), binding.clone(), writable));
        Ok(binding)
    }

    /// Same gate contract as `publish_live_segment`, for permanent exports.
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    pub(super) fn publish_permanent_segment<H: InventoryAccess>(
        inventory: &mut H,
        target: &mut LiveTarget,
        segment: &super::super::nvmeof::live_storage::LiveWalSegment,
    ) -> Result<(super::super::nvmeof::live_target::LiveTargetBinding, bool), TcpError> {
        use super::super::nvmeof::live_storage::wal_export_name;
        let key = (segment.epoch, segment.file_index);
        if let Some((existing, binding, writable)) = inventory.live_bindings.get(&key) {
            if existing != segment || !writable {
                return Err(TcpError::new("live segment identity or access changed"));
            }
            return Ok((binding.clone(), false));
        }
        let files = super::super::nvmeof::live_storage::FILES_PER_SUBSYSTEM;
        let name = wal_export_name(
            files,
            segment.database,
            segment.partition,
            segment.epoch,
            segment.file_index,
        );
        let file = live_file(segment);
        let export_span = export_span(segment);
        let binding = inventory
            .released(|| target.export_permanent(&name, file))
            .map_err(TcpError::component)?;
        export_span.end_at(segment.file_index);
        inventory
            .live_bindings
            .insert(key, (segment.clone(), binding.clone(), true));
        Ok((binding, true))
    }

    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    fn revoke_live_targets<H: InventoryAccess>(
        inventory: &mut H,
        target: &mut LiveTarget,
    ) -> Result<(), TcpError> {
        let claimed = std::mem::take(&mut inventory.live_bindings);
        let idle = std::mem::take(&mut inventory.idle_history_exports);
        let result = inventory.released(|| revoke_bindings(target, claimed.values()));
        if result.is_err() {
            inventory.live_bindings = claimed;
            inventory.idle_history_exports = idle;
        }
        result
    }
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    fn revoke_live_files<H: InventoryAccess>(
        inventory: &mut H,
        target: &mut LiveTarget,
        epoch: u64,
        files: &[u64],
    ) -> Result<(), TcpError> {
        let bytes = inventory.config.partition.wal_file_bytes;
        let retired = |(binding_epoch, file): &(u64, u64)| {
            *binding_epoch == epoch && files.contains(&(file * bytes))
        };
        let keys: Vec<_> = inventory
            .live_bindings
            .keys()
            .copied()
            .filter(&retired)
            .collect();
        let claimed: Vec<_> = keys
            .iter()
            .filter_map(|key| inventory.live_bindings.remove_entry(key))
            .collect();
        let result = inventory.released(|| revoke_bindings(target, claimed.iter().map(|(_, b)| b)));
        if result.is_err() {
            inventory.live_bindings.extend(claimed);
            return result;
        }
        inventory.idle_history_exports.retain(|key| !retired(key));
        Ok(())
    }

    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    pub(super) fn acquire_live_export_pin(
        &mut self,
        epoch: u64,
        segment: &super::super::nvmeof::live_storage::LiveWalSegment,
        binding: &super::super::nvmeof::live_target::LiveTargetBinding,
    ) -> Result<[u8; 16], TcpError> {
        self.active(epoch)?;
        let key = (segment.epoch, segment.file_index);
        if !self
            .live_bindings
            .get(&key)
            .is_some_and(|(current, exported, _)| {
                current == segment && exported == binding && !exported.permanent
            })
        {
            return Err(TcpError::new("export pin binding identity mismatch"));
        }
        if self.live_export_pins.len() >= 1024 {
            return Err(TcpError::new("live export pin capacity reached"));
        }
        let token = (0..16)
            .find_map(|_| {
                let token = rand::random::<[u8; 16]>();
                (token != [0; 16]
                    && !self.live_export_pins.contains_key(&token)
                    && !self.fenced_export_pins.contains_key(&token))
                .then_some(token)
            })
            .ok_or_else(|| TcpError::new("live export pin token allocation failed"))?;
        self.live_export_pins.insert(token, (epoch, key));
        // A connected history export cannot be an idle-owner eviction victim.
        self.idle_history_exports.retain(|file| *file != key);
        Ok(token)
    }

    /// The caller holds the lifecycle gate; `target` is its content.
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    pub(super) fn release_live_export_pin<H: InventoryAccess>(
        inventory: &mut H,
        target: Option<&mut LiveTarget>,
        epoch: u64,
        token: [u8; 16],
    ) -> Result<(), TcpError> {
        let Some(&(authority, key)) = inventory.live_export_pins.get(&token) else {
            return if inventory.fenced_export_pins.get(&token) == Some(&epoch) {
                Ok(())
            } else {
                Err(TcpError::new("unknown live export pin"))
            };
        };
        if authority != epoch {
            return Err(TcpError::new(
                "export pin belongs to another authority epoch",
            ));
        }
        Self::release_live_bindings(inventory, target, [key], Some(token))?;
        inventory.live_export_pins.remove(&token);
        Ok(())
    }

    /// The caller holds the lifecycle gate; `target` is its content.
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    pub(super) fn release_live_pin<H: InventoryAccess>(
        inventory: &mut H,
        target: Option<&mut LiveTarget>,
        epoch: u64,
        token: [u8; 16],
    ) -> Result<(), TcpError> {
        let Some((owner, _)) = inventory.live_pins.get(&token) else {
            return Ok(());
        };
        if *owner != epoch {
            return Err(TcpError::new("live pin belongs to another authority epoch"));
        }
        let (authority, leases) = inventory
            .live_pins
            .remove(&token)
            .ok_or_else(|| TcpError::new("live pin disappeared"))?;
        let result = Self::release_live_bindings(
            inventory,
            target,
            leases
                .iter()
                .map(|lease| (lease.segment.epoch, lease.segment.file_index)),
            None,
        );
        if result.is_err() {
            // A revoke can fail after a partial target change. Restore the
            // record leases, keeping their inode/read pins until a fence.
            inventory.live_pins.insert(token, (authority, leases));
        }
        result
    }

    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    fn release_live_bindings<H: InventoryAccess>(
        inventory: &mut H,
        mut target: Option<&mut LiveTarget>,
        keys: impl IntoIterator<Item = (u64, u64)>,
        export_token: Option<[u8; 16]>,
    ) -> Result<(), TcpError> {
        let mut revoke = std::collections::BTreeSet::new();
        let mut retain = std::collections::BTreeSet::new();
        {
            let inventory: &NodeInventory = inventory;
            for key in keys {
                let releasable = match inventory.live_bindings.get(&key) {
                    Some((_, binding, writable)) if binding.permanent || !writable => false,
                    Some(_) => inventory
                        .logs
                        .get(&key.0)
                        .ok_or_else(|| TcpError::new("live segment stream absent"))?
                        .can_release_live_segment(key.1)
                        .map_err(TcpError::component)?,
                    None => false,
                };
                if !inventory.live_pins.values().any(|(_, leases)| {
                    leases
                        .iter()
                        .any(|lease| (lease.segment.epoch, lease.segment.file_index) == key)
                }) && !inventory
                    .live_export_pins
                    .iter()
                    .any(|(other, (_, file))| Some(*other) != export_token && *file == key)
                {
                    if releasable {
                        revoke.insert(key);
                    } else if inventory
                        .live_bindings
                        .get(&key)
                        .is_some_and(|(_, binding, writable)| !binding.permanent && !writable)
                    {
                        retain.insert(key);
                    }
                }
            }
        }
        for key in revoke {
            let target = target
                .as_deref_mut()
                .ok_or_else(|| TcpError::new("live target disappeared"))?;
            // Claim before releasing the inventory: no pin can attach to a
            // binding whose namespace is being revoked.
            let entry = inventory
                .live_bindings
                .remove(&key)
                .ok_or_else(|| TcpError::new("live binding disappeared"))?;
            let revoke_span = revoke_span(&entry.0);
            let result = inventory.released(|| target.revoke(&entry.1));
            revoke_span.end_at(entry.0.file_index);
            if let Err(error) = result {
                inventory.live_bindings.insert(key, entry);
                inventory.healthy = false;
                return Err(TcpError::component(error));
            }
        }
        for key in retain {
            Self::retain_idle_history_export(inventory, target.as_deref_mut(), key)?;
        }
        Ok(())
    }

    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    fn retain_idle_history_export<H: InventoryAccess>(
        inventory: &mut H,
        target: Option<&mut LiveTarget>,
        key: (u64, u64),
    ) -> Result<(), TcpError> {
        inventory.idle_history_exports.retain(|idle| *idle != key);
        if inventory.idle_history_exports.len() == MAX_IDLE_HISTORY_EXPORTS {
            let oldest = *inventory
                .idle_history_exports
                .front()
                .ok_or_else(|| TcpError::new("idle history queue disappeared"))?;
            let target = target.ok_or_else(|| TcpError::new("live target disappeared"))?;
            let entry = inventory
                .live_bindings
                .remove(&oldest)
                .ok_or_else(|| TcpError::new("idle history binding disappeared"))?;
            inventory.idle_history_exports.pop_front();
            let revoke_span = revoke_span(&entry.0);
            if let Err(error) = inventory.released(|| target.revoke(&entry.1)) {
                inventory.live_bindings.insert(oldest, entry);
                inventory.idle_history_exports.push_front(oldest);
                inventory.healthy = false;
                return Err(TcpError::component(error));
            }
            revoke_span.end_at(entry.0.file_index);
        }
        inventory.idle_history_exports.push_back(key);
        Ok(())
    }
    /// Seal an exclusively owned inventory. A live inventory needs its target
    /// registry: use `seal_epoch` with the lifecycle gate instead.
    pub fn seal(&mut self, epoch: u64) -> Result<(), TcpError> {
        let mut inventory = self;
        Self::seal_epoch(&mut inventory, &mut LifecycleSlot::default(), epoch, |_| {
            Ok(())
        })
    }

    /// Seal `epoch`. Same gate contract as `fence`: revocation, drain, the
    /// log seal and the manifest install all run with the inventory released.
    pub(super) fn seal_epoch<H: InventoryAccess>(
        inventory: &mut H,
        slot: &mut LifecycleSlot,
        epoch: u64,
        drain_log: impl FnOnce(&Arc<PartitionLog>) -> Result<(), TcpError>,
    ) -> Result<(), TcpError> {
        inventory.ensure_healthy()?;
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        if !inventory.live_pins.is_empty() || !inventory.live_export_pins.is_empty() {
            return Err(TcpError::new("live NVMe-oF pins must drain before sealing"));
        }
        if inventory.manifest.epochs.iter().any(|e| {
            e.epoch == epoch && matches!(e.state, EpochState::Sealed | EpochState::Abandoned)
        }) {
            return Ok(());
        }
        let log = inventory.active(epoch)?;
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        let target = live_target(inventory, slot)?;
        #[cfg(not(all(feature = "experimental-nvmeof", target_os = "linux")))]
        let _ = slot;
        inventory.healthy = false;
        inventory.readers.write().invalidate();
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        if let Some(target) = target {
            Self::revoke_live_targets(inventory, target)?;
            inventory.released(|| {
                drain_log(&log)?;
                log.seal_live_epoch(RequestAuthority { epoch }, || Ok(()))
                    .map_err(TcpError::component)
            })?;
        } else {
            inventory
                .released(|| log.seal_epoch(RequestAuthority { epoch }))
                .map_err(TcpError::component)?;
        }
        #[cfg(not(all(feature = "experimental-nvmeof", target_os = "linux")))]
        {
            let _ = &drain_log;
            inventory
                .released(|| log.seal_epoch(RequestAuthority { epoch }))
                .map_err(TcpError::component)?;
        }
        drop(log);
        inventory.manifest.epochs.last_mut().unwrap().state = EpochState::Sealed;
        Self::persist_manifest(inventory)?;
        inventory.healthy = true;
        Ok(())
    }
    /// The caller holds the lifecycle gate; `target` is its content.
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    pub(super) fn shutdown_live_target<H: InventoryAccess>(
        inventory: &mut H,
        target: &mut LiveTarget,
        epoch: u64,
    ) -> Result<super::super::nvmeof::LiveTargetStats, TcpError> {
        inventory.ensure_healthy()?;
        if !inventory.live_pins.is_empty() {
            return Err(TcpError::new(
                "live NVMe-oF pins must drain before shutdown",
            ));
        }
        if !inventory
            .manifest
            .epochs
            .last()
            .is_some_and(|entry| entry.epoch == epoch && entry.state == EpochState::Sealed)
        {
            return Err(TcpError::new(
                "live target shutdown requires latest sealed authority",
            ));
        }
        Self::finish_target_shutdown(inventory, target)
    }

    /// The caller holds the lifecycle gate; `slot` is its content.
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    pub(super) fn finish_live_target_shutdown<H: InventoryAccess>(
        inventory: &mut H,
        slot: &mut LifecycleSlot,
    ) -> Result<super::super::nvmeof::LiveTargetStats, TcpError> {
        inventory.ensure_healthy()?;
        if !inventory.live_pins.is_empty() {
            return Err(TcpError::new(
                "live NVMe-oF pins must drain before shutdown",
            ));
        }
        if let Some(epoch) = inventory
            .manifest
            .epochs
            .last()
            .filter(|entry| entry.state == EpochState::Active)
            .map(|entry| entry.epoch)
        {
            Self::seal_epoch(inventory, slot, epoch, |_| Ok(()))?;
        }
        let target = slot
            .as_mut()
            .ok_or_else(|| TcpError::new("live target not configured"))?;
        Self::finish_target_shutdown(inventory, target)
    }

    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    pub(super) fn wal_prewrite_stats(&self) -> (u64, u64, u64) {
        let mut bytes = self.retired_prewrite_bytes;
        let mut ns = self.retired_prewrite_ns;
        let mut files = self.retired_prewrite_files;
        for log in self.logs.values() {
            let preparation = log.live_file_preparation_stats();
            bytes = bytes.saturating_add(preparation.live_file_prewrite_bytes);
            ns = ns.saturating_add(preparation.live_file_prewrite_ns);
            files = files.saturating_add(preparation.live_file_prewrite_files);
        }
        (bytes, ns, files)
    }

    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    fn finish_target_shutdown<H: InventoryAccess>(
        inventory: &mut H,
        target: &mut LiveTarget,
    ) -> Result<super::super::nvmeof::LiveTargetStats, TcpError> {
        if !inventory.live_shutdown
            && let Err(error) = inventory.released(|| target.shutdown())
        {
            inventory.healthy = false;
            return Err(TcpError::component(error));
        }
        inventory.live_shutdown = true;
        let stats = match inventory.released(|| target.stats()) {
            Ok(stats) => stats,
            Err(error) => {
                inventory.healthy = false;
                return Err(TcpError::component(error));
            }
        };
        if stats.owned_namespace_count != 0 {
            inventory.healthy = false;
            return Err(TcpError::new("live target shutdown left owned namespaces"));
        }
        inventory.live_bindings.clear();
        inventory.idle_history_exports.clear();
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

    /// The caller holds the lifecycle gate (`slot` is its content) or owns the
    /// inventory exclusively.
    pub(super) fn retire_files<H: InventoryAccess>(
        inventory: &mut H,
        slot: &mut LifecycleSlot,
        authority_epoch: u64,
        epoch: u64,
        prefix: usize,
        files: &[u64],
    ) -> Result<(), TcpError> {
        inventory.require_history(false)?;
        Self::retire_files_inner(inventory, slot, authority_epoch, epoch, Some(prefix), files)
    }

    /// Same gate contract as `retire_files`.
    pub(super) fn retire_wal_files<H: InventoryAccess>(
        inventory: &mut H,
        slot: &mut LifecycleSlot,
        authority_epoch: u64,
        epoch: u64,
        files: &[u64],
    ) -> Result<(), TcpError> {
        inventory.require_history(true)?;
        Self::retire_files_inner(inventory, slot, authority_epoch, epoch, None, files)
    }

    fn retire_files_inner<H: InventoryAccess>(
        inventory: &mut H,
        slot: &mut LifecycleSlot,
        authority_epoch: u64,
        epoch: u64,
        prefix: Option<usize>,
        files: &[u64],
    ) -> Result<(), TcpError> {
        if files.len() > 256 {
            return Err(TcpError::new("retirement file count exceeds bound"));
        }
        let log = inventory.retirement_log(authority_epoch, epoch)?;
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        if !inventory.live_pins.is_empty() {
            return Err(TcpError::new(
                "live NVMe-oF pins must drain before retirement",
            ));
        }
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        if inventory
            .live_export_pins
            .values()
            .any(|(_, (source, file))| {
                *source == epoch
                    && files.contains(&(file * inventory.config.partition.wal_file_bytes))
            })
        {
            return Err(TcpError::new(
                "live connection pins must drain before retirement",
            ));
        }
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        let mut target = live_target(inventory, slot)?;
        #[cfg(not(all(feature = "experimental-nvmeof", target_os = "linux")))]
        let _ = slot;
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        let retired_identities = if let Some(target) = target.as_deref_mut() {
            if let Err(error) = Self::revoke_live_files(inventory, target, epoch, files) {
                inventory.healthy = false;
                return Err(error);
            }
            let root = epoch_path(&inventory.config.root, epoch);
            let bytes = inventory.config.partition.wal_file_bytes;
            inventory.released(|| {
                let mut identities = Vec::new();
                for offset in files {
                    let path = root.join(format!("wal_{:016x}", offset / bytes));
                    if let Some(identity) =
                        target.sealed_file(&path).map_err(TcpError::component)?
                    {
                        identities.push(identity);
                    }
                }
                Ok::<_, TcpError>(identities)
            })?
        } else {
            Vec::new()
        };
        inventory.released(|| {
            match prefix {
                Some(prefix) => log.retire_files(prefix, files),
                None => log.retire_wal_files(files),
            }
            .map_err(TcpError::component)?;
            // Each tombstone release syncs the WAL directory before its marker
            // removal, so a crash cannot resurrect an untracked old file.
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            if let Some(target) = target {
                for identity in &retired_identities {
                    target
                        .retire_deleted_file(identity)
                        .map_err(TcpError::component)?;
                }
            }
            Ok(())
        })
    }

    /// Validate under the inventory, then persist and digest the prefix with
    /// it released. The caller holds the lifecycle gate or owns the inventory.
    pub(super) fn checkpoint_proof<H: InventoryAccess>(
        inventory: &mut H,
        authority_epoch: u64,
        epoch: u64,
        prefix: usize,
    ) -> Result<checkpoint::CheckpointProof, TcpError> {
        inventory.require_history(false)?;
        inventory.active(authority_epoch)?;
        let record = inventory
            .manifest
            .epochs
            .iter()
            .find(|entry| entry.epoch == epoch)
            .ok_or_else(|| TcpError::new("checkpoint epoch absent"))?;
        let log = match record.state {
            EpochState::Active | EpochState::Sealed => Some(
                inventory
                    .logs
                    .get(&epoch)
                    .cloned()
                    .ok_or_else(|| TcpError::new("checkpoint stream absent"))?,
            ),
            EpochState::Abandoned => None,
            EpochState::Preparing => return Err(TcpError::new("checkpoint epoch incomplete")),
        };
        let database = inventory.config.partition.inventory.database;
        let partition = inventory.config.partition.partition;
        inventory.released(|| {
            let retained = log.as_ref().map_or(0, |log| log.receipt_count());
            if prefix > retained {
                return Err(TcpError::new("checkpoint prefix beyond retained history"));
            }
            if prefix != 0 {
                let log = log
                    .as_ref()
                    .ok_or_else(|| TcpError::new("checkpoint stream absent"))?;
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
            Ok(checkpoint::CheckpointProof {
                database,
                partition,
                epoch,
                prefix,
                retained,
                digest: match &log {
                    Some(log) => log.checkpoint_prefix(prefix).map_err(TcpError::component)?,
                    None => {
                        checkpoint::prefix_digest(database, partition, epoch, std::iter::empty())
                    }
                },
            })
        })
    }

    /// Validate under the inventory, then read and persist with it released.
    /// The caller holds the lifecycle gate or owns the inventory.
    pub(super) fn recovery_page<H: InventoryAccess>(
        inventory: &mut H,
        epoch: u64,
        cursor: usize,
        limit: usize,
        limits: TcpLimits,
    ) -> Result<Response, TcpError> {
        inventory.require_history(false)?;
        inventory.ensure_healthy()?;
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        if inventory.live_target_configured {
            return Err(TcpError::new(
                "live NVMe-oF recovery requires metadata-only control",
            ));
        }
        if limit == 0 || limit > limits.recovery_page_batches {
            return Err(TcpError::new("recovery page count exceeds bound"));
        }
        let record = inventory
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
        let log = inventory.logs[&epoch].clone();
        inventory.released(|| Self::read_recovery_page(&log, cursor, limit, limits))
    }

    fn read_recovery_page(
        log: &PartitionLog,
        cursor: usize,
        limit: usize,
        limits: TcpLimits,
    ) -> Result<Response, TcpError> {
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

    /// Owner incarnation created when this owner opened.
    pub(super) fn incarnation(&self) -> [u8; 16] {
        self.incarnation
    }

    /// Log of stream `epoch` for a durable-progress poll under `authority_epoch`.
    /// `None`: the epoch exists but holds no log handle, an empty stream.
    pub(super) fn durable_progress_log(
        &self,
        authority_epoch: u64,
        epoch: u64,
    ) -> Result<Option<Arc<super::super::storage::PartitionLog>>, TcpError> {
        self.active(authority_epoch)?;
        let record = self
            .manifest
            .epochs
            .iter()
            .find(|entry| entry.epoch == epoch)
            .ok_or_else(|| TcpError::new("durable progress epoch absent"))?;
        match record.state {
            EpochState::Active | EpochState::Sealed => Ok(self.logs.get(&epoch).cloned()),
            EpochState::Abandoned => Err(TcpError::new("durable progress epoch abandoned")),
            EpochState::Preparing => Err(TcpError::new("durable progress epoch incomplete")),
        }
    }

    /// Validate under the inventory, then replay with it released. The caller
    /// holds the lifecycle gate or owns the inventory.
    pub(super) fn replay_wal_page<H: InventoryAccess>(
        inventory: &mut H,
        authority_epoch: u64,
        epoch: u64,
        from: u64,
        limit: usize,
        require_end: u64,
        limits: TcpLimits,
    ) -> Result<Response, TcpError> {
        inventory.require_history(true)?;
        inventory.active(authority_epoch)?;
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        if inventory.live_target_configured {
            return Err(TcpError::new(
                "live NVMe-oF recovery requires metadata-only control",
            ));
        }
        if limit == 0 || limit > limits.recovery_page_batches {
            return Err(TcpError::new("recovery page count exceeds bound"));
        }
        let record = inventory
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
        let log = inventory
            .logs
            .get(&epoch)
            .cloned()
            .ok_or_else(|| TcpError::new("recovery stream absent"))?;
        inventory.released(|| Self::read_replay_page(&log, from, limit, require_end, limits))
    }

    fn read_replay_page(
        log: &PartitionLog,
        from: u64,
        limit: usize,
        require_end: u64,
        limits: TcpLimits,
    ) -> Result<Response, TcpError> {
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

/// Fail before any mutation if this owner root holds state of a removed mode:
/// admissions of the sealed-export registry, or a live target marker written
/// before the removal of the `spdk` and `export_mode` keys. An empty sealed
/// registry holds nothing and is ignored. No compatibility path exists.
fn reject_legacy_owner_state(root: &Path) -> Result<(), TcpError> {
    #[derive(Deserialize)]
    struct ExportLeases {
        #[serde(default)]
        entries: Vec<serde_yaml::Value>,
        #[serde(default)]
        epoch_pins: Vec<serde_yaml::Value>,
    }
    #[derive(Deserialize)]
    struct Marker {
        config: serde_yaml::Mapping,
    }
    let legacy = |mode: &str| {
        TcpError::new(format!(
            "unsupported legacy distributed mode {mode}: use archive/multi-design"
        ))
    };
    let present = |path: &Path| match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(TcpError::from(error)),
    };
    let leases = root.join(LEGACY_EXPORT_LEASES);
    if present(&leases)? {
        let leases: ExportLeases = read_metadata(&leases).map_err(TcpError::component)?;
        if !leases.entries.is_empty() || !leases.epoch_pins.is_empty() {
            return Err(legacy("sealed_export"));
        }
    }
    let marker = root.join(LIVE_TARGET_CONFIG);
    if present(&marker)? {
        let marker: Marker = read_metadata(&marker).map_err(TcpError::component)?;
        if marker
            .config
            .get("spdk")
            .is_some_and(|spdk| !spdk.is_null())
        {
            return Err(legacy("spdk"));
        }
        match marker
            .config
            .get("export_mode")
            .map(serde_yaml::Value::as_str)
        {
            Some(Some(mode @ ("on_demand" | "per_file"))) => return Err(legacy(mode)),
            Some(_) => return Err(legacy("pre_refactor_live_target_marker")),
            None if marker.config.contains_key("spdk") => {
                return Err(legacy("pre_refactor_live_target_marker"));
            }
            None => {}
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed::tcp::tests::{config, metrics};

    fn proof(
        inventory: &mut NodeInventory,
        authority_epoch: u64,
        epoch: u64,
        prefix: usize,
    ) -> Result<checkpoint::CheckpointProof, TcpError> {
        NodeInventory::checkpoint_proof(&mut &mut *inventory, authority_epoch, epoch, prefix)
    }

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

    /// Write `value` as `file` in a closed owner root and check that open
    /// fails with legacy `mode` before any mutation.
    fn assert_legacy_open_rejected(file: &str, value: serde_yaml::Value, mode: &str) {
        let temporary = tempdir::TempDir::new("node-legacy-state").unwrap();
        let config = config(&temporary.path().join("node"));
        let mut inventory = NodeInventory::open(config.clone(), metrics()).unwrap();
        inventory.fence_to(1).unwrap();
        drop(inventory);
        install_metadata(&config.root, file, &encode_metadata(&value).unwrap()).unwrap();
        let listing = || {
            let mut names: Vec<_> = fs::read_dir(&config.root)
                .unwrap()
                .map(|entry| entry.unwrap().file_name())
                .collect();
            names.sort();
            names
        };
        let (before, manifest) = (listing(), fs::read(config.root.join(MANIFEST)).unwrap());
        let error = match NodeInventory::open(config.clone(), metrics()) {
            Ok(_) => panic!("legacy owner state {mode} opened"),
            Err(error) => error.to_string(),
        };
        assert!(
            error.contains(&format!(
                "unsupported legacy distributed mode {mode}: use archive/multi-design"
            )),
            "{error}"
        );
        assert_eq!(listing(), before);
        assert_eq!(fs::read(config.root.join(MANIFEST)).unwrap(), manifest);
    }

    fn legacy_marker(spdk: serde_yaml::Value, export_mode: Option<&str>) -> serde_yaml::Value {
        let mut config = serde_yaml::Mapping::new();
        config.insert("address".into(), "127.0.0.1".into());
        config.insert("spdk".into(), spdk);
        if let Some(mode) = export_mode {
            config.insert("export_mode".into(), mode.into());
        }
        let mut marker = serde_yaml::Mapping::new();
        marker.insert("config".into(), config.into());
        marker.insert("identity".into(), serde_yaml::Mapping::new().into());
        marker.into()
    }

    #[test]
    fn sealed_export_admission_fails_open_before_mutation() {
        let entry: serde_yaml::Value = serde_yaml::from_str("{owner_epoch: 1}").unwrap();
        for (entries, pins) in [(vec![entry.clone()], vec![]), (vec![], vec![entry])] {
            let mut registry = serde_yaml::Mapping::new();
            registry.insert("version".into(), 1.into());
            registry.insert("entries".into(), entries.into());
            registry.insert("epoch_pins".into(), pins.into());
            assert_legacy_open_rejected(LEGACY_EXPORT_LEASES, registry.into(), "sealed_export");
        }
    }

    #[test]
    fn empty_sealed_export_registry_is_ignored() {
        let temporary = tempdir::TempDir::new("node-empty-registry").unwrap();
        let config = config(&temporary.path().join("node"));
        drop(NodeInventory::open(config.clone(), metrics()).unwrap());
        let mut registry = serde_yaml::Mapping::new();
        registry.insert("version".into(), 1.into());
        registry.insert("entries".into(), serde_yaml::Sequence::new().into());
        install_metadata(
            &config.root,
            LEGACY_EXPORT_LEASES,
            &encode_metadata(&registry).unwrap(),
        )
        .unwrap();
        assert!(NodeInventory::open(config, metrics()).is_ok());
    }

    #[test]
    fn spdk_live_target_marker_fails_open_before_mutation() {
        let spdk: serde_yaml::Value =
            serde_yaml::from_str("{rpc_socket: /run/spdk.sock, executable: /usr/bin/spdk}")
                .unwrap();
        assert_legacy_open_rejected(
            LIVE_TARGET_CONFIG,
            legacy_marker(spdk, Some("on_demand")),
            "spdk",
        );
    }

    #[test]
    fn on_demand_and_per_file_live_target_markers_fail_open_before_mutation() {
        for mode in ["on_demand", "per_file"] {
            assert_legacy_open_rejected(
                LIVE_TARGET_CONFIG,
                legacy_marker(serde_yaml::Value::Null, Some(mode)),
                mode,
            );
        }
    }

    #[test]
    fn pre_refactor_per_owner_live_target_marker_fails_open_before_mutation() {
        for export_mode in [Some("per_owner"), None] {
            assert_legacy_open_rejected(
                LIVE_TARGET_CONFIG,
                legacy_marker(serde_yaml::Value::Null, export_mode),
                "pre_refactor_live_target_marker",
            );
        }
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
            matches!(NodeInventory::recovery_page(&mut &mut inventory,2,0,32,TcpLimits::default()).unwrap(),Response::Recovery{batches,next:None} if batches.is_empty())
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
        let prefix = proof(&mut inventory, 1, 1, 1).unwrap();
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
            proof(&mut inventory, 1, 1, 2).unwrap().digest
        );
        assert!(proof(&mut inventory, 1, 1, 3).is_err());
        assert!(proof(&mut inventory, 1, 99, 0).is_err());
        inventory.fence_to(2).unwrap();
        assert!(proof(&mut inventory, 1, 1, 1).is_err());
        assert_eq!(proof(&mut inventory, 2, 1, 1).unwrap(), prefix);
        drop(inventory);
        let mut inventory = NodeInventory::open(config, metrics()).unwrap();
        inventory.fence_to(3).unwrap();
        assert_eq!(proof(&mut inventory, 3, 1, 1).unwrap(), prefix);
    }
}
