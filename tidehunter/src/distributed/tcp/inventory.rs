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

mod capacity;
mod file_maintenance;
pub(super) mod lease;
pub(super) mod r1_events;
mod ready_binding;
mod relocation;
mod retirement;

pub use file_maintenance::FileMaintenanceStats;

pub(super) const MANIFEST: &str = "tcp-node-inventory-v1";
pub(super) const LIVE_TARGET_CONFIG: &str = "tcp-node-live-target-v1";
/// Registry of the removed sealed-export route; see `reject_legacy_owner_state`.
const LEGACY_EXPORT_LEASES: &str = "nvmeof-export-leases-v1";

#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum OpenMode {
    /// Provisioning (or a direct test fixture) can initialize the inventory.
    Prepare,
    /// A Ready host entry requires the surviving ledger and target identity.
    Ready,
}
/// The owner request that created a live export inline, while its caller
/// waited; selects the `LiveInlineExportStats` field it counts in.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum InlineExport {
    Lease,
    Rollover,
    Prelease,
    Resolve,
    Replay,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
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
/// Owner fencing ledger (format `generations::FORMAT`). Retained entries can
/// be trimmed; `highest_fenced_generation` never decreases and is the only
/// source of the stale-fence check.
#[derive(Clone, Serialize, Deserialize)]
struct Manifest {
    format: u32,
    partition: PartitionConfig,
    shape_yaml: String,
    highest_fenced_generation: u64,
    /// The one generation whose directory deletion is durably intended.
    trim_pending: Option<u64>,
    epochs: Vec<Epoch>,
}
mod generations;
#[derive(Serialize, Deserialize)]
struct LiveTargetMarker {
    config: super::super::nvmeof::LiveTargetConfig,
    identity: super::super::nvmeof::live_target::LiveTargetIdentity,
}

/// Database drop: the persisted target marker of one database root, or
/// `None` when it does not exist. A non-regular file is corruption.
pub(super) fn read_live_target_marker(
    root: &Path,
) -> Result<
    Option<(
        super::super::nvmeof::LiveTargetConfig,
        super::super::nvmeof::live_target::LiveTargetIdentity,
    )>,
    TcpError,
> {
    let marker = root.join(LIVE_TARGET_CONFIG);
    match fs::symlink_metadata(&marker) {
        Ok(metadata) if metadata.file_type().is_file() => {
            let saved: LiveTargetMarker = read_metadata(&marker).map_err(|error| {
                TcpError::with_kind(
                    io::ErrorKind::InvalidData,
                    format!("live target marker {}: {error}", marker.display()),
                )
            })?;
            Ok(Some((saved.config, saved.identity)))
        }
        Ok(_) => Err(TcpError::with_kind(
            io::ErrorKind::InvalidData,
            "live target configuration is not a regular file",
        )),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

#[cfg(any(test, feature = "test-utils"))]
std::thread_local! {
    static LIVE_FIXTURE: std::cell::RefCell<Option<PathBuf>> = const { std::cell::RefCell::new(None) };
}

/// Select only configfs emulation; `open` still commissions and validates the
/// normal durable marker while holding the node lock, before recovering WALs.
#[cfg(any(test, feature = "test-utils"))]
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

#[cfg(any(test, feature = "test-utils"))]
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

/// What the lifecycle gate owns: the live target registry when one is configured.
pub(super) type LifecycleSlot = Option<super::super::nvmeof::live_target::LiveTargetManager>;

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

type LiveTarget = super::super::nvmeof::live_target::LiveTargetManager;
type LiveBinding = (
    super::super::nvmeof::live_storage::LiveWalSegment,
    super::super::nvmeof::live_storage::LiveTargetBinding,
    bool,
);

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
    // Owner incarnation, created once at open (see `durable_progress::new_incarnation`).
    incarnation: [u8; 16],
    /// Ledger revision of this incarnation, raised per `persist_manifest`; pages freeze at one.
    revision: u64,
    #[cfg(test)]
    trim_crash: Option<super::generation_control::OwnerTrimCrash>,
    #[cfg(test)]
    trim_detach_pause: Option<generations::TrimPause>,
    #[cfg(test)]
    trim_delete_pause: Option<generations::TrimPause>,
    /// Value WAL files passed to a successful retirement unlink step by this
    /// process. Read by the R0 inventory page; R0 asserts it does not move.
    value_wal_unlinks: u64,
    /// Changed only under this mutex; see `FileMaintenanceStats`.
    pub(super) file_maintenance: FileMaintenanceStats,
    /// Ordered R1 evidence stream of this owner incarnation, when enabled.
    pub(super) r1_events: Option<r1_events::OwnerR1Events>,
    /// Whether a live target is installed. Immutable after installation, so
    /// reading it needs no gate.
    pub(super) live_target_configured: bool,
    /// Cloned out at node open. Never locked by inventory methods: callers
    /// take it before the inventory (see `LifecycleGate`).
    pub(super) lifecycle: LifecycleGate,
    live_shutdown: bool,
    // Keep preparation costs when fencing replaces an active log with recovery.
    retired_prewrite_bytes: u64,
    retired_prewrite_ns: u64,
    retired_prewrite_files: u64,
    // Changed only under this inventory's lock, when an export's binding is
    // installed.
    pub(super) inline_exports: LiveInlineExportStats,
    live_bindings: BTreeMap<
        (u64, u64),
        (
            super::super::nvmeof::live_storage::LiveWalSegment,
            super::super::nvmeof::live_storage::LiveTargetBinding,
            bool,
        ),
    >,
    /// Test-only `(epoch, start)` victims whose retirement revoke step reports
    /// nothing revoked, as if their binding were already gone. The revoke
    /// itself runs: a live file without a binding breaks `OpenLiveExports`.
    #[cfg(test)]
    retirement_revoke_report_skips: Vec<(u64, u64)>,
    pub(super) live_pins: BTreeMap<
        [u8; 16],
        (
            u64,
            Vec<super::super::nvmeof::live_storage::LiveRecordLease>,
        ),
    >,
    lock: Arc<DbLock>,
}

#[derive(Default)]
pub(super) struct ReaderAuthority {
    active_epoch: Option<u64>,
    logs: BTreeMap<u64, Arc<PartitionLog>>,
    partition: u32,
}

pub(super) struct ReaderSnapshot {
    pub(super) members: Vec<(Arc<PartitionLog>, PhysicalValueAddress)>,
}

impl ReaderAuthority {
    pub(super) fn invalidate(&mut self) {
        self.active_epoch = None;
    }

    fn publish(&mut self, epoch: u64, logs: &BTreeMap<u64, Arc<PartitionLog>>) {
        self.logs.clone_from(logs);
        self.active_epoch = Some(epoch);
    }
    /// Resolve member addresses to the logs of their serving generations. The
    /// key is the address alone (no batch epoch: a relocated position keeps its
    /// logical epoch but names the generation holding its bytes); a foreign
    /// partition is `InvalidData`, a generation no longer held is `NotFound`
    /// (the kind the value path retries once).
    pub(super) fn snapshot(
        &self,
        epoch: u64,
        members: &[PhysicalValueAddress],
        terminal: &AtomicBool,
    ) -> Result<ReaderSnapshot, TcpError> {
        if terminal.load(Ordering::Acquire) {
            return Err(TcpError::new("node requires restart and sealed recovery"));
        }
        if self.active_epoch != Some(epoch) {
            return Err(TcpError::new("stale or sealed request authority"));
        }
        let mut resolved = Vec::with_capacity(members.len());
        for address in members {
            if address.partition != self.partition {
                let kind = io::ErrorKind::InvalidData;
                return Err(TcpError::with_kind(kind, "foreign member partition"));
            }
            let Some(log) = self.logs.get(&address.segment_generation) else {
                let kind = io::ErrorKind::NotFound;
                return Err(TcpError::with_kind(kind, "retired member generation"));
            };
            resolved.push((Arc::clone(log), *address));
        }
        Ok(ReaderSnapshot { members: resolved })
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
    pub(in crate::distributed::tcp) fn persisted_high_water(
        config: &NodeConfig,
    ) -> Result<u64, TcpError> {
        let manifest = generations::read_manifest(&config.root.join(MANIFEST))?;
        let shape = config
            .validated_shape()?
            .to_yaml()
            .map_err(TcpError::component)?;
        generations::validate_manifest(&manifest, &config.partition, &shape)?;
        Ok(manifest.highest_fenced_generation)
    }

    #[cfg(test)]
    pub fn open(config: NodeConfig, metrics: Arc<Metrics>) -> Result<Self, TcpError> {
        Self::open_with_mode(config, metrics, OpenMode::Prepare)
    }

    pub(super) fn open_with_mode(
        mut config: NodeConfig,
        metrics: Arc<Metrics>,
        mode: OpenMode,
    ) -> Result<Self, TcpError> {
        let shape = config.validated_shape()?;
        config.shape_yaml = shape.to_yaml().map_err(TcpError::component)?;
        if mode == OpenMode::Prepare {
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
        }
        directory(&config.root)?;
        config.root = config.root.canonicalize()?;
        let lock = Arc::new(DbLock::acquire(&config.root, Duration::ZERO)?);
        reject_legacy_owner_state(&config.root)?;
        let manifest_path = config.root.join(MANIFEST);
        let mut manifest = match fs::symlink_metadata(&manifest_path) {
            Ok(metadata) if metadata.file_type().is_file() => {
                generations::read_manifest(&manifest_path)?
            }
            Ok(_) => return Err(TcpError::new("node manifest is not a regular file")),
            Err(error) if mode == OpenMode::Ready && error.kind() == io::ErrorKind::NotFound => {
                return Err(TcpError::with_kind(
                    io::ErrorKind::InvalidData,
                    format!(
                        "Ready owner inventory ledger is missing: {}",
                        manifest_path.display()
                    ),
                ));
            }
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
                let manifest = generations::new_manifest(&config);
                persist(&config.root, &manifest)?;
                manifest
            }
            Err(error) => return Err(error.into()),
        };
        generations::validate_manifest(&manifest, &config.partition, &config.shape_yaml)?;
        // Hold the node lock before revoking capabilities: another process
        // must never revoke a currently running owner's namespaces.
        let mut live_target = {
            let marker = config.root.join(LIVE_TARGET_CONFIG);
            #[cfg(not(any(test, feature = "test-utils")))]
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
                    #[cfg(any(test, feature = "test-utils"))]
                    let manager = open_live_target_fixture(saved.config, Some(saved.identity))
                        .map_err(TcpError::component)?;
                    #[cfg(not(any(test, feature = "test-utils")))]
                    let manager = LiveTargetManager::open_existing(saved.config, saved.identity)
                        .map_err(TcpError::component)?;
                    Some(manager)
                }
                Ok(_) => {
                    return Err(TcpError::new(
                        "live target configuration is not a regular file",
                    ));
                }
                Err(error)
                    if mode == OpenMode::Ready && error.kind() == io::ErrorKind::NotFound =>
                {
                    return Err(TcpError::with_kind(
                        io::ErrorKind::InvalidData,
                        format!(
                            "Ready owner live target identity marker is missing: {}",
                            marker.display()
                        ),
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
                        // The host slot's target directory is created only after the
                        // owner inventory has reached its first durable image.
                        super::host::prepare_host_target(&config.root, &target_config)?;
                        #[cfg(any(test, feature = "test-utils"))]
                        let manager = open_live_target_fixture(target_config.clone(), None)
                            .map_err(TcpError::component)?;
                        #[cfg(not(any(test, feature = "test-utils")))]
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
        // A live WAL file that is already empty or overlong is corrupt before
        // the target open seals it; refuse it with its named cause so the
        // recovery scan never meets a bare short read.
        for epoch in &manifest.epochs {
            if matches!(epoch.state, EpochState::Active | EpochState::Sealed) {
                let path = epoch_path(&config.root, epoch.epoch);
                if PartitionLog::is_live_path(&path).map_err(TcpError::component)? {
                    let expected = partition_at(&config.partition, epoch.epoch);
                    let synced = PartitionLog::live_synced_end(&path, &expected)
                        .map_err(TcpError::component)?;
                    // A file whose durable inventory witness exists was
                    // preallocated before any byte was granted; an active epoch
                    // must still show that size, whatever the durable end
                    // records. A sealed epoch trims its files.
                    let witnessed = if epoch.state == EpochState::Active {
                        super::super::storage::file_inventory::witnessed_files(&path)
                            .map_err(TcpError::component)?
                    } else {
                        Vec::new()
                    };
                    super::super::nvmeof::live_target::verify_live_epoch_sizes(
                        &path,
                        expected.wal_file_bytes,
                        synced,
                        &witnessed,
                    )
                    .map_err(TcpError::component)?;
                }
            }
        }
        // A durably intended trim names the only directory open accepts without
        // a ledger entry. Finish it before any log is recovered or published.
        generations::resume_trim(&config.root, &mut manifest, live_target.as_mut())?;
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
                    let active = epoch.state == EpochState::Active;
                    let log = if PartitionLog::is_live_path(&path).map_err(TcpError::component)? {
                        let target = live_target.as_mut().ok_or_else(|| {
                            TcpError::new(
                                "live WAL recovery requires its configured target manager",
                            )
                        })?;
                        // Manager::open already synchronously revoked and drained
                        // the persisted targets while the root lock was held. The
                        // marker identity is checked before any WAL read, so a
                        // truncated or replaced backing file names its cause.
                        target
                            .verify_live_epoch(&path, active)
                            .map_err(TcpError::component)?;
                        PartitionLog::recover_live_sealed(&path, &expected, metrics.clone(), || {
                            Ok(())
                        })
                    } else {
                        PartitionLog::recover_sealed(&path, &expected, metrics.clone())
                    };
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
        // Restart always seals the old authority, even if it previously shut
        // down cleanly. It never resumes an old logical counter/writer epoch.
        persist(&config.root, &manifest)?;
        let readers = Arc::new(RwLock::new(ReaderAuthority {
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
            incarnation: super::durable_progress::new_incarnation()?,
            revision: 0,
            #[cfg(test)]
            trim_crash: None,
            #[cfg(test)]
            trim_detach_pause: None,
            #[cfg(test)]
            trim_delete_pause: None,
            value_wal_unlinks: 0,
            file_maintenance: FileMaintenanceStats::default(),
            r1_events: None,
            live_target_configured: live_target.is_some(),
            lifecycle: Arc::new(Mutex::new(live_target)),
            live_shutdown: false,
            retired_prewrite_bytes: 0,
            retired_prewrite_ns: 0,
            retired_prewrite_files: 0,
            inline_exports: LiveInlineExportStats::default(),
            live_bindings: BTreeMap::new(),
            #[cfg(test)]
            retirement_revoke_report_skips: Vec::new(),
            live_pins: BTreeMap::new(),
            lock,
        })
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
    pub(super) fn recoverable_status(&self) -> Result<NodeStatus, TcpError> {
        if !self.healthy || !self.live_target_configured || self.live_shutdown {
            return Err(TcpError::new("live owner requires restart"));
        }
        self.status_snapshot(false)
    }
    fn status_snapshot(&self, healthy: bool) -> Result<NodeStatus, TcpError> {
        Ok(NodeStatus {
            partition: self.config.partition.clone(),
            shape_yaml: self.config.shape_yaml.clone(),
            highest_fenced_generation: self.manifest.highest_fenced_generation,
            inventory_revision: self.revision,
            active_epoch: self
                .manifest
                .epochs
                .last()
                .filter(|e| e.state == EpochState::Active)
                .map(|e| e.epoch),
            healthy,
            max_frame_bytes: HARD_FRAME_LIMIT,
            max_reads: 256,
            recovery_page_batches: 1024,
            polling: PollingPolicy::Adaptive,
            live_nvmeof: self.live_target_configured && !self.live_shutdown,
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
    #[allow(
        clippy::expect_used,
        reason = "an open manifest always retains its newest epoch entry"
    )]
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
        if inventory.live_shutdown {
            return Err(TcpError::new("live target has shut down; reopen required"));
        }
        // Only a retained Active entry at the high-water on a healthy owner makes
        // an equal fence idempotent; the high-water, never the retained list,
        // rejects every other stale or repeated generation.
        let highest = inventory.manifest.highest_fenced_generation;
        if epoch == highest
            && inventory
                .manifest
                .epochs
                .last()
                .is_some_and(|e| e.epoch == epoch && e.state == EpochState::Active)
        {
            return inventory.status();
        }
        if epoch == 0 || epoch <= highest {
            return Err(TcpError::new(format!(
                "fencing generation {epoch} already fenced or sealed (owner high-water {highest})"
            )));
        }
        let target = live_target(inventory, slot)?;
        let live = target.is_some();
        if let Some(target) = target {
            // The new authority fences the prior coordinator before any old
            // allocation is scanned or its namespace-backed pins are dropped.
            inventory.healthy = false;
            {
                let mut readers = inventory.readers.write();
                readers.invalidate();
                readers.logs.clear();
            }
            let sealing = inventory
                .manifest
                .epochs
                .last()
                .filter(|e| e.state == EpochState::Active)
                .map(|e| e.epoch);
            if let Some(sealing) = sealing {
                Self::protect_live_targets(inventory, target, sealing)?;
            }
            let inventory = &mut **inventory;
            let record_pins: Vec<_> = inventory
                .live_pins
                .iter()
                .map(|(token, (_, leases))| (*token, r1_events::lease_files(leases)))
                .collect();
            // Dropping the record leases releases their inode/read pins.
            inventory.live_pins.clear();
            if let Some(events) = inventory.r1_events.as_mut() {
                for (token, files) in record_pins {
                    events.pin("pin_released", "record", token, files, "fence");
                }
            }
        }
        if let Some(old) = inventory
            .manifest
            .epochs
            .last()
            .filter(|e| e.state == EpochState::Active)
            .copied()
        {
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
                // All target capabilities were synchronously drained above.
                // The core opens its own partition DbLock; no lock is bypassed.
                let log = inventory.released(|| {
                    let log =
                        PartitionLog::recover_live_sealed(&path, &expected, metrics, || Ok(()))
                            .map_err(TcpError::component)?;
                    let log = Arc::new(log.retain_local_authority(authority));
                    Ok::<_, TcpError>(log)
                })?;
                inventory.logs.insert(old.epoch, log);
                inventory
                    .manifest
                    .epochs
                    .last_mut()
                    .expect("newest epoch")
                    .state = EpochState::Sealed;
                Self::persist_manifest(inventory)?;
                inventory.healthy = true;
            } else {
                Self::seal_epoch(inventory, slot, old.epoch, |_| Ok(()))?;
            }
        }
        inventory.healthy = false;
        inventory.readers.write().invalidate();
        // A Preparing entry left by a failed fence of this incarnation can never
        // grant; a strictly higher fence resolves it to Abandoned.
        if let Some(last) = inventory
            .manifest
            .epochs
            .last_mut()
            .filter(|e| e.state == EpochState::Preparing)
        {
            last.state = EpochState::Abandoned;
        }
        // The high-water and Preparing are installed together, before the
        // generation directory is touched.
        inventory.manifest.highest_fenced_generation = epoch;
        inventory.manifest.epochs.push(Epoch {
            epoch,
            state: EpochState::Preparing,
        });
        Self::persist_manifest(inventory)?;
        let path = epoch_path(&inventory.config.root, epoch);
        let partition = partition_at(&inventory.config.partition, epoch);
        let shape = inventory.shape.clone();
        let metrics = inventory.metrics.clone();
        let prewrite = inventory.config.wal_prewrite;
        let authority = inventory.lock.clone();
        let log = inventory.released(|| {
            let log = if live {
                PartitionLog::create_live_with_prewrite(&path, partition, &shape, metrics, prewrite)
            } else {
                PartitionLog::create(&path, partition, &shape, metrics)
            };
            let log = Arc::new(
                log.map_err(TcpError::component)?
                    .retain_local_authority(authority),
            );
            Ok::<_, TcpError>(log)
        })?;
        inventory
            .manifest
            .epochs
            .last_mut()
            .expect("newest epoch")
            .state = EpochState::Active;
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
        inventory.bump_revision()?;
        let bytes = encode_metadata(&inventory.manifest).map_err(TcpError::component)?;
        let root = inventory.config.root.clone();
        inventory
            .released(|| install_metadata(&root, MANIFEST, &bytes))
            .map_err(TcpError::component)
    }
    #[cfg(test)]
    pub(super) fn sealed_log_for_test(&self, epoch: u64) -> Arc<PartitionLog> {
        assert!(
            self.manifest
                .epochs
                .iter()
                .any(|entry| { entry.epoch == epoch && entry.state == EpochState::Sealed })
        );
        Arc::clone(self.logs.get(&epoch).expect("sealed stream missing"))
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

    /// Test view of the installed bindings: key, binding and writability.
    #[cfg(test)]
    pub(super) fn live_binding_snapshot(
        &self,
    ) -> Vec<(
        (u64, u64),
        super::super::nvmeof::live_storage::LiveTargetBinding,
        bool,
    )> {
        self.live_bindings
            .iter()
            .map(|(key, (_, binding, writable))| (*key, binding.clone(), *writable))
            .collect()
    }

    /// Test view of one installed binding.
    #[cfg(test)]
    pub(super) fn live_binding_snapshot_for(
        &self,
        key: (u64, u64),
    ) -> Option<(super::super::nvmeof::live_storage::LiveTargetBinding, bool)> {
        self.live_bindings
            .get(&key)
            .map(|(_, binding, writable)| (binding.clone(), *writable))
    }

    /// Count one export that `kind` created inline.
    pub(super) fn record_inline_export(&mut self, kind: InlineExport) {
        let stats = &mut self.inline_exports;
        let count = match kind {
            InlineExport::Lease => &mut stats.lease,
            InlineExport::Rollover => &mut stats.rollover,
            InlineExport::Prelease => &mut stats.prelease,
            InlineExport::Resolve => &mut stats.resolve,
            InlineExport::Replay => &mut stats.replay,
        };
        *count = count.saturating_add(1);
    }

    /// Return the permanent binding of `segment`, creating it if needed. A
    /// sealed segment can only be resolved read-only. The caller holds the
    /// lifecycle gate; the export runs with the inventory released.
    pub(super) fn publish_permanent_segment<H: InventoryAccess>(
        inventory: &mut H,
        target: &mut LiveTarget,
        segment: &super::super::nvmeof::live_storage::LiveWalSegment,
        readonly: bool,
    ) -> Result<(super::super::nvmeof::live_storage::LiveTargetBinding, bool), TcpError> {
        use super::super::nvmeof::live_storage::wal_export_name;
        let key = (segment.epoch, segment.file_index);
        if let Some((existing, binding, writable)) = inventory.live_bindings.get(&key) {
            if existing != segment || (!readonly && !writable) {
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
            .released(|| target.export_permanent(&name, file, readonly))
            .map_err(TcpError::component)?;
        export_span.end_at(segment.file_index);
        inventory
            .live_bindings
            .insert(key, (segment.clone(), binding.clone(), !readonly));
        Ok((binding, true))
    }

    /// Write-protect every writable binding of `epoch` and mark it read-only.
    /// Exports stay alive and readable. The caller holds the lifecycle gate;
    /// the target's configfs and controller work runs with the inventory
    /// released. On error the bindings are left as they were and the owner is
    /// unhealthy, because the device outcome is unknown.
    fn protect_live_targets<H: InventoryAccess>(
        inventory: &mut H,
        target: &mut LiveTarget,
        epoch: u64,
    ) -> Result<(), TcpError> {
        let keys: Vec<_> = {
            let inventory: &NodeInventory = inventory;
            inventory
                .live_bindings
                .iter()
                .filter(|((binding_epoch, _), (_, _, writable))| {
                    *binding_epoch == epoch && *writable
                })
                .map(|(key, (_, binding, _))| (*key, binding.clone()))
                .collect()
        };
        if keys.is_empty() {
            return Ok(());
        }
        let bindings: Vec<_> = keys.iter().map(|(_, binding)| binding.clone()).collect();
        let result = inventory.released(|| target.write_protect(&bindings));
        if let Err(error) = result {
            inventory.healthy = false;
            return Err(TcpError::component(error));
        }
        for (key, _) in keys {
            if let Some((_, binding, writable)) = inventory.live_bindings.get_mut(&key) {
                *writable = false;
                binding.readonly = true;
            }
        }
        Ok(())
    }

    /// The caller holds the lifecycle gate; `target` is its content.
    pub(super) fn release_live_pin<H: InventoryAccess>(
        inventory: &mut H,
        _target: Option<&mut LiveTarget>,
        epoch: u64,
        token: [u8; 16],
    ) -> Result<(), TcpError> {
        let Some((owner, _)) = inventory.live_pins.get(&token) else {
            return Ok(());
        };
        if *owner != epoch {
            return Err(TcpError::new("live pin belongs to another authority epoch"));
        }
        let (_, leases) = inventory
            .live_pins
            .remove(&token)
            .ok_or_else(|| TcpError::new("live pin disappeared"))?;
        // Only an enabled stream pays for the file list (read release path).
        let files = inventory
            .r1_events
            .is_some()
            .then(|| r1_events::lease_files(&leases));
        // Dropping the record leases releases their inode/read pins. The
        // permanent export stays for the life of its file.
        drop(leases);
        if let (Some(events), Some(files)) = (inventory.r1_events.as_mut(), files) {
            events.pin("pin_released", "record", token, files, "release");
        }
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
    #[allow(
        clippy::expect_used,
        reason = "an open manifest always retains its newest epoch entry"
    )]
    pub(super) fn seal_epoch<H: InventoryAccess>(
        inventory: &mut H,
        slot: &mut LifecycleSlot,
        epoch: u64,
        drain_log: impl FnOnce(&Arc<PartitionLog>) -> Result<(), TcpError>,
    ) -> Result<(), TcpError> {
        inventory.ensure_healthy()?;
        if !inventory.live_pins.is_empty() {
            return Err(TcpError::new("live NVMe-oF pins must drain before sealing"));
        }
        if inventory.manifest.epochs.iter().any(|e| {
            e.epoch == epoch && matches!(e.state, EpochState::Sealed | EpochState::Abandoned)
        }) {
            return Ok(());
        }
        let log = inventory.active(epoch)?;
        let target = live_target(inventory, slot)?;
        inventory.healthy = false;
        inventory.readers.write().invalidate();
        if let Some(target) = target {
            Self::protect_live_targets(inventory, target, epoch)?;
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
        drop(log);
        inventory
            .manifest
            .epochs
            .last_mut()
            .expect("open manifest retains its newest epoch")
            .state = EpochState::Sealed;
        Self::persist_manifest(inventory)?;
        inventory.healthy = true;
        Ok(())
    }
    /// The caller holds the lifecycle gate; `target` is its content.
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
        Ok(stats)
    }
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
    pub(super) fn retirement_log(
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

    pub(super) fn checkpoint_log(
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
    #[test]
    fn interrupted_preparing_epoch_is_retained_and_never_activated() {
        let temporary = tempdir::TempDir::new("node-preparing-fence").unwrap();
        let config = config(&temporary.path().join("node"));
        let mut inventory = NodeInventory::open(config.clone(), metrics()).unwrap();
        inventory.fence_to(1).unwrap();
        inventory.seal(1).unwrap();
        inventory.manifest.highest_fenced_generation = 2;
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
        let status = inventory.status().unwrap();
        assert_eq!(status.highest_fenced_generation, 2);
        assert_eq!(status.active_epoch, None);
        assert!(
            inventory
                .manifest
                .epochs
                .iter()
                .map(|e| (e.epoch, e.state == EpochState::Active))
                .eq([(1, false), (2, false)])
        );
        assert!(inventory.manifest.epochs[1].state == EpochState::Abandoned);
        assert!(inventory.fence_to(2).is_err());
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
}
