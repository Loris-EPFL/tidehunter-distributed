//! Owner host: one process, one host root, a bounded set of databases.
//!
//! The host owns the root directory lock, the durable catalogue
//! (`host/catalog.rs`), the target slot pool and the permanent drop tombstones
//! (`host/tombstone.rs`).
//! Each provisioned database is an independent [`StorageNode`] under
//! `<root>/db-<32 hex>/` with its own inventory, directory lock, reader
//! authority, live target registry and control owner.
//!
//! Sessions bind by their first request (`tcp/reactor/host_session.rs`):
//! `Status { database: None }` binds the host (provision, drop, status only);
//! `Status { database: Some(id) }` binds that database for the session's life.
//!
//! Concurrency (kvstore-concurrency design order):
//! - Catalogue, slots and tombstones have one owner, the cold lifecycle
//!   actor (`host/actor.rs`), reached only through its bounded queue.
//! - The routing map is a short `RwLock` held only to copy or replace one
//!   `Arc`; never across I/O, a sync or a wait.
//! - Lock order: host `DbLock` → database `DbLock` → database lifecycle gate
//!   → inventory/pin/reader locks. The actor takes the database lock only
//!   through `StorageNode::open` and the drop teardown.

pub(super) mod actor;
pub(crate) mod catalog;
pub mod identity;
pub(crate) mod tombstone;

#[cfg(test)]
mod tests;
pub use catalog::{DatabaseLiveTarget, DatabaseProvisionSpec, LiveTargetPool};

use super::{LiveInlineExportStats, NodeConfig, StorageNode, TcpError, TcpLimits, TlsFiles};
use crate::distributed::durability::DurabilityConfig;
use crate::lock::DbLock;
use crate::metrics::Metrics;
#[cfg(test)]
use parking_lot::Mutex;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{self, Read};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

/// Default host database bound: perpetual, checkpoints, the current and next
/// epoch databases and their consensus stores at Sui's default retention,
/// plus margin.
const DEFAULT_MAX_DATABASES: usize = 16;
/// Hard ceiling of `max_databases`; keeps slot ranges and the catalogue small.
const MAX_DATABASES: usize = 4096;

fn default_max_databases() -> usize {
    DEFAULT_MAX_DATABASES
}

/// Durability settings shared by every database of the host. The tier is
/// not here: each database's tier is fixed by its provision request.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct HostDurabilityDefaults {
    pub max_unsynced_age_ms: Option<u64>,
    pub max_pending_completions: usize,
}

impl Default for HostDurabilityDefaults {
    fn default() -> Self {
        let native = DurabilityConfig::default();
        Self {
            max_unsynced_age_ms: native.max_unsynced_age_ms,
            max_pending_completions: native.max_pending_completions,
        }
    }
}

/// Public owner process configuration. Databases are provisioned at runtime
/// by their coordinators; no partition, shape or target appears here.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnerHostConfig {
    pub root: PathBuf,
    #[serde(default = "default_max_databases")]
    pub max_databases: usize,
    pub live_target_pool: LiveTargetPool,
    /// Principal of the trusted coordinator host; required only for the
    /// trusted (no-TLS) transports, where no client CA defines the principal.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trusted_coordinator_id: Option<[u8; 32]>,
    #[serde(default = "super::default_async_read_policy")]
    pub wal_member_read_policy: crate::distributed::storage::WalMemberReadPolicy,
    #[serde(default = "super::default_wal_prewrite")]
    pub wal_prewrite: bool,
    #[serde(default = "super::default_device_reads")]
    pub max_device_reads_per_status: usize,
    #[serde(default)]
    pub durability: HostDurabilityDefaults,
}

impl OwnerHostConfig {
    /// Validate without opening files, sockets or nvmet.
    pub fn validate(&self) -> Result<(), TcpError> {
        if !(1..=MAX_DATABASES).contains(&self.max_databases) {
            return Err(TcpError::with_kind(
                io::ErrorKind::InvalidInput,
                format!("max_databases must be in 1..={MAX_DATABASES}"),
            ));
        }
        self.live_target_pool.validate(self.max_databases)?;
        super::validate_device_read_limit(self.max_device_reads_per_status)
    }
}

/// Authenticated host-scope Status body.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct HostStatus {
    /// Owner client-trust principal (see `identity::principal_from_anchors`).
    pub principal: [u8; 32],
    pub max_frame_bytes: u64,
    pub max_connections: u16,
    pub max_reads: u16,
    pub recovery_page_batches: u16,
}

/// Persisted boundaries of provision and drop, in order. Test hooks run at
/// each one; returning an error abandons the operation exactly there, which
/// with a reopen of the host models a crash with that durable image.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum HostStep {
    ProvisionBeforeReserve,
    ProvisionAfterReserve,
    ProvisionAfterDirectory,
    ProvisionAfterSetup,
    ProvisionAfterReady,
    DropAfterClose,
    DropAfterIntent,
    DropAfterTargetCleanup,
    DropAfterFilesOnly,
    DropAfterUnlink,
    DropAfterRootSync,
    DropAfterTombstone,
    DropAfterCatalogClear,
}

#[cfg(test)]
pub(crate) type StepHook = Arc<dyn Fn(HostStep) -> Result<(), TcpError> + Send + Sync>;

/// Test harnesses: runs on every database the host opens (provision setup
/// and restart recovery), after its provisioned tier is applied and before
/// it is routed. Per-database test settings and owner fault injection only.
#[cfg(any(test, feature = "test-utils"))]
pub type DatabaseOpenHook = Arc<dyn Fn(&Arc<StorageNode>) -> Result<(), TcpError> + Send + Sync>;

/// State shared by the host handle, its sessions and its lifecycle actor.
pub(super) struct HostCore {
    root: PathBuf,
    config: OwnerHostConfig,
    metrics: Arc<Metrics>,
    r1_events: Option<crate::distributed::r1_events::R1EventConfig>,
    /// Routed (Ready and open) databases. Short lock: copy or replace one
    /// handle; never held across I/O or a wait.
    routes: RwLock<BTreeMap<[u8; 16], Arc<StorageNode>>>,
    /// Fixed by the first `serve`: transport limits and the client-trust
    /// principal derived from its credentials.
    serving: OnceLock<(TcpLimits, [u8; 32])>,
    #[cfg(any(test, feature = "test-utils"))]
    fixture_configfs: Option<PathBuf>,
    #[cfg(test)]
    step_hook: Mutex<Option<StepHook>>,
    #[cfg(any(test, feature = "test-utils"))]
    open_hook: Option<DatabaseOpenHook>,
}

impl HostCore {
    pub(super) fn route(&self, database: &[u8; 16]) -> Option<Arc<StorageNode>> {
        self.routes.read().get(database).cloned()
    }

    pub(super) fn serving(&self) -> Result<(TcpLimits, [u8; 32]), TcpError> {
        self.serving
            .get()
            .copied()
            .ok_or_else(|| TcpError::new("owner host is not serving"))
    }

    pub(super) fn host_status(&self) -> Result<HostStatus, TcpError> {
        let (limits, principal) = self.serving()?;
        let narrow = |value: usize| u16::try_from(value).unwrap_or(u16::MAX);
        Ok(HostStatus {
            principal,
            max_frame_bytes: limits.max_frame_bytes as u64,
            max_connections: narrow(limits.max_connections),
            max_reads: narrow(limits.max_reads),
            recovery_page_batches: narrow(limits.recovery_page_batches),
        })
    }

    fn step(&self, step: HostStep) -> Result<(), TcpError> {
        #[cfg(test)]
        {
            let hook = self.step_hook.lock().clone();
            if let Some(hook) = hook {
                return hook(step);
            }
        }
        let _ = step;
        Ok(())
    }

    /// The per-database owner configuration of one catalogue entry.
    fn database_config(&self, entry: &catalog::HostEntry) -> Result<NodeConfig, TcpError> {
        let root = catalog::database_root(&self.root, &entry.database);
        let live_target = self.config.live_target_pool.target_config(
            entry.slot,
            &entry.database,
            entry.spec.partition.partition,
            &root,
        )?;
        Ok(NodeConfig {
            root,
            partition: entry.spec.partition.clone(),
            shape_yaml: entry.spec.shape_yaml.clone(),
            wal_member_read_policy: self.config.wal_member_read_policy,
            wal_prewrite: self.config.wal_prewrite,
            max_device_reads_per_status: self.config.max_device_reads_per_status,
            live_target: Some(live_target),
        })
    }

    /// Open one catalogue entry's database and apply its provisioned tier.
    fn open_database(&self, entry: &catalog::HostEntry) -> Result<Arc<StorageNode>, TcpError> {
        let config = self.database_config(entry)?;
        config.validate()?;
        let mode = match entry.state {
            catalog::HostState::Preparing => super::inventory::OpenMode::Prepare,
            catalog::HostState::Ready => super::inventory::OpenMode::Ready,
            catalog::HostState::Dropping { .. } => {
                return Err(TcpError::with_kind(
                    io::ErrorKind::InvalidData,
                    "attempt to open a dropping database",
                ));
            }
        };
        let open = || StorageNode::open_inner(config, Arc::clone(&self.metrics), mode);
        #[cfg(any(test, feature = "test-utils"))]
        let node = match &self.fixture_configfs {
            Some(configfs) => super::inventory::with_live_target_fixture(configfs.clone(), open)?,
            None => open()?,
        };
        #[cfg(not(any(test, feature = "test-utils")))]
        let node = open()?;
        node.set_durability(DurabilityConfig {
            tier: entry.spec.live_target.durability_tier,
            max_unsynced_age_ms: self.config.durability.max_unsynced_age_ms,
            max_pending_completions: self.config.durability.max_pending_completions,
        })?;
        if let Some(config) = &self.r1_events {
            node.enable_r1_events(config)?;
        }
        #[cfg(any(test, feature = "test-utils"))]
        if let Some(hook) = &self.open_hook {
            hook(&node)?;
        }
        Ok(node)
    }

    /// Ordinary-file target fixture of test harnesses; target teardown only.
    fn fixture_configfs(&self) -> Option<PathBuf> {
        #[cfg(any(test, feature = "test-utils"))]
        {
            self.fixture_configfs.clone()
        }
        #[cfg(not(any(test, feature = "test-utils")))]
        {
            None
        }
    }
}

/// One routed database and its live-target shutdown statistics (`None` when
/// it owns no live target).
pub type DatabaseTargetShutdown = (
    [u8; 16],
    Option<crate::distributed::nvmeof::LiveTargetStats>,
);

/// The owner process host. See the module documentation.
pub struct OwnerHost {
    core: Arc<HostCore>,
    actor: actor::ActorHandle,
    /// Released last: no other process may open this root while any
    /// database below it is open.
    _lock: DbLock,
}

impl OwnerHost {
    /// Open or create the host root, recover the catalogue and open every
    /// Ready database. Pending drops resume on the lifecycle actor.
    pub fn open(config: OwnerHostConfig, metrics: Arc<Metrics>) -> Result<Arc<Self>, TcpError> {
        Self::open_with_r1_events(config, metrics, None)
    }

    /// Open a host with one independently identified R1 stream per database
    /// incarnation, including databases provisioned after startup.
    pub fn open_with_r1_events(
        config: OwnerHostConfig,
        metrics: Arc<Metrics>,
        r1_events: Option<crate::distributed::r1_events::R1EventConfig>,
    ) -> Result<Arc<Self>, TcpError> {
        Self::open_inner(
            config,
            metrics,
            r1_events,
            #[cfg(any(test, feature = "test-utils"))]
            None,
            #[cfg(any(test, feature = "test-utils"))]
            None,
        )
    }

    /// Test harnesses: open with a [`DatabaseOpenHook`] that runs on every
    /// database this host opens, including the Ready ones recovered here.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn open_with_database_hook(
        config: OwnerHostConfig,
        metrics: Arc<Metrics>,
        hook: DatabaseOpenHook,
    ) -> Result<Arc<Self>, TcpError> {
        Self::open_inner(
            config,
            metrics,
            None,
            #[cfg(any(test, feature = "test-utils"))]
            None,
            Some(hook),
        )
    }

    /// Test harnesses: every database this host opens uses the ordinary-file
    /// live target fixture under `configfs` (`hosts`, `ports`, `subsystems`)
    /// instead of kernel nvmet. Journal-only: it does not prove target drain.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn open_with_live_fixture(
        config: OwnerHostConfig,
        metrics: Arc<Metrics>,
        configfs: PathBuf,
    ) -> Result<Arc<Self>, TcpError> {
        Self::open_inner(config, metrics, None, Some(configfs), None)
    }

    fn open_inner(
        mut config: OwnerHostConfig,
        metrics: Arc<Metrics>,
        r1_events: Option<crate::distributed::r1_events::R1EventConfig>,
        #[cfg(any(test, feature = "test-utils"))] fixture_configfs: Option<PathBuf>,
        #[cfg(any(test, feature = "test-utils"))] open_hook: Option<DatabaseOpenHook>,
    ) -> Result<Arc<Self>, TcpError> {
        if let Some(error) = super::super::sys::route_unavailable() {
            return Err(TcpError::new(error));
        }
        config.validate()?;
        create_private_directory(&config.root)?;
        config.root = config.root.canonicalize()?;
        let lock = DbLock::acquire(&config.root, Duration::ZERO)?;
        let root = config.root.clone();
        let catalog = recover_catalog(&root, &config)?;
        let core = Arc::new(HostCore {
            root,
            config,
            metrics,
            r1_events,
            routes: RwLock::new(BTreeMap::new()),
            serving: OnceLock::new(),
            #[cfg(any(test, feature = "test-utils"))]
            fixture_configfs,
            #[cfg(test)]
            step_hook: Mutex::new(None),
            #[cfg(any(test, feature = "test-utils"))]
            open_hook,
        });
        for entry in &catalog.entries {
            if entry.state == catalog::HostState::Ready {
                let node = core.open_database(entry)?;
                core.routes.write().insert(entry.database, node);
            }
        }
        let actor = actor::ActorHandle::start(core.clone(), catalog)?;
        Ok(Arc::new(Self {
            core,
            actor,
            _lock: lock,
        }))
    }

    /// Serve host and database sessions until `shutdown`. The trust principal
    /// is derived from `tls`: the client CA anchor for mTLS, the configured
    /// `trusted_coordinator_id` for the trusted transports.
    pub fn serve(
        self: &Arc<Self>,
        listener: TcpListener,
        tls: TlsFiles,
        limits: TcpLimits,
        shutdown: Arc<AtomicBool>,
    ) -> Result<(), TcpError> {
        limits.validate()?;
        let principal = trust_principal(&tls, self.core.config.trusted_coordinator_id)?;
        if self.core.serving.get_or_init(|| (limits, principal)) != &(limits, principal) {
            return Err(TcpError::new(
                "owner host already serves with other limits or credentials",
            ));
        }
        for node in self.core.routes.read().values() {
            node.validate_limits(limits)?;
        }
        let host = self.clone();
        super::listener::serve_listener(
            listener,
            tls,
            limits,
            shutdown,
            Arc::new(move |session, verifier, shutdown, wake| {
                super::reactor::serve_host_session(
                    &host.core,
                    &host.actor,
                    session,
                    limits,
                    shutdown,
                    wake,
                    verifier,
                )
            }),
        )
    }

    /// Routed database handle, for fixtures and owner tooling.
    pub fn database(&self, database: &[u8; 16]) -> Option<Arc<StorageNode>> {
        self.core.route(database)
    }

    /// IDs of the routed databases, sorted.
    pub fn databases(&self) -> Vec<[u8; 16]> {
        self.core.routes.read().keys().copied().collect()
    }

    /// Finalize every routed stream after the server has joined its sessions.
    pub fn finish_r1_events(&self) -> Result<(), TcpError> {
        if self.core.r1_events.is_none() {
            return Ok(());
        }
        let nodes: Vec<_> = self.core.routes.read().values().cloned().collect();
        let mut result = Ok(());
        for node in nodes {
            let finished = node.finish_r1_events();
            result = result.and(finished);
        }
        result
    }

    /// Final owner-process shutdown of every routed database's live target,
    /// after serving sessions have stopped. Not a database drop.
    pub fn finish_live_target_shutdown(&self) -> Result<Vec<DatabaseTargetShutdown>, TcpError> {
        let nodes: Vec<_> = self
            .core
            .routes
            .read()
            .iter()
            .map(|(id, node)| (*id, node.clone()))
            .collect();
        nodes
            .into_iter()
            .map(|(id, node)| node.finish_live_target_shutdown().map(|stats| (id, stats)))
            .collect()
    }

    /// Preparation totals summed over routed databases.
    pub fn wal_prewrite_stats(&self) -> (u64, u64, u64) {
        self.core
            .routes
            .read()
            .values()
            .map(|node| node.wal_prewrite_stats())
            .fold((0, 0, 0), |sum, next| {
                (sum.0 + next.0, sum.1 + next.1, sum.2 + next.2)
            })
    }

    /// Inline live export counts summed over routed databases.
    pub fn live_inline_export_stats(&self) -> LiveInlineExportStats {
        let mut total = LiveInlineExportStats::default();
        for node in self.core.routes.read().values() {
            let next = node.live_inline_export_stats();
            total.lease += next.lease;
            total.rollover += next.rollover;
            total.prelease += next.prelease;
            total.resolve += next.resolve;
            total.replay += next.replay;
        }
        total
    }

    /// Install a hook that runs at every persisted lifecycle boundary.
    #[cfg(test)]
    pub(crate) fn set_step_hook(&self, hook: Option<StepHook>) {
        *self.core.step_hook.lock() = hook;
    }
}

impl Drop for OwnerHost {
    fn drop(&mut self) {
        // Close the queue, let the actor finish its current job, join it.
        self.actor.shutdown();
        self.core.routes.write().clear();
    }
}

pub(super) fn prepare_host_target(
    root: &Path,
    target: &crate::distributed::nvmeof::LiveTargetConfig,
) -> Result<(), TcpError> {
    if target.state_dir == root.join(catalog::LIVE_TARGET_DIR) {
        create_private_directory(&target.state_dir)?;
    }
    Ok(())
}

/// Create a private (0700) directory if missing and sync its parent; an
/// existing directory must be a real directory, never a symlink.
pub(super) fn create_private_directory(path: &Path) -> Result<(), TcpError> {
    use std::os::unix::fs::DirBuilderExt;
    match fs::DirBuilder::new().mode(0o700).create(path) {
        Ok(()) => {
            let parent = path
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
                .unwrap_or(Path::new("."));
            File::open(parent)?.sync_all()?;
            Ok(())
        }
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            if fs::symlink_metadata(path)?.file_type().is_dir() {
                Ok(())
            } else {
                Err(TcpError::with_kind(
                    io::ErrorKind::InvalidData,
                    format!("{} is not a directory", path.display()),
                ))
            }
        }
        Err(error) => Err(error.into()),
    }
}

/// Load or create the catalogue and check the host root holds nothing else
/// than the catalogue, the tombstones and registered database directories.
fn recover_catalog(
    root: &Path,
    config: &OwnerHostConfig,
) -> Result<catalog::HostCatalog, TcpError> {
    let corrupt = |reason: String| TcpError::with_kind(io::ErrorKind::InvalidData, reason);
    if fs::symlink_metadata(root.join(super::inventory::MANIFEST)).is_ok() {
        return Err(corrupt(format!(
            "unsupported legacy single-database owner root {}: use archive/multi-design",
            root.display()
        )));
    }
    let catalog = match catalog::HostCatalog::load(root)? {
        Some(catalog) => catalog,
        None => {
            let catalog = catalog::HostCatalog::empty(config.live_target_pool.clone());
            catalog.persist(root, config.max_databases)?;
            catalog
        }
    };
    catalog.validate(config.max_databases)?;
    if !catalog.entries.is_empty() && catalog.target_pool != config.live_target_pool {
        return Err(corrupt(
            "live target pool differs from the pool of provisioned databases".into(),
        ));
    }
    tombstone::validate_directory(root)?;
    let pending = format!(".{}.pending", catalog::CATALOG);
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name
            .to_str()
            .ok_or_else(|| corrupt("non-UTF-8 name in owner host root".into()))?;
        if name == "LOCK" || name == catalog::CATALOG || name == pending {
            continue;
        }
        if name == tombstone::DROPPED_DIR {
            if !entry.file_type()?.is_dir() {
                return Err(corrupt("tombstone directory is not a directory".into()));
            }
            continue;
        }
        let registered = name
            .strip_prefix(catalog::DATABASE_PREFIX)
            .and_then(identity::parse_database_hex)
            .is_some_and(|database| catalog.entry(&database).is_some());
        if !registered || !entry.file_type()?.is_dir() {
            return Err(corrupt(format!(
                "unregistered entry {name:?} in owner host root {}",
                root.display()
            )));
        }
    }
    for entry in &catalog.entries {
        let present = match fs::symlink_metadata(catalog::database_root(root, &entry.database)) {
            Ok(metadata) => metadata.file_type().is_dir(),
            Err(error) if error.kind() == io::ErrorKind::NotFound => false,
            Err(error) => return Err(error.into()),
        };
        // A Ready database without its directory and without a drop intent
        // is corruption, never equivalent to an unprovisioned one.
        if entry.state == catalog::HostState::Ready && !present {
            return Err(corrupt(format!(
                "Ready database {} has no directory",
                identity::database_hex(&entry.database)
            )));
        }
    }
    Ok(catalog)
}

/// Owner client-trust principal of the serving credentials.
fn trust_principal(tls: &TlsFiles, trusted: Option<[u8; 32]>) -> Result<[u8; 32], TcpError> {
    if tls.trusted_mode()? {
        return trusted.filter(|id| *id != [0; 32]).ok_or_else(|| {
            TcpError::with_kind(
                io::ErrorKind::InvalidInput,
                "trusted transport requires trusted_coordinator_id",
            )
        });
    }
    let file = File::open(&tls.ca)?;
    let mut anchor = Vec::new();
    file.take(1024 * 1024 + 1).read_to_end(&mut anchor)?;
    if anchor.len() > 1024 * 1024 {
        return Err(TcpError::new("TLS material exceeds size bound"));
    }
    identity::principal_from_anchors(&[anchor])
        .map_err(|reason| TcpError::with_kind(io::ErrorKind::InvalidInput, reason))
}
