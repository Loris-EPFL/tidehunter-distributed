//! Coordinator side of the database lifecycle (Phase 5 L2): provisioned
//! database identity, owner provisioning, the write-ahead drop intent and
//! the recursive drop of nested authorities.
//!
//! Persisted coordinator boundaries, in order:
//! 1. `Provisioning` authority with a fresh random database ID and the owner
//!    reconnect configuration, before any `ProvisionDatabase` is sent;
//! 2. `Ready` once every owner acknowledged the exact provision;
//! 3. `Dropping { generation }` before any destructive owner request;
//! 4. local data unlinked except `LOCK` and the manifest, directory synced;
//! 5. renamed to the sibling quarantine `.tidehunter-drop-<db hex>`, parent
//!    synced; then the quarantine is removed (`LOCK` first, manifest last).
//!
//! Namespace locks (`namespace.rs`) order lifecycle operations on nested
//! paths; they are separate from, and taken before, the native `DbLock`.

mod drop_tx;
mod namespace;

pub(crate) use drop_tx::drop_db;
#[cfg(test)]
pub(crate) use drop_tx::drop_remote;
pub(crate) use namespace::{NamespaceGuard, NamespaceMode};

use super::coordinator_io::CoordinatorIoCategory;
use super::fencing_generations::{AUTHORITY_FORMAT, read_authority_manifest};
use super::{AuthorityManifest, MANIFEST, RemoteDatabaseConfig, RemoteEndpoint, persist_manifest};
use crate::distributed::storage::{LocalDbError, LocalInventoryConfig, read_metadata};
use crate::distributed::tcp::{
    DatabaseLiveTarget, DatabaseProvisionSpec, HostClient, LiveInitiatorIdentity, TlsFiles,
    host_identity,
};
use crate::lock::DbLock;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Durable lifecycle state of one coordinator authority directory.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
// The manifest's flattened mode fields make Serde buffer the rest of the
// document, which cannot decode YAML enum tags. Persist a tagged map instead.
#[serde(tag = "state")]
pub enum AuthorityLifecycle {
    /// Identity and owner reconnect data are durable; provisioning may be
    /// incomplete and is retried with the same identity and spec.
    Provisioning,
    /// Every owner holds the database; L1 fencing and recovery may run.
    Ready,
    /// Write-ahead drop intent: `generation` is the highest fencing
    /// reservation, sent to every owner. Never served again.
    Dropping { generation: u64 },
}

/// What an offline drop needs to reach and authenticate every owner without
/// the routing configuration: credential references (never key bytes), the
/// lifecycle timeout, each owner's trust principal and the exact live target
/// sent in its provision.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OwnerControlConfig {
    pub tls: TlsFiles,
    pub timeout: Duration,
    pub principals: BTreeMap<u32, [u8; 32]>,
    pub live_targets: BTreeMap<u32, DatabaseLiveTarget>,
}

/// Only the lifecycle of any authority manifest, for ancestor checks and
/// quarantine reaping; other fields are ignored.
#[derive(Deserialize)]
struct LifecycleView {
    #[serde(default)]
    lifecycle: Option<AuthorityLifecycle>,
    #[serde(default)]
    storage: Option<StorageIdentityView>,
}

#[derive(Deserialize)]
struct StorageIdentityView {
    database: [u8; 16],
}

/// Recorded lifecycle and database identity of one authority manifest.
type ManifestLifecycle = (Option<AuthorityLifecycle>, Option<[u8; 16]>);

/// Lifecycle and identity of the manifest in `directory`, if it has one.
/// A non-regular manifest is corruption, never absence.
fn read_lifecycle(directory: &Path) -> io::Result<Option<ManifestLifecycle>> {
    let path = directory.join(MANIFEST);
    match fs::symlink_metadata(&path) {
        Ok(metadata) if metadata.file_type().is_file() => {}
        Ok(_) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{} is not a regular file", path.display()),
            ));
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    }
    let view: LifecycleView = read_metadata(&path).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("authority manifest {}: {error}", path.display()),
        )
    })?;
    Ok(Some((
        view.lifecycle,
        view.storage.map(|storage| storage.database),
    )))
}

/// Persisted lifecycle boundaries; test hooks run at each. Returning an
/// error abandons the operation exactly there (a crash with that image).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AuthorityStep {
    OpenAfterNamespace,
    ProvisionAfterPreflight,
    ProvisionAfterManifest,
    ProvisionAfterOwner(usize),
    ProvisionAfterReady,
    DropAfterNamespace,
    DropAfterIntent,
    DropAfterChildren,
    DropAfterOwner(usize),
    DropAfterUnlink,
    DropAfterRename,
    DropAfterParentSync,
    DropAfterQuarantine,
}

#[cfg(test)]
pub(crate) type StepHook = std::sync::Arc<dyn Fn(AuthorityStep) -> io::Result<()> + Send + Sync>;

#[cfg(test)]
std::thread_local! {
    /// Per-thread: open and drop run every step on the calling thread.
    static STEP_HOOK: std::cell::RefCell<Option<StepHook>> = const { std::cell::RefCell::new(None) };
}

/// Install (or clear) this thread's lifecycle step hook.
#[cfg(test)]
pub(crate) fn set_step_hook(hook: Option<StepHook>) {
    STEP_HOOK.with(|slot| *slot.borrow_mut() = hook);
}

pub(crate) fn step(step: AuthorityStep) -> io::Result<()> {
    #[cfg(test)]
    {
        let hook = STEP_HOOK.with(|slot| slot.borrow().clone());
        if let Some(hook) = hook {
            return hook(step);
        }
    }
    let _ = step;
    Ok(())
}

/// Highest fencing reservation recorded by this authority (0 if none).
pub(super) fn highest_reservation(manifest: &AuthorityManifest) -> u64 {
    manifest.next_generation - 1
}

fn invalid_input(message: impl Into<String>) -> LocalDbError {
    LocalDbError::with_kind(io::ErrorKind::InvalidInput, message)
}

fn invalid_data(message: impl Into<String>) -> LocalDbError {
    LocalDbError::with_kind(io::ErrorKind::InvalidData, message)
}

/// The authority directory of an open (or being opened) database: its
/// native lock, canonical path and manifest. `_namespace` holds the
/// lifecycle namespace locks until open setup completes.
pub(super) struct OpenedAuthority {
    pub(super) lock: DbLock,
    pub(super) path: PathBuf,
    pub(super) manifest: AuthorityManifest,
    pub(super) namespace: NamespaceGuard,
}

/// Configured initiator and requested tier of every partition, exactly as
/// sent in its provision.
fn live_targets(config: &RemoteDatabaseConfig) -> BTreeMap<u32, DatabaseLiveTarget> {
    config
        .endpoints
        .iter()
        .map(|endpoint| {
            let live = &config.live_nvme;
            let (initiator_nqn, initiator_id) = match live.owner_identities.get(&endpoint.partition)
            {
                Some(identity) => (identity.host_nqn.clone(), identity.host_id),
                None => (live.host_nqn.clone(), live.host_id),
            };
            let target = DatabaseLiveTarget {
                initiator_nqn,
                initiator_id,
                durability_tier: live.leased_durability,
            };
            (endpoint.partition, target)
        })
        .collect()
}

/// Credential paths resolved once to absolute references.
fn resolved_tls(tls: &TlsFiles) -> Result<TlsFiles, LocalDbError> {
    let resolve = |path: &PathBuf| -> Result<PathBuf, LocalDbError> {
        if path.as_os_str().is_empty() {
            Ok(path.clone())
        } else {
            fs::canonicalize(path).map_err(|error| {
                LocalDbError::with_kind(
                    error.kind(),
                    format!("resolve owner credential {}: {error}", path.display()),
                )
            })
        }
    };
    Ok(TlsFiles {
        transport: tls.transport.clone(),
        ca: resolve(&tls.ca)?,
        cert: resolve(&tls.cert)?,
        key: resolve(&tls.key)?,
        server_name: String::new(),
    })
}

/// Connect a host-scope session to one owner with the stored control data;
/// when a principal is already recorded it must not have changed.
pub(super) fn connect_owner(
    control: &OwnerControlConfig,
    endpoint: &RemoteEndpoint,
) -> Result<(HostClient, [u8; 32]), LocalDbError> {
    let mut tls = control.tls.clone();
    tls.server_name = endpoint.server_name.clone();
    let client =
        HostClient::connect(&endpoint.address, &tls, control.timeout).map_err(|error| {
            LocalDbError::with_kind(
                error.kind(),
                format!("owner {} host session: {error}", endpoint.partition),
            )
        })?;
    let principal = client
        .status()
        .map(|status| status.principal)
        .ok_or_else(|| invalid_data(format!("owner {} sent no host status", endpoint.partition)))?;
    if control
        .principals
        .get(&endpoint.partition)
        .is_some_and(|expected| *expected != principal)
    {
        return Err(LocalDbError::with_kind(
            io::ErrorKind::PermissionDenied,
            format!("owner {} principal differs", endpoint.partition),
        ));
    }
    Ok((client, principal))
}

fn random_database() -> Result<[u8; 16], LocalDbError> {
    let mut database = [0u8; 16];
    rand::rngs::OsRng
        .try_fill_bytes(&mut database)
        .map_err(|error| LocalDbError::new(format!("database identity randomness: {error}")))?;
    if database == [0; 16] {
        return Err(LocalDbError::new("zero random database identity"));
    }
    Ok(database)
}

/// Apply the provisioned identity to the in-memory configuration. A nonzero
/// configured identity is accepted only as an assertion of the same one.
fn resolve_identity(
    config: &mut RemoteDatabaseConfig,
    database: [u8; 16],
) -> Result<(), LocalDbError> {
    if config.storage.database != [0; 16] && config.storage.database != database {
        return Err(invalid_input(
            "storage.database is provisioned and must not appear in remote configuration",
        ));
    }
    config.storage.database = database;
    Ok(())
}

/// Send the exact provision of every partition, in endpoint order. Owners
/// acknowledge an identical retry; a timeout keeps the manifest and retries.
fn provision_owners(
    config: &RemoteDatabaseConfig,
    manifest: &AuthorityManifest,
    shape_yaml: &str,
) -> Result<(), LocalDbError> {
    let control = manifest
        .owner_control
        .as_ref()
        .ok_or_else(|| invalid_data("provisioning authority has no owner control data"))?;
    for (index, endpoint) in manifest.endpoints.iter().enumerate() {
        let live_target = control
            .live_targets
            .get(&endpoint.partition)
            .cloned()
            .ok_or_else(|| {
                invalid_data(format!(
                    "owner {} has no provision spec",
                    endpoint.partition
                ))
            })?;
        let spec = DatabaseProvisionSpec {
            partition: config.partition_config(1, endpoint.partition),
            shape_yaml: shape_yaml.to_owned(),
            live_target,
        };
        let (mut client, _) = connect_owner(control, endpoint)?;
        client
            .provision(manifest.storage.database, &spec)
            .map_err(|error| {
                LocalDbError::with_kind(
                    error.kind(),
                    format!("owner {} ProvisionDatabase: {error}", endpoint.partition),
                )
            })?;
        step(AuthorityStep::ProvisionAfterOwner(index))?;
    }
    Ok(())
}

/// Lock, recover and, for a new path, provision the authority directory.
/// Returns `NotFound` after completing a pending drop found at `path`.
pub(super) fn open_authority(
    path: &Path,
    config: &mut RemoteDatabaseConfig,
    shape_yaml: &str,
    new_manifest: impl FnOnce(&RemoteDatabaseConfig) -> AuthorityManifest,
) -> Result<OpenedAuthority, LocalDbError> {
    let namespace = NamespaceGuard::acquire(path, NamespaceMode::Open)?;
    step(AuthorityStep::OpenAfterNamespace)?;
    let lock = DbLock::acquire(namespace.target(), Duration::ZERO)?;
    let path = namespace.target().to_path_buf();
    drop_tx::reap_quarantines(&path)?;
    let manifest_path = path.join(MANIFEST);
    let existing = match fs::symlink_metadata(&manifest_path) {
        Ok(metadata) if metadata.file_type().is_file() => true,
        Ok(_) => return Err(invalid_data("authority manifest is not a regular file")),
        Err(error) if error.kind() == io::ErrorKind::NotFound => false,
        Err(error) => return Err(error.into()),
    };
    if existing {
        let manifest = read_authority_manifest(&manifest_path)?;
        super::fencing_generations::validate(&manifest)?;
        let lifecycle = manifest
            .lifecycle
            .ok_or_else(|| invalid_data("authority manifest has no lifecycle"))?;
        if let AuthorityLifecycle::Dropping { .. } = lifecycle {
            // Never validate a replacement config or fence a dropping
            // database: finish its drop, then report it missing.
            drop_tx::drop_locked(&path, lock, manifest, &namespace)?;
            return Err(LocalDbError::with_kind(
                io::ErrorKind::NotFound,
                format!("database at {} was dropped", path.display()),
            ));
        }
        resolve_identity(config, manifest.storage.database)?;
        let mut manifest = manifest;
        if lifecycle == AuthorityLifecycle::Provisioning {
            if manifest.storage != config.storage
                || manifest.endpoints != config.endpoints
                || manifest.shape_yaml != shape_yaml
            {
                return Err(invalid_input(
                    "authority configuration differs from the provisioning record",
                ));
            }
            provision_owners(config, &manifest, shape_yaml)?;
            manifest.lifecycle = Some(AuthorityLifecycle::Ready);
            persist_manifest(&path, &manifest, CoordinatorIoCategory::Open)?;
            step(AuthorityStep::ProvisionAfterReady)?;
        }
        return Ok(OpenedAuthority {
            lock,
            path,
            manifest,
            namespace,
        });
    }
    // A new database. Only the lock, a pending manifest and nested
    // authorities (or their quarantines) may already exist here.
    for entry in fs::read_dir(&path)? {
        let entry = entry?;
        let name = entry.file_name();
        let nested = entry.file_type()?.is_dir()
            && (read_lifecycle(&entry.path())?.is_some()
                || name.to_str().is_some_and(drop_tx::is_quarantine_name));
        if name != "LOCK" && name != format!(".{MANIFEST}.pending").as_str() && !nested {
            return Err(invalid_data(
                "missing authority manifest beside retained data",
            ));
        }
    }
    if config.storage.database != [0; 16] {
        return Err(invalid_input(
            "storage.database is provisioned and must not appear in remote configuration",
        ));
    }
    let tls = resolved_tls(&config.tls)?;
    let mut control = OwnerControlConfig {
        tls,
        timeout: config.timeout,
        principals: BTreeMap::new(),
        live_targets: live_targets(config),
    };
    // Preflight: authenticated host status of every owner before any state.
    for endpoint in &config.endpoints {
        let (_, principal) = connect_owner(&control, endpoint)?;
        control.principals.insert(endpoint.partition, principal);
    }
    step(AuthorityStep::ProvisionAfterPreflight)?;
    config.storage.database = random_database()?;
    let mut manifest = new_manifest(config);
    manifest.format = AUTHORITY_FORMAT;
    manifest.lifecycle = Some(AuthorityLifecycle::Provisioning);
    manifest.owner_control = Some(control);
    persist_manifest(&path, &manifest, CoordinatorIoCategory::Open)?;
    step(AuthorityStep::ProvisionAfterManifest)?;
    provision_owners(config, &manifest, shape_yaml)?;
    manifest.lifecycle = Some(AuthorityLifecycle::Ready);
    persist_manifest(&path, &manifest, CoordinatorIoCategory::Open)?;
    step(AuthorityStep::ProvisionAfterReady)?;
    Ok(OpenedAuthority {
        lock,
        path,
        manifest,
        namespace,
    })
}

/// Replace each partition's initiator by its effective identity for this
/// database, derived as the owner derives it, after the persisted
/// configured identities were compared.
pub(super) fn apply_effective_initiators(config: &mut RemoteDatabaseConfig) {
    let database = config.storage.database;
    let endpoints = &config.endpoints;
    let live = &mut config.live_nvme;
    let mut identities = BTreeMap::new();
    for endpoint in endpoints {
        let configured = live
            .owner_identities
            .get(&endpoint.partition)
            .map_or(live.host_id, |identity| identity.host_id);
        identities.insert(
            endpoint.partition,
            LiveInitiatorIdentity {
                host_nqn: host_identity::effective_host_nqn(&database, endpoint.partition),
                host_id: host_identity::effective_host_id(
                    &configured,
                    &database,
                    endpoint.partition,
                ),
            },
        );
    }
    live.owner_identities = identities;
}

/// Serde adapter of `RemoteDatabaseConfig::storage`: the geometry without
/// the database identity, which is always provisioned.
pub(super) mod storage_geometry {
    use super::LocalInventoryConfig;
    use crate::distributed::codec::BatchLimits;
    use crate::distributed::group::GroupLimits;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    #[derive(Serialize)]
    struct GeometryRef<'a> {
        partitions: &'a Vec<u32>,
        batch_limits: &'a BatchLimits,
        group_limits: &'a GroupLimits,
        fragment_bytes: u64,
        wal_file_bytes: u64,
        max_batches_per_partition: usize,
        max_reference_metadata_bytes: usize,
    }

    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Geometry {
        #[serde(default)]
        database: Option<serde::de::IgnoredAny>,
        partitions: Vec<u32>,
        batch_limits: BatchLimits,
        group_limits: GroupLimits,
        fragment_bytes: u64,
        wal_file_bytes: u64,
        max_batches_per_partition: usize,
        max_reference_metadata_bytes: usize,
    }

    pub fn serialize<S: Serializer>(
        storage: &LocalInventoryConfig,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        GeometryRef {
            partitions: &storage.partitions,
            batch_limits: &storage.batch_limits,
            group_limits: &storage.group_limits,
            fragment_bytes: storage.fragment_bytes,
            wal_file_bytes: storage.wal_file_bytes,
            max_batches_per_partition: storage.max_batches_per_partition,
            max_reference_metadata_bytes: storage.max_reference_metadata_bytes,
        }
        .serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<LocalInventoryConfig, D::Error> {
        let geometry = Geometry::deserialize(deserializer)?;
        if geometry.database.is_some() {
            return Err(serde::de::Error::custom(
                "storage.database is provisioned and must not appear in remote configuration",
            ));
        }
        Ok(LocalInventoryConfig {
            database: [0; 16],
            partitions: geometry.partitions,
            batch_limits: geometry.batch_limits,
            group_limits: geometry.group_limits,
            fragment_bytes: geometry.fragment_bytes,
            wal_file_bytes: geometry.wal_file_bytes,
            max_batches_per_partition: geometry.max_batches_per_partition,
            max_reference_metadata_bytes: geometry.max_reference_metadata_bytes,
        })
    }
}

/// Test fixtures: provision `path` on every owner through the real open path
/// (namespace locks, `DbLock`, `Provisioning`, `ProvisionDatabase` to every
/// owner, `Ready`) without fencing or recovery. `config` then carries the
/// provisioned identity, so fixtures can address owner databases before the
/// first serving open.
#[cfg(test)]
pub(crate) fn provision_for_test(
    path: &Path,
    shape: &crate::key_shape::KeyShape,
    config: &mut RemoteDatabaseConfig,
) -> Result<(), LocalDbError> {
    config.validate()?;
    let shape_yaml = shape.to_yaml().map_err(LocalDbError::component)?;
    open_authority(path, config, &shape_yaml, |config| {
        super::new_authority_manifest(config, &shape_yaml)
    })
    .map(drop)
}

#[cfg(test)]
#[path = "authority_lifecycle/tests.rs"]
mod tests;

#[cfg(test)]
#[path = "authority_lifecycle/e2e_tests.rs"]
mod e2e_tests;
