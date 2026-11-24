//! Owned, file-backed live nvmet namespaces.
//!
//! The caller serializes this manager under the owner's node lifecycle gate,
//! never under the node inventory mutex: every configfs change is a file or
//! directory operation, so the inventory stays released across it. The caller
//! retains WAL/read pins until revocation and initiator CQ retirement.
//!
//! There is no target journal. Recovery scans the configfs objects of this
//! owner's NQN scope, seals every namespace it finds enabled, disables it, and
//! removes every owned object before the target is commissioned again. A
//! periodic `flock` on the scope stops two processes from scanning the same
//! objects. The state directory and WAL paths must be protected from untrusted
//! local users. The NVMe data plane assumes trusted, non-Byzantine initiators:
//! namespace write protection can be changed by an authorized controller.
use super::live_connect::{LiveConnectTarget, LiveConnectionRegistry};
use super::live_namespace::LiveNamespaceAccess;
use super::live_storage::LiveTargetBinding;
use super::{LiveTargetConfig, LiveTargetStats, NvmeofTransport};
use crate::distributed::tcp::host_identity::NQN_PREFIX;
use crate::lock::DbLock;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::net::IpAddr;
use std::os::fd::AsRawFd;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, symlink};
use std::path::{Component, Path, PathBuf};
use std::time::{Duration, Instant};

#[path = "live_target/reconcile.rs"]
mod reconcile;
pub(crate) use reconcile::verify_live_epoch_sizes;
#[path = "live_target/retirement.rs"]
mod retirement;
#[path = "live_target/teardown.rs"]
mod teardown;

const CONFIGFS: &str = "/sys/kernel/config/nvmet";
/// One random 16-byte registry identity, written once when the state
/// directory is created and never rewritten.
const TARGET_ID: &str = "target-id";
/// Journal of the removed protocol-35 registry. Its presence is refused.
const LEGACY_STATE: &str = "live-target.json";
const COMMISSIONING_DIRECTORY: &str = "commissioning";

/// Persist in the node-root marker before the first namespace is exposed.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LiveTargetIdentity {
    pub directory_device: u64,
    pub directory_inode: u64,
    pub registry_id: [u8; 16],
}

/// Obtained from the owner, never from an unauthenticated network path.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LiveFileIdentity {
    pub path: PathBuf,
    pub device: u64,
    pub inode: u64,
    pub bytes: u64,
    pub block_size: u32,
}

#[derive(Clone, Debug)]
struct Export {
    binding: LiveTargetBinding,
    file: LiveFileIdentity,
    readonly: bool,
}

/// Crash cut of one configfs mutation, for fixture tests. The hook runs before
/// the mutation and an error is returned as if the process died there.
#[cfg(test)]
pub(crate) type ConfigfsHook = Box<dyn FnMut(usize) -> io::Result<()> + Send>;

/// Open only while holding the owner node lock, before opening any WAL.
/// `open` disables and drains every scoped namespace, removes the scoped
/// configfs objects and then commissions the host and ports again. Any
/// uncertainty is an error, not permission to recover.
pub struct LiveTargetManager {
    configfs: PathBuf,
    config: LiveTargetConfig,
    scope: String,
    registry_id: [u8; 16],
    directory: File,
    sealed_directory: File,
    sealed_device: u64,
    sealed_inode: u64,
    exports: Vec<Export>,
    pins: HashMap<[u8; 16], File>,
    poisoned: bool,
    fixture: bool,
    stats: LiveTargetStats,
    commissioning: Option<LiveConnectionRegistry>,
    _lock: DbLock,
    // Held next to `_lock` for the whole manager lifetime.
    _scope_lock: File,
    #[cfg(test)]
    configfs_hook: std::sync::Mutex<Option<ConfigfsHook>>,
    #[cfg(test)]
    configfs_steps: std::cell::Cell<usize>,
    #[cfg(test)]
    commission_groups: std::cell::Cell<u64>,
}

fn invalid(s: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, s)
}
fn path_error(operation: &str, path: &Path, error: io::Error) -> io::Error {
    io::Error::new(
        error.kind(),
        format!("{operation} {}: {error}", path.display()),
    )
}
fn attribute(p: &Path) -> io::Result<String> {
    Ok(fs::read_to_string(p)
        .map_err(|e| path_error("read target attribute", p, e))?
        .trim()
        .to_owned())
}
fn optional_attribute(p: &Path) -> io::Result<Option<String>> {
    match fs::read_to_string(p) {
        Ok(value) => Ok(Some(value.trim().to_owned())),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(path_error("read target attribute", p, error)),
    }
}
fn nqn_valid(s: &str) -> bool {
    s.starts_with("nqn.")
        && s.len() > 4
        && s.len() <= 223
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b':' | b'-'))
}
fn random_id() -> io::Result<[u8; 16]> {
    let mut id = [0; 16];
    rand::rngs::OsRng
        .try_fill_bytes(&mut id)
        .map_err(|e| io::Error::other(e.to_string()))?;
    if id == [0; 16] {
        return Err(invalid("zero random identity"));
    }
    id[6] = (id[6] & 0x0f) | 0x40;
    id[8] = (id[8] & 0x3f) | 0x80;
    Ok(id)
}
fn uuid(id: [u8; 16]) -> String {
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        id[0],
        id[1],
        id[2],
        id[3],
        id[4],
        id[5],
        id[6],
        id[7],
        id[8],
        id[9],
        id[10],
        id[11],
        id[12],
        id[13],
        id[14],
        id[15]
    )
}
/// The DB/partition scope shared by every owned configfs object: the
/// coordinator host NQN without its trailing `host`. It keeps the NQN prefix,
/// so `scope + "commission"` is itself a valid initiator NQN.
fn owner_scope(host_nqn: &str) -> io::Result<&str> {
    host_nqn
        .strip_suffix("host")
        .filter(|scope| scope.starts_with(NQN_PREFIX) && scope.ends_with('-'))
        .ok_or_else(|| invalid("invalid target configuration"))
}
fn check_path(path: &Path, private: bool) -> io::Result<()> {
    if !path.is_absolute()
        || path
            .components()
            .any(|c| matches!(c, Component::ParentDir | Component::CurDir))
    {
        return Err(invalid("path must be absolute and canonical"));
    }
    for p in path.ancestors() {
        let m = fs::symlink_metadata(p)
            .map_err(|e| path_error("inspect protected target path", p, e))?;
        if m.file_type().is_symlink() || m.uid() != 0 || m.mode() & 0o022 != 0 {
            return Err(invalid("path is not root protected"));
        }
    }
    if private
        && fs::metadata(path)
            .map_err(|e| path_error("inspect target state permissions", path, e))?
            .mode()
            & 0o077
            != 0
    {
        return Err(invalid("state directory is not private"));
    }
    Ok(())
}
pub(crate) fn validate_config(c: &LiveTargetConfig) -> io::Result<()> {
    if c.address.parse::<IpAddr>().is_err()
        || !nqn_valid(&c.host_nqn)
        || c.service == 0
        || c.port_id == 0
        || c.commissioning_service == 0
        || c.commissioning_port_id == 0
        || c.port_id == c.commissioning_port_id
        || (c.address.parse::<IpAddr>().ok() == Some(IpAddr::from([127, 0, 0, 1]))
            && c.service == c.commissioning_service)
    {
        return Err(invalid("invalid target configuration"));
    }
    owner_scope(&c.host_nqn)?;
    Ok(())
}
fn check_configfs() -> io::Result<()> {
    // The mount table is consulted on every production open; a normal directory
    // masquerading as nvmet must never turn a disable into a successful no-op.
    let mounts = fs::read_to_string("/proc/self/mountinfo")
        .map_err(|e| path_error("read mount table", Path::new("/proc/self/mountinfo"), e))?;
    let mut covering: Option<(&str, &str)> = None;
    for line in mounts.lines() {
        let Some((before, after)) = line.split_once(" - ") else {
            continue;
        };
        let Some(mount) = before.split_whitespace().nth(4) else {
            continue;
        };
        let Some(kind) = after.split_whitespace().next() else {
            continue;
        };
        if (CONFIGFS == mount
            || CONFIGFS
                .strip_prefix(mount)
                .is_some_and(|s| s.starts_with('/')))
            && covering.is_none_or(|(previous, _)| mount.len() >= previous.len())
        {
            covering = Some((mount, kind));
        }
    }
    if covering.is_none_or(|(_, kind)| kind != "configfs")
        || !fs::metadata(CONFIGFS)
            .map_err(|e| path_error("inspect nvmet configfs", Path::new(CONFIGFS), e))?
            .is_dir()
    {
        return Err(invalid("nvmet is not on mounted configfs"));
    }
    Ok(())
}

impl LiveTargetManager {
    /// Ordinary-file fixture: no kernel target proves target I/O drain.
    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn open_fixture(config: LiveTargetConfig, configfs: PathBuf) -> io::Result<Self> {
        Self::open_inner(
            config,
            configfs,
            true,
            None,
            #[cfg(test)]
            None,
        )
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn open_existing_fixture(
        config: LiveTargetConfig,
        configfs: PathBuf,
        expected: LiveTargetIdentity,
    ) -> io::Result<Self> {
        Self::open_inner(
            config,
            configfs,
            true,
            Some(expected),
            #[cfg(test)]
            None,
        )
    }

    pub fn open(config: LiveTargetConfig) -> io::Result<Self> {
        check_configfs()?;
        check_path(&config.state_dir, true)?;
        Self::open_inner(
            config,
            PathBuf::from(CONFIGFS),
            false,
            None,
            #[cfg(test)]
            None,
        )
    }
    /// Reopen an already registered node; absence or replacement is fatal.
    pub fn open_existing(
        config: LiveTargetConfig,
        expected: LiveTargetIdentity,
    ) -> io::Result<Self> {
        check_configfs()?;
        check_path(&config.state_dir, true)?;
        Self::open_inner(
            config,
            PathBuf::from(CONFIGFS),
            false,
            Some(expected),
            #[cfg(test)]
            None,
        )
    }
    /// `configfs_hook` (tests only) runs before the first configfs mutation,
    /// so crash cuts cover the commissioning steps of opening as well.
    fn open_inner(
        config: LiveTargetConfig,
        configfs: PathBuf,
        fixture: bool,
        expected: Option<LiveTargetIdentity>,
        #[cfg(test)] configfs_hook: Option<ConfigfsHook>,
    ) -> io::Result<Self> {
        Self::open_inner_mode(
            config,
            configfs,
            fixture,
            expected,
            true,
            #[cfg(test)]
            configfs_hook,
        )
    }
    /// `setup = false` reconciles the scope but never creates the host, ports
    /// or commissioning objects again.
    fn open_inner_mode(
        config: LiveTargetConfig,
        configfs: PathBuf,
        fixture: bool,
        expected: Option<LiveTargetIdentity>,
        setup: bool,
        #[cfg(test)] configfs_hook: Option<ConfigfsHook>,
    ) -> io::Result<Self> {
        validate_config(&config)?;
        let scope = owner_scope(&config.host_nqn)?.to_owned();
        let lock_scope = scope
            .strip_prefix(NQN_PREFIX)
            .ok_or_else(|| invalid("invalid target configuration"))?;
        let lock = DbLock::acquire(&config.state_dir, Duration::ZERO)
            .map_err(|e| path_error("lock target state directory", &config.state_dir, e))?;
        let scope_lock = acquire_scope_lock(&configfs, lock_scope, fixture)?;
        let directory = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&config.state_dir)
            .map_err(|e| path_error("open target state directory", &config.state_dir, e))?;
        let dir_meta = directory
            .metadata()
            .map_err(|e| path_error("inspect target state directory", &config.state_dir, e))?;
        if expected.is_some_and(|e| {
            e.directory_device != dir_meta.dev()
                || e.directory_inode != dir_meta.ino()
                || e.registry_id == [0; 16]
        }) {
            return Err(invalid("target state directory identity changed"));
        }
        let legacy = config.state_dir.join(LEGACY_STATE);
        if fs::symlink_metadata(&legacy).is_ok() {
            return Err(invalid(
                "unsupported legacy live target journal live-target.json: shut the owner down with protocol 35 (ShutdownLiveTarget), then remove the live-target state directory",
            ));
        }
        let sealed_path = config.state_dir.join("sealed");
        let (registry_id, sealed_meta) = match read_target_id(&config.state_dir, fixture)? {
            Some((registry_id, sealed_device, sealed_inode)) => {
                let sealed = open_sealed_directory(&sealed_path, fixture)?;
                let meta = sealed
                    .metadata()
                    .map_err(|e| path_error("inspect sealed marker directory", &sealed_path, e))?;
                if meta.dev() != sealed_device || meta.ino() != sealed_inode {
                    return Err(invalid("sealed marker directory identity changed"));
                }
                (registry_id, meta)
            }
            None => {
                if expected.is_some() {
                    return Err(invalid("target state directory identity changed"));
                }
                // A crashed first open may have created the directory before
                // any namespace was exposed. No marker can be owed then, so
                // reuse it only if it is a real, empty directory.
                if fs::symlink_metadata(&sealed_path).is_ok() {
                    let empty = fs::symlink_metadata(&sealed_path).is_ok_and(|m| m.is_dir())
                        && fs::read_dir(&sealed_path)
                            .is_ok_and(|mut entries| entries.next().is_none());
                    if !empty {
                        return Err(invalid("sealed marker directory identity changed"));
                    }
                } else {
                    fs::DirBuilder::new()
                        .mode(0o700)
                        .create(&sealed_path)
                        .map_err(|e| {
                            path_error("create sealed marker directory", &sealed_path, e)
                        })?;
                    directory.sync_all().map_err(|e| {
                        path_error("sync target state directory", &config.state_dir, e)
                    })?;
                }
                let sealed = open_sealed_directory(&sealed_path, fixture)?;
                let meta = sealed
                    .metadata()
                    .map_err(|e| path_error("inspect sealed marker directory", &sealed_path, e))?;
                let registry_id =
                    create_target_id(&config.state_dir, &directory, meta.dev(), meta.ino())?;
                (registry_id, meta)
            }
        };
        if expected.is_some_and(|e| e.registry_id != registry_id) {
            return Err(invalid("target state directory identity changed"));
        }
        if !sealed_meta.is_dir()
            || (!fixture && (sealed_meta.uid() != 0 || sealed_meta.mode() & 0o077 != 0))
        {
            return Err(invalid("invalid sealed marker directory"));
        }
        let sealed_directory = open_sealed_directory(&sealed_path, fixture)?;
        let mut manager = Self {
            configfs,
            config,
            scope,
            registry_id,
            directory,
            sealed_directory,
            sealed_device: sealed_meta.dev(),
            sealed_inode: sealed_meta.ino(),
            exports: Vec::new(),
            pins: HashMap::new(),
            poisoned: false,
            fixture,
            stats: LiveTargetStats {
                fixture_only: fixture,
                ..LiveTargetStats::default()
            },
            commissioning: None,
            _lock: lock,
            _scope_lock: scope_lock,
            #[cfg(test)]
            configfs_hook: std::sync::Mutex::new(configfs_hook),
            #[cfg(test)]
            configfs_steps: std::cell::Cell::new(0),
            #[cfg(test)]
            commission_groups: std::cell::Cell::new(0),
        };
        // Disconnect local controllers of a previous process before any
        // namespace is drained or removed.
        if !fixture {
            let path = manager.config.state_dir.join(COMMISSIONING_DIRECTORY);
            match fs::DirBuilder::new().mode(0o700).create(&path) {
                Ok(()) => manager.directory.sync_all()?,
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => (),
                Err(e) => return Err(path_error("create commissioning directory", &path, e)),
            }
            check_path(&path, true)?;
            manager.commissioning = Some(LiveConnectionRegistry::open(&path)?);
        }
        // Old writable capabilities are never re-enabled: seal every enabled
        // namespace, drain it, and remove every scoped object.
        manager.reconcile()?;
        if setup {
            manager.setup()?;
        }
        Ok(manager)
    }
    /// Caller must durably bind this identity to the node before export.
    pub fn identity(&self) -> io::Result<LiveTargetIdentity> {
        let meta = self.directory.metadata()?;
        Ok(LiveTargetIdentity {
            directory_device: meta.dev(),
            directory_inode: meta.ino(),
            registry_id: self.registry_id,
        })
    }
    /// Install a test hook that runs before every fixture configfs mutation.
    #[cfg(test)]
    pub(crate) fn set_configfs_hook(&self, hook: ConfigfsHook) {
        *self
            .configfs_hook
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(hook);
    }
    fn configfs_step(&self) -> io::Result<()> {
        #[cfg(test)]
        {
            if !self.fixture {
                return Ok(());
            }
            let index = self.configfs_steps.get();
            self.configfs_steps.set(index.saturating_add(1));
            return match self
                .configfs_hook
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_mut()
            {
                Some(hook) => hook(index),
                None => Ok(()),
            };
        }
        #[cfg(not(test))]
        Ok(())
    }
    fn commissioning_host(&self) -> String {
        format!("{}commission", self.scope)
    }
    fn full(&self, relative: &Path) -> io::Result<PathBuf> {
        if relative.as_os_str().is_empty()
            || relative
                .components()
                .any(|c| !matches!(c, Component::Normal(_)))
        {
            return Err(invalid("invalid registered configfs path"));
        }
        Ok(self.configfs.join(relative))
    }
    fn create_dir(&self, relative: &Path) -> io::Result<PathBuf> {
        let full = self.full(relative)?;
        self.configfs_step()?;
        fs::create_dir(&full).map_err(|e| path_error("create configfs directory", &full, e))?;
        // configfs creates a directory's attributes with the directory. The
        // fixture models that before any later step.
        if self.fixture {
            self.fixture_attributes(relative)?;
        }
        Ok(full)
    }
    fn create_link(&self, relative: &Path, destination: &Path) -> io::Result<()> {
        let full = self.full(relative)?;
        let target = self.full(destination)?;
        self.configfs_step()?;
        symlink(&target, &full).map_err(|e| path_error("create configfs link", &full, e))
    }
    fn link_if_absent(&self, relative: &Path, destination: &Path) -> io::Result<()> {
        let full = self.full(relative)?;
        match fs::symlink_metadata(&full) {
            Ok(_) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                self.create_link(relative, destination)
            }
            Err(error) => Err(path_error("inspect configfs link", &full, error)),
        }
    }
    fn remove_link(&self, relative: &Path) -> io::Result<()> {
        let full = self.full(relative)?;
        self.configfs_step()?;
        fs::remove_file(&full).map_err(|e| path_error("remove configfs link", &full, e))
    }
    fn remove_link_if_present(&self, relative: &Path) -> io::Result<()> {
        let full = self.full(relative)?;
        match fs::symlink_metadata(&full) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(path_error("inspect configfs link", &full, error)),
            Ok(_) => self.remove_link(relative),
        }
    }
    fn remove_dir(&self, relative: &Path) -> io::Result<()> {
        let full = self.full(relative)?;
        // Fail before the fixture child cleanup, so a crash leaves a complete
        // object that the next scan can recognize instead of an empty shell.
        self.configfs_step()?;
        if self.fixture {
            self.fixture_remove_attributes(relative)?;
        }
        fs::remove_dir(&full).map_err(|e| path_error("remove configfs directory", &full, e))
    }
    fn put(&self, relative: &Path, value: &str) -> io::Result<()> {
        let path = self.full(relative)?;
        self.configfs_step()?;
        // Only the write-protection marker is a fixture-created attribute;
        // every other attribute must already exist, as in configfs.
        let mut file = if self.fixture
            && relative
                .file_name()
                .is_some_and(|name| name == "write_protect")
        {
            OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .mode(0o600)
                .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
                .open(&path)
                .map_err(|e| path_error("open target attribute for write", &path, e))?
        } else {
            OpenOptions::new()
                .write(true)
                .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
                .open(&path)
                .map_err(|e| path_error("open target attribute for write", &path, e))?
        };
        file.write_all(value.as_bytes())
            .map_err(|e| path_error("write target attribute", &path, e))?;
        if attribute(&path)? != value {
            return Err(invalid("configfs attribute readback mismatch"));
        }
        Ok(())
    }
    fn setup(&mut self) -> io::Result<()> {
        let host = PathBuf::from("hosts").join(&self.config.host_nqn);
        let private = PathBuf::from("hosts").join(self.commissioning_host());
        let public_port = PathBuf::from("ports").join(self.config.port_id.to_string());
        let private_port =
            PathBuf::from("ports").join(self.config.commissioning_port_id.to_string());
        for p in [&host, &private, &public_port, &private_port] {
            self.create_dir(p)?;
        }
        let (af, addr) = match self
            .config
            .address
            .parse::<IpAddr>()
            .map_err(|_| invalid("invalid address"))?
        {
            IpAddr::V4(a) => ("ipv4", a.to_string()),
            IpAddr::V6(a) => ("ipv6", a.to_string()),
        };
        let transport = match self.config.transport {
            NvmeofTransport::Tcp => "tcp",
            NvmeofTransport::Rdma => "rdma",
        };
        for (p, tr, family, address, service) in [
            (
                &public_port,
                transport,
                af,
                addr.as_str(),
                self.config.service,
            ),
            (
                &private_port,
                "tcp",
                "ipv4",
                "127.0.0.1",
                self.config.commissioning_service,
            ),
        ] {
            let service = service.to_string();
            for (key, value) in [
                ("addr_trtype", tr),
                ("addr_adrfam", family),
                ("addr_traddr", address),
                ("addr_trsvcid", service.as_str()),
            ] {
                self.put(&p.join(key), value)?;
            }
        }
        Ok(())
    }
    fn file(&self, identity: &LiveFileIdentity) -> io::Result<File> {
        if identity.bytes == 0
            || identity.block_size < 512
            || identity.block_size > 4096
            || !identity.block_size.is_power_of_two()
            || !identity
                .bytes
                .is_multiple_of(u64::from(identity.block_size))
            || !identity.path.is_absolute()
            || identity.path.canonicalize()? != identity.path
        {
            return Err(invalid("invalid WAL namespace geometry or path"));
        }
        if !self.fixture {
            check_path(&identity.path, false)?;
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&identity.path)?;
        let m = file.metadata()?;
        if !m.is_file()
            || m.nlink() != 1
            || m.dev() != identity.device
            || m.ino() != identity.inode
            || m.len() != identity.bytes
            || m.blocks().saturating_mul(512) < identity.bytes
        {
            return Err(invalid(
                "WAL backing file identity, size or allocation changed",
            ));
        }
        Ok(file)
    }
    pub fn endpoint(&self) -> (String, u16, NvmeofTransport) {
        (
            self.config.address.clone(),
            self.config.service,
            self.config.transport,
        )
    }

    /// Export `file` under its deterministic subsystem forever (until the file
    /// is retired). A sealed file may only be exported read-only: the read-only
    /// commissioning order leaves the namespace unreachable from the public
    /// host and port until write protection is verified.
    pub fn export_permanent(
        &mut self,
        name: &super::live_storage::WalExportName,
        file: LiveFileIdentity,
        readonly: bool,
    ) -> io::Result<LiveTargetBinding> {
        if self.poisoned {
            return Err(invalid("target manager is quarantined"));
        }
        if let Some(export) = self
            .exports
            .iter()
            .find(|export| export.file.device == file.device && export.file.inode == file.inode)
        {
            if export.binding.uuid == name.uuid {
                return if readonly || !export.readonly {
                    Ok(export.binding.clone())
                } else {
                    Err(invalid("fenced WAL cannot be exported writable again"))
                };
            }
            return Err(invalid("WAL file already exported"));
        }
        // A durable seal marker means this path was fenced or retired: never
        // restore a writable capability.
        if !readonly && self.sealed_file(&file.path)?.is_some() {
            return Err(invalid("fenced WAL cannot be exported writable again"));
        }
        let result = self.export_named(&name.nqn, name.uuid, name.nsid, file, readonly);
        if result.is_err() {
            self.poisoned = true;
            self.stats.errors = self.stats.errors.saturating_add(1);
        }
        result
    }

    /// Export one file as one permanent namespace; the binding stays until the
    /// file is retired.
    fn export_named(
        &mut self,
        nqn: &str,
        id: [u8; 16],
        nsid: u32,
        file: LiveFileIdentity,
        readonly: bool,
    ) -> io::Result<LiveTargetBinding> {
        let pin = self.file(&file)?;
        let sub = PathBuf::from("subsystems").join(nqn);
        let sub_full = self.full(&sub)?;
        let sub_exists = fs::symlink_metadata(&sub_full).is_ok();
        let public_link = PathBuf::from("ports")
            .join(self.config.port_id.to_string())
            .join("subsystems")
            .join(nqn);
        let host_link = sub.join("allowed_hosts").join(&self.config.host_nqn);
        if !sub_exists {
            self.create_dir(&sub)?;
            self.put(&sub.join("attr_allow_any_host"), "0")?;
        }
        if !readonly {
            let host = PathBuf::from("hosts").join(&self.config.host_nqn);
            self.link_if_absent(&host_link, &host)?;
            self.link_if_absent(&public_link, &sub)?;
        } else {
            // A sealed file joins an existing subsystem only after the public
            // host and port can no longer reach it.
            self.remove_link_if_present(&public_link)?;
            self.remove_link_if_present(&host_link)?;
        }
        let binding = LiveTargetBinding {
            nqn: nqn.to_owned(),
            uuid: id,
            nsid,
            bytes: file.bytes,
            block_size: file.block_size,
            address: self.config.address.clone(),
            service: self.config.service,
            transport: self.config.transport,
            readonly,
            permanent: true,
        };
        let ns = sub.join(format!("namespaces/{nsid}"));
        self.create_dir(&ns)?;
        let path = file
            .path
            .to_str()
            .ok_or_else(|| invalid("WAL path is not UTF-8"))?;
        self.put(&ns.join("device_path"), path)?;
        self.put(&ns.join("device_uuid"), &uuid(id))?;
        self.put(&ns.join("buffered_io"), "1")?;
        // Recheck the path and the retained descriptor immediately before enable.
        drop(self.file(&file)?);
        let pinned = pin.metadata()?;
        if pinned.dev() != file.device || pinned.ino() != file.inode || pinned.len() != file.bytes {
            return Err(invalid("pinned WAL changed"));
        }
        self.put(&ns.join("enable"), "1")?;
        if readonly {
            let private_host = PathBuf::from("hosts").join(self.commissioning_host());
            self.link_if_absent(
                &sub.join("allowed_hosts").join(self.commissioning_host()),
                &private_host,
            )?;
            let private_link = PathBuf::from("ports")
                .join(self.config.commissioning_port_id.to_string())
                .join("subsystems")
                .join(nqn);
            self.link_if_absent(&private_link, &sub)?;
            if !self.fixture {
                self.commission(&[&binding])?;
                self.stats.readonly_commission_successes =
                    self.stats.readonly_commission_successes.saturating_add(1);
            }
            self.remove_link(&private_link)?;
            self.remove_link(&sub.join("allowed_hosts").join(self.commissioning_host()))?;
            let host = PathBuf::from("hosts").join(&self.config.host_nqn);
            self.create_link(&host_link, &host)?;
            self.create_link(&public_link, &sub)?;
        }
        self.exports.push(Export {
            binding: binding.clone(),
            file,
            readonly,
        });
        self.pins.insert(id, pin);
        self.stats.namespaces_exposed = self.stats.namespaces_exposed.saturating_add(1);
        Ok(binding)
    }

    /// Commission every binding of one subsystem through one controller. All
    /// namespace devices are resolved and write-protected before the single
    /// disconnect, so a namespace scan cannot outlive the controller removal.
    fn commission(&mut self, bindings: &[&LiveTargetBinding]) -> io::Result<()> {
        let first = bindings
            .first()
            .ok_or_else(|| invalid("empty commissioning group"))?;
        let local = LiveConnectTarget {
            nqn: first.nqn.clone(),
            uuid: first.uuid,
            nsid: first.nsid,
            bytes: first.bytes,
            block_size: first.block_size,
            address: "127.0.0.1".to_owned(),
            service: self.config.commissioning_service,
            transport: NvmeofTransport::Tcp,
            geometry: Default::default(),
        };
        let deadline = Instant::now() + Duration::from_secs(20);
        let host_nqn = self.commissioning_host();
        let (mut controller, _namespace) = self
            .commissioning
            .as_mut()
            .ok_or_else(|| invalid("missing commissioning registry"))?
            .connect(
                &local,
                &host_nqn,
                random_id()?,
                LiveNamespaceAccess::ReadWrite,
                deadline,
            )?;
        let checked = bindings.iter().try_for_each(|binding| {
            controller.set_read_only_member(binding.nsid, binding.uuid, deadline)
        });
        let disconnect = controller.disconnect(Instant::now() + Duration::from_secs(20));
        checked?;
        disconnect
    }

    /// Write-protect every namespace of `bindings` in the device, then mark
    /// their files sealed. Exports stay alive and readable. The caller holds
    /// the lifecycle gate and passes the bindings currently marked writable.
    pub fn write_protect(&mut self, bindings: &[LiveTargetBinding]) -> io::Result<()> {
        if bindings.is_empty() {
            return Ok(());
        }
        if self.poisoned {
            return Err(invalid("target manager is quarantined"));
        }
        let result = self.write_protect_inner(bindings);
        if result.is_err() {
            self.poisoned = true;
            self.stats.errors = self.stats.errors.saturating_add(1);
        }
        result
    }
    fn write_protect_inner(&mut self, bindings: &[LiveTargetBinding]) -> io::Result<()> {
        let mut by_subsystem: BTreeMap<&str, Vec<&LiveTargetBinding>> = BTreeMap::new();
        for binding in bindings {
            if !self.exports.iter().any(|export| export.binding == *binding) {
                return Err(invalid("unknown target binding"));
            }
            by_subsystem
                .entry(binding.nqn.as_str())
                .or_default()
                .push(binding);
        }
        for (nqn, group) in &by_subsystem {
            let sub = PathBuf::from("subsystems").join(nqn);
            let private_host = PathBuf::from("hosts").join(self.commissioning_host());
            let private_host_link = sub.join("allowed_hosts").join(self.commissioning_host());
            self.link_if_absent(&private_host_link, &private_host)?;
            let private_link = PathBuf::from("ports")
                .join(self.config.commissioning_port_id.to_string())
                .join("subsystems")
                .join(nqn);
            self.link_if_absent(&private_link, &sub)?;
            #[cfg(test)]
            self.commission_groups
                .set(self.commission_groups.get().saturating_add(1));
            let mut failure = None;
            if self.fixture {
                for binding in group {
                    if let Err(error) = self.put(
                        &sub.join(format!("namespaces/{}", binding.nsid))
                            .join("write_protect"),
                        "1",
                    ) {
                        failure = Some(error);
                        break;
                    }
                }
            } else {
                let started = Instant::now();
                match self.commission(group) {
                    Ok(()) => {
                        self.stats.write_protect_successes =
                            self.stats.write_protect_successes.saturating_add(1);
                        self.stats.write_protect_time_ns =
                            self.stats.write_protect_time_ns.saturating_add(
                                started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
                            );
                    }
                    Err(error) => failure = Some(error),
                }
            }
            self.remove_link(&private_link)?;
            self.remove_link(&private_host_link)?;
            if let Some(error) = failure {
                return Err(error);
            }
        }
        for binding in bindings {
            let export = self
                .exports
                .iter()
                .find(|export| export.binding == *binding)
                .ok_or_else(|| invalid("unknown target binding"))?;
            self.write_sealed(&export.file)?;
        }
        self.sealed_directory.sync_all()?;
        for binding in bindings {
            if let Some(export) = self
                .exports
                .iter_mut()
                .find(|export| export.binding == *binding)
            {
                export.readonly = true;
                export.binding.readonly = true;
            }
        }
        self.stats.fence_count = self.stats.fence_count.saturating_add(1);
        Ok(())
    }

    /// Seal and remove one export. The subsystem goes with its last namespace.
    pub fn revoke(&mut self, binding: &LiveTargetBinding) -> io::Result<()> {
        self.revoke_all(&[binding])
    }
    /// Revoke several bindings with one catalog pass.
    pub fn revoke_all(&mut self, bindings: &[&LiveTargetBinding]) -> io::Result<()> {
        if bindings.is_empty() {
            return Ok(());
        }
        if self.poisoned {
            return Err(invalid("target manager is quarantined"));
        }
        if bindings
            .iter()
            .any(|binding| !self.exports.iter().any(|e| e.binding == **binding))
        {
            return Err(invalid("unknown target binding"));
        }
        let result = self.revoke_all_inner(bindings);
        if result.is_err() {
            self.poisoned = true;
            self.stats.errors = self.stats.errors.saturating_add(1);
        }
        result
    }
    fn revoke_all_inner(&mut self, bindings: &[&LiveTargetBinding]) -> io::Result<()> {
        let mut sealed = false;
        for binding in bindings {
            let export = self
                .exports
                .iter()
                .find(|export| export.binding == **binding)
                .ok_or_else(|| invalid("unknown target binding"))?;
            let file = export.file.clone();
            let readonly = export.readonly;
            let ns = PathBuf::from("subsystems")
                .join(&binding.nqn)
                .join(format!("namespaces/{}", binding.nsid));
            let ns_full = self.full(&ns)?;
            if fs::symlink_metadata(&ns_full).is_ok() {
                let enabled = optional_attribute(&ns_full.join("enable"))?.as_deref() == Some("1");
                if enabled {
                    if attribute(&ns_full.join("device_uuid"))? != uuid(binding.uuid)
                        || attribute(&ns_full.join("device_path"))?
                            != file
                                .path
                                .to_str()
                                .ok_or_else(|| invalid("invalid WAL path"))?
                    {
                        return Err(invalid("namespace identity changed"));
                    }
                    // The namespace name alone cannot authenticate its
                    // backing inode after a crash or pathname replacement.
                    drop(self.file(&file)?);
                }
                // A file that was ever enabled writable must never regain a
                // writable capability, even after its export is removed.
                if !readonly {
                    self.write_sealed(&file)?;
                    sealed = true;
                }
                if enabled {
                    self.put(&ns.join("enable"), "0")?;
                }
                self.remove_dir(&ns)?;
            }
        }
        if sealed {
            self.sealed_directory.sync_all()?;
        }
        for binding in bindings {
            self.exports
                .retain(|export| export.binding.uuid != binding.uuid);
            self.pins.remove(&binding.uuid);
        }
        let mut empty = Vec::new();
        for binding in bindings {
            if !empty.contains(&binding.nqn)
                && !self
                    .exports
                    .iter()
                    .any(|export| export.binding.nqn == binding.nqn)
            {
                empty.push(binding.nqn.clone());
            }
        }
        for nqn in empty {
            let sub = PathBuf::from("subsystems").join(&nqn);
            for port in [self.config.port_id, self.config.commissioning_port_id] {
                self.remove_link_if_present(
                    &PathBuf::from("ports")
                        .join(port.to_string())
                        .join("subsystems")
                        .join(&nqn),
                )?;
            }
            let allowed = sub.join("allowed_hosts");
            if let Ok(entries) = fs::read_dir(self.full(&allowed)?) {
                let names: Vec<_> = entries
                    .filter_map(Result::ok)
                    .map(|e| e.file_name())
                    .collect();
                for name in names {
                    self.remove_link(&allowed.join(name))?;
                }
            }
            // `namespaces` and `allowed_hosts` are configfs default groups: the
            // kernel refuses to remove them (EPERM) and deletes them with the
            // subsystem directory.
            if fs::symlink_metadata(self.full(&sub)?).is_ok() {
                self.remove_dir(&sub)?;
            }
        }
        Ok(())
    }

    /// Current process counters and extant owned namespaces. Fixture counters
    /// never claim actual kernel disable/drain or RO commissioning.
    pub fn stats(&self) -> io::Result<LiveTargetStats> {
        let mut stats = self.stats.clone();
        stats.owned_namespace_count = 0;
        for export in &self.exports {
            let ns = PathBuf::from("subsystems")
                .join(&export.binding.nqn)
                .join(format!("namespaces/{}", export.binding.nsid));
            if fs::symlink_metadata(self.full(&ns)?).is_ok() {
                stats.owned_namespace_count = stats.owned_namespace_count.saturating_add(1);
            }
        }
        Ok(stats)
    }

    /// Disable and drain every enabled namespace in this scope, then remove
    /// every owned configfs object and port. Objects of another scope are
    /// never touched, and a port shared with a foreign subsystem is refused.
    fn marker_path(&self, wal_path: &Path) -> io::Result<PathBuf> {
        if !wal_path.is_absolute()
            || wal_path
                .components()
                .any(|c| matches!(c, Component::CurDir | Component::ParentDir))
        {
            return Err(invalid("invalid sealed WAL path"));
        }
        let directory = self.config.state_dir.join("sealed");
        let meta = fs::symlink_metadata(&directory)?;
        let pinned = self.sealed_directory.metadata()?;
        if !meta.is_dir()
            || meta.dev() != pinned.dev()
            || meta.ino() != pinned.ino()
            || meta.dev() != self.sealed_device
            || meta.ino() != self.sealed_inode
        {
            return Err(invalid("sealed marker directory was replaced"));
        }
        let digest = blake2b_simd::Params::new()
            .hash_length(32)
            .hash(wal_path.as_os_str().as_bytes());
        let mut name = String::with_capacity(64);
        use std::fmt::Write as _;
        for byte in digest.as_bytes() {
            write!(&mut name, "{byte:02x}").map_err(|_| invalid("marker name encoding failed"))?;
        }
        Ok(directory.join(name))
    }
    /// Bounded path-keyed lookup of an exact retained WAL identity for owner GC.
    pub fn sealed_file(&self, wal_path: &Path) -> io::Result<Option<LiveFileIdentity>> {
        let path = self.marker_path(wal_path)?;
        let file = match OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(path)
        {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        let meta = file.metadata()?;
        if !meta.is_file()
            || meta.len() > 65536
            || (!self.fixture && (meta.uid() != 0 || meta.mode() & 0o077 != 0))
        {
            return Err(invalid("invalid sealed WAL marker"));
        }
        let mut bytes = Vec::new();
        file.take(65537).read_to_end(&mut bytes)?;
        let (checksum, payload) = bytes
            .split_at_checked(4)
            .ok_or_else(|| invalid("truncated sealed WAL marker"))?;
        let checksum = u32::from_le_bytes(
            checksum
                .try_into()
                .map_err(|_| invalid("invalid sealed WAL checksum"))?,
        );
        if crc32fast::hash(payload) != checksum {
            return Err(invalid("sealed WAL marker checksum mismatch"));
        }
        let identity: LiveFileIdentity =
            serde_json::from_slice(payload).map_err(|_| invalid("invalid sealed WAL marker"))?;
        if identity.path != wal_path {
            return Err(invalid("sealed WAL marker path mismatch"));
        }
        Ok(Some(identity))
    }
    /// The caller syncs `sealed_directory` once after its marker batch and
    /// before it retires the matching exports.
    fn write_sealed(&self, identity: &LiveFileIdentity) -> io::Result<()> {
        if let Some(old) = self.sealed_file(&identity.path)? {
            // A preceding process may have published the marker but died
            // before its directory barrier; the caller's barrier covers it.
            return if old == *identity {
                Ok(())
            } else {
                Err(invalid("sealed WAL path identity changed"))
            };
        }
        let destination = self.marker_path(&identity.path)?;
        let temp = self
            .config
            .state_dir
            .join("sealed")
            .join(format!(".pending-{}", uuid(random_id()?)));
        let data =
            serde_json::to_vec(identity).map_err(|_| invalid("cannot encode sealed WAL marker"))?;
        if data.len() + 4 > 65536 {
            return Err(invalid("sealed WAL marker too large"));
        }
        let result = (|| {
            let mut output = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
                .open(&temp)?;
            output.write_all(&crc32fast::hash(&data).to_le_bytes())?;
            output.write_all(&data)?;
            output.sync_all()?;
            fs::hard_link(&temp, &destination)?;
            fs::remove_file(&temp)
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temp);
        }
        result
    }

    /// After owner GC has revoked every namespace, drained all read pins, and
    /// unlinked this exact singly-linked WAL without renaming or hard-linking
    /// it, release the inode tombstone. The directory sync precedes the durable
    /// tombstone update, so a crash cannot resurrect an untracked old file.
    pub fn retire_deleted_file(&mut self, file: &LiveFileIdentity) -> io::Result<()> {
        if self.poisoned {
            return Err(invalid("target manager is quarantined"));
        }
        if self
            .exports
            .iter()
            .any(|e| e.file.device == file.device && e.file.inode == file.inode)
        {
            return Err(invalid("WAL namespace is still registered"));
        }
        if self.sealed_file(&file.path)?.as_ref() != Some(file) {
            return Err(invalid("unknown fenced WAL identity"));
        }
        match fs::symlink_metadata(&file.path) {
            Err(e) if e.kind() == io::ErrorKind::NotFound => (),
            Err(e) => return Err(e),
            Ok(_) => return Err(invalid("WAL path still exists")),
        }
        let parent = file
            .path
            .parent()
            .ok_or_else(|| invalid("WAL has no parent directory"))?;
        if !self.fixture {
            check_path(parent, false)?;
        }
        OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(parent)?
            .sync_all()?;
        let marker = self.marker_path(&file.path)?;
        let result = self
            .configfs_step()
            .and_then(|()| fs::remove_file(marker))
            .and_then(|()| self.sealed_directory.sync_all());
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }
    /// Explicit shutdown; callers must first retire initiator read pins.
    pub fn shutdown(&mut self) -> io::Result<()> {
        let result = (|| {
            if !self.fixture {
                // A failed or timed-out commissioning attempt may still own a
                // controller. Reopening the registry reconciles its durable
                // intents before the scoped objects are removed.
                let path = self.config.state_dir.join(COMMISSIONING_DIRECTORY);
                drop(self.commissioning.take());
                self.commissioning = Some(LiveConnectionRegistry::open(&path)?);
            }
            self.reconcile()
        })();
        self.poisoned = true;
        if result.is_err() {
            self.stats.errors = self.stats.errors.saturating_add(1);
        }
        result
    }
    // Ordinary filesystem attributes are exclusively a unit-test model. They
    // do not model kernel I/O or provide evidence of target drain semantics.
    fn fixture_attributes(&self, rel: &Path) -> io::Result<()> {
        let p = self.full(rel)?;
        let parts: Vec<_> = rel
            .components()
            .map(|p| p.as_os_str().to_string_lossy().into_owned())
            .collect();
        if parts.len() == 2 && parts[0] == "ports" {
            fs::create_dir(p.join("subsystems"))?;
            for name in ["addr_trtype", "addr_adrfam", "addr_traddr", "addr_trsvcid"] {
                fs::write(p.join(name), "")?;
            }
        } else if parts.len() == 2 && parts[0] == "subsystems" {
            fs::create_dir(p.join("namespaces"))?;
            fs::create_dir(p.join("allowed_hosts"))?;
            fs::write(p.join("attr_allow_any_host"), "0")?;
        } else if parts.len() == 4 && parts[0] == "subsystems" && parts[2] == "namespaces" {
            for name in ["device_path", "device_uuid", "buffered_io", "enable"] {
                fs::write(p.join(name), if name == "enable" { "0" } else { "" })?;
            }
        }
        Ok(())
    }
    fn fixture_remove_attributes(&self, rel: &Path) -> io::Result<()> {
        let p = self.full(rel)?;
        if fs::symlink_metadata(&p)?.file_type().is_symlink() {
            return Ok(());
        }
        for entry in fs::read_dir(p)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                self.fixture_remove_attributes(&rel.join(entry.file_name()))?;
                fs::remove_dir(entry.path())?;
            } else if entry.file_type()?.is_file() {
                fs::remove_file(entry.path())?;
            } else {
                return Err(invalid("unexpected fixture object"));
            }
        }
        Ok(())
    }
}

/// Exclusive, non-blocking ownership of one configfs scope. A second process
/// with the same database and partition cannot scan or delete this owner's
/// objects.
fn acquire_scope_lock(configfs: &Path, scope: &str, fixture: bool) -> io::Result<File> {
    let name = format!("tidehunter-nvmet-{scope}.lock");
    let directory = if fixture {
        let directory = configfs.join(".locks");
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(&directory)
            .map_err(|e| path_error("create configfs lock directory", &directory, e))?;
        directory
    } else {
        PathBuf::from("/run/lock")
    };
    let path = directory.join(name);
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(&path)
        .map_err(|e| path_error("open configfs scope lock", &path, e))?;
    // SAFETY: flock operates on the live lock file descriptor.
    if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } < 0 {
        let error = io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::EWOULDBLOCK) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("another live target owns configfs scope {scope}"),
            ));
        }
        return Err(path_error("lock configfs scope", &path, error));
    }
    Ok(file)
}

/// Read the registry identity and the sealed-marker directory identity, which
/// are written together once when the state directory is created.
fn read_target_id(state_dir: &Path, fixture: bool) -> io::Result<Option<([u8; 16], u64, u64)>> {
    let path = state_dir.join(TARGET_ID);
    let mut file = match OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(&path)
    {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(path_error("open target identity", &path, error)),
    };
    let meta = file.metadata()?;
    if !meta.is_file()
        || meta.len() != 32
        || (!fixture && (meta.uid() != 0 || meta.mode() & 0o077 != 0))
    {
        return Err(invalid("invalid target identity file"));
    }
    let mut bytes = [0; 32];
    file.read_exact(&mut bytes)
        .map_err(|e| path_error("read target identity", &path, e))?;
    let mut id = [0; 16];
    id.copy_from_slice(&bytes[..16]);
    if id == [0; 16] {
        return Err(invalid("invalid target identity file"));
    }
    let device = u64::from_le_bytes(
        bytes[16..24]
            .try_into()
            .map_err(|_| invalid("invalid target identity file"))?,
    );
    let inode = u64::from_le_bytes(
        bytes[24..32]
            .try_into()
            .map_err(|_| invalid("invalid target identity file"))?,
    );
    Ok(Some((id, device, inode)))
}

/// Write the identity once, at state-directory creation: a temporary file is
/// synced, renamed, and its directory synced.
fn create_target_id(
    state_dir: &Path,
    directory: &File,
    sealed_device: u64,
    sealed_inode: u64,
) -> io::Result<[u8; 16]> {
    let id = random_id()?;
    let temp = state_dir.join(".target-id.pending");
    let path = state_dir.join(TARGET_ID);
    let mut bytes = [0; 32];
    bytes[..16].copy_from_slice(&id);
    bytes[16..24].copy_from_slice(&sealed_device.to_le_bytes());
    bytes[24..32].copy_from_slice(&sealed_inode.to_le_bytes());
    let result = (|| {
        let mut output = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&temp)
            .map_err(|e| path_error("open pending target identity", &temp, e))?;
        output
            .write_all(&bytes)
            .and_then(|()| output.sync_all())
            .map_err(|e| path_error("write pending target identity", &temp, e))?;
        fs::rename(&temp, &path).map_err(|e| path_error("publish target identity", &path, e))?;
        directory
            .sync_all()
            .map_err(|e| path_error("sync target state directory", state_dir, e))
    })();
    match result {
        Ok(()) => Ok(id),
        Err(error) => {
            let _ = fs::remove_file(&temp);
            Err(error)
        }
    }
}

fn open_sealed_directory(path: &Path, fixture: bool) -> io::Result<File> {
    let directory = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)
        .map_err(|e| path_error("open sealed marker directory", path, e))?;
    let meta = directory
        .metadata()
        .map_err(|e| path_error("inspect sealed marker directory", path, e))?;
    if !meta.is_dir() || (!fixture && (meta.uid() != 0 || meta.mode() & 0o077 != 0)) {
        return Err(invalid("invalid sealed marker directory"));
    }
    Ok(directory)
}

#[cfg(test)]
#[path = "live_target_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "live_target_legacy_tests.rs"]
mod legacy_tests;
