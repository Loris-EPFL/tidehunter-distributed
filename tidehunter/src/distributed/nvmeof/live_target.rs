//! Owned, file-backed live nvmet namespaces.
//!
//! The caller serializes this manager under the owner's node lifecycle gate,
//! never under the node inventory mutex: every registry save syncs a file and
//! its directory. The caller retains WAL/read pins until revocation and
//! initiator CQ retirement. The
//! state directory and WAL paths must be protected from untrusted local users.
//! The NVMe data plane assumes trusted, non-Byzantine initiators: namespace
//! write protection can be changed by an authorized controller.
use super::live_connect::{LiveConnectTarget, LiveConnectionRegistry};
use super::live_namespace::LiveNamespaceAccess;
use super::live_storage::LiveTargetBinding;
use super::{LiveTargetConfig, LiveTargetStats, NvmeofTransport};
use crate::lock::DbLock;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::net::IpAddr;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, symlink};
use std::path::{Component, Path, PathBuf};
use std::time::{Duration, Instant};

#[path = "live_target/retirement.rs"]
mod retirement;
#[path = "live_target/teardown.rs"]
mod teardown;

const CONFIGFS: &str = "/sys/kernel/config/nvmet";
const STATE: &str = "live-target.json";
const MAX_STATE: u64 = 4 * 1024 * 1024;

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

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
enum Kind {
    Directory,
    Link,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Object {
    // Each path is constructed locally from validated identities, never from RPC.
    path: PathBuf,
    group: [u8; 16],
    kind: Kind,
    identity: Option<(u64, u64)>,
    destination: Option<PathBuf>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Export {
    binding: LiveTargetBinding,
    file: LiveFileIdentity,
    readonly: bool,
}
#[derive(Serialize, Deserialize)]
struct State {
    version: u32,
    config: LiveTargetConfig,
    registry_id: [u8; 16],
    private_host: String,
    objects: Vec<Object>,
    exports: Vec<Export>,
    sealed_directory_device: u64,
    sealed_directory_inode: u64,
}

/// A persistence step of the target state journal, for crash-cut tests.
#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SaveStep {
    /// The pending registry is written and synced; the old one is current.
    Prepared,
    /// The new registry is renamed into place and its directory is synced.
    Published,
}

/// Test hook run at each `SaveStep`. An error is returned from `save` as if
/// the process died there; blocking pauses the save with the gate held.
#[cfg(test)]
pub(crate) type SaveHook = Box<dyn FnMut(SaveStep) -> io::Result<()> + Send>;

/// Open only while holding the owner node lock, before opening any WAL.
/// `open` removes all prior port links and synchronously disables/drains every
/// registered namespace. Any uncertainty is an error, not permission to recover.
pub struct LiveTargetManager {
    configfs: PathBuf,
    directory: File,
    sealed_directory: File,
    state: State,
    pins: HashMap<[u8; 16], File>,
    poisoned: bool,
    fixture: bool,
    stats: LiveTargetStats,
    commissioning: Option<LiveConnectionRegistry>,
    _lock: DbLock,
    #[cfg(test)]
    save_hook: std::sync::Mutex<Option<SaveHook>>,
}

fn invalid(s: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, s)
}
/// A registry written before the refactor carries the removed `spdk` key in
/// every export binding (`null` when unused), which the current binding
/// rejects. Name that mode instead of reporting a corrupt registry.
fn unparsable_state_error(payload: &[u8]) -> io::Error {
    let mode = serde_json::from_slice::<serde_json::Value>(payload)
        .ok()
        .and_then(|state| {
            state.get("exports")?.as_array()?.iter().find_map(|export| {
                let spdk = export.get("binding")?.get("spdk")?;
                Some(if spdk.is_null() {
                    "pre_refactor_live_target_registry"
                } else {
                    "spdk"
                })
            })
        });
    match mode {
        Some(mode) => io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported legacy distributed mode {mode}: use archive/multi-design"),
        ),
        None => invalid("invalid target state"),
    }
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
fn object_id(path: &Path, kind: Kind) -> io::Result<(u64, u64)> {
    let m = fs::symlink_metadata(path).map_err(|e| path_error("inspect target object", path, e))?;
    if (kind == Kind::Directory && !m.file_type().is_dir())
        || (kind == Kind::Link && !m.file_type().is_symlink())
    {
        return Err(invalid("configfs object type changed"));
    }
    Ok((m.dev(), m.ino()))
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
    /// Journal-only fixture: ordinary files do not prove kernel target I/O drain.
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
    /// `save_hook` (tests only) runs from the very first journal save, so
    /// crash cuts cover the persistence steps of opening as well.
    fn open_inner(
        config: LiveTargetConfig,
        configfs: PathBuf,
        fixture: bool,
        expected: Option<LiveTargetIdentity>,
        #[cfg(test)] save_hook: Option<SaveHook>,
    ) -> io::Result<Self> {
        Self::open_inner_mode(
            config,
            configfs,
            fixture,
            expected,
            true,
            #[cfg(test)]
            save_hook,
        )
    }
    /// `setup = false` reconciles and cleans the journal but never creates
    /// the host, ports or commissioning objects again.
    fn open_inner_mode(
        config: LiveTargetConfig,
        configfs: PathBuf,
        fixture: bool,
        expected: Option<LiveTargetIdentity>,
        setup: bool,
        #[cfg(test)] save_hook: Option<SaveHook>,
    ) -> io::Result<Self> {
        validate_config(&config)?;
        let lock = DbLock::acquire(&config.state_dir, Duration::ZERO)
            .map_err(|e| path_error("lock target state directory", &config.state_dir, e))?;
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
        let state_path = config.state_dir.join(STATE);
        let state = match OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&state_path)
        {
            Ok(file) => {
                let m = file.metadata()?;
                if !m.is_file()
                    || m.len() > MAX_STATE
                    || (!fixture && (m.uid() != 0 || m.mode() & 0o077 != 0))
                {
                    return Err(invalid("invalid target state file"));
                }
                let mut bytes = Vec::new();
                file.take(MAX_STATE + 1).read_to_end(&mut bytes)?;
                let (checksum, payload) = bytes
                    .split_at_checked(4)
                    .ok_or_else(|| invalid("truncated target state"))?;
                let expected_checksum = u32::from_le_bytes(
                    checksum
                        .try_into()
                        .map_err(|_| invalid("invalid target checksum"))?,
                );
                if crc32fast::hash(payload) != expected_checksum {
                    return Err(invalid("target state checksum mismatch"));
                }
                let s: State =
                    serde_json::from_slice(payload).map_err(|_| unparsable_state_error(payload))?;
                if s.version != 1
                    || s.config != config
                    || !nqn_valid(&s.private_host)
                    || s.registry_id == [0; 16]
                    || expected.is_some_and(|e| e.registry_id != s.registry_id)
                {
                    return Err(invalid(
                        "target state configuration or registry identity mismatch",
                    ));
                }
                s
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound && expected.is_none() => {
                let path = config.state_dir.join("sealed");
                match fs::DirBuilder::new().mode(0o700).create(&path) {
                    Ok(()) => {}
                    // A first open that died before its first registry save
                    // left this directory behind. No registry was durable, so
                    // no export was ever exposed and no marker can be owed:
                    // reuse the directory only if it is a real, empty one.
                    Err(error)
                        if error.kind() == io::ErrorKind::AlreadyExists
                            && fs::symlink_metadata(&path).is_ok_and(|m| m.is_dir())
                            && fs::read_dir(&path)
                                .is_ok_and(|mut entries| entries.next().is_none()) => {}
                    Err(error) => {
                        return Err(path_error("create sealed marker directory", &path, error));
                    }
                }
                directory
                    .sync_all()
                    .map_err(|e| path_error("sync target state directory", &config.state_dir, e))?;
                let meta = fs::symlink_metadata(&path)
                    .map_err(|e| path_error("inspect sealed marker directory", &path, e))?;
                State {
                    version: 1,
                    config,
                    registry_id: random_id()?,
                    private_host: format!(
                        "nqn.2026-01.local.tidehunter:commission-{}",
                        uuid(random_id()?)
                    ),
                    objects: Vec::new(),
                    exports: Vec::new(),
                    sealed_directory_device: meta.dev(),
                    sealed_directory_inode: meta.ino(),
                }
            }
            Err(e) => return Err(path_error("open target state registry", &state_path, e)),
        };
        let sealed_path = state.config.state_dir.join("sealed");
        let sealed_directory = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&sealed_path)
            .map_err(|e| path_error("open sealed marker directory", &sealed_path, e))?;
        let sealed_meta = sealed_directory
            .metadata()
            .map_err(|e| path_error("inspect sealed marker directory", &sealed_path, e))?;
        if sealed_meta.dev() != state.sealed_directory_device
            || sealed_meta.ino() != state.sealed_directory_inode
            || (!fixture && (sealed_meta.uid() != 0 || sealed_meta.mode() & 0o077 != 0))
        {
            return Err(invalid("sealed marker directory identity changed"));
        }
        let mut manager = Self {
            configfs,
            directory,
            sealed_directory,
            state,
            pins: HashMap::new(),
            poisoned: false,
            fixture,
            stats: LiveTargetStats {
                fixture_only: fixture,
                ..LiveTargetStats::default()
            },
            commissioning: None,
            _lock: lock,
            #[cfg(test)]
            save_hook: std::sync::Mutex::new(save_hook),
        };
        // Persist even an empty state before touching configfs. An old state is
        // cleaned first; old writable capabilities are never re-enabled.
        manager.save()?;
        // Disconnect persisted local controllers only after denying new target
        // connections and draining all old namespaces.
        manager.fence(None)?;
        if !fixture {
            let path = manager.state.config.state_dir.join("commissioning");
            match fs::DirBuilder::new().mode(0o700).create(&path) {
                Ok(()) => manager.directory.sync_all()?,
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => (),
                Err(e) => return Err(path_error("create commissioning directory", &path, e)),
            }
            check_path(&path, true)?;
            manager.commissioning = Some(LiveConnectionRegistry::open(&path)?);
        }
        manager.cleanup(None)?;
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
            registry_id: self.state.registry_id,
        })
    }
    /// Install a test hook that runs at every journal persistence step.
    #[cfg(test)]
    pub(crate) fn set_save_hook(&self, hook: SaveHook) {
        *self
            .save_hook
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(hook);
    }
    #[cfg(test)]
    fn save_step(&self, step: SaveStep) -> io::Result<()> {
        match self
            .save_hook
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_mut()
        {
            Some(hook) => hook(step),
            None => Ok(()),
        }
    }
    /// Write-ahead journal record. When this returns, the new state is the
    /// durable registry; callers perform the configfs effect it records only
    /// afterwards, so every crash point recovers a registry that lists every
    /// object that may exist.
    fn save(&self) -> io::Result<()> {
        let bytes =
            serde_json::to_vec(&self.state).map_err(|_| invalid("cannot encode target state"))?;
        if bytes.len() as u64 + 4 > MAX_STATE {
            return Err(invalid("target state too large"));
        }
        let temp = self.state.config.state_dir.join(".live-target.pending");
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&temp)
            .map_err(|e| path_error("open pending target registry", &temp, e))?;
        file.write_all(&crc32fast::hash(&bytes).to_le_bytes())
            .and_then(|()| file.write_all(&bytes))
            .map_err(|e| path_error("write pending target registry", &temp, e))?;
        file.sync_all()
            .map_err(|e| path_error("sync pending target registry", &temp, e))?;
        #[cfg(test)]
        self.save_step(SaveStep::Prepared)?;
        let path = self.state.config.state_dir.join(STATE);
        fs::rename(&temp, &path).map_err(|e| path_error("publish target registry", &path, e))?;
        self.directory.sync_all().map_err(|e| {
            path_error(
                "sync target state directory",
                &self.state.config.state_dir,
                e,
            )
        })?;
        #[cfg(test)]
        self.save_step(SaveStep::Published)?;
        Ok(())
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
    fn record_create(
        &mut self,
        group: [u8; 16],
        path: &Path,
        kind: Kind,
        destination: Option<&Path>,
    ) -> io::Result<()> {
        let full = self.full(path)?;
        match fs::symlink_metadata(&full) {
            Ok(_) => {
                return Err(path_error(
                    "create exclusive configfs object",
                    &full,
                    invalid("refusing to share configfs object"),
                ));
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => (),
            Err(e) => return Err(e),
        }
        self.state.objects.push(Object {
            path: path.to_path_buf(),
            group,
            kind,
            identity: None,
            destination: destination.map(Path::to_path_buf),
        });
        self.save()?;
        match kind {
            Kind::Directory => {
                fs::create_dir(&full)
                    .map_err(|e| path_error("create configfs directory", &full, e))?;
                // configfs creates a directory's attributes with the directory.
                // The fixture models that before any later crash point.
                if self.fixture {
                    self.fixture_attributes(path)?;
                }
            }
            Kind::Link => symlink(
                self.full(destination.ok_or_else(|| invalid("missing link destination"))?)?,
                &full,
            )
            .map_err(|e| path_error("create configfs link", &full, e))?,
        }
        let identity = object_id(&full, kind)?;
        self.state
            .objects
            .last_mut()
            .ok_or_else(|| invalid("missing registration"))?
            .identity = Some(identity);
        self.save()?;
        Ok(())
    }
    fn verify_object(&self, object: &Object) -> io::Result<bool> {
        let path = self.full(&object.path)?;
        let actual = match object_id(&path, object.kind) {
            Ok(id) => id,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(e) => return Err(e),
        };
        if object.identity != Some(actual) {
            return Err(invalid(
                "configfs object was replaced or creation was interrupted",
            ));
        }
        if object.kind == Kind::Link {
            let destination = object
                .destination
                .as_deref()
                .ok_or_else(|| invalid("missing link destination"))?;
            let target = self
                .state
                .objects
                .iter()
                .find(|entry| entry.path == destination && entry.kind == Kind::Directory)
                .ok_or_else(|| invalid("link target is not registered"))?;
            let expected = self.full(destination)?;
            if target.identity != Some(object_id(&expected, Kind::Directory)?)
                || fs::canonicalize(&path)? != fs::canonicalize(&expected)?
            {
                return Err(invalid("configfs link destination changed"));
            }
        }
        Ok(true)
    }
    fn put(&self, relative: &Path, value: &str) -> io::Result<()> {
        let path = self.full(relative)?;
        let mut file = OpenOptions::new()
            .write(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&path)
            .map_err(|e| path_error("open target attribute for write", &path, e))?;
        if self.fixture {
            file.set_len(0)?;
        }
        file.write_all(value.as_bytes())
            .map_err(|e| path_error("write target attribute", &path, e))?;
        if attribute(&path)? != value {
            return Err(invalid("configfs attribute readback mismatch"));
        }
        Ok(())
    }
    fn setup(&mut self) -> io::Result<()> {
        let g = [0; 16];
        let host = PathBuf::from("hosts").join(&self.state.config.host_nqn);
        let private = PathBuf::from("hosts").join(&self.state.private_host);
        let public_port = PathBuf::from("ports").join(self.state.config.port_id.to_string());
        let private_port =
            PathBuf::from("ports").join(self.state.config.commissioning_port_id.to_string());
        for p in [&host, &private, &public_port, &private_port] {
            self.record_create(g, p, Kind::Directory, None)?;
        }
        let (af, addr) = match self
            .state
            .config
            .address
            .parse::<IpAddr>()
            .map_err(|_| invalid("invalid address"))?
        {
            IpAddr::V4(a) => ("ipv4", a.to_string()),
            IpAddr::V6(a) => ("ipv6", a.to_string()),
        };
        let transport = match self.state.config.transport {
            NvmeofTransport::Tcp => "tcp",
            NvmeofTransport::Rdma => "rdma",
        };
        for (p, tr, family, address, service) in [
            (
                &public_port,
                transport,
                af,
                addr.as_str(),
                self.state.config.service,
            ),
            (
                &private_port,
                "tcp",
                "ipv4",
                "127.0.0.1",
                self.state.config.commissioning_service,
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
        let config = &self.state.config;
        (config.address.clone(), config.service, config.transport)
    }

    pub fn export_permanent(
        &mut self,
        name: &super::live_storage::WalExportName,
        file: LiveFileIdentity,
    ) -> io::Result<LiveTargetBinding> {
        if self.poisoned {
            return Err(invalid("target manager is quarantined"));
        }
        if let Some(export) = self
            .state
            .exports
            .iter()
            .find(|export| export.file.device == file.device && export.file.inode == file.inode)
        {
            return if export.binding.uuid == name.uuid {
                Ok(export.binding.clone())
            } else {
                Err(invalid("WAL file already exported"))
            };
        }
        // A durable seal marker means this path was fenced or retired: never
        // restore a writable capability. Refused without quarantining the
        // manager, as `export` does for its writable case.
        if self.sealed_file(&file.path)?.is_some() {
            return Err(invalid("fenced WAL cannot be exported writable again"));
        }
        let result = self.export_permanent_inner(name, file);
        if result.is_err() {
            self.poisoned = true;
            self.stats.errors = self.stats.errors.saturating_add(1);
        }
        result
    }

    fn export_permanent_inner(
        &mut self,
        name: &super::live_storage::WalExportName,
        file: LiveFileIdentity,
    ) -> io::Result<LiveTargetBinding> {
        let sub = PathBuf::from("subsystems").join(&name.nqn);
        if !self.state.objects.iter().any(|object| object.path == sub) {
            self.record_create(name.subsystem_id, &sub, Kind::Directory, None)?;
            self.put(&sub.join("attr_allow_any_host"), "0")?;
            let host = PathBuf::from("hosts").join(&self.state.config.host_nqn);
            self.record_create(
                name.subsystem_id,
                &sub.join("allowed_hosts").join(&self.state.config.host_nqn),
                Kind::Link,
                Some(&host),
            )?;
            let public_link = PathBuf::from("ports")
                .join(self.state.config.port_id.to_string())
                .join("subsystems")
                .join(&name.nqn);
            self.record_create(name.subsystem_id, &public_link, Kind::Link, Some(&sub))?;
        }
        let pin = self.file(&file)?;
        let binding = LiveTargetBinding {
            nqn: name.nqn.clone(),
            uuid: name.uuid,
            nsid: name.nsid,
            bytes: file.bytes,
            block_size: file.block_size,
            address: self.state.config.address.clone(),
            service: self.state.config.service,
            transport: self.state.config.transport,
            readonly: false,
            permanent: true,
        };
        self.state.exports.push(Export {
            binding: binding.clone(),
            file: file.clone(),
            readonly: false,
        });
        self.pins.insert(name.uuid, pin);
        self.save()?;
        let ns = sub.join(format!("namespaces/{}", name.nsid));
        self.record_create(name.uuid, &ns, Kind::Directory, None)?;
        let path = file
            .path
            .to_str()
            .ok_or_else(|| invalid("WAL path is not UTF-8"))?;
        self.put(&ns.join("device_path"), path)?;
        self.put(&ns.join("device_uuid"), &uuid(name.uuid))?;
        self.put(&ns.join("buffered_io"), "1")?;
        drop(self.file(&file)?);
        let pinned = self
            .pins
            .get(&name.uuid)
            .ok_or_else(|| invalid("missing WAL pin"))?
            .metadata()?;
        if pinned.dev() != file.device || pinned.ino() != file.inode || pinned.len() != file.bytes {
            return Err(invalid("pinned WAL changed"));
        }
        self.put(&ns.join("enable"), "1")?;
        self.stats.namespaces_exposed = self.stats.namespaces_exposed.saturating_add(1);
        Ok(binding)
    }
    pub fn export_writable(&mut self, file: LiveFileIdentity) -> io::Result<LiveTargetBinding> {
        let result = self.export(file, false);
        if result.is_err() {
            self.stats.errors = self.stats.errors.saturating_add(1);
        }
        result
    }
    pub fn export_readonly(&mut self, file: LiveFileIdentity) -> io::Result<LiveTargetBinding> {
        let result = self.export(file, true);
        if result.is_err() {
            self.stats.errors = self.stats.errors.saturating_add(1);
        }
        result
    }
    /// Reuse an active writable binding for reads; never replace it with a
    /// read-only capability while its writer can still issue I/O.
    pub fn binding_for_file(&self, file: &LiveFileIdentity) -> Option<LiveTargetBinding> {
        self.state
            .exports
            .iter()
            .find(|entry| &entry.file == file)
            .map(|entry| entry.binding.clone())
    }
    /// Current process counters and verified extant owned namespaces. Fixture
    /// counters never claim actual kernel disable/drain or RO commissioning.
    pub fn stats(&self) -> io::Result<LiveTargetStats> {
        let mut stats = self.stats.clone();
        stats.owned_namespace_count = 0;
        for export in &self.state.exports {
            let path = PathBuf::from("subsystems")
                .join(&export.binding.nqn)
                .join(format!("namespaces/{}", export.binding.nsid));
            if let Some(object) = self.state.objects.iter().find(|object| object.path == path)
                && self.verify_object(object)?
            {
                stats.owned_namespace_count = stats.owned_namespace_count.saturating_add(1);
            }
        }
        Ok(stats)
    }
    fn export(&mut self, file: LiveFileIdentity, readonly: bool) -> io::Result<LiveTargetBinding> {
        if self.poisoned {
            return Err(invalid("target manager is quarantined"));
        }
        if !readonly && let Some(old) = self.sealed_file(&file.path)? {
            return Err(if old == file {
                invalid("fenced WAL cannot be exported writable again")
            } else {
                invalid("sealed WAL path identity changed")
            });
        }
        let pin = self.file(&file)?;
        if self
            .state
            .exports
            .iter()
            .any(|e| e.file.device == file.device && e.file.inode == file.inode)
        {
            return Err(invalid("WAL file already exported"));
        }
        for object in &self.state.objects {
            if !self.verify_object(object)? {
                return Err(invalid("registered configfs object disappeared"));
            }
        }
        let id = random_id()?;
        let binding = LiveTargetBinding {
            nqn: format!("nqn.2026-01.local.tidehunter:live-{}", uuid(id)),
            uuid: id,
            nsid: 1,
            bytes: file.bytes,
            block_size: file.block_size,
            address: self.state.config.address.clone(),
            service: self.state.config.service,
            transport: self.state.config.transport,
            readonly,
            permanent: false,
        };
        self.state.exports.push(Export {
            binding: binding.clone(),
            file: file.clone(),
            readonly,
        });
        self.pins.insert(id, pin);
        let result = self.export_inner(&binding, &file, readonly);
        if result.is_err() {
            self.poisoned = true;
        }
        result.map(|()| binding)
    }
    fn export_inner(
        &mut self,
        binding: &LiveTargetBinding,
        file: &LiveFileIdentity,
        readonly: bool,
    ) -> io::Result<()> {
        self.save()?; // durable quarantine precedes any possible exposure
        let id = binding.uuid;
        let sub = PathBuf::from("subsystems").join(&binding.nqn);
        let ns = sub.join(format!("namespaces/{}", binding.nsid));
        self.record_create(id, &sub, Kind::Directory, None)?;
        self.put(&sub.join("attr_allow_any_host"), "0")?;
        self.record_create(id, &ns, Kind::Directory, None)?;
        let file_path = file
            .path
            .to_str()
            .ok_or_else(|| invalid("WAL path is not UTF-8"))?;
        self.put(&ns.join("device_path"), file_path)?;
        self.put(&ns.join("device_uuid"), &uuid(id))?;
        self.put(&ns.join("buffered_io"), "1")?;
        // Recheck the path and the retained descriptor immediately before enable.
        drop(self.file(file)?);
        let pinned = self
            .pins
            .get(&id)
            .ok_or_else(|| invalid("missing WAL pin"))?
            .metadata()?;
        if pinned.dev() != file.device || pinned.ino() != file.inode || pinned.len() != file.bytes {
            return Err(invalid("pinned WAL changed"));
        }
        self.put(&ns.join("enable"), "1")?;
        let private_host = PathBuf::from("hosts").join(&self.state.private_host);
        self.record_create(
            id,
            &sub.join("allowed_hosts").join(&self.state.private_host),
            Kind::Link,
            Some(&private_host),
        )?;
        let private_link = PathBuf::from("ports")
            .join(self.state.config.commissioning_port_id.to_string())
            .join("subsystems")
            .join(&binding.nqn);
        self.record_create(id, &private_link, Kind::Link, Some(&sub))?;
        if !self.fixture {
            let local = LiveConnectTarget {
                nqn: binding.nqn.clone(),
                uuid: id,
                nsid: binding.nsid,
                bytes: binding.bytes,
                block_size: binding.block_size,
                address: "127.0.0.1".to_owned(),
                service: self.state.config.commissioning_service,
                transport: NvmeofTransport::Tcp,
                geometry: Default::default(),
            };
            let deadline = Instant::now() + Duration::from_secs(20);
            let (mut controller, namespace) = self
                .commissioning
                .as_mut()
                .ok_or_else(|| invalid("missing commissioning registry"))?
                .connect(
                    &local,
                    &self.state.private_host,
                    random_id()?,
                    LiveNamespaceAccess::ReadWrite,
                    deadline,
                )?;
            drop(namespace);
            let checked = if readonly {
                controller
                    .set_read_only(binding.nsid, deadline)
                    .and_then(|()| {
                        controller
                            .resolve_verified(LiveNamespaceAccess::ReadOnly, deadline)
                            .map(drop)
                    })
            } else {
                Ok(())
            };
            let disconnect = controller.disconnect(Instant::now() + Duration::from_secs(20));
            checked?;
            disconnect?;
            if readonly {
                self.stats.readonly_commission_successes =
                    self.stats.readonly_commission_successes.saturating_add(1);
            }
        }
        self.remove_one(&private_link)?;
        let private_allowed = sub.join("allowed_hosts").join(&self.state.private_host);
        self.remove_one(&private_allowed)?;
        let host = PathBuf::from("hosts").join(&self.state.config.host_nqn);
        self.record_create(
            id,
            &sub.join("allowed_hosts").join(&self.state.config.host_nqn),
            Kind::Link,
            Some(&host),
        )?;
        let public_link = PathBuf::from("ports")
            .join(self.state.config.port_id.to_string())
            .join("subsystems")
            .join(&binding.nqn);
        self.record_create(id, &public_link, Kind::Link, Some(&sub))?;
        self.stats.namespaces_exposed = self.stats.namespaces_exposed.saturating_add(1);
        Ok(())
    }
    fn remove_one(&mut self, path: &Path) -> io::Result<()> {
        let index = self
            .state
            .objects
            .iter()
            .position(|o| o.path == path)
            .ok_or_else(|| invalid("unregistered configfs object"))?;
        let object = &self.state.objects[index];
        if self.verify_object(object)? {
            let full = self.full(&object.path)?;
            match object.kind {
                Kind::Link => fs::remove_file(full)?,
                Kind::Directory => {
                    if self.fixture {
                        self.fixture_remove_attributes(&object.path)?;
                    }
                    fs::remove_dir(full)?;
                }
            }
        }
        self.state.objects.remove(index);
        // The caller persists the registry once after its removal batch. A
        // durable record of an object that is already gone is harmless:
        // `verify_object` reports it absent and recovery skips it. Only a
        // creation needs its durable record first.
        Ok(())
    }
    /// `groups: None` selects every registered object and export.
    fn fence(&mut self, groups: Option<&[[u8; 16]]>) -> io::Result<()> {
        // Check shared parents as well as the selected namespace before mutation.
        for object in &self.state.objects {
            self.verify_object(object)?;
        }
        // Stop new connections on both listeners before synchronously draining.
        let links: Vec<_> = self
            .state
            .objects
            .iter()
            .filter(|o| {
                groups.is_none_or(|g| g.contains(&o.group))
                    && o.kind == Kind::Link
                    && o.path.components().any(|c| c.as_os_str() == "ports")
            })
            .map(|o| o.path.clone())
            .collect();
        for link in links {
            self.remove_one(&link)?;
        }
        let exports: Vec<_> = self
            .state
            .exports
            .iter()
            .filter(|e| groups.is_none_or(|g| g.contains(&e.binding.uuid)))
            .collect();
        let mut disabled = false;
        for export in &exports {
            let ns = PathBuf::from("subsystems")
                .join(&export.binding.nqn)
                .join(format!("namespaces/{}", export.binding.nsid));
            if let Some(object) = self.state.objects.iter().find(|o| o.path == ns)
                && self.verify_object(object)?
            {
                let full = self.full(&ns)?;
                let enabled = attribute(&full.join("enable"))?;
                if enabled == "1" {
                    if attribute(&full.join("device_uuid"))? != uuid(export.binding.uuid)
                        || attribute(&full.join("device_path"))?
                            != export
                                .file
                                .path
                                .to_str()
                                .ok_or_else(|| invalid("invalid WAL path"))?
                    {
                        return Err(invalid("namespace identity changed"));
                    }
                    // The namespace name alone cannot authenticate its
                    // backing inode after a crash or pathname replacement.
                    drop(self.file(&export.file)?);
                    let start = Instant::now();
                    self.put(&ns.join("enable"), "0")?;
                    disabled = true;
                    if !self.fixture {
                        self.stats.disable_drain_successes =
                            self.stats.disable_drain_successes.saturating_add(1);
                        self.stats.disable_drain_time_ns =
                            self.stats.disable_drain_time_ns.saturating_add(
                                start.elapsed().as_nanos().min(u64::MAX as u128) as u64,
                            );
                    }
                } else if enabled != "0" {
                    return Err(invalid("invalid namespace enable state"));
                }
                // Successful enable=0 is synchronous nvmet_ns_disable: it
                // kills lookup references and waits for target I/O to drain.
            }
        }
        let sealed = exports.iter().any(|export| !export.readonly);
        for export in exports {
            if !export.readonly {
                self.write_sealed(&export.file)?;
            }
        }
        // One barrier makes every marker durable before a registry save can
        // retire its export record.
        if sealed {
            self.sealed_directory.sync_all()?;
        }
        self.save()?;
        if disabled {
            self.stats.fence_count = self.stats.fence_count.saturating_add(1);
        }
        Ok(())
    }
    fn cleanup(&mut self, groups: Option<&[[u8; 16]]>) -> io::Result<()> {
        self.fence(groups)?;
        let mut paths: Vec<_> = self
            .state
            .objects
            .iter()
            .filter(|o| groups.is_none_or(|g| g.contains(&o.group)))
            .map(|o| o.path.clone())
            .collect();
        paths.reverse();
        for path in paths {
            self.remove_one(&path)?;
        }
        self.state
            .exports
            .retain(|e| groups.is_some_and(|g| !g.contains(&e.binding.uuid)));
        if let Some(groups) = groups {
            for group in groups {
                self.pins.remove(group);
            }
        } else {
            self.pins.clear();
        }
        self.save()
    }
    /// Caller has stopped admission and retains read pins until CQ retirement.
    pub fn revoke(&mut self, binding: &LiveTargetBinding) -> io::Result<()> {
        self.revoke_all(&[binding])
    }
    /// Revoke several bindings with one fence and one cleanup pass, so the
    /// registry is persisted a fixed number of times instead of once per
    /// binding. Same caller contract as `revoke`.
    pub fn revoke_all(&mut self, bindings: &[&LiveTargetBinding]) -> io::Result<()> {
        if bindings.is_empty() {
            return Ok(());
        }
        if self.poisoned {
            return Err(invalid("target manager is quarantined"));
        }
        if bindings
            .iter()
            .any(|binding| !self.state.exports.iter().any(|e| e.binding == **binding))
        {
            return Err(invalid("unknown target binding"));
        }
        let namespaces: Vec<_> = bindings.iter().map(|binding| binding.uuid).collect();
        let result = self.cleanup(Some(&namespaces)).and_then(|()| {
            // A permanent subsystem goes with its last namespace.
            let mut subsystems = Vec::new();
            for binding in bindings {
                if !binding.permanent
                    || self
                        .state
                        .exports
                        .iter()
                        .any(|export| export.binding.nqn == binding.nqn)
                {
                    continue;
                }
                let sub = PathBuf::from("subsystems").join(&binding.nqn);
                let group = self
                    .state
                    .objects
                    .iter()
                    .find(|object| object.path == sub)
                    .ok_or_else(|| invalid("missing subsystem registration"))?
                    .group;
                if !subsystems.contains(&group) {
                    subsystems.push(group);
                }
            }
            if subsystems.is_empty() {
                Ok(())
            } else {
                self.cleanup(Some(&subsystems))
            }
        });
        if result.is_err() {
            self.poisoned = true;
            self.stats.errors = self.stats.errors.saturating_add(1);
        }
        result
    }

    fn marker_path(&self, wal_path: &Path) -> io::Result<PathBuf> {
        if !wal_path.is_absolute()
            || wal_path
                .components()
                .any(|c| matches!(c, Component::CurDir | Component::ParentDir))
        {
            return Err(invalid("invalid sealed WAL path"));
        }
        let directory = self.state.config.state_dir.join("sealed");
        let meta = fs::symlink_metadata(&directory)?;
        let pinned = self.sealed_directory.metadata()?;
        if !meta.is_dir()
            || meta.dev() != pinned.dev()
            || meta.ino() != pinned.ino()
            || meta.dev() != self.state.sealed_directory_device
            || meta.ino() != self.state.sealed_directory_inode
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
    /// before it persists a registry that retires the matching exports.
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
            .state
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
            .state
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
        let result = fs::remove_file(self.marker_path(&file.path)?)
            .and_then(|()| self.sealed_directory.sync_all());
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }
    /// Explicit shutdown; callers must first retire initiator read pins.
    pub fn shutdown(&mut self) -> io::Result<()> {
        let result = (|| {
            self.fence(None)?;
            if !self.fixture {
                // A failed or timed-out commissioning attempt may still own a
                // controller. Reconcile its durable journal after target drain.
                drop(self.commissioning.take());
                let path = self.state.config.state_dir.join("commissioning");
                self.commissioning = Some(LiveConnectionRegistry::open(&path)?);
            }
            self.cleanup(None)
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
        for entry in fs::read_dir(p)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
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

#[cfg(test)]
#[path = "live_target_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "live_target_legacy_tests.rs"]
mod legacy_tests;
