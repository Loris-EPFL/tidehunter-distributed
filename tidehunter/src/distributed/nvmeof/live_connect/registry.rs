use super::*;

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub(super) struct Intent {
    pub(super) target: LiveConnectTarget,
    pub(super) host_nqn: String,
    pub(super) host_id: [u8; 16],
}
pub(super) struct RegistryInner {
    pub(super) directory: PathBuf,
    pub(super) _lock: File,
    pub(super) admission: Mutex<()>,
    pub(super) uncertain: AtomicBool,
}
#[derive(Clone)]
pub struct LiveConnectionRegistry {
    pub(super) inner: Arc<RegistryInner>,
}
impl LiveConnectionRegistry {
    /// Takes exclusive ownership of a private persistent directory and retires
    /// recorded controllers before returning. Host identities must be unique to
    /// this registry; authority open reconciles here before fencing the targets.
    pub fn open(directory: &Path) -> io::Result<Self> {
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(directory)?;
        let meta = std::fs::metadata(directory)?;
        if !meta.is_dir() || meta.mode() & 0o077 != 0 {
            return Err(invalid("connection registry must be private"));
        }
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(directory.join("lock"))?;
        // SAFETY: flock operates on the live lock file descriptor.
        if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } < 0 {
            return Err(io::Error::last_os_error());
        }
        let registry = Self {
            inner: Arc::new(RegistryInner {
                directory: directory.to_owned(),
                _lock: lock,
                admission: Mutex::new(()),
                uncertain: AtomicBool::new(false),
            }),
        };
        registry.reconcile()?;
        Ok(registry)
    }
    pub(super) fn reconcile(&self) -> io::Result<()> {
        let deadline = Instant::now() + CLEANUP_TIMEOUT;
        for entry in std::fs::read_dir(&self.inner.directory)? {
            remaining(deadline)?;
            let entry = entry?;
            if entry.path().extension().and_then(|s| s.to_str()) != Some("json") {
                continue;
            }
            let intent: Intent = serde_json::from_slice(&std::fs::read(entry.path())?)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let controllers = match std::fs::read_dir("/sys/class/nvme") {
                Ok(v) => v,
                Err(e) if e.kind() == io::ErrorKind::NotFound => {
                    self.remove_intent(&entry.path())?;
                    continue;
                }
                Err(e) => return Err(e),
            };
            for controller in controllers {
                let controller = controller?;
                let name = controller.file_name();
                let Some(name) = name.to_str() else { continue };
                if !name
                    .strip_prefix("nvme")
                    .is_some_and(|s| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()))
                {
                    continue;
                }
                let path = controller.path();
                let directory = match File::open(&path) {
                    Ok(directory) => directory,
                    Err(error) if vanished(&error) => continue,
                    Err(error) => return Err(with_context("open controller sysfs", &path, error)),
                };
                let Some(nqn) = controller_attribute(&path, &directory, "subsysnqn")? else {
                    continue;
                };
                if nqn != intent.target.nqn {
                    continue;
                }
                let Some(host_nqn) = controller_attribute(&path, &directory, "hostnqn")? else {
                    continue;
                };
                let Some(host_id) = controller_attribute(&path, &directory, "hostid")? else {
                    continue;
                };
                if host_nqn != intent.host_nqn || host_id != uuid_string(intent.host_id) {
                    continue;
                }
                let metadata = directory.metadata()?;
                let instance = name
                    .strip_prefix("nvme")
                    .and_then(|s| s.parse().ok())
                    .ok_or_else(|| invalid("invalid controller instance"))?;
                let mut owned = LiveController {
                    resolver: Arc::new(LiveNamespaceResolver {
                        target: intent.target.clone(),
                        host_nqn: intent.host_nqn.clone(),
                        host_id: intent.host_id,
                        name: name.to_owned(),
                        directory,
                        inode: metadata.ino(),
                        device: metadata.dev(),
                        queue: OnceLock::new(),
                    }),
                    instance,
                    deletion: DeletionState::NotStarted,
                    registry: self.clone(),
                    intent_path: entry.path(),
                    teardown_locks: Vec::new(),
                };
                owned.verify()?;
                // A previous process may have issued the delete before dying.
                // A controller already deleting is only observed, never deleted
                // again; its original sysfs identity still must disappear.
                let state = attr(&owned.pinned().join("state"))?;
                if matches!(state.as_str(), "deleting" | "deleting (no IO)" | "dead") {
                    owned.deletion = DeletionState::Initiated;
                }
                owned.retire(deadline)?;
            }
            self.remove_intent(&entry.path())?;
        }
        Ok(())
    }
    pub(super) fn remove_intent(&self, path: &Path) -> io::Result<()> {
        let _guard = self
            .inner
            .admission
            .lock()
            .map_err(|_| invalid("registry lock poisoned"))?;
        match std::fs::remove_file(path) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        if let Err(error) = File::open(&self.inner.directory).and_then(|dir| dir.sync_all()) {
            self.inner.uncertain.store(true, Ordering::Release);
            return Err(error);
        }
        Ok(())
    }
    pub(super) fn reserve(&self, intent: &Intent) -> io::Result<PathBuf> {
        let _guard = self
            .inner
            .admission
            .lock()
            .map_err(|_| invalid("registry lock poisoned"))?;
        if self.inner.uncertain.load(Ordering::Acquire) {
            return Err(invalid("registry durability is uncertain"));
        }
        for entry in std::fs::read_dir(&self.inner.directory)? {
            let entry = entry?;
            if entry.path().extension().and_then(|s| s.to_str()) != Some("json") {
                continue;
            }
            let previous: Intent = serde_json::from_slice(&std::fs::read(entry.path())?)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            if previous.target.nqn == intent.target.nqn && previous.host_nqn == intent.host_nqn {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "connection intent remains active or quarantined",
                ));
            }
        }
        let mut nonce = [0u8; 16];
        File::open("/dev/urandom")?.read_exact(&mut nonce)?;
        let nonce = uuid_string(nonce);
        let path = self.inner.directory.join(format!("{nonce}.json"));
        let temporary = self.inner.directory.join(format!("{nonce}.tmp"));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temporary)?;
        serde_json::to_writer(&mut file, intent).map_err(io::Error::other)?;
        file.sync_all()?;
        // Publish a complete record atomically without replacing an existing
        // intent. Recovery ignores abandoned temporary files.
        std::fs::hard_link(&temporary, &path)?;
        std::fs::remove_file(&temporary)?;
        File::open(&self.inner.directory)?.sync_all()?;
        Ok(path)
    }
    pub(crate) fn has_intent(&self, nqn: &str, host_nqn: &str) -> io::Result<bool> {
        let _guard = self
            .inner
            .admission
            .lock()
            .map_err(|_| invalid("registry lock poisoned"))?;
        if self.inner.uncertain.load(Ordering::Acquire) {
            return Err(invalid("registry durability is uncertain"));
        }
        for entry in std::fs::read_dir(&self.inner.directory)? {
            let entry = entry?;
            if entry.path().extension().and_then(|s| s.to_str()) != Some("json") {
                continue;
            }
            let intent: Intent = serde_json::from_slice(&std::fs::read(entry.path())?)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            if intent.target.nqn == nqn && intent.host_nqn == host_nqn {
                return Ok(true);
            }
        }
        Ok(false)
    }
    pub fn connect(
        &self,
        target: &LiveConnectTarget,
        host_nqn: &str,
        host_id: [u8; 16],
        access: LiveNamespaceAccess,
        deadline: Instant,
    ) -> io::Result<(LiveController, LiveNamespaceBinding)> {
        LiveController::connect(self, target, host_nqn, host_id, access, deadline)
    }
    /// Connect and verify the controller alone, for admin-only users such as
    /// the commissioning write-protect path.
    pub(crate) fn connect_controller(
        &self,
        target: &LiveConnectTarget,
        host_nqn: &str,
        host_id: [u8; 16],
        deadline: Instant,
    ) -> io::Result<LiveController> {
        LiveController::connect_controller(self, target, host_nqn, host_id, deadline)
    }
}
