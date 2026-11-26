use super::*;

/// Whether opening or reconciling the registry retires recorded controllers
/// or keeps one live controller per intent for the client pool to adopt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RegistryMode {
    /// Delete every recorded controller before returning. Commissioning and
    /// other admin-only users own no namespace I/O and must not inherit a
    /// previous process's controller.
    Retire,
    /// Keep the one live controller that matches a durable intent so a client
    /// pool can reuse it after an abrupt coordinator exit.
    Adopt,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub(super) struct Intent {
    pub(super) target: LiveConnectTarget,
    pub(super) host_nqn: String,
    pub(super) host_id: [u8; 16],
}

/// One controller whose sysfs identity matches an intent, read through a
/// pinned directory so a reused `nvmeN` number cannot be adopted.
struct ControllerMatch {
    name: String,
    directory: File,
    inode: u64,
    device: u64,
    instance: u32,
    state: String,
}

pub(super) struct RegistryInner {
    pub(super) directory: PathBuf,
    /// Controller sysfs root; injectable for fixture tests.
    pub(super) controller_root: PathBuf,
    /// systemd-udevd queue path; injectable for fixture tests.
    pub(super) udev_queue: PathBuf,
    pub(super) mode: RegistryMode,
    /// The keep-alive every connect through this registry requests.
    pub(super) keep_alive_tmo: u32,
    pub(super) _lock: File,
    pub(super) admission: Mutex<()>,
    pub(super) uncertain: AtomicBool,
}

#[derive(Clone)]
pub struct LiveConnectionRegistry {
    pub(super) inner: Arc<RegistryInner>,
}

/// One adoption decision made under the admission lock. The controllers are
/// built but not retired inside the lock, so a slow teardown cannot block a
/// concurrent `reserve` or `remove_intent`.
enum AdoptionPlan {
    /// Reuse `controller`; retire every other matching controller. The intent
    /// stays until `disconnect_inner` removes it.
    Adopt {
        controller: Box<LiveController>,
        retire: Vec<LiveController>,
        intent_path: PathBuf,
    },
    /// The intent and every matching controller must go before a fresh connect.
    Declined {
        retire: Vec<LiveController>,
        intent_path: PathBuf,
    },
}

impl LiveConnectionRegistry {
    /// Takes exclusive ownership of a private persistent directory and
    /// reconciles its durable intents before returning: `Retire` deletes every
    /// recorded controller, `Adopt` keeps the one live controller per intent.
    /// Host identities must be unique to this registry; authority open
    /// reconciles here before fencing the targets.
    ///
    /// The mode is a required argument: an admin-only registry must never adopt
    /// a controller it does not own.
    pub(crate) fn open(directory: &Path, mode: RegistryMode) -> io::Result<Self> {
        Self::open_with_keep_alive(
            directory,
            mode,
            crate::distributed::nvmeof::live_connect::KEEP_ALIVE_TMO_SECS,
        )
    }

    /// Opens a registry whose connects request `keep_alive_tmo`, derived from
    /// the configured resume budget.
    pub(crate) fn open_with_keep_alive(
        directory: &Path,
        mode: RegistryMode,
        keep_alive_tmo: u32,
    ) -> io::Result<Self> {
        Self::open_at(
            directory,
            Path::new("/sys/class/nvme"),
            Path::new("/run/udev/queue"),
            mode,
            keep_alive_tmo,
        )
    }

    /// Testable constructor with the controller sysfs root and udev queue
    /// explicitly named. Production passes the kernel paths above.
    pub(super) fn open_at(
        directory: &Path,
        controller_root: &Path,
        udev_queue: &Path,
        mode: RegistryMode,
        keep_alive_tmo: u32,
    ) -> io::Result<Self> {
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
                controller_root: controller_root.to_owned(),
                udev_queue: udev_queue.to_owned(),
                mode,
                keep_alive_tmo,
                _lock: lock,
                admission: Mutex::new(()),
                uncertain: AtomicBool::new(false),
            }),
        };
        registry.reconcile()?;
        Ok(registry)
    }

    pub(super) fn adopts(&self) -> bool {
        self.inner.mode == RegistryMode::Adopt
    }

    /// Every durable intent, in stable path order so a multi-intent registry
    /// reconciles deterministically.
    fn intents(&self) -> io::Result<Vec<(PathBuf, Intent)>> {
        let mut intents = Vec::new();
        for entry in std::fs::read_dir(&self.inner.directory)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) != Some("json") {
                continue;
            }
            let intent: Intent = serde_json::from_slice(&std::fs::read(&path)?)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            intents.push((path, intent));
        }
        intents.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
        Ok(intents)
    }

    pub(super) fn reconcile(&self) -> io::Result<()> {
        match self.inner.mode {
            RegistryMode::Retire => self.reconcile_retire(),
            RegistryMode::Adopt => self.reconcile_adopt(),
        }
    }

    /// Retire one intent's matching controllers and remove the intent. Shared
    /// by the whole-registry reconcile and the `-EALREADY` bounded reconcile.
    fn reconcile_retire_intent(
        &self,
        intent_path: &Path,
        intent: &Intent,
        deadline: Instant,
    ) -> io::Result<()> {
        for controller in self.matching_controllers(intent)? {
            remaining(deadline)?;
            let mut owned = self.controller_from_match(
                intent.target.clone(),
                &intent.host_nqn,
                intent.host_id,
                controller,
                intent_path.to_owned(),
            );
            owned.verify()?;
            let state = attr(&owned.pinned().join("state"))?;
            if matches!(state.as_str(), "deleting" | "deleting (no IO)" | "dead") {
                owned.deletion = DeletionState::Initiated;
            }
            owned.retire(deadline)?;
        }
        self.remove_intent(intent_path)
    }

    fn reconcile_retire(&self) -> io::Result<()> {
        let deadline = Instant::now() + CLEANUP_TIMEOUT;
        for (path, intent) in self.intents()? {
            remaining(deadline)?;
            self.reconcile_retire_intent(&path, &intent, deadline)?;
        }
        Ok(())
    }

    /// Keep one live controller per intent and retire every other match. The
    /// kept controller is left untouched and its intent stays durable.
    fn reconcile_adopt(&self) -> io::Result<()> {
        let deadline = Instant::now() + CLEANUP_TIMEOUT;
        for (path, intent) in self.intents()? {
            remaining(deadline)?;
            self.keep_one_matching_controller(&path, &intent, deadline)?;
        }
        Ok(())
    }

    fn keep_one_matching_controller(
        &self,
        intent_path: &Path,
        intent: &Intent,
        deadline: Instant,
    ) -> io::Result<()> {
        let matches = self.matching_controllers(intent)?;
        let mut kept = false;
        for candidate in matches {
            remaining(deadline)?;
            let live = candidate.state == "live";
            let mut controller = self.controller_from_match(
                intent.target.clone(),
                &intent.host_nqn,
                intent.host_id,
                candidate,
                intent_path.to_owned(),
            );
            if !kept
                && live
                && controller.verify().is_ok()
                && controller.verify_queue_geometry().is_ok()
            {
                kept = true;
                // Drop without retiring: this is the controller the pool keeps.
                continue;
            }
            controller.retire(deadline)?;
        }
        if kept {
            Ok(())
        } else {
            self.remove_intent(intent_path)
        }
    }

    /// Keep or remove only the intents of one identity. The `-EALREADY`
    /// fallback uses this so it can never touch another subsystem's in-flight
    /// connect in the same registry.
    fn reconcile_identity(
        &self,
        target: &LiveConnectTarget,
        host_nqn: &str,
        host_id: [u8; 16],
        deadline: Instant,
    ) -> io::Result<()> {
        for (path, intent) in self.intents()? {
            remaining(deadline)?;
            if intent.target.nqn != target.nqn
                || intent.host_nqn != host_nqn
                || intent.host_id != host_id
            {
                continue;
            }
            match self.inner.mode {
                RegistryMode::Retire => {
                    self.reconcile_retire_intent(&path, &intent, deadline)?;
                }
                RegistryMode::Adopt => {
                    self.keep_one_matching_controller(&path, &intent, deadline)?;
                }
            }
        }
        Ok(())
    }

    /// Whether a controller with this subsystem and host identity remains in
    /// the kernel. `deleting` and `dead` controllers do not block a connect;
    /// everything else does, including `connecting` and `resetting`.
    fn has_identity_controller(
        &self,
        target: &LiveConnectTarget,
        host_nqn: &str,
        host_id: [u8; 16],
    ) -> io::Result<bool> {
        let intent = Intent {
            target: target.clone(),
            host_nqn: host_nqn.to_owned(),
            host_id,
        };
        Ok(self
            .matching_controllers(&intent)?
            .into_iter()
            .any(|m| !matches!(m.state.as_str(), "deleting" | "deleting (no IO)" | "dead")))
    }

    /// One bounded reconcile after `-EALREADY`. `Ok(None)` means no controller
    /// for the identity remains and the caller may retry the connect once; the
    /// typed error is the operator-visible fail-stop when one remains. The
    /// reconcile never runs past the caller's acquisition deadline.
    pub(super) fn already_connected_outcome(
        &self,
        target: &LiveConnectTarget,
        host_nqn: &str,
        host_id: [u8; 16],
        deadline: Instant,
    ) -> io::Result<Option<io::Error>> {
        let deadline = deadline.min(Instant::now() + CLEANUP_TIMEOUT);
        remaining(deadline)?;
        self.reconcile_identity(target, host_nqn, host_id, deadline)?;
        remaining(deadline)?;
        if self.has_identity_controller(target, host_nqn, host_id)? {
            return Ok(Some(unowned_identity(target, host_nqn)));
        }
        Ok(None)
    }

    /// Every controller whose sysfs identity matches `intent`, lowest instance
    /// first. Identity is `subsysnqn`, `hostnqn` and `hostid`, exactly what the
    /// kernel uses to reject a duplicate connect.
    fn matching_controllers(&self, intent: &Intent) -> io::Result<Vec<ControllerMatch>> {
        let entries = match std::fs::read_dir(&self.inner.controller_root) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(error),
        };
        let mut matches = Vec::new();
        for controller in entries {
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
            let Some(state) = controller_attribute(&path, &directory, "state")? else {
                continue;
            };
            let metadata = directory.metadata()?;
            let instance = name
                .strip_prefix("nvme")
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| invalid("invalid controller instance"))?;
            matches.push(ControllerMatch {
                name: name.to_owned(),
                directory,
                inode: metadata.ino(),
                device: metadata.dev(),
                instance,
                state,
            });
        }
        matches.sort_unstable_by_key(|m| m.instance);
        Ok(matches)
    }

    fn controller_from_match(
        &self,
        target: LiveConnectTarget,
        host_nqn: &str,
        host_id: [u8; 16],
        m: ControllerMatch,
        intent_path: PathBuf,
    ) -> LiveController {
        let deletion = if matches!(m.state.as_str(), "deleting" | "deleting (no IO)" | "dead") {
            DeletionState::Initiated
        } else {
            DeletionState::NotStarted
        };
        LiveController {
            resolver: Arc::new(LiveNamespaceResolver {
                target,
                host_nqn: host_nqn.to_owned(),
                host_id,
                name: m.name,
                directory: m.directory,
                inode: m.inode,
                device: m.device,
                queue: OnceLock::new(),
                controller_root: self.inner.controller_root.clone(),
                udev_queue: self.inner.udev_queue.clone(),
                keep_alive_tmo: self.inner.keep_alive_tmo,
            }),
            instance: m.instance,
            deletion,
            registry: self.clone(),
            intent_path,
            teardown_locks: Vec::new(),
        }
    }

    /// Under the admission lock, find the durable intent for this identity and
    /// decide whether its live controller may be adopted. The caller resolves
    /// the namespace after this returns; a resolve failure retires the
    /// controller and removes the intent before a fresh connect.
    pub(super) fn try_adopt_controller(
        &self,
        target: &LiveConnectTarget,
        host_nqn: &str,
        host_id: [u8; 16],
        deadline: Instant,
    ) -> io::Result<Option<LiveController>> {
        let plan = {
            let _guard = self
                .inner
                .admission
                .lock()
                .map_err(|_| invalid("registry lock poisoned"))?;
            if self.inner.uncertain.load(Ordering::Acquire) {
                return Err(invalid("registry durability is uncertain"));
            }
            remaining(deadline)?;
            let Some((intent_path, intent)) = self.intents()?.into_iter().find(|(_, intent)| {
                intent.target.nqn == target.nqn
                    && intent.host_nqn == host_nqn
                    && intent.host_id == host_id
            }) else {
                return Ok(None);
            };
            // The intent's controller-defining configuration must match the
            // caller before anything is built for a namespace. A geometry change
            // is operator configuration, not part of the authority manifest, so
            // an adopted controller would otherwise keep the old queue set.
            let compatible = intent.target.nqn == target.nqn
                && intent.target.address == target.address
                && intent.target.service == target.service
                && intent.target.transport == target.transport
                && intent.target.geometry == target.geometry;
            if !compatible {
                let retire = self
                    .matching_controllers(&intent)?
                    .into_iter()
                    .map(|m| {
                        self.controller_from_match(
                            intent.target.clone(),
                            &intent.host_nqn,
                            intent.host_id,
                            m,
                            intent_path.clone(),
                        )
                    })
                    .collect();
                AdoptionPlan::Declined {
                    retire,
                    intent_path,
                }
            } else {
                let mut controllers = self
                    .matching_controllers(&intent)?
                    .into_iter()
                    .map(|m| {
                        let live = m.state == "live";
                        (
                            self.controller_from_match(
                                intent.target.clone(),
                                &intent.host_nqn,
                                intent.host_id,
                                m,
                                intent_path.clone(),
                            ),
                            live,
                        )
                    })
                    .collect::<Vec<_>>();
                if controllers.is_empty() {
                    AdoptionPlan::Declined {
                        retire: Vec::new(),
                        intent_path,
                    }
                } else {
                    let kept = controllers.iter().position(|(controller, live)| {
                        *live
                            && controller.verify().is_ok()
                            && controller.verify_queue_geometry().is_ok()
                    });
                    match kept {
                        Some(index) => {
                            let (mut controller, _) = controllers.swap_remove(index);
                            // The controller was verified against the intent's
                            // member. The caller resolves a different namespace
                            // of the same subsystem, and compatibility already
                            // proved every controller-defining field equal.
                            let resolver = Arc::get_mut(&mut controller.resolver)
                                .ok_or_else(|| invalid("adopted controller resolver is shared"))?;
                            resolver.target = target.clone();
                            AdoptionPlan::Adopt {
                                controller: Box::new(controller),
                                retire: controllers.into_iter().map(|(c, _)| c).collect(),
                                intent_path,
                            }
                        }
                        None => AdoptionPlan::Declined {
                            retire: controllers.into_iter().map(|(c, _)| c).collect(),
                            intent_path,
                        },
                    }
                }
            }
        };
        match plan {
            AdoptionPlan::Adopt {
                controller,
                retire,
                intent_path: _kept_intent,
            } => {
                // Retire duplicate matches before any I/O on the adopted
                // controller, so the one-controller invariant holds.
                for mut controller in retire {
                    controller.retire(deadline)?;
                }
                Ok(Some(*controller))
            }
            AdoptionPlan::Declined {
                retire,
                intent_path,
            } => {
                for mut controller in retire {
                    controller.retire(deadline)?;
                }
                self.remove_intent(&intent_path)?;
                Ok(None)
            }
        }
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
    /// The keep-alive every connect through this registry requests.
    pub(crate) fn keep_alive_tmo(&self) -> u32 {
        self.inner.keep_alive_tmo
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
