//! Bounded controller sharing. Connection setup and shutdown are blocking and
//! must run outside the reactor. A cached lease needs no pool lock to open I/O.
use super::live_connect::{
    LiveConnectTarget, LiveConnectionRegistry, LiveController, LiveNamespaceResolver,
};
use super::live_namespace::{LiveNamespace, LiveNamespaceAccess, LiveNamespaceBinding};
use crate::distributed::tcp::checkpoint::RetirementResult;
use crate::distributed::tcp::{LiveExportPin, LiveExportPinControl, LiveExportPinRequest};
use std::collections::HashMap;
use std::io;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
fn full() -> io::Error {
    io::Error::new(
        io::ErrorKind::WouldBlock,
        "live controller pool capacity exhausted",
    )
}
fn timeout() -> io::Error {
    io::Error::new(io::ErrorKind::TimedOut, "live controller pool deadline")
}

#[derive(Clone)]
struct Identity {
    target: LiveConnectTarget,
    host_nqn: String,
    host_id: [u8; 16],
    access: LiveNamespaceAccess,
    // Retirement routing only, not part of the controller's transport identity.
    file: Option<(u32, u64, u64)>,
}
impl PartialEq for Identity {
    fn eq(&self, other: &Self) -> bool {
        self.target == other.target
            && self.host_nqn == other.host_nqn
            && self.host_id == other.host_id
            && self.access == other.access
    }
}
impl Eq for Identity {}

/// The lease is cheap to share. Namespace handles opened through this object
/// retain an independent shutdown pin until their file descriptor is dropped.
/// Clients must release idle leases and namespace handles to permit rollover
/// eviction; active cached bindings are never evicted.
pub struct LiveControllerLease {
    binding: LiveNamespaceBinding,
    pin: Arc<()>,
    instance: u32,
    resolver: Arc<LiveNamespaceResolver>,
    // Cold-path lock only: first file open is serialized and bounded by the
    // connect deadline. In-flight reads hold an Arc and never take this lock.
    namespaces: Mutex<HashMap<(u32, bool), Arc<LiveNamespace>>>,
    #[cfg(any(test, feature = "test-utils"))]
    local_fixture: bool,
}
impl LiveControllerLease {
    pub fn binding(&self) -> &LiveNamespaceBinding {
        &self.binding
    }
    /// Instance returned by the successful kernel fabrics creation.
    pub fn controller_instance(&self) -> u32 {
        self.instance
    }
    pub fn open_namespace(&self) -> io::Result<LiveNamespace> {
        #[cfg(any(test, feature = "test-utils"))]
        if self.local_fixture {
            let mut namespace = LiveNamespace::open_local_file(
                &self.binding.namespace_path,
                self.binding.bytes,
                self.binding.block_size,
                self.binding.access,
            )?;
            namespace.controller_lease = Some(self.pin.clone());
            return Ok(namespace);
        }
        let mut namespace = LiveNamespace::open(&self.binding)?;
        namespace.controller_lease = Some(self.pin.clone());
        Ok(namespace)
    }
    pub fn namespace(
        &self,
        nsid: u32,
        uuid: [u8; 16],
        access: LiveNamespaceAccess,
        deadline: Instant,
    ) -> io::Result<Arc<LiveNamespace>> {
        let mut namespaces = self
            .namespaces
            .lock()
            .map_err(|_| invalid("namespace map poisoned"))?;
        let key = (nsid, access == LiveNamespaceAccess::ReadWrite);
        if let Some(namespace) = namespaces.get(&key) {
            return Ok(namespace.clone());
        }
        let binding = self
            .resolver
            .resolve_namespace(nsid, uuid, access, deadline)?;
        let mut namespace = LiveNamespace::open(&binding)?;
        namespace.controller_lease = Some(self.pin.clone());
        let namespace = Arc::new(namespace);
        namespaces.insert(key, namespace.clone());
        Ok(namespace)
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LivePoolAcquisition {
    Connected,
    Reused,
}
/// Blocking work one acquisition performed, for evidence. Both are `None` when
/// the acquisition was a cache hit.
#[derive(Clone, Copy, Debug, Default)]
pub struct LivePoolTimings {
    /// Kernel fabrics connect and namespace verification of a new controller.
    pub connect: Option<Duration>,
    /// Deletion of an idle controller evicted to make room.
    pub disconnect: Option<Duration>,
}
fn idle(lease: &Arc<LiveControllerLease>) -> bool {
    // A setup caller holds an external lease while resolving a namespace.
    // Reject it before taking its map lock under the pool state lock.
    if Arc::strong_count(lease) != 1 {
        return false;
    }
    let Ok(namespaces) = lease.namespaces.lock() else {
        std::process::abort()
    };
    namespaces
        .values()
        .all(|namespace| Arc::strong_count(namespace) == 1)
        && Arc::strong_count(&lease.pin) == namespaces.len() + 1
}
// Keep bounded controller state inline; boxing would add an allocation at setup.
#[allow(clippy::large_enum_variant)]
enum Entry {
    Pending(Identity),
    Quarantined {
        _export_pin: Option<LiveExportPin>,
    },
    /// An idle controller whose deletion runs outside the pool lock. The entry
    /// still counts against capacity; waiters block until it is removed or, on
    /// a failed delete, restored as an unusable `Ready` entry.
    Closing {
        identity: Identity,
        lease: Arc<LiveControllerLease>,
        export_pin: Option<LiveExportPin>,
    },
    Ready {
        identity: Identity,
        lease: Arc<LiveControllerLease>,
        controller: LiveController,
        usable: bool,
        export_pin: Option<LiveExportPin>,
    },
}
struct State {
    entries: HashMap<String, Entry>,
    // One cold metadata connection per configured owner, independent of entry
    // count. Entry creation/eviction never borrows a caller's Client lock.
    owners: HashMap<(String, [u8; 32], u32), Arc<LiveExportPinControl>>,
    closing: bool,
}
pub struct LiveControllerPool {
    registry: LiveConnectionRegistry,
    capacity: usize,
    max_pending: usize,
    state: Mutex<State>,
    changed: Condvar,
    #[cfg(any(test, feature = "test-utils"))]
    local_targets: Mutex<HashMap<String, std::path::PathBuf>>,
}
impl LiveControllerPool {
    /// Local-file pool fixture. It does not create or qualify a kernel controller.
    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn cache_local_shared_namespace(
        &self,
        target: &LiveConnectTarget,
        host_nqn: &str,
        host_id: [u8; 16],
        path: &std::path::Path,
    ) -> io::Result<()> {
        let existing = {
            let state = self
                .state
                .lock()
                .map_err(|_| invalid("controller pool poisoned"))?;
            match state.entries.get(&target.nqn) {
                Some(Entry::Ready { lease, .. }) => Some(lease.clone()),
                _ => None,
            }
        };
        if let Some(lease) = existing {
            let mut namespace = LiveNamespace::open_local_file(
                path,
                target.bytes,
                target.block_size,
                LiveNamespaceAccess::ReadShared,
            )?;
            namespace.controller_lease = Some(lease.pin.clone());
            lease
                .namespaces
                .lock()
                .map_err(|_| invalid("namespace map poisoned"))?
                .insert((target.nsid, false), Arc::new(namespace));
            return Ok(());
        }
        let parent = path
            .parent()
            .ok_or_else(|| invalid("fixture path has no parent"))?;
        let controller = LiveController::deleted_for_test(&self.registry, parent, target)?;
        let pin = Arc::new(());
        let mut namespace = LiveNamespace::open_local_file(
            path,
            target.bytes,
            target.block_size,
            LiveNamespaceAccess::ReadShared,
        )?;
        namespace.controller_lease = Some(pin.clone());
        let mut namespaces = HashMap::new();
        namespaces.insert((target.nsid, false), Arc::new(namespace));
        let lease = Arc::new(LiveControllerLease {
            binding: LiveNamespaceBinding {
                namespace_path: path.to_owned(),
                transport: target.transport,
                nqn: target.nqn.clone(),
                uuid: target.uuid,
                nsid: target.nsid,
                bytes: target.bytes,
                block_size: target.block_size,
                access: LiveNamespaceAccess::ReadShared,
                spdk: None,
            },
            pin,
            instance: 0,
            resolver: controller.resolver(),
            namespaces: Mutex::new(namespaces),
            local_fixture: true,
        });
        let mut identity = Identity {
            target: target.clone(),
            host_nqn: host_nqn.to_owned(),
            host_id,
            access: LiveNamespaceAccess::ReadWrite,
            file: None,
        };
        identity.target.nsid = 0;
        identity.target.uuid = [0; 16];
        self.state
            .lock()
            .map_err(|_| invalid("controller pool poisoned"))?
            .entries
            .insert(
                target.nqn.clone(),
                Entry::Ready {
                    identity,
                    lease,
                    controller,
                    usable: true,
                    export_pin: None,
                },
            );
        Ok(())
    }

    /// Local-file fixture for a nonshared recovery controller identity.
    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn cache_local_namespace(
        &self,
        target: &LiveConnectTarget,
        host_nqn: &str,
        host_id: [u8; 16],
        access: LiveNamespaceAccess,
        path: &std::path::Path,
    ) -> io::Result<()> {
        self.cache_local_shared_namespace(target, host_nqn, host_id, path)?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| invalid("controller pool poisoned"))?;
        let Some(Entry::Ready { identity, .. }) = state.entries.get_mut(&target.nqn) else {
            return Err(invalid("fixture controller absent"));
        };
        identity.target = target.clone();
        identity.access = access;
        Ok(())
    }

    pub fn new(
        registry: LiveConnectionRegistry,
        max_controllers: usize,
        max_pending: usize,
    ) -> io::Result<Self> {
        if max_controllers == 0 || max_pending == 0 || max_pending > max_controllers {
            return Err(invalid("invalid controller pool limits"));
        }
        Ok(Self {
            registry,
            capacity: max_controllers,
            max_pending,
            state: Mutex::new(State {
                entries: HashMap::new(),
                owners: HashMap::new(),
                closing: false,
            }),
            changed: Condvar::new(),
            #[cfg(any(test, feature = "test-utils"))]
            local_targets: Mutex::new(HashMap::new()),
        })
    }

    /// Poison cleanup on a bounded setup/lane worker, never an I/O reactor.
    /// One capacity-bounded snapshot; active and pending entries are left for
    /// their owners to drain. Every uncertain outcome keeps its connection pin.
    pub(crate) fn drain_idle(&self, deadline: Instant) -> io::Result<()> {
        let keys: Vec<_> = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| invalid("controller pool poisoned"))?;
            state.closing = true;
            state
                .entries
                .iter()
                .filter_map(|(key, entry)| match entry {
                    Entry::Ready {
                        lease,
                        usable: true,
                        ..
                    } if idle(lease) => Some(key.clone()),
                    _ => None,
                })
                .take(self.capacity)
                .collect()
        };
        let mut first_error = None;
        for key in keys {
            if Instant::now() >= deadline {
                return Err(first_error.unwrap_or_else(timeout));
            }
            let mut state = self
                .state
                .lock()
                .map_err(|_| invalid("controller pool poisoned"))?;
            // Another lane may already have tried this snapshot's entry.
            // Never retry its uncertain delete/release during poison cleanup.
            if !matches!(state.entries.get(&key), Some(Entry::Ready { lease, usable: true, .. }) if idle(lease))
            {
                continue;
            }
            let controller = Self::begin_close(&mut state, &key)?;
            let (state, result) =
                self.finish_close(state, &key, controller, |c| c.disconnect(deadline))?;
            drop(state);
            if let Err(error) = result
                && first_error.is_none()
            {
                first_error = Some(error);
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    #[cfg(test)]
    pub(crate) fn lose_next_pin_reply_for_test(
        &self,
        recipe: &Arc<crate::distributed::tcp::LivePinRecipe>,
    ) {
        self.state
            .lock()
            .unwrap()
            .owners
            .entry(recipe.key.clone())
            .or_insert_with(|| Arc::new(LiveExportPinControl::new(recipe.clone())))
            .lose_next_reply();
    }
    /// Disconnect only controllers for the exact retirement files. Busy/pending
    /// entries refuse immediately; unrelated controllers stay cached.
    pub(crate) fn evict_files(
        &self,
        partition: u32,
        epoch: u64,
        bytes: u64,
        files: &[u64],
        deadline: Instant,
    ) -> io::Result<RetirementResult> {
        let matches = |identity: &Identity| {
            identity.file.is_some_and(|(owner, source, file)| {
                owner == partition && source == epoch && files.contains(&(file * bytes))
            })
        };
        let mut state = self
            .state
            .lock()
            .map_err(|_| invalid("controller pool poisoned"))?;
        loop {
            if Instant::now() >= deadline {
                return Err(timeout());
            }
            // Check all matching users before disconnecting any idle sibling.
            if state.entries.values().any(|entry| match entry {
                Entry::Pending(identity) | Entry::Closing { identity, .. } => matches(identity),
                Entry::Ready {
                    identity, lease, ..
                } => matches(identity) && !idle(lease),
                Entry::Quarantined { .. } => false,
            }) {
                return Ok(RetirementResult::Busy);
            }
            let Some(key) = state.entries.iter().find_map(|(key, entry)| match entry {
                Entry::Ready { identity, .. } if matches(identity) => Some(key.clone()),
                _ => None,
            }) else {
                return Ok(RetirementResult::Completed);
            };
            let controller = Self::begin_close(&mut state, &key)?;
            let (next, result) =
                self.finish_close(state, &key, controller, |c| c.disconnect(deadline))?;
            state = next;
            result?;
        }
    }

    #[cfg(test)]
    pub(crate) fn register_local_target(&self, nqn: &str, path: std::path::PathBuf) {
        self.local_targets
            .lock()
            .unwrap()
            .insert(nqn.to_owned(), path);
    }

    #[cfg(test)]
    pub(crate) fn evict_for_test(
        &self,
        key: &str,
        before_disconnect: impl FnOnce() -> io::Result<()>,
    ) -> io::Result<()> {
        let mut state = self.state.lock().unwrap();
        let controller = Self::begin_close(&mut state, key)?;
        let (_state, result) = self.finish_close(state, key, controller, |controller| {
            before_disconnect()?;
            controller.disconnect(Instant::now() + Duration::from_secs(5))
        })?;
        result
    }

    fn connect(
        &self,
        target: &LiveConnectTarget,
        host_nqn: &str,
        host_id: [u8; 16],
        access: LiveNamespaceAccess,
        deadline: Instant,
    ) -> io::Result<(LiveController, LiveNamespaceBinding)> {
        #[cfg(test)]
        if let Some(path) = self
            .local_targets
            .lock()
            .map_err(|_| invalid("local target map poisoned"))?
            .get(&target.nqn)
        {
            let parent = path
                .parent()
                .ok_or_else(|| invalid("local target has no parent"))?;
            return Ok((
                LiveController::deleted_for_test(&self.registry, parent, target)?,
                LiveNamespaceBinding {
                    namespace_path: path.clone(),
                    transport: target.transport,
                    nqn: target.nqn.clone(),
                    uuid: target.uuid,
                    nsid: target.nsid,
                    bytes: target.bytes,
                    block_size: target.block_size,
                    access,
                    spdk: None,
                },
            ));
        }
        self.registry
            .connect(target, host_nqn, host_id, access, deadline)
    }

    /// Must run on a bounded setup worker, never the reactor. Existing in-flight
    /// requests for this exact target wait for the one shared connection.
    pub fn get_or_connect(
        &self,
        target: &LiveConnectTarget,
        host_nqn: &str,
        host_id: [u8; 16],
        access: LiveNamespaceAccess,
        deadline: Instant,
    ) -> io::Result<(Arc<LiveControllerLease>, LivePoolAcquisition)> {
        self.acquire(target, host_nqn, host_id, access, deadline)
            .map(|(lease, acquisition, _)| (lease, acquisition))
    }
    /// `get_or_connect` that also reports the blocking connect and eviction
    /// durations. The pool lock is held only for map operations: a controller
    /// connect or delete never runs under it.
    pub fn acquire(
        &self,
        target: &LiveConnectTarget,
        host_nqn: &str,
        host_id: [u8; 16],
        access: LiveNamespaceAccess,
        deadline: Instant,
    ) -> io::Result<(
        Arc<LiveControllerLease>,
        LivePoolAcquisition,
        LivePoolTimings,
    )> {
        self.acquire_inner(target, host_nqn, host_id, access, deadline, false, None)
    }

    pub fn acquire_shared(
        &self,
        target: &LiveConnectTarget,
        host_nqn: &str,
        host_id: [u8; 16],
        access: LiveNamespaceAccess,
        deadline: Instant,
    ) -> io::Result<(
        Arc<LiveControllerLease>,
        LivePoolAcquisition,
        LivePoolTimings,
    )> {
        self.acquire_inner(target, host_nqn, host_id, access, deadline, true, None)
    }

    /// A non-permanent export must be pinned before creating its controller.
    /// Hits share the entry's existing token, including across reader/writer
    /// sessions. Permanent exports keep the unpinned acquisition path.
    pub(crate) fn acquire_pinned(
        &self,
        target: &LiveConnectTarget,
        host_nqn: &str,
        host_id: [u8; 16],
        access: LiveNamespaceAccess,
        deadline: Instant,
        request: LiveExportPinRequest<'_>,
    ) -> io::Result<(
        Arc<LiveControllerLease>,
        LivePoolAcquisition,
        LivePoolTimings,
    )> {
        self.acquire_inner(
            target,
            host_nqn,
            host_id,
            access,
            deadline,
            false,
            Some(request),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn acquire_inner(
        &self,
        target: &LiveConnectTarget,
        host_nqn: &str,
        host_id: [u8; 16],
        access: LiveNamespaceAccess,
        deadline: Instant,
        shared: bool,
        pin_request: Option<LiveExportPinRequest<'_>>,
    ) -> io::Result<(
        Arc<LiveControllerLease>,
        LivePoolAcquisition,
        LivePoolTimings,
    )> {
        let mut identity = Identity {
            target: target.clone(),
            host_nqn: host_nqn.to_owned(),
            host_id,
            access,
            file: pin_request.as_ref().map(|request| {
                (
                    request.segment.partition,
                    request.segment.epoch,
                    request.segment.file_index,
                )
            }),
        };
        if shared {
            identity.target.nsid = 0;
            identity.target.uuid = [0; 16];
            identity.access = LiveNamespaceAccess::ReadWrite;
        }
        let key = target.nqn.clone();
        let mut timings = LivePoolTimings::default();
        let mut state = self
            .state
            .lock()
            .map_err(|_| invalid("controller pool poisoned"))?;
        loop {
            if state.closing {
                return Err(invalid("controller pool is closing"));
            }
            if Instant::now() >= deadline {
                return Err(timeout());
            }
            match state.entries.get(&key) {
                Some(Entry::Ready {
                    identity: current,
                    lease,
                    usable,
                    ..
                }) => {
                    if !usable {
                        return Err(invalid("controller is quarantined"));
                    }
                    if *current != identity {
                        return Err(invalid("controller identity conflicts with cached target"));
                    }
                    return Ok((lease.clone(), LivePoolAcquisition::Reused, timings));
                }
                Some(entry @ (Entry::Pending(_) | Entry::Closing { .. })) => {
                    if let Entry::Pending(current) = entry
                        && *current != identity
                    {
                        return Err(invalid("controller identity conflicts with pending target"));
                    }
                    let remaining = deadline
                        .checked_duration_since(Instant::now())
                        .ok_or_else(timeout)?;
                    let (next, wait) = self
                        .changed
                        .wait_timeout(state, remaining)
                        .map_err(|_| invalid("controller pool poisoned"))?;
                    state = next;
                    if wait.timed_out() {
                        return Err(timeout());
                    }
                }
                Some(Entry::Quarantined { .. }) => {
                    return Err(invalid("connection outcome remains quarantined"));
                }
                None => {
                    if state
                        .entries
                        .values()
                        .filter(|e| matches!(e, Entry::Pending(_) | Entry::Quarantined { .. }))
                        .count()
                        >= self.max_pending
                    {
                        return Err(full());
                    }
                    if state.entries.len() >= self.capacity {
                        let idle = state
                            .entries
                            .iter()
                            .find_map(|(name, entry)| match entry {
                                Entry::Ready { lease, .. } if idle(lease) => Some(name.clone()),
                                _ => None,
                            })
                            .ok_or_else(full)?;
                        let controller = Self::begin_close(&mut state, &idle)?;
                        let started = Instant::now();
                        let (next, result) = self
                            .finish_close(state, &idle, controller, |c| c.disconnect(deadline))?;
                        state = next;
                        timings.disconnect = Some(started.elapsed());
                        result?;
                        // The freed slot may have been taken, or the wanted key
                        // created, while the lock was released: decide again.
                        continue;
                    }
                    state
                        .entries
                        .insert(key.clone(), Entry::Pending(identity.clone()));
                    break;
                }
            }
        }
        drop(state);
        let mut export_pin = if let Some(request) = &pin_request {
            let control = {
                let mut state = self
                    .state
                    .lock()
                    .map_err(|_| invalid("controller pool poisoned"))?;
                state
                    .owners
                    .entry(request.recipe.key.clone())
                    .or_insert_with(|| Arc::new(LiveExportPinControl::new(request.recipe.clone())))
                    .clone()
            };
            match control.acquire(request) {
                Ok(pin) => Some(pin),
                Err(error) => {
                    let mut state = self
                        .state
                        .lock()
                        .map_err(|_| invalid("controller pool poisoned"))?;
                    if error.outcome_unknown {
                        state
                            .entries
                            .insert(key.clone(), Entry::Quarantined { _export_pin: None });
                    } else {
                        state.entries.remove(&key);
                    }
                    self.changed.notify_all();
                    return Err(error.error);
                }
            }
        } else {
            None
        };
        let started = Instant::now();
        let result = self.connect(target, host_nqn, host_id, access, deadline);
        let connect_time = started.elapsed();
        let mut state = self
            .state
            .lock()
            .map_err(|_| invalid("controller pool poisoned"))?;
        match result {
            Ok((controller, binding)) => {
                let lease = Arc::new(LiveControllerLease {
                    binding,
                    pin: Arc::new(()),
                    instance: controller.instance(),
                    resolver: controller.resolver(),
                    namespaces: Mutex::new(HashMap::new()),
                    #[cfg(any(test, feature = "test-utils"))]
                    local_fixture: self
                        .local_targets
                        .lock()
                        .map_err(|_| invalid("local target map poisoned"))?
                        .contains_key(&target.nqn),
                });
                state.entries.insert(
                    key,
                    Entry::Ready {
                        identity,
                        lease: lease.clone(),
                        controller,
                        usable: true,
                        export_pin,
                    },
                );
                self.changed.notify_all();
                timings.connect = Some(connect_time);
                Ok((lease, LivePoolAcquisition::Connected, timings))
            }
            Err(error) => {
                // Every failure after intent publication can leave a controller
                // or a late worker, not just timeouts. An unreadable registry
                // is also unknown; release capacity only on confirmed absence.
                if !matches!(self.registry.has_intent(&target.nqn, host_nqn), Ok(false)) {
                    state.entries.insert(
                        key,
                        Entry::Quarantined {
                            _export_pin: export_pin,
                        },
                    );
                } else {
                    drop(state);
                    // No kernel intent exists: the failed connect left no
                    // controller. Keep a failed release quarantined and bounded.
                    if let Some(pin) = &mut export_pin
                        && let Err(release) = pin.release()
                    {
                        self.state
                            .lock()
                            .map_err(|_| invalid("controller pool poisoned"))?
                            .entries
                            .insert(
                                key,
                                Entry::Quarantined {
                                    _export_pin: export_pin,
                                },
                            );
                        self.changed.notify_all();
                        return Err(release);
                    }
                    self.state
                        .lock()
                        .map_err(|_| invalid("controller pool poisoned"))?
                        .entries
                        .remove(&key);
                    self.changed.notify_all();
                    return Err(error);
                }
                self.changed.notify_all();
                Err(error)
            }
        }
    }
    /// Marks an idle `Ready` entry `Closing` and takes its controller, so the
    /// delete can run without the pool lock. Caller must hold the lock.
    fn begin_close(state: &mut State, key: &str) -> io::Result<LiveController> {
        match state.entries.remove(key) {
            Some(Entry::Ready {
                identity,
                lease,
                controller,
                export_pin,
                ..
            }) => {
                lease
                    .namespaces
                    .lock()
                    .map_err(|_| invalid("namespace map poisoned"))?
                    .clear();
                state.entries.insert(
                    key.to_owned(),
                    Entry::Closing {
                        identity,
                        lease,
                        export_pin,
                    },
                );
                Ok(controller)
            }
            Some(other) => {
                state.entries.insert(key.to_owned(), other);
                Err(invalid("controller pool accounting changed"))
            }
            None => Err(invalid("controller pool accounting changed")),
        }
    }
    /// Runs `delete` with the pool lock released, then relocks. Success removes
    /// the `Closing` entry. Failure restores it as an unusable `Ready` entry that
    /// keeps its controller and durable intent for retry or reconciliation: the
    /// delete may have taken effect before the error. Waiters are woken either way.
    fn finish_close<'a>(
        &'a self,
        state: MutexGuard<'a, State>,
        key: &str,
        mut controller: LiveController,
        delete: impl FnOnce(&mut LiveController) -> io::Result<()>,
    ) -> io::Result<(MutexGuard<'a, State>, io::Result<()>)> {
        drop(state);
        let mut result = delete(&mut controller);
        if result.is_ok() {
            let mut export_pin = {
                let mut state = self
                    .state
                    .lock()
                    .map_err(|_| invalid("controller pool poisoned"))?;
                match state.entries.get_mut(key) {
                    Some(Entry::Closing { export_pin, .. }) => export_pin.take(),
                    _ => return Err(invalid("closing controller disappeared")),
                }
            };
            // The controller is confirmed gone before the owner can reap its
            // export. Both operations run outside the pool's accounting lock.
            if let Some(pin) = &mut export_pin {
                result = pin.release();
            }
            if result.is_err() {
                let mut state = self
                    .state
                    .lock()
                    .map_err(|_| invalid("controller pool poisoned"))?;
                if let Some(Entry::Closing {
                    export_pin: retained,
                    ..
                }) = state.entries.get_mut(key)
                {
                    *retained = export_pin;
                }
            }
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| invalid("controller pool poisoned"))?;
        if result.is_ok() {
            state.entries.remove(key);
        } else if let Some(Entry::Closing {
            identity,
            lease,
            export_pin,
        }) = state.entries.remove(key)
        {
            state.entries.insert(
                key.to_owned(),
                Entry::Ready {
                    identity,
                    lease,
                    controller,
                    usable: false,
                    export_pin,
                },
            );
        }
        self.changed.notify_all();
        Ok((state, result))
    }
    /// All accepted I/O must be drained, all namespace descriptors dropped and
    /// every externally held lease released before shutdown. Failed disconnects
    /// retain their entry and durable intent for retry or startup reconciliation.
    pub fn shutdown(&self, deadline: Instant) -> io::Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| invalid("controller pool poisoned"))?;
        for entry in state.entries.values() {
            match entry {
                Entry::Pending(_) | Entry::Quarantined { .. } | Entry::Closing { .. } => {
                    return Err(full());
                }
                Entry::Ready { lease, .. } if !idle(lease) => {
                    return Err(io::Error::new(
                        io::ErrorKind::WouldBlock,
                        "controller still has namespace or lease users",
                    ));
                }
                Entry::Ready { .. } => {}
            }
        }
        state.closing = true;
        let keys: Vec<String> = state.entries.keys().cloned().collect();
        for key in keys {
            let controller = Self::begin_close(&mut state, &key)?;
            let (next, result) =
                self.finish_close(state, &key, controller, |c| c.disconnect(deadline))?;
            state = next;
            result?;
        }
        Ok(())
    }
}

impl Drop for LiveControllerPool {
    fn drop(&mut self) {
        // Pool destruction is blocking, like explicit shutdown. Do not release
        // a pin after an uncertain delete; the next epoch fence reclaims it.
        if let Err(error) = self.shutdown(Instant::now() + Duration::from_secs(30)) {
            eprintln!("live controller pool cleanup failed; pins retained until fence: {error}");
        }
        let Ok(state) = self.state.get_mut() else {
            std::process::abort()
        };
        // A dropped pool must never release its registry lock while a tracked
        // namespace can still have kernel I/O or an external lease can open it.
        if state.entries.values().any(|entry| match entry {
            Entry::Pending(_) => true,
            // Nothing can open I/O on a controller whose delete was started.
            Entry::Quarantined { .. } | Entry::Closing { .. } => false,
            Entry::Ready { lease, .. } => !idle(lease),
        }) {
            std::process::abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::NvmeofTransport;
    use super::*;

    fn local_resolver(dir: &std::path::Path) -> Arc<LiveNamespaceResolver> {
        let registry = LiveConnectionRegistry::open(&dir.join("resolver-registry")).unwrap();
        let target = LiveConnectTarget {
            nqn: "nqn.2026-01.test:wal".into(),
            uuid: [1; 16],
            nsid: 1,
            bytes: 4096,
            block_size: 4096,
            address: "127.0.0.1".into(),
            service: 4420,
            transport: NvmeofTransport::Tcp,
            spdk: None,
            geometry: Default::default(),
        };
        LiveController::deleted_for_test(&registry, dir, &target)
            .unwrap()
            .resolver()
    }

    #[test]
    fn shared_acquisition_uses_subsystem_identity_but_strict_acquisition_keeps_namespace_identity()
    {
        let dir = tempdir::TempDir::new("live-pool-shared").unwrap();
        let registry = LiveConnectionRegistry::open(&dir.path().join("registry")).unwrap();
        let pool = LiveControllerPool::new(registry, 2, 1).unwrap();
        let target = ready_idle(&pool, dir.path(), "nqn.2026-01.test:shared");
        if let Entry::Ready { identity, .. } = pool
            .state
            .lock()
            .unwrap()
            .entries
            .get_mut(&target.nqn)
            .unwrap()
        {
            identity.target.nsid = 0;
            identity.target.uuid = [0; 16];
        }
        let mut other = target.clone();
        other.nsid = 2;
        other.uuid = [3; 16];
        let deadline = Instant::now() + Duration::from_secs(10);
        let (first, how, _) = pool
            .acquire_shared(
                &target,
                "nqn.2026-01.test:host",
                [2; 16],
                LiveNamespaceAccess::ReadWrite,
                deadline,
            )
            .unwrap();
        assert_eq!(how, LivePoolAcquisition::Reused);
        let (second, how, _) = pool
            .acquire_shared(
                &other,
                "nqn.2026-01.test:host",
                [2; 16],
                LiveNamespaceAccess::ReadShared,
                deadline,
            )
            .unwrap();
        assert_eq!(how, LivePoolAcquisition::Reused);
        assert!(Arc::ptr_eq(&first, &second));
        drop((first, second));
        pool.shutdown(deadline).unwrap();

        let registry = LiveConnectionRegistry::open(&dir.path().join("strict")).unwrap();
        let pool = LiveControllerPool::new(registry, 2, 1).unwrap();
        let target = ready_idle(&pool, dir.path(), "nqn.2026-01.test:strict");
        let mut other = target.clone();
        other.nsid = 2;
        other.uuid = [3; 16];
        assert!(
            matches!(pool.acquire(&other, "nqn.2026-01.test:host", [2; 16], LiveNamespaceAccess::ReadWrite, deadline),
            Err(error) if error.kind() == io::ErrorKind::InvalidData)
        );
        pool.shutdown(deadline).unwrap();
    }

    #[test]
    fn pending_and_capacity_are_bounded_without_connecting() {
        let dir = tempdir::TempDir::new("live-pool").unwrap();
        let registry = LiveConnectionRegistry::open(&dir.path().join("registry")).unwrap();
        let pool = LiveControllerPool::new(registry, 1, 1).unwrap();
        let target = LiveConnectTarget {
            nqn: "nqn.2026-01.test:one".into(),
            uuid: [1; 16],
            nsid: 1,
            bytes: 4096,
            block_size: 4096,
            address: "127.0.0.1".into(),
            service: 4420,
            transport: NvmeofTransport::Tcp,
            spdk: None,
            geometry: Default::default(),
        };
        let identity = Identity {
            target: target.clone(),
            host_nqn: "nqn.2026-01.test:host".into(),
            host_id: [2; 16],
            access: LiveNamespaceAccess::ReadWrite,
            file: None,
        };
        pool.state
            .lock()
            .unwrap()
            .entries
            .insert(target.nqn.clone(), Entry::Pending(identity));
        let mut other = target.clone();
        other.nqn = "nqn.2026-01.test:two".into();
        let deadline = Instant::now() + std::time::Duration::from_secs(1);
        let mut different_proof = target.clone();
        different_proof.spdk = Some(std::sync::Arc::new(super::super::SpdkTarget {
            rpc_socket: dir.path().join("rpc.sock"),
            pid: 1,
            process_start_ticks: 1,
            bdev_name: "test".into(),
            image_path: dir.path().join("wal"),
        }));
        assert!(matches!(
            pool.get_or_connect(&different_proof, "nqn.2026-01.test:host", [2; 16], LiveNamespaceAccess::ReadWrite, deadline),
            Err(e) if e.kind() == io::ErrorKind::InvalidData
        ));
        assert!(matches!(
            pool.get_or_connect(&target, "nqn.2026-01.test:host", [2; 16], LiveNamespaceAccess::ReadOnly, deadline),
            Err(e) if e.kind() == io::ErrorKind::InvalidData
        ));
        assert!(
            matches!(pool.get_or_connect(&other, "nqn.2026-01.test:host", [2;16], LiveNamespaceAccess::ReadWrite, deadline), Err(e) if e.kind() == io::ErrorKind::WouldBlock)
        );
        assert!(matches!(pool.shutdown(deadline), Err(e) if e.kind() == io::ErrorKind::WouldBlock));
        pool.state
            .lock()
            .unwrap()
            .entries
            .insert(target.nqn.clone(), Entry::Quarantined { _export_pin: None });
        assert!(
            matches!(pool.get_or_connect(&other, "nqn.2026-01.test:host", [2;16], LiveNamespaceAccess::ReadWrite, deadline), Err(e) if e.kind() == io::ErrorKind::WouldBlock)
        );
        assert!(matches!(pool.shutdown(deadline), Err(e) if e.kind() == io::ErrorKind::WouldBlock));
        pool.state.lock().unwrap().entries.clear();
        let journal = dir.path().join("registry").join("unresolved.json");
        let intent = serde_json::json!({ "target": &target, "host_nqn": "nqn.2026-01.test:host", "host_id": vec![2;16] });
        std::fs::write(&journal, serde_json::to_vec(&intent).unwrap()).unwrap();
        assert!(
            pool.get_or_connect(
                &target,
                "nqn.2026-01.test:host",
                [2; 16],
                LiveNamespaceAccess::ReadWrite,
                deadline
            )
            .is_err()
        );
        assert!(matches!(
            pool.state.lock().unwrap().entries.get(&target.nqn),
            Some(Entry::Quarantined { .. })
        ));
        assert!(matches!(pool.shutdown(deadline), Err(e) if e.kind() == io::ErrorKind::WouldBlock));
        std::fs::remove_file(journal).unwrap();
        pool.state.lock().unwrap().entries.clear();
        pool.shutdown(deadline).unwrap();
    }
    #[test]
    fn idle_requires_released_lease_and_local_namespace_fd() {
        let dir = tempdir::TempDir::new("live-pool-pin").unwrap();
        let path = dir.path().join("wal");
        let file = std::fs::File::create(&path).unwrap();
        file.set_len(4096).unwrap();
        let lease = Arc::new(LiveControllerLease {
            binding: LiveNamespaceBinding {
                namespace_path: path.clone(),
                transport: NvmeofTransport::Tcp,
                nqn: "nqn.2026-01.test:wal".into(),
                uuid: [1; 16],
                nsid: 1,
                bytes: 4096,
                block_size: 4096,
                access: LiveNamespaceAccess::ReadWrite,
                spdk: None,
            },
            pin: Arc::new(()),
            instance: 0,
            resolver: local_resolver(dir.path()),
            namespaces: Mutex::new(HashMap::new()),
            local_fixture: false,
        });
        assert!(idle(&lease));
        let external = lease.clone();
        assert!(!idle(&lease));
        let mut namespace =
            LiveNamespace::open_local_file(&path, 4096, 4096, LiveNamespaceAccess::ReadWrite)
                .unwrap();
        namespace.controller_lease = Some(external.pin.clone());
        drop(external);
        assert!(!idle(&lease));
        drop(namespace);
        assert!(idle(&lease));
    }

    fn ready_idle(
        pool: &LiveControllerPool,
        dir: &std::path::Path,
        nqn: &str,
    ) -> LiveConnectTarget {
        let target = LiveConnectTarget {
            nqn: nqn.into(),
            uuid: [1; 16],
            nsid: 1,
            bytes: 4096,
            block_size: 4096,
            address: "127.0.0.1".into(),
            service: 4420,
            transport: NvmeofTransport::Tcp,
            spdk: None,
            geometry: Default::default(),
        };
        let lease = Arc::new(LiveControllerLease {
            binding: LiveNamespaceBinding {
                namespace_path: dir.join("wal"),
                transport: NvmeofTransport::Tcp,
                nqn: nqn.into(),
                uuid: [1; 16],
                nsid: 1,
                bytes: 4096,
                block_size: 4096,
                access: LiveNamespaceAccess::ReadWrite,
                spdk: None,
            },
            pin: Arc::new(()),
            instance: 0,
            resolver: local_resolver(dir),
            namespaces: Mutex::new(HashMap::new()),
            local_fixture: false,
        });
        let identity = Identity {
            target: target.clone(),
            host_nqn: "nqn.2026-01.test:host".into(),
            host_id: [2; 16],
            access: LiveNamespaceAccess::ReadWrite,
            file: None,
        };
        pool.state.lock().unwrap().entries.insert(
            nqn.into(),
            Entry::Ready {
                identity,
                lease,
                controller: LiveController::deleted_for_test(&pool.registry, dir, &target).unwrap(),
                usable: true,
                export_pin: None,
            },
        );
        target
    }

    #[test]
    fn delete_runs_without_the_pool_lock_and_failure_keeps_an_unusable_entry() {
        let dir = tempdir::TempDir::new("live-pool-close").unwrap();
        let registry = LiveConnectionRegistry::open(&dir.path().join("registry")).unwrap();
        let pool = LiveControllerPool::new(registry, 2, 1).unwrap();
        let target = ready_idle(&pool, dir.path(), "nqn.2026-01.test:one");
        let deadline = Instant::now() + Duration::from_secs(5);
        let key = target.nqn.clone();

        let mut state = pool.state.lock().unwrap();
        let controller = LiveControllerPool::begin_close(&mut state, &key).unwrap();
        assert!(matches!(
            state.entries.get(&key),
            Some(Entry::Closing { .. })
        ));
        let (state, result) = pool
            .finish_close(state, &key, controller, |_| {
                // The pool lock is free while the delete runs.
                let state = pool
                    .state
                    .try_lock()
                    .expect("delete must not hold the pool lock");
                assert!(matches!(
                    state.entries.get("nqn.2026-01.test:one"),
                    Some(Entry::Closing { .. })
                ));
                Err(io::Error::other("delete failed"))
            })
            .unwrap();
        // A failed delete restores the entry, unusable, with its controller.
        assert!(result.is_err());
        assert!(matches!(
            state.entries.get(&key),
            Some(Entry::Ready { usable: false, .. })
        ));
        drop(state);
        assert!(matches!(
            pool.get_or_connect(&target, "nqn.2026-01.test:host", [2; 16], LiveNamespaceAccess::ReadWrite, deadline),
            Err(e) if e.kind() == io::ErrorKind::InvalidData
        ));

        // Poison cleanup must not retry another lane's uncertain delete.
        pool.drain_idle(deadline).unwrap();
        assert!(matches!(
            pool.state.lock().unwrap().entries.get(&key),
            Some(Entry::Ready { usable: false, .. })
        ));

        // Retry through shutdown deletes it and removes the entry.
        pool.shutdown(deadline).unwrap();
        assert!(pool.state.lock().unwrap().entries.is_empty());
    }

    #[test]
    fn callers_for_a_closing_target_wait_for_the_delete_to_end() {
        use std::sync::atomic::{AtomicBool, Ordering};
        let dir = tempdir::TempDir::new("live-pool-wait").unwrap();
        let registry = LiveConnectionRegistry::open(&dir.path().join("registry")).unwrap();
        let pool = LiveControllerPool::new(registry, 2, 1).unwrap();
        let target = ready_idle(&pool, dir.path(), "nqn.2026-01.test:two");
        let key = target.nqn.clone();
        let deleted = AtomicBool::new(false);
        let mut state = pool.state.lock().unwrap();
        let controller = LiveControllerPool::begin_close(&mut state, &key).unwrap();
        std::thread::scope(|scope| {
            let waiter = scope.spawn(|| {
                let result = pool.get_or_connect(
                    &target,
                    "nqn.2026-01.test:host",
                    [2; 16],
                    LiveNamespaceAccess::ReadWrite,
                    Instant::now() + Duration::from_secs(10),
                );
                (result, deleted.load(Ordering::SeqCst))
            });
            let (state, result) = pool
                .finish_close(state, &key, controller, |_| {
                    std::thread::sleep(Duration::from_millis(200));
                    deleted.store(true, Ordering::SeqCst);
                    Err(io::Error::other("delete failed"))
                })
                .unwrap();
            assert!(result.is_err());
            drop(state);
            // Closing is not reusable and not an error: the waiter returns only
            // after the delete ended, and then sees the unusable entry.
            let (result, delete_had_ended) = waiter.join().unwrap();
            assert!(delete_had_ended);
            assert!(matches!(result, Err(e) if e.kind() == io::ErrorKind::InvalidData));
        });
        pool.shutdown(Instant::now() + Duration::from_secs(5))
            .unwrap();
    }
}
