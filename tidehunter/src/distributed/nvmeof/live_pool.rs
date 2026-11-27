//! Bounded controller sharing. Connection setup and shutdown are blocking and
//! must run outside the reactor. A cached lease needs no pool lock to open I/O.
use super::live_connect::{
    ControllerAdoption, LiveConnectTarget, LiveConnectionRegistry, LiveController,
    LiveNamespaceResolver,
};
use super::live_metrics::LiveControllerEvidence;
use super::live_namespace::{LiveNamespace, LiveNamespaceAccess, LiveNamespaceBinding};
use super::live_storage::FILES_PER_SUBSYSTEM;
use crate::distributed::tcp::checkpoint::RetirementResult;
use std::collections::HashMap;
use std::io;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

#[cfg(test)]
mod tests;

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
    // Retirement routing only, not part of the controller's transport identity:
    // partition, source epoch and WAL subsystem of the files it serves.
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

/// Members opened through one shared controller, keyed by NSID and write
/// access. Each handle keeps the binding it was resolved and verified from.
type LeaseNamespaces = HashMap<(u32, bool), (Arc<LiveNamespace>, LiveNamespaceBinding)>;

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
    namespaces: Mutex<LeaseNamespaces>,
    #[cfg(any(test, feature = "test-utils"))]
    local_fixture: bool,
    /// Test override of the liveness probe: 0 uses the real probe, 1 reports
    /// lost, 2 reports live. A local-file fixture has no kernel controller.
    #[cfg(test)]
    controller_lost_override: std::sync::atomic::AtomicU8,
}
impl LiveControllerLease {
    /// The namespace this controller was connected and verified with. Under a
    /// shared lease, other members of the subsystem have their own bindings.
    pub fn binding(&self) -> &LiveNamespaceBinding {
        &self.binding
    }
    /// Instance returned by the successful kernel fabrics creation.
    pub fn controller_instance(&self) -> u32 {
        self.instance
    }
    /// Queue geometry and sysfs identity the connect-time check recorded;
    /// `None` for a local-file fixture.
    pub(crate) fn evidence(&self) -> Option<LiveControllerEvidence> {
        self.resolver.evidence()
    }
    /// Whether this lease's pinned controller is verifiably not live, or its
    /// namespace node vanished. Reads sysfs and stat, so it is an error-path
    /// check only: a caller runs it after an `EIO`/`EBADF` to tell a media
    /// verdict from a controller the kernel has already stopped.
    pub(crate) fn controller_lost(&self) -> bool {
        #[cfg(test)]
        match self
            .controller_lost_override
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            1 => return true,
            2 => return false,
            _ => {}
        }
        #[cfg(any(test, feature = "test-utils"))]
        if self.local_fixture {
            return false;
        }
        if self.resolver.lost() {
            return true;
        }
        match std::fs::metadata(&self.binding.namespace_path) {
            Ok(_) => false,
            Err(error) => error.kind() == io::ErrorKind::NotFound,
        }
    }
    /// Test only: drive the liveness probe without a kernel controller.
    #[cfg(test)]
    pub(crate) fn force_controller_lost(&self, lost: bool) {
        self.controller_lost_override.store(
            if lost { 1 } else { 2 },
            std::sync::atomic::Ordering::Relaxed,
        );
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
    /// Opens one member of a shared subsystem. Returns the handle and the
    /// binding of that member, which route evidence must name.
    pub fn namespace(
        &self,
        nsid: u32,
        uuid: [u8; 16],
        access: LiveNamespaceAccess,
        deadline: Instant,
    ) -> io::Result<(Arc<LiveNamespace>, LiveNamespaceBinding)> {
        let mut namespaces = self
            .namespaces
            .lock()
            .map_err(|_| invalid("namespace map poisoned"))?;
        let key = (nsid, access == LiveNamespaceAccess::ReadWrite);
        if let Some((namespace, binding)) = namespaces.get(&key) {
            return Ok((namespace.clone(), binding.clone()));
        }
        // The ordinary-file fixture has no kernel controller to resolve a
        // member on; the connect path named the member's file directly.
        #[cfg(any(test, feature = "test-utils"))]
        if self.local_fixture && nsid == self.binding.nsid {
            let mut namespace = LiveNamespace::open_local_file(
                &self.binding.namespace_path,
                self.binding.bytes,
                self.binding.block_size,
                access,
            )?;
            namespace.controller_lease = Some(self.pin.clone());
            let namespace = Arc::new(namespace);
            let binding = LiveNamespaceBinding {
                access,
                ..self.binding.clone()
            };
            namespaces.insert(key, (namespace.clone(), binding.clone()));
            return Ok((namespace, binding));
        }
        let binding = self
            .resolver
            .resolve_namespace(nsid, uuid, access, deadline)?;
        let mut namespace = LiveNamespace::open(&binding)?;
        namespace.controller_lease = Some(self.pin.clone());
        let namespace = Arc::new(namespace);
        namespaces.insert(key, (namespace.clone(), binding.clone()));
        Ok((namespace, binding))
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
/// One controller per subsystem NQN: a permanent export shares its subsystem
/// NQN across up to 1024 files, and the kernel groups every controller of a
/// subsystem under one `nvme-subsysN`. Namespaces resolve through that lease.
type PoolKey = String;
/// Fixture-only export path by subsystem NQN and namespace.
#[cfg(any(test, feature = "test-utils"))]
type LocalTargetKey = (String, u32);

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
        .all(|(namespace, _)| Arc::strong_count(namespace) == 1)
        && Arc::strong_count(&lease.pin) == namespaces.len() + 1
}
/// Local-file fixture member of a shared lease, keyed like `namespace`.
#[cfg(any(test, feature = "test-utils"))]
fn cache_local_member(
    lease: &LiveControllerLease,
    target: &LiveConnectTarget,
    access: LiveNamespaceAccess,
    path: &std::path::Path,
) -> io::Result<()> {
    let mut namespace =
        LiveNamespace::open_local_file(path, target.bytes, target.block_size, access)?;
    namespace.controller_lease = Some(lease.pin.clone());
    let binding = LiveNamespaceBinding {
        namespace_path: path.to_owned(),
        transport: target.transport,
        nqn: target.nqn.clone(),
        uuid: target.uuid,
        nsid: target.nsid,
        bytes: target.bytes,
        block_size: target.block_size,
        access,
    };
    lease
        .namespaces
        .lock()
        .map_err(|_| invalid("namespace map poisoned"))?
        .insert(
            (target.nsid, access == LiveNamespaceAccess::ReadWrite),
            (Arc::new(namespace), binding),
        );
    Ok(())
}
/// Why a connect outcome remains quarantined. Only a completed connect can be
/// cleared by a fresh recovery acquisition; an outcome whose worker may still
/// run, or whose durable intent could not be read, stays sealed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum QuarantineCause {
    /// The connect finished with an error and left its durable intent.
    Completed,
    /// The caller stopped waiting for a worker that may still create a
    /// controller; clearing the intent could orphan that controller.
    WorkerUncertain,
    /// The durable intent could not be read, so neither absence nor safety is
    /// proven.
    RegistryUnknown,
}

// Keep bounded controller state inline; boxing would add an allocation at setup.
#[allow(clippy::large_enum_variant)]
enum Entry {
    Pending(Identity),
    Quarantined(QuarantineCause),
    /// An idle controller whose deletion runs outside the pool lock. The entry
    /// still counts against capacity; waiters block until it is removed or, on
    /// a failed delete, restored as an unusable `Ready` entry.
    Closing {
        identity: Identity,
        lease: Arc<LiveControllerLease>,
    },
    Ready {
        identity: Identity,
        lease: Arc<LiveControllerLease>,
        controller: LiveController,
        usable: bool,
    },
}
struct State {
    entries: HashMap<PoolKey, Entry>,
    // One cold metadata connection per configured owner, independent of entry
    // count. Entry creation/eviction never borrows a caller's Client lock.
    closing: bool,
    /// Park events on a `Pending` or `Closing` entry (a re-park counts again). Test
    /// barrier only, see `LiveControllerPool::wait_parked_waiters`.
    #[cfg(test)]
    parked_waiters: u64,
}
pub struct LiveControllerPool {
    registry: LiveConnectionRegistry,
    capacity: usize,
    max_pending: usize,
    state: Mutex<State>,
    changed: Condvar,
    #[cfg(any(test, feature = "test-utils"))]
    local_targets: Mutex<HashMap<LocalTargetKey, std::path::PathBuf>>,
    /// Test only: the next connects report a controller the kernel stopped,
    /// exactly as a first-connect verify does. A count keeps a retry budget
    /// observable: the pool must consume exactly one per lost connect.
    #[cfg(test)]
    fail_next_connect_lost: std::sync::atomic::AtomicU32,
    /// Test only: every connect attempt, lost or not. A retry-budget test
    /// reads it to prove the number of page attempts, independent of how many
    /// fixture losses were armed.
    #[cfg(test)]
    connect_attempts: std::sync::atomic::AtomicU32,
    /// Test only: connects the fixture answered with a lost controller. A
    /// budget test reads it to prove every expected attempt saw its loss.
    #[cfg(test)]
    lost_connect_injections: std::sync::atomic::AtomicU32,
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
            return cache_local_member(&lease, target, LiveNamespaceAccess::ReadShared, path);
        }
        let parent = path
            .parent()
            .ok_or_else(|| invalid("fixture path has no parent"))?;
        let controller = LiveController::deleted_for_test(&self.registry, parent, target)?;
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
            },
            pin: Arc::new(()),
            instance: 0,
            resolver: controller.resolver(),
            namespaces: Mutex::new(HashMap::new()),
            local_fixture: true,
            #[cfg(test)]
            controller_lost_override: std::sync::atomic::AtomicU8::new(0),
        });
        cache_local_member(&lease, target, LiveNamespaceAccess::ReadShared, path)?;
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
                },
            );
        Ok(())
    }

    /// Local-file fixture: one more member of an already cached shared lease.
    #[cfg(test)]
    pub(crate) fn cache_local_member_for_test(
        &self,
        target: &LiveConnectTarget,
        access: LiveNamespaceAccess,
        path: &std::path::Path,
    ) -> io::Result<()> {
        let lease = match self
            .state
            .lock()
            .map_err(|_| invalid("controller pool poisoned"))?
            .entries
            .get(&target.nqn)
        {
            Some(Entry::Ready { lease, .. }) => lease.clone(),
            _ => return Err(invalid("fixture controller absent")),
        };
        cache_local_member(&lease, target, access, path)
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
                closing: false,
                #[cfg(test)]
                parked_waiters: 0,
            }),
            changed: Condvar::new(),
            #[cfg(any(test, feature = "test-utils"))]
            local_targets: Mutex::new(HashMap::new()),
            #[cfg(test)]
            fail_next_connect_lost: std::sync::atomic::AtomicU32::new(0),
            #[cfg(test)]
            connect_attempts: std::sync::atomic::AtomicU32::new(0),
            #[cfg(test)]
            lost_connect_injections: std::sync::atomic::AtomicU32::new(0),
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
        if bytes == 0 {
            return Err(invalid("live file size is zero"));
        }
        // A controller serves every namespace of its subsystem, so a victim
        // file matches the controller of its subsystem. Busy controllers defer.
        let subsystems: Vec<u64> = files
            .iter()
            .map(|start| start / bytes / FILES_PER_SUBSYSTEM)
            .collect();
        let matches = |identity: &Identity| {
            identity.file.is_some_and(|(owner, source, subsystem)| {
                owner == partition && source == epoch && subsystems.contains(&subsystem)
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
                Entry::Quarantined(_) => false,
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

    /// Evict the cached controller of one subsystem after its data path broke.
    ///
    /// Retires the entry only when its lease is verifiably not live: a healthy
    /// replacement another lane cached is left alone. `deadline` is the
    /// teardown's own bound (the callers pass `connect_timeout_ms`), never a
    /// nearly spent operation budget. A delete that does not confirm removal
    /// while the lease is still verifiably not live drops the entry anyway: the
    /// durable intent stays in the registry and the next connect reconciles it,
    /// so a slow teardown never quarantines the subsystem.
    pub(crate) fn evict_lost(&self, nqn: &str, deadline: Instant) -> io::Result<()> {
        let key = nqn.to_owned();
        let lease = {
            let state = self
                .state
                .lock()
                .map_err(|_| invalid("controller pool poisoned"))?;
            match state.entries.get(&key) {
                // Nothing cached: the next acquisition connects its controller.
                None => return Ok(()),
                // A pending connect owns its entry, a closing one is already
                // being retired, and a quarantined outcome has no controller.
                Some(Entry::Pending(_) | Entry::Closing { .. } | Entry::Quarantined(_)) => {
                    return Ok(());
                }
                Some(Entry::Ready { lease, .. }) => lease.clone(),
            }
        };
        // The probe reads sysfs, so it runs with no pool lock held.
        if !lease.controller_lost() {
            return Ok(());
        }
        let controller = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| invalid("controller pool poisoned"))?;
            match state.entries.get(&key) {
                Some(Entry::Ready { lease: current, .. }) if Arc::ptr_eq(current, &lease) => {}
                // Another lane retired or replaced it between the probe and now.
                _ => return Ok(()),
            }
            Self::begin_close(&mut state, &key)?
        };
        let mut controller = controller;
        let result = controller.disconnect(deadline);
        let remaining_lost = result.is_err() && lease.controller_lost();
        let mut state = self
            .state
            .lock()
            .map_err(|_| invalid("controller pool poisoned"))?;
        if result.is_ok() || remaining_lost {
            state.entries.remove(&key);
        } else if let Some(Entry::Closing { identity, lease }) = state.entries.remove(&key) {
            // The lease is live again, so the delete failed for another reason:
            // keep the controller for reconciliation, unusable until then.
            state.entries.insert(
                key.clone(),
                Entry::Ready {
                    identity,
                    lease,
                    controller,
                    usable: false,
                },
            );
        }
        self.changed.notify_all();
        drop(state);
        if result.is_ok() || remaining_lost {
            Ok(())
        } else {
            result
        }
    }

    /// Test only: the next connect fails with the typed lost-controller error,
    /// as a first connect that verifies a controller the kernel stopped does.
    #[cfg(test)]
    pub(crate) fn fail_next_connect_lost(&self) {
        self.fail_next_connect_lost
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Test only: how many armed lost-connects remain unconsumed. A retry
    /// budget test reads it to prove every attempt reached its connect.
    #[cfg(test)]
    pub(crate) fn connect_losses_remaining(&self) -> u32 {
        self.fail_next_connect_lost
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Test only: how many connects this pool attempted.
    #[cfg(test)]
    pub(crate) fn connect_attempts(&self) -> u32 {
        self.connect_attempts
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Test only: how many connects the fixture answered as a lost controller.
    #[cfg(test)]
    pub(crate) fn lost_connect_injections(&self) -> u32 {
        self.lost_connect_injections
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Controllers a fresh recovery acquisition retired in this process.
    pub(crate) fn retired_leftover_controllers(&self) -> u64 {
        self.registry.retired_leftover_controllers()
    }

    /// Whether the subsystem's cached controller is verifiably not live. Reads
    /// sysfs, so a caller runs it only after an acquisition failed.
    pub(crate) fn entry_lost(&self, nqn: &str) -> bool {
        let lease = {
            let Ok(state) = self.state.lock() else {
                return false;
            };
            match state.entries.get(nqn) {
                Some(Entry::Ready { lease, .. }) => lease.clone(),
                _ => return false,
            }
        };
        lease.controller_lost()
    }

    /// The lease a subsystem currently caches, if it is `Ready`. A test reads
    /// it before and after a data break to prove the resume evicted the lost
    /// controller instead of reopening on it.
    #[cfg(test)]
    pub(crate) fn cached_lease(&self, nqn: &str) -> Option<Arc<LiveControllerLease>> {
        match self.state.lock().ok()?.entries.get(nqn) {
            Some(Entry::Ready { lease, .. }) => Some(lease.clone()),
            _ => None,
        }
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn register_local_target(
        &self,
        target: &LiveConnectTarget,
        access: LiveNamespaceAccess,
        path: std::path::PathBuf,
    ) -> io::Result<()> {
        // A poisoned map holds only fixture paths; recover it rather than panic.
        self.local_targets
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert((target.nqn.clone(), target.nsid), path.clone());
        // A subsystem lease already serving another file resolves this member
        // through its own map, not the kernel.
        let lease = self
            .state
            .lock()
            .map_err(|_| invalid("controller pool poisoned"))?
            .entries
            .get(&target.nqn)
            .and_then(|entry| match entry {
                Entry::Ready { lease, .. } => Some(lease.clone()),
                _ => None,
            });
        match lease {
            Some(lease) => cache_local_member(&lease, target, access, &path),
            None => Ok(()),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn connect(
        &self,
        target: &LiveConnectTarget,
        host_nqn: &str,
        host_id: [u8; 16],
        access: LiveNamespaceAccess,
        adoption: ControllerAdoption,
        deadline: Instant,
    ) -> io::Result<(LiveController, LiveNamespaceBinding)> {
        #[cfg(test)]
        self.connect_attempts
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        // Exactly one armed loss per connect: a retry budget must be able to
        // spend several without swallowing the rest.
        #[cfg(test)]
        if self
            .fail_next_connect_lost
            .fetch_update(
                std::sync::atomic::Ordering::Relaxed,
                std::sync::atomic::Ordering::Relaxed,
                |armed| armed.checked_sub(1),
            )
            .is_ok()
        {
            self.lost_connect_injections
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return Err(crate::distributed::nvmeof::live_namespace::controller_lost_error());
        }
        #[cfg(any(test, feature = "test-utils"))]
        if let Some(path) = self
            .local_targets
            .lock()
            .map_err(|_| invalid("local target map poisoned"))?
            .get(&(target.nqn.clone(), target.nsid))
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
                },
            ));
        }
        self.registry
            .connect(target, host_nqn, host_id, access, adoption, deadline)
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
        self.acquire_inner(
            target,
            host_nqn,
            host_id,
            access,
            ControllerAdoption::Adopt,
            deadline,
            None,
        )
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
        self.acquire_inner(
            target,
            host_nqn,
            host_id,
            access,
            ControllerAdoption::Adopt,
            deadline,
            None,
        )
    }

    /// File grant: tag the subsystem controller with the WAL subsystem it
    /// serves, so a retirement evicts the controller of its victims.
    /// `adoption` is explicit at each call: a recovery read must never reuse a
    /// controller the previous owner incarnation's process left behind.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn acquire_file(
        &self,
        target: &LiveConnectTarget,
        host_nqn: &str,
        host_id: [u8; 16],
        access: LiveNamespaceAccess,
        adoption: ControllerAdoption,
        deadline: Instant,
        file: (u32, u64, u64),
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
            adoption,
            deadline,
            Some(file),
        )
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
        self.acquire_inner(
            target,
            host_nqn,
            host_id,
            access,
            ControllerAdoption::Adopt,
            deadline,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn acquire_inner(
        &self,
        target: &LiveConnectTarget,
        host_nqn: &str,
        host_id: [u8; 16],
        access: LiveNamespaceAccess,
        adoption: ControllerAdoption,
        deadline: Instant,
        file: Option<(u32, u64, u64)>,
    ) -> io::Result<(
        Arc<LiveControllerLease>,
        LivePoolAcquisition,
        LivePoolTimings,
    )> {
        // One controller serves every namespace of a subsystem. Identity names
        // the subsystem, not the member that triggered this acquisition; the
        // member resolves through `LiveControllerLease::namespace`.
        let mut identity = Identity {
            target: target.clone(),
            host_nqn: host_nqn.to_owned(),
            host_id,
            access: LiveNamespaceAccess::ReadWrite,
            file: file
                .map(|(partition, epoch, file)| (partition, epoch, file / FILES_PER_SUBSYSTEM)),
        };
        identity.target.nsid = 0;
        identity.target.uuid = [0; 16];
        let key = identity.target.nqn.clone();
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
                    #[cfg(test)]
                    {
                        state.parked_waiters += 1;
                        self.changed.notify_all();
                    }
                    let (next, wait) = self
                        .changed
                        .wait_timeout(state, remaining)
                        .map_err(|_| invalid("controller pool poisoned"))?;
                    state = next;
                    if wait.timed_out() {
                        return Err(timeout());
                    }
                }
                Some(Entry::Quarantined(cause)) => {
                    // A fresh recovery connect may clear only a completed
                    // outcome. A worker that may still run, or an unreadable
                    // registry, can hold a controller whose intent must stay
                    // sealed until a process restart reconciles it.
                    if adoption == ControllerAdoption::Adopt || *cause != QuarantineCause::Completed
                    {
                        return Err(invalid("connection outcome remains quarantined"));
                    }
                    state.entries.remove(&key);
                    // The freed slot and pending count changed: decide again.
                    continue;
                }
                None => {
                    if state
                        .entries
                        .values()
                        .filter(|e| matches!(e, Entry::Pending(_) | Entry::Quarantined(_)))
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
        let started = Instant::now();
        let result = self.connect(target, host_nqn, host_id, access, adoption, deadline);
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
                        .contains_key(&(target.nqn.clone(), target.nsid)),
                    #[cfg(test)]
                    controller_lost_override: std::sync::atomic::AtomicU8::new(0),
                });
                state.entries.insert(
                    key,
                    Entry::Ready {
                        identity,
                        lease: lease.clone(),
                        controller,
                        usable: true,
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
                let cause = if super::live_connect::connect_worker_uncertain(&error) {
                    // A late worker can still create a controller even when
                    // the intent currently reads absent.
                    QuarantineCause::WorkerUncertain
                } else {
                    match self.registry.has_intent(&target.nqn, host_nqn) {
                        Ok(false) => {
                            drop(state);
                            // No kernel intent exists: the failed connect left
                            // no controller.
                            self.state
                                .lock()
                                .map_err(|_| invalid("controller pool poisoned"))?
                                .entries
                                .remove(&key);
                            self.changed.notify_all();
                            return Err(error);
                        }
                        Ok(true) => QuarantineCause::Completed,
                        Err(_) => QuarantineCause::RegistryUnknown,
                    }
                };
                state.entries.insert(key, Entry::Quarantined(cause));
                self.changed.notify_all();
                Err(error)
            }
        }
    }
    /// Marks an idle `Ready` entry `Closing` and takes its controller, so the
    /// delete can run without the pool lock. Caller must hold the lock.
    fn begin_close(state: &mut State, key: &PoolKey) -> io::Result<LiveController> {
        match state.entries.remove(key) {
            Some(Entry::Ready {
                identity,
                lease,
                controller,
                ..
            }) => {
                lease
                    .namespaces
                    .lock()
                    .map_err(|_| invalid("namespace map poisoned"))?
                    .clear();
                state
                    .entries
                    .insert(key.clone(), Entry::Closing { identity, lease });
                Ok(controller)
            }
            Some(other) => {
                state.entries.insert(key.clone(), other);
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
        key: &PoolKey,
        mut controller: LiveController,
        delete: impl FnOnce(&mut LiveController) -> io::Result<()>,
    ) -> io::Result<(MutexGuard<'a, State>, io::Result<()>)> {
        drop(state);
        let result = delete(&mut controller);
        let mut state = self
            .state
            .lock()
            .map_err(|_| invalid("controller pool poisoned"))?;
        if result.is_ok() {
            state.entries.remove(key);
        } else if let Some(Entry::Closing { identity, lease }) = state.entries.remove(key) {
            state.entries.insert(
                key.to_owned(),
                Entry::Ready {
                    identity,
                    lease,
                    controller,
                    usable: false,
                },
            );
        }
        self.changed.notify_all();
        Ok((state, result))
    }
    /// Test barrier: block until `count` park events happened on a `Pending` or `Closing`
    /// entry. The count rises under the pool lock right before a caller parks, so a test
    /// that sees it knows a caller is parked. A caller that wakes and parks again counts
    /// again, so a test with more than one waiter must not read the count as a number of
    /// distinct callers. Panics with a message if `deadline` (a hang guard, never a
    /// premise) passes first.
    #[cfg(test)]
    fn wait_parked_waiters(&self, count: u64, deadline: Instant) {
        let mut state = self.state.lock().expect("pool lock");
        while state.parked_waiters < count {
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .unwrap_or_default();
            assert!(
                !remaining.is_zero(),
                "parked waiters stayed below {count} (now {}) before the hang-guard deadline",
                state.parked_waiters
            );
            state = self
                .changed
                .wait_timeout(state, remaining)
                .expect("pool lock")
                .0;
        }
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
                Entry::Pending(_) | Entry::Quarantined(_) | Entry::Closing { .. } => {
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
        let keys: Vec<PoolKey> = state.entries.keys().cloned().collect();
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
        // a pin after an uncertain delete.
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
            Entry::Quarantined(_) | Entry::Closing { .. } => false,
            Entry::Ready { lease, .. } => !idle(lease),
        }) {
            std::process::abort();
        }
    }
}
