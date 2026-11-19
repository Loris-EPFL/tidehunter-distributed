//! The host lifecycle actor: sole owner of the catalogue, the slot pool and
//! the tombstones. Host sessions submit provision and drop jobs through one
//! bounded queue and wait on a capacity-one reply channel.
//!
//! Queue: capacity `max_databases + 1` (one job per possible database plus a
//! retry). Full-queue policy: reject with `WouldBlock("owner lifecycle queue
//! full")` before any effect. The reply send never blocks (capacity one, one
//! reply per job); a session that timed out has dropped its receiver and the
//! finished job's reply is discarded. Shutdown: the handle drops the sender,
//! the actor finishes its current job, drains no further jobs (each pending
//! reply channel closes, which the session reports as an unknown outcome)
//! and exits; the handle joins it.
//!
//! Every persisted step is followed by a named `HostStep` boundary.

use super::catalog::{self, DatabaseProvisionSpec, DropPhase, HostCatalog, HostEntry, HostState};
use super::tombstone::{self, DroppedDb};
use super::{HostCore, HostStep, identity};
use crate::distributed::durability::DurabilityTier;
use crate::distributed::storage::PartitionLog;
use crate::distributed::tcp::{LIFECYCLE_TIMEOUT, StorageNode, TcpError, TcpLimits};
use crate::key_shape::KeyShape;
use crate::lock::DbLock;
use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io;
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

/// A provision request after decoding, before any validation.
pub(crate) struct ProvisionRequest {
    pub database: [u8; 16],
    pub spec: DatabaseProvisionSpec,
}

/// Success of a provision: the echoed identity and the database's tier.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ProvisionAck {
    pub database: [u8; 16],
    pub partition: u32,
    pub durability_tier: DurabilityTier,
}

pub(crate) struct DropRequest {
    pub database: [u8; 16],
    pub authority: [u8; 32],
    pub generation: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DropAck {
    pub database: [u8; 16],
    pub generation: u64,
}

type Reply<T> = SyncSender<Result<T, TcpError>>;

enum Job {
    Provision {
        // Boxed: a provision request is far larger than a drop, and the
        // bounded queue stores every slot at the largest variant's size.
        request: Box<ProvisionRequest>,
        limits: TcpLimits,
        principal: [u8; 32],
        reply: Reply<ProvisionAck>,
    },
    Drop {
        request: DropRequest,
        principal: [u8; 32],
        reply: Reply<DropAck>,
    },
}

pub(crate) struct ActorHandle {
    sender: Mutex<Option<SyncSender<Job>>>,
    thread: Mutex<Option<JoinHandle<()>>>,
}

fn invalid_input(message: String) -> TcpError {
    TcpError::with_kind(io::ErrorKind::InvalidInput, message)
}

fn corrupt(message: String) -> TcpError {
    TcpError::with_kind(io::ErrorKind::InvalidData, message)
}

impl ActorHandle {
    pub(super) fn start(core: Arc<HostCore>, catalog: HostCatalog) -> Result<Self, TcpError> {
        let capacity = core.config.max_databases + 1;
        let (sender, jobs) = mpsc::sync_channel(capacity);
        let thread = thread::Builder::new()
            .name("owner-host-lifecycle".into())
            .spawn(move || {
                let mut actor = Actor {
                    core,
                    catalog,
                    closing: BTreeMap::new(),
                };
                actor.resume_pending_drops();
                actor.run(&jobs);
            })?;
        Ok(Self {
            sender: Mutex::new(Some(sender)),
            thread: Mutex::new(Some(thread)),
        })
    }

    fn submit<T>(
        &self,
        make: impl FnOnce(Reply<T>) -> Job,
    ) -> Result<Receiver<Result<T, TcpError>>, TcpError> {
        let (reply, receiver) = mpsc::sync_channel(1);
        let sender = self.sender.lock().clone();
        let sender = sender.ok_or_else(|| TcpError::new("owner lifecycle actor stopped"))?;
        match sender.try_send(make(reply)) {
            Ok(()) => Ok(receiver),
            Err(TrySendError::Full(_)) => Err(TcpError::with_kind(
                io::ErrorKind::WouldBlock,
                "owner lifecycle queue full",
            )),
            Err(TrySendError::Disconnected(_)) => {
                Err(TcpError::new("owner lifecycle actor stopped"))
            }
        }
    }

    pub(crate) fn submit_provision(
        &self,
        request: ProvisionRequest,
        limits: TcpLimits,
        principal: [u8; 32],
    ) -> Result<Receiver<Result<ProvisionAck, TcpError>>, TcpError> {
        self.submit(|reply| Job::Provision {
            request: Box::new(request),
            limits,
            principal,
            reply,
        })
    }

    pub(crate) fn submit_drop(
        &self,
        request: DropRequest,
        principal: [u8; 32],
    ) -> Result<Receiver<Result<DropAck, TcpError>>, TcpError> {
        self.submit(|reply| Job::Drop {
            request,
            principal,
            reply,
        })
    }

    pub(super) fn shutdown(&self) {
        self.sender.lock().take();
        if let Some(thread) = self.thread.lock().take() {
            // A panicked actor already stopped serving; nothing to propagate.
            let _ = thread.join();
        }
    }
}

struct Actor {
    core: Arc<HostCore>,
    catalog: HostCatalog,
    /// Databases closed by a drop whose sessions have not all exited yet.
    closing: BTreeMap<[u8; 16], Arc<StorageNode>>,
}

impl Actor {
    fn run(&mut self, jobs: &Receiver<Job>) {
        // One job at a time: lifecycle work is cold and its syncs serialize
        // on the catalogue anyway. The loop ends when the handle closes.
        while let Ok(job) = jobs.recv() {
            match job {
                Job::Provision {
                    request,
                    limits,
                    principal,
                    reply,
                } => {
                    let _ = reply.try_send(self.provision(*request, limits, principal));
                }
                Job::Drop {
                    request,
                    principal,
                    reply,
                } => {
                    let _ = reply.try_send(self.drop_database(request, principal));
                }
            }
        }
    }

    /// Startup: finish drops whose intent is durable. A failure leaves the
    /// entry for the coordinator's retried Drop; it never routes the database.
    fn resume_pending_drops(&mut self) {
        let pending: Vec<_> = self
            .catalog
            .entries
            .iter()
            .filter_map(|entry| match entry.state {
                HostState::Dropping {
                    final_generation,
                    phase,
                } => Some((entry.clone(), final_generation, phase)),
                _ => None,
            })
            .collect();
        for (entry, final_generation, phase) in pending {
            if let Err(error) = self.finish_drop(&entry, final_generation, phase) {
                eprintln!(
                    "owner host: pending drop of {} not finished: {error}",
                    identity::database_hex(&entry.database)
                );
            }
        }
    }

    fn persist(&mut self, next: HostCatalog) -> Result<(), TcpError> {
        next.persist(&self.core.root, self.core.config.max_databases)?;
        self.catalog = next;
        Ok(())
    }

    fn set_state(&mut self, database: &[u8; 16], state: HostState) -> Result<(), TcpError> {
        let mut next = self.catalog.clone();
        let entry = next
            .entry_mut(database)
            .ok_or_else(|| corrupt("catalogue entry vanished".into()))?;
        entry.state = state;
        self.persist(next)
    }

    fn validate_spec(&self, request: &ProvisionRequest, limits: TcpLimits) -> Result<(), TcpError> {
        let hex = identity::database_hex(&request.database);
        let partition = &request.spec.partition;
        if request.database == [0; 16] {
            return Err(invalid_input("ProvisionDatabase: zero database ID".into()));
        }
        partition
            .validate()
            .map_err(|error| invalid_input(format!("ProvisionDatabase {hex}: {error}")))?;
        if partition.inventory.database != request.database
            || partition.inventory.epoch != 1
            || partition.inventory.segment_generation != 1
        {
            return Err(invalid_input(format!(
                "ProvisionDatabase {hex}: partition must bind this database at template generation 1"
            )));
        }
        KeyShape::from_yaml(&request.spec.shape_yaml)
            .map_err(|error| invalid_input(format!("ProvisionDatabase {hex}: shape: {error}")))?;
        request.spec.live_target.validate()?;
        // Fits the actual host transport, not a dedicated node.
        if partition.group_limits.max_encoded_bytes > limits.max_frame_bytes.saturating_sub(512) {
            return Err(invalid_input(format!(
                "ProvisionDatabase {hex}: complete groups exceed the host frame bound"
            )));
        }
        Ok(())
    }

    fn provision(
        &mut self,
        request: ProvisionRequest,
        limits: TcpLimits,
        principal: [u8; 32],
    ) -> Result<ProvisionAck, TcpError> {
        self.validate_spec(&request, limits)?;
        let hex = identity::database_hex(&request.database);
        if tombstone::load(&self.core.root, &request.database)?.is_some() {
            return Err(invalid_input(format!(
                "ProvisionDatabase {hex}: database was dropped"
            )));
        }
        let entry = match self.catalog.entry(&request.database).cloned() {
            Some(entry) => {
                if entry.principal != principal {
                    return Err(TcpError::with_kind(
                        io::ErrorKind::PermissionDenied,
                        format!("ProvisionDatabase {hex}: principal differs"),
                    ));
                }
                if entry.spec != request.spec {
                    return Err(invalid_input(format!(
                        "ProvisionDatabase {hex}: provision spec differs"
                    )));
                }
                match entry.state {
                    HostState::Ready => return self.ack(&entry),
                    HostState::Dropping { .. } => {
                        return Err(invalid_input(format!("database {hex} is dropping")));
                    }
                    // Retry the same reservation, never a second slot.
                    HostState::Preparing => entry,
                }
            }
            None => {
                let max = self.core.config.max_databases;
                // Every state counts: a Preparing or Dropping entry keeps its slot.
                let slot = (self.catalog.entries.len() < max)
                    .then(|| self.catalog.least_free_slot(max))
                    .flatten()
                    .ok_or_else(|| invalid_input(format!("owner database limit {max} reached")))?;
                self.core.step(HostStep::ProvisionBeforeReserve)?;
                let entry = HostEntry {
                    database: request.database,
                    principal,
                    slot,
                    spec: request.spec,
                    state: HostState::Preparing,
                };
                let mut next = self.catalog.clone();
                next.insert(entry.clone());
                self.persist(next)?;
                self.core.step(HostStep::ProvisionAfterReserve)?;
                entry
            }
        };
        self.setup(&entry)
    }

    /// Replayable setup of a Preparing entry; the catalogue is the sole
    /// authority for what it may create.
    fn setup(&mut self, entry: &HostEntry) -> Result<ProvisionAck, TcpError> {
        let root = catalog::database_root(&self.core.root, &entry.database);
        super::create_private_directory(&root)?;
        self.core.step(HostStep::ProvisionAfterDirectory)?;
        // Creates and syncs the inventory, target journal and identity marker.
        let node = self.core.open_database(entry)?;
        let (limits, _) = self.core.serving()?;
        node.validate_limits(limits)?;
        self.core.step(HostStep::ProvisionAfterSetup)?;
        self.set_state(&entry.database, HostState::Ready)?;
        self.core.step(HostStep::ProvisionAfterReady)?;
        self.core.routes.write().insert(entry.database, node);
        self.ack(entry)
    }

    fn ack(&self, entry: &HostEntry) -> Result<ProvisionAck, TcpError> {
        let node = self.core.route(&entry.database).ok_or_else(|| {
            corrupt(format!(
                "Ready database {} is not routed",
                identity::database_hex(&entry.database)
            ))
        })?;
        Ok(ProvisionAck {
            database: entry.database,
            partition: entry.spec.partition.partition,
            durability_tier: node.durability().tier,
        })
    }

    fn drop_database(
        &mut self,
        request: DropRequest,
        principal: [u8; 32],
    ) -> Result<DropAck, TcpError> {
        let hex = identity::database_hex(&request.database);
        let denied = || {
            TcpError::with_kind(
                io::ErrorKind::PermissionDenied,
                format!("DropDatabase {hex}: principal differs"),
            )
        };
        if request.authority != principal {
            return Err(denied());
        }
        let ack = DropAck {
            database: request.database,
            generation: request.generation,
        };
        let Some(entry) = self.catalog.entry(&request.database).cloned() else {
            if let Some(dropped) = tombstone::load(&self.core.root, &request.database)? {
                if dropped.principal != request.authority {
                    return Err(denied());
                }
                if dropped.final_generation != request.generation {
                    return Err(invalid_input(format!(
                        "DropDatabase {hex}: terminal generation differs"
                    )));
                }
                return Ok(ack);
            }
            // Provision never reached this owner: a terminal tombstone blocks
            // a delayed Provision of the same identity.
            if fs::symlink_metadata(catalog::database_root(&self.core.root, &request.database))
                .is_ok()
            {
                return Err(corrupt(format!(
                    "database directory {hex} exists without a catalogue entry"
                )));
            }
            tombstone::ensure_directory(&self.core.root)?;
            tombstone::install(
                &self.core.root,
                &DroppedDb {
                    database: request.database,
                    principal,
                    final_generation: request.generation,
                },
            )?;
            return Ok(ack);
        };
        if entry.principal != request.authority {
            return Err(denied());
        }
        match entry.state {
            HostState::Dropping {
                final_generation,
                phase,
            } => {
                if final_generation != request.generation {
                    return Err(invalid_input(format!(
                        "DropDatabase {hex}: terminal generation differs"
                    )));
                }
                self.finish_drop(&entry, final_generation, phase)
            }
            HostState::Preparing | HostState::Ready => {
                if let Some(node) = self
                    .core
                    .route(&request.database)
                    .or_else(|| self.closing.get(&request.database).cloned())
                {
                    node.close_for_drop(request.generation)?;
                } else if entry.state == HostState::Ready {
                    // A retry may see an unrouted Ready database if the prior
                    // command failed between releasing it and persisting intent.
                    let config = self.core.database_config(&entry)?;
                    let high_water =
                        super::super::inventory::NodeInventory::persisted_high_water(&config)?;
                    if request.generation < high_water {
                        return Err(invalid_input(format!(
                            "DropDatabase {hex}: stale fencing generation {} < {high_water}",
                            request.generation
                        )));
                    }
                }
                self.close(&request.database)?;
                self.core.step(HostStep::DropAfterClose)?;
                let root = catalog::database_root(&self.core.root, &entry.database);
                let present = match fs::symlink_metadata(&root) {
                    Ok(metadata) if metadata.file_type().is_dir() => true,
                    Ok(_) => {
                        return Err(corrupt(format!("database {hex} root is not a directory")));
                    }
                    Err(error) if error.kind() == io::ErrorKind::NotFound => false,
                    Err(error) => return Err(error.into()),
                };
                // A Preparing entry that never created its directory has no
                // target journal to reconcile; a Ready one must have it.
                let phase = match (present, entry.state) {
                    (true, _) => DropPhase::TargetCleanup,
                    (false, HostState::Preparing) => DropPhase::FilesOnly,
                    (false, _) => {
                        return Err(corrupt(format!("Ready database {hex} has no directory")));
                    }
                };
                self.set_state(
                    &entry.database,
                    HostState::Dropping {
                        final_generation: request.generation,
                        phase,
                    },
                )?;
                self.core.step(HostStep::DropAfterIntent)?;
                self.finish_drop(&entry, request.generation, phase)
            }
        }
    }

    /// Close this database only: unroute it, close its sessions and
    /// readers, wait for every session to release it, then drop it (joining
    /// its control owner, flusher and syncers and releasing its locks).
    fn close(&mut self, database: &[u8; 16]) -> Result<(), TcpError> {
        let removed = self.core.routes.write().remove(database);
        if let Some(node) = removed {
            self.closing.insert(*database, node);
        }
        let Some(mut node) = self.closing.remove(database) else {
            return Ok(());
        };
        let deadline = Instant::now() + LIFECYCLE_TIMEOUT;
        loop {
            match Arc::try_unwrap(node) {
                Ok(node) => {
                    return if self.core.r1_events.is_some() {
                        node.finish_r1_events()
                    } else {
                        Ok(())
                    };
                }
                Err(shared) if Instant::now() >= deadline => {
                    self.closing.insert(*database, shared);
                    return Err(TcpError::with_kind(
                        io::ErrorKind::TimedOut,
                        format!(
                            "DropDatabase {}: sessions did not release the database",
                            identity::database_hex(database)
                        ),
                    ));
                }
                Err(shared) => {
                    node = shared;
                    // Sessions observe the close within one sweep.
                    thread::sleep(Duration::from_millis(5));
                }
            }
        }
    }

    fn finish_drop(
        &mut self,
        entry: &HostEntry,
        final_generation: u64,
        phase: DropPhase,
    ) -> Result<DropAck, TcpError> {
        let root = catalog::database_root(&self.core.root, &entry.database);
        if phase == DropPhase::TargetCleanup {
            self.teardown_target(entry)?;
            self.core.step(HostStep::DropAfterTargetCleanup)?;
            self.set_state(
                &entry.database,
                HostState::Dropping {
                    final_generation,
                    phase: DropPhase::FilesOnly,
                },
            )?;
            self.core.step(HostStep::DropAfterFilesOnly)?;
        }
        // FilesOnly never opens a target: absence is already proven.
        match fs::symlink_metadata(&root) {
            Ok(metadata) if metadata.file_type().is_dir() => {
                let _lock = DbLock::acquire(&root, Duration::ZERO)?;
                // remove_dir_all does not follow symlinks below the root.
                fs::remove_dir_all(&root)?;
            }
            Ok(_) => {
                return Err(corrupt(format!(
                    "database {} root is not a directory",
                    identity::database_hex(&entry.database)
                )));
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        self.core.step(HostStep::DropAfterUnlink)?;
        File::open(&self.core.root)?.sync_all()?;
        self.core.step(HostStep::DropAfterRootSync)?;
        tombstone::ensure_directory(&self.core.root)?;
        tombstone::install(
            &self.core.root,
            &DroppedDb {
                database: entry.database,
                principal: entry.principal,
                final_generation,
            },
        )?;
        self.core.step(HostStep::DropAfterTombstone)?;
        // Only now may the slot be reused.
        let mut next = self.catalog.clone();
        next.remove(&entry.database);
        self.persist(next)?;
        self.core.step(HostStep::DropAfterCatalogClear)?;
        Ok(DropAck {
            database: entry.database,
            generation: final_generation,
        })
    }

    /// `TargetCleanup`: reconcile the journal and prove target absence. The
    /// marker and journal stay until the proof, so a crash resumes here.
    fn teardown_target(&self, entry: &HostEntry) -> Result<(), TcpError> {
        let root = catalog::database_root(&self.core.root, &entry.database);
        let hex = identity::database_hex(&entry.database);
        if !fs::symlink_metadata(&root).is_ok_and(|metadata| metadata.file_type().is_dir()) {
            // The intent recorded TargetCleanup only for a present root.
            return Err(corrupt(format!(
                "database {hex} root vanished before target cleanup"
            )));
        }
        let _lock = DbLock::acquire(&root, Duration::ZERO)?;
        {
            use crate::distributed::nvmeof::live_target::LiveTargetManager;
            let fixture = self.core.fixture_configfs();
            if let Some((config, identity)) =
                crate::distributed::tcp::inventory::read_live_target_marker(&root)?
            {
                return LiveTargetManager::teardown(config, Some(identity), fixture).map_err(
                    |error| {
                        TcpError::with_kind(
                            error.kind(),
                            format!(
                                "DropDatabase {hex} partition {} target cleanup in {}: {error}",
                                entry.spec.partition.partition,
                                root.display()
                            ),
                        )
                    },
                );
            }
            // No marker: a journal saved before the marker was installed
            // (the first open died in between) still owns objects.
            let state_dir = root.join(catalog::LIVE_TARGET_DIR);
            if fs::symlink_metadata(state_dir.join(LiveTargetManager::journal_name())).is_ok() {
                let config = self
                    .core
                    .database_config(entry)?
                    .live_target
                    .ok_or_else(|| {
                        corrupt(format!(
                            "database {hex} has a target journal but no live target"
                        ))
                    })?;
                return LiveTargetManager::teardown(config, None, fixture).map_err(|error| {
                    TcpError::with_kind(
                        error.kind(),
                        format!(
                            "DropDatabase {hex} target cleanup in {}: {error}",
                            root.display()
                        ),
                    )
                });
            }
        }
        // Neither marker nor journal: no target was ever commissioned, unless
        // a live WAL exists, which would make the absence unexplained.
        for child in fs::read_dir(&root)? {
            let child = child?;
            let is_epoch = child
                .file_name()
                .to_str()
                .is_some_and(|name| name.starts_with("epoch_"));
            if is_epoch
                && child.file_type()?.is_dir()
                && PartitionLog::is_live_path(&child.path()).map_err(TcpError::component)?
            {
                return Err(corrupt(format!(
                    "database {hex}: live WAL target identity marker is missing"
                )));
            }
        }
        Ok(())
    }
}
