//! Owner side of unlimited fencing generations (roadmap Phase 5 L1).
//!
//! The ledger keeps a monotone `highest_fenced_generation` that is never
//! derived from the retained entries, so trimming old entries can never let a
//! stale fence reactivate a generation. The authenticated coordinator commits
//! its trim intent only after `generation_unused` proves its durable replay
//! and relocation watermarks cover the stream. The owner trusts that decision;
//! it does not treat the recovered end as a grant high-water.
//!
//! Under the live-control and lifecycle gates, the owner validates the durable
//! SEAL against the recovered end, requires retirement and absence below it,
//! checks pins, and fences reader snapshots. For an abandoned generation it
//! first installs a durable zero-end SEAL.
//!
//! The owner then installs `trim_pending`, releases each lookahead marker and
//! syncs the marker directory before unlinking its WAL and syncing the WAL
//! directory, removes the generation directory and syncs the owner root, and
//! clears `trim_pending`. A restart with the intent repeats deletion before
//! publishing readers. Recovery refuses to extend an existing SEAL.
//!
//! Gate scope: the validation and the `trim_pending` install (phase 1) and
//! the final install (phase 3) hold the live-control and lifecycle gates. The
//! deletion (phase 2) holds no gate; each live target step takes the lifecycle
//! gate alone. The durable order of fsync, rename and unlink is the same in
//! all phases and in the open-time resume, which share `delete_generation`.

#[cfg(test)]
use super::super::generation_control::OwnerTrimCrash;
use super::super::generation_control::{
    InventoryGeneration, InventoryPageReply, InventoryPageRequest, OwnerGenerationState,
    TrimReadiness, page_capacity,
};
use super::*;

/// Owner inventory record format. 2 replaces `max_epochs` with the monotone
/// high-water and the pending trim intent (number assigned at merge).
pub(super) const FORMAT: u32 = 2;

/// Live target registry of a trim, when the owner has one.
type TrimTarget<'a> = Option<&'a mut LiveTarget>;

#[derive(Deserialize)]
struct FormatProbe {
    format: u32,
}

/// Read the owner ledger. A record of another format fails with its number
/// before any field of the current format is interpreted.
pub(super) fn read_manifest(path: &Path) -> Result<Manifest, TcpError> {
    let probe: FormatProbe = read_metadata(path).map_err(TcpError::component)?;
    if probe.format != FORMAT {
        return Err(TcpError::new(format!(
            "unsupported distributed owner inventory format {} (expected {FORMAT})",
            probe.format
        )));
    }
    read_metadata(path).map_err(TcpError::component)
}

pub(super) fn new_manifest(config: &NodeConfig) -> Manifest {
    Manifest {
        format: FORMAT,
        partition: config.partition.clone(),
        shape_yaml: config.shape_yaml.clone(),
        highest_fenced_generation: 0,
        trim_pending: None,
        epochs: Vec::new(),
    }
}

/// Structural ledger invariants: nonzero strictly ascending entries, all at or
/// below the high-water; only the newest entry may be `Preparing`/`Active`; a
/// pending trim is at or below the high-water and absent from the entries.
pub(super) fn validate_manifest(
    manifest: &Manifest,
    partition: &PartitionConfig,
    shape_yaml: &str,
) -> Result<(), TcpError> {
    let hw = manifest.highest_fenced_generation;
    if manifest.format != FORMAT
        || &manifest.partition != partition
        || manifest.shape_yaml != shape_yaml
        || manifest.epochs.iter().any(|e| e.epoch == 0 || e.epoch > hw)
        || manifest.epochs.windows(2).any(|w| w[0].epoch >= w[1].epoch)
        || manifest.epochs.iter().enumerate().any(|(index, e)| {
            matches!(e.state, EpochState::Active | EpochState::Preparing)
                && index + 1 != manifest.epochs.len()
        })
        || manifest.trim_pending.is_some_and(|pending| {
            pending == 0 || pending > hw || manifest.epochs.iter().any(|e| e.epoch == pending)
        })
    {
        return Err(TcpError::new("node inventory/configuration differs"));
    }
    Ok(())
}

/// Resume a durably intended trim at open, under the node lock and after the
/// live target registry revoked and drained every persisted namespace.
pub(super) fn resume_trim(
    root: &Path,
    manifest: &mut Manifest,
    target: TrimTarget<'_>,
) -> Result<(), TcpError> {
    let Some(generation) = manifest.trim_pending else {
        return Ok(());
    };
    let mut access = TrimTargetAccess::Owned(target);
    delete_generation(
        root,
        generation,
        manifest.partition.wal_file_bytes,
        TrimCrashCuts::default(),
        &mut access,
    )?;
    manifest.trim_pending = None;
    persist(root, manifest)
}

/// Offsets of `wal_<16 hex>` files present in a generation directory.
fn present_wal_files(path: &Path) -> Result<Vec<u64>, TcpError> {
    let mut files = Vec::new();
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(id) = name
            .to_str()
            .and_then(|name| name.strip_prefix("wal_"))
            .filter(|id| id.len() == 16)
            .and_then(|id| u64::from_str_radix(id, 16).ok())
        else {
            continue;
        };
        if !entry.file_type()?.is_file() {
            return Err(TcpError::new("trim WAL path is not a regular file"));
        }
        files.push(id);
    }
    files.sort_unstable();
    Ok(files)
}

fn wal_name(directory: &Path, id: u64) -> PathBuf {
    directory.join(format!("wal_{id:016x}"))
}

#[derive(Clone, Copy, Default)]
struct TrimCrashCuts {
    before_marker_sync: bool,
    after_marker: bool,
    after_unlink: bool,
    after_file_sync: bool,
}

/// Test pause of one trim phase: `(entered, resume)`.
#[cfg(test)]
pub(super) type TrimPause = (
    std::sync::mpsc::SyncSender<()>,
    std::sync::mpsc::Receiver<()>,
);

/// A pause and the test side of it: the receiver observes entry, the sender
/// resumes.
#[cfg(test)]
fn trim_pause() -> (
    TrimPause,
    (
        std::sync::mpsc::Receiver<()>,
        std::sync::mpsc::SyncSender<()>,
    ),
) {
    let (entered, observed) = std::sync::mpsc::sync_channel(0);
    let (resume, resume_rx) = std::sync::mpsc::sync_channel(0);
    ((entered, resume_rx), (observed, resume))
}

/// Report entry to the test, then wait for its resume.
#[cfg(test)]
fn hold_trim_pause(pause: Option<TrimPause>) {
    if let Some((entered, resume)) = pause {
        let _ = entered.send(());
        let _ = resume.recv();
    }
}

/// Live target access of one deletion step.
enum TrimTargetAccess<'a> {
    /// Offline (`resume_trim` or an owned test inventory): the caller owns the
    /// lifecycle slot, so the target can be used directly.
    Owned(TrimTarget<'a>),
    /// Online: the caller holds no gate. Each step takes the lifecycle gate and
    /// the inventory, and releases the inventory across its I/O.
    Gated {
        inventory: &'a Mutex<NodeInventory>,
        lifecycle: &'a LifecycleGate,
    },
}

impl TrimTargetAccess<'_> {
    /// Run one deletion step with the live target (`None` when the node has no
    /// target). The target borrow never outlives this call.
    fn step<R>(
        &mut self,
        run: impl FnOnce(Option<&mut LiveTarget>) -> Result<R, TcpError>,
    ) -> Result<R, TcpError> {
        match self {
            Self::Owned(target) => run(target.as_deref_mut()),
            Self::Gated {
                inventory,
                lifecycle,
            } => {
                let mut slot = lifecycle.lock();
                let mut guard = inventory.lock();
                let target = live_target(&guard, &mut slot)?;
                guard.released(|| run(target))
            }
        }
    }

    /// Test pause after one file step, with no gate and no inventory held. The
    /// pause is taken out of the inventory first, so its guard does not live
    /// through the wait. The owned access has no inventory and never pauses.
    #[cfg(test)]
    fn hold_after_file_step(&self) {
        if let Self::Gated { inventory, .. } = self {
            let pause = inventory.lock().trim_delete_pause.take();
            hold_trim_pause(pause);
        }
    }
}

/// Delete the intended generation and sync the owner root. The seal remains
/// until all WAL files are gone. An interrupted final directory removal may
/// lose the seal only after marker cleanup and WAL deletion have completed.
/// The offline resume and the online phase use this one function, so their
/// sync, unlink and rename order is identical; only the target access differs.
fn delete_generation(
    root: &Path,
    generation: u64,
    file_bytes: u64,
    cuts: TrimCrashCuts,
    access: &mut TrimTargetAccess<'_>,
) -> Result<(), TcpError> {
    let path = epoch_path(root, generation);
    match fs::symlink_metadata(&path) {
        Ok(metadata) if metadata.file_type().is_dir() => {
            let present = present_wal_files(&path)?;
            let first_ahead = if present.is_empty() {
                None
            } else {
                let end = crate::distributed::storage::trim_sealed_end(&path, generation)
                    .map_err(TcpError::component)?
                    .ok_or_else(|| TcpError::new("trim WAL files have no sealed end"))?;
                Some(end.div_ceil(file_bytes))
            };
            // A crash after one unlink leaves exactly one marker without its
            // WAL: the file above the present set. Retire it before the loop.
            if let Some(id) = present.last().map_or(Some(0), |last| last.checked_add(1)) {
                let file = wal_name(&path, id);
                access.step(|target| match target {
                    Some(target) => probe_retire(target, &file),
                    None => Ok(()),
                })?;
            }
            for &id in present.iter().rev() {
                let file = wal_name(&path, id);
                let lookahead = first_ahead.is_some_and(|first| id >= first);
                access.step(|target| delete_one_file(&path, &file, target, lookahead, cuts))?;
                #[cfg(test)]
                access.hold_after_file_step();
            }
            fs::remove_dir_all(&path)?;
        }
        Ok(_) => {
            return Err(TcpError::new(format!(
                "trimmed fencing generation {generation} path {} is not a directory",
                path.display()
            )));
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    File::open(root)?.sync_all()?;
    Ok(())
}

/// Retire the sealed marker of the one file above the present set.
fn probe_retire(target: &mut LiveTarget, file: &Path) -> Result<(), TcpError> {
    let Some(identity) = target.sealed_file(file).map_err(TcpError::component)? else {
        return Ok(());
    };
    target
        .retire_deleted_file(&identity)
        .map_err(TcpError::component)
}

/// Delete one WAL file of the intended generation. The marker identity is read
/// and retired in the same target hold; it never crosses a gate release.
fn delete_one_file(
    path: &Path,
    file: &Path,
    target: Option<&mut LiveTarget>,
    lookahead: bool,
    cuts: TrimCrashCuts,
) -> Result<(), TcpError> {
    let Some(target) = target else {
        fs::remove_file(file)?;
        File::open(path)?.sync_all()?;
        return Ok(());
    };
    if lookahead {
        target
            .release_trim_lookahead(file, cuts.before_marker_sync)
            .map_err(TcpError::component)?;
        if cuts.after_marker {
            return Err(TcpError::new("injected owner crash after lookahead marker"));
        }
        fs::remove_file(file)?;
        if cuts.after_unlink {
            return Err(TcpError::new(
                "injected owner crash before WAL directory sync",
            ));
        }
        File::open(path)?.sync_all()?;
        if cuts.after_file_sync {
            return Err(TcpError::new("injected owner crash after lookahead unlink"));
        }
        return Ok(());
    }
    let identity = target.sealed_file(file).map_err(TcpError::component)?;
    fs::remove_file(file)?;
    if let Some(identity) = identity {
        target
            .retire_deleted_file(&identity)
            .map_err(TcpError::component)?;
    } else {
        File::open(path)?.sync_all()?;
    }
    Ok(())
}

fn owner_state(state: EpochState) -> OwnerGenerationState {
    match state {
        EpochState::Preparing => OwnerGenerationState::Preparing,
        EpochState::Active => OwnerGenerationState::Active,
        EpochState::Sealed => OwnerGenerationState::Sealed,
        EpochState::Abandoned => OwnerGenerationState::Abandoned,
    }
}

impl NodeInventory {
    /// Raise the incarnation-local revision before a ledger install, under the
    /// same inventory hold that changed the in-memory ledger: a page served
    /// after this point can no longer match an earlier sweep.
    pub(super) fn bump_revision(&mut self) -> Result<(), TcpError> {
        self.revision = self
            .revision
            .checked_add(1)
            .ok_or_else(|| TcpError::new("owner inventory revision exhausted"))?;
        Ok(())
    }

    pub(in crate::distributed::tcp) fn highest_fenced_generation(&self) -> u64 {
        self.manifest.highest_fenced_generation
    }

    #[cfg(test)]
    pub(super) fn inventory_revision(&self) -> u64 {
        self.revision
    }

    pub(in crate::distributed::tcp) fn inventory_page(
        &self,
        request: &InventoryPageRequest,
        frame_limit: usize,
    ) -> Result<InventoryPageReply, TcpError> {
        self.ensure_healthy()?;
        if request.incarnation != self.incarnation || request.revision != self.revision {
            return Err(TcpError::new(format!(
                "owner {} inventory changed during pagination",
                self.config.partition.partition
            )));
        }
        let limit = usize::from(request.limit);
        if limit == 0 || limit > page_capacity(frame_limit) {
            return Err(TcpError::new("inventory page limit exceeds bound"));
        }
        let start = request.after.map_or(0, |after| {
            self.manifest.epochs.partition_point(|e| e.epoch <= after)
        });
        let remaining = &self.manifest.epochs[start..];
        let mut entries = Vec::with_capacity(limit.min(remaining.len()));
        for e in remaining.iter().take(limit) {
            let state = owner_state(e.state);
            let durable_end = match self.logs.get(&e.epoch) {
                Some(log) => log.synced_frontier(),
                // Owner open creates or recovers a log for every Sealed and Active
                // generation. Reporting 0 for one would read as an empty stream
                // (covered, trim-eligible, nothing to relocate), so refuse instead.
                None if matches!(
                    state,
                    OwnerGenerationState::Sealed | OwnerGenerationState::Active
                ) =>
                {
                    return Err(TcpError::new(format!(
                        "owner {} holds no log for generation {}",
                        self.config.partition.partition, e.epoch
                    )));
                }
                None => 0,
            };
            entries.push(InventoryGeneration {
                generation: e.epoch,
                state,
                durable_end,
            });
        }
        let next = (remaining.len() > limit)
            .then(|| entries.last().map(|e| e.generation))
            .flatten();
        Ok(InventoryPageReply {
            incarnation: self.incarnation,
            revision: self.revision,
            entries,
            next,
        })
    }

    /// A retained record pin prevents trimming its backing files.
    fn trim_blocker(&self, generation: u64) -> Option<&'static str> {
        self.live_pins
            .values()
            .any(|(_, leases)| leases.iter().any(|lease| lease.segment.epoch == generation))
            .then_some("pin")
    }

    /// Readiness of `generation` for trim under the current fence
    /// `authority_generation`, with the reason when it is `Blocked`.
    pub(in crate::distributed::tcp) fn trim_readiness<H: InventoryAccess>(
        inventory: &mut H,
        authority_generation: u64,
        generation: u64,
    ) -> Result<(TrimReadiness, &'static str), TcpError> {
        inventory.active(authority_generation)?;
        if generation == 0 || generation >= authority_generation {
            return Ok((TrimReadiness::Blocked, "live-or-future"));
        }
        if let Some(reason) = inventory.trim_blocker(generation) {
            return Ok((TrimReadiness::Blocked, reason));
        }
        let state = inventory
            .manifest
            .epochs
            .iter()
            .find(|entry| entry.epoch == generation)
            .map(|entry| entry.state);
        if inventory.manifest.trim_pending == Some(generation) {
            return Ok((TrimReadiness::Blocked, "cleanup"));
        }
        if matches!(state, Some(EpochState::Preparing | EpochState::Active)) {
            return Ok((TrimReadiness::Blocked, "live-or-future"));
        }
        let frontier = if state == Some(EpochState::Sealed) {
            let Some(log) = inventory.logs.get(&generation) else {
                return Ok((TrimReadiness::Blocked, "cleanup"));
            };
            if log.recovery_found_hole() {
                return Ok((TrimReadiness::Blocked, "durability proof"));
            }
            let end = log.appended_frontier();
            Some((end, log.trim_retired_prefix(end)))
        } else {
            None
        };
        let path = epoch_path(&inventory.config.root, generation);
        let file_bytes = inventory.config.partition.wal_file_bytes;
        // The lifecycle gate excludes changes while the inventory is released.
        let result = inventory.released(|| -> Result<_, TcpError> {
            match fs::symlink_metadata(&path) {
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    return Ok(if state.is_none() {
                        (TrimReadiness::Absent, "")
                    } else if state == Some(EpochState::Abandoned) {
                        (TrimReadiness::EmptySealed, "")
                    } else {
                        (TrimReadiness::Blocked, "cleanup")
                    });
                }
                Err(error) => return Err(error.into()),
                Ok(metadata) if !metadata.file_type().is_dir() => {
                    return Err(TcpError::new("generation path is not a directory"));
                }
                Ok(_) if state.is_none() => return Ok((TrimReadiness::Blocked, "cleanup")),
                Ok(_) => {}
            }
            let ids = present_wal_files(&path)?;
            if let Some((end, retired)) = frontier {
                let Some(seal) = crate::distributed::storage::trim_sealed_end(&path, generation)
                    .map_err(TcpError::component)?
                else {
                    return Ok((TrimReadiness::Blocked, "missing sealed end"));
                };
                if seal != end {
                    return Ok((TrimReadiness::Blocked, "sealed end mismatch"));
                }
                if !retired {
                    return Ok((TrimReadiness::Blocked, "durability proof"));
                }
                if ids.iter().any(|id| *id < end.div_ceil(file_bytes)) {
                    return Ok((TrimReadiness::Blocked, "durability proof"));
                }
            }
            Ok(if frontier.map_or(0, |(end, _)| end) == 0 {
                (TrimReadiness::EmptySealed, "")
            } else {
                (TrimReadiness::Retired, "")
            })
        })?;
        inventory.active(authority_generation)?;
        let current = inventory
            .manifest
            .epochs
            .iter()
            .find(|entry| entry.epoch == generation)
            .map(|entry| entry.state);
        if current != state
            || inventory.manifest.trim_pending == Some(generation)
            || inventory.trim_blocker(generation).is_some()
            || frontier.is_some_and(|(end, _)| {
                inventory
                    .logs
                    .get(&generation)
                    .is_none_or(|log| log.appended_frontier() != end)
            })
        {
            return Ok((TrimReadiness::Blocked, "changed during readiness check"));
        }
        Ok(result)
    }

    /// Phase 1 of a fenced trim: validate the authority and the readiness,
    /// detach the readers entry and the log, finish the tombstones, revoke the
    /// bindings, install the empty seal of an abandoned generation, and install
    /// the ledger with `trim_pending` durable. The caller holds the live-control
    /// gate and the lifecycle gate (`slot` is its content) or owns the inventory
    /// exclusively. Returns whether the directory deletion must follow.
    fn trim_generation_prepare<H: InventoryAccess>(
        inventory: &mut H,
        slot: &mut LifecycleSlot,
        authority_generation: u64,
        generation: u64,
    ) -> Result<bool, TcpError> {
        let partition = inventory.config.partition.partition;
        let refuse = |reason: &str| {
            TcpError::new(format!(
                "owner {partition} cannot trim fencing generation {generation}: {reason}"
            ))
        };
        inventory.active(authority_generation)?;
        if generation == 0 || generation >= authority_generation {
            return Err(refuse("live-or-future"));
        }
        let mut target = live_target(inventory, slot)?;
        let registered = inventory
            .manifest
            .epochs
            .binary_search_by_key(&generation, |e| e.epoch);
        match registered {
            Err(_) if inventory.manifest.trim_pending == Some(generation) => {}
            // Already trimmed: success only with the same absent-state checks.
            Err(_) => {
                return match Self::trim_readiness(inventory, authority_generation, generation)? {
                    (TrimReadiness::Absent, _) => Ok(false),
                    (_, reason) => Err(refuse(reason)),
                };
            }
            Ok(index) => {
                if inventory.manifest.trim_pending.is_some() {
                    return Err(refuse("cleanup"));
                }
                match Self::trim_readiness(inventory, authority_generation, generation)? {
                    (TrimReadiness::EmptySealed | TrimReadiness::Retired, _) => {}
                    (_, reason) => return Err(refuse(reason)),
                }
                // Lock order is inventory then readers. Removing the reader
                // entry prevents new snapshots; the sole-Arc check catches
                // snapshots and leases obtained before that removal.
                inventory.readers.write().logs.remove(&generation);
                #[cfg(test)]
                hold_trim_pause(inventory.trim_detach_pause.take());
                let detached = inventory.logs.remove(&generation);
                if detached
                    .as_ref()
                    .is_some_and(|log| Arc::strong_count(log) != 1)
                {
                    if let Some(log) = detached {
                        inventory
                            .readers
                            .write()
                            .logs
                            .insert(generation, log.clone());
                        inventory.logs.insert(generation, log);
                    }
                    return Err(TcpError::with_kind(
                        io::ErrorKind::WouldBlock,
                        format!(
                            "owner {partition} cannot trim fencing generation {generation}: reader/actor/log handle pending"
                        ),
                    ));
                }
                let preparation = (|| -> Result<(Manifest, Vec<u8>), TcpError> {
                    if inventory.manifest.epochs[index].state == EpochState::Abandoned {
                        let root = inventory.config.root.clone();
                        let path = epoch_path(&root, generation);
                        inventory.released(|| {
                            match fs::symlink_metadata(&path) {
                                Ok(metadata) if metadata.file_type().is_dir() => {}
                                Ok(_) => {
                                    return Err(TcpError::new(
                                        "generation path is not a directory",
                                    ));
                                }
                                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                                    fs::create_dir(&path)?;
                                }
                                Err(error) => return Err(error.into()),
                            }
                            File::open(&root)?.sync_all()?;
                            crate::distributed::storage::install_empty_trim_seal(&path, generation)
                                .map_err(TcpError::component)
                        })?;
                        #[cfg(test)]
                        if inventory.take_trim_crash(OwnerTrimCrash::AfterEmptySeal) {
                            return Err(TcpError::new("injected owner crash after empty seal"));
                        }
                    }
                    if let Some(target) = target.as_deref_mut()
                        && let Some(log) = detached.as_ref()
                    {
                        let pending = log.pending_file_retirements();
                        inventory.released(|| {
                            super::retirement::finish_tombstones(target, log, &pending, |_| {})
                        })?;
                    }
                    if let Some(target) = target.as_deref_mut() {
                        let keys: Vec<_> = inventory
                            .live_bindings
                            .keys()
                            .copied()
                            .filter(|key| key.0 == generation)
                            .collect();
                        let claimed: Vec<_> = keys
                            .iter()
                            .filter_map(|key| inventory.live_bindings.remove_entry(key))
                            .collect();
                        let result = inventory.released(|| {
                            revoke_bindings(target, claimed.iter().map(|(_, binding)| binding))
                        });
                        if let Err(error) = result {
                            inventory.live_bindings.extend(claimed);
                            inventory.healthy = false;
                            return Err(error);
                        }
                    }
                    let mut manifest = inventory.manifest.clone();
                    manifest.epochs.remove(index);
                    manifest.trim_pending = Some(generation);
                    let bytes = encode_metadata(&manifest).map_err(TcpError::component)?;
                    inventory.bump_revision()?;
                    Ok((manifest, bytes))
                })();
                let (manifest, bytes) = match preparation {
                    Ok(prepared) => prepared,
                    Err(error) => {
                        if let Some(log) = detached {
                            inventory
                                .readers
                                .write()
                                .logs
                                .insert(generation, log.clone());
                            inventory.logs.insert(generation, log);
                        }
                        return Err(error);
                    }
                };
                drop(detached);
                inventory.manifest = manifest;
                let root = inventory.config.root.clone();
                if let Err(error) = inventory
                    .released(|| install_metadata(&root, MANIFEST, &bytes))
                    .map_err(TcpError::component)
                {
                    // The rename may already be durable. Never restore access.
                    inventory.healthy = false;
                    inventory.readers.write().invalidate();
                    return Err(error);
                }
            }
        }
        #[cfg(test)]
        if inventory.take_trim_crash(OwnerTrimCrash::AfterLedger) {
            inventory.healthy = false;
            return Err(TcpError::new("injected owner crash after trim ledger"));
        }
        Ok(true)
    }

    /// Phase 2 of a fenced trim: delete the generation whose intent is durable.
    /// The caller holds no gate. Each target step takes the lifecycle gate with
    /// the inventory, and releases the inventory across its I/O, so live work
    /// interleaves between files. The sync order is `delete_generation`'s.
    fn trim_generation_delete(
        inventory: &Mutex<NodeInventory>,
        lifecycle: &LifecycleGate,
        generation: u64,
    ) -> Result<(), TcpError> {
        let (root, file_bytes, cuts) = {
            let mut guard = inventory.lock();
            (
                guard.config.root.clone(),
                guard.config.partition.wal_file_bytes,
                Self::take_trim_cuts(&mut guard),
            )
        };
        let mut access = TrimTargetAccess::Gated {
            inventory,
            lifecycle,
        };
        let result = delete_generation(&root, generation, file_bytes, cuts, &mut access);
        let mut guard = inventory.lock();
        if result.is_err() {
            guard.healthy = false;
            return result;
        }
        #[cfg(test)]
        if guard.take_trim_crash(OwnerTrimCrash::AfterUnlink) {
            guard.healthy = false;
            return Err(TcpError::new("injected owner crash after trim unlink"));
        }
        Ok(())
    }

    /// Phase 3 of a fenced trim: clear the durable intent. The caller holds both
    /// gates, so the ledger install stays the single lifecycle writer.
    fn trim_generation_finish<H: InventoryAccess>(inventory: &mut H) -> Result<(), TcpError> {
        inventory.manifest.trim_pending = None;
        Self::persist_manifest(inventory)
    }

    /// One fenced trim from the control owner: phase 1 and phase 3 under both
    /// gates, phase 2 with no gate held.
    ///
    /// The queue slot of the request stays exclusive for all three phases
    /// (`control_active`), so queued live-lane lease renewals still wait
    /// behind the deletion. Do not call this from an online trigger until the
    /// deletion runs off the control queue or lease renewal is exempt from
    /// `control_active` (see the generation-trim section of
    /// `docs/distributed/ARCHITECTURE.md`). Today the trim request comes only
    /// from the coordinator's open-time `maintain`, before any serving session.
    pub(in crate::distributed::tcp) fn trim_generation_online(
        inventory: &Mutex<NodeInventory>,
        lifecycle: &LifecycleGate,
        live_control_gate: &Mutex<()>,
        authority_generation: u64,
        generation: u64,
    ) -> Result<(), TcpError> {
        let delete = {
            let _gate = live_control_gate.lock();
            let mut slot = lifecycle.lock();
            let mut guard = inventory.lock();
            Self::trim_generation_prepare(&mut guard, &mut slot, authority_generation, generation)?
        };
        if delete {
            Self::trim_generation_delete(inventory, lifecycle, generation)?;
            let _gate = live_control_gate.lock();
            let _slot = lifecycle.lock();
            Self::trim_generation_finish(&mut inventory.lock())?;
        }
        Ok(())
    }

    /// Offline trim with an exclusively owned inventory: the caller holds the
    /// lifecycle slot and no other thread can observe it. Test support for the
    /// crash-point matrix; production trims use `trim_generation_online`.
    #[cfg(test)]
    pub(in crate::distributed::tcp) fn trim_generation<H: InventoryAccess>(
        inventory: &mut H,
        slot: &mut LifecycleSlot,
        authority_generation: u64,
        generation: u64,
    ) -> Result<(), TcpError> {
        if !Self::trim_generation_prepare(inventory, slot, authority_generation, generation)? {
            return Ok(());
        }
        let root = inventory.config.root.clone();
        let file_bytes = inventory.config.partition.wal_file_bytes;
        let cuts = Self::take_trim_cuts(inventory);
        let target = live_target(&*inventory, slot)?;
        let mut access = TrimTargetAccess::Owned(target);
        let result = inventory
            .released(|| delete_generation(&root, generation, file_bytes, cuts, &mut access));
        if result.is_err() {
            inventory.healthy = false;
            return result;
        }
        if inventory.take_trim_crash(OwnerTrimCrash::AfterUnlink) {
            inventory.healthy = false;
            return Err(TcpError::new("injected owner crash after trim unlink"));
        }
        Self::trim_generation_finish(inventory)
    }

    #[cfg(test)]
    fn take_trim_cuts<H: InventoryAccess>(inventory: &mut H) -> TrimCrashCuts {
        TrimCrashCuts {
            before_marker_sync: inventory
                .take_trim_crash(OwnerTrimCrash::AfterLookaheadMarkerUnlink),
            after_marker: inventory.take_trim_crash(OwnerTrimCrash::AfterLookaheadMarker),
            after_unlink: inventory.take_trim_crash(OwnerTrimCrash::AfterLookaheadFileUnlink),
            after_file_sync: inventory.take_trim_crash(OwnerTrimCrash::AfterLookaheadUnlink),
        }
    }

    #[cfg(not(test))]
    fn take_trim_cuts(_inventory: &mut impl InventoryAccess) -> TrimCrashCuts {
        TrimCrashCuts::default()
    }

    /// The trim intent in the ledger file on disk (not the in-memory ledger).
    #[cfg(test)]
    pub(in crate::distributed::tcp) fn durable_trim_pending(
        &self,
    ) -> Result<Option<u64>, TcpError> {
        read_manifest(&self.config.root.join(MANIFEST)).map(|manifest| manifest.trim_pending)
    }

    /// Pause after the first deleted WAL file, with no gate and no inventory
    /// held: the next file is still present.
    #[cfg(test)]
    pub(in crate::distributed::tcp) fn pause_trim_after_file_step(
        &mut self,
    ) -> (
        std::sync::mpsc::Receiver<()>,
        std::sync::mpsc::SyncSender<()>,
    ) {
        let (pause, handles) = trim_pause();
        self.trim_delete_pause = Some(pause);
        handles
    }

    /// Pause after reader detachment, while the inventory lock is held.
    #[cfg(test)]
    pub(in crate::distributed::tcp) fn pause_trim_after_detach(
        &mut self,
    ) -> (
        std::sync::mpsc::Receiver<()>,
        std::sync::mpsc::SyncSender<()>,
    ) {
        let (pause, handles) = trim_pause();
        self.trim_detach_pause = Some(pause);
        handles
    }

    #[cfg(test)]
    pub(in crate::distributed::tcp) fn fail_next_trim(&mut self, point: OwnerTrimCrash) {
        self.trim_crash = Some(point);
    }

    #[cfg(test)]
    fn take_trim_crash(&mut self, point: OwnerTrimCrash) -> bool {
        if self.trim_crash == Some(point) {
            self.trim_crash = None;
            return true;
        }
        false
    }
}

#[cfg(test)]
#[path = "generations_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "trim_scheduling_tests.rs"]
mod trim_scheduling_tests;
