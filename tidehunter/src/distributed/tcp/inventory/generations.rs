//! Owner side of unlimited fencing generations (roadmap Phase 5 L1).
//!
//! The ledger keeps a monotone `highest_fenced_generation` that is never
//! derived from the retained entries, so trimming old entries can never let a
//! stale fence reactivate a generation. Trim of one generation is three durable
//! steps under the lifecycle gate:
//!
//! 1. install the ledger without the generation and with `trim_pending`;
//! 2. delete the exact generation directory (live WAL files first, each with
//!    its sealed target marker), then sync the owner root;
//! 3. install the ledger with `trim_pending` cleared.
//!
//! A restart between 1 and 3 finds `trim_pending`, which is the only
//! unregistered generation directory open accepts, and resumes step 2 before
//! any log is recovered or reader authority published.

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
    delete_generation(root, generation, target)?;
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
        files.push(id);
    }
    files.sort_unstable();
    Ok(files)
}

fn wal_name(directory: &Path, id: u64) -> PathBuf {
    directory.join(format!("wal_{id:016x}"))
}

/// Delete exactly `epoch_path(root, generation)` and sync the owner root.
/// Repeatable: an absent directory only repeats the root sync.
///
/// Live WAL files are removed in descending file order, each followed by the
/// release of its sealed target marker (`retire_deleted_file` syncs the WAL
/// directory before it removes the marker). A crash between the unlink and the
/// marker release therefore leaves the files below the removed one present, and
/// the resume probes the marker of the next file above the highest present one
/// (file 0 when none is present). Trim deletes only empty streams as a unit or
/// streams whose files were all retired, so the remaining files are contiguous.
fn delete_generation(root: &Path, generation: u64, target: TrimTarget<'_>) -> Result<(), TcpError> {
    let path = epoch_path(root, generation);
    match fs::symlink_metadata(&path) {
        Ok(metadata) if metadata.file_type().is_dir() => {
            if let Some(target) = target {
                let present = present_wal_files(&path)?;
                let probe = present.last().map_or(Some(0), |last| last.checked_add(1));
                if let Some(identity) = probe
                    .map(|id| target.sealed_file(&wal_name(&path, id)))
                    .transpose()
                    .map_err(TcpError::component)?
                    .flatten()
                {
                    target
                        .retire_deleted_file(&identity)
                        .map_err(TcpError::component)?;
                }
                for &id in present.iter().rev() {
                    let file = wal_name(&path, id);
                    let identity = target.sealed_file(&file).map_err(TcpError::component)?;
                    fs::remove_file(&file)?;
                    if let Some(identity) = identity {
                        target
                            .retire_deleted_file(&identity)
                            .map_err(TcpError::component)?;
                    }
                }
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

    #[cfg(test)]
    pub(super) fn highest_fenced_generation(&self) -> u64 {
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
        let entries: Vec<_> = remaining
            .iter()
            .take(limit)
            .map(|e| InventoryGeneration {
                generation: e.epoch,
                state: owner_state(e.state),
            })
            .collect();
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

    /// A retained record or export pin prevents trimming its backing files.
    fn trim_blocker(&self, generation: u64) -> Option<&'static str> {
        (self
            .live_pins
            .values()
            .any(|(_, leases)| leases.iter().any(|lease| lease.segment.epoch == generation))
            || self
                .live_export_pins
                .values()
                .any(|(_, (source, _))| *source == generation))
        .then_some("pin")
    }

    /// Readiness of `generation` for trim under the current fence
    /// `authority_generation`, with the reason when it is `Blocked`.
    pub(in crate::distributed::tcp) fn trim_readiness(
        &self,
        authority_generation: u64,
        generation: u64,
    ) -> Result<(TrimReadiness, &'static str), TcpError> {
        self.active(authority_generation)?;
        if generation == 0 || generation >= authority_generation {
            return Ok((TrimReadiness::Blocked, "live-or-future"));
        }
        let Ok(index) = self
            .manifest
            .epochs
            .binary_search_by_key(&generation, |e| e.epoch)
        else {
            if self.manifest.trim_pending == Some(generation) {
                return Ok((TrimReadiness::Blocked, "cleanup"));
            }
            return match fs::symlink_metadata(epoch_path(&self.config.root, generation)) {
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    Ok((TrimReadiness::Absent, ""))
                }
                Err(error) => Err(error.into()),
                Ok(_) => Ok((TrimReadiness::Blocked, "cleanup")),
            };
        };
        if let Some(reason) = self.trim_blocker(generation) {
            return Ok((TrimReadiness::Blocked, reason));
        }
        match self.manifest.epochs[index].state {
            EpochState::Preparing | EpochState::Active => {
                Ok((TrimReadiness::Blocked, "live-or-future"))
            }
            // Never activated: no group can have been accepted.
            EpochState::Abandoned => Ok((TrimReadiness::EmptySealed, "")),
            EpochState::Sealed => {
                let Some(log) = self.logs.get(&generation) else {
                    return Ok((TrimReadiness::Blocked, "cleanup"));
                };
                // A sealed recovery that found no valid frame and no hole: the
                // stream never accepted a group.
                if log.appended_frontier() == 0 && !log.recovery_found_hole() {
                    return Ok((TrimReadiness::EmptySealed, ""));
                }
                // Retired: no unretired file in the WAL root and no WAL file
                // left on disk (every retirement unlink completed).
                let unretired = log.wal_file_page(None, 1).map_err(TcpError::component)?;
                let present = present_wal_files(&epoch_path(&self.config.root, generation))?;
                if unretired.is_empty() && present.is_empty() {
                    Ok((TrimReadiness::Retired, ""))
                } else {
                    Ok((TrimReadiness::Blocked, "durability proof"))
                }
            }
        }
    }

    /// Fenced trim of `generation`. The caller holds the lifecycle gate
    /// (`slot` is its content) or owns the inventory exclusively. The ledger
    /// install, the directory deletion and the final install run with the
    /// inventory released; readiness is rechecked here, never trusted from a
    /// previous `CheckTrimGeneration`.
    pub(in crate::distributed::tcp) fn trim_generation<H: InventoryAccess>(
        inventory: &mut H,
        slot: &mut LifecycleSlot,
        authority_generation: u64,
        generation: u64,
    ) -> Result<(), TcpError> {
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
                return match inventory.trim_readiness(authority_generation, generation)? {
                    (TrimReadiness::Absent, _) => Ok(()),
                    (_, reason) => Err(refuse(reason)),
                };
            }
            Ok(index) => {
                if inventory.manifest.trim_pending.is_some() {
                    return Err(refuse("cleanup"));
                }
                match inventory.trim_readiness(authority_generation, generation)? {
                    (TrimReadiness::EmptySealed | TrimReadiness::Retired, _) => {}
                    (_, reason) => return Err(refuse(reason)),
                }
                // Detach the log: readers first, then this inventory's handle.
                inventory.readers.write().logs.remove(&generation);
                if let Some(log) = inventory.logs.remove(&generation) {
                    if Arc::strong_count(&log) != 1 {
                        inventory
                            .readers
                            .write()
                            .logs
                            .insert(generation, log.clone());
                        inventory.logs.insert(generation, log);
                        return Err(refuse("reader/actor/log handle pending"));
                    }
                    drop(log);
                }
                // Idle history exports of this generation have no pin (checked
                // above); revoke them before the namespace backing disappears.
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
                    let result = inventory
                        .released(|| revoke_bindings(target, claimed.iter().map(|(_, b)| b)));
                    if let Err(error) = result {
                        inventory.live_bindings.extend(claimed);
                        inventory.healthy = false;
                        return Err(error);
                    }
                    inventory
                        .idle_history_exports
                        .retain(|key| key.0 != generation);
                }
                inventory.manifest.epochs.remove(index);
                inventory.manifest.trim_pending = Some(generation);
                Self::persist_manifest(inventory)?;
            }
        }
        #[cfg(test)]
        if inventory.take_trim_crash(OwnerTrimCrash::AfterLedger) {
            inventory.healthy = false;
            return Err(TcpError::new("injected owner crash after trim ledger"));
        }
        let root = inventory.config.root.clone();
        inventory.released(|| delete_generation(&root, generation, target))?;
        #[cfg(test)]
        if inventory.take_trim_crash(OwnerTrimCrash::AfterUnlink) {
            inventory.healthy = false;
            return Err(TcpError::new("injected owner crash after trim unlink"));
        }
        inventory.manifest.trim_pending = None;
        Self::persist_manifest(inventory)
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
