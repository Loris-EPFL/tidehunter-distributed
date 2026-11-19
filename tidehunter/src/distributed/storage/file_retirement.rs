//! Owner whole-file retirement of exact-dead WAL files.
//!
//! Order for every retired file: durable root (offset retired, exact inode in
//! `cleanup_pending`, seal removed) -> detach from the in-process lookup ->
//! unlink of that inode -> WAL directory sync -> target tombstone cleanup (by
//! the node inventory) -> durable root without the pending entry. A crash at
//! any step either leaves the file in place (no durable decision) or leaves a
//! pending entry that recovery and the next retirement repeat. A different
//! inode at a retired offset, or a retired dirent without its pending entry,
//! is an error, never a permit to unlink.
//!
//! Lock order: `persistence` -> `state`, as for every root writer. `state` is
//! dropped across root, unlink and directory syncs on the active stream; the
//! older sealed-stream path keeps its original holds (no writer exists).
use super::*;

/// Local inode identity of one WAL file; bound to the logical `WalFileKey`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WalInode {
    pub device: u64,
    pub inode: u64,
}

/// One file of a retirement decision. Files in a request are sorted by start.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RetireProof {
    pub file_start: u64,
    /// Required for an active-stream file. An older sealed stream may omit it;
    /// the owner then binds the inode it observes at authorization.
    pub inode: Option<WalInode>,
    /// `FileSeal.root_generation` of an active-stream file; `None` only for
    /// an older sealed stream.
    pub seal_generation: Option<u64>,
}

/// Coordinator witness for active-stream victims, taken from one adopted
/// Exact root and its persisted Version B record. The owner rechecks every
/// bound it can compare with its own state.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RetirementWitness {
    pub checkpoint_generation: u64,
    pub metadata_digest: [u8; 32],
    /// Adopted `StreamPosition.replay_from` of the victim stream.
    pub replay_from: u64,
    /// Adopted Exact durable end of the victim stream.
    pub durable_end: u64,
    /// First byte relocation callbacks may still scan in this stream, over
    /// every selectable adopted root and registered in-flight pass.
    pub callback_from: u64,
    /// Every batch sequence below this is covered by the adopted Exact root.
    pub covered_next: u64,
    /// Persisted Version B `CutRecord.live.next` (Native tier); `None` for
    /// the Synchronous tier, which has no B cut.
    pub persisted_cut_next: Option<u64>,
}

/// `Busy` means this request made no durable decision; retry after a later
/// root, roll, unpin or cleanup event, never in a tight loop.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum RetirementOutcome {
    Completed,
    Busy,
}

/// What the retirement unlink step found for one pending entry. Both are
/// covered by the same durable `cleanup_pending` record.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnlinkOutcome {
    /// This call removed the exact pending inode.
    Unlinked,
    /// The name was already gone (a repeated step after recovery or retry).
    AlreadyAbsent,
}

/// Result of one authorization: entries newly recorded in a durable root and
/// that root's generation (empty/`None` when it changed nothing).
#[derive(Debug)]
pub(crate) struct Authorized {
    pub(crate) outcome: RetirementOutcome,
    pub(crate) recorded: Vec<PendingRetirement>,
    pub(crate) root_generation: Option<u64>,
}

/// Entries one cleanup clear removed, and the root generation that did it.
#[derive(Debug)]
pub(crate) struct Cleared {
    pub(crate) root_generation: u64,
    pub(crate) removed: Vec<PendingRetirement>,
}

/// Test-only crash cuts between the persisted steps. Root install cuts reuse
/// `fail_wal_root` phases 1-3 (before temp write, after temp sync, after
/// rename before directory sync).
#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(crate) enum CrashCut {
    /// Seal root durable, not yet published in memory.
    SealAfterRoot = 1,
    /// Retirement root durable, not yet published in memory.
    RetireAfterRoot = 2,
    /// Published, before the in-process detach.
    BeforeDetach = 3,
    /// Detached, before any unlink.
    BeforeUnlink = 4,
    /// First victim unlinked, the rest not, no directory sync.
    AfterFirstUnlink = 5,
    /// Directory synced, pending entries not yet cleared.
    AfterDirectorySync = 6,
}

impl PartitionLog {
    #[cfg(test)]
    pub(crate) fn arm_crash_cut(&self, cut: CrashCut) {
        self.r1_crash.store(cut as u8, Ordering::Release);
    }

    /// Park the next serving-stream retirement after its detach and before
    /// its unlinks. Returns (entered, resume).
    #[cfg(test)]
    pub(crate) fn pause_before_unlink(
        &self,
    ) -> (
        std::sync::mpsc::Receiver<()>,
        std::sync::mpsc::SyncSender<()>,
    ) {
        let (entered, started) = std::sync::mpsc::sync_channel(1);
        let (resume, released) = std::sync::mpsc::sync_channel(1);
        *self.before_unlink.lock() = Some((entered, released));
        (started, resume)
    }

    #[cfg(test)]
    pub(super) fn crash_at(&self, cut: CrashCut) -> Result<(), StorageError> {
        if self
            .r1_crash
            .compare_exchange(cut as u8, 0, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            return Err(StorageError::Invalid("injected seal or retirement crash"));
        }
        Ok(())
    }

    /// Durable seal and pending sets, for tests and the file inventory page.
    pub(crate) fn file_retirement_state(&self) -> (Vec<FileSeal>, Vec<PendingRetirement>) {
        let root = self.state.lock().wal_root.clone();
        root.map_or_else(Default::default, |root| {
            (root.sealed_files.clone(), root.cleanup_pending.clone())
        })
    }

    /// Snapshot of the exact WAL inodes still awaiting target cleanup.
    pub(crate) fn pending_file_retirements(&self) -> Vec<PendingRetirement> {
        let root = self.state.lock().wal_root.clone();
        root.map_or_else(Vec::new, |root| root.cleanup_pending.clone())
    }

    /// Pending entries whose unlink this process's recovery repeated, with
    /// what it found. Returned once; later calls return nothing.
    pub(crate) fn take_recovered_cleanup(&self) -> Vec<(PendingRetirement, UnlinkOutcome)> {
        std::mem::take(&mut *self.recovered_cleanup.lock())
    }

    /// Encoded size of the installed root, for the constant-size tests.
    #[cfg(test)]
    pub(crate) fn wal_root_encoded_len(&self) -> usize {
        let root = self.state.lock().wal_root.clone();
        root.and_then(|root| root.encode().ok())
            .map_or(0, |bytes| bytes.len())
    }

    /// Retire without a target manager: decision, unlink, directory sync and
    /// cleanup clear in one call. The node inventory instead calls the steps
    /// and finishes target tombstones between the directory sync and the clear.
    pub fn retire_proved_files(
        &self,
        proofs: &[RetireProof],
        witness: Option<&RetirementWitness>,
    ) -> Result<RetirementOutcome, StorageError> {
        let (outcome, done) = if self.state.lock().sealed {
            if witness.is_some() {
                return Err(StorageError::Invalid(
                    "older sealed stream retirement takes no Exact witness",
                ));
            }
            let outcome = self.retire_sealed_stream_files(proofs)?;
            // Completed: every pending entry was unlinked and its directory synced.
            let done = if outcome == RetirementOutcome::Completed {
                self.pending_file_retirements()
            } else {
                Vec::new()
            };
            (outcome, done)
        } else {
            let witness = witness.ok_or(StorageError::Invalid(
                "active stream retirement requires an Exact witness",
            ))?;
            let outcome = self.authorize_file_retirement(proofs, witness)?;
            (outcome, self.finish_file_retirement()?)
        };
        self.clear_retirement_cleanup(&done)?;
        Ok(outcome)
    }

    /// Validate an active-stream retirement against the current root, the
    /// observed inodes and the witness without any durable change. The node
    /// calls this before it revokes any namespace, so a wrong or stale proof
    /// never disables a healthy binding. Seals are only added and the root
    /// windows only grow, so a request valid here stays valid at
    /// `authorize_file_retirement` unless another retirement or the pending
    /// bound intervenes (that call rechecks everything).
    pub(crate) fn precheck_file_retirement(
        &self,
        proofs: &[RetireProof],
        witness: &RetirementWitness,
    ) -> Result<RetirementOutcome, StorageError> {
        let size = self.config().wal_file_bytes;
        check_proof_order(proofs, size)?;
        let observed = self.observe_proofs(proofs)?;
        let current = self.active_retirement_root()?;
        let entries = retirement_entries(&current, proofs, &observed, witness)?;
        Ok(pending_outcome(&current, &entries))
    }

    /// Durably authorize sealed files of this active stream. Validates each
    /// proof against the exact durable `FileSeal`, the observed inode and the
    /// witness, then installs a root that retires the offsets, moves their
    /// identities to `cleanup_pending` and drops their seals. Repeating an
    /// already retired file with the same identity is a no-op.
    pub(crate) fn authorize_file_retirement(
        &self,
        proofs: &[RetireProof],
        witness: &RetirementWitness,
    ) -> Result<RetirementOutcome, StorageError> {
        Ok(self
            .authorize_file_retirement_recorded(proofs, witness)?
            .outcome)
    }

    /// `authorize_file_retirement`, also reporting the entries this call
    /// recorded and the generation of the root that made them durable (both
    /// empty/`None` when every proof repeated an already retired file).
    pub(crate) fn authorize_file_retirement_recorded(
        &self,
        proofs: &[RetireProof],
        witness: &RetirementWitness,
    ) -> Result<Authorized, StorageError> {
        let size = self.config().wal_file_bytes;
        check_proof_order(proofs, size)?;
        // Path observations are I/O: take them before any lock.
        let observed = self.observe_proofs(proofs)?;
        let _persistence = self.persistence.lock();
        let current = self.active_retirement_root()?;
        let entries = retirement_entries(&current, proofs, &observed, witness)?;
        let outcome = pending_outcome(&current, &entries);
        if entries.is_empty() || outcome == RetirementOutcome::Busy {
            return Ok(Authorized {
                outcome,
                recorded: Vec::new(),
                root_generation: None,
            });
        }
        let root = retirement_root(&current, &entries, size)?;
        let root_generation = Some(root.generation);
        self.commit_root(root, CommitStep::Retire)?;
        Ok(Authorized {
            outcome: RetirementOutcome::Completed,
            recorded: entries,
            root_generation,
        })
    }

    fn observe_proofs(
        &self,
        proofs: &[RetireProof],
    ) -> Result<Vec<Option<WalInode>>, StorageError> {
        proofs
            .iter()
            .map(|proof| self.observe_wal_inode(proof.file_start))
            .collect()
    }

    fn active_retirement_root(&self) -> Result<Arc<WalRoot>, StorageError> {
        let state = self.state.lock();
        Self::check_active_retirement_state(&state)?;
        state
            .wal_root
            .clone()
            .ok_or(StorageError::Invalid("request requires WAL replay history"))
    }

    /// Unlink every pending file of this stream and sync the WAL directory.
    /// Returns the entries whose unlink and directory sync are durable; the
    /// caller finishes target tombstones and then clears them. An empty
    /// result with pending entries left means a victim is still mapped
    /// writable on this live mapper; a later call repeats the work.
    pub(crate) fn finish_file_retirement(&self) -> Result<Vec<PendingRetirement>, StorageError> {
        Ok(self
            .finish_file_retirement_outcomes()?
            .into_iter()
            .map(|(entry, _)| entry)
            .collect())
    }

    /// `finish_file_retirement`, also reporting what the unlink of each
    /// entry found (for the ordered R1 evidence stream).
    pub(crate) fn finish_file_retirement_outcomes(
        &self,
    ) -> Result<Vec<(PendingRetirement, UnlinkOutcome)>, StorageError> {
        let (pending, writer) = {
            let state = self.state.lock();
            let Some(root) = state.wal_root.as_ref() else {
                return Ok(Vec::new());
            };
            let writer = state.writer.as_ref().map(WalWriter::external_file_preparer);
            (root.cleanup_pending.clone(), writer)
        };
        if pending.is_empty() {
            return Ok(Vec::new());
        }
        #[cfg(test)]
        self.crash_at(CrashCut::BeforeDetach)?;
        if let Some(detacher) = writer {
            return self.finish_active_retirement(pending, &detacher);
        }
        // No writer or mapper: the registry has no other publisher.
        let size = self.config().wal_file_bytes;
        let mut present = Vec::with_capacity(pending.len());
        let mut outcomes = Vec::with_capacity(pending.len());
        for entry in pending {
            let outcome = if self.check_unlink_target(&entry)? {
                present.push(crate::wal::position::WalFileId::from_u64(
                    entry.start / size,
                ));
                UnlinkOutcome::Unlinked
            } else {
                UnlinkOutcome::AlreadyAbsent
            };
            outcomes.push((entry, outcome));
        }
        #[cfg(test)]
        self.crash_at(CrashCut::BeforeUnlink)?;
        self.wal.retire_files(&present)?;
        if present.is_empty() {
            File::open(&self.path)?.sync_all()?;
        }
        #[cfg(test)]
        self.crash_at(CrashCut::AfterDirectorySync)?;
        Ok(outcomes)
    }

    /// Live mapper: detach through the mapper (its sole publisher), then
    /// unlink and sync here. The mapper never waits for this I/O.
    fn finish_active_retirement(
        &self,
        pending: Vec<PendingRetirement>,
        detacher: &crate::wal::ExternalFilePreparer,
    ) -> Result<Vec<(PendingRetirement, UnlinkOutcome)>, StorageError> {
        let size = self.config().wal_file_bytes;
        let ids: Vec<_> = pending
            .iter()
            .map(|entry| crate::wal::position::WalFileId::from_u64(entry.start / size))
            .collect();
        // The handles stay open until after the unlink; readers holding their
        // own clones keep the inode alive, as Unix unlink allows.
        let _detached = match detacher.detach_retired_files(ids) {
            Ok(detached) => detached,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(Vec::new()),
            Err(error) => return Err(StorageError::Io(error)),
        };
        #[cfg(test)]
        self.crash_at(CrashCut::BeforeUnlink)?;
        #[cfg(test)]
        {
            // No lock is held here: foreground writes must keep going. The
            // wait is bounded so a failing test cannot hang its scope join.
            let pause = self.before_unlink.lock().take();
            if let Some((entered, resume)) = pause {
                let _ = entered.send(());
                let _ = resume.recv_timeout(std::time::Duration::from_secs(120));
            }
        }
        let mut outcomes = Vec::with_capacity(pending.len());
        for (index, entry) in pending.into_iter().enumerate() {
            let mut outcome = UnlinkOutcome::AlreadyAbsent;
            if self.check_unlink_target(&entry)? {
                match fs::remove_file(self.wal_file_path(entry.start)) {
                    Ok(()) => outcome = UnlinkOutcome::Unlinked,
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error.into()),
                }
            }
            outcomes.push((entry, outcome));
            #[cfg(test)]
            if index == 0 {
                self.crash_at(CrashCut::AfterFirstUnlink)?;
            }
            #[cfg(not(test))]
            let _ = index;
        }
        File::open(&self.path)?.sync_all()?;
        #[cfg(test)]
        self.crash_at(CrashCut::AfterDirectorySync)?;
        Ok(outcomes)
    }

    /// Durably remove finished entries from `cleanup_pending`. Call only after
    /// their unlink, directory sync and target tombstone cleanup are durable.
    /// Returns the entries this call removed and the generation of the root
    /// that removed them, or `None` when none was still pending.
    pub(crate) fn clear_retirement_cleanup(
        &self,
        done: &[PendingRetirement],
    ) -> Result<Option<Cleared>, StorageError> {
        if done.is_empty() {
            return Ok(None);
        }
        let _persistence = self.persistence.lock();
        let current = self
            .state
            .lock()
            .wal_root
            .clone()
            .ok_or(StorageError::Invalid("request requires WAL replay history"))?;
        let mut root = WalRoot::clone(&current);
        root.cleanup_pending.retain(|entry| !done.contains(entry));
        if root.cleanup_pending.len() == current.cleanup_pending.len() {
            return Ok(None);
        }
        root.generation = root
            .generation
            .checked_add(1)
            .ok_or(StorageError::Capacity)?;
        let cleared = Cleared {
            root_generation: root.generation,
            removed: current
                .cleanup_pending
                .iter()
                .filter(|entry| done.contains(entry))
                .copied()
                .collect(),
        };
        self.commit_root(root, CommitStep::Clear)?;
        Ok(Some(cleared))
    }

    /// Older sealed stream: the original path, now with inode-bound pending
    /// entries and interval retirement. No writer exists; `state` and read
    /// admission stay held across the root, unlink and directory sync, as
    /// before. Leaves `cleanup_pending` for the caller's target cleanup.
    pub(crate) fn retire_sealed_stream_files(
        &self,
        proofs: &[RetireProof],
    ) -> Result<RetirementOutcome, StorageError> {
        let size = self.config().wal_file_bytes;
        check_proof_order(proofs, size)?;
        if proofs.iter().any(|proof| proof.seal_generation.is_some()) {
            return Err(StorageError::Invalid(
                "older sealed stream files carry no file seal",
            ));
        }
        let _persistence = self.persistence.lock();
        let mut state = self.state.lock();
        if !state.sealed || state.writer.is_some() || state.poison.is_some() {
            return Err(StorageError::Invalid(
                "retirement requires healthy sealed storage",
            ));
        }
        let current = state
            .wal_root
            .clone()
            .ok_or(StorageError::Invalid("request requires WAL replay history"))?;
        if proofs.is_empty() {
            return Ok(RetirementOutcome::Completed);
        }
        let inventory_end = highest_wal_end(&self.path, self.config())?.max(current.retired.end());
        let mut entries = Vec::with_capacity(proofs.len());
        for proof in proofs {
            if proof.file_start >= inventory_end {
                return Err(StorageError::Invalid(
                    "retirement outside registered WAL inventory",
                ));
            }
            let observed = self.observe_wal_inode(proof.file_start)?;
            if current.retired.contains(proof.file_start) {
                check_repeated_retirement(&current, proof, observed)?;
                continue;
            }
            let observed = observed.ok_or(StorageError::Invalid("unexplained missing WAL file"))?;
            if proof.inode.is_some_and(|inode| inode != observed) {
                return Err(StorageError::Invalid(
                    "retirement proof differs from the WAL inode",
                ));
            }
            entries.push(PendingRetirement {
                start: proof.file_start,
                device: observed.device,
                inode: observed.inode,
                seal_generation: None,
            });
        }
        if current.cleanup_pending.len() + entries.len() > retired_files::MAX_PENDING_RETIREMENTS {
            return Ok(RetirementOutcome::Busy);
        }
        let mut admission = self.read_admission.write();
        if self.live_read_pins.load(Ordering::Acquire) != 0 {
            return Err(StorageError::Invalid(
                "live namespace or read pin prevents retirement",
            ));
        }
        // An interrupted retirement keeps read admission closed until reopen.
        *admission = false;
        let pending = if entries.is_empty() {
            current.cleanup_pending.clone()
        } else {
            let root = retirement_root(&current, &entries, size)?;
            self.install_wal_root(&root)?;
            let pending = root.cleanup_pending.clone();
            state.wal_root = Some(Arc::new(root));
            pending
        };
        #[cfg(test)]
        if self.fail_wal_retire.swap(false, Ordering::AcqRel) {
            return Err(StorageError::Invalid("injected crash before WAL unlink"));
        }
        let mut present = Vec::with_capacity(pending.len());
        for entry in &pending {
            if self.check_unlink_target(entry)? {
                present.push(crate::wal::position::WalFileId::from_u64(
                    entry.start / size,
                ));
            }
        }
        self.wal.retire_files(&present)?;
        if present.is_empty() && !pending.is_empty() {
            File::open(&self.path)?.sync_all()?;
        }
        *admission = true;
        Ok(RetirementOutcome::Completed)
    }

    fn check_active_retirement_state(state: &State) -> Result<(), StorageError> {
        let live = state.live && !state.sealing;
        if state.sealed || state.writer.is_none() || !live {
            return Err(StorageError::Invalid(
                "active retirement requires the serving live stream",
            ));
        }
        if state.poison.is_some() {
            return Err(StorageError::Poisoned);
        }
        Ok(())
    }

    /// Install `root` with `persistence` held and `state` released during the
    /// syncs, then publish it. A failed install after its rename may have
    /// made the root durable: fail-stop the stream and require recovery.
    pub(super) fn commit_root(&self, root: WalRoot, step: CommitStep) -> Result<(), StorageError> {
        if let Err(error) = self.install_wal_root(&root) {
            self.state.lock().poison = Some(WriterPoison::Fatal);
            return Err(error);
        }
        #[cfg(test)]
        {
            let cut = match step {
                CommitStep::Seal => Some(CrashCut::SealAfterRoot),
                CommitStep::Retire => Some(CrashCut::RetireAfterRoot),
                CommitStep::Clear => None,
            };
            if let Some(cut) = cut
                && let Err(error) = self.crash_at(cut)
            {
                self.state.lock().poison = Some(WriterPoison::Fatal);
                return Err(error);
            }
        }
        let _ = step;
        self.state.lock().wal_root = Some(Arc::new(root));
        Ok(())
    }

    /// Canonical path of the WAL file at `start`, exactly as live segments
    /// and target markers name it.
    pub(crate) fn wal_file_path(&self, start: u64) -> PathBuf {
        self.config().layout().wal_file_name(
            &self.path,
            crate::wal::position::WalFileId::from_u64(start / self.config().wal_file_bytes),
        )
    }

    /// The inode the WAL path names now, or `None` when the name is absent.
    pub(super) fn observe_wal_inode(&self, start: u64) -> Result<Option<WalInode>, StorageError> {
        match fs::symlink_metadata(self.wal_file_path(start)) {
            Ok(metadata) if metadata.file_type().is_file() => Ok(Some(WalInode {
                device: metadata.dev(),
                inode: metadata.ino(),
            })),
            Ok(_) => Err(StorageError::Invalid("WAL path is not a regular file")),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    /// Whether the exact pending inode still needs an unlink. Another inode
    /// at the path is never deleted.
    fn check_unlink_target(&self, entry: &PendingRetirement) -> Result<bool, StorageError> {
        match self.observe_wal_inode(entry.start)? {
            None => Ok(false),
            Some(found) if found.device == entry.device && found.inode == entry.inode => Ok(true),
            Some(_) => Err(StorageError::Invalid(
                "retired WAL path names another inode",
            )),
        }
    }
}

/// Which root writer installed a root; selects the test crash cut.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CommitStep {
    /// Only the lease-logical seal (NVMe-oF live streams) installs a seal.
    Seal,
    Retire,
    Clear,
}

fn check_proof_order(proofs: &[RetireProof], size: u64) -> Result<(), StorageError> {
    if proofs.len() > retired_files::MAX_PENDING_RETIREMENTS
        || proofs
            .windows(2)
            .any(|pair| pair[0].file_start >= pair[1].file_start)
        || proofs.iter().any(|proof| {
            !proof.file_start.is_multiple_of(size) || proof.file_start.checked_add(size).is_none()
        })
    {
        return Err(StorageError::Invalid(
            "retirement proofs are unsorted, misaligned or exceed the bound",
        ));
    }
    Ok(())
}

/// An already retired file may repeat only with its own identity.
fn check_repeated_retirement(
    root: &WalRoot,
    proof: &RetireProof,
    observed: Option<WalInode>,
) -> Result<(), StorageError> {
    let pending = root
        .cleanup_pending
        .binary_search_by_key(&proof.file_start, |entry| entry.start)
        .ok()
        .map(|index| root.cleanup_pending[index]);
    let recorded = pending.map(|entry| WalInode {
        device: entry.device,
        inode: entry.inode,
    });
    let conflicting = |inode: Option<WalInode>| {
        inode.is_some_and(|inode| recorded.is_none_or(|recorded| recorded != inode))
    };
    if conflicting(observed) || (recorded.is_some() && conflicting(proof.inode)) {
        return Err(StorageError::Invalid(
            "retired WAL offset names a different inode",
        ));
    }
    Ok(())
}

/// Pending entries newly authorized by `proofs` (sorted, not yet retired).
/// Every proof must match its exact durable seal, the observed inode and the
/// witness; an already retired file may repeat only with its own identity.
fn retirement_entries(
    current: &WalRoot,
    proofs: &[RetireProof],
    observed: &[Option<WalInode>],
    witness: &RetirementWitness,
) -> Result<Vec<PendingRetirement>, StorageError> {
    let mut entries = Vec::with_capacity(proofs.len());
    for (proof, observed) in proofs.iter().zip(observed.iter().copied()) {
        if current.retired.contains(proof.file_start) {
            check_repeated_retirement(current, proof, observed)?;
            continue;
        }
        let seal = current
            .sealed_files
            .binary_search_by_key(&proof.file_start, |seal| seal.start)
            .map(|index| current.sealed_files[index])
            .map_err(|_| StorageError::Invalid("retirement proof has no durable file seal"))?;
        let identity = WalInode {
            device: seal.device,
            inode: seal.inode,
        };
        if proof.seal_generation != Some(seal.root_generation) || proof.inode != Some(identity) {
            return Err(StorageError::Invalid(
                "retirement proof differs from the durable file seal",
            ));
        }
        if observed != Some(identity) {
            return Err(StorageError::Invalid(
                "WAL path no longer names the sealed inode",
            ));
        }
        check_witness(current, &seal, witness)?;
        entries.push(PendingRetirement {
            start: seal.start,
            device: seal.device,
            inode: seal.inode,
            seal_generation: Some(seal.root_generation),
        });
    }
    Ok(entries)
}

/// `Busy` when the new entries would exceed the bounded pending set.
fn pending_outcome(current: &WalRoot, entries: &[PendingRetirement]) -> RetirementOutcome {
    if current.cleanup_pending.len() + entries.len() > retired_files::MAX_PENDING_RETIREMENTS {
        RetirementOutcome::Busy
    } else {
        RetirementOutcome::Completed
    }
}

/// Owner-side recheck of the coordinator witness for one sealed file.
fn check_witness(
    root: &WalRoot,
    seal: &FileSeal,
    witness: &RetirementWitness,
) -> Result<(), StorageError> {
    let end = seal.end;
    let windows = end <= root.scan_from
        && end <= root.synced_end
        && end <= witness.replay_from
        && end <= witness.durable_end
        && end <= witness.callback_from;
    let sequences = seal.last_batch_sequence.is_none_or(|last| {
        last < witness.covered_next && witness.persisted_cut_next.is_none_or(|next| last < next)
    });
    if !windows || !sequences {
        return Err(StorageError::Invalid(
            "retirement witness does not cover the sealed file",
        ));
    }
    Ok(())
}

/// Root that retires `entries` (sorted, not yet retired): offsets enter the
/// retired intervals, identities enter `cleanup_pending`, seals leave.
fn retirement_root(
    current: &WalRoot,
    entries: &[PendingRetirement],
    size: u64,
) -> Result<WalRoot, StorageError> {
    let starts: Vec<u64> = entries.iter().map(|entry| entry.start).collect();
    let mut root = WalRoot::clone(current);
    root.retired.insert_authorized_batch(&starts, size)?;
    root.sealed_files
        .retain(|seal| starts.binary_search(&seal.start).is_err());
    root.cleanup_pending.extend_from_slice(entries);
    root.cleanup_pending
        .sort_unstable_by_key(|entry| entry.start);
    root.generation = root
        .generation
        .checked_add(1)
        .ok_or(StorageError::Capacity)?;
    Ok(root)
}

/// Every retired dirent present at recovery must be the exact inode of a
/// durable pending entry. `present` holds sorted `(start, device, inode)`.
pub(super) fn check_retired_dirents(
    pending: &[PendingRetirement],
    present: &[(u64, u64, u64)],
) -> Result<(), StorageError> {
    for &(start, device, inode) in present {
        let matches = pending
            .binary_search_by_key(&start, |entry| entry.start)
            .is_ok_and(|index| pending[index].device == device && pending[index].inode == inode);
        if !matches {
            return Err(StorageError::Invalid(
                "retired WAL file without its pending cleanup identity",
            ));
        }
    }
    Ok(())
}
