//! Owner side of `RetireWalFiles` for both stream kinds.
//!
//! Older sealed stream: the original lifecycle-gated path, now finishing the
//! target tombstone from the durable pending entry before clearing it.
//!
//! Serving stream (`epoch == authority`): file maintenance, not a lifecycle
//! barrier. The lifecycle gate and the inventory are held together only to
//! check per-victim pins, check that the next grant's export is already
//! published (a prepared successor goes first) and revoke the victims' exact
//! namespaces (`enable=0` drains target I/O and leaves a durable tombstone).
//! The retirement root, unlink and directory sync run with neither held. The
//! gate is taken again only for tombstone removal. Lock order: lifecycle gate
//! -> inventory, as everywhere on the owner.
use super::*;
use crate::distributed::storage::{RetireProof, RetirementOutcome, RetirementWitness};

impl NodeInventory {
    /// Older sealed stream. The caller holds the lifecycle gate (`slot` is
    /// its content) or owns the inventory exclusively.
    pub(in crate::distributed::tcp) fn retire_wal_files<H: InventoryAccess>(
        inventory: &mut H,
        slot: &mut LifecycleSlot,
        authority_epoch: u64,
        epoch: u64,
        proofs: &[RetireProof],
    ) -> Result<RetirementOutcome, TcpError> {
        if proofs.len() > 256 {
            return Err(TcpError::new("retirement file count exceeds bound"));
        }
        if proofs.iter().any(|proof| proof.seal_generation.is_some()) {
            return Err(TcpError::new(
                "older sealed stream files carry no file seal",
            ));
        }
        let log = inventory.retirement_log(authority_epoch, epoch)?;
        let Some(reservation) = inventory
            .released(|| log.reserve_sealed_retirement(proofs))
            .map_err(TcpError::component)?
        else {
            inventory.file_maintenance.busy_retirements += 1;
            return Ok(RetirementOutcome::Busy);
        };
        if !Arc::ptr_eq(&inventory.retirement_log(authority_epoch, epoch)?, &log) {
            return Err(TcpError::new("retirement authority changed"));
        }
        let files = reservation.files().to_vec();
        let mut target = {
            // A pin on a victim file defers the request: a cached read-lane
            // connection or an in-flight read is transient, and the next pass
            // asks again (the same `Busy` the serving path answers). Pins on
            // other files never block it (`victims_pinned`). The coordinator
            // disconnects its own idle pool controllers of the victims before
            // it asks (`Client::evict_retirement_files`).
            if inventory.victims_pinned(epoch, &files) {
                #[cfg(test)]
                inventory.record_victim_pins(&log, epoch, &files);
                inventory.file_maintenance.busy_retirements += 1;
                return Ok(RetirementOutcome::Busy);
            }
            let target = live_target(inventory, slot)?;
            if let Some(target) = target {
                if let Err(error) = Self::revoke_live_files(inventory, target, epoch, &files) {
                    inventory.healthy = false;
                    return Err(error);
                }
                Some(target)
            } else {
                None
            }
        };
        // Counted on submission, before any root install or unlink, so a
        // partially failed retirement still shows that unlinks were attempted.
        inventory.value_wal_unlinks = inventory
            .value_wal_unlinks
            .saturating_add(u64::try_from(files.len()).unwrap_or(u64::MAX));
        let emit = inventory.r1_events.is_some();
        let result = inventory.released(|| -> Result<_, TcpError> {
            let outcome = log
                .finish_sealed_retirement(reservation)
                .map_err(TcpError::component)?;
            if outcome == RetirementOutcome::Busy {
                return Ok((outcome, Vec::new(), Vec::new(), None));
            }
            let done = log.pending_file_retirements();
            let mut tombstones = Vec::new();
            if let Some(target) = target.as_mut() {
                if emit {
                    tombstones.reserve_exact(done.len());
                }
                finish_tombstones(target, &log, &done, |removed| {
                    if emit {
                        tombstones.push(removed);
                    }
                })?;
            }
            let cleared = log
                .clear_retirement_cleanup(&done)
                .map_err(TcpError::component)?;
            Ok((outcome, done, tombstones, cleared))
        });
        let (outcome, done, tombstones, cleared) = match result {
            Ok(result) => result,
            Err(error) => {
                inventory.healthy = false;
                return Err(error);
            }
        };
        // Older sealed stream: only the cleanup completions are published
        // (its decision and unlink steps are not part of the R1 contract).
        if let Some(events) = inventory.r1_events.as_mut() {
            emit_tombstones(events, epoch, &done, &tombstones, Some("older_sealed"));
            if let Some(cleared) = &cleared {
                emit_cleared(events, epoch, cleared, Some("older_sealed"));
            }
        }
        Ok(outcome)
    }

    /// Serving stream: retire durably sealed files proved dead by `witness`.
    /// No foreground grant, adoption or position proof waits for this call:
    /// it holds the lifecycle gate only for the scoped revoke, never across
    /// the root, unlink or directory syncs.
    pub(in crate::distributed::tcp) fn retire_active_files(
        inventory: &Mutex<NodeInventory>,
        lifecycle: &Mutex<LifecycleSlot>,
        authority_epoch: u64,
        proofs: &[RetireProof],
        witness: &RetirementWitness,
    ) -> Result<RetirementOutcome, TcpError> {
        if proofs.is_empty() || proofs.len() > 256 {
            return Err(TcpError::new("retirement file count exceeds bound"));
        }
        let log = inventory.lock().active(authority_epoch)?;
        // Validate seal, inode and witness before any namespace changes: a
        // wrong or stale proof must never disable a healthy binding. The
        // authorization below repeats every check before the root commit.
        if log
            .precheck_file_retirement(proofs, witness)
            .map_err(TcpError::component)?
            == RetirementOutcome::Busy
        {
            inventory.lock().file_maintenance.busy_retirements += 1;
            return Ok(RetirementOutcome::Busy);
        }
        // Durable seals as of this request, read before any lock: the victims'
        // seals are reported before their first retirement step (the
        // preparer reports seals only after a whole kick).
        let seals = log.file_retirement_state().0;
        #[cfg(test)]
        let parked: Option<RevokeParkOwner>;
        {
            let mut slot = lifecycle.lock();
            let mut guard = inventory.lock();
            if !Arc::ptr_eq(&guard.active(authority_epoch)?, &log) {
                return Err(TcpError::new("retirement authority changed"));
            }
            let files: Vec<u64> = proofs.iter().map(|proof| proof.file_start).collect();
            if guard.victims_pinned(authority_epoch, &files)
                || !guard.successor_published(&log, authority_epoch)?
            {
                #[cfg(test)]
                {
                    guard.record_victim_pins(&log, authority_epoch, &files);
                    if !guard.victims_pinned(authority_epoch, &files) {
                        for &file in &files {
                            log.record_busy(
                                file,
                                crate::distributed::storage::BusyCause::Other(
                                    "successor not published",
                                ),
                            );
                        }
                    }
                }
                guard.file_maintenance.busy_retirements += 1;
                return Ok(RetirementOutcome::Busy);
            }
            if let Some(events) = guard.r1_events.as_mut() {
                events.seals(authority_epoch, &seals);
            }
            if let Some(target) = live_target(&guard, &mut slot)? {
                let revoked =
                    match Self::revoke_live_files(&mut guard, target, authority_epoch, &files) {
                        Ok(revoked) => revoked,
                        Err(error) => {
                            guard.healthy = false;
                            return Err(error);
                        }
                    };
                // Only files whose binding this call revoked: zero victim
                // pins were checked above, and `enable=0` drained their
                // target I/O and left durable tombstones. A victim with no
                // binding (never exported, evicted, or revoked by an
                // earlier attempt) had no target step here.
                if let Some(events) = guard.r1_events.as_mut() {
                    events.victims(
                        "target_io_drained",
                        authority_epoch,
                        &revoked,
                        &serde_json::json!({
                            "record_pins": 0,
                            "drain": "namespace_enable_0",
                        }),
                    );
                    events.victims(
                        "target_removed",
                        authority_epoch,
                        &revoked,
                        &serde_json::json!({"tombstone_durable": true}),
                    );
                }
            }
            #[cfg(test)]
            {
                parked = guard.after_revoke.take();
            }
        }
        #[cfg(test)]
        if let Some((entered, resumed)) = parked {
            // Test hook. This runs on an owner control thread, which must not
            // panic: a hook left installed by an early return or a failed
            // assertion has a dropped test end, and that means "do not park".
            if entered.send(()).is_ok() {
                // Err only when the test dropped its resume sender: proceed.
                let _ = resumed.recv();
            }
        }
        let authorized = log
            .authorize_file_retirement_recorded(proofs, witness)
            .map_err(TcpError::component)?;
        let outcome = authorized.outcome;
        if outcome == RetirementOutcome::Busy {
            inventory.lock().file_maintenance.busy_retirements += 1;
            return Ok(outcome);
        }
        // Only entries this call recorded in a new durable root; a repeated
        // proof of an already retired file made no root change.
        if let Some(events) = inventory.lock().r1_events.as_mut() {
            for entry in &authorized.recorded {
                events.victims(
                    "retirement_root_durable",
                    authority_epoch,
                    &pending_files(std::slice::from_ref(entry)),
                    &serde_json::json!({
                        "seal_generation": entry.seal_generation,
                        "root_generation": authorized.root_generation,
                    }),
                );
            }
        }
        Self::finish_active_retirement(inventory, lifecycle, &log)?;
        Ok(outcome)
    }

    /// Finish every recorded cleanup of `log`: unlink and directory sync
    /// (no lock), tombstone removal (gate only), then the durable clear.
    pub(in crate::distributed::tcp) fn finish_active_retirement(
        inventory: &Mutex<NodeInventory>,
        lifecycle: &Mutex<LifecycleSlot>,
        log: &Arc<PartitionLog>,
    ) -> Result<(), TcpError> {
        let outcomes = log
            .finish_file_retirement_outcomes()
            .map_err(TcpError::component)?;
        if outcomes.is_empty() {
            // Pending entries with no outcome: the pass was deferred (see
            // `FileMaintenanceStats::deferred_retirements`), never lost.
            if !log.pending_file_retirements().is_empty() {
                let mut guard = inventory.lock();
                guard.file_maintenance.deferred_retirements = guard
                    .file_maintenance
                    .deferred_retirements
                    .saturating_add(1);
            }
            return Ok(());
        }
        let generation = log.config().inventory.epoch;
        if let Some(events) = inventory.lock().r1_events.as_mut() {
            events.unlinked(generation, &outcomes, "retirement");
        }
        let done: Vec<_> = outcomes.into_iter().map(|(entry, _)| entry).collect();
        let (configured, emit) = {
            let guard = inventory.lock();
            (guard.live_target_configured, guard.r1_events.is_some())
        };
        if configured {
            let mut slot = lifecycle.lock();
            let target = slot
                .as_mut()
                .ok_or_else(|| TcpError::new("live target disappeared"))?;
            let mut removed = Vec::with_capacity(if emit { done.len() } else { 0 });
            finish_tombstones(target, log, &done, |did_remove| {
                if emit {
                    removed.push(did_remove);
                }
            })?;
            drop(slot);
            if emit && let Some(events) = inventory.lock().r1_events.as_mut() {
                emit_tombstones(events, generation, &done, &removed, None);
            }
        }
        let cleared = log
            .clear_retirement_cleanup(&done)
            .map_err(TcpError::component)?;
        if let (Some(cleared), Some(events)) = (&cleared, inventory.lock().r1_events.as_mut()) {
            emit_cleared(events, generation, cleared, None);
        }
        let mut guard = inventory.lock();
        let count = u64::try_from(done.len()).unwrap_or(u64::MAX);
        guard.value_wal_unlinks = guard.value_wal_unlinks.saturating_add(count);
        guard.file_maintenance.retired_files =
            guard.file_maintenance.retired_files.saturating_add(count);
        Ok(())
    }

    /// Test-only: make the retirement revoke step report one victim file as
    /// not revoked, as if its binding were already gone (the R1 contract then
    /// emits no target step for it). The revoke itself still runs, so the log
    /// never holds a live file without a binding. Returns whether the file had
    /// a live binding.
    #[cfg(test)]
    pub(in crate::distributed::tcp) fn skip_retirement_revoke_report_for_test(
        &mut self,
        epoch: u64,
        file: u64,
    ) -> bool {
        if !self.live_bindings.contains_key(&(epoch, file)) {
            return false;
        }
        self.retirement_revoke_report_skips
            .push((epoch, file * self.config.partition.wal_file_bytes));
        true
    }

    /// See `skip_retirement_revoke_report_for_test`. `start` is a byte offset,
    /// as the retirement pages use.
    #[cfg(test)]
    pub(in crate::distributed::tcp) fn retirement_revoke_report_is_skipped(
        &self,
        epoch: u64,
        start: u64,
    ) -> bool {
        self.retirement_revoke_report_skips
            .contains(&(epoch, start))
    }

    #[cfg(test)]
    fn record_victim_pins(&self, log: &PartitionLog, epoch: u64, files: &[u64]) {
        use crate::distributed::storage::BusyCause;
        let bytes = self.config.partition.wal_file_bytes;
        for (_, leases) in self.live_pins.values() {
            for lease in leases {
                let file = lease.segment.file_index * bytes;
                if lease.segment.epoch == epoch && files.contains(&file) {
                    log.record_busy(file, BusyCause::RecordPin);
                }
            }
        }
    }

    /// Test hook: park the next serving retirement between its revoke and its
    /// retirement root. Nothing marks a victim in that window except the
    /// scheduler's exclusion of exports, so a test can observe it.
    #[cfg(test)]
    pub(in crate::distributed::tcp) fn pause_after_revoke(&mut self) -> RevokeParkTest {
        let (entered, waiting) = std::sync::mpsc::sync_channel(1);
        let (release, resumed) = std::sync::mpsc::sync_channel(1);
        self.after_revoke = Some((entered, resumed));
        (waiting, release)
    }

    /// Any record lease on a victim defers the request. Pins on other files
    /// of the stream never block it.
    fn victims_pinned(&self, epoch: u64, files: &[u64]) -> bool {
        let bytes = self.config.partition.wal_file_bytes;
        let victim = |source: u64, file: u64| source == epoch && files.contains(&(file * bytes));
        self.live_pins.values().any(|(_, leases)| {
            leases
                .iter()
                .any(|lease| victim(lease.segment.epoch, lease.segment.file_index))
        })
    }

    /// Prepared-successor priority: with a live target (permanent per-owner
    /// exports), maintenance takes the lifecycle gate only when the file of
    /// the next grant already has its published binding, so the grant needs
    /// no gate.
    fn successor_published(&self, log: &PartitionLog, epoch: u64) -> Result<bool, TcpError> {
        if !self.live_target_configured {
            return Ok(true);
        }
        let file = log.live_allocator_file().map_err(TcpError::component)?;
        Ok(self.live_bindings.contains_key(&(epoch, file)))
    }
}

/// Revoke-side helper of the retirement paths above. A separate impl block
/// keeps it out of the baselined `inventory.rs`; only this module calls it.
impl NodeInventory {
    /// Revoke the live bindings of `files` in `epoch`. Returns `(start,
    /// device, inode)` of every binding actually revoked; a file without a
    /// binding (never exported, evicted, or revoked by an earlier attempt) is
    /// absent from the result.
    fn revoke_live_files<H: InventoryAccess>(
        inventory: &mut H,
        target: &mut LiveTarget,
        epoch: u64,
        files: &[u64],
    ) -> Result<Vec<(u64, u64, u64)>, TcpError> {
        let bytes = inventory.config.partition.wal_file_bytes;
        let retired = |(binding_epoch, file): &(u64, u64)| {
            *binding_epoch == epoch && files.contains(&(file * bytes))
        };
        let keys: Vec<_> = inventory
            .live_bindings
            .keys()
            .copied()
            .filter(&retired)
            .collect();
        let claimed: Vec<_> = keys
            .iter()
            .filter_map(|key| inventory.live_bindings.remove_entry(key))
            .collect();
        let result = inventory.released(|| revoke_bindings(target, claimed.iter().map(|(_, b)| b)));
        if let Err(error) = result {
            inventory.live_bindings.extend(claimed);
            return Err(error);
        }
        let revoked: Vec<(u64, u64, u64)> = claimed
            .iter()
            .map(|((_, file), (segment, _, _))| (file * bytes, segment.device, segment.inode))
            .collect();
        // Test-only: report chosen victims as not revoked, as if their binding
        // were already gone. Only this result feeds the R1 target steps, and
        // the revoke itself ran, so no live file is left without a binding
        // beyond the pass that unlinks it (the invariant `OpenLiveExports`
        // relies on).
        #[cfg(test)]
        let revoked = {
            let mut revoked = revoked;
            revoked.retain(|(start, _, _)| {
                !inventory.retirement_revoke_report_is_skipped(epoch, *start)
            });
            revoked
        };
        Ok(revoked)
    }
}

/// Remove each retired inode's tombstone after its unlink and directory sync.
/// Report per entry whether this call removed its tombstone.
pub(super) fn finish_tombstones(
    target: &mut LiveTarget,
    log: &PartitionLog,
    done: &[crate::distributed::storage::PendingRetirement],
    mut report: impl FnMut(bool),
) -> Result<(), TcpError> {
    for entry in done {
        let removed = target
            .finish_retired_file(&log.wal_file_path(entry.start), entry.device, entry.inode)
            .map_err(TcpError::component)?;
        report(removed);
    }
    Ok(())
}

/// `(start, device, inode)` of pending retirement entries.
fn pending_files(done: &[crate::distributed::storage::PendingRetirement]) -> Vec<(u64, u64, u64)> {
    done.iter()
        .map(|entry| (entry.start, entry.device, entry.inode))
        .collect()
}

/// One `target_tombstone_synced` per entry: the marker directory sync is
/// durable; `tombstone_removed` says whether this call removed the marker.
fn emit_tombstones(
    events: &mut r1_events::OwnerR1Events,
    generation: u64,
    done: &[crate::distributed::storage::PendingRetirement],
    removed: &[bool],
    stream: Option<&str>,
) {
    for (entry, removed) in done.iter().zip(removed) {
        events.victims(
            "target_tombstone_synced",
            generation,
            &pending_files(std::slice::from_ref(entry)),
            &serde_json::json!({"tombstone_removed": removed, "stream": stream}),
        );
    }
}

/// One `cleanup_root_durable` per entry the clear removed, with its root.
fn emit_cleared(
    events: &mut r1_events::OwnerR1Events,
    generation: u64,
    cleared: &crate::distributed::storage::Cleared,
    stream: Option<&str>,
) {
    events.victims(
        "cleanup_root_durable",
        generation,
        &pending_files(&cleared.removed),
        &serde_json::json!({"root_generation": cleared.root_generation, "stream": stream}),
    );
}
