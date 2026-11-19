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
        let files: Vec<u64> = proofs.iter().map(|proof| proof.file_start).collect();
        let mut target = {
            if !inventory.live_pins.is_empty() {
                return Err(TcpError::new(
                    "live NVMe-oF pins must drain before retirement",
                ));
            }
            if inventory.victims_pinned(epoch, &files) {
                return Err(TcpError::new(
                    "live connection pins must drain before retirement",
                ));
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
        inventory.released(|| {
            let outcome = log
                .retire_sealed_stream_files(proofs)
                .map_err(TcpError::component)?;
            if outcome == RetirementOutcome::Busy {
                return Ok(outcome);
            }
            let done = log.pending_file_retirements();
            if let Some(target) = target.as_mut() {
                finish_tombstones(target, &log, &done)?;
            }
            log.clear_retirement_cleanup(&done)
                .map_err(TcpError::component)?;
            Ok(outcome)
        })
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
                guard.file_maintenance.busy_retirements += 1;
                return Ok(RetirementOutcome::Busy);
            }
            if let Some(target) = live_target(&guard, &mut slot)?
                && let Err(error) =
                    Self::revoke_live_files(&mut guard, target, authority_epoch, &files)
            {
                guard.healthy = false;
                return Err(error);
            }
        }
        let outcome = log
            .authorize_file_retirement(proofs, witness)
            .map_err(TcpError::component)?;
        if outcome == RetirementOutcome::Busy {
            inventory.lock().file_maintenance.busy_retirements += 1;
            return Ok(outcome);
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
        let done = log.finish_file_retirement().map_err(TcpError::component)?;
        if done.is_empty() {
            return Ok(());
        }
        let configured = inventory.lock().live_target_configured;
        if configured {
            let mut slot = lifecycle.lock();
            let target = slot
                .as_mut()
                .ok_or_else(|| TcpError::new("live target disappeared"))?;
            finish_tombstones(target, log, &done)?;
        }
        log.clear_retirement_cleanup(&done)
            .map_err(TcpError::component)?;
        let mut guard = inventory.lock();
        let count = u64::try_from(done.len()).unwrap_or(u64::MAX);
        guard.value_wal_unlinks = guard.value_wal_unlinks.saturating_add(count);
        guard.file_maintenance.retired_files =
            guard.file_maintenance.retired_files.saturating_add(count);
        Ok(())
    }

    /// Any record lease or connection pin on a victim defers the request.
    /// Pins on other files of the stream never block it.
    fn victims_pinned(&self, epoch: u64, files: &[u64]) -> bool {
        let bytes = self.config.partition.wal_file_bytes;
        let victim = |source: u64, file: u64| source == epoch && files.contains(&(file * bytes));
        self.live_pins.values().any(|(_, leases)| {
            leases
                .iter()
                .any(|lease| victim(lease.segment.epoch, lease.segment.file_index))
        }) || self
            .live_export_pins
            .values()
            .any(|(_, (source, file))| victim(*source, *file))
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

/// Remove each retired inode's tombstone after its unlink and directory sync.
pub(super) fn finish_tombstones(
    target: &mut LiveTarget,
    log: &PartitionLog,
    done: &[crate::distributed::storage::PendingRetirement],
) -> Result<(), TcpError> {
    for entry in done {
        target
            .finish_retired_file(&log.wal_file_path(entry.start), entry.device, entry.inode)
            .map_err(TcpError::component)?;
    }
    Ok(())
}
