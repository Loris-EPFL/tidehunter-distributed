//! Grant fast path: an already published permanent writable binding.
//!
//! A grant whose file was prepared and published in the background needs no
//! target work, so it takes only the inventory mutex: never the lifecycle
//! gate that file maintenance (seal-time nothing, retirement-time revoke and
//! tombstone removal) and preparation hold. This is what keeps foreground
//! writes and prepared rollovers independent of R1 maintenance.
use super::*;

impl NodeInventory {
    /// The identity-matched published writable permanent binding of
    /// `segment`, if one exists. `None` sends the grant to the gated
    /// fallback, which reports any identity mismatch. Errors only for a lost
    /// authority: the caller's log is no longer the active one.
    pub(in crate::distributed::tcp) fn ready_permanent_binding(
        &mut self,
        epoch: u64,
        log: &Arc<PartitionLog>,
        segment: &super::super::super::nvmeof::live_storage::LiveWalSegment,
    ) -> Result<Option<super::super::super::nvmeof::live_storage::LiveTargetBinding>, TcpError>
    {
        if !self.live_target_configured {
            return Ok(None);
        }
        let Some((existing, binding, writable)) =
            self.live_bindings.get(&(epoch, segment.file_index))
        else {
            return Ok(None);
        };
        if existing != segment || !*writable || !binding.permanent {
            return Ok(None);
        }
        let binding = binding.clone();
        if !Arc::ptr_eq(&self.active(epoch)?, log) {
            return Err(TcpError::new("live lease authority changed"));
        }
        self.file_maintenance.ready_binding_grants =
            self.file_maintenance.ready_binding_grants.saturating_add(1);
        Ok(Some(binding))
    }

    /// Account one grant that waited for the lifecycle gate. `published`
    /// tells whether its file already had a binding when the grant began.
    pub(in crate::distributed::tcp) fn record_grant_gate_wait(&mut self, published: bool, ns: u64) {
        let stats = &mut self.file_maintenance;
        if published {
            stats.prepared_gate_waits = stats.prepared_gate_waits.saturating_add(1);
            stats.prepared_gate_wait_ns = stats.prepared_gate_wait_ns.saturating_add(ns);
        } else {
            stats.unprepared_gate_waits = stats.unprepared_gate_waits.saturating_add(1);
            stats.unprepared_gate_wait_ns = stats.unprepared_gate_wait_ns.saturating_add(ns);
        }
    }

    /// Whether `(epoch, file)` has any installed binding.
    pub(in crate::distributed::tcp) fn has_live_binding(&self, epoch: u64, file: u64) -> bool {
        self.live_bindings.contains_key(&(epoch, file))
    }
}
