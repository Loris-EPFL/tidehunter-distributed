//! `StorageNode` accessors that the control owner hosts: durability selection
//! and the test hooks that read the owner's logs and the durable-progress cap.
//! They live here so `control.rs` stays within its file-length budget; the
//! fields they touch belong to `tcp.rs` and are visible to this module.

use super::super::StorageNode;
use super::*;
use crate::distributed::durability::DurabilityConfig;

impl StorageNode {
    /// Select the write durability tier. Call after `open` and before the first
    /// client request; it fails once an append has fixed the policy.
    pub fn set_durability(&self, config: DurabilityConfig) -> Result<(), TcpError> {
        self.control.set_durability(config)
    }

    /// The durability configuration the append workers apply now.
    pub fn durability(&self) -> DurabilityConfig {
        self.control.durability()
    }

    /// Test accessor: the owner's log of `epoch`, for frontier assertions.
    #[cfg(test)]
    pub(crate) fn log_for_test(
        &self,
        epoch: u64,
    ) -> Arc<super::super::super::storage::PartitionLog> {
        self.inventory.lock().active(epoch).unwrap()
    }

    /// Inspect a sealed generation without requiring an active authority.
    /// Drop the returned handle before testing generation trimming.
    #[cfg(test)]
    pub(crate) fn sealed_log_for_test(
        &self,
        epoch: u64,
    ) -> Arc<super::super::super::storage::PartitionLog> {
        self.inventory.lock().sealed_log_for_test(epoch)
    }

    /// Test hook: the log of `epoch` with its stream frontier and syncer
    /// attached, as the first lease (formerly the first append) does.
    #[cfg(test)]
    pub(crate) fn stream_log_for_test(
        &self,
        epoch: u64,
    ) -> Arc<super::super::super::storage::PartitionLog> {
        let log = self.log_for_test(epoch);
        self.control.shared.frontier_for(&log).unwrap();
        log
    }

    /// Test accessor: the owner's log of stream `source` while `authority`
    /// serves (the active log, or an older sealed one), to read records of
    /// either kind directly from the owner WAL.
    #[cfg(test)]
    pub(crate) fn source_log_for_test(
        &self,
        authority: u64,
        source: u64,
    ) -> Arc<super::super::super::storage::PartitionLog> {
        let inventory = self.inventory.lock();
        if source == authority {
            inventory.active(source).unwrap()
        } else {
            inventory.retirement_log(authority, source).unwrap()
        }
    }

    /// Test-only: (grants, lapsed) of this database's coordinator lease.
    #[cfg(test)]
    pub(crate) fn coordinator_lease_state_for_test(&self) -> (u64, bool) {
        let lease = &self.control_context.coordinator_lease;
        (lease.grants(), lease.is_lapsed())
    }

    /// Test hook: cap of a durable-progress wait for requests submitted from now on.
    #[cfg(test)]
    pub(crate) fn set_durable_progress_cap_for_test(&self, cap: Duration) {
        // Relaxed: read by the next poll; tests order it before the submission.
        self.control.shared.progress_cap_ms.store(
            u64::try_from(cap.as_millis()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
    }
}
