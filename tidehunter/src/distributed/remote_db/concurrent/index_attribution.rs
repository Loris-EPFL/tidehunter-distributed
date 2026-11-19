//! Exact checkpoint attribution for the serving handle (roadmap Phase 6 A0).
//! Local reads only: no RPC, no profile arming.

use super::super::{
    CheckpointDiagnosticsSnapshot, IndexCheckpointSnapshot, LocalDbError, RecoveryStats,
};
use super::ConcurrentRemoteDatabase;

impl ConcurrentRemoteDatabase {
    /// Arm the bounded checkpoint ring and suffix tracker for this database.
    pub fn arm_index_checkpoint_diagnostics(&self) {
        self.database.arm_index_checkpoint_diagnostics();
    }

    pub fn index_checkpoint_snapshot(&self) -> IndexCheckpointSnapshot {
        self.database.index_checkpoint_snapshot()
    }

    /// Ring rows with `cursor >= cursor`, at most the ring capacity (128).
    pub fn checkpoint_diagnostics_since(&self, cursor: u64) -> CheckpointDiagnosticsSnapshot {
        self.database.checkpoint_diagnostics_since(cursor)
    }

    pub fn drain_index_checkpoints(&self, force: bool) -> Result<(), LocalDbError> {
        self.database.drain_index_checkpoints(force)
    }

    /// Engine recovery evidence of this open.
    pub fn recovery_stats(&self) -> RecoveryStats {
        self.database.recovery_stats()
    }
}
