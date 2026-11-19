//! Exact checkpoint attribution through the Sui adapter (roadmap Phase 6 A0).
//! Native databases have no Exact worker; every accessor reports absence.

use super::super::remote_db::{
    AttributionTotals, CheckpointDiagnosticsSnapshot, FileIo, IndexCheckpointSnapshot,
    RecoveryStats, coordinator_io_snapshot,
};
use super::{Backend, Db, Result, error};

impl Db {
    /// Arm the bounded checkpoint ring and suffix tracker of a remote database,
    /// independently of the request profile and timeline.
    pub fn arm_index_checkpoint_diagnostics(&self) -> Result<()> {
        match &self.backend {
            Backend::Remote(remote) => {
                remote.db.arm_index_checkpoint_diagnostics();
                Ok(())
            }
            Backend::Native(_) => Err(error(
                "index checkpoint diagnostics require a remote backend",
            )),
        }
    }

    /// Live suffix, admission, hard-wait and coordinator I/O state; no I/O.
    pub fn index_checkpoint_snapshot(&self) -> Option<IndexCheckpointSnapshot> {
        match &self.backend {
            Backend::Remote(remote) => Some(remote.db.index_checkpoint_snapshot()),
            Backend::Native(_) => None,
        }
    }

    /// Bounded ring rows at or after `cursor` (at most 128 per call).
    pub fn index_checkpoint_diagnostics_since(
        &self,
        cursor: u64,
    ) -> Option<CheckpointDiagnosticsSnapshot> {
        match &self.backend {
            Backend::Remote(remote) => Some(remote.db.checkpoint_diagnostics_since(cursor)),
            Backend::Native(_) => None,
        }
    }

    /// Wait for natural checkpoint work; `force` adopts one more root marked
    /// `harness_drain`. Never a durability proxy for typed-store `flush`.
    pub fn drain_index_checkpoints(&self, force: bool) -> Result<()> {
        match &self.backend {
            Backend::Remote(remote) => remote.db.drain_index_checkpoints(force).map_err(error),
            Backend::Native(_) => Err(error(
                "index checkpoint diagnostics require a remote backend",
            )),
        }
    }

    /// Engine recovery evidence of this open; `None` for native databases.
    pub fn remote_recovery_stats(&self) -> Option<RecoveryStats> {
        match &self.backend {
            Backend::Remote(remote) => Some(remote.db.recovery_stats()),
            Backend::Native(_) => None,
        }
    }
}

/// Timing-visitor names for the Exact attribution totals and the process-wide
/// coordinator file counters. Cumulative; a window is a difference of two visits.
pub(super) fn visit_attribution(
    totals: &AttributionTotals,
    set: &mut impl FnMut(&'static str, u64),
) {
    set("write_index_checkpoint_cleaned", totals.cleaned);
    set("write_index_checkpoint_failed", totals.failed);
    set("write_index_checkpoint_io_bytes", totals.io.bytes);
    set("write_index_checkpoint_io_file_syncs", totals.io.file_syncs);
    set("write_index_checkpoint_io_dir_syncs", totals.io.dir_syncs);
    set(
        "write_index_checkpoint_io_allocated_bytes",
        totals.io.allocated_bytes,
    );
    set(
        "write_index_checkpoint_rewritten_cells",
        totals.rewritten_cells,
    );
    set("write_index_checkpoint_reused_cells", totals.reused_cells);
    set("write_index_checkpoint_empty_cells", totals.empty_cells);
    set("write_index_checkpoint_worker_cpu_ns", totals.worker_cpu_ns);
    set(
        "write_index_checkpoint_min_capture_to_cleanup_ns",
        totals.min_capture_to_cleanup_ns,
    );
    set(
        "write_index_checkpoint_max_capture_to_cleanup_ns",
        totals.max_capture_to_cleanup_ns,
    );
    let io = coordinator_io_snapshot();
    let mut category = |names: [&'static str; 4], value: FileIo| {
        set(names[0], value.bytes);
        set(names[1], value.file_syncs);
        set(names[2], value.dir_syncs);
        set(names[3], value.allocated_bytes);
    };
    macro_rules! categories {
        ($($field:ident),+ $(,)?) => {
            $(category(
                [
                    concat!("coordinator_io_", stringify!($field), "_bytes"),
                    concat!("coordinator_io_", stringify!($field), "_file_syncs"),
                    concat!("coordinator_io_", stringify!($field), "_dir_syncs"),
                    concat!("coordinator_io_", stringify!($field), "_allocated_bytes"),
                ],
                io.$field,
            );)+
        };
    }
    categories!(
        cells,
        root,
        wrapper,
        manifest_stage,
        manifest_adopt,
        manifest_cleanup,
        cleanup_names,
        open,
        durable_cut,
    );
    set("coordinator_io_allocation_errors", io.allocation_errors);
}
