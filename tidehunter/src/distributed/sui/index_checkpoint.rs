//! Coordinator evidence through the Sui adapter: the engine's recovery record
//! and the process-wide coordinator file counters. The Exact checkpoint ring,
//! suffix tracker and attribution totals no longer exist: the native index
//! snapshots on its own cadence and reports through its own metrics.

use super::super::remote_db::{FileIo, RecoveryStats, coordinator_io_snapshot};
use super::{Backend, Db};

impl Db {
    /// Engine recovery evidence of this open; `None` for native databases.
    pub fn remote_recovery_stats(&self) -> Option<RecoveryStats> {
        match &self.backend {
            Backend::Remote(remote) => Some(remote.db.recovery_stats()),
            Backend::Native(_) => None,
        }
    }
}

/// Timing-visitor names for the process-wide coordinator file counters.
/// Cumulative; a window is a difference of two visits.
pub(super) fn visit_coordinator_io(set: &mut impl FnMut(&'static str, u64)) {
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
        manifest_stage,
        manifest_adopt,
        manifest_cleanup,
        cleanup_names,
        open,
        durable_cut,
    );
    set("coordinator_io_allocation_errors", io.allocation_errors);
}
