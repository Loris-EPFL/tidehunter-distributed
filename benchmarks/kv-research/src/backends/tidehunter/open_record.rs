//! Open timing and the replay-tail evidence of one coordinator open.
//!
//! The hybrid coordinator recovers by native replay from the snapshot's
//! consistent prefix, so the recovery evidence is the replay report the engine
//! folds into `RecoveryStats` (tail batches, tail bytes, replay time) plus the
//! cut decision and the connection and fencing phases of the open. The record
//! adds what only the caller can observe: the open window and the first
//! successful get after it.
use serde_json::{Value, json};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tidehunter::distributed::remote_db::{RecoveryStats, diagnostics_clock_ns};
use tidehunter::distributed::sui;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum OpenKind {
    /// The coordinator directory was empty before open.
    Create,
    Reopen,
}

/// External open timing and the engine recovery evidence of one open.
#[derive(Clone)]
pub(super) struct OpenRecord {
    kind: OpenKind,
    open_start_ns: u64,
    open_return_ns: u64,
    /// First successful point get after open; zero until one completes.
    first_get_ns: Arc<AtomicU64>,
    recovery: Option<RecoveryStats>,
}

impl OpenRecord {
    /// Call right after `open`: the return clock and the recovery evidence are
    /// read here.
    pub(super) fn new(kind: OpenKind, open_start_ns: u64, db: &sui::Db) -> Self {
        Self {
            kind,
            open_start_ns,
            open_return_ns: diagnostics_clock_ns(),
            first_get_ns: Arc::default(),
            recovery: db.remote_recovery_stats(),
        }
    }

    /// Record the first successful get once; later calls cost one load.
    pub(super) fn observe_get(&self) {
        // Relaxed: an independent diagnostic timestamp, set at most once.
        if self.first_get_ns.load(Ordering::Relaxed) == 0 {
            let _ = self.first_get_ns.compare_exchange(
                0,
                diagnostics_clock_ns().max(1),
                Ordering::Relaxed,
                Ordering::Relaxed,
            );
        }
    }

    pub(super) fn json(&self) -> Value {
        let kind = match self.kind {
            OpenKind::Create => "create",
            OpenKind::Reopen => "reopen",
        };
        // Relaxed: the timestamp is read once, after the timed windows.
        let first_get = match self.first_get_ns.load(Ordering::Relaxed) {
            0 => None,
            at => Some(at),
        };
        json!({
            "kind": kind,
            "open_start_ns": self.open_start_ns,
            "open_return_ns": self.open_return_ns,
            "open_elapsed_ns": self.open_return_ns.saturating_sub(self.open_start_ns),
            "first_successful_get_ns": first_get,
            "open_start_to_first_get_ns": first_get.map(|at| at.saturating_sub(self.open_start_ns)),
            "recovery": self.recovery.as_ref().map(recovery_json),
        })
    }
}

fn recovery_json(stats: &RecoveryStats) -> Value {
    json!({
        "tail_batches": stats.tail_batches,
        "tail_bytes": stats.tail_bytes,
        "tail_replay_ns": stats.tail_replay_ns,
        "old_generation_replayed_from_zero": stats.old_generation_replayed_from_zero,
        "stale_region_rows_skipped": stats.stale_region_rows_skipped,
        "connection_setup_ns": stats.connection_setup_ns,
        "fencing_ns": stats.fencing_ns,
        "tls_config_ns": stats.tls_config_ns,
        "cut": {
            "discarded_batches": stats.cut_discarded_batches,
            "extended_batches": stats.cut_extended_batches,
            "skipped_versions": stats.cut_skipped_versions,
            "continuity_bound": stats.cut_continuity_bound,
        },
        "semantics": "engine timing fields may nest; tail counts are batches the index received",
    })
}
