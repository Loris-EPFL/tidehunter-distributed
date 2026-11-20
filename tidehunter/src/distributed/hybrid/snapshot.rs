//! The snapshot pass and the thread that paces it.
//!
//! This is native `Db::rebuild_control_region_from` (`db.rs:1627-1730`) and its
//! periodic driver (`db.rs:505-555`) over the hybrid index. The pass is the same
//! sequence with two substitutions, and nothing else:
//!
//! * the **stale-cell threshold** and the **root trigger** are value-byte
//!   distances (`snapshot_unload_threshold`, `snapshot_written_bytes`) exactly
//!   as native, taken from the monotone published-byte counter (I2b);
//! * the **value WAL fsync** becomes a release-mode proof that the region head
//!   is at or below the persisted Version B durable cut. Native syncs because
//!   its `last_processed` is an in-process frontier with no durable record; the
//!   hybrid has one, and it is fsynced before this region, so every position
//!   the stored region names is already owner-synced or aborted. A head above
//!   the cut is a Version B defect: the pass stores nothing and reports
//!   `Invalid`, which is the fail-closed behaviour native's `expect` provides.
//!
//! Ordering that must not be rearranged:
//!
//! 1. a cut latch is pinned across both passes, so promotion cannot walk past
//!    the head this pass will prove (native's flush frontier cannot pass
//!    `last_processed` for the same reason);
//! 2. both passes run unconditionally, so a stalled cut still force-flushes
//!    stale cells, relocates and collects garbage exactly as native does;
//! 3. the cut the region names is native's `replay_from` — the minimum
//!    `last_processed` over the snapshot's non-empty cells, capped at the head —
//!    not the head itself: a dirty cell that was not flushed is replayed from
//!    its own frontier instead of being lost. The cut is proved durable and the
//!    owner views are drained at it **after** the snapshot, because the
//!    per-cell frontiers the snapshot captured are what the region will name;
//! 4. only the region store and the unlink are conditional on the cut having
//!    advanced. Without a new region on disk, deleting a file the previous
//!    region names would break recovery.
//!
//! Everything after the pass-1 barrier is cold: it runs once per
//! `snapshot_written_bytes` or hour, never on a put or a get.

use super::region::{
    HybridControlRegion, MAX_REGION_STREAMS, max_region_cells, snapshot_due, stale_threshold,
};
use super::table::HybridTable;
use crate::control::RelocateFiles;
use crate::distributed::remote_db::RemoteDatabase;
use crate::distributed::storage::LocalDbError;
use crate::distributed::types::BatchVersion;
use crate::metrics::TimerExt;
use crate::wal::position::WalFileId;
use std::collections::HashSet;
use std::sync::Weak;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

/// Cadence of the snapshot thread. Native hardcodes one death check per second,
/// sixty per pass, and one timed root per hour; the defaults reproduce that.
#[derive(Clone, Copy, Debug)]
pub(crate) struct SnapshotConfig {
    /// Native `snapshot_written_bytes`: value bytes between roots.
    pub(crate) written_bytes: u64,
    /// Native `SNAPSHOT_EVERY_SECS`: a root per interval whatever the bytes.
    pub(crate) interval: Duration,
    /// Liveness poll between checks.
    pub(crate) poll: Duration,
    /// Polls per check; `poll * polls_per_check` is the byte-check cadence.
    pub(crate) polls_per_check: u32,
}

impl SnapshotConfig {
    /// Native cadence: 60 s polls, hourly root.
    pub(crate) fn native(written_bytes: u64) -> Self {
        Self {
            written_bytes,
            interval: Duration::from_secs(3600),
            poll: Duration::from_secs(1),
            polls_per_check: 60,
        }
    }
}

/// Runs the pass until the database is dropped or a pass fails.
///
/// A failed pass is fail-stop: the exact cause becomes the engine's first
/// recorded failure, the way native's `expect("Failed to rebuild control
/// region")` stops the process, so a caller cannot keep serving from an index
/// whose root no longer advances.
pub(crate) fn spawn_snapshot_thread(
    db: Weak<RemoteDatabase>,
    config: SnapshotConfig,
) -> std::io::Result<JoinHandle<()>> {
    // Native initialises the distance at the position current when the thread
    // starts, so the first root after a restart comes a full window later.
    let mut stored = db.upgrade().map_or(0, |db| db.index_value_bytes());
    thread::Builder::new()
        .name("hybrid-snapshot".to_owned())
        .spawn(move || {
            let mut last_root = Instant::now();
            loop {
                for _ in 0..config.polls_per_check {
                    if db.strong_count() == 0 {
                        return;
                    }
                    thread::sleep(config.poll);
                }
                let Some(db) = db.upgrade() else {
                    return;
                };
                let current = db.index_value_bytes();
                let timed = last_root.elapsed() >= config.interval;
                if !timed && !snapshot_due(current, stored, config.written_bytes) {
                    continue;
                }
                // The distance is measured from the pass start, as native measures
                // it from the position captured when the thread started. A pass that
                // found nothing new to store leaves the distance alone, so it runs
                // again at the next poll instead of losing a whole window.
                let pass_start = current;
                match db.snapshot_index() {
                    Ok(pass) => {
                        if pass.stored {
                            stored = pass_start;
                        }
                    }
                    Err(error) => {
                        db.record_snapshot_failure(&error);
                        return;
                    }
                }
                last_root = Instant::now();
            }
        })
}

/// What one snapshot pass did.
///
/// Both facts come from the same call so a caller cannot disagree about whether
/// a region was written: the relocation/retirement pass must not adopt a scan cut
/// when the store was a no-op, and the cadence thread must not reset its byte
/// window either.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct StoredSnapshot {
    /// The cut the stored region names; also correct when nothing was stored.
    pub(crate) cut: BatchVersion,
    /// A new region was written. `false` means the region and every per-stream
    /// front are byte-identical to the previous store.
    pub(crate) stored: bool,
    /// A store at an unchanged head, requested by the relocation path so its
    /// scan cut is covered. The normal cadence path never sets it.
    pub(crate) forced: bool,
    /// The flush frontier the store actually used. Equal to `cut` on the cadence
    /// path (and on a no-op, where `cut` is the previous head); a forced store at
    /// an unchanged head reports the frontier captured for it, so a consumer can
    /// assert its scan cut is covered instead of trusting the caller's bound
    /// (Amendment 24).
    pub(crate) promotion_cut: BatchVersion,
}

/// The snapshot's frontier fell below the stored region cut during a forced
/// store. Storing at the stored cut would name a region whose dirty cells are
/// older than its cut, so the pass fails with this typed cause instead of a
/// message built at the call site. Reachable only through a reader latch pinned
/// below the stored head (the checkpoint pin race); `stored_target()` and the
/// pass's own pin cannot produce it.
#[derive(Debug)]
struct CutBelowStored {
    cut: BatchVersion,
    stored: BatchVersion,
}

impl std::fmt::Display for CutBelowStored {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "snapshot cut {:?} is below the stored region cut {:?}: a reader latch is \
             pinned below the stored head",
            self.cut, self.stored
        )
    }
}

impl std::error::Error for CutBelowStored {}

/// One native snapshot pass over the hybrid index.
pub(crate) fn rebuild_control_region(
    table: &HybridTable,
    force_store: bool,
    promotion_cut: Option<BatchVersion>,
) -> Result<StoredSnapshot, LocalDbError> {
    let stored = table.stored_cut();
    // The head this pass stores at. It is `min(durable, applied, lowest held
    // latch)` raised to the stored head, so it never moves back and never
    // exceeds the durable cut. Pinning the clamp at exactly this value keeps the
    // two cuts from being compared: every cell frontier the snapshot captures is
    // at or below the pin, whatever `last_processed()` reports before or after.
    let head = table.stored_target();
    // The frontier the flush actually covers: the relocation clamp on a forced
    // pass, the head otherwise.
    let flush_frontier = promotion_cut.unwrap_or(head);
    if force_store && promotion_cut.is_none() {
        return Err(LocalDbError::new(
            "forced store requires a promotion frontier",
        ));
    }
    // A frontier above the head would flush cells past the cut the region stores,
    // so the stored region would name positions its own head excludes. Fail
    // closed on the caller's precondition before doing any work.
    if flush_frontier > head {
        return Err(LocalDbError::new(
            "forced promotion cut above the stored target",
        ));
    }
    // Pin the clamp across both passes. Without it the frontier advances while
    // the pass runs, and the region would name flushed cells above the head this
    // pass proves durable. The pin is released when it drops at the end.
    let latch = table.pin_cut(head);

    let large_table = table.large_table();
    let loader = table.loader();
    let index_wal = loader.index_wal();
    let index_writer = loader.index_writer();
    let config = table.config();

    // Files written by this pass must not enter the relocation candidate set.
    let pre_snapshot_index_position = index_writer.position();
    let layout = index_wal.layout().clone();

    // Pass 1: queue async flushes for cells stale by at least the threshold and
    // accumulate per-file live bytes; no I/O under a cell lock. `threshold` is
    // an inclusive frontier: a cell is skipped when its `last_processed` is
    // above it. The forced path therefore passes the current byte position (the
    // hybrid analogue of native's `force_rebuild_control_region` using
    // `wal_writer.position()`), which flushes every dirty cell — zero would skip
    // them all, and the re-pointed cells are dirty whatever their byte age.
    let threshold = if force_store {
        table.value_bytes()
    } else {
        stale_threshold(table.value_bytes(), config.snapshot_unload_threshold)
    };
    let alive_bytes = large_table.flush_and_accumulate(loader, &layout, threshold);
    large_table.flusher.barrier();
    #[cfg(test)]
    if let Some(error) = injected(SnapshotBoundary::AfterFlush) {
        return Err(error);
    }

    let relocate_files = RelocateFiles::from_accumulator(
        alive_bytes,
        layout,
        config.index_min_occupancy_pct,
        pre_snapshot_index_position,
    );

    // Pass 2: force-relocate low-occupancy files, wait for them, snapshot.
    let mut region = table.region().lock();
    let _timer = table
        .metrics()
        .rebuild_control_region_time_mcs
        .clone()
        .mcs_timer();
    let _snapshot_timer = table.metrics().snapshot_lock_time_mcs.clone().mcs_timer();
    let snapshot = large_table.relocate_and_snapshot(loader, &relocate_files);

    // Sparse GC: a file is reclaimable iff no position in the snapshot names it
    // and it sits below both safety bounds — a file written during this pass
    // (its fresh blob's updated levels live only in memory) and the highest
    // live file (the writer rewinds to `next_after(max(live))`).
    let layout = index_wal.layout();
    let pre_snapshot_file = layout.locate_file(pre_snapshot_index_position);
    let mut live_files: HashSet<WalFileId> = HashSet::new();
    let mut max_live_file: Option<WalFileId> = None;
    for position in snapshot.data.iter_valid_val_positions() {
        let file = layout.locate_file(position.offset());
        live_files.insert(file);
        max_live_file = Some(max_live_file.map_or(file, |current| current.max(file)));
    }
    let to_delete = match max_live_file {
        Some(max_live_file) => {
            let cutoff = pre_snapshot_file.min(max_live_file);
            index_wal
                .file_ids()
                .into_iter()
                .filter(|file| *file < cutoff && !live_files.contains(file))
                .collect()
        }
        // Empty snapshot: restart rewinds the writer to offset 0, so every file
        // it may traverse must stay.
        None => Vec::new(),
    };
    // The region depends on the index blobs it references. The map syncer is
    // asynchronous, so persist those files before the region can become
    // durable; the processed frontier was captured before these syncs, so every
    // write below it has finished.
    let index_complete_below = layout.locate_file(index_writer.last_processed().as_u64());
    if let Some(max_live_file) = max_live_file {
        index_wal.sync_files_through(max_live_file, index_complete_below)?;
    }
    #[cfg(test)]
    if let Some(error) = injected(SnapshotBoundary::AfterIndexSync) {
        return Err(error);
    }

    // The cut the region names is native's: the minimum `last_processed` over the
    // snapshot's non-empty cells (`snapshot.replay_from`), capped at the head.
    // A cell whose puts are below the head but newer than its last flush is
    // dirty and unflushed: it is in neither a blob nor the replay range of a
    // region headed at `head`, so a crash would lose it. Heading the region (and
    // the owner drain, and so the stored fronts) at the cell's own frontier makes
    // recovery replay it, with no extra flush: the byte-distance windows above
    // stay exactly native's, and a cell that stays dirty only holds the cut back
    // until the next stale-cell flush, as in native. The pin keeps every cell
    // frontier at or below `head`, so the cap is a guard rather than a rule.
    let cut = head.min(snapshot.replay_from);
    // The no-op decision gates only the durable proof, the drain, the store and
    // the unlink: both passes above always run, so a stalled cut still
    // force-flushes, relocates and collects garbage exactly as native does.
    //
    // `cut < stored` is not reachable through `stored_target()` (monotone) or
    // through the pin above. The one path is native `ks_snapshot`, which assigns
    // a clean cell the loader's `last_processed()` unconditionally, so a reader
    // latch below `stored` lowers `replay_from`. A latch can land there only
    // through the checkpoint pin race (sampled at `current()`, pinned after
    // `stored_target()` advanced past it), which the iterator/checkpoint slice
    // closes by pinning above the stored head.
    //
    // The two modes answer it differently on purpose. A forced store must store
    // something, and storing at `stored` would name a region whose dirty cells
    // are older than its cut, so it fails with a typed invariant error: a latch
    // landing below `stored` during a relocation pass is a failed pass, never a
    // deferral. The cadence pass stores nothing in this case, which leaves the
    // previous region and its fronts valid and byte-identical; an error there
    // would fail-stop the database over a reader race that lost no data, so it
    // is the same safe no-op as an unmoved cut.
    if cut < stored && force_store {
        return Err(LocalDbError::component(CutBelowStored { cut, stored }));
    }
    if !force_store && cut <= stored {
        if cut < stored {
            // A reader latch pinned below the stored head lowered the cut. The
            // pass stores nothing, so the no-op is safe, but it must not be
            // silent: a latch stuck below the head holds every later snapshot
            // back the same way. The table counts each deferral
            // (`snapshot_latch_deferrals`) so that shows up in its metrics.
            table.record_snapshot_latch_deferral();
        }
        // Nothing the region could add: keep the previous region and its
        // per-stream fronts byte-identical.
        return Ok(StoredSnapshot {
            cut: stored,
            stored: false,
            forced: false,
            promotion_cut: stored,
        });
    }
    // A forced pass whose cut did move is an ordinary store: the re-pointed
    // cells were flushed anyway, and the new cut is what the region must name.
    let forced_same_cut = force_store && cut == stored;
    // What the caller sees as the promotion frontier: the cut on the ordinary
    // path, the relocation clamp on a same-cut forced store.
    let stored_promotion = if forced_same_cut { flush_frontier } else { cut };
    // Native fsyncs the value WAL here; the persisted durable cut is the proof.
    // The blobs this pass wrote were flushed at `flush_frontier` (the head on
    // the ordinary path, the relocation clamp on the forced one), which can be
    // above `cut`, so that is the frontier proved, not the cut. A frontier above
    // the durable cut is a Version B defect and must fail closed, before the
    // store, so no region ever names an unproven position.
    loader.ensure_durable_target(flush_frontier)?;
    #[cfg(test)]
    if let Some(error) = injected(SnapshotBoundary::AfterDurableTarget) {
        return Err(error);
    }
    // Freeze each owner worker's allocation view at the cut and acknowledge, so
    // the fronts stored below cover every allocated extent from the cut on,
    // including one whose batch has not been published yet. A front above a live
    // extent would leave that extent out of recovery.
    table.drain_owner_streams(cut)?;
    #[cfg(test)]
    if let Some(error) = injected(SnapshotBoundary::AfterDrain) {
        return Err(error);
    }

    // Store before unlinking: a crash between the two leaves extra files, which
    // the next pass tolerates, never a region naming a deleted file. The region
    // cut is monotone (`cut >= stored` above), which is what the head assertion
    // of the next store compares against; a forced store keeps the cut exactly
    // as it was and only re-covers it with the new per-cell bases.
    let next_region = HybridControlRegion::new(
        snapshot.data,
        cut,
        table.shape(),
        table.stream_fronts(),
        table.cell_bytes(),
    );
    // Fail closed: a region over the stream or cell bound is never written
    // (and never truncated), so the previous region stays authoritative.
    next_region.validate_bounds(MAX_REGION_STREAMS, max_region_cells(table.shape()))?;
    region.store(next_region)?;
    index_writer.delete_files(to_delete)?;
    drop(latch);
    Ok(StoredSnapshot {
        cut,
        stored: true,
        forced: forced_same_cut,
        promotion_cut: stored_promotion,
    })
}

/// A durable step of the pass. A pass that fails at one has performed every
/// side effect up to it and none after, exactly what a process crash at that
/// boundary leaves on disk.
#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SnapshotBoundary {
    /// Pass-1 flushes completed; nothing stored.
    AfterFlush,
    /// The region head is at or below the durable cut; nothing stored.
    AfterDurableTarget,
    /// Owner allocation views drained to the head; nothing stored.
    AfterDrain,
    /// Index WAL files synced; no region stored.
    AfterIndexSync,
}

#[cfg(test)]
thread_local! {
    static SNAPSHOT_FAULT: std::cell::Cell<Option<SnapshotBoundary>> =
        const { std::cell::Cell::new(None) };
}

/// Test hook: fail the next pass at `boundary` on this thread.
#[cfg(test)]
pub(crate) fn inject_snapshot_fault(boundary: SnapshotBoundary) {
    SNAPSHOT_FAULT.with(|slot| slot.set(Some(boundary)));
}

#[cfg(test)]
fn injected(boundary: SnapshotBoundary) -> Option<LocalDbError> {
    if SNAPSHOT_FAULT.with(std::cell::Cell::get) == Some(boundary) {
        SNAPSHOT_FAULT.with(|slot| slot.set(None));
        Some(LocalDbError::new(format!(
            "injected snapshot crash at {boundary:?}"
        )))
    } else {
        None
    }
}
