//! Option C recovery with mixed cell cuts (checkpoint option C spec
//! §8.1-§8.3). Not wired yet: Exact recovery stays the open path until the
//! commit stage activates `PerCell`.
//!
//! # Input
//!
//! - A [`CoveragePlan`] built from the loader's decoded and checked format-5
//!   root, and the index the loader built from that root: every root cell
//!   present, bases at their frame cuts `B_i`, promoted frontier and
//!   publication watermark at the replay floor `F`, not at the newest frame.
//! - The retained owner WAL suffix as [`ReplayStream`]s, one per
//!   `(partition, owner epoch)`, each started at its wrapper cursor
//!   ([`StreamCursor`]).
//!
//! # Replay
//!
//! Streams merge in bounded pages by `BatchVersion`, the order of exact
//! recovery: owner epoch, then sequence. A stream holds one owner epoch and
//! strictly increasing versions; no version repeats across streams. Batches
//! at or below `F` are skipped after these checks. Every whole authenticated
//! batch above `F` is replayed, also when every member is already in a cell
//! base: it fixes the order, the identity and the replay-tail account. A
//! member whose batch is at or below `max(C_i, B_i)` of its cell is held by
//! the root base: its exact reference is compared and never inserted or
//! charged again, and an absent or different reference fails recovery.
//! Every other member is staged with the original batch id, digest, address
//! and operation index. Spec §8.2: one cell covering a batch never skips the
//! batch for another cell. At the end, every base reference above `F` must
//! have met its suffix batch.
//!
//! # Bounded memory
//!
//! If the next batch would raise the resident index charge above `M`, every
//! dirty cell is flushed offline at the replayed prefix, one cell at a time,
//! through [`OfflineCellFlusher`]: at most one capture and one encoded cell
//! exist at once, pages stay bounded, and the suffix is never materialized.
//!
//! # Crash consistency (spec §8.3)
//!
//! Recovery writes no root, wrapper or manifest, returns no credit and
//! retires no WAL. Offline flush outputs are new canonical files that the
//! adopted root does not reference: after a crash during recovery the next
//! open deletes them as orphans and replays again from the same root and the
//! full retained suffix.
//!
//! # Serving
//!
//! The index moves in and comes back only from a successful replay. No read,
//! pin or write handle exists during reconstruction, and a failure drops the
//! index. Recovery is one thread: it takes the writer and then one shard, or
//! one shard alone, and never holds a guard across a flush.
#![cfg_attr(
    not(test),
    expect(dead_code, reason = "option C recovery, wired by the commit stage")
)]

use super::checkpoint_contract::CellCheckpointStream;
use super::*;
use crate::cell::CellId;
use crate::distributed::codec::BatchLimits;
use crate::distributed::native_index::{
    CellCaptureOutcome, CellDirty, CellFlushResult, CellInstall, CellRoot, PrefixCut, PreparedCell,
};

/// Offline cell flush for recovery (spec §8.1 step 7). The flusher stage
/// implements it over its cell store.
pub(crate) trait OfflineCellFlusher {
    /// Encode `prepared.encode_source()`, append it to a new canonical
    /// segment created with `create_new`, seal it, sync the file and its
    /// directory, map it, and return `prepared.finish(base)`. The output
    /// stays unadopted. An error drops `prepared`; that is allowed only
    /// because recovery then fails and drops the index.
    fn flush(&mut self, prepared: PreparedCell) -> Result<CellFlushResult, LocalDbError>;
}

/// One retained owner stream, already positioned at its wrapper cursor.
pub(super) trait ReplayStream {
    /// Owner epoch of the stream; every batch version is in it.
    fn epoch(&self) -> u64;

    /// Append at most `limit` (nonzero) next complete batches, authenticated
    /// by the owner transport, in stream order. Return `false`, appending
    /// nothing, once the stream is exhausted; otherwise append at least one
    /// batch, skipping inside the stream any page that its cursor filters to
    /// nothing. Thus every call makes progress.
    fn fill(
        &mut self,
        limit: usize,
        page: &mut VecDeque<(CompleteBatch, DurableBatch)>,
    ) -> Result<bool, LocalDbError>;
}

/// Where one retained stream starts and what it skips, from a format-4
/// wrapper stream (C0 contract `CellCheckpointStream`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum StreamCursor {
    /// The sealed history holds no batch above `F`: the stream is not read.
    Complete,
    /// WAL-replay mode: scan from the fragment boundary `from`. Complete
    /// batches at offsets up to `skip_through`, the last at or below `F`,
    /// are skipped. `durable_end` is the owner proof end through `U`, the
    /// end a scan must reach, never the skip position.
    Wal {
        from: u64,
        skip_through: Option<u64>,
        durable_end: u64,
    },
    /// Receipt-history mode: replay from the through-`F` receipt proof
    /// prefix. The fenced owner proof gives the retained end.
    Receipts { from: usize },
}

impl StreamCursor {
    pub(super) fn from_wrapper(
        stream: &CellCheckpointStream,
        persistence: HistoryPersistence,
    ) -> Result<Self, LocalDbError> {
        let wal = persistence == HistoryPersistence::WalReplay;
        match (&stream.receipt_prefix, wal) {
            (None, true) if stream.complete => Ok(Self::Complete),
            (None, true) => Ok(Self::Wal {
                from: stream.replay_from,
                skip_through: stream.last_offset,
                durable_end: stream.durable_end,
            }),
            (Some(_), false) if stream.complete => Ok(Self::Complete),
            (Some(prefix), false) => Ok(Self::Receipts {
                from: prefix.proof.prefix,
            }),
            _ => Err(LocalDbError::new(
                "wrapper stream cursor does not match the history persistence",
            )),
        }
    }

    /// True if the complete batch at physical `offset` is past the
    /// through-`F` skip and must be read.
    pub(super) fn reads(self, offset: u64) -> bool {
        match self {
            Self::Complete => false,
            Self::Wal { skip_through, .. } => skip_through.is_none_or(|last| offset > last),
            Self::Receipts { .. } => true,
        }
    }
}

/// A cell whose root base can hold members above `F`.
struct AheadCell {
    /// `max(C_i, B_i)`: members of batches through it are in the root base.
    through: BatchVersion,
    /// Base references above `F` at the start of replay.
    expected: u64,
    /// Covered members compared equal so far.
    matched: u64,
}

/// Per-cell coverage of an adopted root, reduced to what replay needs
/// (spec §8.1 step 5). Built before the root moves into the loader.
pub(super) struct CoveragePlan {
    floor: PrefixCut,
    ahead: HashMap<KeySpace, HashMap<CellId, AheadCell>>,
}

impl CoveragePlan {
    pub(super) fn new(root: &CellRoot) -> Self {
        let floor = root.header.replay_floor;
        let mut ahead: HashMap<KeySpace, HashMap<CellId, AheadCell>> = HashMap::new();
        for cell in &root.cells {
            let through = cell
                .base
                .as_ref()
                .map_or(cell.coverage, |base| base.frame_cut.max(cell.coverage));
            if let PrefixCut::Through(through) = through
                && !floor.contains(through)
            {
                ahead.entry(cell.keyspace).or_default().insert(
                    cell.cell.clone(),
                    AheadCell {
                        through,
                        expected: 0,
                        matched: 0,
                    },
                );
            }
        }
        Self { floor, ahead }
    }

    /// The cell's root base holds the members of `batch`, a batch above `F`.
    fn covering(
        &mut self,
        keyspace: KeySpace,
        cell: &CellId,
        batch: BatchVersion,
    ) -> Option<&mut AheadCell> {
        self.ahead
            .get_mut(&keyspace)?
            .get_mut(cell)
            .filter(|ahead| batch <= ahead.through)
    }
}

pub(super) struct CellReplayConfig<'a> {
    pub shape: &'a KeyShape,
    pub limits: BatchLimits,
    /// Complete batches buffered per stream; nonzero.
    pub page_limit: usize,
    /// The hard replay-tail bound of receipt-history mode. `None` in
    /// WAL-replay mode, where the tail only counts.
    pub tail_bound: Option<ReplayLimits>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct CellReplayStats {
    /// Whole batches above `F`, fully covered ones included.
    pub batches: u64,
    pub encoded_bytes: u64,
    pub fully_covered_batches: u64,
    /// Batches at or below `F` that the streams returned.
    pub skipped_batches: u64,
    pub applied_members: u64,
    pub covered_members: u64,
    pub offline_flush_passes: u64,
    pub offline_flushed_cells: u64,
    /// Largest resident index charge after a replayed batch.
    pub peak_resident_bytes: usize,
}

/// A completed reconstruction. The caller installs the final publication,
/// stream and identity state and the new authority epoch before serving.
pub(super) struct CellReplayed {
    pub index: NativeIndex,
    /// Every batch above `F`, fully covered ones included.
    pub known: HashMap<BatchId, AppendedBatch>,
    /// The complete replayed prefix: the newest batch above `F`, or `F`.
    /// The watermark covers it.
    pub prefix: PrefixCut,
    pub stats: CellReplayStats,
}

struct Lane {
    page: VecDeque<(CompleteBatch, DurableBatch)>,
    more: bool,
    last: Option<BatchVersion>,
}

/// Refill an empty lane with one bounded page and check its stream order.
fn refill(
    stream: &mut impl ReplayStream,
    lane: &mut Lane,
    limit: usize,
) -> Result<(), LocalDbError> {
    if !lane.page.is_empty() || !lane.more {
        return Ok(());
    }
    lane.more = stream.fill(limit, &mut lane.page)?;
    if lane.page.len() > limit || lane.more == lane.page.is_empty() {
        return Err(LocalDbError::new("invalid bounded replay stream page"));
    }
    for (batch, _) in &lane.page {
        let version = batch.version();
        if version.epoch != stream.epoch() || lane.last.is_some_and(|last| last >= version) {
            return Err(LocalDbError::new("recovery stream is not strictly ordered"));
        }
        lane.last = Some(version);
    }
    Ok(())
}

/// Replay the retained suffix over the loaded per-cell index (spec §8.1
/// steps 4-8). `retry_known` answers whether the through-`F` retry
/// authority (WAL-replay recent window or receipt history) holds an id.
pub(super) fn replay_mixed_cuts<S: ReplayStream>(
    index: NativeIndex,
    mut plan: CoveragePlan,
    streams: &mut [S],
    config: &CellReplayConfig<'_>,
    flusher: &mut impl OfflineCellFlusher,
    mut retry_known: impl FnMut(BatchId) -> Result<bool, LocalDbError>,
) -> Result<CellReplayed, LocalDbError> {
    if config.page_limit == 0 {
        return Err(LocalDbError::new("recovery page limit is zero"));
    }
    let floor = plan.floor;
    for (keyspace, cells) in &mut plan.ahead {
        for (cell, ahead) in cells {
            ahead.expected = index
                .base_references_above(*keyspace, cell, floor)
                .map_err(LocalDbError::component)?
                .ok_or_else(|| LocalDbError::new("root cell missing from the loaded index"))?;
        }
    }
    let mut lanes: Vec<Lane> = Vec::with_capacity(streams.len());
    let mut ready = BinaryHeap::with_capacity(streams.len());
    for (at, stream) in streams.iter_mut().enumerate() {
        let mut lane = Lane {
            page: VecDeque::with_capacity(config.page_limit),
            more: true,
            last: None,
        };
        refill(stream, &mut lane, config.page_limit)?;
        if let Some((batch, _)) = lane.page.front() {
            ready.push(Reverse((batch.version(), at)));
        }
        lanes.push(lane);
    }
    let mut known = HashMap::new();
    let mut stats = CellReplayStats::default();
    let mut prefix = floor;
    let mut previous: Option<BatchVersion> = None;
    let mut apply = Vec::new();
    let mut dirty = Vec::new();
    while let Some(Reverse((version, at))) = ready.pop() {
        let (batch, durable) = lanes[at]
            .page
            .pop_front()
            .ok_or_else(|| LocalDbError::new("recovery lane lost its head"))?;
        refill(&mut streams[at], &mut lanes[at], config.page_limit)?;
        if let Some((next, _)) = lanes[at].page.front() {
            ready.push(Reverse((next.version(), at)));
        }
        if previous.is_some_and(|previous| previous >= version) {
            return Err(LocalDbError::new(
                "duplicate logical version across recovery streams",
            ));
        }
        previous = Some(version);
        if floor.contains(version) {
            stats.skipped_batches += 1;
            continue;
        }
        if known.contains_key(&batch.id()) || retry_known(batch.id())? {
            return Err(LocalDbError::new(
                "duplicate authoritative identity or logical version",
            ));
        }
        // The whole batch is authenticated once: receipt binding, canonical
        // encoding, member checksums and every member reference.
        let prepared = PreparedNativePublication::prepare_authenticated(
            config.shape,
            config.limits,
            &batch,
            durable,
        )
        .map_err(LocalDbError::component)?;
        apply.clear();
        let mut charge = 0usize;
        for (operation, reference) in prepared.members() {
            if let Some(cell) = plan.covering(operation.ks(), operation.cell_id(), version) {
                if index.exact_member(
                    operation.ks(),
                    operation.cell_id(),
                    operation.reduced_key(),
                    reference.mutation_version(),
                ) != Some(*reference)
                {
                    return Err(LocalDbError::new(format!(
                        "covered member {} of batch {version:?} is absent from or different in its cell base",
                        reference.descriptor.operation_index
                    )));
                }
                cell.matched += 1;
                apply.push(false);
            } else {
                charge = index
                    .estimated_routed_bytes(std::slice::from_ref(operation))
                    .ok()
                    .and_then(|bytes| charge.checked_add(bytes))
                    .ok_or_else(|| LocalDbError::new("recovery batch index charge overflow"))?;
                apply.push(true);
            }
        }
        let applied = apply.iter().filter(|apply| **apply).count();
        if applied != 0 && index.check_capacity(charge).is_err() {
            offline_flush(&index, flusher, prefix, &mut dirty, &mut stats)?;
            index.check_capacity(charge).map_err(|error| {
                LocalDbError::new(format!(
                    "recovery batch {version:?} exceeds the index budget after offline flushing: \
                     {error} (charge {charge} B, resident {} B)",
                    index.charged_metadata_bytes()
                ))
            })?;
        }
        let last = batch
            .mutations()
            .len()
            .checked_sub(1)
            .and_then(|operation| batch.mutation_version(operation))
            .ok_or_else(|| LocalDbError::new("recovery batch has no mutation"))?;
        let mut writer = index.writer();
        let staged = prepared
            .publish_members(&apply, &mut writer)
            .map_err(LocalDbError::component)?;
        let promoted = writer
            .promote_committed()
            .map_err(LocalDbError::component)?;
        if staged != applied || promoted != applied {
            return Err(LocalDbError::new(format!(
                "batch {version:?} member already present above its cell coverage"
            )));
        }
        writer.advance_replayed(last);
        writer.publish_latest();
        drop(writer);
        known.insert(batch.id(), durable.appended());
        prefix = PrefixCut::Through(version);
        stats.batches += 1;
        stats.encoded_bytes += batch.encoded().len() as u64;
        stats.applied_members += applied as u64;
        stats.covered_members += (apply.len() - applied) as u64;
        stats.fully_covered_batches += u64::from(applied == 0);
        stats.peak_resident_bytes = stats
            .peak_resident_bytes
            .max(index.charged_metadata_bytes());
        if config.tail_bound.is_some_and(|bound| {
            stats.batches > bound.max_batches as u64
                || stats.encoded_bytes > bound.max_encoded_bytes as u64
        }) {
            return Err(LocalDbError::new(
                "retained tail exceeds committed admission bound",
            ));
        }
    }
    for cells in plan.ahead.values() {
        if cells.values().any(|cell| cell.matched != cell.expected) {
            return Err(LocalDbError::new(
                "cell base holds a reference above F that no suffix batch replayed",
            ));
        }
    }
    Ok(CellReplayed {
        index,
        known,
        prefix,
        stats,
    })
}

/// Flush every dirty cell at `target`, the complete replayed prefix, one
/// cell at a time: no serving user exists, so no capture can be stale or
/// skipped. Overlay references are all at or below `target` (fully covered
/// batches add none), so each capture leaves no residual.
fn offline_flush(
    index: &NativeIndex,
    flusher: &mut impl OfflineCellFlusher,
    target: PrefixCut,
    dirty: &mut Vec<((KeySpace, CellId), CellDirty)>,
    stats: &mut CellReplayStats,
) -> Result<(), LocalDbError> {
    stats.offline_flush_passes += 1;
    for shard in 0..index.shard_count() {
        index.shard_dirty_cells(shard, dirty);
        for ((keyspace, cell), state) in dirty.drain(..) {
            if state.versions == 0 {
                continue;
            }
            let permit = index
                .reserve_cell_flush(0)
                .map_err(LocalDbError::component)?;
            let outcome = index
                .writer()
                .capture_cell(keyspace, &cell, target, permit)
                .map_err(LocalDbError::component)?;
            let capture = match outcome {
                CellCaptureOutcome::Captured(capture) => capture,
                CellCaptureOutcome::Clean { .. } => continue,
                CellCaptureOutcome::Skipped { reason, .. } => {
                    return Err(LocalDbError::new(format!(
                        "offline recovery flush skipped a cell: {reason:?}"
                    )));
                }
            };
            let result = flusher.flush(capture.prepare().map_err(LocalDbError::component)?)?;
            let installed = index.writer().install_cell(result);
            match installed {
                // Replaced sources drop here, after the writer guard.
                CellInstall::Installed { retired, .. } => drop(retired),
                CellInstall::Stale { .. } => {
                    return Err(LocalDbError::new("offline recovery flush became stale"));
                }
                CellInstall::Rejected { error, .. } => {
                    return Err(LocalDbError::new(format!(
                        "offline recovery flush rejected: {error}"
                    )));
                }
            }
            stats.offline_flushed_cells += 1;
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "cell_recovery_tests.rs"]
mod tests;
