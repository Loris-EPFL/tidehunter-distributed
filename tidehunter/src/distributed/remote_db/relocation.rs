//! R1 relocation pass: native `WalBased` relocation semantics on leased owner
//! WALs (paper §2.3 principle 3 and §4.4: relocation never blocks writes).
//!
//! Convention (A13): cuts are exclusive (`below(cut)` is `version < cut`),
//! starts are inclusive and ends are exclusive.
//!
//! One pass:
//! 1. Freeze the scan cut `Q` = `Loader::last_processed()` (`min` of the
//!    Version B durable cut, the contiguous index-applied cut and any held
//!    checkpoint latch) and, per retained stream `(partition, fencing
//!    generation)`, the scan interval `[callback start, durable end)`.
//! 2. Page each stream's original batches from its owner (`RelocationPage`,
//!    metadata only) and merge them in global
//!    `(generation, batch sequence, member)` order with one page and one head
//!    per stream. Every authenticated put of a filtered keyspace is read
//!    through the ordinary position read and passed to the filter once,
//!    including puts the index no longer holds. The first `Stop` ends all
//!    callbacks, later members of its batch included. `Keep` is not a copy
//!    decision: like native at the parity tip, a filtered keyspace copies
//!    nothing.
//! 3. Per stream: terminal = the `Stop` batch, the first batch after `Stop`
//!    (other streams), the first batch above `Q`, or the clean tail at the
//!    durable end. The target is the native rule
//!    `round_down_to_file(min(terminal, lo + budget))` with the per-owner
//!    `relocation_max_reclaim_pct` budget. Without one complete file before a
//!    target the pass is `NoWork`: no copy, no start, no snapshot, no
//!    retirement.
//! 4. `DidWork`: a filtered keyspace copies nothing and drops every entry
//!    below its stream's TERMINAL unread, as native's cutoff
//!    (`relocation/mod.rs` `relocate_stripe`, `RelocationFlush::Cutoff`) does;
//!    entries from the target to the terminal go with them. Every other
//!    keyspace copies each live entry below its own stream's target to the
//!    serving generation at the head, then re-points it by compare-and-set on
//!    its exact expected position (never `insert`: equal order would be
//!    dropped, I4b). A copy keeps the logical version and operation index of
//!    the position it re-points and gets a new address and payload CRC, so the
//!    winner never changes by version.
//! 5. Make it durable: one snapshot stores the re-pointed index and the
//!    control region (per-stream `replay_from`), one manifest replacement
//!    raises the callback starts monotonically, then retirement runs under the
//!    advanced watermark. Bytes below both terms hold no live value, so no
//!    `Keep` survivor is ever unreachable and no reference walk is needed.
//!
//! A `NoWork` pass changes nothing. Offsets of different streams are never
//! compared: every target and every watermark term is per `(partition,
//! generation)`, because independent owner WALs have no shared byte order.

use super::concurrent::RelocatedCopies;
use super::dead_wal_files::core_retire_dead_wal_files;
use super::file_inventory::{OwnerSpan, span_budget_end};
use super::*;
use crate::cell::CellId;
use crate::distributed::hybrid::RelocationApply;
use crate::distributed::hybrid::StoredSnapshot;
use crate::distributed::hybrid::position::VersionPosition;
use crate::distributed::storage::RelocationBatch;
use crate::wal::position::IndexCut;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};

/// Batches requested per owner page; one page per stream is the merge state.
const PAGE_BATCHES: u16 = 256;
/// Pause between polls of an owner whose adoption is behind the required end.
const BEHIND_POLL: Duration = Duration::from_millis(2);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RelocationGate {
    /// Nothing to do: no target, or no whole file before one.
    NoWork,
    /// The pass had work. Whether it persisted it is `RelocationReport`'s
    /// `stored_target`/`deferred` pair: `DidWork` with `stored_target: Some`
    /// adopted the starts and retired; `DidWork` with `deferred: true` changed
    /// the index, persisted nothing and retired nothing (the routine outcome
    /// under a held checkpoint latch).
    DidWork,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CallbackDecision {
    Keep,
    Remove,
    Stop,
}

/// One callback, recorded only for an observed (test) pass.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(
    not(feature = "test_methods"),
    allow(dead_code, reason = "read only by the observed test pass")
)]
pub(crate) struct CallbackRecord {
    pub keyspace: KeySpace,
    pub key: Bytes,
    pub value: Bytes,
    /// Stream-local offset of the member's batch frame.
    pub wal_position: u64,
    pub decision: CallbackDecision,
}

/// Per scanned stream bounds, sorted by `(partition, fencing_generation)`.
/// Offsets of different streams are never comparable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct StreamReport {
    pub partition: u32,
    pub fencing_generation: u64,
    /// Oldest unretired position at or after the callback start.
    pub lo: u64,
    pub processed_upper: u64,
    /// The stream's durable end (the scan's required end).
    pub stream_end: u64,
    pub terminal_position: u64,
    pub rounded_target: u64,
}

#[derive(Clone, Debug)]
#[cfg_attr(
    not(feature = "test_methods"),
    allow(dead_code, reason = "production passes report through fail-stop only")
)]
pub(crate) struct RelocationReport {
    pub gate: RelocationGate,
    pub streams: Vec<StreamReport>,
    pub callbacks: Vec<CallbackRecord>,
    /// Survivors copied to the serving generation and re-pointed.
    pub copied_survivors: u64,
    /// Survivors whose bytes failed the indexed CRC. They keep their old
    /// position and their stream's start does not advance this pass.
    pub rejected_survivors: u64,
    /// The scan cut `Q` this pass copied against.
    pub scan_cut: BatchVersion,
    /// The stored target that covers this pass's re-points, or `None` when the
    /// store was a no-op or could not cover the scan cut: no start moved and no
    /// file was retired. A forced store reports the previous target, which is
    /// below `Q` by construction, and is still usable: see `stored_forced`.
    pub stored_target: Option<BatchVersion>,
    /// True when the covering store was the forced relocation path: the region
    /// was rewritten at the previous target after the dirty re-pointed cells
    /// were flushed and the index WAL synced.
    pub stored_forced: bool,
    /// The flush frontier the attempted store actually used (for a forced store
    /// the promotion clamp, for a non-forced one its own cut), or `None` when no
    /// store was attempted. Evidence for why a pass deferred.
    pub promotion_cut: Option<BatchVersion>,
    /// The pass may have mutated the index (its copies, and its drops, since
    /// `relocation_apply` returns no drop count) and did not persist that
    /// mutation: nothing adopted, nothing retired, the copies stay in the
    /// serving generation. It does not mean "nothing happened". Together with
    /// `NoWork` and `DidWork` with `stored_target: Some`, this receipts the
    /// three cases without a third gate value.
    pub deferred: bool,
}

/// Raise the adopted starts by the proposals of one pass. Every proposal names
/// a retained stream and is file-aligned; a proposal below the adopted start
/// leaves the adopted one (starts never move back).
pub(super) fn merge_callback_starts(
    adopted: &BTreeMap<(u32, u64), u64>,
    proposed: &BTreeMap<(u32, u64), u64>,
    retained_epochs: &[Vec<u64>],
    partition_indexes: &HashMap<u32, usize>,
    wal_file_bytes: u64,
) -> Result<BTreeMap<(u32, u64), u64>, LocalDbError> {
    let mut merged = adopted.clone();
    for (&(partition, generation), &from) in proposed {
        let retained = partition_indexes
            .get(&partition)
            .and_then(|node| retained_epochs.get(*node))
            .is_some_and(|epochs| epochs.contains(&generation));
        if !retained || from % wal_file_bytes != 0 {
            return Err(LocalDbError::new(
                "relocation callback start names no retained stream or is not file-aligned",
            ));
        }
        let start = merged.entry((partition, generation)).or_insert(0);
        *start = (*start).max(from);
    }
    Ok(merged)
}

/// One stream's scanned interval, frozen with `Q`.
struct StreamScan {
    node: usize,
    partition: u32,
    generation: u64,
    from: u64,
    required_end: u64,
}

/// Merge cursor of one stream: at most one owner page of batches.
struct Cursor {
    scan: StreamScan,
    next: Option<u64>,
    buffer: VecDeque<RelocationBatch>,
    /// First unretired offset at or after `from`, from the first page.
    start: Option<u64>,
    /// Set once nothing more will be fetched.
    upper: Option<u64>,
}

/// One value copy for the serving generation: full key, value and the logical
/// version the copy re-points.
type RelocatedValue = (Bytes, Bytes, BatchVersion);

fn cancelled() -> LocalDbError {
    LocalDbError::new("relocation pass cancelled by shutdown")
}

/// Whether a store may carry the pass's adoption and retirement. A non-forced
/// store must itself cover the scan cut (`cut >= q`, Amendment 13a). A forced
/// store keeps the previous target by design, so it is bounded by the flush
/// frontier it actually used instead (`promotion_cut >= q`); its `cut` is the
/// previous target, not the gate value, and is ignored whether it sits below,
/// at or above `q`. A store that did not store is never usable, whatever the
/// other fields say.
pub(super) fn usable_after_snapshot(
    stored: bool,
    forced: bool,
    cut: BatchVersion,
    promotion_cut: BatchVersion,
    q: BatchVersion,
) -> bool {
    stored && if forced { promotion_cut >= q } else { cut >= q }
}

/// Whether `position` lies below the exclusive byte target of its own stream.
/// A single global target would misclassify a two-generation cell: offsets of
/// different owner WALs have no shared order.
fn below_target(position: &VersionPosition, targets: &[(u32, u64, u64)]) -> bool {
    targets.iter().any(|&(partition, generation, end)| {
        partition == position.address.partition
            && generation == position.address.segment_generation
            && position.address.offset < end
    })
}

impl ConcurrentRemoteDatabase {
    /// One production pass. Native panics its relocator on a pass error and
    /// its background health then fails every later operation; the leased
    /// pass fail-stops the authority the same way. A shutdown cancellation is
    /// not a failure.
    pub(crate) fn background_relocation_pass(&self, cancel: &AtomicBool) {
        if let Err(error) = self.relocation_pass(false, cancel) {
            // A pass whose copies did not become durable in time applied
            // nothing; it is retried, never a database failure. Counted, so a
            // persistently failing owner sync is visible.
            if error.is_copies_not_durable() {
                self.write_core()
                    .copy_deferrals
                    .fetch_add(1, Ordering::Relaxed);
                return;
            }
            // Relaxed: the flag only selects the outcome; no data is handed off.
            if !cancel.load(Ordering::Relaxed) {
                self.poison(&error);
            }
        }
    }

    /// Run one relocation pass. `observe` records every callback with its key
    /// and value (test observation only; production keeps none). `cancel` is
    /// checked between merged batches and owner polls, before any copy.
    pub(crate) fn relocation_pass(
        &self,
        observe: bool,
        cancel: &AtomicBool,
    ) -> Result<RelocationReport, LocalDbError> {
        self.ensure_healthy()?;
        let core = self.write_core();
        let (cut, streams) = self.freeze_relocation_scan()?;
        let file = core.config.storage.wal_file_bytes;
        let pct = core.config.index.relocation_max_reclaim_pct;
        // Native reports the callbacks its filters produced even when the
        // pass has no whole file before a target and reclaims nothing
        // (`relocation/mod.rs`: the observed callbacks are pushed before the
        // `target_position < wal_file_size + min_wal_position` early return
        // leaves the gate at `NoWork`). The scan is paid for either way, so a
        // `NoWork` report carries it too.
        let empty =
            |streams: Vec<StreamReport>, callbacks: Vec<CallbackRecord>, scan_cut: BatchVersion| {
                RelocationReport {
                    gate: RelocationGate::NoWork,
                    streams,
                    callbacks,
                    copied_survivors: 0,
                    rejected_survivors: 0,
                    scan_cut,
                    stored_target: None,
                    stored_forced: false,
                    promotion_cut: None,
                    deferred: false,
                }
            };
        let Some(q) = cut else {
            return Ok(empty(Vec::new(), Vec::new(), BatchVersion::NONE));
        };
        let mut cursors: Vec<Cursor> = streams
            .into_iter()
            .map(|scan| Cursor {
                next: Some(scan.from),
                scan,
                buffer: VecDeque::new(),
                start: None,
                upper: None,
            })
            .collect();
        let mut clients = self.relocation_clients()?;
        // One page per stream first: the per-owner budget needs the stream's
        // current oldest unretired offset, and a kept survivor must never fall
        // outside a budget the pass later reclaims against.
        for cursor in &mut cursors {
            self.fill_cursor(&mut clients[cursor.scan.node], cursor, q, cancel)?;
        }
        let mut callbacks = Vec::new();
        let mut stop: Option<(usize, BatchVersion, u64)> = None;
        'merge: loop {
            // Relaxed: a cancellation hint; shutdown joins the worker.
            if cancel.load(Ordering::Relaxed) {
                return Err(cancelled());
            }
            for cursor in &mut cursors {
                self.fill_cursor(&mut clients[cursor.scan.node], cursor, q, cancel)?;
            }
            let Some(index) = (0..cursors.len())
                .filter(|&index| !cursors[index].buffer.is_empty())
                .min_by_key(|&index| cursors[index].buffer[0].version)
            else {
                break;
            };
            let batch = cursors[index]
                .buffer
                .pop_front()
                .ok_or_else(|| LocalDbError::new("relocation merge head vanished"))?;
            for put in &batch.puts {
                let keyspace = KeySpace(put.keyspace);
                let descriptor = self.shape.ks(keyspace);
                if descriptor.destroyed() {
                    continue;
                }
                let Some(filter) = descriptor.relocation_filter() else {
                    continue;
                };
                let (key, value) =
                    self.relocation_member(descriptor, &put.reduced_key, put.position)?;
                let decision = match filter(&key, &value) {
                    crate::Decision::Keep => CallbackDecision::Keep,
                    crate::Decision::Remove => CallbackDecision::Remove,
                    crate::Decision::StopRelocation => CallbackDecision::Stop,
                };
                if observe {
                    callbacks.push(CallbackRecord {
                        keyspace,
                        key: key.clone(),
                        value,
                        wal_position: batch.address.offset,
                        decision,
                    });
                }
                match decision {
                    // Native's scan stops at the Stop record's own position, so
                    // the record and every later one survive and the earlier
                    // members of its batch (called before it in this loop) are
                    // dropped with the rest of the prefix.
                    CallbackDecision::Stop => {
                        stop = Some((index, batch.version, put.position.address.offset));
                        break 'merge;
                    }
                    // Native at the parity tip: a filtered keyspace decides
                    // only where to stop. `Keep` and `Remove` copy nothing.
                    CallbackDecision::Keep | CallbackDecision::Remove => {}
                }
            }
        }
        let reports = stream_reports(&cursors, stop, file, pct, core.epoch)?;
        // One exclusive byte end per stream below which its files are
        // reclaimed: the native whole-file target.
        let mut targets: Vec<(u32, u64, u64)> = reports
            .iter()
            .map(|stream| {
                (
                    stream.partition,
                    stream.fencing_generation,
                    stream.rounded_target,
                )
            })
            .collect();
        targets.retain(|&(partition, generation, target)| {
            let lo = reports
                .iter()
                .find(|stream| {
                    stream.partition == partition && stream.fencing_generation == generation
                })
                .map_or(0, |stream| stream.lo);
            if generation < core.epoch {
                // A sealed stream only needs progress past its oldest live
                // byte; its last file may be partial.
                target > lo
            } else {
                // Native: one whole completed file before its bytes move.
                target >= lo.saturating_add(file)
            }
        });
        if targets.is_empty() {
            // Native: the filters were called, nothing else happens.
            return Ok(empty(reports, callbacks, q));
        }
        // Native's unread-drop bound: a filtered keyspace drops its entries
        // below the TERMINAL unread, never below the rounded target
        // (`relocation/mod.rs` `relocate_stripe`, `RelocationFlush::Cutoff`
        // carries `terminal_position`). Entries between the target and the
        // terminal go with them, exactly as native's cutoff removes them.
        //
        // Safety of the bound: `relocation_apply` compares an entry with the
        // terminal of its OWN `(partition, generation)` stream only, never
        // with another stream's terminal and never with a global offset. Each
        // terminal is at or below the durable end the scan read, and the scan
        // cut `Q` was frozen under `last_processed` (paper 4.4: relocation
        // reclaims processed bytes). A write admitted after the scan gets a
        // position above `Q`, so it lies above its stream's terminal and is
        // never dropped by this pass.
        // Only the streams that qualified above can drop: a stream with no
        // whole file before its target reclaims nothing, and native's early
        // return leaves its entries alone (`relocation/mod.rs`, the
        // `target_position < wal_file_size + min_wal_position` return).
        let targeted: BTreeSet<(u32, u64)> = targets
            .iter()
            .map(|&(partition, generation, _)| (partition, generation))
            .collect();
        let drop_targets: Vec<(u32, u64, u64)> = reports
            .iter()
            .filter(|stream| targeted.contains(&(stream.partition, stream.fencing_generation)))
            .map(|stream| {
                (
                    stream.partition,
                    stream.fencing_generation,
                    stream.terminal_position,
                )
            })
            .collect();
        let (copied, rejected, blocked) =
            self.relocate_cells(q, &targets, &drop_targets, cancel)?;
        // A survivor this pass could not verify or copy keeps its stream's
        // start where it was: advancing it would let retirement reclaim bytes
        // that still hold a live value. The copy of that key is not applied.
        targets.retain(|&(partition, generation, _)| !blocked.contains(&(partition, generation)));
        let callback_starts: BTreeMap<(u32, u64), u64> = targets
            .iter()
            .map(|&(partition, generation, target)| ((partition, generation), target))
            .collect();
        // The durable index, the control-region fronts and the allocation view
        // the watermark reads, in one native snapshot. Under Amendment 11 the
        // store is a no-op when its target does not advance, and the re-points
        // of this pass live only in the overlay until a store covers them:
        // a re-point keeps the old version, so nothing else persists it and
        // recovery rebuilds every position from the batch's own address.
        // A no-op store is the routine case for this pass: it re-points and
        // drops *old* versions, while a held checkpoint latch freezes the
        // target. The returned cut alone cannot tell a real store from a no-op
        // (a no-op returns the previous target, wherever it sits relative to
        // `Q`), so the store reports itself. `relocation_apply` returns no drop
        // count, so a pass that copied nothing still counts as mutating when it
        // had a target to drop against: deferring costs one pass and changes no
        // state, while adopting early can lose data.
        let first = core.snapshot_table()?;
        // A real store needs its target to advance, but relocation re-points
        // *old* versions: on an idle database or under a held latch the normal
        // store is a no-op forever and nothing would ever be reclaimed. The
        // forced path therefore runs whenever the normal store did not cover
        // the scan cut. It is unconditional on mutation, which is unknowable
        // until `relocation_apply` returns a drop count; a pass with a target
        // may always have dropped. It force-flushes the dirty re-pointed cells,
        // syncs the index WAL and rewrites the region at the previous cut,
        // which is native's `rebuild_control_region_from(target)` before
        // `wal.gc`.
        let StoredSnapshot {
            cut,
            stored,
            forced,
            promotion_cut,
        } = if first.stored && first.cut >= q {
            first
        } else {
            core.snapshot_table_for_relocation(q)?
        };
        // A forced store keeps the previous target and is bounded by its flush
        // frontier instead: it returns `stored = true` only when
        // `Q <= last_processed_at_capture`, so `retain_processed` kept nothing
        // of `[Q, capture)` in the overlay. A non-forced store must actually
        // cover the scan cut (Amendment 13a). Every re-pointed version is below
        // `Q` (exclusive as-of view), so `cut >= Q` covers all of them.
        // The forced branch's safety lives in the store's flush frontier, so the
        // precondition is checked here instead of trusted: a forced store is
        // usable only when the frontier it used reaches `Q`; a non-forced store
        // must cover the scan cut itself (Amendment 13a).
        let usable = usable_after_snapshot(stored, forced, cut, promotion_cut, q);
        if !usable {
            // The copies and in-memory re-points stay, unreferenced but
            // present; the start must not move and no file may be retired, or
            // a replay of the stored blobs would read deleted bytes.
            return Ok(RelocationReport {
                gate: RelocationGate::DidWork,
                streams: reports,
                callbacks,
                copied_survivors: copied,
                rejected_survivors: rejected,
                scan_cut: q,
                stored_target: None,
                stored_forced: false,
                promotion_cut: Some(promotion_cut),
                deferred: true,
            });
        }
        // Starts rise monotonically; a crash before this repeats the pass and
        // never retires a file the start does not cover.
        core.adopt_callback_starts(&callback_starts)?;
        // Test hook: the crash boundary after the durable capture (the region
        // store above and the adopted starts) and before any pruning. The
        // pass fails with every side effect up to it in place, the way a
        // process crash there leaves the files.
        #[cfg(test)]
        if std::mem::take(&mut core.test_hooks.lock().relocation_prune_interruption) {
            return Err(LocalDbError::new("injected relocation prune interruption"));
        }
        // Retirement under the advanced watermark. Like native, a held
        // checkpoint does not keep owner WAL files: the watermark is the two
        // stored terms alone (`dead_wal_files::take_snapshot`).
        {
            let _retirement = core.retirement.lock();
            core_retire_dead_wal_files(core)?;
        }
        Ok(RelocationReport {
            gate: RelocationGate::DidWork,
            streams: reports,
            callbacks,
            copied_survivors: copied,
            rejected_survivors: rejected,
            scan_cut: q,
            stored_target: Some(cut),
            stored_forced: forced,
            promotion_cut: Some(promotion_cut),
            deferred: false,
        })
    }

    /// Drop every entry of a filtered keyspace below its stream's terminal,
    /// then copy and re-point every live entry of every other keyspace below
    /// its own stream's target. Native's rule at the parity tip: a filtered
    /// keyspace copies nothing (`relocation/mod.rs` calls only
    /// `sync_flush_for_relocation(.., Cutoff)` for it). No exclusive gate and
    /// no write barrier: entries at or above their target are untouched, and a
    /// concurrent write above `Q` keeps its version because a re-point never
    /// changes a version.
    #[allow(
        clippy::type_complexity,
        reason = "three scalars of one pass outcome; a struct would rename the tuple fields only"
    )]
    #[allow(
        clippy::too_many_arguments,
        reason = "one internal call site; the accumulators are the pass's own outputs"
    )]
    fn relocate_cells(
        &self,
        q: BatchVersion,
        targets: &[(u32, u64, u64)],
        drop_targets: &[(u32, u64, u64)],
        cancel: &AtomicBool,
    ) -> Result<(u64, u64, BTreeSet<(u32, u64)>), LocalDbError> {
        let limit = self.relocation_batch_max_bytes();
        let mut copied = 0u64;
        let mut rejected = 0u64;
        let mut blocked: BTreeSet<(u32, u64)> = BTreeSet::new();
        // A stream whose survivor was rejected keeps no target: neither a copy
        // re-point nor a drop may touch its entries for the rest of the pass.
        let mut live_targets: Vec<(u32, u64, u64)> = targets.to_vec();
        for descriptor in self.shape.iter_ks() {
            let keyspace = descriptor.id();
            if descriptor.destroyed() {
                continue;
            }
            let filtered = descriptor.relocation_filter().is_some();
            let mut cell = descriptor.first_cell();
            loop {
                if cancel.load(Ordering::Relaxed) {
                    return Err(cancelled());
                }
                live_targets.retain(|&(partition, generation, _)| {
                    !blocked.contains(&(partition, generation))
                });
                if filtered {
                    // Native copies nothing for a filtered keyspace: the
                    // entries it removes are dropped unread below the terminal.
                    self.write_core().table.relocation_apply(
                        keyspace,
                        &cell,
                        RelocationApply::Drop,
                        drop_targets,
                    )?;
                } else {
                    copied += self.relocate_cell(
                        keyspace,
                        descriptor,
                        &cell,
                        q,
                        &live_targets,
                        limit,
                        cancel,
                        &mut rejected,
                        &mut blocked,
                    )?;
                }
                let Some(next) = self.write_core().table.next_cell(descriptor, &cell, false) else {
                    break;
                };
                cell = next;
            }
        }
        Ok((copied, rejected, blocked))
    }

    /// One cell: copy its live survivors in bounded chunks and re-point each
    /// chunk before the next. The view holds a cut latch, so promotion cannot
    /// pass `Q` while its positions are still being copied (I6).
    #[allow(
        clippy::too_many_arguments,
        reason = "one internal call site; the accumulators are the pass's own outputs"
    )]
    fn relocate_cell(
        &self,
        keyspace: KeySpace,
        descriptor: &crate::key_shape::KeySpaceDesc,
        cell: &CellId,
        q: BatchVersion,
        targets: &[(u32, u64, u64)],
        limit: usize,
        cancel: &AtomicBool,
        rejected: &mut u64,
        blocked: &mut BTreeSet<(u32, u64)>,
    ) -> Result<u64, LocalDbError> {
        let (view, latch) = self.write_core().table.relocation_view(keyspace, cell, q)?;
        let mut chunk: Vec<RelocatedValue> = Vec::new();
        let mut expected: Vec<(Bytes, VersionPosition)> = Vec::new();
        let mut bytes = 0usize;
        let mut copied = 0u64;
        for (reduced_key, position) in view.iter() {
            if cancel.load(Ordering::Relaxed) {
                return Err(cancelled());
            }
            // Below its own stream's target only: offsets of other streams are
            // never comparable.
            if !below_target(&position, targets) {
                continue;
            }
            // A survivor whose bytes do not verify against the indexed CRC is
            // not copied: the entry keeps its old position, the rejection is
            // counted, and its stream's start will not advance this pass.
            let value = match self
                .value_reader()
                .read_indexed(descriptor, &reduced_key, position)
            {
                Ok(Some((key, value))) => Some((key, value)),
                Ok(None) => None,
                Err(_) => {
                    *rejected += 1;
                    blocked.insert((
                        position.address.partition,
                        position.address.segment_generation,
                    ));
                    continue;
                }
            };
            let Some((key, value)) = value else {
                // A tombstone has no value to copy. A filtered keyspace never
                // reaches here; another keyspace keeps the tombstone's entry,
                // and native copies nothing for it either.
                continue;
            };
            bytes = bytes.saturating_add(value.len());
            chunk.push((key, value, position.batch));
            expected.push((reduced_key, position));
            if bytes >= limit {
                copied += self.flush_relocated(
                    keyspace,
                    cell,
                    &mut chunk,
                    &mut expected,
                    targets,
                    rejected,
                    blocked,
                )?;
                bytes = 0;
            }
        }
        // A stream that lost a survivor anywhere in this cell keeps no target
        // for the remaining chunks either, or a re-point would name bytes the
        // pass did not verify.
        let live: Vec<(u32, u64, u64)> = targets
            .iter()
            .copied()
            .filter(|&(partition, generation, _)| !blocked.contains(&(partition, generation)))
            .collect();
        copied += self.flush_relocated(
            keyspace,
            cell,
            &mut chunk,
            &mut expected,
            &live,
            rejected,
            blocked,
        )?;
        drop(latch);
        Ok(copied)
    }

    /// Write one chunk of copies to the serving generation, then re-point the
    /// expected positions. The copy is written before the compare-and-set, so a
    /// crash leaves either the old position (copy unreferenced) or the new one.
    #[allow(
        clippy::too_many_arguments,
        reason = "one internal call site; a context struct would only rename the parameters"
    )]
    fn flush_relocated(
        &self,
        keyspace: KeySpace,
        cell: &CellId,
        chunk: &mut Vec<RelocatedValue>,
        expected: &mut Vec<(Bytes, VersionPosition)>,
        targets: &[(u32, u64, u64)],
        rejected: &mut u64,
        blocked: &mut BTreeSet<(u32, u64)>,
    ) -> Result<u64, LocalDbError> {
        if chunk.is_empty() {
            return Ok(0);
        }
        let RelocatedCopies {
            members,
            last_version,
        } = self.write_relocated_batch(keyspace, cell, chunk.as_slice())?;
        // The invariant: an index entry never names a position that is not
        // durable-proven. A re-point keeps its entry's old version, so no cut
        // check on the index covers the copy; on the native tier the copy is
        // only in the owner's page cache until this returns. On error nothing is
        // applied: the copies stay unreferenced and the next pass retries.
        self.write_core().await_copies_durable(last_version)?;
        #[cfg(test)]
        {
            let pause = self
                .write_core()
                .test_hooks
                .lock()
                .relocation_copy_pause
                .take();
            if let Some((entered, resume)) = pause {
                let _ = entered.try_send((keyspace, expected.first().map(|(key, _)| key.clone())));
                let _ = resume.recv();
            }
        }
        if members.len() != chunk.len() || expected.len() != chunk.len() {
            return Err(LocalDbError::new(
                "relocation copy returned a different member count",
            ));
        }
        let mut updates: Vec<(Bytes, VersionPosition, VersionPosition)> =
            Vec::with_capacity(chunk.len());
        for ((key, expected_position), member) in expected.drain(..).zip(members) {
            // The owner's CRC verifies the transfer. It is never stored: a
            // write that differs from the indexed bytes is rejected, keeps its
            // old position and withdraws its stream's target, so a later read
            // can never succeed on corrupted bytes.
            if member.payload_crc != expected_position.payload_crc {
                *rejected += 1;
                blocked.insert((
                    expected_position.address.partition,
                    expected_position.address.segment_generation,
                ));
                continue;
            }
            let new = VersionPosition {
                batch: expected_position.batch,
                operation_index: expected_position.operation_index,
                // The indexed CRC is the authority for the copy as well.
                payload_crc: expected_position.payload_crc,
                address: member.address,
            };
            updates.push((key, expected_position, new));
        }
        let copied = u64::try_from(updates.len()).unwrap_or(u64::MAX);
        chunk.clear();
        // A stream blocked by this write keeps no target: its entries are
        // neither re-pointed nor dropped in the rest of the pass.
        let live: Vec<(u32, u64, u64)> = targets
            .iter()
            .copied()
            .filter(|&(partition, generation, _)| !blocked.contains(&(partition, generation)))
            .collect();
        self.write_core().table.relocation_apply(
            keyspace,
            cell,
            RelocationApply::Repoint(&updates),
            &live,
        )?;
        Ok(copied)
    }

    /// `Q` and the retained streams' scan intervals, frozen together under the
    /// publisher lock.
    fn freeze_relocation_scan(
        &self,
    ) -> Result<(Option<BatchVersion>, Vec<StreamScan>), LocalDbError> {
        let core = self.write_core();
        let q = core.table.last_processed();
        let mut publisher = core.publisher.lock();
        let published_next = publisher.published_next;
        let state = publisher.checkpoint_state();
        // Cuts are exclusive (`below(cut) = version < cut`) and `published_next`
        // is the next sequence to publish, so `q == published_next` names only
        // published batches: it is the normal idle state (everything published
        // and processed). A cut ABOVE the published floor names a batch that may
        // still be published under us; the publisher state is the authority.
        if q != BatchVersion::NONE && q.epoch == core.epoch && q.sequence > published_next {
            return Err(LocalDbError::new(
                "relocation stable cut exceeds published floor",
            ));
        }
        let q = (q != BatchVersion::NONE).then_some(q);
        let mut streams = Vec::new();
        for (node, generations) in state.retained_epochs.iter().enumerate() {
            let partition = core.config.endpoints[node].partition;
            for &generation in generations {
                let Some(position) = state.positions.get(&(partition, generation)) else {
                    continue;
                };
                // A stream whose durable prefix is not contiguous yet has no
                // scan bound that proves completeness: skip it, never guess.
                if !position.complete {
                    continue;
                }
                // A sealed stream whose adopted start reached its end has no
                // byte left to scan. The clean-tail target is rounded up to
                // the end of the last file, so the start can lie above the
                // end; the owner refuses a page whose start is above its end.
                let from = state
                    .callback_starts
                    .get(&(partition, generation))
                    .copied()
                    .unwrap_or(0);
                if generation < core.epoch && from >= position.durable_end {
                    continue;
                }
                streams.push(StreamScan {
                    node,
                    partition,
                    generation,
                    from,
                    required_end: position.durable_end,
                });
            }
        }
        streams.sort_by_key(|stream| (stream.partition, stream.generation));
        Ok((q, streams))
    }

    /// One dedicated authenticated session per owner: relocation pages never
    /// use the owner worker's control lane.
    fn relocation_clients(&self) -> Result<Vec<Client>, LocalDbError> {
        let config = &self.write_core().config;
        let tls_config = config
            .tls
            .client_config_with_identity()
            .map_err(LocalDbError::component)?;
        config
            .endpoints
            .iter()
            .map(|endpoint| {
                let mut tls = config.tls.clone();
                tls.server_name = endpoint.server_name.clone();
                Client::connect_shared(
                    &endpoint.address,
                    &tls,
                    tls_config.clone(),
                    config.timeout,
                    config.partition_config(1, endpoint.partition),
                    &self.shape,
                )
                .map_err(|error| {
                    LocalDbError::new(format!(
                        "relocation session to partition {} failed: {error}",
                        endpoint.partition
                    ))
                })
            })
            .collect()
    }

    /// Give `cursor` a head batch or mark it ended. Batches above `Q` end
    /// the stream at their offset. Waits, bounded by the configured timeout,
    /// while the owner has not adopted the required end.
    fn fill_cursor(
        &self,
        client: &mut Client,
        cursor: &mut Cursor,
        q: BatchVersion,
        cancel: &AtomicBool,
    ) -> Result<(), LocalDbError> {
        let deadline = Instant::now() + self.config.timeout;
        while cursor.buffer.is_empty() && cursor.upper.is_none() {
            if cancel.load(Ordering::Relaxed) {
                return Err(cancelled());
            }
            let Some(from) = cursor.next else {
                cursor.upper = Some(cursor.scan.required_end);
                break;
            };
            let page = client
                .relocation_page(
                    self.epoch,
                    cursor.scan.generation,
                    from,
                    cursor.scan.required_end,
                    PAGE_BATCHES,
                )
                .map_err(|error| {
                    LocalDbError::new(format!(
                        "relocation page of partition {} generation {} failed: {error}",
                        cursor.scan.partition, cursor.scan.generation
                    ))
                })?;
            cursor.start.get_or_insert(page.start);
            if page.available_end < cursor.scan.required_end {
                if Instant::now() >= deadline {
                    return Err(LocalDbError::new(
                        "relocation scan: owner adoption stays below the published cut",
                    ));
                }
                std::thread::sleep(BEHIND_POLL);
                continue;
            }
            for batch in page.batches {
                if batch.version > q {
                    cursor.upper = Some(batch.address.offset);
                    break;
                }
                cursor.buffer.push_back(batch);
            }
            cursor.next = page.next;
        }
        Ok(())
    }

    /// The member value of one exact position through the native decode
    /// helper. The scan's `payload_crc` covers the bytes the owner just read,
    /// so a corrupt frame fails here and never reaches a copy; the copy ladder
    /// re-points from the index view, whose own CRC is the authority.
    fn relocation_member(
        &self,
        descriptor: &crate::key_shape::KeySpaceDesc,
        reduced_key: &[u8],
        position: VersionPosition,
    ) -> Result<(Bytes, Bytes), LocalDbError> {
        self.value_reader()
            .read_indexed(descriptor, reduced_key, position)?
            .ok_or_else(|| LocalDbError::new("relocation member is not a put"))
    }
}

/// Per-stream lo, terminal and native whole-file target. `stop` names the
/// cursor, version and offset of the first Stop batch.
fn stream_reports(
    cursors: &[Cursor],
    stop: Option<(usize, BatchVersion, u64)>,
    file: u64,
    pct: u8,
    serving: u64,
) -> Result<Vec<StreamReport>, LocalDbError> {
    let mut reports: Vec<StreamReport> = cursors
        .iter()
        .enumerate()
        .map(|(index, cursor)| {
            let start = cursor
                .start
                .unwrap_or(cursor.scan.from)
                .max(cursor.scan.from);
            let lo = start - start % file;
            let processed_upper = cursor.upper.unwrap_or(cursor.scan.required_end);
            // A stream that reached its durable end without a Stop ends
            // there, as the native clean tail does: the leased scan only
            // completes adopted groups, so no torn frame lies below.
            let ended = || processed_upper;
            let terminal_position = match stop {
                Some((stream, _, offset)) if stream == index => offset,
                // Every unconsumed head is after the Stop in logical order, so
                // this stream stops at its first record, the position native's
                // linear scan would have stopped at.
                Some(_) => cursor.buffer.front().map_or_else(ended, |head| {
                    head.puts
                        .first()
                        .map_or(head.address.offset, |put| put.position.address.offset)
                }),
                None => ended(),
            };
            StreamReport {
                partition: cursor.scan.partition,
                fencing_generation: cursor.scan.generation,
                lo,
                processed_upper,
                stream_end: cursor.scan.required_end,
                terminal_position,
                rounded_target: 0,
            }
        })
        .collect();
    // Per owner: budget over its streams, oldest generation first.
    let mut partitions: Vec<u32> = reports.iter().map(|stream| stream.partition).collect();
    partitions.dedup();
    for partition in partitions {
        let spans: Vec<OwnerSpan> = reports
            .iter()
            .filter(|stream| stream.partition == partition)
            .map(|stream| OwnerSpan {
                generation: stream.fencing_generation,
                lo: stream.lo,
                hi: stream.processed_upper.max(stream.lo),
            })
            .collect();
        let ends = span_budget_end(&spans, file, pct);
        for stream in reports
            .iter_mut()
            .filter(|stream| stream.partition == partition)
        {
            let budget_end = ends
                .get(&stream.fencing_generation)
                .copied()
                .ok_or_else(|| LocalDbError::new("relocation budget lost a stream"))?;
            let target = stream.terminal_position.min(budget_end);
            // A sealed stream can never append, so its clean tail (the exact
            // stream end) is rounded UP to the end of its last file: a
            // callback start is a file-aligned offset (the manifest and the
            // merge refuse any other), and a start below the stream end could
            // never satisfy trim eligibility (`callback_start >= durable_end`)
            // or the full-coverage retirement of the last partial file. The
            // bytes between the stream end and the file end hold no frame,
            // so no copy is skipped. Any other target, a mid-file Stop or a
            // percentage budget included, rounds down as native does;
            // otherwise the file would be retired with live bytes above the
            // target that no copy covers.
            // The clean tail is exactly the stream end: the scan ended there
            // without a Stop and without a budget cut.
            let clean_tail = stream.fencing_generation < serving
                && target == stream.processed_upper
                && target == stream.stream_end;
            stream.rounded_target = if clean_tail {
                target
                    .checked_next_multiple_of(file)
                    .ok_or_else(|| LocalDbError::new("relocation clean-tail target overflow"))?
            } else {
                target - target % file
            };
        }
    }
    Ok(reports)
}

#[cfg(test)]
#[path = "relocation_unit_tests.rs"]
mod unit_tests;

#[cfg(test)]
#[path = "relocation_promoted_l1_tests.rs"]
mod promoted_l1_tests;
