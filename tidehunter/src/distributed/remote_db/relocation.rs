//! R1 relocation pass: native relocation semantics on leased owner WALs
//! (paper §2.3 principle 3 and §4.4: relocation never blocks writes).
//!
//! Convention (A13): cuts are exclusive (`below(cut)` is `version < cut`),
//! starts are inclusive and ends are exclusive.
//!
//! `HybridIndexConfig::relocation_strategy` selects one of native's two
//! strategies. Steps 1, 3, 5 and the whole-file rule are shared; steps 2 and 4
//! differ.
//!
//! * `WalBased` (default, native's): scan every owner batch in version order,
//!   decide per record with the filter, and copy only the latest live entry
//!   per key below each stream's target.
//! * `IndexBased(t)`: walk the coordinator index at `Q`, decide per live
//!   entry, copy each survivor below its own stream's target, and remove the
//!   entries the filter marks removed (native lets WAL GC make those frames
//!   unreachable; a hybrid live read of a retired frame is the typed eviction
//!   error, so the entry itself must go).
//!
//! One pass:
//! 1. Freeze the scan cut `Q` = `Loader::last_processed()` (`min` of the
//!    Version B durable cut, the contiguous index-applied cut and any held
//!    checkpoint latch) and, per retained stream `(partition, fencing
//!    generation)`, the scan interval `[callback start, durable end)`.
//! 2. Page each stream's original batches from its owner (`RelocationPage`,
//!    metadata only). `WalBased` merges them in global
//!    `(generation, batch sequence, member)` order with one page and one head
//!    per stream. Every authenticated put of a filtered keyspace is read
//!    through the ordinary position read and passed to the filter once,
//!    including puts the index no longer holds. The first `Stop` ends all
//!    callbacks, later members of its batch included. `Keep` is not a copy
//!    decision: like native at the parity tip, a filtered keyspace copies
//!    nothing. `IndexBased` reads no value here and calls no filter: the pages
//!    only name each stream's first unprocessed byte, which bounds every
//!    callback start.
//! 3. Per stream: the scan stops at the first batch at or above `Q` (cuts are
//!    exclusive, so that batch is not read) and otherwise at the durable end.
//!    Terminal = the `Stop` batch, the first batch after `Stop` (other
//!    streams), the durable end, or (`IndexBased`) the configured byte limit
//!    min the first unprocessed byte. This fork's D27 rule gives the hybrid
//!    that end even without a `Stop`: its leased scan completes only adopted
//!    groups, so no torn frame lies below the end it reports. Native falls
//!    back to its upper limit only when the scan ends exactly there on
//!    `EndOfWal` or a zeroed header; an end below that limit, or a torn frame
//!    at it, leaves native's terminal at 0. The target is the native rule
//!    `round_down_to_file(min(terminal, lo + budget))` with the per-owner
//!    `relocation_max_reclaim_pct` budget. Without one complete file before a
//!    target the pass is `NoWork`: no copy, no start, no snapshot. It still
//!    runs retirement when a stream has a non-zero whole-file target, as
//!    native re-runs its gc against a target that did not advance.
//! 4. `DidWork`: `WalBased` drops a filtered keyspace's entries below its
//!    stream's TERMINAL unread, as native's cutoff
//!    (`relocation/mod.rs` `relocate_stripe`, `RelocationFlush::Cutoff`) does;
//!    entries from the target to the terminal go with them. Every other
//!    keyspace copies each live entry below its own stream's target to the
//!    serving generation at the head, then re-points it by compare-and-set on
//!    its exact expected position (never `insert`: equal order would be
//!    dropped, I4b). A copy keeps the logical version and operation index of
//!    the position it re-points and gets a new address and payload CRC, so the
//!    winner never changes by version. `IndexBased` walks the index cells
//!    instead (L0 merged over the overlay, a promoted L1 blob loaded on
//!    demand), skips entries at or above `Q` (`offset >= effective_limit` in
//!    native), copies `Keep` and removes `Remove` and unreachable entries. Its
//!    `Stop` ends the current cell's walk, as native's loop does, and caps that
//!    stream's target at the file below the lowest skipped byte so no start
//!    ever passes a value the filter told the pass to keep.
//! 5. Make it durable: one snapshot stores the re-pointed index and the
//!    control region (per-stream `replay_from`), one manifest replacement
//!    raises the callback starts monotonically, then retirement runs under the
//!    advanced watermark. Bytes below both terms hold no live value, so no
//!    `Keep` survivor is ever unreachable and no reference walk is needed.
//!
//! A `NoWork` pass copies nothing, stores nothing and moves no start; only its
//! retirement retry can delete retired files. Offsets of different streams are
//! never compared: every target and every watermark term is per `(partition,
//! generation)`, because independent owner WALs have no shared byte order.

use super::concurrent::RelocatedCopies;
use super::dead_wal_files::core_retire_dead_wal_files;
use super::file_inventory::{OwnerSpan, span_budget_end};
use super::*;
use crate::cell::CellId;
use crate::distributed::hybrid::RelocationApply;
use crate::distributed::hybrid::StoredSnapshot;
use crate::distributed::hybrid::position::VersionPosition;
use crate::distributed::storage::{RelocationBatch, RelocationPage};
use crate::relocation::RelocationStrategy;
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

/// Whether `version` is at or above the scan cut `q`. Cuts are exclusive, so
/// the batch at `q` is the first unprocessed one and the scan stops there, as
/// native's `position.offset() >= upper_limit` test does.
fn at_or_above_cut(version: BatchVersion, q: BatchVersion) -> bool {
    version >= q
}

/// One value copy for the serving generation: full key, value and the logical
/// version the copy re-points.
type RelocatedValue = (Bytes, Bytes, BatchVersion);

/// Streams a pass may no longer target, keyed by `(partition, generation)`: a
/// survivor the pass could not verify or remove keeps its bytes.
type BlockedStreams = BTreeSet<(u32, u64)>;

/// The lowest byte a `Stop` skipped, per `(partition, generation)` stream: the
/// stream's target may not pass it.
type StopHolds = BTreeMap<(u32, u64), u64>;

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

/// Whether a shadowed as-of position still needs a retirement cap.
///
/// `shadowing` is the cell entry that stopped the re-point, so a read cut at or
/// below that version still resolves the old position. A held latch at or below
/// it therefore needs the file; a latch above it resolves the shadowing entry or
/// a newer one. With no latch, only a shadowing version the published cut does
/// not cover can still be named by a latch created later: `published` is read
/// before `lowest_held` (the SeqCst Dekker pair at `CutLatches::publish_min` and
/// `min_held`, loader.rs), so a racing pin either appears in the latch read or
/// samples a cut at or above the published one. The pass's store also waits for
/// the publisher critical section that inserted the shadowing entry
/// (`publish_durable` advances the published front in that section and
/// `WriteCore::older_streams` takes the same lock), so the uncovered branch is
/// reachable only after a publication failure. Paper 4.4: the GC watermark
/// tracks the oldest position that may still be referenced.
///
/// `dead_wal_files::RetirementGate::caps` re-evaluates the retained witness with
/// this predicate on every retirement, not only on the pass that saw the
/// shadow.
pub(super) fn shadow_needs_cap(
    held: Option<BatchVersion>,
    published: BatchVersion,
    shadowing: BatchVersion,
) -> bool {
    match held {
        Some(held) => cut_at_or_below(held, shadowing),
        None => !published_covers(published, shadowing),
    }
}

/// A cut is exclusive: a read at `cut` resolves versions below it, so it
/// resolves `version`'s predecessor exactly when `cut` is at or below `version`.
/// Epochs are fencing generations; an older epoch is entirely below a newer one.
fn cut_at_or_below(cut: BatchVersion, version: BatchVersion) -> bool {
    if cut.epoch != version.epoch {
        return cut.epoch < version.epoch;
    }
    cut.sequence <= version.sequence
}

/// Whether the published cut `published` (the next sequence to publish) covers
/// `version`: every batch at or below it is published, so a latch taken later
/// has a cut above `version` and resolves it or a newer entry.
fn published_covers(published: BatchVersion, version: BatchVersion) -> bool {
    if published.epoch != version.epoch {
        return published.epoch > version.epoch;
    }
    published.sequence > version.sequence
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

    /// Run one relocation pass with the strategy the configuration names.
    /// `observe` records every callback with its key and value (test
    /// observation only; production keeps none). `cancel` is checked between
    /// merged batches and owner polls, before any copy.
    pub(crate) fn relocation_pass(
        &self,
        observe: bool,
        cancel: &AtomicBool,
    ) -> Result<RelocationReport, LocalDbError> {
        self.ensure_healthy()?;
        match self.write_core().config.index.relocation_strategy {
            RelocationStrategy::WalBased => self.wal_based_relocation_pass(observe, cancel),
            RelocationStrategy::IndexBased(target) => {
                self.index_based_relocation_pass(target, observe, cancel)
            }
        }
    }

    /// Native's WAL-based pass: scan each owner stream in version order, stop
    /// at the first `StopRelocation`, copy every live entry below its own
    /// stream's whole-file target, store, adopt the starts and retire.
    fn wal_based_relocation_pass(
        &self,
        observe: bool,
        cancel: &AtomicBool,
    ) -> Result<RelocationReport, LocalDbError> {
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
        let targets = whole_file_targets(&reports, file, core.epoch);
        if targets.is_empty() {
            // Native runs its GC with the whole-file target on every run that
            // has one, including a target that did not advance; the hybrid
            // retries the retirement here under the already adopted watermark.
            // Nothing is copied, dropped or stored.
            if reports.iter().any(|stream| stream.rounded_target > 0) {
                let mut gate = core.retirement.lock();
                core_retire_dead_wal_files(core, &mut gate)?;
            }
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
        let mut shadowed: Vec<(VersionPosition, BatchVersion)> = Vec::new();
        let (copied, rejected, blocked) =
            self.relocate_cells(q, &targets, &drop_targets, cancel, &mut shadowed)?;
        self.finish_relocation_pass(
            q, reports, callbacks, copied, rejected, targets, &blocked, shadowed,
        )
    }

    /// The durable ladder every pass shares, whichever strategy decided what
    /// to copy: merge the shadow witness, store a region that covers the scan
    /// cut, adopt the file-aligned callback starts, then retire. A pass that
    /// cannot prove a stored region at its cut adopts nothing and retires
    /// nothing.
    #[allow(
        clippy::too_many_arguments,
        reason = "one call site per strategy; the arguments are that pass's own report fields"
    )]
    fn finish_relocation_pass(
        &self,
        q: BatchVersion,
        reports: Vec<StreamReport>,
        callbacks: Vec<CallbackRecord>,
        copied: u64,
        rejected: u64,
        mut targets: Vec<(u32, u64, u64)>,
        blocked: &BTreeSet<(u32, u64)>,
        shadowed: Vec<(VersionPosition, BatchVersion)>,
    ) -> Result<RelocationReport, LocalDbError> {
        let core = self.write_core();
        // A survivor this pass could not verify or copy keeps its stream's
        // start where it was: advancing it would let retirement reclaim bytes
        // that still hold a live value. The copy of that key is not applied.
        targets.retain(|&(partition, generation, _)| !blocked.contains(&(partition, generation)));
        // This pass's shadowed positions join the retirement witness before any
        // early return below: a pass whose store is not usable retires nothing,
        // but the witness it produced still constrains the next pass that does
        // retire, and the next pass may not target this stream at all.
        if !shadowed.is_empty() {
            core.retirement.lock().merge(&shadowed);
        }
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
        // Retirement under the advanced watermark. The shadow witness kept in
        // `core.retirement` (merged above and on every earlier pass) caps each
        // stream's watermark inside `take_snapshot`, whatever this pass
        // targeted; see `RetirementGate` and `shadow_needs_cap` for the proof
        // and the publication-floor/latch read order.
        {
            let mut gate = core.retirement.lock();
            core_retire_dead_wal_files(core, &mut gate)?;
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

    /// Native's index-based pass (`relocation/mod.rs`
    /// `index_based_relocation`): walk the coordinator index at the frozen
    /// scan cut `q`, copy every live value below its own stream's whole-file
    /// target to the serving generation, remove the entries its filter marks
    /// removed, then store, adopt the starts and retire.
    ///
    /// Recorded differences from native (D33 register):
    /// * `IndexBased(t)` is a per-stream byte limit. Owner WAL offsets have no
    ///   shared order, so one number applies to each stream's own offsets, as
    ///   `relocation_max_reclaim_pct` already does; one owner reduces to
    ///   native's single WAL. `None` processes every live entry below `q`.
    /// * The pass copies only what a whole-file target can reclaim, so a limit
    ///   below one owner file is `NoWork` and copies nothing. Native copies
    ///   below the raw limit and reclaims nothing; no read observes the
    ///   difference.
    /// * `StopRelocation` ends the current cell's walk, as native's loop does,
    ///   but no start advances past a skipped entry: that stream's target is
    ///   capped at the file below the lowest skipped byte. Native's single GC
    ///   watermark can reclaim bytes the filter told it to keep.
    fn index_based_relocation_pass(
        &self,
        target: Option<u64>,
        observe: bool,
        cancel: &AtomicBool,
    ) -> Result<RelocationReport, LocalDbError> {
        let core = self.write_core();
        let file = core.config.storage.wal_file_bytes;
        let pct = core.config.index.relocation_max_reclaim_pct;
        let (cut, streams) = self.freeze_relocation_scan()?;
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
        let mut clients = self.relocation_clients()?;
        let mut reports: Vec<StreamReport> = Vec::with_capacity(streams.len());
        for scan in &streams {
            let (lo, processed_end) =
                self.stream_scan_bound(&mut clients[scan.node], scan, q, cancel)?;
            // Every batch of a stream past its durable end reports no boundary,
            // which is the durable end itself.
            let processed_end = processed_end.unwrap_or(scan.required_end);
            reports.push(StreamReport {
                partition: scan.partition,
                fencing_generation: scan.generation,
                lo,
                processed_upper: processed_end,
                stream_end: scan.required_end,
                // Native's `effective_limit = min(target, last_processed)`; the
                // stream's own first unprocessed byte is the second term.
                terminal_position: target.map_or(processed_end, |limit| limit.min(processed_end)),
                rounded_target: 0,
            });
        }
        finalize_stream_targets(&mut reports, file, pct, core.epoch)?;
        let mut targets = whole_file_targets(&reports, file, core.epoch);
        if targets.is_empty() {
            // Native re-runs its GC against a target that did not advance; the
            // hybrid retries the retirement under the already adopted
            // watermark, and nothing is copied, dropped or stored.
            if reports.iter().any(|stream| stream.rounded_target > 0) {
                let mut gate = core.retirement.lock();
                core_retire_dead_wal_files(core, &mut gate)?;
            }
            return Ok(empty(reports, Vec::new(), q));
        }
        let mut callbacks = Vec::new();
        let mut shadowed: Vec<(VersionPosition, BatchVersion)> = Vec::new();
        let mut rejected = 0u64;
        let (copied, blocked, holds) = self.relocate_index_cells(
            q,
            &targets,
            observe,
            &mut callbacks,
            cancel,
            &mut rejected,
            &mut shadowed,
        )?;
        if !holds.is_empty() {
            // A cell's `Stop` skipped entries the pass did not copy. Their
            // bytes must stay: cap each stream's target at the file below the
            // lowest skipped byte and re-derive the whole-file gate.
            for stream in &mut reports {
                if let Some(&hold) = holds.get(&(stream.partition, stream.fencing_generation)) {
                    stream.terminal_position = stream.terminal_position.min(hold);
                }
            }
            finalize_stream_targets(&mut reports, file, pct, core.epoch)?;
            targets = whole_file_targets(&reports, file, core.epoch);
        }
        self.finish_relocation_pass(
            q, reports, callbacks, copied, rejected, targets, &blocked, shadowed,
        )
    }

    /// `lo` (the stream's oldest unretired file start) and the offset of the
    /// first batch at or above `q`, from metadata pages only. No filter runs
    /// and no value is read. `None` means every batch of the stream is below
    /// `q`, so its processed end is its durable end.
    fn stream_scan_bound(
        &self,
        client: &mut Client,
        scan: &StreamScan,
        q: BatchVersion,
        cancel: &AtomicBool,
    ) -> Result<(u64, Option<u64>), LocalDbError> {
        let file = self.config.storage.wal_file_bytes;
        let mut from = Some(scan.from);
        let mut start = None;
        while let Some(cursor) = from {
            if cancel.load(Ordering::Relaxed) {
                return Err(cancelled());
            }
            let page = self.fetch_relocation_page(client, scan, cursor, cancel)?;
            let page_start = *start.get_or_insert(page.start);
            if let Some(batch) = page
                .batches
                .iter()
                .find(|batch| at_or_above_cut(batch.version, q))
            {
                return Ok((page_start - page_start % file, Some(batch.address.offset)));
            }
            from = page.next;
        }
        let start = start.unwrap_or(scan.from);
        Ok((start - start % file, None))
    }

    /// Walk every cell of every live keyspace at the scan cut `q` and copy,
    /// remove and observe. Returns the survivors re-pointed, the streams a
    /// rejection blocked, and the lowest skipped byte per stream (a `Stop`
    /// ended that cell's walk).
    #[allow(
        clippy::too_many_arguments,
        reason = "one internal call site; the accumulators are the pass's own outputs"
    )]
    fn relocate_index_cells(
        &self,
        q: BatchVersion,
        targets: &[(u32, u64, u64)],
        observe: bool,
        callbacks: &mut Vec<CallbackRecord>,
        cancel: &AtomicBool,
        rejected: &mut u64,
        shadowed: &mut Vec<(VersionPosition, BatchVersion)>,
    ) -> Result<(u64, BlockedStreams, StopHolds), LocalDbError> {
        let mut copied = 0u64;
        let mut blocked = BlockedStreams::new();
        let mut holds = StopHolds::new();
        for descriptor in self.shape.iter_ks() {
            let keyspace = descriptor.id();
            if descriptor.destroyed() {
                continue;
            }
            let mut cell = descriptor.first_cell();
            loop {
                if cancel.load(Ordering::Relaxed) {
                    return Err(cancelled());
                }
                copied += self.relocate_index_cell(
                    keyspace,
                    descriptor,
                    &cell,
                    q,
                    targets,
                    observe,
                    callbacks,
                    cancel,
                    rejected,
                    &mut blocked,
                    &mut holds,
                    shadowed,
                )?;
                let Some(next) = self.write_core().table.next_cell(descriptor, &cell, false) else {
                    break;
                };
                cell = next;
            }
        }
        Ok((copied, blocked, holds))
    }

    /// One cell: copy its live survivors in bounded chunks, remove the entries
    /// the filter or an unreachable frame marks removed, and record a `Stop`'s
    /// skipped bytes.
    #[allow(
        clippy::too_many_arguments,
        reason = "one internal call site; the accumulators are the pass's own outputs"
    )]
    fn relocate_index_cell(
        &self,
        keyspace: KeySpace,
        descriptor: &crate::key_shape::KeySpaceDesc,
        cell: &CellId,
        q: BatchVersion,
        targets: &[(u32, u64, u64)],
        observe: bool,
        callbacks: &mut Vec<CallbackRecord>,
        cancel: &AtomicBool,
        rejected: &mut u64,
        blocked: &mut BlockedStreams,
        holds: &mut StopHolds,
        shadowed: &mut Vec<(VersionPosition, BatchVersion)>,
    ) -> Result<u64, LocalDbError> {
        let (view, latch) = self.write_core().table.relocation_view(keyspace, cell, q)?;
        let limit = self.relocation_batch_max_bytes();
        let mut chunk: Vec<RelocatedValue> = Vec::new();
        let mut expected: Vec<(Bytes, VersionPosition)> = Vec::new();
        let mut removals: Vec<(Bytes, VersionPosition)> = Vec::new();
        let mut bytes = 0usize;
        let mut copied = 0u64;
        let mut stopped = false;
        for (reduced_key, position) in view.iter() {
            if cancel.load(Ordering::Relaxed) {
                return Err(cancelled());
            }
            // A promoted level can expose an entry the cut has not processed;
            // native skips it (`offset >= effective_limit`), and its version is
            // ours to leave alone.
            if at_or_above_cut(position.batch, q) {
                continue;
            }
            let stream = (
                position.address.partition,
                position.address.segment_generation,
            );
            if stopped {
                // Native ends the cell's walk here. This pass ends it too, but
                // it must not then reclaim a byte the filter told it to keep.
                let hold = holds.entry(stream).or_insert(position.address.offset);
                *hold = (*hold).min(position.address.offset);
                continue;
            }
            if !below_target(&position, targets) || blocked.contains(&stream) {
                continue;
            }
            let value = match self
                .value_reader()
                .read_indexed(descriptor, &reduced_key, position)
            {
                Ok(value) => value,
                Err(_) => {
                    *rejected += 1;
                    blocked.insert(stream);
                    continue;
                }
            };
            let Some((key, value)) = value else {
                // An unreachable frame: native marks the entry removed.
                removals.push((reduced_key, position));
                continue;
            };
            let decision = match descriptor.relocation_filter() {
                Some(filter) => match filter(&key, &value) {
                    crate::Decision::Keep => CallbackDecision::Keep,
                    crate::Decision::Remove => CallbackDecision::Remove,
                    crate::Decision::StopRelocation => CallbackDecision::Stop,
                },
                None => CallbackDecision::Keep,
            };
            if observe {
                callbacks.push(CallbackRecord {
                    keyspace,
                    key: key.clone(),
                    value: value.clone(),
                    wal_position: position.address.offset,
                    decision,
                });
            }
            match decision {
                CallbackDecision::Keep => {
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
                            shadowed,
                        )?;
                        bytes = 0;
                    }
                }
                CallbackDecision::Remove => removals.push((reduced_key, position)),
                CallbackDecision::Stop => {
                    // The Stop entry itself is not copied (native breaks before
                    // it), so its bytes stay too.
                    let hold = holds.entry(stream).or_insert(position.address.offset);
                    *hold = (*hold).min(position.address.offset);
                    stopped = true;
                }
            }
        }
        copied += self.flush_relocated(
            keyspace,
            cell,
            &mut chunk,
            &mut expected,
            targets,
            rejected,
            blocked,
            shadowed,
        )?;
        if !removals.is_empty() {
            let live: Vec<(u32, u64, u64)> = targets
                .iter()
                .copied()
                .filter(|&(partition, generation, _)| !blocked.contains(&(partition, generation)))
                .collect();
            let not_applied = self.write_core().table.relocation_apply(
                keyspace,
                cell,
                RelocationApply::Remove(&removals),
                &live,
            )?;
            shadowed.extend(not_applied);
        }
        drop(latch);
        Ok(copied)
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
        shadowed: &mut Vec<(VersionPosition, BatchVersion)>,
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
                    // A drop names no re-point, so it produces no shadowed
                    // expectation.
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
                        shadowed,
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
    /// chunk before the next. The view holds a cut latch, so promotion cannot pass
    /// `Q` while its positions are still being copied (I6).
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
        shadowed: &mut Vec<(VersionPosition, BatchVersion)>,
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
                    shadowed,
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
            shadowed,
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
        shadowed: &mut Vec<(VersionPosition, BatchVersion)>,
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
        let not_applied = self.write_core().table.relocation_apply(
            keyspace,
            cell,
            RelocationApply::Repoint(&updates),
            &live,
        )?;
        // A position whose re-point did not land is still named by a read cut at
        // or below the entry that shadowed it; `relocation_pass` turns the
        // lowest of these per stream into a retirement cap.
        shadowed.extend(not_applied);
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

    /// One metadata page of `scan`'s stream from `from`, after waiting for the
    /// owner to adopt the scan's required end. The wait is bounded by the
    /// configured timeout; `cancel` ends it early.
    fn fetch_relocation_page(
        &self,
        client: &mut Client,
        scan: &StreamScan,
        from: u64,
        cancel: &AtomicBool,
    ) -> Result<RelocationPage, LocalDbError> {
        let deadline = Instant::now() + self.config.timeout;
        loop {
            if cancel.load(Ordering::Relaxed) {
                return Err(cancelled());
            }
            let page = client
                .relocation_page(
                    self.epoch,
                    scan.generation,
                    from,
                    scan.required_end,
                    PAGE_BATCHES,
                )
                .map_err(|error| {
                    LocalDbError::new(format!(
                        "relocation page of partition {} generation {} failed: {error}",
                        scan.partition, scan.generation
                    ))
                })?;
            if page.available_end >= scan.required_end {
                return Ok(page);
            }
            if Instant::now() >= deadline {
                return Err(LocalDbError::new(
                    "relocation scan: owner adoption stays below the published cut",
                ));
            }
            std::thread::sleep(BEHIND_POLL);
        }
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
        while cursor.buffer.is_empty() && cursor.upper.is_none() {
            if cancel.load(Ordering::Relaxed) {
                return Err(cancelled());
            }
            let Some(from) = cursor.next else {
                cursor.upper = Some(cursor.scan.required_end);
                break;
            };
            let page = self.fetch_relocation_page(client, &cursor.scan, from, cancel)?;
            cursor.start.get_or_insert(page.start);
            for batch in page.batches {
                if at_or_above_cut(batch.version, q) {
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

/// One exclusive byte end per stream below which its files may be reclaimed:
/// the native whole-file target. A sealed stream only needs progress past its
/// oldest live byte (its last file may be partial); a serving stream needs one
/// whole completed file before its bytes move.
fn whole_file_targets(reports: &[StreamReport], file: u64, serving: u64) -> Vec<(u32, u64, u64)> {
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
            .find(|stream| stream.partition == partition && stream.fencing_generation == generation)
            .map_or(0, |stream| stream.lo);
        if generation < serving {
            target > lo
        } else {
            target >= lo.saturating_add(file)
        }
    });
    targets
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
            // A stream that reached its durable end without a Stop ends there:
            // the leased scan only completes adopted groups, so no torn frame
            // lies below. This is the fork's D27 rule: the hybrid reclaims up
            // to the durable end, where native takes its frontier only when
            // the scan ends exactly there.
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
    finalize_stream_targets(&mut reports, file, pct, serving)?;
    Ok(reports)
}

/// Native's whole-file rounding and the per-owner percentage budget, applied
/// to each stream's `terminal_position`. Shared by the WAL-based pass (the
/// terminal is the scan's) and the index-based pass (the terminal is the
/// configured byte limit bounded by the stream's processed end), so both
/// propose file-aligned callback starts by one rule.
fn finalize_stream_targets(
    reports: &mut [StreamReport],
    file: u64,
    pct: u8,
    serving: u64,
) -> Result<(), LocalDbError> {
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
    Ok(())
}

#[cfg(test)]
#[path = "relocation_unit_tests.rs"]
mod unit_tests;

#[cfg(test)]
#[path = "relocation_shadow_tests.rs"]
mod shadow_tests;

#[cfg(test)]
#[path = "relocation_promoted_l1_tests.rs"]
mod promoted_l1_tests;

#[cfg(test)]
#[path = "relocation_witness_tests.rs"]
mod witness_tests;
