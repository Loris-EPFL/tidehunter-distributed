//! Option C per-cell capture, read sources and installation (checkpoint option
//! C spec §5, §6.2, §9). Exact mode never reaches this state: a database uses
//! one checkpoint protocol, and the writer refuses to mix them.
//!
//! # Read sources
//!
//! Besides its immutable base a cell has the active overlay (new publications),
//! an optional immutable carry (published references a completed flush left
//! above its target) and at most one [`CellFlight`] with the captured carry and
//! the captured active overlay. Readers merge every present source under the
//! cell's shard lock by logical version, so a flight is invisible to them:
//! during it the cell has at most four overlay sources, never a list of
//! sealed generations.
//!
//! # Capture, work, install
//!
//! 1. A flusher reserves a [`CellFlushPermit`] (transient bytes, one bounded
//!    flight slot and a preallocated flight envelope) outside every serving
//!    lock.
//! 2. [`IndexWriter::capture_cell`] runs under writer -> (option-D publisher,
//!    held by the caller) -> one shard. It either advances coverage (no
//!    reference through the target is missing) or swaps the active overlay
//!    into the envelope and moves the carry into the flight: O(1), no history
//!    copy and no allocation.
//! 3. [`CellCapture::prepare`] copies the captured references above the target
//!    into a residual overlay, outside every lock, within the permit's bytes.
//! 4. Persistence (later stages) encodes [`PreparedCell::encode_source`] and
//!    produces a [`NewCellBase`]. [`PreparedCell::finish`] builds the result.
//! 5. [`IndexWriter::install_cell`] checks the flight token, then swaps base,
//!    carry and flight under the cell's shard lock in one critical section and
//!    adjusts accounting from scalar totals. Replaced holders are returned in
//!    [`CellRetired`], dropped by the caller after it releases the guards.
//!
//! A flush changes the storage location of references, never visibility: the
//! publication watermark is untouched, and the new base holds exactly the
//! captured references through the target that the cell drops from its
//! overlays.
//!
//! # Crash consistency
//!
//! Nothing here writes, syncs or deletes a file, and nothing returns replay or
//! identity credit. Coverage `D_i` is in-memory metadata; a crash loses it and
//! recovery rebuilds the index from the adopted root plus the retained Value
//! WAL suffix. A new base is installed only after the caller made its bytes
//! durable (later stages), and installing it never authorizes WAL deletion.
//!
//! # Lock order
//!
//! `writer` -> (publisher, held by the caller) -> one shard -> retention `cold`
//! (permit growth only). Permits are reserved and released with `cold` alone
//! or below these guards; nothing here waits while holding a guard.
#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "option C per-cell flush API, wired by the flusher and snapshot stages"
    )
)]

use super::shards::{CellKey, add, sub};
use super::*;
use std::sync::Arc;
use std::sync::atomic::Ordering as AtomicOrdering;
use std::time::Instant;

type References = Overlay<ValueRecordReference>;

/// Outstanding permits per flusher (spec §6.2: at most 32 active group results
/// per worker). With `P` flushers at most `32 P` captures exist at once.
pub(crate) const CELL_FLIGHTS_PER_FLUSHER: usize = 32;

/// Most cell descriptors captured under one writer hold (spec §7.2 step 2).
pub(crate) const ROOT_DESCRIPTOR_SLICE: usize = 32;

/// Exact scalar totals of one overlay source, maintained on insert so capture
/// and installation never rescan histories.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct SourceTotals {
    pub versions: usize,
    /// Rows the source added; an upper bound on its distinct keys once two
    /// sources are combined.
    pub rows: usize,
    /// Index accounting charge: `VERSION_CHARGE + key.len()` per version.
    pub charge: usize,
    /// Encoded-contribution upper bound (spec §3.1 `overlay_upper`):
    /// `4 + 8 + key.len()` per row plus `REFERENCE_BYTES` per version.
    pub upper: usize,
    /// Smallest batch version in the source: the earliest unrepresented
    /// published batch.
    pub min: Option<BatchVersion>,
}

impl SourceTotals {
    fn record(&mut self, key_len: usize, batch: BatchVersion, new_row: bool) {
        self.versions += 1;
        self.charge += VERSION_CHARGE + key_len;
        self.upper = self.upper.saturating_add(REFERENCE_BYTES);
        if new_row {
            self.rows += 1;
            self.upper = self.upper.saturating_add(12 + key_len);
        }
        self.min = Some(self.min.map_or(batch, |min| min.min(batch)));
    }

    fn combine(self, other: Self) -> Self {
        Self {
            versions: self.versions + other.versions,
            rows: self.rows + other.rows,
            charge: self.charge + other.charge,
            upper: self.upper.saturating_add(other.upper),
            min: match (self.min, other.min) {
                (None, min) | (min, None) => min,
                (Some(a), Some(b)) => Some(a.min(b)),
            },
        }
    }
}

/// The one in-progress flush of a cell. Lives in the permit's preallocated
/// envelope, so capture allocates nothing under the serving locks.
pub(super) struct CellFlight {
    id: u64,
    lifecycle: u64,
    base_token: u64,
    target: PrefixCut,
    carry: Option<Arc<References>>,
    active: Arc<References>,
    totals: SourceTotals,
    dirty_since: Option<Instant>,
}

impl CellFlight {
    fn envelope() -> Self {
        Self {
            id: 0,
            lifecycle: 0,
            base_token: 0,
            target: PrefixCut::BeforeAll,
            carry: None,
            active: Arc::new(References::new()),
            totals: SourceTotals::default(),
            dirty_since: None,
        }
    }
}

/// Per-cell option C metadata, held in the cell under its shard lock. In
/// Exact mode only the active totals change.
pub(super) struct CellFlushState {
    carry: Option<Arc<References>>,
    flight: Option<Box<CellFlight>>,
    active: SourceTotals,
    carry_totals: SourceTotals,
    /// `D_i`: the base holds every retained reference of this cell through
    /// this complete prefix.
    coverage: PrefixCut,
    /// Changes on every base replacement by [`IndexWriter::install_cell`].
    base_token: u64,
    /// `H_i` of a base installed here; a base that was never replaced here
    /// reports the floor of its legacy container.
    base_floor: Option<MutationVersion>,
    /// When the oldest unrepresented published reference arrived.
    dirty_since: Option<Instant>,
    /// The scheduler holds a queued descriptor for this cell.
    queued: bool,
}

impl Default for CellFlushState {
    fn default() -> Self {
        Self::new(PrefixCut::BeforeAll)
    }
}

impl CellFlushState {
    /// A cell first inserted now has no mutation at or below any complete
    /// prefix already published, so `coverage` (the adopted replay floor)
    /// is safe.
    pub(super) fn new(coverage: PrefixCut) -> Self {
        Self {
            carry: None,
            flight: None,
            active: SourceTotals::default(),
            carry_totals: SourceTotals::default(),
            coverage,
            base_token: 0,
            base_floor: None,
            dirty_since: None,
            queued: false,
        }
    }

    /// Frozen read sources: carry, captured carry, captured active.
    pub(super) fn sources(&self) -> [Option<&References>; 3] {
        let flight = self.flight.as_deref();
        [
            self.carry.as_deref(),
            flight.and_then(|flight| flight.carry.as_deref()),
            flight.map(|flight| &*flight.active),
        ]
    }

    /// Record one reference promoted into the active overlay.
    pub(super) fn record_active(&mut self, key_len: usize, batch: BatchVersion, new_row: bool) {
        self.active.record(key_len, batch, new_row);
        if self.dirty_since.is_none() {
            self.dirty_since = Some(Instant::now());
        }
    }

    /// Smallest batch version not represented by the base, over every source.
    fn oldest_uncovered(&self) -> Option<BatchVersion> {
        let flight = self.flight.as_ref().map(|flight| flight.totals);
        flight
            .into_iter()
            .chain([self.active, self.carry_totals])
            .fold(SourceTotals::default(), SourceTotals::combine)
            .min
    }

    /// Advance `D_i` to `target` when no reference through it is missing.
    fn advance_clean(&mut self, target: PrefixCut) {
        if self
            .oldest_uncovered()
            .is_none_or(|batch| !target.contains(batch))
        {
            self.coverage = self.coverage.max(target);
        }
    }
}

/// Writer-side per-cell flush state. Held under `NativeIndex::writer`.
pub(super) struct CellDirectory {
    /// Coverage of a newly inserted cell: the adopted replay floor `F`.
    pub floor: PrefixCut,
    lifecycle: u64,
    next_flight: u64,
    flights: usize,
    /// Set by the first capture; Exact capture refuses afterwards.
    pub active: bool,
}

impl Default for CellDirectory {
    fn default() -> Self {
        Self {
            floor: PrefixCut::BeforeAll,
            lifecycle: 0,
            next_flight: 0,
            flights: 0,
            active: false,
        }
    }
}

/// Scheduler view of one cell (spec §5.1, §6.1). Taken under one shard read
/// lock.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CellDirty {
    pub coverage: PrefixCut,
    pub oldest_uncovered: Option<BatchVersion>,
    /// Unrepresented references outside a flight (active plus carry).
    pub versions: usize,
    pub upper: usize,
    pub dirty_since: Option<Instant>,
    pub in_flight: bool,
    pub queued: bool,
}

/// Bounded capture allowance of one flush: transient bytes for residual
/// copies, one flight slot, and the preallocated flight envelope. Reserve it
/// before any serving lock; dropping it returns everything.
pub(crate) struct CellFlushPermit {
    envelope: Option<Box<CellFlight>>,
    bytes: usize,
    retention: Arc<RetentionState>,
}

impl CellFlushPermit {
    pub(crate) fn bytes(&self) -> usize {
        self.bytes
    }

    fn belongs_to(&self, retention: &Arc<RetentionState>) -> bool {
        Arc::ptr_eq(&self.retention, retention)
    }

    /// Grow without waiting. Called under the writer; takes `cold` only.
    fn try_grow(&mut self, bytes: usize, limit: usize) -> bool {
        if bytes <= self.bytes {
            return true;
        }
        if !self.retention.grow_cell_flush(bytes - self.bytes, limit) {
            return false;
        }
        self.bytes = bytes;
        true
    }
}

impl Drop for CellFlushPermit {
    fn drop(&mut self) {
        self.retention.release_cell_flush(self.bytes);
    }
}

/// Why [`IndexWriter::capture_cell`] did not capture. The permit is returned.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CellSkip {
    /// The cell already has a flight.
    InFlight,
    /// Exact checkpoint state exists; the protocols are exclusive.
    ExactMode,
    /// The residual bound exceeds the permit, which could not grow now.
    NeedsPermit { bytes: usize },
}

// Returned once per flush attempt and moved, never stored in bulk. Boxing the
// capture would allocate under the writer lock.
#[expect(
    clippy::large_enum_variant,
    reason = "moved once per flush; boxing allocates under the writer"
)]
pub(crate) enum CellCaptureOutcome {
    Captured(CellCapture),
    /// No reference through the target was missing: coverage advanced without
    /// I/O. Later publications are recorded under the same serialization, so
    /// none can be omitted.
    Clean {
        coverage: PrefixCut,
        permit: CellFlushPermit,
    },
    Skipped {
        reason: CellSkip,
        permit: CellFlushPermit,
    },
}

/// Exclusive handle of one cell flight. The cell keeps its captured sources
/// readable until the result is installed or restored.
pub(crate) struct CellCapture {
    // Field order is drop order: the permit is released after the holders.
    base: Option<SortedBlock>,
    carry: Option<Arc<References>>,
    active: Arc<References>,
    identity: CellKey,
    flight: u64,
    lifecycle: u64,
    base_token: u64,
    target: PrefixCut,
    /// Committed history floor `H` at capture; the frame floor `H_i`.
    floor: IndexReadCut,
    totals: SourceTotals,
    permit: CellFlushPermit,
}

/// Inputs of one cell encoding: base plus captured carry and active, the
/// target selection and the history floor. The encoder must merge both
/// overlays by version (spec §3.2). Without a carry, the existing one-overlay
/// encoder applies as is to `FrozenCell { base, overlay: Arc::clone(active) }`
/// with no copy.
pub(crate) struct CellEncodeSource<'a> {
    pub(super) identity: &'a CellKey,
    pub(super) base: Option<&'a SortedBlock>,
    pub(super) carry: Option<&'a References>,
    pub(super) active: &'a Arc<References>,
    pub(super) cut: IndexReadCut,
    pub(super) floor: IndexReadCut,
}

impl CellCapture {
    pub(crate) fn identity(&self) -> &CellKey {
        &self.identity
    }

    pub(crate) fn target(&self) -> PrefixCut {
        self.target
    }

    pub(crate) fn totals(&self) -> SourceTotals {
        self.totals
    }

    /// Build the residual overlay of captured references above the target,
    /// outside every lock. Its charge fits the permit: it is a subset of the
    /// captured references, whose total charge the capture reserved.
    pub(crate) fn prepare(self) -> Result<PreparedCell, NativeIndexError> {
        let target = self.target;
        let (residual, residual_totals) = copy_references(
            [self.carry.as_deref(), Some(&*self.active)],
            |reference| !target.contains(reference.batch_version),
            self.permit.bytes,
        )?;
        Ok(PreparedCell {
            capture: self,
            residual,
            residual_totals,
        })
    }

    /// Return every captured reference to the cell as its carry, base
    /// unchanged. Used after a stale result or a failed encoding. Without a
    /// captured carry the captured active overlay becomes the carry as is;
    /// otherwise both merge into one copy within the permit.
    pub(crate) fn restore(self) -> Result<CellFlushResult, NativeIndexError> {
        let (carry, totals) = if self.carry.is_none() {
            (Some(Arc::clone(&self.active)), self.totals)
        } else {
            copy_references(
                [self.carry.as_deref(), Some(&*self.active)],
                |_| true,
                self.permit.bytes,
            )?
        };
        Ok(CellFlushResult {
            capture: self,
            kind: ResultKind::Restore { carry, totals },
        })
    }
}

/// A capture with its residual. The residual coexists with the captured
/// sources until installation; the permit covers it.
pub(crate) struct PreparedCell {
    residual: Option<Arc<References>>,
    residual_totals: SourceTotals,
    capture: CellCapture,
}

impl PreparedCell {
    pub(crate) fn identity(&self) -> &CellKey {
        &self.capture.identity
    }

    pub(crate) fn target(&self) -> PrefixCut {
        self.capture.target
    }

    pub(crate) fn residual_totals(&self) -> SourceTotals {
        self.residual_totals
    }

    pub(crate) fn encode_source(&self) -> CellEncodeSource<'_> {
        CellEncodeSource {
            identity: &self.capture.identity,
            base: self.capture.base.as_ref(),
            carry: self.capture.carry.as_deref(),
            active: &self.capture.active,
            cut: self.capture.target.index_cut(),
            floor: self.capture.floor.clone(),
        }
    }

    /// Bind the durable encoding of [`Self::encode_source`].
    pub(crate) fn finish(self, base: NewCellBase) -> CellFlushResult {
        CellFlushResult {
            kind: ResultKind::Flushed {
                base: base.0,
                residual: self.residual,
                residual_totals: self.residual_totals,
            },
            capture: self.capture,
        }
    }

    /// Discard the residual and restore every captured reference.
    pub(crate) fn restore(self) -> Result<CellFlushResult, NativeIndexError> {
        drop(self.residual);
        self.capture.restore()
    }
}

/// The durable encoding of a capture: a validated frame selected at the
/// capture target, or no frame when no row is retained.
pub(crate) struct NewCellBase(Option<SortedBlock>);

impl NewCellBase {
    pub(crate) fn empty() -> Self {
        Self(None)
    }

    /// `block.cut` must be the capture target's index cut; install checks it.
    pub(super) fn frame(block: SortedBlock) -> Self {
        Self(Some(block))
    }
}

enum ResultKind {
    Flushed {
        base: Option<SortedBlock>,
        residual: Option<Arc<References>>,
        residual_totals: SourceTotals,
    },
    Restore {
        carry: Option<Arc<References>>,
        totals: SourceTotals,
    },
}

pub(crate) struct CellFlushResult {
    kind: ResultKind,
    capture: CellCapture,
}

impl CellFlushResult {
    pub(crate) fn identity(&self) -> &CellKey {
        &self.capture.identity
    }
}

/// Holders replaced by an installation. Drop after releasing every serving
/// guard: their memory is freed before the permit is returned.
pub(crate) struct CellRetired {
    _flight: Option<Box<CellFlight>>,
    _base: Option<SortedBlock>,
    _capture: CellCapture,
}

pub(crate) enum CellInstall {
    /// Installed. `coverage` is the cell's `D_i` afterwards.
    Installed {
        coverage: PrefixCut,
        retired: CellRetired,
    },
    /// The lifecycle changed after capture: the new base is not installed.
    /// Drop `discarded` outside the guards, then install
    /// [`PreparedCell::restore`].
    Stale {
        prepared: PreparedCell,
        discarded: NewCellBase,
    },
    /// Foreign, forged or inconsistent result. Nothing changed. This is an
    /// invariant violation: the caller applies the fail-stop rule.
    Rejected {
        error: NativeIndexError,
        result: CellFlushResult,
    },
}

/// One cell of a metadata root capture (spec §7.2 step 2): exported coverage
/// `C_i = min(D_i, T)` and a pin of the current base.
pub(crate) struct RootCellDescriptor {
    pub(super) identity: CellKey,
    pub(super) coverage: PrefixCut,
    pub(super) base: Option<RootCellFrame>,
}

pub(super) struct RootCellFrame {
    pub block: SortedBlock,
    /// `B_i`: no reference of the frame is newer.
    pub frame_cut: PrefixCut,
    /// `H_i`.
    pub frame_floor: Option<MutationVersion>,
}

impl RootCellDescriptor {
    pub(crate) fn identity(&self) -> &CellKey {
        &self.identity
    }

    pub(crate) fn coverage(&self) -> PrefixCut {
        self.coverage
    }

    pub(crate) fn frame_cut(&self) -> Option<PrefixCut> {
        self.base.as_ref().map(|base| base.frame_cut)
    }
}

/// Copy the references of `sources` selected by `keep` into one overlay,
/// merging rows by version. The sources hold distinct versions.
fn copy_references(
    sources: [Option<&References>; 2],
    keep: impl Fn(&ValueRecordReference) -> bool,
    limit: usize,
) -> Result<(Option<Arc<References>>, SourceTotals), NativeIndexError> {
    let mut output = References::new();
    let mut totals = SourceTotals::default();
    for source in sources.into_iter().flatten() {
        for (key, values) in source {
            // Versions ascend, so a version predicate monotone in the batch
            // keeps a suffix; `keep` is either that or always true.
            let kept = &values[values.partition_point(|value| !keep(value))..];
            if kept.is_empty() {
                continue;
            }
            let row = output.entry(key.clone()).or_default();
            let new_row = row.is_empty();
            for (n, reference) in kept.iter().enumerate() {
                if insert_unique_by(row, *reference, |a, b| {
                    a.mutation_version().cmp(&b.mutation_version())
                })
                .is_some()
                {
                    return Err(NativeIndexError::Invalid(
                        "captured cell sources share a version",
                    ));
                }
                totals.record(key.len(), reference.batch_version, new_row && n == 0);
            }
            if totals.charge > limit {
                return Err(NativeIndexError::Invalid(
                    "cell residual exceeds its permit",
                ));
            }
        }
    }
    Ok(if output.is_empty() {
        (None, totals)
    } else {
        (Some(Arc::new(output)), totals)
    })
}

fn frame_cut(cut: &IndexReadCut) -> Result<PrefixCut, NativeIndexError> {
    match cut {
        IndexReadCut::BeforeAll => Ok(PrefixCut::BeforeAll),
        // A base selects a published cut; every operation of its batch is
        // published, so the batch bounds every frame reference.
        IndexReadCut::At(version) => Ok(PrefixCut::Through(version.batch)),
        IndexReadCut::Latest | IndexReadCut::Sparse(_) => {
            Err(NativeIndexError::Invalid("cell frame cut is not a prefix"))
        }
    }
}

fn same_arc<T>(a: Option<&Arc<T>>, b: Option<&Arc<T>>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(a), Some(b)) => Arc::ptr_eq(a, b),
        _ => false,
    }
}

impl NativeIndex {
    /// Reserve a flush permit of `bytes` transient residual allowance and one
    /// flight slot. Never waits: a full allowance returns `Capacity`, and the
    /// caller waits outside every serving lock. The allowance (`M` bytes,
    /// `32 P` slots) is independent of the resident budget and of the sparse
    /// view budget, so a flush never consumes a publication's reservation.
    pub(crate) fn reserve_cell_flush(
        &self,
        bytes: usize,
    ) -> Result<CellFlushPermit, NativeIndexError> {
        self.retention.reserve_cell_flush(
            bytes,
            self.max_metadata_bytes,
            self.cell_flight_limit(),
        )?;
        Ok(CellFlushPermit {
            envelope: Some(Box::new(CellFlight::envelope())),
            bytes,
            retention: Arc::clone(&self.retention),
        })
    }

    pub(crate) fn cell_flight_limit(&self) -> usize {
        CELL_FLIGHTS_PER_FLUSHER * cell_flushers(self.shards.len())
    }

    /// The newest complete published prefix `L` the watermark proves: the
    /// newest batch of a dense watermark, or a sparse view's stable floor.
    pub(crate) fn published_prefix(&self) -> PrefixCut {
        match &**self.published.load() {
            IndexReadCut::At(upper) => PrefixCut::Through(upper.batch),
            IndexReadCut::Sparse(view) => view.floor.map_or(PrefixCut::BeforeAll, |floor| {
                PrefixCut::Through(floor.batch)
            }),
            IndexReadCut::BeforeAll | IndexReadCut::Latest => PrefixCut::BeforeAll,
        }
    }

    /// Batch-level check that every batch of `target` is published. The
    /// watermark compares batches, so the operation index is irrelevant.
    fn check_prefix_published(&self, target: PrefixCut) -> Result<(), NativeIndexError> {
        match target {
            PrefixCut::BeforeAll => Ok(()),
            PrefixCut::Through(batch) => self.check_published(&IndexReadCut::At(MutationVersion {
                batch,
                operation_index: 0,
            })),
        }
    }

    /// Scheduler metadata of one cell, under its shard read lock.
    pub(crate) fn cell_dirty(&self, keyspace: KeySpace, cell: &CellId) -> Option<CellDirty> {
        let identity = (keyspace, cell.clone());
        self.shard_for(&identity)
            .read()
            .get(&identity)
            .map(|cell| dirty(&cell.flush))
    }

    /// Scheduler metadata of every cell of one shard, under one read lock.
    /// `output` is cleared first; reserve it outside the lock.
    pub(crate) fn shard_dirty_cells(&self, shard: usize, output: &mut Vec<(CellKey, CellDirty)>) {
        output.clear();
        let Some(shard) = self.shards.get(shard) else {
            return;
        };
        output.extend(
            shard
                .read()
                .iter()
                .map(|(identity, cell)| (identity.clone(), dirty(&cell.flush))),
        );
    }

    /// Mark a queued descriptor for a cell. False when one is queued, a
    /// flight exists, or the cell is unknown: a cell has at most one queued
    /// descriptor or flight. Capture clears the mark.
    pub(crate) fn try_queue_cell(&self, keyspace: KeySpace, cell: &CellId) -> bool {
        let identity = (keyspace, cell.clone());
        let mut shard = self.shard_for(&identity).write();
        let Some(cell) = shard.get_mut(&identity) else {
            return false;
        };
        let queued = !cell.flush.queued && cell.flush.flight.is_none();
        cell.flush.queued |= queued;
        queued
    }

    /// Clear a queued mark whose descriptor was dropped before capture.
    pub(crate) fn unqueue_cell(&self, keyspace: KeySpace, cell: &CellId) {
        let identity = (keyspace, cell.clone());
        if let Some(cell) = self.shard_for(&identity).write().get_mut(&identity) {
            cell.flush.queued = false;
        }
    }

    /// True if any reference was promoted since the last call. A coalesced
    /// maintenance hint: the scheduler rereads cell metadata under shard locks
    /// for the facts. Acquire pairs with the Release store in promotion.
    pub(crate) fn take_cell_dirty_signal(&self) -> bool {
        self.cell_dirty.swap(false, AtomicOrdering::Acquire)
    }
}

fn dirty(state: &CellFlushState) -> CellDirty {
    let pending = state.active.combine(state.carry_totals);
    CellDirty {
        coverage: state.coverage,
        oldest_uncovered: state.oldest_uncovered(),
        versions: pending.versions,
        upper: pending.upper,
        dirty_since: state
            .flight
            .as_ref()
            .and_then(|flight| flight.dirty_since)
            .into_iter()
            .chain(state.dirty_since)
            .min(),
        in_flight: state.flight.is_some(),
        queued: state.queued,
    }
}

impl IndexWriter<'_> {
    /// Set the coverage of cells inserted from now on to the adopted replay
    /// floor `F`. Monotonic and at most the published prefix.
    pub(crate) fn set_cell_floor(&mut self, floor: PrefixCut) -> Result<(), NativeIndexError> {
        if floor < self.state.cells.floor {
            return Err(NativeIndexError::Invalid("cell coverage floor regressed"));
        }
        self.index.check_prefix_published(floor)?;
        self.state.cells.floor = floor;
        Ok(())
    }

    /// Start a new lifecycle generation. A flight captured before it can no
    /// longer install a new base; it restores its sources instead.
    pub(crate) fn advance_cell_lifecycle(&mut self) -> u64 {
        // Equality-only token; 2^64 lifecycle events cannot occur.
        self.state.cells.lifecycle = self.state.cells.lifecycle.wrapping_add(1);
        self.state.cells.lifecycle
    }

    /// Cells with a flight.
    pub(crate) fn cell_flights(&self) -> usize {
        self.state.cells.flights
    }

    /// Length of the append-only cell directory: the stable boundary of a
    /// metadata root capture.
    pub(crate) fn cell_directory_len(&self) -> usize {
        self.state.cell_order.len()
    }

    /// Capture one cell at `target`, a complete published prefix (normally
    /// [`NativeIndex::published_prefix`]). Call under the option-D publisher
    /// lock so publication is serialized; every committed reference through
    /// `target` is then promoted. O(1) under one shard lock: no history copy,
    /// no allocation beyond the cell identity and a base handle.
    pub(crate) fn capture_cell(
        &mut self,
        keyspace: KeySpace,
        cell: &CellId,
        target: PrefixCut,
        mut permit: CellFlushPermit,
    ) -> Result<CellCaptureOutcome, NativeIndexError> {
        let index = self.index;
        let state = &mut *self.state;
        if !permit.belongs_to(&index.retention) {
            return Err(NativeIndexError::Invalid("foreign cell flush permit"));
        }
        index.check_prefix_published(target)?;
        if state.generation != 0
            || state.sealed_count != 0
            || state.capture.is_some()
            || state.installation.is_some()
        {
            return Ok(CellCaptureOutcome::Skipped {
                reason: CellSkip::ExactMode,
                permit,
            });
        }
        let identity = (keyspace, cell.clone());
        let floor = index.retention.floor();
        let mut shard = index.shard_for(&identity).write();
        let source = shard
            .get_mut(&identity)
            .ok_or(NativeIndexError::Invalid("unknown index cell"))?;
        if source.flush.flight.is_some() {
            return Ok(CellCaptureOutcome::Skipped {
                reason: CellSkip::InFlight,
                permit,
            });
        }
        let totals = source.flush.active.combine(source.flush.carry_totals);
        if totals.min.is_none_or(|batch| !target.contains(batch)) {
            // The descriptor is consumed by a clean advance or a capture. A
            // skip returns it to the worker, so the mark stays.
            source.flush.queued = false;
            source.flush.coverage = source.flush.coverage.max(target);
            return Ok(CellCaptureOutcome::Clean {
                coverage: source.flush.coverage,
                permit,
            });
        }
        if !permit.try_grow(totals.charge, index.max_metadata_bytes) {
            return Ok(CellCaptureOutcome::Skipped {
                reason: CellSkip::NeedsPermit {
                    bytes: totals.charge,
                },
                permit,
            });
        }
        let mut flight = permit
            .envelope
            .take()
            .ok_or(NativeIndexError::Invalid("cell flush permit already used"))?;
        let envelope = Arc::get_mut(&mut flight.active)
            .filter(|envelope| envelope.is_empty())
            .ok_or(NativeIndexError::Invalid(
                "cell flush envelope is not empty",
            ))?;
        // O(1): the active overlay moves into the preallocated Arc and the
        // cell continues with the envelope's empty map.
        std::mem::swap(envelope, &mut source.overlay);
        source.flush.queued = false;
        state.cells.next_flight += 1;
        flight.id = state.cells.next_flight;
        flight.lifecycle = state.cells.lifecycle;
        flight.base_token = source.flush.base_token;
        flight.target = target;
        flight.carry = source.flush.carry.take();
        flight.totals = totals;
        flight.dirty_since = source.flush.dirty_since.take();
        source.flush.active = SourceTotals::default();
        source.flush.carry_totals = SourceTotals::default();
        let capture = CellCapture {
            base: source.base.clone(),
            carry: flight.carry.clone(),
            active: Arc::clone(&flight.active),
            identity,
            flight: flight.id,
            lifecycle: flight.lifecycle,
            base_token: flight.base_token,
            target,
            floor,
            totals,
            permit,
        };
        source.flush.flight = Some(flight);
        drop(shard);
        state.cells.flights += 1;
        state.cells.active = true;
        Ok(CellCaptureOutcome::Captured(capture))
    }

    /// Install a flush or restore result under the cell's shard lock in one
    /// critical section. The token (index, flight id, lifecycle, base token
    /// and captured source identities) must match the cell's flight. Readers
    /// see the cell entirely before or entirely after the swap.
    pub(crate) fn install_cell(&mut self, result: CellFlushResult) -> CellInstall {
        let index = self.index;
        let state = &mut *self.state;
        let capture = &result.capture;
        if !capture.permit.belongs_to(&index.retention) {
            return CellInstall::Rejected {
                error: NativeIndexError::Invalid("foreign cell flush result"),
                result,
            };
        }
        if let ResultKind::Flushed {
            base: Some(block), ..
        } = &result.kind
            && block.cut != capture.target.index_cut()
        {
            return CellInstall::Rejected {
                error: NativeIndexError::Invalid("cell frame cut differs from its target"),
                result,
            };
        }
        let mut shard = index.shard_for(&capture.identity).write();
        let Some(cell) = shard.get_mut(&capture.identity) else {
            drop(shard);
            return CellInstall::Rejected {
                error: NativeIndexError::Invalid("unknown index cell"),
                result,
            };
        };
        let matches = cell.flush.base_token == capture.base_token
            && cell.flush.flight.as_ref().is_some_and(|flight| {
                flight.id == capture.flight
                    && flight.lifecycle == capture.lifecycle
                    && flight.base_token == capture.base_token
                    && flight.target == capture.target
                    && Arc::ptr_eq(&flight.active, &capture.active)
                    && same_arc(flight.carry.as_ref(), capture.carry.as_ref())
            });
        if !matches {
            drop(shard);
            return CellInstall::Rejected {
                error: NativeIndexError::Invalid("cell flush token"),
                result,
            };
        }
        let accounting = &index.accounting;
        let CellFlushResult { kind, capture } = result;
        let (carry, carry_totals, base) = match kind {
            ResultKind::Flushed {
                base,
                residual,
                residual_totals,
            } => {
                if capture.lifecycle != state.cells.lifecycle {
                    drop(shard);
                    return CellInstall::Stale {
                        prepared: PreparedCell {
                            residual,
                            residual_totals,
                            capture,
                        },
                        discarded: NewCellBase(base),
                    };
                }
                (residual, residual_totals, Some(base))
            }
            ResultKind::Restore { carry, totals } => (carry, totals, None),
        };
        let flight = cell.flush.flight.take();
        // Remove the captured references and add the carry: a subset of them
        // (residual) or all of them (restore). Scalar totals; no rescan.
        sub(&accounting.overlay, capture.totals.charge);
        sub(&accounting.retained_bytes, capture.totals.charge);
        sub(&accounting.retained_versions, capture.totals.versions);
        add(&accounting.overlay, carry_totals.charge);
        add(&accounting.retained_bytes, carry_totals.charge);
        add(&accounting.retained_versions, carry_totals.versions);
        let mut retired_base = None;
        if let Some(base) = base {
            if let Some(old) = &cell.base {
                sub(&accounting.retained_bytes, old.retained_bytes);
                sub(&accounting.retained_versions, old.retained_versions);
                if !old.mapped {
                    sub(&accounting.loaded, old.bytes.len());
                }
            }
            if let Some(new) = &base {
                add(&accounting.retained_bytes, new.retained_bytes);
                add(&accounting.retained_versions, new.retained_versions);
                if !new.mapped {
                    add(&accounting.loaded, new.bytes.len());
                }
            }
            retired_base = std::mem::replace(&mut cell.base, base);
            // Equality-only token; 2^64 installs of one cell cannot occur.
            cell.flush.base_token = cell.flush.base_token.wrapping_add(1);
            cell.flush.base_floor = position(&capture.floor);
            // Every reference through the target was published before the
            // capture, so it is in the new base; later ones are newer.
            cell.flush.coverage = cell.flush.coverage.max(capture.target);
        }
        if carry.is_some() {
            cell.flush.dirty_since = flight
                .as_ref()
                .and_then(|flight| flight.dirty_since)
                .into_iter()
                .chain(cell.flush.dirty_since)
                .min();
        }
        cell.flush.carry = carry;
        cell.flush.carry_totals = carry_totals;
        let coverage = cell.flush.coverage;
        drop(shard);
        state.cells.flights -= 1;
        CellInstall::Installed {
            coverage,
            retired: CellRetired {
                _flight: flight,
                _base: retired_base,
                _capture: capture,
            },
        }
    }

    /// Root capture slice (spec §7.2 step 2): descriptors of the directory
    /// entries in `range`, one shard lock at a time. A cell with no missing
    /// reference through `target` (`T`) advances coverage to it; every
    /// descriptor exports `min(D_i, T)` and pins the current base, whose
    /// frame cut can exceed `T`. `output` must have room for the slice:
    /// reserve it outside the serving locks.
    pub(crate) fn capture_cell_descriptors(
        &mut self,
        range: std::ops::Range<usize>,
        target: PrefixCut,
        output: &mut Vec<RootCellDescriptor>,
    ) -> Result<(), NativeIndexError> {
        let index = self.index;
        let state = &*self.state;
        if range.len() > ROOT_DESCRIPTOR_SLICE
            || range.end > state.cell_order.len()
            || output.capacity() - output.len() < range.len()
        {
            return Err(NativeIndexError::Invalid("root descriptor slice"));
        }
        index.check_prefix_published(target)?;
        for identity in &state.cell_order[range] {
            let mut shard = index.shard_for(identity).write();
            let cell = shard
                .get_mut(identity)
                .ok_or(NativeIndexError::Invalid("directory cell exists"))?;
            cell.flush.advance_clean(target);
            let base = match &cell.base {
                Some(block) => Some(RootCellFrame {
                    block: block.clone(),
                    frame_cut: frame_cut(&block.cut)?,
                    frame_floor: if cell.flush.base_token == 0 {
                        block.file.as_ref().and_then(|file| position(&file.floor))
                    } else {
                        cell.flush.base_floor
                    },
                }),
                None => None,
            };
            output.push(RootCellDescriptor {
                identity: identity.clone(),
                coverage: cell.flush.coverage.min(target),
                base,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "cell_flush_tests.rs"]
mod tests;
