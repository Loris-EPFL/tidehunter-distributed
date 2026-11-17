//! Read-cut retention: the compaction floor, read pins and sparse-view quota.
//!
//! The hot read path never takes a shared mutex here:
//!
//! * `validate` reads immutable [`Bounds`] (floor and in-progress floor)
//!   through `ArcSwap`.
//! * A pin registers in one of [`PIN_SHARDS`] mutexes chosen by a per-thread
//!   slot, so concurrent readers rarely meet. The registration quota is one
//!   padded atomic.
//!
//! The rare operations (checkpoint floor selection, sparse-view reservation)
//! use the `cold` mutex and may lock every pin shard.
//!
//! # Floor owners
//!
//! At most one lease owns the in-progress floor (`Bounds::preparing`) at a
//! time:
//!
//! * [`CaptureLease`]: the exact global checkpoint (`Cold::active`).
//! * [`RootFloorLease`]: the per-cell metadata root (option C spec §5.2,
//!   §7.2 step 4, §9.2 step 1; `Cold::root_floor`). It selects
//!   `H_new = max(H, min(requested, F, lowest pin or sparse-view floor))`,
//!   so `H_new <= F` and no pinned reader becomes invalid, and makes it the
//!   committed floor only when the adopting manifest is durable. Cell flush
//!   captures read the committed floor, never the in-progress one: a flight
//!   captured at the old floor over-retains after `H` advances, and a frame
//!   floor never exceeds the `H` of a root that can reference it. Cell
//!   flights therefore never hold or advance the floor themselves.
//!
//! # Why a pin and a floor change never race
//!
//! A pin validates against `bounds` and inserts itself while holding its pin
//! shard's mutex. Floor selection ([`RetentionState::select_floor`]) holds
//! `cold`, then locks every pin shard in ascending order, computes the
//! minimum pin, publishes the new `bounds`, and only then unlocks them. For
//! any pin shard either the reader's critical section ran before the
//! selection took that shard (the selection sees the pin and clamps the floor
//! below it) or after it released the shard (the reader's mutex acquire
//! happens after the `bounds` store, so it validates against the new bounds
//! and fails instead of registering below the floor).
//!
//! Lock order: `cold` -> pin shards ascending. Callers may hold the writer
//! and the option-D publisher (spec §2.2: writer -> publisher -> one index
//! shard -> `cold` -> pin shards); nothing here takes them.

use super::*;
use arc_swap::ArcSwap;
use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Pin registries. Fixed and small: capture locks all of them.
const PIN_SHARDS: usize = 16;

/// A clone shares one registration. The last owner releases the exact read cut.
#[derive(Clone, Debug)]
pub struct IndexReadPin(Arc<ReadPin>);

#[derive(Debug)]
struct ReadPin {
    cut: IndexReadCut,
    retention: Arc<RetentionState>,
    shard: usize,
}

impl IndexReadPin {
    pub fn cut(&self) -> IndexReadCut {
        self.0.cut.clone()
    }
}

impl Drop for ReadPin {
    fn drop(&mut self) {
        self.retention.release_pin(self.shard, position(&self.cut));
    }
}

/// Immutable snapshot of the compaction bounds; replaced as a whole.
#[derive(Debug)]
struct Bounds {
    floor: IndexReadCut,
    preparing: Option<IndexReadCut>,
}

impl Bounds {
    fn validate(&self, cut: &IndexReadCut) -> Result<(), NativeIndexError> {
        if position(cut) < position(&self.floor) {
            return Err(NativeIndexError::ExpiredCut);
        }
        if self
            .preparing
            .as_ref()
            .is_some_and(|floor| position(cut) < position(floor))
        {
            return Err(NativeIndexError::RetentionInProgress);
        }
        Ok(())
    }
}

/// State under the `cold` mutex: rare transitions only.
#[derive(Debug, Default)]
struct Cold {
    transient_bytes: usize,
    view_bytes: usize,
    reserved_view_bytes: usize,
    /// Live standing-budget sparse views (installed or reader-held). Their
    /// bytes are not in `view_bytes`: the standing budget owns them.
    uncharged_view_bytes: usize,
    /// Sparse views pin their stable floor like a reader pin.
    view_pins: BTreeMap<Option<MutationVersion>, usize>,
    /// The exact [`CaptureLease`] owns `Bounds::preparing`.
    active: bool,
    /// The per-cell [`RootFloorLease`] owns `Bounds::preparing`. Exclusive
    /// with `active`: both leases restore `preparing` on drop.
    root_floor: bool,
    /// Option C cell flush permits: residual bytes and outstanding flights.
    /// An allowance independent of the capture and view allowances.
    cell_bytes: usize,
    cell_flights: usize,
}

/// One pin registry in its own cache line.
#[repr(align(64))]
#[derive(Debug, Default)]
struct PinShard(Mutex<BTreeMap<Option<MutationVersion>, usize>>);

/// Independent registrations (not `Arc` owners). Padded: every pin touches it.
#[repr(align(64))]
#[derive(Debug, Default)]
struct Registrations(AtomicUsize);

pub(super) struct RetentionState {
    bounds: ArcSwap<Bounds>,
    pin_shards: Box<[PinShard]>,
    registrations: Registrations,
    cold: Mutex<Cold>,
}

impl fmt::Debug for RetentionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RetentionState")
            .field("bounds", &*self.bounds.load())
            .field(
                "registrations",
                &self.registrations.0.load(Ordering::Relaxed),
            )
            .finish_non_exhaustive()
    }
}

/// Stable per-thread pin shard slot: threads are spread round-robin on first use.
fn thread_slot() -> usize {
    use std::cell::Cell;
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    thread_local! {
        static SLOT: Cell<Option<usize>> = const { Cell::new(None) };
    }
    SLOT.with(|slot| match slot.get() {
        Some(value) => value,
        None => {
            // Relaxed: only distinctness matters, not ordering.
            let value = NEXT.fetch_add(1, Ordering::Relaxed);
            slot.set(Some(value));
            value
        }
    })
}

impl RetentionState {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            bounds: ArcSwap::from_pointee(Bounds {
                floor: IndexReadCut::BeforeAll,
                preparing: None,
            }),
            pin_shards: (0..PIN_SHARDS).map(|_| PinShard::default()).collect(),
            registrations: Registrations::default(),
            cold: Mutex::new(Cold::default()),
        })
    }

    /// Lock-free. Floors only advance, so a `Latest`-derived cut that passes
    /// here and is then held through a pin stays valid.
    pub(super) fn validate(&self, cut: &IndexReadCut) -> Result<(), NativeIndexError> {
        self.bounds.load().validate(cut)
    }

    pub(super) fn floor(&self) -> IndexReadCut {
        self.bounds.load().floor.clone()
    }

    pub(super) fn capture_active(&self) -> bool {
        self.cold.lock().active
    }

    /// Replace the persisted floor. Every `bounds` writer holds `cold`, so the
    /// read-modify-write cannot lose an update.
    pub(super) fn set_floor(&self, floor: IndexReadCut) {
        let _cold = self.cold.lock();
        let preparing = self.bounds.load().preparing.clone();
        self.bounds.store(Arc::new(Bounds { floor, preparing }));
    }

    /// Select an in-progress floor and publish it as `Bounds::preparing`.
    /// The caller holds `cold` (passed in) and owns `preparing`.
    ///
    /// Lock every pin shard in ascending index (see module documentation),
    /// clamp `proposed` below the lowest reader pin and sparse-view floor,
    /// raise the result to at least `at_least`, publish it, and only then
    /// release the shards. A pre-genesis pin clamps to `BeforeAll`. The guards
    /// live in a fixed array: nothing allocates under the locks.
    fn select_floor(
        &self,
        cold: &Cold,
        proposed: IndexReadCut,
        at_least: &IndexReadCut,
    ) -> IndexReadCut {
        let pin_guards: [_; PIN_SHARDS] =
            std::array::from_fn(|shard| self.pin_shards[shard].0.lock());
        let lowest_pin = pin_guards
            .iter()
            .filter_map(|pins| pins.first_key_value().map(|(pin, _)| *pin))
            .chain(cold.view_pins.first_key_value().map(|(pin, _)| *pin))
            .min();
        let clamped = lowest_pin
            .filter(|pin| *pin < position(&proposed))
            .map_or(proposed, |pin| {
                pin.map_or(IndexReadCut::BeforeAll, IndexReadCut::At)
            });
        let floor = if position(&clamped) < position(at_least) {
            at_least.clone()
        } else {
            clamped
        };
        let committed = self.bounds.load().floor.clone();
        self.bounds.store(Arc::new(Bounds {
            floor: committed,
            preparing: Some(floor.clone()),
        }));
        drop(pin_guards);
        floor
    }

    fn pin(
        self: &Arc<Self>,
        cut: IndexReadCut,
        limit: usize,
    ) -> Result<IndexReadPin, NativeIndexError> {
        let shard = thread_slot() % PIN_SHARDS;
        let key = position(&cut);
        let mut pins = self.pin_shards[shard].0.lock();
        // Validate under the pin shard mutex; see the module documentation.
        self.bounds.load().validate(&cut)?;
        // This is a separate registration-count quota, not a shared heap-byte
        // charge: Arc headers and BTree nodes are outside index/capture stats.
        // Relaxed: the counter guards no other data. A concurrent registration
        // can make the limit edge reject slightly early, never late.
        if self.registrations.0.fetch_add(1, Ordering::Relaxed) >= limit {
            self.registrations.0.fetch_sub(1, Ordering::Relaxed);
            return Err(NativeIndexError::Capacity);
        }
        *pins.entry(key).or_default() += 1;
        drop(pins);
        Ok(IndexReadPin(Arc::new(ReadPin {
            cut,
            retention: Arc::clone(self),
            shard,
        })))
    }

    fn release_pin(&self, shard: usize, key: Option<MutationVersion>) {
        let mut pins = self.pin_shards[shard].0.lock();
        if let Some(count) = pins.get_mut(&key) {
            *count -= 1;
            if *count == 0 {
                pins.remove(&key);
            }
            drop(pins);
            self.registrations.0.fetch_sub(1, Ordering::Relaxed);
        }
    }

    pub(super) fn release_view(
        &self,
        key: Option<MutationVersion>,
        charge: usize,
        uncharged: usize,
    ) {
        let mut cold = self.cold.lock();
        let count = cold
            .view_pins
            .get_mut(&key)
            .expect("view retention registration");
        *count -= 1;
        if *count == 0 {
            cold.view_pins.remove(&key);
        }
        cold.view_bytes -= charge;
        cold.uncharged_view_bytes -= uncharged;
    }

    #[cfg(test)]
    pub(super) fn registrations(&self) -> usize {
        self.registrations.0.load(Ordering::Relaxed)
    }

    /// Pins of every shard merged by cut position.
    #[cfg(test)]
    pub(super) fn pin_counts(&self) -> BTreeMap<Option<MutationVersion>, usize> {
        let mut merged = BTreeMap::new();
        for shard in self.pin_shards.iter() {
            for (key, count) in shard.0.lock().iter() {
                *merged.entry(*key).or_default() += count;
            }
        }
        merged
    }
}

impl RetentionState {
    /// Reserve one cell flush permit without waiting.
    pub(super) fn reserve_cell_flush(
        &self,
        bytes: usize,
        max_bytes: usize,
        max_flights: usize,
    ) -> Result<(), NativeIndexError> {
        let mut cold = self.cold.lock();
        if cold.cell_flights >= max_flights
            || cold
                .cell_bytes
                .checked_add(bytes)
                .is_none_or(|total| total > max_bytes)
        {
            return Err(NativeIndexError::Capacity);
        }
        cold.cell_flights += 1;
        cold.cell_bytes += bytes;
        Ok(())
    }

    /// Add `extra` bytes to an existing permit without waiting.
    pub(super) fn grow_cell_flush(&self, extra: usize, max_bytes: usize) -> bool {
        let mut cold = self.cold.lock();
        let Some(total) = cold
            .cell_bytes
            .checked_add(extra)
            .filter(|total| *total <= max_bytes)
        else {
            return false;
        };
        cold.cell_bytes = total;
        true
    }

    pub(super) fn release_cell_flush(&self, bytes: usize) {
        let mut cold = self.cold.lock();
        cold.cell_flights -= 1;
        cold.cell_bytes -= bytes;
    }
}

pub(super) fn position(cut: &IndexReadCut) -> Option<MutationVersion> {
    match cut {
        IndexReadCut::BeforeAll => None,
        IndexReadCut::At(version) => Some(*version),
        IndexReadCut::Sparse(view) => view.floor,
        IndexReadCut::Latest => unreachable!("read cuts are resolved before retention"),
    }
}

/// One capture owns the transient budget and reserves its proposed floor until
/// installation or abort. This also prevents out-of-order image installation.
pub(super) struct CaptureLease {
    retention: Arc<RetentionState>,
    pub floor: IndexReadCut,
}

impl CaptureLease {
    pub fn install(&self) {
        self.retention.set_floor(self.floor.clone());
    }
}

impl Drop for CaptureLease {
    fn drop(&mut self) {
        let mut cold = self.retention.cold.lock();
        let floor = self.retention.bounds.load().floor.clone();
        self.retention.bounds.store(Arc::new(Bounds {
            floor,
            preparing: None,
        }));
        cold.transient_bytes = 0;
        cold.active = false;
    }
}

/// The one per-cell root-floor lease (module documentation; spec §5.2, §7.2
/// step 4). It reserves `H_new` as the in-progress floor from root capture
/// until the manifest adopting the root is durable
/// ([`NativeIndex::install_root_floor`]) or the root is abandoned (drop: the
/// committed floor is unchanged). It holds no byte charge.
#[derive(Debug)]
pub(crate) struct RootFloorLease {
    retention: Arc<RetentionState>,
    /// The committed floor when the lease began.
    previous: IndexReadCut,
    /// `H_new`: scalar, at least `previous`, at most the root's `F`.
    floor: IndexReadCut,
    /// True until installation: the lease owns `Cold::root_floor` and
    /// `Bounds::preparing`.
    owns: bool,
}

impl RootFloorLease {
    /// `H_new`, the root's history floor (`None` = `BeforeAll`).
    pub(crate) fn floor(&self) -> Option<MutationVersion> {
        position(&self.floor)
    }

    /// The committed floor when the lease began.
    pub(crate) fn previous(&self) -> Option<MutationVersion> {
        position(&self.previous)
    }

    /// True if installing this lease raises the committed floor.
    pub(crate) fn advances(&self) -> bool {
        self.floor() > self.previous()
    }
}

impl Drop for RootFloorLease {
    fn drop(&mut self) {
        if !self.owns {
            return;
        }
        // Abandoned root: keep the committed floor, release the reservation.
        let mut cold = self.retention.cold.lock();
        let floor = self.retention.bounds.load().floor.clone();
        self.retention.bounds.store(Arc::new(Bounds {
            floor,
            preparing: None,
        }));
        cold.root_floor = false;
    }
}

/// A pre-admitted sparse publication allocation. Dropping it releases its quota.
#[derive(Debug)]
pub(crate) struct PublicationViewReservation {
    retention: Arc<RetentionState>,
    charge: usize,
    max_excluded: usize,
}

impl PublicationViewReservation {
    pub(super) fn max_excluded(&self) -> usize {
        self.max_excluded
    }

    pub(super) fn charge(&self) -> usize {
        self.charge
    }

    pub(super) fn belongs_to(&self, retention: &Arc<RetentionState>) -> bool {
        Arc::ptr_eq(&self.retention, retention)
    }

    /// Hand the reserved quota to a live view; nothing is released on drop.
    pub(super) fn consume(&mut self) {
        self.charge = 0;
    }
}

impl Drop for PublicationViewReservation {
    fn drop(&mut self) {
        if self.charge != 0 {
            self.retention.cold.lock().reserved_view_bytes -= self.charge;
        }
    }
}

/// The constant sparse-view quota of the one serialized publisher, reserved
/// once at open. It covers the installed view and the replacement being built
/// beside it (two views at the worst-case size), so a standing install takes no
/// quota and cannot fail on view capacity after its batches are durable. Views
/// installed from it carry no own charge: replacing one returns its share to
/// this budget at the store, not when the last reader-held clone drops. Such a
/// clone keeps only its retention floor pin. Dropping the budget releases it.
///
/// Bound on the memory outside `max_index_bytes`: replaced views still held by
/// readers are not charged. Extra memory <= (distinct replaced views still
/// held by readers) x `view_charge(max_pending_batches - 1)`. The concurrent
/// read limit bounds the number of such views, and each lasts one read.
/// `NativeIndexStats::reader_held_view_bytes` reports the current amount.
#[derive(Debug)]
pub(crate) struct StandingViewBudget {
    retention: Arc<RetentionState>,
    charge: usize,
    max_excluded: usize,
}

impl StandingViewBudget {
    pub(super) fn max_excluded(&self) -> usize {
        self.max_excluded
    }

    pub(super) fn belongs_to(&self, retention: &Arc<RetentionState>) -> bool {
        Arc::ptr_eq(&self.retention, retention)
    }
}

impl Drop for StandingViewBudget {
    fn drop(&mut self) {
        if self.charge != 0 {
            self.retention.cold.lock().reserved_view_bytes -= self.charge;
        }
    }
}

pub(super) fn view_charge(capacity: usize) -> Result<usize, NativeIndexError> {
    if capacity == 0 {
        return Ok(0);
    }
    // Include Arc bookkeeping and a conservative share of the pin tree.
    let header = std::mem::size_of::<SparseIndexView>()
        .checked_add(2 * std::mem::size_of::<usize>())
        .and_then(|n| n.checked_add(128))
        .ok_or(NativeIndexError::Capacity)?;
    capacity
        .checked_mul(std::mem::size_of::<BatchVersion>())
        .and_then(|n| n.checked_add(header))
        .ok_or(NativeIndexError::Capacity)
}

impl NativeIndex {
    /// Reserve the worst-case sparse view before any irreversible publication.
    pub(crate) fn reserve_publication_view(
        &self,
        max_excluded: usize,
    ) -> Result<PublicationViewReservation, NativeIndexError> {
        let charge = view_charge(max_excluded)?;
        self.reserve_view_bytes(charge)?;
        Ok(PublicationViewReservation {
            retention: Arc::clone(&self.retention),
            charge,
            max_excluded,
        })
    }

    /// Reserve the publisher's constant view quota: the installed view plus
    /// its replacement, each with `max_excluded` exclusions.
    pub(crate) fn reserve_standing_view_budget(
        &self,
        max_excluded: usize,
    ) -> Result<StandingViewBudget, NativeIndexError> {
        let charge = view_charge(max_excluded)?
            .checked_mul(2)
            .ok_or(NativeIndexError::Capacity)?;
        self.reserve_view_bytes(charge)?;
        Ok(StandingViewBudget {
            retention: Arc::clone(&self.retention),
            charge,
            max_excluded,
        })
    }

    fn reserve_view_bytes(&self, charge: usize) -> Result<(), NativeIndexError> {
        let mut cold = self.retention.cold.lock();
        let total = cold
            .view_bytes
            .checked_add(cold.reserved_view_bytes)
            .and_then(|n| n.checked_add(charge))
            .ok_or(NativeIndexError::Capacity)?;
        if total > self.max_metadata_bytes {
            return Err(NativeIndexError::Capacity);
        }
        cold.reserved_view_bytes += charge;
        Ok(())
    }

    /// Validate a sparse view's floor under the retention lock and pin it.
    /// A per-call reservation moves its quota from reserved to allocated
    /// (`charge`); a standing-budget view has `charge` 0 and reports its bytes
    /// as `uncharged`. Called by the writer.
    pub(super) fn register_live_view(
        &self,
        stable_floor: &IndexReadCut,
        charge: usize,
        uncharged: usize,
        reservation: Option<&mut PublicationViewReservation>,
        sparse: bool,
    ) -> Result<(), NativeIndexError> {
        let key = position(stable_floor);
        let mut cold = self.retention.cold.lock();
        self.retention.validate(stable_floor)?;
        if let Some(reservation) = reservation {
            cold.reserved_view_bytes -= reservation.charge();
            reservation.consume();
        }
        if sparse {
            cold.view_bytes += charge;
            cold.uncharged_view_bytes += uncharged;
            *cold.view_pins.entry(key).or_default() += 1;
        }
        Ok(())
    }

    pub fn validate_cut(&self, cut: IndexReadCut) -> Result<(), NativeIndexError> {
        let cut = self.resolve_cut(cut);
        self.validate_resolved(&cut)
    }

    /// `cut` must not be `Latest`. An explicit `At` cut is public API, so a
    /// caller can name a version whose batch is applied to some shards but not
    /// published. Such a cut is rejected: every cut a reader can hold is
    /// covered by the publication watermark.
    pub(super) fn validate_resolved(&self, cut: &IndexReadCut) -> Result<(), NativeIndexError> {
        if let IndexReadCut::Sparse(view) = cut
            && !Arc::ptr_eq(&view.retention, &self.retention)
        {
            return Err(NativeIndexError::Invalid("foreign sparse view"));
        }
        self.retention.validate(cut)?;
        self.check_published(cut)
    }

    /// `BeforeAll` is empty and a `Sparse` view is minted by this index after
    /// its entries were applied, so only `At` needs the check. Publication is
    /// per batch, so `At(v)` is covered when `v`'s batch is not above the
    /// watermark's newest batch and no unpublished (excluded) batch is at or
    /// below it. Any operation index inside a published batch is fine: all of
    /// that batch's entries are applied. The watermark only grows, so a cut that
    /// was covered stays covered.
    pub(super) fn check_published(&self, cut: &IndexReadCut) -> Result<(), NativeIndexError> {
        let IndexReadCut::At(version) = cut else {
            return Ok(());
        };
        let published = self.published.load();
        let covered = match &**published {
            IndexReadCut::At(upper) => version.batch <= upper.batch,
            IndexReadCut::Sparse(view) => {
                version.batch <= view.upper.batch
                    && view
                        .excluded
                        .first()
                        .is_none_or(|hole| *hole > version.batch)
            }
            IndexReadCut::BeforeAll | IndexReadCut::Latest => false,
        };
        if covered {
            Ok(())
        } else {
            Err(NativeIndexError::UnpublishedCut)
        }
    }

    pub fn pin_cut(&self, cut: IndexReadCut) -> Result<IndexReadPin, NativeIndexError> {
        let cut = self.resolve_cut(cut);
        if let IndexReadCut::Sparse(view) = &cut
            && !Arc::ptr_eq(&view.retention, &self.retention)
        {
            return Err(NativeIndexError::Invalid("foreign sparse view"));
        }
        self.check_published(&cut)?;
        self.retention.pin(
            cut,
            self.max_metadata_bytes / std::mem::size_of::<ReadPin>(),
        )
    }

    pub fn retention_floor(&self) -> IndexReadCut {
        self.retention.floor()
    }

    /// Select and reserve the retention floor of one capture. `latest` is the
    /// writer's newest promoted version.
    pub(super) fn capture_lease(
        &self,
        cut: &IndexReadCut,
        floor: IndexReadCut,
        latest: Option<MutationVersion>,
    ) -> Result<CaptureLease, NativeIndexError> {
        let retention = &self.retention;
        let mut cold = retention.cold.lock();
        let bounds = retention.bounds.load_full();
        bounds.validate(cut)?;
        if matches!(cut, IndexReadCut::Sparse(_))
            || matches!(floor, IndexReadCut::Latest | IndexReadCut::Sparse(_))
            || position(&floor) < position(&bounds.floor)
            || position(&floor) > position(cut)
            || position(&floor)
                > position(&latest.map_or(IndexReadCut::BeforeAll, IndexReadCut::At))
        {
            return Err(NativeIndexError::Invalid("nonmonotonic retention floor"));
        }
        if cold.active || cold.root_floor {
            return Err(NativeIndexError::CheckpointInProgress);
        }
        // Historical mapped bytes do not enter either budget. Capture duplicates
        // at most the bounded resident overlay plus the cell directory.
        let transient_bytes = self.resident_bytes();
        if transient_bytes > self.max_metadata_bytes {
            return Err(NativeIndexError::Capacity);
        }
        let floor = retention.select_floor(&cold, floor, &IndexReadCut::BeforeAll);
        cold.transient_bytes = transient_bytes;
        cold.active = true;
        Ok(CaptureLease {
            retention: Arc::clone(retention),
            floor,
        })
    }

    /// Begin the per-cell root-floor lease of one metadata root (spec §7.2
    /// step 4): `H_new = max(H, min(requested, F, lowest pin or sparse-view
    /// floor))`, where `H` is the committed floor and `F = cap` the root's
    /// replay floor. `H_new` becomes the in-progress floor, so no new pin can
    /// register below it; an existing pin never becomes invalid. A pin below
    /// `requested` clamps silently, as for the exact capture. `requested`
    /// must be scalar (`BeforeAll` or `At`); `cap` must be published and at
    /// or above `H`. One lease at a time, exclusive with the exact capture.
    /// Takes `cold` and the pin shards only: callable with or without the
    /// writer and publisher held.
    pub(crate) fn begin_root_floor(
        &self,
        requested: IndexReadCut,
        cap: PrefixCut,
    ) -> Result<RootFloorLease, NativeIndexError> {
        if matches!(requested, IndexReadCut::Latest | IndexReadCut::Sparse(_)) {
            return Err(NativeIndexError::Invalid(
                "root history floor must be a scalar cut",
            ));
        }
        if let PrefixCut::Through(batch) = cap {
            // The watermark compares batches: the operation is irrelevant.
            self.check_published(&IndexReadCut::At(MutationVersion {
                batch,
                operation_index: 0,
            }))?;
        }
        let cap = cap.index_cut();
        let retention = &self.retention;
        let mut cold = retention.cold.lock();
        if cold.active || cold.root_floor {
            return Err(NativeIndexError::CheckpointInProgress);
        }
        let previous = retention.bounds.load().floor.clone();
        if position(&previous) > position(&cap) {
            return Err(NativeIndexError::Invalid(
                "committed history floor above the replay floor",
            ));
        }
        let proposed = if position(&requested) < position(&cap) {
            requested
        } else {
            cap
        };
        let floor = retention.select_floor(&cold, proposed, &previous);
        cold.root_floor = true;
        Ok(RootFloorLease {
            retention: Arc::clone(retention),
            previous,
            floor,
            owns: true,
        })
    }

    /// Commit a root-floor lease once the manifest adopting its root is
    /// durable (spec §9.2 step 1): the committed floor becomes `H_new` and
    /// the in-progress floor is cleared. Later cell captures encode at the
    /// new floor; flights captured before keep the old one and over-retain.
    /// A foreign or stale lease is refused and dropped (which restores the
    /// in-progress floor); the committed floor is then unchanged.
    pub(crate) fn install_root_floor(
        &self,
        mut lease: RootFloorLease,
    ) -> Result<(), NativeIndexError> {
        if !Arc::ptr_eq(&lease.retention, &self.retention) {
            return Err(NativeIndexError::Invalid("foreign root floor lease"));
        }
        let mut cold = self.retention.cold.lock();
        let committed = self.retention.bounds.load().floor.clone();
        if !lease.owns || !cold.root_floor || position(&committed) != lease.previous() {
            // Release `cold` before the lease's drop takes it.
            drop(cold);
            return Err(NativeIndexError::Invalid("stale root floor lease"));
        }
        // Monotonic: `floor >= previous == committed` by construction.
        self.retention.bounds.store(Arc::new(Bounds {
            floor: lease.floor.clone(),
            preparing: None,
        }));
        cold.root_floor = false;
        lease.owns = false;
        Ok(())
    }

    pub(super) fn transient_bytes(&self) -> usize {
        // The capture, view and cell flush allowances are independent. View
        // accounting includes pre-admitted reservations as well as allocated
        // views; cell flush accounting is the reserved permit bytes.
        let cold = self.retention.cold.lock();
        cold.transient_bytes
            .saturating_add(cold.view_bytes)
            .saturating_add(cold.reserved_view_bytes)
            .saturating_add(cold.cell_bytes)
    }

    /// Bytes of standing-budget views that were replaced but are still held by
    /// readers; not charged to the budget. Sampled without the writer: the
    /// view being installed counts for the instant between its registration
    /// and the watermark store.
    pub(super) fn reader_held_view_bytes(&self) -> usize {
        // Drop the watermark guard before the cold lock: it can own the last
        // reference to a replaced view, whose drop takes the cold lock.
        let installed = match &**self.published.load() {
            IndexReadCut::Sparse(view) => view.uncharged,
            _ => 0,
        };
        // Saturating: the installed view can be replaced and dropped between
        // the load and the lock.
        self.retention
            .cold
            .lock()
            .uncharged_view_bytes
            .saturating_sub(installed)
    }
}

#[cfg(test)]
#[path = "root_floor_tests.rs"]
mod root_floor_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::key_shape::KeyShapeBuilder;

    #[test]
    fn duplicate_pin_registrations_share_cut_entry_but_clones_share_one_quota_slot() {
        let mut builder = KeyShapeBuilder::new();
        builder.add_key_space("pins", 4, 1, KeyType::uniform(1));
        let index = NativeIndex::new(
            &builder.build(),
            [7; 16],
            2 * std::mem::size_of::<ReadPin>(),
        )
        .unwrap();
        let first = index.pin_cut(IndexReadCut::BeforeAll).unwrap();
        let clone = first.clone();
        let duplicate = index.pin_cut(IndexReadCut::BeforeAll).unwrap();
        assert_eq!(index.retention.registrations(), 2);
        let pins = index.retention.pin_counts();
        assert_eq!(pins.len(), 1);
        assert_eq!(pins[&None], 2);
        assert!(matches!(
            index.pin_cut(IndexReadCut::BeforeAll),
            Err(NativeIndexError::Capacity)
        ));
        drop(first);
        assert_eq!(
            index.retention.registrations(),
            2,
            "a clone still owns the first registration"
        );
        assert!(matches!(
            index.pin_cut(IndexReadCut::BeforeAll),
            Err(NativeIndexError::Capacity)
        ));
        drop(clone);
        assert_eq!(index.retention.registrations(), 1);
        let replacement = index.pin_cut(IndexReadCut::BeforeAll).unwrap();
        assert_eq!(
            index.retention.registrations(),
            2,
            "the released quota slot is reusable"
        );
        drop(replacement);
        drop(duplicate);
        assert_eq!(index.retention.registrations(), 0);
        assert!(index.retention.pin_counts().is_empty());
    }

    #[test]
    fn pins_from_many_threads_spread_over_shards_and_release_everywhere() {
        let mut builder = KeyShapeBuilder::new();
        builder.add_key_space("pins", 4, 1, KeyType::uniform(1));
        let index = Arc::new(NativeIndex::new(&builder.build(), [7; 16], 1 << 20).unwrap());
        let workers: Vec<_> = (0..8)
            .map(|_| {
                let index = Arc::clone(&index);
                std::thread::spawn(move || {
                    let pins: Vec<_> = (0..50)
                        .map(|_| index.pin_cut(IndexReadCut::BeforeAll).unwrap())
                        .collect();
                    pins.len()
                })
            })
            .collect();
        for worker in workers {
            assert_eq!(worker.join().unwrap(), 50);
        }
        assert_eq!(index.retention.registrations(), 0);
        assert!(index.retention.pin_counts().is_empty());
    }
}
