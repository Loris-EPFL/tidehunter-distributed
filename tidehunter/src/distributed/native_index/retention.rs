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
//! The rare operations (checkpoint floor selection, sparse-view reservation,
//! the first owner charge of a standing-budget view) use the `cold` mutex and
//! may lock every pin shard.
//!
//! # Why a pin and a floor change never race
//!
//! A pin validates against `bounds` and inserts itself while holding its pin
//! shard's mutex. Floor selection (`capture_lease`) holds `cold`, then locks
//! every pin shard in ascending order, computes the minimum pin, publishes the
//! new `bounds`, and only then unlocks them. For any pin shard either the
//! reader's critical section ran before the selection took that shard (the
//! selection sees the pin and clamps the floor below it) or after it released
//! the shard (the reader's mutex acquire happens after the `bounds` store, so
//! it validates against the new bounds and fails instead of registering below
//! the floor).

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
    /// Allocated views charged to the view allowance: per-call reservations
    /// moved here at install, and standing-budget views charged to an owner
    /// (see [`NativeIndex::charge_owned_cut`]).
    view_bytes: usize,
    reserved_view_bytes: usize,
    /// Live standing-budget sparse views without an owner charge (installed,
    /// or held inside one read). Their bytes are not in `view_bytes`.
    uncharged_view_bytes: usize,
    /// Sparse views pin their stable floor like a reader pin.
    view_pins: BTreeMap<Option<MutationVersion>, usize>,
    active: bool,
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

    /// `owner_charged` is read from the dropped view; it changes only under
    /// `cold`, and the last reference is gone, so no charge can race this.
    pub(super) fn release_view(
        &self,
        key: Option<MutationVersion>,
        charge: usize,
        uncharged: usize,
        owner_charged: bool,
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
        if owner_charged {
            cold.view_bytes -= uncharged;
        } else {
            cold.uncharged_view_bytes -= uncharged;
        }
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
/// this budget at the store. Dropping the budget releases it.
///
/// A cut that leaves one read call (public cut, pin, checkpoint, iterator)
/// takes an owner charge of its view's size from the view allowance (see
/// [`NativeIndex::charge_owned_cut`]); the view holds it until its last
/// reference drops. That API fails with `Capacity` when the allowance is
/// full; publication never takes this charge.
///
/// Bound on the memory outside `max_index_bytes`: only views held inside one
/// read stay uncharged, and no read holds one while it waits for read
/// admission or while it is admitted. Extra memory <= `max_pending_reads` x
/// `view_charge(max_pending_batches - 1)` (in fact zero for admitted and
/// waiting reads), plus the transient and fixed holders listed here:
///
/// * Admitted and waiting reads: once its index lookups are done, a point,
///   batch or multi-window read pins only the retention floor
///   ([`NativeIndex::pin_call_floor`]). That pin protects the same history as
///   the view, and moves into the read flights admitted under
///   `max_pending_reads`. A multi-window read looks every key up before its
///   first window waits.
/// * Transient: a caller holds its resolved cut on its own stack while it
///   looks the index up; this never waits.
/// * Fixed: the one `&mut RemoteDatabase` read call in progress (its scan
///   cursor keeps the cut for the whole scan), and the write-gated Sui
///   maintenance call (at most three views, one call at a time).
///
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
        self.check_view_allowance(&cold, charge)?;
        cold.reserved_view_bytes += charge;
        Ok(())
    }

    fn check_view_allowance(&self, cold: &Cold, charge: usize) -> Result<(), NativeIndexError> {
        let total = cold
            .view_bytes
            .checked_add(cold.reserved_view_bytes)
            .and_then(|n| n.checked_add(charge))
            .ok_or(NativeIndexError::Capacity)?;
        if total > self.max_metadata_bytes {
            return Err(NativeIndexError::Capacity);
        }
        Ok(())
    }

    /// Charge a standing-budget sparse view to the view allowance before its
    /// cut leaves one read call. The charge belongs to the view, so every
    /// owner of one view shares it and the last reference releases it once
    /// (`release_view`). Scalar cuts and views with their own reservation
    /// (`charge != 0`) need nothing. Fails with `Capacity` when the allowance
    /// is full; the caller then drops the cut.
    pub(super) fn charge_owned_cut(&self, cut: &IndexReadCut) -> Result<(), NativeIndexError> {
        let IndexReadCut::Sparse(view) = cut else {
            return Ok(());
        };
        if !Arc::ptr_eq(&view.retention, &self.retention) {
            return Err(NativeIndexError::Invalid("foreign sparse view"));
        }
        // Relaxed: the flag changes once, false to true, under `cold`, and
        // guards no data read outside `cold`. A stale false takes the lock
        // and checks again; a true is never stale.
        if view.uncharged == 0 || view.owner_charged.load(Ordering::Relaxed) {
            return Ok(());
        }
        let mut cold = self.retention.cold.lock();
        if view.owner_charged.load(Ordering::Relaxed) {
            return Ok(());
        }
        self.check_view_allowance(&cold, view.uncharged)?;
        cold.uncharged_view_bytes -= view.uncharged;
        cold.view_bytes += view.uncharged;
        view.owner_charged.store(true, Ordering::Relaxed);
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

    /// Pin a cut that the caller can keep after this call: a standing-budget
    /// view takes its owner charge first (`charge_owned_cut`).
    pub fn pin_cut(&self, cut: IndexReadCut) -> Result<IndexReadPin, NativeIndexError> {
        let cut = self.resolve_cut(cut);
        self.check_pin(&cut)?;
        self.charge_owned_cut(&cut)?;
        self.register_pin(cut)
    }

    /// Pin without an owner charge, keeping the view. Only for a read that
    /// still looks the index up through the pin (a scan cursor). See
    /// [`StandingViewBudget`] for the bound.
    pub(crate) fn pin_call_cut(&self, cut: IndexReadCut) -> Result<IndexReadPin, NativeIndexError> {
        let cut = self.resolve_cut(cut);
        self.check_pin(&cut)?;
        self.register_pin(cut)
    }

    /// Pin only the retention floor of `cut`, for a read whose index lookups
    /// are done. A sparse view's retention key is its stable floor
    /// (`position`), so a scalar pin at that floor protects the same history
    /// without keeping the view alive while the read waits for admission.
    pub(crate) fn pin_call_floor(
        &self,
        cut: IndexReadCut,
    ) -> Result<IndexReadPin, NativeIndexError> {
        let cut = self.resolve_cut(cut);
        self.check_pin(&cut)?;
        let IndexReadCut::Sparse(view) = &cut else {
            return self.register_pin(cut);
        };
        let floor = view.floor.map_or(IndexReadCut::BeforeAll, IndexReadCut::At);
        // The live view pins this same floor in `cold.view_pins` until `cut`
        // drops after the registration, so no capture can pass the floor in
        // between and the floor validates exactly like the view did.
        let pin = self.register_pin(floor);
        drop(cut);
        pin
    }

    fn check_pin(&self, cut: &IndexReadCut) -> Result<(), NativeIndexError> {
        if let IndexReadCut::Sparse(view) = cut
            && !Arc::ptr_eq(&view.retention, &self.retention)
        {
            return Err(NativeIndexError::Invalid("foreign sparse view"));
        }
        self.check_published(cut)
    }

    fn register_pin(&self, cut: IndexReadCut) -> Result<IndexReadPin, NativeIndexError> {
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
        if cold.active {
            return Err(NativeIndexError::CheckpointInProgress);
        }
        // Historical mapped bytes do not enter either budget. Capture duplicates
        // at most the bounded resident overlay plus the cell directory.
        let transient_bytes = self.resident_bytes();
        if transient_bytes > self.max_metadata_bytes {
            return Err(NativeIndexError::Capacity);
        }
        // Lock every pin shard in ascending index (see module documentation),
        // select the floor from the pins, publish the in-progress floor, and
        // only then release them. A pre-genesis pin clamps to BeforeAll and
        // prevents any advancement.
        let pin_guards: Vec<_> = retention
            .pin_shards
            .iter()
            .map(|shard| shard.0.lock())
            .collect();
        let lowest_pin = pin_guards
            .iter()
            .filter_map(|pins| pins.first_key_value().map(|(pin, _)| *pin))
            .chain(cold.view_pins.first_key_value().map(|(pin, _)| *pin))
            .min();
        let floor = lowest_pin
            .filter(|pin| *pin < position(&floor))
            .map_or(floor, |pin| {
                pin.map_or(IndexReadCut::BeforeAll, IndexReadCut::At)
            });
        cold.transient_bytes = transient_bytes;
        cold.active = true;
        retention.bounds.store(Arc::new(Bounds {
            floor: bounds.floor.clone(),
            preparing: Some(floor.clone()),
        }));
        drop(pin_guards);
        Ok(CaptureLease {
            retention: Arc::clone(retention),
            floor,
        })
    }

    pub(super) fn transient_bytes(&self) -> usize {
        // The capture and view allowances are independent. View accounting
        // includes pre-admitted reservations as well as allocated views.
        let cold = self.retention.cold.lock();
        cold.transient_bytes
            .saturating_add(cold.view_bytes)
            .saturating_add(cold.reserved_view_bytes)
    }

    /// Bytes of standing-budget views held inside one read only (replaced,
    /// without an owner charge). Not in either budget; see
    /// [`StandingViewBudget`] for the bound. Sampled without the writer: the
    /// view being installed counts for the instant between its registration
    /// and the watermark store.
    pub(super) fn reader_held_view_bytes(&self) -> usize {
        // Own the installed cut across the cold lock so its bytes stay counted,
        // and drop it only after the unlock: it can be the last reference to
        // a replaced view, whose drop takes the cold lock.
        let installed = self.published.load_full();
        let held = {
            let cold = self.retention.cold.lock();
            let installed_uncharged = match &*installed {
                // Under `cold`, the flag agrees with the counters.
                IndexReadCut::Sparse(view) if !view.owner_charged.load(Ordering::Relaxed) => {
                    view.uncharged
                }
                _ => 0,
            };
            // The live installed view is in `uncharged_view_bytes` unless it
            // has an owner charge, so this cannot underflow.
            cold.uncharged_view_bytes - installed_uncharged
        };
        drop(installed);
        held
    }
}

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
