//! Read-cut retention: the compaction floor, read pins and sparse-view quota.
//!
//! The hot read path never takes a shared mutex here:
//!
//! * `validate` is two `ArcSwap` loads of an immutable [`Bounds`] (floor and
//!   in-progress floor).
//! * A pin registers in one of [`PIN_SHARDS`] mutexes chosen by a per-thread
//!   slot, so concurrent readers rarely meet. The registration quota is one
//!   padded atomic.
//!
//! The rare operations (checkpoint floor selection, sparse-view reservation)
//! use the `cold` mutex and may lock every pin shard.
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
    view_bytes: usize,
    reserved_view_bytes: usize,
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

    pub(super) fn release_view(&self, key: Option<MutationVersion>, charge: usize) {
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
    /// Additional quota needed to support this exclusion capacity, without shrinking.
    pub(crate) fn additional_charge(&self, max_excluded: usize) -> Result<usize, NativeIndexError> {
        if max_excluded <= self.max_excluded {
            return Ok(0);
        }
        Ok(view_charge(max_excluded)? - self.charge)
    }

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
    pub(crate) fn publication_view_charge(max_excluded: usize) -> Result<usize, NativeIndexError> {
        view_charge(max_excluded)
    }

    /// Callers that preflight and then upgrade several reservations hold the
    /// index writer across both steps, which serializes them against other
    /// reservers on the owner. Each step is itself atomic under `cold`.
    pub(crate) fn check_publication_view_capacity(
        &self,
        additional_bytes: usize,
    ) -> Result<(), NativeIndexError> {
        let cold = self.retention.cold.lock();
        let total = cold
            .view_bytes
            .checked_add(cold.reserved_view_bytes)
            .and_then(|n| n.checked_add(additional_bytes))
            .ok_or(NativeIndexError::Capacity)?;
        if total > self.max_metadata_bytes {
            return Err(NativeIndexError::Capacity);
        }
        Ok(())
    }

    pub(crate) fn grow_publication_view(
        &self,
        reservation: &mut PublicationViewReservation,
        max_excluded: usize,
    ) -> Result<(), NativeIndexError> {
        if !reservation.belongs_to(&self.retention) {
            return Err(NativeIndexError::Invalid(
                "foreign publication view reservation",
            ));
        }
        let additional = reservation.additional_charge(max_excluded)?;
        let mut cold = self.retention.cold.lock();
        let total = cold
            .view_bytes
            .checked_add(cold.reserved_view_bytes)
            .and_then(|n| n.checked_add(additional))
            .ok_or(NativeIndexError::Capacity)?;
        if total > self.max_metadata_bytes {
            return Err(NativeIndexError::Capacity);
        }
        cold.reserved_view_bytes += additional;
        reservation.charge += additional;
        reservation.max_excluded = reservation.max_excluded.max(max_excluded);
        Ok(())
    }

    /// Reserve the worst-case sparse view before any irreversible publication.
    pub(crate) fn reserve_publication_view(
        &self,
        max_excluded: usize,
    ) -> Result<PublicationViewReservation, NativeIndexError> {
        let charge = view_charge(max_excluded)?;
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
        Ok(PublicationViewReservation {
            retention: Arc::clone(&self.retention),
            charge,
            max_excluded,
        })
    }

    /// Check a sparse view against its reservation and move its quota from
    /// reserved to allocated. Called by the writer; the view pins its floor.
    pub(super) fn register_live_view(
        &self,
        stable_floor: &IndexReadCut,
        charge: usize,
        reservation: &mut PublicationViewReservation,
    ) -> Result<(), NativeIndexError> {
        let key = position(stable_floor);
        let mut cold = self.retention.cold.lock();
        self.retention.validate(stable_floor)?;
        cold.reserved_view_bytes -= reservation.charge();
        reservation.consume();
        if charge != 0 {
            cold.view_bytes += charge;
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
        cut: IndexReadCut,
        floor: IndexReadCut,
        latest: Option<MutationVersion>,
    ) -> Result<CaptureLease, NativeIndexError> {
        let retention = &self.retention;
        let mut cold = retention.cold.lock();
        let bounds = retention.bounds.load_full();
        bounds.validate(&cut)?;
        if matches!(cut, IndexReadCut::Sparse(_))
            || matches!(floor, IndexReadCut::Latest | IndexReadCut::Sparse(_))
            || position(&floor) < position(&bounds.floor)
            || position(&floor) > position(&cut)
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
