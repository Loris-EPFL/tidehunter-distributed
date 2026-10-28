use super::*;
use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::sync::Arc;

/// A clone shares one registration. The last owner releases the exact read cut.
#[derive(Clone, Debug)]
pub struct IndexReadPin(Arc<ReadPin>);

#[derive(Debug)]
struct ReadPin {
    cut: IndexReadCut,
    retention: Arc<Mutex<Retention>>,
}

impl IndexReadPin {
    pub fn cut(&self) -> IndexReadCut {
        self.0.cut.clone()
    }
}

impl Drop for ReadPin {
    fn drop(&mut self) {
        let mut retention = self.retention.lock();
        let key = position(&self.cut);
        if let Some(count) = retention.pins.get_mut(&key) {
            *count -= 1;
            if *count == 0 {
                retention.pins.remove(&key);
            }
            retention.registrations -= 1;
        }
    }
}

#[derive(Debug)]
pub(super) struct Retention {
    pub floor: IndexReadCut,
    preparing_floor: Option<IndexReadCut>,
    pins: BTreeMap<Option<MutationVersion>, usize>,
    /// Independent registrations, not Arc owners. Each owns one ReadPin
    /// allocation; the per-cut map has at most this many entries.
    registrations: usize,
    transient_bytes: usize,
    view_bytes: usize,
    reserved_view_bytes: usize,
    active: bool,
}

impl Default for Retention {
    fn default() -> Self {
        Self {
            floor: IndexReadCut::BeforeAll,
            preparing_floor: None,
            pins: BTreeMap::new(),
            registrations: 0,
            transient_bytes: 0,
            view_bytes: 0,
            reserved_view_bytes: 0,
            active: false,
        }
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

impl Retention {
    fn validate(&self, cut: &IndexReadCut) -> Result<(), NativeIndexError> {
        if position(cut) < position(&self.floor) {
            return Err(NativeIndexError::ExpiredCut);
        }
        if self
            .preparing_floor
            .as_ref()
            .is_some_and(|floor| position(cut) < position(floor))
        {
            return Err(NativeIndexError::RetentionInProgress);
        }
        Ok(())
    }

    pub(super) fn release_view(&mut self, key: Option<MutationVersion>, charge: usize) {
        let count = self
            .pins
            .get_mut(&key)
            .expect("view retention registration");
        *count -= 1;
        if *count == 0 {
            self.pins.remove(&key);
        }
        self.view_bytes -= charge;
    }
}

/// One capture owns the transient budget and reserves its proposed floor until
/// installation or abort. This also prevents out-of-order image installation.
pub(super) struct CaptureLease {
    retention: Arc<Mutex<Retention>>,
    pub floor: IndexReadCut,
}

impl CaptureLease {
    pub fn install(&self) {
        self.retention.lock().floor = self.floor.clone();
    }
}

impl Drop for CaptureLease {
    fn drop(&mut self) {
        let mut retention = self.retention.lock();
        retention.preparing_floor = None;
        retention.transient_bytes = 0;
        retention.active = false;
    }
}

/// A pre-admitted sparse publication allocation. Dropping it releases its quota.
#[derive(Debug)]
pub(crate) struct PublicationViewReservation {
    retention: Arc<Mutex<Retention>>,
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
}

impl Drop for PublicationViewReservation {
    fn drop(&mut self) {
        if self.charge != 0 {
            self.retention.lock().reserved_view_bytes -= self.charge;
        }
    }
}

fn view_charge(capacity: usize) -> Result<usize, NativeIndexError> {
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

    /// The caller holds the index write guard across preflight and all upgrades.
    pub(crate) fn check_publication_view_capacity(
        &self,
        additional_bytes: usize,
    ) -> Result<(), NativeIndexError> {
        let retention = self.retention.lock();
        let total = retention
            .view_bytes
            .checked_add(retention.reserved_view_bytes)
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
        if !Arc::ptr_eq(&reservation.retention, &self.retention) {
            return Err(NativeIndexError::Invalid(
                "foreign publication view reservation",
            ));
        }
        let additional = reservation.additional_charge(max_excluded)?;
        let mut retention = self.retention.lock();
        let total = retention
            .view_bytes
            .checked_add(retention.reserved_view_bytes)
            .and_then(|n| n.checked_add(additional))
            .ok_or(NativeIndexError::Capacity)?;
        if total > self.max_metadata_bytes {
            return Err(NativeIndexError::Capacity);
        }
        retention.reserved_view_bytes += additional;
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
        let mut retention = self.retention.lock();
        let total = retention
            .view_bytes
            .checked_add(retention.reserved_view_bytes)
            .and_then(|n| n.checked_add(charge))
            .ok_or(NativeIndexError::Capacity)?;
        if total > self.max_metadata_bytes {
            return Err(NativeIndexError::Capacity);
        }
        retention.reserved_view_bytes += charge;
        Ok(PublicationViewReservation {
            retention: Arc::clone(&self.retention),
            charge,
            max_excluded,
        })
    }

    /// Install only after the entire promoted batch is available, while holding
    /// the index write guard. An empty exclusion list restores the cheap prefix.
    pub fn install_live_view(
        &mut self,
        upper: MutationVersion,
        stable_floor: IndexReadCut,
        excluded: Vec<BatchVersion>,
    ) -> Result<(), NativeIndexError> {
        let max_excluded = if excluded.is_empty() {
            0
        } else {
            excluded.capacity()
        };
        let reservation = self.reserve_publication_view(max_excluded)?;
        self.install_reserved_live_view(upper, stable_floor, excluded, reservation)
    }

    pub(crate) fn install_reserved_live_view(
        &mut self,
        upper: MutationVersion,
        stable_floor: IndexReadCut,
        excluded: Vec<BatchVersion>,
        mut reservation: PublicationViewReservation,
    ) -> Result<(), NativeIndexError> {
        if !Arc::ptr_eq(&reservation.retention, &self.retention) {
            return Err(NativeIndexError::Invalid(
                "foreign publication view reservation",
            ));
        }
        let charge = if excluded.is_empty() {
            0
        } else {
            if excluded.capacity() > reservation.max_excluded {
                return Err(NativeIndexError::Invalid(
                    "publication view exceeds reservation",
                ));
            }
            view_charge(excluded.capacity())?
        };
        if charge > reservation.charge {
            return Err(NativeIndexError::Invalid(
                "publication view exceeds reservation",
            ));
        }
        if matches!(stable_floor, IndexReadCut::Latest | IndexReadCut::Sparse(_))
            || position(&stable_floor).is_some_and(|floor| floor > upper)
            || self.latest.is_none_or(|latest| upper > latest)
            || excluded.windows(2).any(|pair| pair[0] >= pair[1])
            || excluded.iter().any(|batch| {
                *batch > upper.batch
                    || position(&stable_floor).is_some_and(|floor| *batch <= floor.batch)
            })
        {
            return Err(NativeIndexError::Invalid("invalid sparse publication view"));
        }
        let key = position(&stable_floor);
        {
            let mut retention = self.retention.lock();
            retention.validate(&stable_floor)?;
            retention.reserved_view_bytes -= reservation.charge;
            reservation.charge = 0;
            if charge != 0 {
                retention.view_bytes += charge;
                *retention.pins.entry(key).or_default() += 1;
            }
        }
        if excluded.is_empty() {
            self.live_view = None;
            return Ok(());
        }
        self.live_view = Some(IndexReadCut::Sparse(Arc::new(SparseIndexView {
            upper,
            floor: key,
            excluded,
            retention: Arc::clone(&self.retention),
            charge,
        })));
        Ok(())
    }

    pub fn validate_cut(&self, cut: IndexReadCut) -> Result<(), NativeIndexError> {
        let cut = self.resolve_cut(cut);
        if let IndexReadCut::Sparse(view) = &cut
            && !Arc::ptr_eq(&view.retention, &self.retention)
        {
            return Err(NativeIndexError::Invalid("foreign sparse view"));
        }
        self.retention.lock().validate(&cut)
    }

    pub fn pin_cut(&self, cut: IndexReadCut) -> Result<IndexReadPin, NativeIndexError> {
        let cut = self.resolve_cut(cut);
        let mut retention = self.retention.lock();
        if let IndexReadCut::Sparse(view) = &cut
            && !Arc::ptr_eq(&view.retention, &self.retention)
        {
            return Err(NativeIndexError::Invalid("foreign sparse view"));
        }
        retention.validate(&cut)?;
        let key = position(&cut);
        // This is a separate registration-count quota, not a shared heap-byte
        // charge: Arc headers and BTree nodes are outside index/capture stats.
        if retention.registrations >= self.max_metadata_bytes / std::mem::size_of::<ReadPin>() {
            return Err(NativeIndexError::Capacity);
        }
        *retention.pins.entry(key).or_default() += 1;
        retention.registrations += 1;
        Ok(IndexReadPin(Arc::new(ReadPin {
            cut,
            retention: Arc::clone(&self.retention),
        })))
    }

    pub fn retention_floor(&self) -> IndexReadCut {
        self.retention.lock().floor.clone()
    }

    pub(super) fn capture_lease(
        &self,
        cut: IndexReadCut,
        floor: IndexReadCut,
    ) -> Result<CaptureLease, NativeIndexError> {
        let mut retention = self.retention.lock();
        retention.validate(&cut)?;
        if matches!(cut, IndexReadCut::Sparse(_))
            || matches!(floor, IndexReadCut::Latest | IndexReadCut::Sparse(_))
            || position(&floor) < position(&retention.floor)
            || position(&floor) > position(&cut)
            || position(&floor)
                > position(
                    &self
                        .latest
                        .map_or(IndexReadCut::BeforeAll, IndexReadCut::At),
                )
        {
            return Err(NativeIndexError::Invalid("nonmonotonic retention floor"));
        }
        if retention.active {
            return Err(NativeIndexError::CheckpointInProgress);
        }
        // Selection and reservation use the same mutex as pin registration.
        // A pre-genesis pin clamps to BeforeAll and prevents any advancement.
        let floor = retention
            .pins
            .first_key_value()
            .filter(|(pin, _)| **pin < position(&floor))
            .map_or(floor, |(pin, _)| {
                pin.map_or(IndexReadCut::BeforeAll, IndexReadCut::At)
            });
        // Historical mapped bytes do not enter either budget. Capture duplicates
        // at most the bounded resident overlay plus the cell directory.
        let transient_bytes = self.resident_bytes();
        if transient_bytes > self.max_metadata_bytes {
            return Err(NativeIndexError::Capacity);
        }
        retention.transient_bytes = transient_bytes;
        retention.active = true;
        retention.preparing_floor = Some(floor.clone());
        Ok(CaptureLease {
            retention: Arc::clone(&self.retention),
            floor,
        })
    }

    pub(super) fn transient_bytes(&self) -> usize {
        // The capture and view allowances are independent. View accounting
        // includes pre-admitted reservations as well as allocated views.
        let retention = self.retention.lock();
        retention
            .transient_bytes
            .saturating_add(retention.view_bytes)
            .saturating_add(retention.reserved_view_bytes)
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
        {
            let retention = index.retention.lock();
            assert_eq!(retention.registrations, 2);
            assert_eq!(retention.pins.len(), 1);
            assert_eq!(retention.pins[&None], 2);
        }
        assert!(matches!(
            index.pin_cut(IndexReadCut::BeforeAll),
            Err(NativeIndexError::Capacity)
        ));
        drop(first);
        assert_eq!(
            index.retention.lock().registrations,
            2,
            "a clone still owns the first registration"
        );
        assert!(matches!(
            index.pin_cut(IndexReadCut::BeforeAll),
            Err(NativeIndexError::Capacity)
        ));
        drop(clone);
        assert_eq!(index.retention.lock().registrations, 1);
        let replacement = index.pin_cut(IndexReadCut::BeforeAll).unwrap();
        assert_eq!(
            index.retention.lock().registrations,
            2,
            "the released quota slot is reusable"
        );
        drop(replacement);
        drop(duplicate);
        let retention = index.retention.lock();
        assert_eq!(retention.registrations, 0);
        assert!(retention.pins.is_empty());
    }
}
