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
        self.0.cut
    }
}

impl Drop for ReadPin {
    fn drop(&mut self) {
        let mut retention = self.retention.lock();
        let key = position(self.cut);
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
            active: false,
        }
    }
}

pub(super) fn position(cut: IndexReadCut) -> Option<MutationVersion> {
    match cut {
        IndexReadCut::BeforeAll => None,
        IndexReadCut::At(version) => Some(version),
        IndexReadCut::Latest => unreachable!("read cuts are resolved before retention"),
    }
}

impl Retention {
    fn validate(&self, cut: IndexReadCut) -> Result<(), NativeIndexError> {
        if position(cut) < position(self.floor) {
            return Err(NativeIndexError::ExpiredCut);
        }
        if self
            .preparing_floor
            .is_some_and(|floor| position(cut) < position(floor))
        {
            return Err(NativeIndexError::RetentionInProgress);
        }
        Ok(())
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
        self.retention.lock().floor = self.floor;
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

impl NativeIndex {
    pub fn validate_cut(&self, cut: IndexReadCut) -> Result<(), NativeIndexError> {
        self.retention.lock().validate(self.resolve_cut(cut))
    }

    pub fn pin_cut(&self, cut: IndexReadCut) -> Result<IndexReadPin, NativeIndexError> {
        let cut = self.resolve_cut(cut);
        let mut retention = self.retention.lock();
        retention.validate(cut)?;
        let key = position(cut);
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
        self.retention.lock().floor
    }

    pub(super) fn capture_lease(
        &self,
        cut: IndexReadCut,
        floor: IndexReadCut,
    ) -> Result<CaptureLease, NativeIndexError> {
        let mut retention = self.retention.lock();
        retention.validate(cut)?;
        if floor == IndexReadCut::Latest
            || position(floor) < position(retention.floor)
            || position(floor) > position(cut)
            || position(floor) > position(self.latest_cut())
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
            .filter(|(pin, _)| **pin < position(floor))
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
        retention.preparing_floor = Some(floor);
        Ok(CaptureLease {
            retention: Arc::clone(&self.retention),
            floor,
        })
    }

    pub(super) fn transient_bytes(&self) -> usize {
        self.retention.lock().transient_bytes
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
