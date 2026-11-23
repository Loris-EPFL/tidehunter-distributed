use crate::index::index_table::IndexTable;
use crate::wal::position::{IndexPosition, WalPosition};
use minibytes::Bytes;

/// Holds value positions that were relocated.
/// Updates are essentially compare-and-set commands that modify values in
/// the index if they are the same as they were when update was created.
///
/// `P` is the value position; the cut is that position's frontier type, so a
/// batch-version instantiation compares batch versions instead of WAL offsets.
pub struct RelocationUpdates<P: IndexPosition = WalPosition> {
    last_processed: P::Cut,
    updates: Vec<(Bytes, P)>,
}

impl<P: IndexPosition> RelocationUpdates<P> {
    /// `last_processed` is the index frontier captured when the pass started:
    /// an update only re-points an entry below it, so an entry written after
    /// the capture keeps its own (newer) position.
    pub(crate) fn new(last_processed: P::Cut) -> Self {
        Self {
            last_processed,
            updates: Default::default(),
        }
    }

    pub(crate) fn add(&mut self, reduced_key: Bytes, new_value: P) {
        self.updates.push((reduced_key, new_value));
    }

    /// Apply relocation updates to the index table.
    ///
    /// Compare-and-set per key: an entry whose position is not below the
    /// captured frontier was written after the pass started and is left
    /// alone. The entry kind is preserved — a relocation moves bytes, it does
    /// not change whether the write was a tombstone.
    pub(crate) fn apply(self, index: &mut IndexTable<P>) {
        for (reduced_key, new_value) in self.updates {
            index.apply_update(&reduced_key, |v| {
                if P::tagged_position(*v).below(self.last_processed) {
                    let kind = P::kind(*v);
                    *v = P::tag(new_value, kind);
                }
            });
        }
    }

    /// Reduced keys of the updates, in insertion order.
    pub(crate) fn reduced_keys(&self) -> impl Iterator<Item = &Bytes> {
        self.updates.iter().map(|(reduced_key, _)| reduced_key)
    }

    pub fn last_processed(&self) -> P::Cut {
        self.last_processed
    }

    pub fn is_empty(&self) -> bool {
        self.updates.is_empty()
    }
}
