use crate::cell::CellId;
#[cfg(feature = "experimental-distributed")]
use crate::db::DbResult;
#[cfg(feature = "experimental-distributed")]
use crate::index::index_format::IndexIterCaches;
#[cfg(feature = "experimental-distributed")]
use crate::key_shape::{KeySpace, KeySpaceDesc};
use minibytes::Bytes;

pub mod db_iterator;

pub(crate) struct IteratorResult<T> {
    pub cell: Option<CellId>,
    pub key: Bytes,
    pub value: T,
}

impl<T> IteratorResult<T> {
    /// Replaces key and value in the result with provided values and returns new iterator result
    pub fn with_key_value<V>(self, key: Bytes, value: V) -> IteratorResult<V> {
        IteratorResult {
            cell: self.cell,
            key,
            value,
        }
    }
}

/// The index walk a [`db_iterator::DbIterator`] needs when the database's index
/// does not live in a native [`crate::db::Db`].
///
/// This is the complete surface [`db_iterator::IterationSource`] uses: the same
/// three calls the native `Db` and `DbCheckpoint` sources answer. Keeping it to
/// exactly those three is what makes a distributed iterator a *native* iterator
/// — bounds, key reduction, reversal and the skip/stop rules stay in
/// `DbIterator` alone, so a remote database cannot drift from native iteration
/// semantics, and no second walk implementation exists to keep in step.
///
/// The implementation owns whatever state keeps its cut answerable (the hybrid
/// table's held cut latch), so the view lives exactly as long as the iterator
/// that reads it.
#[cfg(feature = "experimental-distributed")]
pub(crate) trait DistributedIterSource: Send + Sync {
    /// Description of `ks`; the walk reads cell bounds and reduction rules here.
    fn ks(&self, ks: KeySpace) -> &KeySpaceDesc;

    /// Next cell after `cell` in iteration order, or `None` past the end.
    fn next_cell(&self, ks: &KeySpaceDesc, cell: &CellId, reverse: bool) -> Option<CellId>;

    /// Next entry at or after `prev_key` within `cell`, reading the value
    /// itself (a distributed index stores positions, not values).
    fn next_entry(
        &self,
        ks: KeySpace,
        cell: CellId,
        prev_key: Option<Bytes>,
        end_cell_exclusive: &Option<CellId>,
        reverse: bool,
        cache: &mut IndexIterCaches,
    ) -> DbResult<Option<IteratorResult<Bytes>>>;
}
