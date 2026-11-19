//! R1 logical pruning lookups (CAPACITY-R0-R1.md §5). Index membership never
//! gates a relocation callback; it gates only whether an observed `Remove`
//! becomes a prune candidate.

use super::*;

/// The exact head of one key at a scalar cut and the cell that holds it.
pub(crate) struct RelocationHead {
    pub cell: CellId,
    /// Runtime birth of the materialized cell (L3); a recreated cell differs.
    pub birth: u64,
    pub reduced_key: Bytes,
    /// Newest selectable reference of any kind, after L3 masks.
    pub head: Option<ValueRecordReference>,
}

impl NativeIndex {
    /// Newest reference of `full_key` at `cut`, including a delete head, with
    /// its cell identity and birth. `None` when the cell is not materialized.
    /// One shard read lock; no allocation besides the owned reduced key.
    pub(crate) fn relocation_head(
        &self,
        keyspace: KeySpace,
        full_key: &[u8],
        cut: &IndexReadCut,
    ) -> Result<Option<RelocationHead>, NativeIndexError> {
        if matches!(cut, IndexReadCut::Latest | IndexReadCut::Sparse(_)) {
            return Err(NativeIndexError::Invalid(
                "relocation head needs a scalar stable cut",
            ));
        }
        self.validate_resolved(cut)?;
        let (_, reduced, cell) = self.route(keyspace, full_key)?;
        let identity = (keyspace, cell);
        let shard = self.shard_for(&identity).read();
        Ok(shard.get(&identity).map(|index| RelocationHead {
            cell: identity.1.clone(),
            birth: index.birth,
            reduced_key: Bytes::copy_from_slice(&reduced),
            head: index.get(&reduced, cut),
        }))
    }
}
