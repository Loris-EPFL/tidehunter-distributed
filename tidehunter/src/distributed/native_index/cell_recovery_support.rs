//! Index side of option C mixed-cut recovery (checkpoint option C spec §8.1
//! steps 5, 6 and 8), used by `remote_db::cell_recovery`.
//!
//! Recovery owns the index alone while it replays: no reader, pin, flusher or
//! serving writer exists yet. So these calls compare against cell contents
//! above the publication watermark without the watermark check that reads
//! need. They take one shard lock at a time, or the writer then one shard.

use super::cell_contract::PrefixCut;
use super::*;

impl NativeIndex {
    /// The retained reference of exactly `version` for `reduced_key` in one
    /// cell: base and every overlay source, tombstones included. Recovery
    /// compares a claimed covered member with it before the batch publishes.
    pub(crate) fn exact_member(
        &self,
        keyspace: KeySpace,
        cell: &CellId,
        reduced_key: &[u8],
        version: MutationVersion,
    ) -> Option<ValueRecordReference> {
        let identity = (keyspace, cell.clone());
        self.shard_for(&identity)
            .read()
            .get(&identity)?
            .exact(reduced_key, version)
    }

    /// Number of base references of one cell whose batch is above `floor`,
    /// or `None` for an unknown cell. At the start of replay, these are the
    /// references that whole suffix batches must meet as covered members.
    /// Walks the whole base under the shard read lock: recovery only.
    pub(crate) fn base_references_above(
        &self,
        keyspace: KeySpace,
        cell: &CellId,
        floor: PrefixCut,
    ) -> Result<Option<u64>, NativeIndexError> {
        let identity = (keyspace, cell.clone());
        let shard = self.shard_for(&identity).read();
        let Some(cell) = shard.get(&identity) else {
            return Ok(None);
        };
        let Some(block) = &cell.base else {
            return Ok(Some(0));
        };
        let mut above = 0u64;
        for row in 0..block.count {
            for bytes in block.entry(row).1.chunks_exact(REFERENCE_BYTES) {
                if !floor.contains(decode_reference(bytes)?.batch_version) {
                    above += 1;
                }
            }
        }
        Ok(Some(above))
    }
}

impl IndexWriter<'_> {
    /// Raise the promoted frontier to `last`, the final mutation of a
    /// replayed batch whose members are all present (applied or compared
    /// equal to a cell base). Promotion raises it only for inserted
    /// references, so without this a fully covered batch would stay hidden
    /// from `publish_latest`. Monotonic.
    pub(crate) fn advance_replayed(&mut self, last: MutationVersion) {
        self.state.latest = Some(self.state.latest.map_or(last, |latest| latest.max(last)));
    }

    /// Test fixture: give an index built by publication and per-cell flushes
    /// the state the format-5 loader documents. Root cells missing from the
    /// index are inserted with no base; the new-cell floor is `F`; the
    /// promoted frontier and watermark are `F`, not the newest frame cut.
    /// Every base must have been encoded at its root frame cut.
    #[cfg(test)]
    pub(crate) fn emulate_loaded_root(
        &mut self,
        root: &super::cell_contract::CellRoot,
    ) -> Result<(), NativeIndexError> {
        let index = self.index;
        let state = &mut *self.state;
        let floor = root.header.replay_floor;
        for entry in &root.cells {
            let identity = (entry.keyspace, entry.cell.clone());
            let mut shard = index.shard_for(&identity).write();
            let frame_cut = shard
                .get(&identity)
                .and_then(|cell| cell.base.as_ref())
                .map(|block| block.cut.clone());
            if frame_cut != entry.base.as_ref().map(|base| base.frame_cut.index_cut()) {
                return Err(NativeIndexError::Invalid("fixture base differs from root"));
            }
            if let std::collections::hash_map::Entry::Vacant(slot) = shard.entry(identity) {
                let charge = cell_directory_charge(index.mode, &entry.cell);
                index.check_capacity(charge)?;
                super::shards::add(&index.accounting.directory, charge);
                state.cell_order.push(slot.key().clone());
                slot.insert(CellIndex {
                    generation: state.generation,
                    flush: index.new_cell_flush(entry.coverage),
                    ..Default::default()
                });
            }
        }
        state.cells.floor = floor;
        state.latest = match floor {
            PrefixCut::BeforeAll => None,
            PrefixCut::Through(batch) => Some(MutationVersion {
                batch,
                operation_index: u32::MAX,
            }),
        };
        index
            .published
            .store(std::sync::Arc::new(floor.index_cut()));
        Ok(())
    }
}

impl NativeIndex {
    /// Test encoder of a capture with history floor `BeforeAll`: every
    /// reference through the target, rows sorted. The block is marked mapped
    /// so accounting treats it as a sealed mapped segment (pageable, not
    /// resident), as the production store installs it; no file is written.
    /// Needs no index, like a flusher that owns only its store.
    #[cfg(test)]
    pub(crate) fn encode_mapped_test_base(
        shape: &KeyShape,
        database: [u8; 16],
        source: &cell_flush::CellEncodeSource<'_>,
    ) -> Result<cell_flush::NewCellBase, NativeIndexError> {
        use std::collections::BTreeMap;
        if source.floor != IndexReadCut::BeforeAll {
            return Err(NativeIndexError::Invalid(
                "test encoder needs floor BeforeAll",
            ));
        }
        let mut rows: BTreeMap<Vec<u8>, Vec<ValueRecordReference>> = BTreeMap::new();
        if let Some(base) = source.base {
            for row in 0..base.count {
                let (key, versions) = base.entry(row);
                for bytes in versions.chunks_exact(REFERENCE_BYTES) {
                    rows.entry(key.to_vec())
                        .or_default()
                        .push(decode_reference(bytes)?);
                }
            }
        }
        for overlay in [source.carry, Some(&**source.active)].into_iter().flatten() {
            for (key, values) in overlay {
                for value in values.iter().filter(|value| source.cut.contains(value)) {
                    rows.entry(key.to_vec()).or_default().push(*value);
                }
            }
        }
        if rows.is_empty() {
            return Ok(cell_flush::NewCellBase::empty());
        }
        let mut payload = Vec::new();
        payload.extend_from_slice(&(rows.len() as u32).to_be_bytes());
        let directory = payload.len();
        payload.resize(directory + 4 * rows.len(), 0);
        for (row, (key, versions)) in rows.iter_mut().enumerate() {
            versions.sort_by_key(|version| version.mutation_version());
            let offset = payload.len() as u32;
            payload[directory + 4 * row..directory + 4 * row + 4]
                .copy_from_slice(&offset.to_be_bytes());
            payload.extend_from_slice(&(key.len() as u32).to_be_bytes());
            payload.extend_from_slice(key);
            payload.extend_from_slice(&(versions.len() as u32).to_be_bytes());
            for version in versions.iter() {
                encode_reference(version, &mut payload);
            }
        }
        let (keyspace, cell) = source.identity;
        Ok(cell_flush::NewCellBase::frame(SortedBlock::validated(
            payload.into(),
            true,
            database,
            *keyspace,
            shape.ks(*keyspace),
            cell,
            source.cut.clone(),
        )?))
    }
}
