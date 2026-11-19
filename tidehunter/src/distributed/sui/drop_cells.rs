//! Remote `drop_cells_in_range`: one singleton `DropCells` batch.
//!
//! Native semantics: `from` and `to` are full keys whose reductions are the
//! first and last keys of their cells; the drop clears every key of every
//! reduced cell between them. The remote path reuses native reduction and
//! boundary math (checked), rejects bad input before any slot or WAL byte,
//! and commits one batch under the shared write gate. It never enumerates
//! keys, fetches records or takes the exclusive gate.

use super::{Backend, Db, Result, error};
use crate::db::MAX_KEY_LEN;
use crate::key_shape::{CellRangeError, KeyIndexing, KeySpace};
use std::sync::atomic::Ordering;

impl Db {
    pub(super) fn drop_remote_cells(&self, ks: KeySpace, from: &[u8], to: &[u8]) -> Result<()> {
        let Backend::Remote(remote) = &self.backend else {
            return Err(error("not a remote database"));
        };
        let Some(descriptor) = self
            .shape
            .iter_ks()
            .find(|descriptor| descriptor.id() == ks && !descriptor.destroyed())
        else {
            return Err(error(format!(
                "drop_cells_in_range: unknown keyspace {}",
                ks.0
            )));
        };
        if descriptor.index_key_size().is_none() {
            return Err(error(format!(
                "drop_cells_in_range: keyspace {} requires fixed-length index keys",
                descriptor.name()
            )));
        }
        // Check the full-key size before reduction slices it.
        for (bound, key) in [("from", from), ("to", to)] {
            let valid = key.len() <= MAX_KEY_LEN
                && match descriptor.key_indexing() {
                    KeyIndexing::Fixed(len) => key.len() == *len,
                    KeyIndexing::Reduction(len, range) => {
                        key.len() == *len && range.start <= range.end && range.end <= key.len()
                    }
                    KeyIndexing::Hash | KeyIndexing::VariableLength => true,
                };
            if !valid {
                return Err(error(format!(
                    "drop_cells_in_range: {bound} has invalid key length/reduction"
                )));
            }
        }
        let reduced_from = descriptor.reduce_key(from);
        let reduced_to = descriptor.reduce_key(to);
        let (first_cell, last_cell) = descriptor
            .try_map_key_range_to_cell_range(&reduced_from, &reduced_to)
            .map_err(|rejected| {
                error(match rejected {
                    CellRangeError::VariableLength => format!(
                        "drop_cells_in_range: keyspace {} requires fixed-length index keys",
                        descriptor.name()
                    ),
                    CellRangeError::InvalidFrom => {
                        "drop_cells_in_range: from has invalid key length/reduction".to_owned()
                    }
                    CellRangeError::InvalidTo => {
                        "drop_cells_in_range: to has invalid key length/reduction".to_owned()
                    }
                    CellRangeError::FromNotFirst => {
                        "from_inclusive is not the first key in its cell".to_owned()
                    }
                    CellRangeError::ToNotLast => {
                        "to_inclusive is not the last key in its cell".to_owned()
                    }
                })
            })?;
        if first_cell > last_cell {
            return Err(error("drop_cells_in_range: reversed cell range"));
        }
        let _guard = self.shared_write_gate();
        let sequence = self.operations.fetch_add(1, Ordering::Relaxed);
        let mut operation = [0u8; 16];
        operation[..8].copy_from_slice(&remote.epoch.to_le_bytes());
        operation[8..].copy_from_slice(&sequence.to_le_bytes());
        remote.write_commit.run(|| {
            remote
                .db
                .drop_cells(operation, ks, first_cell, last_cell)
                .map(|_| ())
                .map_err(error)
        })
    }
}
