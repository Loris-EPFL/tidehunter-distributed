//! A held read view of a distributed database, and its reads.
//!
//! Native `DbCheckpoint` pins the index frontier with a latch and reads values
//! through the ordinary WAL path; its docs say the view does not hold WAL files
//! from being reclaimed. The hybrid view pins the same frontier on the
//! coordinator index and reads each member from its owner, so a frame a
//! relocation pass reclaimed answers absent: native's result for an unreachable
//! position (`Db::resolve_record_frame` returns `None`, and the checkpoint reads
//! through it).

use super::*;

/// A held read view (native `DbCheckpoint`): the publication cut at the time of
/// the call and the latch that keeps flushes from promoting past it, so a later
/// overwrite does not change what this view reads.
pub struct ReadCheckpoint {
    database: [u8; 16],
    /// The serving epoch of the open that took the view.
    epoch: u64,
    cut: BatchVersion,
    _latch: CutLatch,
}

impl RemoteDatabase {
    /// A read view of everything acknowledged so far (native `DbCheckpoint`):
    /// the publication cut after a drain, latched so no flush promotes past it.
    pub fn checkpoint(&mut self) -> Result<ReadCheckpoint, LocalDbError> {
        self.drain()?;
        let (cut, latch) = self.core.table.pin_checkpoint();
        Ok(ReadCheckpoint {
            database: self.core.config.storage.database,
            epoch: self.core.epoch,
            cut,
            _latch: latch,
        })
    }

    pub fn get_at(
        &mut self,
        ks: KeySpace,
        key: &[u8],
        checkpoint: &ReadCheckpoint,
    ) -> Result<Option<Bytes>, LocalDbError> {
        if checkpoint.database != self.core.config.storage.database {
            return Err(LocalDbError::new("checkpoint belongs to another database"));
        }
        // The latch pins the table of the open that took the view: after a
        // reopen it pins nothing here, and the cut names old-epoch versions.
        if checkpoint.epoch != self.core.epoch {
            return Err(LocalDbError::new(
                "checkpoint belongs to an earlier open of this database",
            ));
        }
        // A checkpoint read never refills the native per-cell LRU: it may see a
        // value older than the live one, and seeding the LRU with it would serve
        // that value to a later live read (native `DbCheckpoint::get` makes the
        // same choice). The LRU is still consulted, under native's guard that a
        // key unchanged since the cut is resident with its as-of value.
        //
        // A frame a pass reclaimed answers absent, as native answers for an
        // unreachable position. The live read keeps its typed eviction.
        match self.get_at_cut(ks, key, checkpoint.cut, false) {
            Err(error) if error.is_evicted_during_read() => Ok(None),
            other => other,
        }
    }
}
