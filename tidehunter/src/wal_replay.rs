//! WAL replay driver.
//!
//! Drives a [`ReplaySource`] through every replay frame and accumulates
//! per-cell writes into a [`ReplayBuffer`]. At end-of-stream the buffer is
//! drained into the [`LargeTable`] in bulk via `apply_replay_buffer`:
//!
//!   - one row lock per mutex shard instead of one per WAL record,
//!   - one per-cell sort + flat-buffer build instead of per-record BTreeMap
//!     inserts (see `replay_buffer` for the per-cell accumulator).
//!
//! The driver is generic over the value position `P`: natively it consumes a
//! [`WalIterator`] over WAL offsets, and a distributed coordinator consumes
//! its owner streams, with the batch-version position from
//! `distributed::hybrid`. Only the source differs; frame handling, batch
//! accounting and the apply path are shared.
//!
//! ## BatchStart handling
//!
//! A `WalEntry::BatchStart(N)` declares the next `N` frames as a single
//! atomic unit — if any of them is missing (CRC fail or EOF before the
//! count is reached) the whole batch must be discarded. The straightforward
//! approach was to buffer those `N` entries into a `VecDeque` and only
//! dispatch once the batch completed; profiling showed the deque was a
//! ~26% bottleneck because its 88-byte slots get evicted from L1 between
//! the push pass and the drain pass.
//!
//! Instead we apply each in-batch record inline, the same as any other
//! WAL record, while remembering the BatchStart's position. If the batch
//! is interrupted, `apply_replay_buffer` receives that position as a cutoff
//! and silently drops any per-cell entries whose position does not precede
//! it. Per-cell entries are pushed in write order — the per-cell buffer is
//! already sorted — so the cutoff translates to a single `partition_point` +
//! `truncate` at apply time.
//!
//! Lives in its own module so `db.rs` doesn't carry the loop body.

use crate::compressed_batch::{InnerIter, decompress_wal_entry};
use crate::context::KsContextVec;
use crate::db::{DbError, WalEntry};
use crate::key_shape::KeyShape;
use crate::large_table::{LargeTable, Loader};
use crate::metrics::Metrics;
use crate::replay_buffer::ReplayBuffer;
use crate::wal::position::{IndexPosition, WalPosition};
use crate::wal::{WalError, WalIterator, WalWriter};

/// One unit of replay input, already decoded into a [`WalEntry`]. `P` is the
/// value position: a WAL offset natively, a batch version for the distributed
/// coordinator.
pub(crate) enum ReplayFrame<P: IndexPosition> {
    /// Opens an atomic group of `count` entries. `position` is the group's
    /// first position; it is both the rewind point when the group is torn and
    /// the cutoff below which already-applied entries must be discarded.
    BatchStart { position: P, count: u32 },
    /// One entry at `position`. A `BatchStart` group's members carry their own
    /// positions in ascending order.
    Entry { position: P, entry: WalEntry },
}

/// A replay input stream.
///
/// Implementations own the transport detail (a local WAL iterator, an owner
/// RPC pull) and report end-of-input as `Ok(None)`. A torn tail is not an
/// error: the source decides what counts as an acceptable end (natively the
/// first bad CRC or end-of-WAL; distributed, a Version-B-truncated tail).
pub(crate) trait ReplaySource<P: IndexPosition> {
    type Error: std::fmt::Debug;
    /// What `finish` yields — natively the writer positioned after replay.
    type Output;

    fn next(&mut self) -> Result<Option<ReplayFrame<P>>, Self::Error>;

    /// Consumes the source. `cutoff` is `Some(position)` when replay ended
    /// inside a `BatchStart` group, so a source that must rewind its writer to
    /// the group's first position receives it here.
    fn finish(self, cutoff: Option<P>) -> Result<Self::Output, Self::Error>;
}

/// Replay failure: the source failed, or applying an entry to the index did.
#[derive(Debug)]
pub(crate) enum ReplayError<E> {
    Source(E),
    Db(DbError),
}

impl<E> ReplayError<E> {
    /// Collapse into the database error type. Native replay's source error is
    /// [`WalError`], which converts; a source with its own error type reports
    /// through [`std::fmt::Display`] instead.
    pub(crate) fn into_db_error(self) -> DbError
    where
        E: Into<DbError>,
    {
        match self {
            Self::Source(error) => error.into(),
            Self::Db(error) => error,
        }
    }
}

impl<E: std::fmt::Display> std::fmt::Display for ReplayError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Source(error) => write!(f, "replay source failed: {error}"),
            Self::Db(error) => write!(f, "replay apply failed: {error:?}"),
        }
    }
}

/// The native source: a WAL iterator over the value WAL.
///
/// Replay stops at the first bad CRC or at the end of the WAL, which is the
/// native recovery contract: a tail that does not verify is not applied.
impl ReplaySource<WalPosition> for WalIterator {
    type Error = WalError;
    type Output = WalWriter;

    fn next(&mut self) -> Result<Option<ReplayFrame<WalPosition>>, Self::Error> {
        let (position, raw_entry) = match WalIterator::next(self) {
            Ok(frame) => frame,
            Err(WalError::Crc(_) | WalError::EndOfWal) => return Ok(None),
            Err(error) => return Err(error),
        };
        Ok(Some(match WalEntry::from_bytes(raw_entry) {
            WalEntry::BatchStart(count) => ReplayFrame::BatchStart { position, count },
            entry => ReplayFrame::Entry { position, entry },
        }))
    }

    fn finish(self, cutoff: Option<WalPosition>) -> Result<Self::Output, Self::Error> {
        Ok(self.into_writer(cutoff.map(|position| position.offset())))
    }
}

pub(crate) fn replay_wal<P: IndexPosition, S: ReplaySource<P>, L: Loader<Position = P> + Sync>(
    contexts: &KsContextVec,
    large_table: &LargeTable<P>,
    key_shape: &KeyShape,
    mut source: S,
    metrics: &Metrics,
    loader: &L,
) -> Result<S::Output, ReplayError<S::Error>> {
    let mut buffer = ReplayBuffer::<P>::new(key_shape);
    // Number of records still expected inside the currently-open BatchStart
    // group. `0` means we're not inside a batch.
    let mut batch_remaining: u32 = 0;
    // Position of the BatchStart frame. `Some(_)` only while a batch is open;
    // serves both as the `finish` rewind argument and as the apply-time cutoff
    // if replay terminates with a partial batch.
    let mut batch_start_position: Option<P> = None;

    while let Some(frame) = source.next().map_err(ReplayError::Source)? {
        let (position, entry) = match frame {
            ReplayFrame::BatchStart { position, count } => {
                // A batch opened inside another batch is a writer bug, exactly
                // as an unexpected entry kind inside a batch is: keep the same
                // loud failure the entry-level match used to produce.
                if batch_remaining > 0 {
                    panic!(
                        "encountered entry BatchStart({count}) at position {position:?} during replay, while expected record or tombstone"
                    );
                }
                // `BatchStart(0)` is unreachable from the writer: only
                // `Db::write_batch_into_wal` (`db.rs:649`) constructs a
                // `BatchStart`, with `entries.len() as u32`, and
                // `Db::do_write_batch` (`db.rs:439-441`) short-circuits empty
                // batches before that call. CRC has already validated this
                // frame's bytes, so a 0 here is a writer bug, not random
                // corruption. Asserting loudly is strictly better than
                // skipping silently — a silent skip would mask the bug and a
                // forged 0 here would still be catastrophic anyway (it would
                // latch `batch_start_position` forever and the apply-time
                // cutoff would drop every record past this frame).
                assert!(
                    count > 0,
                    "WalEntry::BatchStart(0) at {position:?} — writer invariant violated"
                );
                batch_start_position = Some(position);
                batch_remaining = count;
                continue;
            }
            ReplayFrame::Entry { position, entry } => (position, entry),
        };

        // Inside a BatchStart group: apply inline and tick down the counter.
        // The `batch_start_position` carried forward lets us drop these
        // records at apply time if the batch never completes.
        if batch_remaining > 0 {
            match entry {
                WalEntry::Record(ks, k, _v, relocated) => {
                    metrics.replayed_wal_records.inc();
                    let context = contexts.ks_context(ks);
                    if !relocated && !context.destroyed() {
                        let reduced_key = context.ks_config.reduced_key_bytes(k);
                        let cell = context.ks_config.cell_id(&reduced_key);
                        buffer.insert(ks, cell, reduced_key, position);
                    }
                }
                WalEntry::Remove(ks, k) => {
                    metrics.replayed_wal_records.inc();
                    let context = contexts.ks_context(ks);
                    if !context.destroyed() {
                        let reduced_key = context.ks_config.reduced_key_bytes(k);
                        let cell = context.ks_config.cell_id(&reduced_key);
                        buffer.remove(ks, cell, reduced_key, position);
                    }
                }
                other => panic!(
                    "encountered entry {other:?} at position {position:?} during replay, while expected record or tombstone"
                ),
            }
            batch_remaining -= 1;
            if batch_remaining == 0 {
                batch_start_position = None;
            }
            continue;
        }

        match entry {
            WalEntry::Record(ks, k, _v, relocated) => {
                metrics.replayed_wal_records.inc();
                if relocated {
                    // Nothing needs to be done for the relocated record
                    continue;
                }
                let context = contexts.ks_context(ks);
                if context.destroyed() {
                    continue;
                }
                let reduced_key = context.ks_config.reduced_key_bytes(k);
                let cell = context.ks_config.cell_id(&reduced_key);
                buffer.insert(ks, cell, reduced_key, position);
            }
            WalEntry::Index(_ks, _bytes) => {
                unreachable!("Should not have index entries in wal");
            }
            WalEntry::Remove(ks, k) => {
                metrics.replayed_wal_records.inc();
                let context = contexts.ks_context(ks);
                if context.destroyed() {
                    continue;
                }
                let reduced_key = context.ks_config.reduced_key_bytes(k);
                let cell = context.ks_config.cell_id(&reduced_key);
                buffer.remove(ks, cell, reduced_key, position);
            }
            WalEntry::BatchStart(_) => {
                // A source reports batch boundaries as `ReplayFrame`, never as
                // an entry: only the source knows the group's anchor position.
                unreachable!("replay source yielded BatchStart as an entry");
            }
            WalEntry::DropCells(ks, from_cell, to_cell) => {
                metrics.replayed_wal_records.inc();
                let context = contexts.ks_context(ks);
                if context.destroyed() {
                    continue;
                }
                // Drop on both sides: the buffer (so pre-drop writes don't
                // resurrect at apply time) and `large_table` (in case the
                // cells exist as Unloaded entries from a CR snapshot loaded
                // at replay start).
                buffer.drop_cells_in_range(ks, &from_cell, &to_cell);
                large_table
                    .drop_cells_in_range(context, &from_cell, &to_cell)
                    .map_err(ReplayError::Db)?;
            }
            entry @ WalEntry::CompressedBatch(..) => {
                // A compressed batch is one durable WAL frame — either it
                // CRC-verifies (full batch present) or it does not. There is
                // no half-state, so we apply every inner entry
                // unconditionally; all of them share `position`.
                let decompressed = decompress_wal_entry(entry).expect("matched above");
                for inner in InnerIter::new(decompressed) {
                    metrics.replayed_wal_records.inc();
                    match inner {
                        WalEntry::Record(ks, key, _value, _relocated) => {
                            let context = contexts.ks_context(ks);
                            if context.destroyed() {
                                continue;
                            }
                            let reduced_key = context.ks_config.reduced_key_bytes(key);
                            let cell = context.ks_config.cell_id(&reduced_key);
                            buffer.insert(ks, cell, reduced_key, position);
                        }
                        WalEntry::Remove(ks, key) => {
                            let context = contexts.ks_context(ks);
                            if context.destroyed() {
                                continue;
                            }
                            let reduced_key = context.ks_config.reduced_key_bytes(key);
                            let cell = context.ks_config.cell_id(&reduced_key);
                            buffer.remove(ks, cell, reduced_key, position);
                        }
                        other => panic!(
                            "Unexpected inner entry in CompressedBatch at {position:?}: {other:?}"
                        ),
                    }
                }
            }
        }
    }

    // If replay ended mid-batch, anything we already buffered at or after
    // `batch_start_position` is from an incomplete batch and must be
    // discarded. `None` (= complete or no batch) keeps everything.
    large_table.apply_replay_buffer(buffer, batch_start_position, loader);
    source
        .finish(batch_start_position)
        .map_err(ReplayError::Source)
}

#[cfg(test)]
mod tests;
