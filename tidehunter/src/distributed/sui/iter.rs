//! Native iterators over the distributed index.
//!
//! The hybrid keeps the index in the coordinator's `LargeTable`, not in a
//! native [`crate::db::Db`], so an ordered walk cannot hold a `Db` or a
//! `DbCheckpoint`. It holds a [`HybridIterSource`] instead: the three calls
//! [`crate::iterators::db_iterator::IterationSource`] makes (`ks`, `next_cell`,
//! `next_entry`). The index walk is the hybrid table's, at the view's cut; the
//! value of each entry is then fetched from its owner by position, as native
//! `Db::next_entry` reads the record of a `WalPosition`. Bounds, key reduction,
//! reversal and the skip/stop rules stay in the native `DbIterator`, so a
//! remote database iterates exactly as a native one does.
//!
//! A live iterator is native's `IterationSource::Db`: unfiltered, no latch.
//! A held [`RemoteCheckpoint`] is native's `DbCheckpoint`: it pins the current
//! publication cut and every read is filtered to versions below it, so a
//! concurrent overwrite stays invisible to the view (plan invariant I6). The
//! latch lives in the source, so it is released exactly when the iterator that
//! reads the view is dropped; there is no separate snapshot lifetime to get
//! wrong.

use super::remote_error;
use crate::cell::CellId;
use crate::db::DbResult;
use crate::distributed::hybrid::loader::CutLatch;
use crate::distributed::hybrid::table::ResolvedValue;
use crate::distributed::remote_db::ConcurrentRemoteDatabase;
use crate::distributed::storage::LocalDbError;
use crate::distributed::types::BatchVersion;
use crate::index::index_format::IndexIterCaches;
use crate::iterators::db_iterator::{DbIterator, IterationSource};
use crate::iterators::{DistributedIterSource, IteratorResult};
use crate::key_shape::{KeySpace, KeySpaceDesc};
use minibytes::Bytes;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// One ordered view over the coordinator index: the live one (`cut: None`) or
/// a latched checkpoint (`cut: Some`).
struct HybridIterSource {
    /// Keeps the database alive for the iterator's lifetime, exactly as the
    /// native sources keep their `Db`.
    db: Arc<ConcurrentRemoteDatabase>,
    /// Versions at or above this cut are invisible. `None` reads the live
    /// index, as native's `IterationSource::Db` does.
    cut: Option<BatchVersion>,
    /// Held for a checkpoint only: promotion may not pass `cut` while it
    /// exists, so every version the view could see stays answerable.
    _latch: Option<CutLatch>,
    /// Entries a held checkpoint skipped after the bounded re-resolve ran (the
    /// file under the position was retired and the same walk step no longer
    /// finds the entry). Native skips these silently too, but a latch is
    /// supposed to keep every visible row readable, so a rise here is the
    /// observable symptom of a latch that failed. Plain tombstones are routine
    /// and not counted. Shared with the database's timing metrics; `None` for a
    /// live view, where a concurrent delete is an ordinary reason to skip.
    latched_skips: Option<Arc<AtomicU64>>,
    /// Rows returned by iterators of this database: the `scan_records` counter
    /// the Sui metrics sampler reads from the adapter.
    scanned: Arc<AtomicU64>,
}

/// The live ordered view of `ks`: no latch, so writes published while the
/// iterator runs may become visible, which is native's live-iterator contract.
pub(super) fn live_iterator(
    db: &Arc<ConcurrentRemoteDatabase>,
    scanned: Arc<AtomicU64>,
    ks: KeySpace,
) -> DbIterator {
    let source = HybridIterSource {
        db: Arc::clone(db),
        cut: None,
        _latch: None,
        latched_skips: None,
        scanned,
    };
    DbIterator::new(IterationSource::Distributed(Arc::new(source)), ks)
}

/// A held checkpoint of a distributed database.
///
/// Created by `Db::checkpoint`; consuming it into an iterator moves the pinned
/// cut into the iterator, so the latch is held for exactly the view's life.
pub struct RemoteCheckpoint {
    source: Arc<HybridIterSource>,
}

impl RemoteCheckpoint {
    /// Pin the current publication cut and build the view that reads it.
    ///
    /// `pin_checkpoint` is the hybrid's native `DbCheckpoint` latch: it returns
    /// the monotone publication cut together with a latch that keeps promotion
    /// from passing it (see `HybridTable::pin_checkpoint` for why the cut is the
    /// publication frontier and not `last_processed`, which a latch can lower).
    pub(super) fn new(
        db: Arc<ConcurrentRemoteDatabase>,
        scanned: Arc<AtomicU64>,
        latched_skips: Arc<AtomicU64>,
    ) -> Self {
        let (cut, latch) = db.hybrid_table().pin_checkpoint();
        Self {
            source: Arc::new(HybridIterSource {
                db,
                cut: Some(cut),
                _latch: Some(latch),
                latched_skips: Some(latched_skips),
                scanned,
            }),
        }
    }

    pub(super) fn iterator(self, ks: KeySpace) -> DbIterator {
        DbIterator::new(IterationSource::Distributed(self.source), ks)
    }
}

impl DistributedIterSource for HybridIterSource {
    fn ks(&self, ks: KeySpace) -> &KeySpaceDesc {
        self.db.hybrid_table().ks_desc(ks)
    }

    fn next_cell(&self, ks: &KeySpaceDesc, cell: &CellId, reverse: bool) -> Option<CellId> {
        self.db.hybrid_table().next_cell(ks, cell, reverse)
    }

    fn next_entry(
        &self,
        ks: KeySpace,
        cell: CellId,
        prev_key: Option<Bytes>,
        end_cell_exclusive: &Option<CellId>,
        reverse: bool,
        cache: &mut IndexIterCaches,
    ) -> DbResult<Option<IteratorResult<Bytes>>> {
        // The fail-stop gate the old row-at-a-time scan applied: a failed
        // authority must stop an open iterator, not keep serving its view.
        self.db.validate_cached_read().map_err(remote_error)?;
        let table = self.db.hybrid_table();
        let reader = self.db.value_reader();
        let latched = self.cut.is_some();
        // A live scan refills the LRU from the record it read, as native
        // `Db::next_entry` does; a latched scan never does (native
        // `DbCheckpoint::next_entry`). Without a value cache there is nothing to
        // fill.
        let refill = self.cut.is_none() && table.ks_desc(ks).value_cache_size().is_some();
        // The cell stamp the latest walk step started from, taken before that
        // step. A refill is dropped when a write reached the cell since.
        let epoch = std::cell::RefCell::new(None::<(CellId, u64)>);
        let mut skipped = 0;
        let entry = next_resolved(
            cell,
            prev_key,
            |cell, prev_key, fresh| {
                // The re-resolve walks with empty caches: the first read failed,
                // so nothing cached by the first walk step can be trusted.
                let mut scratch;
                let cache = if fresh {
                    scratch = IndexIterCaches::new();
                    &mut scratch
                } else {
                    &mut *cache
                };
                if refill {
                    *epoch.borrow_mut() =
                        Some((cell.clone(), table.value_epoch_for_cell(ks, cell)));
                }
                table.next_entry(
                    ks,
                    cell.clone(),
                    prev_key.clone(),
                    end_cell_exclusive,
                    reverse,
                    cache,
                    self.cut,
                )
            },
            |key, resolved, found_cell, resolve| match resolved {
                // A live walk already answered from the native per-cell LRU: the
                // row needs no owner contact, exactly as native `Db::next_entry`.
                ResolvedValue::Resident { full_key, value } => Ok(Some((full_key, value))),
                ResolvedValue::Position(position) => {
                    let row =
                        reader.read_entry_with_resolve(ks, key, position, resolve, latched)?;
                    if let Some((full_key, value)) = &row
                        && let Some((walked_cell, stamp)) = epoch.borrow().clone()
                        && found_cell == Some(&walked_cell)
                    {
                        table.refill_value(ks, key.clone(), full_key.clone(), value.clone(), stamp);
                    }
                    Ok(row)
                }
            },
            &mut skipped,
        );
        if let Some(counter) = self.latched_skips.as_ref().filter(|_| skipped != 0) {
            // A pure statistic: no other memory is published through it.
            counter.fetch_add(skipped, Ordering::Relaxed);
        }
        if matches!(entry, Ok(Some(_))) {
            self.scanned.fetch_add(1, Ordering::Relaxed);
        }
        entry.map_err(remote_error)
    }
}

/// The native `Db::next_entry` skip loop over an index walk and a value fetch.
///
/// `walk(cell, prev_key, fresh)` is one index step; `fresh` asks for empty
/// iterator caches (the re-resolve). Its value is the index answer: a member
/// position, or a value the native per-cell LRU already holds. `fetch(key,
/// answer, found_in, resolve)` produces the entry's full key and value, calling
/// `resolve` at most once to repeat the same walk step if the position's file
/// was retired meanwhile; `found_in` is the cell the walk found the answer in,
/// which a live step compares with the cell whose value epoch it sampled. A
/// fetch that finds a tombstone, or an entry no longer held, yields
/// `None` and the loop skips it, resuming in the cell the entry was found in,
/// after its key. The walk ends only when the index walk itself reports no
/// further entry, so a skipped last entry of a cell never truncates the
/// iteration. This is the same skip native `Db::next_entry` and
/// `DbCheckpoint::next_entry` apply to an unreadable record, and it applies to a
/// latched view too: a relocation filter may remove an index entry while a
/// checkpoint is held, exactly as native allows.
/// `skipped` counts the entries skipped *after the bounded re-resolve ran*:
/// the file under a position was retired and the same walk step no longer finds
/// the entry. A plain tombstone is routine on every scan and is not counted, so
/// for a latched view the counter isolates the one case that can mean the latch
/// failed to keep a visible row.
fn next_resolved<P>(
    mut cell: CellId,
    mut prev_key: Option<Bytes>,
    mut walk: impl FnMut(
        &CellId,
        &Option<Bytes>,
        bool,
    ) -> Result<Option<IteratorResult<P>>, LocalDbError>,
    mut fetch: impl FnMut(
        &Bytes,
        P,
        Option<&CellId>,
        &mut dyn FnMut() -> Result<Option<P>, LocalDbError>,
    ) -> Result<Option<(Bytes, Bytes)>, LocalDbError>,
    skipped: &mut u64,
) -> Result<Option<IteratorResult<Bytes>>, LocalDbError> {
    loop {
        let Some(found) = walk(&cell, &prev_key, false)? else {
            return Ok(None);
        };
        let mut re_resolved = false;
        let entry = fetch(&found.key, found.value, found.cell.as_ref(), &mut || {
            re_resolved = true;
            Ok(walk(&cell, &prev_key, true)?
                .filter(|again| again.key == found.key)
                .map(|again| again.value))
        })?;
        match entry {
            Some((key, value)) => {
                return Ok(Some(IteratorResult {
                    cell: found.cell,
                    key,
                    value,
                }));
            }
            None => {
                if re_resolved {
                    *skipped += 1;
                }
                // Native always reports the cell an entry was found in; if a
                // walk ever omits it, keep walking the same cell past the key.
                if let Some(found_in) = found.cell {
                    cell = found_in;
                }
                prev_key = Some(found.key);
            }
        }
    }
}

#[cfg(test)]
#[path = "iter_tests.rs"]
mod tests;
