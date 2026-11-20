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
            |key, position, resolve| reader.read_entry_with_resolve(ks, key, position, resolve),
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
/// iterator caches (the re-resolve). `fetch(key, position, resolve)` reads the
/// entry's full key and value, calling `resolve` at most once to repeat the same
/// walk step if the position's file was retired meanwhile. A fetch that finds a
/// tombstone, or an entry no longer held, yields `None` and the loop skips it,
/// resuming in the cell the entry was found in, after its key. The walk ends
/// only when the index walk itself reports no further entry, so a skipped last
/// entry of a cell never truncates the iteration. This is the same skip native
/// `Db::next_entry` and `DbCheckpoint::next_entry` apply to an unreadable
/// record, and it applies to a latched view too: a relocation filter may remove
/// an index entry while a checkpoint is held, exactly as native allows.
/// `skipped` counts the entries skipped *after the bounded re-resolve ran*:
/// the file under a position was retired and the same walk step no longer finds
/// the entry. A plain tombstone is routine on every scan and is not counted, so
/// for a latched view the counter isolates the one case that can mean the latch
/// failed to keep a visible row.
fn next_resolved<P: Copy>(
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
        &mut dyn FnMut() -> Result<Option<P>, LocalDbError>,
    ) -> Result<Option<(Bytes, Bytes)>, LocalDbError>,
    skipped: &mut u64,
) -> Result<Option<IteratorResult<Bytes>>, LocalDbError> {
    loop {
        let Some(found) = walk(&cell, &prev_key, false)? else {
            return Ok(None);
        };
        let mut re_resolved = false;
        let entry = fetch(&found.key, found.value, &mut || {
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
mod tests {
    use super::*;
    use std::cell::Cell;

    type Fetched = Result<Option<(Bytes, Bytes)>, LocalDbError>;
    type Resolve<'a> = &'a mut dyn FnMut() -> Result<Option<u32>, LocalDbError>;

    /// Position 0 is a tombstone; any other position `p` holds the value `[p]`.
    fn rows(keys: &[u8], tombstones: &[u8]) -> Vec<(u8, u32)> {
        keys.iter()
            .map(|key| {
                let position = if tombstones.contains(key) {
                    0
                } else {
                    u32::from(*key) + 1
                };
                (*key, position)
            })
            .collect()
    }

    /// One in-memory cell walked strictly after (forward) or before (reverse)
    /// `prev_key`. `report_cell` false models a walk that omits the cell.
    fn walk_over(
        rows: Vec<(u8, u32)>,
        reverse: bool,
        report_cell: bool,
    ) -> impl FnMut(&CellId, &Option<Bytes>, bool) -> Result<Option<IteratorResult<u32>>, LocalDbError>
    {
        move |cell, prev_key, _fresh| {
            let prev = prev_key.as_ref().map(|key| key[0]);
            let row = if reverse {
                rows.iter()
                    .rev()
                    .find(|(key, _)| prev.is_none_or(|prev| *key < prev))
            } else {
                rows.iter()
                    .find(|(key, _)| prev.is_none_or(|prev| *key > prev))
            };
            Ok(row.map(|(key, position)| IteratorResult {
                cell: report_cell.then(|| cell.clone()),
                key: Bytes::from(vec![*key]),
                value: *position,
            }))
        }
    }

    fn fetch_values(key: &Bytes, position: u32, _resolve: Resolve<'_>) -> Fetched {
        Ok((position != 0).then(|| (key.clone(), Bytes::from(vec![position as u8]))))
    }

    /// Drive `next_resolved` the way `DbIterator` does: continue from the cell
    /// and key of the previous result.
    fn drain<P: Copy>(
        mut walk: impl FnMut(
            &CellId,
            &Option<Bytes>,
            bool,
        ) -> Result<Option<IteratorResult<P>>, LocalDbError>,
        mut fetch: impl FnMut(&Bytes, P, &mut dyn FnMut() -> Result<Option<P>, LocalDbError>) -> Fetched,
    ) -> (Vec<(u8, u8)>, u64) {
        let mut skipped = 0;
        let mut out = Vec::new();
        let (mut cell, mut prev_key) = (CellId::Integer(0), None);
        while let Some(entry) = next_resolved(
            cell.clone(),
            prev_key.clone(),
            &mut walk,
            &mut fetch,
            &mut skipped,
        )
        .unwrap()
        {
            out.push((entry.key[0], entry.value[0]));
            cell = entry.cell.unwrap_or(cell);
            prev_key = Some(entry.key);
        }
        (out, skipped)
    }

    fn keys(rows: Vec<(u8, u32)>, reverse: bool, report_cell: bool) -> Vec<u8> {
        drain(walk_over(rows, reverse, report_cell), fetch_values)
            .0
            .into_iter()
            .map(|(key, _)| key)
            .collect()
    }

    #[test]
    fn only_skips_after_a_re_resolve_are_counted_for_the_latched_view_metric() {
        // Plain tombstones are routine: skipped, never counted.
        let (rows_seen, skipped) = drain(
            walk_over(rows(&[1, 2, 3, 4, 5], &[2, 4]), false, true),
            fetch_values,
        );
        assert_eq!(rows_seen.len(), 3);
        assert_eq!(skipped, 0, "tombstones are not latch failures");
        // An entry whose file was retired and that the re-walk no longer finds
        // is the counted case.
        let gone = |key: &Bytes, position: u32, resolve: Resolve<'_>| -> Fetched {
            if key[0] == 2 || key[0] == 4 {
                return Ok(resolve()?
                    .filter(|again| *again != position)
                    .map(|again| (key.clone(), Bytes::from(vec![again as u8]))));
            }
            fetch_values(key, position, resolve)
        };
        let (rows_seen, skipped) = drain(walk_over(rows(&[1, 2, 3, 4, 5], &[]), false, true), gone);
        assert_eq!(rows_seen.len(), 3);
        assert_eq!(skipped, 2);
        let (_, none) = drain(walk_over(rows(&[1, 2, 3], &[]), true, true), fetch_values);
        assert_eq!(none, 0, "a walk with nothing to skip counts nothing");
    }

    #[test]
    fn a_tombstone_between_live_keys_is_skipped_in_both_directions() {
        for report_cell in [true, false] {
            let rows = rows(&[1, 2, 3, 4, 5], &[3]);
            assert_eq!(keys(rows.clone(), false, report_cell), [1, 2, 4, 5]);
            assert_eq!(keys(rows, true, report_cell), [5, 4, 2, 1]);
        }
    }

    #[test]
    fn a_skipped_last_entry_ends_the_walk_without_dropping_live_keys() {
        for report_cell in [true, false] {
            assert_eq!(
                keys(rows(&[1, 2, 3], &[3]), false, report_cell),
                [1, 2],
                "forward, last entry skipped"
            );
            assert_eq!(
                keys(rows(&[1, 2, 3], &[1]), true, report_cell),
                [3, 2],
                "reverse, last entry skipped"
            );
        }
    }

    #[test]
    fn consecutive_skipped_entries_do_not_end_the_walk() {
        let rows = rows(&[1, 2, 3, 4, 5], &[2, 3, 4]);
        assert_eq!(keys(rows.clone(), false, true), [1, 5]);
        assert_eq!(keys(rows, true, false), [5, 1]);
    }

    #[test]
    fn a_retired_entry_is_reresolved_once_and_skipped_when_gone() {
        // Key 3's file is retired: the re-resolve walks again, still finds key 3
        // at the same position, so the fetch reports it gone.
        let attempts = Cell::new(0);
        let fetch = |key: &Bytes, position: u32, resolve: Resolve<'_>| -> Fetched {
            if key[0] == 3 {
                attempts.set(attempts.get() + 1);
                return Ok(match resolve()? {
                    Some(again) if again != position => {
                        Some((key.clone(), Bytes::from(vec![again as u8])))
                    }
                    _ => None,
                });
            }
            fetch_values(key, position, resolve)
        };
        let walk = walk_over(rows(&[1, 2, 3, 4], &[]), false, true);
        let (rows_seen, skipped) = drain(walk, fetch);
        let keys: Vec<u8> = rows_seen.into_iter().map(|(key, _)| key).collect();
        assert_eq!(keys, [1, 2, 4]);
        assert_eq!(skipped, 1);
        assert_eq!(attempts.get(), 1, "one fetch, one bounded re-resolve");
    }

    #[test]
    fn a_moved_entry_is_returned_from_its_new_position() {
        // The first walk names position 4 for key 3; by the re-resolve the entry
        // has been re-pointed to position 77.
        let table = rows(&[1, 2, 3, 4], &[]);
        let walk = |cell: &CellId, prev: &Option<Bytes>, fresh: bool| {
            let after = prev.as_ref().map(|key| key[0]);
            let row = table
                .iter()
                .find(|(key, _)| after.is_none_or(|after| *key > after));
            Ok(row.map(|(key, position)| IteratorResult {
                cell: Some(cell.clone()),
                key: Bytes::from(vec![*key]),
                value: if fresh && *key == 3 { 77 } else { *position },
            }))
        };
        let first = Cell::new(true);
        let fetch = |key: &Bytes, position: u32, resolve: Resolve<'_>| -> Fetched {
            if key[0] == 3 && first.replace(false) {
                return Ok(resolve()?.map(|again| (key.clone(), Bytes::from(vec![again as u8]))));
            }
            fetch_values(key, position, resolve)
        };
        assert_eq!(drain(walk, fetch).0, [(1, 2), (2, 3), (3, 77), (4, 5)]);
    }
}
