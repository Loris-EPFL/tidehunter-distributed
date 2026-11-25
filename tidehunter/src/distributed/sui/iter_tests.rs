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

fn fetch_values(
    key: &Bytes,
    position: u32,
    _found_in: Option<&CellId>,
    _resolve: Resolve<'_>,
) -> Fetched {
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
    mut fetch: impl FnMut(
        &Bytes,
        P,
        Option<&CellId>,
        &mut dyn FnMut() -> Result<Option<P>, LocalDbError>,
    ) -> Fetched,
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
    let gone = |key: &Bytes, position: u32, found_in: Option<&CellId>, resolve: Resolve<'_>| {
        if key[0] == 2 || key[0] == 4 {
            return Ok(resolve()?
                .filter(|again| *again != position)
                .map(|again| (key.clone(), Bytes::from(vec![again as u8]))));
        }
        fetch_values(key, position, found_in, resolve)
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
    let fetch =
        |key: &Bytes, position: u32, found_in: Option<&CellId>, resolve: Resolve<'_>| -> Fetched {
            if key[0] == 3 {
                attempts.set(attempts.get() + 1);
                return Ok(match resolve()? {
                    Some(again) if again != position => {
                        Some((key.clone(), Bytes::from(vec![again as u8])))
                    }
                    _ => None,
                });
            }
            fetch_values(key, position, found_in, resolve)
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
    let fetch =
        |key: &Bytes, position: u32, found_in: Option<&CellId>, resolve: Resolve<'_>| -> Fetched {
            if key[0] == 3 && first.replace(false) {
                return Ok(resolve()?.map(|again| (key.clone(), Bytes::from(vec![again as u8]))));
            }
            fetch_values(key, position, found_in, resolve)
        };
    assert_eq!(drain(walk, fetch).0, [(1, 2), (2, 3), (3, 77), (4, 5)]);
}
