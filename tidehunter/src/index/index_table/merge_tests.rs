//! Merging a fresher side over a disk-loaded base, across a tombstone and a
//! live re-insert of the same key.
//!
//! `from_disk_position` keeps a disk tombstone's sentinel offset but rewrites
//! its length word, so the tagged entry is not `P::INVALID` field-wise.
//! Comparisons that spell the sentinel as a whole position therefore miss it:
//! `fold_dirty_flat` compared the sentinel offset against a live position and
//! `retain_processed` read the normalised tombstone as a valid write.

use super::*;
use crate::key_shape::{KeyIndexing, KeyShape, KeySpace, KeyType};

fn keyspace() -> KeySpaceDesc {
    let shape = KeyShape::new_single_config_indexing(
        KeyIndexing::variable_length(),
        1,
        KeyType::uniform(1),
        Default::default(),
    );
    shape.ks(KeySpace::first()).clone()
}

/// A table as an index blob read back from disk: `flat` only, with tombstones
/// decoded to the normalised sentinel position. `None` records a delete.
fn disk_loaded(entries: &[(u8, Option<u64>)]) -> IndexTable {
    let ks = keyspace();
    let mut written = IndexTable::default();
    for (key, offset) in entries {
        let key = Bytes::from(vec![*key]);
        match offset {
            Some(offset) => written.insert(key, WalPosition::test_value(*offset)),
            None => written.remove(key, WalPosition::test_value(0)),
        };
    }
    let mut buf = BytesMut::new();
    written.serialize_index_entries(&ks, &mut buf);
    IndexTable::<WalPosition>::deserialize_index_entries(&ks, Bytes::from(buf.freeze()))
}

/// Delete-then-reinsert is a normal sequence: an L0 blob holds the tombstone,
/// and the fresher side holds the live write. The fold's ordering assert must
/// treat the tombstone as uncomparable instead of comparing the sentinel
/// offset against a live position (which panicked in debug builds).
#[test]
fn fold_dirty_flat_orders_a_live_reinsert_above_a_disk_tombstone() {
    let mut merged = disk_loaded(&[(1, Some(10)), (2, None)]);
    assert_eq!(
        merged.get(&[2]),
        Some(WalPosition::INVALID),
        "the L0 tombstone must be visible before the merge"
    );

    let new_level = disk_loaded(&[(2, Some(40))]);
    assert!(
        !new_level.data_contains_key(&[2]),
        "the re-insert must be resident in flat, not in the data overlay"
    );

    merged.merge_dirty_no_clean(&new_level);

    assert_eq!(
        merged.get(&[2]),
        Some(WalPosition::test_value(40)),
        "the live re-insert must shadow the tombstone"
    );
    assert_eq!(
        merged.get(&[1]),
        Some(WalPosition::test_value(10)),
        "an untouched key keeps its position"
    );
}

/// The frontier filter is sound only while `data` holds genuine writes. A disk
/// tombstone is the tagged form `from_disk_position` builds from the sentinel;
/// the assert must see it even though the native `from_disk` normalises its
/// length word. The overlay is seeded directly: no production path lifts such
/// an entry, which is exactly what the assert guards.
#[test]
#[cfg(debug_assertions)]
#[should_panic(expected = "disk-origin")]
fn retain_processed_rejects_a_disk_tombstone_in_the_data_overlay() {
    let mut table = IndexTable::default();
    table.insert(Bytes::from(vec![1]), WalPosition::test_value(10));
    table.data_insert_sorted(
        Bytes::from(vec![2]),
        WalPosition::from_disk_position(WalPosition::INVALID),
    );

    table.retain_processed(LastProcessed::new(1_000_000));
}
