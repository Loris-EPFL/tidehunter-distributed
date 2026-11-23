//! `IndexTable::apply_update` over a position whose `==` is its *ordering*
//! equality (it orders by version alone) and which also stores an address. A
//! relocation re-point keeps the ordering position and changes only the
//! address, so "did the update change the entry" must be answered physically.

use super::*;
use crate::wal::test_position::{LogicalPosition, TestCut};

fn key(n: u8) -> Bytes {
    Bytes::from(vec![n])
}

fn at(ordinal: u64, address: u32) -> LogicalPosition {
    LogicalPosition::new(ordinal, address)
}

/// A table whose key `1` (ordinal 10, address 100) is resident in the flat
/// buffer only.
fn table_with_flat_key() -> IndexTable<LogicalPosition> {
    let mut table = IndexTable::<LogicalPosition>::default();
    table.insert(key(1), at(10, 100));
    table.insert(key(2), at(11, 110));
    assert!(
        table.promote_to_flat_force(TestCut(100)),
        "both entries are below the cut and move to flat"
    );
    assert_eq!(table.get(&[1]).map(|p| p.address), Some(100));
    table
}

fn repoint(address: u32) -> impl FnOnce(&mut IndexWalPosition<LogicalPosition>) {
    move |tagged| {
        let (position, kind) = LogicalPosition::untag(*tagged);
        *tagged = LogicalPosition::new(position.ordinal, address).tag(kind);
    }
}

#[test]
fn flat_resident_repoint_with_unchanged_ordinal_is_applied() {
    let mut table = table_with_flat_key();
    table.apply_update(&[1], repoint(777));
    assert_eq!(
        table.get(&[1]).map(|p| (p.ordinal, p.address)),
        Some((10, 777)),
        "the re-pointed address must be visible to readers"
    );
    assert_eq!(
        table.get(&[2]).map(|p| p.address),
        Some(110),
        "an untouched flat key keeps its address"
    );
}

#[test]
fn flat_resident_noop_update_stays_in_flat() {
    let mut table = table_with_flat_key();
    let data_keys_before = table.total_key_bytes();
    table.apply_update(&[1], |_| {});
    assert_eq!(table.get(&[1]).map(|p| p.address), Some(100));
    assert_eq!(
        table.total_key_bytes(),
        data_keys_before,
        "an unchanged entry is not lifted into the data overlay"
    );
}

#[test]
fn data_resident_repoint_with_unchanged_ordinal_is_applied() {
    let mut table = IndexTable::<LogicalPosition>::default();
    table.insert(key(1), at(10, 100));
    table.apply_update(&[1], repoint(555));
    assert_eq!(
        table.get(&[1]).map(|p| (p.ordinal, p.address)),
        Some((10, 555))
    );
}
