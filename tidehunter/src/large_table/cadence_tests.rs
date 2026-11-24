//! Byte-cadence (`flushed_at`) tests for the position-generic `LargeTable`.
//!
//! A cell's cadence drives the byte-threshold snapshot and unload decisions.
//! Natively it is the frontier's own offset; a version-carrying engine has no
//! byte meaning in its cut, so the loader supplies it. These tests run both
//! instantiations side by side.

use super::*;
use crate::index::levels::IndexLevels;
use crate::key_shape::KeyType;
use crate::wal::test_position::{CadenceLoader, TestCut, TestPosition};

fn shape() -> KeyShape {
    KeyShape::new_single(8, 1, KeyType::uniform(1))
}

/// A table whose every cell is restored from a snapshot that already holds an
/// on-disk blob at frontier `cut`, i.e. an `Unloaded` cell after a reopen.
fn reopened<P: IndexPosition>(
    shape: &KeyShape,
    cut: P::Cut,
    loader: &CadenceLoader<P>,
) -> LargeTable<P> {
    let blob = IndexLevels::single(WalPosition::test_value(1));
    LargeTable::<P>::from_unloaded(
        shape,
        &LargeTableContainer::new_from_key_shape(shape, SnapshotEntryData::from_levels(blob, cut)),
        Arc::new(Config::small()),
        IndexFlusher::new_unstarted_for_test(),
        Metrics::new(),
        SharedLastProcessed::default(),
        loader,
        Arc::default(),
    )
}

fn only_cadence<P: IndexPosition>(table: &LargeTable<P>) -> u64 {
    let context = table.ks_context(KeySpace::first());
    let cell = context.ks_config.cell_id(&key(1));
    table
        .flushed_at_for_test(context, &cell)
        .expect("the shape has one cell")
}

fn key(n: u64) -> Bytes {
    Bytes::from(n.to_be_bytes().to_vec())
}

/// Reopen seeding, native: the persisted frontier is the byte position.
#[test]
fn reopened_native_cell_takes_its_cadence_from_the_frontier() {
    let shape = shape();
    let loader = CadenceLoader::<WalPosition>::new(|cut| cut.byte_position());
    let table = reopened(&shape, LastProcessed::new(500), &loader);
    assert_eq!(only_cadence(&table), 500);
}

/// Reopen seeding, versioned: the cut has no byte meaning, so a reopened cell
/// starts at 0, and a live write into the restored (Unloaded) cell must not
/// re-derive it: the cadence only restarts on a flush or on the Empty -> dirty
/// transition.
#[test]
fn live_write_into_a_reopened_versioned_cell_keeps_its_cadence() {
    let shape = shape();
    let loader = CadenceLoader::<TestPosition>::new(|cut| cut.0 * 1_000 + 7);
    let table = reopened(&shape, TestCut(40), &loader);
    assert_eq!(only_cadence(&table), 0, "the cut carries no byte meaning");

    let context = table.ks_context(KeySpace::first());
    let at = TestPosition(WalPosition::test_value(900));
    table
        .insert(
            context,
            key(1),
            key(1),
            at,
            &Bytes::from_static(b"v"),
            &loader,
        )
        .expect("the cadence loader cannot fail");
    assert_eq!(only_cadence(&table), 0);
}

/// The engine restores a reopened cell's own counter with `set_flushed_at`:
/// it sticks across a live write and is what `flushed_at_snapshot` reports.
#[cfg(feature = "experimental-distributed")]
#[test]
fn set_flushed_at_restores_a_reopened_versioned_cell() {
    let shape = shape();
    let loader = CadenceLoader::<TestPosition>::new(|cut| cut.0 * 1_000 + 7);
    let table = reopened(&shape, TestCut(40), &loader);
    assert_eq!(only_cadence(&table), 0, "the cut carries no byte meaning");

    let context = table.ks_context(KeySpace::first());
    let cell = context.ks_config.cell_id(&key(1));
    table.set_flushed_at(context, &cell, 12_345);
    assert_eq!(only_cadence(&table), 12_345);
    assert_eq!(
        table.flushed_at_snapshot(),
        BTreeMap::from([((KeySpace::first().0, cell), 12_345)]),
        "the snapshot lists every cell with its cadence"
    );

    // A live write into the restored (Unloaded) cell must not re-derive it.
    let at = TestPosition(WalPosition::test_value(900));
    table
        .insert(
            context,
            key(1),
            key(1),
            at,
            &Bytes::from_static(b"v"),
            &loader,
        )
        .expect("the cadence loader cannot fail");
    assert_eq!(only_cadence(&table), 12_345);
}

/// The live insert asks the loader for the cadence once, on the Empty -> dirty
/// transition only: later writes to the same dirty cell leave it alone.
#[test]
fn live_insert_takes_the_loader_cadence_on_first_dirtying_only() {
    let shape = shape();
    let loader = CadenceLoader::<TestPosition>::new(|cut| cut.0 * 1_000 + 7);
    let table = LargeTable::<TestPosition>::from_unloaded(
        &shape,
        &LargeTableContainer::new_from_key_shape(&shape, SnapshotEntryData::empty()),
        Arc::new(Config::small()),
        IndexFlusher::new_unstarted_for_test(),
        Metrics::new(),
        SharedLastProcessed::default(),
        &loader,
        Arc::default(),
    );
    let context = table.ks_context(KeySpace::first());
    let write = |offset| {
        table
            .insert(
                context,
                key(1),
                key(1),
                TestPosition(WalPosition::test_value(offset)),
                &Bytes::from_static(b"v"),
                &loader,
            )
            .expect("the cadence loader cannot fail")
    };
    write(100);
    assert_eq!(only_cadence(&table), 100 * 1_000 + 7);
    write(200);
    assert_eq!(
        only_cadence(&table),
        100 * 1_000 + 7,
        "a second write to a dirty cell does not move the cadence"
    );
}

/// A clean cell's stored frontier is left alone by a snapshot, and the
/// snapshot's `replay_from` is the minimum of that frontier and the loader's
/// cut read before the walk: the frontier bounds the writes the walk cannot
/// see, so it may only make replay conservative, never skip a write.
#[test]
fn snapshot_leaves_a_clean_cell_frontier_and_bounds_replay_from() {
    let shape = shape();
    let loader = CadenceLoader::<TestPosition>::new(|cut| cut.0);
    let table = reopened(&shape, TestCut(40), &loader);

    let snapshot = table.snapshot(&loader);

    let cell = snapshot
        .data
        .iter_cells()
        .next()
        .expect("the shape has one cell");
    assert_eq!(cell.last_processed, TestCut(40));
    assert_eq!(
        snapshot.replay_from,
        TestCut::NONE,
        "the loader's cut ({:?}) is below the cell's, so it bounds replay_from",
        loader.last_processed()
    );
}
