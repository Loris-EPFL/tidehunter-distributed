//! The table-level anchor scenario end to end, with the old table dropped
//! before the reopen.
//!
//! Durable-arrival order across owners can insert v5 into an empty cell before
//! v4. The cell's first-write anchor must then be v4, not v5: `applied` advances
//! only after both inserts, a cadence snapshot stores a region whose cut is the
//! cell's frontier, and a reopen replays from that cut (inclusive). A cut above
//! v4 would skip the batch and lose its put.
//!
//! Negative control (mutation run of production code): the S1S2 default anchor,
//! `first.frontier()` with no `anchor_cut` override, anchors the cell at v5; the
//! stored cut is then above v4 and the first assertion fails.

use super::{Fixture, open, position, version};
use crate::distributed::codec::RecordKind;
use crate::distributed::durability::DurabilityTier;
use crate::distributed::hybrid::loader::CutSources;
use crate::distributed::hybrid::table::PublishedMutation;
use minibytes::Bytes;

#[test]
fn v5_then_v4_in_one_cell_survives_a_snapshot_a_drop_and_a_reopen() {
    let Fixture { dir, table, cuts } = Fixture::new("hybrid-anchor-v5-v4");
    let keyspace = table.shape().iter_ks().next().expect("one keyspace").id();
    let insert = |table: &super::HybridTable, key: u8, sequence: u64, offset: u64| {
        table
            .insert_published(
                &[PublishedMutation {
                    keyspace,
                    key: &[key; 4],
                    kind: RecordKind::Put,
                    position: position(sequence, offset),
                    value: Some(&Bytes::from_static(b"fixture value")),
                }],
                64,
            )
            .expect("publish");
    };
    insert(&table, 1, 5, 1000);
    insert(&table, 2, 4, 2000);
    // Only now, with both inserts in, may the publication prefix cover them.
    table.advance_applied(version(6));
    cuts.set_durable(version(6));
    let stored = table.snapshot().expect("cadence snapshot");
    assert!(stored.stored);
    assert!(
        stored.cut <= version(4),
        "replay must restart at or below v4: {stored:?}"
    );
    // The put is still only in the dirty cell: the cell was not flushed, so the
    // region alone cannot serve v4 after the reopen and the replay must.
    drop(table);

    let reopened_cuts = CutSources::new(2, DurabilityTier::Native);
    let reopened = open(&dir, &reopened_cuts);
    let replay_from = reopened.stored_cut();
    assert!(replay_from <= version(4), "{replay_from:?}");
    // The replay inserts every owner batch at or above the stored cut, in
    // version order; the lower version is among them.
    for (key, sequence, offset) in [(2u8, 4u64, 2000u64), (1, 5, 1000)] {
        if version(sequence) >= replay_from {
            insert(&reopened, key, sequence, offset);
        }
    }
    let ks = keyspace;
    for (key, offset) in [(2u8, 2000u64), (1, 1000)] {
        let found = reopened
            .get(ks, &[key; 4], version(7))
            .expect("lookup")
            .map(|position| position.address.offset);
        assert_eq!(
            found,
            Some(super::member_offset(offset)),
            "key {key} survives the crash"
        );
    }
}

/// A cell re-created after `DropCells` is anchored afresh: the dropped cell's
/// old first write (v300) must not pin or raise the new anchor, and the lower of
/// the two new versions (v270 arriving after v280) is the cut the snapshot may
/// name. The premise that the drop emptied the cell is asserted first.
///
/// Negative control (mutation run): the S1S2 default anchor (`first.frontier()`)
/// anchors the new cell at v280 and the cut assertion fails.
#[test]
fn a_cell_recreated_after_drop_cells_is_anchored_at_its_lowest_new_version() {
    let fixture = Fixture::new("hybrid-anchor-drop");
    fixture.publish(RecordKind::Put, 1, 300, 1000);
    fixture
        .table
        .drop_cells(
            fixture.ks(),
            crate::cell::CellId::Integer(0),
            crate::cell::CellId::Integer(0),
            version(300),
        )
        .expect("drop the only cell");
    assert!(
        fixture.get(1, version(1000)).is_none(),
        "premise: the drop emptied the cell"
    );
    fixture.publish(RecordKind::Put, 1, 280, 3000);
    fixture.publish(RecordKind::Put, 2, 270, 4000);
    fixture.table.advance_applied(version(301));
    fixture.cuts.set_durable(version(301));
    let stored = fixture.table.snapshot().expect("cadence snapshot");
    assert!(stored.stored);
    assert!(
        stored.cut <= version(270),
        "the re-created cell must anchor at its lowest new version: {stored:?}"
    );
}
