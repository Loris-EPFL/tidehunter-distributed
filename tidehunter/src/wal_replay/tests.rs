//! Replay-driver tests over a scripted [`ReplaySource`].
//!
//! The driver is generic over the position type and the source, so these run
//! it with no WAL on disk. They pin the frame accounting (completed and torn
//! batches, tombstones, the nested-batch failure) and the byte cadence that a
//! cell first created by replay receives from the loader.

use super::*;
use crate::cell::CellId;
use crate::config::Config;
use crate::container::LargeTableContainer;
use crate::context::KsContext;
use crate::flusher::IndexFlusher;
use crate::key_shape::{KeySpace, KeyType};
use crate::large_table::{GetResult, SnapshotEntryData};
use crate::wal::position::{IndexCut, LastProcessed};
use crate::wal::test_position::{CadenceLoader, LogicalPosition, TestCut, TestPosition};
use minibytes::Bytes;
use std::collections::VecDeque;
use std::convert::Infallible;
use std::sync::Arc;

/// A source that replays a fixed script and records what `finish` was given.
struct Scripted<P: IndexPosition> {
    frames: VecDeque<ReplayFrame<P>>,
}

impl<P: IndexPosition> Scripted<P> {
    fn new(frames: Vec<ReplayFrame<P>>) -> Self {
        Self {
            frames: frames.into(),
        }
    }
}

impl<P: IndexPosition> ReplaySource<P> for Scripted<P> {
    type Error = Infallible;
    /// The cutoff `finish` received: the rewind point of a torn batch.
    type Output = Option<P>;

    fn next(&mut self) -> Result<Option<ReplayFrame<P>>, Infallible> {
        Ok(self.frames.pop_front())
    }

    fn finish(self, cutoff: Option<P>) -> Result<Option<P>, Infallible> {
        Ok(cutoff)
    }
}

struct Fixture<P: IndexPosition> {
    shape: KeyShape,
    contexts: KsContextVec,
    table: LargeTable<P>,
    loader: CadenceLoader<P>,
    metrics: Arc<Metrics>,
}

impl<P: IndexPosition> Fixture<P> {
    fn new(cadence: fn(P::Cut) -> u64) -> Self {
        let shape = KeyShape::new_single(8, 1, KeyType::uniform(1));
        let config = Arc::new(Config::small());
        let metrics = Metrics::new();
        let loader = CadenceLoader::<P>::new(cadence);
        let table = LargeTable::<P>::from_unloaded(
            &shape,
            &LargeTableContainer::new_from_key_shape(&shape, SnapshotEntryData::empty()),
            config.clone(),
            IndexFlusher::new_unstarted_for_test(),
            metrics.clone(),
            &loader,
            Arc::default(),
        );
        let contexts = KsContextVec::new(&shape, config, metrics.clone());
        Self {
            shape,
            contexts,
            table,
            loader,
            metrics,
        }
    }

    fn replay(&self, frames: Vec<ReplayFrame<P>>) -> Option<P> {
        replay_wal(
            &self.contexts,
            &self.table,
            &self.shape,
            Scripted::new(frames),
            &self.metrics,
            &self.loader,
        )
        .expect("a scripted source cannot fail")
    }

    fn lookup(&self, key: u64) -> Option<P> {
        let context = self.table.ks_context(KeySpace::first());
        match self
            .table
            .get(context, &key.to_be_bytes(), &self.loader)
            .expect("the cadence loader cannot fail")
        {
            GetResult::WalPosition(position, _) => Some(position),
            GetResult::NotFound => None,
            GetResult::Value(..) => unreachable!("replay never fills the value cache"),
        }
    }

    /// The cell of the first keyspace holding `key`, with its context.
    fn cell_of(&self, key: u64) -> (&KsContext, CellId) {
        let context = self.table.ks_context(KeySpace::first());
        let reduced = context
            .ks_config
            .reduced_key_bytes(Bytes::from(key.to_be_bytes().to_vec()));
        let cell = context.ks_config.cell_id(&reduced);
        (context, cell)
    }

    /// The byte cadence of the cell holding `key`; `None` when it has no entry.
    fn cadence_of(&self, key: u64) -> Option<u64> {
        let (context, cell) = self.cell_of(key);
        self.table.flushed_at_for_test(context, &cell)
    }

    /// The durable frontier (`last_processed`) of the cell holding `key`.
    fn frontier_of(&self, key: u64) -> Option<P::Cut> {
        let (context, cell) = self.cell_of(key);
        self.table.last_processed_for_test(context, &cell)
    }
}

fn record<P: IndexPosition>(position: P, key: u64) -> ReplayFrame<P> {
    ReplayFrame::Entry {
        position,
        entry: WalEntry::Record(
            KeySpace::first(),
            Bytes::from(key.to_be_bytes().to_vec()),
            Bytes::from_static(b"value"),
            false,
        ),
    }
}

fn tombstone<P: IndexPosition>(position: P, key: u64) -> ReplayFrame<P> {
    ReplayFrame::Entry {
        position,
        entry: WalEntry::Remove(KeySpace::first(), Bytes::from(key.to_be_bytes().to_vec())),
    }
}

fn at(offset: u64) -> TestPosition {
    TestPosition(WalPosition::test_value(offset))
}

/// A cadence unrelated to the cut's own byte meaning, so a stored value can only
/// have come from the loader.
fn scaled(cut: TestCut) -> u64 {
    cut.0 * 1_000 + 7
}

#[test]
fn replay_applies_records_and_tombstones_in_write_order() {
    let fixture = Fixture::<TestPosition>::new(scaled);
    let cutoff = fixture.replay(vec![
        record(at(100), 1),
        record(at(200), 2),
        tombstone(at(300), 1),
        record(at(400), 3),
    ]);
    assert_eq!(cutoff, None, "no batch was open at the end of the script");
    assert_eq!(
        fixture.lookup(1),
        None,
        "tombstone wins over the earlier write"
    );
    assert_eq!(fixture.lookup(2), Some(at(200)));
    assert_eq!(fixture.lookup(3), Some(at(400)));
    assert_eq!(fixture.lookup(4), None);
}

#[test]
fn replay_keeps_a_completed_batch() {
    let fixture = Fixture::<TestPosition>::new(scaled);
    let cutoff = fixture.replay(vec![
        ReplayFrame::BatchStart {
            position: at(100),
            count: 2,
        },
        record(at(110), 1),
        tombstone(at(120), 2),
        record(at(200), 3),
    ]);
    assert_eq!(cutoff, None);
    assert_eq!(fixture.lookup(1), Some(at(110)));
    assert_eq!(fixture.lookup(3), Some(at(200)));
}

#[test]
fn replay_discards_a_torn_batch_and_reports_its_start() {
    let fixture = Fixture::<TestPosition>::new(scaled);
    let cutoff = fixture.replay(vec![
        record(at(50), 9),
        ReplayFrame::BatchStart {
            position: at(100),
            count: 3,
        },
        record(at(110), 1),
        record(at(120), 2),
        // The third member never arrives: the source ends here.
    ]);
    assert_eq!(
        cutoff,
        Some(at(100)),
        "finish must receive the torn batch's first position"
    );
    assert_eq!(
        fixture.lookup(9),
        Some(at(50)),
        "writes before the batch stay"
    );
    assert_eq!(fixture.lookup(1), None);
    assert_eq!(fixture.lookup(2), None);
}

#[test]
#[should_panic(expected = "encountered entry BatchStart(2)")]
fn replay_rejects_a_batch_opened_inside_a_batch() {
    let fixture = Fixture::<TestPosition>::new(scaled);
    fixture.replay(vec![
        ReplayFrame::BatchStart {
            position: at(100),
            count: 2,
        },
        ReplayFrame::BatchStart {
            position: at(110),
            count: 2,
        },
    ]);
}

#[test]
#[should_panic(expected = "writer invariant violated")]
fn replay_rejects_an_empty_batch() {
    let fixture = Fixture::<TestPosition>::new(scaled);
    fixture.replay(vec![ReplayFrame::BatchStart {
        position: at(100),
        count: 0,
    }]);
}

/// A cell first created by replay takes its byte cadence from the loader, the
/// way the live `insert` path does, instead of falling back to the cut's own
/// byte position (0 for a cut with no byte meaning, which would make the cell
/// look permanently behind its flush threshold).
#[test]
fn replay_seeds_a_new_cells_cadence_from_the_loader() {
    let fixture = Fixture::<TestPosition>::new(scaled);
    fixture.replay(vec![record(at(300), 1), record(at(500), 2)]);

    let first = at(300);
    let expected = scaled(first.frontier());
    // Negative control: the pre-loader behaviour stored this, so the assertion
    // below fails against it. The two values must differ for the test to bite.
    assert_ne!(expected, first.frontier().byte_position());

    assert_eq!(
        fixture.cadence_of(1),
        Some(expected),
        "cadence comes from the first replayed position's frontier"
    );
}

/// Native behaviour is unchanged: the loader's cadence for a `LastProcessed`
/// cut is its own offset, which is what replay stored before the loader was
/// threaded through.
#[test]
fn replay_native_cadence_is_the_first_positions_offset() {
    let fixture = Fixture::<WalPosition>::new(|cut: LastProcessed| cut.byte_position());
    fixture.replay(vec![
        record(WalPosition::test_value(300), 1),
        record(WalPosition::test_value(500), 2),
    ]);
    assert_eq!(fixture.cadence_of(1), Some(300));
}

/// Replay pushes a cell's writes in ascending order, so a cell first created by
/// replay is anchored at its first replayed frontier, whatever the loader's
/// insertion bound. The cadence loader's bound is `NONE`; pulling the anchor
/// down to it would leave `replay_from` at `NONE` until the cell flushed.
/// Negative control: passing the loader's bound (`|| loader.insert_anchor_bound()`)
/// instead of the first frontier in `apply_replay_buffer` stores `TestCut(0)`.
#[test]
fn replay_anchors_a_cell_at_its_first_replayed_frontier_whatever_the_loader_bound() {
    let fixture = Fixture::<LogicalPosition>::new(|cut: TestCut| cut.0);
    fixture.replay(vec![
        record(LogicalPosition::new(300, 1), 1),
        record(LogicalPosition::new(500, 2), 2),
    ]);
    assert_eq!(fixture.frontier_of(1), Some(TestCut(300)));
}

/// Native twin: the anchor is the first write's offset, as before the policy.
#[test]
fn replay_native_anchor_is_the_first_positions_offset() {
    let fixture = Fixture::<WalPosition>::new(|cut: LastProcessed| cut.byte_position());
    fixture.replay(vec![
        record(WalPosition::test_value(300), 1),
        record(WalPosition::test_value(500), 2),
    ]);
    assert_eq!(fixture.frontier_of(1), Some(LastProcessed::new(300)));
}
