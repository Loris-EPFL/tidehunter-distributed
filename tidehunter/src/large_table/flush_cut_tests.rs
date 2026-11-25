//! Which cut an async flush works to, and what its completion commits.
//!
//! The flush writes the overlay positions below the cut captured when it was
//! *requested* (`FlushKind::Flush::cut`), the same cut the cell commits and
//! unmerges below at completion, never the loader's live frontier at execution
//! time. A frontier that can fall (a latch held between the request and the
//! execution) would otherwise leave positions in `[live, cut)` out of the blob
//! while completion still removes them from the overlay: acknowledged writes
//! would vanish from the index. Completion also never moves a cell's frontier
//! back (`commit_flushed_cut`).
//!
//! The tests play the flusher thread: they take the queued command, move the
//! loader's live frontier, run the real `handle_command` and report the
//! completion.

use super::*;
use crate::flusher::{FlusherCommand, IndexFlushCommand, handle_command};
use crate::key_shape::KeyType;
use crate::wal::test_position::{LogicalPosition, TestCut};
use std::convert::Infallible;
use std::sync::atomic::AtomicU64;
use std::sync::mpsc;

/// Loader whose live frontier a test moves freely. `flush` records the blobs
/// the flusher wrote and `load` serves the latest one. With `as_flat` a blob is
/// stored the way a real one reads back (disk-derived entries live in `flat`).
struct MovingFrontierLoader<P: IndexPosition> {
    live: AtomicU64,
    cut_of: fn(u64) -> P::Cut,
    bytes_of: fn(P::Cut) -> u64,
    current: P,
    as_flat: bool,
    disk: parking_lot::Mutex<IndexTable<P>>,
    flushed: parking_lot::Mutex<Vec<IndexTable<P>>>,
}

impl<P: IndexPosition> Loader for MovingFrontierLoader<P> {
    type Position = P;
    type Error = Infallible;

    fn load(
        &self,
        _ks: &KeySpaceDesc,
        _position: WalPosition,
    ) -> Result<IndexTable<P>, Infallible> {
        Ok(self.disk.lock().clone())
    }

    fn index_reader(&self, _position: WalPosition) -> Result<WalRandomRead, Infallible> {
        Ok(WalRandomRead::Mapped(Bytes::new()))
    }

    fn flush_supported(&self) -> bool {
        true
    }

    fn flush(&self, _ks: KeySpace, data: &IndexTable<P>) -> Result<WalPosition, Infallible> {
        self.flushed.lock().push(data.clone());
        let mut stored = data.clone();
        if self.as_flat {
            // Reads back as `flat`; returns false when nothing is left to promote.
            stored.promote_to_flat_force((self.cut_of)(u64::MAX));
        }
        *self.disk.lock() = stored;
        Ok(WalPosition::test_value(100))
    }

    fn last_processed(&self) -> P::Cut {
        (self.cut_of)(self.live.load(Ordering::Relaxed))
    }

    fn current(&self) -> P::Cut {
        (self.cut_of)(5_000)
    }

    fn min_position(&self) -> P::Cut {
        (self.cut_of)(0)
    }

    fn current_position(&self) -> P {
        self.current
    }

    fn flushed_at_bytes(&self, cut: P::Cut) -> u64 {
        (self.bytes_of)(cut)
    }
}

impl MovingFrontierLoader<WalPosition> {
    fn native() -> Self {
        Self::new(
            LastProcessed::new,
            |cut| cut.byte_position(),
            WalPosition::test_value(5_000),
            false,
        )
    }
}

impl MovingFrontierLoader<LogicalPosition> {
    fn logical() -> Self {
        Self::new(TestCut, |cut| cut.0, LogicalPosition::new(5_000, 0), true)
    }
}

impl<P: IndexPosition> MovingFrontierLoader<P> {
    fn new(
        cut_of: fn(u64) -> P::Cut,
        bytes_of: fn(P::Cut) -> u64,
        current: P,
        as_flat: bool,
    ) -> Self {
        Self {
            live: AtomicU64::new(0),
            cut_of,
            bytes_of,
            current,
            as_flat,
            disk: Default::default(),
            flushed: Default::default(),
        }
    }
}

fn key(n: u64) -> Bytes {
    Bytes::from(n.to_be_bytes().to_vec())
}

fn shape() -> KeyShape {
    KeyShape::new_single(8, 1, KeyType::uniform(1))
}

/// A one-cell table with a loader whose flusher queue the test drains itself.
struct Fixture<P: IndexPosition> {
    shape: KeyShape,
    table: LargeTable<P>,
    loader: MovingFrontierLoader<P>,
    commands: mpsc::Receiver<FlusherCommand<P>>,
}

impl<P: IndexPosition> Fixture<P> {
    fn new(loader: MovingFrontierLoader<P>) -> Self {
        let shape = shape();
        let (sender, commands) = mpsc::channel();
        let table = LargeTable::<P>::from_unloaded(
            &shape,
            &LargeTableContainer::new_from_key_shape(&shape, SnapshotEntryData::empty()),
            Arc::new(Config::small()),
            IndexFlusher::new(vec![sender], Metrics::new()),
            Metrics::new(),
            SharedLastProcessed::default(),
            &loader,
            Arc::default(),
        );
        Self {
            shape,
            table,
            loader,
            commands,
        }
    }

    fn context(&self) -> &KsContext {
        self.table.ks_context(KeySpace::first())
    }

    fn cell(&self) -> CellId {
        self.context().ks_config.cell_id(&key(1))
    }

    fn set_live(&self, live: u64) {
        self.loader.live.store(live, Ordering::Relaxed);
    }

    fn insert(&self, n: u64, position: P) {
        self.table
            .insert(
                self.context(),
                key(n),
                key(n),
                position,
                &Bytes::from_static(b"v"),
                &self.loader,
            )
            .expect("the loader cannot fail");
    }

    /// Requests an async flush with the frontier at `cut`.
    fn request(&self, cut: u64) -> IndexFlushCommand<P> {
        self.set_live(cut);
        self.table
            .with_entry_for_test(self.context(), &self.cell(), |entry| {
                entry.request_flush_if_behind(
                    &self.table.flusher,
                    self.loader.last_processed(),
                    cut,
                    u64::MAX,
                );
            });
        let FlusherCommand::Command(command) = self
            .commands
            .try_recv()
            .expect("requesting the flush queued a command")
        else {
            panic!("expected a flush command");
        };
        command
    }

    /// Runs the flush work with the loader's live frontier at `live_at_run`,
    /// then reports the completion to the cell.
    fn run_and_complete(&self, command: &IndexFlushCommand<P>, live_at_run: u64) {
        self.set_live(live_at_run);
        let (original, new_levels) =
            handle_command(&self.loader, command, self.context(), RelocationFlush::None)
                .expect("a dirty flush produces levels");
        let _dropped =
            self.table
                .update_flushed_index(self.context(), &self.cell(), original, new_levels);
    }

    fn state(&self) -> String {
        self.table
            .entry_state_for_test(self.context(), &self.cell())
    }

    fn last_processed(&self) -> P::Cut {
        self.table
            .last_processed_for_test(self.context(), &self.cell())
            .expect("the fixture's cell has an entry")
    }

    fn cadence(&self) -> Option<u64> {
        self.table.flushed_at_for_test(self.context(), &self.cell())
    }
}

fn native_with_two_keys(live_at_request: u64) -> Fixture<WalPosition> {
    let fixture = Fixture::new(MovingFrontierLoader::native());
    fixture.set_live(live_at_request);
    fixture.insert(1, WalPosition::test_value(100));
    fixture.insert(2, WalPosition::test_value(200));
    fixture
}

/// The frontier falls after the request (a latch below the request cut) and
/// before the flusher runs: every overlay position below the *request* cut is
/// still written, because completion removes exactly those from the overlay.
#[test]
fn a_frontier_that_falls_after_the_request_does_not_shrink_what_the_flush_writes() {
    let fixture = native_with_two_keys(1_000);
    let command = fixture.request(1_000);
    // A latch now holds the frontier below key 2's position (offset 200).
    fixture.run_and_complete(&command, 150);

    let flushed = fixture.loader.flushed.lock();
    assert_eq!(flushed.len(), 1, "exactly one blob was written");
    assert_eq!(flushed[0].get(&key(1)), Some(WalPosition::test_value(100)));
    assert_eq!(
        flushed[0].get(&key(2)),
        Some(WalPosition::test_value(200)),
        "key 2 is below the request cut, so it is in the blob"
    );
    drop(flushed);
    assert_eq!(
        fixture.state(),
        "Unloaded",
        "completion removed both from the overlay, so the blob is their only copy"
    );
}

/// Native behaviour: the frontier only advances, so the run-time frontier is at
/// or above the request cut. Positions between the two were never unmerged and
/// stay in the overlay for the next flush.
#[test]
fn positions_between_the_request_cut_and_a_later_frontier_wait_one_cycle() {
    let fixture = native_with_two_keys(150);
    let command = fixture.request(150);
    fixture.run_and_complete(&command, 5_000);

    let flushed = fixture.loader.flushed.lock();
    assert_eq!(flushed.len(), 1, "exactly one blob was written");
    assert_eq!(flushed[0].get(&key(1)), Some(WalPosition::test_value(100)));
    assert_eq!(
        flushed[0].get(&key(2)),
        None,
        "key 2 is at or above the request cut, so it is not in the blob"
    );
    drop(flushed);
    assert_eq!(
        fixture.state(),
        "DirtyUnloaded",
        "key 2 stays in the overlay"
    );
}

/// Hybrid policy (`committed_cut` = max): a flush requested while a latch pins
/// the frontier low (a transient low cut) completes without dragging the
/// cell's frontier back below what its earlier flush committed; the byte
/// cadence still takes the new flush's value.
#[test]
fn a_transient_low_commit_does_not_move_a_versioned_cell_frontier_back() {
    let fixture = Fixture::new(MovingFrontierLoader::logical());
    fixture.set_live(1_000);
    fixture.insert(1, at(10, 100));
    fixture.insert(2, at(20, 110));
    let first = fixture.request(1_000);
    fixture.run_and_complete(&first, 1_000);
    assert_eq!(fixture.last_processed(), TestCut(1_000));
    assert_eq!(fixture.cadence(), Some(1_000));

    // A later write, then a flush requested under a latch pinned at 150.
    fixture.insert(3, at(3_000, 120));
    let second = fixture.request(150);
    fixture.run_and_complete(&second, 150);
    assert_eq!(
        fixture.last_processed(),
        TestCut(1_000),
        "the commit of a lower cut must not move the frontier back"
    );
    assert_eq!(
        fixture.cadence(),
        Some(150),
        "the cadence clock still restarts at the flush that just completed"
    );
}

/// Native policy (`committed_cut` = the new cut, a plain store): the same
/// transient low commit stores the lower cut, exactly as before the position
/// type was generic. Pins that the guard above is hybrid-only.
#[test]
fn natively_a_lower_commit_cut_is_stored_as_before() {
    let fixture = native_with_two_keys(1_000);
    let first = fixture.request(1_000);
    fixture.run_and_complete(&first, 1_000);
    assert_eq!(fixture.last_processed(), LastProcessed::new(1_000));

    fixture.insert(3, WalPosition::test_value(3_000));
    let second = fixture.request(150);
    fixture.run_and_complete(&second, 150);
    assert_eq!(fixture.last_processed(), LastProcessed::new(150));
}

/// Native anchored-`Empty` cell: `mark_dirty` anchors the cell at the
/// minimum of its first write `P` (offset 300) and the tracker frontier read
/// under the row lock (150), so the cell never sits above a write it applied.
/// A flush requested at `pending = 150 <= P` (an older write was still in
/// flight) then stores `pending`, the more conservative value. Snapshot
/// `replay_from` covers the write either way. Negative control: anchoring at
/// the write's own frontier would make the snapshot record `replay_from = 300`
/// with the write at 300 unprocessed (`<` is strict), so a crash before its
/// flush would lose it.
#[test]
fn natively_an_anchored_empty_cell_is_clamped_by_the_tracker_frontier() {
    let fixture = Fixture::new(MovingFrontierLoader::native());
    fixture.set_live(150);
    fixture.insert(1, WalPosition::test_value(300));
    assert_eq!(
        fixture.last_processed(),
        LastProcessed::new(150),
        "the first write anchors the Empty cell at min(its frontier, the tracker frontier)"
    );
    let command = fixture.request(150);
    fixture.run_and_complete(&command, 150);
    assert_eq!(fixture.last_processed(), LastProcessed::new(150));
    assert_eq!(
        fixture.table.snapshot(&fixture.loader).replay_from,
        LastProcessed::new(150),
        "replay starts below the unflushed write"
    );
}

/// Hybrid policy (`anchor_cut` = `min(first, bound)`): an `Empty` cell is
/// anchored at the loader's insertion bound (150) when that is below its first
/// write (300), and the commit policy (`max`) keeps it there.
#[test]
fn a_versioned_cell_is_anchored_at_most_at_the_insertion_bound() {
    let fixture = Fixture::new(MovingFrontierLoader::logical());
    fixture.set_live(150);
    fixture.insert(1, at(300, 100));
    assert_eq!(fixture.last_processed(), TestCut(150));
    let command = fixture.request(150);
    fixture.run_and_complete(&command, 150);
    assert_eq!(fixture.last_processed(), TestCut(150));
    assert_eq!(
        fixture.table.snapshot(&fixture.loader).replay_from,
        TestCut(150)
    );
}

/// A write with a lower version can arrive in a cell after a higher one (the
/// engine inserts in durable-arrival order). The anchor set by the higher
/// write must not sit above it: after a snapshot, a crash and a reopen the
/// lower write is replayed. Negative control: anchoring at the first write's
/// own frontier (the native rule) leaves `replay_from` at 300 and loses key 2.
#[test]
fn a_lower_version_arriving_after_the_anchoring_write_survives_a_reopen() {
    let wal = [(1, at(300, 100)), (2, at(200, 110))];
    let fixture = Fixture::new(MovingFrontierLoader::logical());
    fixture.set_live(150);
    for (n, position) in wal {
        fixture.insert(n, position);
    }
    // A flush at the loader cut writes neither (both are at or above it).
    let command = fixture.request(150);
    fixture.run_and_complete(&command, 150);

    let snapshot = fixture.table.snapshot(&fixture.loader);
    let reopened = reopen_from(&fixture, &snapshot);
    for (n, position) in wal {
        if position.ordinal >= snapshot.replay_from.0 {
            reopened.insert(n, position);
        }
    }
    assert_eq!(served(&reopened, 1), Some((300, 100)));
    assert_eq!(
        served(&reopened, 2),
        Some((200, 110)),
        "the lower version arrived after the anchor and is still replayed"
    );
}

/// Native monotone sequence: each commit raises the frontier, as before.
#[test]
fn a_rising_sequence_of_commits_raises_the_cell_frontier_each_time() {
    let fixture = native_with_two_keys(150);
    let first = fixture.request(150);
    fixture.run_and_complete(&first, 150);
    assert_eq!(fixture.last_processed(), LastProcessed::new(150));

    let second = fixture.request(1_000);
    fixture.run_and_complete(&second, 1_000);
    assert_eq!(fixture.last_processed(), LastProcessed::new(1_000));
    assert_eq!(fixture.cadence(), Some(1_000));
}

fn at(ordinal: u64, address: u32) -> LogicalPosition {
    LogicalPosition::new(ordinal, address)
}

/// A table reopened from `snapshot` after a crash: the snapshot's cells, the
/// latest blob of `fixture`'s loader, and a fresh flusher queue.
fn reopen_from(
    fixture: &Fixture<LogicalPosition>,
    snapshot: &LargeTableSnapshot<TestCut>,
) -> Fixture<LogicalPosition> {
    let (sender, commands) = mpsc::channel();
    Fixture {
        shape: fixture.shape.clone(),
        table: LargeTable::<LogicalPosition>::from_unloaded(
            &fixture.shape,
            &snapshot.data,
            Arc::new(Config::small()),
            IndexFlusher::new(vec![sender], Metrics::new()),
            Metrics::new(),
            SharedLastProcessed::default(),
            &fixture.loader,
            Arc::default(),
        ),
        loader: MovingFrontierLoader {
            disk: parking_lot::Mutex::new(fixture.loader.disk.lock().clone()),
            ..MovingFrontierLoader::logical()
        },
        commands,
    }
}

/// Re-points key `n` (same ordinal, new address) in the cell's overlay, the way
/// an in-place relocation update does: the flat-resident entry is lifted into
/// the overlay and the cell becomes dirty.
fn repoint(fixture: &Fixture<LogicalPosition>, n: u64, ordinal: u64, address: u32) {
    fixture
        .table
        .with_entry_for_test(fixture.context(), &fixture.cell(), |entry| {
            entry
                .maybe_load(&fixture.loader)
                .expect("the loader cannot fail");
            entry.data.make_mut().apply_update(&key(n), |tagged| {
                let (position, kind) = LogicalPosition::untag(*tagged);
                assert_eq!(
                    position.ordinal, ordinal,
                    "re-pointing the intended version"
                );
                *tagged = LogicalPosition::new(position.ordinal, address).tag(kind);
            });
            entry.state.mark_dirty();
        });
}

/// What a reopened table serves for key `n`, once its cell is loaded: the
/// ordinal and address of the key's position, or `None`.
fn served(fixture: &Fixture<LogicalPosition>, n: u64) -> Option<(u64, u32)> {
    fixture
        .table
        .with_entry_for_test(fixture.context(), &fixture.cell(), |entry| {
            entry
                .maybe_load(&fixture.loader)
                .expect("the loader cannot fail");
            entry.data.get(&key(n)).map(|p| (p.ordinal, p.address))
        })
}

/// Precondition for the monotone commit: a relocation flush at an as-of cut
/// `q` below the cell's frontier `lp`, then a crash and reopen from the stored
/// snapshot (no further flush), loses nothing.
///
/// Key 1 is re-pointed below `q`: the flush writes it. Key 2 is re-pointed at
/// a version in `[q, lp)`: the flush keeps that re-point out of the blob (it
/// stays in memory, as the cut filter intends), so after the crash the blob
/// still names the original address, which stays valid because its file is
/// not retired until a snapshot covers the re-point. Both orders of the
/// commit (frontier kept at `lp`, or lowered to `q`) therefore reopen to the
/// same index: with the frontier at `lp` replay starts at `lp`, with it at `q`
/// replay re-inserts the originals at or above `q`, which are already there.
#[test]
fn a_relocation_flush_below_the_cell_frontier_loses_nothing_across_a_reopen() {
    // The value WAL: every acknowledged write, in version order.
    let wal = [(1, at(10, 100)), (2, at(20, 110)), (3, at(30, 120))];
    let fixture = Fixture::new(MovingFrontierLoader::logical());
    fixture.set_live(1_000);
    for (n, position) in wal {
        fixture.insert(n, position);
    }
    // The cell's frontier is 1,000 once its first flush completes.
    let first = fixture.request(1_000);
    fixture.run_and_complete(&first, 1_000);
    assert_eq!(fixture.last_processed(), TestCut(1_000));

    // A relocation pass moves keys 1 and 2 in the overlay.
    repoint(&fixture, 1, 10, 900);
    repoint(&fixture, 2, 20, 910);
    // It pins its as-of cut q = 15, below the cell frontier, and flushes there.
    let relocation = fixture.request(15);
    fixture.run_and_complete(&relocation, 15);

    // Crash: only the stored snapshot, the latest blob and the WAL survive.
    let snapshot = fixture.table.snapshot(&fixture.loader);
    let reopened = reopen_from(&fixture, &snapshot);
    // Replay: every acknowledged write at or above the snapshot's replay point.
    for (n, position) in wal {
        if position.ordinal >= snapshot.replay_from.0 {
            reopened.insert(n, position);
        }
    }

    assert_eq!(
        served(&reopened, 1),
        Some((10, 900)),
        "a re-point below q was written by the relocation flush"
    );
    assert_eq!(
        served(&reopened, 2),
        Some((20, 110)),
        "a re-point at or above q was not durable; the original address is served"
    );
    assert_eq!(served(&reopened, 3), Some((30, 120)));
}
