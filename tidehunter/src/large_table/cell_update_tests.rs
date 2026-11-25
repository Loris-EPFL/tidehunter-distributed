//! `LargeTable::update_cell_index`: an in-place update of a cell's merged
//! index (relocation / compaction of a versioned engine), not a write.

use super::*;
use crate::db::{DbError, DbResult};
use crate::failpoints::FailPoint;
use crate::flusher::{FlushKind, FlusherCommand};
use crate::index::levels::IndexLevels;
use crate::key_shape::KeyType;
use crate::wal::test_position::{LogicalPosition, TestCut};
use std::convert::Infallible;
use std::sync::atomic::AtomicBool;
use std::sync::mpsc;

/// Loader double over native positions: `load` serves a fixed on-disk table
/// (or fails on demand) and `flush` records what the flusher wrote.
struct DiskLoader {
    on_disk: IndexTable,
    fail_load: AtomicBool,
    flushed: parking_lot::Mutex<Vec<IndexTable>>,
}

impl DiskLoader {
    fn new() -> Self {
        let mut on_disk = IndexTable::default();
        on_disk.insert(key(1), WalPosition::test_value(10));
        on_disk.insert(key(2), WalPosition::test_value(20));
        Self {
            on_disk,
            fail_load: AtomicBool::new(false),
            flushed: Default::default(),
        }
    }
}

impl Loader for DiskLoader {
    type Position = WalPosition;
    type Error = DbError;

    fn load(&self, _ks: &KeySpaceDesc, _position: WalPosition) -> DbResult<IndexTable> {
        if self.fail_load.load(Ordering::Relaxed) {
            return Err(DbError::CrCorrupted);
        }
        Ok(self.on_disk.clone())
    }

    fn index_reader(&self, _position: WalPosition) -> DbResult<WalRandomRead> {
        Ok(WalRandomRead::Mapped(Bytes::new()))
    }

    fn flush_supported(&self) -> bool {
        true
    }

    fn flush(&self, _ks: KeySpace, data: &IndexTable) -> DbResult<WalPosition> {
        self.flushed.lock().push(data.clone());
        Ok(WalPosition::test_value(100))
    }

    fn last_processed(&self) -> LastProcessed {
        LastProcessed::new(5_000)
    }

    fn current(&self) -> LastProcessed {
        LastProcessed::new(5_000)
    }

    fn min_position(&self) -> LastProcessed {
        LastProcessed::none()
    }

    fn current_position(&self) -> WalPosition {
        WalPosition::test_value(5_000)
    }

    fn flushed_at_bytes(&self, cut: LastProcessed) -> u64 {
        cut.byte_position()
    }
}

fn key(n: u64) -> Bytes {
    Bytes::from(n.to_be_bytes().to_vec())
}

const FRONTIER: u64 = 40;

struct Fixture {
    table: LargeTable,
    loader: DiskLoader,
}

impl Fixture {
    /// Every cell is restored from a snapshot holding one on-disk blob at
    /// frontier [`FRONTIER`]: an `Unloaded` cell, as after a reopen.
    fn reopened() -> Self {
        let shape = KeyShape::new_single(8, 1, KeyType::uniform(1));
        let loader = DiskLoader::new();
        let blob = IndexLevels::single(WalPosition::test_value(1));
        let entry = SnapshotEntryData::from_levels(blob, LastProcessed::new(FRONTIER));
        let table = LargeTable::from_unloaded(
            &shape,
            &LargeTableContainer::new_from_key_shape(&shape, entry),
            Arc::new(Config::small()),
            IndexFlusher::new_unstarted_for_test(),
            Metrics::new(),
            SharedLastProcessed::default(),
            &loader,
            Arc::default(),
        );
        Self { table, loader }
    }

    fn context(&self) -> &KsContext {
        self.table.ks_context(KeySpace::first())
    }

    fn cell(&self) -> CellId {
        self.context().ks_config.cell_id(&key(1))
    }

    fn state(&self) -> String {
        self.table
            .entry_state_for_test(self.context(), &self.cell())
    }

    /// The fields an index-only update must not move.
    fn frontier_and_cadence(&self) -> (LastProcessed, u64) {
        self.table
            .with_entry_for_test(self.context(), &self.cell(), |entry| {
                (entry.last_processed, entry.flushed_at)
            })
    }

    fn update<R>(
        &self,
        f: impl FnOnce(&mut IndexTable) -> (R, bool),
    ) -> Result<Option<R>, CellUpdateError<DbError>> {
        self.table.update_cell_index(
            self.context(),
            &self.cell(),
            &self.loader,
            |index, _levels| Ok(f(index)),
        )
    }
}

#[test]
fn empty_cell_is_absent_and_is_not_touched() {
    let shape = KeyShape::new_single(8, 1, KeyType::uniform(1));
    let loader = DiskLoader::new();
    let table = LargeTable::<WalPosition>::from_unloaded(
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
    let cell = context.ks_config.cell_id(&key(1));
    let mut ran = false;
    let result = table
        .update_cell_index(context, &cell, &loader, |_, _levels| {
            ran = true;
            Ok(((), true))
        })
        .expect("an absent cell is not an error");
    assert!(result.is_none());
    assert!(!ran, "the update must not run on an empty cell");
    assert_eq!(table.entry_state_for_test(context, &cell), "Empty");
}

#[test]
fn unloaded_cell_is_loaded_first_and_unchanged_stays_clean() {
    let fixture = Fixture::reopened();
    assert_eq!(fixture.state(), "Unloaded");
    let seen = fixture
        .update(|table| {
            (
                (table.get(&key(1)), table.get(&key(2)), table.get(&key(3))),
                false,
            )
        })
        .expect("load succeeds")
        .expect("the cell holds an entry");
    assert_eq!(
        seen,
        (
            Some(WalPosition::test_value(10)),
            Some(WalPosition::test_value(20)),
            None
        ),
        "the update sees the loaded L0 blob"
    );
    assert_eq!(fixture.state(), "Loaded", "unchanged leaves the cell clean");
    assert!(fixture.loader.flushed.lock().is_empty());
}

#[test]
fn changed_cell_is_marked_dirty_and_the_flusher_writes_it() {
    let fixture = Fixture::reopened();
    fixture
        .update(|table| {
            table.insert(key(1), WalPosition::test_value(99));
            ((), true)
        })
        .expect("load succeeds")
        .expect("the cell holds an entry");
    assert_eq!(fixture.state(), "DirtyLoaded");

    fixture
        .table
        .with_entry_for_test(fixture.context(), &fixture.cell(), |entry| {
            entry.sync_flush(&fixture.loader, RelocationFlush::None)
        })
        .expect("the flush succeeds");
    let flushed = fixture.loader.flushed.lock();
    assert_eq!(flushed.len(), 1, "exactly one blob was written");
    assert_eq!(flushed[0].get(&key(1)), Some(WalPosition::test_value(99)));
    assert_eq!(flushed[0].get(&key(2)), Some(WalPosition::test_value(20)));
}

/// Negative control: an implementation that dirtied the cell through the write
/// path (`mark_dirty` / `set_last_processed`) would move the frontier or the
/// cadence; both must be bit-for-bit what the restore left.
#[test]
fn update_leaves_the_frontier_and_cadence_alone() {
    let fixture = Fixture::reopened();
    let before = fixture.frontier_and_cadence();
    assert_eq!(before, (LastProcessed::new(FRONTIER), FRONTIER));
    for changed in [false, true] {
        fixture
            .update(|table| {
                table.insert(key(2), WalPosition::test_value(30 + u64::from(changed)));
                ((), changed)
            })
            .expect("load succeeds");
        assert_eq!(
            fixture.frontier_and_cadence(),
            before,
            "changed={changed}: an index-only update must not move last_processed or flushed_at"
        );
    }
}

#[test]
fn loader_failure_maps_to_the_loader_error() {
    let fixture = Fixture::reopened();
    fixture.loader.fail_load.store(true, Ordering::Relaxed);
    let mut ran = false;
    let error = fixture
        .update(|_| {
            ran = true;
            ((), true)
        })
        .expect_err("the load fails");
    assert!(matches!(
        error,
        CellUpdateError::Loader(DbError::CrCorrupted)
    ));
    assert!(!ran);
    assert_eq!(
        fixture.state(),
        "Unloaded",
        "a failed load leaves the cell as it was"
    );
}

// ---- a re-point racing an async flush -------------------------------------
//
// `IndexTable::unmerge_flushed` identifies flushed entries logically, and a
// re-point keeps the identity and changes only the address, so a re-point that
// landed between a flush snapshot and its completion would be unmerged away.
// `update_cell_index` is correct only because it waits the flush out. The
// double below has exactly that identity ([`LogicalPosition`] orders and
// compares by ordinal alone).

/// The frontier every flush in this section is made durable up to.
const PROCESSED: TestCut = TestCut(1_000);

/// Loader over [`LogicalPosition`] with a one-slot "disk": `flush` stores the
/// flushed table, `load` reads it back.
#[derive(Default)]
struct LogicalDiskLoader {
    disk: parking_lot::Mutex<IndexTable<LogicalPosition>>,
}

impl Loader for LogicalDiskLoader {
    type Position = LogicalPosition;
    type Error = Infallible;

    fn load(
        &self,
        _ks: &KeySpaceDesc,
        _position: WalPosition,
    ) -> Result<IndexTable<LogicalPosition>, Infallible> {
        Ok(self.disk.lock().clone())
    }

    fn index_reader(&self, _position: WalPosition) -> Result<WalRandomRead, Infallible> {
        Ok(WalRandomRead::Mapped(Bytes::new()))
    }

    fn flush_supported(&self) -> bool {
        true
    }

    fn flush(
        &self,
        _ks: KeySpace,
        data: &IndexTable<LogicalPosition>,
    ) -> Result<WalPosition, Infallible> {
        *self.disk.lock() = data.clone();
        Ok(WalPosition::test_value(100))
    }

    fn last_processed(&self) -> TestCut {
        PROCESSED
    }

    fn current(&self) -> TestCut {
        PROCESSED
    }

    fn min_position(&self) -> TestCut {
        TestCut(0)
    }

    fn current_position(&self) -> LogicalPosition {
        LogicalPosition::new(PROCESSED.0, 0)
    }

    fn flushed_at_bytes(&self, cut: TestCut) -> u64 {
        cut.0
    }
}

/// Re-points key 1 to `address`, keeping its identity.
fn repoint_key(index: &mut IndexTable<LogicalPosition>, address: u32) {
    index.apply_update(&key(1), |tagged| {
        let (position, kind) = LogicalPosition::untag(*tagged);
        *tagged = LogicalPosition::new(position.ordinal, address).tag(kind);
    });
}

/// A cell holding key 1 at (ordinal 10, address 100) whose async flush has
/// been requested and is held in flight: the test plays the flusher thread and
/// decides when the flush completes.
struct InFlight {
    table: LargeTable<LogicalPosition>,
    loader: LogicalDiskLoader,
    /// The overlay snapshot the flusher was handed.
    dirty: Arc<IndexTable<LogicalPosition>>,
    /// One message when a writer first finds the cell waiting for its flush.
    waiting: mpsc::Receiver<()>,
}

impl InFlight {
    fn start() -> Self {
        let shape = KeyShape::new_single(8, 1, KeyType::uniform(1));
        let loader = LogicalDiskLoader::default();
        let (sender, commands) = mpsc::channel();
        let table = LargeTable::<LogicalPosition>::from_unloaded(
            &shape,
            &LargeTableContainer::new_from_key_shape(&shape, SnapshotEntryData::empty()),
            Arc::new(Config::small()),
            IndexFlusher::new(vec![sender], Metrics::new()),
            Metrics::new(),
            SharedLastProcessed::default(),
            &loader,
            Arc::default(),
        );
        let (notify, waiting) = mpsc::channel();
        let first_wait = AtomicBool::new(true);
        table.fp.0.write().fp_waiting_for_flush = FailPoint::from_fn(move || {
            // Relaxed: this flag only limits the notification to one.
            if first_wait.swap(false, Ordering::Relaxed) {
                notify.send(()).expect("the test outlives the writer");
            }
        });

        let context = table.ks_context(KeySpace::first());
        let cell = context.ks_config.cell_id(&key(1));
        table
            .insert(
                context,
                key(1),
                key(1),
                LogicalPosition::new(10, 100),
                &Bytes::from_static(b"v"),
                &loader,
            )
            .expect("the loader cannot fail");
        table.with_entry_for_test(context, &cell, |entry| {
            entry.request_flush_if_behind(&table.flusher, PROCESSED, 1_000, u64::MAX);
        });
        let FlusherCommand::Command(command) = commands
            .try_recv()
            .expect("requesting the flush queued a command")
        else {
            panic!("expected a flush command");
        };
        let FlushKind::Flush { dirty, .. } = command.flush_kind else {
            panic!("expected a flush of the dirty overlay");
        };
        Self {
            table,
            loader,
            dirty,
            waiting,
        }
    }

    fn context(&self) -> &KsContext {
        self.table.ks_context(KeySpace::first())
    }

    fn cell(&self) -> CellId {
        self.context().ks_config.cell_id(&key(1))
    }

    /// What the flusher thread does once the flush work is done: write the
    /// snapshot and report the completion to the cell.
    fn complete_flush(&self) {
        let position = self
            .loader
            .flush(KeySpace::first(), &self.dirty)
            .expect("the loader cannot fail");
        self.table.update_flushed_index(
            self.context(),
            &self.cell(),
            self.dirty.clone(),
            IndexLevels::single(position),
        );
    }

    /// The flushed blob's address for key 1.
    fn flushed_address(&self) -> Option<u32> {
        self.loader.disk.lock().get(&key(1)).map(|p| p.address)
    }
}

/// A re-point that arrives while the cell's flush is in flight waits for it,
/// then applies on top of the flushed state: the cell serves the new address
/// and is dirty, so the next flush persists it. (The flushed blob keeps the
/// address it was snapshotted with.)
#[test]
fn repoint_during_an_in_flight_flush_waits_for_it_and_survives_its_completion() {
    let flight = InFlight::start();
    let applied = AtomicBool::new(false);
    let (table, loader) = (&flight.table, &flight.loader);
    let (context, cell) = (flight.context(), flight.cell());
    thread::scope(|scope| {
        let updater = scope.spawn(|| {
            let result = table.update_cell_index(context, &cell, loader, |index, _levels| {
                repoint_key(index, 777);
                Ok(((), true))
            });
            applied.store(true, Ordering::SeqCst);
            result
        });
        flight
            .waiting
            .recv_timeout(Duration::from_secs(20))
            .expect("the re-point must find the cell waiting for its flush");
        // The flush is still pending, so the re-point cannot have applied.
        assert!(
            !applied.load(Ordering::SeqCst),
            "the re-point ran while the flush was in flight"
        );
        flight.complete_flush();
        let outcome = updater.join().expect("the updater does not panic");
        assert_eq!(outcome.expect("the loader cannot fail"), Some(()));
    });

    assert_eq!(
        flight.flushed_address(),
        Some(100),
        "the flush wrote the address it snapshotted"
    );
    let GetResult::WalPosition(position, _) = flight
        .table
        .get(flight.context(), &key(1), &flight.loader)
        .expect("the loader cannot fail")
    else {
        panic!("key 1 must resolve to a position");
    };
    assert_eq!(
        (position.ordinal, position.address),
        (10, 777),
        "the cell serves the re-pointed address"
    );
    assert_eq!(
        flight
            .table
            .entry_state_for_test(flight.context(), &flight.cell()),
        "DirtyLoaded",
        "the re-point is dirty and waits for the next flush"
    );
}

/// Control for the test above: the wait is what keeps the re-point. The same
/// mutation applied to the cell *without* waiting (bypassing
/// `update_cell_index`) is unmerged away by the flush completion, because
/// `unmerge_flushed` matches the flushed entry to the live one by identity and
/// sees no change. Pins the reason `update_cell_index` must serialise.
#[test]
fn control_unserialised_repoint_is_lost_to_the_logical_unmerge() {
    let flight = InFlight::start();
    flight
        .table
        .with_entry_for_test(flight.context(), &flight.cell(), |entry| {
            repoint_key(entry.data.make_mut(), 777);
        });
    flight.complete_flush();
    let live = flight
        .table
        .with_entry_for_test(flight.context(), &flight.cell(), |entry| {
            entry.data.get(&key(1))
        });
    assert_eq!(
        live, None,
        "the unmerge removed the re-pointed overlay entry"
    );
    assert_eq!(
        flight.flushed_address(),
        Some(100),
        "so only the stale address survives"
    );
}

/// `update_index` asserts that no flush is pending when its mutation applies.
#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "update_index while an async flush is in flight")]
fn update_index_asserts_no_flush_is_pending() {
    let flight = InFlight::start();
    flight
        .table
        .with_entry_for_test(flight.context(), &flight.cell(), |entry| {
            entry.update_index(&flight.loader, |index, _levels| {
                repoint_key(index, 777);
                Ok(((), true))
            })
        })
        .expect("the assertion fires before the loader is asked");
}
