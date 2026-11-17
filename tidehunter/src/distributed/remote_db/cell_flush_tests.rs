//! Flusher pool scheduling, ownership, bounded queues, stop and failure.
//! Deterministic barriers only: a store gate that blocks a chosen worker
//! inside `append`, and condvar waits on observable events (store calls,
//! hook calls, scheduler passes). Each wait has a guard deadline that fails
//! the test; no test sleeps or retries. The test store installs empty bases,
//! so most tests check scheduling and capture lifetimes, not frame content;
//! one test runs the pool over the production `IndexStoreWriter`.

use super::*;
use crate::distributed::native_index::{IndexReadCut, IndexStore, IndexStoreFileId};
use crate::distributed::native_publication::NativePendingSink;
use crate::distributed::types::{
    BatchId, MemberChecksum, PayloadDigest, PhysicalValueAddress, RecordDescriptor, RecordKind,
    ValueRecordReference,
};
use crate::index::pending_table::Transaction;
use crate::key_shape::{KeyIndexing, KeyShape, KeyShapeBuilder, KeySpaceConfig, KeyType};
use minibytes::Bytes;
use std::collections::HashMap;

const DATABASE: [u8; 16] = [7; 16];
const EPOCH: u64 = 11;
const KS: KeySpace = KeySpace(0);
const BUDGET: usize = 16 << 20;
const GUARD: Duration = Duration::from_secs(30);

fn shape() -> KeyShape {
    let mut builder = KeyShapeBuilder::new();
    // Variable keys routed by their first byte: one cell per first byte.
    builder.add_key_space_config_indexing(
        "cells",
        KeyIndexing::variable_length(),
        1,
        KeyType::prefix_uniform(1, 0),
        KeySpaceConfig::new(),
    );
    builder.build()
}

struct Fixture {
    shape: KeyShape,
    index: Arc<NativeIndex>,
    next: u64,
}

impl Fixture {
    fn new(shards: usize) -> Self {
        let shape = shape();
        let index = Arc::new(NativeIndex::with_shards(&shape, DATABASE, BUDGET, shards).unwrap());
        Self {
            shape,
            index,
            next: 0,
        }
    }

    fn cell(&self, first: u8) -> CellKey {
        (KS, self.shape.ks(KS).cell_id(&[first, 0]))
    }

    /// First bytes whose cells live in `shard`.
    fn cells_in_shard(&self, shard: usize, count: usize) -> Vec<u8> {
        (0..=u8::MAX)
            .filter(|first| {
                let (keyspace, cell) = self.cell(*first);
                self.index.shard_of_cell(keyspace, &cell) == shard
            })
            .take(count)
            .collect()
    }

    /// Publish one batch with one put per key, as the option-D publisher
    /// does under one writer hold. Returns its sequence.
    fn publish(&mut self, keys: &[[u8; 2]]) -> u64 {
        self.next += 1;
        let sequence = self.next;
        let mut writer = self.index.writer();
        let transaction = Transaction::default();
        for (operation, key) in keys.iter().enumerate() {
            let cell = self.shape.ks(KS).cell_id(key);
            let value = reference(sequence, u32::try_from(operation).unwrap());
            writer
                .with_pending_table(KS, &cell, |table| {
                    table.insert(Bytes::copy_from_slice(key), value, None, &transaction);
                })
                .unwrap();
        }
        transaction.commit();
        writer.promote_committed().unwrap();
        writer.publish_latest();
        sequence
    }

    fn dirty(&self, first: u8) -> CellDirty {
        let (keyspace, cell) = self.cell(first);
        self.index.cell_dirty(keyspace, &cell).unwrap()
    }

    fn latest(&self, key: [u8; 2]) -> Option<u64> {
        self.index
            .get(KS, &key, IndexReadCut::Latest)
            .unwrap()
            .map(|reference| reference.batch_version.sequence)
    }
}

fn through(sequence: u64) -> PrefixCut {
    PrefixCut::Through(BatchVersion {
        epoch: EPOCH,
        sequence,
    })
}

fn reference(sequence: u64, operation_index: u32) -> ValueRecordReference {
    ValueRecordReference {
        batch_id: BatchId {
            database: DATABASE,
            operation: u128::from(sequence).to_be_bytes(),
        },
        batch_version: BatchVersion {
            epoch: EPOCH,
            sequence,
        },
        batch_digest: PayloadDigest([0; 32]),
        batch_address: PhysicalValueAddress {
            partition: 0,
            segment_generation: 1,
            offset: sequence * 64,
            len: 64,
        },
        descriptor: RecordDescriptor {
            operation_index,
            offset: 0,
            len: 16,
            keyspace: KS.0,
            kind: RecordKind::Put,
            key_len: 2,
            value_offset: 8,
            value_len: 8,
            checksum: MemberChecksum(0),
        },
    }
}

/// Wait on `changed` until `done` holds under `state`, or fail the test.
fn wait_until<T>(
    state: &Mutex<T>,
    changed: &Condvar,
    what: &str,
    mut done: impl FnMut(&mut T) -> bool,
) {
    let started = Instant::now();
    let mut guard = state.lock();
    while !done(&mut guard) {
        let remaining = GUARD
            .checked_sub(started.elapsed())
            .unwrap_or_else(|| panic!("timed out waiting for {what}"));
        let _ = changed.wait_for(&mut guard, remaining);
    }
}

#[derive(Default)]
struct HookEvents {
    regrants: u64,
    failures: Vec<FlushError>,
}

#[derive(Default)]
struct TestHooks {
    publisher: Mutex<()>,
    pressure: Mutex<ReplayPressure>,
    now: Mutex<Option<Instant>>,
    events: Mutex<HookEvents>,
    changed: Condvar,
}

impl FlushHooks for Arc<TestHooks> {
    fn with_publisher<'w, R>(
        &self,
        writer: &mut IndexWriter<'w>,
        f: impl FnOnce(&mut IndexWriter<'w>) -> R,
    ) -> R {
        let _publisher = self.publisher.lock();
        f(writer)
    }

    fn replay_pressure(&self) -> ReplayPressure {
        *self.pressure.lock()
    }

    fn regrant(&self) {
        self.events.lock().regrants += 1;
        self.changed.notify_all();
    }

    fn fail(&self, error: &FlushError) {
        self.events.lock().failures.push(error.clone());
        self.changed.notify_all();
    }

    fn now(&self) -> Instant {
        self.now.lock().unwrap_or_else(Instant::now)
    }
}

#[derive(Default)]
struct StoreState {
    /// `(worker, cell)` per append, in order.
    appends: Vec<(usize, CellKey)>,
    seals: Vec<usize>,
    /// Workers whose `append` blocks until the gate opens.
    closed: Vec<bool>,
    blocked: Vec<bool>,
    panic: Vec<bool>,
}

#[derive(Default)]
struct StoreLog {
    state: Mutex<StoreState>,
    changed: Condvar,
}

impl StoreLog {
    fn new(workers: usize) -> Arc<Self> {
        let log = Self::default();
        {
            let mut state = log.state.lock();
            state.closed = vec![false; workers];
            state.blocked = vec![false; workers];
            state.panic = vec![false; workers];
        }
        Arc::new(log)
    }

    fn set_gate(&self, worker: usize, closed: bool) {
        self.state.lock().closed[worker] = closed;
        self.changed.notify_all();
    }

    fn wait_blocked(&self, worker: usize) {
        wait_until(&self.state, &self.changed, "a blocked append", |state| {
            state.blocked[worker]
        });
    }

    fn appends(&self) -> Vec<(usize, CellKey)> {
        self.state.lock().appends.clone()
    }
}

struct TestStore {
    worker: usize,
    log: Arc<StoreLog>,
    pending: usize,
}

impl CellStore for TestStore {
    fn append(&mut self, cell: &PreparedCell) -> Result<u64, NativeIndexError> {
        let mut state = self.log.state.lock();
        state.appends.push((self.worker, cell.identity().clone()));
        self.log.changed.notify_all();
        assert!(!state.panic[self.worker], "injected flusher panic");
        let started = Instant::now();
        while state.closed[self.worker] {
            state.blocked[self.worker] = true;
            self.log.changed.notify_all();
            let remaining = GUARD
                .checked_sub(started.elapsed())
                .unwrap_or_else(|| panic!("store gate {} never opened", self.worker));
            let _ = self.log.changed.wait_for(&mut state, remaining);
        }
        state.blocked[self.worker] = false;
        self.pending += 1;
        Ok(0)
    }

    fn seal(&mut self, bases: &mut Vec<NewCellBase>) -> Result<(), NativeIndexError> {
        self.log.state.lock().seals.push(self.worker);
        self.log.changed.notify_all();
        bases.extend((0..self.pending).map(|_| NewCellBase::empty()));
        self.pending = 0;
        Ok(())
    }
}

fn start(
    fixture: &Fixture,
    hooks: &Arc<TestHooks>,
    log: &Arc<StoreLog>,
    workers: usize,
    policy: FlushPolicy,
) -> CellFlushPool<Arc<TestHooks>> {
    CellFlushPool::start_with(
        Arc::clone(&fixture.index),
        Arc::clone(hooks),
        policy,
        workers,
        |worker| {
            Ok(TestStore {
                worker,
                log: Arc::clone(log),
                pending: 0,
            })
        },
    )
    .unwrap()
}

/// Every append was made by the worker that owns the cell's shard, and no
/// cell was appended by two workers.
fn assert_owned(fixture: &Fixture, appends: &[(usize, CellKey)], workers: usize) {
    let shards = fixture.index.shard_count();
    let mut owner: HashMap<CellKey, usize> = HashMap::new();
    for (worker, (keyspace, cell)) in appends {
        let shard = fixture.index.shard_of_cell(*keyspace, cell);
        assert!(shard_range(*worker, shards, workers).contains(&shard));
        let previous = owner.insert((*keyspace, cell.clone()), *worker);
        assert!(previous.is_none_or(|previous| previous == *worker));
    }
}

fn assert_flushed_through(fixture: &Fixture, firsts: &[u8], target: PrefixCut) {
    for first in firsts {
        let dirty = fixture.dirty(*first);
        assert!(
            dirty
                .oldest_uncovered
                .is_none_or(|oldest| !target.contains(oldest)),
            "cell {first} still misses a mutation through {target:?}"
        );
        assert!(dirty.coverage >= target);
        assert!(!dirty.queued && !dirty.in_flight);
    }
}

#[test]
fn shard_ranges_are_disjoint_and_cover_every_shard() {
    for shards in 1..=64 {
        for workers in 1..=shards.min(8) {
            let mut next = 0;
            for worker in 0..workers {
                let range = shard_range(worker, shards, workers);
                assert_eq!(range.start, next);
                assert!(!range.is_empty());
                next = range.end;
            }
            assert_eq!(next, shards);
        }
    }
    let index = NativeIndex::with_shards(&shape(), DATABASE, BUDGET, 64).unwrap();
    assert_eq!(
        flushers(&index),
        crate::parallelism::available().clamp(1, 8)
    );
}

#[test]
fn two_ranges_flush_concurrently_while_one_blocks() {
    let mut fixture = Fixture::new(2);
    let (left, right) = (fixture.cells_in_shard(0, 3), fixture.cells_in_shard(1, 3));
    for first in left.iter().chain(&right) {
        fixture.publish(&[[*first, 0]]);
    }
    let target = through(6);
    let hooks = Arc::new(TestHooks::default());
    let log = StoreLog::new(2);
    log.set_gate(0, true);
    let pool = start(&fixture, &hooks, &log, 2, FlushPolicy::default());
    pool.request_target(target);
    log.wait_blocked(0);
    // Worker 1 flushes and installs its whole range while worker 0 is held
    // inside `append`.
    wait_until(&hooks.events, &hooks.changed, "worker 1 installs", |_| {
        right
            .iter()
            .all(|first| fixture.dirty(*first).oldest_uncovered.is_none())
    });
    let seals = log.state.lock().seals.clone();
    assert!(!seals.is_empty() && seals.iter().all(|worker| *worker == 1));
    let held = left.iter().filter(|first| fixture.dirty(**first).in_flight);
    assert_eq!(held.count(), 1);
    assert!(
        left.iter()
            .all(|first| fixture.dirty(*first).oldest_uncovered.is_some())
    );
    assert!(log.state.lock().blocked[0]);
    log.set_gate(0, false);
    pool.wait_covered(target, GUARD).unwrap();
    let all: Vec<u8> = left.iter().chain(&right).copied().collect();
    assert_flushed_through(&fixture, &all, target);
    let appends = log.appends();
    assert_eq!(appends.len(), 6);
    assert_owned(&fixture, &appends, 2);
    pool.shutdown(StopMode::Drain).unwrap();
    assert_eq!(fixture.index.writer().cell_flights(), 0);
    assert!(hooks.events.lock().failures.is_empty());
}

#[test]
fn full_queue_keeps_dirty_cells_and_loses_no_work() {
    let mut fixture = Fixture::new(1);
    let firsts: Vec<u8> = (0..40).collect();
    for first in &firsts {
        fixture.publish(&[[*first, 0]]);
    }
    let target = through(40);
    let hooks = Arc::new(TestHooks::default());
    let log = StoreLog::new(1);
    log.set_gate(0, true);
    // No deadline pass: only the request and install progress wake the
    // scheduler, so the first pass's dispatch count stays exact.
    let policy = FlushPolicy {
        deadline: Duration::from_secs(3600),
        ..FlushPolicy::default()
    };
    let pool = start(&fixture, &hooks, &log, 1, policy);
    pool.request_target(target);
    log.wait_blocked(0);
    pool.wait_passes(1, GUARD);
    // A pass selects at most a queue's worth of cells per range.
    let stats = pool.stats();
    assert_eq!((stats.dispatched, stats.queue_full), (32, 0));
    // The worker holds one cell in `append`, so one slot is free: the next
    // pass sends one more cell and then finds the queue full.
    pool.wake(WakeReason::Dirty);
    pool.wait_passes(2, GUARD);
    let stats = pool.stats();
    assert_eq!((stats.dispatched, stats.queue_full), (33, 1));
    let dispatched = usize::try_from(stats.dispatched).unwrap();
    let mut sent = 0;
    for first in &firsts {
        let dirty = fixture.dirty(*first);
        if dirty.queued || dirty.in_flight {
            sent += 1;
        } else {
            // Not sent: the dirty state stays for a later pass.
            assert_eq!(dirty.versions, 1);
            assert!(dirty.oldest_uncovered.is_some());
        }
    }
    assert_eq!(sent, dispatched);
    log.set_gate(0, false);
    pool.wait_covered(target, GUARD).unwrap();
    assert_flushed_through(&fixture, &firsts, target);
    assert_eq!(pool.stats().captured, 40);
    pool.shutdown(StopMode::Drain).unwrap();
    assert_eq!(fixture.index.writer().cell_flights(), 0);
}

#[test]
fn pressure_target_is_covered_under_fair_service_without_arrivals() {
    let mut fixture = Fixture::new(4);
    let firsts: Vec<u8> = (0..70).collect();
    for first in &firsts {
        fixture.publish(&[[*first, 0]]);
    }
    let target = through(70);
    let hooks = Arc::new(TestHooks::default());
    // Lightly dirty cells, below every size threshold and never aged.
    *hooks.now.lock() = Some(fixture.dirty(0).dirty_since.unwrap());
    *hooks.pressure.lock() = ReplayPressure {
        lagging: PrefixCut::BeforeAll,
        drain: true,
    };
    // The production pool size `P = cell_flushers(4)`.
    let workers = flushers(&fixture.index);
    let log = StoreLog::new(workers);
    let pool = CellFlushPool::start(
        Arc::clone(&fixture.index),
        Arc::clone(&hooks),
        FlushPolicy::default(),
        |worker| {
            Ok(TestStore {
                worker,
                log: Arc::clone(&log),
                pending: 0,
            })
        },
    )
    .unwrap();
    assert_eq!(pool.workers(), workers);
    pool.wake(WakeReason::AdmissionBlocked);
    pool.wait_covered(target, GUARD).unwrap();
    assert_eq!(pool.covered(), target);
    assert_flushed_through(&fixture, &firsts, target);
    let appends = log.appends();
    assert_eq!(appends.len(), 70);
    assert_owned(&fixture, &appends, workers);
    // The first pass finds every cell missing; a later pass, woken by install
    // progress, proves the fixed target covered.
    let stats = pool.stats();
    assert!(stats.passes >= 2);
    assert_eq!((stats.captured, stats.installed), (70, 70));
    pool.shutdown(StopMode::Drain).unwrap();
}

#[test]
fn offline_flush_binds_one_sealed_base() {
    let mut fixture = Fixture::new(1);
    fixture.publish(&[[3, 0], [3, 1]]);
    let log = StoreLog::new(1);
    let mut store = TestStore {
        worker: 0,
        log: Arc::clone(&log),
        pending: 0,
    };
    let (keyspace, cell) = fixture.cell(3);
    let permit = fixture.index.reserve_cell_flush(0).unwrap();
    let outcome = {
        let mut writer = fixture.index.writer();
        let target = writer.published_prefix();
        writer
            .capture_cell(keyspace, &cell, target, permit)
            .unwrap()
    };
    let CellCaptureOutcome::Captured(capture) = outcome else {
        panic!("a dirty cell is captured");
    };
    let result = flush_prepared(&mut store, capture.prepare().unwrap()).unwrap();
    let installed = fixture.index.writer().install_cell(result);
    assert!(matches!(installed, CellInstall::Installed { coverage, .. } if coverage == through(1)));
    drop(installed);
    assert_eq!(log.state.lock().seals, vec![0]);
    assert_eq!(fixture.dirty(3).oldest_uncovered, None);
    assert_eq!(fixture.index.writer().cell_flights(), 0);
}

/// The production store under the production pool: each worker writes its
/// own segment stream; every installed base is mapped from a synced
/// canonical segment of that stream; reads are unchanged; GC keeps every
/// segment a live base names.
#[test]
fn index_store_writers_flush_real_segments_through_the_pool() {
    let mut fixture = Fixture::new(4);
    let firsts: Vec<u8> = (0..70).collect();
    for first in &firsts {
        fixture.publish(&[[*first, 0], [*first, 1]]);
    }
    let target = through(70);
    let temp = tempdir::TempDir::new("cell-flush-index-store").unwrap();
    let (store, scan) = IndexStore::open(temp.path(), EPOCH, &[]).unwrap();
    assert!(scan.deleted.is_empty());
    let hooks = Arc::new(TestHooks::default());
    let workers = flushers(&fixture.index);
    let pool = CellFlushPool::start(
        Arc::clone(&fixture.index),
        Arc::clone(&hooks),
        FlushPolicy::default(),
        |worker| IndexStoreWriter::new(&store, u16::try_from(worker).unwrap(), 1 << 20),
    )
    .unwrap();
    pool.request_target(target);
    pool.wait_covered(target, GUARD).unwrap();
    assert_flushed_through(&fixture, &firsts, target);
    let stats = pool.stats();
    assert_eq!((stats.captured, stats.installed), (70, 70));
    pool.shutdown(StopMode::Drain).unwrap();
    assert!(hooks.events.lock().failures.is_empty());
    assert_eq!(fixture.index.writer().cell_flights(), 0);
    for (sequence, first) in (1..).zip(&firsts) {
        assert_eq!(fixture.latest([*first, 0]), Some(sequence));
        assert_eq!(fixture.latest([*first, 1]), Some(sequence));
    }
    let reachable = fixture.index.reachable_files();
    let mut on_disk: Vec<String> = std::fs::read_dir(temp.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect();
    on_disk.sort();
    // Each segment holds only installed frames of cells flushed once.
    assert_eq!(reachable, on_disk);
    for name in &reachable {
        let Some(IndexStoreFileId::Segment { stream_id, .. }) =
            IndexStoreFileId::parse_canonical(name)
        else {
            panic!("{name} is not a canonical segment");
        };
        assert!(usize::from(stream_id) < workers);
    }
    let sweep = store.collect_garbage(&[], &fixture.index).unwrap();
    assert!(sweep.deleted.is_empty() && !sweep.more);
}

#[test]
fn aged_cold_cell_is_served_before_a_hot_range() {
    let mut fixture = Fixture::new(1);
    let cold = 200;
    fixture.publish(&[[cold, 0]]);
    let cold_since = fixture.dirty(cold).dirty_since.unwrap();
    // More hot cells than the queue holds, each at the size threshold.
    let hot: Vec<u8> = (0..40).collect();
    for first in &hot {
        fixture.publish(&[[*first, 0], [*first, 1], [*first, 2], [*first, 3]]);
    }
    let hooks = Arc::new(TestHooks::default());
    *hooks.now.lock() = Some(cold_since + Duration::from_secs(1));
    let log = StoreLog::new(1);
    log.set_gate(0, true);
    let policy = FlushPolicy {
        dirty_versions: 4,
        ..FlushPolicy::default()
    };
    let pool = start(&fixture, &hooks, &log, 1, policy);
    pool.wake(WakeReason::Dirty);
    log.wait_blocked(0);
    assert_eq!(log.appends()[0].1, fixture.cell(cold));
    log.set_gate(0, false);
    let target = through(41);
    pool.request_target(target);
    pool.wait_covered(target, GUARD).unwrap();
    let mut all = hot.clone();
    all.push(cold);
    assert_flushed_through(&fixture, &all, target);
    pool.shutdown(StopMode::Drain).unwrap();
}

#[test]
fn cancel_restores_captures_unqueues_and_joins() {
    let mut fixture = Fixture::new(1);
    let firsts: Vec<u8> = (0..5).collect();
    for first in &firsts {
        fixture.publish(&[[*first, 0]]);
    }
    let hooks = Arc::new(TestHooks::default());
    let log = StoreLog::new(1);
    log.set_gate(0, true);
    let pool = start(&fixture, &hooks, &log, 1, FlushPolicy::default());
    pool.request_target(through(5));
    log.wait_blocked(0);
    // The worker stays held in `append`, so the pool is not joined: the
    // waiter is released by the scheduler's exit alone.
    let waited = thread::scope(|scope| {
        let waiter = scope.spawn(|| pool.wait_covered(through(5), GUARD));
        pool.request_stop(StopMode::Cancel);
        waiter.join().unwrap()
    });
    assert!(matches!(waited, Err(FlushError::Stopped)));
    assert!(log.state.lock().blocked[0]);
    log.set_gate(0, false);
    let stats = {
        let stats = pool.stats();
        pool.join().unwrap();
        stats
    };
    assert!(stats.dispatched >= 1);
    assert_eq!(fixture.index.writer().cell_flights(), 0);
    let state = log.state.lock();
    assert_eq!(state.appends.len(), 1);
    assert!(state.seals.is_empty());
    drop(state);
    for (sequence, first) in (1..).zip(&firsts) {
        let dirty = fixture.dirty(*first);
        assert!(!dirty.queued && !dirty.in_flight);
        // Nothing became durable: every reference is still unrepresented
        // and readable.
        assert_eq!(dirty.versions, 1);
        assert_eq!(dirty.coverage, PrefixCut::BeforeAll);
        assert_eq!(fixture.latest([*first, 0]), Some(sequence));
    }
    assert!(hooks.events.lock().failures.is_empty());
}

#[test]
fn worker_panic_fails_the_core_and_wakes_waiters() {
    let mut fixture = Fixture::new(2);
    let left = fixture.cells_in_shard(0, 1)[0];
    let right = fixture.cells_in_shard(1, 1)[0];
    fixture.publish(&[[left, 0]]);
    fixture.publish(&[[right, 0]]);
    let target = through(2);
    let hooks = Arc::new(TestHooks::default());
    let log = StoreLog::new(2);
    log.state.lock().panic[0] = true;
    let pool = start(&fixture, &hooks, &log, 2, FlushPolicy::default());
    let waited = thread::scope(|scope| {
        let waiter = scope.spawn(|| pool.wait_covered(target, GUARD));
        pool.request_target(target);
        waiter.join().unwrap()
    });
    let panicked = |error: &FlushError| {
        matches!(
            error,
            FlushError::Panicked {
                role: FlushRole::Worker(0)
            }
        )
    };
    assert!(panicked(&waited.unwrap_err()));
    let failures = hooks.events.lock().failures.clone();
    assert_eq!(failures.len(), 1);
    assert!(panicked(&failures[0]));
    assert!(panicked(&pool.shutdown(StopMode::Drain).unwrap_err()));
}
