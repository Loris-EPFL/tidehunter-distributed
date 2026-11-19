//! Compactor at leased Exact captures over five real owners
//! (COMPACTOR-PARITY.md §5): the named force without the Sui write gate,
//! reopen without callbacks, the recovery baseline, and a paused callback.

use super::pipeline_tests::Fixture;
use super::*;
use crate::distributed::native_index::CompactorCadence;
use crate::distributed::sui::Db;
use crate::key_shape::{Compactor, KeyShapeBuilder, KeySpaceConfig, KeyType};
use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrder};

const ID_BYTES: usize = 4;

fn object_key(id: u8, version: u8) -> Vec<u8> {
    vec![id, 0, 0, 0, 0, 0, 0, version]
}

fn cadence(max_dirty_keys: usize) -> CompactorCadence {
    CompactorCadence {
        max_dirty_keys,
        unload_jitter_pct: 0,
        snapshot_written_bytes: u64::MAX,
        snapshot_unload_threshold: u64::MAX,
    }
}

/// Replica of Sui's `objects` compactor: reverse scan, keep the greatest key
/// of each ObjectID prefix. Counts invocations.
fn objects_compactor(calls: &Arc<AtomicUsize>) -> Compactor {
    let calls = Arc::clone(calls);
    Box::new(move |keys| {
        calls.fetch_add(1, AtomicOrder::SeqCst);
        let mut retain = HashSet::new();
        let mut last: Option<Vec<u8>> = None;
        for key in keys.rev() {
            let id = &key[..ID_BYTES];
            if last.as_deref() != Some(id) {
                last = Some(id.to_vec());
                retain.insert(key.clone());
            }
        }
        retain
    })
}

/// `objects` (callback, one cell) and `plain` (no callback).
fn shape(compactor: Compactor) -> KeyShape {
    let mut builder = KeyShapeBuilder::new();
    builder.add_key_space_config(
        "objects",
        8,
        1,
        KeyType::uniform(1),
        KeySpaceConfig::new().with_compactor(compactor),
    );
    builder.add_key_space("plain", 8, 1, KeyType::uniform(1));
    builder.build()
}

fn fixture(compactor: Compactor, max_dirty_keys: usize) -> Fixture {
    Fixture::new_with_config(128, shape(compactor), 2, |config| {
        config.compactor_cadence = cadence(max_dirty_keys);
    })
}

/// Every key the database returns for `objects`, by both scan directions,
/// checked against point reads and multi-get of `written`.
fn visible_objects(db: &Arc<Db>, written: &[Vec<u8>]) -> Vec<Vec<u8>> {
    let objects = db.ks("objects");
    let forward: Vec<Vec<u8>> = db
        .iterator(objects)
        .map(|row| row.unwrap().0.to_vec())
        .collect();
    let mut reverse_iterator = db.iterator(objects);
    reverse_iterator.reverse();
    let mut reverse: Vec<Vec<u8>> = reverse_iterator
        .map(|row| row.unwrap().0.to_vec())
        .collect();
    reverse.reverse();
    assert_eq!(forward, reverse);
    let multi = db.multi_get(objects, written);
    for (key, value) in written.iter().zip(multi) {
        let present = forward.contains(key);
        assert_eq!(value.unwrap().is_some(), present, "multi_get {key:?}");
        assert_eq!(db.exists(objects, key).unwrap(), present, "exists {key:?}");
        assert_eq!(
            db.get(objects, key).unwrap().is_some(),
            present,
            "get {key:?}"
        );
    }
    forward
}

#[test]
fn named_force_compacts_objects_without_the_write_gate_and_survives_reopen() {
    let calls = Arc::new(AtomicUsize::new(0));
    let fixture = fixture(objects_compactor(&calls), 1 << 20);
    let path = fixture.temp.path().join("authority");
    let db = Db::open_remote_local_fixture(&path, fixture.shape.clone(), fixture.config.clone())
        .unwrap();
    let objects = db.ks("objects");
    // 16 objects with 1-3 versions, written version round by version round
    // (in batches within the fixture's 4-operation limit); every seventh
    // latest version is an object tombstone (a value-bearing put).
    let mut written = Vec::new();
    let mut latest = BTreeMap::new();
    for version in 0..3u8 {
        let round: Vec<u8> = (0..16u8).filter(|id| version <= id % 3).collect();
        for ids in round.chunks(4) {
            let mut batch = db.write_batch();
            for &id in ids {
                let key = object_key(id, version);
                let tombstone = version == id % 3 && id % 7 == 0;
                batch.write(objects, key.clone(), vec![u8::from(tombstone); 4]);
                written.push(key.clone());
                latest.insert(id, key);
            }
            batch.commit().unwrap();
        }
    }
    let mut expected: Vec<Vec<u8>> = latest.into_values().collect();
    expected.sort();
    assert_eq!(visible_objects(&db, &written).len(), written.len());
    // The force completes while this thread holds the shared write gate, so
    // it never takes the exclusive one.
    let gate = db.shared_write_gate_for_test();
    let (done, completed) = mpsc::sync_channel(1);
    let forcing = Arc::clone(&db);
    let worker = thread::spawn(move || {
        let _ = done.send(forcing.force_rebuild_control_region());
    });
    completed
        .recv_timeout(Duration::from_secs(30))
        .expect("named force needs no exclusive write gate")
        .unwrap();
    drop(gate);
    worker.join().unwrap();
    assert_eq!(calls.load(AtomicOrder::SeqCst), 1);
    assert_eq!(visible_objects(&db, &written), expected);
    drop(db);
    // Reopen with a callback that only counts: no invocation through open,
    // replay or reads, and the same compacted view.
    let reopened = Arc::new(AtomicUsize::new(0));
    let db = Db::open_remote_local_fixture(
        &path,
        shape(objects_compactor(&reopened)),
        fixture.config.clone(),
    )
    .unwrap();
    assert_eq!(visible_objects(&db, &written), expected);
    assert_eq!(reopened.load(AtomicOrder::SeqCst), 0);
}

#[test]
fn recovery_baseline_never_calls_back_and_the_next_capture_does() {
    let calls = Arc::new(AtomicUsize::new(0));
    let fixture = fixture(objects_compactor(&calls), 1);
    let objects = |id: u8, version: u8| Mutation::Put {
        keyspace: KeySpace(0),
        key: object_key(id, version).into(),
        value: vec![version; 8].into(),
    };
    let db = fixture.open();
    for version in 0..3u8 {
        db.write_batch(
            fixture.operation(0, u64::from(version)),
            vec![objects(1, version)],
        )
        .unwrap();
    }
    // Crash: the three batches stay a WAL suffix.
    db.core.test_hooks.lock().skip_shutdown_checkpoint = true;
    db.shutdown().unwrap();
    // Release the authority lock before reopening.
    drop(db);
    let mut db = fixture.open();
    assert_eq!(db.core.stats.lock().recovery.tail_batches, 3);
    // The replayed suffix crosses the threshold (3 > 1), but the open-time
    // baseline root runs no callback.
    assert_eq!(calls.load(AtomicOrder::SeqCst), 0);
    for version in 0..3u8 {
        assert!(
            db.get(KeySpace(0), &object_key(1, version))
                .unwrap()
                .is_some()
        );
    }
    // The carried debt is evaluated by the next serving capture.
    db.write_batch(fixture.operation(0, 10), vec![objects(2, 0)])
        .unwrap();
    db.flush_index().unwrap();
    assert_eq!(calls.load(AtomicOrder::SeqCst), 1);
    assert!(db.get(KeySpace(0), &object_key(1, 0)).unwrap().is_none());
    assert!(db.get(KeySpace(0), &object_key(1, 1)).unwrap().is_none());
    assert!(db.get(KeySpace(0), &object_key(1, 2)).unwrap().is_some());
    assert!(db.get(KeySpace(0), &object_key(2, 0)).unwrap().is_some());
}

/// A callback that blocks until released, recording entry.
struct Pause {
    state: parking_lot::Mutex<(bool, bool)>,
    changed: parking_lot::Condvar,
}

impl Pause {
    fn wait_entered(&self) {
        let mut state = self.state.lock();
        while !state.0 {
            assert!(
                !self
                    .changed
                    .wait_for(&mut state, Duration::from_secs(30))
                    .timed_out(),
                "callback never entered"
            );
        }
    }
    fn release(&self) {
        self.state.lock().1 = true;
        self.changed.notify_all();
    }
}

#[test]
fn a_paused_callback_blocks_no_write_and_a_second_force_is_busy() {
    let pause = Arc::new(Pause {
        state: parking_lot::Mutex::new((false, false)),
        changed: parking_lot::Condvar::new(),
    });
    let calls = Arc::new(AtomicUsize::new(0));
    let compactor: Compactor = {
        let pause = Arc::clone(&pause);
        let inner = objects_compactor(&calls);
        Box::new(move |keys| {
            let mut state = pause.state.lock();
            state.0 = true;
            pause.changed.notify_all();
            while !state.1 {
                pause.changed.wait(&mut state);
            }
            drop(state);
            inner(keys)
        })
    };
    let fixture = fixture(compactor, 1 << 20);
    let path = fixture.temp.path().join("authority");
    let db = Db::open_remote_local_fixture(&path, fixture.shape.clone(), fixture.config.clone())
        .unwrap();
    let (objects, plain) = (db.ks("objects"), db.ks("plain"));
    db.insert(objects, object_key(0, 0), vec![0]).unwrap();
    db.insert(objects, object_key(0, 1), vec![1]).unwrap();
    let forcing = Arc::clone(&db);
    let worker = thread::spawn(move || forcing.force_rebuild_control_region());
    pause.wait_entered();
    // While the policy stage is paused: the exact rejected key, another key
    // of that cell and another keyspace all reach ACK.
    db.insert(objects, object_key(0, 0), vec![9]).unwrap();
    db.insert(objects, object_key(5, 0), vec![5]).unwrap();
    db.insert(plain, vec![1; 8], vec![1]).unwrap();
    // The old view still serves every row.
    assert!(db.exists(objects, &object_key(0, 1)).unwrap());
    let busy = db.force_rebuild_control_region().unwrap_err();
    let busy = format!("{busy:?}");
    assert!(busy.contains("busy"), "{busy}");
    pause.release();
    worker.join().unwrap().unwrap();
    assert_eq!(calls.load(AtomicOrder::SeqCst), 1);
    // The rewrite after the evaluation cut wins over its mark.
    assert_eq!(
        db.get(objects, &object_key(0, 0)).unwrap().unwrap()[..],
        [9]
    );
    assert!(db.exists(objects, &object_key(0, 1)).unwrap());
    assert!(db.exists(objects, &object_key(5, 0)).unwrap());
    assert!(db.exists(plain, &[1; 8]).unwrap());
}

fn put_object(id: u8, version: u8) -> Mutation {
    Mutation::Put {
        keyspace: KeySpace(0),
        key: object_key(id, version).into(),
        value: vec![version; 8].into(),
    }
}

fn object_visible(db: &mut RemoteDatabase, id: u8, version: u8) -> bool {
    db.get(KeySpace(0), &object_key(id, version))
        .unwrap()
        .is_some()
}

/// A fault at each boundary of a compacting capture (COMPACTOR-PARITY.md §5).
/// Before the root is adopted the reopened database has every old row and
/// still owes the debt, so the next serving capture evaluates it once. Once
/// the root is adopted the reopened database has the marks and owes nothing.
/// Reopen and suffix replay never call back.
#[test]
fn a_fault_at_each_checkpoint_boundary_recovers_marks_only_after_adoption() {
    // 0 intent, 4 positions, 1 image, 2 metadata, 5 manifest replaced in
    // memory only: the durable manifest is the old one. 6 root adopted,
    // 3 live installed, 7 and 8 cleanup: the new root and its marks.
    for stage in 0..=8u8 {
        let adopted = matches!(stage, 3 | 6 | 7 | 8);
        let calls = Arc::new(AtomicUsize::new(0));
        let fixture = fixture(objects_compactor(&calls), 1);
        let db = fixture.open();
        // An earlier root (another keyspace, no callback), so the faulted
        // capture has an obsolete root to clean: stage 7 interrupts that
        // cleanup, which a first-ever root never runs.
        db.write_batch(
            fixture.operation(0, 20),
            vec![Mutation::Put {
                keyspace: KeySpace(1),
                key: vec![1; 8].into(),
                value: vec![1].into(),
            }],
        )
        .unwrap();
        db.flush_index().unwrap();
        assert_eq!(calls.load(AtomicOrder::SeqCst), 0, "stage {stage}");
        for version in 0..3u8 {
            db.write_batch(
                fixture.operation(0, u64::from(version)),
                vec![put_object(1, version)],
            )
            .unwrap();
        }
        // Three distinct dirty keys cross the threshold of one.
        db.core.test_hooks.lock().checkpoint_interruption = Some(stage);
        assert!(db.flush_index().is_err(), "stage {stage}");
        assert!(db.core.ensure_healthy().is_err(), "stage {stage}");
        let at_fault = calls.load(AtomicOrder::SeqCst);
        assert!(at_fault <= 1, "stage {stage}: {at_fault} callbacks");
        drop(db);
        let mut db = fixture.open();
        assert_eq!(
            calls.load(AtomicOrder::SeqCst),
            at_fault,
            "stage {stage}: reopen called back"
        );
        for version in 0..2 {
            assert_eq!(
                object_visible(&mut db, 1, version),
                !adopted,
                "stage {stage}, version {version}"
            );
        }
        assert!(object_visible(&mut db, 1, 2), "stage {stage}");
        assert_eq!(
            calls.load(AtomicOrder::SeqCst),
            at_fault,
            "stage {stage}: a read called back"
        );
        // One new key: with the carried debt (3 + 1 > 1) the next capture
        // evaluates; after adoption the debt is 1, not above the threshold.
        db.write_batch(fixture.operation(0, 10), vec![put_object(2, 0)])
            .unwrap();
        db.flush_index().unwrap();
        assert_eq!(
            calls.load(AtomicOrder::SeqCst) - at_fault,
            usize::from(!adopted),
            "stage {stage}"
        );
        for version in 0..2 {
            assert!(!object_visible(&mut db, 1, version), "stage {stage}");
        }
        assert!(object_visible(&mut db, 1, 2), "stage {stage}");
        assert!(object_visible(&mut db, 2, 0), "stage {stage}");
        db.shutdown().unwrap();
    }
}

/// Write one batch, start an ordinary capture and hold it paused after its
/// freeze. Returns the sender that resumes it.
fn pause_frozen_capture(fixture: &Fixture, db: &RemoteDatabase, seed: u8) -> mpsc::SyncSender<()> {
    db.write_batch(
        fixture.operation(0, u64::from(seed)),
        vec![put_object(1, seed)],
    )
    .unwrap();
    let (entered, waiting) = mpsc::sync_channel(1);
    let (resume, gate) = mpsc::sync_channel(1);
    db.core.test_hooks.lock().checkpoint_pause = Some((entered, gate));
    db.core.start_checkpoint().unwrap();
    waiting.recv_timeout(Duration::from_secs(30)).unwrap();
    resume
}

fn force_slot_held(db: &RemoteDatabase) -> bool {
    db.core.force_in_flight.load(AtomicOrder::Acquire)
}

fn wait_until(what: &str, done: impl Fn() -> bool) {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while !done() {
        assert!(std::time::Instant::now() < deadline, "{what}");
        thread::sleep(Duration::from_millis(5));
    }
}

/// COMPACTOR-PARITY.md §7.4: a force that arrives after the running capture
/// froze parks for the successor, which only a progress caller launches. Its
/// expired waiter withdraws it, so the slot frees with no other call and a
/// later force on the idle database runs instead of returning `Busy`.
#[test]
fn an_expired_waiter_withdraws_its_parked_force_and_frees_the_slot_when_idle() {
    let calls = Arc::new(AtomicUsize::new(0));
    let fixture = fixture(objects_compactor(&calls), 1 << 20);
    let db = fixture.open();
    let resume = pause_frozen_capture(&fixture, &db, 1);
    db.core.test_hooks.lock().force_timeout = Some(Duration::from_millis(300));
    let expired = db.core.force_rebuild_index().unwrap_err();
    assert!(
        matches!(expired, IndexForceError::DeadlineExpired),
        "{expired}"
    );
    assert!(!checkpoint_lifecycle::force_parked(&db.core));
    assert!(!force_slot_held(&db));
    // The paused capture finishes; nothing calls progress afterwards.
    resume.send(()).unwrap();
    db.core.test_hooks.lock().force_timeout = None;
    db.write_batch(fixture.operation(0, 2), vec![put_object(1, 2)])
        .unwrap();
    let report = db.core.force_rebuild_index().unwrap();
    assert!(report.cut.is_some());
    assert_eq!(calls.load(AtomicOrder::SeqCst), 1);
    assert!(!force_slot_held(&db));
    db.shutdown().unwrap();
}

/// COMPACTOR-PARITY.md §7.4: a force its capture took stays with the worker
/// past its waiter's deadline (the slot stays held, a second force is `Busy`)
/// and the worker frees the slot alone. A newer force B then parks; a late
/// withdrawal by any other waiter (A's expiry racing B) leaves B's ticket,
/// and B completes.
#[test]
fn a_taken_force_outlives_its_deadline_and_a_stale_waiter_never_withdraws_a_newer_force() {
    let calls = Arc::new(AtomicUsize::new(0));
    let fixture = fixture(objects_compactor(&calls), 1 << 20);
    let db = fixture.open();
    db.write_batch(fixture.operation(0, 1), vec![put_object(1, 1)])
        .unwrap();
    let (entered, waiting) = mpsc::sync_channel(1);
    let (resume, gate) = mpsc::sync_channel(1);
    {
        let mut hooks = db.core.test_hooks.lock();
        hooks.checkpoint_pause = Some((entered, gate));
        hooks.force_timeout = Some(Duration::from_millis(300));
    }
    // Force A launches the job; its capture takes A, then pauses.
    let expired = db.core.force_rebuild_index().unwrap_err();
    assert!(
        matches!(expired, IndexForceError::DeadlineExpired),
        "{expired}"
    );
    waiting.recv_timeout(Duration::from_secs(30)).unwrap();
    assert!(!checkpoint_lifecycle::force_parked(&db.core));
    assert!(force_slot_held(&db));
    let busy = db.core.force_rebuild_index().unwrap_err();
    assert!(matches!(busy, IndexForceError::Busy), "{busy}");
    resume.send(()).unwrap();
    wait_until("the worker never freed force A's slot", || {
        !force_slot_held(&db)
    });
    // The slot frees before job A's cleanup ends: reap A so the next start
    // launches a fresh job instead of coalescing into A.
    db.core.progress_checkpoint(true).unwrap();
    // Force B parks behind another frozen capture.
    let resume = pause_frozen_capture(&fixture, &db, 2);
    db.core.test_hooks.lock().force_timeout = None;
    let core = Arc::clone(&db.core);
    let forcing = thread::spawn(move || core.force_rebuild_index());
    wait_until("force B never parked", || {
        checkpoint_lifecycle::force_parked(&db.core)
    });
    assert!(!checkpoint_lifecycle::withdraw_parked_force(
        &db.core,
        &Arc::new(())
    ));
    assert!(checkpoint_lifecycle::force_parked(&db.core));
    resume.send(()).unwrap();
    let report = forcing.join().unwrap().unwrap();
    assert!(report.cut.is_some());
    assert!(!force_slot_held(&db));
    db.shutdown().unwrap();
}
