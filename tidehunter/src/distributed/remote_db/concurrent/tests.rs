//! Real five-node mTLS/Value-WAL regressions. Holding the existing authenticated
//! control stream mutex pauses append actors, not a mock storage implementation.
use super::super::pipeline_tests::Fixture;
use super::*;

fn put(key: u32, value: u8) -> Mutation {
    Mutation::Put {
        keyspace: KeySpace(0),
        key: key.to_be_bytes().to_vec().into(),
        value: vec![value; 32].into(),
    }
}
fn get(db: &ConcurrentRemoteDatabase, key: u32) -> Option<Vec<u8>> {
    db.get(KeySpace(0), &key.to_be_bytes())
        .unwrap()
        .map(|value| value.to_vec())
}
fn wait_event(
    events: &mpsc::Receiver<PipelineStats>,
    predicate: impl Fn(PipelineStats) -> bool,
) -> Result<PipelineStats, mpsc::RecvTimeoutError> {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let stats = events.recv_timeout(deadline.saturating_duration_since(Instant::now()))?;
        if predicate(stats) {
            return Ok(stats);
        }
    }
}

#[test]
fn first_failure_is_utf8_bounded_and_not_overwritten() {
    let slot = std::sync::OnceLock::new();
    let message = "é".repeat(4096);
    record_first_failure(&slot, Some(5), None, "failure", &message);
    record_first_failure(&slot, None, None, "later", &"other");
    let captured = slot.get().unwrap();
    assert!(captured.len() <= 4096);
    assert!(captured.contains("owner=Some(5)"));
    assert!(!captured.contains("other"));
}

#[test]
fn retryable_retirement_preflight_does_not_stop_concurrent_authority() {
    let mut fixture = Fixture::new();
    fixture.config.read_cache_bytes = 0;
    let original = fixture.open();
    original
        .write_batch(fixture.operation(0, 1), vec![put(1, 1)])
        .unwrap();
    // Inject only the pre-mutation busy result; storage and reads remain real TCP.
    original.core.test_hooks.lock().retire_preflight_busy_once = true;
    let db = ConcurrentRemoteDatabase::new(original).unwrap();
    let pending = db.submit_get(KeySpace(0), &1u32.to_be_bytes()).unwrap();
    let error = db
        .retire_history_before(IndexReadCut::BeforeAll)
        .unwrap_err();
    assert!(error.to_string().contains("retirement preflight busy"));
    assert_eq!(
        db.finish_ready_get(pending.wait())
            .unwrap()
            .unwrap()
            .as_ref(),
        &[1; 32]
    );
    db.write_batch(fixture.operation(1, 2), vec![put(2, 2)])
        .unwrap();
    assert_eq!(get(&db, 2), Some(vec![2; 32]));
    db.retire_history_before(IndexReadCut::BeforeAll).unwrap();
    assert_eq!(get(&db, 1), Some(vec![1; 32]));
    db.shutdown().unwrap();
}

#[test]
fn durable_suffix_completes_while_checkpoint_prefix_is_pending() {
    let fixture = Fixture::new();
    let original = fixture.open();
    original
        .write_batch(fixture.operation(0, 1), vec![put(1, 1)])
        .unwrap();
    let (entered, waiting) = mpsc::sync_channel(1);
    let (resume, gate) = mpsc::sync_channel(1);
    original.core.test_hooks.lock().checkpoint_prefix_pause = Some((entered, gate));
    original.core.start_checkpoint().unwrap();
    waiting.recv_timeout(Duration::from_secs(5)).unwrap();
    let db = Arc::new(ConcurrentRemoteDatabase::new(original).unwrap());
    let writer_db = db.clone();
    let operation = fixture.operation(0, 2);
    let (finished, result) = mpsc::sync_channel(1);
    let writer = thread::spawn(move || {
        let _ = finished.send(writer_db.write_batch(operation, vec![put(2, 2)]));
    });
    let before_resume = result.recv_timeout(Duration::from_secs(5));
    let _ = resume.send(());
    writer.join().unwrap();
    let version = before_resume.unwrap().unwrap();
    db.flush_index().unwrap();
    Arc::try_unwrap(db).ok().unwrap().shutdown().unwrap();
    let reopened = ConcurrentRemoteDatabase::new(fixture.open()).unwrap();
    assert_eq!(get(&reopened, 1), Some(vec![1; 32]));
    assert_eq!(get(&reopened, 2), Some(vec![2; 32]));
    assert_eq!(
        reopened.write_batch(operation, vec![put(2, 2)]).unwrap(),
        version
    );
    reopened.shutdown().unwrap();
}

#[test]
fn durable_suffix_completes_while_checkpoint_adoption_is_paused() {
    let fixture = Fixture::new();
    let original = fixture.open();
    original
        .write_batch(fixture.operation(0, 1), vec![put(1, 1)])
        .unwrap();
    original.flush_index().unwrap();
    original
        .write_batch(fixture.operation(1, 2), vec![put(2, 2)])
        .unwrap();
    let (entered, waiting) = mpsc::sync_channel(1);
    let (resume, gate) = mpsc::sync_channel(1);
    original.core.test_hooks.lock().checkpoint_adoption_pause = Some((entered, gate));
    original.core.start_checkpoint().unwrap();
    waiting.recv_timeout(Duration::from_secs(5)).unwrap();
    let db = Arc::new(ConcurrentRemoteDatabase::new(original).unwrap());
    let writer_db = db.clone();
    let operation = fixture.operation(2, 3);
    let (finished, result) = mpsc::sync_channel(1);
    let writer = thread::spawn(move || {
        let _ = finished.send(writer_db.write_batch(operation, vec![put(3, 3)]));
    });
    let before_resume = result.recv_timeout(Duration::from_secs(5));
    resume.send(()).unwrap();
    writer.join().unwrap();
    let version = before_resume.unwrap().unwrap();
    assert_eq!(get(&db, 3), Some(vec![3; 32]));
    db.flush_index().unwrap();
    Arc::try_unwrap(db).ok().unwrap().shutdown().unwrap();
    let reopened = ConcurrentRemoteDatabase::new(fixture.open()).unwrap();
    assert_eq!(get(&reopened, 3), Some(vec![3; 32]));
    assert_eq!(
        reopened.write_batch(operation, vec![put(3, 3)]).unwrap(),
        version
    );
    reopened.shutdown().unwrap();
}

#[test]
fn durable_suffix_completes_while_checkpoint_cleanup_is_paused() {
    let fixture = Fixture::new();
    let original = fixture.open();
    original
        .write_batch(fixture.operation(0, 11), vec![put(11, 1)])
        .unwrap();
    original.flush_index().unwrap();
    original
        .write_batch(fixture.operation(1, 12), vec![put(12, 2)])
        .unwrap();
    let (entered, waiting) = mpsc::sync_channel(1);
    let (resume, gate) = mpsc::sync_channel(1);
    original.core.test_hooks.lock().checkpoint_cleanup_pause = Some((entered, gate));
    original.core.start_checkpoint().unwrap();
    let db = Arc::new(ConcurrentRemoteDatabase::new(original).unwrap());
    // On an early failure, release the paused worker before owner teardown.
    let release_cleanup = resume;
    waiting.recv_timeout(Duration::from_secs(5)).unwrap();
    let writer_db = db.clone();
    let operation = fixture.operation(2, 13);
    let (finished, result) = mpsc::sync_channel(1);
    let writer = thread::spawn(move || {
        let _ = finished.send(writer_db.write_batch(operation, vec![put(13, 3)]));
    });
    let before_resume = result.recv_timeout(Duration::from_secs(5));
    release_cleanup.send(()).unwrap();
    writer.join().unwrap();
    let version = before_resume.unwrap().unwrap();
    assert_eq!(get(&db, 13), Some(vec![3; 32]));
    db.flush_index().unwrap();
    Arc::try_unwrap(db).ok().unwrap().shutdown().unwrap();
    let reopened = ConcurrentRemoteDatabase::new(fixture.open()).unwrap();
    assert_eq!(get(&reopened, 13), Some(vec![3; 32]));
    assert_eq!(
        reopened.write_batch(operation, vec![put(13, 3)]).unwrap(),
        version
    );
    reopened.shutdown().unwrap();
}

#[test]
fn checkpoint_pressure_defers_fifo_head_without_blocking_durable_replies() {
    let fixture = Fixture::new_with_capacity(4);
    let original = fixture.open();
    original
        .write_batch(fixture.operation(0, 1), vec![put(1, 1)])
        .unwrap();
    let (entered, waiting) = mpsc::sync_channel(1);
    let (resume, gate) = mpsc::sync_channel(1);
    original.core.test_hooks.lock().checkpoint_pause = Some((entered, gate));
    original.core.start_checkpoint().unwrap();
    waiting.recv_timeout(Duration::from_secs(5)).unwrap();
    original
        .write_batch(fixture.operation(1, 2), vec![put(2, 2)])
        .unwrap();
    original
        .write_batch(fixture.operation(2, 3), vec![put(3, 3)])
        .unwrap();
    let connection = original.core.clients[3].client.clone();
    let held = connection.lock();
    let db = Arc::new(ConcurrentRemoteDatabase::new(original).unwrap());
    // Unwind releases both blockers before dropping the database that joins them.
    let release_checkpoint = resume;
    let held_connection = held;
    // Writers are joined before the database is unwrapped: a writer drops its
    // Arc clone only after sending its result.
    let writers = std::cell::RefCell::new(Vec::new());
    let write = |operation, mutations| {
        let (sender, receiver) = mpsc::sync_channel(1);
        let db = db.clone();
        writers.borrow_mut().push(thread::spawn(move || {
            let _ = sender.send(db.write_batch(operation, mutations));
        }));
        receiver
    };
    // The fourth identity fills the bound and waits only on its held owner.
    let fourth = write(fixture.operation(3, 4), vec![put(4, 4)]);
    let deadline = Instant::now() + Duration::from_secs(5);
    while db.database.core.next_sequence() != 4 {
        assert!(Instant::now() < deadline, "fourth write did not allocate");
        thread::yield_now();
    }
    // The fifth is the checkpoint-pressured FIFO head; the conflicting retry
    // and the sixth queue behind it in arrival order.
    let fifth_operation = fixture.operation(4, 5);
    let fifth = write(fifth_operation, vec![put(5, 5)]);
    await_waiters(&db.admission, 1);
    let conflict = write(fifth_operation, vec![put(5, 9)]);
    await_waiters(&db.admission, 2);
    let sixth = write(fixture.operation(0, 6), vec![put(6, 6)]);
    await_waiters(&db.admission, 3);
    drop(held_connection);
    let fourth_before_resume = fourth.recv_timeout(Duration::from_secs(5));
    let fifth_before_resume = fifth.try_recv();
    let conflict_before_resume = conflict.try_recv();
    release_checkpoint.send(()).unwrap();
    let fifth_result = fifth.recv_timeout(Duration::from_secs(10));
    let conflict_result = conflict.recv_timeout(Duration::from_secs(10));
    let sixth_result = sixth.recv_timeout(Duration::from_secs(10));
    let fourth_version = fourth_before_resume.unwrap().unwrap();
    assert!(matches!(
        fifth_before_resume,
        Err(mpsc::TryRecvError::Empty)
    ));
    assert!(matches!(
        conflict_before_resume,
        Err(mpsc::TryRecvError::Empty)
    ));
    let fifth_version = fifth_result.unwrap().unwrap();
    assert!(conflict_result.unwrap().is_err());
    let sixth_version = sixth_result.unwrap().unwrap();
    assert_eq!(fifth_version.sequence, fourth_version.sequence + 1);
    assert_eq!(sixth_version.sequence, fifth_version.sequence + 1);
    assert_eq!(get(&db, 5), Some(vec![5; 32]));
    assert_eq!(get(&db, 6), Some(vec![6; 32]));
    let admission = db.admission.clone();
    for writer in writers.take() {
        writer.join().unwrap();
    }
    Arc::try_unwrap(db).ok().unwrap().shutdown().unwrap();
    let charges = admission.charges();
    assert_eq!((charges.requests, charges.bytes), (0, 0));
    // A client clone keeps the live connection registry (and its lock) alive.
    drop(connection);
    let reopened = ConcurrentRemoteDatabase::new(fixture.open()).unwrap();
    assert_eq!(get(&reopened, 5), Some(vec![5; 32]));
    assert_eq!(get(&reopened, 6), Some(vec![6; 32]));
    reopened.shutdown().unwrap();
}

#[test]
fn failed_paused_checkpoint_releases_deferred_credits_without_false_acknowledgement() {
    let fixture = Fixture::new_with_capacity(4);
    let original = fixture.open();
    original
        .write_batch(fixture.operation(0, 11), vec![put(11, 1)])
        .unwrap();
    let (entered, waiting) = mpsc::sync_channel(1);
    let (resume, gate) = mpsc::sync_channel(1);
    {
        let mut hooks = original.core.test_hooks.lock();
        hooks.checkpoint_pause = Some((entered, gate));
        hooks.checkpoint_interruption = Some(0);
    }
    original.core.start_checkpoint().unwrap();
    waiting.recv_timeout(Duration::from_secs(5)).unwrap();
    original
        .write_batch(fixture.operation(1, 12), vec![put(12, 2)])
        .unwrap();
    original
        .write_batch(fixture.operation(2, 13), vec![put(13, 3)])
        .unwrap();
    original
        .write_batch(fixture.operation(3, 14), vec![put(14, 4)])
        .unwrap();
    let db = Arc::new(ConcurrentRemoteDatabase::new(original).unwrap());
    let release_checkpoint = resume;
    // Writers are joined before the database is unwrapped: a writer drops its
    // Arc clone only after sending its result.
    let writers = std::cell::RefCell::new(Vec::new());
    let write = |operation, mutations| {
        let (sender, receiver) = mpsc::sync_channel(1);
        let db = db.clone();
        writers.borrow_mut().push(thread::spawn(move || {
            let _ = sender.send(db.write_batch(operation, mutations));
        }));
        receiver
    };
    // Both callers are deferred by identity (checkpoint) pressure in FIFO order.
    let first = write(fixture.operation(4, 15), vec![put(15, 5)]);
    await_waiters(&db.admission, 1);
    let second = write(fixture.operation(0, 16), vec![put(16, 6)]);
    await_waiters(&db.admission, 2);
    let first_early = first.try_recv();
    release_checkpoint.send(()).unwrap();
    let first_result = first.recv_timeout(Duration::from_secs(10));
    let second_result = second.recv_timeout(Duration::from_secs(10));
    assert!(matches!(first_early, Err(mpsc::TryRecvError::Empty)));
    let first_error = first_result.unwrap().unwrap_err().to_string();
    let second_error = second_result.unwrap().unwrap_err().to_string();
    assert!(first_error.contains("injected interruption after checkpoint intent"));
    assert!(second_error.contains("injected interruption after checkpoint intent"));
    let admission = db.admission.clone();
    for writer in writers.take() {
        writer.join().unwrap();
    }
    assert!(Arc::try_unwrap(db).ok().unwrap().shutdown().is_err());
    let charges = admission.charges();
    assert_eq!((charges.requests, charges.bytes), (0, 0));
    let reopened = ConcurrentRemoteDatabase::new(fixture.open()).unwrap();
    assert_eq!(get(&reopened, 14), Some(vec![4; 32]));
    assert_eq!(get(&reopened, 15), None);
    assert_eq!(get(&reopened, 16), None);
    reopened.shutdown().unwrap();
}
#[test]
fn blocked_appends_allow_committed_reads_and_overlap_distinct_nodes() {
    let fixture = Fixture::new();
    let mut original = fixture.open();
    original
        .write_batch(fixture.operation(4, 0), vec![put(0, 9)])
        .unwrap();
    assert!(
        original
            .get(KeySpace(0), &0u32.to_be_bytes())
            .unwrap()
            .is_some()
    );
    let old_cut = original.published_cut().unwrap();
    let connections: Vec<_> = original
        .core
        .clients
        .iter()
        .take(2)
        .map(|worker| worker.client.clone())
        .collect();
    let held: Vec<_> = connections.iter().map(|client| client.lock()).collect();
    let db = Arc::new(ConcurrentRemoteDatabase::new(original).unwrap());
    let (replies, received) = mpsc::channel();
    let mut writers = Vec::new();
    for node in 0..2 {
        let db = db.clone();
        let replies = replies.clone();
        let operation = fixture.operation(node, u64::from(node) + 10);
        writers.push(thread::spawn(move || {
            replies
                .send(db.write_batch(
                    operation,
                    vec![
                        put(1 + node * 2, 10 + node as u8),
                        put(2 + node * 2, 10 + node as u8),
                    ],
                ))
                .unwrap();
        }));
    }
    // Both blocked writes are READY in their own owner queues; capture the
    // pipeline counters at that instant (the owners are still held).
    let dispatched = {
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            if (0..2).all(|owner| db.database.core.clients[owner].queue.snapshot().2.len() == 1) {
                break Some(db.pipeline_stats());
            }
            if Instant::now() >= deadline {
                break None;
            }
            thread::yield_now();
        }
    };
    let (read_reply, read_result) = mpsc::sync_channel(1);
    let reader_db = db.clone();
    let reader = thread::spawn(move || {
        let cached = reader_db.get(KeySpace(0), &0u32.to_be_bytes());
        let absent = reader_db.get(KeySpace(0), &99u32.to_be_bytes());
        let cut = reader_db.published_cut();
        read_reply.send((cached, absent, cut)).unwrap();
    });
    let completed_read = read_result.recv_timeout(Duration::from_secs(2));
    let early_reply = received.try_recv();
    // Release even when the bounded observation failed, so assertion unwinding
    // cannot strand the real append workers behind a test-owned stream guard.
    drop(held);
    let first = received
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    let second = received
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    for writer in writers {
        writer.join().unwrap();
    }
    reader.join().unwrap();
    let stats = dispatched.unwrap();
    assert_eq!(stats.concurrent_pending_peak, 2);
    // Exactly the baseline worker group exists: both new owners are held, so
    // their READY entries cannot form another group yet.
    assert_eq!(stats.append_requests, 1);
    assert!(matches!(early_reply, Err(mpsc::TryRecvError::Empty)));
    let (cached, absent, cut) = completed_read.unwrap();
    assert_eq!(cached.unwrap().unwrap().as_ref(), &[9; 32]);
    assert!(absent.unwrap().is_none());
    assert_eq!(cut.unwrap(), old_cut);
    assert_ne!(first, second);
    for node in 0..2 {
        let cut = db.published_cut().unwrap();
        let a = db
            .index_reference(KeySpace(0), &(1 + node * 2u32).to_be_bytes(), cut.clone())
            .unwrap()
            .unwrap();
        let b = db
            .index_reference(KeySpace(0), &(2 + node * 2u32).to_be_bytes(), cut)
            .unwrap()
            .unwrap();
        assert_eq!(a.mutation_version().batch, b.mutation_version().batch);
        assert!(
            db.index_reference(
                KeySpace(0),
                &(1 + node * 2u32).to_be_bytes(),
                old_cut.clone()
            )
            .unwrap()
            .is_none()
        );
        assert_eq!(get(&db, 1 + node * 2), Some(vec![10 + node as u8; 32]));
        assert_eq!(get(&db, 2 + node * 2), Some(vec![10 + node as u8; 32]));
    }
    db.flush_index().unwrap();
    Arc::try_unwrap(db).ok().unwrap().shutdown().unwrap();
    // A client clone keeps the live connection registry (and its lock) alive.
    drop(connections);
    let reopened = ConcurrentRemoteDatabase::new(fixture.open()).unwrap();
    assert_eq!(get(&reopened, 0), Some(vec![9; 32]));
    for node in 0..2 {
        assert_eq!(
            get(&reopened, 1 + node * 2),
            Some(vec![10 + node as u8; 32])
        );
        assert_eq!(
            get(&reopened, 2 + node * 2),
            Some(vec![10 + node as u8; 32])
        );
    }
    reopened.shutdown().unwrap();
}

#[test]
fn unknown_append_fences_cached_absent_and_detached_reads_until_reopen() {
    let fixture = Fixture::new();
    let mut original = fixture.open();
    original
        .write_batch(fixture.operation(0, 20), vec![put(0, 7)])
        .unwrap();
    original.get(KeySpace(0), &0u32.to_be_bytes()).unwrap();
    let db = ConcurrentRemoteDatabase::new(original).unwrap();
    let cached = db
        .submit_get(KeySpace(0), &0u32.to_be_bytes())
        .unwrap()
        .wait();
    let cache_inserts_before_unknown = db.read_cache_stats().inserts;
    // The owner-1 frame reaches its WAL file but its completion reports EIO:
    // the outcome is unknown to the authority, as for a lost reply.
    db.database.core.clients[1]
        .client
        .lock()
        .leased_test_fail_write(-libc::EIO);
    let operation = fixture.operation(1, 21);
    let error = db
        .write_batch(operation, vec![put(1, 8), put(2, 8)])
        .unwrap_err();
    assert_eq!(db.read_cache_stats().inserts, cache_inserts_before_unknown);
    let uncertain = error
        .uncertain_version
        .expect("admitted unknown append keeps its version");
    let cause = db
        .database
        .core
        .first_failure
        .get()
        .expect("worker failure is retained");
    assert!(cause.contains("owner=Some(1)"));
    assert!(cause.contains("stage="));
    assert!(db.finish_ready_get(cached).is_err());
    assert!(db.get(KeySpace(0), &0u32.to_be_bytes()).is_err());
    assert!(db.get(KeySpace(0), &99u32.to_be_bytes()).is_err());
    let admission_error = db
        .write_batch(fixture.operation(2, 22), vec![put(3, 9)])
        .unwrap_err();
    assert!(admission_error.to_string().contains(&**cause));
    assert!(admission_error.uncertain_version.is_none());
    assert!(db.shutdown().is_err());
    let reopened = ConcurrentRemoteDatabase::new(fixture.open()).unwrap();
    assert_eq!(get(&reopened, 0), Some(vec![7; 32]));
    assert_eq!(get(&reopened, 1), Some(vec![8; 32]));
    assert_eq!(get(&reopened, 2), Some(vec![8; 32]));
    assert_eq!(
        reopened
            .write_batch(operation, vec![put(1, 8), put(2, 8)])
            .unwrap(),
        uncertain
    );
    reopened.shutdown().unwrap();
}
#[test]
fn coordinator_resolution_samples_include_mixed_and_local_outcomes() {
    let mut fixture = Fixture::new();
    fixture.config.read_cache_bytes = 64 * 1024;
    let original = fixture.open();
    original
        .write_batch(fixture.operation(0, 29), vec![put(0, 5), put(1, 6)])
        .unwrap();
    original.shutdown().unwrap();
    drop(original);
    let original = fixture.open();
    let db = ConcurrentRemoteDatabase::new(original).unwrap();
    // Populate only key 0 before selecting the measurement population.
    assert_eq!(get(&db, 0), Some(vec![5; 32]));
    db.set_read_profile_limit(2);
    let monitor = db.read_profile_monitor();
    let keys = [0u32.to_be_bytes(), 1u32.to_be_bytes(), 99u32.to_be_bytes()];
    let values = db
        .get_many_cut(KeySpace(0), &keys, IndexReadCut::Latest)
        .unwrap();
    assert!(values[0].as_ref().unwrap().is_some());
    assert!(values[1].as_ref().unwrap().is_some());
    assert!(values[2].as_ref().unwrap().is_none());
    assert_eq!(get(&db, 99), None);
    let snapshot = monitor.coordinator_resolution_snapshot();
    assert_eq!(
        (snapshot.selected, snapshot.point, snapshot.bulk),
        (2, 1, 1)
    );
    assert_eq!(snapshot.logical_members, 4);
    assert_eq!(
        (
            snapshot.cache_hits,
            snapshot.remote_misses,
            snapshot.index_misses
        ),
        (1, 1, 2)
    );
    assert_eq!(snapshot.errors, 0);
    assert_eq!(snapshot.cache_lookup.count, 2);
    // The sharded index has no read lock to wait for, so this stage is unsampled.
    assert_eq!(snapshot.index_lock_wait.count, 0);
    assert_eq!(snapshot.index_lookup.count, 4);
    assert_eq!(snapshot.cut_validation.count, 1);
    assert_eq!(snapshot.cut_capture.count, 0);
    assert_eq!(snapshot.route_preparation.count, 1);
    assert_eq!(snapshot.pin_preparation.count, 1);
    assert_eq!(snapshot.batch_preparation.count, 1);
    assert_eq!(get(&db, 99), None);
    assert_eq!(monitor.coordinator_resolution_snapshot().selected, 2);
    db.set_read_profile_limit(0);
    assert_eq!(monitor.coordinator_resolution_snapshot().selected, 2);
    db.set_read_profile_limit(1);
    assert_eq!(monitor.coordinator_resolution_snapshot().selected, 0);
    assert_eq!(get(&db, 99), None);
    let reset = monitor.coordinator_resolution_snapshot();
    assert_eq!(
        (reset.selected, reset.index_misses, reset.cache_lookup.count),
        (1, 1, 0)
    );
    db.shutdown().unwrap();
}

#[test]
fn scalar_and_actor_samples_have_separate_populations_and_credit_wakeups() {
    let mut fixture = Fixture::new();
    fixture.config.read_cache_bytes = 0;
    fixture.config.max_pending_reads = 1;
    fixture.config.read_lanes = Some(1);
    let original = fixture.open();
    original
        .write_batch(fixture.operation(0, 30), vec![put(0, 5), put(1, 6)])
        .unwrap();
    let db = ConcurrentRemoteDatabase::new(original).unwrap();
    assert!(!db.stats().read_resolve_ns_available);
    // A live inline read needs an idle lane with a cached grant: park owner
    // 0's lane worker and install the owner-local grant of key 0's file.
    let reference = db
        .index_reference(KeySpace(0), &0u32.to_be_bytes(), IndexReadCut::Latest)
        .unwrap()
        .unwrap();
    assert_eq!(reference.batch_address.partition, 0);
    let parked = db.reads.bench_park_live_lane(0);
    db.reads
        .bench_install_owner_local(&fixture.nodes[0], reference);
    let rpcs = db.read_rpc_count();
    db.set_read_profile_limit(2);
    let monitor = db.read_profile_monitor();
    assert_eq!(get(&db, 0), Some(vec![5; 32]));
    assert!(db.stats().read_resolve_ns_available);
    // The actor read below needs the lane worker.
    drop(parked);
    let pending = db.submit_get(KeySpace(0), &1u32.to_be_bytes()).unwrap();
    let generation = db.read_admission_generation();
    assert!(RemoteDatabase::read_backpressured(
        &db.submit_get(KeySpace(0), &0u32.to_be_bytes())
            .err()
            .unwrap()
    ));
    assert_eq!(
        db.finish_ready_get(pending.wait())
            .unwrap()
            .unwrap()
            .as_ref(),
        &[6; 32]
    );
    db.wait_read_admission(generation, Some(Instant::now() + Duration::from_secs(1)))
        .unwrap();
    // Repeated credit releases must not let an expired caller retry forever.
    assert!(
        db.wait_read_admission(generation, Some(Instant::now()))
            .is_err()
    );
    let mut counters = HashMap::new();
    monitor.visit(&mut |name, value| {
        counters.insert(name, value);
    });
    assert_eq!(counters["read_profile_selected"], 2);
    assert_eq!(counters["read_profile_completed"], 2);
    assert_eq!(counters["read_profile_inline_completed"], 1);
    assert_eq!(counters["read_profile_actor_completed"], 1);
    assert!(counters["read_profile_inline_ns"] > 0);
    let resolution = monitor.coordinator_resolution_snapshot();
    assert_eq!(resolution.remote_misses, 2);
    assert_eq!(resolution.route_preparation.count, 2);
    assert_eq!(resolution.pin_preparation.count, 2);
    assert_eq!(resolution.batch_preparation.count, 0);
    assert_eq!(db.read_rpc_count() - rpcs, 2);
    db.shutdown().unwrap();
}

/// Native parity: a read that finds the coordinator read budget exhausted
/// waits for credit, however long another read holds it, and completes once
/// the credit is released. Load alone never fails it with a deadline.
#[test]
fn ordinary_read_waits_for_credit_held_longer_than_timeout() {
    let mut fixture = Fixture::new();
    fixture.config.read_cache_bytes = 0;
    fixture.config.max_pending_reads = 1;
    fixture.config.timeout = Duration::from_secs(1);
    let original = fixture.open();
    original
        .write_batch(fixture.operation(0, 50), vec![put(0, 5), put(1, 6)])
        .unwrap();
    let db = ConcurrentRemoteDatabase::new(original).unwrap();
    // The only read credit is held by a read whose value is not consumed.
    let held = db.submit_get(KeySpace(0), &1u32.to_be_bytes()).unwrap();
    assert!(RemoteDatabase::read_backpressured(
        &db.submit_get(KeySpace(0), &0u32.to_be_bytes())
            .err()
            .unwrap()
    ));
    let (sender, receiver) = std::sync::mpsc::channel();
    std::thread::scope(|scope| {
        let waiter = scope.spawn(|| {
            let value = get(&db, 0);
            sender.send(()).unwrap();
            value
        });
        // Hold the credit for three times the configured timeout.
        assert!(
            receiver.recv_timeout(3 * fixture.config.timeout).is_err(),
            "the read must wait for credit, not return"
        );
        assert_eq!(
            db.finish_ready_get(held.wait()).unwrap().unwrap().as_ref(),
            &[6; 32]
        );
        assert_eq!(waiter.join().unwrap(), Some(vec![5; 32]));
    });
    db.shutdown().unwrap();
}

#[test]
fn cancelled_late_read_failure_poison_is_shared_with_writer_and_cache() {
    let mut fixture = Fixture::new();
    fixture.config.read_cache_bytes = 0;
    let original = fixture.open();
    original
        .write_batch(fixture.operation(0, 40), vec![put(0, 3)])
        .unwrap();
    let lane = original.core.reads.lane_client(0, 0);
    let held = lane.lock();
    fixture.fence_owner(0, original.epoch() + 1);
    let db = ConcurrentRemoteDatabase::new(original).unwrap();
    let pending = db.submit_get(KeySpace(0), &0u32.to_be_bytes()).unwrap();
    drop(pending); // cancellation cannot detach a late transport failure
    let observed = db.read_admission_generation();
    drop(held);
    let _ = db.wait_read_admission(observed, Some(Instant::now() + Duration::from_secs(3)));
    assert!(db.validate_cached_read().is_err());
    assert!(db.get(KeySpace(0), &99u32.to_be_bytes()).is_err());
    assert!(
        db.write_batch(fixture.operation(1, 41), vec![put(1, 4)])
            .is_err()
    );
    assert!(db.shutdown().is_err());
}

#[test]
fn sui_shared_gate_publishes_disjoint_later_batch_and_freezes_sparse_view() {
    use crate::distributed::sui::Db;
    let fixture = Fixture::new();
    let path = fixture.temp.path().join("authority");
    let db = Db::open_remote_local_fixture(&path, fixture.shape.clone(), fixture.config.clone())
        .unwrap();
    let ks = db.ks("root");
    let partition = |sequence: u64| {
        let mut operation = [0; 16];
        operation[..8].copy_from_slice(&1u64.to_le_bytes());
        operation[8..].copy_from_slice(&sequence.to_le_bytes());
        fixture
            .config
            .inventory(1)
            .partition_for(BatchId {
                database: fixture.config.storage.database,
                operation,
            })
            .unwrap()
    };
    // Public Sui commits use (authority epoch, monotonic operation counter).
    // Choose consecutive operations routed to distinct real WAL nodes.
    let prefix = (1..64u64)
        .find(|sequence| partition(*sequence) != partition(*sequence + 1))
        .unwrap();
    for _ in 0..prefix {
        db.insert(ks, 0u32.to_be_bytes().to_vec(), vec![9; 32])
            .unwrap();
    }
    db.get(ks, &0u32.to_be_bytes()).unwrap();
    let connection = db
        .remote_for_test()
        .io_clients
        .iter()
        .find(|(node, _)| *node == partition(prefix))
        .unwrap()
        .1
        .clone();
    let held = connection.client.lock();
    let (progress, events) = mpsc::sync_channel(64);
    db.remote_for_test()
        .database
        .core
        .test_hooks
        .lock()
        .concurrent_progress = Some(progress);
    let (first_replies, first_received) = mpsc::channel();
    let (second_replies, second_received) = mpsc::channel();
    let first_db = db.clone();
    let first = thread::spawn(move || {
        let mut batch = first_db.write_batch();
        batch.write(ks, 0u32.to_be_bytes().to_vec(), vec![7; 32]);
        batch.write(ks, 1u32.to_be_bytes().to_vec(), vec![1; 32]);
        batch.write(ks, 2u32.to_be_bytes().to_vec(), vec![1; 32]);
        first_replies.send(batch.commit()).unwrap();
    });
    let first_admitted = wait_event(&events, |stats| stats.concurrent_admitted == prefix + 1);
    let second_db = db.clone();
    let second = thread::spawn(move || {
        let mut batch = second_db.write_batch();
        batch.write(ks, 3u32.to_be_bytes().to_vec(), vec![2; 32]);
        batch.write(ks, 4u32.to_be_bytes().to_vec(), vec![2; 32]);
        second_replies.send(batch.commit()).unwrap();
    });
    let later_durable = wait_event(&events, |stats| stats.concurrent_admitted == prefix + 2);
    let early_ack = second_received.recv_timeout(Duration::from_secs(5));
    let (reads, read_results) = mpsc::sync_channel(1);
    let reader_db = db.clone();
    let reader = thread::spawn(move || {
        let cached = reader_db.get(ks, &0u32.to_be_bytes());
        let absent = reader_db.get(ks, &99u32.to_be_bytes());
        let view = reader_db.checkpoint();
        let rows = reader_db
            .checkpoint()
            .iterator(ks)
            .collect::<Result<Vec<_>, _>>();
        reads.send((cached, absent, view, rows)).unwrap();
    });
    let observed = read_results.recv_timeout(Duration::from_secs(2));
    let early_first = first_received.try_recv();
    let later_values = [3u32, 4].map(|key| db.get(ks, &key.to_be_bytes()));
    drop(held);
    let first_ack = first_received.recv_timeout(Duration::from_secs(5));
    let second_ack = if early_ack.is_err() {
        Some(second_received.recv_timeout(Duration::from_secs(5)))
    } else {
        None
    };
    first.join().unwrap();
    second.join().unwrap();
    reader.join().unwrap();
    first_admitted.unwrap();
    // The later batch on a distinct owner was acknowledged (early_ack below)
    // while the first owner stayed held: owners overlap without a shared stage.
    later_durable.unwrap();
    assert!(matches!(early_first, Err(mpsc::TryRecvError::Empty)));
    early_ack.unwrap().unwrap();
    for value in later_values {
        assert_eq!(value.unwrap().unwrap().as_ref(), &[2; 32]);
    }
    first_ack.unwrap().unwrap();
    if let Some(ack) = second_ack {
        ack.unwrap().unwrap();
    }
    let (cached, absent, view, rows) = observed.unwrap();
    assert_eq!(cached.unwrap().unwrap().as_ref(), &[9; 32]);
    assert!(absent.unwrap().is_none());
    let rows = rows.unwrap();
    assert_eq!(rows.len(), 3);
    for (key, expected) in [(0u32, 9u8), (3, 2), (4, 2)] {
        assert!(
            rows.iter()
                .any(|(k, v)| k.as_ref() == key.to_be_bytes() && v.as_ref() == [expected; 32])
        );
    }
    // Exercise retention while the sparse checkpoint still pins its old value.
    db.flush().unwrap();
    assert_eq!(
        view.iterator(ks).collect::<Result<Vec<_>, _>>().unwrap(),
        rows
    );
    let rows = db
        .checkpoint()
        .iterator(ks)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(rows.len(), 5);
    assert_eq!(
        db.get(ks, &0u32.to_be_bytes()).unwrap().unwrap().as_ref(),
        &[7; 32]
    );
    for key in 1..=4u32 {
        assert_eq!(
            db.get(ks, &key.to_be_bytes()).unwrap().unwrap().as_ref(),
            if key < 3 { &[1; 32] } else { &[2; 32] }
        );
    }
    db.wait_for_background_threads_to_finish().unwrap();
    // The io client clone keeps the live connection registry (and its lock) alive.
    drop(connection);
    let reopened =
        Db::open_remote_local_fixture(&path, fixture.shape.clone(), fixture.config.clone())
            .unwrap();
    assert_eq!(
        reopened
            .iterator(ks)
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .len(),
        5
    );
    reopened.wait_for_background_threads_to_finish().unwrap();
}

#[test]
fn checkpoint_network_wait_does_not_lock_committed_index_or_cache() {
    let fixture = Fixture::new();
    let mut original = fixture.open();
    original
        .write_batch(fixture.operation(0, 50), vec![put(0, 7)])
        .unwrap();
    original.get(KeySpace(0), &0u32.to_be_bytes()).unwrap();
    let connection = original.core.clients[0].client.clone();
    let held = connection.lock();
    let (entered, entering) = mpsc::sync_channel(1);
    original.core.test_hooks.lock().before_checkpoint_io = Some(entered);
    let db = Arc::new(ConcurrentRemoteDatabase::new(original).unwrap());
    let flusher_db = db.clone();
    let flusher = thread::spawn(move || flusher_db.flush_index());
    let checkpoint_entered = entering.recv_timeout(Duration::from_secs(2));
    let (sender, receiver) = mpsc::sync_channel(1);
    let reader_db = db.clone();
    let reader = thread::spawn(move || {
        sender
            .send((
                reader_db.get(KeySpace(0), &0u32.to_be_bytes()),
                reader_db.get(KeySpace(0), &99u32.to_be_bytes()),
                reader_db.index_page(KeySpace(0), None, false, IndexReadCut::Latest),
            ))
            .unwrap();
    });
    let read = receiver.recv_timeout(Duration::from_secs(2));
    drop(held);
    reader.join().unwrap();
    flusher.join().unwrap().unwrap();
    checkpoint_entered.unwrap();
    let (cached, absent, page) = read.unwrap();
    assert_eq!(cached.unwrap().unwrap().as_ref(), &[7; 32]);
    assert!(absent.unwrap().is_none());
    assert_eq!(page.unwrap().len(), 1);
    Arc::try_unwrap(db).ok().unwrap().shutdown().unwrap();
}

#[test]
fn writer_panic_resolves_all_admitted_acknowledgements_and_fences_reads() {
    let fixture = Fixture::new();
    let original = fixture.open();
    let connections: Vec<_> = original
        .core
        .clients
        .iter()
        .take(2)
        .map(|worker| worker.client.clone())
        .collect();
    let held: Vec<_> = connections.iter().map(|client| client.lock()).collect();
    original
        .core
        .test_hooks
        .lock()
        .panic_after_concurrent_admissions = Some(2);
    let db = Arc::new(ConcurrentRemoteDatabase::new(original).unwrap());
    let (sender, receiver) = mpsc::channel();
    let mut writers = Vec::new();
    for node in 0..2 {
        let db = db.clone();
        let sender = sender.clone();
        let operation = fixture.operation(node, 60 + u64::from(node));
        writers.push(thread::spawn(move || {
            sender
                .send(db.write_batch(operation, vec![put(node, 9)]))
                .unwrap();
        }));
    }
    let first = receiver.recv_timeout(Duration::from_secs(2));
    let second = receiver.recv_timeout(Duration::from_secs(2));
    drop(held);
    for writer in writers {
        writer.join().unwrap();
    }
    assert!(first.unwrap().unwrap_err().uncertain_version.is_some());
    assert!(second.unwrap().unwrap_err().uncertain_version.is_some());
    assert!(db.validate_cached_read().is_err());
    assert!(Arc::try_unwrap(db).ok().unwrap().shutdown().is_err());
    // Client clones keep the live connection registry (and its lock) alive.
    drop(connections);
    let reopened = ConcurrentRemoteDatabase::new(fixture.open()).unwrap();
    // Recovery owns any completely appended unknown work; no caller got success.
    reopened.validate_cached_read().unwrap();
    reopened.shutdown().unwrap();
}

#[test]
fn sui_bulk_reads_group_shards_preserve_key_errors_and_cache_cuts() {
    use crate::distributed::sui::Db;
    let mut fixture = Fixture::new();
    fixture.config.read_cache_bytes = 64 * 1024;
    fixture.config.max_pending_reads = 1;
    fixture.config.max_pending_read_bytes = 16 * 1024;
    let original = fixture.open();
    original
        .write_batch(fixture.operation(0, 70), vec![put(0, 1), put(1, 2)])
        .unwrap();
    original
        .write_batch(fixture.operation(1, 71), vec![put(2, 3), put(3, 4)])
        .unwrap();
    original.shutdown().unwrap();
    drop(original);
    let db = Db::open_remote_local_fixture(
        &fixture.temp.path().join("authority"),
        fixture.shape.clone(),
        fixture.config.clone(),
    )
    .unwrap();
    let ks = db.ks("root");
    let cut = db.remote_for_test().published_cut().unwrap();
    let mut keys: Vec<_> = [0u32, 3, 1, 2, 0, 99]
        .into_iter()
        .map(|key| key.to_be_bytes().to_vec())
        .collect();
    keys.push(Vec::new()); // malformed key must not discard other results
    let before = db.remote_for_test().read_rpc_count();
    let values = db.multi_get(ks, &keys);
    for (slot, expected) in [1u8, 4, 2, 3, 1].into_iter().enumerate() {
        assert_eq!(
            values[slot].as_ref().unwrap().as_deref(),
            Some([expected; 32].as_slice())
        );
    }
    assert!(values[5].as_ref().unwrap().is_none());
    assert!(values[6].is_err());
    assert_eq!(db.remote_for_test().read_rpc_count() - before, 2);
    db.validate_cached_read().unwrap();
    let before = db.remote_for_test().read_rpc_count();
    assert!(db.multi_get(ks, &keys)[6].is_err());
    assert_eq!(db.remote_for_test().read_rpc_count(), before);
    db.insert(ks, 0u32.to_be_bytes().to_vec(), vec![9; 32])
        .unwrap();
    let retained = db.remote_for_test().get_many_cut(ks, &keys, cut).unwrap();
    assert_eq!(
        retained[0].as_ref().unwrap().as_deref(),
        Some([1; 32].as_slice())
    );
    let latest = db.multi_get(ks, &keys);
    for slot in [0, 4] {
        assert_eq!(
            latest[slot].as_ref().unwrap().as_deref(),
            Some([9; 32].as_slice())
        );
    }
    assert!(latest[6].is_err());
    db.wait_for_background_threads_to_finish().unwrap();
}

#[test]
fn bulk_read_windows_preserve_mixed_duplicate_results_across_cache_chunks_and_cuts() {
    let mut fixture = Fixture::new();
    fixture.config.read_cache_bytes = 64 * 1024;
    let original = fixture.open();
    original
        .write_batch(fixture.operation(0, 72), vec![put(0, 1), put(1, 2)])
        .unwrap();
    original.shutdown().unwrap();
    drop(original);

    let db = ConcurrentRemoteDatabase::new(fixture.open()).unwrap();
    // Leave key 1 cold while key 0 is resident, then retain the old cut.
    assert_eq!(get(&db, 0), Some(vec![1; 32]));
    let old_cut = db.published_cut().unwrap();
    db.write_batch(
        fixture.operation(1, 73),
        vec![
            put(0, 9),
            Mutation::Delete {
                keyspace: KeySpace(0),
                key: 1u32.to_be_bytes().to_vec().into(),
            },
        ],
    )
    .unwrap();
    let latest = db.published_cut().unwrap();
    let keys: Vec<Vec<u8>> = (0..70)
        .map(|slot| match slot % 4 {
            0 => 0u32.to_be_bytes().to_vec(),
            1 => 1u32.to_be_bytes().to_vec(),
            2 => 99u32.to_be_bytes().to_vec(),
            _ => Vec::new(),
        })
        .collect();
    for (cut, first, second) in [(old_cut, Some(1), Some(2)), (latest, Some(9), None)] {
        let values = db.get_many_cut(KeySpace(0), &keys, cut).unwrap();
        assert_eq!(values.len(), keys.len());
        for (slot, value) in values.iter().enumerate() {
            match slot % 4 {
                0 | 1 => {
                    let expected = if slot % 4 == 0 { first } else { second };
                    let expected = expected.map(|byte| [byte; 32]);
                    assert_eq!(
                        value.as_ref().unwrap().as_deref(),
                        expected.as_ref().map(<[u8; 32]>::as_slice)
                    );
                }
                2 => assert!(value.as_ref().unwrap().is_none()),
                _ => assert!(value.is_err()),
            }
        }
    }
    db.shutdown().unwrap();
}

#[test]
fn competing_grouped_readers_share_one_credit_without_losing_members() {
    let mut fixture = Fixture::new();
    fixture.config.read_cache_bytes = 0;
    fixture.config.max_pending_reads = 1;
    fixture.config.max_pending_read_bytes = 16 * 1024;
    let original = fixture.open();
    original
        .write_batch(fixture.operation(0, 80), vec![put(0, 1), put(1, 2)])
        .unwrap();
    original
        .write_batch(fixture.operation(1, 81), vec![put(2, 3), put(3, 4)])
        .unwrap();
    let db = ConcurrentRemoteDatabase::new(original).unwrap();
    let cut = db.published_cut().unwrap();
    let keys: Vec<_> = [0u32, 3, 1, 2, 0, 99]
        .into_iter()
        .map(|key| key.to_be_bytes().to_vec())
        .collect();
    let ready = std::sync::Barrier::new(2);
    thread::scope(|scope| {
        for _ in 0..2 {
            let (db, keys, ready, cut) = (&db, &keys, &ready, cut.clone());
            scope.spawn(move || {
                ready.wait();
                for _ in 0..8 {
                    let values = db.get_many_cut(KeySpace(0), keys, cut.clone()).unwrap();
                    assert_eq!(values.len(), keys.len());
                    for (slot, expected) in [1u8, 4, 2, 3, 1].into_iter().enumerate() {
                        assert_eq!(
                            values[slot].as_ref().unwrap().as_deref(),
                            Some([expected; 32].as_slice())
                        );
                    }
                    assert!(values[5].as_ref().unwrap().is_none());
                }
            });
        }
    });
    db.shutdown().unwrap();
}

#[test]
fn acknowledged_writes_prime_bounded_cache_for_immutable_cuts() {
    let mut fixture = Fixture::new();
    fixture.config.read_cache_bytes = 64 * 1024;
    let db = ConcurrentRemoteDatabase::new(fixture.open()).unwrap();

    db.write_batch(fixture.operation(0, 90), vec![put(7, 1)])
        .unwrap();
    let old_cut = db.published_cut().unwrap();
    let old_reference = db
        .index_reference(KeySpace(0), &7u32.to_be_bytes(), old_cut.clone())
        .unwrap()
        .unwrap();
    let rpc_count = db.read_rpc_count();
    assert_eq!(
        db.get_many_cut(KeySpace(0), &[7u32.to_be_bytes().to_vec()], old_cut.clone())
            .unwrap()[0]
            .as_ref()
            .unwrap()
            .as_deref(),
        Some([1; 32].as_slice())
    );
    assert_eq!(db.read_rpc_count(), rpc_count);

    db.write_batch(fixture.operation(1, 91), vec![put(7, 2)])
        .unwrap();
    let updated_cut = db.published_cut().unwrap();
    let updated_reference = db
        .index_reference(KeySpace(0), &7u32.to_be_bytes(), updated_cut.clone())
        .unwrap()
        .unwrap();
    assert_ne!(old_reference, updated_reference);
    db.write_batch(
        fixture.operation(2, 92),
        vec![Mutation::Delete {
            keyspace: KeySpace(0),
            key: 7u32.to_be_bytes().to_vec().into(),
        }],
    )
    .unwrap();
    let deleted_cut = db.published_cut().unwrap();
    let keys = [7u32.to_be_bytes().to_vec()];
    let before_reads = db.read_rpc_count();
    let old = db.get_many_cut(KeySpace(0), &keys, old_cut).unwrap();
    let updated = db.get_many_cut(KeySpace(0), &keys, updated_cut).unwrap();
    let deleted = db.get_many_cut(KeySpace(0), &keys, deleted_cut).unwrap();
    assert_eq!(
        old[0].as_ref().unwrap().as_deref(),
        Some([1; 32].as_slice())
    );
    assert_eq!(
        updated[0].as_ref().unwrap().as_deref(),
        Some([2; 32].as_slice())
    );
    assert!(deleted[0].as_ref().unwrap().is_none());
    assert_eq!(db.read_rpc_count(), before_reads);
    let cache = db.read_cache_stats();
    assert!(cache.inserts >= 3);
    assert!(cache.retained_bytes <= cache.budget_bytes);
    let mut metrics = None;
    db.visit_write_metrics(|snapshot| metrics = Some(snapshot));
    assert_eq!(
        metrics
            .unwrap()
            .authority
            .durable_receipt_publication_blocked_batches,
        0
    );
    db.shutdown().unwrap();
}

#[test]
fn write_profile_arming_resets_checkpoint_phase_metrics() {
    let mut fixture = Fixture::new();
    fixture.config.write_profile_limit = 0;
    let original = fixture.open();
    let warm_operation = fixture.operation(0, 201);
    let warm_version = original
        .write_batch(warm_operation, vec![put(201, 1)])
        .unwrap();
    let (entered, waiting) = mpsc::sync_channel(1);
    let (resume, gate) = mpsc::sync_channel(1);
    original.core.test_hooks.lock().checkpoint_adoption_pause = Some((entered, gate));
    original.core.start_checkpoint().unwrap();
    waiting.recv_timeout(Duration::from_secs(5)).unwrap();
    let db = Arc::new(ConcurrentRemoteDatabase::new(original).unwrap());
    let arm_db = db.clone();
    let (finished, result) = mpsc::sync_channel(1);
    let arm = thread::spawn(move || {
        let _ = finished.send(arm_db.arm_write_profile(1));
    });
    let early = result.recv_timeout(Duration::from_millis(100));
    resume.send(()).unwrap();
    arm.join().unwrap();
    assert!(matches!(early, Err(mpsc::RecvTimeoutError::Timeout)));
    result
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    let summary = || {
        let mut snapshot = None;
        db.visit_write_metrics(|metrics| snapshot = Some(metrics.checkpoint_diagnostics));
        snapshot.unwrap()
    };
    let fresh = summary();
    assert!(fresh.available);
    assert_eq!(fresh.records, 0);
    assert_eq!(fresh.root_adoption_count, 0);
    let operation = fixture.operation(1, 202);
    let version = db.write_batch(operation, vec![put(202, 2)]).unwrap();
    db.flush_index().unwrap();
    let measured = summary();
    assert!(measured.root_adoption_count > 0);
    assert!(measured.owner_adoption_count > 0);
    assert!(measured.obsolete_cleanup_count > 0);
    db.arm_write_profile(0).unwrap();
    let disabled = summary();
    assert!(!disabled.available);
    assert_eq!(disabled.records, 0);
    assert_eq!(disabled.root_adoption_count, 0);
    db.arm_write_profile(1).unwrap();
    let rearmed = summary();
    assert!(rearmed.available);
    assert_eq!(rearmed.records, 0);
    assert_eq!(rearmed.root_adoption_count, 0);
    assert_eq!(rearmed.owner_adoption_count, 0);
    assert_eq!(rearmed.obsolete_cleanup_count, 0);
    assert_eq!(get(&db, 201), Some(vec![1; 32]));
    assert_eq!(get(&db, 202), Some(vec![2; 32]));
    assert_eq!(
        db.write_batch(warm_operation, vec![put(201, 1)]).unwrap(),
        warm_version
    );
    assert_eq!(
        db.write_batch(operation, vec![put(202, 2)]).unwrap(),
        version
    );
    db.write_batch(fixture.operation(2, 203), vec![put(203, 3)])
        .unwrap();
    db.flush_index().unwrap();
    let next = summary();
    assert!(next.root_adoption_count > 0);
    assert!(next.owner_adoption_count > 0);
    assert!(next.obsolete_cleanup_count > 0);
    Arc::try_unwrap(db).ok().unwrap().shutdown().unwrap();
}

#[test]
fn write_profile_is_sampled_only_on_owning_append_connections() {
    let mut fixture = Fixture::new();
    fixture.config.write_profile_limit = 0;
    let db = ConcurrentRemoteDatabase::new(fixture.open()).unwrap();
    db.write_batch(fixture.operation(1, 92), vec![put(8, 3)])
        .unwrap();
    db.arm_write_profile(1).unwrap();
    db.write_batch(fixture.operation(1, 93), vec![put(9, 4)])
        .unwrap();
    db.finish_write_profile().unwrap();
    db.arm_write_profile(1).unwrap();
    let mut before = None;
    db.visit_write_metrics(|metrics| before = Some(metrics));
    let before = before.unwrap();
    let leased_frames = || {
        db.live_metrics
            .as_ref()
            .unwrap()
            .snapshot()
            .counts
            .leased_frames
    };
    let frames_before = leased_frames();
    db.write_batch(fixture.operation(1, 94), vec![put(10, 5)])
        .unwrap();
    let mut after = None;
    db.visit_write_metrics(|metrics| after = Some(metrics));
    let after = after.unwrap();
    assert_eq!(
        after.pipeline.append_requests - before.pipeline.append_requests,
        1
    );
    assert_eq!(
        after.pipeline.concurrent_admitted - before.pipeline.concurrent_admitted,
        1
    );
    // The sampled write is one coordinator-written leased frame.
    assert_eq!(leased_frames() - frames_before, 1);
    db.finish_write_profile().unwrap();
    let mut snapshot = None;
    db.visit_write_metrics(|metrics| snapshot = Some(metrics));
    let profile = snapshot.unwrap().write_profile;
    assert_eq!(profile.limit_per_connection, 1);
    assert_eq!(profile.expected_connections, 5);
    assert_eq!(profile.sampled_requests, 1);
    assert_eq!(profile.captured_connections, profile.expected_connections);
    assert_eq!(profile.profile_activation_errors, 0);
    assert_eq!(profile.profile_capture_errors, 0);
    // Owners only adopt leased frames: their append path records no call.
    assert_eq!(profile.profile.storage_write.append_calls, 0);
    db.shutdown().unwrap();
}

#[test]
fn wait_metrics_reconcile_concurrent_tcp_puts_beyond_timeline_limit() {
    let fixture = Fixture::new().synchronous();
    let db = ConcurrentRemoteDatabase::new(fixture.open()).unwrap();
    db.arm_write_timeline(1, 1).unwrap();
    let gate = std::sync::Barrier::new(4);
    std::thread::scope(|scope| {
        for worker in 0..4 {
            let db = &db;
            let fixture = &fixture;
            let gate = &gate;
            scope.spawn(move || {
                gate.wait();
                for offset in 0..8 {
                    let key = 10_000 + worker * 8 + offset;
                    db.write_batch(fixture.operation(worker, u64::from(key)), vec![put(key, 7)])
                        .unwrap();
                }
            });
        }
    });
    let export = db
        .finish_write_timeline_until(Instant::now() + Duration::from_secs(5))
        .unwrap();
    let metrics = &export.write_wait_metrics;
    assert!(metrics.enabled);
    assert!(metrics.complete);
    assert!(!metrics.saturated);
    assert!(export.records.is_empty());
    for (index, row) in metrics.windows.iter().enumerate() {
        assert_eq!(row.observations, 32);
        assert!(row.depth_sum >= row.observations);
        assert!((1..=4).contains(&row.depth_max));
        assert!(row.wake_coalesced <= row.wake_attempts);
        match index {
            0 => assert_eq!(row.wake_attempts, 32),
            1 => assert_eq!(row.wake_attempts, 0),
            2 => assert!((1..=32).contains(&row.wake_attempts)),
            _ => unreachable!(),
        }
    }
    let json = serde_json::to_value(&export).unwrap();
    assert_eq!(json["write_wait_metrics"]["windows"][0]["observations"], 32);
    for key in 10_000..10_032 {
        assert_eq!(get(&db, key), Some(vec![7; 32]));
    }
    db.shutdown().unwrap();
}

#[test]
fn timeline_preserves_validation_error_without_inventing_batch() {
    let fixture = Fixture::new();
    let db = ConcurrentRemoteDatabase::new(fixture.open()).unwrap();
    let trace = &db.write_timeline;
    db.arm_write_timeline(1, 1).unwrap();
    let operation = fixture.operation(1, 903);
    let sample = trace.begin(19).unwrap();
    trace.operation(sample, operation);
    let result = db.write_batch_traced(operation, Vec::new(), Some(sample));
    assert!(result.is_err());
    trace.complete(sample, result.err().map(|e| e.to_string()));
    let export = db.finish_write_timeline().unwrap();
    assert!(export.records[0].error.is_some());
    assert!(export.records[0].batch.is_none());
    db.shutdown().unwrap();
}

#[test]
fn sui_timeline_finish_deadline_does_not_wait_for_shared_gate() {
    use crate::distributed::sui::Db;
    let fixture = Fixture::new();
    let db = Db::open_remote_local_fixture(
        &fixture.temp.path().join("authority"),
        fixture.shape.clone(),
        fixture.config.clone(),
    )
    .unwrap();
    db.arm_write_timeline(1, 1).unwrap();
    let guard = db.shared_write_gate_for_test();
    assert!(
        db.finish_write_timeline_until(Instant::now() + Duration::from_millis(20))
            .is_err()
    );
    drop(guard);
    db.arm_write_timeline(1, 1).unwrap();
    db.finish_write_timeline_until(Instant::now() + Duration::from_secs(5))
        .unwrap();
    db.wait_for_background_threads_to_finish().unwrap();
}

fn scalar_window_fixture() -> Fixture {
    Fixture::new_with_config(
        128,
        KeyShape::new_single(4, 1, crate::key_shape::KeyType::uniform(1)),
        16,
        |config| {
            config.storage.batch_limits.max_encoded_bytes = 128 * 1024;
            config.storage.group_limits.max_encoded_bytes = 2 * 1024 * 1024 - 8;
            config.storage.fragment_bytes = 2 * 1024 * 1024;
            config.storage.wal_file_bytes = 4 * 1024 * 1024;
            config.max_pending_bytes = 16 * 1024 * 1024;
        },
    )
}

fn large_put(key: u32) -> Mutation {
    Mutation::Put {
        keyspace: KeySpace(0),
        key: key.to_be_bytes().to_vec().into(),
        value: vec![key as u8; 64 * 1024].into(),
    }
}

#[test]
fn sui_timing_visitors_export_authority_split_counters() {
    use crate::distributed::sui::Db;
    let fixture = Fixture::new();
    let db = Db::open_remote_local_fixture(
        &fixture.temp.path().join("authority"),
        fixture.shape.clone(),
        fixture.config.clone(),
    )
    .unwrap();
    for key in [1u32, 2] {
        let mut batch = db.write_batch();
        batch.write(db.ks("root"), key.to_be_bytes().to_vec(), vec![3u8; 32]);
        batch.commit_traced(u64::from(key)).unwrap();
    }
    let mut counters = HashMap::new();
    db.visit_timing_metrics(|name, value| {
        counters.insert(name, value);
    });
    assert_eq!(counters["write_authority_windows"], 2);
    assert_eq!(counters["write_authority_batches"], 2);
    assert!(counters["write_authority_prepare_ns"] > 0);
    assert!(counters["write_authority_admit_ns"] > 0);
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        counters.clear();
        if db
            .try_visit_timing_metrics(|name, value| {
                counters.insert(name, value);
            })
            .is_some()
        {
            break;
        }
        assert!(Instant::now() < deadline);
        thread::yield_now();
    }
    assert_eq!(counters["write_authority_windows"], 2);
    assert_eq!(counters["write_authority_batches"], 2);
    assert!(counters["write_authority_prepare_ns"] > 0);
    assert!(counters["write_authority_admit_ns"] > 0);
    db.wait_for_background_threads_to_finish().unwrap();
}

#[test]
fn authority_split_counters_cover_single_and_multiple_batch_windows() {
    for batches in [1, 4] {
        let fixture = scalar_window_fixture();
        let original = fixture.open();
        let window: Vec<_> = (0..batches)
            .map(|index| {
                (
                    fixture.operation(0, u64::from(index)),
                    vec![large_put(index)],
                )
            })
            .collect();
        original.submit_many(&window).unwrap();
        let db = ConcurrentRemoteDatabase::new(original).unwrap();
        db.flush_index().unwrap();
        db.visit_write_metrics(|snapshot| {
            assert_eq!(snapshot.authority.authority_windows, 1);
            assert_eq!(snapshot.authority.authority_batches, u64::from(batches));
            assert!(snapshot.authority.authority_prepare_ns > 0);
            assert!(snapshot.authority.authority_admit_ns > 0);
        });
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if db
                .try_visit_write_metrics(|snapshot| {
                    assert_eq!(snapshot.authority.authority_windows, 1);
                    assert_eq!(snapshot.authority.authority_batches, u64::from(batches));
                    assert!(snapshot.authority.authority_prepare_ns > 0);
                    assert!(snapshot.authority.authority_admit_ns > 0);
                })
                .is_some()
            {
                break;
            }
            assert!(Instant::now() < deadline);
            thread::yield_now();
        }
        db.shutdown().unwrap();
    }
}

#[test]
fn submit_many_window_keeps_admission_order() {
    let fixture = scalar_window_fixture();
    let original = fixture.open();
    let window: Vec<_> = (0..16)
        .map(|index| {
            (
                fixture.operation(0, u64::from(index)),
                vec![large_put(index)],
            )
        })
        .collect();
    let versions = original.submit_many(&window).unwrap();
    let db = ConcurrentRemoteDatabase::new(original).unwrap();
    for (index, version) in versions.iter().enumerate() {
        assert_eq!(version.epoch, versions[0].epoch);
        assert_eq!(version.sequence, versions[0].sequence + index as u64);
        assert_eq!(get(&db, index as u32), Some(vec![index as u8; 64 * 1024]));
    }
    db.flush_index().unwrap();
    let stats = db.pipeline_stats();
    assert_eq!(stats.concurrent_admitted, 16);
    assert_eq!(stats.prepare_calls, 1);
    assert!(stats.prepare_task_ns > 0);
    db.shutdown().unwrap();
}

#[test]
fn failed_later_segment_reports_uncertain_prefix_and_whole_window_retries() {
    let mut fixture = Fixture::new_with_capacity(4);
    fixture.config.max_admitted_batches = 64;
    fixture.config.max_pending_batches = 64;
    let mut db = fixture.open();
    // Every identity maps to one partition with a four-batch fresh directory,
    // so the window splits into segments 0..4 and 4..8.
    let writes: Vec<_> = (0..8u32)
        .map(|key| {
            (
                fixture.operation(0, 2000 + u64::from(key)),
                vec![put(key, key as u8 + 1)],
            )
        })
        .collect();
    let next = db.core.next_sequence();
    // Encoding of the second segment fails after its allocation (one-shot).
    db.core.test_hooks.lock().fail_prepared_operation = Some(writes[5].0);
    let error = db.submit_many(&writes).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("injected caller encoding failure")
    );
    // The first segment was admitted: the error is an uncertain prefix.
    assert_eq!(
        error.uncertain_version.map(|version| version.sequence),
        Some(next)
    );
    db.core.ensure_healthy().unwrap();
    db.drain().unwrap();
    // Retrying the whole window returns the admitted prefix's versions and
    // allocates fresh versions after the aborted second segment's gap.
    let versions = db.submit_many(&writes).unwrap();
    for (index, version) in versions.iter().enumerate() {
        let gap = if index < 4 { 0 } else { 4 };
        assert_eq!(version.sequence, next + index as u64 + gap);
    }
    db.drain().unwrap();
    for key in 0..8u32 {
        assert_eq!(
            db.get(KeySpace(0), &key.to_be_bytes())
                .unwrap()
                .unwrap()
                .as_ref(),
            &[key as u8 + 1; 32]
        );
    }
    db.core.wait_settled().unwrap();
    assert_eq!(db.core.admission.charges().partition_batches, 0);
    db.shutdown().unwrap();
}

#[test]
fn caller_middle_failure_allocates_nothing_or_leaves_only_an_aborted_slot() {
    for injected in [false, true] {
        let fixture = scalar_window_fixture();
        let original = fixture.open();
        let failed = fixture.operation(0, 2);
        if injected {
            original.core.test_hooks.lock().fail_prepared_operation = Some(failed);
        }
        let db = ConcurrentRemoteDatabase::new(original).unwrap();
        let first = db
            .write_batch(fixture.operation(0, 1), vec![large_put(1)])
            .unwrap();
        let bad = if injected {
            large_put(2)
        } else {
            Mutation::Put {
                keyspace: KeySpace(0),
                key: vec![0; 3].into(),
                value: vec![2; 64 * 1024].into(),
            }
        };
        assert!(db.write_batch(failed, vec![bad]).is_err());
        let last = db
            .write_batch(fixture.operation(0, 3), vec![large_put(3)])
            .unwrap();
        // Validation rejects before allocation: no version hole. An encoding
        // failure after allocation aborts exactly its own generation (D6),
        // which the published floor passes; sealed recovery tolerates the gap.
        assert_eq!(last.epoch, first.epoch);
        assert_eq!(last.sequence, first.sequence + 1 + u64::from(injected));
        assert_eq!(get(&db, 1), Some(vec![1; 64 * 1024]));
        assert_eq!(get(&db, 3), Some(vec![3; 64 * 1024]));
        if injected {
            // The injected failure is one-shot: the same identity retries cleanly.
            let retried = db.write_batch(failed, vec![large_put(2)]).unwrap();
            assert_eq!(retried.sequence, last.sequence + 1);
            assert_eq!(get(&db, 2), Some(vec![2; 64 * 1024]));
        }
        db.ensure_healthy().unwrap();
        db.shutdown().unwrap();
    }
}

#[test]
fn duplicate_identity_admits_once() {
    let fixture = scalar_window_fixture();
    let original = fixture.open();
    let operation = fixture.operation(0, 1);
    let original_version = original.write_batch(operation, vec![large_put(1)]).unwrap();
    let db = ConcurrentRemoteDatabase::new(original).unwrap();
    let admitted = db.stats().admitted_batches;
    assert_eq!(
        db.write_batch(operation, vec![large_put(1)]).unwrap(),
        original_version
    );
    let new_operation = fixture.operation(0, 2);
    let version = db.write_batch(new_operation, vec![large_put(2)]).unwrap();
    assert_eq!(
        db.write_batch(new_operation, vec![large_put(2)]).unwrap(),
        version
    );
    assert!(db.write_batch(new_operation, vec![large_put(9)]).is_err());
    let last = db
        .write_batch(fixture.operation(0, 3), vec![large_put(3)])
        .unwrap();
    assert_eq!(version.sequence, original_version.sequence + 1);
    assert_eq!(last.sequence, version.sequence + 1);
    assert_eq!(get(&db, 2), Some(vec![2; 64 * 1024]));
    db.flush_index().unwrap();
    // Retries are acknowledged, but only the two new identities allocated.
    assert_eq!(db.stats().admitted_batches, admitted + 2);
    db.shutdown().unwrap();
}

fn fifo_admission(requests: usize, bytes: usize) -> Arc<Admission> {
    let shape = KeyShape::new_single(4, 1, crate::key_shape::KeyType::uniform(1));
    Arc::new(Admission {
        budget: Mutex::new(WriteBudget::default()),
        drained: parking_lot::Condvar::new(),
        max_requests: requests,
        max_bytes: bytes,
        max_identities: requests,
        max_index_bytes: 1024 * 1024,
        partition_batches: 1024,
        reference_bytes: 1024 * 1024,
        index: Arc::new(NativeIndex::new(&shape, [71; 16], 1024 * 1024).unwrap()),
        timeout: Duration::from_secs(30),
        first_failure: Arc::new(std::sync::OnceLock::new()),
    })
}

fn await_waiters(admission: &Admission, expected: usize) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while admission.budget.lock().waiters.len() != expected {
        assert!(Instant::now() < deadline, "admission waiters did not queue");
        thread::yield_now();
    }
}

#[test]
fn publication_release_validates_all_dimensions_before_returning_any_credit() {
    let admission = fifo_admission(4, 4096);
    {
        let mut budget = admission.budget.lock();
        budget.index_bytes = 1024;
        budget.slots = 3;
        budget.checkpoint.partition_batches = vec![2, 1];
        budget.checkpoint.partition_records = vec![5, 4];
    }
    for (index, slots, partitions) in [
        (1025, 1, vec![(1, 2), (0, 0)]),
        (512, 4, vec![(1, 2), (0, 0)]),
        (512, 1, vec![(1, 2)]),
        (512, 1, vec![(3, 2), (0, 0)]),
        (512, 1, vec![(1, 2), (1, 5)]),
    ] {
        assert!(
            admission
                .release_publication(index, slots, &partitions, (0, 0))
                .is_err()
        );
        let budget = admission.budget.lock();
        assert_eq!((budget.index_bytes, budget.slots), (1024, 3));
        assert_eq!(budget.checkpoint.partition_batches, [2, 1]);
        assert_eq!(budget.checkpoint.partition_records, [5, 4]);
    }
    admission
        .release_publication(768, 2, &[(2, 5), (1, 4)], (0, 0))
        .unwrap();
    {
        let budget = admission.budget.lock();
        assert_eq!((budget.index_bytes, budget.slots), (256, 1));
        assert_eq!(budget.checkpoint.partition_batches, [0, 0]);
        assert_eq!(budget.checkpoint.partition_records, [0, 0]);
    }
    admission
        .release_publication(256, 1, &[(0, 0), (0, 0)], (0, 0))
        .unwrap();
    let budget = admission.budget.lock();
    assert_eq!((budget.index_bytes, budget.slots), (0, 0));
}

#[test]
fn fifo_admission_heterogeneous_head_blocks_smaller_reservations() {
    let admission = fifo_admission(4, 10);
    let mut budget = admission.budget.lock();
    Admission::charge(&mut budget, 8);
    let first = admission.enqueue(&mut budget, 5);
    let second = admission.enqueue(&mut budget, 2);
    let third = admission.enqueue(&mut budget, 1);
    assert!(admission.grant_waiters(&mut budget).is_empty());
    assert_eq!(budget.bytes, 8);
    assert_eq!(budget.waiters.len(), 3);
    // Make the existing credit available, then inspect actual grant order. The
    // caller guards below cancel the unclaimed grants and return their credits.
    budget.requests -= 1;
    budget.bytes -= 8;
    let granted = admission.grant_waiters(&mut budget);
    assert_eq!(granted.len(), 3);
    assert!(Arc::ptr_eq(&granted[0], &first.waiter));
    assert!(Arc::ptr_eq(&granted[1], &second.waiter));
    assert!(Arc::ptr_eq(&granted[2], &third.waiter));
    assert_eq!((budget.requests, budget.bytes), (3, 8));
    drop(budget);
    wake_granted(granted);
    drop((first, second, third));
    let budget = admission.budget.lock();
    assert_eq!(
        (budget.requests, budget.bytes, budget.live_waiters),
        (0, 0, 0)
    );
}

#[test]
fn fifo_admission_cancelled_head_does_not_strand_eligible_waiter() {
    let admission = fifo_admission(4, 10);
    let credit = admission.reserve(8, || Ok(())).unwrap();
    let mut budget = admission.budget.lock();
    let first = admission.enqueue(&mut budget, 5);
    let second = admission.enqueue(&mut budget, 2);
    drop(budget);
    drop(first);
    assert_eq!(second.waiter.state.load(Ordering::Acquire), WAITER_GRANTED);
    assert_eq!(admission.budget.lock().bytes, 10);
    drop(second);
    drop(credit);
    assert_eq!(admission.budget.lock().bytes, 0);
}

#[test]
fn fifo_admission_cancelled_grant_hands_exact_credit_to_successor() {
    let admission = fifo_admission(1, 8);
    let credit = admission.reserve(8, || Ok(())).unwrap();
    let mut budget = admission.budget.lock();
    let first = admission.enqueue(&mut budget, 7);
    let second = admission.enqueue(&mut budget, 3);
    drop(budget);
    drop(credit);
    assert_eq!(first.waiter.state.load(Ordering::Acquire), WAITER_GRANTED);
    assert_eq!(second.waiter.state.load(Ordering::Acquire), WAITER_QUEUED);
    // Cancellation between grant and claim (including unwinding) must transfer
    // the reserved bytes, not merely remove the caller's queue node.
    drop(first);
    assert_eq!(second.waiter.state.load(Ordering::Acquire), WAITER_GRANTED);
    assert_eq!(admission.budget.lock().bytes, 3);
    drop(second);
    let budget = admission.budget.lock();
    assert_eq!(
        (budget.requests, budget.bytes, budget.live_waiters),
        (0, 0, 0)
    );
}

#[test]
fn fifo_admission_close_wakes_every_blocked_caller() {
    let admission = fifo_admission(1, 8);
    let credit = admission.reserve(8, || Ok(())).unwrap();
    let (finished, received) = mpsc::sync_channel(8);
    let threads: Vec<_> = (0..8)
        .map(|_| {
            let admission = admission.clone();
            let finished = finished.clone();
            thread::spawn(move || {
                finished
                    .send(admission.reserve(1, || Ok(())).is_err())
                    .unwrap();
            })
        })
        .collect();
    await_waiters(&admission, 8);
    admission.close();
    for _ in 0..8 {
        assert!(received.recv_timeout(Duration::from_secs(5)).unwrap());
    }
    for thread in threads {
        thread.join().unwrap();
    }
    drop(credit);
    let budget = admission.budget.lock();
    assert!(budget.waiters.is_empty());
    assert_eq!(
        (budget.requests, budget.bytes, budget.live_waiters),
        (0, 0, 0)
    );
}

#[test]
fn fifo_admission_concurrent_reserve_release_has_no_lost_wakeup() {
    let admission = fifo_admission(2, 7);
    let start = Arc::new(std::sync::Barrier::new(9));
    let (finished, received) = mpsc::sync_channel(8);
    let threads: Vec<_> = (0..8)
        .map(|writer| {
            let admission = admission.clone();
            let start = start.clone();
            let finished = finished.clone();
            thread::spawn(move || {
                start.wait();
                for index in 0..256 {
                    let bytes = (writer + index) % 7 + 1;
                    let credit = admission.reserve(bytes, || Ok(())).unwrap();
                    {
                        let budget = admission.budget.lock();
                        assert!(budget.requests <= 2 && budget.bytes <= 7);
                        assert!(budget.waiters.len() <= budget.live_waiters);
                    }
                    thread::yield_now();
                    drop(credit);
                }
                finished.send(()).unwrap();
            })
        })
        .collect();
    start.wait();
    for _ in 0..8 {
        if received.recv_timeout(Duration::from_secs(10)).is_err() {
            admission.close();
            panic!("bounded admission stress lost a wakeup");
        }
    }
    for thread in threads {
        thread.join().unwrap();
    }
    let budget = admission.budget.lock();
    assert!(budget.waiters.is_empty());
    assert_eq!(
        (budget.requests, budget.bytes, budget.live_waiters),
        (0, 0, 0)
    );
}

#[test]
fn fifo_admission_serves_blocked_callers_in_order() {
    let admission = fifo_admission(1, 8);
    let credit = admission.reserve(8, || Ok(())).unwrap();
    let (finished, received) = mpsc::sync_channel(3);
    let mut releases = Vec::new();
    let mut threads = Vec::new();
    for (index, bytes) in [7, 3, 1].into_iter().enumerate() {
        let thread_admission = admission.clone();
        let finished = finished.clone();
        let (release, released) = mpsc::sync_channel(1);
        releases.push(release);
        threads.push(thread::spawn(move || {
            let credit = thread_admission.reserve(bytes, || Ok(())).unwrap();
            finished.send(index).unwrap();
            released.recv_timeout(Duration::from_secs(5)).unwrap();
            drop(credit);
        }));
        await_waiters(&admission, index + 1);
    }
    drop(credit);
    for (index, release) in releases.into_iter().enumerate() {
        assert_eq!(
            received.recv_timeout(Duration::from_secs(5)).unwrap(),
            index
        );
        release.send(()).unwrap();
    }
    for thread in threads {
        thread.join().unwrap();
    }
    let budget = admission.budget.lock();
    assert_eq!(
        (budget.requests, budget.bytes, budget.live_waiters),
        (0, 0, 0)
    );
}

#[test]
fn fifo_admission_failed_claim_returns_grant_and_wakes_successor() {
    let admission = fifo_admission(1, 8);
    let credit = admission.reserve(8, || Ok(())).unwrap();
    let (finished, received) = mpsc::sync_channel(2);
    let first_admission = admission.clone();
    let first_finished = finished.clone();
    let first = thread::spawn(move || {
        let checks = AtomicU64::new(0);
        let result = first_admission.reserve(7, || {
            if checks.fetch_add(1, Ordering::Relaxed) == 0 {
                Ok(())
            } else {
                Err(LocalDbError::new("cancelled before claiming credit"))
            }
        });
        first_finished.send(result.is_err()).unwrap();
    });
    await_waiters(&admission, 1);
    let second_admission = admission.clone();
    let second = thread::spawn(move || {
        let credit = second_admission.reserve(3, || Ok(())).unwrap();
        drop(credit);
        finished.send(true).unwrap();
    });
    await_waiters(&admission, 2);
    drop(credit);
    for _ in 0..2 {
        assert!(received.recv_timeout(Duration::from_secs(5)).unwrap());
    }
    first.join().unwrap();
    second.join().unwrap();
    let budget = admission.budget.lock();
    assert_eq!(
        (budget.requests, budget.bytes, budget.live_waiters),
        (0, 0, 0)
    );
}
#[test]
fn all_invalid_writes_release_budget_and_keep_owner_healthy() {
    let mut fixture = scalar_window_fixture();
    fixture.config.max_pending_batches = 4;
    let original = fixture.open();
    let next = original.core.next_sequence();
    let invalid = |index| {
        (
            fixture.operation(0, index),
            vec![Mutation::Put {
                keyspace: KeySpace(0),
                key: vec![0; 3].into(),
                value: vec![1; 32].into(),
            }],
        )
    };
    // One whole invalid window, then four invalid scalar callers.
    assert!(
        original
            .submit_many(&(0..4).map(invalid).collect::<Vec<_>>())
            .is_err()
    );
    let db = ConcurrentRemoteDatabase::new(original).unwrap();
    for index in 0..4 {
        let (operation, mutations) = invalid(index);
        assert!(db.write_batch(operation, mutations).is_err());
    }
    let charges = db.admission.charges();
    assert_eq!((charges.requests, charges.bytes, charges.slots), (0, 0, 0));
    let version = db
        .write_batch(fixture.operation(0, 5), vec![large_put(5)])
        .unwrap();
    assert_eq!(version.sequence, next);
    assert_eq!(get(&db, 5), Some(vec![5; 64 * 1024]));
    db.ensure_healthy().unwrap();
    db.shutdown().unwrap();
}

/// Readers blocked on read admission hold no sparse view: after its lookup a
/// point read pins only the retention floor. Each reader resolves a different
/// view that is then replaced, yet the reader-held bytes stay within
/// `max_pending_reads` views (here none), and every read completes once
/// admission is free.
#[test]
fn readers_waiting_for_admission_hold_no_sparse_view() {
    const READERS: u32 = 8;
    let mut fixture = Fixture::new();
    fixture.config.read_cache_bytes = 0;
    fixture.config.max_pending_reads = 1;
    let original = fixture.open();
    original
        .write_batch(fixture.operation(1, 100), vec![put(0, 1)])
        .unwrap();
    for reader in 0..READERS {
        original
            .write_batch(
                fixture.operation(1 + reader % 4, 101 + u64::from(reader)),
                vec![put(10 + reader, 10 + reader as u8)],
            )
            .unwrap();
    }
    let held_connection = original.core.clients[0].client.clone();
    let held = held_connection.lock();
    let db = Arc::new(ConcurrentRemoteDatabase::new(original).unwrap());
    // An unresolved write on owner 0 makes every later publication sparse.
    let blocked = {
        let db = db.clone();
        let operation = fixture.operation(0, 200);
        thread::spawn(move || db.write_batch(operation, vec![put(50, 50)]))
    };
    let deadline = Instant::now() + Duration::from_secs(10);
    while db.database.core.clients[0].queue.snapshot().2.len() != 1 {
        assert!(Instant::now() < deadline, "blocked write never queued");
        thread::yield_now();
    }
    let mut published = 0u64;
    let mut publish = |db: &ConcurrentRemoteDatabase| {
        published += 1;
        let key = 60 + published as u32;
        db.write_batch(
            fixture.operation(1 + key % 4, 300 + published),
            vec![put(key, 7)],
        )
        .unwrap();
    };
    publish(&db);
    assert!(matches!(db.index.call_cut(), IndexReadCut::Sparse(_)));
    // One replaced, uncharged view held inside a read measures one view.
    let probe = db.index.call_cut();
    publish(&db);
    let view = db.index_stats().reader_held_view_bytes;
    assert!(view > 0);
    drop(probe);
    assert_eq!(db.index_stats().reader_held_view_bytes, 0);
    // Sample every resolution: a reader's pin stage is recorded after its
    // pin returns, when its resolved view is already dropped.
    db.set_read_profile_limit(64);
    let monitor = db.read_profile_monitor();
    let pinned = || {
        monitor
            .coordinator_resolution_snapshot()
            .pin_preparation
            .count
    };
    // The one admitted read keeps the only read credit until it is finished.
    let pending = db.submit_get(KeySpace(0), &0u32.to_be_bytes()).unwrap();
    let (results, received) = mpsc::channel();
    let mut readers = Vec::new();
    for reader in 0..READERS {
        let before = pinned();
        let (db_reader, results) = (db.clone(), results.clone());
        readers.push(thread::spawn(move || {
            let key = 10 + reader;
            results
                .send((key, db_reader.get(KeySpace(0), &key.to_be_bytes())))
                .unwrap();
        }));
        // The reader resolved the current view and pinned its floor; it now
        // waits for admission, and the next publication replaces that view.
        while pinned() == before {
            assert!(Instant::now() < deadline, "reader {reader} never resolved");
            thread::yield_now();
        }
        publish(&db);
    }
    // Every reader is past its lookup, so none holds a view any more; the
    // admitted read holds only a floor pin as well.
    let held_bytes = db.index_stats().reader_held_view_bytes;
    assert!(held_bytes <= db.config.max_pending_reads * view);
    assert_eq!(held_bytes, 0, "waiting and admitted reads hold no view");
    assert!(
        matches!(received.try_recv(), Err(mpsc::TryRecvError::Empty)),
        "readers must still wait for admission"
    );
    assert_eq!(
        db.finish_ready_get(pending.wait())
            .unwrap()
            .unwrap()
            .as_ref(),
        &[1; 32]
    );
    for reader in readers {
        reader.join().unwrap();
    }
    drop(results);
    let mut values: Vec<_> = received
        .iter()
        .map(|(key, value)| (key, value.unwrap().unwrap().to_vec()))
        .collect();
    values.sort_unstable_by_key(|(key, _)| *key);
    let expected: Vec<_> = (0..READERS)
        .map(|reader| (10 + reader, vec![10 + reader as u8; 32]))
        .collect();
    assert_eq!(values, expected);
    assert_eq!(db.index_stats().reader_held_view_bytes, 0);
    drop(held);
    blocked.join().unwrap().unwrap();
    Arc::try_unwrap(db).ok().unwrap().shutdown().unwrap();
}

/// A Sui `multi_get` of more than one window takes no owner charge: with the
/// view allowance exhausted by reservations and a retained checkpoint cut, it
/// still succeeds, and every window reads the one sparse cut (the held batch
/// stays invisible, the later batch is visible).
#[test]
fn sui_multi_window_get_succeeds_with_view_budget_exhausted() {
    use crate::distributed::sui::{Checkpoint, Db};
    const KEYS: u32 = 260;
    let mut fixture = Fixture::new();
    // No cache: every window fetches its misses under the shared floor pin.
    fixture.config.read_cache_bytes = 0;
    let path = fixture.temp.path().join("authority");
    let db = Db::open_remote_local_fixture(&path, fixture.shape.clone(), fixture.config.clone())
        .unwrap();
    let ks = db.ks("root");
    let partition = |sequence: u64| {
        let mut operation = [0; 16];
        operation[..8].copy_from_slice(&1u64.to_le_bytes());
        operation[8..].copy_from_slice(&sequence.to_le_bytes());
        fixture
            .config
            .inventory(1)
            .partition_for(BatchId {
                database: fixture.config.storage.database,
                operation,
            })
            .unwrap()
    };
    // Public Sui commits use one operation per commit, from zero.
    let per_commit = fixture.config.storage.batch_limits.max_operations;
    let mut commits = 0u64;
    for first in (0..KEYS).step_by(per_commit as usize) {
        let mut batch = db.write_batch();
        for key in first..(first + per_commit).min(KEYS) {
            batch.write(ks, key.to_be_bytes().to_vec(), vec![1; 32]);
        }
        batch.commit().unwrap();
        commits += 1;
    }
    // The held commit routes to partition(prefix); the two later commits must
    // route elsewhere so they publish around it.
    let prefix = (commits..commits + 64)
        .find(|sequence| {
            partition(*sequence) != partition(*sequence + 1)
                && partition(*sequence) != partition(*sequence + 2)
        })
        .unwrap();
    for _ in commits..prefix {
        db.insert(ks, 0u32.to_be_bytes().to_vec(), vec![1; 32])
            .unwrap();
    }
    let connection = db
        .remote_for_test()
        .io_clients
        .iter()
        .find(|(node, _)| *node == partition(prefix))
        .unwrap()
        .1
        .clone();
    let held = connection.client.lock();
    let (progress, events) = mpsc::sync_channel(64);
    db.remote_for_test()
        .database
        .core
        .test_hooks
        .lock()
        .concurrent_progress = Some(progress);
    let (first_reply, first_received) = mpsc::channel();
    let first_db = db.clone();
    let first = thread::spawn(move || {
        let mut batch = first_db.write_batch();
        batch.write(ks, 1u32.to_be_bytes().to_vec(), vec![7; 32]);
        first_reply.send(batch.commit()).unwrap();
    });
    wait_event(&events, |stats| stats.concurrent_admitted == prefix + 1).unwrap();
    let mut later = db.write_batch();
    later.write(ks, 2u32.to_be_bytes().to_vec(), vec![8; 32]);
    later.commit().unwrap();
    let remote = db.remote_for_test();
    assert!(matches!(remote.index.call_cut(), IndexReadCut::Sparse(_)));
    // Exhaust the view allowance: reservations, then one retained checkpoint
    // cut of the sparse view takes the last owner charge.
    let max_excluded = fixture.config.max_pending_batches - 1;
    let mut reservations = Vec::new();
    loop {
        match remote.index.reserve_publication_view(max_excluded) {
            Ok(reservation) => reservations.push(reservation),
            Err(super::super::super::native_index::NativeIndexError::Capacity) => break,
            Err(error) => panic!("unexpected reservation failure: {error}"),
        }
    }
    reservations.pop();
    let checkpoint = db.checkpoint();
    assert!(matches!(checkpoint, Checkpoint::Remote(_, Ok(_))));
    assert!(remote.index.reserve_publication_view(max_excluded).is_err());
    // A newer sparse view has no owner charge yet: a charged cut of it would
    // now fail with `Capacity`.
    let mut newest = db.write_batch();
    newest.write(ks, 3u32.to_be_bytes().to_vec(), vec![9; 32]);
    newest.commit().unwrap();
    assert!(matches!(remote.index.call_cut(), IndexReadCut::Sparse(_)));
    assert!(remote.published_cut().is_err());
    let charged = remote.index_stats().transient_metadata_bytes;
    let keys: Vec<_> = (0..KEYS).map(|key| key.to_be_bytes().to_vec()).collect();
    let values = db.multi_get(ks, &keys);
    assert_eq!(values.len(), keys.len());
    for (key, value) in (0..KEYS).zip(values) {
        let expected = match key {
            2 => 8,
            3 => 9,
            _ => 1,
        };
        assert_eq!(
            value.unwrap().unwrap().as_ref(),
            &[expected; 32],
            "key {key}"
        );
    }
    assert_eq!(remote.index_stats().transient_metadata_bytes, charged);
    assert_eq!(remote.index_stats().reader_held_view_bytes, 0);
    drop(checkpoint);
    drop(reservations);
    drop(held);
    first_received
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    first.join().unwrap();
    assert_eq!(
        db.get(ks, &1u32.to_be_bytes()).unwrap().unwrap().as_ref(),
        &[7; 32]
    );
    db.wait_for_background_threads_to_finish().unwrap();
}
