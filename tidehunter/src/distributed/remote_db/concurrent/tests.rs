//! Real five-node mTLS/Value-WAL regressions. Holding the existing authenticated
//! control stream mutex pauses append actors, not a mock storage implementation.
use super::super::pipeline_tests::Fixture;
use super::*;

/// Liveness guard for hook handshakes; never a latency bound.
const WAIT: Duration = Duration::from_secs(30);

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

/// Native parity for read admission: load alone never fails an ordinary read
/// (it passes no deadline), and only an explicit deadline can expire. The only
/// credit is held by a caller's request, so the parked wait is proven from the
/// admission state at the wait, never from a timing bound. The read's terminal
/// completion is held so its flight cannot retire and change the generation
/// while the waiter is observed.
#[test]
fn ordinary_reads_never_fail_on_load_and_explicit_deadlines_expire() {
    let mut fixture = Fixture::new();
    // No cache: the read must take a credit, and the fixture's default I/O
    // timeout stays in force so the acknowledged put cannot fail on load.
    fixture.config.read_cache_bytes = 0;
    let original = fixture.open();
    original
        .write_batch(fixture.operation(0, 1), vec![put(1, 1)])
        .unwrap();
    let db = Arc::new(ConcurrentRemoteDatabase::new(original).unwrap());
    let position = db
        .hybrid_table()
        .get(
            KeySpace(0),
            &1u32.to_be_bytes(),
            db.hybrid_table().current(),
        )
        .unwrap()
        .expect("the acknowledged put has an index position");
    let node = db
        .config
        .endpoints
        .iter()
        .position(|endpoint| endpoint.partition == position.address.partition)
        .expect("an endpoint owns the position");
    // The caller keeps the credit: it is released when this request is
    // consumed, whatever its lane does meanwhile. Hold the terminal completion
    // first, so the flight cannot retire and change the generation under the
    // waiter: the caller's credit is then the only admission change it can see.
    let (completed, completion) = db.reads.bench_hold_completion();
    let held = db.reads.submit_positions(node, &[position], None).unwrap();
    completed
        .recv_timeout(WAIT)
        .expect("the held read reaches its terminal completion");
    let observed = db.read_admission_generation();
    let (entered, resume) = db.reads.pause_admission_wait();
    let waiter_db = Arc::clone(&db);
    let (finished, result) = mpsc::sync_channel(1);
    let waiter = thread::spawn(move || {
        let _ = finished.send(waiter_db.wait_read_admission(observed, None));
    });
    // Observable state: the waiter reached the admission wait with the only
    // credit still held and the generation unchanged, so it cannot have
    // returned.
    let (credits, generation) = entered
        .recv_timeout(WAIT)
        .expect("the waiter must reach the admission wait");
    assert_eq!(credits, 1, "the request holds the only read credit");
    assert_eq!(
        generation, observed,
        "the waiter must observe no credit change"
    );
    resume.send(()).expect("the paused waiter resumes");
    // Retiring the held completion changes the generation and wakes the waiter;
    // the caller's member then reads the stored result.
    completion.release();
    held.finish_member().expect("the held read completes");
    result
        .recv_timeout(WAIT)
        .expect("a retired read must wake the waiter")
        .expect("a wait with no deadline must not fail on load");
    waiter.join().unwrap();
    // An explicit deadline is honoured: an already expired one is reported.
    let error = db
        .wait_read_admission(
            db.read_admission_generation(),
            Some(Instant::now() - Duration::from_millis(1)),
        )
        .unwrap_err();
    assert!(error.to_string().contains("deadline"), "{error}");
    Arc::try_unwrap(db).ok().unwrap().shutdown().unwrap();
}

/// A terminal read failure is one fail-stop state: it fences reads, cached-read
/// validation, writes and shutdown. The poison is injected at the read
/// authority, so this proves the sharing; the trigger is covered by
/// `live_lane`'s own failure cases.
#[test]
fn a_read_failure_fences_reads_writes_and_shutdown() {
    let mut fixture = Fixture::new();
    fixture.config.read_cache_bytes = 0;
    let original = fixture.open();
    original
        .write_batch(fixture.operation(0, 40), vec![put(0, 3)])
        .unwrap();
    let db = ConcurrentRemoteDatabase::new(original).unwrap();
    db.reads.poison();
    assert!(db.validate_cached_read().is_err());
    assert!(db.get(KeySpace(0), &0u32.to_be_bytes()).is_err());
    assert!(db.get(KeySpace(0), &99u32.to_be_bytes()).is_err());
    assert!(
        db.write_batch(fixture.operation(1, 41), vec![put(1, 4)])
            .is_err(),
        "a poisoned read authority fences the writer"
    );
    assert!(db.shutdown().is_err(), "shutdown reports the read failure");
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
    let old_cut = original.core.table.current();
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
        let table = db.hybrid_table();
        let a = table
            .get(KeySpace(0), &(1 + node * 2u32).to_be_bytes(), cut)
            .unwrap()
            .unwrap();
        let b = table
            .get(KeySpace(0), &(2 + node * 2u32).to_be_bytes(), cut)
            .unwrap()
            .unwrap();
        // Both members of one batch carry the same batch version.
        assert_eq!(a.batch, b.batch);
        assert!(
            table
                .get(KeySpace(0), &(1 + node * 2u32).to_be_bytes(), old_cut)
                .unwrap()
                .is_none()
        );
        assert_eq!(get(&db, 1 + node * 2), Some(vec![10 + node as u8; 32]));
        assert_eq!(get(&db, 2 + node * 2), Some(vec![10 + node as u8; 32]));
    }
    db.write_core().flush_index().unwrap();
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
fn unknown_append_fences_cached_absent_reads_until_reopen() {
    let fixture = Fixture::new();
    let mut original = fixture.open();
    original
        .write_batch(fixture.operation(0, 20), vec![put(0, 7)])
        .unwrap();
    original.get(KeySpace(0), &0u32.to_be_bytes()).unwrap();
    let db = ConcurrentRemoteDatabase::new(original).unwrap();
    assert!(get(&db, 0).is_some());
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
    // Accepted weakening: an unknown write whose receipt falls outside the
    // recovered retry window is re-applied under the new epoch instead of
    // being answered with the recorded version.
    let retried = reopened
        .write_batch(operation, vec![put(1, 8), put(2, 8)])
        .unwrap();
    assert!(retried.epoch > uncertain.epoch);
    assert_eq!(get(&reopened, 1), Some(vec![8; 32]));
    reopened.shutdown().unwrap();
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
fn sui_bulk_reads_group_shards_preserve_absent_keys_and_cache_cuts() {
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
    let (cut, _pin) = db.remote_for_test().hybrid_table().pin_checkpoint();
    let mut keys: Vec<_> = [0u32, 3, 1, 2, 0, 99]
        .into_iter()
        .map(|key| key.to_be_bytes().to_vec())
        .collect();
    keys.push(Vec::new()); // malformed key must not discard other results
    let before = db.remote_for_test().live_read_submission_count();
    let values = db.multi_get(ks, &keys);
    for (slot, expected) in [1u8, 4, 2, 3, 1].into_iter().enumerate() {
        assert_eq!(
            values[slot].as_ref().unwrap().as_deref(),
            Some([expected; 32].as_slice())
        );
    }
    assert!(values[5].as_ref().unwrap().is_none());
    // Native reads a zero-length key as an absent row: `Db::get` has no
    // read-side length rule (db.rs:685-711). The length rule is write-side
    // (key_shape.rs:1227-1240) and in the distributed write codec
    // (codec.rs:955-967); native's suite states the read contract at
    // db_tests/db_tests.rs:2977-2978. The hybrid read path has no check.
    assert!(values[6].as_ref().unwrap().is_none());
    assert_eq!(
        db.remote_for_test().live_read_submission_count() - before,
        2
    );
    db.validate_cached_read().unwrap();
    let before = db.remote_for_test().live_read_submission_count();
    assert!(db.multi_get(ks, &keys)[6].as_ref().unwrap().is_none());
    assert_eq!(db.remote_for_test().live_read_submission_count(), before);
    db.insert(ks, 0u32.to_be_bytes().to_vec(), vec![9; 32])
        .unwrap();
    let retained = db
        .remote_for_test()
        .database
        .get_many_at_cut(ks, &keys, cut, None, false)
        .unwrap();
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
    assert!(latest[6].as_ref().unwrap().is_none());
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
    let (old_cut, _pin) = db.hybrid_table().pin_checkpoint();
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
        let values = db
            .database
            .get_many_at_cut(KeySpace(0), &keys, cut, None, false)
            .unwrap();
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
                // A zero-length key reads as absent, as native `Db::get` does
                // (db.rs:685-711; db_tests/db_tests.rs:2977-2978).
                _ => assert!(value.as_ref().unwrap().is_none()),
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
            let (db, keys, ready, cut) = (&db, &keys, &ready, cut);
            scope.spawn(move || {
                ready.wait();
                for _ in 0..8 {
                    let values = db
                        .database
                        .get_many_at_cut(KeySpace(0), keys, cut, None, false)
                        .unwrap();
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
    let (old_cut, old_pin) = db.hybrid_table().pin_checkpoint();
    let old_position = db
        .hybrid_table()
        .get(KeySpace(0), &7u32.to_be_bytes(), old_cut)
        .unwrap()
        .unwrap();
    let keys = [7u32.to_be_bytes().to_vec()];
    let rpc_count = db.live_read_submission_count();
    assert_eq!(
        db.database
            .get_many_at_cut(KeySpace(0), &keys, old_cut, None, false)
            .unwrap()[0]
            .as_ref()
            .unwrap()
            .as_deref(),
        Some([1; 32].as_slice())
    );
    assert_eq!(
        db.live_read_submission_count(),
        rpc_count,
        "publication primes the cache, so an acknowledged immutable cut needs no owner read"
    );

    db.write_batch(fixture.operation(1, 91), vec![put(7, 2)])
        .unwrap();
    let (updated_cut, updated_pin) = db.hybrid_table().pin_checkpoint();
    let updated_position = db
        .hybrid_table()
        .get(KeySpace(0), &7u32.to_be_bytes(), updated_cut)
        .unwrap()
        .unwrap();
    assert_ne!(old_position, updated_position);
    db.write_batch(
        fixture.operation(2, 92),
        vec![Mutation::Delete {
            keyspace: KeySpace(0),
            key: 7u32.to_be_bytes().to_vec().into(),
        }],
    )
    .unwrap();
    let deleted_cut = db.published_cut().unwrap();
    let before_reads = db.live_read_submission_count();
    let old = db
        .database
        .get_many_at_cut(KeySpace(0), &keys, old_cut, None, false)
        .unwrap();
    let updated = db
        .database
        .get_many_at_cut(KeySpace(0), &keys, updated_cut, None, false)
        .unwrap();
    let deleted = db
        .database
        .get_many_at_cut(KeySpace(0), &keys, deleted_cut, None, false)
        .unwrap();
    assert_eq!(
        old[0].as_ref().unwrap().as_deref(),
        Some([1; 32].as_slice())
    );
    assert_eq!(
        updated[0].as_ref().unwrap().as_deref(),
        Some([2; 32].as_slice())
    );
    assert!(deleted[0].as_ref().unwrap().is_none());
    assert_eq!(
        db.live_read_submission_count(),
        before_reads,
        "every acknowledged cut stays cache-resident"
    );
    let cache = db.read_cache_stats();
    // One prime per acknowledged put; the reads above inserted nothing because
    // every read was a hit.
    assert!(cache.inserts >= 2);
    assert!(cache.retained_bytes <= cache.budget_bytes);
    // The older cuts were held by their pins for every read above.
    drop((old_pin, updated_pin));
    db.shutdown().unwrap();
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
        db.write_core().flush_index().unwrap();
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
    db.write_core().flush_index().unwrap();
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
    db.write_core().flush_index().unwrap();
    // Retries are acknowledged, but only the two new identities allocated.
    assert_eq!(db.stats().admitted_batches, admitted + 2);
    db.shutdown().unwrap();
}

fn fifo_admission(requests: usize, bytes: usize) -> Arc<Admission> {
    Arc::new(Admission {
        budget: Mutex::new(WriteBudget::default()),
        drained: parking_lot::Condvar::new(),
        max_requests: requests,
        max_bytes: bytes,
        partition_batches: 1024,
        reference_bytes: 1024 * 1024,
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
        budget.slots = 3;
        budget.checkpoint.partition_batches = vec![2, 1];
        budget.checkpoint.partition_records = vec![5, 4];
    }
    for (slots, partitions) in [
        (4, vec![(1, 2), (0, 0)]),
        (1, vec![(1, 2)]),
        (1, vec![(3, 2), (0, 0)]),
        (1, vec![(1, 2), (1, 5)]),
    ] {
        assert!(admission.release_publication(slots, &partitions).is_err());
        let budget = admission.budget.lock();
        assert_eq!(budget.slots, 3);
        assert_eq!(budget.checkpoint.partition_batches, [2, 1]);
        assert_eq!(budget.checkpoint.partition_records, [5, 4]);
    }
    admission.release_publication(2, &[(2, 5), (1, 4)]).unwrap();
    {
        let budget = admission.budget.lock();
        assert_eq!(budget.slots, 1);
        assert_eq!(budget.checkpoint.partition_batches, [0, 0]);
        assert_eq!(budget.checkpoint.partition_records, [0, 0]);
    }
    admission.release_publication(1, &[(0, 0), (0, 0)]).unwrap();
    assert_eq!(admission.budget.lock().slots, 0);
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
