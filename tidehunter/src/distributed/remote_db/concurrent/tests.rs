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
        .clients
        .iter()
        .take(2)
        .map(|worker| worker.client.clone())
        .collect();
    let held: Vec<_> = connections.iter().map(|client| client.lock()).collect();
    let (progress, events) = mpsc::sync_channel(64);
    *original.concurrent_progress.lock() = Some(progress);
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
    let dispatched = wait_event(&events, |stats| stats.peak_inflight_nodes == 2);
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
    assert_eq!(stats.direct_appends, 1); // baseline only; concurrent writes never use drain_single
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
    fixture.nodes[1].inject_drop_next_append_response();
    let operation = fixture.operation(1, 21);
    let error = db
        .write_batch(operation, vec![put(1, 8), put(2, 8)])
        .unwrap_err();
    assert_eq!(db.read_cache_stats().inserts, cache_inserts_before_unknown);
    let uncertain = error
        .uncertain_version
        .expect("admitted unknown append keeps its version");
    assert!(db.finish_ready_get(cached).is_err());
    assert!(db.get(KeySpace(0), &0u32.to_be_bytes()).is_err());
    assert!(db.get(KeySpace(0), &99u32.to_be_bytes()).is_err());
    assert!(
        db.write_batch(fixture.operation(2, 22), vec![put(3, 9)])
            .is_err()
    );
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
    let mut original = fixture.open();
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
    assert_eq!(snapshot.index_lock_wait.count, 2);
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
    let mut original = fixture.open();
    original
        .write_batch(fixture.operation(0, 30), vec![put(0, 5), put(1, 6)])
        .unwrap();
    let db = ConcurrentRemoteDatabase::new(original).unwrap();
    assert!(!db.stats().read_resolve_ns_available);
    db.set_read_profile_limit(2);
    let monitor = db.read_profile_monitor();
    assert_eq!(get(&db, 0), Some(vec![5; 32]));
    assert!(db.stats().read_resolve_ns_available);
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
    db.wait_read_admission(generation, Instant::now() + Duration::from_secs(1))
        .unwrap();
    // Repeated credit releases must not let an expired caller retry forever.
    assert!(db.wait_read_admission(generation, Instant::now()).is_err());
    let mut counters = HashMap::new();
    monitor.visit(&mut |name, value| {
        counters.insert(name, value);
    });
    assert_eq!(counters["read_profile_selected"], 2);
    assert_eq!(counters["read_profile_completed"], 2);
    assert_eq!(counters["read_profile_inline_completed"], 1);
    assert_eq!(counters["read_profile_actor_completed"], 1);
    assert_eq!(counters["read_profile_returned"], 1);
    assert!(counters["read_profile_inline_ns"] > 0);
    assert_eq!(counters["read_profile_server_response_recheck_samples"], 2);
    let resolution = monitor.coordinator_resolution_snapshot();
    assert_eq!(resolution.remote_misses, 2);
    assert_eq!(resolution.route_preparation.count, 2);
    assert_eq!(resolution.pin_preparation.count, 2);
    assert_eq!(resolution.batch_preparation.count, 0);
    assert_eq!(db.read_rpc_count(), 2);
    db.shutdown().unwrap();
}

#[test]
fn cancelled_late_read_failure_poison_is_shared_with_writer_and_cache() {
    let mut fixture = Fixture::new();
    fixture.config.read_cache_bytes = 0;
    let mut original = fixture.open();
    original
        .write_batch(fixture.operation(0, 40), vec![put(0, 3)])
        .unwrap();
    let lane = original.reads.lane_client(0, 0);
    let held = lane.lock();
    original.clients[0]
        .client
        .lock()
        .fence_to(original.epoch + 1)
        .unwrap();
    let db = ConcurrentRemoteDatabase::new(original).unwrap();
    let pending = db.submit_get(KeySpace(0), &0u32.to_be_bytes()).unwrap();
    drop(pending); // cancellation cannot detach a late transport failure
    let observed = db.read_admission_generation();
    drop(held);
    let _ = db.wait_read_admission(observed, Instant::now() + Duration::from_secs(3));
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
    let db = Db::open_remote(&path, fixture.shape.clone(), fixture.config.clone()).unwrap();
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
    let held = connection.lock();
    let (progress, events) = mpsc::sync_channel(64);
    *db.remote_for_test().progress.lock() = Some(progress);
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
    let later_durable = wait_event(&events, |stats| {
        stats.concurrent_admitted == prefix + 2 && stats.durable_batches > prefix
    });
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
    let overlap = later_durable.unwrap();
    assert!(overlap.peak_inflight_nodes >= 2);
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
    let reopened = Db::open_remote(&path, fixture.shape.clone(), fixture.config.clone()).unwrap();
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
    let connection = original.clients[0].client.clone();
    let held = connection.lock();
    let (entered, entering) = mpsc::sync_channel(1);
    original.before_checkpoint_io = Some(entered);
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
    let mut original = fixture.open();
    let connections: Vec<_> = original
        .clients
        .iter()
        .take(2)
        .map(|worker| worker.client.clone())
        .collect();
    let held: Vec<_> = connections.iter().map(|client| client.lock()).collect();
    original.panic_after_concurrent_admissions = Some(2);
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
    let mut original = fixture.open();
    original
        .write_batch(fixture.operation(0, 70), vec![put(0, 1), put(1, 2)])
        .unwrap();
    original
        .write_batch(fixture.operation(1, 71), vec![put(2, 3), put(3, 4)])
        .unwrap();
    original.shutdown().unwrap();
    drop(original);
    let db = Db::open_remote(
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
fn competing_grouped_readers_share_one_credit_without_losing_members() {
    let mut fixture = Fixture::new();
    fixture.config.read_cache_bytes = 0;
    fixture.config.max_pending_reads = 1;
    fixture.config.max_pending_read_bytes = 16 * 1024;
    let mut original = fixture.open();
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
    assert_eq!(
        after.pipeline.concurrent_completed - before.pipeline.concurrent_completed,
        1
    );
    assert_eq!(
        after.pipeline.durable_batches - before.pipeline.durable_batches,
        1
    );
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
    assert_eq!(profile.profile.storage_write.append_calls, 1);
    db.shutdown().unwrap();
}
