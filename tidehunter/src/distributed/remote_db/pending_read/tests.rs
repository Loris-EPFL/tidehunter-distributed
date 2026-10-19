use super::*;
use crate::distributed::remote_db::pipeline_tests::Fixture;

pub(super) fn put(key: u32, value: u8) -> Mutation {
    Mutation::Put {
        keyspace: KeySpace(0),
        key: key.to_be_bytes().to_vec().into(),
        value: vec![value; 32].into(),
    }
}

pub(super) fn reopen_cold(fixture: &Fixture, mut db: RemoteDatabase) -> RemoteDatabase {
    db.shutdown().unwrap();
    drop(db);
    fixture.open()
}

pub(super) fn node_for(db: &RemoteDatabase, key: u32) -> usize {
    let reference = db
        .index_reference(KeySpace(0), &key.to_be_bytes(), IndexReadCut::Latest)
        .unwrap()
        .unwrap();
    db.partition_indexes[&reference.batch_address.partition]
}
pub(super) fn value(byte: u8) -> Option<Bytes> {
    Some(vec![byte; 32].into())
}
#[test]
fn bounded_read_profiles_count_rpc_misses_not_hits_absences_or_backpressure() {
    let mut fixture = Fixture::new();
    fixture.config.max_pending_reads = 1;
    let mut db = fixture.open();
    db.write_batch(
        fixture.operation(0, 0),
        vec![put(0, 7), put(1, 8), put(2, 9)],
    )
    .unwrap();
    let mut db = reopen_cold(&fixture, db);
    db.set_read_profile_limit(2);
    let monitor = db.read_profile_monitor();
    let count = |name| {
        let mut value = None;
        monitor.visit(&mut |key, n| {
            if key == name {
                value = Some(n);
            }
        });
        value.expect("named diagnostic counter")
    };
    let absent = db
        .submit_get(KeySpace(0), &99u32.to_be_bytes())
        .unwrap()
        .wait();
    assert_eq!(db.finish_ready_get(absent).unwrap(), None);
    assert_eq!(count("read_profile_selected"), 0);

    let lane = db.reads.lane_client(node_for(&db, 0), 0);
    let held = lane.lock();
    let first = db.submit_get(KeySpace(0), &0u32.to_be_bytes()).unwrap();
    assert!(RemoteDatabase::read_backpressured(
        &db.submit_get(KeySpace(0), &1u32.to_be_bytes())
            .err()
            .unwrap()
    ));
    assert_eq!(count("read_profile_selected"), 1);
    drop(held);
    assert_eq!(db.finish_ready_get(first.wait()).unwrap(), value(7));
    let hit = db
        .submit_get(KeySpace(0), &0u32.to_be_bytes())
        .unwrap()
        .wait();
    assert_eq!(db.finish_ready_get(hit).unwrap(), value(7));
    assert_eq!(count("read_profile_selected"), 1);

    for key in 1u32..3 {
        let completed = db
            .submit_get(KeySpace(0), &key.to_be_bytes())
            .unwrap()
            .wait();
        assert_eq!(
            db.finish_ready_get(completed).unwrap(),
            value(key as u8 + 7)
        );
    }
    assert_eq!(db.read_rpc_count(), 3);
    assert_eq!(count("read_profile_selected"), 2);
    assert_eq!(count("read_profile_completed"), 2);
    assert_eq!(count("read_profile_errors"), 0);
    assert_eq!(count("read_profile_returned"), 2);
    assert_eq!(count("read_profile_server_response_recheck_samples"), 2);
    assert_eq!(count("read_profile_server_available"), 1);
    assert_eq!(count("read_profile_transport_progress_available"), 1);
    assert_eq!(count("read_profile_transport_cq_available"), 0); // TCP, not missing EFA evidence.
    assert_eq!(*db.reads.state.retained.lock(), (0, 0));
    db.shutdown().unwrap();
}

fn synchronous_read_budget_case(scan: bool, byte_limited: bool) {
    let mut fixture = Fixture::new();
    fixture.config.read_cache_bytes = 0;
    fixture.config.max_pending_reads = if byte_limited { 256 } else { 1 };
    let mut db = fixture.open();
    // More than one scan page exercises both per-page shard admission and
    // lookahead competing with an earlier page for the same global credits.
    let count = if scan { 260u32 } else { 8u32 };
    for batch in 0..count.div_ceil(4) {
        let mutations = (batch * 4..(batch * 4 + 4).min(count))
            .map(|key| put(key, key as u8))
            .collect();
        db.write_batch(fixture.operation(batch % 2, batch as u64), mutations)
            .unwrap();
    }
    if byte_limited {
        let pending = db.submit_get(KeySpace(0), &0u32.to_be_bytes()).unwrap();
        let singleton_charge = db.reads.state.retained.lock().1;
        db.finish_get(pending).unwrap();
        // One member fits, but a same-shard group does not. Synchronous reads
        // must split into bounded waves rather than reject a legal collection.
        Arc::get_mut(&mut db.reads).unwrap().max_bytes = singleton_charge;
    }
    if scan {
        let rows = db.scan(KeySpace(0)).unwrap();
        assert_eq!(rows.len(), count as usize);
        for (index, (key, actual)) in rows.into_iter().enumerate() {
            assert_eq!(key.as_ref(), (index as u32).to_be_bytes().as_slice());
            assert_eq!(Some(actual), value(index as u8));
        }
    } else {
        let keys: Vec<_> = (0..count).map(|key| key.to_be_bytes().to_vec()).collect();
        assert_eq!(
            db.get_many(KeySpace(0), &keys).unwrap(),
            (0..count).map(|key| value(key as u8)).collect::<Vec<_>>()
        );
    }
    assert!(db.healthy);
    assert!(!db.reads.failed());
    assert_eq!(*db.reads.state.retained.lock(), (0, 0));
    db.shutdown().unwrap();
}

#[test]
fn synchronous_get_many_progresses_with_one_request_credit() {
    synchronous_read_budget_case(false, false);
}

#[test]
fn synchronous_scan_progresses_with_one_request_credit() {
    synchronous_read_budget_case(true, false);
}

#[test]
fn synchronous_get_many_splits_groups_into_byte_budget_waves() {
    synchronous_read_budget_case(false, true);
}

#[test]
fn synchronous_scan_splits_groups_into_byte_budget_waves() {
    synchronous_read_budget_case(true, true);
}

#[test]
fn external_read_pressure_does_not_poison_synchronous_conveniences() {
    let mut fixture = Fixture::new();
    fixture.config.max_pending_reads = 1;
    fixture.config.read_cache_bytes = 0;
    let mut db = fixture.open();
    db.write_batch(fixture.operation(0, 0), vec![put(0, 7)])
        .unwrap();
    db.write_batch(fixture.operation(1, 1), vec![put(1, 8)])
        .unwrap();
    let lane = db.reads.lane_client(0, 0);
    let held = lane.lock();
    let pending = db.submit_get(KeySpace(0), &0u32.to_be_bytes()).unwrap();
    let keys = vec![1u32.to_be_bytes().to_vec()];
    assert!(RemoteDatabase::read_backpressured(
        &db.get_many(KeySpace(0), &keys).unwrap_err()
    ));
    // A scan may join the occupied immutable reference. Let that real I/O
    // complete; the original unconsumed handle still owns the only physical
    // credit, so the scan's distinct next shard must report backpressure.
    let scan = thread::spawn(move || {
        let result = db.scan(KeySpace(0));
        (db, pending, result)
    });
    drop(held);
    let (mut db, pending, result) = scan.join().unwrap();
    assert!(RemoteDatabase::read_backpressured(&result.unwrap_err()));
    assert!(db.healthy);
    assert!(!db.reads.failed());
    assert_eq!(db.finish_get(pending).unwrap(), value(7));
    assert_eq!(db.get_many(KeySpace(0), &keys).unwrap(), vec![value(8)]);
    assert_eq!(db.scan(KeySpace(0)).unwrap().len(), 2);
    db.shutdown().unwrap();
}

#[test]
fn byte_bounded_reads_accept_request_limits_above_channel_capacity() {
    let mut fixture = Fixture::new();
    fixture.config.max_pending_reads = usize::MAX;
    fixture.config.max_pending_read_bytes = 4096;
    fixture.config.read_cache_bytes = 0;
    let mut db = fixture.open();
    for batch in 0u32..2 {
        db.write_batch(
            fixture.operation(batch % 2, u64::from(batch)),
            (batch * 4..batch * 4 + 4)
                .map(|key| put(key, key as u8))
                .collect(),
        )
        .unwrap();
    }
    let mut db = reopen_cold(&fixture, db);
    let keys = (0u32..8)
        .map(|key| key.to_be_bytes().to_vec())
        .collect::<Vec<_>>();
    assert_eq!(
        db.get_many(KeySpace(0), &keys).unwrap(),
        (0..8).map(value).collect::<Vec<_>>()
    );
    assert_eq!(*db.reads.state.retained.lock(), (0, 0));
    db.shutdown().unwrap();
}

#[test]
fn oversized_scalar_read_does_not_poison_write_authority() {
    let mut fixture = Fixture::new();
    fixture.config.max_pending_read_bytes = ReadService::MIN_FLIGHT_BYTES;
    let mut db = fixture.open();
    db.write_batch(fixture.operation(0, 0), vec![put(0, 7)])
        .unwrap();
    let mut db = reopen_cold(&fixture, db);
    assert!(admission_error(
        &db.get(KeySpace(0), &0u32.to_be_bytes()).unwrap_err()
    ));
    db.write_batch(fixture.operation(1, 1), vec![put(1, 8)])
        .unwrap();
    assert!(db.exists(KeySpace(0), &1u32.to_be_bytes()).unwrap());
    db.shutdown().unwrap();
}

#[test]
fn inline_reads_reserve_idle_lanes_with_unrelated_retained_credits() {
    let mut fixture = Fixture::new();
    fixture.config.read_lanes = 1;
    fixture.config.max_pending_reads = 4;
    fixture.config.read_cache_bytes = 0;
    let mut db = fixture.open();
    db.write_batch(fixture.operation(0, 0), vec![put(0, 7)])
        .unwrap();
    db.write_batch(fixture.operation(1, 1), vec![put(1, 8)])
        .unwrap();
    let first = db
        .index_reference(KeySpace(0), &0u32.to_be_bytes(), IndexReadCut::Latest)
        .unwrap()
        .unwrap();
    let second = db
        .index_reference(KeySpace(0), &1u32.to_be_bytes(), IndexReadCut::Latest)
        .unwrap()
        .unwrap();
    let first_node = db.partition_indexes[&first.batch_address.partition];
    let second_node = db.partition_indexes[&second.batch_address.partition];
    assert_ne!(first_node, second_node);

    let lane = db.reads.lane_client(second_node, 0);
    let held = lane.lock();
    let pending = db.submit_get(KeySpace(0), &1u32.to_be_bytes()).unwrap();
    assert_eq!(db.reads.state.retained.lock().0, 1);

    let inline = db
        .reads
        .read_inline(first_node, db.epoch, &first, 4, None, None)
        .unwrap()
        .expect("idle independent lane should make progress inline");
    assert!(
        matches!(inline.as_slice(), [Mutation::Put { value, .. }] if value.as_ref() == [7; 32])
    );
    assert_eq!(db.reads.state.retained.lock().0, 1);

    assert!(
        db.reads
            .read_inline(second_node, db.epoch, &second, 4, None, None)
            .unwrap()
            .is_none(),
        "queued actor ownership must block inline theft"
    );
    drop(held);
    assert_eq!(db.finish_get(pending).unwrap(), value(8));
    assert_eq!(*db.reads.state.retained.lock(), (0, 0));
    db.shutdown().unwrap();
}

#[test]
fn lane_reservation_backpressure_does_not_signal_false_capacity() {
    let mut fixture = Fixture::new();
    fixture.config.read_lanes = 1;
    fixture.config.max_pending_reads = 4;
    fixture.config.read_cache_bytes = 0;
    let mut db = fixture.open();
    db.write_batch(fixture.operation(0, 0), vec![put(0, 7)])
        .unwrap();
    let reference = db
        .index_reference(KeySpace(0), &0u32.to_be_bytes(), IndexReadCut::Latest)
        .unwrap()
        .unwrap();
    let node = db.partition_indexes[&reference.batch_address.partition];
    let lane = db.reads.lanes.read()[node][0].clone();
    lane.scheduling.lock().inline_reserved = true;
    let generation = db.reads.admission_generation();

    assert!(
        db.reads
            .submit(node, vec![reference], 0, false, None)
            .is_err()
    );
    assert_eq!(db.reads.admission_generation(), generation);
    assert_eq!(*db.reads.state.retained.lock(), (0, 0));

    lane.scheduling.lock().inline_reserved = false;
    db.shutdown().unwrap();
}

#[test]
fn inline_transport_failure_poison_is_terminal() {
    let fixture = Fixture::new();
    let mut db = fixture.open();
    db.write_batch(fixture.operation(0, 0), vec![put(0, 7)])
        .unwrap();
    let reference = db
        .index_reference(KeySpace(0), &0u32.to_be_bytes(), IndexReadCut::Latest)
        .unwrap()
        .unwrap();
    let node = db.partition_indexes[&reference.batch_address.partition];
    db.clients[node]
        .client
        .lock()
        .fence_to(db.epoch + 1)
        .unwrap();
    assert!(
        db.reads
            .read_inline(node, db.epoch, &reference, 4, None, None)
            .is_err()
    );
    assert!(db.reads.failed());
    assert!(db.get(KeySpace(0), &0u32.to_be_bytes()).is_err());
    assert!(db.shutdown().is_err());
}
#[test]
fn cached_hit_and_same_shard_miss_progress_while_first_miss_is_blocked() {
    let fixture = Fixture::new();
    let mut db = fixture.open();
    db.write_batch(
        fixture.operation(0, 0),
        vec![put(0, 7), put(1, 8), put(3, 10)],
    )
    .unwrap();
    db.write_batch(fixture.operation(1, 1), vec![put(2, 9)])
        .unwrap();
    let mut db = reopen_cold(&fixture, db);
    assert_eq!(db.get(KeySpace(0), &2u32.to_be_bytes()).unwrap(), value(9));
    let first_node = node_for(&db, 0);
    assert_eq!(first_node, node_for(&db, 1));
    let lane = db.reads.lane_client(first_node, 0);
    let held = lane.lock();
    let mut first = db.submit_get(KeySpace(0), &0u32.to_be_bytes()).unwrap();
    assert!(db.poll_get(&mut first).unwrap().is_none());
    let mut hit = db.submit_get(KeySpace(0), &2u32.to_be_bytes()).unwrap();
    assert_eq!(db.poll_get(&mut hit).unwrap(), Some(value(9)));
    assert!(db.poll_get(&mut hit).is_err());
    let second = db.submit_get(KeySpace(0), &1u32.to_be_bytes()).unwrap();
    assert_eq!(db.finish_get(second).unwrap(), value(8));
    assert!(db.poll_get(&mut first).unwrap().is_none());
    let third = db.submit_get(KeySpace(0), &3u32.to_be_bytes()).unwrap();
    assert_eq!(db.finish_get(third).unwrap(), value(10));
    drop(held);
    assert_eq!(db.finish_get(first).unwrap(), value(7));
    db.shutdown().unwrap();
}

#[test]
fn one_connection_admits_several_reads_and_retains_completion_credits() {
    let mut fixture = Fixture::new();
    fixture.config.read_lanes = 1;
    fixture.config.read_cache_bytes = 0;
    fixture.config.max_pending_reads = 4;
    let mut db = fixture.open();
    db.write_batch(
        fixture.operation(0, 0),
        vec![put(0, 7), put(1, 8), put(2, 9), put(3, 10)],
    )
    .unwrap();
    let lane = db.reads.lane_client(0, 0);
    let held = lane.lock();
    let mut pending = Vec::new();
    for key in 0u32..4 {
        pending.push(db.submit_get(KeySpace(0), &key.to_be_bytes()).unwrap());
    }
    assert_eq!(db.reads.state.retained.lock().0, 4);
    // An identical immutable miss joins even when all physical slots are full.
    let duplicate = db.submit_get(KeySpace(0), &0u32.to_be_bytes()).unwrap();
    assert_eq!(db.reads.state.retained.lock().0, 4);
    assert!(!db.reads.failed());
    drop(held);
    assert_eq!(db.finish_get(duplicate).unwrap(), value(7));
    // Caller consumption order is independent of transport completion order.
    for (key, request) in pending.into_iter().enumerate().rev() {
        assert_eq!(db.finish_get(request).unwrap(), value(key as u8 + 7));
    }
    assert!(db.reads.state.peak_connection_reads.load(Ordering::Relaxed) >= 2);
    assert_eq!(*db.reads.state.retained.lock(), (0, 0));
    assert!(!db.reads.failed());
    db.shutdown().unwrap();
}

#[test]
fn pending_read_keeps_submission_reference_across_overwrite_and_delete() {
    let fixture = Fixture::new();
    let mut db = fixture.open();
    db.write_batch(fixture.operation(0, 0), vec![put(0, 7)])
        .unwrap();
    let lane = db.reads.lane_client(0, 0);
    let held = lane.lock();
    let old = db.submit_get(KeySpace(0), &0u32.to_be_bytes()).unwrap();
    // Writes have separate authenticated lanes and do not wait for the reader.
    db.write_batch(fixture.operation(0, 1), vec![put(0, 8)])
        .unwrap();
    db.write_batch(
        fixture.operation(0, 2),
        vec![Mutation::Delete {
            keyspace: KeySpace(0),
            key: 0u32.to_be_bytes().to_vec().into(),
        }],
    )
    .unwrap();
    assert_eq!(db.get(KeySpace(0), &0u32.to_be_bytes()).unwrap(), None);
    drop(held);
    assert_eq!(db.finish_get(old).unwrap(), value(7));
    assert_eq!(db.get(KeySpace(0), &0u32.to_be_bytes()).unwrap(), None);
    db.shutdown().unwrap();
}

#[test]
fn dropped_handle_retains_worker_credit_and_shutdown_joins_before_release() {
    let mut fixture = Fixture::new();
    fixture.config.max_pending_reads = 1;
    fixture.config.read_lanes = 1;
    let mut db = fixture.open();
    db.write_batch(fixture.operation(0, 0), vec![put(0, 7), put(1, 8)])
        .unwrap();
    let mut db = reopen_cold(&fixture, db);
    let lane = db.reads.lane_client(node_for(&db, 0), 0);
    let held = lane.lock();
    let first = db.submit_get(KeySpace(0), &0u32.to_be_bytes()).unwrap();
    drop(first);
    assert_eq!(db.reads.state.retained.lock().0, 1);
    let error = db
        .submit_get(KeySpace(0), &1u32.to_be_bytes())
        .err()
        .unwrap();
    assert!(RemoteDatabase::read_backpressured(&error));
    assert!(!db.reads.failed());
    let state = db.reads.state.clone();
    let (done, completed) = mpsc::sync_channel(1);
    let closer = thread::spawn(move || {
        let result = db.shutdown();
        done.send(result).unwrap();
    });
    assert!(matches!(
        completed.try_recv(),
        Err(mpsc::TryRecvError::Empty)
    ));
    drop(held);
    completed
        .recv_timeout(Duration::from_secs(10))
        .unwrap()
        .unwrap();
    closer.join().unwrap();
    assert_eq!(*state.retained.lock(), (0, 0));
    // The native authority lock is gone only after lane shutdown finishes.
    let mut reopened = fixture.open();
    assert_eq!(
        reopened.get(KeySpace(0), &1u32.to_be_bytes()).unwrap(),
        value(8)
    );
    reopened.shutdown().unwrap();
}

#[test]
fn byte_admission_counts_completed_but_unconsumed_results() {
    let fixture = Fixture::new();
    let mut db = fixture.open();
    db.write_batch(fixture.operation(0, 0), vec![put(0, 7), put(1, 8)])
        .unwrap();
    let mut db = reopen_cold(&fixture, db);
    let first = db.submit_get(KeySpace(0), &0u32.to_be_bytes()).unwrap();
    if let GetState::Pending { read, .. } = &first.state {
        // Observe the real completion without consuming caller-owned credits.
        let slot = read.parts[0].0.slot;
        let completion = &read.parts[0].0.completion;
        let deadline = Instant::now() + Duration::from_secs(5);
        assert!(db.reads.state.flight_ready[slot].wait_until(deadline));
        assert!(completion.result.lock().is_some());
    } else {
        panic!("expected miss");
    }
    let charge = db.reads.state.retained.lock().1;
    Arc::get_mut(&mut db.reads).unwrap().max_bytes = charge;
    let error = db
        .submit_get(KeySpace(0), &1u32.to_be_bytes())
        .err()
        .unwrap();
    assert!(RemoteDatabase::read_backpressured(&error));
    assert_eq!(db.finish_get(first).unwrap(), value(7));
    Arc::get_mut(&mut db.reads).unwrap().max_bytes = charge - 1;
    assert!(db.submit_get(KeySpace(0), &1u32.to_be_bytes()).is_err());
    assert_eq!(*db.reads.state.retained.lock(), (0, 0));
    assert!(!db.reads.failed());
    Arc::get_mut(&mut db.reads).unwrap().max_bytes = charge;
    let second = db.submit_get(KeySpace(0), &1u32.to_be_bytes()).unwrap();
    assert_eq!(db.finish_get(second).unwrap(), value(8));
    assert_eq!(*db.reads.state.retained.lock(), (0, 0));
    db.shutdown().unwrap();
}

#[test]
fn published_result_stays_unavailable_until_credit_retirement() {
    let fixture = Fixture::new();
    let mut db = fixture.open();
    let state = db.reads.state.clone();
    let credit = Arc::new(Credit {
        state: state.clone(),
        bytes: 512,
        requests: 1,
    });
    *state.retained.lock() = (1, 512);
    state.flight_ready[0].begin();
    let flight = Arc::new(ReadFlight {
        node: 0,
        references: Vec::new(),
        slot: 0,
        completion: Arc::new(FlightCompletion::new()),
        _pin: None,
        _credit: credit.clone(),
    });
    *flight.completion.result.lock() = Some(Ok((vec![put(0, 7)], None)));
    let mut parts = SmallVec::new();
    parts.push((flight, 0));
    let read = ReadRequest {
        parts,
        credit: credit.clone(),
        deadline: Instant::now() + Duration::from_secs(5),
        sampled: false,
        profile: None,
    };
    drop(credit);

    assert!(read.try_result().unwrap().is_none());
    assert_eq!(*state.retained.lock(), (1, 512));

    let deadline = read.deadline;
    let waiter = thread::spawn(move || read.finish());
    while state.flight_ready[0].waiters.load(Ordering::Acquire) == 0 && Instant::now() < deadline {
        thread::yield_now();
    }
    assert_eq!(state.flight_ready[0].waiters.load(Ordering::Acquire), 1);
    assert_eq!(*state.retained.lock(), (1, 512));

    state.flight_ready[0].retire();
    let (values, _) = waiter.join().unwrap().unwrap();
    assert_eq!(values, vec![put(0, 7)]);
    assert_eq!(*state.retained.lock(), (0, 0));
    db.shutdown().unwrap();
}

#[test]
fn subset_joiner_keeps_cancelled_initiators_full_result_credit() {
    let mut fixture = Fixture::new();
    fixture.config.read_cache_bytes = 0;
    fixture.config.read_lanes = 1;
    let mut db = fixture.open();
    db.write_batch(fixture.operation(0, 0), vec![put(0, 7), put(1, 8)])
        .unwrap();
    let mut db = reopen_cold(&fixture, db);
    let first = db
        .index_reference(KeySpace(0), &0u32.to_be_bytes(), IndexReadCut::Latest)
        .unwrap()
        .unwrap();
    let second = db
        .index_reference(KeySpace(0), &1u32.to_be_bytes(), IndexReadCut::Latest)
        .unwrap()
        .unwrap();
    let node = node_for(&db, 0);
    let lane = db.reads.lane_client(node, 0);
    let held = lane.lock();

    let initiator = db
        .reads
        .submit(node, vec![first, second], 0, false, None)
        .unwrap();
    let initiator_charge = initiator.credit.bytes;
    drop(initiator);
    let joiner = db.reads.submit(node, vec![first], 0, false, None).unwrap();
    let retained_after_join = initiator_charge + joiner.credit.bytes;
    let slot = joiner.parts[0].0.slot;
    drop(held);

    let deadline = Instant::now() + Duration::from_secs(5);
    assert!(db.reads.state.flight_ready[slot].wait_until(deadline));
    let retained = *db.reads.state.retained.lock();
    assert_eq!(retained.0, 1);
    assert_eq!(retained.1, retained_after_join);
    let (values, _) = joiner.finish().unwrap();
    assert_eq!(values, vec![put(0, 7)]);
    assert_eq!(*db.reads.state.retained.lock(), (0, 0));
    db.shutdown().unwrap();
}

#[test]
fn dropped_failed_read_poison_cannot_be_hidden_by_a_cached_hit() {
    let mut fixture = Fixture::new();
    fixture.config.read_lanes = 1;
    let mut db = fixture.open();
    db.write_batch(fixture.operation(0, 0), vec![put(0, 7)])
        .unwrap();
    db.write_batch(fixture.operation(1, 1), vec![put(1, 8)])
        .unwrap();
    let mut db = reopen_cold(&fixture, db);
    db.get(KeySpace(0), &1u32.to_be_bytes()).unwrap();
    db.set_read_profile_limit(1);
    let profile = db.read_profile_monitor();
    let node = node_for(&db, 0);
    let lane = db.reads.lane_client(node, 0);
    let held = lane.lock();
    let pending = db.submit_get(KeySpace(0), &0u32.to_be_bytes()).unwrap();
    drop(pending);
    db.clients[node]
        .client
        .lock()
        .fence_to(db.epoch + 1)
        .unwrap();
    drop(held);
    db.reads.stop(); // deterministic completion barrier, including discarded replies
    assert!(db.reads.failed());
    assert_eq!(*db.reads.state.retained.lock(), (0, 0));
    let mut counts = std::collections::BTreeMap::new();
    profile.visit(&mut |name, value| {
        counts.insert(name, value);
    });
    assert_eq!(counts["read_profile_selected"], 1);
    assert_eq!(counts["read_profile_completed"], 1);
    assert_eq!(counts["read_profile_errors"], 1);
    assert_eq!(counts["read_profile_returned"], 0);
    assert_eq!(counts["read_profile_server_response_recheck_samples"], 0);
    assert_eq!(counts["read_profile_server_available"], 0);
    let hits = db.read_cache_stats().hits;
    assert!(db.get(KeySpace(0), &1u32.to_be_bytes()).is_err());
    assert_eq!(db.read_cache_stats().hits, hits);
    assert!(db.shutdown().is_err());
}

#[test]
fn expired_queue_poison_and_foreign_epoch_handles_are_rejected() {
    let fixture = Fixture::new();
    let mut db = fixture.open();
    db.write_batch(fixture.operation(0, 0), vec![put(0, 7)])
        .unwrap();
    let mut db = reopen_cold(&fixture, db);
    let lane = db.reads.lane_client(node_for(&db, 0), 0);
    let held = lane.lock();
    let mut pending = db.submit_get(KeySpace(0), &0u32.to_be_bytes()).unwrap();
    if let GetState::Pending { read, .. } = &mut pending.state {
        read.deadline = Instant::now() - Duration::from_secs(1);
    } else {
        panic!("expected miss");
    }
    assert!(db.poll_get(&mut pending).is_err());
    assert!(db.reads.failed());
    drop(held);
    drop(pending);
    assert!(db.shutdown().is_err());
    drop(db);
    let mut db = fixture.open();
    let old = db.submit_get(KeySpace(0), &999u32.to_be_bytes()).unwrap();
    db.shutdown().unwrap();
    drop(db);
    let mut reopened = fixture.open();
    assert!(reopened.finish_get(old).is_err());
    assert_eq!(
        reopened.get(KeySpace(0), &0u32.to_be_bytes()).unwrap(),
        value(7)
    );
    reopened.shutdown().unwrap();
}

#[test]
fn detached_completion_rechecks_authority_after_wait() {
    let fixture = Fixture::new();
    let mut db = fixture.open();
    db.write_batch(fixture.operation(0, 0), vec![put(1, 7)])
        .unwrap();
    let completed = db
        .submit_get(KeySpace(0), &1u32.to_be_bytes())
        .unwrap()
        .wait();
    db.healthy = false;
    assert!(db.finish_ready_get(completed).is_err());
}

#[test]
fn detached_scan_index_mismatch_poisons_cached_reads() {
    let fixture = Fixture::new();
    let mut db = fixture.open();
    db.write_batch(fixture.operation(0, 0), vec![put(1, 7)])
        .unwrap();
    assert_eq!(db.get(KeySpace(0), &1u32.to_be_bytes()).unwrap(), value(7));
    let reference = db
        .index_reference(KeySpace(0), &1u32.to_be_bytes(), IndexReadCut::Latest)
        .unwrap()
        .unwrap();
    let completed = db.submit_record(reference).unwrap().wait();
    assert!(
        db.finish_record(completed, KeySpace(0), &2u32.to_be_bytes())
            .is_err()
    );
    assert!(db.get(KeySpace(0), &1u32.to_be_bytes()).is_err());
}
