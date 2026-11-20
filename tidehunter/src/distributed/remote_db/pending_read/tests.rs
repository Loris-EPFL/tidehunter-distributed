//! Reads over the leased live fixture, so the module needs nvmeof.
use super::*;
use crate::distributed::remote_db::pipeline_tests::Fixture;

pub(super) fn put(key: u32, value: u8) -> Mutation {
    Mutation::Put {
        keyspace: KeySpace(0),
        key: key.to_be_bytes().to_vec().into(),
        value: vec![value; 32].into(),
    }
}

pub(super) fn reopen_cold(fixture: &Fixture, db: RemoteDatabase) -> RemoteDatabase {
    db.shutdown().unwrap();
    drop(db);
    fixture.open()
}

pub(super) fn node_for(db: &RemoteDatabase, key: u32) -> usize {
    let reference = db
        .index_reference(KeySpace(0), &key.to_be_bytes(), IndexReadCut::Latest)
        .unwrap()
        .unwrap();
    db.core.partition_indexes[&reference.batch_address.partition]
}
pub(super) fn value(byte: u8) -> Option<Bytes> {
    Some(vec![byte; 32].into())
}

#[test]
fn batch_signal_registers_only_pending_flights_and_notifies_target() {
    let first = FlightSignal::new();
    let second = FlightSignal::new();
    let a = Arc::new(BatchSignal::new());
    let b = Arc::new(BatchSignal::new());
    first.begin();
    second.begin();
    assert!(first.subscribe(&a));
    assert!(second.subscribe(&b));
    first.retire();
    assert_eq!(a.notifications.load(Ordering::Relaxed), 1);
    assert_eq!(b.notifications.load(Ordering::Relaxed), 0);
    assert!(
        !first.subscribe(&b),
        "a retired flight needs no subscription"
    );
    second.retire();
    assert_eq!(b.notifications.load(Ordering::Relaxed), 1);
    assert!(first.wait_lock.lock().is_empty());
    assert!(second.wait_lock.lock().is_empty());
}

#[test]
fn flight_retirement_cannot_publish_reusable_slot_before_subscription_drain() {
    let flight = Arc::new(FlightSignal::new());
    let old = Arc::new(BatchSignal::new());
    flight.begin();
    assert!(flight.subscribe(&old));
    let held = flight.wait_lock.lock();
    let retiring = flight.clone();
    let worker = thread::spawn(move || retiring.retire());
    let deadline = Instant::now() + Duration::from_secs(5);
    while flight.retiring.load(Ordering::Acquire) == 0 {
        assert!(
            Instant::now() < deadline,
            "retirement did not reach the held lock"
        );
        thread::yield_now();
    }
    assert!(
        !flight.is_ready(),
        "the old generation became reusable before its drain"
    );
    drop(held);
    worker.join().unwrap();
    assert_eq!(old.notifications.load(Ordering::Relaxed), 1);
    flight.begin();
    let next = Arc::new(BatchSignal::new());
    assert!(flight.subscribe(&next));
    flight.retire();
    assert_eq!(next.notifications.load(Ordering::Relaxed), 1);
}

#[test]
fn cancelled_coalesced_batch_subscriptions_do_not_accumulate() {
    let mut fixture = Fixture::new();
    fixture.config.read_lanes = Some(1);
    fixture.config.read_cache_bytes = 0;
    let db = fixture.open();
    db.write_batch(fixture.operation(0, 0), vec![put(0, 7)])
        .unwrap();
    let reference = db
        .index_reference(KeySpace(0), &0u32.to_be_bytes(), IndexReadCut::Latest)
        .unwrap()
        .unwrap();
    let node = db.core.partition_indexes[&reference.batch_address.partition];
    let lane = db.core.reads.lane_client(node, 0);
    let client = lane.lock();
    let mut groups = vec![Vec::new(); db.core.partition_indexes.len()];
    groups[node].push((0, reference));
    let mut original = ReadBatch::new(&db.core.reads, vec![None], groups).unwrap();
    original.dispatch(&db.core.reads).unwrap();
    let slot = original.reads[0].1.parts[0].0.slot;
    for _ in 0..128 {
        let mut groups = vec![Vec::new(); db.core.partition_indexes.len()];
        groups[node].extend([(0, reference), (1, reference)]);
        let mut joined = ReadBatch::new(&db.core.reads, vec![None; 2], groups).unwrap();
        joined.dispatch(&db.core.reads).unwrap();
        assert_eq!(
            db.core.reads.state.flight_ready[slot]
                .wait_lock
                .lock()
                .len(),
            2
        );
        drop(joined);
        let subscribers = db.core.reads.state.flight_ready[slot].wait_lock.lock();
        assert_eq!(subscribers.len(), 1);
        assert!(subscribers.capacity() <= 4);
    }
    drop(client);
    original.finish_one().unwrap();
    assert_eq!(original.values, vec![Some(put(0, 7))]);
    assert_eq!(*db.core.reads.state.retained.lock(), (0, 0));
    db.shutdown().unwrap();
}

#[test]
fn unrelated_flight_completion_does_not_notify_waiting_batch() {
    let mut fixture = Fixture::new();
    fixture.config.read_lanes = Some(1);
    fixture.config.read_cache_bytes = 0;
    let db = fixture.open();
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
    let first_node = db.core.partition_indexes[&first.batch_address.partition];
    let second_node = db.core.partition_indexes[&second.batch_address.partition];
    assert_ne!(first_node, second_node);
    let lane = db.core.reads.lane_client(first_node, 0);
    let client = lane.lock();
    let mut groups = vec![Vec::new(); db.core.partition_indexes.len()];
    groups[first_node].push((0, first));
    let mut batch = ReadBatch::new(&db.core.reads, vec![None], groups).unwrap();
    batch.dispatch(&db.core.reads).unwrap();
    let signal = batch.signal.as_ref().unwrap().clone();
    let (done, result) = mpsc::sync_channel(1);
    let waiter = thread::spawn(move || {
        batch.finish_one().unwrap();
        done.send(batch.values).unwrap();
    });
    let deadline = Instant::now() + Duration::from_secs(5);
    while signal.waiters.load(Ordering::Acquire) == 0 {
        assert!(Instant::now() < deadline);
        thread::yield_now();
    }
    let other = db
        .core
        .reads
        .submit(second_node, vec![second], 0, false, None)
        .unwrap();
    assert_eq!(other.finish().unwrap(), vec![put(1, 8)]);
    assert_eq!(signal.notifications.load(Ordering::Relaxed), 0);
    assert!(matches!(result.try_recv(), Err(mpsc::TryRecvError::Empty)));
    drop(client);
    assert_eq!(
        result.recv_timeout(Duration::from_secs(5)).unwrap(),
        vec![Some(put(0, 7))]
    );
    waiter.join().unwrap();
    assert_eq!(*db.core.reads.state.retained.lock(), (0, 0));
    db.shutdown().unwrap();
}

#[test]
fn poison_and_stop_wake_waiting_batch_without_forging_readiness() {
    for stop in [false, true] {
        let mut fixture = Fixture::new();
        fixture.config.read_lanes = Some(1);
        fixture.config.read_cache_bytes = 0;
        let db = fixture.open();
        db.write_batch(fixture.operation(0, 0), vec![put(0, 7)])
            .unwrap();
        let reference = db
            .index_reference(KeySpace(0), &0u32.to_be_bytes(), IndexReadCut::Latest)
            .unwrap()
            .unwrap();
        let node = db.core.partition_indexes[&reference.batch_address.partition];
        let lane = db.core.reads.lane_client(node, 0);
        let client = lane.lock();
        let mut groups = vec![Vec::new(); db.core.partition_indexes.len()];
        groups[node].push((0, reference));
        let mut batch = ReadBatch::new(&db.core.reads, vec![None], groups).unwrap();
        batch.dispatch(&db.core.reads).unwrap();
        let signal = batch.signal.as_ref().unwrap().clone();
        let slot = batch.reads[0].1.parts[0].0.slot;
        let (done, result) = mpsc::sync_channel(1);
        let waiter = thread::spawn(move || done.send(batch.finish_one()).unwrap());
        let deadline = Instant::now() + Duration::from_secs(5);
        while signal.waiters.load(Ordering::Acquire) == 0 {
            assert!(Instant::now() < deadline);
            thread::yield_now();
        }
        let stopper = if stop {
            let reads = db.core.reads.clone();
            Some(thread::spawn(move || reads.stop()))
        } else {
            db.core.reads.state.poison();
            None
        };
        assert!(
            result
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
                .is_err()
        );
        waiter.join().unwrap();
        if stop {
            assert!(!db.core.reads.state.flight_ready[slot].is_ready());
        }
        drop(client);
        if let Some(stopper) = stopper {
            stopper.join().unwrap();
        }
        let deadline = Instant::now() + Duration::from_secs(5);
        while *db.core.reads.state.retained.lock() != (0, 0) {
            assert!(Instant::now() < deadline, "accepted work did not retire");
            thread::yield_now();
        }
        let _ = db.shutdown();
    }
}

#[test]
fn nvme_profile_counts_completion_without_inventing_owner_timing() {
    let diagnostics = ReadDiagnostics::default();
    diagnostics.nvme_complete(Some(41), false);
    diagnostics.nvme_complete(None, true);
    assert_eq!(diagnostics.completed.load(Ordering::Relaxed), 2);
    assert_eq!(diagnostics.nvme_completed.load(Ordering::Relaxed), 2);
    assert_eq!(diagnostics.nvme_flight_count.load(Ordering::Relaxed), 1);
    assert_eq!(diagnostics.nvme_flight_ns.load(Ordering::Relaxed), 41);
    assert_eq!(diagnostics.nvme_flight_max_ns.load(Ordering::Relaxed), 41);
    assert_eq!(diagnostics.errors.load(Ordering::Relaxed), 1);
    assert_eq!(
        diagnostics
            .failed_owner_profile_not_applicable
            .load(Ordering::Relaxed),
        1
    );
}

#[test]
fn nonconcurrent_bulk_records_pin_lock_separately() {
    let mut fixture = Fixture::new();
    fixture.config.read_cache_bytes = 0;
    let mut db = fixture.open();
    db.write_batch(fixture.operation(0, 0), vec![put(0, 7)])
        .unwrap();
    db.set_read_profile_limit(1);
    let monitor = db.read_profile_monitor();
    let values = db
        .get_many(KeySpace(0), &[0u32.to_be_bytes().to_vec()])
        .unwrap();
    assert_eq!(values[0], value(7));
    let snapshot = monitor.coordinator_resolution_snapshot();
    // The index has no single lock to wait for: cut capture, member lookup and
    // pinning are separate calls and none holds anything across the others.
    assert_eq!(snapshot.index_lock_wait.count, 0);
    assert_eq!(snapshot.index_lookup.count, 1);
    assert_eq!(snapshot.cut_capture.count, 1);
    assert_eq!(snapshot.pin_preparation.count, 1);
    db.shutdown().unwrap();
}

#[test]
fn concurrent_batch_disabled_cache_counts_each_member_and_profiles_completed_stages() {
    let mut fixture = Fixture::new();
    fixture.config.read_cache_bytes = 0;
    let original = fixture.open();
    original
        .write_batch(fixture.operation(0, 0), vec![put(0, 7), put(1, 8)])
        .unwrap();
    let db = ConcurrentRemoteDatabase::new(original).unwrap();
    let before = db.read_cache_stats().bypass_disabled;
    db.set_read_profile_limit(1);
    let monitor = db.read_profile_monitor();
    let keys: Vec<_> = (0u32..70).map(|i| (i % 2).to_be_bytes()).collect();
    let values = db
        .get_many_cut(KeySpace(0), &keys, IndexReadCut::Latest)
        .unwrap();
    for (i, value) in values.iter().enumerate() {
        assert_eq!(
            value.as_ref().unwrap().as_deref(),
            Some([if i % 2 == 0 { 7 } else { 8 }; 32].as_slice())
        );
    }
    assert_eq!(db.read_cache_stats().bypass_disabled - before, 70);
    let snapshot = monitor.coordinator_resolution_snapshot();
    assert_eq!(snapshot.batch_wait.count, 1);
    assert_eq!(snapshot.postread_cache_fill.count, 1);
    assert_eq!(snapshot.result_materialization.count, 1);
    db.shutdown().unwrap();
}

#[test]
fn sampled_owner_profiles_are_attributed_to_configured_partitions() {
    let mut fixture = Fixture::new();
    fixture.config.read_cache_bytes = 0;
    let mut db = fixture.open();
    db.write_batch(fixture.operation(0, 0), vec![put(0, 7)])
        .unwrap();
    db.write_batch(fixture.operation(1, 1), vec![put(1, 8)])
        .unwrap();
    db.set_read_profile_limit(2);
    let monitor = db.read_profile_monitor();
    assert_eq!(db.get(KeySpace(0), &0u32.to_be_bytes()).unwrap(), value(7));
    assert_eq!(db.get(KeySpace(0), &1u32.to_be_bytes()).unwrap(), value(8));
    let mut global = std::collections::BTreeMap::new();
    monitor.visit(&mut |name, value| {
        global.insert(name, value);
    });
    let mut owners = std::collections::BTreeMap::new();
    monitor.visit_partitions(&mut |partition, name, value| {
        owners.insert((partition, name), value);
    });
    for partition in [0, 1] {
        assert_eq!(owners[&(partition, "read_profile_completed")], 1);
    }
    assert_eq!(
        owners
            .iter()
            .filter(|((_, key), _)| *key == "read_profile_completed")
            .map(|(_, value)| *value)
            .sum::<u64>(),
        global["read_profile_completed"]
    );
    db.shutdown().unwrap();
}

#[test]
fn traced_bulk_call_preserves_owner_and_member_identity() {
    let mut fixture = Fixture::new();
    fixture.config.read_cache_bytes = 0;
    let original = fixture.open();
    original
        .write_batch(fixture.operation(0, 0), vec![put(0, 7)])
        .unwrap();
    original
        .write_batch(fixture.operation(1, 1), vec![put(1, 8)])
        .unwrap();
    let original = reopen_cold(&fixture, original);
    let db = ConcurrentRemoteDatabase::new(original).unwrap();
    db.set_read_profile_limit(1);
    let trace = db.begin_read_trace(123, 3).unwrap();
    // Tracing is best effort: a record (call status, group, or event) that
    // meets a held trace lock is shed and counted in `contention_drops`
    // (read_trace.rs `update`), one count per lost record. The two groups'
    // I/O completions record events from owner threads while this thread is
    // still creating the second group, so any record can be shed. Measure the
    // sheds of this call alone and require every missing record to be
    // covered by a counted shed. Extra sheds may hit records this test does
    // not assert (lane, classification, intermediate events), so the bound
    // is `lost <= sheds`, not equality.
    let sheds_before = db.read_trace_snapshot().contention_drops;
    let keys = [0u32.to_be_bytes(), 1u32.to_be_bytes(), 0u32.to_be_bytes()];
    let values = db
        .traced_get_many_cut(KeySpace(0), &keys, IndexReadCut::Latest, Some(&trace), 0)
        .unwrap();
    assert!(values.iter().all(|value| value.as_ref().unwrap().is_some()));
    trace.finish(true);
    let snapshot = db.read_trace_snapshot();
    let sheds = snapshot.contention_drops - sheds_before;
    assert_eq!(snapshot.calls.len(), 1);
    let call = &snapshot.calls[0];
    assert_eq!((call.caller_id, call.members), (123, 3));
    assert_eq!(call.dropped_children, 0);
    // A shed `finish` leaves the call "incomplete"; nothing else may.
    let status_lost = u64::from(call.status != "complete");
    assert!(
        call.status == "complete" || call.status == "incomplete",
        "unexpected call status {}",
        call.status
    );
    let shared_group = call.groups.iter().find(|group| group.members == [0, 2]);
    let single_group = call.groups.iter().find(|group| group.members == [1]);
    let groups_lost = u64::from(shared_group.is_none()) + u64::from(single_group.is_none());
    assert_eq!(call.groups.len() as u64 + groups_lost, 2);
    if let (Some(first), Some(second)) = (shared_group, single_group) {
        assert_ne!(first.owner, second.owner);
    }
    let deliveries_lost = call
        .groups
        .iter()
        .filter(|group| {
            !group
                .events
                .iter()
                .any(|event| event.name == "request_result_delivered")
        })
        .count() as u64;
    let lost = status_lost + groups_lost + deliveries_lost;
    assert!(
        lost <= sheds,
        "{lost} records missing (status {status_lost}, groups {groups_lost}, \
         deliveries {deliveries_lost}) with {sheds} counted sheds"
    );
    db.shutdown().unwrap();
}

#[test]
fn bounded_read_profiles_count_misses_not_hits_absences_or_backpressure() {
    let mut fixture = Fixture::new();
    fixture.config.max_pending_reads = 1;
    let db = fixture.open();
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

    let lane = db.core.reads.lane_client(node_for(&db, 0), 0);
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
    assert_eq!(db.live_read_submission_count(), 3);
    assert_eq!(count("read_profile_selected"), 2);
    assert_eq!(count("read_profile_completed"), 2);
    assert_eq!(count("read_profile_errors"), 0);
    assert_eq!(*db.core.reads.state.retained.lock(), (0, 0));
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
        let singleton_charge = db.core.reads.state.retained.lock().1;
        db.finish_get(pending).unwrap();
        // One member fits, but a same-shard group does not. Synchronous reads
        // must split into bounded waves rather than reject a legal collection.
        db.core
            .reads
            .set_limits_for_test(Some(singleton_charge), None);
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
    assert!(db.ensure_healthy().is_ok());
    assert!(!db.core.reads.failed());
    assert_eq!(*db.core.reads.state.retained.lock(), (0, 0));
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
    let lane = db.core.reads.lane_client(0, 0);
    let held = lane.lock();
    let pending = db.submit_get(KeySpace(0), &0u32.to_be_bytes()).unwrap();
    let keys = vec![1u32.to_be_bytes().to_vec()];
    let rpcs_before_rejection = db.live_read_submission_count();
    assert!(RemoteDatabase::read_backpressured(
        &db.get_many(KeySpace(0), &keys).unwrap_err()
    ));
    assert_eq!(db.live_read_submission_count(), rpcs_before_rejection);
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
    assert!(db.ensure_healthy().is_ok());
    assert!(!db.core.reads.failed());
    assert_eq!(db.finish_get(pending).unwrap(), value(7));
    assert_eq!(db.get_many(KeySpace(0), &keys).unwrap(), vec![value(8)]);
    assert_eq!(db.scan(KeySpace(0)).unwrap().len(), 2);
    db.shutdown().unwrap();
}

#[test]
fn completion_is_not_visible_before_actor_releases_its_credit() {
    let mut fixture = Fixture::new();
    fixture.config.max_pending_reads = 1;
    fixture.config.read_cache_bytes = 0;
    let mut db = fixture.open();
    db.write_batch(fixture.operation(0, 0), vec![put(0, 7)])
        .unwrap();
    db.write_batch(fixture.operation(1, 1), vec![put(1, 8)])
        .unwrap();
    let (entered, waiting) = mpsc::sync_channel(1);
    let (release, resumed) = mpsc::sync_channel(1);
    *db.core.reads.state.completion_pause.lock() = Some((entered, resumed));
    let mut pending = db.submit_get(KeySpace(0), &0u32.to_be_bytes()).unwrap();
    waiting.recv_timeout(Duration::from_secs(5)).unwrap();

    let observed = db.poll_get(&mut pending);
    let rejected = db.submit_get(KeySpace(0), &1u32.to_be_bytes());
    let rpcs_after_rejection = db.live_read_submission_count();
    release.send(()).unwrap();

    assert_eq!(observed.unwrap(), None);
    assert!(RemoteDatabase::read_backpressured(&rejected.err().unwrap()));
    assert_eq!(rpcs_after_rejection, 1);
    assert_eq!(db.finish_get(pending).unwrap(), value(7));
    assert_eq!(
        db.get_many(KeySpace(0), &[1u32.to_be_bytes().to_vec()])
            .unwrap(),
        vec![value(8)]
    );
    assert_eq!(db.scan(KeySpace(0)).unwrap().len(), 2);
    db.shutdown().unwrap();
}

#[test]
fn scan_releases_each_completed_wave_before_admitting_the_next() {
    let mut fixture = Fixture::new();
    fixture.config.max_pending_reads = 1;
    fixture.config.read_cache_bytes = 0;
    let mut db = fixture.open();
    db.write_batch(fixture.operation(0, 0), vec![put(0, 7)])
        .unwrap();
    db.write_batch(fixture.operation(1, 1), vec![put(1, 8)])
        .unwrap();
    assert_eq!(
        db.get_many(KeySpace(0), &[1u32.to_be_bytes().to_vec()])
            .unwrap(),
        vec![value(8)]
    );
    let (entered, waiting) = mpsc::sync_channel(1);
    let (release, resumed) = mpsc::sync_channel(1);
    *db.core.reads.state.completion_pause.lock() = Some((entered, resumed));
    let (done, result) = mpsc::sync_channel(1);
    let scan = thread::spawn(move || {
        let rows = db.scan(KeySpace(0));
        done.send((db, rows)).unwrap();
    });
    waiting.recv_timeout(Duration::from_secs(5)).unwrap();
    let early = result.try_recv();
    release.send(()).unwrap();
    let (db, rows) = result.recv_timeout(Duration::from_secs(5)).unwrap();
    scan.join().unwrap();
    assert!(matches!(early, Err(mpsc::TryRecvError::Empty)));
    assert_eq!(rows.unwrap().len(), 2);
    db.shutdown().unwrap();
}

#[test]
fn byte_bounded_reads_accept_request_limits_above_channel_capacity() {
    let mut fixture = Fixture::new();
    fixture.config.max_pending_reads = usize::MAX;
    fixture.config.max_pending_read_bytes = ReadService::MIN_FLIGHT_BYTES + 4096;
    fixture.config.read_cache_bytes = 0;
    let db = fixture.open();
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
    assert_eq!(*db.core.reads.state.retained.lock(), (0, 0));
    db.shutdown().unwrap();
}

#[test]
fn oversized_scalar_read_does_not_poison_write_authority() {
    let mut fixture = Fixture::new();
    fixture.config.max_pending_read_bytes = ReadService::MIN_FLIGHT_BYTES;
    let db = fixture.open();
    db.write_batch(
        fixture.operation(0, 0),
        vec![
            put(0, 7),
            Mutation::Put {
                keyspace: KeySpace(0),
                key: 1u32.to_be_bytes().to_vec().into(),
                value: vec![8; 1024].into(),
            },
        ],
    )
    .unwrap();
    let mut db = reopen_cold(&fixture, db);
    assert_eq!(db.get(KeySpace(0), &0u32.to_be_bytes()).unwrap(), value(7));
    assert!(admission_error(
        &db.get(KeySpace(0), &1u32.to_be_bytes()).unwrap_err()
    ));
    db.write_batch(fixture.operation(1, 1), vec![put(2, 8)])
        .unwrap();
    assert!(db.exists(KeySpace(0), &2u32.to_be_bytes()).unwrap());
    db.shutdown().unwrap();
}

#[test]
fn sui_bulk_accepts_borrowed_keys_with_duplicates() {
    let fixture = Fixture::new();
    let original = fixture.open();
    original
        .write_batch(fixture.operation(0, 0), vec![put(0, 7)])
        .unwrap();
    original.shutdown().unwrap();
    drop(original);
    let db = crate::distributed::sui::Db::open_remote_local_fixture(
        &fixture.temp.path().join("authority"),
        fixture.shape.clone(),
        fixture.config.clone(),
    )
    .unwrap();
    let key = 0u32.to_be_bytes();
    let absent = 99u32.to_be_bytes();
    let keys: [&[u8]; 3] = [&key, &absent, &key];
    let results = db.multi_get(db.ks("root"), &keys);
    assert_eq!(results[0].as_ref().unwrap(), &value(7));
    assert_eq!(results[1].as_ref().unwrap(), &None);
    assert_eq!(results[2].as_ref().unwrap(), &value(7));
    db.wait_for_background_threads_to_finish().unwrap();
}

#[test]
fn cancelled_batch_keeps_accepted_flight_until_completion() {
    let mut fixture = Fixture::new();
    fixture.config.read_lanes = Some(1);
    fixture.config.read_cache_bytes = 0;
    let mut db = fixture.open();
    db.write_batch(fixture.operation(0, 0), vec![put(0, 7)])
        .unwrap();
    let reference = db
        .index_reference(KeySpace(0), &0u32.to_be_bytes(), IndexReadCut::Latest)
        .unwrap()
        .unwrap();
    let node = db.core.partition_indexes[&reference.batch_address.partition];
    let lane = db.core.reads.lane_client(node, 0);
    let held = lane.lock();
    let mut groups = vec![Vec::new(); db.core.partition_indexes.len()];
    groups[node].extend([(0, reference), (1, reference)]);
    let mut batch = ReadBatch::new(&db.core.reads, vec![None; 2], groups).unwrap();
    db.core.reads.state.trace.arm(2);
    let trace = db.core.reads.state.trace.begin(91, 2).unwrap();
    batch
        .dispatch_traced(&db.core.reads, Some(&trace), 0)
        .unwrap();
    let slot = batch.reads[0].1.parts[0].0.slot;
    drop(batch);
    drop(trace);
    assert_eq!(db.core.reads.state.retained.lock().0, 1);
    drop(held);
    assert!(
        db.core.reads.state.flight_ready[slot].wait_until(Instant::now() + Duration::from_secs(5))
    );
    assert_eq!(*db.core.reads.state.retained.lock(), (0, 0));
    let snapshot = db.core.reads.state.trace.snapshot();
    let call = &snapshot.calls[0];
    assert_eq!(
        (call.caller_id, call.status),
        (91, "cancelled_or_incomplete")
    );
    assert!(
        call.groups[0]
            .events
            .iter()
            .any(|event| event.name == "flight_completed")
    );
    assert_eq!(call.groups[0].members, [0, 1]);
    assert_eq!(db.get(KeySpace(0), &0u32.to_be_bytes()).unwrap(), value(7));
    db.shutdown().unwrap();
}

#[test]
fn traced_fenced_read_records_failure_and_retires_transport() {
    let mut fixture = Fixture::new();
    fixture.config.read_lanes = Some(1);
    fixture.config.read_cache_bytes = 0;
    let db = fixture.open();
    db.write_batch(fixture.operation(0, 0), vec![put(0, 7)])
        .unwrap();
    let reference = db
        .index_reference(KeySpace(0), &0u32.to_be_bytes(), IndexReadCut::Latest)
        .unwrap()
        .unwrap();
    let node = db.core.partition_indexes[&reference.batch_address.partition];
    let lane = db.core.reads.lane_client(node, 0);
    let held = lane.lock();
    db.core.reads.state.trace.arm(1);
    let trace = db.core.reads.state.trace.begin(92, 1).unwrap();
    let mut groups = vec![Vec::new(); db.core.partition_indexes.len()];
    groups[node].push((0, reference));
    let mut batch = ReadBatch::new(&db.core.reads, vec![None], groups).unwrap();
    batch
        .dispatch_traced(&db.core.reads, Some(&trace), 0)
        .unwrap();
    let slot = batch.reads[0].1.parts[0].0.slot;
    fixture.fence_owner(node, db.epoch() + 1);
    drop(held);
    assert!(
        db.core.reads.state.flight_ready[slot].wait_until(Instant::now() + Duration::from_secs(5))
    );
    assert!(batch.finish_one().is_err());
    drop(batch);
    trace.finish(false);
    db.core.reads.stop();
    assert_eq!(*db.core.reads.state.retained.lock(), (0, 0));
    let snapshot = db.core.reads.state.trace.snapshot();
    let call = &snapshot.calls[0];
    assert_eq!((call.caller_id, call.status), (92, "error"));
    assert!(
        call.groups[0]
            .events
            .iter()
            .any(|event| event.name == "flight_error")
    );
    assert!(db.shutdown().is_err());
}

#[test]
fn batch_retires_ready_owner_before_blocked_owner_and_preserves_duplicates() {
    let mut fixture = Fixture::new();
    fixture.config.read_lanes = Some(1);
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
    let first_node = db.core.partition_indexes[&first.batch_address.partition];
    let second_node = db.core.partition_indexes[&second.batch_address.partition];
    assert!(first_node < second_node);
    db.set_read_profile_limit(2);
    let lane = db.core.reads.lane_client(first_node, 0);
    let held = lane.lock();
    let mut groups = vec![Vec::new(); db.core.partition_indexes.len()];
    groups[first_node].push((0, first));
    groups[second_node].extend([(1, second), (2, second)]);
    let mut batch = ReadBatch::new(&db.core.reads, vec![None; 3], groups).unwrap();
    batch.dispatch(&db.core.reads).unwrap();
    assert_eq!(batch.reads.len(), 2);
    batch.finish_one().unwrap();
    assert!(batch.values[0].is_none());
    assert!(
        matches!(&batch.values[1], Some(Mutation::Put { value, .. }) if value.as_ref() == [8; 32])
    );
    assert!(
        matches!(&batch.values[2], Some(Mutation::Put { value, .. }) if value.as_ref() == [8; 32])
    );
    assert_eq!(db.core.reads.state.retained.lock().0, 1);
    drop(held);
    batch.finish_one().unwrap();
    assert!(
        matches!(&batch.values[0], Some(Mutation::Put { value, .. }) if value.as_ref() == [7; 32])
    );
    assert!(batch.complete());
    assert_eq!(
        db.core
            .reads
            .state
            .diagnostics
            .completed
            .load(Ordering::Relaxed),
        2
    );
    assert_eq!(
        db.core
            .reads
            .state
            .diagnostics
            .inline_completed
            .load(Ordering::Relaxed)
            + db.core
                .reads
                .state
                .diagnostics
                .actor_completed
                .load(Ordering::Relaxed),
        2
    );
    for node in [first_node, second_node] {
        assert_eq!(
            db.core.reads.state.owner_diagnostics[node]
                .1
                .completed
                .load(Ordering::Relaxed),
            1
        );
    }
    assert_eq!(*db.core.reads.state.retained.lock(), (0, 0));
    db.shutdown().unwrap();
}

#[test]
fn blocked_owner_driver_does_not_block_another_owners_read() {
    let mut fixture = Fixture::new();
    fixture.config.read_lanes = Some(1);
    fixture.config.read_cache_bytes = 0;
    let db = fixture.open();
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
    let first_node = db.core.partition_indexes[&first.batch_address.partition];
    let second_node = db.core.partition_indexes[&second.batch_address.partition];
    assert_ne!(first_node, second_node);
    let lane = db.core.reads.lanes.read()[first_node][0].clone();
    let held = lane.driver.lock();
    let first_read = db
        .core
        .reads
        .submit(first_node, vec![first], 0, false, None)
        .unwrap();
    let second_read = db
        .core
        .reads
        .submit(second_node, vec![second], 0, false, None)
        .unwrap();
    let (finished, result) = mpsc::sync_channel(1);
    let waiter = thread::spawn(move || finished.send(second_read.finish()).unwrap());
    let independent = result.recv_timeout(Duration::from_secs(5));
    drop(held);
    let second_values = independent
        .expect("other owner stalled behind blocked driver")
        .unwrap();
    waiter.join().unwrap();
    assert_eq!(second_values, vec![put(1, 8)]);
    assert_eq!(first_read.finish().unwrap(), vec![put(0, 7)]);
    assert_eq!(*db.core.reads.state.retained.lock(), (0, 0));
    db.shutdown().unwrap();
}

#[test]
fn lane_reservation_backpressure_does_not_signal_false_capacity() {
    let mut fixture = Fixture::new();
    fixture.config.read_lanes = Some(1);
    fixture.config.max_pending_reads = 4;
    fixture.config.read_cache_bytes = 0;
    let db = fixture.open();
    db.write_batch(fixture.operation(0, 0), vec![put(0, 7)])
        .unwrap();
    let reference = db
        .index_reference(KeySpace(0), &0u32.to_be_bytes(), IndexReadCut::Latest)
        .unwrap()
        .unwrap();
    let node = db.core.partition_indexes[&reference.batch_address.partition];
    let lane = db.core.reads.lanes.read()[node][0].clone();
    lane.scheduling.lock().inline_reserved = true;
    let generation = db.core.reads.admission_generation();

    assert!(
        db.core
            .reads
            .submit(node, vec![reference], 0, false, None)
            .is_err()
    );
    assert_eq!(db.core.reads.admission_generation(), generation);
    assert_eq!(*db.core.reads.state.retained.lock(), (0, 0));

    lane.scheduling.lock().inline_reserved = false;
    db.shutdown().unwrap();
}

#[test]
fn cached_hit_and_same_shard_miss_progress_while_first_miss_is_blocked() {
    let fixture = Fixture::new();
    let db = fixture.open();
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
    let lane = db.core.reads.lane_client(first_node, 0);
    let held = lane.lock();
    let mut first = db.submit_get(KeySpace(0), &0u32.to_be_bytes()).unwrap();
    assert!(db.poll_get(&mut first).unwrap().is_none());
    let rpcs_before_hit = db.live_read_submission_count();
    let mut hit = db.submit_get(KeySpace(0), &2u32.to_be_bytes()).unwrap();
    assert_eq!(db.poll_get(&mut hit).unwrap(), Some(value(9)));
    assert_eq!(db.live_read_submission_count(), rpcs_before_hit);
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
    fixture.config.read_lanes = Some(1);
    fixture.config.read_cache_bytes = 0;
    fixture.config.max_pending_reads = 4;
    let mut db = fixture.open();
    db.write_batch(
        fixture.operation(0, 0),
        vec![put(0, 7), put(1, 8), put(2, 9), put(3, 10)],
    )
    .unwrap();
    let lane = db.core.reads.lane_client(0, 0);
    let held = lane.lock();
    let mut pending = Vec::new();
    for key in 0u32..4 {
        pending.push(db.submit_get(KeySpace(0), &key.to_be_bytes()).unwrap());
    }
    assert_eq!(db.core.reads.state.retained.lock().0, 4);
    // An identical immutable miss joins even when all physical slots are full.
    let duplicate = db.submit_get(KeySpace(0), &0u32.to_be_bytes()).unwrap();
    assert_eq!(db.core.reads.state.retained.lock().0, 4);
    assert!(!db.core.reads.failed());
    drop(held);
    assert_eq!(db.finish_get(duplicate).unwrap(), value(7));
    // Caller consumption order is independent of transport completion order.
    for (key, request) in pending.into_iter().enumerate().rev() {
        assert_eq!(db.finish_get(request).unwrap(), value(key as u8 + 7));
    }
    // Every read was offered to the one lane while its worker was blocked.
    assert!(
        db.core
            .reads
            .state
            .diagnostics
            .live_lane_depth_max
            .load(Ordering::Relaxed)
            >= 2
    );
    assert_eq!(*db.core.reads.state.retained.lock(), (0, 0));
    assert!(!db.core.reads.failed());
    db.shutdown().unwrap();
}

#[test]
fn pending_read_keeps_submission_reference_across_overwrite_and_delete() {
    let fixture = Fixture::new();
    let mut db = fixture.open();
    db.write_batch(fixture.operation(0, 0), vec![put(0, 7)])
        .unwrap();
    let lane = db.core.reads.lane_client(0, 0);
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
    fixture.config.read_lanes = Some(1);
    let db = fixture.open();
    db.write_batch(fixture.operation(0, 0), vec![put(0, 7), put(1, 8)])
        .unwrap();
    let mut db = reopen_cold(&fixture, db);
    let lane = db.core.reads.lane_client(node_for(&db, 0), 0);
    let held = lane.lock();
    let first = db.submit_get(KeySpace(0), &0u32.to_be_bytes()).unwrap();
    drop(first);
    assert_eq!(db.core.reads.state.retained.lock().0, 1);
    let error = db
        .submit_get(KeySpace(0), &1u32.to_be_bytes())
        .err()
        .unwrap();
    assert!(RemoteDatabase::read_backpressured(&error));
    assert!(!db.core.reads.failed());
    let state = db.core.reads.state.clone();
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
    // The native authority lock is gone only after lane shutdown finishes;
    // the lane client clone keeps the live connection registry alive.
    drop(lane);
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
    let db = fixture.open();
    db.write_batch(fixture.operation(0, 0), vec![put(0, 7), put(1, 8)])
        .unwrap();
    let mut db = reopen_cold(&fixture, db);
    let first = db.submit_get(KeySpace(0), &0u32.to_be_bytes()).unwrap();
    if let GetState::Pending { read, .. } = &first.state {
        // Observe the real completion without consuming caller-owned credits.
        let slot = read.parts[0].0.slot;
        let completion = &read.parts[0].0.completion;
        let deadline = Instant::now() + Duration::from_secs(5);
        assert!(db.core.reads.state.flight_ready[slot].wait_until(deadline));
        assert!(completion.result.lock().is_some());
    } else {
        panic!("expected miss");
    }
    let charge = db.core.reads.state.retained.lock().1;
    db.core.reads.set_limits_for_test(Some(charge), None);
    let error = db
        .submit_get(KeySpace(0), &1u32.to_be_bytes())
        .err()
        .unwrap();
    assert!(RemoteDatabase::read_backpressured(&error));
    assert_eq!(db.finish_get(first).unwrap(), value(7));
    db.core.reads.set_limits_for_test(Some(charge - 1), None);
    assert!(db.submit_get(KeySpace(0), &1u32.to_be_bytes()).is_err());
    assert_eq!(*db.core.reads.state.retained.lock(), (0, 0));
    assert!(!db.core.reads.failed());
    db.core.reads.set_limits_for_test(Some(charge), None);
    let second = db.submit_get(KeySpace(0), &1u32.to_be_bytes()).unwrap();
    assert_eq!(db.finish_get(second).unwrap(), value(8));
    assert_eq!(*db.core.reads.state.retained.lock(), (0, 0));
    db.shutdown().unwrap();
}

#[test]
fn published_result_stays_unavailable_until_credit_retirement() {
    let fixture = Fixture::new();
    let db = fixture.open();
    let state = db.core.reads.state.clone();
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
        completion: FlightCompletion::new(),
        io_deadline: Mutex::new(None),
        _pin: None,
        _credit: credit.clone(),
    });
    *flight.completion.result.lock() = Some(Ok(vec![put(0, 7)]));
    let mut parts = SmallVec::new();
    parts.push((flight, 0));
    let read = ReadRequest {
        subscription: None,
        parts,
        credit: credit.clone(),
        admitted: Instant::now(),
        timeout: Duration::from_secs(5),
        trace: None,
    };
    drop(credit);

    assert!(read.try_result().unwrap().is_none());
    assert_eq!(*state.retained.lock(), (1, 512));

    let deadline = Instant::now() + Duration::from_secs(5);
    let waiter = thread::spawn(move || read.finish());
    while state.flight_ready[0].waiters.load(Ordering::Acquire) == 0 && Instant::now() < deadline {
        thread::yield_now();
    }
    assert_eq!(state.flight_ready[0].waiters.load(Ordering::Acquire), 1);
    assert_eq!(*state.retained.lock(), (1, 512));

    state.flight_ready[0].retire();
    let values = waiter.join().unwrap().unwrap();
    assert_eq!(values, vec![put(0, 7)]);
    assert_eq!(*state.retained.lock(), (0, 0));
    db.shutdown().unwrap();
}

#[test]
fn subset_joiner_keeps_cancelled_initiators_full_result_credit() {
    let mut fixture = Fixture::new();
    fixture.config.read_cache_bytes = 0;
    fixture.config.read_lanes = Some(1);
    let db = fixture.open();
    db.write_batch(fixture.operation(0, 0), vec![put(0, 7), put(1, 8)])
        .unwrap();
    let db = reopen_cold(&fixture, db);
    let first = db
        .index_reference(KeySpace(0), &0u32.to_be_bytes(), IndexReadCut::Latest)
        .unwrap()
        .unwrap();
    let second = db
        .index_reference(KeySpace(0), &1u32.to_be_bytes(), IndexReadCut::Latest)
        .unwrap()
        .unwrap();
    let node = node_for(&db, 0);
    let lane = db.core.reads.lane_client(node, 0);
    let held = lane.lock();

    let initiator = db
        .core
        .reads
        .submit(node, vec![first, second], 0, false, None)
        .unwrap();
    let initiator_charge = initiator.credit.bytes;
    drop(initiator);
    let joiner = db
        .core
        .reads
        .submit(node, vec![first], 0, false, None)
        .unwrap();
    let retained_after_join = initiator_charge + joiner.credit.bytes;
    let slot = joiner.parts[0].0.slot;
    drop(held);

    let deadline = Instant::now() + Duration::from_secs(5);
    assert!(db.core.reads.state.flight_ready[slot].wait_until(deadline));
    let retained = *db.core.reads.state.retained.lock();
    assert_eq!(retained.0, 1);
    assert_eq!(retained.1, retained_after_join);
    let values = joiner.finish().unwrap();
    assert_eq!(values, vec![put(0, 7)]);
    assert_eq!(*db.core.reads.state.retained.lock(), (0, 0));
    db.shutdown().unwrap();
}

#[test]
fn exact_flight_index_shares_cancellation_and_reuses_retired_slot() {
    let mut fixture = Fixture::new();
    fixture.config.read_cache_bytes = 0;
    fixture.config.read_lanes = Some(1);
    fixture.config.max_pending_reads = 1;
    let db = fixture.open();
    db.write_batch(fixture.operation(0, 0), vec![put(0, 7), put(1, 8)])
        .unwrap();
    let first = db
        .index_reference(KeySpace(0), &0u32.to_be_bytes(), IndexReadCut::Latest)
        .unwrap()
        .unwrap();
    let second = db
        .index_reference(KeySpace(0), &1u32.to_be_bytes(), IndexReadCut::Latest)
        .unwrap()
        .unwrap();
    let node = db.core.partition_indexes[&first.batch_address.partition];
    let lane = db.core.reads.lane_client(node, 0);
    let held = lane.lock();
    let producer = db
        .core
        .reads
        .submit(node, vec![first, second, first], 0, false, None)
        .unwrap();
    let joiner = db
        .core
        .reads
        .submit(node, vec![second, first, second], 0, false, None)
        .unwrap();
    let slot = producer.parts[0].0.slot;
    assert!(Arc::ptr_eq(&producer.parts[0].0, &joiner.parts[0].0));
    assert_eq!(
        (joiner.parts[0].1, joiner.parts[1].1, joiner.parts[2].1),
        (1, 0, 1)
    );
    {
        let registry = db.core.reads.state.flights.lock();
        let mut changed = first;
        changed.batch_version.epoch ^= 1;
        assert!(registry.find(node, &changed).is_none());
        changed = first;
        changed.batch_address.offset ^= 1;
        assert!(registry.find(node, &changed).is_none());
        changed = first;
        changed.batch_address.segment_generation ^= 1;
        assert!(registry.find(node, &changed).is_none());
        changed = first;
        changed.batch_digest.0[0] ^= 1;
        assert!(registry.find(node, &changed).is_none());
        assert!(registry.find(node.wrapping_add(1), &first).is_none());
    }
    drop(producer);
    let mut different = first;
    different.descriptor.offset ^= 1;
    assert!(admission_error(
        &db.core
            .reads
            .submit(node, vec![different], 0, false, None)
            .err()
            .unwrap()
    ));
    drop(held);
    assert_eq!(
        joiner.finish().unwrap(),
        vec![put(1, 8), put(0, 7), put(1, 8)]
    );
    assert_eq!(*db.core.reads.state.retained.lock(), (0, 0));
    assert!(
        db.core
            .reads
            .state
            .flights
            .lock()
            .find(node, &first)
            .is_none()
    );

    let next = db
        .core
        .reads
        .submit(node, vec![second], 0, false, None)
        .unwrap();
    assert_eq!(next.parts[0].0.slot, slot);
    {
        let registry = db.core.reads.state.flights.lock();
        assert!(registry.find(node, &first).is_none());
        assert!(registry.find(node, &second).is_some());
    }
    assert_eq!(next.finish().unwrap(), vec![put(1, 8)]);
    assert_eq!(*db.core.reads.state.retained.lock(), (0, 0));
    db.shutdown().unwrap();
}

#[test]
fn duplicate_map_resize_is_charged_at_group_budget_boundaries() {
    let mut fixture = Fixture::new();
    fixture.config.read_cache_bytes = 0;
    let db = fixture.open();
    for first in (0u32..29).step_by(4) {
        db.write_batch(
            fixture.operation(0, u64::from(first)),
            (first..(first + 4).min(29))
                .map(|key| put(key, key as u8))
                .collect(),
        )
        .unwrap();
    }
    let references: Vec<_> = (0u32..29)
        .map(|key| {
            db.index_reference(KeySpace(0), &key.to_be_bytes(), IndexReadCut::Latest)
                .unwrap()
                .unwrap()
        })
        .collect();
    let node = db.core.partition_indexes[&references[0].batch_address.partition];
    for count in [15, 29] {
        let members = &references[..count];
        let mut map = HashMap::new();
        for (index, reference) in members.iter().copied().enumerate() {
            map.insert(reference, index);
        }
        // HashMap's current 7/8 load factor exposes 28 and 56 usable entries
        // for these resize boundaries, backed by 32 and 64 buckets.
        let buckets = map.capacity() + map.capacity() / 7;
        let bucket_bytes = std::mem::size_of::<(ValueRecordReference, usize)>() + 1;
        assert!(3 * count * bucket_bytes + 512 >= buckets * bucket_bytes + 16);
        let charge = 512
            + members
                .iter()
                .map(|r| member_charge(r).unwrap())
                .sum::<usize>();
        db.core.reads.set_limits_for_test(Some(charge), None);
        let read = db
            .core
            .reads
            .submit(node, members.to_vec(), 0, false, None)
            .unwrap();
        assert_eq!(db.core.reads.state.retained.lock().1, charge);
        assert_eq!(
            read.finish().unwrap(),
            (0..count)
                .map(|key| put(key as u32, key as u8))
                .collect::<Vec<_>>()
        );
        assert_eq!(*db.core.reads.state.retained.lock(), (0, 0));
        db.core.reads.set_limits_for_test(Some(charge - 1), None);
        assert!(admission_error(
            &db.core
                .reads
                .submit(node, members.to_vec(), 0, false, None)
                .err()
                .unwrap()
        ));
    }
    db.shutdown().unwrap();
}

#[test]
fn dropped_failed_read_poison_cannot_be_hidden_by_a_cached_hit() {
    let mut fixture = Fixture::new();
    fixture.config.read_lanes = Some(1);
    let db = fixture.open();
    db.write_batch(fixture.operation(0, 0), vec![put(0, 7)])
        .unwrap();
    db.write_batch(fixture.operation(1, 1), vec![put(1, 8)])
        .unwrap();
    let mut db = reopen_cold(&fixture, db);
    db.get(KeySpace(0), &1u32.to_be_bytes()).unwrap();
    db.set_read_profile_limit(1);
    let profile = db.read_profile_monitor();
    let node = node_for(&db, 0);
    let lane = db.core.reads.lane_client(node, 0);
    let held = lane.lock();
    let pending = db.submit_get(KeySpace(0), &0u32.to_be_bytes()).unwrap();
    drop(pending);
    fixture.fence_owner(node, db.epoch() + 1);
    drop(held);
    db.core.reads.stop(); // deterministic completion barrier, including discarded replies
    assert!(db.core.reads.failed());
    let cause = db
        .core
        .first_failure
        .get()
        .expect("read failure is retained")
        .to_string();
    assert!(cause.contains("stage=read_completion"), "{cause}");
    assert!(!cause.contains("stage=owner_health"), "{cause}");
    let batch_error = db
        .get_many(KeySpace(0), &[0u32.to_be_bytes().to_vec()])
        .unwrap_err()
        .to_string();
    assert!(batch_error.contains(&cause), "{batch_error}");
    assert_eq!(*db.core.reads.state.retained.lock(), (0, 0));
    let mut counts = std::collections::BTreeMap::new();
    profile.visit(&mut |name, value| {
        counts.insert(name, value);
    });
    assert_eq!(counts["read_profile_selected"], 1);
    assert_eq!(counts["read_profile_completed"], 1);
    assert_eq!(counts["read_profile_errors"], 1);
    assert_eq!(
        counts["read_profile_failed_owner_profile_not_applicable"],
        1
    );
    let mut owner_counts = std::collections::BTreeMap::new();
    profile.visit_partitions(&mut |partition, name, value| {
        owner_counts.insert((partition, name), value);
    });
    assert_eq!(
        owner_counts[&(0, "read_profile_failed_owner_profile_not_applicable")],
        1
    );
    let hits = db.read_cache_stats().hits;
    assert!(db.get(KeySpace(0), &1u32.to_be_bytes()).is_err());
    assert_eq!(db.read_cache_stats().hits, hits);
    assert!(db.shutdown().is_err());
}

#[test]
fn expired_io_poison_and_foreign_epoch_handles_are_rejected() {
    let fixture = Fixture::new();
    let db = fixture.open();
    db.write_batch(fixture.operation(0, 0), vec![put(0, 7)])
        .unwrap();
    let mut db = reopen_cold(&fixture, db);
    let lane = db.core.reads.lane_client(node_for(&db, 0), 0);
    let held = lane.lock();
    let mut pending = db.submit_get(KeySpace(0), &0u32.to_be_bytes()).unwrap();
    let GetState::Pending { read, .. } = &pending.state else {
        panic!("expected miss");
    };
    *read.parts[0].0.io_deadline.lock() = Some(Instant::now() - Duration::from_secs(1));
    assert!(db.poll_get(&mut pending).is_err());
    assert!(db.core.reads.failed());
    drop(held);
    drop(pending);
    assert!(db.shutdown().is_err());
    drop(db);
    // The lane client clone keeps the live connection registry (and its lock) alive.
    drop(lane);
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

/// An admitted read does not spend its I/O timeout in the live lane inbox.
#[test]
fn queued_read_waits_for_busy_lane_longer_than_timeout() {
    let mut fixture = Fixture::new();
    fixture.config.read_lanes = Some(1);
    fixture.config.read_cache_bytes = 0;
    fixture.config.timeout = Duration::from_secs(1);
    let db = fixture.open();
    db.write_batch(fixture.operation(0, 0), vec![put(0, 7)])
        .unwrap();
    let mut db = reopen_cold(&fixture, db);
    let lane = db.core.reads.lane_client(node_for(&db, 0), 0);
    let held = lane.lock();
    let mut pending = db.submit_get(KeySpace(0), &0u32.to_be_bytes()).unwrap();
    thread::sleep(3 * fixture.config.timeout);
    let queued = db.poll_get(&mut pending);
    drop(held);
    assert_eq!(queued.unwrap(), None, "the queued read must stay pending");
    assert!(!db.core.reads.failed());
    assert_eq!(db.core.reads.state.retained.lock().0, 1);
    assert_eq!(db.finish_get(pending).unwrap(), value(7));
    db.shutdown().unwrap();
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
    // Authority health gate only, as the removed `healthy = false` field write.
    db.core.concurrent_failure.store(true, Ordering::Release);
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
