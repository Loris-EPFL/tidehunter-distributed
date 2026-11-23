//! Deterministic lifecycle cuts over real five-node authenticated WAL storage.
//! These are recovery/retention regressions, not performance measurements.
//! Every case opens the leased live fixture, so the module needs nvmeof.
//!
//! The Exact checkpoint lifecycle cases (start/progress/adopt a checkpoint,
//! index-byte pressure, resident identities) are gone with the Exact index;
//! their hybrid counterparts live in `hybrid/snapshot_tests.rs` (snapshot
//! stages and crash cuts) and `hybrid/replay_tests.rs`.

use super::pipeline_tests::Fixture;
use super::*;

fn put(value: u8) -> Mutation {
    Mutation::Put {
        keyspace: KeySpace(0),
        key: 1u32.to_be_bytes().to_vec().into(),
        value: vec![value; 32].into(),
    }
}

fn put_key(key: u32, value: u8) -> Mutation {
    Mutation::Put {
        keyspace: KeySpace(0),
        key: key.to_be_bytes().to_vec().into(),
        value: vec![value; 32].into(),
    }
}

#[test]
fn completed_owner_metadata_is_bounded_across_lifetime_capacity_and_reopens() {
    let fixture = Fixture::new_with_capacity(8);
    let original = fixture.operation(0, 100);
    let mut original_version = None;
    let mut next = 100u64;
    for _ in 0..3 {
        let mut db = fixture.open();
        let bound = db.core.metadata_history_bound();
        let mut epoch_retry = None;
        for _ in 0..4 {
            for _ in 0..16 {
                let operation = fixture.operation((next % 5) as u32, next);
                let version = db.write_batch(operation, vec![put(next as u8)]).unwrap();
                if next == 100 {
                    original_version = Some(version);
                }
                next += 1;
                // Finalization (slot return) follows the caller's outcome.
                db.core.wait_settled().unwrap();
                assert_eq!(db.core.retained_slots(), 0);
                assert_eq!(db.core.unresolved_slots(), 0);
            }
            db.flush_index().unwrap();
            assert!(db.core.publisher.lock().recent.len() <= WAL_REPLAY_WINDOW_BATCHES);
            let charge = db.metadata_history_bytes();
            assert!(
                charge <= bound,
                "resident completed bookkeeping {charge} exceeds the retry-window bound {bound}"
            );
            // Within one epoch the retry window answers with the recorded
            // version and writes nothing. A reopen empties it (OWNER decision,
            // BOARD 2026-11-20, decision 2), so the first retry of an earlier
            // epoch's identity is a new write under the new epoch and carries
            // its value; later retries in that epoch return that first
            // retry's version and write nothing.
            let before_row = db.get(KeySpace(0), &1u32.to_be_bytes()).unwrap();
            let recorded = original_version.unwrap();
            let retried = db.write_batch(original, vec![put(100)]).unwrap();
            // `write_batch` returns once the batch is readable; the drain keeps
            // the read below from racing later publication.
            db.drain().unwrap();
            let after_row = db.get(KeySpace(0), &1u32.to_be_bytes()).unwrap();
            if retried.epoch == recorded.epoch {
                assert_eq!(retried, recorded);
                assert_eq!(after_row, before_row);
            } else if let Some(first) = epoch_retry {
                assert_eq!(retried, first, "an in-epoch retry changed its version");
                assert_eq!(after_row, before_row);
            } else {
                assert!(retried.epoch > recorded.epoch);
                assert_eq!(after_row, Some(vec![100; 32].into()));
                epoch_retry = Some(retried);
            }
        }
        db.shutdown().unwrap();
    }
}

/// Faithful plateau of the retry window: past `WAL_REPLAY_WINDOW_BATCHES`
/// batches the retained retry bookkeeping stops growing. After each flush,
/// once the window is full, `recent` holds exactly the window, and the
/// resident charge stays within the static bound of that window, which does
/// not depend on lifetime history. Ignored in the normal suite for its length
/// (three windows of writes); the gate's ignored-test stage runs it.
#[test]
#[ignore = "writes past the 4,096-batch retry window; run by the gate's ignored-test stage"]
fn completed_owner_metadata_plateaus_once_the_retry_window_is_full() {
    const FLUSH_EVERY: u64 = 256;
    let window = WAL_REPLAY_WINDOW_BATCHES as u64;
    // Wide fragments: a 4 KiB leased frame never closes its extent, so the
    // run measures bookkeeping, not WAL file churn.
    let fixture = Fixture::new_with_config(
        64,
        KeyShape::new_single(4, 1, crate::key_shape::KeyType::uniform(1)),
        2,
        |config| {
            config.storage.fragment_bytes = 1024 * 1024;
            config.storage.wal_file_bytes = 8 * 1024 * 1024;
        },
    );
    let db = fixture.open();
    let bound = db.core.metadata_history_bound();
    let mut written = 0u64;
    while written < 3 * window {
        let mut last = None;
        for _ in 0..FLUSH_EVERY {
            let operation = fixture.operation((written % 5) as u32, written);
            let version = db.write_batch(operation, vec![put(written as u8)]).unwrap();
            last = Some((operation, written, version));
            written += 1;
        }
        db.flush_index().unwrap();
        let recent = db.core.publisher.lock().recent.len();
        let charge = db.metadata_history_bytes();
        assert!(
            charge <= bound,
            "resident completed bookkeeping {charge} exceeds the retry-window bound {bound}"
        );
        if written >= window {
            assert_eq!(recent, WAL_REPLAY_WINDOW_BATCHES, "after {written} writes");
        } else {
            assert_eq!(recent as u64, written);
        }
        // The newest write is inside the window: its retry keeps its version.
        let (operation, value, version) = last.unwrap();
        assert_eq!(
            db.write_batch(operation, vec![put(value as u8)]).unwrap(),
            version
        );
    }
    db.shutdown().unwrap();
}

#[test]
fn skewed_submit_window_spills_one_partition_without_lifetime_rejection() {
    let mut fixture = Fixture::new_with_capacity(4);
    fixture.config.max_admitted_batches = 64;
    fixture.config.max_pending_batches = 64;
    let mut db = fixture.open();
    // One prevalidated window is larger than this node's four-batch metadata
    // window. Every identity deliberately maps to the same physical partition.
    let writes: Vec<_> = (0..19u64)
        .map(|sequence| {
            (
                fixture.operation(0, 1000 + sequence),
                vec![put(sequence as u8)],
            )
        })
        .collect();
    let versions = db.submit_many(&writes).unwrap();
    db.drain().unwrap();
    assert_eq!(versions.len(), writes.len());
    for (sequence, version) in versions.iter().enumerate() {
        assert_eq!(version.sequence, sequence as u64);
    }
    assert_eq!(db.stats().admitted_batches, writes.len() as u64);
    db.core.wait_settled().unwrap();
    assert_eq!(db.core.retained_slots(), 0);
    assert_eq!(db.core.admission.charges().partition_batches, 0);
    assert_eq!(
        db.get(KeySpace(0), &1u32.to_be_bytes())
            .unwrap()
            .unwrap()
            .as_ref(),
        &[18; 32]
    );
    db.flush_index().unwrap();
    db.shutdown().unwrap();
    drop(db);
    let reopened = fixture.open();
    // Accepted weakening: a reopen empties the retry window, so each retry of
    // an earlier-epoch operation is re-applied under the new epoch.
    for ((operation, mutations), version) in writes.into_iter().zip(versions) {
        let retried = reopened.write_batch(operation, mutations).unwrap();
        assert!(retried.epoch > version.epoch);
    }
    reopened.validate_cached_read().unwrap();
}

#[test]
fn publication_failure_fences_and_recovers_dispatched_successor_group() {
    let fixture = Fixture::new();
    let db = fixture.open();
    // Every batch writes key 1, so each generation depends on the previous
    // one. The predecessor on owner 1 stays held, so the owner-0 groups become
    // durable and park before the failing publication can run. Publication
    // runs on the owner-0 worker after its receipt: without this order, that
    // worker can harvest the first group's reply and fail-stop before it has
    // sent the successor group at all.
    let predecessor_connection = std::sync::Arc::clone(&db.core.clients[1].client);
    let held_predecessor = predecessor_connection.lock();
    let mut identities = Vec::new();
    let operation = fixture.operation(1, 100);
    let version = db.submit(operation, vec![put(100)]).unwrap();
    identities.push((operation, 100, version));
    let connection = std::sync::Arc::clone(&db.core.clients[0].client);
    let held = connection.lock();
    for key in 0..4u32 {
        let operation = fixture.operation(0, u64::from(key));
        let version = db.submit(operation, vec![put(key as u8)]).unwrap();
        identities.push((operation, key as u8, version));
    }
    // A native publication callback error for the first owner-0 generation:
    // its already dispatched, durable successor group outlives it. Recovery
    // must reconcile that storage outcome rather than discard it.
    db.core.test_hooks.lock().fail_publication = Some(identities[1].2.sequence);
    drop(held);
    // Liveness guard only: both owner-0 groups are durable and parked.
    let deadline = Instant::now() + Duration::from_secs(30);
    for (_, _, version) in &identities[1..] {
        while !db.core.publication_settled(version.sequence) {
            db.core.ensure_healthy().unwrap();
            assert!(Instant::now() < deadline, "owner-0 receipts not processed");
            thread::yield_now();
        }
    }
    drop(held_predecessor);
    assert!(db.drain().is_err());
    assert!(db.core.ensure_healthy().is_err());
    assert!(db.ensure_healthy().is_err());
    // The failed publication call returns before it advances the floor, so
    // the predecessor published in that same call is not installed either.
    assert_eq!(db.core.published_next(), 0);
    assert!(db.core.admission.charges().slots > 0);
    drop(db);
    // Client clones keep the live connection registry (and its lock) alive.
    drop((connection, predecessor_connection));

    let mut reopened = fixture.open();
    assert_eq!(reopened.recovery_stats().tail_batches, 5);
    // Accepted weakening: the reopened retry window is empty, so every retry is
    // re-applied under the new epoch. Key 1 still ends at the last value.
    for (operation, value, version) in identities {
        let retried = reopened.submit(operation, vec![put(value)]).unwrap();
        assert!(retried.epoch > version.epoch);
    }
    assert_eq!(
        reopened.get(KeySpace(0), &1u32.to_be_bytes()).unwrap(),
        Some(vec![3; 32].into())
    );
    // Accepted weakening: all five retries above were re-applied and admitted
    // because the reopened retry window is empty.
    assert_eq!(reopened.stats().admitted_batches, 5);
    reopened.shutdown().unwrap();
}

/// A disjoint later batch publishes and survives a crash while an earlier
/// batch never reached the owner WAL: the published receipts replay, the
/// missing batch is absent, and its retry takes a new epoch.
#[test]
fn sparse_ack_survives_crash_when_earlier_disjoint_batch_never_reached_wal() {
    let fixture = Fixture::new();
    let db = fixture.open();
    // Queueing is immediate now. Hold the actual owner before enqueue so the
    // missing batch still has no WAL effect at the simulated coordinator crash.
    let connection = Arc::clone(&db.core.clients[0].client);
    let held = connection.lock();
    let suffix_connection = Arc::clone(&db.core.clients[1].client);
    let suffix_held = suffix_connection.lock();
    let missing = fixture.operation(0, 40);
    let acknowledged = fixture.operation(1, 41);
    let missing_version = db.submit(missing, vec![put_key(1, 1)]).unwrap();
    let acknowledged_version = db.submit(acknowledged, vec![put_key(2, 2)]).unwrap();
    let other_version = db
        .submit(fixture.operation(1, 42), vec![put_key(3, 3)])
        .unwrap();
    drop(suffix_held);
    super::pipeline_tests::complete_owner(
        &db,
        1,
        &[acknowledged_version.sequence, other_version.sequence],
    );
    assert!(!db.core.is_published(missing_version));
    assert!(db.core.is_published(acknowledged_version));
    assert_eq!(
        db.core.clients[0].queue.snapshot(),
        (1, 2, vec![(1, missing_version.sequence, 1)]),
    );
    assert_eq!(held.leased_flights_in_flight(), 0);
    // Simulate abrupt authority loss with the accepted batch still in its
    // owner queue and no transport submission. begin_failure stores the
    // shared worker failure flag before the owner client is released.
    db.core
        .begin_failure(&LocalDbError::new("injected coordinator loss"));
    drop(held);
    drop(db);
    // Client clones keep the live connection registry (and its lock) alive.
    drop((connection, suffix_connection));

    let mut reopened = fixture.open();
    assert!(
        reopened
            .get(KeySpace(0), &1u32.to_be_bytes())
            .unwrap()
            .is_none()
    );
    assert_eq!(
        reopened
            .get(KeySpace(0), &2u32.to_be_bytes())
            .unwrap()
            .unwrap()
            .as_ref(),
        &[2; 32]
    );
    assert_eq!(
        reopened
            .get(KeySpace(0), &3u32.to_be_bytes())
            .unwrap()
            .unwrap()
            .as_ref(),
        &[3; 32]
    );
    // Accepted weakening: the reopened retry window is empty, so the retry of a
    // published batch is re-applied under the new epoch; its value stays.
    let retried = reopened.submit(acknowledged, vec![put_key(2, 2)]).unwrap();
    assert!(retried.epoch > acknowledged_version.epoch);
    assert_eq!(
        reopened
            .get(KeySpace(0), &2u32.to_be_bytes())
            .unwrap()
            .unwrap()
            .as_ref(),
        &[2; 32]
    );
    // An unacknowledged absent batch can be admitted in the new epoch; its
    // original sequence must not be reused as though the old write succeeded.
    let retry = reopened.write_batch(missing, vec![put_key(1, 1)]).unwrap();
    assert!(retry.epoch > missing_version.epoch);
    reopened.shutdown().unwrap();
}

/// A diagnostic control cannot pass a control that is parked in its owner
/// worker: the worker dispatches controls in FIFO order. The parked control
/// signals that it is in flight and blocks on the test's gate, so "still
/// pending" is an observed state, not a timing bound.
#[test]
fn public_diagnostic_controls_wait_for_pending_control() {
    #[cfg(all(target_os = "linux", feature = "test-utils"))]
    let cases = 4;
    #[cfg(not(all(target_os = "linux", feature = "test-utils")))]
    let cases = 2;
    for case in 0..cases {
        let fixture = Fixture::new();
        let mut db = fixture.open();
        db.write_batch(fixture.operation(0, 73), vec![put(1)])
            .unwrap();
        // Park owner 0's worker: the control runs at dispatch, signals that it
        // is in flight, then waits for the gate.
        let (entered, in_flight) = mpsc::sync_channel(1);
        let (gate, release) = mpsc::sync_channel(1);
        db.core.clients[0]
            .send(Command::Control(Box::new(move |_client| {
                let _ = entered.send(());
                let _ = release.recv();
            })))
            .unwrap();
        in_flight.recv_timeout(Duration::from_secs(5)).unwrap();
        let (started, starting) = mpsc::sync_channel(1);
        let (finished, result) = mpsc::sync_channel(1);
        let owner = thread::spawn(move || {
            let _ = started.send(());
            let outcome = match case {
                0 => db.storage_stats().map(|_| ()),
                1 => db.connection_profiles(true, true).map(|_| ()),
                #[cfg(all(target_os = "linux", feature = "test-utils"))]
                2 => db.read_evidence_for_benchmark(false).map(|_| ()),
                #[cfg(all(target_os = "linux", feature = "test-utils"))]
                3 => db.process_resources_for_benchmark().map(|_| ()),
                _ => unreachable!(),
            };
            let _ = finished.send((db, outcome));
        });
        starting.recv_timeout(Duration::from_secs(5)).unwrap();
        // The parked control is in flight and the gate is closed, so the
        // worker cannot have dispatched the diagnostic control.
        assert!(
            result.try_recv().is_err(),
            "a later diagnostic control passed the parked control (case {case})"
        );
        gate.send(()).unwrap();
        let (mut db, outcome) = result
            .recv_timeout(Duration::from_secs(5))
            .expect("the diagnostic control completes once the parked control returns");
        owner.join().unwrap();
        outcome.unwrap();
        if case == 1 {
            db.connection_profiles(false, false).unwrap();
        }
        db.shutdown().unwrap();
    }
}

/// Mapped-WAL eviction is a control on every owner worker, so it waits behind
/// a control already parked there; the gate makes the wait an observed state.
#[cfg(all(target_os = "linux", feature = "test-utils"))]
#[test]
fn concurrent_eviction_waits_for_pending_control() {
    let mut fixture = Fixture::new();
    fixture.config.read_cache_bytes = 0;
    let db = fixture.open();
    db.write_batch(fixture.operation(0, 76), vec![put(1)])
        .unwrap();
    let (entered, in_flight) = mpsc::sync_channel(1);
    let (gate, release) = mpsc::sync_channel(1);
    db.core.clients[0]
        .send(Command::Control(Box::new(move |_client| {
            let _ = entered.send(());
            let _ = release.recv();
        })))
        .unwrap();
    in_flight.recv_timeout(Duration::from_secs(5)).unwrap();
    let db = ConcurrentRemoteDatabase::new(db).unwrap();
    let (started, starting) = mpsc::sync_channel(1);
    let (finished, result) = mpsc::sync_channel(1);
    let owner = thread::spawn(move || {
        let _ = started.send(());
        let outcome = db.evict_mapped_wal_for_benchmark();
        let _ = finished.send((db, outcome));
    });
    starting.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(
        result.try_recv().is_err(),
        "mapped-WAL eviction passed the parked owner control"
    );
    gate.send(()).unwrap();
    let (db, outcome) = result
        .recv_timeout(Duration::from_secs(5))
        .expect("eviction completes once the parked control returns");
    owner.join().unwrap();
    assert_eq!(outcome.unwrap().len(), 5);
    db.shutdown().unwrap();
}

#[test]
fn cancelled_control_reply_drains_before_worker_shutdown() {
    let fixture = Fixture::new();
    let db = fixture.open();
    db.write_batch(fixture.operation(0, 51), vec![put(1)])
        .unwrap();
    let (reply, cancelled) = mpsc::sync_channel(1);
    db.core.clients[0].send(Command::Stats(reply)).unwrap();
    drop(cancelled);
    let operation = fixture.operation(0, 52);
    let version = db.write_batch(operation, vec![put(2)]).unwrap();
    db.shutdown().unwrap();
    drop(db);
    let reopened = fixture.open();
    // Accepted weakening: the reopened retry window is empty, so the retry is
    // re-applied under the new epoch.
    let retried = reopened.write_batch(operation, vec![put(2)]).unwrap();
    assert!(retried.epoch > version.epoch);
    reopened.shutdown().unwrap();
}
