//! Deterministic lifecycle cuts over real five-node authenticated WAL storage.
//! These are recovery/retention regressions, not performance measurements.

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
fn control_monitor_waits_for_occupied_checkpoint_slot_and_stops_cleanly() {
    let fixture = Fixture::new().history(HistoryPersistence::OnDisk);
    let db = fixture.open();
    db.write_batch(fixture.operation(0, 70), vec![put(1)])
        .unwrap();
    let monitor = db.read_evidence_monitor();
    let (entered, release) = fixture.nodes[0].pause_inventory();
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    let (reply, proof) = mpsc::sync_channel(1);
    db.core.clients[0]
        .send(Command::Checkpoint(db.epoch(), db.epoch(), 1, reply))
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if db.core.clients[0].client.lock().checkpoint_pending() {
            break;
        }
        if Instant::now() >= deadline {
            drop(release);
            panic!("checkpoint control was not submitted");
        }
        thread::yield_now();
    }
    let (called, called_rx) = mpsc::sync_channel(1);
    let (done, completed) = mpsc::sync_channel(1);
    db.core.clients[0]
        .send(Command::Control(Box::new(move |client| {
            let _ = called.send(());
            let _ = done.send(client.read_io_stats());
        })))
        .unwrap();
    assert!(called_rx.try_recv().is_err());
    let monitoring = ReadEvidenceMonitor {
        clients: monitor.clients.clone(),
        reads: monitor.reads.clone(),
    };
    let (begun, starting) = mpsc::sync_channel(1);
    let (finished, result) = mpsc::sync_channel(1);
    let sampler = thread::spawn(move || {
        let _ = begun.send(());
        let _ = finished.send(monitoring.sample_owner_read_io());
    });
    starting.recv_timeout(Duration::from_secs(5)).unwrap();
    let early = result.recv_timeout(Duration::from_millis(100));
    release.send(()).unwrap();
    let checkpoint = proof.recv_timeout(Duration::from_secs(5)).unwrap().unwrap();
    assert_eq!(checkpoint.prefix, 1);
    called_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    completed
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    let sampled = match early {
        Ok(result) => result,
        Err(mpsc::RecvTimeoutError::Timeout) => {
            result.recv_timeout(Duration::from_secs(5)).unwrap()
        }
        Err(error) => panic!("monitor disconnected: {error}"),
    };
    sampler.join().unwrap();
    assert_eq!(sampled.unwrap().len(), 5);
    assert_eq!(monitor.sample_owner_read_io().unwrap().len(), 5);
    db.shutdown().unwrap();
    assert!(monitor.sample_owner_read_io().is_err());
}

#[test]
fn public_diagnostic_controls_wait_for_checkpoint_without_consuming_proof() {
    #[cfg(all(target_os = "linux", feature = "test-utils"))]
    let cases = 4;
    #[cfg(not(all(target_os = "linux", feature = "test-utils")))]
    let cases = 2;
    for case in 0..cases {
        let fixture = Fixture::new().history(HistoryPersistence::OnDisk);
        let mut db = fixture.open();
        db.write_batch(fixture.operation(0, 73), vec![put(1)])
            .unwrap();
        let receipt = *db.core.publisher.lock().known.values().next().unwrap();
        let expected = super::super::tcp::checkpoint::prefix_digest(
            db.core.config.storage.database,
            0,
            db.epoch(),
            std::iter::once(receipt.into()),
        );
        let (entered, release) = fixture.nodes[0].pause_inventory();
        entered.recv_timeout(Duration::from_secs(5)).unwrap();
        let (reply, proof) = mpsc::sync_channel(1);
        db.core.clients[0]
            .send(Command::Checkpoint(db.epoch(), db.epoch(), 1, reply))
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        while !db.core.clients[0].client.lock().checkpoint_pending() {
            if Instant::now() >= deadline {
                drop(release);
                panic!("checkpoint control was not submitted");
            }
            thread::yield_now();
        }
        let (started, starting) = mpsc::sync_channel(1);
        let (finished, result) = mpsc::sync_channel(1);
        let owner = thread::spawn(move || {
            let _ = started.send(());
            let outcome = match case {
                0 => db.read_io_stats().map(|_| ()),
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
        let early = result.recv_timeout(Duration::from_millis(100));
        release.send(()).unwrap();
        let proof = proof.recv_timeout(Duration::from_secs(5)).unwrap().unwrap();
        assert_eq!(proof.prefix, 1);
        assert_eq!(proof.digest, expected);
        let completed = match early {
            Ok(result) => Ok(result),
            Err(mpsc::RecvTimeoutError::Timeout) => result.recv_timeout(Duration::from_secs(5)),
            Err(error) => Err(error),
        };
        let (mut db, outcome) = match completed {
            Ok(result) => result,
            Err(error) => {
                std::mem::forget(fixture);
                panic!("diagnostic control did not complete: {error}");
            }
        };
        owner.join().unwrap();
        outcome.unwrap();
        if case == 1 {
            db.connection_profiles(false, false).unwrap();
        }
        db.shutdown().unwrap();
    }
}

#[test]
fn write_behind_pending_checkpoint_is_durable_and_recoverable() {
    let fixture = Fixture::new();
    let db = fixture.open();
    db.write_batch(fixture.operation(0, 74), vec![put(1)])
        .unwrap();
    let (entered, release) = fixture.nodes[0].pause_inventory();
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    db.core.start_checkpoint().unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while !db.core.clients[0].client.lock().checkpoint_pending() {
        if Instant::now() >= deadline {
            drop(release);
            panic!("checkpoint control was not submitted");
        }
        thread::yield_now();
    }
    let operation = fixture.operation(0, 75);
    let (started, starting) = mpsc::sync_channel(1);
    let (finished, result) = mpsc::sync_channel(1);
    let owner = thread::spawn(move || {
        let _ = started.send(());
        let outcome = db.write_batch(operation, vec![put(2)]);
        let _ = finished.send((db, outcome));
    });
    starting.recv_timeout(Duration::from_secs(5)).unwrap();
    release.send(()).unwrap();
    let (db, outcome) = match result.recv_timeout(Duration::from_secs(5)) {
        Ok(result) => result,
        Err(error) => {
            std::mem::forget(fixture);
            panic!("nonpipelined write did not complete: {error}");
        }
    };
    owner.join().unwrap();
    let version = outcome.unwrap();
    db.flush_index().unwrap();
    db.shutdown().unwrap();
    drop(db);
    let mut reopened = fixture.open();
    assert_eq!(
        reopened.write_batch(operation, vec![put(2)]).unwrap(),
        version
    );
    assert_eq!(
        reopened
            .get(KeySpace(0), &1u32.to_be_bytes())
            .unwrap()
            .unwrap()
            .as_ref(),
        &[2; 32]
    );
    reopened.shutdown().unwrap();
}

#[cfg(all(target_os = "linux", feature = "test-utils"))]
#[test]
fn concurrent_eviction_waits_for_pending_checkpoint_control() {
    let mut fixture = Fixture::new().history(HistoryPersistence::OnDisk);
    fixture.config.read_cache_bytes = 0;
    let db = fixture.open();
    db.write_batch(fixture.operation(0, 76), vec![put(1)])
        .unwrap();
    let (entered, release) = fixture.nodes[0].pause_inventory();
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    let (reply, proof) = mpsc::sync_channel(1);
    db.core.clients[0]
        .send(Command::Checkpoint(db.epoch(), db.epoch(), 1, reply))
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while !db.core.clients[0].client.lock().checkpoint_pending() {
        if Instant::now() >= deadline {
            drop(release);
            panic!("checkpoint control was not submitted");
        }
        thread::yield_now();
    }
    let db = ConcurrentRemoteDatabase::new(db).unwrap();
    let (started, starting) = mpsc::sync_channel(1);
    let (finished, result) = mpsc::sync_channel(1);
    let owner = thread::spawn(move || {
        let _ = started.send(());
        let outcome = db.evict_mapped_wal_for_benchmark();
        let _ = finished.send((db, outcome));
    });
    starting.recv_timeout(Duration::from_secs(5)).unwrap();
    let early = result.recv_timeout(Duration::from_millis(100));
    release.send(()).unwrap();
    assert_eq!(
        proof
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap()
            .prefix,
        1
    );
    let completed = match early {
        Ok(result) => Ok(result),
        Err(mpsc::RecvTimeoutError::Timeout) => result.recv_timeout(Duration::from_secs(5)),
        Err(error) => Err(error),
    };
    let (db, outcome) = match completed {
        Ok(result) => result,
        Err(error) => {
            std::mem::forget(fixture);
            panic!("concurrent eviction did not complete: {error}");
        }
    };
    owner.join().unwrap();
    assert_eq!(outcome.unwrap().len(), 5);
    db.shutdown().unwrap();
}

#[test]
fn scalar_write_uses_worker_while_checkpoint_awaits_verification() {
    let fixture = Fixture::new();
    let db = fixture.open();
    db.write_batch(fixture.operation(0, 71), vec![put(1)])
        .unwrap();
    let before = db.pipeline_stats().append_requests;
    let (entered, waiting) = mpsc::sync_channel(1);
    let (resume, gate) = mpsc::sync_channel(1);
    db.core.test_hooks.lock().checkpoint_prefix_pause = Some((entered, gate));
    db.core.start_checkpoint().unwrap();
    waiting.recv_timeout(Duration::from_secs(5)).unwrap();
    let operation = fixture.operation(0, 72);
    let (finished, result) = mpsc::sync_channel(1);
    let owner = thread::spawn(move || {
        let outcome = db.write_batch(operation, vec![put(2)]);
        let append_requests = db.pipeline_stats().append_requests;
        let _ = finished.send((db, outcome, append_requests));
    });
    let before_resume = result.recv_timeout(Duration::from_secs(5));
    let _ = resume.send(());
    let (db, outcome, append_requests) = match before_resume {
        Ok(result) => result,
        Err(error) => {
            std::mem::forget(fixture);
            panic!("scalar write did not finish before checkpoint verification: {error}");
        }
    };
    owner.join().unwrap();
    let version = outcome.unwrap();
    // The scalar write formed exactly one worker group while the checkpoint
    // was paused before verification.
    assert_eq!(append_requests, before + 1);
    db.shutdown().unwrap();
    drop(db);
    let reopened = fixture.open();
    assert_eq!(
        reopened.write_batch(operation, vec![put(2)]).unwrap(),
        version
    );
    reopened.shutdown().unwrap();
}

#[test]
fn cancelled_checkpoint_reply_drains_before_worker_shutdown() {
    let fixture = Fixture::new().history(HistoryPersistence::OnDisk);
    let db = fixture.open();
    db.write_batch(fixture.operation(0, 51), vec![put(1)])
        .unwrap();
    let (reply, cancelled) = mpsc::sync_channel(1);
    db.core.clients[0]
        .send(Command::Checkpoint(db.epoch(), db.epoch(), 1, reply))
        .unwrap();
    drop(cancelled);
    let operation = fixture.operation(0, 52);
    let version = db.write_batch(operation, vec![put(2)]).unwrap();
    db.shutdown().unwrap();
    drop(db);
    let reopened = fixture.open();
    assert_eq!(
        reopened.write_batch(operation, vec![put(2)]).unwrap(),
        version
    );
    reopened.shutdown().unwrap();
}

#[test]
fn failed_shutdown_joins_checkpoint_before_worker_senders() {
    let fixture = Fixture::new();
    let db = fixture.open();
    db.write_batch(fixture.operation(0, 54), vec![put(1)])
        .unwrap();
    let (entered, waiting) = mpsc::sync_channel(1);
    let (resume, gate) = mpsc::sync_channel(1);
    db.core.test_hooks.lock().checkpoint_prefix_pause = Some((entered, gate));
    db.core.start_checkpoint().unwrap();
    waiting.recv_timeout(Duration::from_secs(5)).unwrap();
    db.core
        .begin_failure(&LocalDbError::new("injected shutdown failure"));
    let (finished, result) = mpsc::sync_channel(1);
    let owner = thread::spawn(move || {
        let result = db.shutdown();
        let _ = finished.send(result);
    });
    let _ = resume.send(());
    let shutdown = match result.recv_timeout(Duration::from_secs(5)) {
        Ok(result) => result,
        Err(error) => {
            // A stuck owner may still hold node connections. Do not join it or
            // block fixture teardown on the very deadlock this test detects.
            std::mem::forget(fixture);
            panic!("checkpoint shutdown did not complete: {error}");
        }
    };
    owner.join().unwrap();
    assert!(shutdown.is_err());
}

#[test]
fn checkpoint_rejects_mismatched_captured_prefix_digest_before_adoption() {
    let fixture = Fixture::new().history(HistoryPersistence::OnDisk);
    let db = fixture.open();
    let operation = fixture.operation(0, 55);
    let version = db.write_batch(operation, vec![put(1)]).unwrap();
    let previous = db
        .core
        .publisher
        .lock()
        .manifest
        .checkpoint
        .as_ref()
        .map(|checkpoint| (checkpoint.epoch, checkpoint.generation));
    db.core.test_hooks.lock().corrupt_checkpoint_proof = true;
    db.core.start_checkpoint().unwrap();
    let error = db.core.progress_checkpoint(true).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("checkpoint prefix differs from published receipts")
    );
    assert_eq!(
        db.core
            .publisher
            .lock()
            .manifest
            .checkpoint
            .as_ref()
            .map(|checkpoint| (checkpoint.epoch, checkpoint.generation)),
        previous
    );
    assert!(db.shutdown().is_err());
    drop(db);
    let reopened = fixture.open();
    assert_eq!(
        reopened.write_batch(operation, vec![put(1)]).unwrap(),
        version
    );
    reopened.shutdown().unwrap();
}

#[test]
fn invalid_checkpoint_prefix_fails_without_adoption() {
    let fixture = Fixture::new().history(HistoryPersistence::OnDisk);
    let db = fixture.open();
    db.write_batch(fixture.operation(0, 53), vec![put(1)])
        .unwrap();
    let previous = db
        .core
        .publisher
        .lock()
        .manifest
        .checkpoint
        .as_ref()
        .map(|checkpoint| (checkpoint.epoch, checkpoint.generation));
    let (reply, pending) = mpsc::sync_channel(1);
    db.core.clients[0]
        .send(Command::Checkpoint(
            db.epoch(),
            db.epoch(),
            usize::MAX,
            reply,
        ))
        .unwrap();
    assert!(
        pending
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .is_err()
    );
    assert!(
        db.core
            .concurrent_failure
            .load(std::sync::atomic::Ordering::Acquire)
    );
    assert_eq!(
        db.core
            .publisher
            .lock()
            .manifest
            .checkpoint
            .as_ref()
            .map(|checkpoint| (checkpoint.epoch, checkpoint.generation)),
        previous
    );
    assert!(db.shutdown().is_err());
}

#[test]
fn checkpoint_over_gap_and_lost_response_replay_sparse_ack_and_retry_identity() {
    checkpoint_over_gap_and_lost_response_replay_sparse_ack_and_retry_identity_mode(
        HistoryPersistence::OnDisk,
    );
}
#[test]
fn wal_replay_checkpoint_over_gap_and_lost_response_replay_sparse_ack_and_retry_identity() {
    checkpoint_over_gap_and_lost_response_replay_sparse_ack_and_retry_identity_mode(
        HistoryPersistence::WalReplay,
    );
}
fn checkpoint_over_gap_and_lost_response_replay_sparse_ack_and_retry_identity_mode(
    mode: HistoryPersistence,
) {
    let fixture = Fixture::new().history(mode);
    let mut db = fixture.open();
    let baseline = fixture.operation(4, 30);
    db.write_batch(baseline, vec![put_key(0, 1)]).unwrap();
    let connection = Arc::clone(&db.core.clients[0].client);
    let held = connection.lock();
    let suffix_connection = Arc::clone(&db.core.clients[1].client);
    let suffix_held = suffix_connection.lock();
    let gap = fixture.operation(0, 31);
    let suffix = fixture.operation(1, 32);
    let gap_version = db.submit(gap, vec![put_key(1, 2)]).unwrap();
    let suffix_version = db.submit(suffix, vec![put_key(2, 3)]).unwrap();
    let other_version = db
        .submit(fixture.operation(1, 33), vec![put_key(3, 4)])
        .unwrap();
    drop(suffix_held);
    super::pipeline_tests::complete_owner(
        &db,
        1,
        &[suffix_version.sequence, other_version.sequence],
    );
    let sparse_published = db.core.is_published(suffix_version);
    let gap_published = db.core.is_published(gap_version);
    let suffix_value = db.get(KeySpace(0), &2u32.to_be_bytes());
    let retry = db.submit(suffix, vec![put_key(2, 3)]);
    // Canonical encoded bytes of the three batches after the baseline: the
    // unpublished gap still holds its payload; published members are measured
    // from their receipts (frame length minus the 8-byte trailer).
    let receipt_bytes = |sequence| {
        (db.core
            .durable_receipt(sequence)
            .unwrap()
            .appended()
            .address()
            .len
            - 8) as usize
    };
    let retained_bytes = db.core.pending_encoded_len(gap_version.sequence).unwrap()
        + receipt_bytes(suffix_version.sequence)
        + receipt_bytes(other_version.sequence);
    let checkpoint_started = db.core.start_checkpoint();
    drop(held);
    assert!(sparse_published);
    assert!(!gap_published);
    assert_eq!(suffix_value.unwrap().unwrap().as_ref(), &[3; 32]);
    assert_eq!(retry.unwrap(), suffix_version);
    checkpoint_started.unwrap();
    db.core.progress_checkpoint(true).unwrap();
    // The released gap publishes on its worker; the cut stays the baseline.
    db.drain().unwrap();
    {
        let publisher = db.core.publisher.lock();
        assert_eq!(publisher.checkpoint_tail_batches, 3);
        assert_eq!(publisher.checkpoint_tail_bytes, retained_bytes);
    }
    let charges = db.core.admission.charges();
    assert_eq!(charges.replay_batches, 3);
    assert_eq!(charges.replay_bytes, retained_bytes);

    // The response loss happens after the stable image was adopted. Recovery
    // must merge both its acknowledged sparse suffix and this uncertain WAL.
    fixture.nodes[2].inject_drop_next_append_response();
    let uncertain = fixture.operation(2, 34);
    let uncertain_version = db.submit(uncertain, vec![put_key(4, 5)]).unwrap();
    assert!(db.drain().is_err());
    assert!(db.core.ensure_healthy().is_err());
    drop(db);

    let mut reopened = fixture.open();
    for (key, expected) in [(2u32, 3u8), (3, 4), (4, 5)] {
        assert_eq!(
            reopened
                .get(KeySpace(0), &key.to_be_bytes())
                .unwrap()
                .unwrap()
                .as_ref(),
            &[expected; 32]
        );
    }
    assert_eq!(
        reopened.submit(suffix, vec![put_key(2, 3)]).unwrap(),
        suffix_version
    );
    assert_eq!(
        reopened.submit(uncertain, vec![put_key(4, 5)]).unwrap(),
        uncertain_version
    );
    assert!(reopened.submit(suffix, vec![put_key(2, 9)]).is_err());
    reopened.shutdown().unwrap();
}

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
    assert_eq!(held.append_flights_in_flight(), 0);
    // Simulate abrupt authority loss with the accepted batch still in its
    // owner queue and no transport submission. begin_failure stores the
    // shared worker failure flag before the owner client is released.
    db.core
        .begin_failure(&LocalDbError::new("injected coordinator loss"));
    drop(held);
    drop(db);

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
    assert_eq!(
        reopened.submit(acknowledged, vec![put_key(2, 2)]).unwrap(),
        acknowledged_version
    );
    // An unacknowledged absent batch can be admitted in the new epoch; its
    // original sequence must not be reused as though the old write succeeded.
    let retry = reopened.write_batch(missing, vec![put_key(1, 1)]).unwrap();
    assert!(retry.epoch > missing_version.epoch);
    reopened.shutdown().unwrap();
}

#[test]
fn acknowledged_suffix_survives_every_background_checkpoint_adoption_stage() {
    acknowledged_suffix_survives_every_background_checkpoint_adoption_stage_mode(
        HistoryPersistence::OnDisk,
    );
}
#[test]
fn wal_replay_acknowledged_suffix_survives_every_background_checkpoint_adoption_stage() {
    acknowledged_suffix_survives_every_background_checkpoint_adoption_stage_mode(
        HistoryPersistence::WalReplay,
    );
}
fn acknowledged_suffix_survives_every_background_checkpoint_adoption_stage_mode(
    mode: HistoryPersistence,
) {
    for stage in 0..=8 {
        let fixture = Fixture::new().history(mode);
        let mut db = fixture.open();
        let first = fixture.operation(0, 1);
        let captured = fixture.operation(0, 2);
        let suffix = fixture.operation(0, 3);
        let first_version = db.write_batch(first, vec![put(1)]).unwrap();
        db.flush_index().unwrap();
        let captured_version = db.write_batch(captured, vec![put(2)]).unwrap();
        let (entered, waiting) = mpsc::sync_channel(1);
        let (resume, gate) = mpsc::sync_channel(1);
        {
            let mut hooks = db.core.test_hooks.lock();
            hooks.checkpoint_pause = Some((entered, gate));
            hooks.checkpoint_interruption = Some(stage);
        }
        db.core.start_checkpoint().unwrap();
        waiting.recv_timeout(Duration::from_secs(5)).unwrap();
        // This is the same physical stream as the frozen prefix. Its later
        // append must not invalidate the earlier exact-prefix checkpoint proof.
        let suffix_version = db.write_batch(suffix, vec![put(3)]).unwrap();
        assert_eq!(
            db.authority_write_stats()
                .background_checkpoint_suffix_batches,
            1
        );
        assert_eq!(
            db.get(KeySpace(0), &1u32.to_be_bytes())
                .unwrap()
                .unwrap()
                .as_ref(),
            &[3; 32]
        );
        resume.send(()).unwrap();
        assert!(db.core.progress_checkpoint(true).is_err());
        if stage == 4 && mode != HistoryPersistence::WalReplay {
            use std::os::unix::fs::FileExt;
            let ordinal = db
                .core
                .publisher
                .lock()
                .receipt_history
                .as_ref()
                .unwrap()
                .counts[&(0, db.epoch())];
            let stream = db
                .core
                .path
                .join(format!("receipt-history-v2/stream-0-{}.pages", db.epoch()));
            let file = fs::OpenOptions::new().write(true).open(stream).unwrap();
            // Full-length torn metadata outside the adopted prefix is not
            // authority. Recovery must rebuild it from durable complete WALs.
            file.write_all_at(&[0; 256], ordinal as u64 * 256).unwrap();
            file.sync_all().unwrap();
        }
        drop(db);

        let mut reopened = fixture.open();
        assert_eq!(
            reopened
                .get(KeySpace(0), &1u32.to_be_bytes())
                .unwrap()
                .unwrap()
                .as_ref(),
            &[3; 32]
        );
        assert_eq!(
            reopened.write_batch(first, vec![put(1)]).unwrap(),
            first_version
        );
        assert_eq!(
            reopened.write_batch(captured, vec![put(2)]).unwrap(),
            captured_version
        );
        assert_eq!(
            reopened.write_batch(suffix, vec![put(3)]).unwrap(),
            suffix_version
        );
        assert!(reopened.write_batch(first, vec![put(9)]).is_err());
        reopened.validate_cached_read().unwrap();
    }
}

#[test]
fn failed_staging_intent_fences_serving_without_authorizing_outputs() {
    let fixture = Fixture::new();
    let db = fixture.open();
    db.write_batch(fixture.operation(0, 1), vec![put(1)])
        .unwrap();
    let (entered, waiting) = mpsc::sync_channel(1);
    let (resume, gate) = mpsc::sync_channel(1);
    db.core.test_hooks.lock().checkpoint_pause = Some((entered, gate));
    db.core.start_checkpoint().unwrap();
    waiting.recv_timeout(Duration::from_secs(5)).unwrap();
    let staged = db
        .core
        .publisher
        .lock()
        .manifest
        .checkpoint_staging
        .as_ref()
        .unwrap()
        .names();

    // Persistence is blocked before the intent reaches disk. Owner WAL progress
    // remains available, but no output can be written without the intent.
    db.write_batch(fixture.operation(1, 2), vec![put(2)])
        .unwrap();
    let pending = db.core.path.join(format!(".{MANIFEST}.pending"));
    fs::create_dir(&pending).unwrap();
    resume.send(()).unwrap();
    assert!(db.core.progress_checkpoint(true).is_err());
    assert!(db.core.ensure_healthy().is_err());
    assert!(db.ensure_healthy().is_err());

    let persisted: AuthorityManifest = read_metadata(&db.core.path.join(MANIFEST)).unwrap();
    assert!(persisted.checkpoint_staging.is_none());
    assert!(persisted.checkpoint_obsolete.is_none());
    assert!(persisted.checkpoint.is_none());
    for name in staged {
        assert!(!db.core.path.join(name).exists());
    }

    fs::remove_dir(pending).unwrap();
    drop(db);
    let mut reopened = fixture.open();
    assert_eq!(
        reopened
            .get(KeySpace(0), &1u32.to_be_bytes())
            .unwrap()
            .unwrap()
            .as_ref(),
        &[2; 32]
    );
}

#[test]
fn completed_owner_metadata_is_bounded_across_lifetime_capacity_and_reopens() {
    let fixture = Fixture::new_with_capacity(8).history(HistoryPersistence::OnDisk);
    let original = fixture.operation(0, 100);
    let mut original_version = None;
    let mut resident_bound = None;
    let mut next = 100u64;
    for _ in 0..3 {
        let db = fixture.open();
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
                assert!(
                    db.core.publisher.lock().known.len() <= fixture.config.max_admitted_batches
                );
            }
            db.flush_index().unwrap();
            assert!(
                db.core.publisher.lock().known.is_empty(),
                "completed outcomes must page out after durable adoption"
            );
            let charge = db.metadata_history_bytes();
            let bound = *resident_bound.get_or_insert(charge);
            assert!(
                charge <= bound,
                "resident completed bookkeeping grew with lifetime history"
            );
            assert_eq!(
                db.write_batch(original, vec![put(100)]).unwrap(),
                original_version.unwrap()
            );
        }
        db.shutdown().unwrap();
    }
}

#[test]
fn absent_or_corrupt_pageable_retry_authority_fails_closed() {
    for missing in [false, true] {
        let fixture = Fixture::new().history(HistoryPersistence::OnDisk);
        let db = fixture.open();
        let operation = fixture.operation(0, 1);
        db.write_batch(operation, vec![put(1)]).unwrap();
        db.flush_index().unwrap();
        let nodes = db.core.path.join("receipt-history-v2/nodes.pages");
        if missing {
            fs::remove_file(&nodes).unwrap();
        } else {
            use std::os::unix::fs::FileExt;
            let file = fs::OpenOptions::new().write(true).open(&nodes).unwrap();
            file.write_all_at(&[0; 256], 0).unwrap();
            file.sync_all().unwrap();
        }
        // Reopen, rather than a live external file mutation, is the supported
        // authority-corruption boundary. Never treat a missing root as no IDs.
        db.core
            .begin_failure(&LocalDbError::new("injected crash before reopen"));
        drop(db);
        assert!(
            RemoteDatabase::open(
                &fixture.temp.path().join("authority"),
                &fixture.shape,
                fixture.config.clone(),
                Metrics::new_in_enabled(&prometheus::Registry::new(), false),
            )
            .is_err()
        );
    }
}

/// Reserve one grant through the real caller Admission path. A blocked
/// checkpoint-bound dimension requests a checkpoint with no lock held.
fn reserve_pressure(
    core: &Arc<write_core::core::WriteCore>,
    grant: concurrent::WriteGrant,
    block: bool,
) -> Result<concurrent::WriteCredit, LocalDbError> {
    core.admission.reserve_grant(
        grant,
        block,
        || core.ensure_writable(),
        |_| core.request_checkpoint(),
    )
}

fn index_grant(index_bytes: usize) -> concurrent::WriteGrant {
    concurrent::WriteGrant {
        requests: 1,
        index_bytes,
        ..concurrent::WriteGrant::default()
    }
}

#[test]
fn index_pressure_waits_for_existing_capture_before_rejecting_live_residency() {
    let fixture = Fixture::new();
    let db = fixture.open();
    db.write_batch(fixture.operation(0, 1), vec![put(1)])
        .unwrap();

    let resident = db.index_stats().charged_metadata_bytes;
    let additional_bytes = db
        .core
        .config
        .max_index_bytes
        .checked_sub(resident)
        .unwrap()
        .checked_add(1)
        .unwrap();
    assert!(db.core.index.check_capacity(additional_bytes).is_err());

    let (entered, waiting) = mpsc::sync_channel(1);
    let (resume, gate) = mpsc::sync_channel(1);
    db.core.test_hooks.lock().checkpoint_pause = Some((entered, gate));
    db.core.start_checkpoint().unwrap();
    waiting.recv_timeout(Duration::from_secs(5)).unwrap();
    let generation = db.core.publisher.lock().snapshot_generation;
    let release = std::thread::spawn(move || resume.send(()).unwrap());

    // The blocked index grant is granted by the existing capture's adoption.
    let credit = reserve_pressure(&db.core, index_grant(additional_bytes), true).unwrap();
    release.join().unwrap();
    db.core.progress_checkpoint(true).unwrap();

    assert_eq!(db.core.publisher.lock().snapshot_generation, generation);
    assert!(db.core.publisher.lock().background_checkpoint.is_none());
    db.core.index.check_capacity(additional_bytes).unwrap();
    drop(credit);
}

#[test]
fn drained_partition_pressure_does_not_wait_for_an_unrelated_capture() {
    let mut fixture = Fixture::new_with_capacity(1);
    fixture.config.max_admitted_batches = 4;
    fixture.config.max_pending_batches = 4;
    let mut db = fixture.open();
    db.write_batch(fixture.operation(0, 1), vec![put(1)])
        .unwrap();
    let (entered, waiting) = mpsc::sync_channel(1);
    let (resume, gate) = mpsc::sync_channel(1);
    db.core.test_hooks.lock().checkpoint_pause = Some((entered, gate));
    db.core.start_checkpoint().unwrap();
    waiting.recv_timeout(Duration::from_secs(5)).unwrap();

    let connection = Arc::clone(&db.core.clients[1].client);
    let held = connection.lock();
    db.submit(fixture.operation(1, 2), vec![put(2)]).unwrap();
    let partition_grant = || {
        let mut grant = concurrent::WriteGrant {
            requests: 1,
            ..concurrent::WriteGrant::default()
        };
        grant.add_partition(1, 1).unwrap();
        grant
    };
    let initially_full = reserve_pressure(&db.core, partition_grant(), false).is_err();
    let (finished, completion) = mpsc::sync_channel(1);
    let core = Arc::clone(&db.core);
    let owner = thread::spawn(move || {
        let result = reserve_pressure(&core, partition_grant(), true);
        finished.send(result.is_ok()).unwrap();
        result.map(drop)
    });
    drop(held);
    // The channel bounds a dependency deadlock, not a device-latency target.
    let observed = completion.recv_timeout(Duration::from_secs(5));
    resume.send(()).unwrap();
    let result = owner.join().unwrap();
    assert!(initially_full);
    assert!(observed.unwrap());
    result.unwrap();
    assert_eq!(
        db.get(KeySpace(0), &1u32.to_be_bytes())
            .unwrap()
            .unwrap()
            .as_ref(),
        &[2; 32]
    );
    db.flush_index().unwrap();
}

#[test]
fn adopted_capture_releases_identity_pressure_without_waiting_for_another_cut() {
    let fixture = Fixture::new();
    let db = fixture.open();
    db.write_batch(fixture.operation(0, 1), vec![put(1)])
        .unwrap();
    let (entered, waiting) = mpsc::sync_channel(1);
    let (resume, gate) = mpsc::sync_channel(1);
    db.core.test_hooks.lock().checkpoint_pause = Some((entered, gate));
    db.core.start_checkpoint().unwrap();
    waiting.recv_timeout(Duration::from_secs(5)).unwrap();
    let mut suffix = None;
    for sequence in 1..fixture.config.max_admitted_batches {
        let operation = fixture.operation((sequence % 5) as u32, sequence as u64 + 1);
        let version = db.write_batch(operation, vec![put(2)]).unwrap();
        suffix = Some((operation, version));
    }
    let identity_grant = || concurrent::WriteGrant {
        requests: 1,
        identities: 1,
        replay_batches: 1,
        replay_bytes: fixture.config.storage.batch_limits.max_encoded_bytes,
        ..concurrent::WriteGrant::default()
    };
    assert!(reserve_pressure(&db.core, identity_grant(), false).is_err());
    let (next_entered, _next_waiting) = mpsc::sync_channel(1);
    let (next_resume, next_gate) = mpsc::sync_channel(1);
    db.core.test_hooks.lock().checkpoint_pause = Some((next_entered, next_gate));
    let (finished, completion) = mpsc::sync_channel(1);
    let core = Arc::clone(&db.core);
    let grant = identity_grant();
    let owner = thread::spawn(move || {
        let result = reserve_pressure(&core, grant, true);
        finished.send(result.is_ok()).unwrap();
        result.map(drop)
    });
    resume.send(()).unwrap();
    // An unnecessary full catch-up would now depend on next_resume.
    let observed = completion.recv_timeout(Duration::from_secs(5));
    // Sampled before next_resume: a successor capture cannot have adopted.
    let tail = db.core.publisher.lock().checkpoint_tail_batches;
    next_resume.send(()).unwrap();
    let result = owner.join().unwrap();
    assert!(observed.unwrap());
    result.unwrap();
    assert_eq!(tail, fixture.config.max_admitted_batches - 1);
    db.core
        .begin_failure(&LocalDbError::new("injected crash before reopen"));
    drop(db);
    let mut reopened = fixture.open();
    assert_eq!(
        reopened
            .get(KeySpace(0), &1u32.to_be_bytes())
            .unwrap()
            .unwrap()
            .as_ref(),
        &[2; 32]
    );
    let (operation, version) = suffix.unwrap();
    assert_eq!(
        reopened.write_batch(operation, vec![put(2)]).unwrap(),
        version
    );
}

#[test]
fn remaining_index_pressure_requires_one_new_capture_after_the_existing_cut() {
    remaining_index_pressure_case(false);
}

/// The blocked caller queues and requests a checkpoint while the existing
/// capture is still paused, so that request starts nothing. Adopting the
/// existing capture frees too little and grants nobody; only the checkpoint
/// thread's wake of the FIFO head lets the caller request the successor
/// capture. Without that wake the caller sleeps to its admission deadline.
#[test]
fn checkpoint_finish_wakes_index_blocked_caller_to_request_successor_capture() {
    remaining_index_pressure_case(true);
}

fn remaining_index_pressure_case(parked_before_resume: bool) {
    let mut fixture = Fixture::new();
    let db = fixture.open();
    let mutations = |value| {
        (1u32..=3)
            .map(|key| Mutation::Put {
                keyspace: KeySpace(0),
                key: key.to_be_bytes().to_vec().into(),
                value: vec![value; 32].into(),
            })
            .collect::<Vec<_>>()
    };
    db.write_batch(fixture.operation(0, 1), mutations(0))
        .unwrap();
    db.flush_index().unwrap();
    let resident_directory = db.index_stats().charged_metadata_bytes;
    let incoming = CompleteBatch::new(
        &fixture.shape,
        BatchId {
            database: fixture.config.storage.database,
            operation: fixture.operation(2, 4),
        },
        BatchVersion {
            epoch: db.epoch(),
            sequence: db.core.next_sequence(),
        },
        mutations(3),
        fixture.config.storage.batch_limits,
    )
    .unwrap();
    let additional_bytes = db.core.index.estimated_batch_bytes(&incoming).unwrap();
    db.shutdown().unwrap();
    drop(db);
    fixture.config.max_index_bytes = resident_directory.checked_add(additional_bytes).unwrap();
    // Open reserves the publisher's standing sparse-view budget (two views of
    // max_pending_batches - 1 exclusions) against this same byte limit,
    // independently of residency; this tight limit cannot hold it at the
    // fixture's 64. These writes are sequential, so one pending batch makes
    // that budget zero and leaves the residency bound exactly this tight.
    fixture.config.max_pending_batches = 1;
    let mut db = fixture.open();
    let (entered, waiting) = mpsc::sync_channel(1);
    let (resume, gate) = mpsc::sync_channel(1);
    db.core.test_hooks.lock().checkpoint_pause = Some((entered, gate));
    db.write_batch(fixture.operation(0, 2), vec![put(1)])
        .unwrap();
    db.core.start_checkpoint().unwrap();
    waiting.recv_timeout(Duration::from_secs(5)).unwrap();
    db.write_batch(fixture.operation(1, 3), vec![put(2)])
        .unwrap();
    let generation = db.core.publisher.lock().snapshot_generation;
    assert!(reserve_pressure(&db.core, index_grant(additional_bytes), false).is_err());
    // The existing capture predates the second write; only one new capture
    // after its cut frees the remaining index residency.
    let credit = if parked_before_resume {
        let (requested, request_seen) = mpsc::sync_channel(1);
        let (finished, outcome) = mpsc::sync_channel(1);
        let core = Arc::clone(&db.core);
        let caller = thread::spawn(move || {
            let result = core.admission.reserve_grant(
                index_grant(additional_bytes),
                true,
                || core.ensure_writable(),
                |_| {
                    let result = core.request_checkpoint();
                    // Queued before this callback; a wake that precedes the
                    // park is kept by the thread's unpark token.
                    let _ = requested.try_send(());
                    result
                },
            );
            let _ = finished.send(result.is_ok());
            result
        });
        request_seen.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(db.core.publisher.lock().snapshot_generation, generation);
        resume.send(()).unwrap();
        // The channel bounds a lost wake (30 s admission deadline), not a
        // device-latency target.
        let observed = outcome.recv_timeout(Duration::from_secs(10));
        assert_eq!(
            observed,
            Ok(true),
            "checkpoint finish left the caller parked"
        );
        caller.join().unwrap().unwrap()
    } else {
        resume.send(()).unwrap();
        reserve_pressure(&db.core, index_grant(additional_bytes), true).unwrap()
    };
    db.core.progress_checkpoint(true).unwrap();
    assert_eq!(db.core.publisher.lock().snapshot_generation, generation + 1);
    assert!(db.core.publisher.lock().background_checkpoint.is_none());
    db.core.index.check_capacity(additional_bytes).unwrap();
    drop(credit);
    assert_eq!(
        db.get(KeySpace(0), &1u32.to_be_bytes())
            .unwrap()
            .unwrap()
            .as_ref(),
        &[2; 32]
    );
    db.flush_index().unwrap();
}

/// The standing sparse-view budget is reserved once at open. A max_index_bytes
/// that cannot hold it fails open before serving, with a clear error and no
/// effect on recoverable state.
#[test]
fn open_fails_cleanly_when_index_bytes_cannot_hold_standing_view_budget() {
    let mut fixture = Fixture::new();
    let db = fixture.open();
    db.write_batch(fixture.operation(0, 1), vec![put(7)])
        .unwrap();
    // No view is installed and no capture runs: the transient charge is
    // exactly the standing budget, unchanged by the write.
    let stats = db.index_stats();
    let budget = stats.transient_metadata_bytes;
    assert!(budget > 0);
    db.shutdown().unwrap();
    drop(db);
    fixture.config.max_index_bytes = budget - 1;
    assert!(fixture.config.max_index_bytes >= stats.charged_metadata_bytes);
    let open = |config: &RemoteDatabaseConfig| {
        RemoteDatabase::open(
            &fixture.temp.path().join("authority"),
            &fixture.shape,
            config.clone(),
            Metrics::new_in_enabled(&prometheus::Registry::new(), false),
        )
    };
    let error = open(&fixture.config)
        .err()
        .expect("open must reserve the standing view budget");
    assert!(
        error
            .to_string()
            .contains("cannot hold the standing sparse publication view budget"),
        "{error}"
    );
    fixture.config.max_index_bytes = budget;
    let mut reopened = open(&fixture.config).unwrap();
    assert_eq!(reopened.index_stats().transient_metadata_bytes, budget);
    assert_eq!(
        reopened
            .get(KeySpace(0), &1u32.to_be_bytes())
            .unwrap()
            .unwrap()
            .as_ref(),
        &[7; 32]
    );
    reopened.shutdown().unwrap();
}

#[test]
fn unchanged_flush_waits_for_captured_cut_and_then_covers_its_acknowledged_suffix() {
    let fixture = Fixture::new();
    let db = fixture.open();
    db.write_batch(fixture.operation(0, 1), vec![put(1)])
        .unwrap();
    let (entered, waiting) = mpsc::sync_channel(1);
    let (resume, gate) = mpsc::sync_channel(1);
    db.core.test_hooks.lock().checkpoint_pause = Some((entered, gate));
    db.core.start_checkpoint().unwrap();
    waiting.recv_timeout(Duration::from_secs(5)).unwrap();
    db.write_batch(fixture.operation(1, 2), vec![put(2)])
        .unwrap();
    resume.send(()).unwrap();
    db.flush_index().unwrap();
    {
        let publisher = db.core.publisher.lock();
        assert_eq!(publisher.checkpoint_tail_batches, 0);
        assert_eq!(publisher.checkpoint_tail_bytes, 0);
        assert!(publisher.background_checkpoint.is_none());
        assert!(publisher.known.is_empty());
    }
    db.core
        .begin_failure(&LocalDbError::new("injected crash before reopen"));
    drop(db);
    let reopened = fixture.open();
    assert_eq!(reopened.recovery_stats().tail_batches, 0);
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
    for ((operation, mutations), version) in writes.into_iter().zip(versions) {
        assert_eq!(reopened.write_batch(operation, mutations).unwrap(), version);
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

    let mut reopened = fixture.open();
    assert_eq!(reopened.recovery_stats().tail_batches, 5);
    for (operation, value, version) in identities {
        assert_eq!(
            reopened.submit(operation, vec![put(value)]).unwrap(),
            version
        );
    }
    assert_eq!(
        reopened.get(KeySpace(0), &1u32.to_be_bytes()).unwrap(),
        Some(vec![3; 32].into())
    );
    assert_eq!(reopened.stats().admitted_batches, 0);
    reopened.shutdown().unwrap();
}
