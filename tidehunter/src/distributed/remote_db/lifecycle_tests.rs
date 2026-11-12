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
    let mut db = fixture.open();
    db.write_batch(fixture.operation(0, 70), vec![put(1)])
        .unwrap();
    let monitor = db.read_evidence_monitor();
    let (entered, release) = fixture.nodes[0].pause_inventory();
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    let (reply, proof) = mpsc::sync_channel(1);
    db.clients[0]
        .send(Command::Checkpoint(db.epoch, db.epoch, 1, reply))
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if db.clients[0].client.lock().checkpoint_control_pending() {
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
    db.clients[0]
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
        let receipt = *db.known.values().next().unwrap();
        let expected = super::super::tcp::checkpoint::prefix_digest(
            db.config.storage.database,
            0,
            db.epoch,
            std::iter::once(receipt.into()),
        );
        let (entered, release) = fixture.nodes[0].pause_inventory();
        entered.recv_timeout(Duration::from_secs(5)).unwrap();
        let (reply, proof) = mpsc::sync_channel(1);
        db.clients[0]
            .send(Command::Checkpoint(db.epoch, db.epoch, 1, reply))
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        while !db.clients[0].client.lock().checkpoint_control_pending() {
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
fn nonpipelined_write_behind_pending_checkpoint_is_durable_and_recoverable() {
    let fixture = Fixture::new();
    let mut db = fixture.open();
    db.config.pipelined_writes = false;
    db.write_batch(fixture.operation(0, 74), vec![put(1)])
        .unwrap();
    let (entered, release) = fixture.nodes[0].pause_inventory();
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    db.start_checkpoint().unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while !db.clients[0].client.lock().checkpoint_control_pending() {
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
    let (mut db, outcome) = match result.recv_timeout(Duration::from_secs(5)) {
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
    let mut db = fixture.open();
    db.write_batch(fixture.operation(0, 76), vec![put(1)])
        .unwrap();
    let (entered, release) = fixture.nodes[0].pause_inventory();
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    let (reply, proof) = mpsc::sync_channel(1);
    db.clients[0]
        .send(Command::Checkpoint(db.epoch, db.epoch, 1, reply))
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while !db.clients[0].client.lock().checkpoint_control_pending() {
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
    let mut db = fixture.open();
    db.write_batch(fixture.operation(0, 71), vec![put(1)])
        .unwrap();
    let before = db.pipeline_stats.direct_appends;
    let (entered, waiting) = mpsc::sync_channel(1);
    let (resume, gate) = mpsc::sync_channel(1);
    db.checkpoint_prefix_pause = Some((entered, gate));
    db.start_checkpoint().unwrap();
    waiting.recv_timeout(Duration::from_secs(5)).unwrap();
    let operation = fixture.operation(0, 72);
    let (finished, result) = mpsc::sync_channel(1);
    let owner = thread::spawn(move || {
        let outcome = db.write_batch(operation, vec![put(2)]);
        let direct_appends = db.pipeline_stats.direct_appends;
        let _ = finished.send((db, outcome, direct_appends));
    });
    let before_resume = result.recv_timeout(Duration::from_secs(5));
    let _ = resume.send(());
    let (mut db, outcome, direct_appends) = match before_resume {
        Ok(result) => result,
        Err(error) => {
            std::mem::forget(fixture);
            panic!("scalar write did not finish before checkpoint verification: {error}");
        }
    };
    owner.join().unwrap();
    let version = outcome.unwrap();
    assert_eq!(direct_appends, before);
    db.shutdown().unwrap();
    drop(db);
    let mut reopened = fixture.open();
    assert_eq!(
        reopened.write_batch(operation, vec![put(2)]).unwrap(),
        version
    );
    reopened.shutdown().unwrap();
}

#[test]
fn cancelled_checkpoint_reply_drains_before_worker_shutdown() {
    let fixture = Fixture::new().history(HistoryPersistence::OnDisk);
    let mut db = fixture.open();
    db.write_batch(fixture.operation(0, 51), vec![put(1)])
        .unwrap();
    let (reply, cancelled) = mpsc::sync_channel(1);
    db.clients[0]
        .send(Command::Checkpoint(db.epoch, db.epoch, 1, reply))
        .unwrap();
    drop(cancelled);
    let operation = fixture.operation(0, 52);
    let version = db.write_batch(operation, vec![put(2)]).unwrap();
    db.shutdown().unwrap();
    drop(db);
    let mut reopened = fixture.open();
    assert_eq!(
        reopened.write_batch(operation, vec![put(2)]).unwrap(),
        version
    );
    reopened.shutdown().unwrap();
}

#[test]
fn failed_shutdown_joins_checkpoint_before_worker_senders() {
    let fixture = Fixture::new();
    let mut db = fixture.open();
    db.write_batch(fixture.operation(0, 54), vec![put(1)])
        .unwrap();
    let (entered, waiting) = mpsc::sync_channel(1);
    let (resume, gate) = mpsc::sync_channel(1);
    db.checkpoint_prefix_pause = Some((entered, gate));
    db.start_checkpoint().unwrap();
    waiting.recv_timeout(Duration::from_secs(5)).unwrap();
    db.concurrent_failure
        .store(true, std::sync::atomic::Ordering::Release);
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
    let mut db = fixture.open();
    let operation = fixture.operation(0, 55);
    let version = db.write_batch(operation, vec![put(1)]).unwrap();
    let previous = db
        .manifest
        .checkpoint
        .as_ref()
        .map(|checkpoint| (checkpoint.epoch, checkpoint.generation));
    db.corrupt_checkpoint_proof = true;
    db.start_checkpoint().unwrap();
    let error = db.progress_checkpoint(true).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("checkpoint prefix differs from published receipts")
    );
    assert_eq!(
        db.manifest
            .checkpoint
            .as_ref()
            .map(|checkpoint| (checkpoint.epoch, checkpoint.generation)),
        previous
    );
    assert!(db.shutdown().is_err());
    drop(db);
    let mut reopened = fixture.open();
    assert_eq!(
        reopened.write_batch(operation, vec![put(1)]).unwrap(),
        version
    );
    reopened.shutdown().unwrap();
}

#[test]
fn invalid_checkpoint_prefix_fails_without_adoption() {
    let fixture = Fixture::new().history(HistoryPersistence::OnDisk);
    let mut db = fixture.open();
    db.write_batch(fixture.operation(0, 53), vec![put(1)])
        .unwrap();
    let previous = db
        .manifest
        .checkpoint
        .as_ref()
        .map(|checkpoint| (checkpoint.epoch, checkpoint.generation));
    let (reply, pending) = mpsc::sync_channel(1);
    db.clients[0]
        .send(Command::Checkpoint(db.epoch, db.epoch, usize::MAX, reply))
        .unwrap();
    assert!(
        pending
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .is_err()
    );
    assert!(
        db.concurrent_failure
            .load(std::sync::atomic::Ordering::Acquire)
    );
    assert_eq!(
        db.manifest
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
    let connection = Arc::clone(&db.clients[0].client);
    let held = connection.lock();
    let gap = fixture.operation(0, 31);
    let suffix = fixture.operation(1, 32);
    let gap_version = db.submit(gap, vec![put_key(1, 2)]).unwrap();
    let suffix_version = db.submit(suffix, vec![put_key(2, 3)]).unwrap();
    db.submit(fixture.operation(1, 33), vec![put_key(3, 4)])
        .unwrap();
    let group = db.inflight[1].pop_front().unwrap();
    db.inflight_bytes[1] -= group.charge;
    let receipts = group
        .reply
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    db.accept_receipts(receipts).unwrap();
    db.publish_completed().unwrap();
    let sparse_published = db.publication.is_published(suffix_version);
    let gap_published = db.publication.is_published(gap_version);
    let suffix_value = db.get(KeySpace(0), &2u32.to_be_bytes());
    let retry = db.submit(suffix, vec![put_key(2, 3)]);
    let retained_bytes: usize = db.submissions.iter().map(|entry| entry.encoded_bytes).sum();
    let checkpoint_started = db.start_checkpoint();
    drop(held);
    assert!(sparse_published);
    assert!(!gap_published);
    assert_eq!(suffix_value.unwrap().unwrap().as_ref(), &[3; 32]);
    assert_eq!(retry.unwrap(), suffix_version);
    checkpoint_started.unwrap();
    db.progress_checkpoint(true).unwrap();
    assert_eq!(db.checkpoint_tail_batches, 3);
    assert_eq!(db.checkpoint_tail_bytes, retained_bytes);

    // The response loss happens after the stable image was adopted. Recovery
    // must merge both its acknowledged sparse suffix and this uncertain WAL.
    fixture.nodes[2].inject_drop_next_append_response();
    let uncertain = fixture.operation(2, 34);
    let uncertain_version = db.submit(uncertain, vec![put_key(4, 5)]).unwrap();
    assert!(db.drain().is_err());
    assert!(!db.healthy);
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
    let mut db = fixture.open();
    let missing = fixture.operation(0, 40);
    let acknowledged = fixture.operation(1, 41);
    let missing_version = db.submit(missing, vec![put_key(1, 1)]).unwrap();
    let acknowledged_version = db.submit(acknowledged, vec![put_key(2, 2)]).unwrap();
    db.submit(fixture.operation(1, 42), vec![put_key(3, 3)])
        .unwrap();
    let group = db.inflight[1].pop_front().unwrap();
    db.inflight_bytes[1] -= group.charge;
    let receipts = group
        .reply
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    db.accept_receipts(receipts).unwrap();
    db.publish_completed().unwrap();
    assert!(!db.publication.is_published(missing_version));
    assert!(db.publication.is_published(acknowledged_version));
    assert_eq!(db.staged[0].len(), 1);
    assert!(db.inflight[0].is_empty());
    // Simulate abrupt authority loss without allowing Drop to dispatch the
    // earlier batch. Its only copy is still in this process's staging queue.
    db.concurrent_failure
        .store(true, std::sync::atomic::Ordering::Release);
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
        db.checkpoint_pause = Some((entered, gate));
        db.checkpoint_interruption = Some(stage);
        db.start_checkpoint().unwrap();
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
        assert!(db.progress_checkpoint(true).is_err());
        if stage == 4 && mode != HistoryPersistence::WalReplay {
            use std::os::unix::fs::FileExt;
            let ordinal = db.receipt_history.as_ref().unwrap().counts[&(0, db.epoch)];
            let stream = db
                .path
                .join(format!("receipt-history-v2/stream-0-{}.pages", db.epoch));
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
    let mut db = fixture.open();
    db.write_batch(fixture.operation(0, 1), vec![put(1)])
        .unwrap();
    let (entered, waiting) = mpsc::sync_channel(1);
    let (resume, gate) = mpsc::sync_channel(1);
    db.checkpoint_pause = Some((entered, gate));
    db.start_checkpoint().unwrap();
    waiting.recv_timeout(Duration::from_secs(5)).unwrap();
    let staged = db.manifest.checkpoint_staging.as_ref().unwrap().names();

    // Persistence is blocked before the intent reaches disk. Owner WAL progress
    // remains available, but no output can be written without the intent.
    db.write_batch(fixture.operation(1, 2), vec![put(2)])
        .unwrap();
    let pending = db.path.join(format!(".{MANIFEST}.pending"));
    fs::create_dir(&pending).unwrap();
    resume.send(()).unwrap();
    assert!(db.progress_checkpoint(true).is_err());
    assert!(!db.healthy);
    assert!(db.ensure_healthy().is_err());

    let persisted: AuthorityManifest = read_metadata(&db.path.join(MANIFEST)).unwrap();
    assert!(persisted.checkpoint_staging.is_none());
    assert!(persisted.checkpoint_obsolete.is_none());
    assert!(persisted.checkpoint.is_none());
    for name in staged {
        assert!(!db.path.join(name).exists());
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
        let mut db = fixture.open();
        for _ in 0..4 {
            for _ in 0..16 {
                let operation = fixture.operation((next % 5) as u32, next);
                let version = db.write_batch(operation, vec![put(next as u8)]).unwrap();
                if next == 100 {
                    original_version = Some(version);
                }
                next += 1;
                assert!(db.submissions.is_empty());
                assert_eq!(db.publication.retained_batches(), 0);
                assert!(db.known.len() <= fixture.config.max_admitted_batches);
            }
            db.flush_index().unwrap();
            assert!(
                db.known.is_empty(),
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
        let mut db = fixture.open();
        let operation = fixture.operation(0, 1);
        db.write_batch(operation, vec![put(1)]).unwrap();
        db.flush_index().unwrap();
        let nodes = db.path.join("receipt-history-v2/nodes.pages");
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
        db.healthy = false;
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

#[test]
fn index_pressure_waits_for_existing_capture_before_rejecting_live_residency() {
    let fixture = Fixture::new();
    let mut db = fixture.open();
    db.write_batch(fixture.operation(0, 1), vec![put(1)])
        .unwrap();

    let resident = db.index_stats().charged_metadata_bytes;
    let additional_bytes = db
        .config
        .max_index_bytes
        .checked_sub(resident)
        .unwrap()
        .checked_add(1)
        .unwrap();
    assert!(db.index.check_capacity(additional_bytes).is_err());

    let (entered, waiting) = mpsc::sync_channel(1);
    let (resume, gate) = mpsc::sync_channel(1);
    db.checkpoint_pause = Some((entered, gate));
    db.start_checkpoint().unwrap();
    waiting.recv_timeout(Duration::from_secs(5)).unwrap();
    let generation = db.snapshot_generation;
    let release = std::thread::spawn(move || resume.send(()).unwrap());

    db.ensure_admission_pressure(AdmissionPressure::Index { additional_bytes })
        .unwrap();
    release.join().unwrap();

    assert_eq!(db.snapshot_generation, generation);
    assert!(db.background_checkpoint.is_none());
    db.index.check_capacity(additional_bytes).unwrap();
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
    db.checkpoint_pause = Some((entered, gate));
    db.start_checkpoint().unwrap();
    waiting.recv_timeout(Duration::from_secs(5)).unwrap();

    let connection = Arc::clone(&db.clients[1].client);
    let held = connection.lock();
    db.submit(fixture.operation(1, 2), vec![put(2)]).unwrap();
    let pressure = AdmissionPressure::Partition {
        partition: 1,
        records: 1,
    };
    let initially_full = !db.admission_pressure_fits(pressure);
    let (finished, completion) = mpsc::sync_channel(1);
    let owner = thread::spawn(move || {
        let result = db.ensure_admission_pressure(pressure);
        finished.send(result.is_ok()).unwrap();
        (db, result)
    });
    drop(held);
    // The channel bounds a dependency deadlock, not a device-latency target.
    let observed = completion.recv_timeout(Duration::from_secs(5));
    resume.send(()).unwrap();
    let (mut db, result) = owner.join().unwrap();
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
    let mut db = fixture.open();
    db.write_batch(fixture.operation(0, 1), vec![put(1)])
        .unwrap();
    let (entered, waiting) = mpsc::sync_channel(1);
    let (resume, gate) = mpsc::sync_channel(1);
    db.checkpoint_pause = Some((entered, gate));
    db.start_checkpoint().unwrap();
    waiting.recv_timeout(Duration::from_secs(5)).unwrap();
    let mut suffix = None;
    for sequence in 1..fixture.config.max_admitted_batches {
        let operation = fixture.operation((sequence % 5) as u32, sequence as u64 + 1);
        let version = db.write_batch(operation, vec![put(2)]).unwrap();
        suffix = Some((operation, version));
    }
    let pressure = AdmissionPressure::Tail {
        encoded_bytes: fixture.config.storage.batch_limits.max_encoded_bytes,
        identity: true,
    };
    assert!(!db.admission_pressure_fits(pressure));
    let (next_entered, _next_waiting) = mpsc::sync_channel(1);
    let (next_resume, next_gate) = mpsc::sync_channel(1);
    db.checkpoint_pause = Some((next_entered, next_gate));
    let (finished, completion) = mpsc::sync_channel(1);
    let owner = thread::spawn(move || {
        let result = db.ensure_admission_pressure(pressure);
        finished.send(result.is_ok()).unwrap();
        (db, result)
    });
    resume.send(()).unwrap();
    // An unnecessary full catch-up would now depend on next_resume.
    let observed = completion.recv_timeout(Duration::from_secs(5));
    next_resume.send(()).unwrap();
    let (mut db, result) = owner.join().unwrap();
    assert!(observed.unwrap());
    result.unwrap();
    assert_eq!(
        db.checkpoint_tail_batches,
        fixture.config.max_admitted_batches - 1
    );
    db.healthy = false;
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
    let mut fixture = Fixture::new();
    let mut db = fixture.open();
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
            epoch: db.epoch,
            sequence: db.submission_next(),
        },
        mutations(3),
        fixture.config.storage.batch_limits,
    )
    .unwrap();
    let additional_bytes = db.index.estimated_batch_bytes(&incoming).unwrap();
    db.shutdown().unwrap();
    drop(db);
    fixture.config.max_index_bytes = resident_directory.checked_add(additional_bytes).unwrap();
    let mut db = fixture.open();
    let (entered, waiting) = mpsc::sync_channel(1);
    let (resume, gate) = mpsc::sync_channel(1);
    db.checkpoint_pause = Some((entered, gate));
    db.write_batch(fixture.operation(0, 2), vec![put(1)])
        .unwrap();
    db.start_checkpoint().unwrap();
    waiting.recv_timeout(Duration::from_secs(5)).unwrap();
    db.write_batch(fixture.operation(1, 3), vec![put(2)])
        .unwrap();
    let generation = db.snapshot_generation;
    let pressure = AdmissionPressure::Index { additional_bytes };
    assert!(!db.admission_pressure_fits(pressure));
    resume.send(()).unwrap();
    db.ensure_admission_pressure(pressure).unwrap();
    assert_eq!(db.snapshot_generation, generation + 1);
    assert!(db.background_checkpoint.is_none());
    db.index.check_capacity(additional_bytes).unwrap();
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
fn unchanged_flush_waits_for_captured_cut_and_then_covers_its_acknowledged_suffix() {
    let fixture = Fixture::new();
    let mut db = fixture.open();
    db.write_batch(fixture.operation(0, 1), vec![put(1)])
        .unwrap();
    let (entered, waiting) = mpsc::sync_channel(1);
    let (resume, gate) = mpsc::sync_channel(1);
    db.checkpoint_pause = Some((entered, gate));
    db.start_checkpoint().unwrap();
    waiting.recv_timeout(Duration::from_secs(5)).unwrap();
    db.write_batch(fixture.operation(1, 2), vec![put(2)])
        .unwrap();
    resume.send(()).unwrap();
    db.flush_index().unwrap();
    assert_eq!(db.checkpoint_tail_batches, 0);
    assert_eq!(db.checkpoint_tail_bytes, 0);
    assert!(db.background_checkpoint.is_none());
    assert!(db.known.is_empty());
    db.healthy = false;
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
    assert!(db.submissions.is_empty());
    assert!(db.partition_admissions.iter().all(|count| *count == 0));
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
    let mut reopened = fixture.open();
    for ((operation, mutations), version) in writes.into_iter().zip(versions) {
        assert_eq!(reopened.write_batch(operation, mutations).unwrap(), version);
    }
    reopened.validate_cached_read().unwrap();
}

#[test]
fn publication_failure_fences_and_recovers_dispatched_successor_group() {
    let fixture = Fixture::new();
    let mut db = fixture.open();
    let connection = std::sync::Arc::clone(&db.clients[0].client);
    let held = connection.lock();
    let mut identities = Vec::new();
    for key in 0..4u32 {
        let operation = fixture.operation(0, u64::from(key));
        let version = db.submit(operation, vec![put(key as u8)]).unwrap();
        identities.push((operation, version));
    }
    drop(held);

    // Already dispatched successor work can outlive a publication failure.
    // Recovery must reconcile its storage outcome rather than discard it.
    db.submissions[0].routing = None;
    assert!(db.drain().is_err());
    assert!(!db.healthy);
    assert!(db.ensure_healthy().is_err());
    assert_eq!(db.publication.published_prefix().next_sequence(), 0);
    assert!(db.pending_bytes > 0);

    assert!(db.inflight.iter().all(VecDeque::is_empty));
    drop(db);

    let mut reopened = fixture.open();
    assert_eq!(reopened.recovery_stats().tail_batches, 4);
    for (operation, version) in identities {
        assert_eq!(
            reopened
                .submit(operation, vec![put(version.sequence as u8)])
                .unwrap(),
            version
        );
    }
    assert_eq!(
        reopened.get(KeySpace(0), &1u32.to_be_bytes()).unwrap(),
        Some(vec![3; 32].into())
    );
    assert_eq!(reopened.stats.admitted_batches, 0);
    reopened.shutdown().unwrap();
}
