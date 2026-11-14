//! Position proofs share the wire session, not lease progress or its file barrier.
use super::leased_tests::worker;
use super::*;
use crate::distributed::durability::DurabilityTier;
use crate::distributed::storage::LivePausePoint;
use crate::distributed::tcp::leased_tests::Fixture;
use std::sync::atomic::Ordering;

fn ready(fixture: &Fixture) -> (Client, DurableBatch) {
    let mut client = fixture.writer();
    let first = client
        .submit_and_drain_leased_group(1, vec![fixture.batch(0)], &WriteTimeline::default(), 2)
        .unwrap()[0];
    client.wait_leased_control(&mut Vec::new(), None).unwrap();
    client.submit_leased_adoption().unwrap();
    client.wait_leased_control(&mut Vec::new(), None).unwrap();
    (client, first)
}

fn through(receipt: DurableBatch) -> u64 {
    let address = receipt.appended().address();
    address.offset + u64::from(address.len)
}

#[test]
fn paused_position_allows_adoption_prelease_and_two_extent_switches() {
    for tier in [DurabilityTier::Native, DurabilityTier::Synchronous] {
        let fixture = Fixture::new(tier, 16384);
        let (client, first) = ready(&fixture);
        let log = fixture.node.log_for_test(1);
        let (entered, release) = log.pause_live(LivePausePoint::PositionBeforeData);
        let (worker, failure) = worker(&fixture, client);
        let (reply, proof) = mpsc::sync_channel(1);
        worker
            .send(Command::CheckpointPosition(1, 1, through(first), reply))
            .unwrap();
        entered.recv_timeout(Duration::from_secs(5)).unwrap();
        let overlapping = (|| -> Result<Vec<DurableBatch>, String> {
            let mut receipts = vec![first];
            for n in 1..=10 {
                let pending = worker
                    .append(1, vec![fixture.batch(n)])
                    .map_err(|e| e.to_string())?;
                let result = pending
                    .recv_timeout(Duration::from_secs(5))
                    .map_err(|e| e.to_string())?;
                receipts.push(result.map_err(|e| e.to_string())?[0]);
            }
            Ok(receipts)
        })();
        let pending = matches!(proof.try_recv(), Err(mpsc::TryRecvError::Empty));
        let counts = fixture.metrics.snapshot().counts;
        release.send(()).unwrap();
        let receipts = overlapping.expect("position data sync must not own lease progress");
        assert!(pending);
        assert!(counts.extent_switch_samples >= 2);
        assert!(counts.extent_preleases >= 2);
        assert!(counts.adoption_async_samples > 1);
        assert!(
            proof
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
                .unwrap()
                .durable_end
                >= through(first)
        );
        receive(worker.seal(1).unwrap()).unwrap();
        for (n, receipt) in receipts.into_iter().enumerate() {
            let batch = fixture.batch(n as u64);
            let reference = batch
                .record_reference(receipt.appended().address(), 0)
                .unwrap();
            assert_eq!(log.read_record(&reference).unwrap(), batch.mutations()[0]);
        }
        drop(worker);
        assert!(!failure.load(Ordering::Acquire));
    }
}

#[test]
fn independent_position_slots_merge_roots_in_reverse_completion_order() {
    for tier in [DurabilityTier::Native, DurabilityTier::Synchronous] {
        let fixture = Fixture::new(tier, 128 * 1024);
        let (mut client, first) = ready(&fixture);
        // Status is exclusive. Connect before pausing active stream work.
        let mut other = fixture.client();
        let second = client
            .submit_and_drain_leased_group(1, vec![fixture.batch(1)], &WriteTimeline::default(), 2)
            .unwrap()[0];
        let log = fixture.node.log_for_test(1);
        let (entered, release) = log.pause_live(LivePausePoint::PositionBeforeData);
        assert!(
            client
                .submit_checkpoint_position(
                    1,
                    1,
                    through(first),
                    Instant::now() + Duration::from_secs(5)
                )
                .unwrap()
        );
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            client.progress_appends().unwrap();
            if entered.try_recv().is_ok() {
                break;
            }
            assert!(Instant::now() < deadline);
            thread::yield_now();
        }
        // Capacity is exactly one position, independently of the ordinary control.
        assert!(
            !client
                .submit_checkpoint_position(1, 1, through(second), deadline)
                .unwrap()
        );
        let newer = other.checkpoint_position(1, 1, through(second));
        release.send(()).unwrap();
        let newer = newer.unwrap();
        let older = loop {
            client.progress_appends().unwrap();
            if let Some(result) = client.take_checkpoint_position(1, through(first)) {
                break result.unwrap();
            }
            assert!(Instant::now() < deadline);
            thread::yield_now();
        };
        assert_eq!(older.durable_end, newer.durable_end);
        assert!(older.durable_end >= through(second));
        client.close_leased_extent(&mut Vec::new(), None).unwrap();
    }
}

#[test]
fn position_deadline_poison_clears_both_slots() {
    for tier in [DurabilityTier::Native, DurabilityTier::Synchronous] {
        let fixture = Fixture::new(tier, 128 * 1024);
        let (mut client, first) = ready(&fixture);
        client
            .submit_and_drain_leased_group(1, vec![fixture.batch(1)], &WriteTimeline::default(), 2)
            .unwrap();
        let log = fixture.node.log_for_test(1);
        let (entered, release) = log.pause_live(LivePausePoint::PositionBeforeData);
        let deadline = Instant::now() + Duration::from_secs(5);
        assert!(
            client
                .submit_checkpoint_position(1, 1, through(first), deadline)
                .unwrap()
        );
        loop {
            client.progress_appends().unwrap();
            if entered.try_recv().is_ok() {
                break;
            }
            assert!(Instant::now() < deadline);
            thread::yield_now();
        }
        client.submit_leased_adoption().unwrap();
        assert!(client.checkpoint_control_pending());
        client.expire_position_for_test();
        let failed = client.progress_appends();
        release.send(()).unwrap();
        assert!(matches!(failed, Err(error) if error.to_string().contains("deadline expired")));
        assert!(!client.checkpoint_control_pending());
        assert!(client.take_checkpoint_position(1, through(first)).is_none());
        assert!(
            client
                .submit_checkpoint_position(1, 1, through(first), deadline)
                .unwrap_err()
                .to_string()
                .contains("requires replacement")
        );
    }
}

#[test]
fn shutdown_drains_an_inflight_position_and_lease_controls() {
    let fixture = Fixture::new(DurabilityTier::Native, 16384);
    let (client, first) = ready(&fixture);
    let (entered, release) = fixture
        .node
        .log_for_test(1)
        .pause_live(LivePausePoint::PositionBeforeData);
    let (worker, failure) = worker(&fixture, client);
    let (reply, proof) = mpsc::sync_channel(1);
    worker
        .send(Command::CheckpointPosition(1, 1, through(first), reply))
        .unwrap();
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    let (closed, done) = mpsc::sync_channel(1);
    let shutdown = thread::spawn(move || {
        drop(worker);
        closed.send(()).unwrap();
    });
    let was_pending = matches!(
        done.recv_timeout(Duration::from_millis(20)),
        Err(mpsc::RecvTimeoutError::Timeout)
    );
    release.send(()).unwrap();
    assert!(was_pending);
    assert!(proof.recv_timeout(Duration::from_secs(5)).unwrap().is_ok());
    done.recv_timeout(Duration::from_secs(5)).unwrap();
    shutdown.join().unwrap();
    assert!(!failure.load(Ordering::Acquire));
}

#[test]
fn failed_position_proof_poison_is_published_before_more_commands() {
    let fixture = Fixture::new(DurabilityTier::Native, 16384);
    let (client, _) = ready(&fixture);
    let (worker, failure) = worker(&fixture, client);
    let (reply, proof) = mpsc::sync_channel(1);
    worker
        .send(Command::CheckpointPosition(1, 1, u64::MAX, reply))
        .unwrap();
    assert!(proof.recv_timeout(Duration::from_secs(5)).unwrap().is_err());
    let deadline = Instant::now() + Duration::from_secs(5);
    while !failure.load(Ordering::Acquire) {
        assert!(Instant::now() < deadline);
        thread::yield_now();
    }
    let _ = worker.stats();
    drop(worker);
    assert!(failure.load(Ordering::Acquire));
}

#[test]
fn prepared_prelease_completes_during_serialized_root_install() {
    let fixture = Fixture::new(DurabilityTier::Native, 128 * 1024);
    let mut client = fixture.writer_without_ahead();
    let first = client
        .submit_and_drain_leased_group(1, vec![fixture.batch(0)], &WriteTimeline::default(), 2)
        .unwrap()[0];
    client.submit_leased_adoption().unwrap();
    client.wait_leased_control(&mut Vec::new(), None).unwrap();
    client.leased_test_enable_ahead();
    // The first grant has already allocated and durably named this file;
    // its next fragment must not depend on an unrelated root transaction.
    let log = fixture.node.log_for_test(1);
    let root_before = log.position_root_for_test();
    let receipt_before = log.synced_frontier();
    let (entered, release) = log.pause_live(LivePausePoint::PositionRootLocked);
    let deadline = Instant::now() + Duration::from_secs(5);
    client
        .submit_checkpoint_position(1, 1, through(first), deadline)
        .unwrap();
    loop {
        client.progress_appends().unwrap();
        if entered.try_recv().is_ok() {
            break;
        }
        assert!(Instant::now() < deadline);
        thread::yield_now();
    }
    client.submit_leased_adoption().unwrap();
    let completed = loop {
        client.progress_appends().unwrap();
        if client.harvest_leased_adoption().unwrap() {
            break true;
        }
        if Instant::now() >= deadline - Duration::from_secs(1) {
            break false;
        }
        thread::yield_now();
    };
    let root_during = log.position_root_for_test();
    let receipt_during = log.synced_frontier();
    let proof_pending = client.take_checkpoint_position(1, through(first)).is_none();
    let pending = client.leased_adoption_pending();
    // Release the hook before asserting, including on a broken fast path.
    release.send(()).unwrap();
    assert!(
        completed,
        "prepared prelease must finish before root install resumes"
    );
    assert!(!pending);
    assert_eq!(root_during, root_before);
    assert_eq!(receipt_during, receipt_before);
    assert!(proof_pending);
    client.wait_leased_control(&mut Vec::new(), None).unwrap();
    assert!(client.leased_test_ahead_state().1);
    loop {
        client.progress_appends().unwrap();
        if let Some(proof) = client.take_checkpoint_position(1, through(first)) {
            assert_eq!(proof.unwrap().durable_end, through(first));
            break;
        }
        assert!(Instant::now() < deadline);
        thread::yield_now();
    }
    assert_eq!(
        log.position_root_for_test(),
        (root_before.0 + 1, root_before.1, through(first))
    );
    assert_eq!(log.synced_frontier(), receipt_before);
    client.close_leased_extent(&mut Vec::new(), None).unwrap();
}
