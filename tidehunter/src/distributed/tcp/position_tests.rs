//! Position proofs share the wire session, not lease progress or its file barrier.
use super::*;
use crate::distributed::durability::DurabilityTier;
use crate::distributed::storage::LivePausePoint;
use crate::distributed::tcp::leased_tests::Fixture;
use crate::distributed::write_timeline::WriteTimeline;

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

fn submit(
    client: &mut Client,
    authority_epoch: u64,
    epoch: u64,
    through: u64,
    deadline: Instant,
) -> Result<bool, TcpError> {
    client.submit_position(
        &Request::CheckpointPosition {
            authority_epoch,
            epoch,
            through,
        },
        deadline,
    )
}

#[test]
fn paused_position_allows_adoption_prelease_and_two_extent_switches() {
    for tier in [DurabilityTier::Native, DurabilityTier::Synchronous] {
        let fixture = Fixture::new(tier, 16384);
        let (mut client, first) = ready(&fixture);
        let log = fixture.node.log_for_test(1);
        let (entered, release) = log.pause_live(LivePausePoint::PositionBeforeData);
        let deadline = Instant::now() + Duration::from_secs(5);
        assert!(submit(&mut client, 1, 1, through(first), deadline).unwrap());
        loop {
            client.progress_session().unwrap();
            if entered.try_recv().is_ok() {
                break;
            }
            assert!(Instant::now() < deadline);
            thread::yield_now();
        }
        let overlapping = (|| -> Result<Vec<DurableBatch>, String> {
            let mut receipts = vec![first];
            for n in 1..=10 {
                receipts.extend(
                    client
                        .submit_and_drain_leased_group(
                            1,
                            vec![fixture.batch(n)],
                            &WriteTimeline::default(),
                            2,
                        )
                        .map_err(|error| error.to_string())?,
                );
            }
            Ok(receipts)
        })();
        let pending = client.checkpoint_position_pending();
        let counts = fixture.metrics.snapshot().counts;
        release.send(()).unwrap();
        let receipts = overlapping.expect("position data sync must not own lease progress");
        assert!(pending);
        assert!(counts.extent_switch_samples >= 2);
        assert!(counts.extent_preleases >= 2);
        assert!(counts.adoption_async_samples > 1);
        let proof = loop {
            client.progress_session().unwrap();
            if let Some(proof) = client.take_position() {
                break proof.unwrap();
            }
            assert!(Instant::now() < deadline);
            thread::yield_now();
        };
        let Response::CheckpointPosition { durable_end, .. } = proof else {
            panic!("the position slot returns a CheckpointPosition reply");
        };
        assert!(durable_end >= through(first));
        client.seal(1).unwrap();
        for (n, receipt) in receipts.into_iter().enumerate() {
            let batch = fixture.batch(n as u64);
            let reference = batch
                .record_reference(receipt.appended().address(), 0)
                .unwrap();
            assert_eq!(log.read_record(&reference).unwrap(), batch.mutations()[0]);
        }
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
        let deadline = Instant::now() + Duration::from_secs(5);
        assert!(submit(&mut client, 1, 1, through(first), deadline).unwrap());
        loop {
            client.progress_session().unwrap();
            if entered.try_recv().is_ok() {
                break;
            }
            assert!(Instant::now() < deadline);
            thread::yield_now();
        }
        // Capacity is exactly one position, independently of the ordinary control.
        assert!(!submit(&mut client, 1, 1, through(second), deadline).unwrap());
        let newer = other.checkpoint_position(1, 1, through(second));
        release.send(()).unwrap();
        let newer = newer.unwrap();
        let older = loop {
            client.progress_session().unwrap();
            if let Some(proof) = client.take_position() {
                break proof.unwrap();
            }
            assert!(Instant::now() < deadline);
            thread::yield_now();
        };
        let Response::CheckpointPosition { durable_end, .. } = older else {
            panic!("the position slot returns a CheckpointPosition reply");
        };
        assert_eq!(durable_end, newer.durable_end);
        assert!(durable_end >= through(second));
        client.close_leased_extent(&mut Vec::new(), None).unwrap();
    }
}

#[test]
fn position_deadline_poison_clears_the_slot() {
    for tier in [DurabilityTier::Native, DurabilityTier::Synchronous] {
        let fixture = Fixture::new(tier, 128 * 1024);
        let (mut client, first) = ready(&fixture);
        client
            .submit_and_drain_leased_group(1, vec![fixture.batch(1)], &WriteTimeline::default(), 2)
            .unwrap();
        let log = fixture.node.log_for_test(1);
        let (entered, release) = log.pause_live(LivePausePoint::PositionBeforeData);
        let deadline = Instant::now() + Duration::from_secs(5);
        assert!(submit(&mut client, 1, 1, through(first), deadline).unwrap());
        loop {
            client.progress_session().unwrap();
            if entered.try_recv().is_ok() {
                break;
            }
            assert!(Instant::now() < deadline);
            thread::yield_now();
        }
        client.submit_leased_adoption().unwrap();
        assert!(client.checkpoint_control_pending());
        client.expire_position_for_test();
        let failed = client.progress_session();
        release.send(()).unwrap();
        assert!(matches!(failed, Err(error) if error.to_string().contains("deadline expired")));
        assert!(!client.checkpoint_control_pending());
        assert!(client.take_position().is_none());
        assert!(
            submit(&mut client, 1, 1, through(first), deadline)
                .unwrap_err()
                .to_string()
                .contains("requires replacement")
        );
    }
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
    submit(&mut client, 1, 1, through(first), deadline).unwrap();
    loop {
        client.progress_session().unwrap();
        if entered.try_recv().is_ok() {
            break;
        }
        assert!(Instant::now() < deadline);
        thread::yield_now();
    }
    client.submit_leased_adoption().unwrap();
    let completed = loop {
        client.progress_session().unwrap();
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
    let proof_pending = client.take_position().is_none();
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
        client.progress_session().unwrap();
        if let Some(proof) = client.take_position() {
            let Response::CheckpointPosition { durable_end, .. } = proof.unwrap() else {
                panic!("the position slot returns a CheckpointPosition reply");
            };
            assert_eq!(durable_end, through(first));
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
