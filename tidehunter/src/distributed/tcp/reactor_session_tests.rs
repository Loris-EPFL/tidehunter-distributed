use super::*;
use crate::distributed::storage::WalMemberReadPolicy;
use crate::distributed::tcp::tests::multiplexed::Running;

fn take_wire_responses(client: &mut Client) -> Vec<Envelope<Response>> {
    let mut responses = Vec::new();
    while let Some((frame, frame_len)) = client.stream.receive(client.frame_limit).unwrap() {
        let response = client
            .stream
            .decode(&frame[..frame_len], client.frame_limit)
            .unwrap();
        client.stream.recycle_input(frame);
        responses.push(response);
    }
    responses
}

#[test]
fn one_session_pipelines_appends_but_keeps_fence_and_seal_as_barriers() {
    // The server closes a session whose accepted request waits longer than
    // `io_timeout`. This test holds the first append paused across two client
    // waits of up to 2 s each, so the server bound must exceed their sum.
    let server = Running::with_limits(
        Transport::Tcp,
        WalMemberReadPolicy::Positional,
        crate::distributed::tcp::default_read_bytes(),
        crate::distributed::tcp::default_device_reads(),
        crate::distributed::tcp::TcpLimits {
            io_timeout: Duration::from_secs(10),
            max_frame_bytes: 2 * 1024 * 1024,
            ..TcpLimits::default()
        },
    );
    server
        .node
        .set_durability(crate::distributed::durability::DurabilityConfig::with_tier(
            crate::distributed::durability::DurabilityTier::Synchronous,
        ))
        .unwrap();
    let mut client = server.client();
    client.fence_to(1).unwrap();
    let log = server.node.inventory.lock().active(1).unwrap();
    let (persist_entered, resume_persist) = log.pause_next_persist();

    let first = crate::distributed::tcp::tests::batch(&server.config, 1, 0);
    let second = crate::distributed::tcp::tests::batch(&server.config, 1, 1);
    let after_fence = crate::distributed::tcp::tests::batch(&server.config, 2, 0);
    let append_id = client.reactor.allocate_id().unwrap();
    client
        .stream
        .queue(
            append_id,
            &Request::Append {
                epoch: 1,
                batches: vec![first.encoded().clone().into()],
            },
            client.frame_limit,
        )
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        client.stream.progress(None).unwrap();
        if persist_entered.try_recv().is_ok() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "first append did not reach persistence"
        );
        thread::yield_now();
    }

    let second_append_id = client.reactor.allocate_id().unwrap();
    let fence_id = client.reactor.allocate_id().unwrap();
    let post_fence_append_id = client.reactor.allocate_id().unwrap();
    let seal_id = client.reactor.allocate_id().unwrap();
    for (id, request) in [
        (
            second_append_id,
            Request::Append {
                epoch: 1,
                batches: vec![second.encoded().clone().into()],
            },
        ),
        (fence_id, Request::Fence { epoch: 2 }),
        (
            post_fence_append_id,
            Request::Append {
                epoch: 2,
                batches: vec![after_fence.encoded().clone().into()],
            },
        ),
        (seal_id, Request::Seal { epoch: 2 }),
    ] {
        client
            .stream
            .queue(id, &request, client.frame_limit)
            .unwrap();
    }

    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        client.stream.progress(None).unwrap();
        assert!(
            take_wire_responses(&mut client).is_empty(),
            "an append reply escaped before its persistence barrier"
        );
        if log.receipt_count() >= 2 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "later same-session append did not reach the FIFO control owner"
        );
        thread::yield_now();
    }
    assert!(
        take_wire_responses(&mut client).is_empty(),
        "fence or seal replied while an earlier append was paused"
    );
    resume_persist.send(()).unwrap();

    let deadline = Instant::now() + Duration::from_secs(3);
    let mut responses = Vec::new();
    while responses.len() != 5 {
        client.stream.progress(None).unwrap();
        responses.extend(take_wire_responses(&mut client));
        assert!(
            Instant::now() < deadline,
            "barrier responses did not complete"
        );
        thread::yield_now();
    }
    let mut ids: Vec<_> = responses.iter().map(|response| response.id).collect();
    ids.sort_unstable();
    let mut expected_ids = vec![
        append_id,
        second_append_id,
        fence_id,
        post_fence_append_id,
        seal_id,
    ];
    expected_ids.sort_unstable();
    assert_eq!(ids, expected_ids, "every request keeps its response ID");
    for (id, expected) in [
        (append_id, "first append"),
        (second_append_id, "second append"),
        (post_fence_append_id, "post-fence append"),
    ] {
        let response = responses.iter().find(|response| response.id == id).unwrap();
        assert!(
            matches!(response.body, Response::Appended(_)),
            "{expected} must return its durable receipt"
        );
    }
    let fence = responses
        .iter()
        .find(|response| response.id == fence_id)
        .unwrap();
    let Response::Status(status) = &fence.body else {
        panic!("fence must return status after prior appends");
    };
    assert_eq!(status.active_epoch, Some(2));
    let seal = responses
        .iter()
        .find(|response| response.id == seal_id)
        .unwrap();
    assert!(matches!(seal.body, Response::Sealed));
}

#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
#[test]
fn session_and_node_use_the_same_stream_class_for_every_wal_request() {
    use crate::distributed::durability::DurabilityTier;
    use crate::distributed::storage::LivePausePoint;
    use crate::distributed::tcp::leased_tests::Fixture;
    for variant in 0..7 {
        let fixture = Fixture::new(DurabilityTier::Native, 128 * 1024);
        let mut client = fixture.writer_without_ahead();
        let receipt = client
            .submit_and_drain_leased_group(
                1,
                vec![fixture.batch(0)],
                &crate::distributed::write_timeline::WriteTimeline::default(),
                2,
            )
            .unwrap()[0];
        let through =
            receipt.appended().address().offset + u64::from(receipt.appended().address().len);
        let (entered, release) = fixture
            .node
            .log_for_test(1)
            .pause_live(LivePausePoint::PositionBeforeData);
        let deadline = Instant::now() + Duration::from_secs(5);
        assert!(
            client
                .submit_checkpoint_position(1, 1, through, deadline)
                .unwrap()
        );
        loop {
            client.progress_appends().unwrap();
            if entered.try_recv().is_ok() {
                break;
            }
            assert!(Instant::now() < deadline);
            std::thread::yield_now();
        }
        let group = crate::distributed::group::CompleteGroup::new_live(
            &KeyShape::from_yaml(&fixture.config.shape_yaml).unwrap(),
            vec![fixture.batch(1)],
            fixture.config.partition.batch_limits,
            fixture.config.partition.group_limits,
        )
        .unwrap();
        let descriptor =
            crate::distributed::nvmeof::live_storage::LiveGroupDescriptor::from_group(&group)
                .unwrap()
                .0;
        // Absent epoch: a real error reply proves admission through BOTH
        // schedulers without creating a new allocation or altering the log.
        let request = match variant {
            0 => Request::Append {
                epoch: 2,
                batches: vec![fixture.batch(1).encoded().clone().into()],
            },
            1 => Request::CheckpointPosition {
                authority_epoch: 1,
                epoch: 2,
                through: 0,
            },
            2 => Request::LeaseLiveExtent { epoch: 2 },
            3 => Request::PreleaseLiveExtent {
                epoch: 2,
                after: [0; 16],
            },
            4 => Request::AdoptLiveExtent {
                epoch: 2,
                token: [0; 16],
                through: 0,
                close: false,
                lease_next: false,
            },
            5 => Request::CompleteLiveGroup {
                epoch: 2,
                token: [0; 16],
            },
            _ => Request::ReserveLiveGroup {
                epoch: 2,
                descriptor,
            },
        };
        assert!(control::is_stream_request(&request));
        assert!(client.submit_control(&request, deadline).unwrap());
        let response = loop {
            client.progress_appends().unwrap();
            if let Some(response) = client.take_control() {
                break Some(response);
            }
            if Instant::now() >= deadline - Duration::from_secs(1) {
                break None;
            }
            std::thread::yield_now();
        };
        let proof_pending = client.reactor.position_result.is_none();
        release.send(()).unwrap();
        assert!(
            response
                .expect("both schedulers must admit this stream request")
                .is_err()
        );
        assert!(proof_pending);
        loop {
            client.progress_appends().unwrap();
            if let Some(proof) = client.take_checkpoint_position(1, through) {
                proof.unwrap();
                break;
            }
            assert!(Instant::now() < deadline);
            std::thread::yield_now();
        }
    }
}

#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
#[test]
fn seal_fence_and_retirement_wait_for_active_position_proofs() {
    use crate::distributed::durability::DurabilityTier;
    use crate::distributed::storage::LivePausePoint;
    use crate::distributed::tcp::leased_tests::Fixture;
    // Same-session exclusion and node-wide exclusion are separate barriers.
    for scenario in 0..6 {
        let variant = scenario % 3;
        let fixture = Fixture::new(DurabilityTier::Native, 128 * 1024);
        let mut client = fixture.writer_without_ahead();
        let mut other = fixture.client();
        let receipt = client
            .submit_and_drain_leased_group(
                1,
                vec![fixture.batch(0)],
                &crate::distributed::write_timeline::WriteTimeline::default(),
                2,
            )
            .unwrap()[0];
        let through =
            receipt.appended().address().offset + u64::from(receipt.appended().address().len);
        if variant == 2 {
            client.close_leased_extent(&mut Vec::new(), None).unwrap();
            client.seal(1).unwrap();
            client.fence_to(2).unwrap();
        }
        let authority = if variant == 2 { 2 } else { 1 };
        let log = fixture
            .node
            .inventory
            .lock()
            .checkpoint_log(authority, 1)
            .unwrap()
            .unwrap();
        let (entered, release) = log.pause_live(LivePausePoint::PositionBeforeData);
        drop(log); // Fence must not see a test-owned outstanding storage handle.
        let deadline = Instant::now() + Duration::from_secs(5);
        client
            .submit_checkpoint_position(authority, 1, through, deadline)
            .unwrap();
        loop {
            client.progress_appends().unwrap();
            if entered.try_recv().is_ok() {
                break;
            }
            assert!(Instant::now() < deadline);
            std::thread::yield_now();
        }
        let request = match variant {
            0 => Request::Seal { epoch: 1 },
            1 => Request::Fence { epoch: 2 },
            _ => Request::RetireWalFiles {
                authority_epoch: 2,
                epoch: 1,
                files: Vec::new(),
            },
        };
        assert!(!control::is_stream_request(&request));
        let session_waits = fixture.node.control.session_barrier_waits();
        let control = if scenario < 3 {
            &mut client
        } else {
            &mut other
        };
        assert!(control.submit_control(&request, deadline).unwrap());
        let queued = loop {
            client.progress_appends().unwrap();
            other.progress_appends().unwrap();
            let observed = if scenario < 3 {
                fixture.node.control.session_barrier_waits() > session_waits
            } else {
                fixture.node.control.exclusive_waits_for_stream()
            };
            if observed {
                break true;
            }
            let control = if scenario < 3 { &client } else { &other };
            if control.reactor.control_result.is_some()
                || Instant::now() >= deadline - Duration::from_secs(1)
            {
                break false;
            }
            std::thread::yield_now();
        };
        let control = if scenario < 3 { &client } else { &other };
        let pending = control.reactor.control_result.is_none();
        release.send(()).unwrap();
        assert!(queued, "control must be observed queued behind the proof");
        assert!(pending);
        let mut proof_done = false;
        let mut control_done = false;
        while !proof_done || !control_done {
            client.progress_appends().unwrap();
            other.progress_appends().unwrap();
            if !proof_done && let Some(proof) = client.take_checkpoint_position(1, through) {
                proof.unwrap();
                proof_done = true;
            }
            let control = if scenario < 3 {
                &mut client
            } else {
                &mut other
            };
            if !control_done && let Some(response) = control.take_control() {
                response.unwrap();
                control_done = true;
            }
            assert!(Instant::now() < deadline);
        }
    }
}

#[test]
fn position_and_control_admission_keep_the_existing_session_bound() {
    let server = Running::new(Transport::TcpTrusted, WalMemberReadPolicy::Positional);
    let mut client = server.client();
    for _ in 0..SESSION_REQUESTS {
        let id = client.reactor.allocate_id().unwrap();
        client
            .stream
            .queue(id, &Request::Stats, client.frame_limit)
            .unwrap();
    }
    let next_id = client.reactor.next_id;
    let bytes = client.stream.output_bytes;
    for _ in 0..128 {
        let deadline = Instant::now() + Duration::from_secs(5);
        assert!(
            !client
                .submit_checkpoint_position(1, 1, 0, deadline)
                .unwrap()
        );
        assert!(!client.submit_control(&Request::Stats, deadline).unwrap());
        assert_eq!(client.reactor.next_id, next_id);
        assert_eq!(client.stream.output.len(), SESSION_REQUESTS);
        assert_eq!(client.stream.output_bytes, bytes);
        assert!(client.reactor.position.is_none());
        assert!(client.reactor.control.is_none());
    }
    assert!(bytes <= SESSION_BYTES);
}

#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
#[test]
fn lease_triggers_file_preparation_and_fence_joins_its_worker() {
    use crate::distributed::durability::DurabilityTier;
    use crate::distributed::storage::LivePausePoint;
    use crate::distributed::tcp::leased_tests::Fixture;
    for paused in [false, true] {
        let fixture = Fixture::new(DurabilityTier::Native, 16 * 1024);
        let mut client = fixture.writer_without_ahead();
        let log = fixture.node.log_for_test(1);
        let pause = paused.then(|| log.pause_live(LivePausePoint::PrepareBeforePublish));
        assert!(matches!(
            client
                .request(Request::LeaseLiveExtent { epoch: 1 })
                .unwrap(),
            Response::LiveLeased(_)
        ));
        if let Some((entered, _)) = &pause {
            entered.recv_timeout(Duration::from_secs(5)).unwrap();
        } else {
            let deadline = Instant::now() + Duration::from_secs(5);
            while log.live_file_preparation_stats().live_files_prepared == 0 {
                assert!(
                    Instant::now() < deadline,
                    "grant did not trigger preparation"
                );
                thread::yield_now();
            }
            assert_eq!(log.live_file_preparation_stats().live_files_prepared, 1);
            assert_eq!(
                log.live_file_preparation_stats().live_file_prepare_failures,
                0
            );
        }
        assert_eq!(fixture.node.control.preparer_count(), 1);
        drop(log); // Fence recovery requires no test-owned log handles.
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        let fence = thread::spawn(move || tx.send(client.fence_to(2)).unwrap());
        if let Some((_, release)) = pause {
            let deadline = Instant::now() + Duration::from_secs(5);
            while fixture.node.control.preparer_count() != 0 {
                assert!(Instant::now() < deadline, "fence did not retire preparer");
                thread::yield_now();
            }
            assert!(rx.try_recv().is_err(), "fence must join the paused worker");
            release.send(()).unwrap();
        }
        let result = rx.recv_timeout(Duration::from_secs(5));
        fence.join().unwrap();
        result.unwrap().unwrap();
        assert_eq!(fixture.node.control.preparer_count(), 0);
    }
}
