use super::*;
use crate::distributed::storage::WalMemberReadPolicy;
use crate::distributed::tcp::tests::multiplexed::Running;

#[test]
fn owner_deadline_close_names_the_expired_request_to_the_client() {
    // The owner ends a TLS session whose accepted request outlives its
    // `io_timeout`. The test holds the owner inventory, so the position proof
    // stays executing until the client has its error: the owner's deadline,
    // never the client's, ends the call. (The owner-RPC append this test held
    // at its persist barrier was removed with the payload route.)
    let io_timeout = Duration::from_millis(300);
    let server = Running::with_limits(
        Transport::Tcp,
        WalMemberReadPolicy::Positional,
        crate::distributed::tcp::default_device_reads(),
        crate::distributed::tcp::TcpLimits {
            io_timeout,
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
    client.timeout = Duration::from_secs(60);
    client.fence_to(1).unwrap();
    let inventory = server.node.inventory.lock();

    let error = client
        .checkpoint_position(1, 1, 0)
        .err()
        .unwrap()
        .to_string();
    assert!(
        error.starts_with("owner closed the session: owner request deadline expired: request "),
        "{error}"
    );
    assert!(
        error.contains("(kind 18) was still executing after "),
        "{error}"
    );
    assert!(
        error.contains(&format!("(owner deadline {io_timeout:?})")),
        "{error}"
    );
    drop(inventory);
    assert!(
        client
            .status()
            .unwrap_err()
            .to_string()
            .contains("connection requires replacement"),
        "a session the owner closed must not be reused"
    );
}

#[test]
fn session_and_node_use_the_same_stream_class_for_every_wal_request() {
    use crate::distributed::durability::DurabilityTier;
    use crate::distributed::storage::LivePausePoint;
    use crate::distributed::tcp::leased_tests::Fixture;
    for variant in 0..4 {
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
                .submit_position(
                    &Request::CheckpointPosition {
                        authority_epoch: 1,
                        epoch: 1,
                        through,
                    },
                    deadline,
                )
                .unwrap()
        );
        loop {
            client.progress_session().unwrap();
            if entered.try_recv().is_ok() {
                break;
            }
            assert!(Instant::now() < deadline);
            std::thread::yield_now();
        }
        // Absent epoch: a real error reply proves admission through BOTH
        // schedulers without creating a new allocation or altering the log.
        let request = match variant {
            0 => Request::CheckpointPosition {
                authority_epoch: 1,
                epoch: 2,
                through: 0,
            },
            1 => Request::LeaseLiveExtent { epoch: 2 },
            2 => Request::PreleaseLiveExtent {
                epoch: 2,
                after: [0; 16],
            },
            _ => Request::AdoptLiveExtent {
                epoch: 2,
                token: [0; 16],
                through: 0,
                close: false,
                lease_next: false,
            },
        };
        assert!(control::is_stream_request(&request));
        assert!(client.submit_control(&request, deadline).unwrap());
        let response = loop {
            client.progress_session().unwrap();
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
            client.progress_session().unwrap();
            if let Some(proof) = client.take_position() {
                let proof = proof.unwrap();
                assert!(
                    matches!(
                        proof,
                        Response::CheckpointPosition { durable_end, .. } if durable_end >= through
                    ),
                    "the position reply must cover the requested cut"
                );
                break;
            }
            assert!(Instant::now() < deadline);
            std::thread::yield_now();
        }
    }
}

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
            .submit_position(
                &Request::CheckpointPosition {
                    authority_epoch: authority,
                    epoch: 1,
                    through,
                },
                deadline,
            )
            .unwrap();
        loop {
            client.progress_session().unwrap();
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
                witness: None,
            },
        };
        assert!(!control::is_stream_request(&request));
        let session_waits = fixture.node.control.session_barrier_waits();
        let control = if scenario < 3 {
            &mut client
        } else {
            &mut other
        };
        // Seal, fence and retirement are lifecycle operations (fsyncs, and for
        // a live fence target revocation and recovery) that the client grants
        // LIFECYCLE_TIMEOUT; a data budget would assert their latency. The
        // proof keeps its own 5 s deadline: it completes before the control runs.
        let control_deadline = Instant::now() + LIFECYCLE_TIMEOUT;
        assert!(control.submit_control(&request, control_deadline).unwrap());
        let queued = loop {
            client.progress_session().unwrap();
            other.progress_session().unwrap();
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
            client.progress_session().unwrap();
            other.progress_session().unwrap();
            if !proof_done && let Some(proof) = client.take_position() {
                let proof = proof.unwrap();
                assert!(
                    matches!(
                        proof,
                        Response::CheckpointPosition { durable_end, .. } if durable_end >= through
                    ),
                    "the position reply must cover the requested cut"
                );
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
            assert!(Instant::now() < control_deadline);
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
                .submit_position(
                    &Request::CheckpointPosition {
                        authority_epoch: 1,
                        epoch: 1,
                        through: 0,
                    },
                    deadline,
                )
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
        // The fence starts the new epoch's first-file preparation before it
        // replies (`prepare_fenced_epoch`), which registers that epoch's
        // preparer slot; the preparation itself runs in the background worker.
        // So one slot (the fenced epoch's) outlives the fence. The old epoch's
        // slot was joined and removed; the paused case waits for that above,
        // before it releases the worker.
        assert_eq!(fixture.node.control.preparer_count(), 1);
    }
}

impl Client {
    /// Test only: leave the session unusable exactly as a transport failure
    /// does, including the abort that clears its buffers.
    pub(crate) fn fail_connection_for_test(&mut self) {
        self.fail_connection();
    }

    /// Test only: leave the session unusable and park one control exchange, so
    /// a replacement refuses while that exchange's outcome is unknown.
    pub(crate) fn park_control_for_test(&mut self) {
        self.broken = true;
        self.reactor.control = Some((1, Instant::now() + Duration::from_secs(1)));
    }

    /// Test only: release the parked exchange; the session stays unusable.
    pub(crate) fn clear_parked_control_for_test(&mut self) {
        self.reactor.control = None;
    }
}
