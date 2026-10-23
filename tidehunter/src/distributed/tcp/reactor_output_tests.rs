//! Real rustls framing with a deterministic partial transport writer. The native
//! endpoint tests separately drive actual generated provider callbacks/CQEs.
use super::super::tests::certificates;
use super::*;
use std::io::Cursor;

struct Pair {
    session: Session,
    peer: rustls::Connection,
    client_verifier: Arc<dyn rustls::server::danger::ClientCertVerifier>,
    _control_peer: TcpStream,
    _temp: tempdir::TempDir,
}

fn pair() -> Pair {
    let temp = tempdir::TempDir::new("frame-fences").unwrap();
    let tls = certificates(temp.path());
    let mut client = rustls::Connection::Client(
        ClientConnection::new(
            tls.client_config().unwrap(),
            ServerName::try_from("localhost").unwrap(),
        )
        .unwrap(),
    );
    let config = tls.server_config().unwrap();
    let mut server =
        rustls::Connection::Server(ServerConnection::new(config.config.unwrap()).unwrap());
    for _ in 0..64 {
        fn transfer(sender: &mut rustls::Connection, receiver: &mut rustls::Connection) {
            let mut bytes = Vec::new();
            sender.write_tls(&mut bytes).unwrap();
            let mut bytes = Cursor::new(bytes);
            while bytes.position() < bytes.get_ref().len() as u64 {
                assert!(receiver.read_tls(&mut bytes).unwrap() != 0);
                receiver.process_new_packets().unwrap();
            }
        }
        transfer(&mut client, &mut server);
        transfer(&mut server, &mut client);
        if !client.is_handshaking()
            && !server.is_handshaking()
            && !client.wants_write()
            && !server.wants_write()
        {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let socket = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
            let (control_peer, _) = listener.accept().unwrap();
            client.set_buffer_limit(Some(1024));
            return Pair {
                session: Session::new(
                    client,
                    DeadlineIo::new(socket, Duration::from_secs(2)).unwrap(),
                )
                .unwrap(),
                peer: server,
                client_verifier: config.verifier.unwrap(),
                _control_peer: control_peer,
                _temp: temp,
            };
        }
    }
    panic!("TLS handshake did not finish");
}
#[test]
fn client_does_not_request_grants_without_negotiated_rma() {
    assert_eq!(client_grant_limit(ReadClass::Point, false, false, 8, 0), 0);
    assert_eq!(client_grant_limit(ReadClass::Point, false, true, 8, 2), 6);
    assert_eq!(client_grant_limit(ReadClass::Point, true, true, 8, 2), 0);
    assert_eq!(
        client_grant_limit(ReadClass::ScanNoFill, false, true, 8, 2),
        0
    );
}

#[cfg(feature = "experimental-fabric")]
#[test]
fn owned_record_segments_preserve_protocol_wire_bytes() {
    let mut pair = pair();
    let members = vec![
        WireBytes::from(vec![1, 2, 3]),
        WireBytes::from(minibytes::Bytes::from(vec![4, 5, 6, 7])),
    ];
    pair.session
        .queue_native_records(19, members, None, None, 4096)
        .unwrap();
    let output = pair.session.output.back().unwrap();
    assert!(output.bytes.is_empty());
    assert!(output.segments.is_some());
    let mut sources = [std::io::IoSlice::new(&[]); MAX_NATIVE_IOV];
    let (source_count, source_bytes) = output.native_slices(PROGRESS_BYTES, &mut sources);
    assert!(output.len <= PROGRESS_BYTES);
    assert_eq!(source_bytes, output.len);
    assert_eq!(
        source_bytes,
        sources[..source_count]
            .iter()
            .map(|slice| slice.len())
            .sum::<usize>()
    );
    let frame = output.flatten();
    let response: Envelope<Response> = pair.session.decode(&frame[4..], 4096).unwrap();
    assert_eq!(response.protocol, PROTOCOL);
    assert_eq!(response.id, 19);
    let Response::Records(records) = response.body else {
        panic!("unexpected response variant")
    };
    assert_eq!(records[0].as_slice(), [1, 2, 3]);
    assert_eq!(records[1].as_slice(), [4, 5, 6, 7]);
    let output = pair.session.output.back_mut().unwrap();
    output.advance(7);
    assert_eq!(output.offset, 7);
    assert_eq!(output.flatten(), frame);

    let profile = ServerReadProfile::default();
    pair.session
        .queue_native_records(
            20,
            vec![WireBytes::from(vec![8; 64])],
            None,
            Some(profile),
            4096,
        )
        .unwrap();
    let frame = pair.session.output.back().unwrap().flatten();
    let response: Envelope<Response> = pair.session.decode(&frame[4..], 4096).unwrap();
    assert!(matches!(
        response.body,
        Response::RecordsProfiled { records, .. } if records[0].as_slice() == [8; 64]
    ));
}

#[test]
fn work_profiles_disable_reset_and_preserve_live_frames() {
    let mut pair = pair();
    pair.session.queue(1, &vec![7_u8; 32], 1024).unwrap();
    assert_eq!(pair.session.transport_stats().frame_encode.calls, 0);
    pair.session.configure_transport_stats(true, true);
    pair.session.queue(2, &vec![8_u8; 32], 1024).unwrap();
    let frame = pair.session.output.back().unwrap().flatten()[4..].to_vec();
    let decoded: Envelope<Vec<u8>> = pair.session.decode(&frame, 1024).unwrap();
    assert_eq!(decoded.body, vec![8; 32]);
    assert!(
        pair.session
            .decode::<Envelope<Vec<u8>>>(&[0], 1024)
            .is_err()
    );
    assert!(pair.session.queue(3, &vec![9_u8; 2048], 1024).is_err());
    let stats = pair.session.transport_stats();
    assert_eq!(stats.frame_encode.calls, 2);
    assert_eq!(stats.frame_encode.errors, 1);
    assert_eq!(stats.frame_encode.bytes, frame.len() as u64 + 4);
    assert_eq!(stats.frame_decode.calls, 2);
    assert_eq!(stats.frame_decode.errors, 1);
    assert_eq!(stats.frame_decode.bytes, frame.len() as u64);
    assert!(pair.session.write_plaintext(PROGRESS_BYTES).unwrap() != 0);
    assert!(pair.session.transport_stats().plaintext_write.bytes > 0);
    let owned = pair.session.output_bytes;
    pair.session.configure_transport_stats(false, true);
    let stats = pair.session.transport_stats();
    assert_eq!(stats.frame_encode.calls, 0);
    assert_eq!(stats.plaintext_write.calls, 0);
    assert_eq!(stats.output_frames, 2);
    assert_eq!(stats.output_bytes, owned as u64);
    assert!(pair.session.output_pending(1));
    assert!(pair.session.output_pending(2));
    let _: Envelope<Vec<u8>> = pair.session.decode(&frame, 1024).unwrap();
    assert_eq!(pair.session.transport_stats().frame_decode.calls, 0);
}
fn send_tls_plaintext(pair: &mut Pair, bytes: &[u8]) {
    pair.peer.writer().write_all(bytes).unwrap();
    let mut encrypted = Vec::new();
    pair.peer.write_tls(&mut encrypted).unwrap();
    let mut encrypted = Cursor::new(encrypted);
    while encrypted.position() < encrypted.get_ref().len() as u64 {
        let conn = pair.session.conn.as_mut().unwrap();
        assert_ne!(conn.read_tls(&mut encrypted).unwrap(), 0);
        let state = conn.process_new_packets().unwrap();
        pair.session.plaintext_pending = state.plaintext_bytes_to_read();
        pair.session.peer_closed = state.peer_has_closed();
    }
}
#[test]
fn output_only_turn_preserves_buffered_unread_tls_plaintext() {
    let mut pair = pair();
    let frame = encode_frame(
        &Envelope {
            protocol: PROTOCOL,
            id: 1,
            body: Response::Error("buffered".to_owned()),
        },
        8192,
    )
    .unwrap();
    send_tls_plaintext(&mut pair, &frame);
    let pending = pair.session.plaintext_pending;
    assert!(pending > 0);
    pair.session.queue(2, &0_u8, 1024).unwrap();
    pair.session.progress_output().unwrap();
    assert_eq!(pair.session.plaintext_pending, pending);
    let (bytes, len) = pair.session.receive(8192).unwrap().unwrap();
    let decoded: Envelope<Response> = pair.session.decode(&bytes[..len], 8192).unwrap();
    assert_eq!(decoded.id, 1);
    assert_eq!(pair.session.plaintext_pending, 0);
}
#[test]
fn output_budget_is_shared_across_the_two_turns_of_one_sweep() {
    let mut pair = pair();
    for id in 1..=8 {
        pair.session.queue(id, &id, 1024).unwrap();
    }
    let submitted_before = pair.session.stats.frames_submitted;
    pair.session.progress().unwrap();
    let submitted_after = pair.session.stats.frames_submitted;
    assert_eq!(
        submitted_after - submitted_before,
        OUTPUT_FRAME_BUDGET as u64
    );
    pair.session.progress_output().unwrap();
    assert_eq!(pair.session.stats.frames_submitted, submitted_after);
}

#[test]
fn recycled_plaintext_buffer_decodes_frames_without_stale_trailing_bytes() {
    fn error_frame(id: u64, message: &str) -> Vec<u8> {
        encode_frame(
            &Envelope {
                protocol: PROTOCOL,
                id,
                body: Response::Error(message.to_owned()),
            },
            8192,
        )
        .unwrap()
    }
    let mut pair = pair();
    let large_message = "L".repeat(4096);
    let large = error_frame(1, &large_message);
    send_tls_plaintext(&mut pair, &large);
    let (received, frame_len) = pair.session.receive(8192).unwrap().unwrap();
    let envelope: Envelope<Response> = pair.session.decode(&received[..frame_len], 8192).unwrap();
    assert_eq!(envelope.protocol, PROTOCOL);
    assert_eq!(envelope.id, 1);
    assert!(matches!(envelope.body, Response::Error(message) if message == large_message));
    pair.session.recycle_input(received);

    let small_message = "S".repeat(8);
    let small = error_frame(2, &small_message);
    let medium_message = "M".repeat(2048);
    let medium = error_frame(3, &medium_message);
    let mut remainder_and_next = small[9..].to_vec();
    remainder_and_next.extend_from_slice(&medium);
    send_tls_plaintext(&mut pair, &small[..9]);
    assert!(pair.session.receive(8192).unwrap().is_none());
    send_tls_plaintext(&mut pair, &remainder_and_next);
    let (received, frame_len) = pair.session.receive(8192).unwrap().unwrap();
    let envelope: Envelope<Response> = pair.session.decode(&received[..frame_len], 8192).unwrap();
    assert_eq!(envelope.protocol, PROTOCOL);
    assert_eq!(envelope.id, 2);
    assert!(matches!(envelope.body, Response::Error(message) if message == small_message));
    pair.session.recycle_input(received);

    let (received, frame_len) = pair.session.receive(8192).unwrap().unwrap();
    let envelope: Envelope<Response> = pair.session.decode(&received[..frame_len], 8192).unwrap();
    assert_eq!(envelope.protocol, PROTOCOL);
    assert_eq!(envelope.id, 3);
    assert!(matches!(envelope.body, Response::Error(message) if message == medium_message));
}

#[test]
fn reused_server_authentication_rejects_expired_client_credentials() {
    let pair = pair();
    revalidate_client_peer(
        Some(&pair.peer),
        Some(pair.client_verifier.as_ref()),
        rustls::pki_types::UnixTime::now(),
    )
    .unwrap();
    let expired = rustls::pki_types::UnixTime::since_unix_epoch(Duration::from_secs(4_102_444_800));
    assert!(
        revalidate_client_peer(
            Some(&pair.peer),
            Some(pair.client_verifier.as_ref()),
            expired
        )
        .is_err()
    );
}

#[derive(Default)]
struct Wire {
    bytes: Vec<u8>,
    accepted: u64,
    allowance: usize,
}
impl Write for Wire {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.allowance == 0 {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        let n = bytes.len().min(self.allowance);
        self.bytes.extend_from_slice(&bytes[..n]);
        self.allowance -= n;
        self.accepted += 1;
        Ok(n)
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn step(session: &mut Session, wire: &mut Wire, completed: u64) {
    session.write_plaintext(PROGRESS_BYTES).unwrap();
    if session.wants_tls_write() {
        match session.conn.as_mut().unwrap().write_tls(wire) {
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
            Err(error) => panic!("unexpected TLS write error: {error}"),
        }
    }
    session.finish_output(wire.accepted, completed);
}
fn submit_frames(session: &mut Session, wire: &mut Wire, count: usize) {
    for _ in 0..4096 {
        step(session, wire, 0);
        if session.submitted == count {
            return;
        }
    }
    panic!("frames failed to reach transport acceptance");
}

#[test]
fn later_tls_frame_submits_before_earlier_fence_and_preserves_stream_order() {
    let mut pair = pair();
    pair.session.configure_transport_stats(true, true);
    pair.session.queue(1, &vec![11u8; 8192], 16 * 1024).unwrap();
    pair.session.queue(2, &vec![22u8; 2048], 16 * 1024).unwrap();
    let expected: Vec<_> = pair
        .session
        .output
        .iter()
        .flat_map(|frame| frame.bytes.iter().copied())
        .collect();
    let retained = pair.session.output_bytes;
    let mut wire = Wire {
        allowance: 17,
        ..Wire::default()
    };
    step(&mut pair.session, &mut wire, 0);
    assert!(pair.session.wants_tls_write());
    assert_eq!(pair.session.submitted, 0);
    assert_eq!(pair.session.output[1].offset, 0);
    assert_eq!(pair.session.output_bytes, retained);
    assert!(pair.session.transport_stats().frame_gate_stalls > 0);
    // The next sweep cannot advance the second frame while ciphertext is blocked.
    step(&mut pair.session, &mut wire, 0);
    assert_eq!(pair.session.output[1].offset, 0);
    wire.allowance = usize::MAX;
    submit_frames(&mut pair.session, &mut wire, 2);
    assert!(pair.session.output_pending(1) && pair.session.output_pending(2));
    assert_eq!(pair.session.output_bytes, retained);
    assert_eq!(pair.session.transport_stats().frames_retired, 0);
    assert_eq!(pair.session.transport_stats().awaiting_retirement_frames, 2);
    let timing = pair.session.transport_stats();
    assert_eq!(timing.output_queue_samples, 2);
    assert_eq!(timing.output_submit_samples, 2);
    assert_eq!(timing.output_retire_samples, 0);
    let first_fence = pair.session.output[0].fence.unwrap();
    let second_fence = pair.session.output[1].fence.unwrap();
    assert!(second_fence > first_fence);

    let mut ciphertext = Cursor::new(wire.bytes);
    let mut plaintext = Vec::new();
    while ciphertext.position() < ciphertext.get_ref().len() as u64 {
        pair.peer.read_tls(&mut ciphertext).unwrap();
        let ready = pair
            .peer
            .process_new_packets()
            .unwrap()
            .plaintext_bytes_to_read();
        let start = plaintext.len();
        plaintext.resize(start + ready, 0);
        pair.peer
            .reader()
            .read_exact(&mut plaintext[start..])
            .unwrap();
    }
    assert_eq!(plaintext, expected);
    pair.session.finish_output(second_fence, first_fence);
    assert!(!pair.session.output_pending(1));
    assert!(pair.session.output_pending(2));
    assert_eq!(pair.session.transport_stats().output_retire_samples, 1);
    pair.session.finish_output(second_fence, second_fence);
    assert!(pair.session.output.is_empty());
    assert_eq!(pair.session.output_bytes, 0);
    assert_eq!(pair.session.transport_stats().output_retire_samples, 2);
}

#[test]
fn stalled_fences_bound_output_and_keep_actual_reply_leases() {
    let mut pair = pair();
    let admission = Arc::new(
        read_executor::ReadAdmission::new(read_executor::ReadExecutorConfig::default()).unwrap(),
    );
    let mut pending = HashMap::new();
    let mut retained = 0;
    for id in 1..=SESSION_REQUESTS as u64 {
        let charge = 4096;
        pair.session.queue(id, &vec![id as u8; 32], 1024).unwrap();
        pending.insert(
            id,
            ServerPending {
                deadline: Instant::now() + Duration::from_secs(2),
                charge,
                epoch: Some(1),
                read_generation: None,
                read_references: Vec::new(),
                authority_owners: Vec::new(),
                grant_limit: 0,
                grant_max_bytes: 0,
                profile: None,
                record_started: None,
                read_profile: ServerReadProfile::default(),
                response_queued: true,
                ready_response: None,
                response_ready_at: None,
                measured: false,
                output_started: None,
                _lease: admission.reserve_reply(charge).unwrap(),
                async_started: None,
                async_handler_ns: 0,
                control_queued: None,
                is_append: false,
            },
        );
        retained += charge;
        pair.session.stats.retained_reply_credit_bytes += charge as u64;
    }
    let owned = pair.session.output_bytes;
    let mut wire = Wire {
        allowance: usize::MAX,
        ..Wire::default()
    };
    submit_frames(&mut pair.session, &mut wire, SESSION_REQUESTS);
    for _ in 0..64 {
        step(&mut pair.session, &mut wire, 0);
        retire_responses(&mut pair.session, &mut pending, &mut retained, None);
    }
    assert_eq!(pending.len(), SESSION_REQUESTS);
    assert_eq!(admission.stats().inflight_requests, SESSION_REQUESTS);
    assert_eq!(admission.stats().inflight_bytes, retained);
    assert_eq!(pair.session.output_bytes, owned);
    assert!(owned <= SESSION_BYTES);
    assert!(pair.session.queue(33, &0u8, 1024).is_err());
    assert_eq!(
        pair.session.transport_stats().peak_output_frames,
        SESSION_REQUESTS as u64
    );
    pair.session.configure_transport_stats(true, true);
    assert_eq!(
        pair.session.transport_stats().retained_reply_credit_bytes,
        retained as u64
    );
    let first_fence = pair.session.output[0].fence.unwrap();
    pair.session.finish_output(wire.accepted, first_fence);
    retire_responses(&mut pair.session, &mut pending, &mut retained, None);
    assert_eq!(pending.len(), SESSION_REQUESTS - 1);
    assert_eq!(admission.stats().inflight_requests, SESSION_REQUESTS - 1);
    pair.session.finish_output(wire.accepted, wire.accepted);
    retire_responses(&mut pair.session, &mut pending, &mut retained, None);
    assert_eq!(admission.stats().inflight_requests, 0);
    assert_eq!(retained, 0);
    assert_eq!(
        pair.session.transport_stats().retained_reply_credit_bytes,
        0
    );
}

#[test]
fn abort_cancels_delivery_without_pretending_send_fences_completed() {
    let mut pair = pair();
    pair.session.queue(1, &vec![42u8; 4096], 8192).unwrap();
    let mut wire = Wire {
        allowance: usize::MAX,
        ..Wire::default()
    };
    submit_frames(&mut pair.session, &mut wire, 1);
    assert_eq!(pair.session.stats.frames_retired, 0);
    pair.session.abort();
    assert!(pair.session.output.is_empty());
    assert_eq!(pair.session.transport_stats().frames_retired, 0);
    assert!(pair.session.progress().is_err());
    assert!(pair.session.queue(2, &0u8, 1024).is_err());
}

#[test]
fn tcp_output_retires_at_ciphertext_acceptance_without_fabric_cq() {
    let mut pair = pair();
    pair.session.configure_transport_stats(true, true);
    pair.session.queue(1, &vec![7u8; 4096], 8192).unwrap();
    let mut wire = Wire {
        allowance: usize::MAX,
        ..Wire::default()
    };
    for _ in 0..4096 {
        pair.session.write_plaintext(PROGRESS_BYTES).unwrap();
        pair.session
            .conn
            .as_mut()
            .unwrap()
            .write_tls(&mut wire)
            .unwrap();
        // Ordinary socket writes transfer ownership immediately: fence zero.
        pair.session.finish_output(0, 0);
        if pair.session.output.is_empty() {
            assert_eq!(pair.session.transport_stats().frames_retired, 1);
            assert_eq!(pair.session.transport_stats().cq_polls, 0);
            assert_eq!(pair.session.transport_stats().output_retire_samples, 1);
            assert_eq!(pair.session.transport_stats().output_retire_ns, 0);
            return;
        }
    }
    panic!("TCP frame failed to retire");
}

#[test]
fn previous_wire_protocol_is_rejected_before_request_dispatch() {
    let temp = tempdir::TempDir::new("stale-transport-alpn").unwrap();
    let tls = certificates(temp.path());
    let mut config = (*tls.client_config().unwrap()).clone();
    config.alpn_protocols = vec![format!("tidehunter-tcp-v{}", PROTOCOL - 1).into_bytes()];
    let mut client =
        ClientConnection::new(Arc::new(config), ServerName::try_from("localhost").unwrap())
            .unwrap();
    let mut server = ServerConnection::new(tls.server_config().unwrap().config.unwrap()).unwrap();
    let mut hello = Vec::new();
    client.write_tls(&mut hello).unwrap();
    server.read_tls(&mut Cursor::new(hello)).unwrap();
    assert!(server.process_new_packets().is_err());
}

#[test]
fn fabric_control_health_is_separate_and_detects_death_at_its_bound() {
    let pair = pair();
    let start = Instant::now();
    let mut health = ControlPeerHealth { next_check: start };
    assert!(
        health
            .check(&pair.session.sock.socket, start, false)
            .unwrap()
    );
    pair._control_peer.shutdown(Shutdown::Both).unwrap();
    for _ in 0..1024 {
        assert!(
            !health
                .check(
                    &pair.session.sock.socket,
                    start + CONTROL_PEER_CHECK_INTERVAL / 2,
                    false,
                )
                .unwrap()
        );
    }
    let error = health
        .check(
            &pair.session.sock.socket,
            start + CONTROL_PEER_CHECK_INTERVAL,
            false,
        )
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
}

#[test]
fn fabric_control_health_rejects_unexpected_bytes_and_force_bypasses_gate() {
    let mut pair = pair();
    let start = Instant::now();
    let mut health = ControlPeerHealth { next_check: start };
    assert!(
        health
            .check(&pair.session.sock.socket, start, false)
            .unwrap()
    );
    pair._control_peer
        .write_all(b"invalid control traffic")
        .unwrap();
    assert!(
        !health
            .check(&pair.session.sock.socket, start, false)
            .unwrap()
    );
    let error = health
        .check(&pair.session.sock.socket, start, true)
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
}

#[test]
fn idle_readiness_wait_respects_deadline_and_shutdown_cap() {
    let now = Instant::now();
    assert_eq!(idle_wait_timeout(now - Duration::from_secs(1), now), 0);
    assert_eq!(idle_wait_timeout(now + Duration::from_micros(500), now), 0);
    assert_eq!(idle_wait_timeout(now + Duration::from_millis(2), now), 2);
    assert_eq!(idle_wait_timeout(now + Duration::from_secs(60), now), 10);
}

#[test]
fn only_drained_adaptive_tcp_can_wait_for_socket_readiness() {
    let mut pair = pair();
    let limits = TcpLimits::default();
    let idle_since = Instant::now();
    let deadline = idle_since + limits.io_timeout;
    assert_eq!(
        pair.session
            .idle_wait_deadline(limits, false, false, false, idle_since),
        Some(deadline),
    );
    for (progressed, pending) in [(true, false), (false, true), (true, true)] {
        assert_eq!(
            pair.session
                .idle_wait_deadline(limits, progressed, pending, false, idle_since),
            None,
            "workers, io_uring and deferred requests must keep making progress",
        );
    }
    assert_eq!(
        pair.session.idle_wait_deadline(
            TcpLimits {
                polling: PollingPolicy::Busy,
                ..limits
            },
            false,
            false,
            false,
            idle_since,
        ),
        None,
    );
    let established_deadline = pair
        .session
        .idle_wait_deadline(limits, false, false, true, idle_since)
        .unwrap();
    assert!(established_deadline > Instant::now());
    assert!(established_deadline <= Instant::now() + IDLE_SHUTDOWN_CHECK_INTERVAL);

    let partial_started = idle_since - Duration::from_secs(1);
    pair.session.input_started = Some(partial_started);
    assert_eq!(
        pair.session
            .idle_wait_deadline(limits, false, false, false, idle_since),
        Some(partial_started + limits.io_timeout),
        "partial-frame deadline must not be renewed by the idle policy",
    );
    assert!(
        pair.session
            .partial_expired(partial_started + limits.io_timeout, limits.io_timeout)
    );
    pair.session.input_started = None;
    pair.session
        .conn
        .as_mut()
        .unwrap()
        .writer()
        .write_all(b"TLS output")
        .unwrap();
    assert!(pair.session.wants_tls_write());
    assert_eq!(
        pair.session
            .idle_wait_deadline(limits, false, false, false, idle_since),
        None,
    );
}

#[test]
fn queued_response_never_parks_on_socket_readability() {
    let mut pair = pair();
    pair.session.queue(1, &vec![7_u8; 32], 1024).unwrap();
    assert_eq!(
        pair.session
            .idle_wait_deadline(TcpLimits::default(), false, false, false, Instant::now(),),
        None,
    );
}

#[test]
fn idle_socket_wait_wakes_on_data_without_consuming_it() {
    let pair = pair();
    assert!(!wait_for_readable(&pair.session.sock.socket, Instant::now()).unwrap());
    let (start, started) = mpsc::sync_channel(1);
    let mut peer = pair._control_peer.try_clone().unwrap();
    let sender = thread::spawn(move || {
        started.recv_timeout(Duration::from_secs(2)).unwrap();
        peer.write_all(b"wake").unwrap();
    });
    start.send(()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    while !wait_for_readable(&pair.session.sock.socket, deadline).unwrap() {
        assert!(
            Instant::now() < deadline,
            "readiness never observed the peer"
        );
    }
    sender.join().unwrap();
    let mut bytes = [0; 4];
    let available = pair.session.sock.socket.peek(&mut bytes).unwrap();
    assert_ne!(available, 0);
    assert_eq!(&bytes[..available], &b"wake"[..available]);
    // Readiness is level-triggered, not an edge that can be lost before parking.
    assert!(wait_for_readable(&pair.session.sock.socket, Instant::now()).unwrap());
}

#[test]
fn idle_socket_wait_observes_peer_close() {
    let pair = pair();
    pair._control_peer.shutdown(Shutdown::Both).unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    while !wait_for_readable(&pair.session.sock.socket, deadline).unwrap() {
        assert!(Instant::now() < deadline, "readiness never observed EOF");
    }
    assert_eq!(pair.session.sock.socket.peek(&mut [0]).unwrap(), 0);
}

#[test]
fn only_fabric_spins_and_each_empty_burst_is_bounded() {
    let mut polling = ActivePoll::default();
    for expected in 1..=64 {
        assert_eq!(
            polling.pause(PollingPolicy::Adaptive, true, false),
            (false, true)
        );
        assert_eq!(polling.spins, expected);
    }
    assert_eq!(
        polling.pause(PollingPolicy::Adaptive, true, false),
        (true, false)
    );
    assert_eq!(polling.spins, 0);
    assert_eq!(
        polling.pause(PollingPolicy::Adaptive, false, false),
        (true, false)
    );
    assert_eq!(polling.spins, 0);
    polling.pause(PollingPolicy::Adaptive, true, false);
    polling.pause(PollingPolicy::Adaptive, true, true);
    assert_eq!(polling.spins, 0);
}

#[test]
fn busy_polling_never_requests_scheduler_yield_for_either_transport() {
    let mut polling = ActivePoll::default();
    for fabric in [false, true] {
        for _ in 0..256 {
            assert_eq!(
                polling.pause(PollingPolicy::Busy, fabric, false),
                (false, true)
            );
        }
        assert_eq!(
            polling.pause(PollingPolicy::Busy, fabric, true),
            (false, false)
        );
    }
}

#[test]
fn ready_output_turn_does_not_poll_receive_or_cq() {
    let mut pair = pair();
    pair.session.configure_transport_stats(true, true);
    pair.session.queue(1, &vec![7_u8; 32], 1024).unwrap();
    assert!(pair.session.progress_output().unwrap());
    let stats = pair.session.transport_stats();
    assert_eq!(stats.frames_retired, 1);
    assert_eq!(stats.output_queue_samples, 1);
    assert_eq!(stats.output_submit_samples, 1);
    assert_eq!(stats.output_retire_samples, 1);
    assert_eq!(stats.tls_read.calls, 0);
    assert_eq!(stats.cq_polls, 0);
    pair.session.abort();
    assert!(pair.session.progress_output().is_err());
}

#[test]
fn reset_drops_timing_not_live_output_ownership() {
    let mut pair = pair();
    pair.session.configure_transport_stats(true, true);
    pair.session.queue(1, &vec![7_u8; 32], 1024).unwrap();
    let mut wire = Wire {
        allowance: usize::MAX,
        ..Wire::default()
    };
    submit_frames(&mut pair.session, &mut wire, 1);
    assert_eq!(pair.session.transport_stats().output_submit_samples, 1);
    pair.session.configure_transport_stats(true, true);
    assert!(pair.session.output_pending(1));
    pair.session.finish_output(wire.accepted, wire.accepted);
    assert_eq!(pair.session.transport_stats().frames_retired, 1);
    assert_eq!(pair.session.transport_stats().output_retire_samples, 0);
    pair.session.configure_transport_stats(false, false);
    pair.session.queue(2, &vec![8_u8; 32], 1024).unwrap();
    assert!(pair.session.progress_output().unwrap());
    assert_eq!(pair.session.transport_stats().output_queue_samples, 0);
    assert_eq!(pair.session.transport_stats().output_submit_samples, 0);
}

fn completed_read(measured: bool, profiled: bool) -> HashMap<u64, ServerPending> {
    let admission =
        read_executor::ReadAdmission::new(read_executor::ReadExecutorConfig::default()).unwrap();
    HashMap::from([(
        1,
        ServerPending {
            deadline: Instant::now() + Duration::from_secs(2),
            charge: 4096,
            epoch: Some(1),
            read_generation: None,
            read_references: Vec::new(),
            authority_owners: Vec::new(),
            grant_limit: 0,
            grant_max_bytes: 0,
            profile: profiled.then(Instant::now),
            record_started: profiled.then(Instant::now),
            read_profile: ServerReadProfile::default(),
            response_queued: false,
            ready_response: None,
            response_ready_at: None,
            measured,
            output_started: None,
            _lease: Arc::new(admission).reserve_reply(4096).unwrap(),
            async_started: measured.then(Instant::now),
            async_handler_ns: 0,
            control_queued: None,
            is_append: false,
        },
    )])
}
#[cfg(feature = "experimental-fabric")]
#[test]
#[ignore = "requires libfabric tcp;ofi_rxm software provider"]
fn forced_server_grant_request_with_real_authority_uses_message_only_fallback() {
    use super::super::tests::{batch, config, metrics};
    use crate::distributed::storage::WalMemberReadPolicy;

    let mut pair = pair();
    let mut source_endpoint = tidehunter_fabric::Endpoint::open("tcp;ofi_rxm", Some("lo")).unwrap();
    let mut peer_endpoint = tidehunter_fabric::Endpoint::open("tcp;ofi_rxm", Some("lo")).unwrap();
    source_endpoint
        .set_peer(&peer_endpoint.address().unwrap())
        .unwrap();
    peer_endpoint
        .set_peer(&source_endpoint.address().unwrap())
        .unwrap();
    pair.session.sock.fabric = Some(source_endpoint);
    let mut node_config = config(&pair._temp.path().join("node"));
    let source = ResidentReadConfig {
        source_bytes: 4096,
        source_regions: 2,
        max_read_bytes: 1024,
        ..ResidentReadConfig::default()
    };
    node_config.wal_member_read = WalMemberReadPolicy::Async;
    node_config.resident_reads = source;
    let node = StorageNode::open(node_config.clone(), metrics()).unwrap();
    node.inventory.lock().fence_to(1).unwrap();

    let batch = batch(&node_config, 1, 0);
    let Response::Appended(receipts) = node
        .handle(
            Request::Append {
                epoch: 1,
                batches: vec![batch.encoded().clone().into()],
            },
            TcpLimits::default(),
        )
        .unwrap()
    else {
        panic!("append did not return receipts");
    };
    let reference = batch.record_reference(receipts[0].address, 0).unwrap();
    let snapshot = node.reader_snapshot(1, &[reference]).unwrap();
    let owner = snapshot.members[0]
        .0
        .plan_record_read(&reference)
        .unwrap()
        .resident_owner();
    snapshot.members[0]
        .0
        .validate_durable_record_reference(&reference)
        .unwrap();
    let records = vec![
        snapshot.members[0]
            .0
            .read_record_encoded(&reference)
            .unwrap(),
    ];
    assert_eq!(
        CompleteBatch::decode_record(
            &KeyShape::from_yaml(&node_config.shape_yaml).unwrap(),
            &reference,
            records[0].clone(),
        )
        .unwrap(),
        batch.mutations()[0]
    );

    let request = ServerPending {
        deadline: Instant::now() + Duration::from_secs(2),
        charge: read_charge(&[reference]).unwrap(),
        epoch: Some(1),
        read_generation: Some(snapshot.generation),
        read_references: vec![reference],
        authority_owners: vec![owner],
        grant_limit: 1,
        grant_max_bytes: 1024,
        profile: None,
        record_started: None,
        read_profile: ServerReadProfile::default(),
        response_queued: false,
        ready_response: None,
        response_ready_at: None,
        measured: false,
        output_started: None,
        _lease: None,
        async_started: None,
        async_handler_ns: 0,
        control_queued: None,
        is_append: false,
    };
    assert!(
        resident_grants_for_read(
            &node,
            &mut pair.session,
            1,
            source,
            &mut Vec::new(),
            &request,
            &records,
        )
        .unwrap()
        .is_none()
    );
    assert_eq!(node.resident_grants.live_stats(), (0, 0, 0));
}
fn finish_test_read(
    node: &StorageNode,
    pending: &mut HashMap<u64, ServerPending>,
    result: Result<Vec<minibytes::Bytes>, TcpError>,
) -> Result<(), TcpError> {
    finish_read(node, pending, 1, result)
}
#[test]
fn completed_response_rejects_fenced_or_poisoned_authority() {
    use super::super::tests::{config, metrics};
    for poison in [false, true] {
        let mut pair = pair();
        let node = StorageNode::open(config(&pair._temp.path().join("node")), metrics()).unwrap();
        node.inventory.lock().fence_to(1).unwrap();
        let generation = node.reader_snapshot(1, &[]).unwrap().generation;
        let mut pending = completed_read(true, true);
        pending.get_mut(&1).unwrap().read_generation = Some(generation);
        finish_test_read(
            &node,
            &mut pending,
            Ok(vec![b"completed record"[..].into()]),
        )
        .unwrap();
        pending.get_mut(&1).unwrap().response_ready_at =
            Some(Instant::now() - Duration::from_millis(1));
        let mut profile = ConnectionProfile::default();
        if poison {
            node.poison_readers();
        } else {
            node.inventory.lock().fence_to(2).unwrap();
        }
        assert!(
            flush_responses(
                &node,
                &mut pair.session,
                &mut pending,
                TcpLimits::default(),
                Some(&mut profile),
            )
            .unwrap()
        );
        assert_eq!(profile.read_response_recheck_samples, 1);
        assert_eq!(profile.read_response_recheck_deferrals, 0);
        assert_eq!(profile.read_response_recheck_errors, 1);
        assert_eq!(profile.response_send_ns, 0);
        let frame = pair.session.output.front().unwrap().flatten()[4..].to_vec();
        let reply: Envelope<Response> = pair.session.decode(&frame, 4096).unwrap();
        assert!(matches!(reply.body, Response::Error(_)));
        assert!(pending[&1]._lease.is_some());
        assert!(queue_response(&mut pending, 1, Response::Error("duplicate".into())).is_err());
    }
}

#[test]
fn recheck_profiles_success_and_preserves_member_errors() {
    use super::super::tests::{config, metrics};
    for failed_read in [false, true] {
        let mut pair = pair();
        let node = StorageNode::open(config(&pair._temp.path().join("node")), metrics()).unwrap();
        node.inventory.lock().fence_to(1).unwrap();
        let generation = node.reader_snapshot(1, &[]).unwrap().generation;
        let mut pending = completed_read(false, true);
        pending.get_mut(&1).unwrap().read_generation = Some(generation);
        let result = if failed_read {
            Err(TcpError::new("member read failed"))
        } else {
            Ok(vec![b"completed record"[..].into()])
        };
        finish_test_read(&node, &mut pending, result).unwrap();
        assert!(
            flush_responses(
                &node,
                &mut pair.session,
                &mut pending,
                TcpLimits::default(),
                None,
            )
            .unwrap()
        );
        let frame = pair.session.output.front().unwrap().flatten()[4..].to_vec();
        let reply: Envelope<Response> = pair.session.decode(&frame, 4096).unwrap();
        match reply.body {
            Response::RecordsProfiled { records, profile } if !failed_read => {
                assert_eq!(records[0].as_slice(), b"completed record");
                assert_eq!(profile.response_recheck_samples, 1);
                assert_eq!(profile.response_recheck_deferrals, 0);
                assert_eq!(
                    wire_codec(4096).serialized_size(&profile).unwrap() as usize,
                    SERVER_READ_PROFILE_BYTES,
                );
            }
            Response::Error(message) if failed_read => {
                assert_eq!(message, "member read failed");
            }
            _ => panic!("read error or profile was lost"),
        }
    }
}

#[test]
fn ordinary_completed_reads_do_not_start_recheck_clocks() {
    use super::super::tests::{config, metrics};
    let pair = pair();
    let node = StorageNode::open(config(&pair._temp.path().join("node")), metrics()).unwrap();
    node.inventory.lock().fence_to(1).unwrap();
    let generation = node.reader_snapshot(1, &[]).unwrap().generation;
    let mut pending = completed_read(false, false);
    pending.get_mut(&1).unwrap().read_generation = Some(generation);
    finish_test_read(&node, &mut pending, Ok(vec![b"record"[..].into()])).unwrap();
    assert!(pending[&1].response_ready_at.is_none());
    assert_eq!(pending[&1].read_profile.response_recheck_deferrals, 0);
    assert!(matches!(
        pending[&1].ready_response.as_ref(),
        Some(Response::Records(_))
    ));
}

#[cfg(target_os = "linux")]
#[test]
fn submitted_ready_cqes_share_rechecks_deadlines_and_reply_retirement() {
    use super::super::tests::{batch, config, metrics};
    for (fenced, expired) in [(false, false), (true, false), (false, true)] {
        let mut pair = pair();
        let config = config(&pair._temp.path().join("node"));
        let node = StorageNode::open(config.clone(), metrics()).unwrap();
        node.inventory.lock().fence_to(1).unwrap();
        let batch = batch(&config, 1, 0);
        let Response::Appended(receipts) = node
            .handle(
                Request::Append {
                    epoch: 1,
                    batches: vec![batch.encoded().clone().into()],
                },
                TcpLimits::default(),
            )
            .unwrap()
        else {
            panic!("append did not return receipts")
        };
        let reference = batch.record_reference(receipts[0].address, 0).unwrap();
        let snapshot = node.reader_snapshot(1, &[reference]).unwrap();
        let mut pending = completed_read(true, true);
        let request = pending.get_mut(&1).unwrap();
        request.read_generation = Some(snapshot.generation);
        request.charge = read_charge(&[reference]).unwrap();
        request._lease = node.read_admission.reserve_reply(request.charge).unwrap();
        let mut executor =
            read_executor::ReadExecutor::new(node.read_config, node.read_admission.clone())
                .unwrap();
        let ReadPlanning::Ready(plans) = resident_read_plans(snapshot.members).unwrap() else {
            panic!("current append authority must be resident")
        };
        assert!(matches!(
            executor
                .submit_planned(
                    1,
                    plans,
                    ReadClass::Point,
                    &mut pending.get_mut(&1).unwrap()._lease,
                )
                .unwrap(),
            read_executor::SubmitOutcome::Pending,
        ));
        // Deterministically leave a real ready CQE for the exact helper called
        // after the server's submission turn. Do not synthesize record bytes.
        executor.wait_for_ready_completion().unwrap();
        #[cfg(feature = "experimental-fabric")]
        let mut resident_grants = Vec::new();
        #[cfg(feature = "experimental-fabric")]
        let source = ResidentReadConfig::default();
        #[cfg(feature = "experimental-fabric")]
        let progressed = harvest_reads(
            &node,
            &mut pair.session,
            1,
            source,
            &mut resident_grants,
            &mut executor,
            &mut pending,
        )
        .unwrap();
        #[cfg(not(feature = "experimental-fabric"))]
        let progressed = harvest_reads(&node, &mut executor, &mut pending).unwrap();
        assert!(progressed);
        #[cfg(feature = "experimental-fabric")]
        let progressed = harvest_reads(
            &node,
            &mut pair.session,
            1,
            source,
            &mut resident_grants,
            &mut executor,
            &mut pending,
        )
        .unwrap();
        #[cfg(not(feature = "experimental-fabric"))]
        let progressed = harvest_reads(&node, &mut executor, &mut pending).unwrap();
        assert!(!progressed);
        assert!(pending[&1].ready_response.is_some());
        assert!(pending[&1]._lease.is_some());
        assert_eq!(node.read_admission.stats().inflight_reads, 0);
        assert_eq!(node.read_admission.stats().inflight_requests, 1);
        if fenced {
            node.inventory.lock().fence_to(2).unwrap();
        }
        if expired {
            pending.get_mut(&1).unwrap().deadline = Instant::now();
        }
        let queued = flush_responses(
            &node,
            &mut pair.session,
            &mut pending,
            TcpLimits::default(),
            None,
        );
        if expired {
            assert!(queued.is_err());
            assert!(pair.session.output.is_empty());
            assert_eq!(node.read_admission.stats().inflight_requests, 1);
            drop(pending);
            assert_eq!(node.read_admission.stats().inflight_requests, 0);
            continue;
        }
        assert!(queued.unwrap());
        let frame = pair.session.output.front().unwrap().flatten()[4..].to_vec();
        let reply: Envelope<Response> = pair.session.decode(&frame, 4096).unwrap();
        if fenced {
            assert!(matches!(reply.body, Response::Error(_)));
        } else {
            let Response::RecordsProfiled { records, .. } = reply.body else {
                panic!("completed read lost its profile")
            };
            assert_eq!(
                CompleteBatch::decode_record(
                    &KeyShape::from_yaml(&config.shape_yaml).unwrap(),
                    &reference,
                    records.into_iter().next().unwrap().into_bytes(),
                )
                .unwrap(),
                batch.mutations()[0],
            );
        }
        let mut wire = Wire {
            allowance: usize::MAX,
            ..Wire::default()
        };
        submit_frames(&mut pair.session, &mut wire, 1);
        let mut retained = pending[&1].charge;
        retire_responses(&mut pair.session, &mut pending, &mut retained, None);
        assert_eq!(pending.len(), 1, "TLS acceptance is not CQ retirement");
        assert_eq!(node.read_admission.stats().inflight_requests, 1);
        pair.session.finish_output(wire.accepted, wire.accepted);
        retire_responses(&mut pair.session, &mut pending, &mut retained, None);
        assert!(pending.is_empty());
        assert_eq!(retained, 0);
        assert_eq!(node.read_admission.stats().inflight_requests, 0);
    }
}

#[cfg(feature = "experimental-fabric")]
#[test]
#[ignore = "requires libfabric tcp;ofi_rxm software provider"]
fn expired_posted_rma_read_fail_stops_with_grant_memory_quarantined() {
    use super::super::tests::{batch, config, metrics};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, Instant};

    let temp = tempdir::TempDir::new("resident-rma-deadline").unwrap();
    let mut node_config = config(&temp.path().join("node"));
    let source = ResidentReadConfig {
        source_bytes: 64 * 1024,
        source_regions: 4,
        max_read_bytes: 4096,
        ..ResidentReadConfig::default()
    };
    let destination = ResidentReadConfig {
        destination_bytes: 16 * 1024,
        destination_slots: 4,
        max_handles: 4,
        max_read_bytes: 4096,
        ..ResidentReadConfig::default()
    };
    node_config.resident_reads = source;
    node_config.wal_member_read = crate::wal::WalMemberReadPolicy::Async;
    let node = StorageNode::open(node_config.clone(), metrics()).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = listener.local_addr().unwrap().to_string();
    let stop = Arc::new(AtomicBool::new(false));
    let server_stop = Arc::clone(&stop);
    let server = node.clone();
    let server_tls = TlsFiles {
        transport: Transport::FabricTcpNative,
        ca: PathBuf::new(),
        cert: PathBuf::new(),
        key: PathBuf::new(),
        server_name: "trusted-software-provider-test".to_owned(),
        resident_reads: source,
    };
    let worker = thread::spawn(move || {
        server
            .serve(listener, server_tls, TcpLimits::default(), server_stop)
            .unwrap();
    });
    let client_tls = TlsFiles {
        transport: Transport::FabricTcpNative,
        ca: PathBuf::new(),
        cert: PathBuf::new(),
        key: PathBuf::new(),
        server_name: "trusted-software-provider-test".to_owned(),
        resident_reads: destination,
    };
    let mut client = Client::connect(
        &endpoint,
        &client_tls,
        Duration::from_secs(3),
        node_config.partition.clone(),
        &serde_yaml::from_str(&node_config.shape_yaml).unwrap(),
    )
    .unwrap();
    client.fence_to(1).unwrap();
    let batch = batch(&node_config, 1, 0);
    let receipt = client.append_many(1, std::slice::from_ref(&batch)).unwrap();
    let reference = batch
        .record_reference(receipt[0].appended().address(), 0)
        .unwrap();
    client
        .read_records_correlated(1, &[reference], ReadClass::Point, None)
        .unwrap();

    let read_id = client
        .submit_read(
            1,
            vec![reference],
            Instant::now() + Duration::from_secs(3),
            ReadClass::Point,
            false,
        )
        .unwrap()
        .unwrap();
    assert!(client.progress_reads().unwrap().is_empty());
    assert_eq!(client.resident_read_stats().rma_posts, 1);
    client.reactor.reads.get_mut(&read_id).unwrap().deadline = Instant::now();
    assert!(client.progress_reads().is_err());
    assert!(client.broken);

    drop(client);
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        if node.resident_grants.live_stats().2 != 0 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "lost DMA owner was not retained in grant quarantine"
        );
        thread::yield_now();
    }
    stop.store(true, Ordering::Release);
    worker.join().unwrap();
}
