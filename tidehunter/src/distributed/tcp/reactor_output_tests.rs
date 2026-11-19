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
    deliver_peer_tls(pair);
}
/// The peer writes `bytes`, then ends its TLS stream with close_notify.
fn close_after_tls_plaintext(pair: &mut Pair, bytes: &[u8]) {
    pair.peer.writer().write_all(bytes).unwrap();
    pair.peer.send_close_notify();
    deliver_peer_tls(pair);
}
fn deliver_peer_tls(pair: &mut Pair) {
    let mut encrypted = Vec::new();
    while pair.peer.wants_write() {
        pair.peer.write_tls(&mut encrypted).unwrap();
    }
    let mut encrypted = Cursor::new(encrypted);
    while encrypted.position() < encrypted.get_ref().len() as u64 {
        let conn = pair.session.conn.as_mut().unwrap();
        assert_ne!(conn.read_tls(&mut encrypted).unwrap(), 0);
        let state = conn.process_new_packets().unwrap();
        pair.session.plaintext_pending = state.plaintext_bytes_to_read();
        pair.session.peer_closed = state.peer_has_closed();
    }
}
/// Decrypts everything the session wrote to its socket until the socket closed.
/// Returns the plaintext and whether the stream ended with close_notify.
fn ciphertext_seen_by_peer(
    control_peer: &mut TcpStream,
    peer: &mut rustls::Connection,
) -> (Vec<u8>, bool) {
    let mut ciphertext = Vec::new();
    control_peer.read_to_end(&mut ciphertext).unwrap();
    let mut ciphertext = Cursor::new(ciphertext);
    let mut closed = false;
    while ciphertext.position() < ciphertext.get_ref().len() as u64 {
        assert_ne!(peer.read_tls(&mut ciphertext).unwrap(), 0);
        closed |= peer.process_new_packets().unwrap().peer_has_closed();
    }
    let mut plaintext = Vec::new();
    match peer.reader().read_to_end(&mut plaintext) {
        // Ok(0) after close_notify; WouldBlock if the stream did not close.
        Ok(_) => {}
        Err(error) => assert_eq!(error.kind(), io::ErrorKind::WouldBlock),
    }
    (plaintext, closed)
}

#[test]
fn close_notice_reaches_the_peer_before_tls_close_notify() {
    let mut pair = pair();
    let reason = "owner request deadline expired: request 7 (kind 29) was still executing";
    pair.session.send_close_notice(reason, 8192);
    pair.session.abort();
    let (plaintext, closed) = ciphertext_seen_by_peer(&mut pair._control_peer, &mut pair.peer);
    assert!(closed, "abort must end the TLS stream with close_notify");
    let len = u32::from_be_bytes(plaintext[..4].try_into().unwrap()) as usize;
    assert_eq!(
        plaintext.len(),
        4 + len,
        "exactly one notice frame precedes the close"
    );
    let notice: Envelope<Response> = pair.session.decode(&plaintext[4..], 8192).unwrap();
    assert_eq!(notice.id, 0, "the notice uses the ID no request can have");
    assert!(matches!(notice.body, Response::Error(message) if message == reason));
}

#[test]
fn dropped_tls_session_ends_with_close_notify() {
    let Pair {
        session,
        mut peer,
        _control_peer: mut control_peer,
        ..
    } = pair();
    drop(session);
    let (plaintext, closed) = ciphertext_seen_by_peer(&mut control_peer, &mut peer);
    assert!(plaintext.is_empty());
    assert!(
        closed,
        "a client going away must not look like a truncated session"
    );
}

#[test]
fn peer_close_is_clean_only_at_a_frame_boundary() {
    let frame = encode_frame(
        &Envelope {
            protocol: PROTOCOL,
            id: 1,
            body: Response::Error("last reply".to_owned()),
        },
        8192,
    )
    .unwrap();

    let mut whole = pair();
    close_after_tls_plaintext(&mut whole, &frame);
    let (bytes, len) = whole.session.receive(8192).unwrap().unwrap();
    assert_eq!(&bytes[..len], &frame[4..]);
    assert!(whole.session.peer_finished());
    assert_eq!(
        whole.session.receive(8192).unwrap_err().to_string(),
        "peer closed the session (TLS close_notify)"
    );

    let mut cut = pair();
    close_after_tls_plaintext(&mut cut, &frame[..frame.len() - 1]);
    let error = loop {
        match cut.session.receive(8192) {
            Ok(None) => {}
            Ok(Some(_)) => panic!("a frame cut by the close was delivered"),
            Err(error) => break error,
        }
    };
    assert!(!cut.session.peer_finished());
    assert_eq!(
        error.to_string(),
        "truncated transport frame: peer closed the session inside a frame"
    );
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
    pair.session.progress(None).unwrap();
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

/// One pending owner reply with the fields the output path reads. The read
/// executor's reply lease was removed with the owner-RPC read route; the
/// session's `charge` accounting is the remaining reply credit.
fn pending_reply(charge: usize, response_queued: bool, deadline: Instant) -> ServerPending {
    ServerPending {
        partition: 0,
        deadline,
        kind: 8,
        timeout: Duration::from_secs(2),
        charge,
        response_queued,
        ready_response: None,
        measured: false,
        output_started: None,
        control_queued: None,
        is_append: false,
        owner_trace: false,
    }
}

#[test]
fn stalled_fences_bound_output_and_keep_actual_reply_leases() {
    let mut pair = pair();
    let mut pending = HashMap::new();
    let mut retained = 0;
    for id in 1..=SESSION_REQUESTS as u64 {
        let charge = 4096;
        pair.session.queue(id, &vec![id as u8; 32], 1024).unwrap();
        pending.insert(
            id,
            pending_reply(charge, true, Instant::now() + Duration::from_secs(2)),
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
    assert_eq!(retained, SESSION_REQUESTS * 4096);
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
    assert_eq!(retained, (SESSION_REQUESTS - 1) * 4096);
    pair.session.finish_output(wire.accepted, wire.accepted);
    retire_responses(&mut pair.session, &mut pending, &mut retained, None);
    assert!(pending.is_empty());
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
    assert!(pair.session.progress(None).is_err());
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

/// Ported from the owner-RPC read-completion test (removed route): a ready
/// reply past its owner deadline closes the session without queueing, and a
/// queued reply keeps its charge until transport retirement, not TLS
/// acceptance.
#[test]
fn ready_reply_deadline_and_retirement_follow_transport_fences() {
    for expired in [false, true] {
        let mut pair = pair();
        let charge = 4096;
        let deadline = if expired {
            Instant::now()
        } else {
            Instant::now() + Duration::from_secs(2)
        };
        let mut request = pending_reply(charge, false, deadline);
        request.ready_response = Some(Response::Error("ready reply".into()));
        let mut pending = HashMap::from([(1, request)]);
        let queued = flush_responses(&mut pair.session, &mut pending, TcpLimits::default(), None);
        if expired {
            assert!(queued.is_err());
            assert!(pair.session.output.is_empty());
            assert_eq!(pair.session.stats.retained_reply_credit_bytes, 0);
            continue;
        }
        assert!(queued.unwrap());
        assert_eq!(
            pair.session.stats.retained_reply_credit_bytes,
            charge as u64
        );
        let frame = pair.session.output.front().unwrap().flatten()[4..].to_vec();
        let reply: Envelope<Response> = pair.session.decode(&frame, 4096).unwrap();
        assert!(matches!(reply.body, Response::Error(message) if message == "ready reply"));
        let mut wire = Wire {
            allowance: usize::MAX,
            ..Wire::default()
        };
        submit_frames(&mut pair.session, &mut wire, 1);
        let mut retained = charge;
        retire_responses(&mut pair.session, &mut pending, &mut retained, None);
        assert_eq!(pending.len(), 1, "TLS acceptance is not CQ retirement");
        pair.session.finish_output(wire.accepted, wire.accepted);
        retire_responses(&mut pair.session, &mut pending, &mut retained, None);
        assert!(pending.is_empty());
        assert_eq!(retained, 0);
        assert_eq!(pair.session.stats.retained_reply_credit_bytes, 0);
    }
}

#[test]
fn owner_timeline_maximal_record_fits_minimum_wire_frame() {
    use crate::distributed::write_timeline::OwnerTimelineRecord;
    use bincode::Options;
    let record = OwnerTimelineRecord {
        owner: u32::MAX,
        envelope_id: u64::MAX,
        epoch: Some(u64::MAX),
        success: false,
        error: Some("\u{1}".repeat(128)),
        error_truncated: true,
        control_queue_ns: Some(u64::MAX),
        session_admission_wait_ns: Some(u64::MAX),
        appender_queue_ns: Some(u64::MAX),
        flusher_queue_ns: Some(u64::MAX),
        append_barrier_wait_ns: Some(u64::MAX),
        append_wal_ns: Some(u64::MAX),
        append_validation_ns: Some(u64::MAX),
        append_basic_validation_ns: Some(u64::MAX),
        append_conflict_lookup_ns: Some(u64::MAX),
        append_geometry_capacity_ns: Some(u64::MAX),
        append_metadata_reservation_ns: Some(u64::MAX),
        append_descriptor_preparation_ns: Some(u64::MAX),
        append_errors: u64::MAX,
        append_retries: u64::MAX,
        append_persist_ns: Some(u64::MAX),
        wal_file_persist_ns: Some(u64::MAX),
        handler_ns: Some(u64::MAX),
        response_ready_ns: Some(u64::MAX),
        namespace_persist_ns: Some(u64::MAX),
        file_persist_calls: u64::MAX,
        namespace_persist_calls: u64::MAX,
        appended_groups: u64::MAX,
        batch_count: Some(usize::MAX),
        encoded_bytes: Some(usize::MAX),
        persistence_capture: Some(crate::distributed::storage::PersistenceCapture {
            durable_end: u64::MAX,
            captured_appended_end: u64::MAX,
            cumulative_appended_groups: u64::MAX,
        }),
        ready_append_prefix: Some(crate::distributed::write_timeline::ReadyAppendPrefix {
            commands: usize::MAX,
            batches: usize::MAX,
            encoded_bytes: usize::MAX,
        }),
    };
    let minimum_frame = Envelope {
        protocol: PROTOCOL,
        id: u64::MAX,
        body: Response::OwnerTimelinePage {
            records: vec![record.clone()],
            dropped: u64::MAX,
            incomplete: u64::MAX,
            more: true,
        },
    };
    assert!(wire_codec(1024).serialized_size(&minimum_frame).unwrap() <= 1024);
}
