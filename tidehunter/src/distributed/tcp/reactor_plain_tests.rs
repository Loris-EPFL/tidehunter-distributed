//! Trusted-network TCP sessions (no TLS) and the session output byte budget.
//! Real loopback sockets and the real session; no storage node is involved.
use super::*;
use std::io::Write as _;
use std::net::TcpStream;

fn socket_pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let left = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let (right, _) = listener.accept().unwrap();
    (left, right)
}

/// A plain session plus the raw peer socket it talks to.
fn plain_session() -> (Session, TcpStream) {
    let (left, right) = socket_pair();
    let io = DeadlineIo::new(left, Duration::from_secs(2)).unwrap();
    (Session::new_plain(io).unwrap(), right)
}

fn drive<T>(deadline: Duration, mut step: impl FnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + deadline;
    loop {
        if let Some(done) = step() {
            return done;
        }
        assert!(Instant::now() < deadline, "plain session made no progress");
        thread::yield_now();
    }
}

#[test]
fn plain_session_writes_the_codec_frame_bytes_unencrypted() {
    let (mut session, mut raw) = plain_session();
    // Larger than one progress turn, so the write crosses several socket writes.
    let body = vec![0xA5_u8; 3 * PROGRESS_BYTES + 17];
    session.queue(7, &body, 1 << 20).unwrap();
    let expected = session.output.back().unwrap().bytes.clone();
    raw.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    let reader = thread::spawn(move || {
        let mut received = vec![0; expected.len()];
        io::Read::read_exact(&mut raw, &mut received).unwrap();
        (received, expected)
    });
    drive(Duration::from_secs(2), || {
        session.progress(None).unwrap();
        session.output.is_empty().then_some(())
    });
    let (received, expected) = reader.join().unwrap();
    assert_eq!(
        received, expected,
        "no TLS record framing may alter the frame"
    );
    let frame: Envelope<Vec<u8>> = session.decode(&received[4..], 1 << 20).unwrap();
    assert_eq!((frame.protocol, frame.id), (PROTOCOL, 7));
    assert_eq!(frame.body, body);
    let stats = session.transport_stats();
    assert_eq!((stats.frames_submitted, stats.frames_retired), (1, 1));
    assert_eq!(session.output_bytes, 0);
}

#[test]
fn plain_session_reassembles_a_frame_split_across_reads() {
    let (mut session, mut raw) = plain_session();
    let body = vec![3_u8; 5000];
    let mut sender = plain_session().0;
    sender.queue(9, &body, 1 << 16).unwrap();
    let frame = sender.output.back().unwrap().bytes.clone();
    // Header split inside its four bytes, then a partial body, then the rest.
    raw.write_all(&frame[..2]).unwrap();
    drive(Duration::from_secs(2), || {
        assert!(session.receive(1 << 16).unwrap().is_none());
        (session.header_used == 2).then_some(())
    });
    raw.write_all(&frame[2..1000]).unwrap();
    drive(Duration::from_secs(2), || {
        assert!(session.receive(1 << 16).unwrap().is_none());
        (session.input_used == 996).then_some(())
    });
    raw.write_all(&frame[1000..]).unwrap();
    let (bytes, len) = drive(Duration::from_secs(2), || session.receive(1 << 16).unwrap());
    let decoded: Envelope<Vec<u8>> = session.decode(&bytes[..len], 1 << 16).unwrap();
    assert_eq!((decoded.id, decoded.body), (9, body));
}

#[test]
fn output_budget_is_never_exceeded_and_rejection_enqueues_nothing() {
    let (mut session, _raw) = plain_session();
    let body = vec![5_u8; 1000];
    let need = session.queue_size(1, &body, 4096).unwrap() + 4;
    let credit_error = |session: &mut Session, id| {
        session
            .queue(id, &body, 4096)
            .unwrap_err()
            .to_string()
            .contains("credit")
    };

    // One byte short: rejected before any allocation joins the queue.
    session.output_bytes = SESSION_BYTES - need + 1;
    assert!(!session.can_queue(need - 4));
    assert!(credit_error(&mut session, 1));
    assert!(session.output.is_empty());
    assert_eq!(session.output_bytes, SESSION_BYTES - need + 1);

    // A retained spare buffer counts at its capacity, not at the frame length.
    session.output_bytes = SESSION_BYTES - need;
    session.spare_output = Vec::with_capacity(need + 64);
    assert!(!session.can_queue(need - 4));
    assert!(credit_error(&mut session, 1));
    assert!(session.spare_output.capacity() >= need + 64);
    session.spare_output = Vec::new();

    // Exactly enough: admitted, landing on the budget and never past it.
    assert!(session.can_queue(need - 4));
    session.queue(2, &body, 4096).unwrap();
    assert!(session.output_bytes <= SESSION_BYTES);
    assert_eq!(session.output_bytes, SESSION_BYTES);
    assert_eq!(session.output.back().unwrap().retained_bytes, need);

    // At the budget every further frame is rejected and accounting is frozen.
    for id in 3..6 {
        assert!(credit_error(&mut session, id));
        assert_eq!(session.output_bytes, SESSION_BYTES);
        assert_eq!(session.output.len(), 1);
    }
}

#[test]
fn output_admission_rule_compares_allocation_capacity_with_the_remaining_budget() {
    let (mut session, _raw) = plain_session();
    for used in [0, 1, SESSION_BYTES / 2, SESSION_BYTES - 1, SESSION_BYTES] {
        session.output_bytes = used;
        let remaining = SESSION_BYTES - used;
        assert!(session.output_fits(remaining));
        assert!(!session.output_fits(remaining + 1));
        assert!(session.output_fits(0));
        assert!(!session.output_fits(usize::MAX));
    }
}
