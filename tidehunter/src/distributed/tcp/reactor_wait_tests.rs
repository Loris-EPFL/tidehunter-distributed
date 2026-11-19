use super::*;

fn plain_session() -> (Session, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let socket = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let (peer, _) = listener.accept().unwrap();
    let io = DeadlineIo::new(socket, Duration::from_secs(120)).unwrap();
    (Session::new_plain(io).unwrap(), peer)
}

/// A reply can arrive before the local retirement of the request frame. Until
/// that frame retires, the lane is not quiet: it keeps progressing and never
/// parks with output that a read-only wait would leave behind.
#[test]
fn unretired_output_keeps_the_lane_out_of_a_wait() {
    let (mut session, _peer) = plain_session();
    assert!(session.wait_quiet());
    session.queue(1, &Request::Stats, 1 << 20).unwrap();
    assert!(!session.wait_quiet());
    session.output_budget.reset();
    assert!(session.progress_output().unwrap());
    // Plain TCP retires a frame as soon as the socket accepted it.
    assert!(session.wait_quiet());
}

/// A partial input frame needs more socket bytes, so the lane may wait for them;
/// decrypted plaintext or an observed peer close is local work.
#[test]
fn partial_frame_waits_but_plaintext_or_peer_close_does_not() {
    let (mut session, _peer) = plain_session();
    session.input_started = Some(Instant::now());
    session.header_used = 2;
    assert!(session.wait_quiet());
    session.plaintext_pending = 1;
    assert!(!session.wait_quiet());
    session.plaintext_pending = 0;
    session.peer_closed = true;
    assert!(!session.wait_quiet());
}
