//! Socket-timeout arming of the blocking deadline adapter.

use super::*;
use std::net::TcpListener;

/// A positive remainder must arm a nonzero socket timeout; a spent or
/// sub-microsecond remainder must report the typed deadline instead. A zero
/// `timeval` would mean "no timeout" and remove the bound entirely.
#[test]
fn prepare_never_arms_a_zero_socket_timeout() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let socket = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    for offset in [1u64, 500, 1_000, 500_000, 999_999, 2_000_000] {
        let deadline = Instant::now() + Duration::from_nanos(offset);
        let mut io = DeadlineIo::new_until(socket.try_clone().unwrap(), deadline).unwrap();
        match io.prepare(true) {
            Ok(()) => {
                let armed = io
                    .socket
                    .read_timeout()
                    .expect("the timeout is readable")
                    .expect("a timeout is armed");
                assert!(
                    !armed.is_zero(),
                    "a {offset} ns remainder armed a zero socket timeout"
                );
            }
            Err(error) => assert_eq!(
                error.kind(),
                io::ErrorKind::TimedOut,
                "a spent {offset} ns remainder keeps the timeout class: {error}"
            ),
        }
    }
}
