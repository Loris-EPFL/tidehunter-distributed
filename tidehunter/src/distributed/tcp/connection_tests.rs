//! Connection replacement: a read-lane control RPC after a forced transport
//! failure runs on the session the lane installs, a spent deadline is a typed
//! timeout that installs nothing, the whole setup is bounded by one absolute
//! deadline, and the promoted cap bounds the lifecycle Status handshake.

use super::*;
use crate::distributed::durability::DurabilityTier;
use crate::distributed::storage::WalMemberReadPolicy;
use crate::distributed::tcp::connect_shared::{self, SessionSetup};
use crate::distributed::tcp::leased_tests::Fixture;
use crate::distributed::tcp::tests::multiplexed::Running;
use crate::key_shape::KeyShape;
use std::path::PathBuf;

/// The owner's lifetime export control runs once before and once after the
/// forced failure; the second run serves on the replaced session.
#[test]
fn a_read_control_after_a_forced_failure_runs_on_the_new_session() {
    let fixture = Fixture::single_partition(DurabilityTier::Native, 16384);
    let mut client = fixture.writer();
    client
        .open_live_exports(1)
        .expect("the owner serves the export control before the failure");
    client.fail_connection_for_test();
    assert_eq!(
        client.session_present(),
        None,
        "a forced transport failure leaves no usable session"
    );
    client
        .replace_read_session(Instant::now() + Duration::from_secs(10))
        .expect("one connect and one Status handshake fit the read deadline");
    assert_eq!(
        client.session_present(),
        Some(true),
        "the replacement installed a usable session"
    );
    client
        .open_live_exports(1)
        .expect("the replaced session serves the export control");
}

/// A replacement that starts at or past the read deadline is a typed timeout;
/// the aborted session stays unusable and nothing is installed.
#[test]
fn a_replacement_at_the_read_deadline_is_a_typed_timeout() {
    let server = Running::new(Transport::Tcp, WalMemberReadPolicy::Async);
    let mut client = server.client();
    client.fail_connection_for_test();
    let error = client
        .replace_read_session(Instant::now())
        .expect_err("a spent read deadline cannot replace the session");
    assert_eq!(
        error.kind(),
        io::ErrorKind::TimedOut,
        "the replacement keeps the timeout class: {error}"
    );
    assert_eq!(
        client.session_present(),
        None,
        "a spent deadline installs no session"
    );
}

/// The promoted per-call cap bounds the lifecycle Status handshake, the
/// request a session replacement runs: a zero cap is a typed timeout checked
/// against the exchange deadline, not a 34.5 s lifecycle wait.
#[test]
fn the_control_cap_bounds_the_status_handshake() {
    let server = Running::new(Transport::Tcp, WalMemberReadPolicy::Async);
    let mut client = server.client();
    client.reactor.control_cap = Some(Duration::ZERO);
    let error = client
        .status()
        .expect_err("a zero cap cannot complete the Status handshake");
    assert_eq!(
        error.kind(),
        io::ErrorKind::TimedOut,
        "the capped handshake keeps the timeout class: {error}"
    );
}

/// The blocking adapter takes the caller's absolute deadline unchanged, so no
/// later stage can restart the budget from "now". This is the structural
/// assertion of the whole-setup bound; the stalled-handshake test below asserts
/// the typed outcome, not a wall-clock threshold.
#[test]
fn the_blocking_stage_carries_the_absolute_deadline() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let socket = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    let io = super::DeadlineIo::new_until(socket, deadline).unwrap();
    assert_eq!(
        io.deadline, deadline,
        "the adapter keeps the caller's absolute deadline"
    );
}

/// A peer that completes TCP but never answers the TLS handshake cannot hold
/// the lane past the replacement's deadline: the stage returns `TimedOut` and
/// installs no session. The deadline, not a wall-clock threshold, decides the
/// outcome; the test needs no sleep.
#[test]
fn a_stalled_tls_handshake_ends_at_the_replacement_deadline() {
    let server = Running::new(Transport::Tcp, WalMemberReadPolicy::Async);
    let mut client = server.client();
    let silent = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    client.rebind_endpoint_for_test(&silent.local_addr().unwrap().to_string());
    client.fail_connection_for_test();
    let error = client
        .replace_read_session(Instant::now() + Duration::from_millis(50))
        .expect_err("a silent peer cannot complete the TLS handshake");
    assert_eq!(
        error.kind(),
        io::ErrorKind::TimedOut,
        "the stalled handshake keeps the timeout class: {error}"
    );
    assert_eq!(
        client.session_present(),
        None,
        "a handshake that missed the deadline installs no session"
    );
}

/// A setup that starts at its deadline installs nothing and spends no network
/// stage: the constructor returns `TimedOut` before the first connect.
#[test]
fn a_setup_at_its_deadline_returns_timed_out_before_connecting() {
    let tls = TlsFiles {
        transport: Transport::TcpTrusted,
        ca: PathBuf::new(),
        cert: PathBuf::new(),
        key: PathBuf::new(),
        server_name: "trusted-test-cluster".to_owned(),
    };
    let client_config = tls.client_config_with_identity().unwrap();
    let server = Running::new(Transport::Tcp, WalMemberReadPolicy::Async);
    let shape = KeyShape::from_yaml(&server.config.shape_yaml).unwrap();
    let error = match connect_shared::connect(
        "127.0.0.1:1",
        &tls,
        client_config,
        server.config.partition.clone(),
        &shape,
        SessionSetup {
            deadline: Instant::now(),
            timeout: Duration::from_secs(2),
            progress: None,
        },
    ) {
        Ok(_) => panic!("a setup at its deadline cannot connect"),
        Err(error) => error,
    };
    assert_eq!(
        error.kind(),
        io::ErrorKind::TimedOut,
        "the spent setup keeps the timeout class: {error}"
    );
}

/// A parked control exchange makes the replacement refuse with the typed
/// class and installs nothing; once the exchange is gone the same call
/// replaces the session. No read-lane call site can park an exchange on a
/// read client, so the state is built with the test hook.
#[test]
fn a_parked_exchange_refuses_the_replacement_until_it_clears() {
    let server = Running::new(Transport::Tcp, WalMemberReadPolicy::Async);
    let mut client = server.client();
    client.park_control_for_test();
    let deadline = Instant::now() + Duration::from_secs(10);
    let error = client
        .replace_read_session(deadline)
        .expect_err("a pending exchange refuses the replacement");
    assert_eq!(
        error.kind(),
        io::ErrorKind::NotConnected,
        "the refusal keeps the typed class: {error}"
    );
    assert_eq!(
        client.session_present(),
        None,
        "no session is installed while the exchange outcome is unknown"
    );
    client.clear_parked_control_for_test();
    client
        .replace_read_session(deadline)
        .expect("the next call replaces the session once the exchange is gone");
    assert_eq!(
        client.session_present(),
        Some(true),
        "the replacement installed a usable session"
    );
}
