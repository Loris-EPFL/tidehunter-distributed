//! Client construction: the transport session and its authenticated Status
//! handshake.
//!
//! Split out of `tcp.rs` to hold that file under its length ratchet. One
//! absolute deadline bounds the TCP connect, the transport handshake and the
//! Status reply. Every stage asks for the remaining budget, so a replacement
//! cannot outlive the read deadline that asked for it, and a stage that finds
//! no budget left returns `TimedOut` without touching the network.

use super::listener;
use super::progress_wait;
use super::reactor;
use super::{
    Client, ClientConnectProfile, ClientTlsConfig, HARD_FRAME_LIMIT, KeyShape, PartitionConfig,
    PollingPolicy, TcpError, TlsFiles, WakeFd, elapsed_ns, live_client,
};
use std::io;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// The immutable authenticated route of one client session, kept so the read
/// lane can rebuild an aborted session on the same owner. It carries no pin
/// state: the lifetime-export model removed the export pins.
#[derive(Clone)]
pub(super) struct ReconnectRoute {
    pub(super) endpoint: String,
    pub(super) tls: TlsFiles,
    pub(super) config: ClientTlsConfig,
    pub(super) timeout: Duration,
    pub(super) expected: PartitionConfig,
    pub(super) shape: KeyShape,
}

/// The time bounds and progress handle of one session setup.
pub(super) struct SessionSetup {
    /// Absolute bound for the connect, the transport handshake and Status.
    pub(super) deadline: Instant,
    /// The session's request policy, stored in the client and its route.
    pub(super) timeout: Duration,
    /// Durable-progress stop handle; `None` for every ordinary client.
    pub(super) progress: Option<Arc<WakeFd>>,
}

/// Connect an authenticated client and run its Status handshake.
///
/// `setup.deadline` bounds every stage, including the Status exchange whose
/// own policy is the lifecycle deadline: the constructor caps it with the
/// remaining budget at that point and clears the cap before returning.
pub(super) fn connect(
    endpoint: &str,
    tls: &TlsFiles,
    client_config: ClientTlsConfig,
    expected: PartitionConfig,
    shape: &KeyShape,
    setup: SessionSetup,
) -> Result<Client, TcpError> {
    let SessionSetup {
        deadline,
        timeout,
        progress,
    } = setup;
    if timeout.is_zero() || timeout > Duration::from_secs(300) {
        return Err(TcpError::new("invalid client timeout"));
    }
    if deadline.saturating_duration_since(Instant::now()).is_zero() {
        return Err(TcpError::with_kind(
            io::ErrorKind::TimedOut,
            "client session setup deadline expired",
        ));
    }
    let route = ReconnectRoute {
        endpoint: endpoint.to_owned(),
        tls: tls.clone(),
        config: client_config.clone(),
        timeout,
        expected: expected.clone(),
        shape: shape.clone(),
    };
    expected.validate().map_err(TcpError::component)?;
    let listener::ClientSession {
        session,
        connect_ns,
        tls_ns,
        fabric_ns,
    } = listener::open_client_session(endpoint, tls, client_config, deadline, progress.is_some())?;
    let shape = shape.clone();
    let mut client = Client {
        stream: session,
        expected,
        shape,
        timeout,
        broken: false,
        frame_limit: HARD_FRAME_LIMIT,
        max_reads: 256,
        page_limit: 1024,
        diagnostics: None,
        reactor: reactor::ClientState::default(),
        connected_status: None,
        connect_profile: ClientConnectProfile {
            connect_ns,
            tls_ns,
            fabric_ns,
            status_ns: 0,
            #[cfg(feature = "experimental-fabric")]
            fabric_open: None,
        },
        polling: PollingPolicy::Adaptive,
        route,
        live_config: None,
        live_exports: None,
        live_pool: None,
        live_writer: None,
        live_writer_closing: false,
        live_leased: live_client::LeasedWriter::default(),
        live_write_failed: false,
        #[cfg(any(test, feature = "test-utils"))]
        live_write_local_fixture: false,
        live_counters: None,
        live_metrics: None,
        progress_wait: progress.map(progress_wait::ClientProgressWait::new),
        lease_validity: None,
    };
    let budget = deadline.saturating_duration_since(Instant::now());
    if budget.is_zero() {
        return Err(TcpError::with_kind(
            io::ErrorKind::TimedOut,
            "client Status deadline expired",
        ));
    }
    let began = Instant::now();
    client.reactor.control_cap = Some(budget);
    let status = client.status();
    client.reactor.control_cap = None;
    client.connected_status = Some(status?);
    client.connect_profile.status_ns = elapsed_ns(began);
    #[cfg(feature = "experimental-fabric")]
    {
        // Provider setup may defer bounded resources until its first CQ
        // progress, inside status rather than endpoint creation.
        client.connect_profile.fabric_open = client
            .stream
            .sock
            .fabric
            .as_ref()
            .map(tidehunter_fabric::Endpoint::open_profile);
    }
    Ok(client)
}
