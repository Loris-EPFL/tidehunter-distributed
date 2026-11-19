//! Bounded accept loop and transport session setup shared by the owner host
//! and the protocol-test database server. The handler receives an
//! authenticated (or explicitly trusted) session before its first request.

use super::*;

/// Serves one accepted session to completion. Called on a bounded worker.
pub(super) type SessionHandler = Arc<
    dyn Fn(
            reactor::Session,
            Option<&dyn rustls::server::danger::ClientCertVerifier>,
            &AtomicBool,
            &WakeFd,
        ) -> Result<(), TcpError>
        + Send
        + Sync,
>;

/// Bounded reusable session workers. Shutdown stops acceptance, drains the
/// bounded socket queue and joins workers; frame I/O has absolute deadlines.
/// The socket queue holds at most `limits.max_connections` sockets; a
/// connection above the limit is closed at accept (reject policy).
pub(super) fn serve_listener(
    listener: TcpListener,
    tls: TlsFiles,
    limits: TcpLimits,
    shutdown: Arc<AtomicBool>,
    handler: SessionHandler,
) -> Result<(), TcpError> {
    tls.transport.validate()?;
    let server = tls.server_config()?;
    let transport = tls.transport;
    listener.set_nonblocking(true)?;
    struct ConnectionPermit(Arc<AtomicUsize>);
    impl Drop for ConnectionPermit {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::AcqRel);
        }
    }
    let connections = Arc::new(AtomicUsize::new(0));
    let shutdown_wake = Arc::new(WakeFd::for_progress("listener shutdown wake")?);
    let (accepted, sockets) =
        std::sync::mpsc::sync_channel::<(TcpStream, ConnectionPermit)>(limits.max_connections);
    let sockets = Arc::new(Mutex::new(sockets));
    let mut threads: Vec<thread::JoinHandle<()>> = Vec::new();
    let mut accept_error = None;
    // This service loop intentionally lives until shutdown. Workers grow
    // only to the admitted peak, never beyond max_connections. Do not join
    // individual sessions here: is_finished does not mean OS thread exit
    // has finished, including io_uring's kernel-task cleanup.
    while !shutdown.load(Ordering::Acquire) {
        if threads.iter().any(thread::JoinHandle::is_finished) {
            accept_error = Some(io::Error::other("session worker exited unexpectedly"));
            break;
        }
        match listener.accept() {
            Ok((socket, _)) => {
                let active = connections.load(Ordering::Acquire);
                if active >= limits.max_connections {
                    drop(socket);
                    continue;
                }
                if active >= threads.len() {
                    let handler = handler.clone();
                    let server = server.clone();
                    let shutdown = shutdown.clone();
                    let shutdown_wake = Arc::clone(&shutdown_wake);
                    let sockets = sockets.clone();
                    let transport = transport.clone();
                    match thread::Builder::new()
                        .name("wal-mtls-session".into())
                        .spawn(move || {
                            // The listener closes this bounded channel at
                            // shutdown. A worker retains one session owner
                            // at a time and is reused across disconnects.
                            loop {
                                let work = sockets.lock().recv();
                                let Ok((socket, _permit)) = work else {
                                    break;
                                };
                                if shutdown.load(Ordering::Acquire) {
                                    continue;
                                }
                                let result =
                                    accept_session(socket, server.clone(), limits, &transport)
                                        .and_then(|(session, verifier)| {
                                            handler(
                                                session,
                                                verifier.as_deref(),
                                                &shutdown,
                                                &shutdown_wake,
                                            )
                                        });
                                if let Err(error) = result {
                                    eprintln!("storage control connection closed: {error}");
                                }
                            }
                        }) {
                        Ok(handle) => threads.push(handle),
                        Err(error) => {
                            accept_error = Some(error);
                            break;
                        }
                    }
                }
                connections.fetch_add(1, Ordering::AcqRel);
                if let Err(error) =
                    accepted.try_send((socket, ConnectionPermit(connections.clone())))
                {
                    accept_error = Some(io::Error::other(format!(
                        "bounded session queue failed: {error}"
                    )));
                    break;
                }
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                // Connection setup is outside steady-state progress. Do
                // not spend another CPU polling an idle listener.
                thread::sleep(Duration::from_millis(5));
            }
            Err(error) => {
                accept_error = Some(error);
                break;
            }
        }
    }
    shutdown.store(true, Ordering::Release);
    let _ = shutdown_wake.signal();
    drop(accepted);
    drop(sockets);
    for handle in threads {
        if handle.join().is_err() && accept_error.is_none() {
            accept_error = Some(io::Error::other("session worker panicked"));
        }
    }
    match accept_error {
        Some(error) => Err(error.into()),
        None => Ok(()),
    }
}

type Verifier = Arc<dyn rustls::server::danger::ClientCertVerifier>;

/// Transport handshake of one accepted socket: mode preface for trusted
/// transports, otherwise the TLS handshake with client authentication, then
/// the fabric bootstrap where selected.
fn accept_session(
    socket: TcpStream,
    config: ServerTlsConfig,
    limits: TcpLimits,
    transport: &Transport,
) -> Result<(reactor::Session, Option<Verifier>), TcpError> {
    let mut io = DeadlineIo::new(socket, limits.io_timeout)?;
    if transport.is_native() {
        read_transport_mode(&mut io, transport)?;
        if !transport.is_fabric() {
            return Ok((reactor::Session::new_plain(io)?, None));
        }
        #[cfg(feature = "experimental-fabric")]
        {
            let endpoint = fabric::bootstrap_idle_parking(&mut io, transport)?;
            io.fabric = Some(endpoint);
            return Ok((reactor::Session::new_native(io)?, None));
        }
        #[cfg(not(feature = "experimental-fabric"))]
        return Err(TcpError::new("trusted OFI transport was not compiled"));
    }
    let server_config = config
        .config
        .ok_or_else(|| TcpError::new("TLS transport has no server credentials"))?;
    let verifier = config
        .verifier
        .ok_or_else(|| TcpError::new("TLS transport has no peer verifier"))?;
    let mut conn = ServerConnection::new(server_config).map_err(TcpError::component)?;
    conn.set_buffer_limit(Some(reactor::TLS_BUFFER_BYTES));
    while conn.is_handshaking() {
        conn.complete_io(&mut io)?;
    }
    if conn.alpn_protocol() != Some(transport.alpn().as_slice()) {
        return Err(TcpError::new("unsupported authenticated protocol"));
    }
    let stream = StreamOwned::new(conn, io);
    #[cfg(feature = "experimental-fabric")]
    let mut stream = stream;
    if transport.is_fabric() {
        #[cfg(feature = "experimental-fabric")]
        {
            let endpoint = fabric::bootstrap_idle_parking(&mut stream, transport)?;
            stream.sock.fabric = Some(endpoint);
        }
        #[cfg(not(feature = "experimental-fabric"))]
        return Err(TcpError::new("OFI transport was not compiled"));
    }
    Ok((
        reactor::Session::new(stream.conn.into(), stream.sock)?,
        Some(verifier),
    ))
}

/// An established client transport session before its first request.
pub(super) struct ClientSession {
    pub(super) session: reactor::Session,
    pub(super) connect_ns: u64,
    pub(super) tls_ns: u64,
    pub(super) fabric_ns: Option<u64>,
}

/// Connect and run the transport handshake of `tls.transport` against one
/// numeric endpoint. Shared by database and host-scope clients.
pub(super) fn open_client_session(
    endpoint: &str,
    tls: &TlsFiles,
    client_config: ClientTlsConfig,
    timeout: Duration,
    progress: bool,
) -> Result<ClientSession, TcpError> {
    #[cfg(not(feature = "experimental-fabric"))]
    let _ = progress;
    tls.transport.validate()?;
    if timeout.is_zero() || timeout > Duration::from_secs(300) {
        return Err(TcpError::new("invalid client timeout"));
    }
    let address: SocketAddr = endpoint.parse().map_err(|_| {
        TcpError::new("endpoint must be numeric IP:port; set TLS server_name separately")
    })?;
    let began = Instant::now();
    let socket = TcpStream::connect_timeout(&address, timeout)?;
    let connect_ns = elapsed_ns(began);
    let mut io = DeadlineIo::new(socket, timeout)?;
    let setup_started = Instant::now();
    if tls.transport.is_native() {
        write_transport_mode(&mut io, &tls.transport)?;
    }
    let (session, tls_ns, fabric_ns) = if tls.transport.is_native() {
        if !tls.transport.is_fabric() {
            (reactor::Session::new_plain(io)?, 0, None)
        } else {
            #[cfg(feature = "experimental-fabric")]
            {
                let endpoint = if progress {
                    fabric::bootstrap_progress(&mut io, &tls.transport)?
                } else {
                    fabric::bootstrap(&mut io, &tls.transport)?
                };
                io.fabric = Some(endpoint);
                let fabric_ns = Some(elapsed_ns(setup_started));
                (reactor::Session::new_native(io)?, 0, fabric_ns)
            }
            #[cfg(not(feature = "experimental-fabric"))]
            {
                return Err(TcpError::new("trusted OFI transport was not compiled"));
            }
        }
    } else {
        let name = ServerName::try_from(tls.server_name.clone()).map_err(TcpError::component)?;
        let client_tls_config = client_config
            .config
            .ok_or_else(|| TcpError::new("TLS transport has no client credentials"))?;
        let mut conn =
            ClientConnection::new(client_tls_config, name).map_err(TcpError::component)?;
        conn.set_buffer_limit(Some(reactor::TLS_BUFFER_BYTES));
        while conn.is_handshaking() {
            conn.complete_io(&mut io)?;
        }
        let tls_ns = elapsed_ns(setup_started);
        if conn.alpn_protocol() != Some(tls.transport.alpn().as_slice()) {
            return Err(TcpError::new(
                "required authenticated transport was not negotiated",
            ));
        }
        let stream = StreamOwned::new(conn, io);
        #[cfg(feature = "experimental-fabric")]
        let mut stream = stream;
        #[cfg(feature = "experimental-fabric")]
        let fabric_ns = if tls.transport.is_fabric() {
            let began = Instant::now();
            stream.sock.fabric = Some(if progress {
                fabric::bootstrap_progress(&mut stream, &tls.transport)?
            } else {
                fabric::bootstrap(&mut stream, &tls.transport)?
            });
            Some(elapsed_ns(began))
        } else {
            None
        };
        #[cfg(not(feature = "experimental-fabric"))]
        let fabric_ns = None;
        (
            reactor::Session::new(stream.conn.into(), stream.sock)?,
            tls_ns,
            fabric_ns,
        )
    };
    Ok(ClientSession {
        session,
        connect_ns,
        tls_ns,
        fabric_ns,
    })
}
