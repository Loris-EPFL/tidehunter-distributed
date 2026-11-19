//! Host session routing (server) and the host-scope client.
//!
//! The first request of every owner-host session decides its binding:
//! `Status { database: Some(id) }` hands the session, with that first request,
//! to the database's ordinary loop; `Status { database: None }` keeps it at host scope.
//! only Status, ProvisionDatabase and DropDatabase are served, one at a
//! time. The binding never changes afterwards.

use super::super::host::actor::{ActorHandle, DropRequest, ProvisionRequest};
use super::super::host::{DatabaseProvisionSpec, HostCore, HostStatus};
use super::*;

/// Poll interval of the cold host loops while nothing progressed.
const HOST_POLL: Duration = Duration::from_millis(1);

fn kind_error(kind: io::ErrorKind, message: String) -> TcpError {
    TcpError::with_kind(kind, message)
}

/// Receive one complete request frame. `idle_deadline` bounds the wait for
/// the first request only; established sessions may idle. `None` means the
/// peer closed at a frame boundary or the host is shutting down.
fn receive_request(
    stream: &mut Session,
    limits: TcpLimits,
    shutdown: &AtomicBool,
    idle_deadline: Option<Instant>,
) -> Result<Option<(Envelope<Request>, usize)>, TcpError> {
    loop {
        if shutdown.load(Ordering::Acquire) {
            return Ok(None);
        }
        let now = Instant::now();
        if idle_deadline.is_some_and(|deadline| now >= deadline) {
            return Err(TcpError::new(format!(
                "owner I/O deadline expired: no Status request within {:?} of connect; \
                 the session is closed",
                limits.io_timeout
            )));
        }
        if stream.partial_expired(now, limits.io_timeout) {
            return Err(TcpError::new(
                "owner I/O deadline expired: a request frame stayed incomplete",
            ));
        }
        let (progressed, _) = stream.progress(None)?;
        match stream.receive(limits.max_frame_bytes) {
            Ok(Some((frame, frame_len))) => {
                let envelope = stream.decode(&frame[..frame_len], limits.max_frame_bytes)?;
                stream.recycle_input(frame);
                return Ok(Some((envelope, frame_len)));
            }
            Ok(None) => {}
            Err(_) if stream.peer_finished() => return Ok(None),
            Err(error) => return Err(error),
        }
        if !progressed {
            thread::sleep(HOST_POLL);
        }
    }
}

/// Queue one response and drive output until the transport accepted it.
fn send_response(
    stream: &mut Session,
    id: u64,
    response: Response,
    limits: TcpLimits,
) -> Result<(), TcpError> {
    stream.queue_response(id, response, limits.max_frame_bytes)?;
    let deadline = Instant::now() + limits.io_timeout;
    while stream.output_pending(id) {
        if Instant::now() >= deadline {
            return Err(TcpError::new(
                "owner I/O deadline expired while sending a host response",
            ));
        }
        let (progressed, _) = stream.progress(None)?;
        if !progressed {
            thread::sleep(HOST_POLL);
        }
    }
    Ok(())
}

pub(in super::super) fn serve_host_session(
    core: &Arc<HostCore>,
    actor: &ActorHandle,
    mut stream: Session,
    limits: TcpLimits,
    shutdown: &AtomicBool,
    shutdown_wake: &WakeFd,
    client_verifier: Option<&dyn rustls::server::danger::ClientCertVerifier>,
) -> Result<(), TcpError> {
    let Some((first, frame_len)) = receive_request(
        &mut stream,
        limits,
        shutdown,
        Some(Instant::now() + limits.io_timeout),
    )?
    else {
        return Ok(());
    };
    if first.protocol != PROTOCOL || first.id == 0 {
        return Err(TcpError::new(
            "invalid, duplicate or nonmonotonic request ID",
        ));
    }
    let bound = match &first.body {
        Request::Status {
            protocol: PROTOCOL,
            database: Some(database),
        } => Some(*database),
        Request::Status {
            protocol: PROTOCOL,
            database: None,
        } => None,
        _ => return Err(TcpError::new("status/protocol handshake required")),
    };
    if let Some(database) = bound {
        let Some(node) = core.route(&database) else {
            // Typed absence for the coordinator, then close the session.
            let error = kind_error(
                io::ErrorKind::NotFound,
                format!(
                    "database {} is not provisioned",
                    super::super::host_identity::database_hex(&database)
                ),
            );
            send_response(&mut stream, first.id, Response::Error(error.into()), limits)?;
            stream.abort();
            return Ok(());
        };
        let deadline = Instant::now() + first.body.timeout(limits.io_timeout);
        return serve_session(
            &node,
            stream,
            limits,
            shutdown,
            shutdown_wake,
            client_verifier,
            Some((first, frame_len, deadline)),
        );
    }
    // The first request already passed the handshake checks above.
    let mut last_id = 0;
    let mut request = Some(first);
    loop {
        let envelope = match request.take() {
            Some(envelope) => envelope,
            None => match receive_request(&mut stream, limits, shutdown, None)? {
                Some((envelope, _)) => envelope,
                None => return Ok(()),
            },
        };
        if envelope.protocol != PROTOCOL || envelope.id <= last_id {
            return Err(TcpError::new(
                "invalid, duplicate or nonmonotonic request ID",
            ));
        }
        if last_id != 0 {
            // Reusing an established TLS channel must not let an expired
            // credential act: revalidate before every later host request.
            revalidate_client_peer(
                stream.conn.as_ref(),
                client_verifier,
                rustls::pki_types::UnixTime::now(),
            )?;
        }
        last_id = envelope.id;
        let deadline = Instant::now() + envelope.body.timeout(limits.io_timeout);
        let response = match envelope.body {
            Request::Status {
                protocol: PROTOCOL,
                database: None,
            } => core
                .host_status()
                .map(|status| Response::Status(StatusReply::Host(status))),
            Request::ProvisionDatabase {
                database,
                partition,
                shape_yaml,
                live_target,
            } => core.serving().and_then(|(limits, principal)| {
                let receiver = actor.submit_provision(
                    ProvisionRequest {
                        database,
                        spec: DatabaseProvisionSpec {
                            partition,
                            shape_yaml,
                            live_target,
                        },
                    },
                    limits,
                    principal,
                )?;
                wait_reply(&receiver, deadline, shutdown).map(|ack| Response::ProvisionDatabase {
                    database: ack.database,
                    partition: ack.partition,
                    durability_tier: ack.durability_tier,
                })
            }),
            Request::DropDatabase {
                database,
                authority,
                generation,
            } => core.serving().and_then(|(_, principal)| {
                let receiver = actor.submit_drop(
                    DropRequest {
                        database,
                        authority,
                        generation,
                    },
                    principal,
                )?;
                wait_reply(&receiver, deadline, shutdown).map(|ack| Response::DropDatabase {
                    database: ack.database,
                    generation: ack.generation,
                })
            }),
            _ => {
                let error = kind_error(
                    io::ErrorKind::InvalidInput,
                    "host session accepts only provision, drop and status".into(),
                );
                send_response(
                    &mut stream,
                    envelope.id,
                    Response::Error(error.into()),
                    limits,
                )?;
                stream.abort();
                return Ok(());
            }
        };
        let response = response.unwrap_or_else(|error| Response::Error(error.into()));
        send_response(&mut stream, envelope.id, response, limits)?;
    }
}

/// Wait for the actor's reply within the request deadline. A timeout is an
/// unknown outcome: the job may still complete; the coordinator retries the
/// exact same request.
fn wait_reply<T>(
    receiver: &mpsc::Receiver<Result<T, TcpError>>,
    deadline: Instant,
    shutdown: &AtomicBool,
) -> Result<T, TcpError> {
    loop {
        let now = Instant::now();
        if now >= deadline || shutdown.load(Ordering::Acquire) {
            return Err(kind_error(
                io::ErrorKind::TimedOut,
                "owner lifecycle request did not finish in time; outcome unknown".into(),
            ));
        }
        match receiver.recv_timeout((deadline - now).min(Duration::from_millis(50))) {
            Ok(result) => return result,
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                return Err(TcpError::new(
                    "owner lifecycle actor stopped; outcome unknown",
                ));
            }
        }
    }
}

/// Host-scope client: authenticated Status, provision and drop. Cold path:
/// one request at a time, absolute deadlines, no retries in place.
pub(crate) struct HostClient {
    stream: Session,
    endpoint: String,
    next_id: u64,
    timeout: Duration,
    frame_limit: usize,
    status: Option<HostStatus>,
}

impl HostClient {
    /// Connect and bind the session to the host with `Status { None }`.
    pub(crate) fn connect(
        endpoint: &str,
        tls: &TlsFiles,
        timeout: Duration,
    ) -> Result<Self, TcpError> {
        let client_config = tls.client_config_with_identity()?;
        let session = super::super::listener::open_client_session(
            endpoint,
            tls,
            client_config,
            timeout,
            false,
        )?
        .session;
        let mut client = Self {
            stream: session,
            endpoint: endpoint.to_owned(),
            next_id: 1,
            timeout,
            frame_limit: HARD_FRAME_LIMIT,
            status: None,
        };
        match client.exchange(&Request::Status {
            protocol: PROTOCOL,
            database: None,
        })? {
            Response::Status(StatusReply::Host(status)) => {
                client.frame_limit = usize::try_from(status.max_frame_bytes)
                    .unwrap_or(HARD_FRAME_LIMIT)
                    .min(HARD_FRAME_LIMIT);
                client.status = Some(status);
                Ok(client)
            }
            response => Err(kind_error(
                io::ErrorKind::InvalidData,
                format!(
                    "owner {endpoint} answered host Status with {}",
                    response.as_ref()
                ),
            )),
        }
    }

    /// Authenticated host Status captured at connect.
    pub(crate) fn status(&self) -> Option<&HostStatus> {
        self.status.as_ref()
    }

    /// Provision (or confirm) one database. The echoed identity, partition
    /// and tier are checked before the success is used.
    pub(crate) fn provision(
        &mut self,
        database: [u8; 16],
        spec: &DatabaseProvisionSpec,
    ) -> Result<crate::distributed::durability::DurabilityTier, TcpError> {
        let response = self.exchange(&Request::ProvisionDatabase {
            database,
            partition: spec.partition.clone(),
            shape_yaml: spec.shape_yaml.clone(),
            live_target: spec.live_target.clone(),
        })?;
        match response {
            Response::ProvisionDatabase {
                database: echoed,
                partition,
                durability_tier,
            } if echoed == database
                && partition == spec.partition.partition
                && durability_tier == spec.live_target.durability_tier =>
            {
                Ok(durability_tier)
            }
            Response::ProvisionDatabase { .. } => Err(kind_error(
                io::ErrorKind::InvalidData,
                format!(
                    "owner {} echoed a different provision identity or tier",
                    self.endpoint
                ),
            )),
            response => Err(kind_error(
                io::ErrorKind::InvalidData,
                format!(
                    "owner {} answered ProvisionDatabase with {}",
                    self.endpoint,
                    response.as_ref()
                ),
            )),
        }
    }

    /// Drop one database on this owner; checks the echoed identity.
    pub(crate) fn drop_database(
        &mut self,
        database: [u8; 16],
        authority: [u8; 32],
        generation: u64,
    ) -> Result<(), TcpError> {
        match self.exchange(&Request::DropDatabase {
            database,
            authority,
            generation,
        })? {
            Response::DropDatabase {
                database: echoed,
                generation: echoed_generation,
            } if echoed == database && echoed_generation == generation => Ok(()),
            response => Err(kind_error(
                io::ErrorKind::InvalidData,
                format!(
                    "owner {} answered DropDatabase with {} or a different identity",
                    self.endpoint,
                    response.as_ref()
                ),
            )),
        }
    }

    fn exchange(&mut self, request: &Request) -> Result<Response, TcpError> {
        let id = self.next_id;
        self.next_id = id
            .checked_add(1)
            .ok_or_else(|| TcpError::new("host request IDs exhausted"))?;
        let deadline = Instant::now() + request.timeout(self.timeout);
        self.stream.queue(id, request, self.frame_limit)?;
        loop {
            if Instant::now() >= deadline {
                return Err(kind_error(
                    io::ErrorKind::TimedOut,
                    format!(
                        "owner {} {} timed out; outcome unknown",
                        self.endpoint,
                        request_kind_code(request)
                    ),
                ));
            }
            let (progressed, _) = self.stream.progress(None)?;
            let Some((frame, frame_len)) = self.stream.receive(self.frame_limit)? else {
                if !progressed {
                    thread::sleep(HOST_POLL);
                }
                continue;
            };
            let envelope: Envelope<Response> =
                self.stream.decode(&frame[..frame_len], self.frame_limit)?;
            self.stream.recycle_input(frame);
            if envelope.protocol != PROTOCOL {
                return Err(TcpError::new("owner host protocol mismatch"));
            }
            if envelope.id == 0 {
                return Err(match envelope.body {
                    Response::Error(reason) => {
                        TcpError::new(format!("owner closed the session: {reason}"))
                    }
                    _ => TcpError::new("invalid unsolicited response"),
                });
            }
            if envelope.id != id {
                return Err(TcpError::new("unexpected host response ID"));
            }
            return match envelope.body {
                Response::Error(error) => Err(error.into()),
                response => Ok(response),
            };
        }
    }
}
