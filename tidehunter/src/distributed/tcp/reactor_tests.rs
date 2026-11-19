use super::*;
use crate::distributed::storage::WalMemberReadPolicy;
use crate::distributed::tcp::reactor::Envelope;
use std::sync::mpsc;

pub(in crate::distributed::tcp) struct Running {
    pub(in crate::distributed::tcp) config: NodeConfig,
    tls: TlsFiles,
    pub(in crate::distributed::tcp) node: Arc<StorageNode>,
    endpoint: String,
    shutdown: Arc<AtomicBool>,
    worker: Option<thread::JoinHandle<()>>,
    // Fields drop in declaration order. The directory must outlive `node`:
    // the WAL mapper thread of the node can still create the next WAL file.
    _temp: tempdir::TempDir,
}
impl Running {
    pub(in crate::distributed::tcp) fn new(
        transport: Transport,
        policy: WalMemberReadPolicy,
    ) -> Self {
        Self::with_limits(
            transport,
            policy,
            default_device_reads(),
            TcpLimits {
                io_timeout: Duration::from_secs(3),
                max_frame_bytes: 2 * 1024 * 1024,
                ..TcpLimits::default()
            },
        )
    }
    pub(in crate::distributed::tcp) fn with_limits(
        transport: Transport,
        policy: WalMemberReadPolicy,
        device_reads: usize,
        limits: TcpLimits,
    ) -> Self {
        Self::with_config(transport, policy, device_reads, limits, |_| {})
    }
    fn with_config(
        transport: Transport,
        policy: WalMemberReadPolicy,
        device_reads: usize,
        limits: TcpLimits,
        configure: impl FnOnce(&mut NodeConfig),
    ) -> Self {
        let temp = tempdir::TempDir::new("multiplexed-session").unwrap();
        let tls = if transport.is_native() {
            TlsFiles {
                transport,
                ca: PathBuf::new(),
                cert: PathBuf::new(),
                key: PathBuf::new(),
                server_name: "trusted-test-cluster".to_owned(),
            }
        } else {
            let mut tls = certificates(temp.path());
            tls.transport = transport;
            tls
        };
        let mut config = config(&temp.path().join("node"));
        config.wal_member_read = policy;
        config.read_max_device_reads = device_reads;
        configure(&mut config);
        let node = StorageNode::open(config.clone(), metrics()).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        let shutdown = Arc::new(AtomicBool::new(false));
        let server = node.clone();
        let stop = shutdown.clone();
        let server_tls = tls.clone();
        let worker =
            thread::spawn(move || server.serve(listener, server_tls, limits, stop).unwrap());
        Self {
            _temp: temp,
            config,
            tls,
            node,
            endpoint,
            shutdown,
            worker: Some(worker),
        }
    }
    pub(in crate::distributed::tcp) fn client(&self) -> Client {
        Client::connect(
            &self.endpoint,
            &self.tls,
            Duration::from_secs(2),
            self.config.partition.clone(),
            &shape(),
        )
        .unwrap()
    }
}
impl Drop for Running {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            worker.join().unwrap();
        }
    }
}
#[test]
fn correlated_lifecycle_control_expires_at_its_separate_deadline_under_stall() {
    let server = Running::new(Transport::TcpTrusted, WalMemberReadPolicy::Positional);
    let mut client = server.client();
    client.timeout = Duration::from_secs(5);
    let request = Request::Fence { epoch: 1 };
    assert_eq!(request.timeout(client.timeout), LIFECYCLE_TIMEOUT);
    assert_eq!(
        request.timeout(Duration::from_secs(60)),
        Duration::from_secs(60),
        "an explicitly larger lifecycle budget must not shrink",
    );
    let inventory = server.node.inventory.lock();
    let started = Instant::now();
    let deadline = started + request.timeout(client.timeout);
    assert!(client.submit_control(&request, deadline).unwrap());
    client
        .progress_correlated_at(started + Duration::from_secs(5))
        .unwrap();
    client
        .progress_correlated_at(deadline - Duration::from_nanos(1))
        .unwrap();
    let result = client.progress_correlated_at(deadline + Duration::from_nanos(1));
    drop(inventory);
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("remote operation deadline expired")
    );
}

#[test]
fn correlated_position_proof_keeps_data_deadline_under_stall() {
    let server = Running::new(Transport::TcpTrusted, WalMemberReadPolicy::Positional);
    let mut client = server.client();
    client.timeout = Duration::from_secs(5);
    client.fence_to(1).unwrap();
    let inventory = server.node.inventory.lock();
    let deadline = client.position_deadline();
    assert!(deadline <= Instant::now() + Duration::from_secs(5));
    assert!(
        client
            .submit_checkpoint_position(1, 1, 0, deadline)
            .unwrap()
    );
    client
        .progress_correlated_at(deadline - Duration::from_nanos(1))
        .unwrap();
    let result = client.progress_correlated_at(deadline + Duration::from_nanos(1));
    drop(inventory);
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("remote operation deadline expired")
    );
}

#[test]
fn owner_session_deadlines_extend_fence() {
    let io_timeout = Duration::from_millis(300);
    let server = Running::with_limits(
        Transport::TcpTrusted,
        WalMemberReadPolicy::Positional,
        default_device_reads(),
        TcpLimits {
            io_timeout,
            ..TcpLimits::default()
        },
    );
    let mut client = server.client();
    client.timeout = Duration::from_secs(5);
    let admission_deadline = Instant::now() + Duration::from_secs(2);
    while server.node.control.pending_requests_for_test() != 0 {
        assert!(Instant::now() < admission_deadline);
        thread::yield_now();
    }

    let inventory = server.node.inventory.lock();
    let request = Request::Fence { epoch: 1 };
    assert!(
        client
            .submit_control(&request, Instant::now() + request.timeout(client.timeout))
            .unwrap()
    );
    // Start the hold only after this Fence has entered the owner's queue.
    while server.node.control.pending_requests_for_test() == 0 {
        client.progress_session().unwrap();
        assert!(
            Instant::now() < admission_deadline,
            "Fence was not admitted"
        );
        thread::yield_now();
    }
    thread::sleep(io_timeout + Duration::from_millis(100));
    drop(inventory);
    let completion_deadline = Instant::now() + Duration::from_secs(2);
    let response = loop {
        client.progress_session().unwrap();
        if let Some(response) = client.take_control() {
            break response.unwrap();
        }
        assert!(Instant::now() < completion_deadline, "Fence did not finish");
        thread::yield_now();
    };
    assert!(
        matches!(response, Response::Status(StatusReply::Database(status)) if status.active_epoch == Some(1))
    );
}

#[test]
fn status_shape_mismatch_names_request_and_both_sha256_values() {
    let server = Running::new(Transport::TcpTrusted, WalMemberReadPolicy::Positional);
    let different = KeyShape::new_single(4, 1024, KeyType::uniform(1));
    let error = match Client::connect(
        &server.endpoint,
        &server.tls,
        Duration::from_secs(2),
        server.config.partition.clone(),
        &different,
    ) {
        Ok(_) => panic!("owner with a different key shape was accepted"),
        Err(error) => error,
    };
    let owner = Client::shape_sha256(&shape().to_yaml().unwrap());
    let client = Client::shape_sha256(&different.to_yaml().unwrap());
    assert_ne!(owner, client);
    assert_eq!(
        error.to_string(),
        format!("Status reply failed check: shape_yaml SHA-256 owner={owner} client={client}")
    );
    assert_eq!(
        Client::shape_sha256(""),
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    );
}

#[test]
fn invalid_status_bounds_and_wrong_variants_name_the_failure() {
    let server = Running::new(Transport::TcpTrusted, WalMemberReadPolicy::Positional);
    let mut client = server.client();
    let mut status = client.status().unwrap();
    status.max_reads = 0;
    assert_eq!(
        client
            .check_status("Fence", status)
            .unwrap_err()
            .to_string(),
        "Fence reply failed check: max_reads=0; expected 1..=256"
    );
    assert!(client.broken);
    let error = client
        .wrong_variant::<()>(
            "Seal",
            &Response::Retired(crate::distributed::storage::RetirementOutcome::Completed),
            "Sealed",
        )
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "Seal reply failed check: expected Sealed, received Retired"
    );
}

fn observe_idle_readiness_wait(client: &mut Client) {
    client.connection_profile(true, true).unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        // Give the drained owner an idle interval; correctness is established
        // by readiness counters and successful replies, not a latency threshold.
        thread::sleep(Duration::from_millis(20));
        let profile = client.connection_profile(true, false).unwrap();
        if profile.polling.idle_wait_calls != 0 {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "idle owner did not park on readiness"
        );
    }
}

fn adaptive_idle_controls_and_shutdown(transport: Transport) {
    let server = Running::with_limits(
        transport,
        WalMemberReadPolicy::Positional,
        default_device_reads(),
        TcpLimits {
            polling: PollingPolicy::Adaptive,
            ..TcpLimits::default()
        },
    );
    let mut client = server.client();
    client.fence_to(1).unwrap();
    for _ in 0..3 {
        observe_idle_readiness_wait(&mut client);
        assert_eq!(client.status().unwrap().active_epoch, Some(1));
    }
    observe_idle_readiness_wait(&mut client);
    drop(server);
    assert!(client.status().is_err());
}

#[test]
fn adaptive_idle_tcp_wakes_for_control_request_and_shutdown() {
    adaptive_idle_controls_and_shutdown(Transport::Tcp);
}

#[cfg(feature = "experimental-fabric")]
#[test]
#[ignore = "requires libfabric tcp;ofi_rxm and local network permission"]
fn software_provider_adaptive_idle_wakes_for_control_request_and_shutdown() {
    for transport in [
        software_transport(FabricSecurity::MutualTls),
        software_transport(FabricSecurity::Trusted),
    ] {
        adaptive_idle_controls_and_shutdown(transport);
    }
}

fn established_connection_survives_idle_timeout(transport: Transport) {
    let limits = TcpLimits {
        io_timeout: Duration::from_millis(300),
        max_frame_bytes: 2 * 1024 * 1024,
        ..TcpLimits::default()
    };
    let server = Running::with_limits(
        transport,
        WalMemberReadPolicy::Positional,
        default_device_reads(),
        limits,
    );
    // Fencing persists the fixture. Do that before opening the timed session:
    // this regression tests idle reuse, not an fsync deadline under
    // contention from other tests on the same disk.
    server
        .node
        .handle(Request::Fence { epoch: 1 }, limits)
        .unwrap();
    let mut client = server.client();
    thread::sleep(limits.io_timeout + Duration::from_millis(100));
    assert_eq!(
        client.status().unwrap().active_epoch,
        Some(1),
        "an authenticated established connection remains usable after an idle interval",
    );
}

#[test]
fn established_tcp_connection_survives_idle_timeout() {
    established_connection_survives_idle_timeout(Transport::Tcp);
}

#[cfg(feature = "experimental-fabric")]
#[test]
#[ignore = "requires libfabric tcp;ofi_rxm software provider"]
fn established_software_fabric_connection_survives_idle_timeout() {
    established_connection_survives_idle_timeout(software_transport(FabricSecurity::MutualTls));
}

#[cfg(feature = "experimental-fabric")]
#[test]
#[ignore = "requires libfabric tcp;ofi_rxm software provider"]
fn software_fabric_owner_transport_aggregate_sums_sessions_and_keeps_closed_ones() {
    let server = Running::new(
        software_transport(FabricSecurity::Trusted),
        WalMemberReadPolicy::Positional,
    );
    let sent = |reply: &PartitionStatsReply| {
        let transport = reply.transport;
        transport.send_size_le_4k
            + transport.send_size_le_16k
            + transport.send_size_le_64k
            + transport.send_size_gt_64k
    };
    let mut first = server.client();
    first.fence_to(1).unwrap();
    let mut second = server.client();
    for client in [&mut first, &mut second] {
        for _ in 0..8 {
            assert_eq!(client.status().unwrap().active_epoch, Some(1));
        }
    }
    // A session publishes its counters at the end of every sweep, so its own
    // later stats request already includes everything it sent before.
    let both_open = first.partition_stats().unwrap();
    let second_only = second.partition_stats().unwrap();
    assert!(sent(&both_open) >= 16, "{:?}", both_open.transport);
    assert!(sent(&second_only) >= sent(&both_open));
    drop(first);
    drop(second);
    let reopened = server.client().partition_stats().unwrap();
    assert!(
        sent(&reopened) >= sent(&second_only),
        "closed sessions must stay in the total"
    );
}

#[cfg(feature = "experimental-fabric")]
#[test]
#[ignore = "requires libfabric tcp;ofi_rxm software provider"]
fn fabric_tcp_native_mode_mismatch_with_tls_fails_closed() {
    let native_server = Running::new(
        software_transport(FabricSecurity::Trusted),
        WalMemberReadPolicy::Positional,
    );
    let tls_dir = native_server._temp.path().join("tls-client");
    fs::create_dir(&tls_dir).unwrap();
    let mut tls_client = certificates(&tls_dir);
    tls_client.transport = Transport::Tcp;
    assert!(
        Client::connect(
            &native_server.endpoint,
            &tls_client,
            Duration::from_secs(2),
            native_server.config.partition.clone(),
            &shape(),
        )
        .is_err(),
        "a TLS client must not be accepted by the native listener",
    );
    drop(native_server);

    let tls_server = Running::new(Transport::Tcp, WalMemberReadPolicy::Positional);
    let native_client = TlsFiles {
        transport: software_transport(FabricSecurity::Trusted),
        ca: PathBuf::new(),
        cert: PathBuf::new(),
        key: PathBuf::new(),
        server_name: "trusted-test-cluster".into(),
    };
    assert!(
        Client::connect(
            &tls_server.endpoint,
            &native_client,
            Duration::from_secs(2),
            tls_server.config.partition.clone(),
            &shape(),
        )
        .is_err(),
        "a native client must not downgrade to the TLS listener",
    );
}
#[derive(Clone, Copy)]
enum Fault {
    BadId,
    StaleProtocol,
    Truncated,
    Deadline,
    PartialValid,
}
trait FaultPeerIo: Read + Write {}
impl<T: Read + Write> FaultPeerIo for T {}

#[cfg(feature = "experimental-fabric")]
struct NativeFaultPeer {
    endpoint: tidehunter_fabric::Endpoint,
    _control: DeadlineIo,
    deadline: Instant,
}
#[cfg(feature = "experimental-fabric")]
impl Read for NativeFaultPeer {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        self.endpoint.receive(bytes, self.deadline)
    }
}
#[cfg(feature = "experimental-fabric")]
impl Write for NativeFaultPeer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.endpoint.send(bytes, self.deadline)
    }
    fn flush(&mut self) -> io::Result<()> {
        let fence = self.endpoint.accepted_send_fence()?;
        while self.endpoint.completed_send_fence()? < fence {
            self.endpoint.progress(self.deadline)?;
            thread::yield_now();
        }
        Ok(())
    }
}

/// A scripted owner answers the client's first correlated control request
/// (a Fence) with a real Fence reply, damaged as `fault` selects.
fn faulty_peer(fault: Fault, transport: Transport) {
    let temp = tempdir::TempDir::new("correlated-reply-fault").unwrap();
    let tls = if transport.is_native() {
        TlsFiles {
            transport: transport.clone(),
            ca: PathBuf::new(),
            cert: PathBuf::new(),
            key: PathBuf::new(),
            server_name: "trusted-test-cluster".to_owned(),
        }
    } else {
        certificates(temp.path())
    };
    let config = config(&temp.path().join("node"));
    let node = StorageNode::open(config.clone(), metrics()).unwrap();
    let status = node
        .handle(
            Request::Status {
                protocol: PROTOCOL,
                database: Some(node.database()),
            },
            TcpLimits::default(),
        )
        .unwrap();
    let response = node
        .handle(Request::Fence { epoch: 1 }, TcpLimits::default())
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = listener.local_addr().unwrap().to_string();
    let server_config = tls.server_config().unwrap().config;
    let (finished, completed) = mpsc::sync_channel(1);
    let worker = thread::spawn(move || {
        let (socket, _) = listener.accept().unwrap();
        let mut io = DeadlineIo::new(socket, Duration::from_secs(2)).unwrap();
        let mut stream: Box<dyn FaultPeerIo> = if transport.is_native() {
            #[cfg(feature = "experimental-fabric")]
            {
                read_transport_mode(&mut io, &transport).unwrap();
                let endpoint = fabric::bootstrap(&mut io, &transport).unwrap();
                Box::new(NativeFaultPeer {
                    endpoint,
                    _control: io,
                    deadline: Instant::now() + Duration::from_secs(3),
                })
            }
            #[cfg(not(feature = "experimental-fabric"))]
            unreachable!("native fault peer requires experimental-fabric")
        } else {
            let mut conn = ServerConnection::new(server_config.unwrap()).unwrap();
            while conn.is_handshaking() {
                conn.complete_io(&mut io).unwrap();
            }
            Box::new(StreamOwned::new(conn, io))
        };
        let hello: Envelope<Request> = read_frame(&mut stream, HARD_FRAME_LIMIT).unwrap();
        write_frame(
            &mut stream,
            &Envelope {
                protocol: PROTOCOL,
                id: hello.id,
                body: status,
            },
            HARD_FRAME_LIMIT,
        )
        .unwrap();
        let request: Envelope<Request> = read_frame(&mut stream, HARD_FRAME_LIMIT).unwrap();
        let reply = Envelope {
            protocol: if matches!(fault, Fault::StaleProtocol) {
                2
            } else {
                PROTOCOL
            },
            id: request.id + u64::from(matches!(fault, Fault::BadId)),
            body: response,
        };
        let encoded = encode_frame(&reply, HARD_FRAME_LIMIT).unwrap();
        match fault {
            Fault::BadId | Fault::StaleProtocol => {
                write_frame(&mut stream, &reply, HARD_FRAME_LIMIT).unwrap();
            }
            Fault::Truncated => {
                stream.write_all(&encoded[..encoded.len() - 1]).unwrap();
                stream.flush().unwrap();
            }
            Fault::Deadline => {}
            Fault::PartialValid => {
                for byte in encoded {
                    stream.write_all(&[byte]).unwrap();
                    stream.flush().unwrap();
                }
            }
        }
        if !matches!(fault, Fault::Truncated) {
            // Keep the peer live until the client accepts/rejects the actual
            // reply. EOF must not make an integrity failure appear to pass.
            completed.recv_timeout(Duration::from_secs(3)).unwrap();
        }
    });
    let mut client = Client::connect(
        &endpoint,
        &tls,
        Duration::from_secs(2),
        config.partition,
        &shape(),
    )
    .unwrap();
    let timeout = if matches!(fault, Fault::Deadline) {
        Duration::from_millis(40)
    } else {
        Duration::from_secs(2)
    };
    let request = Request::Fence { epoch: 1 };
    assert!(
        client
            .submit_control(&request, Instant::now() + timeout)
            .unwrap()
    );
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if client.progress_session().is_err() {
            assert!(!matches!(fault, Fault::PartialValid));
            assert!(client.status().is_err());
            break;
        }
        if let Some(result) = client.take_control() {
            assert!(matches!(fault, Fault::PartialValid));
            assert!(matches!(
                result.unwrap(),
                Response::Status(StatusReply::Database(status)) if status.active_epoch == Some(1)
            ));
            break;
        }
        assert!(Instant::now() < deadline);
        thread::yield_now();
    }
    if !matches!(fault, Fault::Truncated) {
        finished.send(()).unwrap();
    }
    worker.join().unwrap();
}
#[test]
fn unknown_reply_id_fails_connection() {
    faulty_peer(Fault::BadId, Transport::Tcp);
}
#[test]
fn stale_v2_reply_fails_connection() {
    faulty_peer(Fault::StaleProtocol, Transport::Tcp);
}
#[test]
fn truncated_reply_fails_connection() {
    faulty_peer(Fault::Truncated, Transport::Tcp);
}
#[test]
fn expired_inflight_deadline_fails_connection() {
    faulty_peer(Fault::Deadline, Transport::Tcp);
}
#[test]
fn tls_records_and_frame_header_can_arrive_one_byte_at_a_time() {
    faulty_peer(Fault::PartialValid, Transport::Tcp);
}
#[cfg(feature = "experimental-fabric")]
#[test]
#[ignore = "requires libfabric tcp;ofi_rxm software provider"]
fn fabric_control_violation_closes_a_live_session_without_waiting_for_rpc_deadline() {
    let server = Running::new(
        software_transport(FabricSecurity::MutualTls),
        WalMemberReadPolicy::Positional,
    );
    let mut client = server.client();
    client.fence_to(1).unwrap();
    assert_eq!(client.status().unwrap().active_epoch, Some(1));
    client
        .stream
        .sock
        .socket
        .write_all(b"forbidden post-bootstrap TCP data")
        .unwrap();
    // No pending app operation: the result must come from peer-health progress,
    // not from a request timeout (client=2s, server=3s). Allow scheduler slack.
    let deadline = Instant::now() + Duration::from_secs(1);
    loop {
        if client.progress_session().is_err() {
            assert!(client.status().is_err());
            break;
        }
        assert!(
            Instant::now() < deadline,
            "control violation was not detected"
        );
        thread::yield_now();
    }
}
