use super::*;
use crate::distributed::storage::WalMemberReadPolicy;
use crate::distributed::tcp::reactor::Envelope;
use std::sync::mpsc;

pub(in crate::distributed::tcp) struct Running {
    _temp: tempdir::TempDir,
    pub(in crate::distributed::tcp) config: NodeConfig,
    tls: TlsFiles,
    pub(in crate::distributed::tcp) node: Arc<StorageNode>,
    endpoint: String,
    shutdown: Arc<AtomicBool>,
    worker: Option<thread::JoinHandle<()>>,
}
impl Running {
    pub(in crate::distributed::tcp) fn new(
        transport: Transport,
        policy: WalMemberReadPolicy,
    ) -> Self {
        Self::with_bounds(
            transport,
            policy,
            default_read_bytes(),
            default_device_reads(),
        )
    }
    fn with_bounds(
        transport: Transport,
        policy: WalMemberReadPolicy,
        bytes: usize,
        device_reads: usize,
    ) -> Self {
        Self::with_cache(transport, policy, bytes, device_reads, 0)
    }
    fn with_cache(
        transport: Transport,
        policy: WalMemberReadPolicy,
        bytes: usize,
        device_reads: usize,
        cache_bytes: usize,
    ) -> Self {
        Self::with_limits(
            transport,
            policy,
            bytes,
            device_reads,
            cache_bytes,
            TcpLimits {
                io_timeout: Duration::from_secs(3),
                max_frame_bytes: 2 * 1024 * 1024,
                ..TcpLimits::default()
            },
        )
    }
    fn with_limits(
        transport: Transport,
        policy: WalMemberReadPolicy,
        bytes: usize,
        device_reads: usize,
        cache_bytes: usize,
        limits: TcpLimits,
    ) -> Self {
        Self::with_config(
            transport,
            policy,
            bytes,
            device_reads,
            cache_bytes,
            limits,
            |_| {},
        )
    }
    fn with_config(
        transport: Transport,
        policy: WalMemberReadPolicy,
        bytes: usize,
        device_reads: usize,
        cache_bytes: usize,
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
                resident_reads: ResidentReadConfig::default(),
            }
        } else {
            let mut tls = certificates(temp.path());
            tls.transport = transport;
            tls
        };
        let mut config = config(&temp.path().join("node"));
        config.wal_member_read = policy;
        config.read_max_inflight_bytes = bytes;
        config.read_max_device_reads = device_reads;
        config.read_cache_bytes = cache_bytes;
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
    fn data(&self, client: &mut Client) -> (Vec<CompleteBatch>, Vec<ValueRecordReference>) {
        client.fence_to(1).unwrap();
        let batches = vec![batch(&self.config, 1, 0), batch(&self.config, 1, 1)];
        let receipts = client.append_many(1, &batches).unwrap();
        let references = batches
            .iter()
            .zip(receipts)
            .map(|(batch, receipt)| {
                batch
                    .record_reference(receipt.appended().address(), 0)
                    .unwrap()
            })
            .collect();
        (batches, references)
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
fn collect(
    client: &mut Client,
    count: usize,
) -> Vec<crate::distributed::tcp::reactor::ReadCompletion> {
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut completed = Vec::new();
    while completed.len() != count {
        completed.extend(client.progress_reads().unwrap());
        assert!(Instant::now() < deadline, "read owner did not complete");
        thread::yield_now();
    }
    completed
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

fn adaptive_idle_reads_and_shutdown(transport: Transport) {
    let server = Running::with_limits(
        transport,
        WalMemberReadPolicy::Positional,
        default_read_bytes(),
        default_device_reads(),
        0,
        TcpLimits {
            polling: PollingPolicy::Adaptive,
            ..TcpLimits::default()
        },
    );
    let mut client = server.client();
    let (batches, references) = server.data(&mut client);
    for _ in 0..3 {
        observe_idle_readiness_wait(&mut client);
        assert_eq!(
            client.read_record(1, &references[0]).unwrap(),
            batches[0].mutations()[0],
        );
    }
    observe_idle_readiness_wait(&mut client);
    drop(server);
    assert!(client.read_record(1, &references[0]).is_err());
}

#[test]
fn adaptive_idle_tcp_wakes_for_scalar_read_and_shutdown() {
    adaptive_idle_reads_and_shutdown(Transport::Tcp);
}

#[cfg(feature = "experimental-fabric")]
#[test]
#[ignore = "requires libfabric tcp;ofi_rxm and local network permission"]
fn software_provider_adaptive_idle_wakes_for_scalar_read_and_shutdown() {
    for transport in [Transport::FabricTcp, Transport::FabricTcpNative] {
        adaptive_idle_reads_and_shutdown(transport);
    }
}

fn operational_session_for_transport(transport: Transport) {
    let server = Running::new(transport, WalMemberReadPolicy::Positional);
    let mut serving = server.client();
    let (batches, references) = server.data(&mut serving);
    assert_eq!(
        serving.read_record(1, &references[0]).unwrap(),
        batches[0].mutations()[0],
    );
    let tls = server.tls.client_config_with_identity().unwrap();
    let diagnostic = |expected| {
        Client::connect_operational_shared(
            &server.endpoint,
            &server.tls,
            tls.clone(),
            Duration::from_secs(2),
            expected,
            &shape(),
        )
        .unwrap()
    };
    let mut wrong_owner = server.config.partition.clone();
    wrong_owner.inventory.database[0] ^= 1;
    assert!(diagnostic(wrong_owner).operational_read_io_stats().is_err());
    let next = batch(&server.config, 1, 2);
    for try_status_upgrade in [false, true] {
        let mut client = diagnostic(server.config.partition.clone());
        let stats = client.operational_read_io_stats().unwrap().unwrap();
        assert_eq!(stats.completed_reads, 1);
        if try_status_upgrade {
            assert!(client.status().is_err());
        } else {
            assert!(client.append_many(1, std::slice::from_ref(&next)).is_err());
        }
    }
    let receipts = serving.append_many(1, std::slice::from_ref(&next)).unwrap();
    let reference = next
        .record_reference(receipts[0].appended().address(), 0)
        .unwrap();
    assert_eq!(
        serving.read_record(1, &reference).unwrap(),
        next.mutations()[0],
    );
}

#[test]
fn operational_session_binds_owner_identity_and_cannot_admit_data() {
    operational_session_for_transport(Transport::Tcp);
}

#[cfg(feature = "experimental-fabric")]
#[test]
#[ignore = "requires libfabric tcp;ofi_rxm and local network permission"]
fn software_provider_operational_session_binds_identity_and_cannot_admit_data() {
    for transport in [Transport::FabricTcp, Transport::FabricTcpNative] {
        operational_session_for_transport(transport);
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
        default_read_bytes(),
        default_device_reads(),
        0,
        limits,
    );
    // Fencing and appending persist the fixture. Do that before opening the
    // timed session: this regression tests idle reuse, not an fsync deadline
    // under contention from other tests on the same disk.
    server
        .node
        .handle(Request::Fence { epoch: 1 }, limits)
        .unwrap();
    let value = batch(&server.config, 1, 0);
    let Response::Appended(receipts) = server
        .node
        .handle(
            Request::Append {
                epoch: 1,
                batches: vec![value.encoded().clone().into()],
            },
            limits,
        )
        .unwrap()
    else {
        panic!("fixture append did not return its receipt");
    };
    let reference = value.record_reference(receipts[0].address, 0).unwrap();
    let mut client = server.client();
    thread::sleep(limits.io_timeout + Duration::from_millis(100));
    assert_eq!(
        client.read_record(1, &reference).unwrap(),
        value.mutations()[0],
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
    established_connection_survives_idle_timeout(Transport::FabricTcp);
}

fn delayed_independent(transport: Transport) {
    let server = Running::new(transport, WalMemberReadPolicy::Positional);
    let mut client = server.client();
    let (batches, references) = server.data(&mut client);
    let (entered, entries) = mpsc::sync_channel(1);
    let (release, released) = mpsc::sync_channel(1);
    let released = Mutex::new(released);
    *server.node.read_hook.lock() = Some(Arc::new(move |reference| {
        if reference.batch_version.sequence == 0 {
            entered.send(()).unwrap();
            released
                .lock()
                .recv_timeout(Duration::from_secs(5))
                .unwrap();
        }
    }));
    let slow = client
        .submit_read(
            1,
            vec![references[0]],
            Instant::now() + Duration::from_secs(2),
            ReadClass::Point,
            false,
        )
        .unwrap()
        .unwrap();
    let fast = client
        .submit_read(
            1,
            vec![references[1]],
            Instant::now() + Duration::from_secs(2),
            ReadClass::Point,
            false,
        )
        .unwrap()
        .unwrap();
    assert_ne!(slow, fast);
    let mut first = collect(&mut client, 1);
    entries.recv_timeout(Duration::from_secs(2)).unwrap();
    assert_eq!(
        first[0].id, fast,
        "one connection must not serialize independent reads"
    );
    assert_eq!(
        first.pop().unwrap().result.unwrap().0,
        vec![batches[1].mutations()[0].clone()]
    );
    // Keep a cold/blocking worker pending while the socket is otherwise idle.
    // A new ready read must still progress, and its completion cannot rely on
    // another arrival to wake the owner.
    client.connection_profile(true, true).unwrap();
    let another = client
        .submit_read(
            1,
            vec![references[1]],
            Instant::now() + Duration::from_secs(2),
            ReadClass::Point,
            false,
        )
        .unwrap()
        .unwrap();
    let completed = collect(&mut client, 1).pop().unwrap();
    assert_eq!(completed.id, another);
    assert_eq!(
        completed.result.unwrap().0,
        vec![batches[1].mutations()[0].clone()],
    );
    let profile = client.connection_profile(false, false).unwrap();
    assert_eq!(profile.polling.idle_wait_calls, 0);
    release.send(()).unwrap();
    let mut last = collect(&mut client, 1);
    assert_eq!(last[0].id, slow);
    assert_eq!(
        last.pop().unwrap().result.unwrap().0,
        vec![batches[0].mutations()[0].clone()]
    );
    server.node.read_hook.lock().take();
}
#[test]
fn one_tcp_connection_completes_ready_read_before_delayed_read() {
    delayed_independent(Transport::Tcp);
}
#[cfg(feature = "experimental-fabric")]
#[test]
#[ignore = "requires libfabric tcp;ofi_rxm software provider"]
fn one_software_fabric_connection_completes_out_of_order() {
    delayed_independent(Transport::FabricTcp);
}
#[cfg(feature = "experimental-fabric")]
#[test]
#[ignore = "requires libfabric tcp;ofi_rxm software provider"]
fn native_software_provider_mixed_rpc_reads_complete_out_of_order() {
    delayed_independent(Transport::FabricTcpNative);
}

#[cfg(feature = "experimental-fabric")]
#[test]
#[ignore = "requires libfabric tcp;ofi_rxm software provider"]
fn fabric_tcp_native_rejects_invalid_reference_and_epoch_without_tls() {
    let server = Running::new(Transport::FabricTcpNative, WalMemberReadPolicy::Positional);
    let mut client = server.client();
    let (batches, references) = server.data(&mut client);

    let mut forged_offset = references[0];
    forged_offset.descriptor.offset += 1;
    assert!(client.read_record(1, &forged_offset).is_err());

    let mut forged_checksum = references[0];
    forged_checksum.descriptor.checksum.0 ^= 1;
    assert!(client.read_record(1, &forged_checksum).is_err());
    assert!(client.read_record(2, &references[0]).is_err());

    assert_eq!(
        client.read_record(1, &references[0]).unwrap(),
        batches[0].mutations()[0],
        "rejected references and epochs must not poison a valid native session",
    );
}
#[cfg(feature = "experimental-fabric")]
#[test]
#[ignore = "requires libfabric tcp;ofi_rxm software provider"]
fn fabric_tcp_native_mode_mismatch_with_tls_fails_closed() {
    let native_server = Running::new(Transport::FabricTcpNative, WalMemberReadPolicy::Positional);
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
        transport: Transport::FabricTcpNative,
        ca: PathBuf::new(),
        cert: PathBuf::new(),
        key: PathBuf::new(),
        server_name: "trusted-test-cluster".into(),
        resident_reads: ResidentReadConfig::default(),
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
#[test]
fn fence_while_read_pending_is_rechecked_before_reply() {
    let server = Running::new(Transport::Tcp, WalMemberReadPolicy::Mapped);
    let mut client = server.client();
    let (_, references) = server.data(&mut client);
    let (entered, entries) = mpsc::sync_channel(1);
    let (release, released) = mpsc::sync_channel(1);
    let released = Mutex::new(released);
    *server.node.read_hook.lock() = Some(Arc::new(move |_| {
        entered.send(()).unwrap();
        released
            .lock()
            .recv_timeout(Duration::from_secs(5))
            .unwrap();
    }));
    client
        .submit_read(
            1,
            vec![references[0]],
            Instant::now() + Duration::from_secs(2),
            ReadClass::Point,
            false,
        )
        .unwrap()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        assert!(client.progress_reads().unwrap().is_empty());
        if entries.try_recv().is_ok() {
            break;
        }
        assert!(Instant::now() < deadline);
        thread::yield_now();
    }
    let mut authority = server.client();
    authority.fence_to(2).unwrap();
    release.send(()).unwrap();
    assert!(collect(&mut client, 1).pop().unwrap().result.is_err());
    server.node.read_hook.lock().take();
}

#[cfg(target_os = "linux")]
#[test]
fn async_tcp_multiple_misses_are_bounded_and_report_real_wal_submissions() {
    let server = Running::new(Transport::Tcp, WalMemberReadPolicy::Async);
    let mut client = server.client();
    let (batches, references) = server.data(&mut client);
    let before = client.read_io_stats().unwrap();
    observe_idle_readiness_wait(&mut client);
    let mut ids = Vec::new();
    for index in 0..32 {
        ids.push(
            client
                .submit_read(
                    1,
                    vec![references[index % 2]],
                    Instant::now() + Duration::from_secs(2),
                    ReadClass::Point,
                    false,
                )
                .unwrap()
                .unwrap(),
        );
    }
    assert!(!client.read_admission(&references[..1], false).unwrap());
    assert!(
        client
            .submit_read(
                1,
                vec![references[0]],
                Instant::now() + Duration::from_secs(2),
                ReadClass::Point,
                false
            )
            .unwrap()
            .is_none()
    );
    for completion in collect(&mut client, 32) {
        let index = ids.iter().position(|id| *id == completion.id).unwrap();
        assert_eq!(
            completion.result.unwrap().0,
            vec![batches[index % 2].mutations()[0].clone()]
        );
    }
    let after = client.read_io_stats().unwrap();
    assert_eq!(after.cache_budget, 0);
    assert_eq!(after.cache_bytes, 0);
    assert_eq!(after.wal_reads - before.wal_reads, 32);
    assert_eq!(after.cache_misses - before.cache_misses, 32);
    assert!(after.peak_inflight_requests <= server.config.read_max_inflight_requests);
    assert!(after.peak_inflight_bytes <= server.config.read_max_inflight_bytes);
    assert!(after.peak_inflight_reads <= server.config.read_max_device_reads);
}

#[cfg(target_os = "linux")]
#[test]
fn async_reader_disconnects_do_not_block_new_authenticated_connections() {
    let server = Running::new(Transport::Tcp, WalMemberReadPolicy::Async);
    let mut client = server.client();
    let (batches, references) = server.data(&mut client);
    for index in 0..32 {
        assert_eq!(
            client.read_record(1, &references[index % 2]).unwrap(),
            batches[index % 2].mutations()[0]
        );
        drop(client);
        client = server.client();
    }
    let stats = client.read_io_stats().unwrap();
    assert_eq!(stats.wal_reads, 32);
    assert_eq!(stats.cache_entries, 0);
}

#[cfg(target_os = "linux")]
#[test]
fn aggregate_256_members_split_at_negotiated_device_and_byte_bounds() {
    let server = Running::with_bounds(Transport::Tcp, WalMemberReadPolicy::Async, 32 * 1024, 8);
    let mut client = server.client();
    let (batches, references) = server.data(&mut client);
    let status = client.status().unwrap();
    assert_eq!(status.max_reads, 8);
    assert_eq!(status.max_read_bytes, 32 * 1024);
    let requested: Vec<_> = (0..256).map(|i| references[i % 2]).collect();
    let (values, profile) = client.read_records_profiled(1, &requested).unwrap();
    assert!(profile.requests > 256 / 8); // retained-byte cap further splits
    assert_eq!(profile.server_response_recheck_samples, profile.requests);
    assert_eq!(values.len(), 256);
    for (index, value) in values.iter().enumerate() {
        assert_eq!(value, &batches[index % 2].mutations()[0]);
    }
    let stats = client.read_io_stats().unwrap();
    assert_eq!(stats.wal_reads, 256);
    assert!(stats.peak_inflight_reads <= 8);
    assert!(stats.peak_inflight_bytes <= 32 * 1024);
}

#[derive(Clone, Copy)]
enum Fault {
    BadId,
    StaleProtocol,
    Truncated,
    Deadline,
    PartialValid,
    CorruptMember,
    WrongMemberCount,
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

fn faulty_peer(fault: Fault, transport: Transport) {
    let temp = tempdir::TempDir::new("correlated-reply-fault").unwrap();
    let tls = if transport.is_native() {
        TlsFiles {
            transport,
            ca: PathBuf::new(),
            cert: PathBuf::new(),
            key: PathBuf::new(),
            server_name: "trusted-test-cluster".to_owned(),
            resident_reads: ResidentReadConfig::default(),
        }
    } else {
        certificates(temp.path())
    };
    let config = config(&temp.path().join("node"));
    let node = StorageNode::open(config.clone(), metrics()).unwrap();
    node.inventory.lock().fence_to(1).unwrap();
    let value = batch(&config, 1, 0);
    let receipts = append(&node, 1, std::slice::from_ref(&value));
    let reference = value.record_reference(receipts[0].address, 0).unwrap();
    let status = node
        .handle(Request::Status { protocol: PROTOCOL }, TcpLimits::default())
        .unwrap();
    let mut response = node
        .handle(
            Request::Read {
                epoch: 1,
                references: vec![reference],
                class: ReadClass::Point,
                grant_limit: 0,
                grant_max_bytes: 0,
            },
            TcpLimits::default(),
        )
        .unwrap();
    if let Response::Records(records) = &mut response {
        if matches!(fault, Fault::CorruptMember) {
            let mut bytes = records.pop().unwrap().into_vec();
            *bytes.last_mut().unwrap() ^= 1;
            records.push(bytes.into());
        } else if matches!(fault, Fault::WrongMemberCount) {
            records.clear();
        }
    }
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
                read_transport_mode(&mut io, transport).unwrap();
                let endpoint =
                    fabric::bootstrap(&mut io, transport, ResidentReadConfig::default()).unwrap();
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
            Fault::BadId
            | Fault::StaleProtocol
            | Fault::CorruptMember
            | Fault::WrongMemberCount => {
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
    client
        .submit_read(
            1,
            vec![reference],
            Instant::now() + timeout,
            ReadClass::Point,
            false,
        )
        .unwrap()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        match client.progress_reads() {
            Err(_) => {
                assert!(!matches!(fault, Fault::PartialValid));
                assert!(client.status().is_err());
                break;
            }
            Ok(mut completed) if !completed.is_empty() => {
                assert!(matches!(fault, Fault::PartialValid));
                assert_eq!(
                    completed.pop().unwrap().result.unwrap().0,
                    value.mutations()
                );
                break;
            }
            Ok(_) => {}
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

#[test]
fn authenticated_scalar_payload_corruption_fails_connection() {
    faulty_peer(Fault::CorruptMember, Transport::Tcp);
}

#[test]
fn authenticated_scalar_wrong_member_count_fails_connection() {
    faulty_peer(Fault::WrongMemberCount, Transport::Tcp);
}

#[cfg(feature = "experimental-fabric")]
#[test]
#[ignore = "requires libfabric tcp;ofi_rxm software provider"]
fn fabric_tcp_native_rejects_corrupt_member_reply_without_tls() {
    faulty_peer(Fault::CorruptMember, Transport::FabricTcpNative);
}

#[test]
fn diagnostic_read_policies_reject_async_executor_options() {
    let temp = tempdir::TempDir::new("diagnostic-cache-policy").unwrap();
    for policy in [WalMemberReadPolicy::Mapped, WalMemberReadPolicy::Positional] {
        for (cache_bytes, resident_first) in [(1024 * 1024, false), (0, true)] {
            let mut config = config(&temp.path().join("node"));
            config.wal_member_read = policy;
            config.read_cache_bytes = cache_bytes;
            config.read_resident_first = resident_first;
            assert!(StorageNode::open(config, metrics()).is_err());
        }
    }
}

#[cfg(target_os = "linux")]
#[test]
fn authenticated_scan_class_uses_hits_without_filling_point_cache() {
    let server = Running::with_cache(
        Transport::Tcp,
        WalMemberReadPolicy::Async,
        default_read_bytes(),
        default_device_reads(),
        1024 * 1024,
    );
    let mut client = server.client();
    let (batches, references) = server.data(&mut client);
    assert_eq!(
        client.read_record(1, &references[0]).unwrap(),
        batches[0].mutations()[0]
    );
    let warm = client.read_io_stats().unwrap();
    assert_eq!(warm.cache_inserts, 1);
    for index in [1, 0] {
        let id = client
            .submit_read(
                1,
                vec![references[index]],
                Instant::now() + Duration::from_secs(2),
                ReadClass::ScanNoFill,
                false,
            )
            .unwrap()
            .unwrap();
        let result = collect(&mut client, 1).pop().unwrap();
        assert_eq!(result.id, id);
        assert_eq!(
            result.result.unwrap().0,
            vec![batches[index].mutations()[0].clone()]
        );
        let stats = client.read_io_stats().unwrap();
        assert_eq!(stats.cache_inserts, warm.cache_inserts);
        assert_eq!(stats.cache_bypass_scan, warm.cache_bypass_scan + 1);
        assert_eq!(stats.cache_entries, warm.cache_entries);
        if index == 0 {
            assert_eq!(stats.cache_hits, warm.cache_hits + 1);
        }
    }
    let before_point = client.read_io_stats().unwrap();
    client.read_record(1, &references[0]).unwrap();
    assert_eq!(
        client.read_io_stats().unwrap().cache_hits,
        before_point.cache_hits + 1
    );
}

#[test]
fn idle_reuse_refreshes_status_and_preserves_durable_scalar_outcomes() {
    let server = Running::new(Transport::Tcp, WalMemberReadPolicy::Positional);
    let mut client = server.client();
    let batches = vec![batch(&server.config, 1, 0)];
    client.fence_to(1).unwrap();
    let receipts = client.append_many(1, &batches).unwrap();
    let reference = batches[0]
        .record_reference(receipts[0].appended().address(), 0)
        .unwrap();
    client.refresh_for_reuse().unwrap();
    assert_eq!(client.connected_status().active_epoch, Some(1));
    assert_eq!(client.append_many(1, &batches).unwrap(), receipts);
    assert_eq!(
        client.read_record(1, &reference).unwrap(),
        batches[0].mutations()[0]
    );
    let mut authority = server.client();
    authority.fence_to(2).unwrap();
    client.refresh_for_reuse().unwrap();
    assert_eq!(client.connected_status().active_epoch, Some(2));
    assert!(client.append_many(1, &batches).is_err());
}

#[test]
fn reuse_rejects_live_read_without_consuming_its_result() {
    let server = Running::new(Transport::Tcp, WalMemberReadPolicy::Positional);
    let mut client = server.client();
    let (batches, references) = server.data(&mut client);
    let id = client
        .submit_read(
            1,
            vec![references[0]],
            Instant::now() + Duration::from_secs(2),
            ReadClass::Point,
            false,
        )
        .unwrap()
        .unwrap();
    assert!(client.refresh_for_reuse().is_err());
    let result = collect(&mut client, 1).remove(0);
    assert_eq!(result.id, id);
    assert_eq!(result.result.unwrap().0, batches[0].mutations());
    client.refresh_for_reuse().unwrap();
}

#[test]
fn reuse_revalidates_certificate_time_and_original_trust_snapshot() {
    let server = Running::new(Transport::Tcp, WalMemberReadPolicy::Positional);
    let client = server.client();
    client
        .revalidate_peer_at(rustls::pki_types::UnixTime::now())
        .unwrap();
    let far_future =
        rustls::pki_types::UnixTime::since_unix_epoch(Duration::from_secs(4_102_444_800));
    assert!(client.revalidate_peer_at(far_future).is_err());
    let before = server.tls.client_config_with_identity().unwrap().identity;
    let original = fs::read(&server.tls.cert).unwrap();
    fs::write(&server.tls.cert, b"not a certificate").unwrap();
    assert!(server.tls.client_config_with_identity().is_err());
    // Existing verification must use its authenticated snapshot, not re-read
    // credentials which may now differ from the parent's pool key.
    client
        .revalidate_peer_at(rustls::pki_types::UnixTime::now())
        .unwrap();
    fs::write(&server.tls.cert, original).unwrap();
    assert_eq!(
        server.tls.client_config_with_identity().unwrap().identity,
        before
    );
}

#[test]
fn reuse_fails_closed_after_control_peer_shutdown() {
    let server = Running::new(Transport::Tcp, WalMemberReadPolicy::Positional);
    let mut client = server.client();
    client
        .stream
        .sock
        .socket
        .shutdown(std::net::Shutdown::Both)
        .unwrap();
    assert!(client.refresh_for_reuse().is_err());
    assert!(client.status().is_err());
}

#[cfg(feature = "experimental-fabric")]
#[test]
#[ignore = "requires libfabric tcp;ofi_rxm software provider"]
fn fabric_control_violation_closes_a_live_session_without_waiting_for_rpc_deadline() {
    let server = Running::new(Transport::FabricTcp, WalMemberReadPolicy::Positional);
    let mut client = server.client();
    let (batches, references) = server.data(&mut client);
    client.refresh_for_reuse().unwrap();
    assert_eq!(
        client.read_record(1, &references[0]).unwrap(),
        batches[0].mutations()[0]
    );
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
        if client.progress_reads().is_err() {
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

#[cfg(feature = "test-utils")]
fn ram_probe_round_trip(transport: Transport, polling: PollingPolicy) {
    let server = Running::with_limits(
        transport,
        WalMemberReadPolicy::Positional,
        default_read_bytes(),
        default_device_reads(),
        0,
        TcpLimits {
            polling,
            ..TcpLimits::default()
        },
    );
    let mut client = server.client();
    assert_eq!(client.polling_policy(), polling);
    assert_eq!(client.status().unwrap().polling, polling);
    let mut isolated_profile = TcpReadProfile::default();
    client
        .exchange_correlated(
            &Request::Status { protocol: PROTOCOL },
            Some(&mut isolated_profile),
        )
        .unwrap();
    assert_eq!(isolated_profile.requests, 1);
    assert!(isolated_profile.response_decode_ns > 0);
    assert!(
        client
            .read_ram_probe()
            .unwrap_err()
            .to_string()
            .contains("not prepared")
    );
    let before = client.read_io_stats().unwrap();
    client.connection_profile(true, true).unwrap();
    client.status().unwrap();
    // Boundary and persistent replacement cases; the last zero-length buffer
    // is distinct from the unprepared state.
    for len in [0, 1, 1024, 8192, 65536, 0] {
        client.prepare_ram_probe(len).unwrap();
        // Exercise the genuinely idle policy, including manual fabric CQ
        // progress. No microsecond latency threshold is used as evidence.
        thread::sleep(Duration::from_millis(20));
        for _ in 0..3 {
            let bytes = client.read_ram_probe().unwrap();
            assert_eq!(bytes.len(), len);
            assert!(bytes.iter().enumerate().all(|(i, &b)| b == (i % 251) as u8));
        }
    }
    assert!(client.prepare_ram_probe(65537).is_err());
    // Bypass the public preflight to exercise the server's boundary as well.
    assert!(
        client
            .request(Request::PrepareRamProbe {
                value_bytes: usize::MAX
            })
            .is_err()
    );
    assert_eq!(client.read_ram_probe().unwrap().len(), 0);
    let profile = client.connection_profile(false, false).unwrap();
    assert!(profile.polling.progress_sweeps > 0);
    assert!(profile.polling.request_first_poll_samples > 0);
    assert!(profile.control_queue_samples > 0);
    assert_eq!(
        profile.control_queue_samples,
        profile.control_execution_samples
    );
    assert!(profile.control_execution_ns > 0);
    assert!(profile.handler_ns >= profile.control_execution_ns);
    for stats in [profile.transport, profile.client_transport] {
        assert!(stats.frame_encode.calls > 0);
        assert!(stats.frame_decode.calls > 0);
        assert!(stats.plaintext_write.bytes > 0);
        assert!(stats.plaintext_read.bytes > 0);
        if transport.is_native() {
            assert_eq!(stats.tls_read.calls, 0);
            assert_eq!(stats.tls_write.calls, 0);
            assert_eq!(stats.tls_process.calls, 0);
        } else {
            assert!(stats.tls_read.calls > 0);
            assert!(stats.tls_write.calls > 0);
            assert!(stats.tls_process.calls > 0);
        }
        assert!(stats.output_queue_samples > 0);
        assert!(stats.output_submit_samples > 0);
        assert!(stats.output_retire_samples > 0);
    }
    if transport != Transport::Tcp {
        for transport in [profile.transport, profile.client_transport] {
            assert!(transport.send_copy_calls > 0);
            assert!(transport.receive_copy_calls > 0);
            assert!(transport.send_post_calls > 0);
            assert!(transport.receive_post_calls > 0);
            assert!(transport.cq_timed_calls > 0);
            assert!(transport.cq_dispatch_calls > 0);
        }
    }
    if polling == PollingPolicy::Busy {
        assert_eq!(profile.polling.idle_wait_calls, 0);
        assert_eq!(profile.polling.scheduler_yields, 0);
    }
    let after = client.read_io_stats().unwrap();
    assert_eq!(before.wal_reads, after.wal_reads);
    assert_eq!(before.wal_read_bytes, after.wal_read_bytes);
    assert_eq!(before.completed_reads, after.completed_reads);
    // Probe state belongs to the authenticated session, not node/WAL state.
    let mut other = server.client();
    assert!(other.read_ram_probe().is_err());
    // Shutdown must still terminate a busy idle session while the client lives.
    drop(server);
    assert!(client.read_ram_probe().is_err());
}

#[cfg(feature = "test-utils")]
#[test]
fn authenticated_ram_probe_boundaries_and_busy_shutdown() {
    for polling in [PollingPolicy::Adaptive, PollingPolicy::Busy] {
        ram_probe_round_trip(Transport::Tcp, polling);
    }
}

#[cfg(all(feature = "test-utils", feature = "experimental-fabric"))]
#[test]
#[ignore = "requires libfabric tcp;ofi_rxm software provider"]
fn fabric_authenticated_ram_probe_boundaries_and_busy_shutdown() {
    for polling in [PollingPolicy::Adaptive, PollingPolicy::Busy] {
        ram_probe_round_trip(Transport::FabricTcp, polling);
    }
}
#[cfg(all(feature = "test-utils", feature = "experimental-fabric"))]
#[test]
#[ignore = "requires libfabric tcp;ofi_rxm software provider"]
fn fabric_tcp_native_large_partial_frames_bypass_tls() {
    for polling in [PollingPolicy::Adaptive, PollingPolicy::Busy] {
        ram_probe_round_trip(Transport::FabricTcpNative, polling);
    }
}

#[cfg(feature = "test-utils")]
#[test]
fn ram_probe_frame_bound_and_expired_request_preserve_fail_stop() {
    let server = Running::with_limits(
        Transport::Tcp,
        WalMemberReadPolicy::Positional,
        default_read_bytes(),
        default_device_reads(),
        0,
        TcpLimits {
            max_frame_bytes: 8192,
            polling: PollingPolicy::Busy,
            ..TcpLimits::default()
        },
    );
    let mut client = server.client();
    client.prepare_ram_probe(8168).unwrap();
    assert_eq!(client.read_ram_probe().unwrap().len(), 8168);
    assert!(client.prepare_ram_probe(8169).is_err());
    assert!(
        client
            .request(Request::PrepareRamProbe { value_bytes: 8169 })
            .is_err()
    );
    assert_eq!(client.read_ram_probe().unwrap().len(), 8168);
    // The normal absolute deadline guard is deterministic, not a sleep/race.
    client.timeout = Duration::ZERO;
    assert!(
        client
            .read_ram_probe()
            .unwrap_err()
            .to_string()
            .contains("deadline")
    );
    assert!(client.read_ram_probe().is_err());
}

#[cfg(feature = "test-utils")]
#[test]
fn ram_probe_does_not_acquire_inventory_or_dispatch_read_workers() {
    let server = Running::new(Transport::Tcp, WalMemberReadPolicy::Positional);
    let mut client = server.client();
    client.prepare_ram_probe(8192).unwrap();
    // Both blocking workers are occupied, and the inventory is inaccessible.
    // A reactor-local diagnostic must still produce the real response.
    let (started_tx, started_rx) = mpsc::channel();
    let mut releases = Vec::new();
    for _ in 0..server.config.read_workers {
        let (release_tx, release_rx) = mpsc::channel::<()>();
        releases.push(release_tx);
        let started = started_tx.clone();
        server.node.read_pool.spawn(move || {
            started.send(()).unwrap();
            let _ = release_rx.recv();
        });
    }
    for _ in 0..server.config.read_workers {
        started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    }
    let inventory = server.node.inventory.lock();
    let response = client.read_ram_probe();
    drop(inventory);
    // Always release before asserting so a failing test cannot strand workers.
    drop(releases);
    assert_eq!(response.unwrap().len(), 8192);
}

fn cold_authority_does_not_move_hot_reads_to_the_pool(transport: Transport, fenced: bool) {
    let server = Running::with_config(
        transport,
        WalMemberReadPolicy::Async,
        default_read_bytes(),
        default_device_reads(),
        64 * 1024,
        TcpLimits {
            io_timeout: Duration::from_secs(3),
            ..TcpLimits::default()
        },
        |config| {
            config.partition.max_batches = 1;
            config.partition.group_limits.max_batches = 1;
        },
    );
    let mut client = server.client();
    client.fence_to(1).unwrap();
    let batches = [batch(&server.config, 1, 0), batch(&server.config, 1, 1)];
    let references: Vec<_> = batches
        .iter()
        .map(|batch| {
            let receipt = client
                .append_many(1, std::slice::from_ref(batch))
                .unwrap()
                .remove(0);
            batch
                .record_reference(receipt.appended().address(), 0)
                .unwrap()
        })
        .collect();
    assert_eq!(
        client.read_record(1, &references[1]).unwrap(),
        batches[1].mutations()[0]
    );
    let before = client.read_io_stats().unwrap();
    let log = server.node.inventory.lock().active(1).unwrap();
    log.evict_authority_cache();
    assert!(log.try_plan_record_read(&references[0]).unwrap().is_none());
    assert!(log.try_plan_record_read(&references[1]).unwrap().is_some());
    let (entered, release) = log.pause_next_authority_load();
    let slow = client
        .submit_read(
            1,
            vec![references[0]],
            Instant::now() + Duration::from_secs(2),
            ReadClass::Point,
            false,
        )
        .unwrap()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        assert!(client.progress_reads().unwrap().is_empty());
        if entered.try_recv().is_ok() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "cold authority lookup did not reach its worker"
        );
        thread::yield_now();
    }
    // The cold lookup occupies one worker. Occupy every other worker as well:
    // a warm payload/authority hit must not need an unconditional worker hop.
    let (blocked, blocking) = mpsc::sync_channel(1);
    let (resume, resumed) = mpsc::sync_channel(1);
    server.node.read_pool.spawn(move || {
        blocked.send(()).unwrap();
        resumed.recv_timeout(Duration::from_secs(5)).unwrap();
    });
    blocking.recv_timeout(Duration::from_secs(2)).unwrap();
    let fast = client
        .submit_read(
            1,
            vec![references[1]],
            Instant::now() + Duration::from_secs(2),
            ReadClass::Point,
            false,
        )
        .unwrap()
        .unwrap();
    let completed = collect(&mut client, 1).pop().unwrap();
    assert_eq!(completed.id, fast);
    assert_eq!(
        completed.result.unwrap().0,
        vec![batches[1].mutations()[0].clone()]
    );
    let after = client.read_io_stats().unwrap();
    assert_eq!(after.cache_hits, before.cache_hits + 1);
    assert_eq!(
        after.wal_reads, before.wal_reads,
        "cold metadata has not submitted value I/O"
    );
    if fenced {
        server.client().fence_to(2).unwrap();
    }
    resume.send(()).unwrap();
    release.send(()).unwrap();
    let completed = collect(&mut client, 1).pop().unwrap();
    assert_eq!(completed.id, slow);
    if fenced {
        assert!(
            completed.result.is_err(),
            "a cold plan must recheck its captured generation"
        );
    } else {
        assert_eq!(
            completed.result.unwrap().0,
            vec![batches[0].mutations()[0].clone()]
        );
    }
}

#[test]
fn tcp_cold_authority_preserves_hot_progress_and_rechecks_fencing() {
    for fenced in [false, true] {
        cold_authority_does_not_move_hot_reads_to_the_pool(Transport::Tcp, fenced);
    }
}

#[cfg(feature = "experimental-fabric")]
#[test]
#[ignore = "requires libfabric tcp;ofi_rxm software provider"]
fn software_fabric_cold_authority_preserves_hot_progress_and_rechecks_fencing() {
    for fenced in [false, true] {
        cold_authority_does_not_move_hot_reads_to_the_pool(Transport::FabricTcp, fenced);
    }
}

fn disconnect_retains_cold_planning_credits_until_the_worker_retires_for(transport: Transport) {
    let server = Running::with_config(
        transport,
        WalMemberReadPolicy::Async,
        default_read_bytes(),
        default_device_reads(),
        0,
        TcpLimits {
            io_timeout: Duration::from_secs(3),
            ..TcpLimits::default()
        },
        |config| {
            config.partition.max_batches = 1;
            config.partition.group_limits.max_batches = 1;
        },
    );
    let mut client = server.client();
    client.fence_to(1).unwrap();
    let first = batch(&server.config, 1, 0);
    let receipt = client
        .append_many(1, std::slice::from_ref(&first))
        .unwrap()
        .remove(0);
    client
        .append_many(1, &[batch(&server.config, 1, 1)])
        .unwrap();
    let reference = first
        .record_reference(receipt.appended().address(), 0)
        .unwrap();
    let log = server.node.inventory.lock().active(1).unwrap();
    log.evict_authority_cache();
    let (entered, release) = log.pause_next_authority_load();
    let read_deadline = Instant::now() + Duration::from_secs(2);
    client
        .submit_read(1, vec![reference], read_deadline, ReadClass::Point, false)
        .unwrap()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        assert!(client.progress_reads().unwrap().is_empty());
        if entered.try_recv().is_ok() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "cold authority worker did not start"
        );
        thread::yield_now();
    }
    let charged = server.node.read_admission.stats().inflight_bytes;
    assert!(charged > 0);
    assert_eq!(server.node.read_admission.stats().inflight_requests, 1);
    assert_eq!(server.node.read_admission.stats().wal_reads, 0);
    if transport.is_native() {
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            match client.progress_reads() {
                Err(_) => {
                    assert!(Instant::now() >= read_deadline);
                    break;
                }
                Ok(completed) => assert!(completed.is_empty()),
            }
            assert!(
                Instant::now() < deadline,
                "native outstanding read did not expire at its absolute deadline"
            );
        }
        assert_eq!(server.node.read_admission.stats().inflight_bytes, charged);
    }
    drop(client);
    assert_eq!(server.node.read_admission.stats().inflight_bytes, charged);
    release.send(()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    while server.node.read_admission.stats().inflight_requests != 0 {
        assert!(
            Instant::now() < deadline,
            "cancelled cold planning retained reply credit"
        );
        thread::yield_now();
    }
    assert_eq!(server.node.read_admission.stats().inflight_bytes, 0);
    assert_eq!(server.node.read_admission.stats().inflight_reads, 0);
}
#[test]
fn disconnect_retains_cold_planning_credits_until_the_worker_retires() {
    disconnect_retains_cold_planning_credits_until_the_worker_retires_for(Transport::Tcp);
}

#[cfg(feature = "experimental-fabric")]
#[test]
#[ignore = "requires libfabric tcp;ofi_rxm software provider"]
fn fabric_tcp_native_disconnect_retains_owned_reply_credits_until_retirement() {
    disconnect_retains_cold_planning_credits_until_the_worker_retires_for(
        Transport::FabricTcpNative,
    );
}
