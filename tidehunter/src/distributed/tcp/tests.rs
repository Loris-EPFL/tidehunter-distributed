use super::*;
use crate::distributed::codec::BatchLimits;
use crate::distributed::group::GroupLimits;
use crate::distributed::storage::StaticInventory;
use crate::key_shape::{KeySpace, KeyType};
use std::path::Path;
use std::process::Command;

#[path = "reactor_tests.rs"]
pub(in crate::distributed::tcp) mod multiplexed;
#[path = "position_tests.rs"]
mod position_tests;
#[path = "wal_replay_tests.rs"]
mod wal_replay;

#[cfg(feature = "experimental-fabric")]
pub(super) fn software_transport(security: FabricSecurity) -> Transport {
    Transport::ofi(FabricBackend::SoftwareTcp, security)
}

#[test]
fn default_owner_limits_admit_an_automatic_coordinator_and_its_reopen() {
    use crate::distributed::remote_db::RemoteDatabaseConfig;
    let limits = TcpLimits::default();
    limits.validate().unwrap();
    // A coordinator that omits `read_lanes` sizes its lanes from the host up to the cap;
    // default owner limits must admit two such session sets.
    assert!(
        limits.max_connections
            >= RemoteDatabaseConfig::generated_owner_connection_limit(None).unwrap()
    );
}

#[test]
fn structured_transport_identity_ignores_local_selectors_only() {
    let left = Transport::Ofi {
        fabric: FabricConfig {
            backend: FabricBackend::Verbs,
            domain: Some("mlx5_0".to_owned()),
            source_address: Some("10.1.0.1".parse().unwrap()),
            slots: None,
        },
        security: FabricSecurity::MutualTls,
    };
    let right_fabric = FabricConfig {
        backend: FabricBackend::Verbs,
        domain: Some("mlx5_1".to_owned()),
        source_address: Some("10.1.0.2".parse().unwrap()),
        slots: Some(8),
    };
    let right = Transport::Ofi {
        fabric: right_fabric.clone(),
        security: FabricSecurity::MutualTls,
    };
    assert!(left.same_protocol(&right));
    assert!(!left.same_protocol(&Transport::ofi(
        FabricBackend::SoftwareTcp,
        FabricSecurity::MutualTls,
    )));
    assert!(!left.same_protocol(&Transport::Ofi {
        fabric: right_fabric.clone(),
        security: FabricSecurity::Trusted,
    }));
    assert!(!Transport::Tcp.same_protocol(&Transport::TcpTrusted));
    assert!(Transport::TcpTrusted.same_protocol(&Transport::TcpTrusted));
    assert_eq!(Transport::default(), Transport::Tcp);
    assert_eq!(
        serde_json::to_value(Transport::Tcp).unwrap(),
        serde_json::json!({"kind": "tcp"}),
    );
    assert!(serde_json::from_value::<Transport>(serde_json::json!({"kind": "efa"})).is_err());
}

#[test]
fn removed_efa_backends_are_rejected_and_their_mode_ids_stay_retired() {
    let ofi = |backend: &str| {
        serde_json::from_value::<Transport>(serde_json::json!({
            "kind": "ofi",
            "fabric": {"backend": backend},
            "security": "mutual-tls",
        }))
    };
    assert!(ofi("verbs").is_ok());
    assert!(ofi("efa").is_err());
    assert!(ofi("efa-direct").is_err());
    let mut ids = vec![
        transport_mode_id(&Transport::Tcp),
        transport_mode_id(&Transport::TcpTrusted),
    ];
    for backend in [FabricBackend::Verbs, FabricBackend::SoftwareTcp] {
        for security in [FabricSecurity::MutualTls, FabricSecurity::Trusted] {
            ids.push(transport_mode_id(&Transport::ofi(backend, security)));
        }
    }
    ids.sort_unstable();
    assert_eq!(ids, [1, 6, 7, 8, 9, 10]);
}
fn shape() -> KeyShape {
    KeyShape::new_single(4, 1, KeyType::uniform(1))
}
pub(super) fn metrics() -> Arc<Metrics> {
    Metrics::new_in_enabled(&prometheus::Registry::new(), false)
}
pub(super) fn config(root: &Path) -> NodeConfig {
    NodeConfig {
        root: root.to_path_buf(),
        shape_yaml: shape().to_yaml().unwrap(),
        partition: PartitionConfig {
            inventory: StaticInventory {
                database: [51; 16],
                epoch: 1,
                segment_generation: 1,
                partitions: vec![2, 5],
            },
            partition: 2,
            batch_limits: BatchLimits {
                max_operations: 8,
                max_encoded_bytes: 2048,
            },
            group_limits: GroupLimits {
                max_batches: 8,
                max_encoded_bytes: 4088,
            },
            fragment_bytes: 4096,
            wal_file_bytes: 8192,
            max_batches: 64,
            max_reference_metadata_bytes: 1024 * 1024,
        },
        wal_member_read_policy: Default::default(),
        // Shared fixture keeps fallocated, unwritten WAL extents; prewrite tests opt in.
        wal_prewrite: false,
        max_device_reads_per_status: default_device_reads(),
        live_target: None,
    }
}
#[test]
fn static_node_validation_opens_no_runtime_resources() {
    let temp = tempdir::TempDir::new("static-node-config").unwrap();
    let mut config = config(&temp.path().join("absent-node-root"));
    assert!(config.validate().is_ok());
    for fault in 0..5 {
        let mut invalid = config.clone();
        match fault {
            0 => invalid.max_device_reads_per_status = 0,
            1 => invalid.partition.inventory.epoch = 2,
            2 => invalid.partition.inventory.segment_generation = 2,
            3 => invalid.max_device_reads_per_status = 4097,
            _ => invalid.shape_yaml = "invalid shape".into(),
        }
        assert!(invalid.validate().is_err());
    }
    config.live_target = Some(crate::distributed::nvmeof::LiveTargetConfig {
        address: "127.0.0.1".into(),
        service: 4420,
        transport: crate::distributed::nvmeof::NvmeofTransport::Tcp,
        host_nqn: "nqn.2026-01.test:static".into(),
        port_id: 42,
        state_dir: temp.path().join("absent-target-state"),
        commissioning_port_id: 43,
        commissioning_service: 4421,
    });
    assert!(config.validate().is_ok());
    config.live_target.as_mut().unwrap().commissioning_port_id = 42;
    assert!(config.validate().is_err());
    assert!(!config.root.exists());
    assert!(!config.live_target.unwrap().state_dir.exists());
}

#[test]
fn owner_launch_target_rejects_removed_export_keys() {
    let temp = tempdir::TempDir::new("old-live-launch").unwrap();
    let config = config(&temp.path().join("node"));
    let mut launch = serde_json::to_value(&config).unwrap();
    launch["live_target"] = serde_json::json!({
        "address": "127.0.0.1", "service": 4420, "transport": "tcp",
        "host_nqn": "nqn.2026-01.test:host", "port_id": 42,
        "state_dir": temp.path().join("target"), "commissioning_port_id": 43,
        "commissioning_service": 4421
    });
    // A launch file without the removed keys keeps parsing (per-owner export).
    let serialized = serde_yaml::to_string(&launch).unwrap();
    let restored: NodeConfig = serde_yaml::from_str(&serialized).unwrap();
    assert!(restored.live_target.is_some());
    for (key, value) in [
        ("export_mode", serde_json::json!("on_demand")),
        ("spdk", serde_json::Value::Null),
    ] {
        let mut removed = launch.clone();
        removed["live_target"][key] = value;
        let serialized = serde_yaml::to_string(&removed).unwrap();
        assert!(
            serde_yaml::from_str::<NodeConfig>(&serialized).is_err(),
            "{key}"
        );
    }
}

/// Compiled and run only off Linux; on this Linux workstation it is checked by
/// `cargo check --tests --target x86_64-apple-darwin` and never executed.
#[cfg(not(target_os = "linux"))]
#[test]
fn owner_open_off_linux_fails_with_named_error() {
    let temp = tempdir::TempDir::new("owner-open-off-linux").unwrap();
    let config = config(&temp.path().join("node"));
    let root = config.root.clone();
    let Err(error) = StorageNode::open(config, metrics()) else {
        panic!("owner open succeeded off Linux");
    };
    let expected = format!(
        "distributed route requires Linux (leased NVMe-oF): {}",
        std::env::consts::OS
    );
    assert!(error.to_string().contains(&expected), "{error}");
    assert!(!root.exists(), "the refused open must not create its root");
}

#[test]
fn native_node_has_no_live_target_shutdown_evidence() {
    let temp = tempdir::TempDir::new("native-node-final").unwrap();
    let node = StorageNode::open(config(&temp.path().join("node")), metrics()).unwrap();
    assert!(node.finish_live_target_shutdown().unwrap().is_none());
}
pub(super) fn batch(config: &NodeConfig, epoch: u64, sequence: u64) -> CompleteBatch {
    let id = (sequence * 1024..)
        .map(|n| {
            let mut operation = [0; 16];
            operation[..8].copy_from_slice(&n.to_be_bytes());
            operation[8..].copy_from_slice(&epoch.to_be_bytes());
            BatchId {
                database: config.partition.inventory.database,
                operation,
            }
        })
        .find(|id| {
            config.partition.inventory.partition_for(*id).unwrap() == config.partition.partition
        })
        .unwrap();
    CompleteBatch::new(
        &shape(),
        id,
        BatchVersion { epoch, sequence },
        vec![Mutation::Put {
            keyspace: KeySpace(0),
            key: b"key1"[..].into(),
            value: vec![sequence as u8; 32].into(),
        }],
        config.partition.batch_limits,
    )
    .unwrap()
}
#[test]
fn node_restart_seals_and_never_reactivates_old_epoch() {
    let temp = tempdir::TempDir::new("tcp-node-restart").unwrap();
    let config = config(&temp.path().join("node"));
    let node = StorageNode::open(config.clone(), metrics()).unwrap();
    assert_eq!(node.status().unwrap().active_epoch, None);
    node.inventory.lock().fence_to(7).unwrap();
    assert_eq!(
        node.inventory.lock().fence_to(7).unwrap().active_epoch,
        Some(7)
    );
    drop(node);
    let node = StorageNode::open(config.clone(), metrics()).unwrap();
    let status = node.status().unwrap();
    assert_eq!(status.highest_fenced_generation, 7);
    assert_eq!(status.active_epoch, None);
    assert!(node.inventory.lock().fence_to(7).is_err());
    node.inventory.lock().fence_to(9).unwrap();
    assert_eq!(node.status().unwrap().active_epoch, Some(9));
}

#[test]
fn absent_registered_stream_and_missing_inventory_fail_closed() {
    let temp = tempdir::TempDir::new("tcp-node-missing").unwrap();
    let config = config(&temp.path().join("node"));
    let node = StorageNode::open(config.clone(), metrics()).unwrap();
    node.inventory.lock().fence_to(1).unwrap();
    drop(node);
    let epoch = config.root.join("epoch_0000000000000001");
    let removed = config.root.join("held");
    fs::rename(&epoch, &removed).unwrap();
    assert!(StorageNode::open(config.clone(), metrics()).is_err());
    fs::rename(removed, epoch).unwrap();
    fs::remove_file(config.root.join("tcp-node-inventory-v1")).unwrap();
    assert!(StorageNode::open(config, metrics()).is_err());
}

#[test]
fn wire_frame_rejects_oversize_truncation_and_trailing_bytes() {
    assert!(read_frame::<Request>(&mut &u32::MAX.to_be_bytes()[..], 1024).is_err());
    let mut bytes = Vec::new();
    write_frame(&mut bytes, &Request::Stats, 1024).unwrap();
    assert!(matches!(
        read_frame::<Request>(&mut bytes.as_slice(), 1024).unwrap(),
        Request::Stats
    ));
    bytes.pop();
    assert!(read_frame::<Request>(&mut bytes.as_slice(), 1024).is_err());
    let mut encoded = wire_codec(1024).serialize(&Request::Stats).unwrap();
    encoded.push(0);
    assert!(wire_codec(1024).deserialize::<Request>(&encoded).is_err());
}

#[test]
fn correlated_envelopes_roundtrip_and_reject_the_uncorrelated_v1_schema() {
    use reactor::Envelope;
    let request = Envelope {
        protocol: PROTOCOL,
        id: 19,
        body: Request::Stats,
    };
    let encoded = wire_codec(HARD_FRAME_LIMIT).serialize(&request).unwrap();
    let decoded: Envelope<Request> = wire_codec(HARD_FRAME_LIMIT).deserialize(&encoded).unwrap();
    assert_eq!((decoded.protocol, decoded.id), (PROTOCOL, 19));
    assert!(matches!(decoded.body, Request::Stats));
    // V1's uncorrelated schema is not a supported compatibility path.
    let old = wire_codec(1024)
        .serialize(&(2u32, 7u64, 1u64, 1_000_000u64))
        .unwrap();
    assert!(
        wire_codec(1024)
            .deserialize::<Envelope<Request>>(&old)
            .is_err()
    );
}

#[test]
fn frame_header_and_payload_use_one_application_write() {
    #[derive(Default)]
    struct Output {
        bytes: Vec<u8>,
        writes: usize,
    }
    impl Write for Output {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.writes += 1;
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut output = Output::default();
    write_frame(&mut output, &Request::Stats, 1024).unwrap();
    assert_eq!(output.writes, 1);
    assert!(matches!(
        read_frame::<Request>(&mut output.bytes.as_slice(), 1024).unwrap(),
        Request::Stats
    ));
}

#[test]
fn frame_deadline_is_not_renewed_by_trickling_bytes() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let (start, started) = std::sync::mpsc::sync_channel(1);
    let sender = thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        socket.write_all(b"a").unwrap();
        started.recv().unwrap();
        thread::sleep(Duration::from_millis(80));
        let _ = socket.write_all(b"b");
        thread::sleep(Duration::from_millis(80));
        let _ = socket.write_all(b"c");
    });
    let socket = TcpStream::connect(address).unwrap();
    let mut stream = DeadlineIo::new(socket, Duration::from_millis(100)).unwrap();
    stream.read_exact(&mut [0; 1]).unwrap();
    start.send(()).unwrap();
    let error = stream.read_exact(&mut [0; 2]).unwrap_err();
    assert!(matches!(
        error.kind(),
        io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
    ));
    sender.join().unwrap();
}

fn openssl(root: &Path, args: &[&str]) {
    let output = Command::new("openssl")
        .current_dir(root)
        .args(args)
        .output()
        .expect("local TLS protocol tests require openssl (no network/download)");
    assert!(
        output.status.success(),
        "openssl: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}
pub(super) fn certificates(root: &Path) -> TlsFiles {
    openssl(
        root,
        &[
            "req",
            "-x509",
            "-newkey",
            "ed25519",
            "-nodes",
            "-keyout",
            "ca.pem",
            "-out",
            "ca.der",
            "-outform",
            "DER",
            "-subj",
            "/CN=Tidehunter protocol test CA",
            "-days",
            "1",
        ],
    );
    openssl(
        root,
        &["genpkey", "-algorithm", "ed25519", "-out", "leaf.pem"],
    );
    openssl(
        root,
        &[
            "req",
            "-new",
            "-key",
            "leaf.pem",
            "-out",
            "leaf.csr",
            "-subj",
            "/CN=localhost",
        ],
    );
    fs::write(root.join("extensions"),"basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature\nextendedKeyUsage=serverAuth,clientAuth\nsubjectAltName=DNS:localhost\n").unwrap();
    openssl(
        root,
        &[
            "x509",
            "-req",
            "-in",
            "leaf.csr",
            "-CA",
            "ca.der",
            "-CAform",
            "DER",
            "-CAkey",
            "ca.pem",
            "-CAcreateserial",
            "-out",
            "leaf.der",
            "-outform",
            "DER",
            "-days",
            "1",
            "-extfile",
            "extensions",
        ],
    );
    openssl(
        root,
        &[
            "pkcs8", "-topk8", "-nocrypt", "-in", "leaf.pem", "-outform", "DER", "-out", "key.der",
        ],
    );
    TlsFiles {
        transport: Default::default(),
        ca: root.join("ca.der"),
        cert: root.join("leaf.der"),
        key: root.join("key.der"),
        server_name: "localhost".into(),
    }
}
#[test]
fn mtls_roundtrip_authentication_and_fencing() {
    authenticated_roundtrip(Transport::Tcp);
}

#[cfg(feature = "experimental-fabric")]
#[test]
#[ignore = "requires libfabric tcp;ofi_rxm software provider"]
fn fabric_mtls_authentication_and_fencing() {
    authenticated_roundtrip(Transport::ofi(
        FabricBackend::SoftwareTcp,
        FabricSecurity::MutualTls,
    ));
}

fn authenticated_roundtrip(transport: Transport) {
    let temp = tempdir::TempDir::new("tcp-mtls").unwrap();
    let mut tls = certificates(temp.path());
    tls.transport = transport.clone();
    let config = config(&temp.path().join("node"));
    let node = StorageNode::open(config.clone(), metrics()).unwrap();
    node.set_durability(crate::distributed::durability::DurabilityConfig::with_tier(
        crate::distributed::durability::DurabilityTier::Synchronous,
    ))
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = listener.local_addr().unwrap().to_string();
    let shutdown = Arc::new(AtomicBool::new(false));
    let stop = shutdown.clone();
    let server = node.clone();
    let server_tls = tls.clone();
    let limits = TcpLimits {
        max_frame_bytes: 8192,
        ..TcpLimits::default()
    };
    // This checks authentication and durable recovery, not sub-second storage
    // latency while other tests and compilers share the same disk.
    let timeout = limits.io_timeout;
    let worker = thread::spawn(move || server.serve(listener, server_tls, limits, stop).unwrap());
    let (roots, _, _, _) = tls.material().unwrap();
    let anonymous =
        ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth();
    let socket = TcpStream::connect(&endpoint).unwrap();
    socket.set_read_timeout(Some(timeout)).unwrap();
    socket.set_write_timeout(Some(timeout)).unwrap();
    let connection = ClientConnection::new(
        Arc::new(anonymous),
        ServerName::try_from("localhost").unwrap(),
    )
    .unwrap();
    let mut anonymous = StreamOwned::new(connection, socket);
    let anonymous_result = write_frame(
        &mut anonymous,
        &Request::Status {
            protocol: PROTOCOL,
            database: None,
        },
        1024,
    )
    .and_then(|()| read_frame::<Response>(&mut anonymous, 4096));
    assert!(anonymous_result.is_err());
    drop(anonymous);
    let mut wrong_partition = config.partition.clone();
    wrong_partition.partition = 5;
    assert!(Client::connect(&endpoint, &tls, timeout, wrong_partition, &shape()).is_err());
    let mut wrong_name = tls.clone();
    wrong_name.server_name = "wrong.invalid".into();
    assert!(
        Client::connect(
            &endpoint,
            &wrong_name,
            timeout,
            config.partition.clone(),
            &shape()
        )
        .is_err()
    );
    let alien = tempdir::TempDir::new("tcp-alien-ca").unwrap();
    let mut alien_tls = certificates(alien.path());
    alien_tls.transport = transport.clone();
    alien_tls.ca = tls.ca.clone(); // Trust server, but client certificate is from an untrusted CA.
    assert!(
        Client::connect(
            &endpoint,
            &alien_tls,
            timeout,
            config.partition.clone(),
            &shape()
        )
        .is_err()
    );
    let mut client =
        Client::connect(&endpoint, &tls, timeout, config.partition.clone(), &shape()).unwrap();
    #[cfg(feature = "experimental-fabric")]
    assert_eq!(
        client.stream.sock.fabric.is_some(),
        transport != Transport::Tcp
    );
    if transport.is_fabric() {
        let mut fallback = tls.clone();
        fallback.transport = Transport::Tcp;
        assert!(
            Client::connect(
                &endpoint,
                &fallback,
                timeout,
                config.partition.clone(),
                &shape(),
            )
            .is_err()
        );
    }
    client.fence_to(1).unwrap();
    assert_eq!(client.connection_profile(true, true).unwrap().requests, 0);
    assert_eq!(client.status().unwrap().active_epoch, Some(1));
    #[cfg(all(target_os = "linux", feature = "test-utils"))]
    {
        client.connection_profile(true, true).unwrap();
        assert!(client.process_resources_for_benchmark(0).is_err());
        let resources = client.process_resources_for_benchmark(1).unwrap();
        assert_eq!(resources.pid, std::process::id()); // This server runs in-process.
        assert!(resources.rss_bytes > 0 && resources.peak_rss_bytes >= resources.rss_bytes);
        let controls = client.connection_profile(false, false).unwrap();
        assert_eq!(controls.requests, 0);
        assert_eq!(controls.client.requests, 0);
        assert!(client.read_evidence_for_benchmark(0, true).is_err());
        let evidence = client.read_evidence_for_benchmark(1, true).unwrap();
        assert!(evidence.file_bytes > 0 && evidence.resident_bytes <= evidence.file_bytes);
    }
    client.fence_to(2).unwrap();
    assert_eq!(client.status().unwrap().active_epoch, Some(2));
    client.seal(2).unwrap();
    assert!(client.fence_to(2).is_err());
    drop(client);
    shutdown.store(true, Ordering::Release);
    worker.join().unwrap();
}

#[test]
fn node_yaml_rejects_removed_cache_and_resident_settings() {
    let temp = tempdir::TempDir::new("node-memory-envelope").unwrap();
    let config = config(temp.path());
    let yaml = serde_yaml::to_string(&config).unwrap();
    serde_yaml::from_str::<NodeConfig>(&yaml).unwrap();
    for field in [
        "read_cache_bytes: 4096",
        "read_cache_metadata_bytes: 1024",
        // Owner read-executor settings removed with the owner-RPC read path.
        "read_workers: 2",
        "read_resident_first: false",
        "read_memory_allowance_bytes: 4096",
        "read_max_inflight_requests: 32",
        "read_max_inflight_bytes: 4096",
        "resident_reads: {}",
    ] {
        assert!(serde_yaml::from_str::<NodeConfig>(&format!("{yaml}\n{field}\n")).is_err());
    }
    let tls_yaml = serde_yaml::to_string(&TlsFiles {
        transport: Transport::Tcp,
        ca: PathBuf::new(),
        cert: PathBuf::new(),
        key: PathBuf::new(),
        server_name: String::new(),
    })
    .unwrap();
    assert!(
        serde_yaml::from_str::<TlsFiles>(&format!("{tls_yaml}\nresident_reads: {{}}\n")).is_err()
    );
}

#[test]
fn node_wal_prewrite_defaults_on_and_is_always_serialized() {
    let temp = tempdir::TempDir::new("node-wal-prewrite").unwrap();
    let config = config(temp.path());
    for enabled in [false, true] {
        let mut explicit = config.clone();
        explicit.wal_prewrite = enabled;
        explicit.validate().unwrap();
        let value = serde_yaml::to_value(&explicit).unwrap();
        // Both values are written, so no reader relies on the default.
        assert_eq!(value["wal_prewrite"].as_bool(), Some(enabled));
        let restored: NodeConfig = serde_yaml::from_value(value).unwrap();
        assert_eq!(restored.wal_prewrite, enabled);
    }
    let mut value = serde_yaml::to_value(&config).unwrap();
    value.as_mapping_mut().unwrap().remove("wal_prewrite");
    let restored: NodeConfig = serde_yaml::from_value(value).unwrap();
    assert!(
        restored.wal_prewrite,
        "an absent key is the prewrite default"
    );
}
