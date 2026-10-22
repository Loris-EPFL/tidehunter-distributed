use super::*;
use crate::distributed::codec::BatchLimits;
use crate::distributed::group::GroupLimits;
use crate::distributed::storage::StaticInventory;
use crate::key_shape::{KeySpace, KeyType};
use std::path::Path;
use std::process::Command;

#[path = "reactor_tests.rs"]
pub(in crate::distributed::tcp) mod multiplexed;
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
        max_epochs: 8,
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
        read_workers: 2,
        wal_member_read: Default::default(),
        read_resident_first: false,
        read_cache_bytes: 0,
        read_memory_allowance_bytes: default_read_memory_allowance_bytes(),
        read_max_inflight_requests: default_read_requests(),
        read_max_inflight_bytes: default_read_bytes(),
        read_max_device_reads: default_device_reads(),
    }
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
fn append(node: &StorageNode, epoch: u64, batches: &[CompleteBatch]) -> Vec<Receipt> {
    match node
        .handle(
            Request::Append {
                epoch,
                batches: batches.iter().map(|b| b.encoded().clone().into()).collect(),
            },
            TcpLimits::default(),
        )
        .unwrap()
    {
        Response::Appended(receipts) => receipts,
        _ => panic!("wrong response"),
    }
}

#[test]
fn bounded_server_pool_overlaps_members_preserves_order_and_rechecks_fences() {
    let temp = tempdir::TempDir::new("parallel-member-read").unwrap();
    let config = config(&temp.path().join("node"));
    let node = StorageNode::open(config.clone(), metrics()).unwrap();
    node.inventory.lock().fence_to(1).unwrap();
    let batches = vec![batch(&config, 1, 0), batch(&config, 1, 1)];
    let receipts = append(&node, 1, &batches);
    let references: Vec<_> = batches
        .iter()
        .zip(receipts)
        .map(|(batch, receipt)| batch.record_reference(receipt.address, 0).unwrap())
        .collect();
    for fenced in [false, true] {
        let (entered, entries) = std::sync::mpsc::sync_channel(2);
        let (release, released) = std::sync::mpsc::sync_channel(1);
        let released = Mutex::new(released);
        *node.read_hook.lock() = Some(Arc::new(move |reference| {
            entered.send(reference.batch_version.sequence).unwrap();
            if reference.batch_version.sequence == 0 {
                released
                    .lock()
                    .recv_timeout(Duration::from_secs(5))
                    .unwrap();
            }
        }));
        let limits = TcpLimits {
            max_frame_bytes: 16,
            ..TcpLimits::default()
        };
        assert!(
            node.read_response::<false>(1, references.clone(), limits)
                .is_err()
        );
        assert!(matches!(
            entries.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        ));
        let mut forged = references.clone();
        forged[1].descriptor.offset += 1;
        assert!(
            node.read_response::<false>(1, forged, TcpLimits::default())
                .is_err()
        );
        assert!(matches!(
            entries.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        ));
        let reading = node.clone();
        let requested = references.clone();
        let worker = thread::spawn(move || {
            reading.read_response::<false>(1, requested, TcpLimits::default())
        });
        let mut seen = vec![
            entries.recv_timeout(Duration::from_secs(5)).unwrap(),
            entries.recv_timeout(Duration::from_secs(5)).unwrap(),
        ];
        seen.sort_unstable();
        assert_eq!(seen, vec![0, 1]); // second member enters while first is blocked
        if fenced {
            node.inventory.lock().fence_to(2).unwrap();
        }
        release.send(()).unwrap();
        let result = worker.join().unwrap();
        if fenced {
            assert!(result.is_err());
        } else {
            let Response::Records(records) = result.unwrap() else {
                panic!("wrong response")
            };
            for ((record, reference), batch) in records.into_iter().zip(&references).zip(&batches) {
                assert_eq!(
                    CompleteBatch::decode_record(&shape(), reference, record.into_bytes()).unwrap(),
                    batch.mutations()[0]
                );
            }
        }
        node.read_hook.lock().take();
    }
}
#[test]
fn reads_progress_while_durable_append_holds_inventory() {
    let temp = tempdir::TempDir::new("read-during-durable-append").unwrap();
    let config = config(&temp.path().join("node"));
    let node = StorageNode::open(config.clone(), metrics()).unwrap();
    node.inventory.lock().fence_to(1).unwrap();
    let first = batch(&config, 1, 0);
    let receipt = append(&node, 1, std::slice::from_ref(&first)).remove(0);
    let reference = first.record_reference(receipt.address, 0).unwrap();
    let log = node.inventory.lock().active(1).unwrap();
    let (entered, release) = log.pause_next_persist();
    let writing = node.clone();
    let second = batch(&config, 1, 1);
    let unpersisted = second.clone();
    let writer = thread::spawn(move || append(&writing, 1, &[second]));
    entered
        .recv_timeout(Duration::from_secs(5))
        .expect("append did not reach the durable-persist pause");
    let pending = log.lookup(unpersisted.id()).unwrap().unwrap();
    let pending_reference = unpersisted.record_reference(pending.address(), 0).unwrap();
    assert!(
        log.validate_durable_record_reference(&pending_reference)
            .is_err()
    );
    assert!(
        node.read_response::<false>(1, vec![pending_reference], TcpLimits::default())
            .is_err()
    );

    let result = node
        .read_response::<false>(1, vec![reference], TcpLimits::default())
        .expect("unrelated committed read waited for append ownership");
    let Response::Records(records) = result else {
        panic!("wrong read response");
    };
    assert_eq!(
        CompleteBatch::decode_record(&shape(), &reference, records[0].clone().into_bytes())
            .unwrap(),
        first.mutations()[0]
    );

    release.send(()).unwrap();
    assert_eq!(writer.join().unwrap().len(), 1);
}
#[test]
fn unknown_append_io_and_panic_fence_reader_authority() {
    #[derive(Clone, Copy)]
    enum Failure {
        FileSync,
        NamespaceSync,
        PanicAfterAppend,
    }

    for failure in [
        Failure::FileSync,
        Failure::NamespaceSync,
        Failure::PanicAfterAppend,
    ] {
        let temp = tempdir::TempDir::new("append-fail-stop").unwrap();
        let config = config(&temp.path().join("node"));
        let node = StorageNode::open(config.clone(), metrics()).unwrap();
        node.inventory.lock().fence_to(1).unwrap();
        let first = batch(&config, 1, 0);
        let receipt = append(&node, 1, std::slice::from_ref(&first)).remove(0);
        let reference = first.record_reference(receipt.address, 0).unwrap();
        let second = batch(&config, 1, 1);
        let log = node.inventory.lock().active(1).unwrap();
        match failure {
            Failure::FileSync => log.fail_next_persist(),
            Failure::NamespaceSync => log.fail_next_namespace_persist(),
            Failure::PanicAfterAppend => node.inject_panic_after_next_append(),
        }
        let request = Request::Append {
            epoch: 1,
            batches: vec![second.encoded().clone().into()],
        };
        assert!(
            node.handle_caught(request, TcpLimits::default(), None)
                .is_err()
        );
        let unpersisted = log.lookup(second.id()).unwrap().unwrap();
        let unpersisted_reference = second.record_reference(unpersisted.address(), 0).unwrap();
        assert!(matches!(
            log.validate_durable_record_reference(&unpersisted_reference),
            Err(StorageError::NeedsPersistence)
        ));
        assert!(node.reader_snapshot(1, &[]).is_err());
        assert!(
            node.read_response::<false>(1, vec![reference], TcpLimits::default())
                .is_err()
        );
    }
}

#[test]
fn node_restart_seals_replays_and_never_reactivates_old_epoch() {
    let temp = tempdir::TempDir::new("tcp-node-restart").unwrap();
    let config = config(&temp.path().join("node"));
    let node = StorageNode::open(config.clone(), metrics()).unwrap();
    assert_eq!(node.status().unwrap().active_epoch, None);
    node.inventory.lock().fence_to(7).unwrap();
    assert_eq!(
        node.inventory.lock().fence_to(7).unwrap().active_epoch,
        Some(7)
    );
    let batches = vec![batch(&config, 7, 0), batch(&config, 7, 4)];
    let receipts = append(&node, 7, &batches);
    assert_eq!(
        node.inventory.lock().stats().unwrap().persistence_barriers,
        1
    );
    drop(node);
    let node = StorageNode::open(config.clone(), metrics()).unwrap();
    assert_eq!(
        node.status().unwrap().epochs,
        vec![NodeEpoch {
            epoch: 7,
            sealed: true
        }]
    );
    assert!(node.inventory.lock().fence_to(7).is_err());
    node.inventory.lock().fence_to(9).unwrap();
    assert!(
        node.handle(
            Request::Append {
                epoch: 7,
                batches: vec![batches[0].encoded().clone().into()]
            },
            TcpLimits::default()
        )
        .is_err()
    );
    let page = node
        .handle(
            Request::Recover {
                epoch: 7,
                cursor: 0,
                limit: 1,
            },
            TcpLimits::default(),
        )
        .unwrap();
    assert!(matches!(page, Response::Recovery { next: Some(1), .. }));
    let reference = batches[0].record_reference(receipts[0].address, 0).unwrap();
    assert!(
        node.handle(
            Request::Read {
                class: ReadClass::Point,
                epoch: 7,
                references: vec![reference]
            },
            TcpLimits::default()
        )
        .is_err()
    );
    match node
        .handle(
            Request::Read {
                class: ReadClass::Point,
                epoch: 9,
                references: vec![reference],
            },
            TcpLimits::default(),
        )
        .unwrap()
    {
        Response::Records(bytes) => assert_eq!(
            CompleteBatch::decode_record(&shape(), &reference, bytes[0].clone().into_bytes())
                .unwrap(),
            batches[0].mutations()[0]
        ),
        _ => panic!("wrong response"),
    };
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
fn profiled_reads_preserve_authority_membership_and_frame_bounds() {
    let temp = tempdir::TempDir::new("tcp-profiled-read").unwrap();
    let config = config(&temp.path().join("node"));
    let node = StorageNode::open(config.clone(), metrics()).unwrap();
    node.inventory.lock().fence_to(1).unwrap();
    let batch = batch(&config, 1, 0);
    let receipt = append(&node, 1, std::slice::from_ref(&batch))[0];
    let reference = batch.record_reference(receipt.address, 0).unwrap();
    let request = |epoch, references| Request::ReadProfiled {
        epoch,
        references,
        class: ReadClass::Point,
    };
    let limits = TcpLimits::default();
    let response = node.handle(request(1, vec![reference]), limits).unwrap();
    match response {
        Response::RecordsProfiled { records, profile } => {
            assert_eq!(records.len(), 1);
            assert_eq!(
                CompleteBatch::decode_record(&shape(), &reference, records[0].clone().into_bytes())
                    .unwrap(),
                batch.mutations()[0]
            );
            assert!(profile.handler_ns > 0 && profile.record_read_ns > 0);
            assert!(
                profile.handler_ns
                    >= profile.inventory_wait_ns
                        + profile.authority_lookup_ns
                        + profile.record_read_ns
            );
            assert_eq!(
                wire_codec(HARD_FRAME_LIMIT)
                    .serialized_size(&profile)
                    .unwrap() as usize,
                SERVER_READ_PROFILE_BYTES
            );
        }
        _ => panic!("missing server profile"),
    }
    assert!(node.handle(request(1, vec![]), limits).is_err());
    assert!(
        node.handle(request(1, vec![reference; limits.max_reads + 1]), limits)
            .is_err()
    );
    assert!(node.handle(request(2, vec![reference]), limits).is_err());
    let mut forged = reference;
    forged.descriptor.checksum.0 ^= 1;
    assert!(node.handle(request(1, vec![forged]), limits).is_err());
    let mut outside = reference;
    outside.descriptor.len = u32::MAX;
    assert!(node.handle(request(1, vec![outside]), limits).is_err());

    // A response budget that admits a normal record must still reserve the
    // diagnostic payload. Both variants retain their ordinary frame limits.
    let small = TcpLimits {
        max_frame_bytes: 16 + 8 + reference.descriptor.len as usize,
        ..limits
    };
    assert!(matches!(
        node.handle(
            Request::Read {
                class: ReadClass::Point,
                epoch: 1,
                references: vec![reference]
            },
            small
        )
        .unwrap(),
        Response::Records(_)
    ));
    assert!(node.handle(request(1, vec![reference]), small).is_err());
    node.inventory.lock().fence_to(2).unwrap();
    assert!(node.handle(request(1, vec![reference]), limits).is_err());
    assert!(node.handle(request(2, vec![reference]), limits).is_ok());
}

#[test]
fn bulk_byte_buffers_roundtrip_correlated_envelopes() {
    use reactor::Envelope;
    for length in [0, 1, 255, 8192] {
        let values = vec![vec![17u8; length], vec![231u8; length]];
        let request = Envelope {
            protocol: PROTOCOL,
            id: 19,
            body: Request::Append {
                epoch: 7,
                batches: values.iter().cloned().map(Into::into).collect(),
            },
        };
        let encoded = wire_codec(HARD_FRAME_LIMIT).serialize(&request).unwrap();
        let decoded: Envelope<Request> =
            wire_codec(HARD_FRAME_LIMIT).deserialize(&encoded).unwrap();
        assert_eq!(decoded.id, 19);
        let Request::Append { epoch, batches } = decoded.body else {
            panic!("wrong request")
        };
        assert_eq!(epoch, 7);
        assert!(batches.iter().zip(&values).all(|(a, b)| a.as_slice() == b));
        let response = Envelope {
            protocol: PROTOCOL,
            id: 19,
            body: Response::Records(values.iter().cloned().map(Into::into).collect()),
        };
        let encoded = wire_codec(HARD_FRAME_LIMIT).serialize(&response).unwrap();
        let decoded: Envelope<Response> =
            wire_codec(HARD_FRAME_LIMIT).deserialize(&encoded).unwrap();
        let Response::Records(records) = decoded.body else {
            panic!("wrong response")
        };
        assert!(records.iter().zip(&values).all(|(a, b)| a.as_slice() == b));
    }
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
fn mtls_roundtrip_authentication_lost_response_and_recovery() {
    authenticated_roundtrip(Transport::Tcp);
}

#[cfg(feature = "experimental-fabric")]
#[test]
#[ignore = "requires libfabric tcp;ofi_rxm software provider"]
fn fabric_mtls_authentication_lost_response_and_recovery() {
    authenticated_roundtrip(Transport::FabricTcp);
}

fn authenticated_roundtrip(transport: Transport) {
    let temp = tempdir::TempDir::new("tcp-mtls").unwrap();
    let mut tls = certificates(temp.path());
    tls.transport = transport;
    let config = config(&temp.path().join("node"));
    let node = StorageNode::open(config.clone(), metrics()).unwrap();
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
        &Request::Status { protocol: PROTOCOL },
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
    alien_tls.transport = transport;
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
    if transport != Transport::Tcp {
        let mut fallback = tls.clone();
        fallback.transport = Transport::Tcp;
        let fallback = Client::connect(
            &endpoint,
            &fallback,
            timeout,
            config.partition.clone(),
            &shape(),
        )
        .unwrap();
        #[cfg(feature = "experimental-fabric")]
        assert!(fallback.stream.sock.fabric.is_none());
        drop(fallback);
    }
    client.fence_to(1).unwrap();
    assert_eq!(client.connection_profile(true, true).unwrap().requests, 0);
    let batches = vec![batch(&config, 1, 0), batch(&config, 1, 2)];
    let receipts = client.append_many(1, &batches).unwrap();
    let append_socket = client.stream.sock.profile.unwrap_or_default();
    let append_profile = client.connection_profile(true, false).unwrap();
    assert_eq!(append_profile.client.requests, 1);
    assert_eq!(
        append_profile.client.socket.read_calls,
        append_socket.read_calls
    );
    assert_eq!(
        append_profile.client.socket.write_calls,
        append_socket.write_calls
    );
    let reference = batches[0]
        .record_reference(receipts[0].appended().address(), 0)
        .unwrap();
    assert_eq!(
        client.read_records(1, &[reference, reference]).unwrap(),
        vec![batches[0].mutations()[0].clone(); 2]
    );
    let (profiled, profile) = client.read_record_profiled(1, &reference).unwrap();
    assert_eq!(profiled, batches[0].mutations()[0]);
    assert_eq!(profile.requests, 1);
    assert!(
        profile.request_encode_ns > 0
            && profile.exchange_ns > 0
            && profile.response_decode_ns > 0
            && profile.record_verify_ns > 0
    );
    assert!(profile.server_handler_ns > 0 && profile.server_record_read_ns > 0);
    assert!(
        profile.server_handler_ns
            >= profile.server_inventory_wait_ns
                + profile.server_authority_lookup_ns
                + profile.server_record_read_ns
    );
    assert!(profile.exchange_ns >= profile.server_handler_ns);
    // Interleaved TLS writes/reads are not attributed to one request.
    assert_eq!(profile.request_send_ns, 0);
    assert_eq!(profile.response_header_ns, 0);
    assert_eq!(profile.response_body_ns, 0);
    assert_eq!(profile.socket.read_calls, 0);
    assert_eq!(profile.socket.write_calls, 0);
    let stats = client.stats().unwrap();
    assert_eq!(stats.appended_groups, 1);
    assert_eq!(stats.persistence_barriers, 1);
    assert_eq!(stats.namespace_syncs, 1);
    assert_eq!(stats.inventory_witnesses, 1);
    let server = client.connection_profile(false, false).unwrap();
    assert_eq!(server.requests, 3); // append, grouped read, profiled read
    assert_eq!(server.client.requests, 3);
    assert!(server.client.request_encode_ns > 0 && server.client.exchange_ns > 0);
    assert!(server.client.response_decode_ns > 0 && server.client.socket.read_calls > 0);
    assert!(
        server.request_decode_ns > 0
            && server.response_encode_ns > 0
            && server.response_send_ns > 0
    );
    assert!(
        server.append_decode_ns > 0
            && server.append_group_build_ns > 0
            && server.append_wal_ns > 0
            && server.append_persist_ns > 0
    );
    assert_eq!(server.storage_write.append_calls, 1);
    assert_eq!(server.storage_write.append_errors, 0);
    assert_eq!(server.storage_write.file_persist_calls, 1);
    assert_eq!(server.storage_write.file_persist_successes, 1);
    assert_eq!(server.storage_write.namespace_persist_calls, 1);
    assert_eq!(server.storage_write.namespace_persist_successes, 1);
    assert_eq!(server.storage_write.durable_publications, 1);
    assert!(server.socket.read_calls > 0 && server.socket.write_calls > 0);
    assert_eq!(client.connection_profile(false, true).unwrap().requests, 0);
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
        assert_eq!(
            client.read_record(1, &reference).unwrap(),
            batches[0].mutations()[0]
        );
    }
    // More request metadata than one negotiated frame: client splits bounded
    // requests and preserves the caller's record order.
    assert_eq!(
        client.read_records(1, &[reference; 256]).unwrap(),
        vec![batches[0].mutations()[0].clone(); 256]
    );
    let (profiled, profile) = client.read_records_profiled(1, &[reference; 256]).unwrap();
    assert_eq!(profiled, vec![batches[0].mutations()[0].clone(); 256]);
    assert!(profile.requests > 1);
    assert!(profile.server_handler_ns > 0 && profile.server_record_read_ns > 0);
    let changed = CompleteBatch::new(
        &shape(),
        batches[0].id(),
        batches[0].version(),
        vec![Mutation::Put {
            keyspace: KeySpace(0),
            key: b"key1"[..].into(),
            value: b"changed"[..].into(),
        }],
        config.partition.batch_limits,
    )
    .unwrap();
    assert!(client.append_many(1, &[changed]).is_err());
    assert_eq!(client.status().unwrap().active_epoch, Some(1));
    node.inject_drop_next_append_response();
    let uncertain = batch(&config, 1, 3);
    assert!(
        client
            .append_many(1, std::slice::from_ref(&uncertain))
            .is_err()
    );
    assert!(client.status().is_err());
    let mut client =
        Client::connect(&endpoint, &tls, timeout, config.partition.clone(), &shape()).unwrap();
    // Exact retry on a fresh authenticated connection discovers the same frame.
    let retry = client
        .append_many(1, std::slice::from_ref(&uncertain))
        .unwrap();
    assert_eq!(retry[0].appended().id(), uncertain.id());
    let stats = client.stats().unwrap();
    assert_eq!(stats.appended_groups, 2);
    assert_eq!(stats.persistence_barriers, 2);
    assert_eq!(stats.namespace_syncs, 1);
    assert_eq!(
        stats.inventory_witnesses, 1,
        "same-file groups and exact retries reuse the witness"
    );
    // The compact statistics endpoint carries the same protocol-8 witness count.
    assert!(matches!(
        client.request(Request::Stats).unwrap(),
        Response::Stats {
            groups: 2,
            barriers: 2,
            inventory_witnesses: 1,
            ..
        }
    ));
    client.fence_to(2).unwrap();
    let page = client.recovery_page(1, 0, 128).unwrap();
    assert_eq!(page.batches.len(), 3);
    assert_eq!(page.next, None);
    assert_eq!(
        client.read_record(2, &reference).unwrap(),
        batches[0].mutations()[0]
    );
    assert!(client.read_record(1, &reference).is_err());
    client.seal(2).unwrap();
    assert!(client.fence_to(2).is_err());
    drop(client);
    shutdown.store(true, Ordering::Release);
    worker.join().unwrap();
}

#[test]
fn read_admission_class_is_required_and_roundtrips() {
    for class in [ReadClass::Point, ReadClass::ScanNoFill] {
        let request = reactor::Envelope {
            protocol: PROTOCOL,
            id: 41,
            body: Request::Read {
                epoch: 7,
                references: Vec::new(),
                class,
            },
        };
        let bytes = wire_codec(HARD_FRAME_LIMIT).serialize(&request).unwrap();
        let decoded: reactor::Envelope<Request> =
            wire_codec(HARD_FRAME_LIMIT).deserialize(&bytes).unwrap();
        assert!(matches!(decoded.body, Request::Read { class: decoded, .. } if decoded == class));
        // v2 Read stopped after references; it cannot be silently treated as Point.
        assert!(
            wire_codec(HARD_FRAME_LIMIT)
                .deserialize::<reactor::Envelope<Request>>(&bytes[..bytes.len() - 4])
                .is_err()
        );
    }
    assert!(
        wire_codec(HARD_FRAME_LIMIT)
            .deserialize::<ReadClass>(&[2, 0, 0, 0])
            .is_err()
    );
}

#[test]
fn node_yaml_preserves_large_cache_and_explicit_read_allowance() {
    let temp = tempdir::TempDir::new("node-memory-envelope").unwrap();
    let mut config = config(temp.path());
    config.read_cache_bytes = 2 * 1024 * 1024 * 1024;
    config.read_memory_allowance_bytes = config.read_cache_bytes + config.read_max_inflight_bytes;
    let yaml = serde_yaml::to_string(&config).unwrap();
    let parsed: NodeConfig = serde_yaml::from_str(&yaml).unwrap();
    assert_eq!(parsed.read_cache_bytes, config.read_cache_bytes);
    assert_eq!(
        parsed.read_memory_allowance_bytes,
        config.read_memory_allowance_bytes
    );
    assert!(parsed.read_cache_metadata_bytes() < parsed.read_cache_bytes);
    // Validation alone avoids allocating a multi-GiB table in a unit test.
    assert!(
        read_executor::ReadExecutorConfig {
            cache_bytes: parsed.read_cache_bytes,
            memory_allowance_bytes: parsed.read_memory_allowance_bytes,
            max_bytes: parsed.read_max_inflight_bytes,
            max_requests: parsed.read_max_inflight_requests,
            max_device_reads: parsed.read_max_device_reads,
            resident_first: parsed.read_resident_first,
        }
        .validate()
        .is_ok()
    );
    config.read_memory_allowance_bytes -= 1;
    assert!(StorageNode::open(config, metrics()).is_err());
}

#[test]
fn append_residence_contains_stage_spans_on_success_and_rejection() {
    let temp = tempdir::TempDir::new("append-stage-profile").unwrap();
    let config = config(temp.path());
    let node = StorageNode::open(config.clone(), metrics()).unwrap();
    node.inventory.lock().fence_to(1).unwrap();
    let batch = batch(&config, 1, 0);
    let mut profile = ConnectionProfile::default();
    let response = node
        .handle_profiled(
            Request::Append {
                epoch: 1,
                batches: vec![batch.encoded().to_vec().into()],
            },
            TcpLimits::default(),
            Some(&mut profile),
        )
        .unwrap();
    assert!(matches!(response, Response::Appended(_)));
    assert!(
        profile.append_decode_ns > 0
            && profile.append_group_build_ns > 0
            && profile.append_wal_ns > 0
            && profile.append_persist_ns > 0
    );
    let mut rejected = ConnectionProfile::default();
    assert!(
        node.handle_profiled(
            Request::Append {
                epoch: 1,
                batches: vec![vec![0u8; 8].into()]
            },
            TcpLimits::default(),
            Some(&mut rejected),
        )
        .is_err()
    );
    assert!(rejected.append_decode_ns > 0);
    assert_eq!(rejected.append_wal_ns, 0);
    assert_eq!(rejected.append_persist_ns, 0);
    assert!(node.inventory.lock().healthy);
}
