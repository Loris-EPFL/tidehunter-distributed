use super::*;
use crate::distributed::storage::HistoryPersistence;

#[test]
fn reopened_legacy_owner_reports_stored_history_mode_before_selection() {
    let temp = tempdir::TempDir::new("tcp-stored-history-mode").unwrap();
    let config = config(&temp.path().join("node"));
    let node = StorageNode::open(config.clone(), metrics()).unwrap();
    node.set_history_persistence(HistoryPersistence::OnDisk)
        .unwrap();
    node.inventory.lock().fence_to(1).unwrap();
    append(&node, 1, &[batch(&config, 1, 0)]);
    drop(node);
    let node = StorageNode::open(config, metrics()).unwrap();
    assert_eq!(
        node.status().unwrap().history_persistence,
        HistoryPersistence::OnDisk
    );
    assert_eq!(
        node.set_history_persistence(HistoryPersistence::WalReplay)
            .unwrap_err()
            .to_string(),
        "Invalid(\"history persistence differs from the stored partition\")"
    );
    node.inventory.lock().fence_to(2).unwrap();
    assert_eq!(
        node.inventory
            .lock()
            .active(2)
            .unwrap()
            .history_persistence(),
        HistoryPersistence::OnDisk
    );
}

fn position_proof(
    node: &StorageNode,
    authority: u64,
    epoch: u64,
    through: u64,
) -> checkpoint::CheckpointPositionProof {
    match node
        .handle(
            Request::CheckpointPosition {
                authority_epoch: authority,
                epoch,
                through,
            },
            TcpLimits::default(),
        )
        .unwrap()
    {
        Response::CheckpointPosition {
            database,
            partition,
            epoch,
            durable_end,
        } => checkpoint::CheckpointPositionProof {
            database,
            partition,
            epoch,
            durable_end,
        },
        _ => panic!("expected position proof"),
    }
}

#[test]
fn checkpoint_position_syncs_and_advances_owner_root() {
    let temp = tempdir::TempDir::new("tcp-wal-position").unwrap();
    let config = config(&temp.path().join("node"));
    let node = StorageNode::open(config.clone(), metrics()).unwrap();
    node.inventory.lock().fence_to(1).unwrap();
    let receipts = append(&node, 1, &[batch(&config, 1, 0)]);
    let through = receipts[0].address.offset + u64::from(receipts[0].address.len);
    let proof = position_proof(&node, 1, 1, through);
    assert_eq!(proof.durable_end, through);
    assert_eq!(position_proof(&node, 1, 1, 0).durable_end, through);
    assert!(
        config
            .root
            .join("epoch_0000000000000001/partition-wal-root-v1")
            .is_file()
    );
    drop(node);
    let node = StorageNode::open(config, metrics()).unwrap();
    node.inventory.lock().fence_to(2).unwrap();
    assert_eq!(position_proof(&node, 2, 1, 0), proof);
}

#[test]
fn replay_wal_page_over_tcp_returns_batches_in_wal_order() {
    let temp = tempdir::TempDir::new("tcp-wal-replay").unwrap();
    let config = config(&temp.path().join("node"));
    let node = StorageNode::open(config.clone(), metrics()).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = listener.local_addr().unwrap().to_string();
    let tls = TlsFiles {
        transport: Transport::TcpTrusted,
        ca: PathBuf::new(),
        cert: PathBuf::new(),
        key: PathBuf::new(),
        server_name: "test".into(),
    };
    let stop = Arc::new(AtomicBool::new(false));
    let server_node = node.clone();
    let server_stop = stop.clone();
    let server_tls = tls.clone();
    let server = thread::spawn(move || {
        server_node
            .serve(
                listener,
                server_tls,
                TcpLimits {
                    max_frame_bytes: 4608,
                    ..TcpLimits::default()
                },
                server_stop,
            )
            .unwrap()
    });
    let mut client = Client::connect(
        &endpoint,
        &tls,
        Duration::from_secs(10),
        config.partition.clone(),
        &shape(),
    )
    .unwrap();
    assert_eq!(
        client.status().unwrap().history_persistence,
        HistoryPersistence::WalReplay
    );
    client.fence_to(1).unwrap();
    let batches: Vec<_> = (0..20).map(|n| batch(&config, 1, n)).collect();
    let mut expected = Vec::new();
    for group in batches.chunks(2) {
        expected.extend(client.append_many(1, group).unwrap());
    }
    let last = expected.last().unwrap().appended().address();
    let through = last.offset + u64::from(last.len);
    assert!(
        client
            .checkpoint_position(1, 1, through)
            .unwrap()
            .durable_end
            >= through
    );
    client.fence_to(2).unwrap();
    let mut cursor = 0;
    let mut actual = Vec::new();
    let mut pages = 0;
    loop {
        let page = client.replay_wal_page(2, 1, cursor, 256, through).unwrap();
        let charge: usize = page
            .batches
            .iter()
            .map(|(_, receipt)| receipt.appended().address().len as usize + 256)
            .sum();
        assert!(charge <= 4608 - 32);
        pages += 1;
        for (batch, receipt) in page.batches {
            assert_eq!(batch.version().sequence, actual.len() as u64);
            actual.push(receipt);
        }
        let Some(next) = page.next else { break };
        assert!(next as u64 > cursor);
        cursor = next as u64;
    }
    assert!(
        pages > 1,
        "byte bound must split this receipt-unbounded replay"
    );
    assert_eq!(actual, expected);
    assert!(client.replay_wal_page(2, 1, 0, 3, u64::MAX).is_err());
    client.retire_wal_files(2, 1, vec![0]).unwrap();
    assert!(
        !config
            .root
            .join("epoch_0000000000000001/wal_0000000000000000")
            .exists()
    );
    drop(client);
    stop.store(true, Ordering::Release);
    server.join().unwrap();
}

#[test]
fn old_and_new_recovery_requests_fail_in_the_other_mode() {
    let modes = [
        HistoryPersistence::WalReplay,
        HistoryPersistence::OnDisk,
        #[cfg(feature = "test-utils")]
        HistoryPersistence::MemoryOnly,
    ];
    for mode in modes {
        let temp = tempdir::TempDir::new("tcp-history-gates").unwrap();
        let node = StorageNode::open(config(&temp.path().join("node")), metrics()).unwrap();
        node.set_history_persistence(mode).unwrap();
        assert_eq!(node.status().unwrap().history_persistence, mode);
        node.inventory.lock().fence_to(1).unwrap();
        let old = vec![
            Request::Checkpoint {
                authority_epoch: 1,
                epoch: 1,
                prefix: 0,
            },
            Request::Recover {
                epoch: 1,
                cursor: 0,
                limit: 1,
            },
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            Request::RecoverLiveMetadata {
                authority_epoch: 1,
                source_epoch: 1,
                cursor: 0,
                limit: 1,
            },
        ];
        let new = vec![
            Request::CheckpointPosition {
                authority_epoch: 1,
                epoch: 1,
                through: 0,
            },
            Request::ReplayWal {
                authority_epoch: 1,
                source_epoch: 1,
                from: 0,
                limit: 1,
                require_end: 0,
            },
            Request::RetireWalFiles {
                authority_epoch: 1,
                epoch: 0,
                files: vec![],
            },
            #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
            Request::ReplayLiveWal {
                authority_epoch: 1,
                source_epoch: 1,
                from: 0,
                limit: 1,
                require_end: 0,
            },
        ];
        let (requests, message) = if mode == HistoryPersistence::WalReplay {
            (old, "request requires on-disk history")
        } else {
            (new, "request requires WAL replay history")
        };
        for request in requests {
            let error = match node.handle(request, TcpLimits::default()) {
                Ok(_) => panic!("wrong history mode accepted"),
                Err(error) => error,
            };
            assert_eq!(error.to_string(), message);
        }
    }
}

#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
#[test]
fn replay_live_wal_page_reads_frames_through_namespace_fixture() {
    use crate::distributed::durability::DurabilityTier;
    use crate::distributed::tcp::live_client::leased_tests::Fixture;
    use crate::distributed::write_timeline::WriteTimeline;
    let fixture = Fixture::new(DurabilityTier::Native, 16384);
    let mut client = fixture.writer_without_ahead();
    let batches: Vec<_> = (0..3).map(|n| fixture.batch(n)).collect();
    let receipts = client
        .write_leased_group(1, batches.clone(), &WriteTimeline::default(), 1)
        .unwrap();
    let last = receipts.last().unwrap().appended().address();
    let through = last.offset + u64::from(last.len);
    // The position sync must not settle or close the unadopted extent.
    let before = fixture.metrics.snapshot().counts.adoption_sync_exchanges;
    assert!(
        client
            .checkpoint_position(1, 1, through)
            .unwrap()
            .durable_end
            >= through
    );
    assert_eq!(
        fixture.metrics.snapshot().counts.adoption_sync_exchanges,
        before
    );
    client.close_leased_extent().unwrap();
    client.fence_to(2).unwrap();
    let pool_dir = tempdir::TempDir::new("replay-pool").unwrap();
    let registry = crate::distributed::nvmeof::live_connect::LiveConnectionRegistry::open(
        &pool_dir.path().join("registry"),
    )
    .unwrap();
    client.set_live_pool(Arc::new(
        crate::distributed::nvmeof::live_pool::LiveControllerPool::new(registry, 8, 4).unwrap(),
    ));
    let page = client.replay_live_wal_page(2, 1, 0, 1, through).unwrap();
    assert_eq!(page.batches.len(), 3);
    for ((actual, receipt), (expected, expected_receipt)) in
        page.batches.iter().zip(batches.iter().zip(&receipts))
    {
        assert_eq!(actual.encoded(), expected.encoded());
        assert_eq!(receipt, expected_receipt);
    }
}
