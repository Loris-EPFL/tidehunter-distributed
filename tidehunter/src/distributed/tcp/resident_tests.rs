#[cfg(feature = "experimental-fabric")]
use super::*;
#[cfg(feature = "experimental-fabric")]
#[test]
#[ignore = "requires libfabric tcp;ofi_rxm software provider"]
fn software_provider_read_io_monitor_reports_wal_and_real_rma_lane_activity() {
    use crate::distributed::codec::BatchLimits;
    use crate::distributed::group::GroupLimits;
    use crate::distributed::local_inventory::LocalInventoryConfig;
    use crate::distributed::remote_db::{RemoteDatabase, RemoteDatabaseConfig, RemoteEndpoint};
    use crate::distributed::storage::StaticInventory;
    use crate::key_shape::KeyType;

    let temp = tempdir::TempDir::new("resident-read-monitor").unwrap();
    let shape = KeyShape::new_single(4, 1, KeyType::uniform(1));
    let inventory = LocalInventoryConfig {
        database: [61; 16],
        partitions: vec![2],
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
        max_batches_per_partition: 64,
        max_reference_metadata_bytes: 1024 * 1024,
        max_epochs: 8,
    };
    let resident_source = enabled_source();
    let node_config = NodeConfig {
        root: temp.path().join("node"),
        shape_yaml: shape.to_yaml().unwrap(),
        max_epochs: inventory.max_epochs,
        partition: PartitionConfig {
            inventory: StaticInventory {
                database: inventory.database,
                epoch: 1,
                segment_generation: 1,
                partitions: inventory.partitions.clone(),
            },
            partition: 2,
            batch_limits: inventory.batch_limits,
            group_limits: inventory.group_limits,
            fragment_bytes: inventory.fragment_bytes,
            wal_file_bytes: inventory.wal_file_bytes,
            max_batches: inventory.max_batches_per_partition,
            max_reference_metadata_bytes: inventory.max_reference_metadata_bytes,
        },
        read_workers: 2,
        wal_member_read: WalMemberReadPolicy::Async,
        read_resident_first: false,
        read_cache_bytes: 0,
        read_memory_allowance_bytes: default_read_memory_allowance_bytes(),
        read_max_inflight_requests: default_read_requests(),
        read_max_inflight_bytes: default_read_bytes(),
        read_max_device_reads: default_device_reads(),
        resident_reads: resident_source,
    };
    let node = StorageNode::open(node_config, metrics()).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = listener.local_addr().unwrap().to_string();
    let server_tls = TlsFiles {
        transport: Transport::FabricTcpNative,
        ca: PathBuf::new(),
        cert: PathBuf::new(),
        key: PathBuf::new(),
        server_name: "trusted-software-provider-test".to_owned(),
        resident_reads: ResidentReadConfig::default(),
    };
    let stop = Arc::new(AtomicBool::new(false));
    let thread_stop = stop.clone();
    let server = node.clone();
    let worker = thread::spawn(move || {
        server
            .serve(listener, server_tls, TcpLimits::default(), thread_stop)
            .unwrap();
    });
    let server_thread = ServerThread {
        stop,
        worker: Some(worker),
    };
    {
        let config = RemoteDatabaseConfig {
            storage: inventory,
            endpoints: vec![RemoteEndpoint {
                partition: 2,
                address: endpoint,
                server_name: "trusted-software-provider-test".to_owned(),
            }],
            tls: TlsFiles {
                transport: Transport::FabricTcpNative,
                ca: PathBuf::new(),
                cert: PathBuf::new(),
                key: PathBuf::new(),
                server_name: "trusted-software-provider-test".to_owned(),
                resident_reads: enabled_destination(),
            },
            timeout: Duration::from_secs(3),
            max_admitted_batches: 64,
            max_index_bytes: 1024 * 1024,
            max_pending_batches: 8,
            max_pending_bytes: 1024 * 1024,
            pipelined_writes: true,
            read_cache_bytes: 0,
            read_memory_allowance_bytes: 512 * 1024 * 1024,
            preparation_workers: 2,
            scan_prefetch_pages: 1,
            read_lanes: 1,
            max_pending_reads: 32,
            max_pending_read_bytes: 1024 * 1024,
            write_profile_limit: 0,
        };
        let mut db =
            RemoteDatabase::open(&temp.path().join("authority"), &shape, config, metrics())
                .unwrap();
        let key = b"key1";
        let value = b"monitor-value";
        db.write_batch(
            [17; 16],
            vec![Mutation::Put {
                keyspace: KeySpace(0),
                key: key[..].into(),
                value: value[..].into(),
            }],
        )
        .unwrap();
        assert_eq!(db.get(KeySpace(0), key).unwrap().unwrap().as_ref(), value);
        let monitor = db.read_io_monitor();
        let storage_reads = monitor.sample().unwrap();
        assert_eq!(storage_reads.len(), 1);
        assert!(
            storage_reads[0].1.wal_reads > 0,
            "the remote read lane must account the WAL member"
        );
        let resident_before = monitor.sample_resident_read_lanes().unwrap();
        assert_eq!(resident_before.len(), 1);
        assert_eq!((resident_before[0].0, resident_before[0].1), (2, 0));
        assert_eq!(resident_before[0].2.rma_hits, 0);
        assert_eq!(db.get(KeySpace(0), key).unwrap().unwrap().as_ref(), value);
        let resident_after = monitor.sample_resident_read_lanes().unwrap();
        assert!(
            resident_after[0].2.rma_posts > resident_before[0].2.rma_posts
                && resident_after[0].2.rma_completions > resident_before[0].2.rma_completions
                && resident_after[0].2.rma_hits > resident_before[0].2.rma_hits,
            "monitor must observe initiator READ activity on the read lane"
        );
        let group_keys = (0..6)
            .map(|index| format!("g{index:03}").into_bytes())
            .collect::<Vec<_>>();
        let group_values = group_keys
            .iter()
            .enumerate()
            .map(|(index, key)| Mutation::Put {
                keyspace: KeySpace(0),
                key: key.clone().into(),
                value: vec![index as u8 + 1; 8].into(),
            })
            .collect::<Vec<_>>();
        db.write_batch([18; 16], group_values).unwrap();
        let ordered_keys = vec![
            group_keys[5].clone(),
            group_keys[1].clone(),
            group_keys[3].clone(),
            group_keys[0].clone(),
            group_keys[1].clone(),
            group_keys[4].clone(),
        ];
        let ordered_values = db.get_many(KeySpace(0), &ordered_keys).unwrap();
        assert_eq!(
            ordered_values,
            vec![
                Some(vec![6; 8].into()),
                Some(vec![2; 8].into()),
                Some(vec![4; 8].into()),
                Some(vec![1; 8].into()),
                Some(vec![2; 8].into()),
                Some(vec![5; 8].into()),
            ]
        );
        let storage_after_group = monitor.sample().unwrap();
        assert!(storage_after_group[0].1.wal_reads > storage_reads[0].1.wal_reads);
        db.shutdown().unwrap();
    }
    drop(server_thread);
}
#[cfg(feature = "experimental-fabric")]
use crate::distributed::storage::WalMemberReadPolicy;
#[cfg(feature = "experimental-fabric")]
use std::{
    net::TcpListener,
    sync::atomic::{AtomicBool, Ordering},
    thread,
    time::{Duration, Instant},
};

#[cfg(feature = "experimental-fabric")]
struct ServerThread {
    stop: Arc<AtomicBool>,
    worker: Option<thread::JoinHandle<()>>,
}

#[cfg(feature = "experimental-fabric")]
impl Drop for ServerThread {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            worker.join().unwrap();
        }
    }
}
#[cfg(feature = "experimental-fabric")]
struct ResidentFixture {
    _temp: tempdir::TempDir,
    config: NodeConfig,
    node: Arc<StorageNode>,
    endpoint: String,
    client_tls: TlsFiles,
    stop: Arc<AtomicBool>,
    worker: Option<thread::JoinHandle<()>>,
}

#[cfg(feature = "experimental-fabric")]
impl ResidentFixture {
    fn new(source: ResidentReadConfig, destination: ResidentReadConfig) -> Self {
        let temp = tempdir::TempDir::new("resident-rma").unwrap();
        let mut config = config(&temp.path().join("node"));
        config.resident_reads = source;
        config.wal_member_read = WalMemberReadPolicy::Async;
        let node = StorageNode::open(config.clone(), metrics()).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        let stop = Arc::new(AtomicBool::new(false));
        let server_stop = stop.clone();
        let server = node.clone();
        let tls = TlsFiles {
            transport: Transport::FabricTcpNative,
            ca: PathBuf::new(),
            cert: PathBuf::new(),
            key: PathBuf::new(),
            server_name: "trusted-software-provider-test".to_owned(),
            resident_reads: ResidentReadConfig::default(),
        };
        let server_tls = tls.clone();
        let worker = thread::spawn(move || {
            server
                .serve(listener, server_tls, TcpLimits::default(), server_stop)
                .unwrap();
        });
        let client_tls = TlsFiles {
            resident_reads: destination,
            ..tls
        };
        Self {
            _temp: temp,
            config,
            node,
            endpoint,
            client_tls,
            stop,
            worker: Some(worker),
        }
    }

    fn client(&self) -> Client {
        Client::connect(
            &self.endpoint,
            &self.client_tls,
            Duration::from_secs(3),
            self.config.partition.clone(),
            &shape(),
        )
        .unwrap()
    }

    fn publish(&self, client: &mut Client, sequence: u64) -> (CompleteBatch, ValueRecordReference) {
        let value_batch = batch(&self.config, 1, sequence);
        let receipt = client
            .append_many(1, std::slice::from_ref(&value_batch))
            .unwrap();
        let reference = value_batch
            .record_reference(receipt[0].appended().address(), 0)
            .unwrap();
        (value_batch, reference)
    }
}

#[cfg(feature = "experimental-fabric")]
impl Drop for ResidentFixture {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            worker.join().unwrap();
        }
    }
}
#[cfg(feature = "experimental-fabric")]
struct RemoteFixture {
    _server: ServerThread,
    _temp: tempdir::TempDir,
    shape: KeyShape,
    config: crate::distributed::remote_db::RemoteDatabaseConfig,
    node: Arc<StorageNode>,
}

#[cfg(feature = "experimental-fabric")]
impl RemoteFixture {
    fn new(
        source: ResidentReadConfig,
        destination: ResidentReadConfig,
        read_cache_bytes: usize,
        read_lanes: usize,
    ) -> Self {
        Self::new_with_history_window(source, destination, read_cache_bytes, read_lanes, 64)
    }

    fn new_with_history_window(
        source: ResidentReadConfig,
        destination: ResidentReadConfig,
        read_cache_bytes: usize,
        read_lanes: usize,
        history_window: usize,
    ) -> Self {
        use crate::distributed::codec::BatchLimits;
        use crate::distributed::group::GroupLimits;
        use crate::distributed::local_inventory::LocalInventoryConfig;
        use crate::distributed::remote_db::{RemoteDatabaseConfig, RemoteEndpoint};
        use crate::distributed::storage::StaticInventory;
        use crate::key_shape::KeyType;

        let temp = tempdir::TempDir::new("resident-remote-db").unwrap();
        let shape = KeyShape::new_single(4, 1, KeyType::uniform(1));
        let inventory = LocalInventoryConfig {
            database: [62; 16],
            partitions: vec![2],
            batch_limits: BatchLimits {
                max_operations: 8,
                max_encoded_bytes: 2048,
            },
            group_limits: GroupLimits {
                max_batches: u32::try_from(history_window.min(8)).unwrap(),
                max_encoded_bytes: 4088,
            },
            fragment_bytes: 4096,
            wal_file_bytes: 8192,
            max_batches_per_partition: history_window,
            max_reference_metadata_bytes: 1024 * 1024,
            max_epochs: 8,
        };
        let node_config = NodeConfig {
            root: temp.path().join("node"),
            shape_yaml: shape.to_yaml().unwrap(),
            max_epochs: inventory.max_epochs,
            partition: PartitionConfig {
                inventory: StaticInventory {
                    database: inventory.database,
                    epoch: 1,
                    segment_generation: 1,
                    partitions: inventory.partitions.clone(),
                },
                partition: 2,
                batch_limits: inventory.batch_limits,
                group_limits: inventory.group_limits,
                fragment_bytes: inventory.fragment_bytes,
                wal_file_bytes: inventory.wal_file_bytes,
                max_batches: inventory.max_batches_per_partition,
                max_reference_metadata_bytes: inventory.max_reference_metadata_bytes,
            },
            read_workers: 2,
            wal_member_read: WalMemberReadPolicy::Async,
            read_resident_first: false,
            read_cache_bytes: 0,
            read_memory_allowance_bytes: default_read_memory_allowance_bytes(),
            read_max_inflight_requests: default_read_requests(),
            read_max_inflight_bytes: default_read_bytes(),
            read_max_device_reads: default_device_reads(),
            resident_reads: source,
        };
        let node = StorageNode::open(node_config, metrics()).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = stop.clone();
        let serving = node.clone();
        let server_tls = TlsFiles {
            transport: Transport::FabricTcpNative,
            ca: PathBuf::new(),
            cert: PathBuf::new(),
            key: PathBuf::new(),
            server_name: "trusted-software-provider-test".to_owned(),
            resident_reads: ResidentReadConfig::default(),
        };
        let worker = thread::spawn(move || {
            serving
                .serve(listener, server_tls, TcpLimits::default(), thread_stop)
                .unwrap();
        });
        let server = ServerThread {
            stop,
            worker: Some(worker),
        };
        let config = RemoteDatabaseConfig {
            storage: inventory,
            endpoints: vec![RemoteEndpoint {
                partition: 2,
                address,
                server_name: "trusted-software-provider-test".to_owned(),
            }],
            tls: TlsFiles {
                transport: Transport::FabricTcpNative,
                ca: PathBuf::new(),
                cert: PathBuf::new(),
                key: PathBuf::new(),
                server_name: "trusted-software-provider-test".to_owned(),
                resident_reads: destination,
            },
            timeout: Duration::from_secs(3),
            max_admitted_batches: 64,
            max_index_bytes: 1024 * 1024,
            max_pending_batches: 8,
            max_pending_bytes: 1024 * 1024,
            pipelined_writes: true,
            read_cache_bytes,
            read_memory_allowance_bytes: 512 * 1024 * 1024,
            preparation_workers: 2,
            scan_prefetch_pages: 1,
            read_lanes,
            max_pending_reads: 32,
            max_pending_read_bytes: 1024 * 1024,
            write_profile_limit: 0,
        };
        Self {
            _temp: temp,
            shape,
            config,
            node,
            _server: server,
        }
    }

    fn open(&self) -> crate::distributed::remote_db::RemoteDatabase {
        crate::distributed::remote_db::RemoteDatabase::open(
            &self._temp.path().join("authority"),
            &self.shape,
            self.config.clone(),
            metrics(),
        )
        .unwrap()
    }
}

#[cfg(feature = "experimental-fabric")]
#[test]
#[ignore = "requires libfabric tcp;ofi_rxm software provider"]
fn software_provider_concurrent_cold_acquisition_with_one_handle_returns_both_values() {
    use crate::distributed::remote_db::ConcurrentRemoteDatabase;

    let source = ResidentReadConfig {
        source_regions: 2,
        ..enabled_source()
    };
    let destination = ResidentReadConfig {
        destination_bytes: 4 * 1024,
        destination_slots: 1,
        max_handles: 1,
        max_read_bytes: 4 * 1024,
        ..ResidentReadConfig::default()
    };
    let fixture = RemoteFixture::new(source, destination, 0, 1);
    let mut storage = fixture.open();
    let values = [
        Mutation::Put {
            keyspace: KeySpace(0),
            key: b"a001"[..].into(),
            value: b"cold-first"[..].into(),
        },
        Mutation::Put {
            keyspace: KeySpace(0),
            key: b"a002"[..].into(),
            value: b"cold-second"[..].into(),
        },
    ];
    storage.write_batch([41; 16], values.to_vec()).unwrap();
    let database = ConcurrentRemoteDatabase::new(storage).unwrap();

    let first = database.submit_get(KeySpace(0), b"a001").unwrap();
    let second = database.submit_get(KeySpace(0), b"a002").unwrap();
    assert_eq!(
        database
            .finish_ready_get(first.wait())
            .unwrap()
            .unwrap()
            .as_ref(),
        b"cold-first"
    );
    assert_eq!(
        database
            .finish_ready_get(second.wait())
            .unwrap()
            .unwrap()
            .as_ref(),
        b"cold-second"
    );
    database.validate_cached_read().unwrap();
    let lane_stats = database
        .read_io_monitor()
        .sample_resident_read_lanes()
        .unwrap();
    assert_eq!(lane_stats.len(), 1);
    assert!(lane_stats[0].2.rma_errors == 0);
    assert!(lane_stats[0].2.handles_live <= 1);
    database.shutdown().unwrap();
}
#[cfg(feature = "experimental-fabric")]
fn enabled_source() -> ResidentReadConfig {
    ResidentReadConfig {
        source_bytes: 64 * 1024,
        source_regions: 8,
        max_read_bytes: 4 * 1024,
        ..ResidentReadConfig::default()
    }
}

#[cfg(feature = "experimental-fabric")]
fn enabled_destination() -> ResidentReadConfig {
    ResidentReadConfig {
        destination_bytes: 16 * 1024,
        destination_slots: 4,
        max_handles: 8,
        max_read_bytes: 4 * 1024,
        ..ResidentReadConfig::default()
    }
}

#[cfg(feature = "experimental-fabric")]
#[test]
#[ignore = "requires libfabric tcp;ofi_rxm software provider"]
fn software_provider_durable_grant_reads_real_member_without_read_rpc() {
    let fixture = ResidentFixture::new(enabled_source(), enabled_destination());
    let mut client = fixture.client();
    client.fence_to(1).unwrap();
    let (batch, reference) = fixture.publish(&mut client, 3);
    let expected = batch.mutations()[0].clone();

    // A real durable member RPC opportunistically acquires its exact grant.
    assert_eq!(
        client.read_records(1, &[reference]).unwrap(),
        vec![expected.clone()]
    );
    let before = client.resident_read_stats();
    assert_eq!(before.grant_acquisitions, 1);
    assert_eq!(before.grants_live, 1);
    assert_eq!(before.rma_posts, 0);

    // This drives the actual provider CQ path; no counter or FI_READ is mocked.
    let pending_id = client
        .submit_read(
            1,
            vec![reference],
            Instant::now() + Duration::from_secs(3),
            ReadClass::Point,
            true,
        )
        .unwrap()
        .expect("durable resident grant should admit an RMA read");
    let deadline = Instant::now() + Duration::from_secs(3);
    let completion = loop {
        if let Some(completion) = client
            .progress_reads()
            .unwrap()
            .into_iter()
            .find(|completion| completion.id == pending_id)
        {
            break completion;
        }
        assert!(Instant::now() < deadline, "resident read did not complete");
        thread::yield_now();
    };
    let (values, _) = completion.result.unwrap();
    assert_eq!(values, vec![expected]);
    let after = client.resident_read_stats();
    assert_eq!(after.rma_posts - before.rma_posts, 1);
    assert_eq!(after.rma_completions - before.rma_completions, 1);
    assert_eq!(after.rma_hits - before.rma_hits, 1);
    assert_eq!(after.message_read_requests, before.message_read_requests);

    client.revoke_resident_grants().unwrap();
    assert_eq!(client.resident_read_stats().handles_live, 0);
    assert_eq!(fixture.node.resident_grants.live_stats(), (0, 0, 0));
    client.fence_to(2).unwrap();
}

#[cfg(feature = "experimental-fabric")]
#[test]
#[ignore = "requires libfabric tcp;ofi_rxm software provider"]
fn software_provider_two_clients_submit_rma_before_either_completion_wait() {
    let source = ResidentReadConfig {
        source_regions: 2 * TcpLimits::default().max_connections,
        ..enabled_source()
    };
    let fixture = ResidentFixture::new(source, enabled_destination());
    let mut first = fixture.client();
    let mut second = fixture.client();
    first.fence_to(1).unwrap();
    second.fence_to(1).unwrap();

    let batches = (30..32)
        .map(|sequence| batch(&fixture.config, 1, sequence))
        .collect::<Vec<_>>();
    let receipts = first.append_many(1, &batches).unwrap();
    let references = batches
        .iter()
        .zip(receipts)
        .map(|(batch, receipt)| {
            batch
                .record_reference(receipt.appended().address(), 0)
                .unwrap()
        })
        .collect::<Vec<_>>();
    let values = batches
        .iter()
        .map(|batch| batch.mutations()[0].clone())
        .collect::<Vec<_>>();
    let reversed_references = references.iter().rev().copied().collect::<Vec<_>>();
    let reversed_values = values.iter().rev().cloned().collect::<Vec<_>>();

    assert_eq!(first.read_records(1, &references).unwrap(), values);
    assert_eq!(second.read_records(1, &references).unwrap(), values);
    let first_rpc_before = first.resident_read_stats().message_read_requests;
    let second_rpc_before = second.resident_read_stats().message_read_requests;

    let first_id = first
        .submit_read(
            1,
            references,
            Instant::now() + Duration::from_secs(3),
            ReadClass::Point,
            true,
        )
        .unwrap()
        .expect("first client should admit its resident read");
    let second_id = second
        .submit_read(
            1,
            reversed_references,
            Instant::now() + Duration::from_secs(3),
            ReadClass::Point,
            true,
        )
        .unwrap()
        .expect("second client should admit its resident read");
    // The API submits both candidates without waiting. Give each owner one
    // nonblocking progress sweep to post its FI_READs before waiting for either
    // result; keep any immediately-ready completion until both lanes were swept.
    let first_sweep = first.progress_reads().unwrap();
    let second_sweep = second.progress_reads().unwrap();
    assert!(first.resident_read_stats().rma_posts > 0);
    assert!(second.resident_read_stats().rma_posts > 0);

    let deadline = Instant::now() + Duration::from_secs(3);
    let mut first_result = first_sweep
        .into_iter()
        .find(|completion| completion.id == first_id)
        .map(|completion| completion.result.unwrap().0);
    let mut second_result = second_sweep
        .into_iter()
        .find(|completion| completion.id == second_id)
        .map(|completion| completion.result.unwrap().0);
    while first_result.is_none() || second_result.is_none() {
        for completion in first.progress_reads().unwrap() {
            if completion.id == first_id {
                first_result = Some(completion.result.unwrap().0);
            }
        }
        for completion in second.progress_reads().unwrap() {
            if completion.id == second_id {
                second_result = Some(completion.result.unwrap().0);
            }
        }
        assert!(
            Instant::now() < deadline,
            "both resident reads must complete"
        );
        thread::yield_now();
    }
    assert_eq!(first_result.unwrap(), values);
    assert_eq!(second_result.unwrap(), reversed_values);
    let first_stats = first.resident_read_stats();
    let second_stats = second.resident_read_stats();
    assert!(
        first_stats.rma_posts > 0 && first_stats.rma_completions > 0 && first_stats.rma_hits > 0
    );
    assert!(
        second_stats.rma_posts > 0 && second_stats.rma_completions > 0 && second_stats.rma_hits > 0
    );
    assert_eq!(first_stats.message_read_requests, first_rpc_before);
    assert_eq!(second_stats.message_read_requests, second_rpc_before);

    first.revoke_resident_grants().unwrap();
    second.revoke_resident_grants().unwrap();
    assert_eq!(fixture.node.resident_grants.live_stats(), (0, 0, 0));
    first.fence_to(2).unwrap();
    second.fence_to(2).unwrap();
}

#[cfg(feature = "experimental-fabric")]
#[test]
#[ignore = "requires libfabric tcp;ofi_rxm software provider"]
fn software_provider_idle_holder_cannot_acknowledge_fence_before_release() {
    let fixture = ResidentFixture::new(enabled_source(), enabled_destination());
    let mut client = fixture.client();
    client.fence_to(1).unwrap();
    let (batch, reference) = fixture.publish(&mut client, 7);
    assert_eq!(
        client.read_records(1, &[reference]).unwrap(),
        vec![batch.mutations()[0].clone()]
    );
    assert_eq!(fixture.node.resident_grants.live_stats().1, 1);

    let pending_id = client
        .submit_read(
            1,
            vec![reference],
            Instant::now() + Duration::from_secs(3),
            ReadClass::Point,
            false,
        )
        .unwrap()
        .unwrap();
    assert!(
        client.revoke_resident_grants().is_err(),
        "a queued read must drain before a release acknowledgment"
    );
    let deadline = Instant::now() + Duration::from_secs(3);
    let completion = loop {
        if let Some(completion) = client
            .progress_reads()
            .unwrap()
            .into_iter()
            .find(|completion| completion.id == pending_id)
        {
            break completion;
        }
        assert!(Instant::now() < deadline, "owned read did not drain");
        thread::yield_now();
    };
    assert_eq!(
        completion.result.unwrap().0,
        vec![batch.mutations()[0].clone()]
    );
    // Deliberately bypass Client::fence_to's cooperative release step to verify
    // that the owner cannot acknowledge a barrier while an idle grant is live.
    assert!(
        client.request(Request::Fence { epoch: 2 }).is_err(),
        "a live grant must prevent a fence acknowledgment"
    );
    assert_eq!(fixture.node.resident_grants.live_stats().1, 1);
    assert!(
        client.request(Request::Seal { epoch: 1 }).is_err(),
        "a live grant must prevent a seal acknowledgment"
    );
    assert_eq!(fixture.node.resident_grants.live_stats().1, 1);
    client.revoke_resident_grants().unwrap();
    assert_eq!(fixture.node.resident_grants.live_stats(), (0, 0, 0));
    client.fence_to(2).unwrap();
}

#[cfg(feature = "experimental-fabric")]
#[test]
#[ignore = "requires libfabric tcp;ofi_rxm software provider"]
fn software_provider_source_and_destination_limits_preserve_rpc_values() {
    let source_limited = ResidentReadConfig {
        source_bytes: 1,
        source_regions: 1,
        max_read_bytes: 4 * 1024,
        ..ResidentReadConfig::default()
    };
    let fixture = ResidentFixture::new(source_limited, enabled_destination());
    let mut client = fixture.client();
    client.fence_to(1).unwrap();
    let (batch, reference) = fixture.publish(&mut client, 4);
    let expected = batch.mutations()[0].clone();

    assert_eq!(
        client.read_records(1, &[reference]).unwrap(),
        vec![expected.clone()]
    );
    let rpc_before = client.resident_read_stats().message_read_requests;
    let pending_id = client
        .submit_read(
            1,
            vec![reference],
            Instant::now() + Duration::from_secs(3),
            ReadClass::Point,
            false,
        )
        .unwrap()
        .expect("unsupported source promotion must fall back to the RPC read");
    let deadline = Instant::now() + Duration::from_secs(3);
    let completion = loop {
        if let Some(completion) = client
            .progress_reads()
            .unwrap()
            .into_iter()
            .find(|completion| completion.id == pending_id)
        {
            break completion;
        }
        assert!(Instant::now() < deadline, "RPC fallback did not complete");
        thread::yield_now();
    };
    assert_eq!(completion.result.unwrap().0, vec![expected]);
    let source_stats = client.resident_read_stats();
    assert!(source_stats.message_read_requests > rpc_before);
    assert_eq!(source_stats.rma_hits, 0);
    assert!(source_stats.fallback_missing > 0);

    client.revoke_resident_grants().unwrap();
    client.fence_to(2).unwrap();
    drop(client);
    drop(fixture);

    // The client's effective operation limit is carried in grant acquisition,
    // so the owner declines promotion and the exact-reference RPC remains usable.
    // This verifies destination-limit fallback without asserting an unavailable
    // cached grant or a client-side oversize counter.
    let limited_destination = ResidentReadConfig {
        destination_bytes: 256,
        destination_slots: 1,
        max_handles: 2,
        max_read_bytes: 128,
        ..ResidentReadConfig::default()
    };
    let fixture = ResidentFixture::new(enabled_source(), limited_destination);
    let mut client = fixture.client();
    client.fence_to(1).unwrap();
    let (batch, reference) = fixture.publish(&mut client, 5);
    let expected = batch.mutations()[0].clone();
    assert_eq!(
        client.read_records(1, &[reference]).unwrap(),
        vec![expected.clone()]
    );
    let rpc_before = client.resident_read_stats().message_read_requests;
    let pending_id = client
        .submit_read(
            1,
            vec![reference],
            Instant::now() + Duration::from_secs(3),
            ReadClass::Point,
            false,
        )
        .unwrap()
        .expect("destination-limit refusal must preserve RPC reads");
    let deadline = Instant::now() + Duration::from_secs(3);
    let completion = loop {
        if let Some(completion) = client
            .progress_reads()
            .unwrap()
            .into_iter()
            .find(|completion| completion.id == pending_id)
        {
            break completion;
        }
        assert!(
            Instant::now() < deadline,
            "destination-limit RPC fallback did not complete"
        );
        thread::yield_now();
    };
    assert_eq!(completion.result.unwrap().0, vec![expected]);
    let stats = client.resident_read_stats();
    assert!(stats.message_read_requests > rpc_before);
    assert_eq!(stats.grant_acquisitions, 0);
    assert_eq!(stats.rma_hits, 0);
    assert!(stats.fallback_missing > 0);
    client.revoke_resident_grants().unwrap();
    client.fence_to(2).unwrap();
}
#[cfg(feature = "experimental-fabric")]
#[test]
#[ignore = "requires libfabric tcp;ofi_rxm software provider"]
fn software_provider_group_above_destination_slots_falls_back_without_reordering() {
    let destination_slots = enabled_destination().destination_slots;
    let group_count = destination_slots + 2;
    let destination = ResidentReadConfig {
        destination_bytes: group_count * 4 * 1024,
        max_handles: group_count,
        ..enabled_destination()
    };
    let source = ResidentReadConfig {
        source_bytes: group_count * 4 * 1024,
        source_regions: group_count * TcpLimits::default().max_connections,
        ..enabled_source()
    };
    let fixture = ResidentFixture::new(source, destination);
    let mut client = fixture.client();
    client.fence_to(1).unwrap();
    let batches = (0..group_count)
        .map(|offset| batch(&fixture.config, 1, 10 + offset as u64))
        .collect::<Vec<_>>();
    let receipts = client.append_many(1, &batches).unwrap();
    let references = batches
        .iter()
        .zip(receipts)
        .map(|(batch, receipt)| {
            batch
                .record_reference(receipt.appended().address(), 0)
                .unwrap()
        })
        .collect::<Vec<_>>();

    // A group larger than the configured destination slots cannot be partially
    // posted. The duplicate position must survive the complete RPC fallback.
    let mut request = references.clone();
    request.reverse();
    request.insert(destination_slots, references[destination_slots]);
    let expected = request
        .iter()
        .map(|reference| {
            let index = references
                .iter()
                .position(|candidate| candidate == reference)
                .unwrap();
            batches[index].mutations()[0].clone()
        })
        .collect::<Vec<_>>();
    let values = batches
        .iter()
        .map(|batch| batch.mutations()[0].clone())
        .collect::<Vec<_>>();
    assert_eq!(client.read_records(1, &references).unwrap(), values);
    let warmed = client.resident_read_stats();
    assert_eq!(warmed.grant_acquisitions, group_count as u64);
    assert_eq!(warmed.handles_live, group_count as u64);
    let rpc_before = client.resident_read_stats().message_read_requests;
    let pending_id = client
        .submit_read(
            1,
            request,
            Instant::now() + Duration::from_secs(3),
            ReadClass::Point,
            false,
        )
        .unwrap()
        .expect("group beyond destination slots must use a complete RPC read");
    let deadline = Instant::now() + Duration::from_secs(3);
    let completion = loop {
        if let Some(completion) = client
            .progress_reads()
            .unwrap()
            .into_iter()
            .find(|completion| completion.id == pending_id)
        {
            break completion;
        }
        assert!(
            Instant::now() < deadline,
            "group RPC fallback did not complete"
        );
        thread::yield_now();
    };
    assert_eq!(completion.result.unwrap().0, expected);
    let stats = client.resident_read_stats();
    assert!(stats.message_read_requests > rpc_before);
    assert_eq!(stats.rma_posts, 0);
    assert!(stats.fallback_capacity > 0);
    client.revoke_resident_grants().unwrap();
    client.fence_to(2).unwrap();
}

#[cfg(feature = "experimental-fabric")]
#[test]
#[ignore = "requires libfabric tcp;ofi_rxm software provider"]
fn software_provider_corrupt_envelope_and_member_are_rejected() {
    let fixture = ResidentFixture::new(enabled_source(), enabled_destination());
    let mut client = fixture.client();
    client.fence_to(1).unwrap();
    let (batch, reference) = fixture.publish(&mut client, 20);
    let start = reference.descriptor.offset as usize;
    let end = start + reference.descriptor.len as usize;
    let encoded = batch.encoded();
    let member = encoded.slice(start..end);
    let grant = super::super::resident::ResidentGrant {
        reference,
        request_epoch: 1,
        reader_generation: 1,
        owner_boot: [1; 16],
        session_id: 1,
        grant_id: 1,
        grant_generation: 1,
        region: super::super::resident::RemoteRegion {
            region_id: 1,
            generation: 1,
            remote_address: 0,
            key: 0,
            len: 0,
        },
        source_region_id: 1,
        source_region_generation: 1,
    };
    let valid_envelope = super::super::resident::encode_envelope(grant, member.as_ref()).unwrap();
    assert_eq!(
        super::super::resident::checked_member(grant, &valid_envelope).unwrap(),
        member.as_ref()
    );

    let mut corrupt_envelope = valid_envelope.clone();
    let header_len = u32::from_le_bytes(corrupt_envelope[..4].try_into().unwrap()) as usize;
    let header_end = 4 + header_len;
    corrupt_envelope[4] ^= 1; // semantic magic change; preserve valid header CRC
    let checksum = crc32fast::hash(&corrupt_envelope[4..header_end]);
    corrupt_envelope[header_end..header_end + 4].copy_from_slice(&checksum.to_le_bytes());
    assert!(super::super::resident::checked_member(grant, &corrupt_envelope).is_err());

    let mut corrupt_member = member.to_vec();
    let last = corrupt_member.len() - 1;
    corrupt_member[last] ^= 1;
    let member_envelope = super::super::resident::encode_envelope(grant, &corrupt_member).unwrap();
    let checked = super::super::resident::checked_member(grant, &member_envelope).unwrap();
    assert!(CompleteBatch::decode_record(&shape(), &reference, checked.to_vec().into()).is_err());
    client.revoke_resident_grants().unwrap();
    client.fence_to(2).unwrap();
}

#[cfg(feature = "experimental-fabric")]
#[test]
#[ignore = "requires libfabric tcp;ofi_rxm software provider"]
fn software_provider_wrong_epoch_and_reference_never_return_stale_value() {
    let fixture = ResidentFixture::new(enabled_source(), enabled_destination());
    let mut client = fixture.client();
    client.fence_to(1).unwrap();
    let (batch, reference) = fixture.publish(&mut client, 21);
    let expected = batch.mutations()[0].clone();
    assert_eq!(
        client.read_records(1, &[reference]).unwrap(),
        vec![expected]
    );
    let wrong_epoch_id = client
        .submit_read(
            2,
            vec![reference],
            Instant::now() + Duration::from_secs(3),
            ReadClass::Point,
            false,
        )
        .unwrap()
        .expect("stale epoch must use the rejecting reference RPC");
    let deadline = Instant::now() + Duration::from_secs(3);
    let wrong_epoch = loop {
        if let Some(completion) = client
            .progress_reads()
            .unwrap()
            .into_iter()
            .find(|completion| completion.id == wrong_epoch_id)
        {
            break completion;
        }
        assert!(
            Instant::now() < deadline,
            "wrong-epoch result did not complete"
        );
        thread::yield_now();
    };
    assert!(wrong_epoch.result.is_err());
    client.revoke_resident_grants().unwrap();
    client.fence_to(2).unwrap();
    drop(client);
    drop(fixture);

    // Isolate a malformed reference from the earlier epoch transition. Local
    // validation must reject it before any provider READ can be accepted.
    let fixture = ResidentFixture::new(enabled_source(), enabled_destination());
    let mut client = fixture.client();
    client.fence_to(1).unwrap();
    let (batch, reference) = fixture.publish(&mut client, 22);
    assert_eq!(
        client.read_records(1, &[reference]).unwrap(),
        vec![batch.mutations()[0].clone()]
    );
    let mut wrong_reference = reference;
    wrong_reference.descriptor.offset = wrong_reference.descriptor.offset.checked_add(1).unwrap();
    let posts_before = client.resident_read_stats().rma_posts;
    assert!(
        client
            .submit_read(
                1,
                vec![wrong_reference],
                Instant::now() + Duration::from_secs(3),
                ReadClass::Point,
                false,
            )
            .is_err(),
        "the malformed member must be rejected before transport admission"
    );
    assert_eq!(client.resident_read_stats().rma_posts, posts_before);
}

#[cfg(feature = "experimental-fabric")]
#[test]
#[ignore = "requires libfabric tcp;ofi_rxm software provider"]
fn software_provider_dropped_holder_quarantines_grant_and_blocks_fence() {
    let source = ResidentReadConfig {
        source_regions: 1,
        ..enabled_source()
    };
    let fixture = ResidentFixture::new(source, enabled_destination());
    let mut client = fixture.client();
    client.fence_to(1).unwrap();
    let (batch, reference) = fixture.publish(&mut client, 22);
    assert_eq!(
        client.read_records(1, &[reference]).unwrap(),
        vec![batch.mutations()[0].clone()]
    );
    drop(client); // No RELEASE was sent; ownership is uncertain, not reusable.

    let deadline = Instant::now() + Duration::from_secs(3);
    let quarantined = loop {
        let stats = fixture.node.resident_grants.live_stats();
        if stats.2 != 0 {
            break stats;
        }
        assert!(
            Instant::now() < deadline,
            "lost session was not quarantined"
        );
        thread::yield_now();
    };
    assert_eq!(quarantined.1, 1);
    assert!(quarantined.2 > 0);

    let mut replacement = fixture.client();
    assert_eq!(
        replacement.read_records(1, &[reference]).unwrap(),
        vec![batch.mutations()[0].clone()]
    );
    assert_eq!(replacement.resident_read_stats().grant_acquisitions, 0);
    assert!(replacement.fence_to(2).is_err());
}

#[cfg(feature = "experimental-fabric")]
#[test]
#[ignore = "requires libfabric tcp;ofi_rxm software provider"]
fn software_provider_quarantine_keeps_original_wal_owner_after_node_drop() {
    let temp = tempdir::TempDir::new("resident-quarantine-owner").unwrap();
    let mut node_config = config(&temp.path().join("node"));
    node_config.resident_reads = ResidentReadConfig {
        source_regions: 1,
        ..enabled_source()
    };
    node_config.wal_member_read = WalMemberReadPolicy::Async;
    let node = Arc::new(StorageNode::open(node_config.clone(), metrics()).unwrap());
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = listener.local_addr().unwrap().to_string();
    let stop = Arc::new(AtomicBool::new(false));
    let server_stop = Arc::clone(&stop);
    let server_node = Arc::clone(&node);
    let server_tls = TlsFiles {
        transport: Transport::FabricTcpNative,
        ca: PathBuf::new(),
        cert: PathBuf::new(),
        key: PathBuf::new(),
        server_name: "trusted-software-provider-test".to_owned(),
        resident_reads: ResidentReadConfig::default(),
    };
    let worker = thread::spawn(move || {
        server_node
            .serve(listener, server_tls, TcpLimits::default(), server_stop)
            .unwrap();
    });
    let mut client = Client::connect(
        &endpoint,
        &TlsFiles {
            transport: Transport::FabricTcpNative,
            ca: PathBuf::new(),
            cert: PathBuf::new(),
            key: PathBuf::new(),
            server_name: "trusted-software-provider-test".to_owned(),
            resident_reads: enabled_destination(),
        },
        Duration::from_secs(3),
        node_config.partition.clone(),
        &shape(),
    )
    .unwrap();
    client.fence_to(1).unwrap();
    let value_batch = batch(&node_config, 1, 23);
    let receipt = client
        .append_many(1, std::slice::from_ref(&value_batch))
        .unwrap();
    let reference = value_batch
        .record_reference(receipt[0].appended().address(), 0)
        .unwrap();
    assert_eq!(
        client.read_records(1, &[reference]).unwrap(),
        vec![value_batch.mutations()[0].clone()]
    );
    drop(client); // No exact peer drain: its source registration remains live.

    let deadline = Instant::now() + Duration::from_secs(3);
    let quarantined = loop {
        let stats = node.resident_grants.live_stats();
        if stats.2 != 0 {
            break stats;
        }
        assert!(
            Instant::now() < deadline,
            "lost session was not quarantined before node teardown"
        );
        thread::yield_now();
    };
    assert_eq!(quarantined.1, 1);
    assert!(quarantined.0 > 0, "quarantined source bytes remain charged");
    assert!(quarantined.2 > 0, "quarantined bytes remain charged");
    stop.store(true, Ordering::Release);
    worker.join().unwrap();
    drop(node); // Drops GrantRegistry; only the quarantined source MR can pin WAL ownership.

    assert!(
        StorageNode::open(node_config, metrics()).is_err(),
        "quarantined source MR must retain the original WAL owner and directory lock"
    );
}

#[cfg(feature = "experimental-fabric")]
#[test]
#[ignore = "requires libfabric tcp;ofi_rxm software provider"]
fn software_provider_exact_drain_releases_original_wal_owner() {
    let temp = tempdir::TempDir::new("resident-drained-owner").unwrap();
    let mut node_config = config(&temp.path().join("node"));
    node_config.resident_reads = ResidentReadConfig {
        source_regions: 1,
        ..enabled_source()
    };
    node_config.wal_member_read = WalMemberReadPolicy::Async;
    let node = Arc::new(StorageNode::open(node_config.clone(), metrics()).unwrap());
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = listener.local_addr().unwrap().to_string();
    let stop = Arc::new(AtomicBool::new(false));
    let server_stop = Arc::clone(&stop);
    let server_node = Arc::clone(&node);
    let server_tls = TlsFiles {
        transport: Transport::FabricTcpNative,
        ca: PathBuf::new(),
        cert: PathBuf::new(),
        key: PathBuf::new(),
        server_name: "trusted-software-provider-test".to_owned(),
        resident_reads: ResidentReadConfig::default(),
    };
    let worker = thread::spawn(move || {
        server_node
            .serve(listener, server_tls, TcpLimits::default(), server_stop)
            .unwrap();
    });
    let mut client = Client::connect(
        &endpoint,
        &TlsFiles {
            transport: Transport::FabricTcpNative,
            ca: PathBuf::new(),
            cert: PathBuf::new(),
            key: PathBuf::new(),
            server_name: "trusted-software-provider-test".to_owned(),
            resident_reads: enabled_destination(),
        },
        Duration::from_secs(3),
        node_config.partition.clone(),
        &shape(),
    )
    .unwrap();
    client.fence_to(1).unwrap();
    let value_batch = batch(&node_config, 1, 24);
    let receipt = client
        .append_many(1, std::slice::from_ref(&value_batch))
        .unwrap();
    let reference = value_batch
        .record_reference(receipt[0].appended().address(), 0)
        .unwrap();
    assert_eq!(
        client.read_records(1, &[reference]).unwrap(),
        vec![value_batch.mutations()[0].clone()]
    );
    let live = node.resident_grants.live_stats();
    assert_eq!(live.1, 1);
    assert!(live.0 > 0);
    assert_eq!(live.2, 0);
    client.revoke_resident_grants().unwrap();
    assert_eq!(node.resident_grants.live_stats(), (0, 0, 0));
    drop(client);

    stop.store(true, Ordering::Release);
    worker.join().unwrap();
    drop(node);
    let reopened = StorageNode::open(node_config, metrics())
        .expect("exact peer drain and MR retirement must release the original WAL owner");
    drop(reopened);
}

#[cfg(feature = "experimental-fabric")]
#[test]
#[ignore = "requires libfabric tcp;ofi_rxm software provider"]
fn software_provider_shutdown_releases_grants_before_clean_reopen() {
    let fixture = RemoteFixture::new(enabled_source(), enabled_destination(), 0, 1);
    let mut database = fixture.open();
    let key = b"r001";
    let value = b"reopen-value";
    database
        .write_batch(
            [51; 16],
            vec![Mutation::Put {
                keyspace: KeySpace(0),
                key: key[..].into(),
                value: value[..].into(),
            }],
        )
        .unwrap();
    assert_eq!(
        database.get(KeySpace(0), key).unwrap().unwrap().as_ref(),
        value
    );
    assert_eq!(
        database.get(KeySpace(0), key).unwrap().unwrap().as_ref(),
        value
    );
    let resident = database
        .read_io_monitor()
        .sample_resident_read_lanes()
        .unwrap();
    assert!(resident[0].2.rma_hits > 0);

    database.shutdown().unwrap();
    assert_eq!(fixture.node.resident_grants.live_stats(), (0, 0, 0));
    drop(database);

    let mut reopened = fixture.open();
    assert_eq!(
        reopened.get(KeySpace(0), key).unwrap().unwrap().as_ref(),
        value
    );
    reopened.shutdown().unwrap();
    assert_eq!(fixture.node.resident_grants.live_stats(), (0, 0, 0));
}

#[cfg(feature = "experimental-fabric")]
#[test]
#[ignore = "requires libfabric tcp;ofi_rxm software provider"]
fn software_provider_shutdown_drains_completed_read_while_result_handle_is_held() {
    use crate::distributed::remote_db::ConcurrentRemoteDatabase;

    let fixture = RemoteFixture::new(enabled_source(), enabled_destination(), 0, 1);
    let database = ConcurrentRemoteDatabase::new(fixture.open()).unwrap();
    let key = b"held";
    let value = b"held-result";
    database
        .write_batch(
            [53; 16],
            vec![Mutation::Put {
                keyspace: KeySpace(0),
                key: key[..].into(),
                value: value[..].into(),
            }],
        )
        .unwrap();

    let first = database.submit_get(KeySpace(0), key).unwrap();
    assert_eq!(
        database
            .finish_ready_get(first.wait())
            .unwrap()
            .unwrap()
            .as_ref(),
        value
    );
    let retained = database.submit_get(KeySpace(0), key).unwrap().wait();
    assert!(
        database
            .read_io_monitor()
            .sample_resident_read_lanes()
            .unwrap()[0]
            .2
            .rma_hits
            > 0,
        "the retained result must come from a completed resident read"
    );

    // `retained` still owns its completed value, read-cut pin, and admission
    // credit. Shutdown must drain accepted lane work and release grants without
    // waiting for the consumer to take or drop that result.
    database.shutdown().unwrap();
    assert_eq!(fixture.node.resident_grants.live_stats(), (0, 0, 0));

    let mut reopened = fixture.open();
    assert_eq!(
        reopened.get(KeySpace(0), key).unwrap().unwrap().as_ref(),
        value
    );
    reopened.shutdown().unwrap();
    assert_eq!(fixture.node.resident_grants.live_stats(), (0, 0, 0));
    drop(retained);
}

#[cfg(feature = "experimental-fabric")]
#[test]
#[ignore = "requires libfabric tcp;ofi_rxm software provider"]
fn software_provider_rma_off_history_retirement_drains_accepted_read() {
    use crate::distributed::native_index::IndexReadCut;
    use crate::distributed::remote_db::ConcurrentRemoteDatabase;
    use std::sync::mpsc;

    let fixture = RemoteFixture::new_with_history_window(
        ResidentReadConfig::default(),
        ResidentReadConfig::default(),
        0,
        1,
        1,
    );
    let mut storage = fixture.open();
    let key = b"h001";
    storage
        .write_batch(
            [61; 16],
            vec![Mutation::Put {
                keyspace: KeySpace(0),
                key: key[..].into(),
                value: b"current-value"[..].into(),
            }],
        )
        .unwrap();
    storage
        .write_batch(
            [62; 16],
            vec![Mutation::Put {
                keyspace: KeySpace(0),
                key: b"h002"[..].into(),
                value: b"other-value"[..].into(),
            }],
        )
        .unwrap();
    let database = Arc::new(ConcurrentRemoteDatabase::new(storage).unwrap());
    let log = fixture.node.inventory.lock().active(1).unwrap();
    let first = log
        .lookup(crate::distributed::types::BatchId {
            database: fixture.config.storage.database,
            operation: [61; 16],
        })
        .unwrap()
        .unwrap();
    log.persist_through(first).unwrap();
    let reference = log.record_reference(first, 0).unwrap();
    log.evict_authority_cache();
    assert!(log.try_plan_record_read(&reference).unwrap().is_none());
    let (waiting, release) = log.pause_next_authority_load();

    let reader_db = database.clone();
    let reader = thread::spawn(move || reader_db.get(KeySpace(0), key));
    waiting.recv_timeout(Duration::from_secs(3)).unwrap();
    let (started, starting) = mpsc::sync_channel(1);
    let (retired, retirement_result) = mpsc::sync_channel(1);
    let retirement_db = database.clone();
    let retirement = thread::spawn(move || {
        started.send(()).unwrap();
        retired
            .send(retirement_db.retire_history_before(IndexReadCut::Latest))
            .unwrap();
    });
    starting.recv_timeout(Duration::from_secs(3)).unwrap();
    release.send(()).unwrap();
    retirement_result
        .recv_timeout(Duration::from_secs(3))
        .unwrap()
        .unwrap();
    retirement.join().unwrap();
    assert_eq!(
        reader.join().unwrap().unwrap().unwrap().as_ref(),
        b"current-value"
    );
    assert!(
        database
            .read_io_monitor()
            .sample_resident_read_lanes()
            .unwrap()
            .iter()
            .all(|(_, _, stats)| stats.rma_posts == 0 && stats.rma_hits == 0)
    );
    Arc::try_unwrap(database).ok().unwrap().shutdown().unwrap();
}

#[cfg(feature = "experimental-fabric")]
#[test]
#[ignore = "requires libfabric tcp;ofi_rxm software provider"]
fn software_provider_spontaneous_poison_waits_for_client_release_notification() {
    let fixture = ResidentFixture::new(enabled_source(), enabled_destination());
    let mut client = fixture.client();
    client.fence_to(1).unwrap();
    let (batch, reference) = fixture.publish(&mut client, 70);
    assert_eq!(
        client.read_records(1, &[reference]).unwrap(),
        vec![batch.mutations()[0].clone()]
    );
    assert_eq!(fixture.node.resident_grants.live_stats().1, 1);

    fixture.node.poison_readers();
    assert_eq!(fixture.node.resident_grants.live_stats().1, 1);
    let deadline = Instant::now() + Duration::from_secs(3);
    let _poison_error = loop {
        if let Err(error) = client.progress_resident_control() {
            break error;
        }
        assert!(
            Instant::now() < deadline,
            "spontaneous poison did not drain its registered holder"
        );
        thread::yield_now();
    };
    assert!(client.status().is_err());
    assert_eq!(client.resident_read_stats().handles_live, 0);
    assert_eq!(fixture.node.resident_grants.live_stats(), (0, 0, 0));
    assert!(client.read_records(1, &[reference]).is_err());
}

#[cfg(feature = "experimental-fabric")]
#[test]
#[ignore = "requires libfabric tcp;ofi_rxm software provider"]
fn software_provider_release_immediately_after_rpc_reply_is_accepted() {
    let fixture = ResidentFixture::new(enabled_source(), enabled_destination());
    let mut client = fixture.client();
    client.fence_to(1).unwrap();
    for sequence in 80..84 {
        let (batch, reference) = fixture.publish(&mut client, sequence);
        assert_eq!(
            client.read_records(1, &[reference]).unwrap(),
            vec![batch.mutations()[0].clone()]
        );
        assert_eq!(fixture.node.resident_grants.live_stats().1, 1);
        client.revoke_resident_grants().unwrap();
        assert_eq!(fixture.node.resident_grants.live_stats(), (0, 0, 0));
        assert!(client.status().is_ok());
    }
    client.fence_to(2).unwrap();
}
