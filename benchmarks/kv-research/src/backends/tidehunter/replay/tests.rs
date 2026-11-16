use super::*;
use crate::mainnet_replay::{execute, prepare};
use std::collections::BTreeMap;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use tidehunter::distributed::durability::{DurabilityConfig, DurabilityTier};
use tidehunter::distributed::remote_db::RemoteDatabaseConfig;
use tidehunter::distributed::sui;
use tidehunter::distributed::tcp::{NodeConfig, StorageNode, TcpLimits, TlsFiles, Transport};
use tidehunter::metrics::Metrics;

fn trace() -> Trace {
    Trace::load(
        &Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../experiments/sui-mainnet/fixture/manifest.json"),
        "4264e79e0cbe4a1e36d94468df2f049f988eac57877c9bf2e7e175489dc922fa",
    )
    .unwrap()
}
fn root() -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "distributed-mainnet-replay-{}-{}",
        std::process::id(),
        rand::random::<u64>()
    ));
    std::fs::create_dir_all(&path).unwrap();
    path
}
fn local_options(root: &Path) -> serde_json::Value {
    serde_json::json!({"databases":(0..3).map(|id| serde_json::json!({"db_id":id,"options":{
        "transport":{"kind":"tcp-trusted"},"path":root.join(format!("db-{id}")),"database":vec![id+1;16],
        "owners":1,"owner_wal_geometry":"research","batch_limits":{"max_operations":519,"max_encoded_bytes":500000}
    }})).collect::<Vec<_>>()})
}
#[test]
fn explicit_domains_and_complete_batch_limits_are_admitted_before_open() {
    let trace = trace();
    let root = root();
    let good = local_options(&root);
    TidehunterReplayBackend::validate(&good, &trace).unwrap();
    for field in ["max_operations", "max_encoded_bytes"] {
        let mut bad = good.clone();
        bad["databases"][0]["options"]["batch_limits"][field] = serde_json::json!(32);
        assert!(TidehunterReplayBackend::validate(&bad, &trace).is_err());
    }
    let mut bad = good.clone();
    bad["databases"][1]["options"]["database"] = bad["databases"][0]["options"]["database"].clone();
    assert!(TidehunterReplayBackend::validate(&bad, &trace).is_err());
    let mut bad = good.clone();
    bad["databases"][1]["options"]["path"] = bad["databases"][0]["options"]["path"].clone();
    assert!(TidehunterReplayBackend::validate(&bad, &trace).is_err());
    let mut bad = good;
    bad["databases"].as_array_mut().unwrap().pop();
    assert!(TidehunterReplayBackend::validate(&bad, &trace).is_err());
    assert_eq!(std::fs::read_dir(&root).unwrap().count(), 0);
    std::fs::remove_dir_all(root).unwrap();
}

struct Owners {
    nodes: Vec<Arc<StorageNode>>,
    stop: Arc<AtomicBool>,
    servers: Vec<JoinHandle<()>>,
}
impl Drop for Owners {
    fn drop(&mut self) {
        // Release wakes the owner reactor; join before deleting its WAL directory.
        self.stop.store(true, Ordering::Release);
        for server in self.servers.drain(..) {
            server.join().unwrap();
        }
    }
}
fn fixture(
    root: &Path,
    trace: &Trace,
    live: Option<&str>,
) -> (Arc<TidehunterReplayBackend>, Owners) {
    let mut owners = Owners {
        nodes: Vec::new(),
        stop: Arc::new(AtomicBool::new(false)),
        servers: Vec::new(),
    };
    let mut databases = BTreeMap::new();
    for id in trace
        .batches
        .iter()
        .map(|batch| batch.db)
        .collect::<BTreeSet<_>>()
    {
        let base = root.join(format!("db-{id}"));
        std::fs::create_dir_all(&base).unwrap();
        let shape = shape(&trace.tables, id);
        let tls = TlsFiles {
            transport: Transport::TcpTrusted,
            ca: PathBuf::new(),
            cert: PathBuf::new(),
            key: PathBuf::new(),
            server_name: "local-replay-fixture".into(),
        };
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let mut config: RemoteDatabaseConfig = serde_json::from_value(serde_json::json!({
            "storage":{"database":vec![id as u8+1;16],"partitions":[0],
                "batch_limits":{"max_operations":519,"max_encoded_bytes":500000},
                "group_limits":{"max_batches":32,"max_encoded_bytes":2097152},
                "fragment_bytes":4194304,"wal_file_bytes":16777216,"max_batches_per_partition":8192,
                "max_reference_metadata_bytes":67108864,"max_epochs":4},
            "endpoints":[{"partition":0,"address":listener.local_addr().unwrap().to_string(),"server_name":"local-replay-fixture"}],
            "tls":tls,"timeout":{"secs":30,"nanos":0},"max_admitted_batches":8192,"max_index_bytes":67108864,
            "max_pending_batches":64,"max_pending_bytes":33554432,"owner_append_occupancy":16,
            "pipelined_writes":true,"read_cache_bytes":0,"read_memory_allowance_bytes":134217728,
            "preparation_workers":2,"scan_prefetch_pages":1,"read_lanes":2,"pin_read_lanes":false,
            "max_pending_reads":64,"max_pending_read_bytes":16777216,"write_profile_limit":0,"index_shards":64
        })).unwrap();
        config.history_persistence =
            tidehunter::distributed::storage::HistoryPersistence::WalReplay;
        if let Some(mode) = live {
            config.live_nvme = Some(serde_json::from_value(serde_json::json!({
                "host_nqn":format!("nqn.2026-01.test:mainnet-replay-{id}"),"host_id":vec![id as u8+1;16],
                "max_inflight_requests":16,"max_inflight_bytes":4194304,"connect_timeout_ms":10000,"io_timeout_ms":30000,
                "write_mode":mode,"leased_durability":if mode=="leased" {Some("native")} else {None},
                "leased_write_window":1,"leased_sync":"per_frame_fua","leased_flush_min_frame_bytes":0,
                "inline_reads":true,"lease_ahead":true,"polled_writes":false
            })).unwrap());
        }
        let node_config = NodeConfig {
            root: base.join("owner"),
            partition: tidehunter::distributed::storage::PartitionConfig {
                inventory: tidehunter::distributed::storage::StaticInventory {
                    database: config.storage.database,
                    epoch: 1,
                    segment_generation: 1,
                    partitions: config.storage.partitions.clone(),
                },
                partition: 0,
                batch_limits: config.storage.batch_limits,
                group_limits: config.storage.group_limits,
                fragment_bytes: config.storage.fragment_bytes,
                wal_file_bytes: config.storage.wal_file_bytes,
                max_batches: config.storage.max_batches_per_partition,
                max_reference_metadata_bytes: config.storage.max_reference_metadata_bytes,
            },
            shape_yaml: serde_yaml::to_string(&shape).unwrap(),
            max_epochs: 4,
            read_workers: Some(2),
            wal_member_read: Default::default(),
            wal_prewrite: false,
            read_resident_first: false,
            read_memory_allowance_bytes: 134217728,
            read_max_inflight_requests: 64,
            read_max_inflight_bytes: 16777216,
            read_max_device_reads: 64,
            live_target: None,
        };
        let owner_read = super::super::ExternalOwnerReadConfig {
            partition: node_config.partition.partition,
            read_workers: node_config.read_workers,
            wal_member_read: node_config.wal_member_read,
            read_resident_first: node_config.read_resident_first,
            read_memory_allowance_bytes: node_config.read_memory_allowance_bytes,
            read_max_inflight_requests: node_config.read_max_inflight_requests,
            read_max_inflight_bytes: node_config.read_max_inflight_bytes,
            read_max_device_reads: node_config.read_max_device_reads,
        };
        let node = StorageNode::open(
            node_config,
            Metrics::new_in_enabled(&prometheus::Registry::new(), false),
        )
        .unwrap();
        node.set_durability(DurabilityConfig::with_tier(DurabilityTier::Native))
            .unwrap();
        node.set_history_persistence(config.history_persistence)
            .unwrap();
        #[cfg(all(feature = "nvmeof", feature = "test-utils", target_os = "linux"))]
        if live.is_some() {
            let state = base.join("target");
            let configfs = base.join("configfs");
            std::fs::create_dir_all(&state).unwrap();
            for name in ["hosts", "ports", "subsystems"] {
                std::fs::create_dir_all(configfs.join(name)).unwrap();
            }
            node.set_live_target_fixture(
                tidehunter::distributed::nvmeof::LiveTargetConfig {
                    address: "127.0.0.1".into(),
                    service: 4420,
                    transport: tidehunter::distributed::nvmeof::NvmeofTransport::Tcp,
                    host_nqn: config.live_nvme.as_ref().unwrap().host_nqn.clone(),
                    port_id: 42,
                    state_dir: state,
                    commissioning_port_id: 43,
                    commissioning_service: 4421,
                    spdk: None,
                    export_mode: Default::default(),
                },
                configfs,
            )
            .unwrap();
        }
        let serving = Arc::clone(&node);
        let stop = Arc::clone(&owners.stop);
        owners.servers.push(std::thread::spawn(move || {
            serving
                .serve(listener, tls, TcpLimits::default(), stop)
                .unwrap()
        }));
        owners.nodes.push(node);
        #[cfg(all(feature = "nvmeof", feature = "test-utils", target_os = "linux"))]
        let db = if live.is_some() {
            sui::Db::open_remote_with_local_live_fixture(
                &base.join("coordinator"),
                shape,
                config.clone(),
                false,
            )
            .unwrap()
        } else {
            sui::Db::open_remote(&base.join("coordinator"), shape, config.clone()).unwrap()
        };
        #[cfg(not(all(feature = "nvmeof", feature = "test-utils", target_os = "linux")))]
        let db = {
            assert!(live.is_none());
            sui::Db::open_remote(&base.join("coordinator"), shape, config.clone()).unwrap()
        };
        let keyspace = db.ks(&trace
            .tables
            .iter()
            .find(|table| table.db_id == id)
            .unwrap()
            .node_name);
        databases.insert(id, TidehunterBackend {
            db:Some(db),keyspace,cluster:std::sync::Mutex::new(None),
            endpoints:config.endpoints.iter().map(|e|(e.partition,e.address.clone())).collect(),owners:1,
            backend_name:"tidehunter_tcp",durability_tier:DurabilityTier::Native,evidence_path:base.clone(),
            effective_configuration:serde_json::json!({"fixture_route":live.unwrap_or("owner_rpc"),"remote":config,"owner_read":owner_read}),operational_read_io:None,
            read_evidence_written:false,coordinator_interval_baseline:std::sync::Mutex::new(None),
            #[cfg(all(feature="nvmeof",target_os="linux"))]
            live_write_profile:std::sync::Mutex::new(None),
        });
    }
    let tables = trace
        .tables
        .iter()
        .map(|t| {
            databases
                .get(&t.db_id)
                .map(|backend| (t.db_id, backend.db.as_ref().unwrap().ks(&t.node_name)))
        })
        .collect();
    (
        Arc::new(TidehunterReplayBackend { databases, tables }),
        owners,
    )
}
async fn replay(route: Option<&str>, callers: usize) -> BTreeMap<(u16, Bytes), Bytes> {
    let root = root();
    let trace = trace();
    let prepared = Arc::new(prepare(&trace, 2).unwrap());
    let (backend, owners) = fixture(&root, &trace, route);
    let counts = execute(
        Arc::clone(&backend),
        &trace,
        Arc::clone(&prepared),
        callers,
        Instant::now() + Duration::from_secs(120),
        30000,
    )
    .await
    .unwrap();
    assert_eq!(
        (
            counts.store_gets,
            counts.overlay_demands,
            counts.put_members,
            counts.write_calls,
            counts.transactions,
            counts.checkpoints
        ),
        (1014, 232, 1414, 18, 90, 2)
    );
    assert_eq!(trace.preload.len(), 376);
    assert_eq!(
        trace.preload.iter().map(|r| r.value.len()).sum::<usize>(),
        191216
    );
    assert_eq!(
        trace.preload.iter().map(|r| r.key.len()).sum::<usize>(),
        15040
    );
    assert_eq!(
        trace
            .batches
            .iter()
            .flat_map(|b| &b.entries)
            .map(|r| r.value.len())
            .sum::<usize>(),
        644573
    );
    assert_eq!(
        trace
            .reads
            .iter()
            .flatten()
            .filter(|r| r.producer.is_none())
            .map(|r| r.row.value.len())
            .sum::<usize>(),
        233123
    );
    let mut expected = BTreeMap::new();
    for p in prepared.iter() {
        assert_eq!(
            p.batches
                .iter()
                .map(|b| b.entries.len())
                .collect::<Vec<_>>(),
            [2, 1, 90, 3, 1, 45, 45, 519, 1]
        );
        for row in p
            .preload
            .iter()
            .chain(p.batches.iter().flat_map(|b| &b.entries))
        {
            expected.insert((row.table, row.key.clone()), row.value.clone());
        }
    }
    for ((table, key), value) in &expected {
        assert_eq!(backend.get(*table, key).unwrap().as_ref(), Some(value));
    }
    let original = prepared[0].batches[7].entries[0].clone();
    let changed = Row {
        value: Bytes::from_static(b"must not appear"),
        ..original.clone()
    };
    assert!(
        backend
            .commit(0, &[changed, prepared[0].batches[0].entries[0].clone()])
            .is_err()
    );
    assert_eq!(
        backend.get(original.table, &original.key).unwrap().unwrap(),
        original.value
    );
    backend.check_background_health().unwrap();
    #[cfg(all(feature = "nvmeof", feature = "test-utils", target_os = "linux"))]
    if route.is_some() {
        let stats: Vec<_> = backend
            .databases
            .values()
            .map(|b| b.db.as_ref().unwrap().live_nvmeof_stats().unwrap())
            .collect();
        let evidence = serde_json::to_value(stats).unwrap();
        std::fs::write(
            root.join("live-fixture-evidence.json"),
            serde_json::to_vec_pretty(&evidence).unwrap(),
        )
        .unwrap();
        // These counters establish actual local-file live data traffic, not merely configuration.
        assert!(
            evidence
                .as_array()
                .unwrap()
                .iter()
                .all(|s| s["completed_write_bytes"].as_u64().unwrap_or(0) > 0),
            "{evidence}"
        );
        assert!(
            evidence[0]["completed_read_bytes"].as_u64().unwrap_or(0) > 0,
            "{evidence}"
        );
    }
    let metadata_bound = if route.is_some() {
        let options = serde_json::json!({"databases":backend.databases.iter().map(|(id, backend)| {
            backend.db.as_ref().unwrap().flush().unwrap();
            serde_json::json!({"db_id":id,"options":{
                "transport":{"kind":"tcp-trusted"},"path":root.join(format!("db-{id}")),
                "owners":1,"external_remote_config":backend.effective_configuration["remote"],
                "external_owner_read_config":[backend.effective_configuration["owner_read"]],
                "owner_read_resident_first":false
            }})
        }).collect::<Vec<_>>()});
        Some(TidehunterReplayBackend::coordinator_reservation(&options, &trace, 2).unwrap())
    } else {
        None
    };
    drop(backend);
    drop(owners);
    if let Some(bound) = metadata_bound {
        let mut actual = 0_u64;
        for db in trace
            .batches
            .iter()
            .map(|batch| batch.db)
            .collect::<BTreeSet<_>>()
        {
            let output = std::process::Command::new("du")
                .args(["-s", "-B1"])
                .arg(root.join(format!("db-{db}/coordinator")))
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            actual += std::str::from_utf8(&output.stdout)
                .unwrap()
                .split_whitespace()
                .next()
                .unwrap()
                .parse::<u64>()
                .unwrap();
        }
        let predicted = bound["reservation_bytes"].as_u64().unwrap();
        assert!(actual <= predicted, "actual={actual}; bound={bound}");
        eprintln!(
            "local-file-replay-disk-bound route={route:?} repetitions=2 callers={callers} actual_bytes={actual} predicted_bytes={predicted} measured_to_bound_ratio={}",
            actual as f64 / predicted as f64
        );
    }
    std::fs::remove_dir_all(root).unwrap();
    expected
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_distributed_mainnet_replay_matches_one_and_four_callers() {
    assert_eq!(replay(None, 1).await, replay(None, 4).await);
}
#[cfg(all(feature = "nvmeof", feature = "test-utils", target_os = "linux"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_local_file_live_mainnet_replay_matches_owner_rpc() {
    let rpc = replay(None, 4).await;
    assert_eq!(rpc, replay(Some("reserved"), 4).await);
    assert_eq!(rpc, replay(Some("leased"), 4).await);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn poisoned_owner_lifecycle_fails_replay_health_boundary() {
    let root = root();
    let trace = trace();
    let (backend, owners) = fixture(&root, &trace, None);
    let clone = Arc::clone(&backend);
    assert!(
        std::thread::spawn(move || {
            let _guard = clone.databases[&0].cluster.lock().unwrap();
            panic!("poison replay lifecycle");
        })
        .join()
        .is_err()
    );
    let error = execute(
        Arc::clone(&backend),
        &trace,
        Arc::new(prepare(&trace, 1).unwrap()),
        1,
        Instant::now() + Duration::from_secs(120),
        30000,
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("poisoned"), "{error}");
    drop(backend);
    drop(owners);
    std::fs::remove_dir_all(root).unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stopped_real_owners_fail_replay_and_authority_health_without_test_utils() {
    let root = root();
    let trace = trace();
    let (backend, mut owners) = fixture(&root, &trace, None);
    // Stop the actual owner reactors, not a wrapper health hook. Release pairs
    // with their shutdown checks; joining makes the failure deterministic.
    owners.stop.store(true, Ordering::Release);
    for server in owners.servers.drain(..) {
        server.join().unwrap();
    }
    let error = execute(
        Arc::clone(&backend),
        &trace,
        Arc::new(prepare(&trace, 1).unwrap()),
        1,
        Instant::now() + Duration::from_secs(120),
        30000,
    )
    .await
    .unwrap_err();
    let health = backend.check_background_health().unwrap_err();
    assert!(
        health.to_string().contains("requires recovery"),
        "{error}; {health}"
    );
    drop(backend);
    drop(owners);
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(feature = "test-utils")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unknown_owner_append_fails_replay_and_poisoned_authority_health() {
    let root = root();
    let trace = trace();
    let (backend, owners) = fixture(&root, &trace, None);
    owners.nodes[0].inject_drop_next_append_response();
    let error = execute(
        Arc::clone(&backend),
        &trace,
        Arc::new(prepare(&trace, 1).unwrap()),
        1,
        Instant::now() + Duration::from_secs(120),
        30000,
    )
    .await
    .unwrap_err();
    let reason = error.to_string();
    assert!(
        reason.contains("stage=transport_progress")
            && reason.contains("truncated transport frame")
            && reason.contains("batch=Some(BatchVersion { epoch: 1, sequence: 0 })"),
        "{reason}"
    );
    let health = backend.check_background_health().unwrap_err();
    assert!(health.to_string().contains("requires recovery"), "{health}");
    drop(backend);
    drop(owners);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn perpetual_only_replay_uses_exact_database_population() {
    let root = root();
    let mut trace = trace();
    trace.select_node_databases(&[0]).unwrap();
    let mut options = local_options(&root);
    options["databases"].as_array_mut().unwrap().truncate(1);
    TidehunterReplayBackend::validate(&options, &trace).unwrap();
    let extra = local_options(&root);
    assert!(TidehunterReplayBackend::validate(&extra, &trace).is_err());
    let (backend, owners) = fixture(&root, &trace, None);
    let counts = execute(
        backend.clone(),
        &trace,
        Arc::new(prepare(&trace, 2).unwrap()),
        4,
        Instant::now() + Duration::from_secs(30),
        30000,
    )
    .await
    .unwrap();
    assert_eq!(
        (
            counts.store_gets,
            counts.overlay_demands,
            counts.put_members,
            counts.write_calls
        ),
        (1014, 232, 1308, 6)
    );
    assert_eq!(backend.info().topology["node_databases"], 1);
    drop(backend);
    drop(owners);
    std::fs::remove_dir_all(root).unwrap();
}
