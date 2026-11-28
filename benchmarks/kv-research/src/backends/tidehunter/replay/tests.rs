//! Distributed replay over the rootless leased ordinary-file live fixture:
//! real owner processes' control plane in-process, live I/O to local files.
//! Leased live NVMe-oF is the only distributed route, so every test needs it.
#![cfg(feature = "test-utils")]

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
use tidehunter::distributed::durability::DurabilityTier;
use tidehunter::distributed::remote_db::RemoteDatabaseConfig;
use tidehunter::distributed::sui;
use tidehunter::distributed::tcp::{
    LiveTargetPool, OwnerHost, OwnerHostConfig, TcpLimits, TlsFiles, Transport,
};
use tidehunter::metrics::Metrics;

mod native_oracle_tests;

fn trace() -> Trace {
    Trace::load(
        &Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../experiments/sui-mainnet/fixture/manifest.json"),
        "4264e79e0cbe4a1e36d94468df2f049f988eac57877c9bf2e7e175489dc922fa",
    )
    .unwrap()
}
/// Replay tree of one test case. `Drop` removes it also when an assertion
/// panics.
fn root() -> tempfile::TempDir {
    tempfile::TempDir::with_prefix("distributed-mainnet-replay-").expect("create the replay tree")
}
fn tls() -> TlsFiles {
    TlsFiles {
        transport: Transport::TcpTrusted,
        ca: PathBuf::new(),
        cert: PathBuf::new(),
        key: PathBuf::new(),
        server_name: "local-replay-fixture".into(),
    }
}
/// Leased coordinator configuration of node DB `id` with one owner at `address`.
fn remote_config(id: u16, address: &str) -> RemoteDatabaseConfig {
    serde_json::from_value(serde_json::json!({
        "storage":{"partitions":[0],
            "batch_limits":{"max_operations":519,"max_encoded_bytes":500000},
            "group_limits":{"max_batches":32,"max_encoded_bytes":2097152},
            "fragment_bytes":4194304,"wal_file_bytes":16777216,"max_batches_per_partition":8192,
            "max_reference_metadata_bytes":67108864},
        "endpoints":[{"partition":0,"address":address,"server_name":"local-replay-fixture"}],
        "tls":tls(),"timeout":{"secs":30,"nanos":0},"max_admitted_batches":8192,
        "max_pending_batches":64,"max_pending_bytes":33554432,"owner_append_occupancy":16,
        "pipelined_writes":true,"read_cache_bytes":0,"read_memory_allowance_bytes":134217728,
        "read_lanes":2,"pin_read_lanes":false,
        "max_pending_reads":64,"max_pending_read_bytes":16777216,"write_profile_limit":0,
        "live_nvme":{
            "host_nqn":format!("nqn.2026-01.test:mainnet-replay-{id}"),"host_id":vec![id as u8+1;16],
            "max_inflight_requests":16,"max_inflight_bytes":4194304,"connect_timeout_ms":10000,"io_timeout_ms":30000,
            "leased_durability":"native",
            "leased_write_window":1,"leased_sync":"per_frame_fua","leased_flush_min_frame_bytes":0,
            "inline_reads":true,"lease_ahead":true,"polled_writes":false
        }
    }))
    .unwrap()
}
/// Valid leased replay options for the three node databases (also used by the
/// mainnet replay admission test).
pub(crate) fn local_options(root: &Path) -> serde_json::Value {
    serde_json::json!({"databases":(0..3_u16).map(|id| serde_json::json!({"db_id":id,"options":{
        "transport":{"kind":"tcp-trusted"},"path":root.join(format!("db-{id}")),"owners":1,
        "external_remote_config":remote_config(id, &format!("127.0.0.1:{}", 19100 + id)),
    }})).collect::<Vec<_>>()})
}
/// The embedded coordinator document deserializes into the current
/// `RemoteDatabaseConfig` (`deny_unknown_fields`), and a key of a deleted
/// Exact-era setting is refused, never silently ignored.
#[test]
fn embedded_remote_config_parses_and_refuses_deleted_keys() {
    let document = serde_json::to_value(remote_config(0, "127.0.0.1:19100")).unwrap();
    serde_json::from_value::<RemoteDatabaseConfig>(document.clone()).unwrap();
    for key in [
        "index_shards",
        "compactor_cadence",
        "replay_limits",
        "max_index_bytes",
        "scan_prefetch_pages",
    ] {
        let mut stale = document.clone();
        stale[key] = serde_json::json!(1);
        let error = serde_json::from_value::<RemoteDatabaseConfig>(stale)
            .expect_err("a deleted key must be refused")
            .to_string();
        assert!(
            error.contains(&format!("unknown field `{key}`")),
            "{key} must be refused as an unknown field: {error}"
        );
    }
}
#[test]
fn explicit_domains_and_complete_batch_limits_are_admitted_before_open() {
    let trace = trace();
    let guard = root();
    let root = guard.path().to_path_buf();
    let good = local_options(&root);
    TidehunterReplayBackend::validate(&good, &trace).unwrap();
    for field in ["max_operations", "max_encoded_bytes"] {
        let mut bad = good.clone();
        bad["databases"][0]["options"]["external_remote_config"]["storage"]["batch_limits"]
            [field] = serde_json::json!(32);
        assert!(TidehunterReplayBackend::validate(&bad, &trace).is_err());
    }
    let mut bad = good.clone();
    bad["databases"][1]["options"]["external_remote_config"]["storage"]["database"] =
        serde_json::json!(vec![1; 16]);
    assert!(TidehunterReplayBackend::validate(&bad, &trace).is_err());
    let mut bad = good.clone();
    bad["databases"][1]["options"]["database"] = serde_json::json!(vec![1; 16]);
    assert!(TidehunterReplayBackend::validate(&bad, &trace).is_err());
    let mut bad = good.clone();
    bad["databases"][1]["options"]["path"] = bad["databases"][0]["options"]["path"].clone();
    assert!(TidehunterReplayBackend::validate(&bad, &trace).is_err());
    let mut bad = good;
    bad["databases"].as_array_mut().unwrap().pop();
    assert!(TidehunterReplayBackend::validate(&bad, &trace).is_err());
    assert_eq!(std::fs::read_dir(&root).unwrap().count(), 0);
}

struct Owners {
    stop: Arc<AtomicBool>,
    servers: Vec<JoinHandle<()>>,
}
impl Drop for Owners {
    fn drop(&mut self) {
        // Release wakes the owner reactor; join before deleting its WAL directory.
        self.stop.store(true, Ordering::Release);
        for server in self.servers.drain(..) {
            if let Err(failure) = server.join() {
                // A panic inside this guard while the test already unwinds would
                // abort the process and hide the original failure.
                if std::thread::panicking() {
                    continue;
                }
                std::panic::resume_unwind(failure);
            }
        }
    }
}
fn fixture(root: &Path, trace: &Trace) -> (Arc<TidehunterReplayBackend>, Owners) {
    fixture_at(root, trace, &mut BTreeMap::new())
}

/// Build the leased fixture at `root`. `endpoints` keeps every owner address,
/// so a restarted process binds the address that the authority manifest recorded.
fn fixture_at(
    root: &Path,
    trace: &Trace,
    endpoints: &mut BTreeMap<u16, String>,
) -> (Arc<TidehunterReplayBackend>, Owners) {
    let mut owners = Owners {
        stop: Arc::new(AtomicBool::new(false)),
        servers: Vec::new(),
    };
    let mut databases = BTreeMap::new();
    let mut identities = BTreeSet::new();
    for id in trace
        .batches
        .iter()
        .map(|batch| batch.db)
        .collect::<BTreeSet<_>>()
    {
        let base = root.join(format!("db-{id}"));
        std::fs::create_dir_all(&base).unwrap();
        let shape = shape(&trace.tables, id);
        let listener = match endpoints.get(&id) {
            Some(address) => TcpListener::bind(address).unwrap(),
            None => TcpListener::bind("127.0.0.1:0").unwrap(),
        };
        let address = listener.local_addr().unwrap().to_string();
        endpoints.insert(id, address.clone());
        let config = remote_config(id, &address);
        let host_config = OwnerHostConfig {
            root: base.join("owner"),
            max_databases: 1,
            live_target_pool: LiveTargetPool {
                address: "127.0.0.1".into(),
                transport: tidehunter::distributed::nvmeof::NvmeofTransport::Tcp,
                public_port_base: 42,
                public_service_base: 4420,
                commissioning_port_base: 43,
                commissioning_service_base: 4421,
            },
            trusted_coordinator_id: Some([1; 32]),
            wal_member_read_policy: Default::default(),
            wal_prewrite: false,
            max_device_reads_per_status: 64,
            durability: Default::default(),
        };
        let configfs = base.join("configfs");
        for name in ["hosts", "ports", "subsystems"] {
            std::fs::create_dir_all(configfs.join(name)).unwrap();
        }
        let host = OwnerHost::open_with_live_fixture(
            host_config,
            Metrics::new_in_enabled(&prometheus::Registry::new(), false),
            configfs,
        )
        .unwrap();
        let serving = Arc::clone(&host);
        let stop = Arc::clone(&owners.stop);
        owners.servers.push(std::thread::spawn(move || {
            serving
                .serve(listener, tls(), TcpLimits::default(), stop)
                .unwrap()
        }));
        let open_kind = if std::fs::read_dir(base.join("coordinator"))
            .is_ok_and(|mut entries| entries.next().is_some())
        {
            crate::backends::tidehunter::OpenKind::Reopen
        } else {
            crate::backends::tidehunter::OpenKind::Create
        };
        let open_start_ns = tidehunter::distributed::remote_db::diagnostics_clock_ns();
        let db = sui::Db::open_remote_with_local_live_fixture(
            &base.join("coordinator"),
            shape,
            config.clone(),
            false,
        )
        .unwrap();
        let database = db.remote_database_id().unwrap();
        assert_ne!(database, [0; 16]);
        assert!(identities.insert(database));
        assert_eq!(host.databases(), [database]);
        let open_record =
            crate::backends::tidehunter::OpenRecord::new(open_kind, open_start_ns, &db);
        let keyspace = db.ks(&trace
            .tables
            .iter()
            .find(|table| table.db_id == id)
            .unwrap()
            .node_name);
        databases.insert(id, TidehunterBackend {
            db:Some(db),keyspace,relocation_marker:None,
            endpoints:config.endpoints.iter().map(|e|(e.partition,e.address.clone())).collect(),owners:1,
            backend_name:"tidehunter_tcp",durability_tier:DurabilityTier::Native,evidence_path:base.clone(),
            effective_configuration:serde_json::json!({"fixture_route":"leased","database":tidehunter::distributed::tcp::host::identity::database_hex(&database),"remote":config}),
            read_evidence_written:false,owner_sample_gate:Default::default(),coordinator_interval_baseline:std::sync::Mutex::new(None),
            open_record,
            live_write_profile:std::sync::Mutex::new(None),
            #[cfg(all(feature = "test-utils", target_os = "linux"))]
            local_owners: Vec::new(),
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
async fn replay(callers: usize) -> BTreeMap<(u16, Bytes), Bytes> {
    let guard = root();
    let root = guard.path().to_path_buf();
    let trace = trace();
    let prepared = Arc::new(prepare(&trace, 2).unwrap());
    let (backend, owners) = fixture(&root, &trace);
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
    {
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
    let options = serde_json::json!({"databases":backend.databases.iter().map(|(id, backend)| {
        backend.db.as_ref().unwrap().flush().unwrap();
        serde_json::json!({"db_id":id,"options":{
            "transport":{"kind":"tcp-trusted"},"path":root.join(format!("db-{id}")),
            "owners":1,"external_remote_config":backend.effective_configuration["remote"]
        }})
    }).collect::<Vec<_>>()});
    let bound = TidehunterReplayBackend::coordinator_reservation(&options, &trace, 2).unwrap();
    drop(backend);
    drop(owners);
    {
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
            "local-file-replay-disk-bound repetitions=2 callers={callers} actual_bytes={actual} predicted_bytes={predicted} measured_to_bound_ratio={}",
            actual as f64 / predicted as f64
        );
    }
    expected
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_local_file_live_mainnet_replay_matches_one_and_four_callers() {
    assert_eq!(replay(1).await, replay(4).await);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stopped_real_owners_fail_replay_and_authority_health() {
    let guard = root();
    let root = guard.path().to_path_buf();
    let trace = trace();
    let (backend, mut owners) = fixture(&root, &trace);
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
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn perpetual_only_replay_uses_exact_database_population() {
    let guard = root();
    let root = guard.path().to_path_buf();
    let mut trace = trace();
    trace.select_node_databases(&[0]).unwrap();
    let mut options = local_options(&root);
    options["databases"].as_array_mut().unwrap().truncate(1);
    TidehunterReplayBackend::validate(&options, &trace).unwrap();
    let extra = local_options(&root);
    assert!(TidehunterReplayBackend::validate(&extra, &trace).is_err());
    let (backend, owners) = fixture(&root, &trace);
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
    assert_eq!(backend.info().unwrap().topology["node_databases"], 1);
    drop(backend);
    drop(owners);
}

/// The replay's logical WAL bytes are the put cases' counter: the gate-boundary delta of
/// live leased `completed_write_bytes`, divided by successful committed payload.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_local_file_live_replay_records_logical_wal_amplification() {
    let guard = root();
    let root = guard.path().to_path_buf();
    let mut trace = trace();
    trace.select_node_databases(&[0]).unwrap();
    let prepared = Arc::new(prepare(&trace, 2).unwrap());
    let (backend, owners) = fixture(&root, &trace);
    let config: crate::mainnet_replay::ReplayConfig = serde_json::from_value(serde_json::json!({
        "schema_version":1,"backend":{"kind":"tidehunter_tcp","options":{}},
        "workload":{"kind":"sui_mainnet_replay",
            "manifest_path":Path::new(env!("CARGO_MANIFEST_DIR")).join("../../experiments/sui-mainnet/fixture/manifest.json"),
            "manifest_sha256":"4264e79e0cbe4a1e36d94468df2f049f988eac57877c9bf2e7e175489dc922fa",
            "repetitions":2,"minimum_measurement_seconds":0.0,"node_databases":[0]},
        "execution":{"mode":"closed_loop","concurrency":4,"client_shards":1,"queue_capacity":8,
            "request_timeout_ms":30000,"offered_ops_per_second":null},
        "limits":{"max_keys":10000,"max_bytes":67108864,"max_operations":10000,
            "deadline_seconds":120,"shutdown_grace_seconds":5}
    }))
    .unwrap();
    let output = root.join("result");
    let info = backend.info().unwrap();
    crate::mainnet_replay::run_prepared(
        config,
        &output,
        trace,
        prepared,
        Arc::clone(&backend),
        serde_json::json!({}),
        (Instant::now(), 0.0, Some(info)),
    )
    .await
    .unwrap();
    let summary: serde_json::Value =
        serde_json::from_slice(&std::fs::read(output.join("summary.json")).unwrap()).unwrap();
    let amplification = &summary["phase_counts"]["profile"]["write_amplification"];
    let payload = &amplification["payload"];
    assert_eq!(
        *payload,
        serde_json::json!({"commits":6,"rows":1308,"value_bytes":1_000_648,"key_bytes":47_664,"key_value_bytes":1_048_312}),
        "{amplification}"
    );
    let wal = &amplification["logical_wal"];
    assert_eq!(wal["available"], true, "{amplification}");
    let evidence: serde_json::Value = serde_json::from_slice(
        &std::fs::read(output.join("measurement-read-evidence.json")).unwrap(),
    )
    .unwrap();
    let counters = &evidence["live_nvmeof"]["counters"];
    let bytes = wal["bytes"].as_u64().unwrap();
    assert_eq!(bytes, counters["completed_write_bytes"].as_u64().unwrap());
    assert_eq!(wal["write_ops"], counters["completed_write_ops"]);
    assert_eq!(wal["leased_frames"], counters["leased_frames"]);
    // One complete-batch frame per successful commit; each frame carries its keys and values.
    assert_eq!(wal["leased_frames"], payload["commits"]);
    assert!(bytes >= 1_048_312, "{amplification}");
    // serde_json parses floats to within one ULP (no `float_roundtrip`), so the summary's
    // ratios are compared to the exact quotient with a relative tolerance of a few ULPs.
    for (name, payload_bytes) in [
        ("bytes_per_value_byte", 1_000_648.0),
        ("bytes_per_key_value_byte", 1_048_312.0),
    ] {
        let recorded = wal[name].as_f64().unwrap();
        let exact = bytes as f64 / payload_bytes;
        assert!(
            (recorded - exact).abs() <= 4.0 * f64::EPSILON * exact,
            "{name}: {recorded} != {exact}"
        );
    }
    eprintln!(
        "local-file-live-replay-amplification bytes={bytes} frames={} wal_per_value={} wal_per_key_value={}",
        wal["leased_frames"], wal["bytes_per_value_byte"], wal["bytes_per_key_value_byte"]
    );
    drop(backend);
    drop(owners);
}

/// A real local file replay through `run_prepared` writes its result
/// document: the run reports completion with no unresolved call, the
/// committed payload is counted, and the gate-boundary read evidence names
/// the owner write counters for the single-database TCP backend.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_local_file_live_replay_writes_its_result_document() {
    let guard = root();
    let root = guard.path().to_path_buf();
    let mut trace = trace();
    trace.select_node_databases(&[0]).unwrap();
    let prepared = Arc::new(prepare(&trace, 2).unwrap());
    let (backend, owners) = fixture(&root, &trace);
    let config: crate::mainnet_replay::ReplayConfig = serde_json::from_value(serde_json::json!({
        "schema_version":1,"backend":{"kind":"tidehunter_tcp","options":{}},
        "workload":{"kind":"sui_mainnet_replay",
            "manifest_path":Path::new(env!("CARGO_MANIFEST_DIR")).join("../../experiments/sui-mainnet/fixture/manifest.json"),
            "manifest_sha256":"4264e79e0cbe4a1e36d94468df2f049f988eac57877c9bf2e7e175489dc922fa",
            "repetitions":2,"minimum_measurement_seconds":0.0,"node_databases":[0]},
        "execution":{"mode":"closed_loop","concurrency":4,"client_shards":1,"queue_capacity":8,
            "request_timeout_ms":30000,"offered_ops_per_second":null},
        "limits":{"max_keys":10000,"max_bytes":67108864,"max_operations":10000,
            "deadline_seconds":120,"shutdown_grace_seconds":5}
    }))
    .unwrap();
    let output = root.join("result");
    let info = backend.info().unwrap();
    crate::mainnet_replay::run_prepared(
        config,
        &output,
        trace,
        prepared,
        Arc::clone(&backend),
        serde_json::json!({"binary":{"sha256":"test"}}),
        (Instant::now(), 0.0, Some(info)),
    )
    .await
    .unwrap();
    let summary: serde_json::Value =
        serde_json::from_slice(&std::fs::read(output.join("summary.json")).unwrap()).unwrap();
    assert_eq!(summary["status"], "completed", "{summary}");
    let phase = &summary["phase_counts"];
    assert_eq!(phase["run_status"], "completed", "{phase}");
    assert_eq!(phase["unresolved_workers"], 0, "{phase}");
    assert_eq!(phase["unknown_outcomes"], 0, "{phase}");
    assert_eq!(phase["error"], serde_json::Value::Null, "{phase}");
    // One commit call per batch frame; each frame carries its keys and values.
    let counts = &phase["counts"];
    assert_eq!(counts["write_calls"], 6, "{counts}");
    assert_eq!(counts["put_members"], 1308, "{counts}");
    let profile = &phase["profile"];
    assert_eq!(profile["functional_qualification"], true, "{profile}");
    assert_eq!(profile["minimum_measurement_seconds"], 0.0, "{profile}");
    // The same run writes the gate-boundary read evidence; the owner writer
    // counters are the observation the amplification report is built on.
    let evidence: serde_json::Value = serde_json::from_slice(
        &std::fs::read(output.join("measurement-read-evidence.json")).unwrap(),
    )
    .unwrap();
    let live = &evidence["live_nvmeof"];
    assert_eq!(live["available"], true, "{evidence}");
    assert!(
        live["counters"]["completed_write_ops"].as_u64().unwrap() > 0,
        "{evidence}"
    );
    drop(backend);
    drop(owners);
}

#[test]
fn info_rejects_an_empty_database_set() {
    // `options()` refuses an empty database list before `open`, so this arm is
    // reachable only through a backend built from that internal state; the test
    // pins the arm's contract instead of leaving it untested.
    let backend = TidehunterReplayBackend {
        databases: BTreeMap::new(),
        tables: Vec::new(),
    };
    let error = backend
        .info()
        .expect_err("an empty set must not report info");
    assert_eq!(
        format!("{error}"),
        "distributed replay database set is empty",
        "the empty-set arm names its contract"
    );
}
