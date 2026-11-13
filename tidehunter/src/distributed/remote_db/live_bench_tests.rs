//! Opt-in workstation latency measurements; never hardware qualification.
use super::*;
use crate::distributed::codec::BatchLimits;
use crate::distributed::durability::{DurabilityConfig, DurabilityTier};
use crate::distributed::group::GroupLimits;
use crate::distributed::nvmeof::{
    LiveTargetConfig, NvmeofTransport,
    live_metrics::{LiveNvmeCounts, LiveNvmeMetrics},
    live_target::LiveTargetManager,
};
use crate::distributed::tcp::{
    LiveClientConfig, LiveWriteMode, NodeConfig, StorageNode, Transport,
};
use crate::key_shape::KeyType;
use serde_json::{Value, json};
use std::io::Write;
use std::net::TcpListener;
use std::process::Command as Process;
use std::sync::Barrier;
use std::sync::atomic::{AtomicBool, Ordering};

const OUTPUT: &str = "/home/hybr1d/Desktop/PDM/TideHunter/checkpoint-work/p11-local-bench";
const CACHE: &str = "/home/hybr1d/.cache/tidehunter";
const MIB: usize = 1024 * 1024;

fn metrics() -> Arc<Metrics> {
    Metrics::new_in_enabled(&prometheus::Registry::new(), false)
}
fn live_config(mode: LiveWriteMode, tier: DurabilityTier) -> LiveClientConfig {
    LiveClientConfig {
        lease_ahead: true,
        polled_writes: false,
        host_nqn: "nqn.2026-01.test:p11-bench".into(),
        host_id: [1; 16],
        owner_identities: Default::default(),
        max_inflight_requests: 32,
        max_inflight_bytes: 4 * MIB,
        connect_timeout_ms: 10000,
        io_timeout_ms: 30000,
        write_mode: mode,
        leased_durability: (mode == LiveWriteMode::Leased).then_some(tier),
        inline_reads: true,
        queue_geometry: Default::default(),
        export_mode: Default::default(),
    }
}
fn key(sequence: usize) -> Vec<u8> {
    let mut key = vec![0; 32];
    key[..8].copy_from_slice(&(sequence as u64).to_be_bytes());
    key
}
fn put(sequence: usize, value: &Bytes) -> Mutation {
    Mutation::Put {
        keyspace: KeySpace(0),
        key: key(sequence).into(),
        value: value.clone(),
    }
}
fn operation(sequence: usize) -> [u8; 16] {
    (sequence as u128 + 1).to_le_bytes()
}

struct Fixture {
    node: Arc<StorageNode>,
    config: RemoteDatabaseConfig,
    shape: KeyShape,
    live: Arc<LiveNvmeMetrics>,
    tier: DurabilityTier,
    stop: Arc<AtomicBool>,
    server: Option<JoinHandle<()>>,
    // Drop the owner and its WAL mapper before removing the fixture directory.
    root: tempdir::TempDir,
}
impl Fixture {
    fn new(base: &Path, case: &str, tier: DurabilityTier, arm: &str) -> Self {
        Self::with_limits(
            base,
            case,
            tier,
            arm,
            128 * MIB as u64,
            256 * MIB as u64,
            ReplayLimits {
                max_batches: 65536,
                max_encoded_bytes: 1024 * MIB,
            },
        )
    }

    fn with_limits(
        base: &Path,
        case: &str,
        tier: DurabilityTier,
        arm: &str,
        fragment_bytes: u64,
        wal_file_bytes: u64,
        replay_limits: ReplayLimits,
    ) -> Self {
        let directory = tempdir::TempDir::new_in(base, case).unwrap();
        let root = directory.path();
        let shape = KeyShape::new_single(32, 1, KeyType::uniform(1));
        let tls = TlsFiles {
            transport: Transport::TcpTrusted,
            ca: PathBuf::new(),
            cert: PathBuf::new(),
            key: PathBuf::new(),
            server_name: "trusted-test-cluster".into(),
        };
        let mut config = RemoteDatabaseConfig {
            history_persistence: HistoryPersistence::default(),
            storage: LocalInventoryConfig {
                database: [81; 16],
                partitions: vec![0],
                batch_limits: BatchLimits {
                    max_operations: 32,
                    max_encoded_bytes: 128 * 1024,
                },
                group_limits: GroupLimits {
                    max_batches: 32,
                    max_encoded_bytes: 2 * MIB,
                },
                fragment_bytes,
                wal_file_bytes,
                max_batches_per_partition: 8192,
                max_reference_metadata_bytes: 64 * MIB,
                max_epochs: 8,
            },
            endpoints: Vec::new(),
            tls: tls.clone(),
            timeout: Duration::from_secs(60),
            max_admitted_batches: 8192,
            max_index_bytes: 64 * MIB,
            index_shards: 64,
            max_pending_batches: 64,
            max_pending_bytes: 32 * MIB,
            replay_limits,
            owner_append_occupancy: 16,
            pipelined_writes: true,
            read_cache_bytes: 0,
            read_memory_allowance_bytes: 128 * MIB,
            preparation_workers: std::env::var("P11_BENCH_PREPARATION_WORKERS")
                .map(|value| value.parse().unwrap())
                .unwrap_or(1),
            scan_prefetch_pages: 1,
            read_lanes: 1,
            max_pending_reads: 64,
            max_pending_read_bytes: 16 * MIB,
            write_profile_limit: 0,
            live_nvme: (arm != "A").then(|| {
                live_config(
                    if arm == "B" {
                        LiveWriteMode::Reserved
                    } else {
                        LiveWriteMode::Leased
                    },
                    tier,
                )
            }),
        };
        let node = StorageNode::open(
            NodeConfig {
                root: root.join("owner"),
                partition: config.partition_config(1, 0),
                shape_yaml: shape.to_yaml().unwrap(),
                max_epochs: 8,
                read_workers: 1,
                wal_member_read: Default::default(),
                read_resident_first: false,
                read_memory_allowance_bytes: 128 * MIB,
                read_max_inflight_requests: 64,
                read_max_inflight_bytes: 16 * MIB,
                read_max_device_reads: 64,
                live_target: None,
            },
            metrics(),
        )
        .unwrap();
        node.set_history_persistence(config.history_persistence)
            .unwrap();
        node.set_durability(DurabilityConfig::with_tier(tier))
            .unwrap();
        if arm != "A" {
            let state_dir = root.join("target");
            let configfs = root.join("configfs");
            fs::create_dir(&state_dir).unwrap();
            for name in ["hosts", "ports", "subsystems"] {
                fs::create_dir_all(configfs.join(name)).unwrap();
            }
            let target = LiveTargetManager::open_fixture(
                LiveTargetConfig {
                    address: "127.0.0.1".into(),
                    service: 4420,
                    transport: NvmeofTransport::Tcp,
                    host_nqn: "nqn.2026-01.test:p11-bench".into(),
                    port_id: 42,
                    state_dir,
                    commissioning_port_id: 43,
                    commissioning_service: 4421,
                    spdk: None,
                    export_mode: Default::default(),
                },
                configfs,
            )
            .unwrap();
            node.bench_install_live_target(target);
        }
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        config.endpoints.push(RemoteEndpoint {
            partition: 0,
            address: listener.local_addr().unwrap().to_string(),
            server_name: tls.server_name.clone(),
        });
        let stop = Arc::new(AtomicBool::new(false));
        let serving = node.clone();
        let stopping = stop.clone();
        let server = thread::spawn(move || {
            serving
                .serve(
                    listener,
                    tls,
                    super::super::tcp::TcpLimits {
                        io_timeout: Duration::from_secs(60),
                        ..Default::default()
                    },
                    stopping,
                )
                .unwrap()
        });
        Self {
            root: directory,
            node,
            config,
            shape,
            live: LiveNvmeMetrics::new(),
            tier,
            stop,
            server: Some(server),
        }
    }
    fn open(&self, arm: &str) -> RemoteDatabase {
        let db = RemoteDatabase::open(
            &self.root.path().join("authority"),
            &self.shape,
            self.config.clone(),
            metrics(),
        )
        .unwrap();
        if arm != "A" {
            for worker in &db.clients {
                let mut client = worker.client.lock();
                client
                    .configure_live_nvme(&live_config(
                        if arm == "B" {
                            LiveWriteMode::Reserved
                        } else {
                            LiveWriteMode::Leased
                        },
                        self.tier,
                    ))
                    .unwrap();
                client.bench_local_live_fixture(arm == "C-buf");
                client.set_live_metrics(self.live.clone()).unwrap();
            }
        }
        db
    }
    fn owner_stats(&self) -> PartitionStats {
        self.node.log_for_test(1).stats()
    }
    fn owner_syncer_calls(&self) -> Option<u64> {
        self.node
            .log_for_test(1)
            .frontier()
            .map(|frontier| frontier.stats().fsync_calls)
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        // Release pairs with the real owner service's shutdown check.
        self.stop.store(true, Ordering::Release);
        if let Some(server) = self.server.take() {
            server.join().unwrap();
        }
    }
}

#[test]
fn owner_initiator_map_is_pinned_across_authority_reopen() {
    use crate::distributed::tcp::LiveInitiatorIdentity;
    let base = tempdir::TempDir::new("live-owner-identities").unwrap();
    let mut f = Fixture::new(base.path(), "owner-map", DurabilityTier::Native, "B");
    let identity = LiveInitiatorIdentity {
        host_nqn: f.config.live_nvme.as_ref().unwrap().host_nqn.clone(),
        host_id: [2; 16],
    };
    f.config
        .live_nvme
        .as_mut()
        .unwrap()
        .owner_identities
        .insert(0, identity.clone());
    let root = f.root.path().join("authority");
    let open = |config| RemoteDatabase::open(&root, &f.shape, config, metrics());
    let first = open(f.config.clone()).unwrap();
    super::tests::fail_stop_without_checkpoint(first);
    let same = open(f.config.clone()).unwrap();
    super::tests::fail_stop_without_checkpoint(same);
    for change in 0..4 {
        let mut config = f.config.clone();
        let live = config.live_nvme.as_mut().unwrap();
        match change {
            0 => live.owner_identities.get_mut(&0).unwrap().host_id = [3; 16],
            1 => live
                .owner_identities
                .get_mut(&0)
                .unwrap()
                .host_nqn
                .push_str("-changed"),
            2 => live.owner_identities.clear(),
            _ => {
                live.owner_identities.insert(1, identity.clone());
            }
        }
        assert!(open(config).is_err(), "accepted changed map {change}");
    }
    let mut manifest: AuthorityManifest = read_metadata(&root.join(MANIFEST)).unwrap();
    manifest.live_nvme_owner_identities.clear();
    persist_manifest(&root, &manifest).unwrap();
    assert!(
        open(f.config.clone())
            .err()
            .unwrap()
            .to_string()
            .contains("authority configuration/history differs")
    );
    f.config
        .live_nvme
        .as_mut()
        .unwrap()
        .owner_identities
        .clear();
    let legacy = open(f.config.clone()).unwrap();
    super::tests::fail_stop_without_checkpoint(legacy);
}

#[derive(Serialize)]
struct Row {
    case: String,
    operation: String,
    arm: String,
    tier: String,
    cache: String,
    value_bytes: usize,
    writers: usize,
    repetition: usize,
    n: usize,
    offered: usize,
    successful: usize,
    rejected: usize,
    failed: usize,
    warmup: usize,
    reduced_sync_counts: bool,
    before_case: Value,
    interference: bool,
    mean_us: f64,
    p50_us: f64,
    p99_us: f64,
    p999_us: f64,
    calls_per_second: f64,
    mean_inflight_calls: f64,
    elapsed_seconds: f64,
    counters: Value,
    lifetime_counters: Value,
}
fn row(
    case: String,
    arm: &str,
    tier: DurabilityTier,
    (size, writers, repetition): (usize, usize, usize),
    samples: &[u64],
    elapsed: Duration,
    counters: Value,
) -> Row {
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let nearest =
        |p: usize, scale: usize| sorted[(p * sorted.len()).div_ceil(scale) - 1] as f64 / 1000.0;
    let sum = samples.iter().map(|n| *n as f64).sum::<f64>();
    Row {
        case,
        operation: "put".into(),
        arm: arm.into(),
        tier: tier.to_string(),
        cache: cache_contract(arm).into(),
        value_bytes: size,
        writers,
        repetition,
        n: samples.len(),
        offered: samples.len(),
        successful: samples.len(),
        rejected: 0,
        failed: 0,
        warmup: 512,
        reduced_sync_counts: false,
        before_case: Value::Null,
        interference: false,
        mean_us: sum / samples.len() as f64 / 1000.0,
        p50_us: nearest(50, 100),
        p99_us: nearest(99, 100),
        p999_us: nearest(999, 1000),
        calls_per_second: samples.len() as f64 / elapsed.as_secs_f64(),
        mean_inflight_calls: sum / elapsed.as_nanos() as f64,
        elapsed_seconds: elapsed.as_secs_f64(),
        counters,
        lifetime_counters: Value::Null,
    }
}
fn cache_contract(arm: &str) -> &'static str {
    match arm {
        "A" => "owner buffered/mapped WAL; no coordinator value cache; media hits unmeasured",
        "C-buf" => "buffered regular-file namespace; owner page cache; no coordinator value cache",
        _ => {
            "O_DIRECT regular-file namespace; no coordinator value cache; device/cache hits unmeasured"
        }
    }
}
fn delta(after: LiveNvmeCounts, before: LiveNvmeCounts) -> Value {
    let after = serde_json::to_value(after).unwrap();
    let before = serde_json::to_value(before).unwrap();
    Value::Object(
        after
            .as_object()
            .unwrap()
            .iter()
            .map(|(name, value)| {
                (
                    name.clone(),
                    json!(value.as_u64().unwrap() - before[name].as_u64().unwrap()),
                )
            })
            .collect(),
    )
}
fn write_counters(
    f: &Fixture,
    arm: &str,
    before: LiveNvmeCounts,
    owner: PartitionStats,
    syncer_before: Option<u64>,
) -> Value {
    let now = f.owner_stats();
    let mut counts = delta(f.live.snapshot().counts, before);
    counts["append_rpc_exchanges"] = if arm == "A" {
        json!(now.appended_groups - owner.appended_groups)
    } else {
        json!(0)
    };
    counts["frames"] = if arm == "A" {
        Value::Null
    } else if arm == "B" {
        counts["completed_write_ops"].clone()
    } else {
        counts["leased_frames"].clone()
    };
    counts["owner_explicit_persistence_barriers"] =
        json!(now.persistence_barriers - owner.persistence_barriers);
    counts["owner_total_fdatasync"] = Value::Null;
    counts["owner_frontier_syncer_fsync_calls"] = match (f.owner_syncer_calls(), syncer_before) {
        (Some(after), Some(before)) => json!(after - before),
        _ => Value::Null,
    };
    counts["owner_background_only_fsync_calls"] = Value::Null;
    counts["owner_frontier_file_syncs"] =
        json!(now.live_frontier_file_syncs - owner.live_frontier_file_syncs);
    counts["owner_frontier_namespace_syncs"] =
        json!(now.live_frontier_namespace_syncs - owner.live_frontier_namespace_syncs);
    for stage in ["adoption_async", "adoption_settle"] {
        let samples = counts[format!("{stage}_samples")].as_u64().unwrap();
        counts[format!("{stage}_mean_ns")] = if samples == 0 {
            Value::Null
        } else {
            json!(counts[format!("{stage}_ns")].as_u64().unwrap() as f64 / samples as f64)
        };
    }
    counts["adoption_timing_semantics"] = json!("mean, no distribution");
    counts
}

fn write_phase(
    db: &ConcurrentRemoteDatabase,
    writers: usize,
    start: usize,
    count: usize,
    value: &Bytes,
) -> (Vec<u64>, Duration) {
    let gate = Barrier::new(writers + 1);
    thread::scope(|scope| {
        let mut handles = Vec::with_capacity(writers);
        for writer in 0..writers {
            let gate = &gate;
            handles.push(scope.spawn(move || {
                let mut samples = Vec::with_capacity(count / writers);
                gate.wait();
                for item in (writer..count).step_by(writers) {
                    let sequence = start + item;
                    // Input construction is outside API latency; immutable value bytes are shared.
                    let mutations = vec![put(sequence, value)];
                    let operation = operation(sequence);
                    let began = Instant::now();
                    db.write_batch(operation, mutations).unwrap();
                    samples.push(began.elapsed().as_nanos() as u64);
                }
                samples
            }));
        }
        let began = Instant::now();
        gate.wait();
        let mut samples = Vec::with_capacity(count);
        for handle in handles {
            samples.extend(handle.join().unwrap());
        }
        (samples, began.elapsed())
    })
}
fn run_write(
    base: &Path,
    output: &Path,
    arm: &str,
    tier: DurabilityTier,
    size: usize,
    writers: usize,
    rep: usize,
) -> Row {
    let case = format!("{arm}-{tier}-{size}-w{writers}-r{rep}");
    println!("live_bench {case}");
    let before_case = process_snapshot();
    let fixture = Fixture::new(base, &case, tier, arm);
    let db = ConcurrentRemoteDatabase::new(fixture.open(arm)).unwrap();
    let value: Bytes = vec![0x5a; size].into();
    let (_, warm_elapsed) = write_phase(&db, writers, 0, 512, &value);
    let before = fixture.live.snapshot().counts;
    let owner = fixture.owner_stats();
    let syncer_before = fixture.owner_syncer_calls();
    let full_count = if size == 1024 { 4096 } else { 1024 };
    let reduce = tier == DurabilityTier::Synchronous
        && warm_elapsed.as_secs_f64() * full_count as f64 / 512.0 > 60.0;
    let count = if reduce { full_count / 4 } else { full_count };
    let (samples, elapsed) = write_phase(&db, writers, 512, count, &value);
    let counters = write_counters(&fixture, arm, before, owner, syncer_before);
    let mut result = row(
        case,
        arm,
        tier,
        (size, writers, rep),
        &samples,
        elapsed,
        counters,
    );
    result.reduced_sync_counts = reduce;
    result.interference = before_case["interference"] == true;
    result.before_case = before_case;
    // Capture before teardown, which closes the extent and may write a skip marker.
    result.lifetime_counters = write_counters(
        &fixture,
        arm,
        LiveNvmeCounts::default(),
        PartitionStats::default(),
        Some(0),
    );
    if matches!(arm, "C" | "C-buf") {
        let c = &result.lifetime_counters;
        assert_eq!(c["reserve_exchanges"], 0);
        assert_eq!(c["complete_exchanges"], 0);
        assert_eq!(c["lease_exchanges"], c["extents_leased"]);
        assert_eq!(c["leased_skip_markers"], 0);
        assert_eq!(
            c["fua_writes"],
            if tier == DurabilityTier::Synchronous {
                c["leased_frames"].clone()
            } else {
                json!(0)
            }
        );
    }
    save_raw(output, &result.case, &samples);
    println!(
        "live_bench preparation_workers={} pipeline={:?}",
        db.config.preparation_workers,
        db.pipeline_stats()
    );
    db.shutdown().unwrap();
    result
}
fn save_raw(output: &Path, case: &str, samples: &[u64]) {
    let mut file = File::create(output.join("raw").join(format!("{case}.csv"))).unwrap();
    writeln!(file, "sample,api_latency_ns").unwrap();
    for (index, ns) in samples.iter().enumerate() {
        writeln!(file, "{index},{ns}").unwrap();
    }
}
fn profile(db: &RemoteDatabase) -> Value {
    let mut fields = serde_json::Map::new();
    db.read_profile_monitor().visit(&mut |name, value| {
        fields.insert(name.into(), json!(value));
    });
    Value::Object(fields)
}
fn value_delta(after: &Value, before: &Value) -> Value {
    Value::Object(
        after
            .as_object()
            .unwrap()
            .iter()
            .map(|(name, value)| {
                (
                    name.clone(),
                    json!(
                        value
                            .as_u64()
                            .unwrap()
                            .saturating_sub(before[name].as_u64().unwrap())
                    ),
                )
            })
            .collect(),
    )
}
fn configure_reads(
    db: &mut RemoteDatabase,
    fixture: &Fixture,
    reference: ValueRecordReference,
    inline_reads: bool,
) {
    db.config.live_nvme.as_mut().unwrap().inline_reads = inline_reads;
    let clients = db
        .reads
        .stop_with_clients()
        .into_iter()
        .map(|shared| {
            let mut client = match Arc::try_unwrap(shared) {
                Ok(client) => client.into_inner(),
                Err(_) => panic!("read client retained after stop"),
            };
            super::super::tcp::live_read::tests::configure_local_client(&mut client);
            client.set_live_metrics(fixture.live.clone()).unwrap();
            client
        })
        .collect();
    db.reads = Arc::new(
        ReadService::from_clients(
            &db.config,
            db.epoch,
            clients,
            &mut RecoveryStats::default(),
            db.first_failure.clone(),
        )
        .unwrap(),
    );
    db.reads.bench_install_owner_local(&fixture.node, reference);
}
#[test]
fn leased_settled_dataset_matches_inline_lane_and_owner_rpc_in_both_tiers() {
    let root = tempdir::TempDir::new("leased-read-routes").unwrap();
    for tier in [DurabilityTier::Native, DurabilityTier::Synchronous] {
        let fixture = Fixture::new(root.path(), tier.as_str(), tier, "C");
        let mut db = fixture.open("C");
        let value: Bytes = vec![0x5a; 1024].into();
        for sequence in 0..8 {
            db.write_batch(operation(sequence), vec![put(sequence, &value)])
                .unwrap();
        }
        db.flush_index().unwrap();
        let reference = db
            .index_reference(KeySpace(0), &key(0), IndexReadCut::Latest)
            .unwrap()
            .unwrap();
        for inline_reads in [true, false] {
            configure_reads(&mut db, &fixture, reference, inline_reads);
            db.set_read_profile_limit(32);
            let parked = inline_reads.then(|| db.reads.bench_park_live_lane(0));
            if inline_reads {
                db.reads.bench_install_owner_local(&fixture.node, reference);
            }
            let live_before = fixture.live.snapshot().counts.completed_read_ops;
            let before = profile(&db);
            for sequence in 0..8 {
                assert_eq!(
                    db.get(KeySpace(0), &key(sequence)).unwrap(),
                    Some(value.clone())
                );
            }
            let after = profile(&db);
            if inline_reads {
                assert!(
                    after["read_profile_live_inline"].as_u64().unwrap()
                        > before["read_profile_live_inline"].as_u64().unwrap()
                );
            } else {
                assert_eq!(
                    after["read_profile_actor_completed"].as_u64().unwrap()
                        - before["read_profile_actor_completed"].as_u64().unwrap(),
                    8
                );
                for name in [
                    "read_profile_live_inline",
                    "read_profile_live_inline_fallback_drain",
                    "read_profile_live_inline_fallback_busy",
                    "read_profile_live_inline_fallback_grant",
                    "read_profile_live_inline_fallback_credit",
                    "read_profile_live_inline_fallback_paused",
                    "read_profile_live_inline_fallback_coalesced",
                ] {
                    assert_eq!(after[name], before[name], "{name}");
                }
            }
            assert_eq!(
                fixture.live.snapshot().counts.completed_read_ops - live_before,
                8
            );
            drop(parked);
        }
        let mut owner = Client::connect(
            &fixture.config.endpoints[0].address,
            &fixture.config.tls,
            fixture.config.timeout,
            fixture.config.partition_config(db.epoch, 0),
            &fixture.shape,
        )
        .unwrap();
        for sequence in 0..8 {
            let reference = db
                .index_reference(KeySpace(0), &key(sequence), IndexReadCut::Latest)
                .unwrap()
                .unwrap();
            let (records, profile) = owner.read_records_profiled(db.epoch, &[reference]).unwrap();
            assert_eq!(records, vec![put(sequence, &value)]);
            assert_eq!(profile.requests, 1);
            assert!(profile.server_handler_available);
            assert_eq!(profile.server_response_recheck_samples, 1);
        }
        db.shutdown().unwrap();
    }
}

fn run_reads(base: &Path, output: &Path, arms: &[String], repetitions: &[usize]) -> Vec<Row> {
    let fixture = Fixture::new(base, "reads", DurabilityTier::Native, "C");
    let mut db = fixture.open("C");
    let value: Bytes = vec![0x5a; 1024].into();
    for sequence in 0..4096 {
        db.write_batch(operation(sequence), vec![put(sequence, &value)])
            .unwrap();
    }
    db.write_batch(
        operation(4096),
        (4096..4128).map(|n| put(n, &value)).collect(),
    )
    .unwrap();
    db.flush_index().unwrap();
    let reference = db
        .index_reference(KeySpace(0), &key(0), IndexReadCut::Latest)
        .unwrap()
        .unwrap();
    configure_reads(&mut db, &fixture, reference, true);
    let keys: Vec<_> = (0..4096).map(key).collect();
    let batch_keys: Vec<_> = (4096..4128).map(key).collect();
    // Warm every scalar key through the actual live route, not the value cache.
    for key in &keys {
        assert_eq!(db.get(KeySpace(0), key).unwrap().as_ref(), Some(&value));
    }
    let mut rows = Vec::new();
    for &rep in repetitions {
        for arm in if rep % 2 == 1 {
            ["R1", "R2"]
        } else {
            ["R2", "R1"]
        } {
            if !arms.iter().any(|selected| selected == arm) {
                continue;
            }
            let before_case = process_snapshot();
            configure_reads(&mut db, &fixture, reference, arm == "R1");
            let parked = (arm == "R1").then(|| db.reads.bench_park_live_lane(0));
            if parked.is_some() {
                db.reads.bench_install_owner_local(&fixture.node, reference);
            }
            // Warm the exact measured route after its final grant/queue setup.
            for key in &keys {
                assert_eq!(db.get(KeySpace(0), key).unwrap().as_ref(), Some(&value));
            }
            let before = fixture.live.snapshot().counts;
            let diagnostics = profile(&db);
            let mut samples = Vec::with_capacity(8192);
            let began = Instant::now();
            for n in 0..8192 {
                let key = &keys[(n * 4051) % keys.len()];
                let start = Instant::now();
                let actual = db.get(KeySpace(0), key).unwrap();
                samples.push(start.elapsed().as_nanos() as u64);
                assert_eq!(actual.as_ref(), Some(&value));
            }
            let elapsed = began.elapsed();
            let mut counters = delta(fixture.live.snapshot().counts, before);
            counters["read_diagnostics"] = value_delta(&profile(&db), &diagnostics);
            let inline = counters["read_diagnostics"]["read_profile_live_inline"]
                .as_u64()
                .unwrap();
            if arm == "R1" {
                assert_eq!(inline, samples.len() as u64);
            } else {
                assert_eq!(inline, 0);
            }
            drop(parked);
            let mut result = row(
                format!("{arm}-native-1024-r{rep}"),
                arm,
                DurabilityTier::Native,
                (1024, 1, rep),
                &samples,
                elapsed,
                counters,
            );
            result.operation = "get".into();
            result.warmup = 4096;
            result.interference = before_case["interference"] == true;
            result.before_case = before_case;
            save_raw(output, &result.case, &samples);
            rows.push(result);
        }
    }
    // Batch reads always use the lane. Keep its warmed queue and file grant.
    for &rep in repetitions
        .iter()
        .filter(|_| arms.iter().any(|arm| arm == "R3"))
    {
        let before_case = process_snapshot();
        let before = fixture.live.snapshot().counts;
        let diagnostics = profile(&db);
        let mut samples = Vec::with_capacity(512);
        let began = Instant::now();
        for _ in 0..512 {
            let start = Instant::now();
            let actual = db.get_many(KeySpace(0), &batch_keys).unwrap();
            samples.push(start.elapsed().as_nanos() as u64);
            assert_eq!(actual.len(), 32);
            assert!(actual.iter().all(|v| v.as_ref() == Some(&value)));
        }
        let elapsed = began.elapsed();
        let mut counters = delta(fixture.live.snapshot().counts, before);
        counters["read_diagnostics"] = value_delta(&profile(&db), &diagnostics);
        let mut result = row(
            format!("R3-native-1024-b32-r{rep}"),
            "R3",
            DurabilityTier::Native,
            (1024, 1, rep),
            &samples,
            elapsed,
            counters,
        );
        result.operation = "get_many_32".into();
        result.warmup = 4096;
        result.interference = before_case["interference"] == true;
        result.before_case = before_case;
        save_raw(output, &result.case, &samples);
        rows.push(result);
    }
    db.shutdown().unwrap();
    rows
}

fn command(program: &str, args: &[&str]) -> String {
    let output = Process::new(program).args(args).output().unwrap();
    assert!(
        output.status.success(),
        "{program}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().to_owned()
}
fn process_snapshot() -> Value {
    let text = command("ps", &["-eo", "pid,ppid,pcpu,comm,args", "--sort=-pcpu"]);
    let processes: Vec<_> = text.lines().skip(1).collect();
    let top: Vec<_> = processes.iter().take(5).copied().collect();
    let fields: Vec<Vec<_>> = processes
        .iter()
        .map(|line| line.split_whitespace().take(4).collect())
        .collect();
    let mut ancestors = vec![std::process::id()];
    for _ in 0..64 {
        let Some(parent) = fields
            .iter()
            .find(|p| p[0].parse::<u32>().unwrap() == *ancestors.last().unwrap())
            .map(|p| p[1].parse::<u32>().unwrap())
        else {
            break;
        };
        if parent == 0 || ancestors.contains(&parent) {
            break;
        }
        ancestors.push(parent);
    }
    let foreign = |p: &Vec<&str>| !ancestors.contains(&p[0].parse::<u32>().unwrap());
    let cpu_interference = fields
        .iter()
        .any(|p| foreign(p) && p[2].parse::<f64>().unwrap() > 30.0);
    let build_or_test = |name: &str| {
        matches!(
            name,
            "cargo"
                | "rustc"
                | "ld"
                | "mold"
                | "rust-lld"
                | "lld"
                | "cc"
                | "cc1"
                | "collect2"
                | "clang"
        ) || name.starts_with("cargo-")
            || name.starts_with("tidehunter-")
    };
    let foreign_builds: Vec<_> = processes
        .iter()
        .zip(&fields)
        .filter(|(_, p)| foreign(p) && build_or_test(p[3]))
        .map(|(line, _)| *line)
        .collect();
    let loadavg = fs::read_to_string("/proc/loadavg").unwrap();
    let load_one: f64 = loadavg.split_whitespace().next().unwrap().parse().unwrap();
    json!({ "loadavg": loadavg.trim(), "loadavg_one_minute": load_one,
        "top_5_cpu_processes": top, "foreign_build_or_test_processes": foreign_builds,
        "interference": cpu_interference || !foreign_builds.is_empty() || load_one > 2.0 })
}
fn environment(base: &Path) -> Value {
    let cpuinfo = fs::read_to_string("/proc/cpuinfo").unwrap();
    let model = cpuinfo
        .lines()
        .find_map(|line| line.strip_prefix("model name\t: "))
        .unwrap();
    let source = command(
        "findmnt",
        &["-n", "-o", "SOURCE", "-T", base.to_str().unwrap()],
    );
    let fstype = command(
        "findmnt",
        &["-n", "-o", "FSTYPE", "-T", base.to_str().unwrap()],
    );
    assert_ne!(fstype, "tmpfs", "owner WAL must be on the workstation SSD");
    let parent = command("lsblk", &["-n", "-o", "PKNAME", &source]);
    let dev = if parent.is_empty() {
        Path::new(&source).file_name().unwrap().to_str().unwrap()
    } else {
        &parent
    };
    let write_cache = fs::read_to_string(format!("/sys/block/{dev}/queue/write_cache"))
        .map(|v| v.trim().to_owned())
        .unwrap_or_else(|error| format!("n/a: {error}"));
    json!({ "cpu_model": model, "logical_cores": cpuinfo.lines().filter(|l| l.starts_with("processor\t:")).count(),
        "kernel": command("uname", &["-r"]), "filesystem": fstype, "device": source,
        "parent_device": dev, "volatile_write_cache_report": write_cache,
        "profile": "release", "debug_assertions": cfg!(debug_assertions),
        "git_head": command("git", &["rev-parse", "HEAD"]),
        "git_dirty": !command("git", &["status", "--porcelain"]).is_empty(),
        "binary_sha256": command("sha256sum", &[std::env::current_exe().unwrap().to_str().unwrap()]),
        "cpu_topology": command("lscpu", &["-J"]),
        "wal_directory": base, "before": process_snapshot() })
}
fn markdown(rows: &[Row], environment: &Value) -> String {
    let mut text = String::from(
        "# P11 workstation regular-file fixture latency\n\nReal coordinator/authority actor, publication and index (`ConcurrentRemoteDatabase::write_batch`), real owner control service over trusted loopback TCP; reads use public `RemoteDatabase::get/get_many`. This measures software, control exchanges, copies, thread hops and local file synchronization. No fabric, nvmet or hardware FUA mapping is exercised. Differences are NOT hardware results. No coordinator value cache. A uses the owner's buffered/mapped WAL; B and C use an O_DIRECT regular-file namespace; C-buf uses a buffered regular-file namespace. Read fixtures use O_DIRECT, even after a warming pass; media/cache-hit fractions are unmeasured. O_DIRECT rows must not be described as owner-page-cache-served I/O.\n\nNative buffered writes acknowledge host-page-cache bytes without a power-loss guarantee; native O_DIRECT completion is only the regular-file fixture's completion contract, not the nvmet buffered target contract. Synchronous A/B waits for owner persistence; synchronous C/C-buf waits for RWF_DSYNC completion, not a current owner frontier root. FUA counters count Data/RWF_DSYNC requests, NOT observed hardware FUA. Async adoption is submitted after append replies, but synchronous adoption exchanges are reported rather than assumed absent. Quantiles are exact nearest rank per row, without pooling or outlier removal.\n\n",
    );
    text.push_str(&format!("Environment: `{environment}`\n\n"));
    for (title, writes) in [("Write API latency", true), ("Read API latency", false)] {
        let rate = if writes { "Puts/s" } else { "Calls/s" };
        text.push_str(&format!("## {title}\n\nLatency: microseconds. Case IDs include durability tier and repetition.\n\n| Case | Cache contract | n | Mean | p50 | p99 | p99.9 | {rate} | Mean in-flight | Before load 1m | Interference |\n|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|\n"));
        for r in rows.iter().filter(|r| (r.operation == "put") == writes) {
            text.push_str(&format!(
                "| {} | {} | {} | {:.3} | {:.3} | {:.3} | {:.3} | {:.1} | {:.2} | {} | {} |\n",
                r.case,
                r.cache,
                r.n,
                r.mean_us,
                r.p50_us,
                r.p99_us,
                r.p999_us,
                r.calls_per_second,
                r.mean_inflight_calls,
                r.before_case["loadavg_one_minute"],
                if r.interference { "yes" } else { "no" }
            ));
        }
    }
    text.push_str("\n## Route counters (measured phase)\n\nLive writes are namespace io_uring SQEs, not all owner WAL writes. RPC append exchanges are derived from fresh owner appended-group counts (no retries). `n/a` means no counter or not applicable. Syncer fsync calls include background AND demanded calls and can overlap explicit barriers; DO NOT add them. No background-only or total fdatasync counter exists. F/P is frames/put, SQE/C is read SQEs/API call.\n\n| Case | Cache contract | RPC append | Reserve | Complete | Lease | Adopt async | Adopt sync | Live block writes | Data/FUA requests | Skip | Extents | Frames | F/P or SQE/C | Owner explicit barriers | Syncer fsync calls | Background-only calls | Inline | Fallbacks |\n|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|\n");
    for r in rows {
        let c = &r.counters;
        let mut cells = vec![r.case.clone(), r.cache.clone()];
        for name in [
            "append_rpc_exchanges",
            "reserve_exchanges",
            "complete_exchanges",
            "lease_exchanges",
            "adoption_async_exchanges",
            "adoption_sync_exchanges",
            "completed_write_ops",
            "fua_writes",
            "leased_skip_markers",
            "extents_leased",
            "frames",
        ] {
            cells.push(counter(c, name));
        }
        let numerator = c[if r.operation == "put" {
            "frames"
        } else {
            "completed_read_ops"
        }]
        .as_u64();
        cells.push(
            numerator
                .map(|n| format!("{:.4}", n as f64 / r.n as f64))
                .unwrap_or_else(|| "n/a".into()),
        );
        for name in [
            "owner_explicit_persistence_barriers",
            "owner_frontier_syncer_fsync_calls",
            "owner_background_only_fsync_calls",
        ] {
            cells.push(counter(c, name));
        }
        let d = &c["read_diagnostics"];
        cells.push(counter(d, "read_profile_live_inline"));
        cells.push(
            d.as_object()
                .map(|m| {
                    m.iter()
                        .filter(|(k, _)| k.starts_with("read_profile_live_inline_fallback_"))
                        .map(|(k, v)| {
                            format!(
                                "{}={v}",
                                k.trim_start_matches("read_profile_live_inline_fallback_")
                            )
                        })
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .unwrap_or_else(|| "n/a".into()),
        );
        text.push_str(&format!("| {} |\n", cells.join(" | ")));
    }
    text.push_str("\n### Native reserved barrier evidence\n\n");
    for r in rows.iter().filter(|r| r.arm == "B" && r.tier == "native") {
        let reserve = r.counters["reserve_exchanges"].as_u64().unwrap();
        let barriers = r.counters["owner_explicit_persistence_barriers"]
            .as_u64()
            .unwrap();
        text.push_str(&format!("- `{}` ({}): {} reserves, {} owner explicit barriers; aggregate one-barrier-per-reserve equality: {}. Aggregate counters alone do not time each individual reserve.\n",
            r.case, r.cache, reserve, barriers, if reserve == barriers { "yes" } else { "no" }));
    }
    text.push_str("\n## C versus C-buf timings and adoptions\n\nMicroseconds. CQE timing is the existing transport submit-to-CQE aggregate mean, not device time. Leased-write timing is successful admission-to-CQE mean. Adoption async is successful submission to validated reply. Adoption settle keeps the extent open and includes the wait for pending adoption. Both report mean, no distribution. Aggregate timing cannot supply percentiles.\n\n| Case | Cache contract | p99 | p99.9 | Async adopt | Sync adopt | Mean submit-to-CQE | Mean leased write | Mean async adoption | Mean adoption settle | History-full settles |\n|---|---|---:|---:|---:|---:|---|---|---|---|---:|\n");
    for r in rows
        .iter()
        .filter(|r| matches!(r.arm.as_str(), "C" | "C-buf"))
    {
        text.push_str(&format!(
            "| {} | {} | {:.3} | {:.3} | {} | {} | {} | {} | {} | {} | {} |\n",
            r.case,
            r.cache,
            r.p99_us,
            r.p999_us,
            counter(&r.counters, "adoption_async_exchanges"),
            counter(&r.counters, "adoption_sync_exchanges"),
            counter_mean(&r.counters, "cq_ns", "cq_samples"),
            counter_mean(&r.counters, "leased_write_ns", "leased_write_samples"),
            counter_mean(&r.counters, "adoption_async_ns", "adoption_async_samples"),
            counter_mean(&r.counters, "adoption_settle_ns", "adoption_settle_samples"),
            counter(&r.counters, "adoption_history_full_settles"),
        ));
    }
    text.push_str("\n## Whole-case leased counters (warm-up + measurement, before close)\n\n| Case | Cache contract | Lease | Extents | Reserve | Complete | Frames | Data/FUA requests | Async adopt | Sync adopt | Skip |\n|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n");
    for r in rows
        .iter()
        .filter(|r| matches!(r.arm.as_str(), "C" | "C-buf"))
    {
        let mut cells = vec![r.case.clone(), r.cache.clone()];
        for name in [
            "lease_exchanges",
            "extents_leased",
            "reserve_exchanges",
            "complete_exchanges",
            "leased_frames",
            "fua_writes",
            "adoption_async_exchanges",
            "adoption_sync_exchanges",
            "leased_skip_markers",
        ] {
            cells.push(counter(&r.lifetime_counters, name));
        }
        text.push_str(&format!("| {} |\n", cells.join(" | ")));
    }
    let reduced: Vec<_> = rows
        .iter()
        .filter(|r| r.reduced_sync_counts)
        .map(|r| &r.case)
        .collect();
    text.push_str(&format!("\nSynchronous 4x reductions (warm-up projected >60 s): {reduced:?}. Intentional R2 disable does not increment a fallback counter. Interference is yes for load1m >2, any foreign build/test process, or foreign process >30% of one core. Own process and ancestors (including its cargo runner) are excluded. ps %CPU is lifetime-average, not an instantaneous trace; every case records its pre-arm snapshot.\n\n"));
    text.push_str("## Selection controls\n\nComma-separated env lists; omitted variables select the full matrix: `P11_BENCH_ARMS=A,B,C,C-buf,R1,R2,R3`, `P11_BENCH_TIERS=native,synchronous`, `P11_BENCH_VALUE_BYTES=1024,65536`, `P11_BENCH_WRITERS=1,16`, `P11_BENCH_REPETITIONS=1,2`. Read rows are native 1KiB/one-reader and selected only when native, 1024 and one writer are included. Odd repetitions use A/B/C/C-buf order; even ones reverse it; scalar read pairs also reverse.\n\nFull: `RUST_TEST_THREADS=1 cargo test --release --locked --offline -p tidehunter --features experimental-nvmeof,test-utils --lib live_bench -- --ignored --nocapture`\n\nShort subset: `P11_BENCH_ARMS=C,C-buf P11_BENCH_TIERS=native P11_BENCH_VALUE_BYTES=1024 P11_BENCH_WRITERS=1 P11_BENCH_REPETITIONS=1 RUST_TEST_THREADS=1 cargo test --release --locked --offline -p tidehunter --features experimental-nvmeof,test-utils --lib live_bench -- --ignored --nocapture`\n");
    text
}
fn counter(c: &Value, name: &str) -> String {
    c[name]
        .as_u64()
        .map(|n| n.to_string())
        .unwrap_or_else(|| "n/a".into())
}
fn counter_mean(c: &Value, ns: &str, samples: &str) -> String {
    match (c[ns].as_u64(), c[samples].as_u64()) {
        (Some(ns), Some(samples)) if samples != 0 => {
            format!("{:.3}", ns as f64 / samples as f64 / 1000.0)
        }
        _ => "n/a".into(),
    }
}
fn selection(name: &str, default: &str, allowed: &[&str]) -> Vec<String> {
    let selected: Vec<_> = std::env::var(name)
        .unwrap_or_else(|_| default.into())
        .split(',')
        .map(|s| s.trim().to_owned())
        .collect();
    assert!(
        !selected.is_empty() && selected.iter().all(|s| allowed.contains(&s.as_str())),
        "invalid {name}"
    );
    selected
}

#[test]
#[ignore = "explicit release-only workstation closed-loop measurement"]
fn live_bench_closed_loop() {
    assert!(
        !cfg!(debug_assertions),
        "run this ignored measurement with --release"
    );
    let arms = selection(
        "P11_BENCH_ARMS",
        "A,B,C,C-buf,R1,R2,R3",
        &["A", "B", "C", "C-buf", "R1", "R2", "R3"],
    );
    let tiers = selection(
        "P11_BENCH_TIERS",
        "native,synchronous",
        &["native", "synchronous"],
    );
    let sizes = selection("P11_BENCH_VALUE_BYTES", "1024,65536", &["1024", "65536"]);
    let writers = selection("P11_BENCH_WRITERS", "1,16", &["1", "16"]);
    let repetitions: Vec<usize> = std::env::var("P11_BENCH_REPETITIONS")
        .unwrap_or_else(|_| "1,2".into())
        .split(',')
        .map(|s| s.trim().parse().unwrap())
        .collect();
    assert!(
        !repetitions.is_empty() && repetitions.iter().all(|r| (1..=100).contains(r)),
        "invalid repetitions"
    );
    let pid = std::process::id();
    let base = PathBuf::from(CACHE).join(format!("p11-bench-{pid}"));
    fs::create_dir_all(&base).unwrap();
    let output = PathBuf::from(OUTPUT).join(format!("run-{pid}"));
    fs::create_dir_all(output.join("raw")).unwrap();
    println!("live_bench output: {}", output.display());
    let mut env = environment(&base);
    env["selection"] = json!({ "arms": arms, "tiers": tiers, "value_bytes": sizes, "writers": writers, "repetitions": repetitions });
    let mut rows = Vec::with_capacity(70);
    for tier in [DurabilityTier::Native, DurabilityTier::Synchronous]
        .into_iter()
        .filter(|tier| tiers.contains(&tier.to_string()))
    {
        for size in [1024, 65536]
            .into_iter()
            .filter(|size| sizes.contains(&size.to_string()))
        {
            for writers in [1, 16]
                .into_iter()
                .filter(|n| writers.contains(&n.to_string()))
            {
                for &rep in &repetitions {
                    for arm in if rep % 2 == 1 {
                        ["A", "B", "C", "C-buf"]
                    } else {
                        ["C-buf", "C", "B", "A"]
                    } {
                        if !arms.iter().any(|selected| selected == arm) {
                            continue;
                        }
                        rows.push(run_write(&base, &output, arm, tier, size, writers, rep));
                        fs::write(
                            output.join("results.json"),
                            serde_json::to_vec_pretty(&json!({"environment": env, "rows": rows}))
                                .unwrap(),
                        )
                        .unwrap();
                    }
                }
            }
        }
    }
    if tiers.iter().any(|tier| tier == "native")
        && sizes.iter().any(|size| size == "1024")
        && writers.iter().any(|writers| writers == "1")
        && arms.iter().any(|arm| arm.starts_with('R'))
    {
        rows.extend(run_reads(&base, &output, &arms, &repetitions));
    }
    env["after"] = process_snapshot();
    env["interference"] = json!(
        env["before"]["interference"] == true
            || env["after"]["interference"] == true
            || rows.iter().any(|row| row.interference)
    );
    fs::write(
        output.join("results.json"),
        serde_json::to_vec_pretty(&json!({"environment": env, "rows": rows})).unwrap(),
    )
    .unwrap();
    fs::write(output.join("results.md"), markdown(&rows, &env)).unwrap();
    fs::remove_dir(&base).unwrap();
    println!("live_bench completed: {}", output.display());
}

#[test]
#[ignore = "explicit workstation checkpoint-tail comparison"]
fn leased_checkpoint_tail_workstation() {
    let setting = |name: &str, default: usize| {
        std::env::var(name)
            .map(|value| value.parse::<usize>().unwrap())
            .unwrap_or(default)
    };
    let calls = setting("TH_TAIL_CALLS", 4096);
    let fragment_bytes = setting("TH_TAIL_FRAGMENT_BYTES", 8 * MIB);
    let wal_file_bytes = setting("TH_TAIL_WAL_FILE_BYTES", 256 * MIB);
    let replay_max_batches = setting("TH_TAIL_REPLAY_MAX_BATCHES", 256);
    let replay_max_bytes = setting("TH_TAIL_REPLAY_MAX_BYTES", 16 * MIB);
    let default_geometry = calls == 4096
        && fragment_bytes == 8 * MIB
        && wal_file_bytes == 256 * MIB
        && replay_max_batches == 256
        && replay_max_bytes == 16 * MIB;
    let timeline_output = std::env::var_os("TH_TAIL_TIMELINE_DIR").map(PathBuf::from);
    let base = tempdir::TempDir::new("leased-checkpoint-tail").unwrap();
    let value: Bytes = vec![0x5a; 65536].into();
    for tier in [DurabilityTier::Native, DurabilityTier::Synchronous] {
        let mut fixture = Fixture::with_limits(
            base.path(),
            "tail",
            tier,
            "C-buf",
            fragment_bytes as u64,
            wal_file_bytes as u64,
            ReplayLimits {
                max_batches: replay_max_batches,
                max_encoded_bytes: replay_max_bytes,
            },
        );
        fixture.config.preparation_workers = 8;
        let db = ConcurrentRemoteDatabase::new(fixture.open("C-buf")).unwrap();
        let (samples, elapsed) = if timeline_output.is_some() {
            db.arm_write_timeline(calls).unwrap();
            let gate = Barrier::new(17);
            thread::scope(|scope| {
                let handles: Vec<_> = (0..16)
                    .map(|writer| {
                        let db = &db;
                        let gate = &gate;
                        let value = &value;
                        scope.spawn(move || {
                            let mut samples = Vec::with_capacity(calls.div_ceil(16));
                            gate.wait();
                            for sequence in (writer..calls).step_by(16) {
                                let mutations = vec![put(sequence, value)];
                                let operation = operation(sequence);
                                let began = Instant::now();
                                let sample = db.write_timeline.begin(sequence as u64).unwrap();
                                db.write_timeline.operation(sample, operation);
                                db.write_batch_traced(operation, mutations, Some(sample))
                                    .unwrap();
                                db.write_timeline.complete(sample, None);
                                samples.push(began.elapsed().as_nanos() as u64);
                            }
                            samples
                        })
                    })
                    .collect();
                let began = Instant::now();
                gate.wait();
                let samples = handles
                    .into_iter()
                    .flat_map(|handle| handle.join().unwrap())
                    .collect();
                (samples, began.elapsed())
            })
        } else {
            write_phase(&db, 16, 0, calls, &value)
        };
        let counts = fixture.live.snapshot().counts;
        let manifest: AuthorityManifest =
            read_metadata(&fixture.root.path().join("authority").join(MANIFEST)).unwrap();
        let checkpoints = manifest
            .checkpoint
            .map_or(0, |checkpoint| checkpoint.generation);
        if default_geometry {
            assert!(
                checkpoints >= 3,
                "must cross at least three committed checkpoints"
            );
            assert!(
                counts.extent_switch_samples >= 2,
                "must cross at least two extent boundaries"
            );
        }
        let mut checkpoint_started = 0;
        let mut completed_checkpoint_lifetime_ns = 0;
        db.visit_write_metrics(|metrics| {
            checkpoint_started = metrics.authority.checkpoint_count;
            completed_checkpoint_lifetime_ns = metrics.authority.background_checkpoint_ns;
        });
        let slow = samples.iter().filter(|ns| **ns >= 5_000_000).count();
        let maximum = samples.iter().max().copied().unwrap();
        let mut sorted = samples.clone();
        sorted.sort_unstable();
        let quantile = |p: usize, scale: usize| sorted[(p * sorted.len()).div_ceil(scale) - 1];
        if let Some(output) = &timeline_output {
            fs::create_dir_all(output).unwrap();
            let export = db
                .finish_write_timeline_until(Instant::now() + Duration::from_secs(60))
                .unwrap();
            fs::write(
                output.join(format!("{tier}-timeline.json")),
                serde_json::to_vec(&export).unwrap(),
            )
            .unwrap();
        }
        println!(
            "checkpoint_tail_workstation {}",
            json!({
                "tier": tier.to_string(), "calls": samples.len(), "writers": 16, "value_bytes": 65536,
                "slow_calls_ge_5ms": slow, "max_api_ns": maximum, "elapsed_ns": elapsed.as_nanos(),
                "checkpoints": checkpoints, "extent_switches": counts.extent_switch_samples,
                "extent_switch_waits": counts.extent_switch_waits,
                "checkpoint_started": checkpoint_started,
                "completed_checkpoint_lifetime_ns": completed_checkpoint_lifetime_ns,
                "timeline_enabled": timeline_output.is_some(),
                "fragment_bytes": fragment_bytes, "wal_file_bytes": wal_file_bytes,
                "replay_max_batches": replay_max_batches, "replay_max_bytes": replay_max_bytes,
                "p50_ns": quantile(50, 100), "p99_ns": quantile(99, 100),
                "p999_ns": quantile(999, 1000),
                "largest_five_ns": sorted.iter().rev().take(5).copied().collect::<Vec<_>>(),
                "api_ns": samples,
                "scope": "workstation local-file io_uring; not remote hardware performance"
            })
        );
        // Verify the coordinator index against the real owner's final WAL after
        // an explicit barrier, without asking a workstation for a fabrics device.
        db.flush_durable().unwrap();
        let log = fixture.node.log_for_test(1);
        for sequence in 0..calls {
            let reference = db
                .index_reference(KeySpace(0), &key(sequence), IndexReadCut::Latest)
                .unwrap()
                .unwrap();
            assert_eq!(log.read_record(&reference).unwrap(), put(sequence, &value));
        }
        db.shutdown().unwrap();
    }
}
