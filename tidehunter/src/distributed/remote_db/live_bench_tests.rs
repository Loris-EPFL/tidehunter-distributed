//! Opt-in workstation latency measurements; never hardware qualification.
use super::pipeline_tests::trusted_owner_host_config;
use super::*;
use crate::distributed::codec::BatchLimits;
use crate::distributed::durability::DurabilityTier;
use crate::distributed::group::GroupLimits;
use crate::distributed::nvmeof::live_metrics::{LiveNvmeCounts, LiveNvmeMetrics};
use crate::distributed::tcp::{LeasedSync, LiveClientConfig, OwnerHost, StorageNode, Transport};
use crate::key_shape::KeyType;
use serde_json::{Value, json};
use std::io::Write;
use std::net::TcpListener;
use std::process::Command as Process;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Barrier, OnceLock};

const OUTPUT: &str = "/home/hybr1d/Desktop/PDM/TideHunter/checkpoint-work/p11-local-bench";
const CACHE: &str = "/home/hybr1d/.cache/tidehunter";
const MIB: usize = 1024 * 1024;

fn metrics() -> Arc<Metrics> {
    Metrics::new_in_enabled(&prometheus::Registry::new(), false)
}
fn live_config(tier: DurabilityTier) -> LiveClientConfig {
    LiveClientConfig {
        lease_ahead: true,
        polled_writes: false,
        host_nqn: "nqn.2026-01.test:p11-bench".into(),
        host_id: [1; 16],
        owner_identities: Default::default(),
        leased_write_window: Some(4),
        leased_sync: LeasedSync::PerFrameFua,
        leased_flush_min_frame_bytes: 0,
        max_inflight_requests: 32,
        max_inflight_bytes: 4 * MIB,
        connect_timeout_ms: 10000,
        io_timeout_ms: 30000,
        leased_durability: tier,
        inline_reads: true,
        queue_geometry: Default::default(),
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
    node: OnceLock<Arc<StorageNode>>,
    host: Arc<OwnerHost>,
    config: RemoteDatabaseConfig,
    shape: KeyShape,
    live: Arc<LiveNvmeMetrics>,
    stop: Arc<AtomicBool>,
    server: Option<JoinHandle<()>>,
    target_root: Option<tempdir::TempDir>,
    // Drop the owner and its WAL mapper before removing the fixture directory.
    root: tempdir::TempDir,
}
impl Fixture {
    fn new(base: &Path, case: &str, tier: DurabilityTier) -> Self {
        Self::with_limits(
            base,
            case,
            tier,
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
            storage: LocalInventoryConfig {
                database: [0; 16],
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
            },
            endpoints: Vec::new(),
            tls: tls.clone(),
            timeout: Duration::from_secs(60),
            max_admitted_batches: 8192,
            max_index_bytes: 64 * MIB,
            max_pending_batches: 64,
            max_pending_bytes: 32 * MIB,
            replay_limits,
            owner_append_occupancy: 16,
            pipelined_writes: true,
            leased_submit_redrain: true,
            read_cache_bytes: 0,
            read_memory_allowance_bytes: 128 * MIB,
            scan_prefetch_pages: 1,
            read_lanes: Some(1),
            pin_read_lanes: false,
            max_pending_reads: 64,
            max_pending_read_bytes: 16 * MIB,
            write_profile_limit: 0,
            live_nvme: live_config(tier),
            durable_cut: Default::default(),
            relocation_max_reclaim_pct: 100,
            index: Default::default(),
        };
        let target_root = std::env::var_os("TH_TAIL_TARGET_DIR")
            .map(|path| tempdir::TempDir::new_in(path, "tail-target").unwrap());
        let target_path = target_root
            .as_ref()
            .map_or(root, |directory| directory.path());
        let configfs = target_path.join("configfs");
        for name in ["hosts", "ports", "subsystems"] {
            fs::create_dir_all(configfs.join(name)).unwrap();
        }
        let host = OwnerHost::open_with_live_fixture(
            trusted_owner_host_config(root.join("owner")),
            metrics(),
            configfs,
        )
        .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        config.endpoints.push(RemoteEndpoint {
            partition: 0,
            address: listener.local_addr().unwrap().to_string(),
            server_name: tls.server_name.clone(),
        });
        let stop = Arc::new(AtomicBool::new(false));
        let serving = Arc::clone(&host);
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
            node: OnceLock::new(),
            host,
            config,
            shape,
            live: LiveNvmeMetrics::new(),
            stop,
            server: Some(server),
            target_root,
        }
    }
    /// `arm` `C-buf` selects the buffered regular-file namespace, `C` (and the
    /// read arms) the O_DIRECT one.
    fn open(&self, arm: &str) -> RemoteDatabase {
        let db = RemoteDatabase::open_with_local_live_fixture(
            &self.root.path().join("authority"),
            &self.shape,
            self.config.clone(),
            arm == "C-buf",
        )
        .unwrap();
        self.node
            .get_or_init(|| self.host.database(&db.database_id()).unwrap());
        for worker in &db.core.clients {
            worker
                .client
                .lock()
                .set_live_metrics(self.live.clone())
                .unwrap();
        }
        db
    }
    fn node(&self) -> &StorageNode {
        self.node.get().unwrap()
    }
    fn owner_stats(&self) -> PartitionStats {
        self.node().log_for_test(1).stats()
    }
    fn owner_syncer_calls(&self) -> Option<u64> {
        self.node()
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
fn pin_read_lanes_defaults_off_and_validates_queue_count() {
    let base = tempdir::TempDir::new("pin-read-lanes").unwrap();
    let mut fixture = Fixture::new(base.path(), "config", DurabilityTier::Synchronous);
    let mut serialized = serde_json::to_value(&fixture.config).unwrap();
    serialized.as_object_mut().unwrap().remove("pin_read_lanes");
    let legacy: RemoteDatabaseConfig = serde_json::from_value(serialized).unwrap();
    assert!(!legacy.pin_read_lanes);
    fixture.config.live_nvme = live_config(DurabilityTier::Synchronous);
    fixture.config.pin_read_lanes = true;
    fixture.config.read_lanes = Some(4);
    fixture.config.validate().unwrap();
    fixture.config.read_lanes = Some(5);
    assert!(
        fixture
            .config
            .validate()
            .unwrap_err()
            .to_string()
            .contains("pin_read_lanes")
    );
    fixture.config.read_lanes = None;
    fixture.config.validate().unwrap();
    fixture.config.validate_read_lane_pinning(1).unwrap();
    assert!(
        fixture
            .config
            .validate_read_lane_pinning(16)
            .unwrap_err()
            .to_string()
            .contains("pin_read_lanes")
    );
    fixture.config.pin_read_lanes = false;
    fixture.config.validate().unwrap();
    fixture.config.pin_read_lanes = true;
    fixture.config.live_nvme.queue_geometry.io_queues = 1;
    fixture.config.read_lanes = Some(1);
    fixture.config.validate().unwrap();
    fixture.config.read_lanes = Some(2);
    assert!(fixture.config.validate().is_err());
}

#[test]
fn leased_submit_redrain_defaults_on_and_round_trips_off() {
    let base = tempdir::TempDir::new("leased-submit-redrain").unwrap();
    let mut fixture = Fixture::new(base.path(), "config", DurabilityTier::Synchronous);
    // Absent means on, and on is not serialized: documents are unchanged.
    let serialized = serde_json::to_value(&fixture.config).unwrap();
    assert!(serialized.get("leased_submit_redrain").is_none());
    let legacy: RemoteDatabaseConfig = serde_json::from_value(serialized.clone()).unwrap();
    assert!(legacy.leased_submit_redrain);
    let mut off = serialized;
    off["leased_submit_redrain"] = false.into();
    let off: RemoteDatabaseConfig = serde_json::from_value(off).unwrap();
    assert!(!off.leased_submit_redrain);
    assert_eq!(
        serde_json::to_value(&off).unwrap()["leased_submit_redrain"],
        false
    );
    fixture.config.leased_submit_redrain = false;
    fixture.config.validate().unwrap();
    fixture.config.leased_submit_redrain = true;
    fixture.config.validate().unwrap();
}

#[test]
fn owner_initiator_map_is_pinned_across_authority_reopen() {
    use crate::distributed::tcp::LiveInitiatorIdentity;
    let base = tempdir::TempDir::new("live-owner-identities").unwrap();
    let mut f = Fixture::new(base.path(), "owner-map", DurabilityTier::Native);
    let identity = LiveInitiatorIdentity {
        host_nqn: f.config.live_nvme.host_nqn.clone(),
        host_id: [2; 16],
    };
    f.config
        .live_nvme
        .owner_identities
        .insert(0, identity.clone());
    let root = f.root.path().join("authority");
    let open = |config| RemoteDatabase::open_local_live_fixture(&root, &f.shape, config);
    let first = open(f.config.clone()).unwrap();
    super::tests::fail_stop_without_checkpoint(first);
    let same = open(f.config.clone()).unwrap();
    super::tests::fail_stop_without_checkpoint(same);
    for change in 0..4 {
        let mut config = f.config.clone();
        let live = &mut config.live_nvme;
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
    let configured = std::mem::take(&mut manifest.live_nvme_owner_identities);
    persist_manifest(&root, &manifest, CoordinatorIoCategory::Open).unwrap();
    assert!(
        open(f.config.clone())
            .err()
            .unwrap()
            .to_string()
            .contains("authority configuration differs from the recorded authority")
    );
    manifest.live_nvme_owner_identities = configured;
    persist_manifest(&root, &manifest, CoordinatorIoCategory::Open).unwrap();
    let restored = open(f.config.clone()).unwrap();
    super::tests::fail_stop_without_checkpoint(restored);
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
    before: LiveNvmeCounts,
    owner: PartitionStats,
    syncer_before: Option<u64>,
) -> Value {
    let now = f.owner_stats();
    let mut counts = delta(f.live.snapshot().counts, before);
    counts["frames"] = counts["leased_frames"].clone();
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
    let fixture = Fixture::new(base, &case, tier);
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
    let counters = write_counters(&fixture, before, owner, syncer_before);
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
        LiveNvmeCounts::default(),
        PartitionStats::default(),
        Some(0),
    );
    if matches!(arm, "C" | "C-buf") {
        let c = &result.lifetime_counters;
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
    println!("live_bench pipeline={:?}", db.pipeline_stats());
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
/// The read core is immutable and a reopen would seal the live epoch that the
/// inline route reads. Switch the inline-read policy and configure the
/// existing read-lane clients for the local live route in place, on the same
/// epoch and dataset.
fn configure_reads(
    db: &RemoteDatabase,
    fixture: &Fixture,
    reference: ValueRecordReference,
    inline_reads: bool,
) {
    // Release every cached live grant and owner pin, as the removed rebuild's
    // stop did; the returned admission pause resumes on drop.
    drop(db.core.reads.pause_live_for_retirement().unwrap());
    db.core.reads.set_inline_reads_for_test(inline_reads);
    for node in 0..db.core.config.endpoints.len() {
        for lane in 0..db.core.config.effective_read_lanes() {
            let client = db.core.reads.lane_client(node, lane);
            let mut client = client.lock();
            super::super::tcp::live_read::tests::configure_local_client(&mut client);
            client.set_live_metrics(fixture.live.clone()).unwrap();
        }
    }
    db.core
        .reads
        .bench_install_owner_local(fixture.node(), reference);
}
#[test]
fn leased_settled_dataset_matches_inline_and_worker_lanes_in_both_tiers() {
    let root = tempdir::TempDir::new("leased-read-routes").unwrap();
    for tier in [DurabilityTier::Native, DurabilityTier::Synchronous] {
        let fixture = Fixture::new(root.path(), tier.as_str(), tier);
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
            configure_reads(&db, &fixture, reference, inline_reads);
            db.set_read_profile_limit(32);
            let parked = inline_reads.then(|| db.core.reads.bench_park_live_lane(0));
            if inline_reads {
                db.core
                    .reads
                    .bench_install_owner_local(fixture.node(), reference);
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
        db.shutdown().unwrap();
    }
}

fn run_reads(base: &Path, output: &Path, arms: &[String], repetitions: &[usize]) -> Vec<Row> {
    let fixture = Fixture::new(base, "reads", DurabilityTier::Native);
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
    configure_reads(&db, &fixture, reference, true);
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
            configure_reads(&db, &fixture, reference, arm == "R1");
            let parked = (arm == "R1").then(|| db.core.reads.bench_park_live_lane(0));
            if parked.is_some() {
                db.core
                    .reads
                    .bench_install_owner_local(fixture.node(), reference);
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
        "# P11 workstation regular-file fixture latency\n\nReal coordinator/authority actor, publication and index (`ConcurrentRemoteDatabase::write_batch`), real owner control service over trusted loopback TCP; reads use public `RemoteDatabase::get/get_many`. This measures software, control exchanges, copies, thread hops and local file synchronization. No fabric, nvmet or hardware FUA mapping is exercised. Differences are NOT hardware results. No coordinator value cache. C uses an O_DIRECT regular-file namespace; C-buf uses a buffered regular-file namespace. Read fixtures use O_DIRECT, even after a warming pass; media/cache-hit fractions are unmeasured. O_DIRECT rows must not be described as owner-page-cache-served I/O.\n\nNative buffered writes acknowledge host-page-cache bytes without a power-loss guarantee; native O_DIRECT completion is only the regular-file fixture's completion contract, not the nvmet buffered target contract. Synchronous C/C-buf waits for RWF_DSYNC completion, not a current owner frontier root. FUA counters count Data/RWF_DSYNC requests, NOT observed hardware FUA. Async adoption is submitted after append replies, but synchronous adoption exchanges are reported rather than assumed absent. Quantiles are exact nearest rank per row, without pooling or outlier removal.\n\n",
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
    text.push_str("\n## Route counters (measured phase)\n\nLive writes are namespace io_uring SQEs, not all owner WAL writes. `n/a` means no counter or not applicable. Syncer fsync calls include background AND demanded calls and can overlap explicit barriers; DO NOT add them. No background-only or total fdatasync counter exists. F/P is frames/put, SQE/C is read SQEs/API call.\n\n| Case | Cache contract | Lease | Adopt async | Adopt sync | Live block writes | Data/FUA requests | Skip | Extents | Frames | F/P or SQE/C | Owner explicit barriers | Syncer fsync calls | Background-only calls | Inline | Fallbacks |\n|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|\n");
    for r in rows {
        let c = &r.counters;
        let mut cells = vec![r.case.clone(), r.cache.clone()];
        for name in [
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
    text.push_str("\n## Whole-case leased counters (warm-up + measurement, before close)\n\n| Case | Cache contract | Lease | Extents | Frames | Data/FUA requests | Async adopt | Sync adopt | Skip |\n|---|---|---:|---:|---:|---:|---:|---:|---:|\n");
    for r in rows
        .iter()
        .filter(|r| matches!(r.arm.as_str(), "C" | "C-buf"))
    {
        let mut cells = vec![r.case.clone(), r.cache.clone()];
        for name in [
            "lease_exchanges",
            "extents_leased",
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
    text.push_str("## Selection controls\n\nComma-separated env lists; omitted variables select the full matrix: `P11_BENCH_ARMS=C,C-buf,R1,R2,R3`, `P11_BENCH_TIERS=native,synchronous`, `P11_BENCH_VALUE_BYTES=1024,65536`, `P11_BENCH_WRITERS=1,16`, `P11_BENCH_REPETITIONS=1,2`. Read rows are native 1KiB/one-reader and selected only when native, 1024 and one writer are included. Odd repetitions use C/C-buf order; even ones reverse it; scalar read pairs also reverse.\n\nFull: `RUST_TEST_THREADS=1 cargo test --release --locked --offline -p tidehunter --features experimental-distributed,test-utils --lib live_bench -- --ignored --nocapture`\n\nShort subset: `P11_BENCH_ARMS=C,C-buf P11_BENCH_TIERS=native P11_BENCH_VALUE_BYTES=1024 P11_BENCH_WRITERS=1 P11_BENCH_REPETITIONS=1 RUST_TEST_THREADS=1 cargo test --release --locked --offline -p tidehunter --features experimental-distributed,test-utils --lib live_bench -- --ignored --nocapture`\n");
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
        "C,C-buf,R1,R2,R3",
        &["C", "C-buf", "R1", "R2", "R3"],
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
                        ["C", "C-buf"]
                    } else {
                        ["C-buf", "C"]
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
    let leased_write_window = match std::env::var("TH_TAIL_LEASED_WINDOW").as_deref() {
        Err(_) | Ok("none") => None,
        Ok(value) => Some(
            value
                .parse::<usize>()
                .expect("invalid TH_TAIL_LEASED_WINDOW"),
        ),
    };
    let replay_max_batches = setting("TH_TAIL_REPLAY_MAX_BATCHES", 256);
    let replay_max_bytes = setting("TH_TAIL_REPLAY_MAX_BYTES", 16 * MIB);
    let default_geometry = calls == 4096
        && fragment_bytes == 8 * MIB
        && wal_file_bytes == 256 * MIB
        && replay_max_batches == 256
        && replay_max_bytes == 16 * MIB;
    let timeline_output = std::env::var_os("TH_TAIL_TIMELINE_DIR").map(PathBuf::from);
    let owner_events = std::env::var_os("TH_TAIL_OWNER_EVENTS").is_some_and(|value| value == "1");
    let _capture =
        owner_events.then(|| crate::distributed::owner_events::OWNER_EVENTS_TEST_LOCK.lock());
    if owner_events {
        crate::distributed::owner_events::enable(65_536);
    }
    let base = tempdir::TempDir::new("leased-checkpoint-tail").unwrap();
    let value: Bytes = vec![0x5a; 65536].into();
    for tier in [DurabilityTier::Native, DurabilityTier::Synchronous] {
        let mut fixture = Fixture::with_limits(
            base.path(),
            "tail",
            tier,
            fragment_bytes as u64,
            wal_file_bytes as u64,
            ReplayLimits {
                max_batches: replay_max_batches,
                max_encoded_bytes: replay_max_bytes,
            },
        );
        fixture.config.owner_append_occupancy = fixture
            .config
            .owner_append_occupancy
            .max(leased_write_window.unwrap_or(1));
        fixture.config.live_nvme.leased_write_window = leased_write_window;
        let leased_write_window_bound = fixture
            .config
            .live_nvme
            .max_inflight_requests
            .min(fixture.config.effective_owner_append_occupancy());
        let db = ConcurrentRemoteDatabase::new(fixture.open("C-buf")).unwrap();
        let (samples, elapsed) = if timeline_output.is_some() {
            db.arm_write_timeline(calls, 1).unwrap();
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
        assert!(counts.max_leased_in_flight > 0);
        assert!(
            counts.max_leased_in_flight
                <= leased_write_window.unwrap_or(leased_write_window_bound) as u64
        );
        if leased_write_window == Some(1) {
            assert_eq!(counts.max_leased_in_flight, 1);
        }
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
            let result = output.join(tier.to_string()).join("result");
            fs::create_dir_all(&result).unwrap();
            let export = db
                .finish_write_timeline_until(Instant::now() + Duration::from_secs(60))
                .unwrap();
            fs::write(
                result.join("write-timeline.json"),
                serde_json::to_vec(&json!({
                    "schema_version": 2,
                    "available": true,
                    "status": "complete",
                    "timeline": export,
                }))
                .unwrap(),
            )
            .unwrap();
        }
        let preparation = fixture.node().log_for_test(1).live_file_preparation_stats();
        println!(
            "checkpoint_tail_workstation {}",
            json!({
                "tier": tier.to_string(), "calls": samples.len(), "writers": 16, "value_bytes": 65536,
                "slow_calls_ge_5ms": slow, "max_api_ns": maximum, "elapsed_ns": elapsed.as_nanos(),
                "checkpoints": checkpoints, "extent_switches": counts.extent_switch_samples,
                "extent_switch_waits": counts.extent_switch_waits,
                "leased_write_window": leased_write_window,
                "max_leased_in_flight": counts.max_leased_in_flight,
                "live_files_prepared": preparation.live_files_prepared,
                "live_files_published": preparation.live_files_published,
                "live_leased_write_window": leased_write_window,
                "live_leased_write_window_bound": leased_write_window_bound,
                "live_file_prepare_failures": preparation.live_file_prepare_failures,
                "grant_profiles": fixture.node().bench_live_grant_profiles(),
                "live_file_diagnostics": fixture.node().log_for_test(1).live_test_diagnostics(),
                "adoption": fixture.node().log_for_test(1).live_test_diagnostics().adoption,
                "target_dir": fixture.target_root.as_ref().map_or(fixture.root.path(), |directory| directory.path()),
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
        let log = fixture.node().log_for_test(1);
        for sequence in 0..calls {
            let reference = db
                .index_reference(KeySpace(0), &key(sequence), IndexReadCut::Latest)
                .unwrap()
                .unwrap();
            assert_eq!(log.read_record(&reference).unwrap(), put(sequence, &value));
        }
        db.shutdown().unwrap();
        drop(log);
        drop(fixture);
        if owner_events && let Some(output) = &timeline_output {
            let output = output.join(tier.to_string()).join("owner-events");
            fs::create_dir_all(&output).unwrap();
            crate::distributed::owner_events::drain_json(&output.join("node-0.json")).unwrap();
        }
    }
    let data_path = base.path().to_owned();
    drop(base);
    assert!(!data_path.exists(), "workstation fixture data remains");
    println!(
        "checkpoint_tail_workstation_cleanup {}",
        data_path.display()
    );
}

#[test]
#[ignore = "explicit four-owner small-put contention diagnosis; not hardware evidence"]
fn small_put_multiowner_workstation() {
    let calls = std::env::var("TH_SMALL_PUT_CALLS")
        .map(|value| value.parse::<usize>().unwrap())
        .unwrap_or(8192);
    assert!((256..=262_144).contains(&calls));
    let output = PathBuf::from(std::env::var_os("TH_SMALL_PUT_OUTPUT").unwrap());
    fs::create_dir_all(&output).unwrap();
    let value: Bytes = vec![0x5a; 1024].into();
    for writers in [16, 256] {
        let fixture = super::pipeline_tests::Fixture::new_with_root(
            tempdir::TempDir::new("small-put-multiowner").unwrap(),
            4,
            calls + 4096,
            KeyShape::new_single(32, 1, KeyType::uniform(1)),
            16,
            |config| {
                config.max_pending_batches = 128;
                config.max_pending_bytes = 64 * MIB;
                config.max_index_bytes = 256 * MIB;
                config.read_cache_bytes = 0;
                config.storage.batch_limits.max_encoded_bytes = 128 * 1024;
                config.storage.group_limits.max_encoded_bytes = 2 * MIB;
                config.storage.fragment_bytes = 128 * MIB as u64;
                config.storage.wal_file_bytes = 256 * MIB as u64;
                config.storage.max_reference_metadata_bytes = 64 * MIB;
                config.replay_limits = ReplayLimits {
                    max_batches: calls + 4096,
                    max_encoded_bytes: 256 * MIB,
                };
                let mut live = live_config(DurabilityTier::Synchronous);
                live.leased_write_window = None;
                live.leased_sync = LeasedSync::GroupFlush;
                live.leased_flush_min_frame_bytes = 65536;
                config.live_nvme = live;
            },
            |_| {},
        )
        .synchronous();
        let original = fixture.open();
        let live = LiveNvmeMetrics::new();
        for worker in &original.core.clients {
            let mut client = worker.client.lock();
            client.bench_local_live_fixture(true);
            client.set_live_metrics(live.clone()).unwrap();
        }
        let db = ConcurrentRemoteDatabase::new(original).unwrap();
        // Retain calls across the entire deterministic sequence, not just its
        // first prefix. Whole-window span counters do not depend on this sample.
        let stride = calls.div_ceil(512);
        db.arm_write_timeline(512, stride).unwrap();
        let gate = Barrier::new(writers + 1);
        // Owner workers now frame, submit and publish; callers allocate and
        // encode on the writer threads, which are not sampled here.
        let owner_worker_ticks = || -> u64 {
            fs::read_dir("/proc/self/task")
                .unwrap()
                .take(1024)
                .filter_map(|entry| {
                    let path = entry.ok()?.path();
                    if fs::read_to_string(path.join("comm")).ok()?.trim() != "value-wal-tcp" {
                        return None;
                    }
                    let stat = fs::read_to_string(path.join("stat")).ok()?;
                    let fields: Vec<_> = stat.rsplit_once(") ")?.1.split_whitespace().collect();
                    Some(fields[11].parse::<u64>().unwrap() + fields[12].parse::<u64>().unwrap())
                })
                .sum()
        };
        let cpu_ticks_before = owner_worker_ticks();
        let (samples, elapsed) = thread::scope(|scope| {
            let handles: Vec<_> = (0..writers)
                .map(|writer| {
                    let db = &db;
                    let value = &value;
                    let gate = &gate;
                    scope.spawn(move || {
                        let mut samples = Vec::with_capacity(calls.div_ceil(writers));
                        gate.wait();
                        for sequence in (writer..calls).step_by(writers) {
                            let began = Instant::now();
                            let operation = operation(sequence);
                            let sample = db.write_timeline.begin(sequence as u64);
                            if let Some(sample) = sample {
                                db.write_timeline.operation(sample, operation);
                            }
                            db.write_batch_traced(operation, vec![put(sequence, value)], sample)
                                .unwrap();
                            if let Some(sample) = sample {
                                db.write_timeline.complete(sample, None);
                            }
                            samples.push(began.elapsed().as_nanos() as u64);
                        }
                        samples
                    })
                })
                .collect();
            let began = Instant::now();
            gate.wait();
            let samples: Vec<u64> = handles
                .into_iter()
                .flat_map(|handle| handle.join().unwrap())
                .collect();
            (samples, began.elapsed())
        });
        let cpu_ticks_after = owner_worker_ticks();
        let timeline = db
            .finish_write_timeline_until(Instant::now() + Duration::from_secs(60))
            .unwrap();
        let mut sorted = samples.clone();
        sorted.sort_unstable();
        let quantile = |p: usize, scale: usize| sorted[(p * sorted.len()).div_ceil(scale) - 1];
        let mut counters = json!({});
        db.visit_write_metrics(|metrics| {
            counters = json!({
                "authority_prepare_ns": metrics.authority.authority_prepare_ns,
                "authority_admit_ns": metrics.authority.authority_admit_ns,
                "authority_windows": metrics.authority.authority_windows,
                "authority_batches": metrics.authority.authority_batches,
                "concurrent_pending_peak": metrics.pipeline.concurrent_pending_peak,
            });
        });
        let row = json!({
            "owners": 4, "writers": writers, "calls": calls, "value_bytes": 1024,
            "owner_worker_cpu_ticks": cpu_ticks_after - cpu_ticks_before,
            "clock_ticks_per_second": command("getconf", &["CLK_TCK"]).trim(),
            "elapsed_ns": elapsed.as_nanos(), "ops_per_second": calls as f64 / elapsed.as_secs_f64(),
            "mean_api_inflight": samples.iter().map(|n| *n as u128).sum::<u128>() as f64 / elapsed.as_nanos() as f64,
            "p50_ns": quantile(50, 100), "p99_ns": quantile(99, 100),
            "p999_ns": quantile(999, 1000), "api_ns": samples,
            "live": live.snapshot().counts, "counters": counters, "timeline": timeline,
            "scope": "one workstation; four owners; mTLS control; local-file io_uring; FUA; not remote hardware decision data"
        });
        fs::write(
            output.join(format!("c{writers}.json")),
            serde_json::to_vec(&row).unwrap(),
        )
        .unwrap();
        println!(
            "small_put_multiowner {}",
            json!({
                "writers": writers, "calls": calls, "ops_per_second": row["ops_per_second"],
                "p50_ns": row["p50_ns"], "p99_ns": row["p99_ns"], "mean_api_inflight": row["mean_api_inflight"],
            })
        );
        db.flush_durable().unwrap();
        for sequence in 0..calls {
            let reference = db
                .index_reference(KeySpace(0), &key(sequence), IndexReadCut::Latest)
                .unwrap()
                .unwrap();
            assert_eq!(
                fixture.nodes[reference.batch_address.partition as usize]
                    .log_for_test(1)
                    .read_record(&reference)
                    .unwrap(),
                put(sequence, &value),
            );
        }
        db.shutdown().unwrap();
    }
}

/// Block-device and process write counters of the scratch filesystem. These
/// include other processes on the same filesystem: rows record an idle window
/// and report background-corrected device bytes next to the raw ones.
struct DiskProbe {
    partition: String,
    device: String,
    journal: Option<PathBuf>,
}
impl DiskProbe {
    fn new(base: &Path) -> Self {
        let source = command(
            "findmnt",
            &["-n", "-o", "SOURCE", "-T", base.to_str().unwrap()],
        );
        let partition = Path::new(&source)
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned();
        let parent = command("lsblk", &["-n", "-o", "PKNAME", &source]);
        let device = if parent.is_empty() {
            partition.clone()
        } else {
            parent
        };
        let journal = fs::read_dir("/proc/fs/jbd2").ok().and_then(|entries| {
            entries
                .filter_map(Result::ok)
                .find(|entry| {
                    entry
                        .file_name()
                        .to_str()
                        .is_some_and(|name| name.starts_with(&format!("{partition}-")))
                })
                .map(|entry| entry.path().join("info"))
        });
        Self {
            partition,
            device,
            journal,
        }
    }
    /// Write requests, written bytes and flushes of the partition/device, jbd2
    /// transactions, and this process's storage write bytes.
    fn sample(&self) -> [u64; 6] {
        let stats = fs::read_to_string("/proc/diskstats").unwrap();
        let fields = |name: &str| -> Vec<u64> {
            stats
                .lines()
                .map(|line| line.split_whitespace().collect::<Vec<_>>())
                .find(|fields| fields.get(2) == Some(&name))
                .unwrap()[3..]
                .iter()
                .map(|value| value.parse().unwrap())
                .collect()
        };
        let partition = fields(&self.partition);
        let device = fields(&self.device);
        let transactions = self.journal.as_ref().map_or(0, |path| {
            fs::read_to_string(path)
                .unwrap()
                .split_whitespace()
                .next()
                .unwrap()
                .parse()
                .unwrap()
        });
        let io = fs::read_to_string("/proc/self/io").unwrap();
        let process = |name: &str| -> u64 {
            io.lines()
                .find_map(|line| line.strip_prefix(name))
                .unwrap()
                .trim()
                .parse()
                .unwrap()
        };
        [
            partition[4],
            partition[6] * 512,
            device.get(15).copied().unwrap_or(0),
            transactions,
            process("write_bytes:"),
            process("cancelled_write_bytes:"),
        ]
    }
}
fn disk_delta(after: [u64; 6], before: [u64; 6]) -> Value {
    json!({
        "device_write_requests": after[0] - before[0],
        "device_write_bytes": after[1] - before[1],
        "device_flushes": after[2] - before[2],
        "journal_transactions": after[3] - before[3],
        "process_write_bytes": after[4] - before[4],
        "process_cancelled_write_bytes": after[5] - before[5],
    })
}
/// Storage write bytes of live threads, keyed by thread name and id. A thread
/// that exits between samples moves its bytes into the process-only remainder.
fn thread_writes() -> std::collections::BTreeMap<(String, u64), u64> {
    fs::read_dir("/proc/self/task")
        .unwrap()
        .take(4096)
        .filter_map(|entry| {
            let path = entry.ok()?.path();
            let tid = path.file_name()?.to_str()?.parse().ok()?;
            let name = fs::read_to_string(path.join("comm"))
                .ok()?
                .trim()
                .to_owned();
            let io = fs::read_to_string(path.join("io")).ok()?;
            let bytes = io
                .lines()
                .find_map(|line| line.strip_prefix("write_bytes:"))?
                .trim()
                .parse()
                .ok()?;
            Some(((name, tid), bytes))
        })
        .collect()
}
fn thread_delta(
    after: &std::collections::BTreeMap<(String, u64), u64>,
    before: &std::collections::BTreeMap<(String, u64), u64>,
) -> Value {
    let mut by_name = std::collections::BTreeMap::<String, u64>::new();
    for ((name, tid), bytes) in after {
        let delta = bytes - before.get(&(name.clone(), *tid)).copied().unwrap_or(0);
        if delta != 0 {
            *by_name.entry(name.clone()).or_default() += delta;
        }
    }
    json!(by_name)
}
fn owner_delta(
    after: &[(PartitionStats, u64, u64)],
    before: &[(PartitionStats, u64, u64)],
) -> Value {
    let sum = |field: fn(&(PartitionStats, u64, u64)) -> u64| -> u64 {
        after
            .iter()
            .zip(before)
            .map(|(a, b)| field(a) - field(b))
            .sum()
    };
    json!({
        "persistence_barriers": sum(|s| s.0.persistence_barriers),
        "wal_file_syncs": sum(|s| s.0.wal_file_syncs),
        "namespace_syncs": sum(|s| s.0.namespace_syncs),
        "inventory_witnesses": sum(|s| s.0.inventory_witnesses),
        "live_frontier_requests": sum(|s| s.0.live_frontier_requests),
        "live_frontier_installs": sum(|s| s.0.live_frontier_installs),
        "live_frontier_bytes": sum(|s| s.0.live_frontier_bytes),
        "live_frontier_deferred": sum(|s| s.0.live_frontier_deferred),
        "read_persist_requests": sum(|s| s.0.read_persist_requests),
        "cold_grant_syncs": sum(|s| s.0.cold_grant_syncs),
        "live_frontier_file_syncs": sum(|s| s.0.live_frontier_file_syncs),
        "live_frontier_namespace_syncs": sum(|s| s.0.live_frontier_namespace_syncs),
        "frontier_syncer_runs": sum(|s| s.1),
        "frontier_sync_requests": sum(|s| s.2),
    })
}
fn owner_sample(nodes: &[Arc<StorageNode>]) -> Vec<(PartitionStats, u64, u64)> {
    nodes
        .iter()
        .map(|node| {
            let log = node.log_for_test(1);
            let frontier = log.frontier().map(|frontier| frontier.stats());
            (
                log.stats(),
                frontier.map_or(0, |stats| stats.fsync_calls),
                frontier.map_or(0, |stats| stats.flush_requests),
            )
        })
        .collect()
}
fn pipeline_delta(after: PipelineStats, before: PipelineStats) -> Value {
    json!({
        "owner_drains": after.owner_drains - before.owner_drains,
        "owner_drain_batches": after.owner_drain_batches - before.owner_drain_batches,
        "owner_drain_stop_empty": after.owner_drain_stop_empty - before.owner_drain_stop_empty,
        "owner_drain_stop_allocated": after.owner_drain_stop_allocated - before.owner_drain_stop_allocated,
        "owner_drain_stop_fence": after.owner_drain_stop_fence - before.owner_drain_stop_fence,
        "owner_drain_stop_limit": after.owner_drain_stop_limit - before.owner_drain_stop_limit,
        "owner_drain_stop_credit": after.owner_drain_stop_credit - before.owner_drain_stop_credit,
    })
}

/// One attribution case: two owners like the G35 fleet shape, leased writes to
/// buffered local WAL files (`C-buf`).
#[allow(clippy::too_many_lines)]
fn overhead_case(
    probe: &DiskProbe,
    arm: &str,
    tier: DurabilityTier,
    writers: usize,
    calls: usize,
    repetition: usize,
    idle: Duration,
) -> Value {
    // `C-buf-pre` pre-writes (zero-fills and syncs) each WAL file before grant.
    // A `-base` suffix selects the pre-change write path in this process.
    let base = arm.ends_with("-base");
    let arm_kind = arm.trim_end_matches("-base");
    crate::distributed::storage::BASELINE_WRITE_PATH.store(base, Ordering::Relaxed);
    let prewrite = arm_kind == "C-buf-pre";
    let mut fixture = super::pipeline_tests::Fixture::new_with_root(
        tempdir::TempDir::new_in(CACHE, "write-overhead").unwrap(),
        2,
        calls + 8192,
        KeyShape::new_single(32, 1, KeyType::uniform(1)),
        32,
        |config| {
            config.max_pending_batches = 128;
            config.max_pending_bytes = 64 * MIB;
            config.max_index_bytes = 512 * MIB;
            config.max_admitted_batches = calls + 8192;
            config.owner_append_occupancy = 8;
            config.read_cache_bytes = 0;
            config.storage.batch_limits.max_operations = 32;
            config.storage.batch_limits.max_encoded_bytes = 3 * MIB;
            config.storage.group_limits.max_encoded_bytes = 4 * MIB - 8;
            config.storage.fragment_bytes = 128 * MIB as u64;
            config.storage.wal_file_bytes = 256 * MIB as u64;
            config.storage.max_reference_metadata_bytes = 64 * MIB;
            config.replay_limits = ReplayLimits {
                max_batches: 4096,
                max_encoded_bytes: 256 * MIB,
            };
            let mut live = live_config(tier);
            live.leased_write_window = None;
            live.leased_sync = LeasedSync::GroupFlush;
            live.leased_flush_min_frame_bytes = 65536;
            live.max_inflight_bytes = 16 * MIB;
            config.live_nvme = live;
        },
        |config| config.wal_prewrite = prewrite,
    );
    if tier == DurabilityTier::Synchronous {
        fixture = fixture.synchronous();
    }
    let original = fixture.open();
    let live = LiveNvmeMetrics::new();
    for worker in &original.core.clients {
        let mut client = worker.client.lock();
        client.bench_local_live_fixture(true);
        client.set_live_metrics(live.clone()).unwrap();
    }
    let db = ConcurrentRemoteDatabase::new(original).unwrap();
    let value: Bytes = vec![0x5a; 512].into();
    let warmup = 4096;
    write_phase(&db, writers, 0, warmup, &value);
    // Background writes of other processes on the shared filesystem.
    let idle_before = probe.sample();
    thread::sleep(idle);
    let idle_after = probe.sample();
    let live_before = live.snapshot().counts;
    let owners_before = owner_sample(&fixture.nodes);
    let pipeline_before = db.pipeline_stats();
    let threads_before = thread_writes();
    let prewrite_bytes = || -> u64 {
        fixture
            .nodes
            .iter()
            .map(|node| {
                node.log_for_test(1)
                    .live_file_preparation_stats()
                    .live_file_prewrite_bytes
            })
            .sum()
    };
    let prewrite_before = prewrite_bytes();
    let disk_before = probe.sample();
    let (samples, elapsed) = write_phase(&db, writers, warmup, calls, &value);
    let disk_end = probe.sample();
    let live_end = live.snapshot().counts;
    let owners_end = owner_sample(&fixture.nodes);
    let pipeline_end = db.pipeline_stats();
    // Deferred background persistence and journal work after the API window.
    thread::sleep(idle);
    let disk_tail = probe.sample();
    let threads_tail = thread_writes();
    let owners_tail = owner_sample(&fixture.nodes);
    let prewrite_in_window = prewrite_bytes() - prewrite_before;
    // Second background sample after the case settled: the correction uses the
    // lower of the two idle rates so a burst from another process cannot
    // erase this case's own device bytes.
    let settle_before = probe.sample();
    thread::sleep(idle);
    let settle_after = probe.sample();
    let mut sorted = samples.clone();
    sorted.sort_unstable();
    let quantile =
        |p: usize, scale: usize| sorted[(p * sorted.len()).div_ceil(scale) - 1] as f64 / 1000.0;
    let payload = (calls * 512) as f64;
    let idle_rate = ((idle_after[1] - idle_before[1]) as f64)
        .min((settle_after[1] - settle_before[1]) as f64)
        / idle.as_secs_f64();
    let window = disk_delta(disk_end, disk_before);
    let total = disk_delta(disk_tail, disk_before);
    let live_delta = delta(live_end, live_before);
    let wal = live_delta["completed_write_bytes"].as_u64().unwrap() as f64;
    let corrected = |raw: &Value, seconds: f64| {
        raw["device_write_bytes"].as_u64().unwrap() as f64 - idle_rate * seconds
    };
    let window_seconds = elapsed.as_secs_f64();
    let total_seconds = window_seconds + idle.as_secs_f64();
    let device_window = corrected(&window, window_seconds);
    let device_total = corrected(&total, total_seconds);
    let frames = live_delta["leased_frames"].as_u64().unwrap();
    let ratio = |numerator: &str, denominator: &str| {
        let d = live_delta[denominator].as_u64().unwrap();
        if d == 0 {
            Value::Null
        } else {
            json!(live_delta[numerator].as_u64().unwrap() as f64 / d as f64)
        }
    };
    let row = json!({
        "arm": arm, "tier": tier.to_string(), "writers": writers, "repetition": repetition,
        "loadavg": fs::read_to_string("/proc/loadavg").unwrap().trim(),
        "scope": "loaded laptop: rates and latencies indicative only",
        "calls": calls, "value_bytes": 512, "owners": 2,
        "elapsed_seconds": window_seconds, "puts_per_second": calls as f64 / window_seconds,
        "p50_us": quantile(50, 100), "p99_us": quantile(99, 100), "p999_us": quantile(999, 1000),
        "mean_api_inflight": samples.iter().map(|n| *n as f64).sum::<f64>() / elapsed.as_nanos() as f64,
        "payload_bytes": calls * 512,
        "wal_per_payload": wal / payload,
        "device_per_payload_window": device_window / payload,
        "device_per_payload_with_tail": device_total / payload,
        // Physically the device cannot write less than the WAL bytes of the
        // case: a lower corrected value means background noise was removed
        // from it, so the device columns of this row are invalid.
        "device_valid": device_total >= wal,
        "residual_per_payload_with_tail": (device_total - wal) / payload,
        "process_write_per_payload": window["process_write_bytes"].as_u64().unwrap() as f64 / payload,
        "idle_device_bytes_per_second": idle_rate,
        "batches_per_frame": ratio("leased_frame_batches", "leased_frames"),
        "frames_per_submit_call": if frames == 0 { Value::Null } else { ratio("leased_frames", "write_submit_calls") },
        "disk_window": window, "disk_with_tail": total,
        "owner_window": owner_delta(&owners_end, &owners_before),
        "owner_with_tail": owner_delta(&owners_tail, &owners_before),
        "pipeline": pipeline_delta(pipeline_end, pipeline_before),
        "live": live_delta,
        "prewrite_bytes_with_tail": prewrite_in_window,
        "thread_write_bytes_with_tail": thread_delta(&threads_tail, &threads_before),
    });
    println!(
        "write_overhead {}",
        json!({
            "arm": arm, "tier": row["tier"], "writers": writers, "rep": repetition,
            "puts_per_second": row["puts_per_second"], "p50_us": row["p50_us"], "p99_us": row["p99_us"],
            "wal": row["wal_per_payload"], "device": row["device_per_payload_with_tail"],
            "installs": row["owner_with_tail"]["live_frontier_installs"],
        })
    );
    db.shutdown().unwrap();
    row
}

#[test]
#[ignore = "explicit release-only workstation write-overhead attribution; shared filesystem"]
fn write_overhead_attribution() {
    assert!(
        !cfg!(debug_assertions),
        "run this ignored measurement with --release"
    );
    let tiers = selection(
        "TH_OVERHEAD_TIERS",
        "synchronous,native",
        &["native", "synchronous"],
    );
    let writers = selection("TH_OVERHEAD_WRITERS", "16,64,256", &["16", "64", "256"]);
    let arms = selection(
        "TH_OVERHEAD_ARMS",
        "C-buf-base,C-buf,C-buf-pre-base,C-buf-pre",
        &["C-buf", "C-buf-pre", "C-buf-base", "C-buf-pre-base"],
    );
    let setting = |name: &str, default: usize| {
        std::env::var(name)
            .map(|value| value.parse::<usize>().unwrap())
            .unwrap_or(default)
    };
    let sync_calls = setting("TH_OVERHEAD_SYNC_CALLS", 8192);
    let native_calls = setting("TH_OVERHEAD_NATIVE_CALLS", 65536);
    let repetitions = setting("TH_OVERHEAD_REPETITIONS", 3);
    let idle = Duration::from_millis(setting("TH_OVERHEAD_IDLE_MS", 2000) as u64);
    assert!(
        [sync_calls, native_calls]
            .iter()
            .all(|calls| (1024..=1 << 20).contains(calls))
            && (1..=20).contains(&repetitions)
    );
    let label = std::env::var("TH_OVERHEAD_LABEL").unwrap_or_else(|_| "run".into());
    let output = PathBuf::from(OUTPUT).join(format!("overhead-{label}-{}", std::process::id()));
    fs::create_dir_all(&output).unwrap();
    fs::create_dir_all(CACHE).unwrap();
    let probe = DiskProbe::new(Path::new(CACHE));
    let environment = environment(Path::new(CACHE));
    let mut rows = Vec::new();
    for repetition in 1..=repetitions {
        for tier in [DurabilityTier::Synchronous, DurabilityTier::Native]
            .into_iter()
            .filter(|tier| tiers.contains(&tier.to_string()))
        {
            let mut order: Vec<_> = writers
                .iter()
                .map(|n| n.parse::<usize>().unwrap())
                .collect();
            if repetition % 2 == 0 {
                order.reverse();
            }
            for &writer_count in &order {
                for arm in &arms {
                    rows.push(overhead_case(
                        &probe,
                        arm,
                        tier,
                        writer_count,
                        if tier == DurabilityTier::Synchronous {
                            sync_calls
                        } else {
                            native_calls
                        },
                        repetition,
                        idle,
                    ));
                    fs::write(
                        output.join("results.json"),
                        serde_json::to_vec_pretty(
                            &json!({"environment": environment, "rows": rows}),
                        )
                        .unwrap(),
                    )
                    .unwrap();
                }
            }
        }
    }
    println!("write_overhead output: {}", output.display());
}

/// Owner rule for packing: a lone READY batch with an empty owner queue is
/// submitted at once, alone in its padded frame. The write completes with no
/// other caller, and exactly one frame carrying one batch was released.
#[test]
fn lone_batch_with_empty_queue_is_submitted_without_waiting() {
    for tier in [DurabilityTier::Native, DurabilityTier::Synchronous] {
        let mut fixture = super::pipeline_tests::Fixture::new_with_root(
            tempdir::TempDir::new("lone-batch").unwrap(),
            1,
            64,
            KeyShape::new_single(32, 1, KeyType::uniform(1)),
            32,
            |config| {
                config.storage.batch_limits.max_encoded_bytes = 128 * 1024;
                config.storage.group_limits.max_encoded_bytes = 2 * MIB;
                config.storage.fragment_bytes = 8 * MIB as u64;
                config.storage.wal_file_bytes = 16 * MIB as u64;
                let mut live = live_config(tier);
                live.leased_write_window = None;
                live.leased_sync = LeasedSync::GroupFlush;
                live.leased_flush_min_frame_bytes = 65536;
                live.max_inflight_bytes = 16 * MIB;
                config.live_nvme = live;
                config.max_pending_bytes = 64 * MIB;
                // Reads must reach the owner path, not a coordinator cache.
                config.read_cache_bytes = 0;
            },
            |_| {},
        );
        if tier == DurabilityTier::Synchronous {
            fixture = fixture.synchronous();
        }
        let original = fixture.open();
        let live = LiveNvmeMetrics::new();
        for worker in &original.core.clients {
            let mut client = worker.client.lock();
            client.bench_local_live_fixture(true);
            client.set_live_metrics(live.clone()).unwrap();
        }
        let db = ConcurrentRemoteDatabase::new(original).unwrap();
        let value: Bytes = vec![0x5a; 512].into();
        db.write_batch(operation(0), vec![put(0, &value)]).unwrap();
        let counts = live.snapshot().counts;
        assert_eq!(counts.leased_frames, 1, "{tier}");
        assert_eq!(counts.leased_frame_batches, 1, "{tier}");
        assert_eq!(counts.leased_deferred_submits, 0, "{tier}");
        assert_eq!(
            counts.fua_writes,
            u64::from(tier == DurabilityTier::Synchronous),
            "{tier}"
        );
        // Immediate read of the newest batch. With no per-adoption sync, a
        // synchronous owner read of it persists on demand before serving.
        assert_eq!(
            db.get(KeySpace(0), &key(0)).unwrap(),
            Some(value.clone()),
            "{tier}"
        );
        db.shutdown().unwrap();
    }
}
