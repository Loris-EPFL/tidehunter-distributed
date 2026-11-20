use super::*;
use crate::mainnet_trace::{OBJECTS, SCOPE};
use tidehunter::minibytes::Bytes;
fn trace() -> Trace {
    Trace::load(
        &Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../experiments/sui-mainnet/fixture/manifest.json"),
        "4264e79e0cbe4a1e36d94468df2f049f988eac57877c9bf2e7e175489dc922fa",
    )
    .unwrap()
}
fn temp(label: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "mainnet-replay-{label}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).unwrap();
    root
}
fn config(root: &Path) -> ReplayConfig {
    ReplayConfig {
        schema_version: 1,
        case_id: None,
        source_revision: None,
        backend: BackendSpec {
            kind: "native".into(),
            options: serde_json::json!({"path":root.join("db"),"native_wal_geometry":"small"}),
        },
        workload: ReplayWorkload {
            kind: ReplayKind::SuiMainnetReplay,
            manifest_path: Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../experiments/sui-mainnet/fixture/manifest.json"),
            manifest_sha256: "4264e79e0cbe4a1e36d94468df2f049f988eac57877c9bf2e7e175489dc922fa"
                .into(),
            repetitions: 2,
            minimum_measurement_seconds: 0.0,
            node_databases: None,
            checkpoint_pipeline_depth: None,
        },
        execution: ExecutionConfig {
            mode: ExecutionMode::ClosedLoop,
            concurrency: 1,
            client_shards: Some(1),
            queue_capacity: 8,
            request_timeout_ms: 30000,
            offered_ops_per_second: None,
        },
        limits: LimitsConfig {
            max_keys: 10000,
            max_bytes: 64 * 1024 * 1024,
            max_operations: 10000,
            deadline_seconds: 120,
            shutdown_grace_seconds: 5,
        },
        measurement_gate: None,
        measurement_completion_gate: None,
        diagnostics: DiagnosticsConfig::default(),
        capacity_r0: None,
    }
}
#[test]
fn native_disk_reservation_covers_measured_growth_without_whole_wal_preallocation() {
    let t = trace();
    let payload_per_repeat = t
        .preload
        .iter()
        .chain(t.batches.iter().flat_map(|batch| &batch.entries))
        .map(|row| (row.key.len() + row.value.len()) as u64)
        .sum::<u64>();
    assert_eq!(payload_per_repeat, 876_225);
    let fragment_size = 128 * 1024 * 1024;
    let (reserve, fragments) =
        native_disk_reservation(payload_per_repeat * 5456, fragment_size).unwrap();
    assert_eq!(fragments, 805_306_368);
    assert_eq!(reserve, 10_366_673_568);
    // Real R=5456 physical usage; the 2x payload budget alone exceeds it.
    assert!(reserve - fragments > 4_876_845_056);
    assert_eq!(
        native_disk_reservation(payload_per_repeat * 512, fragment_size).unwrap(),
        (1_702_560_768, fragments)
    );
    assert!(native_disk_reservation(u64::MAX, fragment_size).is_err());
    assert!(native_disk_reservation(0, u64::MAX).is_err());
    assert!(native_disk_reservation((u64::MAX - fragments) / 2 + 1, fragment_size).is_err());
}

#[test]
fn configuration_admission_reports_estimates_without_host_capacity() {
    let root = temp("configuration-admission");
    let cfg = config(&root);
    let (_, prepared, resources) = cfg.admit_configuration().unwrap();
    assert_eq!(prepared.len(), 2);
    assert_eq!(
        resources["host_resources"],
        "not_checked; enforced at run time on the executing host"
    );
    assert!(
        resources["estimated_disk_reservation_bytes"]
            .as_u64()
            .unwrap()
            > 0
    );
    assert!(
        resources["estimated_memory_reservation_bytes"]
            .as_u64()
            .unwrap()
            > 0
    );
    assert!(resources["native_index_geometry"].is_object());
    assert!(resources.get("available_bytes").is_none());
    assert!(resources.get("memory_available_bytes").is_none());
    assert!(!root.join("db").exists());
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn configuration_admission_accepts_estimates_that_execution_rejects() {
    let root = temp("oversized-admission");
    let cfg = config(&root);
    // The distributed arm needs the leased live route; replay::tests owns its
    // fixture options and is gated the same way.
    #[cfg(feature = "test-utils")]
    let arms = [
        cfg.clone(),
        ReplayConfig {
            backend: BackendSpec {
                kind: "tidehunter_tcp".into(),
                options: crate::backends::tidehunter::replay::tests::local_options(&root),
            },
            ..cfg.clone()
        },
    ];
    #[cfg(not(feature = "test-utils"))]
    let arms = [cfg.clone()];
    for cfg in arms {
        let kind = cfg.backend.kind.clone();
        if kind == "tidehunter_tcp" {
            let (_, _, resources) = cfg.admit_configuration().unwrap();
            let coordinator = &resources["coordinator_disk_reservation"];
            assert!(coordinator["reservation_bytes"].as_u64().unwrap() > 0);
            assert_eq!(
                resources["estimated_disk_reservation_bytes"],
                coordinator["reservation_bytes"]
            );
            assert_eq!(resources["wal_fragment_reservation_bytes"], 0);
        }
        for dimension in [
            "estimated_disk_reservation_bytes",
            "estimated_memory_reservation_bytes",
        ] {
            let oversized = |config: &ReplayConfig, trace: &Trace| {
                let mut resources = resource_estimate(config, trace)?;
                // Inject an impossible reservation; never allocate it or fill the filesystem.
                resources[dimension] = u64::MAX.into();
                if dimension == "estimated_memory_reservation_bytes" {
                    resources["estimated_disk_reservation_bytes"] = 0.into();
                }
                Ok(resources)
            };
            let (_, _, resources) = cfg.admit_with(oversized, false).unwrap();
            assert_eq!(resources[dimension], u64::MAX);
            let error = cfg.admit_with(oversized, true).unwrap_err().to_string();
            assert!(
                error.contains(if dimension == "estimated_disk_reservation_bytes" {
                    if kind == "native" {
                        "insufficient native replay disk space"
                    } else {
                        "insufficient distributed replay disk space"
                    }
                } else {
                    "insufficient memory"
                }),
                "{error}"
            );
        }
    }
    assert!(!root.join("db").exists());
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_native_two_repeats_same_final_contents_for_one_and_four_callers() {
    let trace = trace();
    let prepared = Arc::new(prepare(&trace, 2).unwrap());
    let mut finals = Vec::new();
    for n in [1, 4] {
        let root = temp(&format!("native-{n}"));
        let cfg = config(&root);
        let backend =
            Arc::new(NativeReplayBackend::open(&cfg.backend.options, &trace.tables).unwrap());
        let counts = execute(
            Arc::clone(&backend),
            &trace,
            Arc::clone(&prepared),
            n,
            Instant::now() + Duration::from_secs(120),
            30000,
        )
        .await
        .unwrap();
        assert_eq!(counts.store_gets, 1014);
        assert_eq!(counts.overlay_demands, 232);
        assert_eq!(counts.put_members, 1414);
        assert_eq!(counts.write_calls, 18);
        assert_eq!(counts.transactions, 90);
        assert_eq!(counts.checkpoints, 2);
        let mut expected = BTreeMap::new();
        for p in prepared.iter() {
            assert_eq!(
                p.batches
                    .iter()
                    .map(|b| b.entries.len())
                    .collect::<Vec<_>>(),
                vec![2, 1, 90, 3, 1, 45, 45, 519, 1]
            );
            for row in p
                .preload
                .iter()
                .chain(p.batches.iter().flat_map(|b| &b.entries))
            {
                expected.insert((row.table, row.key.clone()), row.value.clone());
            }
        }
        let mut actual = BTreeMap::new();
        for ((table, key), value) in &expected {
            let got = backend.get(*table, key).unwrap().unwrap();
            assert_eq!(&got, value);
            actual.insert((*table, key.clone()), got);
        }
        // A mixed-domain batch is rejected before any member is applied.
        let original = &prepared[0].batches[7].entries[0];
        let changed = Row {
            value: Bytes::from_static(b"not the original value"),
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
        finals.push(actual);
        drop(backend);
        std::fs::remove_dir_all(root).unwrap();
    }
    assert_eq!(finals[0], finals[1]);
    let a = &prepared[0];
    let b = &prepared[1];
    for (left, right) in a
        .preload
        .iter()
        .chain(a.batches.iter().flat_map(|x| &x.entries))
        .zip(
            b.preload
                .iter()
                .chain(b.batches.iter().flat_map(|x| &x.entries)),
        )
    {
        assert_eq!(left.value, right.value);
        assert_eq!(left.key.len(), right.key.len());
        assert_eq!(left.tx, right.tx);
        if trace.tables[usize::from(left.table)].control {
            assert_eq!(left.key, right.key);
        } else {
            assert_ne!(left.key, right.key);
        }
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_report_keeps_parent_batches_and_rejects_short_window() {
    let root = temp("report");
    let mut cfg = config(&root);
    cfg.workload.repetitions = 1;
    cfg.workload.minimum_measurement_seconds = 30.0;
    assert!(
        run(cfg, &root.join("result"))
            .await
            .unwrap_err()
            .to_string()
            .contains("shorter")
    );
    let summary: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("result/summary.json")).unwrap()).unwrap();
    assert_eq!(summary["status"], "failed");
    assert_eq!(summary["phase_counts"]["counts"]["store_gets"], 507);
    assert_eq!(summary["phase_counts"]["profile"]["scope"], SCOPE);
    eprintln!(
        "mainnet-s2-fixture-window={}",
        summary["phase_counts"]["profile"]["measurement_elapsed_seconds"]
    );
    let rows = std::fs::read_to_string(root.join("result/samples.csv")).unwrap();
    assert_eq!(rows.lines().count(), 517);
    assert!(rows.lines().next().unwrap().contains(
        "source_checkpoint,source_tx,repetition,db,table,batch_phase,parent_batch,members"
    ));
    std::fs::remove_dir_all(root).unwrap();
}
#[test]
fn replay_client_shards_validation_is_host_independent() {
    let root = temp("replay-shards");
    let mut cfg = config(&root);
    cfg.execution.concurrency = 16;
    cfg.execution.queue_capacity = 16;
    for shards in [None, Some(1)] {
        cfg.execution.client_shards = shards;
        cfg.validate().unwrap();
    }
    for shards in [0, 2, 16] {
        cfg.execution.client_shards = Some(shards);
        assert!(
            cfg.validate()
                .unwrap_err()
                .to_string()
                .contains("client_shards=1")
        );
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn discriminant_and_manifest_bounds_reject_mixed_synthetic_options() {
    let root = temp("config");
    let cfg = config(&root);
    cfg.validate().unwrap();
    for field in ["dataset", "phases", "seed"] {
        let mut value = serde_json::to_value(&cfg).unwrap();
        value[field] = serde_json::json!({});
        assert!(serde_json::from_value::<ReplayConfig>(value).is_err());
    }
    let mut value = serde_json::to_value(&cfg).unwrap();
    value["workload"]["mix"] = serde_json::json!({"read":1});
    assert!(serde_json::from_value::<ReplayConfig>(value).is_err());
    for mutate in [0, 1, 2, 3, 4] {
        let mut c = cfg.clone();
        match mutate {
            0 => c.workload.repetitions = u64::MAX,
            1 => c.limits.max_bytes = 1,
            2 => c.execution.mode = ExecutionMode::OpenLoop,
            3 => c.execution.concurrency = 0,
            _ => c.workload.minimum_measurement_seconds = f64::INFINITY,
        };
        assert!(c.validate().is_err());
    }
    let mut c = cfg;
    c.backend.kind = "tidehunter".into();
    let error = c.validate().unwrap_err();
    assert!(error.downcast_ref::<UnsupportedReplayBackend>().is_some());
    std::fs::remove_dir_all(root).unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bounded_callers_reject_busy_dispatch_and_join_on_deadline() {
    let root = temp("bounded");
    let t = trace();
    let p = Arc::new(prepare(&t, 1).unwrap());
    let c = config(&root);
    let backend = Arc::new(NativeReplayBackend::open(&c.backend.options, &t.tables).unwrap());
    let now = Instant::now();
    let mut pool = Pool::new(backend, p, 1, now, 30000).unwrap();
    pool.dispatch(0, 0, Operation::Preload, now, now + Duration::from_secs(10))
        .unwrap();
    assert!(
        pool.dispatch(0, 0, Operation::Preload, now, now + Duration::from_secs(10))
            .is_err()
    );
    let result = pool.receive(now + Duration::from_secs(10)).await.unwrap();
    pool.recycle(result).unwrap();
    pool.dispatch(
        0,
        0,
        Operation::Commit(0),
        now,
        now - Duration::from_millis(1),
    )
    .unwrap();
    let result = pool
        .receive_until(now + Duration::from_secs(10))
        .await
        .unwrap();
    assert!(pool.recycle(result).is_err());
    pool.close().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wrong_native_bytes_fail_before_finalization() {
    let root = temp("wrong-bytes");
    let t = trace();
    let cfg = config(&root);
    let mut p = prepare(&t, 1).unwrap();
    let read = p[0]
        .reads
        .iter_mut()
        .flatten()
        .find(|r| r.producer.is_none())
        .unwrap();
    // Preserve the length so a size-only verification cannot pass this test.
    let mut wrong = read.row.value.to_vec();
    wrong[0] ^= 1;
    read.row.value = Bytes::from(wrong);
    let backend = Arc::new(NativeReplayBackend::open(&cfg.backend.options, &t.tables).unwrap());
    assert!(
        execute(
            Arc::clone(&backend),
            &t,
            Arc::new(p),
            4,
            Instant::now() + Duration::from_secs(30),
            30000
        )
        .await
        .is_err()
    );
    assert!(backend.get(11, b"").unwrap().is_none());
    drop(backend);
    std::fs::remove_dir_all(root).unwrap();
}

/// A gate around the real native commit is a deterministic blocked-I/O test hook.
/// It is never used for fixture performance evidence or as a substitute storage engine.
struct GatedNative {
    inner: Arc<NativeReplayBackend>,
    gate: Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>,
    health_failure_at: Option<usize>,
    health_calls: std::sync::atomic::AtomicUsize,
}

impl ReplayBackend for GatedNative {
    fn check_background_health(&self) -> BackendResult<()> {
        self.inner.check_background_health()?;
        if let Some(boundary) = self.health_failure_at {
            let call = self.health_calls.fetch_add(1, Ordering::Relaxed);
            if call >= boundary {
                return Err(crate::backend::native_error(
                    tidehunter::db::DbError::BackgroundFailure(Arc::new(
                        tidehunter::db::BackgroundFailure {
                            role: tidehunter::db::BackgroundThread::Flusher,
                            reason: tidehunter::db::BackgroundFailureReason::Panic(
                                "injected replay background panic".into(),
                            ),
                        },
                    )),
                ));
            }
        }
        Ok(())
    }
    fn get(&self, table: u16, key: &[u8]) -> BackendResult<Option<Bytes>> {
        self.inner.get(table, key)
    }
    fn commit(&self, db: u16, entries: &[Row]) -> BackendResult<()> {
        if entries.last().is_some_and(|row| row.table == 11) {
            let (mutex, condvar) = &*self.gate;
            let mut released = mutex.lock().unwrap();
            while !*released {
                released = condvar.wait(released).unwrap();
            }
        }
        self.inner.commit(db, entries)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blocked_native_commit_times_out_within_deadline_and_grace() {
    let root = temp("blocked-commit");
    let mut cfg = config(&root);
    cfg.workload.repetitions = 1;
    cfg.execution.request_timeout_ms = 1000;
    cfg.limits.deadline_seconds = 2;
    cfg.limits.shutdown_grace_seconds = 1;
    let trace = cfg.validate().unwrap();
    let prepared = Arc::new(prepare(&trace, 1).unwrap());
    let native = Arc::new(NativeReplayBackend::open(&cfg.backend.options, &trace.tables).unwrap());
    #[allow(clippy::mutex_atomic, reason = "Condvar gate needs the mutex")]
    let gate = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
    let backend = Arc::new(GatedNative {
        inner: Arc::clone(&native),
        gate: Arc::clone(&gate),
        health_failure_at: None,
        health_calls: std::sync::atomic::AtomicUsize::new(0),
    });
    let started = Instant::now();
    let result = run_prepared(
        cfg,
        &root.join("result"),
        trace,
        prepared,
        backend,
        serde_json::Value::Null,
        (started, now_unix(), None),
    )
    .await;
    assert!(result.is_err());
    assert!(started.elapsed() < Duration::from_secs(3));
    let summary: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("result/summary.json")).unwrap()).unwrap();
    assert_eq!(summary["status"], "failed");
    assert_eq!(summary["phase_counts"]["run_status"], "timed_out");
    assert_eq!(summary["phase_counts"]["unknown_outcomes"], 1);
    assert_eq!(summary["phase_counts"]["unresolved_workers"], 1);
    assert_eq!(summary["phase_counts"]["sustained_30s_qualified"], false);
    assert_eq!(summary["phase_counts"]["counts"]["write_calls"], 7);
    assert_eq!(summary["phase_counts"]["counts"]["checkpoints"], 0);
    let rows = std::fs::read_to_string(root.join("result/samples.csv")).unwrap();
    assert_eq!(
        rows.lines()
            .filter(|row| row.contains(",unknown_mutation,"))
            .count(),
        1
    );
    let (mutex, condvar) = &*gate;
    *mutex.lock().unwrap() = true;
    condvar.notify_all();
    // The detached call can finish, but the scheduler must never issue phase 8.
    let cleanup_deadline = Instant::now() + Duration::from_secs(2);
    while Arc::strong_count(&native) > 1 && Instant::now() < cleanup_deadline {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(Arc::strong_count(&native), 1);
    assert!(native.get(16, &2_u32.to_be_bytes()).unwrap().is_none());
    drop(native);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn injected_background_failure_rejects_each_replay_boundary() {
    for boundary in 0..3 {
        let root = temp(&format!("health-{boundary}"));
        let mut cfg = config(&root);
        cfg.workload.repetitions = 1;
        let trace = cfg.validate().unwrap();
        let prepared = Arc::new(prepare(&trace, 1).unwrap());
        let native =
            Arc::new(NativeReplayBackend::open(&cfg.backend.options, &trace.tables).unwrap());
        // Delegate all storage to the real engine; inject only its typed health
        // result, since engine-owned worker panic hooks are private to that crate.
        #[allow(clippy::mutex_atomic, reason = "Condvar gate needs the mutex")]
        let backend = Arc::new(GatedNative {
            inner: Arc::clone(&native),
            gate: Arc::new((std::sync::Mutex::new(true), std::sync::Condvar::new())),
            health_failure_at: Some(boundary),
            health_calls: std::sync::atomic::AtomicUsize::new(0),
        });
        let result = run_prepared(
            cfg,
            &root.join("result"),
            trace,
            prepared,
            backend,
            serde_json::Value::Null,
            (Instant::now(), now_unix(), None),
        )
        .await;
        assert!(result.is_err());
        let summary: serde_json::Value =
            serde_json::from_slice(&std::fs::read(root.join("result/summary.json")).unwrap())
                .unwrap();
        assert_eq!(summary["status"], "failed");
        assert_eq!(summary["phase_counts"]["sustained_30s_qualified"], false);
        assert!(
            summary["phase_counts"]["error"]
                .as_str()
                .unwrap()
                .contains("injected replay background panic")
        );
        assert_eq!(
            summary["phase_counts"]["counts"]["checkpoints"],
            if boundary == 0 { 0 } else { 1 }
        );
        drop(native);
        std::fs::remove_dir_all(root).unwrap();
    }
}

/// Inject an uncertain acknowledgement only after a real engine mutation.
struct PartialNative(Arc<NativeReplayBackend>);
impl ReplayBackend for PartialNative {
    fn check_background_health(&self) -> BackendResult<()> {
        self.0.check_background_health()
    }
    fn get(&self, table: u16, key: &[u8]) -> BackendResult<Option<Bytes>> {
        self.0.get(table, key)
    }
    fn commit(&self, db: u16, entries: &[Row]) -> BackendResult<()> {
        if entries.last().is_some_and(|row| row.table == 11) {
            self.0.commit(db, &entries[..1])?;
            return Err(Box::new(UnknownReplayMutation(
                "injected loss after partial application".into(),
            )));
        }
        self.0.commit(db, entries)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unknown_partial_commit_stops_without_retry_or_final_get_recovery() {
    let root = temp("partial");
    let mut cfg = config(&root);
    cfg.workload.repetitions = 1;
    let trace = cfg.validate().unwrap();
    let prepared = Arc::new(prepare(&trace, 1).unwrap());
    let native = Arc::new(NativeReplayBackend::open(&cfg.backend.options, &trace.tables).unwrap());
    let result = run_prepared(
        cfg,
        &root.join("result"),
        trace,
        prepared,
        Arc::new(PartialNative(Arc::clone(&native))),
        serde_json::Value::Null,
        (Instant::now(), now_unix(), None),
    )
    .await;
    assert!(result.is_err());
    let summary: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("result/summary.json")).unwrap()).unwrap();
    assert_eq!(summary["phase_counts"]["unknown_outcomes"], 1);
    assert_eq!(summary["phase_counts"]["counts"]["write_calls"], 7);
    assert_eq!(summary["phase_counts"]["counts"]["checkpoints"], 0);
    assert_eq!(
        summary["phase_counts"]["profile"]["functional_qualification"],
        false
    );
    assert!(summary["phase_counts"]["profile"]["final_verification"].is_null());
    assert!(native.get(11, b"").unwrap().is_none());
    assert!(native.get(16, &2_u32.to_be_bytes()).unwrap().is_none());
    drop(native);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn perpetual_subset_has_exact_counts_and_real_native_contents() {
    let root = temp("perpetual");
    let mut config = config(&root);
    config.workload.node_databases = Some(vec![0]);
    let selected = config.validate().unwrap();
    assert_eq!(
        selected.scope(),
        "archive-derivable node KV subset, node databases: perpetual"
    );
    assert_eq!(selected.batches.len(), 3);
    assert_eq!(
        selected
            .batches
            .iter()
            .map(|b| b.entries.len())
            .sum::<usize>(),
        654
    );
    assert_eq!(
        selected
            .reads
            .iter()
            .flatten()
            .filter(|r| r.producer.is_none())
            .count(),
        507
    );
    assert_eq!(
        selected
            .reads
            .iter()
            .flatten()
            .filter(|r| r.producer.is_some())
            .count(),
        116
    );
    let prepared = prepare(&selected, 2).unwrap();
    let backend =
        Arc::new(NativeReplayBackend::open(&config.backend.options, &selected.tables).unwrap());
    let counts = execute(
        backend.clone(),
        &selected,
        Arc::new(prepared),
        4,
        Instant::now() + Duration::from_secs(30),
        30000,
    )
    .await
    .unwrap();
    assert_eq!(counts.checkpoints, 2);
    assert_eq!(counts.write_calls, 6);
    assert_eq!(counts.put_members, 1308);
    assert_eq!(counts.store_gets, 1014);
    assert_eq!(counts.overlay_demands, 232);
    drop(backend);
    for ids in [vec![], vec![0, 0], vec![1, 0], vec![3]] {
        config.workload.node_databases = Some(ids);
        assert!(config.validate().is_err());
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn all_node_database_subsets_preserve_source_phases() {
    let source = trace();
    for ids in [
        vec![0],
        vec![1],
        vec![2],
        vec![0, 1],
        vec![0, 2],
        vec![1, 2],
        vec![0, 1, 2],
    ] {
        let mut selected = trace();
        selected.select_node_databases(&ids).unwrap();
        let expected = source
            .batches
            .iter()
            .filter(|batch| ids.contains(&(batch.db as u8)))
            .map(|batch| batch.phase)
            .collect::<Vec<_>>();
        assert_eq!(
            selected
                .batches
                .iter()
                .map(|batch| batch.phase)
                .collect::<Vec<_>>(),
            expected
        );
        assert_eq!(
            selected.preload.len(),
            if ids.contains(&0) { 376 } else { 0 }
        );
        assert_eq!(
            selected.reads.iter().flatten().count(),
            if ids.contains(&0) { 623 } else { 0 }
        );
        assert!(
            selected
                .scope()
                .starts_with("archive-derivable node KV subset, node databases: ")
        );
        prepare(&selected, 2).unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_replay_records_committed_payload_and_device_counter_scope() {
    let root = temp("amplification");
    let mut cfg = config(&root);
    cfg.workload.node_databases = Some(vec![0]);
    cfg.execution.concurrency = 4;
    let selected = cfg.validate().unwrap();
    run(cfg, &root.join("result")).await.unwrap();
    let summary: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("result/summary.json")).unwrap()).unwrap();
    let profile = &summary["phase_counts"]["profile"];
    let amplification = &profile["write_amplification"];
    // Perpetual manifest batches (phases 2, 6, 7): 654 rows, 500,324 value and 23,832 key
    // bytes per repetition. Repeated keys keep their source length.
    let values = selected
        .batches
        .iter()
        .flat_map(|b| &b.entries)
        .map(|r| r.value.len() as u64)
        .sum::<u64>();
    let keys = selected
        .batches
        .iter()
        .flat_map(|b| &b.entries)
        .map(|r| r.key.len() as u64)
        .sum::<u64>();
    assert_eq!((values, keys), (500_324, 23_832));
    assert_eq!(amplification["available"], true);
    assert_eq!(
        amplification["payload"],
        serde_json::json!({"commits":6,"rows":1308,"value_bytes":2*values,"key_bytes":2*keys,"key_value_bytes":2*(values+keys)})
    );
    assert_eq!(profile["logical_key_bytes"], 2 * keys);
    // The raw rows reproduce the value-byte denominator independently.
    let rows = std::fs::read_to_string(root.join("result/samples.csv")).unwrap();
    let mut lines = rows.lines();
    let header: Vec<_> = lines.next().unwrap().split(',').collect();
    let column = |name: &str| header.iter().position(|c| *c == name).unwrap();
    let (operation, outcome, bytes, members) = (
        column("operation"),
        column("outcome"),
        column("bytes"),
        column("members"),
    );
    let (mut commits, mut committed_bytes, mut committed_rows) = (0u64, 0u64, 0u64);
    for line in lines {
        let fields: Vec<_> = line.split(',').collect();
        if fields[operation] == "commit" && fields[outcome] == "success" {
            commits += 1;
            committed_bytes += fields[bytes].parse::<u64>().unwrap();
            committed_rows += fields[members].parse::<u64>().unwrap();
        }
    }
    assert_eq!(
        (commits, committed_bytes, committed_rows),
        (6, 2 * values, 1308)
    );
    // The native engine exposes no live leased-write counter; no ratio is fabricated.
    assert_eq!(amplification["logical_wal"]["available"], false);
    assert!(
        amplification["logical_wal"]["error"]
            .as_str()
            .unwrap()
            .contains("completed_write_bytes unavailable"),
        "{amplification}"
    );
    for scope in [
        &amplification["device"],
        &profile["device_counters"],
        &profile["case_identity"]["device_counters"],
    ] {
        assert_eq!(scope["artifact"], "device_counters.json");
        assert_eq!(scope["payload"], "write_amplification.payload");
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn live_write_delta_sums_databases_and_rejects_unusable_counters() {
    let live = |bytes: u64, ops: u64, frames: u64| serde_json::json!({"completed_write_bytes":bytes,"completed_write_ops":ops,"leased_frames":frames});
    let single = |bytes, ops, frames| serde_json::json!({"live_nvmeof": live(bytes, ops, frames)});
    assert_eq!(
        live_write_delta(&single(4096, 1, 1), &single(16384, 4, 3)),
        Ok([12288, 3, 2])
    );
    let databases = |a: serde_json::Value, b: serde_json::Value| serde_json::json!({"databases":{"0":{"live_nvmeof":a},"1":{"live_nvmeof":b}}});
    assert_eq!(
        live_write_delta(
            &databases(live(0, 0, 0), live(8192, 2, 2)),
            &databases(live(4096, 1, 1), live(16384, 4, 3)),
        ),
        Ok([12288, 3, 2])
    );
    assert!(
        live_write_delta(
            &databases(live(0, 0, 0), live(0, 0, 0)),
            &single(4096, 1, 1)
        )
        .is_err()
    );
    assert!(live_write_delta(&single(8192, 2, 2), &single(4096, 2, 2)).is_err());
    assert!(live_write_delta(&single(0, 0, 0), &single(u64::MAX, 2, 2)).is_err());
    assert!(
        live_write_delta(
            &serde_json::json!({"live_nvmeof":null}),
            &single(4096, 1, 1)
        )
        .is_err()
    );
    assert!(live_write_delta(&serde_json::Value::Null, &serde_json::Value::Null).is_err());
}

fn native(root: &Path, trace: &Trace) -> Arc<NativeReplayBackend> {
    Arc::new(NativeReplayBackend::open(&config(root).backend.options, &trace.tables).unwrap())
}

fn assert_contents(backend: &NativeReplayBackend, expected: &FinalRows) {
    for ((table, key), row) in expected {
        assert_eq!(backend.get(*table, key).unwrap().unwrap(), row.value);
    }
}

/// Exact counts of the full three-database fixture, per repetition.
fn assert_full_counts(counts: &Counts, repetitions: u64) {
    assert_eq!(counts.store_gets, 507 * repetitions);
    assert_eq!(counts.overlay_demands, 116 * repetitions);
    assert_eq!(counts.put_members, 707 * repetitions);
    assert_eq!(counts.write_calls, 9 * repetitions);
    assert_eq!(counts.transactions, 45 * repetitions);
    assert_eq!(counts.checkpoints, repetitions);
}

/// One measured call, reduced to what the pipeline ordering rules constrain.
#[derive(Clone, Copy, Debug)]
struct Call {
    repetition: usize,
    phase: Option<u16>,
    tx: Option<u32>,
    dispatched: u64,
    completed: u64,
}

fn calls(m: &Measurements) -> Vec<Call> {
    m.calls
        .iter()
        .map(|(sample, id)| {
            assert_eq!(sample.outcome, "success");
            Call {
                repetition: usize::try_from(id.repetition).unwrap(),
                phase: id.batch_phase,
                tx: id.source_tx,
                dispatched: sample.dispatched_ns,
                completed: sample.completed_ns,
            }
        })
        .collect()
}

/// Observed commits `(repetition, phase)` in dispatch order (W=1: one at a time).
fn commit_order(calls: &[Call]) -> Vec<(usize, u16)> {
    let mut commits = calls
        .iter()
        .filter_map(|c| c.phase.map(|phase| (c.dispatched, c.repetition, phase)))
        .collect::<Vec<_>>();
    commits.sort_unstable();
    commits
        .into_iter()
        .map(|(_, repetition, phase)| (repetition, phase))
        .collect()
}

/// Check the Sui `PipelineStages` rules on the observed call clocks: worker clocks are
/// monotonic and every dependent call is dispatched only after its predecessor's
/// completion reached the scheduler.
fn assert_pipeline_order(calls: &[Call], repetitions: usize, depth: usize, readers: usize) {
    let of = |r: usize| calls.iter().filter(move |c| c.repetition == r);
    let commits = |r: usize, stage: Stage| {
        of(r).filter(move |c| c.phase.is_some_and(|p| batch_stage(p) == stage))
    };
    let reads = |r: usize| of(r).filter(|c| c.phase.is_none());
    let stages = [
        Stage::Sync,
        Stage::Finalize,
        Stage::CommitOutputs,
        Stage::BumpExecuted,
    ];
    for r in 0..repetitions {
        // Execution begins after this checkpoint's sync stage.
        let synced = commits(r, Stage::Sync).map(|c| c.completed).max();
        if let (Some(synced), Some(first)) = (synced, reads(r).map(|c| c.dispatched).min()) {
            assert!(
                first >= synced,
                "checkpoint {r} read before its sync stage ended"
            );
        }
        // Execution ends in checkpoint order, so post-execution commits follow every
        // read of this and all earlier checkpoints.
        let executed = (0..=r)
            .filter_map(|q| reads(q).map(|c| c.completed).max())
            .max()
            .unwrap_or(0);
        for stage in &stages[1..] {
            for c in commits(r, *stage) {
                assert!(
                    c.dispatched >= executed,
                    "checkpoint {r} {stage:?} before execution ended"
                );
            }
        }
        // Phases of one checkpoint commit one after another, in phase order.
        let mut own = of(r).filter(|c| c.phase.is_some()).collect::<Vec<_>>();
        own.sort_by_key(|c| c.dispatched);
        assert!(
            own.windows(2)
                .all(|w| w[0].phase < w[1].phase && w[1].dispatched >= w[0].completed)
        );
        // Stage s of checkpoint r begins after stage s of r-1 ends.
        if r > 0 {
            for stage in stages {
                let previous = commits(r - 1, stage).map(|c| c.completed).max();
                let first = commits(r, stage).map(|c| c.dispatched).min();
                if let (Some(previous), Some(first)) = (previous, first) {
                    assert!(first >= previous, "checkpoint {r} {stage:?} overtook r-1");
                }
            }
        }
        // At most K checkpoints in flight: r starts after r-K retired.
        if r >= depth {
            let retired = of(r - depth).map(|c| c.completed).max().unwrap();
            let first = of(r).map(|c| c.dispatched).min().unwrap();
            assert!(
                first >= retired,
                "checkpoint {r} admitted beyond depth {depth}"
            );
        }
    }
    // W=1: write calls never overlap.
    let mut writes = calls
        .iter()
        .filter(|c| c.phase.is_some())
        .collect::<Vec<_>>();
    writes.sort_by_key(|c| c.dispatched);
    assert!(writes.windows(2).all(|w| w[1].dispatched >= w[0].completed));
    // Read parallelism never exceeds the N read callers.
    let mut edges = calls
        .iter()
        .filter(|c| c.phase.is_none())
        .flat_map(|c| [(c.dispatched, 1i64), (c.completed, -1)])
        .collect::<Vec<_>>();
    edges.sort_unstable();
    let mut in_flight = 0i64;
    for (_, delta) in edges {
        in_flight += delta;
        assert!(
            in_flight <= readers as i64,
            "more reads in flight than callers"
        );
    }
}

#[test]
fn pipeline_depth_is_optional_bounded_and_absent_by_default() {
    let root = temp("depth-config");
    let mut cfg = config(&root);
    let value = serde_json::to_value(&cfg).unwrap();
    assert!(value["workload"].get("checkpoint_pipeline_depth").is_none());
    assert_eq!(cfg.pipeline_depth(), 1);
    for depth in [1, 4, 64] {
        cfg.workload.checkpoint_pipeline_depth = Some(depth);
        cfg.validate().unwrap();
        assert_eq!(cfg.pipeline_depth(), depth as usize);
        let value = serde_json::to_value(&cfg).unwrap();
        assert_eq!(value["workload"]["checkpoint_pipeline_depth"], depth);
        let parsed = serde_json::from_value::<ReplayConfig>(value).unwrap();
        assert_eq!(parsed.workload.checkpoint_pipeline_depth, Some(depth));
    }
    for depth in [0, 65] {
        cfg.workload.checkpoint_pipeline_depth = Some(depth);
        assert!(
            cfg.validate()
                .unwrap_err()
                .to_string()
                .contains("checkpoint_pipeline_depth")
        );
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn depth_one_replays_checkpoints_strictly_in_sequence() {
    let trace = trace();
    let prepared = Arc::new(prepare(&trace, 3).unwrap());
    let root = temp("depth-one");
    let backend = native(&root, &trace);
    let m = replay(
        Arc::clone(&backend),
        &trace,
        Arc::clone(&prepared),
        4,
        1,
        Instant::now() + Duration::from_secs(120),
        30000,
    )
    .await
    .unwrap();
    assert_full_counts(&m.counts, 3);
    assert_eq!(m.max_in_flight, 1);
    assert_eq!((m.cross_overlay_reads, m.cross_store_reads), (0, 0));
    let calls = calls(&m);
    let sequential = (0..3)
        .flat_map(|r| (0..9).map(move |phase| (r, phase)))
        .collect::<Vec<(usize, u16)>>();
    assert_eq!(commit_order(&calls), sequential);
    assert_pipeline_order(&calls, 3, 1, 4);
    // The sequential scheduler never overlaps a commit with a read.
    for c in calls.iter().filter(|c| c.phase.is_some()) {
        for d in calls.iter().filter(|d| d.phase.is_none()) {
            assert!(d.completed <= c.dispatched || c.completed <= d.dispatched);
        }
    }
    let expected = expected_final_rows(&prepared);
    assert_contents(&backend, &expected);
    backend
        .verify_final(&expected, Instant::now() + Duration::from_secs(60))
        .unwrap();
    // Same map as the repetition-then-phase expectation of the sequential scheduler.
    let mut legacy = FinalRows::new();
    for p in prepared.iter() {
        for row in p
            .preload
            .iter()
            .chain(p.batches.iter().flat_map(|b| &b.entries))
        {
            legacy.insert((row.table, row.key.clone()), row.clone());
        }
    }
    assert_eq!(expected.len(), legacy.len());
    assert!(
        expected
            .iter()
            .zip(&legacy)
            .all(|(a, b)| a.0 == b.0 && a.1.value == b.1.value)
    );
    drop(backend);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pipelined_depths_keep_sui_stage_order_counts_and_contents() {
    let trace = trace();
    let repetitions = 4;
    let prepared = Arc::new(prepare(&trace, repetitions as u64).unwrap());
    let expected = expected_final_rows(&prepared);
    for depth in [1, 2, 4] {
        let root = temp(&format!("depth-{depth}"));
        let backend = native(&root, &trace);
        let m = replay(
            Arc::clone(&backend),
            &trace,
            Arc::clone(&prepared),
            4,
            depth,
            Instant::now() + Duration::from_secs(120),
            30000,
        )
        .await
        .unwrap();
        assert_full_counts(&m.counts, repetitions as u64);
        assert_eq!(m.max_in_flight, depth.min(repetitions));
        assert_eq!((m.cross_overlay_reads, m.cross_store_reads), (0, 0));
        let calls = calls(&m);
        let order = commit_order(&calls);
        assert_eq!(order.len(), repetitions * 9);
        for stage in [
            Stage::Sync,
            Stage::Finalize,
            Stage::CommitOutputs,
            Stage::BumpExecuted,
        ] {
            let staged = order
                .iter()
                .filter(|(_, phase)| batch_stage(*phase) == stage)
                .collect::<Vec<_>>();
            assert!(
                staged.windows(2).all(|w| w[0] < w[1]),
                "{stage:?} commits out of order"
            );
        }
        assert_pipeline_order(&calls, repetitions, depth, 4);
        // The sequential oracle holds for every depth.
        assert_contents(&backend, &expected);
        backend
            .verify_final(&expected, Instant::now() + Duration::from_secs(60))
            .unwrap();
        drop(backend);
        std::fs::remove_dir_all(root).unwrap();
    }
}

/// Hold the first committed-outputs batch (phase 7 ends with the table-11 watermark)
/// until the replay reads `release_key`. Storage is the real engine; the hook only
/// orders one commit against one get, so a later checkpoint deterministically reads
/// while the earlier checkpoint's outputs are uncommitted.
struct HeldOutputs {
    inner: Arc<NativeReplayBackend>,
    release_key: Bytes,
    held: std::sync::atomic::AtomicBool,
    gate: (std::sync::Mutex<bool>, std::sync::Condvar),
}

impl ReplayBackend for HeldOutputs {
    fn check_background_health(&self) -> BackendResult<()> {
        self.inner.check_background_health()
    }
    #[allow(clippy::mutex_atomic, reason = "Condvar gate needs the mutex")]
    fn get(&self, table: u16, key: &[u8]) -> BackendResult<Option<Bytes>> {
        if key == &self.release_key[..] {
            *self.gate.0.lock().unwrap() = true;
            self.gate.1.notify_all();
        }
        self.inner.get(table, key)
    }
    fn commit(&self, db: u16, entries: &[Row]) -> BackendResult<()> {
        if entries.last().is_some_and(|row| row.table == 11)
            && !self.held.swap(true, Ordering::Relaxed)
        {
            let (mutex, condvar) = &self.gate;
            let (released, wait) = condvar
                .wait_timeout_while(mutex.lock().unwrap(), Duration::from_secs(30), |released| {
                    !*released
                })
                .unwrap();
            // Release the gate lock before the engine commit.
            drop(released);
            if wait.timed_out() {
                return Err("held outputs were never released".into());
            }
        }
        self.inner.commit(db, entries)
    }
}

/// Perpetual fixture, two repetitions, where repetition 1 transaction `reader` first
/// reads the last ObjectKey that repetition 0 transaction `producer` outputs. Returns
/// (prepared, reader, producer, release key): the release key is a store read of
/// `reader` that no other repetition-1 transaction reads.
fn cross_read_fixture(
    selected: &Trace,
    value: Option<Bytes>,
) -> (Vec<Prepared>, usize, usize, Bytes) {
    let mut prepared = prepare(selected, 2).unwrap();
    let outputs = prepared[0]
        .batches
        .iter()
        .position(|b| b.phase == 7)
        .unwrap();
    let written = prepared[0].batches[outputs]
        .entries
        .iter()
        .rev()
        .find(|row| row.table == OBJECTS)
        .unwrap()
        .clone();
    let producer = written.tx as usize;
    let unique = |key: &Bytes| {
        prepared[1]
            .reads
            .iter()
            .flatten()
            .filter(|read| read.row.key == *key)
            .count()
            == 1
    };
    let (reader, release_key) = (0..selected.transactions)
        .find_map(|tx| {
            prepared[1].reads[tx]
                .iter()
                .find(|read| read.producer.is_none() && unique(&read.row.key))
                .map(|read| (tx, read.row.backend_key.clone()))
        })
        .unwrap();
    let mut row = written;
    if let Some(value) = value {
        row.value = value;
    }
    prepared[1].reads[reader].insert(
        0,
        Read {
            row,
            producer: None,
        },
    );
    prepared[1].overlay[reader].insert(0, None);
    (prepared, reader, producer, release_key)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn later_checkpoint_reads_in_flight_output_through_overlay() {
    let mut selected = trace();
    selected.select_node_databases(&[0]).unwrap();
    let (prepared, reader, producer, release_key) = cross_read_fixture(&selected, None);
    // The producer's publication is observable through its own store reads.
    assert!(
        prepared[0].reads[producer]
            .iter()
            .any(|r| r.producer.is_none())
    );
    let prepared = Arc::new(prepared);
    let deadline = || Instant::now() + Duration::from_secs(60);

    // K=1: repetition 0 committed before repetition 1 starts; the store serves the read.
    let root = temp("cross-depth-one");
    let backend = native(&root, &selected);
    let m = replay(
        Arc::clone(&backend),
        &selected,
        Arc::clone(&prepared),
        2,
        1,
        deadline(),
        30000,
    )
    .await
    .unwrap();
    assert_eq!((m.counts.store_gets, m.counts.overlay_demands), (1015, 232));
    assert_eq!((m.cross_overlay_reads, m.cross_store_reads), (0, 0));
    assert_contents(&backend, &expected_final_rows(&prepared));
    drop(backend);
    std::fs::remove_dir_all(root).unwrap();

    // K=2: the read happens while repetition 0's outputs are held uncommitted.
    let root = temp("cross-depth-two");
    let inner = native(&root, &selected);
    #[allow(clippy::mutex_atomic, reason = "Condvar gate needs the mutex")]
    let held = Arc::new(HeldOutputs {
        inner: Arc::clone(&inner),
        release_key,
        held: std::sync::atomic::AtomicBool::new(false),
        gate: (std::sync::Mutex::new(false), std::sync::Condvar::new()),
    });
    let m = replay(
        held,
        &selected,
        Arc::clone(&prepared),
        2,
        2,
        deadline(),
        30000,
    )
    .await
    .unwrap();
    assert_eq!((m.cross_overlay_reads, m.cross_store_reads), (1, 0));
    assert_eq!((m.counts.store_gets, m.counts.overlay_demands), (1014, 233));
    assert_eq!((m.counts.transactions, m.counts.checkpoints), (90, 2));
    let calls = calls(&m);
    assert_pipeline_order(&calls, 2, 2, 2);
    let first_read = calls
        .iter()
        .filter(|c| c.repetition == 1 && c.tx == Some(reader as u32))
        .map(|c| c.dispatched)
        .min()
        .unwrap();
    let produced = calls
        .iter()
        .filter(|c| c.repetition == 0 && c.tx == Some(producer as u32))
        .map(|c| c.completed)
        .max()
        .unwrap();
    let outputs_committed = calls
        .iter()
        .find(|c| c.repetition == 0 && c.phase == Some(7))
        .unwrap()
        .completed;
    // Gated on the producer's execution, and served before its outputs committed.
    assert!(produced <= first_read && first_read < outputs_committed);
    assert_contents(&inner, &expected_final_rows(&prepared));
    drop(inner);
    std::fs::remove_dir_all(root).unwrap();
}

/// The final-state oracle is independent of the observed commit order: replaying two
/// checkpoints' same-key commits in reverse, or skipping one, fails verification.
#[test]
fn final_oracle_rejects_reversed_or_skipped_same_key_commits() {
    let mut selected = trace();
    selected.select_node_databases(&[0]).unwrap();
    let mut prepared = prepare(&selected, 2).unwrap();
    let outputs = prepared[1]
        .batches
        .iter()
        .position(|b| b.phase == 7)
        .unwrap();
    // The committed watermark (control table 11, empty key) is shared by every repetition;
    // give repetition 1 its own value so the two writes are distinguishable.
    let watermark = prepared[1].batches[outputs]
        .entries
        .iter_mut()
        .find(|row| row.table == 11)
        .unwrap();
    assert!(watermark.key.is_empty());
    watermark.value = Bytes::from_static(b"repetition-1");
    let expected = expected_final_rows(&prepared);
    let apply = |order: &[(usize, usize)]| {
        let root = temp("oracle");
        let backend = native(&root, &selected);
        for chunk in prepared.iter().flat_map(|p| p.preload.chunks(256)) {
            backend.commit(0, chunk).unwrap();
        }
        for &(repetition, batch) in order {
            let batch = &prepared[repetition].batches[batch];
            backend.commit(batch.db, &batch.entries).unwrap();
        }
        let verdict = backend.verify_final(&expected, Instant::now() + Duration::from_secs(60));
        drop(backend);
        std::fs::remove_dir_all(root).unwrap();
        verdict
    };
    let batches = prepared[0].batches.len();
    let sequential = (0..2)
        .flat_map(|r| (0..batches).map(move |b| (r, b)))
        .collect::<Vec<_>>();
    apply(&sequential).unwrap();
    let reversed = (0..2)
        .rev()
        .flat_map(|r| (0..batches).map(move |b| (r, b)))
        .collect::<Vec<_>>();
    assert!(
        apply(&reversed)
            .unwrap_err()
            .to_string()
            .contains("mismatch in table 11")
    );
    let skipped = sequential
        .iter()
        .copied()
        .filter(|&step| step != (1, outputs))
        .collect::<Vec<_>>();
    assert!(apply(&skipped).is_err());
}

#[test]
fn cross_checkpoint_links_gate_reject_conflicts_and_wrong_overlay_values() {
    let mut selected = trace();
    selected.select_node_databases(&[0]).unwrap();
    // A key one checkpoint commits as an output and a later one writes in its sync stage
    // could commit out of order when both are in flight; K=1 and distance >= K are exact.
    let mut conflicting = prepare(&selected, 3).unwrap();
    let outputs = conflicting[0]
        .batches
        .iter()
        .position(|b| b.phase == 7)
        .unwrap();
    let sync = conflicting[0]
        .batches
        .iter()
        .position(|b| b.phase == 2)
        .unwrap();
    let written = conflicting[0].batches[outputs].entries[0].clone();
    conflicting[2].batches[sync].entries[0] = written;
    assert!(
        CrossLinks::new(&selected, &conflicting, 1)
            .unwrap()
            .reads
            .is_empty()
    );
    CrossLinks::new(&selected, &conflicting, 2).unwrap();
    assert!(
        CrossLinks::new(&selected, &conflicting, 3)
            .unwrap_err()
            .to_string()
            .contains("write-write conflict")
    );

    let (wrong, reader, producer, _) = cross_read_fixture(
        &selected,
        Some(Bytes::from_static(b"not the produced object")),
    );
    let links = CrossLinks::new(&selected, &wrong, 2).unwrap();
    assert!(
        CrossLinks::new(&selected, &wrong, 1)
            .unwrap()
            .reads
            .is_empty()
    );
    assert_eq!(links.reads.len(), 1);
    let gate = Gate::Publish {
        repetition: 0,
        tx: producer,
    };
    assert_eq!(links.gates[&(1, reader)], vec![gate]);
    assert_eq!(links.waiters[&gate], vec![(1, reader)]);
    let mut scheduler = Scheduler::new(
        &selected,
        &wrong,
        &links,
        2,
        2,
        Instant::now() + Duration::from_secs(30),
    )
    .unwrap();
    scheduler.admitted = 2;
    scheduler.slots[0].repetition = 0;
    scheduler.slots[1].repetition = 1;
    assert!(!scheduler.gate_open(gate));
    scheduler.slots[0].execution = Execution::Running(Instant::now());
    scheduler.slots[0].completed[producer] = true;
    assert!(scheduler.gate_open(gate));
    let mut m = Measurements::new(&selected, 2).unwrap();
    assert!(
        scheduler
            .next_store_read(1, reader, 0, &mut m)
            .unwrap_err()
            .to_string()
            .contains("cross-checkpoint overlay missing/wrong value")
    );
    // Once every window writer committed, the store serves the read and its bytes decide.
    scheduler.slots[0].next_batch = outputs + 1;
    assert_eq!(
        scheduler.next_store_read(1, reader, 0, &mut m).unwrap(),
        Some(0)
    );
    assert_eq!((m.cross_overlay_reads, m.cross_store_reads), (0, 1));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pipelined_report_records_depth_and_verifies_final_contents() {
    let root = temp("report-depth");
    let mut cfg = config(&root);
    cfg.execution.concurrency = 4;
    cfg.workload.checkpoint_pipeline_depth = Some(2);
    run(cfg, &root.join("result")).await.unwrap();
    let summary: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("result/summary.json")).unwrap()).unwrap();
    assert_eq!(summary["status"], "completed");
    let phase = &summary["phase_counts"];
    assert_eq!(phase["counts"]["store_gets"], 1014);
    assert_eq!(phase["counts"]["checkpoints"], 2);
    let profile = &phase["profile"];
    assert_eq!(profile["functional_qualification"], true);
    assert!(!profile["final_verification"].is_null());
    assert_eq!(profile["case_identity"]["N"], 4);
    assert_eq!(profile["case_identity"]["K"], 2);
    assert_eq!(profile["case_identity"]["W"], 1);
    assert_eq!(profile["pipeline"]["checkpoint_pipeline_depth"], 2);
    assert_eq!(profile["pipeline"]["max_in_flight_checkpoints"], 2);
    assert_eq!(profile["pipeline"]["read_callers"], 4);
    std::fs::remove_dir_all(root).unwrap();
}
