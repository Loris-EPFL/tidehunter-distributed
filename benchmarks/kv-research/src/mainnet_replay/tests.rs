use super::*;
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
        },
        execution: ExecutionConfig {
            mode: ExecutionMode::ClosedLoop,
            concurrency: 1,
            client_shards: 1,
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
        (started, now_unix()),
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
            (Instant::now(), now_unix()),
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
