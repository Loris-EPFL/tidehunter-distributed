use super::*;

#[tokio::test]
async fn sampled_native_scalar_write_reports_real_append_and_index_stages() {
    let root = std::env::temp_dir().join(format!(
        "kv-research-native-write-profile-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let backend = open(
        &serde_json::json!({"path": root.clone(), "durability": "page_cache"}),
        &prometheus::Registry::new(),
    )
    .await
    .unwrap();
    backend.arm_write_profile(1).unwrap();
    const KEY: [u8; 32] = [7; 32];
    backend
        .profiled_put(Bytes::from_static(&KEY), Bytes::from_static(b"value"))
        .await
        .unwrap();
    assert_eq!(backend.get(&KEY).await.unwrap().unwrap().as_ref(), b"value");
    let evidence = backend.finish_write_profile().await.unwrap();
    assert_eq!(evidence["selected_operations"], 1);
    assert!(evidence["stages"]["wal_append_ns"].as_u64().unwrap() > 0);
    assert!(evidence["stages"]["index_apply_ns"].as_u64().unwrap() > 0);
    assert_eq!(
        evidence["value_wal_persistence"]["status"],
        "not_applicable"
    );
    backend.shutdown().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn native_barrier_profile_reports_inventory_and_layout() {
    let root = std::env::temp_dir().join(format!(
        "kv-research-native-barrier-inventory-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let backend = open(
        &serde_json::json!({"path": root.clone(), "durability": "value_wal_barrier"}),
        &prometheus::Registry::new(),
    )
    .await
    .unwrap();
    backend.arm_write_profile(1).unwrap();
    const KEY: [u8; 32] = [8; 32];
    backend
        .profiled_put(Bytes::from_static(&KEY), Bytes::from_static(b"value"))
        .await
        .unwrap();
    let evidence = backend.finish_write_profile().await.unwrap();
    assert_eq!(evidence["value_wal_inventory"]["start"]["available"], true);
    assert_eq!(evidence["value_wal_inventory"]["end"]["available"], true);
    let persistence = &evidence["value_wal_persistence"];
    assert_eq!(persistence["profiled_barriers"], 1);
    assert_eq!(persistence["barrier_errors"], 0);
    assert_eq!(
        persistence["sampled_registered_files_min"],
        persistence["sampled_registered_files_max"]
    );
    assert_eq!(
        persistence["sampled_registered_files_min"],
        persistence["file_calls"]
    );
    backend.shutdown().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn owner_matched_native_geometry_keeps_barrier_and_reads() {
    let root = std::env::temp_dir().join(format!(
        "kv-research-native-owner-geometry-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let backend = open(
        &serde_json::json!({
            "path": root.clone(),
            "durability": "value_wal_barrier",
            "native_wal_geometry": "owner_matched"
        }),
        &prometheus::Registry::new(),
    )
    .await
    .unwrap();
    let info = backend.info();
    assert_eq!(info.configuration["native_wal_geometry"], "owner_matched");
    assert_eq!(info.configuration["value_wal_fragment_bytes"], 4_194_304);
    assert_eq!(
        info.configuration["value_wal_file_bytes"],
        1_073_741_824_u64
    );
    assert_eq!(info.configuration["durability"], "value_wal_barrier");
    backend.arm_write_profile(1).unwrap();
    const KEY: [u8; 32] = [9; 32];
    backend
        .profiled_put(Bytes::from_static(&KEY), Bytes::from(vec![3; 65_536]))
        .await
        .unwrap();
    assert_eq!(
        backend.get(&KEY).await.unwrap().unwrap().as_ref(),
        &[3; 65_536]
    );
    let evidence = backend.finish_write_profile().await.unwrap();
    assert_eq!(evidence["value_wal_persistence"]["profiled_barriers"], 1);
    assert_eq!(evidence["value_wal_persistence"]["barrier_errors"], 0);
    backend.shutdown().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn reopening_native_with_mismatched_geometry_is_rejected() {
    let root = std::env::temp_dir().join(format!(
        "kv-research-native-geometry-mismatch-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let registry = prometheus::Registry::new();
    let options = serde_json::json!({
        "path": root.clone(),
        "durability": "value_wal_barrier",
        "native_wal_geometry": "small"
    });
    let backend = open(&options, &registry).await.unwrap();
    const KEY: [u8; 32] = [10; 32];
    backend
        .put(Bytes::from_static(&KEY), Bytes::from_static(b"preserved"))
        .await
        .unwrap();
    backend.shutdown().await.unwrap();
    for geometry in ["large_files", "owner_matched", "native_default"] {
        let mismatch = serde_json::json!({
            "path": root.clone(), "native_wal_geometry": geometry
        });
        let error = open(&mismatch, &prometheus::Registry::new())
            .await
            .err()
            .unwrap();
        assert!(error.to_string().contains("WAL geometry mismatch"));
    }
    let reopened = open(&options, &prometheus::Registry::new()).await.unwrap();
    assert_eq!(
        reopened.get(&KEY).await.unwrap().unwrap().as_ref(),
        b"preserved"
    );
    reopened.shutdown().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn large_files_native_geometry_keeps_barrier_and_reads() {
    let root = std::env::temp_dir().join(format!(
        "kv-research-native-large-files-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let backend = open(
        &serde_json::json!({
            "path": root.clone(),
            "durability": "value_wal_barrier",
            "native_wal_geometry": "large_files"
        }),
        &prometheus::Registry::new(),
    )
    .await
    .unwrap();
    backend.arm_write_profile(1).unwrap();
    const KEY: [u8; 32] = [11; 32];
    backend
        .profiled_put(Bytes::from_static(&KEY), Bytes::from(vec![4; 65_536]))
        .await
        .unwrap();
    assert_eq!(
        backend.get(&KEY).await.unwrap().unwrap().as_ref(),
        &[4; 65_536]
    );
    let evidence = backend.finish_write_profile().await.unwrap();
    assert_eq!(evidence["value_wal_persistence"]["profiled_barriers"], 1);
    assert_eq!(evidence["value_wal_persistence"]["barrier_errors"], 0);
    backend.shutdown().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn native_benchmark_rejects_nonempty_path_without_geometry_witness() {
    let root = std::env::temp_dir().join(format!(
        "kv-research-native-unwitnessed-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("wal_0000000000000000"), b"existing").unwrap();
    let error = open(
        &serde_json::json!({"path": root.clone(), "native_wal_geometry": "large_files"}),
        &prometheus::Registry::new(),
    )
    .await
    .err()
    .unwrap();
    assert!(error.to_string().contains("no WAL geometry witness"));
    assert_eq!(
        std::fs::read(root.join("wal_0000000000000000")).unwrap(),
        b"existing"
    );
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn grouped_barrier_covers_concurrent_mutations_and_exports_metrics() {
    let root = std::env::temp_dir().join(format!(
        "kv-research-native-group-barrier-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let options = serde_json::json!({
        "path": root,
        "durability": "value_wal_group_barrier",
        "native_wal_geometry": "small",
    });
    validate_options(&options).unwrap();
    let registry = prometheus::Registry::new();
    let backend = open(&options, &registry).await.unwrap();
    let info = backend.info();
    assert_eq!(info.configuration["durability"], "value_wal_group_barrier");
    assert_eq!(
        info.acknowledgement,
        "native write acknowledgement followed by a shared Value-WAL file persistence barrier that started after this operation returned"
    );
    assert_eq!(
        info.measurement_boundary,
        "foreground native Db operation on a dedicated benchmark worker, including a Value-WAL persistence barrier shared with concurrent callers"
    );
    let start = std::sync::Barrier::new(8);
    std::thread::scope(|scope| {
        for worker in 0..8_u32 {
            let backend = &backend;
            let start = &start;
            scope.spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .build()
                    .unwrap();
                start.wait();
                runtime.block_on(async {
                    for offset in 0..64 {
                        let mut key = [0; 32];
                        key[..4].copy_from_slice(&(worker * 64 + offset).to_le_bytes());
                        backend
                            .put(Bytes::copy_from_slice(&key), Bytes::from_static(b"value"))
                            .await
                            .unwrap();
                    }
                });
            });
        }
    });
    for id in 0..512_u32 {
        let mut key = [0; 32];
        key[..4].copy_from_slice(&id.to_le_bytes());
        assert_eq!(backend.get(&key).await.unwrap().unwrap().as_ref(), b"value");
    }
    // Whole-case counters retain the unprofiled writes when profiling is armed.
    backend.arm_write_profile(2).unwrap();
    let key = Bytes::from_static(&[254; 32]);
    backend
        .profiled_put(key.clone(), Bytes::from_static(b"profiled"))
        .await
        .unwrap();
    backend.profiled_delete(key.clone()).await.unwrap();
    assert!(backend.get(&key).await.unwrap().is_none());
    backend
        .put_batch(&[(key.clone(), Bytes::from_static(b"batch"))])
        .await
        .unwrap();
    assert_eq!(backend.get(&key).await.unwrap().unwrap().as_ref(), b"batch");
    backend.delete(key.clone()).await.unwrap();
    assert!(backend.get(&key).await.unwrap().is_none());
    backend.put_batch(&[]).await.unwrap();
    let profile = backend.finish_write_profile().await.unwrap();
    assert_eq!(profile["selected_operations"], 2);
    assert!(profile["stages"]["wal_append_ns"].as_u64().unwrap() > 0);
    assert!(profile["stages"]["index_apply_ns"].as_u64().unwrap() > 0);
    let persistence = &profile["value_wal_persistence"];
    assert_eq!(persistence["status"], "grouped");
    assert_eq!(
        persistence["reason"],
        "concurrent callers share barrier runs; per-operation file and namespace calls are not attributable"
    );
    assert!(persistence.get("file_calls").is_none());
    let snapshot = backend.shards[0].group_barrier.snapshot();
    assert_eq!(snapshot.callers_covered, 516);
    assert!(snapshot.runs > 0 && snapshot.runs <= snapshot.callers_covered);
    assert!(snapshot.max_group_size > 0 && snapshot.max_group_size <= 8);
    assert_eq!(snapshot.errors, 0);
    assert_eq!(
        persistence["group_barrier"],
        serde_json::to_value(&snapshot).unwrap()
    );
    let metrics = registry.gather();
    let metric = |name: &str| {
        metrics
            .iter()
            .find(|family| family.name() == name)
            .unwrap()
            .get_metric()[0]
            .clone()
    };
    assert_eq!(
        metric("kv_research_native_group_barrier_runs_total")
            .get_counter()
            .value(),
        snapshot.runs as f64
    );
    assert_eq!(
        metric("kv_research_native_group_barrier_callers_total")
            .get_counter()
            .value(),
        516.0
    );
    assert_eq!(
        metric("kv_research_native_group_barrier_errors_total")
            .get_counter()
            .value(),
        0.0
    );
    let histogram = metric("kv_research_native_group_barrier_group_size")
        .get_histogram()
        .clone();
    assert_eq!(histogram.sample_count(), snapshot.runs);
    assert_eq!(histogram.sample_sum(), 516.0);
    assert_eq!(
        histogram
            .get_bucket()
            .iter()
            .map(|bucket| bucket.upper_bound())
            .collect::<Vec<_>>(),
        vec![1., 2., 3., 4., 6., 8., 12., 16., 24., 32., 64.]
    );
    backend.shutdown().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn group_barrier_metric_registration_error_propagates_from_open() {
    let registry = prometheus::Registry::new();
    let _barrier = GroupBarrier::new(&registry).unwrap();
    let error = open(
        &serde_json::json!({
            "path": std::env::temp_dir().join("kv-research-native-unused-path"),
            "durability": "value_wal_group_barrier"
        }),
        &registry,
    )
    .await
    .err()
    .unwrap();
    assert!(error.to_string().contains("Duplicate"));
}
