use super::*;
use crate::workload::key_bytes;
use prometheus::Registry;

/// Tree and database path of one test case. `Drop` removes the tree even when
/// an assertion panics.
fn temporary_path(label: &str) -> (tempfile::TempDir, PathBuf) {
    let guard = tempfile::TempDir::with_prefix(format!("kv-research-native-shards-{label}-"))
        .expect("create the shard test tree");
    let database = guard.path().join("database");
    (guard, database)
}

#[test]
fn shard_options_default_normalize_and_validate_bounds() {
    let mut options = serde_json::json!({"path": "unused"});
    assert_eq!(parse_options(&options).unwrap().native_shards, 1);
    normalize_options(&mut options).unwrap();
    assert_eq!(options["native_shards"], 1);
    assert_eq!(options["native_wal_geometry"], "native_default");
    for shards in [1, 5, 16] {
        options["native_shards"] = shards.into();
        validate_options(&options).unwrap();
        normalize_options(&mut options).unwrap();
        assert_eq!(options["native_shards"], shards);
    }
    for shards in [
        serde_json::json!(0),
        serde_json::json!(17),
        serde_json::json!(-1),
        serde_json::json!(1.5),
        serde_json::json!("5"),
        serde_json::Value::Null,
    ] {
        options["native_shards"] = shards;
        assert!(validate_options(&options).is_err());
    }
    options["native_shards"] = 5.into();
    options["unknown_option"] = true.into();
    assert!(validate_options(&options).is_err());
}

#[test]
fn harness_keys_have_pinned_deterministic_five_shard_population() {
    let mut counts = [0; 5];
    for id in 0..4096 {
        let key = key_bytes(&[1; 16], 7, id);
        let shard = shard_index(&key, 5);
        assert_eq!(shard, shard_index(&key_bytes(&[1; 16], 7, id), 5));
        assert_eq!(shard_index(&key, 1), 0);
        counts[shard] += 1;
    }
    assert_eq!(counts, [819, 821, 816, 848, 792]);
    assert!(counts.iter().all(|count| (700..=940).contains(count)));
}

#[tokio::test]
async fn single_shard_keeps_database_path_info_and_unlabelled_metrics() {
    let (_guard, root) = temporary_path("single");
    let mut options = serde_json::json!({
        "path": root,
        "native_wal_geometry": "small",
        "durability": "value_wal_group_barrier"
    });
    let registry = Registry::new();
    let backend = open(&options, &registry).await.unwrap();
    let info = backend.info();
    assert_eq!(backend.shards.len(), 1);
    assert_eq!(backend.shards[0].path, root);
    assert!(!root.join("shard-00").exists());
    assert_eq!(info.configuration["native_shards"], 1);
    assert_eq!(info.topology["path"], serde_json::json!(root));
    assert_eq!(info.topology["shard_paths"], serde_json::json!([root]));
    assert_eq!(info.topology["native_shards"], 1);
    assert_eq!(info.topology["owners"], 1);
    assert_eq!(info.topology["processes"], 1);
    assert_eq!(info.topology["routing"], "fnv1a64(key) mod N");
    assert!(info.topology.get("same_ssd_and_filesystem").is_none());
    assert!(info.topology.get("same_filesystem").is_none());
    assert!(info.topology.get("shard_filesystem_device_ids").is_none());
    assert_eq!(
        info.topology["backing_block_device"],
        "not verified by the adapter"
    );
    assert_eq!(info.batch_atomicity, BatchAtomicity::CrashRecovery);
    assert_eq!(
        info.acknowledgement,
        "native write acknowledgement followed by a shared Value-WAL file persistence barrier that started after this operation returned"
    );
    assert_eq!(
        info.measurement_boundary,
        "foreground native Db operation on a dedicated benchmark worker, including a Value-WAL persistence barrier shared with concurrent callers"
    );
    let key = key_bytes(&[1; 16], 7, 0);
    backend
        .put(key.clone(), Bytes::from_static(b"single"))
        .await
        .unwrap();
    let metrics = registry.gather();
    let grouped: Vec<_> = metrics
        .iter()
        .filter(|metric| {
            metric
                .name()
                .starts_with("kv_research_native_group_barrier_")
        })
        .collect();
    assert_eq!(grouped.len(), 4);
    assert!(
        metrics.len() > grouped.len(),
        "engine metrics stay exported"
    );
    assert!(grouped.iter().all(|family| {
        family
            .get_metric()
            .iter()
            .all(|metric| metric.get_label().is_empty())
    }));
    backend.shutdown().await.unwrap();
    options["native_shards"] = 1.into();
    let reopened = open(&options, &Registry::new()).await.unwrap();
    assert_eq!(
        serde_json::to_value(reopened.info()).unwrap(),
        serde_json::to_value(info).unwrap()
    );
    assert_eq!(
        reopened.get(&key).await.unwrap().unwrap().as_ref(),
        b"single"
    );
    reopened.shutdown().await.unwrap();
}

#[test]
fn filesystem_comparison_requires_nonempty_equal_device_ids() {
    assert!(same_filesystem(&[7, 7]));
    assert!(!same_filesystem(&[7, 8]));
    assert!(!same_filesystem(&[]));
}

#[tokio::test]
async fn two_shard_directories_report_observed_filesystem_devices() {
    let (_guard, root) = temporary_path("filesystem");
    let backend = open(
        &serde_json::json!({
            "path": root,
            "native_shards": 2,
            "native_wal_geometry": "small"
        }),
        &Registry::new(),
    )
    .await
    .unwrap();
    let info = backend.info();
    let observed: Vec<_> = backend
        .shards
        .iter()
        .map(|shard| std::fs::metadata(&shard.path).unwrap().dev())
        .collect();
    assert_eq!(observed.len(), 2);
    assert_eq!(observed[0], observed[1]);
    assert_eq!(
        info.topology["shard_filesystem_device_ids"],
        serde_json::json!(observed)
    );
    assert_eq!(info.topology["same_filesystem"], true);
    assert_eq!(
        info.topology["backing_block_device"],
        "not verified by the adapter"
    );
    assert!(info.topology.get("same_ssd_and_filesystem").is_none());
    backend.shutdown().await.unwrap();
}

async fn concurrent_five_shards(durability: &str) {
    let (_guard, root) = temporary_path(durability);
    let registry = Registry::new();
    let options = serde_json::json!({
        "path": root,
        "key_count": 4096,
        "native_wal_geometry": "small",
        "native_shards": 5,
        "durability": durability,
    });
    let backend = open(&options, &registry).await.unwrap();
    let info = backend.info();
    assert_eq!(info.configuration["native_shards"], 5);
    assert_eq!(info.configuration["key_count"], 4096);
    assert_eq!(info.topology["native_shards"], 5);
    assert_eq!(info.topology["owners"], 5);
    assert_eq!(info.topology["processes"], 1);
    assert_eq!(info.topology["same_filesystem"], true);
    assert_eq!(
        info.topology["shard_filesystem_device_ids"]
            .as_array()
            .unwrap()
            .len(),
        5
    );
    assert_eq!(
        info.topology["backing_block_device"],
        "not verified by the adapter"
    );
    assert_eq!(info.topology["routing"], "fnv1a64(key) mod N");
    for boundary in [&info.acknowledgement, &info.measurement_boundary] {
        assert!(
            boundary.contains(
                "5 independent Db instances in one process; callers are routed by key hash"
            )
        );
    }
    assert_eq!(info.batch_atomicity, BatchAtomicity::Unsupported);
    assert_eq!(backend.shards.len(), 5);
    for (index, shard) in backend.shards.iter().enumerate() {
        assert_eq!(shard.path, root.join(format!("shard-{index:02}")));
        assert_eq!(
            info.topology["shard_paths"][index],
            serde_json::json!(shard.path)
        );
    }
    let start = std::sync::Barrier::new(8);
    std::thread::scope(|scope| {
        for worker in 0..8_u64 {
            let backend = &backend;
            let start = &start;
            scope.spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .build()
                    .unwrap();
                start.wait();
                runtime.block_on(async {
                    for offset in 0..64 {
                        backend
                            .put(
                                key_bytes(&[1; 16], 7, worker * 64 + offset),
                                Bytes::from_static(b"value"),
                            )
                            .await
                            .unwrap();
                    }
                });
            });
        }
    });
    let mut counts = [0_u64; 5];
    let mut keys = Vec::with_capacity(512);
    for id in 0..512 {
        let key = key_bytes(&[1; 16], 7, id);
        let index = shard_index(&key, 5);
        counts[index] += 1;
        assert_eq!(backend.get(&key).await.unwrap().unwrap().as_ref(), b"value");
        // A mutation reaches only the selected Db, not each independent WAL.
        for (candidate, shard) in backend.shards.iter().enumerate() {
            assert_eq!(
                shard.db.get(shard.keyspace, &key).unwrap().is_some(),
                candidate == index
            );
        }
        keys.push(key);
    }
    assert!(counts.iter().all(|count| *count > 0));
    let ordered = [
        keys[511].clone(),
        keys[0].clone(),
        keys[511].clone(),
        key_bytes(&[1; 16], 7, 512),
    ];
    let values = backend.get_many(&ordered).await.unwrap();
    assert_eq!(values.len(), 4);
    for value in &values[..3] {
        assert_eq!(value.as_ref().unwrap().as_ref(), b"value");
    }
    assert!(values[3].is_none());
    for (index, count) in counts.iter_mut().enumerate() {
        let key = keys
            .iter()
            .find(|key| shard_index(key, 5) == index)
            .unwrap();
        backend.delete(key.clone()).await.unwrap();
        assert!(backend.get(key).await.unwrap().is_none());
        *count += 1;
    }
    let absent = key_bytes(&[1; 16], 7, 512);
    for entries in [
        Vec::new(),
        vec![(absent.clone(), Bytes::from_static(b"batch"))],
    ] {
        let error = backend.put_batch(&entries).await.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("cross-shard atomicity is not provided")
        );
        assert_eq!(
            error.downcast_ref::<std::io::Error>().unwrap().kind(),
            std::io::ErrorKind::Unsupported
        );
    }
    let errors = [
        backend.arm_write_profile(0).unwrap_err(),
        backend.arm_write_profile(1).unwrap_err(),
        backend
            .profiled_put(absent.clone(), Bytes::from_static(b"profiled"))
            .await
            .unwrap_err(),
        backend
            .profiled_delete(keys[511].clone())
            .await
            .unwrap_err(),
        backend.finish_write_profile().await.unwrap_err(),
    ];
    for error in errors {
        assert!(
            error
                .to_string()
                .contains("native write profiling requires native_shards=1")
        );
        assert_eq!(
            error.downcast_ref::<std::io::Error>().unwrap().kind(),
            std::io::ErrorKind::Unsupported
        );
    }
    assert!(backend.get(&absent).await.unwrap().is_none());
    let metrics = registry.gather();
    assert_eq!(
        metrics.len(),
        4,
        "private engine metrics must not be exported"
    );
    let grouped = durability == "value_wal_group_barrier";
    let mut total_runs = 0;
    let mut total_callers = 0;
    for family in &metrics {
        assert_eq!(family.get_metric().len(), 5);
        let mut seen = [false; 5];
        for metric in family.get_metric() {
            let labels = metric.get_label();
            assert_eq!(labels.len(), 1);
            assert_eq!(labels[0].name(), "shard");
            let index: usize = labels[0].value().parse().unwrap();
            assert!(!seen[index]);
            seen[index] = true;
            let snapshot = backend.shards[index].group_barrier.snapshot();
            if grouped {
                assert_eq!(snapshot.callers_covered, counts[index]);
                assert!(snapshot.runs > 0 && snapshot.runs <= counts[index]);
                assert!(snapshot.max_group_size > 0 && snapshot.max_group_size <= 8);
            } else {
                assert_eq!(snapshot.runs, 0);
                assert_eq!(snapshot.callers_covered, 0);
                assert_eq!(snapshot.max_group_size, 0);
            }
            assert_eq!(snapshot.errors, 0);
            match family.name() {
                "kv_research_native_group_barrier_runs_total" => {
                    assert_eq!(metric.get_counter().value(), snapshot.runs as f64);
                    total_runs += snapshot.runs;
                }
                "kv_research_native_group_barrier_callers_total" => {
                    assert_eq!(
                        metric.get_counter().value(),
                        snapshot.callers_covered as f64
                    );
                    total_callers += snapshot.callers_covered;
                }
                "kv_research_native_group_barrier_errors_total" => {
                    assert_eq!(metric.get_counter().value(), 0.0)
                }
                "kv_research_native_group_barrier_group_size" => {
                    assert_eq!(metric.get_histogram().sample_count(), snapshot.runs);
                    assert_eq!(
                        metric.get_histogram().sample_sum(),
                        snapshot.callers_covered as f64
                    );
                }
                name => panic!("unexpected exported metric {name}"),
            }
        }
        assert!(seen.iter().all(|seen| *seen));
    }
    if grouped {
        assert_eq!(total_callers, 517);
        assert!(total_runs > 0 && total_runs <= total_callers);
    } else {
        assert_eq!(total_callers, 0);
        assert_eq!(total_runs, 0);
    }
    backend.shutdown().await.unwrap();
}

#[tokio::test]
async fn five_shards_page_cache_routes_concurrent_calls() {
    concurrent_five_shards("page_cache").await;
}

#[tokio::test]
async fn five_shards_value_wal_barrier_routes_concurrent_calls() {
    concurrent_five_shards("value_wal_barrier").await;
}

#[tokio::test]
async fn five_shards_group_barrier_routes_concurrent_calls() {
    concurrent_five_shards("value_wal_group_barrier").await;
}

#[tokio::test]
async fn shard_metric_registration_errors_propagate() {
    let (_guard, root) = temporary_path("duplicate-metrics");
    let registry = Registry::new();
    let _barrier = GroupBarrier::new_for_shard(&registry, 0).unwrap();
    let error = open(
        &serde_json::json!({"path": root, "native_shards": 5}),
        &registry,
    )
    .await
    .err()
    .unwrap();
    assert!(error.to_string().contains("Duplicate"));
    assert!(!root.exists());
}
