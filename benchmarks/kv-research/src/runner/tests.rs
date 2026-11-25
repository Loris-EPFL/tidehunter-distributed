/// Auxiliary exact-oracle primitive cost, not a storage workload. The 90/10
/// read/put sequence exercises the same shared atomics as the native mixed run.
#[test]
#[ignore = "explicit local release-mode accounting measurement"]
fn client_oracle_atomic_cost() {
    for clients in [1, 2, 4] {
        for keys in [1, 4096] {
            let oracle = KeyOracle::new(true, keys, true).unwrap();
            let barrier = std::sync::Barrier::new(clients + 1);
            let elapsed = std::thread::scope(|scope| {
                let mut threads = Vec::with_capacity(clients);
                for shard in 0..clients {
                    let oracle = &oracle;
                    let barrier = &barrier;
                    threads.push(scope.spawn(move || {
                        barrier.wait();
                        for index in 0..1_000_000 / clients {
                            let key = (index * clients + shard) as u64 % keys;
                            if index % 10 == 0 {
                                let ticket = oracle.begin_put(key);
                                oracle.end_put(key, ticket, true);
                            } else {
                                let before = oracle.snapshot(key);
                                std::hint::black_box(oracle.absence_allowed(key, before));
                            }
                        }
                    }));
                }
                let started = Instant::now();
                barrier.wait();
                for thread in threads {
                    thread.join().unwrap();
                }
                started.elapsed()
            });
            println!(
                "client_oracle_atomic_cost clients={clients} keys={keys} aggregate_ns_per_call={:.3}",
                elapsed.as_nanos() as f64 / 1_000_000.0
            );
        }
    }
}

#[tokio::test]
async fn client_shards_one_raw_operation_sequence_matches_original_generator() {
    let guard = result_directory("client-one-sequence");
    let directory = guard.path().to_path_buf();
    let output = directory.join("result");
    let mut config = read_test_config();
    config.execution.concurrency = 1;
    config.execution.queue_capacity = 1;
    config.dataset.keys = 31;
    config.workload.mix = crate::config::OperationMix {
        read: 7,
        put: 3,
        delete: 2,
        missing_read: 1,
        get_many: 2,
        put_batch: 1,
    };
    config.phases.warmup_operations = 11;
    config.phases.aging_operations = 7;
    config.phases.measurement_operations = 127;
    let mut original = WorkloadGenerator::new(
        config.seed ^ 0x6b76_7265_7365_6172,
        config.dataset.keys,
        config.workload.mix.clone(),
        config.workload.batch_size,
        config.workload.distribution.clone(),
        config.workload.put_key_policy,
        Arc::new(AtomicU64::new(0)),
    );
    for _ in 0..18 {
        original.next_item().unwrap();
    }
    let expected = (0..127)
        .map(|_| {
            let work = original.next_item().unwrap();
            (work.operation.as_str(), work.keys.len())
        })
        .collect::<Vec<_>>();
    run(
        MemoryBackend::new(false, false),
        config,
        &output,
        prometheus::Registry::new(),
    )
    .await
    .unwrap();
    let samples = std::fs::read_to_string(output.join("samples.csv")).unwrap();
    let actual = samples
        .lines()
        .filter(|row| row.starts_with("measurement,"))
        .map(|row| {
            let fields = row.split(',').collect::<Vec<_>>();
            assert_eq!(fields[2], "success");
            (fields[1], fields[10].parse::<usize>().unwrap())
        })
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_shards_bound_overload_and_cancel_timed_out_calls() {
    for overload in [false, true] {
        let guard = result_directory("client-shards-bounds");
        let directory = guard.path().to_path_buf();
        let mut config = read_test_config();
        config.execution.concurrency = 7;
        config.execution.queue_capacity = 11;
        config.execution.client_shards = Some(3);
        config.execution.request_timeout_ms = if overload { 1000 } else { 1 };
        if overload {
            config.execution.mode = ExecutionMode::OpenLoop;
            config.execution.offered_ops_per_second = Some(1_000_000_000.0);
        }
        let active = Arc::new(AtomicUsize::new(0));
        let backend = Arc::new(InterferenceBackend {
            inner: Arc::new(MemoryBackend::new(false, false)),
            read_delay: Duration::from_millis(20),
            put_delay: Duration::ZERO,
            active: Arc::clone(&active),
        });
        let oracle = Arc::new(KeyOracle::new(false, 0, false).unwrap());
        let mut reporter = Reporter::create(
            &directory.join("result"),
            prometheus::Registry::new(),
            "memory-test".into(),
        )
        .unwrap();
        let origin = Instant::now();
        let mut generators = client_generators(&config);
        run_phase(
            &backend,
            &config,
            &[0; 16],
            &mut generators,
            "measurement",
            101,
            origin,
            origin + Duration::from_secs(5),
            &oracle,
            &mut reporter,
        )
        .await
        .unwrap();
        assert_eq!(generators.len(), 3);
        assert_eq!(active.load(Ordering::Relaxed), 0);
        let totals = reporter.totals();
        assert_eq!(totals.0, 101);
        assert_eq!(totals.1 + totals.2, 101);
        if overload {
            assert!(totals.2 >= 90);
            assert_eq!(totals.3, 0);
        } else {
            assert_eq!(totals.2, 0);
            assert_eq!(totals.3, 101);
        }
        let receiver = reporter.stall_sample_writer_for_test();
        drop(receiver);
        drop(reporter);
    }
}

#[test]
fn client_shards_one_keeps_original_workload_sequence() {
    let mut config = read_test_config();
    config.execution.client_shards = Some(1);
    config.dataset.keys = 31;
    config.workload.mix = crate::config::OperationMix {
        read: 7,
        put: 3,
        delete: 2,
        missing_read: 1,
        get_many: 2,
        put_batch: 1,
    };
    let mut original = WorkloadGenerator::new(
        config.seed ^ 0x6b76_7265_7365_6172,
        config.dataset.keys,
        config.workload.mix.clone(),
        config.workload.batch_size,
        config.workload.distribution.clone(),
        config.workload.put_key_policy,
        Arc::new(AtomicU64::new(0)),
    );
    let mut clients = client_generators(&config);
    assert_eq!(clients.len(), 1);
    for _phase in 0..3 {
        for _ in 0..257 {
            let expected = original.next_item().unwrap();
            let actual = clients[0].next_item().unwrap();
            assert_eq!(actual.operation, expected.operation);
            assert_eq!(actual.keys, expected.keys);
        }
    }
}

#[test]
fn client_shards_streams_are_deterministic_and_budgets_are_exact() {
    let mut config = read_test_config();
    config.execution.concurrency = 7;
    config.execution.queue_capacity = 11;
    config.execution.client_shards = Some(3);
    config.workload.mix.put = 1;
    let mut first = client_generators(&config);
    let mut second = client_generators(&config);
    for index in 0..101 {
        let shard = index % config.execution.effective_client_shards();
        let left = first[shard].next_item().unwrap();
        let right = second[shard].next_item().unwrap();
        assert_eq!(left.operation, right.operation);
        assert_eq!(left.keys, right.keys);
    }
    for concurrency in 1..32 {
        for clients in 1..=concurrency {
            for capacity in [concurrency, concurrency + 1, concurrency * 3 + 1] {
                assert_eq!(
                    (0..clients)
                        .map(|s| client_budget(concurrency, clients, s))
                        .sum::<usize>(),
                    concurrency
                );
                assert_eq!(
                    (0..clients)
                        .map(|s| client_budget(capacity, clients, s))
                        .sum::<usize>(),
                    capacity
                );
                for shard in 0..clients {
                    assert!(client_budget(concurrency, clients, shard) > 0);
                    assert!(
                        client_budget(capacity, clients, shard)
                            >= client_budget(concurrency, clients, shard)
                    );
                }
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_shards_preserve_sample_counts_and_oracle_results() {
    for clients in [1, 3] {
        for mixed in [false, true] {
            let guard = result_directory("client-shards-counts");
            let directory = guard.path().to_path_buf();
            let output = directory.join("result");
            let mut config = read_test_config();
            config.execution.concurrency = 7;
            config.execution.queue_capacity = 11;
            config.execution.client_shards = Some(clients);
            config.dataset.keys = 31;
            config.phases.warmup_operations = 62;
            config.phases.aging_operations = 19;
            config.phases.measurement_operations = 127;
            if mixed {
                config.workload.mix = crate::config::OperationMix {
                    read: 7,
                    put: 3,
                    delete: 2,
                    missing_read: 1,
                    get_many: 2,
                    put_batch: 1,
                };
            }
            run(
                MemoryBackend::new(false, false),
                config,
                &output,
                prometheus::Registry::new(),
            )
            .await
            .unwrap();
            let samples = std::fs::read_to_string(output.join("samples.csv")).unwrap();
            for (phase, expected) in [("warmup", 62), ("aging", 19), ("measurement", 127)] {
                let rows = samples
                    .lines()
                    .filter(|row| row.starts_with(&format!("{phase},")))
                    .collect::<Vec<_>>();
                assert_eq!(rows.len(), expected);
                for row in rows {
                    let fields = row.split(',').collect::<Vec<_>>();
                    assert_eq!(fields[2], "success");
                    let number = |index: usize| fields[index].parse::<u64>().unwrap();
                    assert_eq!(number(7), number(6) - number(4));
                    assert_eq!(number(8), number(6) - number(3));
                    assert_eq!(number(9), number(4) - number(3));
                    assert!(number(14) >= number(6));
                }
            }
            let summary: serde_json::Value =
                serde_json::from_slice(&std::fs::read(output.join("summary.json")).unwrap())
                    .unwrap();
            assert_eq!(summary["execution"]["effective_client_shards"], clients);
            let mix = summary["phase_counts"]["realized_mix"].as_object().unwrap();
            let measurement_count: u64 = mix
                .iter()
                .filter(|(key, _)| key.starts_with("measurement."))
                .map(|(_, value)| value["offered"].as_u64().unwrap())
                .sum();
            let fraction: f64 = mix
                .iter()
                .filter(|(key, _)| key.starts_with("measurement."))
                .map(|(_, value)| value["fraction"].as_f64().unwrap())
                .sum();
            assert_eq!(measurement_count, 127);
            assert!((fraction - 1.0).abs() < 1e-12);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_shards_sweeps_find_the_same_lost_baseline_key() {
    for clients in [1, 3] {
        for missing in [false, true] {
            let guard = result_directory("client-shards-sweep");
            let directory = guard.path().to_path_buf();
            let output = directory.join("result");
            let mut config = read_test_config();
            config.execution.concurrency = 7;
            config.execution.queue_capacity = 11;
            config.execution.client_shards = Some(clients);
            config.dataset.keys = 31;
            config.phases.warmup_operations = 62;
            config.phases.warmup_key_sweep = true;
            config.phases.measurement_operations = 31;
            config.phases.measurement_key_sweep = true;
            let backend = if missing {
                MemoryBackend::omitting_put(5)
            } else {
                MemoryBackend::new(false, false)
            };
            let result = run(backend, config, &output, prometheus::Registry::new()).await;
            assert_eq!(result.is_ok(), !missing);
            let outcomes = measurement_outcomes(&output);
            assert_eq!(outcomes.len(), 31);
            assert_eq!(
                outcomes
                    .iter()
                    .filter(|value| value.as_str() == "wrong_value")
                    .count(),
                usize::from(missing)
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_shards_open_loop_keeps_global_due_times() {
    for clients in [1, 3] {
        let guard = result_directory("client-shards-arrivals");
        let directory = guard.path().to_path_buf();
        let output = directory.join("result");
        let mut config = read_test_config();
        config.execution.mode = ExecutionMode::OpenLoop;
        config.execution.concurrency = 7;
        config.execution.queue_capacity = 11;
        config.execution.client_shards = Some(clients);
        config.execution.offered_ops_per_second = Some(200.0);
        config.phases.measurement_operations = 17;
        run(
            MemoryBackend::new(false, false),
            config,
            &output,
            prometheus::Registry::new(),
        )
        .await
        .unwrap();
        let samples = std::fs::read_to_string(output.join("samples.csv")).unwrap();
        let mut arrivals = samples
            .lines()
            .filter(|row| row.starts_with("measurement,"))
            .map(|row| {
                let fields = row.split(',').collect::<Vec<_>>();
                assert_eq!(fields[2], "success");
                fields[3].parse::<u64>().unwrap()
            })
            .collect::<Vec<_>>();
        arrivals.sort_unstable();
        assert_eq!(arrivals.len(), 17);
        for (index, arrival) in arrivals.iter().enumerate() {
            assert_eq!(
                *arrival - arrivals[0],
                duration_ns(Duration::from_secs_f64(index as f64 / 200.0))
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_shards_expired_deadline_joins_without_offering_work() {
    let guard = result_directory("client-shards-deadline");
    let directory = guard.path().to_path_buf();
    let mut config = read_test_config();
    config.execution.concurrency = 7;
    config.execution.queue_capacity = 11;
    config.execution.client_shards = Some(3);
    let backend = Arc::new(MemoryBackend::new(false, false));
    let oracle = Arc::new(KeyOracle::new(false, 0, false).unwrap());
    let mut reporter = Reporter::create(
        &directory.join("result"),
        prometheus::Registry::new(),
        "memory-test".into(),
    )
    .unwrap();
    let origin = Instant::now();
    let mut generators = client_generators(&config);
    let result = run_phase(
        &backend,
        &config,
        &[0; 16],
        &mut generators,
        "measurement",
        101,
        origin,
        origin,
        &oracle,
        &mut reporter,
    )
    .await;
    assert!(result.is_err());
    assert_eq!(generators.len(), 3);
    assert_eq!(reporter.totals().0, 0);
    let receiver = reporter.stall_sample_writer_for_test();
    drop(receiver);
    drop(reporter);
}

use super::*;
use crate::backend::BackendInfo;
use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
#[tokio::test]
async fn native_group_barrier_receipt_has_synchronous_cohort() {
    let guard = result_directory("native-group-barrier-receipt");
    let directory = guard.path().to_path_buf();
    let mut config = read_test_config();
    config.backend.kind = "native".to_owned();
    config.backend.options = serde_json::json!({
        "path": directory.join("data"),
        "durability": "value_wal_group_barrier",
        "native_wal_geometry": "small",
    });
    let backend =
        crate::backends::native::open(&config.backend.options, &prometheus::Registry::new())
            .await
            .unwrap();
    let receipt = durability_receipt(&config, &backend.info());
    assert_eq!(receipt["system"], "native");
    assert_eq!(receipt["tier"], "value_wal_group_barrier");
    assert_eq!(
        receipt["comparison_cohort"],
        DurabilityTier::Synchronous.comparison_cohort()
    );
    backend.shutdown().await.unwrap();
}

#[test]
fn external_acknowledgement_receipts_preserve_defaults_and_classify_relaxed_contracts() {
    let mut config = read_test_config();
    for name in ["valkey_tcp", "valkey_rdma", "kvrocks", "garnet"] {
        config.backend.kind = name.into();
        let mut info = BackendInfo {
            name: name.into(),
            server_version: String::new(),
            client_version: String::new(),
            acknowledgement: String::new(),
            consistency: String::new(),
            replication_factor: 1,
            measurement_boundary: String::new(),
            get_many_strategy: String::new(),
            batch_atomicity: crate::backend::BatchAtomicity::Unsupported,
            topology: serde_json::json!({}),
            configuration: serde_json::json!({}),
        };
        let expected = if name.starts_with("valkey") {
            serde_json::json!({"system":name,"tier":"appendfsync_always",
                "comparison_cohort":DurabilityTier::Synchronous.comparison_cohort()})
        } else {
            serde_json::json!({"system":name,"tier":"unclassified","comparison_cohort":"none"})
        };
        assert_eq!(durability_receipt(&config, &info), expected);
        info.configuration["ack_contract"] = serde_json::json!("synced");
        assert_eq!(durability_receipt(&config, &info), expected);
        let cases = if name.starts_with("valkey") {
            vec![
                ("os_buffered", "appendfsync_no", "os_buffered"),
                ("periodic_sync", "appendfsync_everysec", "periodic_sync"),
            ]
        } else if name == "kvrocks" {
            vec![(
                "page_cache",
                "wal_sync_no",
                DurabilityTier::Native.comparison_cohort(),
            )]
        } else {
            Vec::new()
        };
        for (contract, tier, cohort) in cases {
            info.configuration["ack_contract"] = serde_json::json!(contract);
            assert_eq!(
                durability_receipt(&config, &info),
                serde_json::json!({
                    "system":name,"tier":tier,"comparison_cohort":cohort,
                    "confirmed_by_backend_receipt":contract,
                })
            );
        }
    }
}

#[tokio::test]
async fn new_put_keys_reach_all_workers_across_phases_and_manifest() {
    for batch in [false, true] {
        let guard = result_directory(if batch { "new-batch" } else { "new-put" });
        let directory = guard.path().to_path_buf();
        let output = directory.join("result");
        let mut config = read_test_config();
        let namespace = [3; 16];
        config.key_namespace_hex = Some(hex(&namespace));
        config.workload.put_key_policy = crate::config::PutKeyPolicy::New;
        config.workload.mix = crate::config::OperationMix {
            put: u32::from(!batch),
            put_batch: u32::from(batch),
            ..crate::config::OperationMix::default()
        };
        config.execution.concurrency = 4;
        config.execution.queue_capacity = 4;
        config.phases.preload = true;
        config.phases.warmup_operations = 4;
        config.phases.aging_operations = 4;
        config.phases.measurement_operations = 8;
        let expected_keys = config.dataset.keys + 16 * if batch { 2 } else { 1 };
        let backend = MemoryBackend::new(false, false);
        let data = Arc::clone(&backend.data);
        run(
            backend,
            config.clone(),
            &output,
            prometheus::Registry::new(),
        )
        .await
        .unwrap();
        {
            let data = data.lock().unwrap();
            assert_eq!(u64::try_from(data.len()).unwrap(), expected_keys);
            for id in 0..expected_keys {
                assert_eq!(
                    data.get(key_bytes(&namespace, config.seed, id).as_ref()),
                    Some(&value_bytes(
                        &namespace,
                        config.seed,
                        id,
                        config.dataset.value_size,
                        config.dataset.entropy,
                    ))
                );
            }
        }
        let manifest: serde_json::Value =
            serde_json::from_slice(&std::fs::read(output.join("manifest.json")).unwrap()).unwrap();
        assert_eq!(
            manifest["run"]["config"]["workload"]["put_key_policy"],
            "new"
        );
        assert_eq!(manifest["run"]["data_policy"]["put_key_policy"], "new");
    }
}

#[test]
fn nested_lock_counters_are_differenced_and_a_missing_path_is_unavailable() {
    let at = |wait: u64| serde_json::json!({"device_credit_lock": {"wait_ns": wait, "hold_ns": 1}});
    let delta = counter_delta(&at(10), &at(25), &["device_credit_lock.wait_ns"]);
    assert_eq!(delta["counters"]["device_credit_lock.wait_ns"], 15);
    let missing = counter_delta(&at(10), &at(25), &["pool_registry_lock.wait_ns"]);
    assert_eq!(missing["available"], false);
    let reset = counter_delta(&at(25), &at(10), &["device_credit_lock.wait_ns"]);
    assert_eq!(reset["available"], false);
}

#[test]
fn deltas_reject_resets() {
    assert_eq!(
        counter_delta(
            &serde_json::json!({"reads": 4}),
            &serde_json::json!({"reads": 1}),
            &["reads"]
        )["available"],
        false
    );
}

#[tokio::test]
async fn read_evidence_excludes_warmup_and_postqualification() {
    let guard = result_directory("read-boundary");
    let directory = guard.path().to_path_buf();
    let output = directory.join("result");
    let mut config = read_test_config();
    config.phases.preload = true;
    config.phases.warmup_operations = 3;
    config.phases.measurement_operations = 2;
    run(
        MemoryBackend::new(false, false),
        config,
        &output,
        prometheus::Registry::new(),
    )
    .await
    .unwrap();
    let evidence: serde_json::Value = serde_json::from_slice(
        &std::fs::read(output.join("measurement-read-evidence.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(evidence["deltas"]["coordinator"]["counters"]["hits"], 2);
    assert!(evidence["start"]["coordinator"]["hits"].as_u64().unwrap() >= 3);
    assert!(!output.join("read-diagnostics.json").exists());
    for name in [
        "write_authority_prepare_ns",
        "write_authority_admit_ns",
        "write_authority_windows",
        "write_authority_batches",
    ] {
        assert_eq!(evidence["start"]["coordinator"][name], 0);
        assert_eq!(evidence["end"]["coordinator"][name], 0);
        assert_eq!(evidence["deltas"]["coordinator"]["counters"][name], 0);
    }
}

#[cfg(all(target_os = "linux", feature = "test-utils"))]
#[tokio::test]
async fn mapped_wal_eviction_precedes_measurement_and_failure_blocks_gate() {
    for supported in [true, false] {
        let guard = result_directory(if supported { "evict-ok" } else { "evict-fail" });
        let directory = guard.path().to_path_buf();
        let output = directory.join("result");
        let mut config = read_test_config();
        config.backend.kind = "native".into();
        config.phases.warmup_operations = 3;
        config.phases.measurement_operations = 2;
        config.phases.evict_mapped_wal_before_measurement = true;
        if !supported {
            config.measurement_gate = Some(crate::config::MeasurementGateConfig {
                ready_file: directory.join("ready"),
                release_file: directory.join("release"),
                timeout_seconds: 1,
            });
        }
        let mut backend = MemoryBackend::new(false, false);
        backend.eviction_supported = supported;
        let result = run(backend, config, &output, prometheus::Registry::new()).await;
        let eviction: serde_json::Value = serde_json::from_slice(
            &std::fs::read(output.join("pre-measurement-wal-eviction.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(eviction["available"], supported);
        if supported {
            result.unwrap();
            let evidence: serde_json::Value = serde_json::from_slice(
                &std::fs::read(output.join("measurement-read-evidence.json")).unwrap(),
            )
            .unwrap();
            assert_eq!(
                eviction["evidence"]["reads_before"],
                evidence["start"]["coordinator"]["hits"]
            );
            assert_eq!(measurement_outcomes(&output).len(), 2);
        } else {
            assert!(result.is_err());
            assert!(!directory.join("ready").exists());
        }
    }
}

#[tokio::test]
async fn read_profile_is_confined_to_measurement_and_exports_empty_coverage() {
    let guard = result_directory("read-profile-boundary");
    let directory = guard.path().to_path_buf();
    let output = directory.join("result");
    let events = Arc::new(Mutex::new(Vec::new()));
    let mut backend = MemoryBackend::new(false, false);
    backend.profile_events = Some(Arc::clone(&events));
    let mut config = read_test_config();
    config.backend.kind = "tidehunter".to_owned();
    config.phases.warmup_operations = 3;
    config.phases.measurement_operations = 2;
    config.diagnostics.read_profile_limit = 4;
    run(backend, config, &output, prometheus::Registry::new())
        .await
        .unwrap();
    let events = events.lock().unwrap();
    let arm = events.iter().position(|event| *event == "arm").unwrap();
    let finish = events.iter().position(|event| *event == "finish").unwrap();
    assert!(events[..arm].contains(&"read"));
    assert_eq!(
        events[arm + 1..finish]
            .iter()
            .filter(|event| **event == "read")
            .count(),
        2
    );
    assert!(events[finish + 1..].contains(&"read"));
    let evidence: serde_json::Value =
        serde_json::from_slice(&std::fs::read(output.join("read-diagnostics.json")).unwrap())
            .unwrap();
    assert_eq!(evidence["runtime_enabled"], true);
    assert_eq!(evidence["selection"]["requested_limit"], 4);
    assert_eq!(evidence["selection"]["selected_operations"], 0);
    assert_eq!(
        evidence["profile"]["owners"]["7"]["stages"]["server_async"]["available"],
        false
    );
    assert_eq!(
        evidence["profile"]["owners"]["7"]["counters"]["read_profile_server_async_count"],
        0
    );
    assert!(evidence["error"].is_null());
    assert_eq!(
        evidence["profile"]["read_evidence"]["measurement_attempted"],
        true
    );
}

#[tokio::test]
async fn read_profile_exports_arm_failure_and_incomplete_measurement() {
    let guard = result_directory("read-profile-failure");
    let directory = guard.path().to_path_buf();
    std::fs::create_dir_all(&directory).unwrap();
    let mut config = read_test_config();
    config.diagnostics.read_profile_limit = 1;
    let mut backend = MemoryBackend::new(false, false);
    let read_evidence = serde_json::json!({});
    let deadline = Instant::now() + Duration::from_secs(10);
    let error = export_read_diagnostics(
        &backend,
        &config,
        &directory,
        false,
        false,
        false,
        Some("arm failed"),
        deadline,
        &read_evidence,
    )
    .await
    .unwrap();
    assert_eq!(error.as_deref(), Some("arm failed"));
    let artifact: serde_json::Value =
        serde_json::from_slice(&std::fs::read(directory.join("read-diagnostics.json")).unwrap())
            .unwrap();
    assert_eq!(artifact["runtime_enabled"], false);
    assert!(artifact["profile"].is_null());
    let events = Arc::new(Mutex::new(Vec::new()));
    backend.profile_events = Some(Arc::clone(&events));
    backend.arm_read_profile(1).unwrap();
    let error = export_read_diagnostics(
        &backend,
        &config,
        &directory,
        true,
        false,
        true,
        None,
        deadline,
        &read_evidence,
    )
    .await
    .unwrap();
    assert!(error.unwrap().contains("did not complete and drain"));
    assert_eq!(events.lock().unwrap().as_slice(), ["arm", "finish"]);
}

#[tokio::test]
async fn read_profile_rejects_unobserved_activation_and_missing_rpc_samples() {
    let guard = result_directory("read-profile-missing-coverage");
    let directory = guard.path().to_path_buf();
    let mut config = read_test_config();
    config.diagnostics.read_profile_limit = 2;
    let mut backend = MemoryBackend::new(false, false);
    backend.profile_events = Some(Arc::new(Mutex::new(Vec::new())));
    backend.arm_read_profile(1).unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    let evidence = serde_json::json!({"deltas": {"coordinator": {"counters": {
        "hits": 0, "read_rpc_count": 3
    }}}});
    let error = export_read_diagnostics(
        &backend, &config, &directory, true, true, true, None, deadline, &evidence,
    )
    .await
    .unwrap()
    .unwrap();
    assert!(error.contains("activation mismatch"));
    assert!(error.contains("selected no requests"));
    let artifact: serde_json::Value =
        serde_json::from_slice(&std::fs::read(directory.join("read-diagnostics.json")).unwrap())
            .unwrap();
    assert_eq!(artifact["runtime_enabled"], true);
    assert_eq!(artifact["profile"]["runtime_limit"], 1);
    assert!(
        artifact["error"]
            .as_str()
            .unwrap()
            .contains("selected no requests")
    );
}

#[tokio::test]
async fn failed_read_boundaries_fail_case_and_preserve_terminal_evidence() {
    for boundary in [0, 1] {
        let guard = result_directory(&format!("read-boundary-error-{boundary}"));
        let directory = guard.path().to_path_buf();
        let output = directory.join("result");
        let mut backend = MemoryBackend::new(false, false);
        backend.observation_failure_at = Some(boundary);
        let mut config = read_test_config();
        config.phases.preload = true;
        config.phases.measurement_operations = 2;
        let error = run(backend, config, &output, prometheus::Registry::new())
            .await
            .err()
            .unwrap()
            .to_string();
        assert!(error.contains(if boundary == 0 {
            "measurement start boundary failed"
        } else {
            "measurement end boundary failed"
        }));
        assert!(error.contains("adoption boundary did not quiesce"));
        let evidence: serde_json::Value = serde_json::from_slice(
            &std::fs::read(output.join("measurement-read-evidence.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(evidence["measurement_attempted"], boundary == 1);
        assert_eq!(evidence["measurement_drained"], false);
        assert!(output.join("manifest.json").exists());
    }
}

struct MemoryBackend {
    data: Arc<Mutex<HashMap<Vec<u8>, Bytes>>>,
    corrupt_reads: bool,
    reverse_groups: bool,
    omit_put_number: Option<usize>,
    put_count: AtomicUsize,
    read_count: AtomicUsize,
    profile_events: Option<Arc<Mutex<Vec<&'static str>>>>,
    profile_limit: AtomicUsize,
    write_gate_counter: Option<Arc<AtomicUsize>>,
    write_armed: AtomicUsize,
    write_snapshot_calls: Arc<AtomicUsize>,
    write_finish_calls: Arc<AtomicUsize>,
    write_snapshot_error: bool,
    write_snapshot_boundary_error: bool,
    observation_failure_at: Option<usize>,
    observation_calls: AtomicUsize,
    eviction_supported: bool,
    health_failure_at: Option<usize>,
    health_calls: AtomicUsize,
    read_failure_at: Option<usize>,
}

impl MemoryBackend {
    fn new(corrupt_reads: bool, reverse_groups: bool) -> Self {
        Self {
            data: Arc::new(Mutex::new(HashMap::new())),
            corrupt_reads,
            reverse_groups,
            omit_put_number: None,
            put_count: AtomicUsize::new(0),
            read_count: AtomicUsize::new(0),
            profile_limit: AtomicUsize::new(0),
            profile_events: None,
            eviction_supported: false,
            write_gate_counter: None,
            write_armed: AtomicUsize::new(0),
            write_snapshot_calls: Arc::new(AtomicUsize::new(0)),
            write_finish_calls: Arc::new(AtomicUsize::new(0)),
            write_snapshot_error: false,
            write_snapshot_boundary_error: false,
            observation_failure_at: None,
            observation_calls: AtomicUsize::new(0),
            health_failure_at: None,
            health_calls: AtomicUsize::new(0),
            read_failure_at: None,
        }
    }

    fn omitting_put(number: usize) -> Self {
        Self {
            omit_put_number: Some(number),
            ..Self::new(false, false)
        }
    }

    fn should_omit_put(&self) -> bool {
        self.put_count.fetch_add(1, Ordering::Relaxed) == self.omit_put_number.unwrap_or(usize::MAX)
    }

    fn actual_value(&self, value: Bytes) -> Bytes {
        if self.corrupt_reads {
            Bytes::from(vec![0xff; value.len()])
        } else {
            value
        }
    }

    /// Counts GET and MGET calls together; the selected call fails with a typed cause.
    fn count_read(&self) -> BackendResult<()> {
        let call = self.read_count.fetch_add(1, Ordering::Relaxed);
        if self.read_failure_at == Some(call) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "injected backend read failure",
            )
            .into());
        }
        Ok(())
    }
}

impl KvBackend for MemoryBackend {
    fn info(&self) -> BackendInfo {
        BackendInfo {
            name: "memory-test".to_owned(),
            server_version: "test".to_owned(),
            client_version: "test".to_owned(),
            acknowledgement: "test acknowledgement".to_owned(),
            consistency: "test sequential".to_owned(),
            replication_factor: 1,
            measurement_boundary: "in process".to_owned(),
            get_many_strategy: "ordered".to_owned(),
            batch_atomicity: BatchAtomicity::SerializableTransaction,
            topology: serde_json::json!({"owners": 1}),
            // Stands in for the Tidehunter adapter in tests that select that backend kind.
            configuration: serde_json::json!({"durability_tier": "native"}),
        }
    }
    fn check_background_health(&self) -> Option<BackendResult<()>> {
        let failure_at = self.health_failure_at?;
        let call = self.health_calls.fetch_add(1, Ordering::Relaxed);
        Some(if call >= failure_at {
            Err(invalid(
                "native flusher panic: background health regression",
            ))
        } else {
            Ok(())
        })
    }
    fn read_observation(&self) -> BackendResult<serde_json::Value> {
        let boundary = self.observation_calls.fetch_add(1, Ordering::Relaxed);
        if self.observation_failure_at == Some(boundary) {
            return Err(invalid("leased adoption boundary did not quiesce"));
        }
        let reads = self.read_count.load(Ordering::Relaxed);
        Ok(serde_json::json!({"available": true, "coordinator": {
            "read_rpc_count": 0, "hits": reads, "misses": 0, "inserts": 0,
            "evictions": 0, "rejected_oversize": 0,
            "bypass_disabled": 0, "bypass_admission": 0, "bypass_eviction_limit": 0,
            "admitted_bytes": 0, "served_bytes": 0,
            "read_profile_live_inline": 0,
            "write_authority_prepare_ns": 0,
            "write_authority_admit_ns": 0,
            "write_authority_windows": 0,
            "write_authority_batches": 0,
            "read_profile_live_inline_fallback_drain": 0,
            "read_profile_live_inline_fallback_busy": 0,
            "read_profile_live_inline_fallback_grant": 0,
            "read_profile_live_inline_fallback_credit": 0,
            "read_profile_live_inline_fallback_paused": 0,
            "read_profile_live_inline_fallback_coalesced": 0},
            "client_resident": {"available": false, "error": "test unavailable"}}))
    }
    fn arm_write_profile(&self, _limit: usize) -> BackendResult<()> {
        if self.write_gate_counter.is_none() {
            return Err(invalid("write profile unsupported"));
        }
        self.write_armed.store(1, Ordering::Relaxed);
        Ok(())
    }

    fn snapshot_write_profile(&self) -> BackendResult<serde_json::Value> {
        self.write_snapshot_calls.fetch_add(1, Ordering::Relaxed);
        if self.write_armed.load(Ordering::Relaxed) != 1 {
            return Err(invalid("write profile is not armed"));
        }
        if self.write_snapshot_error {
            if self.write_snapshot_boundary_error {
                return Err(Box::new(crate::backend::LiveAdoptionBoundaryError(
                    "snapshot unavailable",
                )));
            }
            return Err(invalid("snapshot unavailable"));
        }
        let counter = self
            .write_gate_counter
            .as_ref()
            .ok_or_else(|| invalid("write profile unsupported"))?;
        Ok(serde_json::json!({"available": true, "value": counter.load(Ordering::Relaxed)}))
    }

    async fn finish_write_profile(&self) -> BackendResult<serde_json::Value> {
        self.write_finish_calls.fetch_add(1, Ordering::Relaxed);
        if self.write_armed.swap(0, Ordering::Relaxed) != 1 {
            return Err(invalid("write profile was disabled before finish"));
        }
        let counter = self
            .write_gate_counter
            .as_ref()
            .ok_or_else(|| invalid("write profile unsupported"))?;
        Ok(
            serde_json::json!({"selected_operations": 1, "post_gate_value": counter.load(Ordering::Relaxed)}),
        )
    }

    fn evict_mapped_wal_before_measurement(&self) -> BackendResult<serde_json::Value> {
        if !self.eviction_supported {
            return Err(invalid("eviction unsupported"));
        }
        Ok(serde_json::json!({"reads_before":self.read_count.load(Ordering::Relaxed)}))
    }
    fn arm_read_profile(&self, limit: usize) -> BackendResult<()> {
        let events = self
            .profile_events
            .as_ref()
            .ok_or_else(|| invalid("read profile unsupported"))?;
        assert!(limit > 0);
        self.profile_limit.store(limit, Ordering::Relaxed);
        events.lock().unwrap().push("arm");
        Ok(())
    }

    async fn finish_read_profile(&self) -> BackendResult<serde_json::Value> {
        let events = self
            .profile_events
            .as_ref()
            .ok_or_else(|| invalid("read profile unsupported"))?;
        events.lock().unwrap().push("finish");
        Ok(serde_json::json!({
            "runtime_enabled": true,
            "runtime_limit": self.profile_limit.swap(0, Ordering::Relaxed),
            "runtime_observation_mode": "test",
            "selected_operations": 0,
            "completed_operations": 0,
            "stages": {},
            "owners": {"7": {
                "stages": {"server_async": {"available": false, "sum_ns": null, "count": 0, "max_ns": null, "unavailable_reason": "no profiled owner responses for this partition"}},
                "counters": {"read_profile_server_async_count": 0}
            }},
            "coverage": {"reason": "no remote reads in test backend"}
        }))
    }

    async fn get(&self, key: &[u8]) -> BackendResult<Option<Bytes>> {
        if let Some(events) = &self.profile_events {
            events.lock().unwrap().push("read");
        }
        self.count_read()?;
        Ok(self
            .data
            .lock()
            .map_err(|_| invalid("poisoned test store"))?
            .get(key)
            .cloned()
            .map(|value| self.actual_value(value)))
    }

    async fn put(&self, key: Bytes, value: Bytes) -> BackendResult<()> {
        if !self.should_omit_put() {
            self.data
                .lock()
                .map_err(|_| invalid("poisoned test store"))?
                .insert(key.as_ref().to_vec(), value);
        }
        Ok(())
    }

    async fn delete(&self, key: Bytes) -> BackendResult<()> {
        self.data
            .lock()
            .map_err(|_| invalid("poisoned test store"))?
            .remove(key.as_ref());
        Ok(())
    }

    async fn get_many(&self, keys: &[Bytes]) -> BackendResult<Vec<Option<Bytes>>> {
        self.count_read()?;
        let data = self
            .data
            .lock()
            .map_err(|_| invalid("poisoned test store"))?;
        let mut values = keys
            .iter()
            .map(|key| {
                data.get(key.as_ref())
                    .cloned()
                    .map(|value| self.actual_value(value))
            })
            .collect::<Vec<_>>();
        if self.reverse_groups {
            values.reverse();
        }
        Ok(values)
    }

    async fn put_batch(&self, entries: &[(Bytes, Bytes)]) -> BackendResult<()> {
        let mut data = self
            .data
            .lock()
            .map_err(|_| invalid("poisoned test store"))?;
        for (key, value) in entries {
            if !self.should_omit_put() {
                data.insert(key.as_ref().to_vec(), value.clone());
            }
        }
        Ok(())
    }
}
#[derive(Default)]
struct TimelineTestState {
    armed: bool,
    arms: usize,
    finishes: usize,
    finish_gate: Option<std_mpsc::Receiver<()>>,
    finish_started: Option<std_mpsc::Sender<()>>,
    mutation_gate: Option<std_mpsc::Receiver<()>>,
    partial_owner_export: bool,
    write_wait_metrics: Option<serde_json::Value>,
    calls: Vec<(&'static str, Option<u64>, bool)>,
}

struct TimelineTestBackend {
    inner: MemoryBackend,
    state: Arc<Mutex<TimelineTestState>>,
}

impl TimelineTestBackend {
    fn record(&self, operation: &'static str, id: Option<u64>) {
        let mut state = self.state.lock().unwrap();
        let armed = state.armed;
        state.calls.push((operation, id, armed));
    }
}

impl KvBackend for TimelineTestBackend {
    fn info(&self) -> BackendInfo {
        self.inner.info()
    }

    fn arm_write_timeline(&self, limit: usize, sample_every: usize) -> BackendResult<()> {
        assert!(limit > 0);
        assert!(sample_every > 0);
        let mut state = self.state.lock().unwrap();
        assert!(!state.armed);
        state.armed = true;
        state.arms += 1;
        Ok(())
    }

    fn finish_write_timeline(&self, _deadline: Instant) -> BackendResult<serde_json::Value> {
        let (gate, started) = {
            let mut state = self.state.lock().unwrap();
            (state.finish_gate.take(), state.finish_started.take())
        };
        if let Some(started) = started {
            let _ = started.send(());
        }
        if let Some(gate) = gate {
            let _ = gate.recv();
        }
        let mut state = self.state.lock().unwrap();
        assert!(state.armed);
        state.armed = false;
        state.finishes += 1;
        let records: Vec<_> = state
            .calls
            .iter()
            .filter_map(|(operation, id, _)| {
                id.map(|id| serde_json::json!({"caller_sample_id": id, "operation": operation}))
            })
            .collect();
        let errors = if state.partial_owner_export {
            serde_json::json!([{"owner": 2, "phase": "fetch", "message": "test fetch failure", "truncated": false}])
        } else {
            serde_json::json!([])
        };
        let mut value = serde_json::json!({
            "records": records,
            "owner_records": [{"owner": 1, "envelope_id": 10}],
            "owner_errors": errors,
            "owner_expected_connections": 2,
            "owner_captured_connections": if state.partial_owner_export { 1 } else { 2 }
        });
        if let Some(metrics) = &state.write_wait_metrics {
            value["write_wait_metrics"] = metrics.clone();
        }
        Ok(value)
    }

    async fn get(&self, key: &[u8]) -> BackendResult<Option<Bytes>> {
        self.inner.get(key).await
    }

    async fn get_many(&self, keys: &[Bytes]) -> BackendResult<Vec<Option<Bytes>>> {
        self.inner.get_many(keys).await
    }

    async fn put(&self, key: Bytes, value: Bytes) -> BackendResult<()> {
        self.record("put", None);
        self.inner.put(key, value).await
    }

    async fn delete(&self, key: Bytes) -> BackendResult<()> {
        self.record("delete", None);
        self.inner.delete(key).await
    }

    async fn put_batch(&self, entries: &[(Bytes, Bytes)]) -> BackendResult<()> {
        self.record("put_batch", None);
        self.inner.put_batch(entries).await
    }

    async fn traced_put(&self, key: Bytes, value: Bytes, id: u64) -> BackendResult<()> {
        self.record("put", Some(id));
        let gate = self.state.lock().unwrap().mutation_gate.take();
        if let Some(gate) = gate {
            let _ = gate.recv();
        }
        self.inner.put(key, value).await
    }

    async fn traced_delete(&self, key: Bytes, id: u64) -> BackendResult<()> {
        self.record("delete", Some(id));
        self.inner.delete(key).await
    }

    async fn traced_put_batch(&self, entries: &[(Bytes, Bytes)], id: u64) -> BackendResult<()> {
        self.record("put_batch", Some(id));
        self.inner.put_batch(entries).await
    }
}

struct GatedBackend {
    calls: AtomicUsize,
    slow_started: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    slow_release: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
}

#[test]
fn unsupported_peer_observation_is_explicitly_unavailable() {
    let peer = GatedBackend {
        calls: AtomicUsize::new(0),
        slow_started: Mutex::new(None),
        slow_release: Mutex::new(None),
    };
    let snapshot = peer.read_observation().unwrap();
    let evidence = measurement_read_evidence(&snapshot, &snapshot, true, true);
    assert_eq!(evidence["deltas"]["available"], false);
    assert!(snapshot["error"].as_str().is_some());
}

impl KvBackend for GatedBackend {
    fn info(&self) -> BackendInfo {
        MemoryBackend::new(false, false).info()
    }

    async fn get(&self, _key: &[u8]) -> BackendResult<Option<Bytes>> {
        if self.calls.fetch_add(1, Ordering::Relaxed) == 0 {
            if let Some(started) = self.slow_started.lock().unwrap().take() {
                let _ = started.send(());
            }
            let release = { self.slow_release.lock().unwrap().take().unwrap() };
            let _ = release.await;
        }
        Ok(Some(Bytes::from_static(b"value")))
    }

    async fn put(&self, _key: Bytes, _value: Bytes) -> BackendResult<()> {
        Ok(())
    }

    async fn delete(&self, _key: Bytes) -> BackendResult<()> {
        Ok(())
    }

    async fn get_many(&self, keys: &[Bytes]) -> BackendResult<Vec<Option<Bytes>>> {
        Ok(vec![None; keys.len()])
    }

    async fn put_batch(&self, _entries: &[(Bytes, Bytes)]) -> BackendResult<()> {
        Ok(())
    }
}

fn read_test_config() -> RunConfig {
    let mut config: RunConfig = serde_json::from_str(include_str!("../../example.json")).unwrap();
    config.dataset.keys = 1;
    config.dataset.value_size = 256;
    config.workload.batch_size = 2;
    config.workload.mix = crate::config::OperationMix {
        read: 1,
        ..crate::config::OperationMix::default()
    };
    config.phases.warmup_operations = 0;
    config.phases.aging_operations = 0;
    config.phases.measurement_operations = 1;
    config.execution.request_timeout_ms = 1000;
    config.limits.deadline_seconds = 30;
    config
}

fn measurement_outcomes(path: &Path) -> Vec<String> {
    std::fs::read_to_string(path.join("samples.csv"))
        .unwrap()
        .lines()
        .filter(|line| line.starts_with("measurement,"))
        .map(|line| line.split(',').nth(2).unwrap().to_owned())
        .collect()
}

/// Result tree of one test case. `Drop` removes the tree also when an
/// assertion panics, so a failing run cannot leave it behind.
fn result_directory(name: &str) -> tempfile::TempDir {
    tempfile::TempDir::with_prefix(format!("kv-research-{name}-"))
        .expect("create the result directory")
}

/// Hang guard for a test task that waits on a marker file another task
/// writes. The measurement gate bounds its own release wait with
/// `timeout_seconds`, so this cap only stops a stuck test: it is never the
/// expected wait and it must stay far above the gate timeout.
const MARKER_WAIT_HANG_GUARD: Duration = Duration::from_secs(120);

/// Wait until another task creates `marker`. The poll observes progress
/// (the file appears); only the cap treats a missing file as a failure.
async fn wait_for_marker(marker: &Path) {
    let waited = tokio::time::timeout(MARKER_WAIT_HANG_GUARD, async {
        while !marker.exists() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    assert!(
        waited.is_ok(),
        "marker {} was not created within {MARKER_WAIT_HANG_GUARD:?}",
        marker.display()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn background_health_failure_at_each_boundary_fails_and_preserves_reason() {
    for boundary in 0..3 {
        let guard = result_directory(&format!("background-health-{boundary}"));
        let directory = guard.path().to_path_buf();
        let mut backend = MemoryBackend::new(false, false);
        backend.health_failure_at = Some(boundary);
        let result = run(
            backend,
            read_test_config(),
            &directory,
            prometheus::Registry::new(),
        )
        .await;
        assert!(result.is_err());
        let summary: serde_json::Value =
            serde_json::from_slice(&std::fs::read(directory.join("summary.json")).unwrap())
                .unwrap();
        assert_eq!(summary["status"], "failed");
        let manifest: serde_json::Value =
            serde_json::from_slice(&std::fs::read(directory.join("manifest.json")).unwrap())
                .unwrap();
        assert!(
            manifest["run"]["qualification_failure"]
                .as_str()
                .unwrap()
                .contains("native flusher panic: background health regression")
        );
    }
}

struct ActiveCall(Arc<AtomicUsize>);
impl Drop for ActiveCall {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

struct InterferenceBackend {
    inner: Arc<MemoryBackend>,
    read_delay: Duration,
    put_delay: Duration,
    active: Arc<AtomicUsize>,
}

impl InterferenceBackend {
    async fn delay(&self, duration: Duration) -> ActiveCall {
        self.active.fetch_add(1, Ordering::Relaxed);
        let active = ActiveCall(Arc::clone(&self.active));
        tokio::time::sleep(duration).await;
        active
    }
}

impl KvBackend for InterferenceBackend {
    fn info(&self) -> BackendInfo {
        self.inner.info()
    }
    async fn get(&self, key: &[u8]) -> BackendResult<Option<Bytes>> {
        let _active = self.delay(self.read_delay).await;
        self.inner.get(key).await
    }
    async fn put(&self, key: Bytes, value: Bytes) -> BackendResult<()> {
        let _active = self.delay(self.put_delay).await;
        self.inner.put(key, value).await
    }
    async fn delete(&self, key: Bytes) -> BackendResult<()> {
        self.inner.delete(key).await
    }
    async fn get_many(&self, keys: &[Bytes]) -> BackendResult<Vec<Option<Bytes>>> {
        self.inner.get_many(keys).await
    }
    async fn put_batch(&self, entries: &[(Bytes, Bytes)]) -> BackendResult<()> {
        self.inner.put_batch(entries).await
    }
}

fn interference_test_config() -> RunConfig {
    let mut c = read_test_config();
    c.case_id = Some("dual-stream-test".into());
    c.key_namespace_hex = Some("01010101010101010101010101010101".into());
    c.execution.mode = ExecutionMode::Interference;
    c.execution.concurrency = 1;
    c.execution.queue_capacity = 3;
    c.phases.measurement_operations = 10_000;
    c.interference = Some(crate::config::InterferenceConfig {
        read_offered_per_second: 100,
        read_queue_capacity: 2,
        put_concurrency: 2,
        duration_seconds: 1,
        key_policy: crate::config::InterferenceKeyPolicy::DisjointReadPut,
    });
    c
}

#[tokio::test]
async fn interference_catches_up_late_reads_but_bounds_scheduler_lateness_and_puts() {
    for lateness in [
        Duration::from_millis(4),
        INTERFERENCE_ARRIVAL_CATCH_UP + Duration::from_millis(10),
    ] {
        let guard = result_directory("interference-arrival-tail");
        let directory = guard.path().to_path_buf();
        let output = directory.join("result");
        let mut config = interference_test_config();
        let settings = config.interference.as_mut().unwrap();
        settings.read_offered_per_second = 1000;
        settings.read_queue_capacity = 1000;
        let backend = Arc::new(MemoryBackend::new(false, false));
        let mut reporter =
            Reporter::create(&output, prometheus::Registry::new(), "arrival-tail".into()).unwrap();
        let mut generator = WorkloadGenerator::new(
            1,
            1,
            config.workload.mix.clone(),
            2,
            crate::config::KeyDistribution::Uniform,
            config.workload.put_key_policy,
            Arc::new(AtomicU64::new(0)),
        );
        let origin = Instant::now();
        let oracle = Arc::new(KeyOracle::new(false, 0, false).unwrap());
        let mut phase = Box::pin(run_interference_phase(
            &backend,
            &config,
            &[1; 16],
            &mut generator,
            origin,
            origin + Duration::from_secs(5),
            &oracle,
            &mut reporter,
        ));
        // The phase uses std::Instant, so pause polling, not the Tokio clock.
        // Workers may finish, but the scheduler cannot offer another arrival.
        std::future::poll_fn(|cx| {
            let data = backend.data.lock().unwrap();
            let polled = phase.as_mut().poll(cx);
            drop(data);
            assert!(polled.is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        sleep_until(TokioInstant::from_std(
            Instant::now() + Duration::from_secs(1) + lateness,
        ))
        .await;
        let (result, status) = phase.await;
        if lateness < INTERFERENCE_ARRIVAL_CATCH_UP {
            status.unwrap();
            assert_eq!(result.read.offered, 1000);
            assert_eq!(result.read.rejected, 0);
        } else {
            let error = status.unwrap_err().to_string();
            assert!(
                error.starts_with("interference arrival scheduler missed the admission window:")
            );
            assert!(error.contains("lateness_ns="));
            assert!(error.contains(&format!("offered={}, planned=1000", result.read.offered)));
            assert!(result.read.offered < 1000);
        }
        assert!(result.measurement_drained);
        assert_eq!(result.read.admitted, result.read.completed);
        assert_eq!(result.put.admitted, result.put.completed);
        assert_eq!(result.put.offered, 2);
        let summary = RunSummary {
            status: "completed".into(),
            durability: serde_json::json!({}),
            started_unix_seconds: 0.0,
            finished_unix_seconds: 0.0,
            backend: backend.info(),
            execution: serde_json::json!({}),
            phase_counts: serde_json::json!({}),
            distributions: reporter.distribution_summaries(),
            limitations: Vec::new(),
        };
        reporter
            .finish(&summary, false, &serde_json::json!({}))
            .unwrap();
        let admission_end_ns = result.window_start_ns + 1_000_000_000;
        let samples = std::fs::read_to_string(output.join("samples.csv")).unwrap();
        let mut last_read = None;
        for row in samples.lines().skip(1) {
            let fields: Vec<_> = row.split(',').collect();
            let scheduled: u64 = fields[3].parse().unwrap();
            assert!(scheduled < admission_end_ns);
            if fields[15] == "read" {
                last_read = Some((scheduled, fields[9].parse::<u64>().unwrap()));
            } else {
                assert_eq!(fields[15], "put");
            }
        }
        if lateness < INTERFERENCE_ARRIVAL_CATCH_UP {
            let (scheduled, scheduling_ns) = last_read.unwrap();
            assert_eq!(scheduled, admission_end_ns - 1_000_000);
            assert!(scheduling_ns >= duration_ns(lateness) + 1_000_000);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interference_keeps_read_arrivals_independent_of_slow_puts_and_drains() {
    for read_delay in [Duration::ZERO, Duration::from_millis(40)] {
        let guard = result_directory("interference-streams");
        let directory = guard.path().to_path_buf();
        let output = directory.join("result");
        let config = interference_test_config();
        let inner = Arc::new(MemoryBackend::new(false, false));
        let active = Arc::new(AtomicUsize::new(0));
        let backend = InterferenceBackend {
            inner: Arc::clone(&inner),
            read_delay,
            put_delay: Duration::from_millis(30),
            active: Arc::clone(&active),
        };
        run(
            backend,
            config.clone(),
            &output,
            prometheus::Registry::new(),
        )
        .await
        .unwrap();
        assert_eq!(active.load(Ordering::Relaxed), 0);
        let result: serde_json::Value = serde_json::from_slice(
            &std::fs::read(output.join("interference_result.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(result["measurement_drained"], true);
        assert_eq!(result["read"]["offered"], 100);
        assert_eq!(result["read_offered_per_second"], 100);
        let start = result["window_start_ns"].as_u64().unwrap();
        let end = result["window_end_ns"].as_u64().unwrap();
        assert_eq!(result["measurement_elapsed_ns"], end - start);
        for stream in ["read", "put"] {
            let s = &result[stream];
            assert_eq!(
                s["offered"].as_u64().unwrap(),
                s["admitted"].as_u64().unwrap() + s["rejected"].as_u64().unwrap()
            );
            assert_eq!(s["admitted"], s["completed"]);
            assert_eq!(s["successful"], s["completed"]);
            assert_eq!(s["failed"], 0);
            assert!(s["first_dispatch_ns"].as_u64().unwrap() >= start);
            assert!(s["last_completion_ns"].as_u64().unwrap() <= end);
        }
        assert_eq!(result["put"]["rejected"], 0);
        if !read_delay.is_zero() {
            assert!(result["read"]["rejected"].as_u64().unwrap() > 0);
        }
        let overlap_start = result["read"]["first_dispatch_ns"]
            .as_u64()
            .unwrap()
            .max(result["put"]["first_dispatch_ns"].as_u64().unwrap());
        let overlap_end = result["read"]["last_completion_ns"]
            .as_u64()
            .unwrap()
            .min(result["put"]["last_completion_ns"].as_u64().unwrap());
        assert!(overlap_end - overlap_start > 800_000_000);
        let samples = std::fs::read_to_string(output.join("samples.csv")).unwrap();
        let mut read_arrivals = Vec::new();
        let mut stream_rows = [0_u64; 2];
        for line in samples
            .lines()
            .skip(1)
            .filter(|line| line.starts_with("measurement,"))
        {
            let fields: Vec<_> = line.split(',').collect();
            let scheduled: u64 = fields[3].parse().unwrap();
            let completed: u64 = fields[6].parse().unwrap();
            let response: u64 = fields[8].parse().unwrap();
            assert_eq!(response, completed - scheduled);
            if fields[15] == "read" {
                read_arrivals.push(scheduled);
                stream_rows[0] += 1;
            } else {
                assert_eq!(fields[15], "put");
                stream_rows[1] += 1;
            }
            assert_eq!(fields[1], fields[15]);
            if fields[5] == "true" {
                let dispatch: u64 = fields[4].parse().unwrap();
                assert_eq!(fields[7].parse::<u64>().unwrap(), completed - dispatch);
                assert_eq!(fields[9].parse::<u64>().unwrap(), dispatch - scheduled);
            }
        }
        read_arrivals.sort_unstable();
        assert_eq!(
            read_arrivals,
            (0..100)
                .map(|index| start + index * 10_000_000)
                .collect::<Vec<_>>()
        );
        assert_eq!(stream_rows[0], 100);
        assert_eq!(stream_rows[1], result["put"]["offered"].as_u64().unwrap());
        // The fake backend acknowledges only after storing each new value.
        let data = inner.data.lock().unwrap();
        for id in 0..result["put"]["successful"].as_u64().unwrap() + 1 {
            assert_eq!(
                data.get(key_bytes(&[1; 16], config.seed, id).as_ref())
                    .unwrap(),
                &value_bytes(
                    &[1; 16],
                    config.seed,
                    id,
                    config.dataset.value_size,
                    config.dataset.entropy
                )
            );
        }
        drop(data);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stalled_sample_writer_fails_interference_without_delaying_admission_or_drain() {
    let guard = result_directory("interference-stalled-writer");
    let directory = guard.path().to_path_buf();
    let output = directory.join("result");
    let mut config = interference_test_config();
    config.interference.as_mut().unwrap().duration_seconds = 20;
    let backend = Arc::new(InterferenceBackend {
        inner: Arc::new(MemoryBackend::new(false, false)),
        read_delay: Duration::from_millis(20),
        put_delay: Duration::from_millis(30),
        active: Arc::new(AtomicUsize::new(0)),
    });
    let mut reporter = Reporter::create(
        &output,
        prometheus::Registry::new(),
        "stalled-writer".into(),
    )
    .unwrap();
    // Keep the bounded receiver alive but never consume a row.
    let stalled_writer = reporter.stall_sample_writer_for_test();
    let mut generator = WorkloadGenerator::new(
        1,
        1,
        config.workload.mix.clone(),
        2,
        crate::config::KeyDistribution::Uniform,
        config.workload.put_key_policy,
        Arc::new(AtomicU64::new(0)),
    );
    let origin = Instant::now();
    let oracle = Arc::new(KeyOracle::new(false, 0, false).unwrap());
    let (result, status) = tokio::time::timeout(
        Duration::from_secs(2),
        run_interference_phase(
            &backend,
            &config,
            &[1; 16],
            &mut generator,
            origin,
            origin + Duration::from_secs(5),
            &oracle,
            &mut reporter,
        ),
    )
    .await
    .expect("telemetry saturation must not await the stalled consumer");
    assert!(
        status
            .unwrap_err()
            .to_string()
            .contains("interference telemetry admission failed")
    );
    assert!(result.read.offered < 2000);
    assert!(result.measurement_drained);
    assert_eq!(result.read.admitted, result.read.completed);
    assert_eq!(result.put.admitted, result.put.completed);
    assert_eq!(backend.active.load(Ordering::Relaxed), 0);
    assert_eq!(Arc::strong_count(&backend), 1);
    let summary = RunSummary {
        status: "failed".into(),
        durability: serde_json::json!({}),
        started_unix_seconds: 0.0,
        finished_unix_seconds: 0.0,
        backend: backend.info(),
        execution: serde_json::json!({}),
        phase_counts: serde_json::json!({}),
        distributions: reporter.distribution_summaries(),
        limitations: Vec::new(),
    };
    assert!(
        reporter
            .finish(
                &summary,
                true,
                &serde_json::json!({"interference":{"schema_version":1}})
            )
            .is_err()
    );
    drop(stalled_writer);
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(output.join("manifest.json")).unwrap()).unwrap();
    assert_eq!(manifest["status"], "failed");
    assert_eq!(manifest["raw_samples_complete"], false);
    assert_eq!(manifest["sample_writer_backpressure_events"], 0);
    assert!(
        manifest["sample_writer_admission"]["submit_errors"]
            .as_u64()
            .unwrap()
            > 0
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interference_timeouts_cancel_async_calls_and_leave_no_pending_work() {
    for (timeout_ms, deadline_ms) in [(10, 5000), (1000, 50)] {
        let guard = result_directory("interference-timeout");
        let directory = guard.path().to_path_buf();
        let mut config = interference_test_config();
        config.execution.request_timeout_ms = timeout_ms;
        let backend = Arc::new(InterferenceBackend {
            inner: Arc::new(MemoryBackend::new(false, false)),
            read_delay: Duration::from_secs(60),
            put_delay: Duration::from_secs(60),
            active: Arc::new(AtomicUsize::new(0)),
        });
        let mut reporter = Reporter::create(
            &directory.join("result"),
            prometheus::Registry::new(),
            "timeout-test".into(),
        )
        .unwrap();
        let mut generator = WorkloadGenerator::new(
            1,
            1,
            config.workload.mix.clone(),
            2,
            crate::config::KeyDistribution::Uniform,
            config.workload.put_key_policy,
            Arc::new(AtomicU64::new(0)),
        );
        let origin = Instant::now();
        let oracle = Arc::new(KeyOracle::new(false, 0, false).unwrap());
        let (result, status) = run_interference_phase(
            &backend,
            &config,
            &[1; 16],
            &mut generator,
            origin,
            origin + Duration::from_millis(deadline_ms),
            &oracle,
            &mut reporter,
        )
        .await;
        assert_eq!(status.is_ok(), deadline_ms == 5000);
        assert!(result.measurement_drained);
        if deadline_ms == 5000 {
            assert_eq!(result.read.offered, 100);
        } else {
            assert!(result.read.offered < 100);
        }
        assert_eq!(result.read.admitted, result.read.completed);
        assert_eq!(result.put.admitted, result.put.completed);
        assert_eq!(result.read.successful + result.put.successful, 0);
        assert_eq!(backend.active.load(Ordering::Relaxed), 0);
        assert!(Arc::strong_count(&backend) == 1);
        let summary = RunSummary {
            status: "failed".into(),
            durability: serde_json::json!({}),
            started_unix_seconds: 0.0,
            finished_unix_seconds: 0.0,
            backend: backend.info(),
            execution: serde_json::json!({}),
            phase_counts: serde_json::json!({}),
            distributions: reporter.distribution_summaries(),
            limitations: Vec::new(),
        };
        reporter
            .finish(&summary, true, &serde_json::json!({}))
            .unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interference_native_barrier_puts_survive_local_reopen() {
    let guard = result_directory("interference-native-barrier");
    let directory = guard.path().to_path_buf();
    let output = directory.join("result");
    let mut config = interference_test_config();
    config.phases.measurement_operations = 1_000_000;
    config.limits.max_operations = 1_000_001;
    config.backend.options = serde_json::json!({
        "path":directory.join("data"), "key_count":1_000_001,
        "durability":"value_wal_barrier", "native_wal_geometry":"small",
    });
    let backend =
        crate::backends::native::open(&config.backend.options, &prometheus::Registry::new())
            .await
            .unwrap();
    run(
        backend,
        config.clone(),
        &output,
        prometheus::Registry::new(),
    )
    .await
    .unwrap();
    let result: serde_json::Value =
        serde_json::from_slice(&std::fs::read(output.join("interference_result.json")).unwrap())
            .unwrap();
    let puts = result["put"]["successful"].as_u64().unwrap();
    assert!(puts > 0);
    assert_eq!(result["put"]["admitted"], puts);
    let reopened =
        crate::backends::native::open(&config.backend.options, &prometheus::Registry::new())
            .await
            .unwrap();
    for id in 0..=puts {
        assert_eq!(
            reopened
                .get(key_bytes(&[1; 16], config.seed, id).as_ref())
                .await
                .unwrap()
                .unwrap(),
            value_bytes(
                &[1; 16],
                config.seed,
                id,
                config.dataset.value_size,
                config.dataset.entropy
            )
        );
    }
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
async fn interference_safety_ceiling_fails_closed_and_still_drains_both_streams() {
    let guard = result_directory("interference-ceiling");
    let directory = guard.path().to_path_buf();
    let output = directory.join("result");
    let mut config = interference_test_config();
    config.phases.measurement_operations = 101;
    let error = run(
        MemoryBackend::new(false, false),
        config,
        &output,
        prometheus::Registry::new(),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("operation safety bound"));
    let result: serde_json::Value =
        serde_json::from_slice(&std::fs::read(output.join("interference_result.json")).unwrap())
            .unwrap();
    assert_eq!(result["measurement_drained"], true);
    for stream in ["read", "put"] {
        assert_eq!(result[stream]["admitted"], result[stream]["completed"]);
        assert_eq!(result[stream]["failed"], 0);
    }
}

#[tokio::test]
async fn write_wait_export_preserves_exact_finish_values_and_legacy_gating() {
    let metrics = serde_json::json!({
        "enabled": true, "saturated": false, "complete": true,
        "windows": (["owner_enqueue_to_dequeue", "admitted_to_append_enqueue", "harvest_to_durable"].map(|wait| serde_json::json!({
            "wait":wait, "observations": 5, "depth_sum": 9, "depth_max": 3, "wake_attempts": 4, "wake_coalesced": 2,
        }))),
    });
    for focused in [false, true] {
        let guard = result_directory("write-wait-export");
        let directory = guard.path().to_path_buf();
        let output = directory.join("result");
        let mut config = read_test_config();
        config.backend.kind = "tidehunter".into();
        config.diagnostics.write_timeline_limit = 2;
        config.case_id = focused.then(|| "g9-test".into());
        let state = Arc::new(Mutex::new(TimelineTestState {
            write_wait_metrics: Some(metrics.clone()),
            ..TimelineTestState::default()
        }));
        let backend = TimelineTestBackend {
            inner: MemoryBackend::new(false, false),
            state: Arc::clone(&state),
        };
        run(backend, config, &output, prometheus::Registry::new())
            .await
            .unwrap();
        assert_eq!(state.lock().unwrap().finishes, 1);
        let path = output.join("write_wait_metrics.json");
        assert_eq!(path.exists(), focused);
        if focused {
            let value: serde_json::Value =
                serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
            assert_eq!(value.as_object().unwrap().len(), 6);
            assert_eq!(value["schema_version"], 1);
            assert_eq!(value["case_id"], "g9-test");
            assert_eq!(value["enabled"], metrics["enabled"]);
            assert_eq!(value["saturated"], metrics["saturated"]);
            assert_eq!(value["complete"], metrics["complete"]);
            assert_eq!(value["windows"], metrics["windows"]);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_worker_starts_replacement_while_first_request_is_gated() {
    let guard = result_directory("idle-worker-progress");
    let directory = guard.path().to_path_buf();
    let output = directory.join("result");
    let mut config: RunConfig = serde_json::from_str(include_str!("../../example.json")).unwrap();
    config.execution.concurrency = 2;
    config.execution.queue_capacity = 2;
    config.execution.request_timeout_ms = 30_000;
    let (slow_started_tx, slow_started_rx) = tokio::sync::oneshot::channel();
    let (slow_release_tx, slow_release_rx) = tokio::sync::oneshot::channel();
    let backend = Arc::new(GatedBackend {
        calls: AtomicUsize::new(0),
        slow_started: Mutex::new(Some(slow_started_tx)),
        slow_release: Mutex::new(Some(slow_release_rx)),
    });
    let mut reporter = Reporter::create(
        &output,
        prometheus::Registry::new(),
        "gated-test".to_owned(),
    )
    .unwrap();
    let origin = Instant::now();
    let deadline = origin + Duration::from_secs(30);
    let oracle = Arc::new(KeyOracle::new(false, 0, false).unwrap());
    let mut pool = WorkerPool::new(&backend, &config, &[0; 16], origin, &oracle).unwrap();
    let read = |id| WorkItem {
        operation: OperationKind::Read,
        keys: vec![id],
    };

    pool.try_dispatch(read(0), "measurement", 0, deadline, false, None)
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), slow_started_rx)
        .await
        .unwrap()
        .unwrap();
    pool.try_dispatch(read(1), "measurement", 0, deadline, false, None)
        .unwrap();
    let fast = tokio::time::timeout(Duration::from_secs(5), pool.receive())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fast.id, 1);
    pool.record_completion(fast, &mut reporter).await.unwrap();

    pool.try_dispatch(read(2), "measurement", 0, deadline, false, None)
        .unwrap();
    let replacement = tokio::time::timeout(Duration::from_secs(5), pool.receive()).await;
    let (replacement, completed_while_slow_pending) = match replacement {
        Ok(Ok(completion)) => {
            let completion_id = completion.id;
            (
                Some(completion),
                completion_id == 2 && pool.pending.contains_key(&0),
            )
        }
        Ok(Err(_)) | Err(_) => (None, false),
    };
    let _ = slow_release_tx.send(());
    let slow = pool.receive().await.unwrap();
    let slow_id = slow.id;
    pool.record_completion(slow, &mut reporter).await.unwrap();
    let replacement_id = if let Some(completion) = replacement {
        let id = completion.id;
        pool.record_completion(completion, &mut reporter)
            .await
            .unwrap();
        id
    } else {
        let completion = pool.receive().await.unwrap();
        let id = completion.id;
        pool.record_completion(completion, &mut reporter)
            .await
            .unwrap();
        id
    };
    pool.shutdown(true);
    drop(pool);
    drop(reporter);
    assert!(completed_while_slow_pending);
    assert_eq!(slow_id, 0);
    assert_eq!(replacement_id, 2);
}

#[tokio::test]
async fn full_preload_missing_scalar_read_is_wrong_value() {
    let guard = result_directory("full-preload-missing-read");
    let directory = guard.path().to_path_buf();
    let mut config = read_test_config();
    config.phases.preload = true;
    let output = directory.join("result");
    let result = run(
        MemoryBackend::omitting_put(5),
        config,
        &output,
        prometheus::Registry::new(),
    )
    .await;
    let outcomes = measurement_outcomes(&output);
    assert!(result.is_err());
    assert_eq!(outcomes, ["wrong_value"]);
}

#[tokio::test]
async fn full_preload_missing_get_many_value_is_wrong_value() {
    let guard = result_directory("full-preload-missing-many");
    let directory = guard.path().to_path_buf();
    let mut config = read_test_config();
    config.dataset.keys = 2;
    config.workload.mix = crate::config::OperationMix {
        get_many: 1,
        ..crate::config::OperationMix::default()
    };
    config.seed = (0..10_000)
        .find(|seed| {
            let mut generator = WorkloadGenerator::new(
                *seed ^ 0x6b76_7265_7365_6172,
                config.dataset.keys,
                config.workload.mix.clone(),
                config.workload.batch_size,
                config.workload.distribution.clone(),
                config.workload.put_key_policy,
                Arc::new(AtomicU64::new(0)),
            );
            let keys = generator.next_item().unwrap().keys;
            keys.contains(&0) && keys.contains(&1)
        })
        .expect("small deterministic seed search finds both dataset keys");
    let output = directory.join("result");
    let result = run(
        MemoryBackend::omitting_put(5),
        config,
        &output,
        prometheus::Registry::new(),
    )
    .await;
    let outcomes = measurement_outcomes(&output);
    assert!(result.is_err());
    assert_eq!(outcomes, ["wrong_value"]);
}

#[tokio::test]
async fn partial_preload_missing_scalar_read_remains_success() {
    let guard = result_directory("partial-preload-missing-read");
    let directory = guard.path().to_path_buf();
    let mut config = read_test_config();
    config.phases.preload = false;
    let output = directory.join("result");
    let result = run(
        MemoryBackend::new(false, false),
        config,
        &output,
        prometheus::Registry::new(),
    )
    .await;
    let outcomes = measurement_outcomes(&output);
    assert!(result.is_ok());
    assert_eq!(outcomes, ["success"]);
}

#[tokio::test]
async fn aging_delete_keeps_later_missing_read_valid() {
    let guard = result_directory("aging-delete-missing-read");
    let directory = guard.path().to_path_buf();
    let mut config = read_test_config();
    config.workload.mix = crate::config::OperationMix {
        read: 1,
        delete: 1,
        ..crate::config::OperationMix::default()
    };
    config.phases.aging_operations = 1;
    config.phases.measurement_operations = 1;
    config.seed = (0..10_000)
        .find(|seed| {
            let mut generator = WorkloadGenerator::new(
                *seed ^ 0x6b76_7265_7365_6172,
                config.dataset.keys,
                config.workload.mix.clone(),
                config.workload.batch_size,
                config.workload.distribution.clone(),
                config.workload.put_key_policy,
                Arc::new(AtomicU64::new(0)),
            );
            generator.next_item().unwrap().operation == OperationKind::Delete
                && generator.next_item().unwrap().operation == OperationKind::Read
        })
        .expect("small deterministic seed search finds delete then read");
    let output = directory.join("result");
    let result = run(
        MemoryBackend::new(false, false),
        config,
        &output,
        prometheus::Registry::new(),
    )
    .await;
    let outcomes = measurement_outcomes(&output);
    assert!(result.is_ok());
    assert_eq!(outcomes, ["success"]);
}

#[tokio::test]
async fn mixed_delete_run_fails_when_a_never_deleted_key_is_lost() {
    let guard = result_directory("mixed-delete-lost-key");
    let directory = guard.path().to_path_buf();
    let mut config = read_test_config();
    config.dataset.keys = 2;
    config.execution.concurrency = 1;
    config.execution.queue_capacity = 1;
    config.workload.mix = crate::config::OperationMix {
        read: 1,
        delete: 1,
        ..crate::config::OperationMix::default()
    };
    // The only measured operation reads key 0, which the backend lost and nothing deleted.
    config.seed = (0..10_000)
        .find(|seed| {
            let mut generator = WorkloadGenerator::new(
                *seed ^ 0x6b76_7265_7365_6172,
                config.dataset.keys,
                config.workload.mix.clone(),
                config.workload.batch_size,
                config.workload.distribution.clone(),
                config.workload.put_key_policy,
                Arc::new(AtomicU64::new(0)),
            );
            let item = generator.next_item().unwrap();
            item.operation == OperationKind::Read && item.keys == [0]
        })
        .expect("small deterministic seed search finds a read of key 0");
    let output = directory.join("result");
    // Qualification issues five puts, so put number 5 is the first preload put (key 0).
    let result = run(
        MemoryBackend::omitting_put(5),
        config,
        &output,
        prometheus::Registry::new(),
    )
    .await;
    let outcomes = measurement_outcomes(&output);
    assert!(result.is_err());
    assert_eq!(outcomes, ["wrong_value"]);
}

#[tokio::test]
async fn samples_carry_a_worker_release_clock_after_the_api_boundary() {
    let guard = result_directory("worker-release-clock");
    let directory = guard.path().to_path_buf();
    let output = directory.join("result");
    let result = run(
        MemoryBackend::new(false, false),
        read_test_config(),
        &output,
        prometheus::Registry::new(),
    )
    .await;
    assert!(result.is_ok());
    let samples = std::fs::read_to_string(output.join("samples.csv")).unwrap();
    let mut lines = samples.lines();
    let columns: Vec<_> = lines.next().unwrap().split(',').collect();
    let completed = columns.iter().position(|c| *c == "completed_ns").unwrap();
    let released = columns.iter().position(|c| *c == "released_ns").unwrap();
    let mut rows = 0_u64;
    for line in lines {
        let fields: Vec<_> = line.split(',').collect();
        let completed: u64 = fields[completed].parse().unwrap();
        let released: u64 = fields[released].parse().unwrap();
        assert!(released >= completed, "{line}");
        rows += 1;
    }
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(output.join("manifest.json")).unwrap()).unwrap();
    assert!(rows > 0);
    assert_eq!(
        manifest["worker_release"]["samples"],
        manifest["completed_operations"]
    );
    assert!(manifest["sample_writer_admission"]["stall_events"].is_u64());
}

#[test]
fn completed_delete_explains_absence_until_a_clean_put_restores_the_key() {
    let oracle = KeyOracle::new(true, 2, true).unwrap();
    assert!(!oracle.absence_allowed(0, oracle.snapshot(0)));
    oracle.begin_delete(0);
    oracle.end_delete(0, true);
    assert!(oracle.absence_allowed(0, oracle.snapshot(0)));
    assert!(!oracle.absence_allowed(1, oracle.snapshot(1)));
    let ticket = oracle.begin_put(0);
    oracle.end_put(0, ticket, true);
    assert!(!oracle.absence_allowed(0, oracle.snapshot(0)));
}

#[test]
fn put_overlapping_a_delete_cannot_restore_the_guarantee() {
    let oracle = KeyOracle::new(true, 1, true).unwrap();
    // Delete in flight when the put starts: the delete may take effect after the put.
    oracle.begin_delete(0);
    let ticket = oracle.begin_put(0);
    oracle.end_delete(0, true);
    oracle.end_put(0, ticket, true);
    assert!(oracle.absence_allowed(0, oracle.snapshot(0)));
    // Delete starting during a later clean put.
    let ticket = oracle.begin_put(0);
    assert!(!ticket.overlapped_delete);
    oracle.begin_delete(0);
    oracle.end_put(0, ticket, true);
    oracle.end_delete(0, true);
    assert!(oracle.absence_allowed(0, oracle.snapshot(0)));
    // An unacknowledged put never restores presence.
    let ticket = oracle.begin_put(0);
    oracle.end_put(0, ticket, false);
    assert!(oracle.absence_allowed(0, oracle.snapshot(0)));
}

#[test]
fn read_spanning_a_delete_cycle_may_see_absence() {
    let oracle = KeyOracle::new(true, 1, true).unwrap();
    let before = oracle.snapshot(0);
    // Delete and a clean rewrite both finish inside the read interval.
    oracle.begin_delete(0);
    oracle.end_delete(0, true);
    let ticket = oracle.begin_put(0);
    oracle.end_put(0, ticket, true);
    assert!(oracle.absence_allowed(0, before));
    assert!(!oracle.absence_allowed(0, oracle.snapshot(0)));
    // A delete still in flight at the end of the read also explains absence.
    let before = oracle.snapshot(0);
    oracle.begin_delete(0);
    assert!(oracle.absence_allowed(0, before));
}

#[test]
fn delete_with_unknown_outcome_keeps_absence_allowed_forever() {
    let oracle = KeyOracle::new(true, 1, true).unwrap();
    oracle.begin_delete(0);
    oracle.end_delete(0, false);
    let ticket = oracle.begin_put(0);
    oracle.end_put(0, ticket, true);
    assert!(oracle.absence_allowed(0, oracle.snapshot(0)));
}

#[test]
fn oracle_without_baseline_accepts_absence_and_without_deletes_rejects_it() {
    let unchecked = KeyOracle::new(false, 4, true).unwrap();
    assert!(unchecked.absence_allowed(3, unchecked.snapshot(3)));
    let strict = KeyOracle::new(true, 4, false).unwrap();
    assert!(!strict.tracks_mutations());
    assert!(!strict.absence_allowed(3, strict.snapshot(3)));
}

#[test]
fn read_overlapping_a_put_may_see_absence_but_not_before_or_after() {
    let oracle = KeyOracle::new(true, 1, true).unwrap();
    // Put in flight when the read starts.
    let ticket = oracle.begin_put(0);
    let before = oracle.snapshot(0);
    oracle.end_put(0, ticket, true);
    assert!(oracle.absence_allowed(0, before));
    // Put starts and ends inside the read interval.
    let before = oracle.snapshot(0);
    let ticket = oracle.begin_put(0);
    oracle.end_put(0, ticket, true);
    assert!(oracle.absence_allowed(0, before));
    // Put still in flight when the read returns.
    let before = oracle.snapshot(0);
    let ticket = oracle.begin_put(0);
    assert!(oracle.absence_allowed(0, before));
    // A put that finished before the read began explains nothing.
    oracle.end_put(0, ticket, false);
    assert!(!oracle.absence_allowed(0, oracle.snapshot(0)));
}

/// Reads return nothing (a lost key); the put blocks until released.
struct OverlapBackend {
    started: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    release: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
}

impl OverlapBackend {
    async fn gate(&self) {
        let started = self.started.lock().unwrap().take();
        if let Some(started) = started {
            let _ = started.send(());
        }
        let release = self.release.lock().unwrap().take();
        if let Some(release) = release {
            let _ = release.await;
        }
    }
}
impl KvBackend for OverlapBackend {
    fn info(&self) -> BackendInfo {
        MemoryBackend::new(false, false).info()
    }
    async fn get(&self, _key: &[u8]) -> BackendResult<Option<Bytes>> {
        Ok(None)
    }
    async fn put(&self, _key: Bytes, _value: Bytes) -> BackendResult<()> {
        self.gate().await;
        Ok(())
    }
    async fn delete(&self, _key: Bytes) -> BackendResult<()> {
        Ok(())
    }
    async fn get_many(&self, keys: &[Bytes]) -> BackendResult<Vec<Option<Bytes>>> {
        Ok(vec![None; keys.len()])
    }
    async fn put_batch(&self, _entries: &[(Bytes, Bytes)]) -> BackendResult<()> {
        self.gate().await;
        Ok(())
    }
}

#[tokio::test]
async fn gated_put_explains_an_absent_read_only_while_it_is_in_flight() {
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let backend = OverlapBackend {
        started: Mutex::new(Some(started_tx)),
        release: Mutex::new(Some(release_rx)),
    };
    let config = read_test_config();
    let oracle = KeyOracle::new(true, 1, true).unwrap();
    let origin = Instant::now();
    let deadline = origin + Duration::from_secs(30);
    let run = |operation| {
        execute_item(
            &backend,
            &config,
            &[0; 16],
            WorkItem {
                operation,
                keys: vec![0],
            },
            false,
            None,
            "measurement",
            0,
            origin,
            deadline,
            &oracle,
            Arc::new(AtomicU64::new(0)),
        )
    };
    let (put, during) = tokio::join!(run(OperationKind::Put), async {
        started_rx.await.unwrap();
        let sample = run(OperationKind::Read).await;
        release_tx.send(()).unwrap();
        sample
    });
    let after = run(OperationKind::Read).await;
    assert_eq!(put.outcome, "success");
    assert_eq!(
        during.outcome, "success",
        "absence during the put is explained"
    );
    assert_eq!(
        after.outcome, "wrong_value",
        "the same lost key after the put is not"
    );
}

#[tokio::test]
async fn a_batch_repeating_one_key_is_one_in_flight_put() {
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let backend = OverlapBackend {
        started: Mutex::new(Some(started_tx)),
        release: Mutex::new(Some(release_rx)),
    };
    let config = read_test_config();
    let oracle = KeyOracle::new(true, 1, true).unwrap();
    let origin = Instant::now();
    // Far more members than workers, all naming key 0: counted per member this would be 2048.
    let members = 2048;
    let put = execute_item(
        &backend,
        &config,
        &[0; 16],
        WorkItem {
            operation: OperationKind::PutBatch,
            keys: vec![0; members],
        },
        false,
        None,
        "measurement",
        0,
        origin,
        origin + Duration::from_secs(60),
        &oracle,
        Arc::new(AtomicU64::new(0)),
    );
    let observe = async {
        started_rx.await.unwrap();
        let during = oracle.snapshot(0);
        release_tx.send(()).unwrap();
        during
    };
    let (_, during) = tokio::join!(put, observe);
    assert_eq!(inflight(during.puts), 1);
    assert_eq!(epoch(during.puts), 1);
    assert_eq!(oracle.snapshot(0).puts & INFLIGHT_MASK, 0);
    assert!(oracle.ensure_capacity(4096).is_ok());
    assert!(
        oracle
            .ensure_capacity(usize::try_from(INFLIGHT_MASK).unwrap())
            .is_err()
    );
}

#[tokio::test]
async fn run_refuses_a_tier_the_backend_receipt_does_not_confirm() {
    let guard = result_directory("tier-unconfirmed");
    let directory = guard.path().to_path_buf();
    let mut config = read_test_config();
    config.backend.kind = "tidehunter".to_owned();
    config.backend.options = serde_json::json!({"durability_tier": "synchronous"});
    let output = directory.join("result");
    // The test backend reports the native tier, so a synchronous request must not run.
    let result = run(
        MemoryBackend::new(false, false),
        config,
        &output,
        prometheus::Registry::new(),
    )
    .await;
    let created = output.exists();
    assert!(result.is_err());
    assert!(!created, "no artifact may describe an unconfirmed tier");
}

#[tokio::test]
async fn confirmed_tier_and_cohort_are_recorded_in_summary_and_manifest() {
    let guard = result_directory("tier-confirmed");
    let directory = guard.path().to_path_buf();
    let mut config = read_test_config();
    config.backend.kind = "tidehunter".to_owned();
    let output = directory.join("result");
    let result = run(
        MemoryBackend::new(false, false),
        config,
        &output,
        prometheus::Registry::new(),
    )
    .await;
    let read = |name: &str| -> serde_json::Value {
        serde_json::from_slice(&std::fs::read(output.join(name)).unwrap()).unwrap()
    };
    let (summary, manifest) = (read("summary.json"), read("manifest.json"));
    assert!(result.is_ok());
    for durability in [&summary["durability"], &manifest["durability"]] {
        assert_eq!(durability["tier"], "native");
        assert_eq!(durability["comparison_cohort"], "native_pagecache");
    }
    assert_eq!(manifest["run"]["durability"], summary["durability"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn open_loop_completions_do_not_release_future_arrivals() {
    let guard = result_directory("arrivals");
    let directory = guard.path().to_path_buf();
    let output = directory.join("result");
    let mut config: RunConfig = serde_json::from_str(include_str!("../../example.json")).unwrap();
    config.dataset.keys = 8;
    config.dataset.value_size = 256;
    config.workload.batch_size = 2;
    config.workload.mix = crate::config::OperationMix {
        read: 1,
        ..crate::config::OperationMix::default()
    };
    config.execution.mode = ExecutionMode::OpenLoop;
    config.execution.concurrency = 1;
    config.execution.queue_capacity = 2;
    config.execution.offered_ops_per_second = Some(100.0);
    config.execution.request_timeout_ms = 1000;
    config.phases.warmup_operations = 0;
    config.phases.aging_operations = 0;
    config.phases.measurement_operations = 8;
    config.limits.deadline_seconds = 30;
    let result = run(
        MemoryBackend::new(false, false),
        config,
        &output,
        prometheus::Registry::new(),
    )
    .await;
    let raw = std::fs::read_to_string(output.join("samples.csv"));
    result.unwrap();
    let raw = raw.unwrap();
    let rows: Vec<_> = raw
        .lines()
        .filter(|line| line.starts_with("measurement,"))
        .collect();
    assert_eq!(
        rows.len(),
        8,
        "every declared arrival must have a terminal row"
    );
    for row in rows {
        let columns: Vec<_> = row.split(',').collect();
        let scheduled: u64 = columns[3].parse().unwrap();
        let dispatched: u64 = columns[4].parse().unwrap();
        let completed: u64 = columns[6].parse().unwrap();
        assert!(
            scheduled <= dispatched && dispatched <= completed,
            "early or inconsistent dispatch: {row}"
        );
    }
}

#[tokio::test]
async fn qualification_rejects_wrong_returned_bytes() {
    let backend = MemoryBackend::new(true, false);
    assert!(
        qualify(
            &backend,
            &[3; 16],
            42,
            Instant::now() + Duration::from_secs(3),
            1000
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn qualification_rejects_reordered_get_many_results() {
    let backend = MemoryBackend::new(false, true);
    assert!(
        qualify(
            &backend,
            &[3; 16],
            42,
            Instant::now() + Duration::from_secs(3),
            1000
        )
        .await
        .is_err()
    );
}

/// Qualification GET/MGET substeps in call order (the memory backend supports batches).
const QUALIFICATION_READ_STEPS: [&str; 5] = [
    "qualification initial read",
    "qualification overwritten read",
    "qualification scalar ordered get_many",
    "qualification ordered batch read",
    "qualification delete verification",
];

#[tokio::test]
async fn qualification_read_errors_name_substep_and_keep_typed_source() {
    for (call, step) in QUALIFICATION_READ_STEPS.into_iter().enumerate() {
        let mut backend = MemoryBackend::new(false, false);
        backend.read_failure_at = Some(call);
        let error = qualify(
            &backend,
            &[3; 16],
            42,
            Instant::now() + Duration::from_secs(3),
            1000,
        )
        .await
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            format!("{step}: injected backend read failure")
        );
        let source = error
            .source()
            .and_then(|source| source.downcast_ref::<std::io::Error>())
            .expect("backend error must remain the source");
        assert_eq!(source.kind(), std::io::ErrorKind::TimedOut);
        assert!(is_timed_out(error.as_ref()));
        assert_eq!(backend.read_count.load(Ordering::Relaxed), call + 1);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn qualification_read_error_substep_is_retained_in_manifest() {
    for (call, step) in QUALIFICATION_READ_STEPS.into_iter().enumerate() {
        let guard = result_directory(&format!("qualification-read-{call}"));
        let directory = guard.path().to_path_buf();
        let mut backend = MemoryBackend::new(false, false);
        backend.read_failure_at = Some(call);
        let result = run(
            backend,
            read_test_config(),
            &directory,
            prometheus::Registry::new(),
        )
        .await;
        let manifest: serde_json::Value =
            serde_json::from_slice(&std::fs::read(directory.join("manifest.json")).unwrap())
                .unwrap();
        assert!(result.is_err());
        let failure = manifest["run"]["qualification_failure"].as_str().unwrap();
        assert!(
            failure.contains(&format!(
                "pre-workload qualification failed: {step}: injected backend read failure"
            )),
            "{failure}"
        );
    }
}

#[test]
fn sample_latency_uses_shared_dispatch_and_completion_instants() {
    let sample = make_sample_at(
        "measurement",
        "read",
        "success",
        3,
        7,
        true,
        17,
        10,
        1,
        8,
        Cow::Borrowed(""),
    );
    assert_eq!(sample.scheduling_ns, 4);
    assert_eq!(sample.response_ns, 14);
}

#[test]
fn backend_timed_out_errors_are_timeout_outcomes() {
    let error = std::io::Error::new(std::io::ErrorKind::TimedOut, "backend deadline");
    assert!(is_timed_out(&error));
    assert_eq!(timeout_outcome(OperationKind::PutBatch), "unknown_mutation");
    assert_eq!(timeout_outcome(OperationKind::GetMany), "timeout");
}
#[test]
fn rejected_arrivals_keep_nonzero_response_and_zero_dispatch() {
    let origin = Instant::now()
        .checked_sub(Duration::from_nanos(100))
        .unwrap();
    let work = WorkItem {
        operation: OperationKind::GetMany,
        keys: vec![1, 2, 3],
    };
    let sample = rejected_sample("measurement", &work, 1, origin, "capacity");
    assert_eq!(sample.dispatched_ns, 0);
    assert!(sample.response_ns >= 99);
    assert_eq!(sample.logical_keys, 3);
    assert_eq!(sample.outcome, "rejected");
}
#[tokio::test]
async fn write_snapshot_precedes_completion_gate_without_finishing_profile() {
    let guard = result_directory("write-profile-gate");
    let directory = guard.path().to_path_buf();
    let output = directory.join("result");
    let ready = directory.join("complete");
    let release = directory.join("observed");
    let counter = Arc::new(AtomicUsize::new(7));
    let mut backend = MemoryBackend::new(false, false);
    backend.write_gate_counter = Some(Arc::clone(&counter));
    let snapshot_calls = Arc::clone(&backend.write_snapshot_calls);
    let finish_calls = Arc::clone(&backend.write_finish_calls);
    let gate_snapshot_calls = Arc::clone(&snapshot_calls);
    let gate_finish_calls = Arc::clone(&finish_calls);
    let mut config = read_test_config();
    config.diagnostics.write_profile_limit = 1;
    config.measurement_completion_gate = Some(crate::config::MeasurementGateConfig {
        ready_file: ready.clone(),
        release_file: release.clone(),
        timeout_seconds: 2,
    });
    let controller = tokio::spawn(async move {
        wait_for_marker(&ready).await;
        assert_eq!(
            gate_snapshot_calls.load(Ordering::Relaxed),
            1,
            "the gate must observe exactly the pre-gate snapshot"
        );
        assert_eq!(
            gate_finish_calls.load(Ordering::Relaxed),
            0,
            "the profile must not finish before the gate releases"
        );
        counter.store(19, Ordering::Relaxed);
        std::fs::write(release, b"observed").unwrap();
    });
    run(backend, config, &output, prometheus::Registry::new())
        .await
        .unwrap();
    controller.await.unwrap();
    assert_eq!(snapshot_calls.load(Ordering::Relaxed), 1);
    assert_eq!(finish_calls.load(Ordering::Relaxed), 1);
    let evidence: serde_json::Value =
        serde_json::from_slice(&std::fs::read(output.join("write-diagnostics.json")).unwrap())
            .unwrap();
    assert_eq!(evidence["pre_completion_gate"]["value"], 7);
    assert_eq!(evidence["profile"]["post_gate_value"], 19);
}

#[tokio::test]
async fn disabled_write_profile_never_snapshots_or_finishes() {
    let guard = result_directory("write-profile-disabled");
    let directory = guard.path().to_path_buf();
    let output = directory.join("result");
    let backend = MemoryBackend::new(false, false);
    let snapshots = Arc::clone(&backend.write_snapshot_calls);
    let finishes = Arc::clone(&backend.write_finish_calls);
    run(
        backend,
        read_test_config(),
        &output,
        prometheus::Registry::new(),
    )
    .await
    .unwrap();
    assert_eq!(snapshots.load(Ordering::Relaxed), 0);
    assert_eq!(finishes.load(Ordering::Relaxed), 0);
    assert!(!output.join("write-diagnostics.json").exists());
    assert!(!output.join("write-timeline.json").exists());
}

#[tokio::test]
async fn timeline_dispatch_and_csv_join_are_bounded_and_disabled_cleanly() {
    for operation in ["put", "delete", "put_batch"] {
        for limit in [0, 3] {
            let guard = result_directory("timeline-end-to-end");
            let directory = guard.path().to_path_buf();
            let output = directory.join("result");
            let state = Arc::new(Mutex::new(TimelineTestState::default()));
            let backend = TimelineTestBackend {
                inner: MemoryBackend::new(false, false),
                state: Arc::clone(&state),
            };
            let mut config = read_test_config();
            config.backend.kind = "tidehunter".to_owned();
            config.dataset.keys = 8;
            config.execution.concurrency = 1;
            config.phases.warmup_operations = 2;
            config.phases.measurement_operations = 6;
            config.diagnostics.write_timeline_limit = limit;
            config.workload.mix = crate::config::OperationMix::default();
            match operation {
                "put" => config.workload.mix.put = 1,
                "delete" => config.workload.mix.delete = 1,
                "put_batch" => config.workload.mix.put_batch = 1,
                _ => unreachable!(),
            }
            run(backend, config, &output, prometheus::Registry::new())
                .await
                .unwrap();
            let csv = std::fs::read_to_string(output.join("samples.csv")).unwrap();
            let mut rows = csv.lines();
            let header: Vec<_> = rows.next().unwrap().split(',').collect();
            let id_column = header
                .iter()
                .position(|name| *name == "caller_sample_id")
                .unwrap();
            let mut raw_ids = Vec::new();
            let mut warmup_count = 0;
            let mut measurement_count = 0;
            for row in rows {
                let fields: Vec<_> = row.split(',').collect();
                if fields[0] == "warmup" {
                    warmup_count += 1;
                    assert_eq!(fields[id_column], "");
                } else if fields[0] == "measurement" {
                    measurement_count += 1;
                    if !fields[id_column].is_empty() {
                        assert_eq!(fields[1], operation);
                        raw_ids.push(fields[id_column].parse::<u64>().unwrap());
                    }
                }
            }
            assert_eq!(warmup_count, 2);
            assert_eq!(measurement_count, 6);
            raw_ids.sort_unstable();
            assert_eq!(raw_ids, (0..limit as u64).collect::<Vec<_>>());
            let state = state.lock().unwrap();
            assert_eq!(state.arms, usize::from(limit > 0));
            assert_eq!(state.finishes, usize::from(limit > 0));
            assert!(
                state
                    .calls
                    .iter()
                    .filter(|(_, id, _)| id.is_some())
                    .all(|(name, _, armed)| *name == operation && *armed)
            );
            if limit > 0 {
                assert_eq!(
                    state
                        .calls
                        .iter()
                        .filter(|(name, id, armed)| *name == operation && id.is_none() && *armed)
                        .count(),
                    3
                );
                assert!(
                    state
                        .calls
                        .iter()
                        .filter(|(name, id, armed)| *name == operation && id.is_none() && !*armed)
                        .count()
                        >= 2
                );
                let evidence: serde_json::Value = serde_json::from_slice(
                    &std::fs::read(output.join("write-timeline.json")).unwrap(),
                )
                .unwrap();
                let mut sidecar_ids: Vec<_> = evidence["timeline"]["records"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|record| record["caller_sample_id"].as_u64().unwrap())
                    .collect();
                sidecar_ids.sort_unstable();
                assert_eq!(sidecar_ids, raw_ids);
            } else {
                assert!(state.calls.iter().all(|(_, id, _)| id.is_none()));
                assert!(
                    state
                        .calls
                        .iter()
                        .filter(|(name, _, _)| *name == operation)
                        .count()
                        >= 8
                );
                assert!(!output.join("write-timeline.json").exists());
            }
            drop(state);
        }
    }
}

#[test]
fn timeline_owner_status_detects_errors_and_coverage_independently() {
    let mut value = serde_json::json!({
        "owner_errors": [],
        "owner_expected_connections": 2,
        "owner_captured_connections": 2
    });
    assert!(write_timeline_owner_error(&value).is_none());
    value["deadline_exceeded"] = serde_json::json!(true);
    assert!(
        write_timeline_owner_error(&value)
            .unwrap()
            .contains("partial evidence")
    );
    value["deadline_exceeded"] = serde_json::json!(false);
    value["owner_captured_connections"] = serde_json::json!(1);
    assert!(write_timeline_owner_error(&value).is_some());
    value["owner_captured_connections"] = serde_json::json!(2);
    value["owner_errors"] = serde_json::json!([{"phase": "disarm"}]);
    assert!(write_timeline_owner_error(&value).is_some());
    value.as_object_mut().unwrap().remove("owner_errors");
    assert!(write_timeline_owner_error(&value).is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_drain_and_stalled_finalizer_still_publish_terminal_artifacts() {
    let guard = result_directory("timeline-stalled-finalizer");
    let directory = guard.path().to_path_buf();
    let output = directory.join("result");
    let (mutation_release, mutation_gate) = std_mpsc::channel();
    let (finish_release, finish_gate) = std_mpsc::channel();
    let (finish_started, started) = std_mpsc::channel();
    let state = Arc::new(Mutex::new(TimelineTestState {
        mutation_gate: Some(mutation_gate),
        finish_gate: Some(finish_gate),
        finish_started: Some(finish_started),
        ..TimelineTestState::default()
    }));
    let backend = TimelineTestBackend {
        inner: MemoryBackend::new(false, false),
        state,
    };
    let mut config = read_test_config();
    config.backend.kind = "tidehunter".to_owned();
    config.diagnostics.write_timeline_limit = 1;
    config.workload.mix = crate::config::OperationMix {
        put: 1,
        ..crate::config::OperationMix::default()
    };
    config.limits.deadline_seconds = 1;
    config.limits.shutdown_grace_seconds = 1;
    let outcome = tokio::time::timeout(
        Duration::from_secs(5),
        run(backend, config, &output, prometheus::Registry::new()),
    )
    .await;
    assert!(outcome.unwrap().is_err());
    assert!(output.join("samples.csv").is_file());
    assert!(output.join("summary.json").is_file());
    let evidence: serde_json::Value =
        serde_json::from_slice(&std::fs::read(output.join("write-timeline.json")).unwrap())
            .unwrap();
    assert_eq!(evidence["status"], "failed");
    assert_eq!(evidence["available"], false);
    assert!(
        evidence["error"]
            .as_str()
            .unwrap()
            .contains("shutdown grace")
    );
    started.recv_timeout(Duration::from_secs(1)).unwrap();
    let _ = mutation_release.send(());
    let _ = finish_release.send(());
}

#[tokio::test]
async fn partial_owner_timeline_export_fails_run_but_retains_records() {
    let guard = result_directory("timeline-partial-owner");
    let directory = guard.path().to_path_buf();
    let output = directory.join("result");
    let state = Arc::new(Mutex::new(TimelineTestState {
        partial_owner_export: true,
        ..TimelineTestState::default()
    }));
    let backend = TimelineTestBackend {
        inner: MemoryBackend::new(false, false),
        state: Arc::clone(&state),
    };
    let mut config = read_test_config();
    config.backend.kind = "tidehunter".to_owned();
    config.diagnostics.write_timeline_limit = 1;
    config.workload.mix = crate::config::OperationMix {
        put: 1,
        ..crate::config::OperationMix::default()
    };
    assert!(
        run(backend, config, &output, prometheus::Registry::new())
            .await
            .is_err()
    );
    let evidence: serde_json::Value =
        serde_json::from_slice(&std::fs::read(output.join("write-timeline.json")).unwrap())
            .unwrap();
    assert_eq!(evidence["available"], true);
    assert_eq!(evidence["status"], "failed");
    assert!(
        evidence["error"]
            .as_str()
            .unwrap()
            .contains("owner timeline capture incomplete")
    );
    assert_eq!(
        evidence["timeline"]["owner_errors"][0]["message"],
        "test fetch failure"
    );
    assert_eq!(evidence["timeline"]["owner_records"][0]["envelope_id"], 10);
    assert_eq!(evidence["timeline"]["records"][0]["caller_sample_id"], 0);
    assert_eq!(state.lock().unwrap().finishes, 1);
}

#[tokio::test]
async fn write_timeline_arm_error_exports_unavailable_evidence() {
    let guard = result_directory("timeline-arm-error");
    let directory = guard.path().to_path_buf();
    let output = directory.join("result");
    let mut config = read_test_config();
    config.backend.kind = "tidehunter".to_owned();
    config.diagnostics.write_timeline_limit = 1;
    assert!(
        run(
            MemoryBackend::new(false, false),
            config,
            &output,
            prometheus::Registry::new()
        )
        .await
        .is_err()
    );
    let evidence: serde_json::Value =
        serde_json::from_slice(&std::fs::read(output.join("write-timeline.json")).unwrap())
            .unwrap();
    assert_eq!(evidence["available"], false);
    assert!(evidence["error"].as_str().unwrap().contains("timeline"));
    assert!(evidence["timeline"].is_null());
}

#[tokio::test]
async fn write_snapshot_error_is_unavailable_but_profile_still_finishes() {
    for adoption_boundary in [false, true] {
        let guard = result_directory(&format!("write-snapshot-error-{adoption_boundary}"));
        let directory = guard.path().to_path_buf();
        let output = directory.join("result");
        let mut backend = MemoryBackend::new(false, false);
        backend.write_gate_counter = Some(Arc::new(AtomicUsize::new(7)));
        backend.write_snapshot_error = true;
        backend.write_snapshot_boundary_error = adoption_boundary;
        let snapshots = Arc::clone(&backend.write_snapshot_calls);
        let finishes = Arc::clone(&backend.write_finish_calls);
        let mut config = read_test_config();
        config.diagnostics.write_profile_limit = 1;
        let result = run(backend, config, &output, prometheus::Registry::new()).await;
        if adoption_boundary {
            assert!(
                result
                    .err()
                    .unwrap()
                    .to_string()
                    .contains("measurement write profile boundary failed")
            );
        } else {
            result.unwrap();
        }
        let evidence: serde_json::Value =
            serde_json::from_slice(&std::fs::read(output.join("write-diagnostics.json")).unwrap())
                .unwrap();
        assert_eq!(snapshots.load(Ordering::Relaxed), 1);
        assert_eq!(finishes.load(Ordering::Relaxed), 1);
        assert_eq!(evidence["pre_completion_gate"]["available"], false);
        assert!(
            evidence["pre_completion_gate"]["reason"]
                .as_str()
                .unwrap()
                .contains("snapshot unavailable")
        );
        assert_eq!(evidence["profile"]["post_gate_value"], 7);
    }
}

#[tokio::test]
async fn measurement_completion_gate_waits_for_release_with_process_live() {
    let guard = result_directory("measurement-completion-gate");
    let directory = guard.path().to_path_buf();
    let ready_file = directory.join("measurement.complete");
    let release_file = directory.join("measurement.observed");
    let gate = crate::config::MeasurementGateConfig {
        ready_file: ready_file.clone(),
        release_file: release_file.clone(),
        timeout_seconds: 2,
    };
    let controller = tokio::spawn(async move {
        wait_for_marker(&ready_file).await;
        std::fs::write(release_file, b"observed\n").unwrap();
    });
    wait_for_measurement_gate(&gate, Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
    controller.await.unwrap();
    assert!(gate.ready_file.exists());
}

#[tokio::test]
async fn measurement_completion_gate_timeout_preserves_ready_marker() {
    let guard = result_directory("measurement-completion-timeout");
    let directory = guard.path().to_path_buf();
    let gate = crate::config::MeasurementGateConfig {
        ready_file: directory.join("measurement.complete"),
        release_file: directory.join("measurement.observed"),
        timeout_seconds: 1,
    };
    assert!(
        wait_for_measurement_gate(&gate, Instant::now() + Duration::from_secs(2))
            .await
            .is_err()
    );
    assert!(gate.ready_file.exists());
}
#[test]
fn timeline_selection_is_measurement_only_and_includes_batches() {
    for op in [
        OperationKind::Put,
        OperationKind::Delete,
        OperationKind::PutBatch,
    ] {
        assert!(!is_timeline_candidate("warmup", op, 0, 1));
        assert!(is_timeline_candidate("measurement", op, 0, 1));
        assert!(!is_timeline_candidate("measurement", op, 1, 1));
        assert!(!is_timeline_candidate("measurement", op, 0, 0));
    }
    assert!(!is_timeline_candidate(
        "measurement",
        OperationKind::Read,
        0,
        1
    ));
}

#[test]
fn diagnostic_selection_is_measurement_only_and_bounded_to_scalar_mutations() {
    assert!(!is_profile_candidate("warmup", OperationKind::Put, 0, 4));
    assert!(!is_profile_candidate(
        "measurement",
        OperationKind::Read,
        0,
        4
    ));
    assert!(!is_profile_candidate(
        "measurement",
        OperationKind::PutBatch,
        0,
        4
    ));
    assert!(is_profile_candidate(
        "measurement",
        OperationKind::Put,
        0,
        4
    ));
    assert!(is_profile_candidate(
        "measurement",
        OperationKind::Delete,
        3,
        4
    ));
    assert!(!is_profile_candidate(
        "measurement",
        OperationKind::Put,
        4,
        4
    ));
}
#[tokio::test]
async fn existing_dataset_checks_all_values_before_measurement_without_overwriting() {
    for scenario in ["present", "missing", "corrupt"] {
        let guard = result_directory(&format!("existing-{scenario}"));
        let directory = guard.path().to_path_buf();
        let mut config = read_test_config();
        let namespace = [9; 16];
        config.key_namespace_hex = Some(hex(&namespace));
        config.dataset.keys = 2;
        config.phases.preload = false;
        config.phases.verify_existing_dataset = true;
        let backend = MemoryBackend::new(false, false);
        for id in 0..2 {
            if scenario == "missing" && id == 1 {
                continue;
            }
            let mut value = value_bytes(
                &namespace,
                config.seed,
                id,
                config.dataset.value_size,
                config.dataset.entropy,
            )
            .to_vec();
            if scenario == "corrupt" && id == 1 {
                value[32] ^= 1;
            }
            backend.data.lock().unwrap().insert(
                key_bytes(&namespace, config.seed, id).to_vec(),
                value.into(),
            );
        }
        let output = directory.join("result");
        let result = run(backend, config, &output, prometheus::Registry::new()).await;
        assert_eq!(result.is_ok(), scenario == "present");
        if scenario != "present" {
            assert!(measurement_outcomes(&output).is_empty());
            let samples = std::fs::read_to_string(output.join("samples.csv")).unwrap();
            assert!(
                samples
                    .lines()
                    .any(|line| line.starts_with("verify_existing_dataset,read,wrong_value,"))
            );
        }
    }
}

#[test]
fn live_interval_requires_complete_monotone_route_counters() {
    let mut stats = serde_json::json!({
        "completed_read_ops": 2, "completed_read_bytes": 4096,
        "completed_write_ops": 3, "completed_write_bytes": 8192,
        "recovery_read_ops": 1, "recovery_read_bytes": 4096,
        "metadata_requests": 4, "namespace_connects": 1,
        "unknown_writes": 0, "errors": 0,
        "queue_ns": 0, "queue_samples": 0, "submit_ns": 0, "submit_samples": 0,
        "cq_ns": 0, "cq_samples": 0, "control_ns": 0, "control_samples": 0,
        "validation_ns": 0, "validation_samples": 0, "publication_ns": 0,
        "publication_samples": 0, "namespace_setup_ns": 0, "namespace_setup_samples": 0,
        "read_io_ns": 0, "read_io_samples": 0,
        "route_evidence_dropped": 0, "fence_evidence_dropped": 0
    });
    // Counters added with the multi-flight lanes; every one must be present to difference.
    for name in [
        "lane_inflight_samples",
        "lane_inflight_request_sum",
        "lane_inflight_op_sum",
        "delivery_ns",
        "delivery_samples",
        "controller_connect_ns",
        "controller_connect_samples",
        "controller_disconnect_ns",
        "controller_disconnect_samples",
        "grant_hits",
        "grant_misses",
        "grant_evictions",
        "copy_bytes_avoided",
        "leased_frame_batches",
        "write_submit_calls",
        "owner_drains",
        "owner_drain_batches",
        "owner_drain_stop_empty",
        "owner_drain_stop_allocated",
        "owner_drain_stop_fence",
        "owner_drain_stop_limit",
        "owner_drain_stop_credit",
        "leased_submit_ready_left",
        "leased_submit_ready_left_batches",
        "leased_submit_ready_left_full",
        "leased_submit_ready_left_full_batches",
        "leased_deferred_submits",
        "lease_exchanges",
        "leased_frames",
        "leased_eventfd_unavailable",
        "leased_worker_waits",
        "leased_worker_wait_timeouts",
        "leased_flushes",
        "leased_flush_ns",
        "leased_flush_samples",
        "leased_flush_covered_frames",
        "read_lane_pin_failures",
        "leased_bytes",
        "leased_skip_markers",
        "extents_leased",
        "adoption_async_exchanges",
        "adoption_sync_exchanges",
        "adoption_history_full_settles",
        "fua_writes",
        "leased_failed_writes",
        "leased_short_writes",
        "leased_timed_out_writes",
        "leased_full_writes",
        "failed_write_ops",
        "short_write_ops",
        "short_write_bytes",
        "lease_reserve_ns",
        "lease_namespace_sync_ns",
        "lease_publish_ns",
        "lease_profile_samples",
        "adopt_owner_ns",
        "adopt_close_scan_ns",
        "adopt_close_scan_bytes",
        "adopt_profile_samples",
        "extent_preleases",
        "extent_switch_waits",
        "resume_attempts",
        "resume_rewrites",
        "resume_frames",
        "resume_refused",
    ] {
        stats[name] = serde_json::json!(0);
    }
    for stage in [
        "adoption_async",
        "adoption_settle",
        "leased_write",
        "extent_switch",
    ] {
        stats[format!("{stage}_ns")] = serde_json::json!(10);
        stats[format!("{stage}_samples")] = serde_json::json!(1);
    }
    let start = serde_json::json!({"available":true, "live_nvmeof": stats});
    let mut end = start.clone();
    end["live_nvmeof"]["completed_write_ops"] = serde_json::json!(5);
    let evidence = measurement_read_evidence(&start, &end, true, true);
    assert_eq!(evidence["live_nvmeof"]["available"], true);
    assert_eq!(
        evidence["live_nvmeof"]["counters"]["completed_write_ops"],
        2
    );
    for name in [
        "lease_reserve_ns",
        "lease_namespace_sync_ns",
        "lease_publish_ns",
        "lease_profile_samples",
        "adopt_owner_ns",
        "adopt_close_scan_ns",
        "adopt_close_scan_bytes",
        "adopt_profile_samples",
        "extent_preleases",
        "extent_switch_waits",
    ] {
        let mut measured = end.clone();
        measured["live_nvmeof"][name] = serde_json::json!(7);
        let delta = measurement_read_evidence(&start, &measured, true, true);
        assert_eq!(delta["live_nvmeof"]["counters"][name], 7, "{name}");
        measured["live_nvmeof"]
            .as_object_mut()
            .unwrap()
            .remove(name);
        assert_eq!(
            measurement_read_evidence(&start, &measured, true, true)["live_nvmeof"]["available"],
            false,
            "{name}",
        );
    }
    for stage in [
        "adoption_async",
        "adoption_settle",
        "leased_write",
        "extent_switch",
    ] {
        let mut measured = end.clone();
        measured["live_nvmeof"][format!("{stage}_ns")] = serde_json::json!(40);
        measured["live_nvmeof"][format!("{stage}_samples")] = serde_json::json!(3);
        let delta = measurement_read_evidence(&start, &measured, true, true);
        assert_eq!(delta["live_nvmeof"]["counters"][format!("{stage}_ns")], 30);
        assert_eq!(
            delta["live_nvmeof"]["counters"][format!("{stage}_samples")],
            2
        );
        measured["live_nvmeof"]
            .as_object_mut()
            .unwrap()
            .remove(&format!("{stage}_samples"));
        assert_eq!(
            measurement_read_evidence(&start, &measured, true, true)["live_nvmeof"]["available"],
            false
        );
        measured["live_nvmeof"][format!("{stage}_samples")] = serde_json::json!(0);
        assert_eq!(
            measurement_read_evidence(&start, &measured, true, true)["live_nvmeof"]["available"],
            false
        );
    }
    let mut incomplete = end.clone();
    incomplete["live_nvmeof"]
        .as_object_mut()
        .unwrap()
        .remove("metadata_requests");
    assert_eq!(
        measurement_read_evidence(&start, &incomplete, true, true)["live_nvmeof"]["available"],
        false
    );
    end["live_nvmeof"]["completed_read_ops"] = serde_json::json!(1);
    assert_eq!(
        measurement_read_evidence(&start, &end, true, true)["live_nvmeof"]["available"],
        false
    );
}

fn read_boundary() -> serde_json::Value {
    let coordinator: serde_json::Map<String, serde_json::Value> = [
        "read_rpc_count",
        "hits",
        "misses",
        "inserts",
        "evictions",
        "rejected_oversize",
        "bypass_disabled",
        "bypass_admission",
        "bypass_eviction_limit",
        "admitted_bytes",
        "served_bytes",
        "read_profile_live_inline",
        "write_authority_prepare_ns",
        "write_authority_admit_ns",
        "write_authority_windows",
        "write_authority_batches",
        "read_profile_live_inline_fallback_drain",
        "read_profile_live_inline_fallback_busy",
        "read_profile_live_inline_fallback_grant",
        "read_profile_live_inline_fallback_credit",
        "read_profile_live_inline_fallback_paused",
        "read_profile_live_inline_fallback_coalesced",
    ]
    .into_iter()
    .map(|name| (name.to_owned(), serde_json::json!(0)))
    .collect();
    serde_json::json!({"available": true, "coordinator": coordinator})
}

#[test]
fn live_inline_boundaries_require_complete_monotone_route_counts() {
    let start = read_boundary();
    let mut end = start.clone();
    end["coordinator"]["read_profile_live_inline"] = serde_json::json!(7);
    let measured = measurement_read_evidence(&start, &end, true, true);
    assert_eq!(
        measured["deltas"]["coordinator"]["counters"]["read_profile_live_inline"],
        7
    );
    let reset = measurement_read_evidence(&end, &start, true, true);
    assert_eq!(reset["deltas"]["coordinator"]["available"], false);
    end["coordinator"]
        .as_object_mut()
        .unwrap()
        .remove("read_profile_live_inline_fallback_grant");
    let missing = measurement_read_evidence(&start, &end, true, true);
    assert_eq!(missing["deltas"]["coordinator"]["available"], false);
}

#[test]
fn authority_split_read_boundaries_require_complete_monotone_counters() {
    for name in [
        "write_authority_prepare_ns",
        "write_authority_admit_ns",
        "write_authority_windows",
        "write_authority_batches",
    ] {
        let mut start = read_boundary();
        start["coordinator"][name] = serde_json::json!(10);
        let mut end = start.clone();
        end["coordinator"][name] = serde_json::json!(17);
        let evidence = measurement_read_evidence(&start, &end, true, true);
        assert_eq!(evidence["start"]["coordinator"][name], 10);
        assert_eq!(evidence["end"]["coordinator"][name], 17);
        assert_eq!(evidence["deltas"]["coordinator"]["counters"][name], 7);
        assert_eq!(evidence["deltas"]["coordinator"]["available"], true);
        let reset = measurement_read_evidence(&end, &start, true, true);
        assert_eq!(reset["deltas"]["coordinator"]["available"], false);
        for invalid in [
            serde_json::Value::Null,
            serde_json::json!("17"),
            serde_json::json!(-1),
        ] {
            let mut invalid_end = end.clone();
            invalid_end["coordinator"][name] = invalid;
            let evidence = measurement_read_evidence(&start, &invalid_end, true, true);
            assert_eq!(evidence["deltas"]["coordinator"]["available"], false);
        }
        end["coordinator"].as_object_mut().unwrap().remove(name);
        let missing = measurement_read_evidence(&start, &end, true, true);
        assert_eq!(missing["deltas"]["coordinator"]["available"], false);
    }
}

/// The exported live delta and peak sets and the observer exporter's admitted
/// sets change together: both are pinned to one checked-in fixture.
#[test]
fn live_nvmeof_counter_fields_match_the_observer_fixture() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../experiments/kv-comparison/observability/live_nvmeof_counter_fields.json");
    let fixture: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    let sorted = |fields: &[&'static str]| {
        let mut fields = fields.to_vec();
        fields.sort_unstable();
        fields
    };
    let expected = serde_json::json!({
        "delta": sorted(LIVE_NVMEOF_DELTA_FIELDS),
        "peak": sorted(LIVE_NVMEOF_PEAK_FIELDS),
    });
    assert_eq!(
        fixture,
        expected,
        "update {} to:\n{}",
        path.display(),
        serde_json::to_string_pretty(&expected).unwrap()
    );
}

/// Every serialized live counter is either a measurement delta or a lifetime peak.
#[test]
fn live_nvmeof_counter_fields_cover_every_serialized_live_counter() {
    let stats = serde_json::to_value(
        tidehunter::distributed::nvmeof::live_metrics::LiveNvmeStats::default(),
    )
    .unwrap();
    // Routes and fences are evidence lists, not counters.
    let mut serialized: Vec<&str> = stats
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .filter(|key| !matches!(*key, "routes" | "fences"))
        .collect();
    serialized.sort_unstable();
    let mut fields = [LIVE_NVMEOF_DELTA_FIELDS, LIVE_NVMEOF_PEAK_FIELDS].concat();
    fields.sort_unstable();
    assert_eq!(serialized, fields);
}
