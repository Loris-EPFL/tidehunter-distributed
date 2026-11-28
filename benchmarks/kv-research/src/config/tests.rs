#[test]
fn client_shards_auto_size_and_are_bounded_by_callers() {
    let valid = config();
    let mut value = serde_json::to_value(&valid).unwrap();
    value["execution"]
        .as_object_mut()
        .unwrap()
        .remove("client_shards");
    let decoded: RunConfig = serde_json::from_value(value).unwrap();
    assert_eq!(decoded.execution.client_shards, None);
    assert_eq!(
        decoded.execution.effective_client_shards(),
        decoded
            .execution
            .concurrency
            .min(tidehunter::parallelism::available())
    );
    decoded.validate().unwrap();
    for clients in [0, 3] {
        let mut invalid = valid.clone();
        invalid.execution.client_shards = Some(clients);
        assert!(
            invalid
                .validate()
                .unwrap_err()
                .to_string()
                .contains("client_shards")
        );
    }
    let mut sharded = valid;
    sharded.execution.client_shards = Some(2);
    assert_eq!(sharded.execution.effective_client_shards(), 2);
    sharded.validate().unwrap();
    sharded.execution.mode = ExecutionMode::Interference;
    assert!(
        sharded
            .validate()
            .unwrap_err()
            .to_string()
            .contains("client_shards=1")
    );
    sharded.execution.client_shards = None;
    assert_eq!(sharded.execution.effective_client_shards(), 1);
    sharded.execution.client_shards = Some(1);
    assert_eq!(sharded.execution.effective_client_shards(), 1);
}

use super::*;

fn config() -> RunConfig {
    RunConfig {
        schema_version: 1,
        case_id: None,
        interference: None,
        backend: BackendSpec {
            kind: "native".to_owned(),
            options: serde_json::json!({}),
        },
        seed: 1,
        namespace: Some("test".to_owned()),
        key_namespace_hex: None,
        dataset: DatasetConfig {
            keys: 100,
            value_size: 256,
            entropy: 1.0,
        },
        workload: WorkloadConfig {
            kind: SyntheticKind::Synthetic,
            distribution: KeyDistribution::Uniform,
            mix: OperationMix {
                read: 1,
                ..OperationMix::default()
            },
            batch_size: 32,
            put_key_policy: PutKeyPolicy::Overwrite,
        },
        execution: ExecutionConfig {
            mode: ExecutionMode::ClosedLoop,
            concurrency: 2,
            client_shards: Some(1),
            queue_capacity: 4,
            request_timeout_ms: 1000,
            offered_ops_per_second: None,
        },
        phases: PhaseConfig {
            preload: true,
            verify_existing_dataset: false,
            evict_mapped_wal_before_measurement: false,
            warmup_operations: 1,
            warmup_key_sweep: false,
            measurement_key_sweep: false,
            aging_operations: 0,
            measurement_operations: 2,
            relocation: false,
        },
        qualification_only: false,
        source_revision: None,
        measurement_gate: None,
        measurement_completion_gate: None,
        diagnostics: DiagnosticsConfig::default(),
        limits: LimitsConfig {
            max_keys: 100,
            max_bytes: 25_600,
            max_operations: 103,
            deadline_seconds: 60,
            shutdown_grace_seconds: 5,
        },
    }
}

#[test]
fn put_key_policy_defaults_to_overwrite_and_rejects_unknown_values() {
    let mut json = serde_json::to_value(config()).unwrap();
    json["workload"]
        .as_object_mut()
        .unwrap()
        .remove("put_key_policy");
    let c: RunConfig = serde_json::from_value(json.clone()).unwrap();
    assert_eq!(c.workload.put_key_policy, PutKeyPolicy::Overwrite);
    assert!(c.validate().is_ok());
    for policy in ["overwrite", "new"] {
        json["workload"]["put_key_policy"] = serde_json::json!(policy);
        let c: RunConfig = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(
            serde_json::to_value(c).unwrap()["workload"]["put_key_policy"],
            policy
        );
    }
    json["workload"]["put_key_policy"] = serde_json::json!("insert");
    assert!(serde_json::from_value::<RunConfig>(json).is_err());
}

#[test]
fn new_put_keys_require_a_put_only_mix_without_measurement_sweep_or_interference() {
    for (put, put_batch) in [(1, 0), (0, 1), (1, 1)] {
        let mut c = config();
        c.workload.put_key_policy = PutKeyPolicy::New;
        c.workload.mix = OperationMix {
            put,
            put_batch,
            ..OperationMix::default()
        };
        assert!(c.validate().is_ok());
        for field in ["read", "get_many", "missing_read", "delete"] {
            let mut json = serde_json::to_value(&c).unwrap();
            json["workload"]["mix"][field] = serde_json::json!(1);
            let bad: RunConfig = serde_json::from_value(json).unwrap();
            assert!(
                bad.validate()
                    .unwrap_err()
                    .to_string()
                    .contains("put_key_policy")
            );
        }
        c.phases.measurement_key_sweep = true;
        assert!(
            c.validate()
                .unwrap_err()
                .to_string()
                .contains("measurement_key_sweep")
        );
    }
    let mut c = config();
    c.workload.put_key_policy = PutKeyPolicy::New;
    assert!(c.validate().is_err());
    c.workload.mix = OperationMix::default();
    assert!(c.validate().is_err());
    let mut c = interference_config();
    c.workload.put_key_policy = PutKeyPolicy::New;
    assert!(
        c.validate()
            .unwrap_err()
            .to_string()
            .contains("interference")
    );
}

#[test]
fn explicit_high_concurrency_capacity_preserves_existing_bounds() {
    for concurrency in [64, 256] {
        let mut c = config();
        c.execution.concurrency = concurrency;
        c.execution.queue_capacity = concurrency;
        assert!(c.validate().is_ok());
        c.execution.queue_capacity = concurrency - 1;
        assert!(c.validate().is_err());
        c.execution.queue_capacity = 65_537;
        assert!(c.validate().is_err());
        c.execution.queue_capacity = 65_536;
        assert!(c.validate().is_ok());
    }
}

fn interference_config() -> RunConfig {
    let mut c = config();
    c.case_id = Some("interference-test".into());
    c.execution.mode = ExecutionMode::Interference;
    c.execution.queue_capacity = 18;
    c.phases.warmup_operations = 0;
    c.phases.measurement_operations = 30_000;
    c.limits.max_operations = 40_000;
    c.limits.max_bytes = 20_000_000;
    c.interference = Some(InterferenceConfig {
        read_offered_per_second: 1000,
        read_queue_capacity: 4096,
        put_concurrency: 16,
        duration_seconds: 20,
        key_policy: InterferenceKeyPolicy::DisjointReadPut,
    });
    c
}

#[test]
fn interference_requires_explicit_consistent_bounded_settings() {
    let c = interference_config();
    assert!(c.validate().is_ok());
    let mut bad = c.clone();
    bad.interference = None;
    assert!(bad.validate().is_err());
    let mut bad = c.clone();
    bad.execution.mode = ExecutionMode::ClosedLoop;
    assert!(bad.validate().is_err());
    for field in [
        "read_offered_per_second",
        "read_queue_capacity",
        "put_concurrency",
        "duration_seconds",
    ] {
        let mut json = serde_json::to_value(&c).unwrap();
        json["interference"][field] = serde_json::json!(0);
        let bad: RunConfig = serde_json::from_value(json).unwrap();
        assert!(bad.validate().is_err(), "{field}");
    }
    for mutate in [
        |c: &mut RunConfig| c.execution.queue_capacity = 17,
        |c: &mut RunConfig| c.execution.queue_capacity = 65_537,
        |c: &mut RunConfig| c.interference.as_mut().unwrap().read_queue_capacity = 1,
        |c: &mut RunConfig| c.interference.as_mut().unwrap().read_queue_capacity = 65_537,
        |c: &mut RunConfig| c.interference.as_mut().unwrap().put_concurrency = usize::MAX,
        |c: &mut RunConfig| c.interference.as_mut().unwrap().read_offered_per_second = u64::MAX,
        |c: &mut RunConfig| c.interference.as_mut().unwrap().duration_seconds = 60,
        |c: &mut RunConfig| c.phases.measurement_operations = 20_000,
        |c: &mut RunConfig| c.limits.max_bytes = 25_600,
        |c: &mut RunConfig| c.phases.warmup_operations = 1,
        |c: &mut RunConfig| c.phases.preload = false,
        |c: &mut RunConfig| c.workload.mix.put = 1,
        |c: &mut RunConfig| c.execution.offered_ops_per_second = Some(1000.0),
        |c: &mut RunConfig| c.case_id = Some("../bad".into()),
    ] {
        let mut bad = c.clone();
        mutate(&mut bad);
        assert!(bad.validate().is_err());
    }
}

#[test]
fn interference_serde_rejects_missing_unknown_and_noninteger_fields() {
    let c = serde_json::to_value(interference_config()).unwrap();
    for field in [
        "read_offered_per_second",
        "read_queue_capacity",
        "put_concurrency",
        "duration_seconds",
        "key_policy",
    ] {
        let mut bad = c.clone();
        bad["interference"].as_object_mut().unwrap().remove(field);
        assert!(serde_json::from_value::<RunConfig>(bad).is_err());
    }
    for value in [
        serde_json::json!(true),
        serde_json::json!(1.5),
        serde_json::json!(-1),
    ] {
        let mut bad = c.clone();
        bad["interference"]["read_queue_capacity"] = value;
        assert!(serde_json::from_value::<RunConfig>(bad).is_err());
    }
    let mut bad = c.clone();
    bad["interference"]["extra"] = serde_json::json!(1);
    assert!(serde_json::from_value::<RunConfig>(bad).is_err());
    let mut bad = c;
    bad["interference"]["key_policy"] = serde_json::json!("shared");
    assert!(serde_json::from_value::<RunConfig>(bad).is_err());
}

#[test]
fn accepts_bounded_valid_configuration_and_counts_preload_operations() {
    assert!(config().validate().is_ok());
    let mut too_few = config();
    too_few.limits.max_operations = 102;
    assert!(too_few.validate().is_err());
}

#[test]
fn mapped_wal_eviction_is_opt_in_and_rejects_unsupported_backends() {
    let mut c = config();
    c.phases.evict_mapped_wal_before_measurement = true;
    c.backend.kind = "valkey_tcp".into();
    assert!(c.validate().is_err());
    c.backend.kind = "native".into();
    assert_eq!(
        c.validate().is_ok(),
        cfg!(all(target_os = "linux", feature = "test-utils"))
    );
    c.qualification_only = true;
    assert!(c.validate().is_err());
}

#[test]
fn warmup_sweep_rejects_partial_or_mutating_working_sets() {
    let mut config = config();
    config.phases.warmup_key_sweep = true;
    assert!(config.validate().is_err());
    config.phases.warmup_operations = config.dataset.keys;
    config.limits.max_operations = 1000;
    assert!(config.validate().is_ok());
    config.workload.mix.put = 1;
    assert!(config.validate().is_err());
    config.workload.mix.put = 0;
    config.phases.preload = false;
    assert!(config.validate().is_err());
    config.phases.preload = true;
    config.workload.mix.read = 0;
    config.workload.mix.get_many = 1;
    config.phases.warmup_operations = config.dataset.keys.div_ceil(32);
    assert!(config.validate().is_ok());
    config.phases.warmup_operations -= 1;
    assert!(config.validate().is_err());
}

#[test]
fn measurement_sweep_rejects_wrap_mutation_and_nonclosed_loop() {
    let mut config = config();
    config.phases.measurement_key_sweep = true;
    assert!(config.validate().is_ok());
    config.phases.measurement_operations = config.dataset.keys + 1;
    config.limits.max_operations = 1000;
    assert!(config.validate().is_err());
    config.phases.measurement_operations = 2;
    config.workload.mix.get_many = 1;
    config.workload.batch_size = 64;
    assert!(config.validate().is_err());
    config.workload.batch_size = 32;
    assert!(config.validate().is_ok());
    config.workload.mix.put = 1;
    assert!(config.validate().is_err());
    config.workload.mix.put = 0;
    config.phases.preload = false;
    assert!(config.validate().is_err());
    config.phases.preload = true;
    config.execution.mode = ExecutionMode::OpenLoop;
    assert!(config.validate().is_err());
}

#[test]
fn reused_key_namespace_requires_full_preload_and_exact_hex_identity() {
    let mut reused = config();
    reused.key_namespace_hex = Some("00112233445566778899aabbccddeeff".to_owned());
    assert!(reused.validate().is_ok());
    reused.phases.preload = false;
    assert!(reused.validate().is_err());
    reused.phases.preload = true;
    for malformed in [
        "0",
        "00112233445566778899aabbccddeeff00",
        "00112233445566778899AABBCCDDEEFF",
    ] {
        reused.key_namespace_hex = Some(malformed.to_owned());
        assert!(reused.validate().is_err());
    }
}

#[test]
fn rejects_unbounded_or_invalid_execution_shapes() {
    let mut invalid = config();
    invalid.execution.queue_capacity = 1;
    assert!(invalid.validate().is_err());
    invalid = config();
    invalid.execution.request_timeout_ms = 0;
    assert!(invalid.validate().is_err());
    invalid = config();
    invalid.dataset.entropy = f64::NAN;
    assert!(invalid.validate().is_err());
}

#[test]
fn open_loop_requires_finite_bounded_arrival_rate() {
    let mut config = config();
    config.execution.mode = ExecutionMode::OpenLoop;
    config.execution.offered_ops_per_second = Some(100.0);
    assert!(config.validate().is_ok());
    config.execution.offered_ops_per_second = Some(f64::INFINITY);
    assert!(config.validate().is_err());
}

#[test]
fn tidehunter_durability_tier_defaults_to_native_and_rejects_other_values() {
    let mut c = config();
    assert_eq!(c.durability_tier().unwrap(), None);
    c.backend.kind = "tidehunter".to_owned();
    c.backend.options = serde_json::json!({});
    assert_eq!(c.durability_tier().unwrap(), Some(DurabilityTier::Native));
    c.backend.options = serde_json::json!({"durability_tier": "synchronous"});
    assert_eq!(
        c.durability_tier().unwrap(),
        Some(DurabilityTier::Synchronous)
    );
    assert!(c.validate().is_ok());
    for bad in [serde_json::json!("sync"), serde_json::json!(1)] {
        c.backend.options = serde_json::json!({"durability_tier": bad});
        assert!(c.validate().is_err());
    }
    assert_ne!(
        DurabilityTier::Native.comparison_cohort(),
        DurabilityTier::Synchronous.comparison_cohort()
    );
}

#[test]
fn rejects_unknown_json_configuration_fields() {
    let decoded =
        serde_json::from_str::<BackendSpec>(r#"{"kind":"native","options":{},"secret":"x"}"#);
    assert!(decoded.is_err());
}
#[test]
fn admits_only_value_sizes_and_tidehunter_batch_limits_that_fit() {
    let mut config = config();
    for size in [64, 512, 4096, 16_384, 65_536] {
        config.dataset.value_size = size;
        config.limits.max_bytes = config.dataset.keys * size as u64;
        assert!(config.validate().is_ok(), "value size {size}");
    }
    config.dataset.value_size = 2048;
    config.limits.max_bytes = config.dataset.keys * 2048;
    assert!(config.validate().is_err());

    config.backend.kind = "tidehunter".to_owned();
    config.backend.options = serde_json::json!({});
    config.dataset.value_size = 8192;
    config.limits.max_bytes = config.dataset.keys * 8192;
    config.workload.mix.put_batch = 1;
    config.workload.batch_size = 32;
    assert!(config.validate().is_ok());

    config.dataset.value_size = 65_536;
    config.limits.max_bytes = config.dataset.keys * 65_536;
    config.workload.mix.put_batch = 0;
    assert!(config.validate().is_ok());
    config.workload.mix.put_batch = 1;
    config.workload.batch_size = 32;
    assert!(config.validate().is_err());
    config.workload.batch_size = 4;
    assert!(config.validate().is_ok());
    config.backend.options = serde_json::json!({
        "external_remote_config": {
            "storage": {
                "batch_limits": {
                    "max_operations": 3,
                    "max_encoded_bytes": 320 * 1024
                }
            }
        }
    });
    assert!(config.validate().is_err());
    config.workload.batch_size = 1;
    config.backend.options["external_remote_config"]["storage"]["batch_limits"]["max_operations"] =
        serde_json::json!(1);
    assert!(config.validate().is_err());
}

#[test]
fn read_profile_is_bounded_and_requires_distributed_tidehunter() {
    let mut config = config();
    assert_eq!(config.diagnostics.read_profile_limit, 0);
    config.diagnostics.read_profile_limit = 1;
    assert!(config.validate().is_err());
    config.backend.kind = "tidehunter".to_owned();
    assert!(config.validate().is_ok());
    config.diagnostics.read_profile_limit = 65_536;
    assert!(config.validate().is_ok());
    config.diagnostics.read_profile_limit = 65_537;
    assert!(config.validate().is_err());
    assert_eq!(
        serde_json::from_str::<DiagnosticsConfig>(r#"{"write_profile_limit":0}"#)
            .unwrap()
            .read_profile_limit,
        0
    );
}

#[test]
fn write_timeline_requires_tidehunter_and_is_bounded_independently() {
    let mut config = config();
    assert_eq!(config.diagnostics.write_timeline_limit, 0);
    assert_eq!(config.diagnostics.write_timeline_sample_every, 1);
    assert_eq!(
        serde_json::from_str::<DiagnosticsConfig>("{}")
            .unwrap()
            .write_timeline_sample_every,
        1
    );
    config.diagnostics.write_timeline_sample_every = 0;
    assert!(config.validate().is_err());
    config.diagnostics.write_timeline_sample_every = 1;
    config.diagnostics.write_timeline_limit = 1;
    assert!(config.validate().is_err());
    config.backend.kind = "tidehunter".to_owned();
    assert!(config.validate().is_ok());
    config.diagnostics.write_timeline_limit = 65_536;
    assert!(config.validate().is_ok());
    config.diagnostics.write_timeline_limit = 65_537;
    assert!(config.validate().is_err());
    config.diagnostics.write_timeline_limit = 1;
    config.diagnostics.write_profile_limit = 1;
    assert!(config.validate().is_err());
    assert_eq!(
        serde_json::from_str::<DiagnosticsConfig>("{}")
            .unwrap()
            .write_timeline_limit,
        0
    );
}

#[test]
fn valkey_profile_requires_the_matched_client_and_bounded_records() {
    let mut config = config();
    config.diagnostics.valkey_profile_limit = 1;
    assert!(config.validate().is_err());
    config.backend.kind = "valkey_tcp".to_owned();
    assert!(config.validate().is_err());
    config.backend.options = serde_json::json!({"client": "libvalkey"});
    assert!(config.validate().is_ok());
    config.diagnostics.valkey_profile_limit = 65_536;
    assert!(config.validate().is_ok());
    config.diagnostics.valkey_profile_limit = 65_537;
    assert!(config.validate().is_err());
    config.backend.kind = "valkey_rdma".to_owned();
    config.diagnostics.valkey_profile_limit = 1;
    assert!(config.validate().is_ok());
}

#[test]
fn validates_optional_measurement_gate_paths_and_timeout() {
    let mut config = config();
    config.measurement_gate = Some(MeasurementGateConfig {
        ready_file: PathBuf::from("/tmp/kv-ready"),
        release_file: PathBuf::from("/tmp/kv-release"),
        timeout_seconds: 60,
    });
    assert!(config.validate().is_ok());
    config.measurement_completion_gate = config.measurement_gate.clone();
    assert!(config.validate().is_err());
    config
        .measurement_completion_gate
        .as_mut()
        .unwrap()
        .ready_file = PathBuf::from("/tmp/kv-completion-ready");
    config
        .measurement_completion_gate
        .as_mut()
        .unwrap()
        .release_file = PathBuf::from("/tmp/kv-completion-release");
    assert!(config.validate().is_ok());
    for gate in [
        config.measurement_gate.as_mut().unwrap(),
        config.measurement_completion_gate.as_mut().unwrap(),
    ] {
        gate.timeout_seconds = 240;
    }
    assert!(config.validate().is_ok());
    for completion in [false, true] {
        for invalid in [0, 301] {
            let gate = if completion {
                config.measurement_completion_gate.as_mut().unwrap()
            } else {
                config.measurement_gate.as_mut().unwrap()
            };
            gate.timeout_seconds = invalid;
            assert!(config.validate().is_err());
        }
        if completion {
            config
                .measurement_completion_gate
                .as_mut()
                .unwrap()
                .timeout_seconds = 240;
        } else {
            config.measurement_gate.as_mut().unwrap().timeout_seconds = 240;
        }
    }
    config
        .measurement_completion_gate
        .as_mut()
        .unwrap()
        .release_file = PathBuf::from("relative");
    assert!(config.validate().is_err());
    config
        .measurement_completion_gate
        .as_mut()
        .unwrap()
        .release_file = PathBuf::from("/tmp/kv-completion-release");
    config.diagnostics.write_profile_limit = 65_537;
    assert!(config.validate().is_err());
}

#[test]
fn relocation_phase_requires_a_completion_gate() {
    let mut config = config();
    config.phases.relocation = true;
    assert!(config.validate().is_err());
    config.measurement_completion_gate = Some(MeasurementGateConfig {
        ready_file: PathBuf::from("/tmp/kv-completion-ready"),
        release_file: PathBuf::from("/tmp/kv-completion-release"),
        timeout_seconds: 240,
    });
    assert!(config.validate().is_ok());
}

#[test]
fn existing_dataset_verification_requires_read_only_owned_namespace() {
    let mut config = config();
    config.phases.preload = false;
    config.phases.verify_existing_dataset = true;
    assert!(config.validate().is_err());
    config.key_namespace_hex = Some("09090909090909090909090909090909".into());
    assert!(config.validate().is_ok());
    config.phases.preload = true;
    assert!(config.validate().is_err());
    config.phases.preload = false;
    config.phases.aging_operations = 1;
    assert!(config.validate().is_err());
    config.phases.aging_operations = 0;
    config.workload.mix.put = 1;
    assert!(config.validate().is_err());
    config.workload.mix.put = 0;
    config.limits.max_operations = 102;
    assert!(
        config.validate().is_err(),
        "existing-data checks consume the operation budget"
    );
}
