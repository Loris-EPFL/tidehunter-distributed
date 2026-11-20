use super::*;
use tidehunter::distributed::durability::DurabilityTier;

fn options_json(stage: &str) -> Value {
    json!({
        "path": "/cases/n1000000-e4-native-r0/db", "owners": 1,
        "transport": {"kind": "tcp-trusted"}, "coordinator_read_cache_bytes": 0,
        "durability_tier": "native",
        "replay_limits": {"max_batches": 4096, "max_encoded_bytes": 268435456},
        "index_fixture": {"stage": stage, "arm": "exact", "max_admitted_batches": 262144,
            "max_index_bytes": 536870912, "fragment_bytes": 4194304,
            "wal_file_bytes": 1073741824}
    })
}

fn run_config(stage: &str) -> RunConfig {
    let overwrite = stage == "overwrite";
    serde_json::from_value(json!({
        "schema_version": 1,
        "case_id": format!("n1000000-e4-native-r0-{stage}"),
        "backend": {"kind": "tidehunter", "options": options_json(stage)},
        "seed": 20261119,
        "key_namespace_hex": "a1a0a1a0a1a0a1a0a1a0a1a0a1a0a1a0",
        "dataset": {"keys": 1000000, "value_size": 1024, "entropy": 1.0},
        "workload": {"kind": "synthetic", "distribution": "uniform",
            "put_key_policy": if overwrite { "overwrite" } else { "new" },
            "mix": {"read": 0, "put": 100, "delete": 0, "missing_read": 0, "get_many": 0,
                "put_batch": 0}, "batch_size": 32},
        "execution": if overwrite {
            json!({"mode": "open_loop", "concurrency": 16, "client_shards": 1,
                "queue_capacity": 64, "request_timeout_ms": 30000,
                "offered_ops_per_second": 10000})
        } else {
            json!({"mode": "closed_loop", "concurrency": 16, "client_shards": 1,
                "queue_capacity": 64, "request_timeout_ms": 30000})
        },
        "phases": {"preload": false, "verify_existing_dataset": false, "warmup_operations": 0,
            "aging_operations": 0,
            "measurement_operations": if overwrite { 10000000 } else { 1000000 }},
        "qualification_only": false,
        "diagnostics": {"index_checkpoint": true, "write_profile_limit": 0,
            "read_profile_limit": 0, "write_timeline_limit": 0},
        "limits": {"max_keys": 10000000, "max_bytes": 21474836480_u64,
            "max_operations": 10000000, "deadline_seconds": 21600, "shutdown_grace_seconds": 300}
    }))
    .unwrap()
}

#[test]
fn generated_stage_configs_validate_and_stage_rules_hold() {
    for stage in ["build", "overwrite"] {
        let config = run_config(stage);
        config.validate().unwrap();
        crate::backends::validate_options("tidehunter", &config.backend.options).unwrap();
    }
    let mut short = run_config("build");
    short.phases.measurement_operations = 999_999;
    assert!(short.validate().is_err());
    let mut new_overwrite = run_config("overwrite");
    new_overwrite.workload.put_key_policy = crate::config::PutKeyPolicy::New;
    assert!(new_overwrite.validate().is_err());
    let mut preloaded = run_config("overwrite");
    preloaded.phases.preload = true;
    assert!(preloaded.validate().is_err());
    let mut unknown = run_config("build");
    unknown.backend.options["index_fixture"]["stage"] = json!("cold");
    assert!(unknown.validate().is_err());
}

#[test]
fn fixture_options_require_consistent_arm_and_geometry() {
    let parse = |value: Value| serde_json::from_value::<IndexFixtureOptions>(value).unwrap();
    let base = options_json("build")["index_fixture"].clone();
    parse(base.clone()).validate().unwrap();
    let mut exact_with_cell = base.clone();
    exact_with_cell["per_cell"] = json!({"dirty_keys": 32, "snapshot_written_bytes": 134217728, "stale_wal_bytes": 268435456});
    assert!(parse(exact_with_cell.clone()).validate().is_err());
    exact_with_cell["arm"] = json!("per_cell");
    parse(exact_with_cell).validate().unwrap();
    let mut cell_without = base.clone();
    cell_without["arm"] = json!("per_cell");
    assert!(parse(cell_without).validate().is_err());
    let mut small = base.clone();
    small["fragment_bytes"] = json!(2097152);
    assert!(parse(small).validate().is_err());
    let mut uneven = base.clone();
    uneven["wal_file_bytes"] = json!(4194304 * 3 + 4096);
    assert!(parse(uneven).validate().is_err());
    let mut renamed = base;
    renamed["write_mode"] = json!("leased");
    assert!(serde_json::from_value::<IndexFixtureOptions>(renamed).is_err());
}

#[test]
fn route_admits_only_one_trusted_owner_without_overrides() {
    let options = super::super::parse_options(&options_json("build")).unwrap();
    assert_eq!(validate_route(&options).unwrap().max_batches, 4096);
    for (key, value) in [
        ("owners", json!(2)),
        ("transport", json!({"kind": "tcp"})),
        ("coordinator_read_cache_bytes", json!(1024)),
        ("path", json!("relative/db")),
    ] {
        let mut document = options_json("build");
        document[key] = value;
        if let Ok(options) = super::super::parse_options(&document) {
            assert!(validate_route(&options).is_err(), "{key}");
        }
    }
    let mut document = options_json("build");
    document.as_object_mut().unwrap().remove("replay_limits");
    let options = super::super::parse_options(&document).unwrap();
    assert!(validate_route(&options).is_err());
}

#[test]
fn receipt_binds_dataset_shape_geometry_tier_and_path() {
    let config = run_config("overwrite");
    let mut options = super::super::parse_options(&config.backend.options).unwrap();
    let fixture = options.index_fixture.take().unwrap();
    let replay_limits = validate_route(&options).unwrap();
    let paths = CasePaths::new(&options, fixture.owner_root.as_deref());
    let dataset = DatasetIdentity::from_config(&config).unwrap();
    assert_eq!(
        dataset.namespace().unwrap().to_vec(),
        [0xa1_u8, 0xa0].repeat(8)
    );
    let shape_yaml =
        serde_yaml::to_string(&KeyShape::new_single(32, 16, KeyType::uniform(64))).unwrap();
    let receipt = BuildReceipt {
        schema_version: 1,
        path: paths.root.clone(),
        dataset: dataset.clone(),
        shape_yaml: shape_yaml.clone(),
        fixture: IndexFixtureOptions {
            stage: IndexFixtureStage::Build,
            ..fixture.clone()
        },
        replay_limits,
        spec: fixture.geometry(replay_limits).spec(
            paths.fixture.clone(),
            DurabilityTier::Native,
            fresh_owners(1).unwrap(),
        ),
        build_case_id: None,
        verification: Value::Null,
    };
    let check = |dataset: &DatasetIdentity, fixture: &IndexFixtureOptions, tier| {
        receipt.check(&paths, dataset, &shape_yaml, fixture, replay_limits, tier)
    };
    check(&dataset, &fixture, DurabilityTier::Native).unwrap();
    let mut seed = dataset.clone();
    seed.seed += 1;
    assert!(check(&seed, &fixture, DurabilityTier::Native).is_err());
    assert!(check(&dataset, &fixture, DurabilityTier::Synchronous).is_err());
    let mut e16 = fixture.clone();
    e16.max_admitted_batches = 16_384;
    assert!(check(&dataset, &e16, DurabilityTier::Native).is_err());
    let other_limits = ReplayLimits {
        max_batches: 16_384,
        max_encoded_bytes: 1 << 30,
    };
    assert!(
        receipt
            .check(
                &paths,
                &dataset,
                &shape_yaml,
                &fixture,
                other_limits,
                DurabilityTier::Native
            )
            .is_err()
    );
}

#[test]
fn default_owner_root_is_unchanged_and_absent_from_serialized_options() {
    let config = run_config("build");
    let mut options = super::super::parse_options(&config.backend.options).unwrap();
    let fixture = options.index_fixture.take().unwrap();
    assert_eq!(fixture.owner_root, None);
    let paths = CasePaths::new(&options, fixture.owner_root.as_deref());
    assert_eq!(
        paths.fixture,
        PathBuf::from("/cases/n1000000-e4-native-r0/db/fixture")
    );
    // Receipts and effective configurations keep their shape without the option.
    assert!(
        serde_json::to_value(&fixture)
            .unwrap()
            .get("owner_root")
            .is_none()
    );
}

#[test]
fn owner_root_option_moves_only_the_owner_tree_and_binds_the_case() {
    let mut document = options_json("build");
    document["index_fixture"]["owner_root"] = json!("/dev/shm/a1/owner");
    let mut options = super::super::parse_options(&document).unwrap();
    let fixture = options.index_fixture.take().unwrap();
    fixture.validate().unwrap();
    let paths = CasePaths::new(&options, fixture.owner_root.as_deref());
    assert_eq!(paths.fixture, PathBuf::from("/dev/shm/a1/owner"));
    assert_eq!(
        paths.coordinator,
        PathBuf::from("/cases/n1000000-e4-native-r0/db/coordinator")
    );
    assert_eq!(
        paths.evidence,
        PathBuf::from("/cases/n1000000-e4-native-r0/db/evidence")
    );
    // The owner root is part of the case identity a receipt binds.
    let mut default_root = fixture.clone();
    default_root.owner_root = None;
    assert!(!fixture.same_case(&default_root));
    // A relative owner root is refused.
    let mut relative = document["index_fixture"].clone();
    relative["owner_root"] = json!("relative/owner");
    assert!(
        serde_json::from_value::<IndexFixtureOptions>(relative)
            .unwrap()
            .validate()
            .is_err()
    );
}

#[test]
fn cold_reopen_refuses_an_external_owner_root() {
    let mut document = options_json("overwrite");
    let mut options = super::super::parse_options(&document).unwrap();
    options
        .index_fixture
        .take()
        .unwrap()
        .require_case_owner_root()
        .unwrap();
    document["index_fixture"]["owner_root"] = json!("/dev/shm/a1/owner");
    let mut options = super::super::parse_options(&document).unwrap();
    let error = options
        .index_fixture
        .take()
        .unwrap()
        .require_case_owner_root()
        .unwrap_err();
    assert!(error.to_string().contains("owner_root"));
}
