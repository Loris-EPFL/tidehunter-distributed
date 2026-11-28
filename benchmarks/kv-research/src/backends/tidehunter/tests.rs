use super::*;

/// A refused preparation must fail for its own reason: a deleted key left in
/// a test document would make `deny_unknown_fields` refuse it instead.
fn refusal(input: &Path, output: &Path) -> String {
    let error = prepare_external_case_file(input, output)
        .unwrap_err()
        .to_string();
    assert!(
        !error.contains("unknown field"),
        "refused for a stale key, not for its reason: {error}"
    );
    error
}

/// Owner storage samples run only at drained boundaries: inside the
/// measurement window the gate refuses without calling the owner.
#[test]
fn owner_sample_never_runs_inside_the_measurement_window() {
    let gate = OwnerSampleGate::default();
    assert_eq!(gate.sample(|| 1), Ok(1));
    gate.set_measuring(true);
    let mut called = false;
    assert!(gate.sample(|| called = true).is_err());
    assert!(!called);
    gate.set_measuring(false);
    assert_eq!(gate.sample(|| 2), Ok(2));
}

#[test]
fn largest_research_batch_fits_native_group_and_fragment() {
    use tidehunter::distributed::{
        codec::{BatchLimits, CompleteBatch, Mutation},
        group::{CompleteGroup, GroupLimits},
        types::{BatchId, BatchVersion},
    };
    let shape = KeyShape::new_single(32, 16, KeyType::uniform(64));
    let keyspace = shape.iter_ks().next().unwrap().id();
    let batch_limits = BatchLimits {
        max_operations: 32,
        max_encoded_bytes: RESEARCH_BATCH_BYTES,
    };
    let group_limits = GroupLimits {
        max_batches: 32,
        max_encoded_bytes: RESEARCH_FRAGMENT_BYTES - 8,
    };
    let mutations = (0..32)
        .map(|id| Mutation::Put {
            keyspace,
            key: crate::workload::key_bytes(&[1; 16], 5, id),
            value: crate::workload::value_bytes(&[1; 16], 5, id, 65_536, 1.0),
        })
        .collect();
    let batch = CompleteBatch::new(
        &shape,
        BatchId {
            database: [1; 16],
            operation: [2; 16],
        },
        BatchVersion {
            epoch: 1,
            sequence: 1,
        },
        mutations,
        batch_limits,
    )
    .unwrap();
    assert!(batch.encoded().len() > 2 * 1024 * 1024);
    CompleteGroup::new(&shape, vec![batch], batch_limits, group_limits).unwrap();
    assert_eq!((1024 * 1024 * 1024) % RESEARCH_FRAGMENT_BYTES, 0);
}

#[test]
fn external_live_options_default_inline_and_reject_unknown_fields() {
    let mut input = serde_json::json!({
        "addresses": ["10.1.0.1"], "transport": "rdma", "service": 4420, "port_id": 4420,
        "max_inflight_requests": 8, "max_inflight_bytes": 8388608,
        "connect_timeout_ms": 10000, "io_timeout_ms": 10000
    });
    let parsed: ExternalLiveNvmeof = serde_json::from_value(input.clone()).unwrap();
    assert!(parsed.inline_reads);
    assert_eq!(parsed.max_databases, 1);
    assert_eq!(
        parsed.leased_sync,
        tidehunter::distributed::tcp::LeasedSync::GroupFlush
    );
    assert_eq!(parsed.leased_flush_min_frame_bytes, 65536);
    let mut removed = input.clone();
    removed["export_mode"] = serde_json::json!("per_owner");
    assert!(serde_json::from_value::<ExternalLiveNvmeof>(removed).is_err());
    assert_eq!(
        serde_json::to_value(parsed.queue_geometry).unwrap(),
        serde_json::json!({"io_queues": 4, "queue_size": 128, "poll_queues": 0})
    );
    input["leased_flush_min_frame_bytes"] = serde_json::json!(0);
    assert_eq!(
        serde_json::from_value::<ExternalLiveNvmeof>(input.clone())
            .unwrap()
            .leased_flush_min_frame_bytes,
        0
    );
    input["inline_reads"] = serde_json::json!(false);
    assert!(
        !serde_json::from_value::<ExternalLiveNvmeof>(input.clone())
            .unwrap()
            .inline_reads
    );
    input["leased_durability"] = serde_json::json!("native");
    assert!(serde_json::from_value::<ExternalLiveNvmeof>(input).is_err());
}

#[test]
fn owner_wal_geometry_requires_both_layout_dimensions() {
    for (fragment_bytes, wal_file_bytes, expected) in [
        (
            134217728,
            10737418240,
            Some(OwnerWalGeometry::NativeDefault),
        ),
        (4194304, 1073741824, Some(OwnerWalGeometry::Research)),
        (16777216, 268435456, Some(OwnerWalGeometry::SuiHarness)),
        (4194304, 10737418240, None),
    ] {
        assert_eq!(
            OwnerWalGeometry::from_layout(fragment_bytes, wal_file_bytes),
            expected
        );
    }
    assert_eq!(
        serde_json::to_value(OwnerWalGeometry::from_layout(
            NATIVE_FRAGMENT_BYTES,
            RESEARCH_WAL_FILE_BYTES,
        ))
        .unwrap(),
        serde_json::Value::Null
    );
    assert_eq!(OwnerWalGeometry::from_layout(4096, 8192), None);
}

#[test]
fn external_preparation_launches_leased_owners_and_rejects_removed_keys() {
    let guard = tempfile::TempDir::with_prefix("kv-history-").expect("test tree");
    let root = guard.path().to_path_buf();
    let input = root.join("input.json");
    let config = serde_json::json!({
        "guest_root": root.join("guest"), "addresses": ["127.0.0.1:44000"],
        "transport": {"kind": "tcp"}, "max_batches_per_partition": 16384,
        "max_admitted_batches": 16384,
        "coordinator_cache_bytes": 0,
        "owner_wal_member_read": "async", "polling": "adaptive",
        "live_nvmeof": test_live_nvmeof(1)
    });
    fs::write(&input, serde_json::to_vec(&config).unwrap()).unwrap();
    let output = root.join("default");
    prepare_external_case_file(&input, &output).unwrap();
    let launch: serde_yaml::Value =
        serde_yaml::from_slice(&fs::read(output.join("node-0/launch.yaml")).unwrap()).unwrap();
    assert!(launch.get("history_persistence").is_none());
    assert!(launch.get("drop_first_append_response").is_none());
    assert!(launch.get("node").is_none());
    assert!(launch["host"]["live_target_pool"].is_mapping());
    assert_eq!(launch["host"]["max_databases"].as_u64(), Some(1));
    assert!(!output.join("external_owner_read_config.json").exists());
    let remote: serde_json::Value =
        serde_json::from_slice(&fs::read(output.join("remote.json")).unwrap()).unwrap();
    assert!(remote["live_nvme"].is_object());
    assert!(remote["storage"].get("database").is_none());
    // Removed inputs (history, owner-RPC reads) are rejected; live_nvmeof is required.
    for (index, key) in [
        "history_persistence",
        "max_index_bytes",
        "owner_read_workers",
        "owner_read_resident_first",
        "owner_read_memory_allowance_bytes",
    ]
    .into_iter()
    .enumerate()
    {
        let mut removed = config.clone();
        removed[key] = serde_json::json!(1);
        fs::write(&input, serde_json::to_vec(&removed).unwrap()).unwrap();
        let rejected = root.join(format!("rejected-{index}"));
        assert!(prepare_external_case_file(&input, &rejected).is_err());
        assert!(!rejected.exists());
    }
    let mut no_live = config;
    no_live.as_object_mut().unwrap().remove("live_nvmeof");
    fs::write(&input, serde_json::to_vec(&no_live).unwrap()).unwrap();
    let rejected = root.join("rejected-no-live");
    refusal(&input, &rejected);
    assert!(!rejected.exists());
}

#[test]
fn backends_require_external_remote_config() {
    let error = validate_options(&serde_json::json!({
        "transport": {"kind": "tcp"}, "path": "/unused"
    }))
    .unwrap_err();
    assert!(error.to_string().contains("require external_remote_config"));
}

#[test]
fn external_preparation_selects_owner_wal_geometry() {
    let guard = tempfile::TempDir::with_prefix("kv-external-wal-geometry-").expect("test tree");
    let root = guard.path().to_path_buf();
    let input = root.join("input.json");
    let mut config = serde_json::json!({
        "guest_root": root.join("guest"),
        "addresses": ["127.0.0.1:44000", "127.0.0.1:44001"],
        "transport": {"kind": "tcp"},
        "max_batches_per_partition": 16384, "max_admitted_batches": 16384,
        "coordinator_cache_bytes": 0,
        "owner_wal_member_read": "async",
        "polling": "adaptive",
        "live_nvmeof": test_live_nvmeof(2)
    });
    for (geometry, override_bytes, fragment_bytes, wal_file_bytes) in [
        (None, None, 134217728_u64, 10737418240_u64),
        (
            Some("native_default"),
            Some(268435456_u64),
            134217728,
            268435456,
        ),
        (Some("research"), None, 4194304, 1073741824),
        (Some("sui_harness"), None, 16777216, 268435456),
    ] {
        if let Some(geometry) = geometry {
            config["owner_wal_geometry"] = serde_json::json!(geometry);
        }
        config["owner_wal_file_bytes"] = serde_json::json!(override_bytes);
        fs::write(&input, serde_json::to_vec(&config).unwrap()).unwrap();
        let output = root.join(format!(
            "{}-{wal_file_bytes}",
            geometry.unwrap_or("native_default")
        ));
        prepare_external_case_file(&input, &output).unwrap();
        let remote: RemoteDatabaseConfig =
            serde_json::from_slice(&fs::read(output.join("remote.json")).unwrap()).unwrap();
        assert_eq!(remote.storage.fragment_bytes, fragment_bytes);
        assert_eq!(remote.storage.wal_file_bytes, wal_file_bytes);
        if geometry == Some("research") {
            assert_eq!(
                OwnerWalGeometry::from_layout(
                    remote.storage.fragment_bytes,
                    remote.storage.wal_file_bytes,
                ),
                Some(OwnerWalGeometry::Research)
            );
        }
        for partition in 0..2 {
            let launch: serde_yaml::Value = serde_yaml::from_slice(
                &fs::read(output.join(format!("node-{partition}/launch.yaml"))).unwrap(),
            )
            .unwrap();
            assert!(launch.get("node").is_none());
            assert_eq!(launch["partition"].as_u64(), Some(partition));
            for (field, name) in [
                ("ca", "ca.der".to_owned()),
                ("cert", format!("tidehunter-node-{partition}.der")),
                ("key", format!("tidehunter-node-{partition}.pk8")),
            ] {
                assert_eq!(
                    launch["tls"][field].as_str(),
                    root.join(format!("guest/node-{partition}/tls/{name}"))
                        .to_str()
                );
                assert!(
                    output
                        .join(format!("node-{partition}/tls/{name}"))
                        .is_file()
                );
                if field != "ca" {
                    assert!(!output.join("tls").join(&name).exists());
                }
            }
        }
    }
}

#[test]
fn external_preparation_rejects_invalid_owner_file_sizes() {
    let guard = tempfile::TempDir::with_prefix("kv-invalid-wal-").expect("test tree");
    let root = guard.path().to_path_buf();
    let input = root.join("input.json");
    let mut config = controller_live_preparation_input();
    config["transport"] = serde_json::json!({"kind": "tcp"});
    config.as_object_mut().unwrap().remove("owner_transports");
    for bytes in [0, 134217728, 268435457, 10871635968_u64] {
        config["owner_wal_file_bytes"] = serde_json::json!(bytes);
        fs::write(&input, serde_json::to_vec(&config).unwrap()).unwrap();
        refusal(&input, &root.join("output"));
        assert!(!root.join("output").exists());
    }
}

// Run-12 G18W-r0-219b8b655801a7844229 input with current write settings.
fn controller_live_preparation_input() -> serde_json::Value {
    serde_json::json!({
        "addresses": ["10.128.15.228:9400"],
        "coordinator_cache_bytes": 0, "durability_tier": "native",
        "guest_root": "/mnt/tidehunter-kv-comparison/bench-b2eec4ef24bc4f90/cells/G18W-r0-219b8b655801a7844229",
        "live_nvmeof": {
            "addresses": ["10.20.15.197"],
            "connect_timeout_ms": 30000,
            "inline_reads": true, "io_timeout_ms": 30000,
            "max_inflight_bytes": 16777216, "max_inflight_requests": 32,
            "port_id": 1000,
            "queue_geometry": {"io_queues": 1, "poll_queues": 0, "queue_size": 32},
            "service": 4420, "transport": "rdma",
            "lease_ahead": true, "polled_writes": false
        },
        "max_admitted_batches": 1000000, "max_batches_per_partition": 16384,
        "owner_transports": [{
            "fabric": {"backend": "verbs", "source_address": "10.20.15.197"},
            "kind": "ofi", "security": "trusted"
        }],
        "owner_wal_geometry": "native_default", "owner_wal_member_read": "async",
        "polling": "adaptive",
        "transport": {
            "fabric": {"backend": "verbs", "source_address": "10.20.15.196"},
            "kind": "ofi", "security": "trusted"
        }
    })
}

#[test]
fn external_live_input_accepts_explicit_write_settings() {
    let mut value = controller_live_preparation_input();
    value["live_nvmeof"]["lease_ahead"] = serde_json::json!(false);
    value["live_nvmeof"]["polled_writes"] = serde_json::json!(true);
    value["live_nvmeof"]["leased_write_window"] = serde_json::json!(8);
    value["live_nvmeof"]["leased_sync"] = serde_json::json!("per_frame_fua");
    value["live_nvmeof"]["leased_flush_min_frame_bytes"] = serde_json::json!(8192);
    let input: ExternalPreparationInput = serde_json::from_value(value).unwrap();
    let live = input.live_nvmeof;
    assert!(!live.lease_ahead);
    assert!(live.polled_writes);
    assert_eq!(live.leased_write_window, Some(8));
    assert_eq!(
        live.leased_sync,
        tidehunter::distributed::tcp::LeasedSync::PerFrameFua
    );
    assert_eq!(live.leased_flush_min_frame_bytes, 8192);
}

#[test]
fn external_live_input_defaults_write_settings_when_absent() {
    let mut value = controller_live_preparation_input();
    let live = value["live_nvmeof"].as_object_mut().unwrap();
    live.remove("lease_ahead");
    live.remove("polled_writes");
    let input: ExternalPreparationInput = serde_json::from_value(value).unwrap();
    let live = input.live_nvmeof;
    assert!(live.lease_ahead);
    assert!(!live.polled_writes);
    assert_eq!(live.leased_write_window, None);
    assert_eq!(
        live.leased_sync,
        tidehunter::distributed::tcp::LeasedSync::GroupFlush
    );
    assert_eq!(live.leased_flush_min_frame_bytes, 65536);
}

#[cfg(feature = "fabric")]
#[test]
fn controller_live_input_preserves_write_settings_and_owner_documents() {
    let guard = tempfile::TempDir::with_prefix("kv-controller-live-settings-").expect("test tree");
    let root = guard.path().to_path_buf();
    let input = root.join("input.json");
    for (name, ahead, polled) in [("controller", true, false), ("polled", false, true)] {
        let mut value = controller_live_preparation_input();
        value["live_nvmeof"]["lease_ahead"] = serde_json::json!(ahead);
        value["live_nvmeof"]["polled_writes"] = serde_json::json!(polled);
        value["live_nvmeof"]["leased_flush_min_frame_bytes"] = serde_json::json!(8192);
        value["live_nvmeof"]["queue_geometry"]["poll_queues"] = serde_json::json!(u8::from(polled));
        fs::write(&input, serde_json::to_vec(&value).unwrap()).unwrap();
        let output = root.join(name);
        prepare_external_case_file(&input, &output).unwrap();
        let remote: serde_json::Value =
            serde_json::from_slice(&fs::read(output.join("remote.json")).unwrap()).unwrap();
        assert!(remote.get("history_persistence").is_none());
        assert_eq!(remote["live_nvme"]["lease_ahead"], ahead);
        assert_eq!(remote["live_nvme"]["polled_writes"], polled);
        assert_eq!(remote["live_nvme"]["leased_flush_min_frame_bytes"], 8192);
        assert_eq!(
            remote["live_nvme"]["queue_geometry"],
            value["live_nvmeof"]["queue_geometry"]
        );
        let launch: serde_yaml::Value =
            serde_yaml::from_slice(&fs::read(output.join("node-0/launch.yaml")).unwrap()).unwrap();
        assert!(launch.get("history_persistence").is_none());
        assert!(launch.get("node").is_none());
        let pool = &launch["host"]["live_target_pool"];
        assert!(pool.is_mapping());
        assert!(pool.get("export_mode").is_none());
        assert!(pool.get("host_nqn").is_none());
        assert_eq!(remote["storage"]["fragment_bytes"], 134217728);
    }
}

#[test]
fn external_coordinator_cache_requires_explicit_adequate_allowance() {
    let guard = tempfile::TempDir::with_prefix("kv-external-cache-budget-").expect("test tree");
    let root = guard.path().to_path_buf();
    let input = root.join("input.json");
    let mut config = serde_json::json!({
        "guest_root": root.join("guest"),
        "addresses": (0..5).map(|i| format!("127.0.0.1:{}", 44000 + i)).collect::<Vec<_>>(),
        "transport": {"kind": "tcp"},
        "owner_wal_geometry": "research",
        "max_batches_per_partition": 16384, "max_admitted_batches": 16384,
        "coordinator_cache_bytes": 1024_u64 * 1024 * 1024,
        "owner_wal_member_read": "async",
        "polling": "adaptive",
        "live_nvmeof": test_live_nvmeof(5)
    });
    let write_input = |value: &serde_json::Value| {
        fs::write(&input, serde_json::to_vec(value).unwrap()).unwrap();
    };
    let default_input: ExternalPreparationInput = serde_json::from_value(config.clone()).unwrap();
    assert_eq!(
        default_input.coordinator_read_memory_allowance_bytes,
        512 * 1024 * 1024
    );
    write_input(&config);
    refusal(&input, &root.join("default-large"));
    config["coordinator_read_memory_allowance_bytes"] = serde_json::json!(1024_u64 * 1024 * 1024);
    write_input(&config);
    refusal(&input, &root.join("insufficient"));
    config["coordinator_cache_bytes"] = serde_json::json!(usize::MAX);
    config["coordinator_read_memory_allowance_bytes"] = serde_json::json!(usize::MAX);
    write_input(&config);
    refusal(&input, &root.join("overflow"));
    config["coordinator_cache_bytes"] = serde_json::json!(1024_u64 * 1024 * 1024);
    let allowance = 1024_u64 * 1024 * 1024 + 64 * 1024 * 1024;
    config["coordinator_read_memory_allowance_bytes"] = serde_json::json!(allowance);
    write_input(&config);
    let output = root.join("explicit");
    prepare_external_case_file(&input, &output).unwrap();
    let remote: serde_json::Value =
        serde_json::from_slice(&fs::read(output.join("remote.json")).unwrap()).unwrap();
    assert_eq!(remote["read_cache_bytes"], 1024_u64 * 1024 * 1024);
    assert_eq!(remote["read_memory_allowance_bytes"], allowance);
    {
        config["live_nvmeof"] = serde_json::json!({
            "addresses": ["10.1.0.1", "10.1.0.2", "10.1.0.3", "10.1.0.4", "10.1.0.5"],
            "transport": "rdma", "service": 4420, "port_id": 4420,
            "max_inflight_requests": 8, "max_inflight_bytes": 8388608,
            "connect_timeout_ms": 10000, "io_timeout_ms": 10000
        });
        write_input(&config);
        let output = root.join("live");
        prepare_external_case_file(&input, &output).unwrap();
        let remote: serde_json::Value =
            serde_json::from_slice(&fs::read(output.join("remote.json")).unwrap()).unwrap();
        assert_eq!(remote["live_nvme"]["leased_durability"], "native");
        assert_eq!(remote["pin_read_lanes"], false);
        assert!(remote.get("read_lanes").is_none());
        assert_eq!(remote["live_nvme"]["inline_reads"], true);
        assert_eq!(remote["live_nvme"]["lease_ahead"], true);
        assert_eq!(remote["live_nvme"]["polled_writes"], false);
        assert_eq!(remote["live_nvme"]["leased_sync"], "group_flush");
        assert_eq!(remote["live_nvme"]["leased_flush_min_frame_bytes"], 65536);
        assert_eq!(
            remote["live_nvme"]["queue_geometry"],
            serde_json::json!({"io_queues": 4, "queue_size": 128, "poll_queues": 0})
        );
        let host_nqn = remote["live_nvme"]["host_nqn"].as_str().unwrap();
        assert!(host_nqn.starts_with("nqn.2014-08.org.nvmexpress:uuid:"));
        for partition in 0..5 {
            let launch: serde_yaml::Value = serde_yaml::from_slice(
                &fs::read(output.join(format!("node-{partition}/launch.yaml"))).unwrap(),
            )
            .unwrap();
            let owner: tidehunter::distributed::tcp::OwnerHostConfig =
                serde_yaml::from_value(launch["host"].clone()).unwrap();
            assert_eq!(owner.max_databases, 1);
            let target = &launch["host"]["live_target_pool"];
            assert_eq!(
                target["address"].as_str().unwrap(),
                format!("10.1.0.{}", partition + 1)
            );
            assert_eq!(target["transport"].as_str().unwrap(), "rdma");
            assert!(target.get("host_nqn").is_none());
            assert_eq!(
                target["commissioning_service_base"].as_u64(),
                Some(4421 + 2 * partition)
            );
        }
        for count in [1, 2, 4, 16] {
            config["addresses"] = serde_json::json!(
                (0..count)
                    .map(|owner| format!("10.1.0.1:{}", 9400 + owner))
                    .collect::<Vec<_>>()
            );
            config["live_nvmeof"]["addresses"] = serde_json::json!(vec!["10.1.0.1"; count]);
            write_input(&config);
            let colocated = root.join(format!("colocated-{count}"));
            prepare_external_case_file(&input, &colocated).unwrap();
            let remote: serde_json::Value =
                serde_json::from_slice(&fs::read(colocated.join("remote.json")).unwrap()).unwrap();
            let identities = remote["live_nvme"]["owner_identities"].as_object().unwrap();
            assert_eq!(identities.len(), count);
            let mut nqns = HashSet::new();
            for owner in 0..count {
                let launch: serde_yaml::Value = serde_yaml::from_slice(
                    &fs::read(colocated.join(format!("node-{owner}/launch.yaml"))).unwrap(),
                )
                .unwrap();
                let target = &launch["host"]["live_target_pool"];
                let identity = &identities[&owner.to_string()];
                assert!(nqns.insert(identity["host_nqn"].as_str().unwrap()));
                assert_eq!(
                    target["public_service_base"].as_u64(),
                    Some(4420 + 2 * owner as u64)
                );
                assert_eq!(
                    target["public_port_base"].as_u64(),
                    Some(4420 + 2 * owner as u64)
                );
                assert_eq!(
                    launch["host"]["root"].as_str().unwrap(),
                    root.join(format!("guest/node-{owner}/storage"))
                        .to_str()
                        .unwrap()
                );
            }
        }
        config["addresses"] = serde_json::json!(
            (1..=5)
                .map(|owner| format!("10.1.0.{owner}:9400"))
                .collect::<Vec<_>>()
        );
        config["live_nvmeof"]["addresses"] =
            serde_json::json!(["10.1.0.1", "10.1.0.2", "10.1.0.3", "10.1.0.4", "10.1.0.5"]);
        config["live_nvmeof"]["queue_geometry"] =
            serde_json::json!({"io_queues": 4, "queue_size": 32, "poll_queues": 0});
        config["pin_read_lanes"] = serde_json::json!(true);
        config["read_lanes"] = serde_json::json!(4);
        for tier in ["native", "synchronous"] {
            for inline_reads in [true, false] {
                config["live_nvmeof"]["inline_reads"] = serde_json::json!(inline_reads);
                config["durability_tier"] = serde_json::json!(tier);
                write_input(&config);
                let output = root.join(format!("live-{tier}-{inline_reads}"));
                prepare_external_case_file(&input, &output).unwrap();
                let remote: serde_json::Value =
                    serde_json::from_slice(&fs::read(output.join("remote.json")).unwrap()).unwrap();
                assert_eq!(remote["live_nvme"]["leased_durability"], tier);
                assert_eq!(remote["pin_read_lanes"], true);
                assert_eq!(remote["read_lanes"], 4);
                assert_eq!(remote["live_nvme"]["inline_reads"], inline_reads);
                assert_eq!(
                    remote["live_nvme"]["queue_geometry"],
                    config["live_nvmeof"]["queue_geometry"]
                );
                let client: tidehunter::distributed::tcp::LiveClientConfig =
                    serde_json::from_value(remote["live_nvme"].clone()).unwrap();
                assert_eq!(client.queue_geometry.io_queues, 4);
                assert_eq!(client.queue_geometry.queue_size, 32);
                assert_eq!(client.queue_geometry.poll_queues, 0);
            }
        }
        config["addresses"][1] = config["addresses"][0].clone();
        write_input(&config);
        let invalid = root.join("live-duplicate");
        refusal(&input, &invalid);
        assert!(!invalid.exists());
        config["addresses"] = serde_json::json!(["127.0.0.1:44000"]);
        config["live_nvmeof"]["addresses"] = serde_json::json!(["10.1.0.1"]);
        config["live_nvmeof"]["max_inflight_bytes"] = serde_json::json!(32 * 1024);
        write_input(&config);
        let undersized = root.join("live-undersized");
        refusal(&input, &undersized);
        assert!(!undersized.exists());
        let required =
            (2 * (RESEARCH_BATCH_BYTES + 8) + 3 * 4095).max(RESEARCH_FRAGMENT_BYTES + 4095);
        config["live_nvmeof"]["max_inflight_bytes"] = serde_json::json!(required - 1);
        write_input(&config);
        let below_boundary = root.join("live-below-boundary");
        refusal(&input, &below_boundary);
        assert!(!below_boundary.exists());
        config["live_nvmeof"]["max_inflight_bytes"] = serde_json::json!(required);
        write_input(&config);
        let one = root.join("one-owner");
        prepare_external_case_file(&input, &one).unwrap();
        let remote: tidehunter::distributed::remote_db::RemoteDatabaseConfig =
            serde_json::from_slice(&fs::read(one.join("remote.json")).unwrap()).unwrap();
        assert_eq!(remote.endpoints.len(), 1);
        assert!(remote.validate().is_ok());
        assert_eq!(
            remote.storage.batch_limits.max_encoded_bytes,
            RESEARCH_BATCH_BYTES
        );
        assert!(one.join("node-0/launch.yaml").exists());
        assert!(!one.join("node-1").exists());
    }
}

#[test]
fn rejects_authority_limit_above_engine_bound_before_emitting_launch_files() {
    let guard = tempfile::TempDir::with_prefix("kv-external-limit-").expect("test tree");
    let root = guard.path().to_path_buf();
    let input = root.join("input.json");
    let output = root.join("prepared");
    let config = serde_json::json!({
        "guest_root": root.join("guest"),
        "addresses": (0..5).map(|i| format!("127.0.0.1:{}", 44000 + i)).collect::<Vec<_>>(),
        "transport": {"kind":"tcp"},
        "max_batches_per_partition": 16384,
        "max_admitted_batches": 1_048_576,
        "coordinator_cache_bytes": 0,
        "owner_wal_member_read": "async",
        "polling": "adaptive",
        "live_nvmeof": test_live_nvmeof(5)
    });
    fs::write(&input, serde_json::to_vec(&config).unwrap()).unwrap();
    let result = prepare_external_case_file(&input, &output);
    let output_exists = output.exists();
    assert!(result.is_err());
    assert!(!output_exists);
}

fn receipt(partition: u32, address: &str, tier: DurabilityTier) -> tcp_cluster::OwnerReceipt {
    tcp_cluster::OwnerReceipt {
        partition,
        address: address.to_owned(),
        launch_nonce: "n".to_owned(),
        pid: 1,
        boot_id: "b".to_owned(),
        started_unix_ns: 1,
        durability: tcp_cluster::ReceiptDurability {
            tier,
            max_unsynced_age_ms: None,
            max_pending_completions: 8,
        },
    }
}

#[test]
fn one_owner_receipt_cannot_confirm_several_partitions_or_foreign_identities() {
    let endpoints = vec![
        (0, "10.0.0.1:9400".to_owned()),
        (1, "10.0.0.2:9400".to_owned()),
    ];
    let good = [
        receipt(0, "10.0.0.1:9400", DurabilityTier::Synchronous),
        receipt(1, "10.0.0.2:9400", DurabilityTier::Synchronous),
    ];
    let confirm = |receipts: &[tcp_cluster::OwnerReceipt], tier| {
        confirm_owner_receipts(receipts, &[0, 1], &endpoints, tier)
    };
    assert!(confirm(&good, DurabilityTier::Synchronous).is_ok());
    // The same receipt listed for both partitions.
    let copied = [good[0].clone(), good[0].clone()];
    assert!(confirm(&copied, DurabilityTier::Synchronous).is_err());
    // A receipt from another endpoint or another tier.
    let foreign = [
        good[0].clone(),
        receipt(1, "10.0.0.9:9400", DurabilityTier::Synchronous),
    ];
    assert!(confirm(&foreign, DurabilityTier::Synchronous).is_err());
    assert!(confirm(&good, DurabilityTier::Native).is_err());
    // A missing receipt, and a receipt with no launch nonce.
    assert!(confirm(&good[..1], DurabilityTier::Synchronous).is_err());
    let mut unlaunched = good;
    unlaunched[1].launch_nonce.clear();
    assert!(confirm(&unlaunched, DurabilityTier::Synchronous).is_err());
}

#[test]
fn owner_transport_selection_never_reuses_a_bound_coordinator_source() {
    let coordinator = Transport::Ofi {
        fabric: tidehunter::distributed::tcp::FabricConfig {
            backend: tidehunter::distributed::tcp::FabricBackend::Verbs,
            domain: Some("mlx5_0".into()),
            source_address: Some(std::net::IpAddr::from([10, 0, 0, 1])),
            slots: None,
        },
        security: FabricSecurity::Trusted,
    };
    assert!(select_owner_transports(&coordinator, None, 2).is_err());
    assert!(select_owner_transports(&Transport::Tcp, Some(vec![Transport::Tcp]), 2).is_err());

    let tcp = select_owner_transports(&Transport::Tcp, None, 2).unwrap();
    assert_eq!(tcp, [Transport::Tcp, Transport::Tcp]);
}

#[cfg(feature = "fabric")]
#[test]
fn owner_transport_selection_preserves_host_local_bindings() {
    use tidehunter::distributed::tcp::{FabricBackend, FabricConfig};

    let coordinator = Transport::Ofi {
        fabric: FabricConfig {
            backend: FabricBackend::Verbs,
            domain: Some("coordinator0".into()),
            source_address: Some(std::net::IpAddr::from([10, 0, 0, 1])),
            slots: None,
        },
        security: FabricSecurity::Trusted,
    };
    let owner_transports = vec![
        Transport::Ofi {
            fabric: FabricConfig {
                backend: FabricBackend::Verbs,
                domain: Some("owner0".into()),
                source_address: Some(std::net::IpAddr::from([10, 1, 0, 1])),
                slots: None,
            },
            security: FabricSecurity::Trusted,
        },
        Transport::Ofi {
            fabric: FabricConfig {
                backend: FabricBackend::Verbs,
                domain: Some("owner1".into()),
                source_address: Some(std::net::IpAddr::from([10, 2, 0, 1])),
                slots: None,
            },
            security: FabricSecurity::Trusted,
        },
    ];
    let selected =
        select_owner_transports(&coordinator, Some(owner_transports.clone()), 2).unwrap();
    assert_eq!(selected, owner_transports);
}

#[test]
fn coordinator_interval_requires_monotone_complete_boundary_snapshots() {
    let baseline = COORDINATOR_INTERVAL_COUNTERS
        .into_iter()
        .map(|key| (key, 10))
        .collect::<BTreeMap<_, _>>();
    let end = COORDINATOR_INTERVAL_COUNTERS
        .into_iter()
        .map(|key| (key, 15))
        .collect::<BTreeMap<_, _>>();
    let delta = coordinator_interval_delta(&baseline, &end).unwrap();
    assert_eq!(delta.len(), COORDINATOR_INTERVAL_COUNTERS.len());
    assert!(delta.values().all(|value| *value == 5));
    for name in [
        "write_authority_prepare_ns",
        "write_authority_admit_ns",
        "write_authority_windows",
        "write_authority_batches",
    ] {
        assert_eq!(delta[name], 5);
    }

    let mut missing = end.clone();
    missing.remove(COORDINATOR_INTERVAL_COUNTERS[0]);
    assert!(coordinator_interval_delta(&baseline, &missing).is_err());

    let mut reset = end;
    reset.insert(COORDINATOR_INTERVAL_COUNTERS[0], 9);
    assert!(coordinator_interval_delta(&baseline, &reset).is_err());
}

#[cfg(not(feature = "fabric"))]
#[test]
fn rejects_selected_ofi_before_services_when_feature_is_disabled() {
    // parse_options rejects a missing external_remote_config (and, without
    // nvmeof, the whole backend) before it reaches the transport, so check
    // the transport guard it then applies directly.
    let transport: Transport = serde_json::from_value(serde_json::json!({
        "kind": "ofi",
        "fabric": {"backend": "verbs", "domain": "mlx5_0"},
        "security": "mutual-tls"
    }))
    .unwrap();
    let error = transport.validate().unwrap_err().to_string();
    assert!(
        error.contains("OFI transport requires experimental-fabric"),
        "{error}"
    );
    assert!(Transport::Tcp.validate().is_ok());
}

#[test]
fn value_cache_size_reaches_the_synthetic_shape() {
    for cache in [0, 1_000] {
        let shape = super::synthetic_shape(cache, false);
        let document = serde_json::to_value(&shape).expect("keyspace shape serializes");
        assert_eq!(
            document["key_spaces"][0]["inner"]["config"]["value_cache_size"],
            serde_json::json!(cache)
        );
    }
}

fn external_remote_options() -> serde_json::Value {
    // Complete external deployment so the test runs without the test-utils
    // local fixture, in every feature profile CI builds.
    serde_json::json!({
        "storage": {
            "partitions": [0],
            "batch_limits": {"max_operations": 64, "max_encoded_bytes": 1_048_576},
            "group_limits": {"max_batches": 8, "max_encoded_bytes": 2_097_152},
            "fragment_bytes": 4_194_304,
            "wal_file_bytes": 16_777_216,
            "max_batches_per_partition": 1_000,
            "max_reference_metadata_bytes": 1_048_576
        },
        "endpoints": [{"partition": 0, "address": "127.0.0.1:24000", "server_name": "node-0"}],
        "tls": {
            "transport": {"kind": "tcp-trusted"},
            "ca": "", "cert": "", "key": "", "server_name": "node-0"
        },
        "timeout": {"secs": 30, "nanos": 0},
        "max_admitted_batches": 1_000,
        "max_pending_batches": 8,
        "max_pending_bytes": 67_108_864,
        "read_cache_bytes": 8_388_608,
        "read_memory_allowance_bytes": 536_870_912,
        "max_pending_reads": 256,
        "max_pending_read_bytes": 67_108_864,
        "live_nvme": {
            "host_nqn": "nqn.2014-08.org.nvmexpress:uuid:11111111-1111-4111-8111-111111111111",
            "host_id": [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
            "max_inflight_requests": 32,
            "max_inflight_bytes": 16_777_216,
            "connect_timeout_ms": 30_000,
            "io_timeout_ms": 30_000,
            "leased_durability": "native"
        }
    })
}

#[test]
fn value_cache_option_is_admitted_and_bounded() {
    let accepted = super::parse_options(&serde_json::json!({
        "transport": {"kind": "tcp-trusted"},
        "path": "/tmp/value-cache-option",
        "owners": 1,
        "value_cache_size": 1_000,
        "external_remote_config": external_remote_options()
    }))
    .expect("value_cache_size is admitted");
    assert_eq!(accepted.value_cache_size, 1_000);
    let default = super::parse_options(&serde_json::json!({
        "transport": {"kind": "tcp-trusted"},
        "path": "/tmp/value-cache-option",
        "owners": 1,
        "external_remote_config": external_remote_options()
    }))
    .expect("the option defaults to zero");
    assert_eq!(default.value_cache_size, 0);
    let error = super::parse_options(&serde_json::json!({
        "transport": {"kind": "tcp-trusted"},
        "path": "/tmp/value-cache-option",
        "owners": 1,
        "value_cache_size": 1_000_001,
        "external_remote_config": external_remote_options()
    }))
    .unwrap_err();
    assert!(error.to_string().contains("value_cache_size"), "{error}");
}

/// The relocation cells declare a second keyspace; every other cell keeps the
/// single root keyspace it has always had, so no other shape or persisted
/// database changes. The marker keyspace's filter is engine-side
/// (`dead_wal_files_tests::dead_sealed_serving_file_is_unlinked_while_writes_continue`
/// proves the Stop terminal retires a file on both tiers with the same
/// construction); this test pins the harness shape and the option.
#[test]
fn only_relocation_cells_declare_the_marker_keyspace() {
    let plain = super::synthetic_shape(0, false);
    let keyspaces: Vec<_> = plain.iter_ks().collect();
    assert_eq!(
        keyspaces.len(),
        1,
        "the default shape stays single-keyspace"
    );
    assert_eq!(keyspaces[0].name(), "root");

    let marker = super::synthetic_shape(0, true);
    let keyspaces: Vec<_> = marker.iter_ks().collect();
    assert_eq!(keyspaces.len(), 2, "the marker shape adds one keyspace");
    assert_eq!(keyspaces[0].name(), "root");
    assert_eq!(keyspaces[1].name(), "relocation_marker");
    assert_ne!(
        keyspaces[0].id(),
        keyspaces[1].id(),
        "the marker keyspace is a distinct keyspace"
    );

    // The option is what selects it, and it defaults off.
    let default: super::Options = super::parse_options(&serde_json::json!({
        "transport": {"kind": "tcp-trusted"},
        "path": "/tmp/relocation-marker-option",
        "owners": 1,
        "external_remote_config": external_remote_options()
    }))
    .expect("the option defaults to false");
    assert!(!default.relocation_marker);
}

/// The replay backend builds multi-keyspace shapes from trace tables; the marker
/// keyspace is declared by name, never by position, so such a shape must report
/// no marker (and the manifest must not claim one).
#[test]
fn a_multi_keyspace_shape_reports_no_marker_without_the_option() {
    let mut builder = tidehunter::key_shape::KeyShapeBuilder::new();
    builder.add_key_space("root", 32, 1, KeyType::uniform(64));
    builder.add_key_space("trace_table_7", 32, 1, KeyType::uniform(64));
    let replay_like = builder.build();
    assert_eq!(super::marker_keyspace(&replay_like, false), None);
    assert_eq!(
        super::marker_keyspace(&replay_like, true),
        None,
        "no keyspace is named relocation_marker"
    );

    // A declared shape resolves it wherever it sits, never by position.
    let mut builder = tidehunter::key_shape::KeyShapeBuilder::new();
    builder.add_key_space("root", 32, 1, KeyType::uniform(64));
    builder.add_key_space("trace_table_7", 32, 1, KeyType::uniform(64));
    builder.add_key_space("relocation_marker", 32, 1, KeyType::uniform(64));
    let declared = builder.build();
    let marker = super::marker_keyspace(&declared, true).expect("declared by name");
    assert_eq!(marker, declared.iter_ks().nth(2).unwrap().id());
    assert_eq!(super::marker_keyspace(&declared, false), None);
}
