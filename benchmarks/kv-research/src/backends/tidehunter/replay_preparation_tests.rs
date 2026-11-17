use super::*;
use blake2::{Blake2b512, Digest};
use std::collections::BTreeMap;

fn temp() -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "replay-owner-preparation-{}-{}",
        std::process::id(),
        rand::random::<u64>()
    ));
    fs::create_dir(&root).unwrap();
    root
}
fn synthetic_input() -> serde_json::Value {
    serde_json::json!({
        "guest_root":"/mnt/tidehunter-preparation-golden", "addresses":["127.0.0.1:9400"],
        "transport":{"kind":"tcp-trusted"}, "owner_wal_geometry":"research",
        "max_batches_per_partition":16384,"max_admitted_batches":16384,"max_index_bytes":536870912,
        "coordinator_cache_bytes":0,"owner_read_workers":2,"owner_wal_member_read":"async",
        "read_lanes":2,
        "owner_read_memory_allowance_bytes":268435456,"polling":"adaptive"
    })
}
fn collect_hashes(root: &Path, directory: &Path, files: &mut BTreeMap<String, String>) {
    for entry in fs::read_dir(directory).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if entry.file_type().unwrap().is_dir() {
            collect_hashes(root, &path, files);
        } else {
            let digest: String = Blake2b512::digest(fs::read(&path).unwrap())
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect();
            files.insert(
                path.strip_prefix(root)
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .to_owned(),
                digest,
            );
        }
    }
}
#[test]
fn absent_replay_preparation_inputs_preserve_every_synthetic_output_byte() {
    let root = temp();
    let input = root.join("input.json");
    fs::write(&input, serde_json::to_vec(&synthetic_input()).unwrap()).unwrap();
    let output = root.join("output");
    prepare_external_case_file(&input, &output).unwrap();
    // Captured from the pre-extension 3bbd706 CLI using this exact trusted-TCP
    // input. No paths, identities, fields or bytes are normalized by this check.
    // Only the launch hash is re-pinned for the uniform overlap budget:
    // explicit 2 lanes plus 2 controls, doubled, now needs 8 connections.
    // The remote hashes are re-pinned for the removed coordinator preparation
    // pool: the only difference is the deleted `preparation_workers: 8` field.
    let expected = [
        ("durability.json", "389117a6498def40a97d89ca4fa7ed4a116597a762532440e391ebddc3d1dd2b155fd923f57db426aa1de4becd585f59767614b7d7bddb4ee58376453035487a"),
        ("external_owner_read_config.json", "1cdb078424efc8a57b42df8f41b193f87d8f5e4efc4fbdc15819bac2bfa919bbb956b58f220a0a60741625016003b163b7d4fda5cd7ea9755069516292da6571"),
        ("node-0/launch.yaml", "e9349a3d6a7fab70a8afc411cb52b62dcaf575683c69bfd60731ac2b014f78c0e70deaf25ed9042d1e0a0ea10312e6ff890b2142522779d0a83b1c5f94888503"),
        ("owner_transports.json", "5c3f7f89d4ec1232155e1e5899765fb285a47198b950f7c9b174c8151bb6d1403c17c839ac9a2ab7b943d62f28c2f8423dd1ca792ebe6c087916156ee3ca54f7"),
        ("remote.json", "4852ecb72a28976ff3aa70c5095dd793eacb24842fcf8daab92e8706b6bd52bbe6b7e4e54ad769a70b84d629905ef68ff92d1e1c88dade300cae55da8b289516"),
        ("remote.yaml", "a0cbd90461ccac4fbf9ebe40dc88a8fc83ad87d55ce2a489e0ad8bd6edd524ec6234c6478abc6e1a8c1860cf66fc0bdd00ff99842d3943791be9fa41161a15f8"),
        ("shape.yaml", "6efb7cce3608d1efd70139999b0ed9bccf26dcd7ce4000ecdbe4726cdfe5f3fa97a2adf46b163fa55078a9e450b9d857e7eba5c3588656c41d90e14999e10f78"),
    ].into_iter().map(|(name, hash)| (name.to_owned(), hash.to_owned())).collect::<BTreeMap<_,_>>();
    let mut actual = BTreeMap::new();
    collect_hashes(&output, &output, &mut actual);
    assert_eq!(actual, expected);
    fs::remove_dir_all(root).unwrap();
}
#[test]
fn automatic_preparation_keeps_runtime_counts_out_of_generated_configs() {
    let root = temp();
    let input = root.join("input.json");
    let mut config = synthetic_input();
    config.as_object_mut().unwrap().remove("read_lanes");
    config.as_object_mut().unwrap().remove("owner_read_workers");
    fs::write(&input, serde_json::to_vec(&config).unwrap()).unwrap();
    let mut previous = None;
    for name in ["first", "second"] {
        let output = root.join(name);
        prepare_external_case_file(&input, &output).unwrap();
        let remote: serde_json::Value =
            serde_json::from_slice(&fs::read(output.join("remote.json")).unwrap()).unwrap();
        assert!(remote.get("read_lanes").is_none());
        let launch: serde_yaml::Value =
            serde_yaml::from_slice(&fs::read(output.join("node-0/launch.yaml")).unwrap()).unwrap();
        assert!(launch["node"].get("read_workers").is_none());
        let owners: serde_json::Value = serde_json::from_slice(
            &fs::read(output.join("external_owner_read_config.json")).unwrap(),
        )
        .unwrap();
        assert!(owners[0].get("read_workers").is_none());
        let mut hashes = BTreeMap::new();
        collect_hashes(&output, &output, &mut hashes);
        if let Some(previous) = &previous {
            assert_eq!(&hashes, previous);
        }
        previous = Some(hashes);
    }
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn generated_owner_connection_budget_covers_auto_and_explicit_lanes_with_overlap() {
    use tidehunter::distributed::remote_db::RemoteDatabaseConfig;
    let root = temp();
    let input = root.join("input.json");
    let mut config = synthetic_input();
    for (name, lanes, expected) in [("auto", None, 132), ("explicit", Some(100), 204)] {
        if let Some(lanes) = lanes {
            config["read_lanes"] = serde_json::json!(lanes);
        } else {
            config.as_object_mut().unwrap().remove("read_lanes");
        }
        fs::write(&input, serde_json::to_vec(&config).unwrap()).unwrap();
        let output = root.join(name);
        prepare_external_case_file(&input, &output).unwrap();
        let launch: serde_yaml::Value =
            serde_yaml::from_slice(&fs::read(output.join("node-0/launch.yaml")).unwrap()).unwrap();
        assert_eq!(launch["limits"]["max_connections"].as_u64(), Some(expected));
        assert_eq!(
            expected,
            2 * (lanes.unwrap_or(RemoteDatabaseConfig::MAX_AUTO_READ_LANES as u64) + 2)
        );
    }
    config["read_lanes"] = serde_json::json!(127);
    fs::write(&input, serde_json::to_vec(&config).unwrap()).unwrap();
    let rejected = root.join("rejected");
    let error = prepare_external_case_file(&input, &rejected)
        .unwrap_err()
        .to_string();
    assert!(error.contains("read_lanes=127"));
    assert!(error.contains("require 258 connections"));
    assert!(error.contains("ceiling 256"));
    assert!(!rejected.exists());
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn replay_preparation_rejects_implicit_identity_limits_and_unknown_node_db() {
    let root = temp();
    let input = root.join("input.json");
    for variant in 0..4 {
        let mut value = synthetic_input();
        value["replay_node_db_id"] = serde_json::json!(if variant == 0 { 3 } else { 0 });
        if variant != 1 {
            value["database"] = serde_json::json!(vec![71; 16]);
        }
        if variant != 2 {
            value["batch_limits"] = serde_json::json!({"max_operations":if variant == 3 {0} else {519},"max_encoded_bytes":500000});
        }
        fs::write(&input, serde_json::to_vec(&value).unwrap()).unwrap();
        let output = root.join(format!("bad-{variant}"));
        assert!(prepare_external_case_file(&input, &output).is_err());
        assert!(!output.exists());
    }
    fs::remove_dir_all(root).unwrap();
}
fn replay_outputs(mode: Option<&str>) {
    use crate::mainnet_trace::Trace;
    use tidehunter::distributed::codec::{CompleteBatch, Mutation};
    use tidehunter::distributed::types::{BatchId, BatchVersion};
    let trace = Trace::load(
        &Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../experiments/sui-mainnet/fixture/manifest.json"),
        "4264e79e0cbe4a1e36d94468df2f049f988eac57877c9bf2e7e175489dc922fa",
    )
    .unwrap();
    let root = temp();
    let mut databases = Vec::new();
    let mut initiators = std::collections::BTreeSet::new();
    for id in 0..3_u16 {
        let mut value = synthetic_input();
        value["guest_root"] = serde_json::json!(root.join(format!("guest-{id}")));
        value["replay_node_db_id"] = serde_json::json!(id);
        value["database"] = serde_json::json!(vec![id as u8 + 71; 16]);
        value["batch_limits"] =
            serde_json::json!({"max_operations":519,"max_encoded_bytes":500000});
        if let Some(mode) = mode {
            value["history_persistence"] = serde_json::json!("wal_replay");
            value["live_nvmeof"] = serde_json::json!({
                "addresses":["127.0.0.1"],"transport":"tcp","service":4420+2*id,"port_id":1000+2*id,
                "commissioning_service":4421+2*id,"commissioning_port_id":1001+2*id,
                "max_inflight_requests":16,"max_inflight_bytes":8388608,"connect_timeout_ms":10000,"io_timeout_ms":30000,
                "write_mode":mode,"leased_write_window":1,"leased_sync":"per_frame_fua",
                "export_mode":if mode == "leased" {"per_owner"} else {"on_demand"},"inline_reads":true
            });
        }
        let input = root.join(format!("input-{id}.json"));
        fs::write(&input, serde_json::to_vec(&value).unwrap()).unwrap();
        let output = root.join(format!("output-{id}"));
        prepare_external_case_file(&input, &output).unwrap();
        let remote: RemoteDatabaseConfig =
            serde_json::from_slice(&fs::read(output.join("remote.json")).unwrap()).unwrap();
        remote.validate().unwrap();
        assert_eq!(remote.storage.database, [id as u8 + 71; 16]);
        assert_eq!(remote.storage.batch_limits.max_operations, 519);
        assert_eq!(remote.storage.batch_limits.max_encoded_bytes, 500000);
        let shape: KeyShape =
            serde_yaml::from_slice(&fs::read(output.join("shape.yaml")).unwrap()).unwrap();
        assert_eq!(
            serde_yaml::to_string(&shape).unwrap(),
            serde_yaml::to_string(&replay::shape(&trace.tables, id)).unwrap()
        );
        let names: HashSet<_> = shape.iter_ks().map(|ks| ks.name()).collect();
        let expected: HashSet<_> = trace
            .tables
            .iter()
            .filter(|t| t.db_id == id)
            .map(|t| t.node_name.as_str())
            .collect();
        assert_eq!(names, expected);
        let launch: serde_yaml::Value =
            serde_yaml::from_slice(&fs::read(output.join("node-0/launch.yaml")).unwrap()).unwrap();
        let node: tidehunter::distributed::tcp::NodeConfig =
            serde_yaml::from_value(launch["node"].clone()).unwrap();
        assert_eq!(node.shape_yaml, serde_yaml::to_string(&shape).unwrap());
        assert_eq!(node.partition.inventory.database, remote.storage.database);
        assert_eq!(node.partition.batch_limits, remote.storage.batch_limits);
        let mut members = 0;
        for batch in trace.batches.iter().filter(|b| b.db == id) {
            let mutations = batch
                .entries
                .iter()
                .map(|row| Mutation::Put {
                    keyspace: shape
                        .iter_ks()
                        .find(|ks| ks.name() == trace.tables[usize::from(row.table)].node_name)
                        .unwrap()
                        .id(),
                    key: row.key.clone(),
                    value: row.value.clone(),
                })
                .collect();
            CompleteBatch::new(
                &shape,
                BatchId {
                    database: remote.storage.database,
                    operation: [0; 16],
                },
                BatchVersion {
                    epoch: 1,
                    sequence: u64::from(batch.phase),
                },
                mutations,
                remote.storage.batch_limits,
            )
            .unwrap();
            members = members.max(batch.entries.len());
        }
        assert_eq!(members, [519, 3, 45][usize::from(id)]);
        if let Some(mode) = mode {
            let live = remote.live_nvme.as_ref().unwrap();
            assert_eq!(serde_json::to_value(live.write_mode).unwrap(), mode);
            assert!(initiators.insert(live.host_id));
            let target = node.live_target.as_ref().unwrap();
            assert_eq!(target.host_nqn, live.host_nqn);
            assert_eq!(target.service, 4420 + 2 * id);
        } else {
            assert!(remote.live_nvme.is_none());
            assert!(node.live_target.is_none());
        }
        let read_config: serde_json::Value = serde_json::from_slice(
            &fs::read(output.join("external_owner_read_config.json")).unwrap(),
        )
        .unwrap();
        databases.push(serde_json::json!({"db_id":id,"options":{
            "transport":{"kind":"tcp-trusted"},"path":root.join(format!("coordinator-{id}")),"owners":1,
            "external_remote_config":remote,"external_owner_read_config":read_config
        }}));
    }
    let good = serde_json::json!({"databases":databases});
    replay::TidehunterReplayBackend::validate(&good, &trace).unwrap();
    let mut insufficient = good;
    insufficient["databases"][0]["options"]["external_remote_config"]["storage"]["batch_limits"]
        ["max_operations"] = serde_json::json!(518);
    assert!(replay::TidehunterReplayBackend::validate(&insufficient, &trace).is_err());
    fs::remove_dir_all(root).unwrap();
}
#[test]
fn external_tcp_replay_preparation_preserves_three_table_domains_and_atomic_output() {
    replay_outputs(None);
}
#[cfg(all(feature = "nvmeof", target_os = "linux"))]
#[test]
fn external_live_replay_preparation_preserves_three_table_domains_and_atomic_output() {
    replay_outputs(Some("reserved"));
    replay_outputs(Some("leased"));
}
