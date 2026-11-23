// Preparation always emits leased live NVMe-oF configurations, which need the feature.

use super::*;
use blake2::{Blake2b512, Digest};
use std::collections::BTreeMap;

/// Input and output tree of one test case. `Drop` removes it also when an
/// assertion panics.
fn temp() -> tempfile::TempDir {
    tempfile::TempDir::with_prefix("replay-owner-preparation-")
        .expect("create the preparation tree")
}
fn synthetic_input() -> serde_json::Value {
    serde_json::json!({
        "guest_root":"/mnt/tidehunter-preparation-golden", "addresses":["127.0.0.1:9400"],
        "transport":{"kind":"tcp-trusted"}, "owner_wal_geometry":"research",
        "max_batches_per_partition":16384,"max_admitted_batches":16384,
        "coordinator_cache_bytes":0,"owner_wal_member_read":"async",
        "read_lanes":2,"polling":"adaptive",
        "live_nvmeof":test_live_nvmeof(1)
    })
}
fn digest(bytes: &[u8]) -> String {
    Blake2b512::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}
/// Digest of every output file. Each preparation draws fresh random initiator
/// identities, so only those fields in `remote.json` and `remote.yaml`
/// are normalized. Owner launch files carry no database-scoped identity.
fn collect_hashes(root: &Path, directory: &Path, files: &mut BTreeMap<String, String>) {
    for entry in fs::read_dir(directory).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if entry.file_type().unwrap().is_dir() {
            collect_hashes(root, &path, files);
            continue;
        }
        let name = path
            .strip_prefix(root)
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned();
        let bytes = fs::read(&path).unwrap();
        let bytes = if name == "remote.json" || name == "remote.yaml" {
            let mut remote: serde_json::Value = if name == "remote.json" {
                serde_json::from_slice(&bytes).unwrap()
            } else {
                serde_yaml::from_slice(&bytes).unwrap()
            };
            let live = remote["live_nvme"].as_object_mut().unwrap();
            for field in ["host_nqn", "host_id", "owner_identities"] {
                live.remove(field).unwrap();
            }
            serde_json::to_vec(&remote).unwrap()
        } else {
            bytes
        };
        files.insert(name, digest(&bytes));
    }
}
#[test]
fn preparation_outputs_are_deterministic_apart_from_initiator_identities() {
    let guard = temp();
    let root = guard.path();
    let input = root.join("input.json");
    fs::write(&input, serde_json::to_vec(&synthetic_input()).unwrap()).unwrap();
    let mut previous = None;
    for name in ["first", "second"] {
        let output = root.join(name);
        prepare_external_case_file(&input, &output).unwrap();
        let mut hashes = BTreeMap::new();
        collect_hashes(&output, &output, &mut hashes);
        assert_eq!(
            hashes.keys().map(String::as_str).collect::<Vec<_>>(),
            [
                "durability.json",
                "node-0/launch.yaml",
                "owner_transports.json",
                "remote.json",
                "remote.yaml",
                "shape.yaml"
            ]
        );
        // Captured from the pre-extension 3bbd706 CLI; these generators are unchanged.
        for (file, hash) in [
            (
                "durability.json",
                "389117a6498def40a97d89ca4fa7ed4a116597a762532440e391ebddc3d1dd2b155fd923f57db426aa1de4becd585f59767614b7d7bddb4ee58376453035487a",
            ),
            (
                "owner_transports.json",
                "5c3f7f89d4ec1232155e1e5899765fb285a47198b950f7c9b174c8151bb6d1403c17c839ac9a2ab7b943d62f28c2f8423dd1ca792ebe6c087916156ee3ca54f7",
            ),
            (
                "shape.yaml",
                "6efb7cce3608d1efd70139999b0ed9bccf26dcd7ce4000ecdbe4726cdfe5f3fa97a2adf46b163fa55078a9e450b9d857e7eba5c3588656c41d90e14999e10f78",
            ),
        ] {
            assert_eq!(hashes[file], hash, "{file}");
        }
        if let Some(previous) = &previous {
            assert_eq!(&hashes, previous);
        }
        previous = Some(hashes);
    }
}
#[test]
fn automatic_preparation_keeps_runtime_counts_out_of_generated_configs() {
    let guard = temp();
    let root = guard.path();
    let input = root.join("input.json");
    let mut config = synthetic_input();
    config.as_object_mut().unwrap().remove("read_lanes");
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
        assert!(launch.get("node").is_none());
        assert!(launch["host"].get("read_workers").is_none());
        let mut hashes = BTreeMap::new();
        collect_hashes(&output, &output, &mut hashes);
        if let Some(previous) = &previous {
            assert_eq!(&hashes, previous);
        }
        previous = Some(hashes);
    }
}

#[test]
fn generated_owner_connection_budget_covers_auto_and_explicit_lanes_with_overlap() {
    use tidehunter::distributed::remote_db::RemoteDatabaseConfig;
    let guard = temp();
    let root = guard.path();
    let input = root.join("input.json");
    let mut config = synthetic_input();
    for (name, lanes, expected) in [("auto", None, 134), ("explicit", Some(100), 206)] {
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
            2 * (lanes.unwrap_or(RemoteDatabaseConfig::MAX_AUTO_READ_LANES as u64)
                + RemoteDatabaseConfig::NON_READ_CONNECTIONS as u64)
        );
    }
    config["read_lanes"] = serde_json::json!(127);
    fs::write(&input, serde_json::to_vec(&config).unwrap()).unwrap();
    let rejected = root.join("rejected");
    let error = prepare_external_case_file(&input, &rejected)
        .unwrap_err()
        .to_string();
    assert!(error.contains("read_lanes=127"));
    assert!(error.contains("require 260 connections"));
    assert!(error.contains("ceiling 256"));
    assert!(!rejected.exists());
}

#[test]
fn replay_preparation_rejects_configured_identity_missing_limits_and_unknown_node_db() {
    let guard = temp();
    let root = guard.path();
    let input = root.join("input.json");
    for variant in 0..4 {
        let mut value = synthetic_input();
        value["replay_node_db_id"] = serde_json::json!(if variant == 0 { 3 } else { 0 });
        if variant == 1 {
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
}
#[test]
fn external_live_replay_preparation_preserves_three_table_domains_and_atomic_output() {
    use crate::mainnet_trace::Trace;
    use tidehunter::distributed::codec::{CompleteBatch, Mutation};
    use tidehunter::distributed::types::{BatchId, BatchVersion};
    let trace = Trace::load(
        &Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../experiments/sui-mainnet/fixture/manifest.json"),
        "4264e79e0cbe4a1e36d94468df2f049f988eac57877c9bf2e7e175489dc922fa",
    )
    .unwrap();
    let guard = temp();
    let root = guard.path();
    let mut databases = Vec::new();
    let mut initiators = std::collections::BTreeSet::new();
    for id in 0..3_u16 {
        let mut value = synthetic_input();
        value["guest_root"] = serde_json::json!(root.join(format!("guest-{id}")));
        value["replay_node_db_id"] = serde_json::json!(id);
        value["batch_limits"] =
            serde_json::json!({"max_operations":519,"max_encoded_bytes":500000});
        value["live_nvmeof"] = serde_json::json!({
            "addresses":["127.0.0.1"],"transport":"tcp","service":4420+2*id,"port_id":1000+2*id,
            "max_inflight_requests":16,"max_inflight_bytes":8388608,"connect_timeout_ms":10000,"io_timeout_ms":30000,
            "leased_write_window":1,"leased_sync":"per_frame_fua",
            "inline_reads":true
        });
        let input = root.join(format!("input-{id}.json"));
        fs::write(&input, serde_json::to_vec(&value).unwrap()).unwrap();
        let output = root.join(format!("output-{id}"));
        prepare_external_case_file(&input, &output).unwrap();
        let json = fs::read(output.join("remote.json")).unwrap();
        let raw: serde_json::Value = serde_json::from_slice(&json).unwrap();
        assert!(raw["storage"].get("database").is_none());
        let remote: RemoteDatabaseConfig = serde_json::from_slice(&json).unwrap();
        remote.validate().unwrap();
        assert_eq!(remote.storage.database, [0; 16]);
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
        assert!(launch.get("node").is_none());
        assert_eq!(launch["partition"].as_u64(), Some(0));
        let host: tidehunter::distributed::tcp::OwnerHostConfig =
            serde_yaml::from_value(launch["host"].clone()).unwrap();
        host.validate().unwrap();
        assert_eq!(host.max_databases, 1);
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
        let live = &remote.live_nvme;
        assert_eq!(live.leased_durability, DurabilityTier::Native);
        assert!(initiators.insert(live.host_id));
        assert_eq!(host.live_target_pool.public_service_base, 4420 + 2 * id);
        assert_eq!(
            host.live_target_pool.commissioning_service_base,
            4421 + 2 * id
        );
        assert_eq!(host.live_target_pool.public_port_base, 1000 + 2 * id);
        databases.push(serde_json::json!({"db_id":id,"options":{
            "transport":{"kind":"tcp-trusted"},"path":root.join(format!("coordinator-{id}")),"owners":1,
            "external_remote_config":remote
        }}));
    }
    let good = serde_json::json!({"databases":databases});
    replay::TidehunterReplayBackend::validate(&good, &trace).unwrap();
    let mut insufficient = good;
    insufficient["databases"][0]["options"]["external_remote_config"]["storage"]["batch_limits"]
        ["max_operations"] = serde_json::json!(518);
    assert!(replay::TidehunterReplayBackend::validate(&insufficient, &trace).is_err());
}
