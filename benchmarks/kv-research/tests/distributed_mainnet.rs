//! Exercise the public replay factory and the existing real-process owner harness.
use serde_json::{Value, json};
use std::path::Path;
use std::process::Command;

#[test]
fn distributed_mainnet_cli_uses_three_real_owner_databases() {
    let root = std::env::temp_dir().join(format!(
        "distributed-mainnet-cli-{}-{}",
        std::process::id(),
        rand::random::<u64>()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let config = json!({
        "schema_version":1,
        "backend":{"kind":"tidehunter_tcp","options":{"databases":(0..3).map(|id|json!({
            "db_id":id,"options":{
                "transport":{"kind":"tcp-trusted"},"path":root.join(format!("db-{id}")),
                "database":vec![id+1;16],"owners":1,"owner_wal_geometry":"research",
                "coordinator_read_cache_bytes":0,
                "batch_limits":{"max_operations":519,"max_encoded_bytes":500000}
            }
        })).collect::<Vec<_>>() }},
        "workload":{"kind":"sui_mainnet_replay",
            "manifest_path":Path::new(env!("CARGO_MANIFEST_DIR")).join("../../experiments/sui-mainnet/fixture/manifest.json"),
            "manifest_sha256":"4264e79e0cbe4a1e36d94468df2f049f988eac57877c9bf2e7e175489dc922fa",
            "repetitions":2,"minimum_measurement_seconds":0},
        "execution":{"mode":"closed_loop","concurrency":4,"client_shards":1,"queue_capacity":8,"request_timeout_ms":30000},
        "limits":{"max_keys":10000,"max_bytes":67108864,"max_operations":10000,"deadline_seconds":120,"shutdown_grace_seconds":5}
    });
    let path = root.join("config.json");
    std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_kv-research"))
        .args(["--config"])
        .arg(path)
        .arg("--output")
        .arg(root.join("result"))
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "root={}; stderr={}",
        root.display(),
        String::from_utf8_lossy(&output.stderr)
    );
    let summary: Value =
        serde_json::from_slice(&std::fs::read(root.join("result/summary.json")).unwrap()).unwrap();
    assert_eq!(summary["status"], "completed");
    assert_eq!(summary["backend"]["topology"]["node_databases"], 3);
    let counts = &summary["phase_counts"]["counts"];
    for (name, expected) in [
        ("store_gets", 1014),
        ("overlay_demands", 232),
        ("put_members", 1414),
        ("write_calls", 18),
        ("transactions", 90),
        ("checkpoints", 2),
    ] {
        assert_eq!(counts[name], expected, "{name}");
    }
    assert_eq!(summary["phase_counts"]["unknown_outcomes"], 0);
    assert_eq!(summary["phase_counts"]["unresolved_workers"], 0);
    assert_eq!(summary["phase_counts"]["sustained_30s_qualified"], false);
    let samples = std::fs::read_to_string(root.join("result/samples.csv")).unwrap();
    assert_eq!(samples.lines().count(), 1033);
    for id in 0..3 {
        assert!(
            root.join(format!("db-{id}/evidence/processes.yaml"))
                .is_file()
        );
        assert!(
            !root
                .join(format!("db-{id}/evidence/lifecycle-errors.txt"))
                .exists()
        );
    }
    std::fs::remove_dir_all(root).unwrap();
}
