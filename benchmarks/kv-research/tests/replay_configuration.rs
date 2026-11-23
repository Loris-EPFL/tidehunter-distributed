use std::path::Path;
use std::process::Command;

#[test]
fn replay_validate_config_reports_estimates_without_host_availability() {
    let guard = tempfile::TempDir::with_prefix("replay-configuration-").expect("test tree");
    let root = guard.path().to_path_buf();
    let config = serde_json::json!({
        "schema_version": 1,
        "backend": {"kind": "native", "options": {"path": root.join("db"), "native_wal_geometry": "small"}},
        "workload": {
            "kind": "sui_mainnet_replay",
            "manifest_path": Path::new(env!("CARGO_MANIFEST_DIR")).join("../../experiments/sui-mainnet/fixture/manifest.json"),
            "manifest_sha256": "4264e79e0cbe4a1e36d94468df2f049f988eac57877c9bf2e7e175489dc922fa",
            "repetitions": 1, "minimum_measurement_seconds": 0
        },
        "execution": {"mode": "closed_loop", "concurrency": 1, "queue_capacity": 1, "request_timeout_ms": 30000},
        "limits": {"max_keys": 10000, "max_operations": 10000, "max_bytes": 67108864,
                   "deadline_seconds": 120, "shutdown_grace_seconds": 5}
    });
    let expected = serde_json::to_value(
        serde_json::from_value::<kv_research::mainnet_replay::ReplayConfig>(config.clone())
            .unwrap(),
    )
    .unwrap();
    let path = root.join("config.json");
    std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
    let result = Command::new(env!("CARGO_BIN_EXE_kv-research"))
        .arg("--config")
        .arg(&path)
        .arg("--validate-config")
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let output: serde_json::Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(output["admitted_config"], expected);
    assert_eq!(
        output["host_resources"],
        "not_checked; enforced at run time on the executing host"
    );
    let resources = &output["workload_admission"];
    assert_eq!(resources["host_resources"], output["host_resources"]);
    assert!(
        resources["estimated_disk_reservation_bytes"]
            .as_u64()
            .unwrap()
            > 0
    );
    assert!(
        resources["estimated_memory_reservation_bytes"]
            .as_u64()
            .unwrap()
            > 0
    );
    assert!(resources.get("available_bytes").is_none());
    assert!(resources.get("memory_available_bytes").is_none());
    assert!(!root.join("db").exists());
}
