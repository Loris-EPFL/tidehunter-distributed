use serde_json::{Value, json};
use std::fs;
use std::path::Path;
use std::process::{Command, Output};
use tidehunter::distributed::tcp::OwnerHostConfig;

fn prepare(input: &Path, output: &Path) -> Output {
    Command::new(env!("CARGO_BIN_EXE_kv-research"))
        .arg("--prepare-external-case")
        .arg(input)
        .arg("--output")
        .arg(output)
        .output()
        .unwrap()
}

fn assert_success(output: &Output) {
    assert!(
        output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn check_owner(output: &Path, partition: u32, slots: usize, bases: (u16, u16, u16, u16)) {
    let launch: serde_yaml::Value = serde_yaml::from_slice(
        &fs::read(output.join(format!("node-{partition}/launch.yaml"))).unwrap(),
    )
    .unwrap();
    assert_eq!(launch["partition"].as_u64(), Some(u64::from(partition)));
    assert!(launch.get("node").is_none());
    let host: OwnerHostConfig = serde_yaml::from_value(launch["host"].clone()).unwrap();
    host.validate().unwrap();
    assert_eq!(host.max_databases, slots);
    let pool = host.live_target_pool;
    assert_eq!(pool.address, "127.0.0.1");
    assert_eq!(
        (
            pool.public_port_base,
            pool.public_service_base,
            pool.commissioning_port_base,
            pool.commissioning_service_base,
        ),
        bases
    );
}

#[test]
fn python_kernel_prepare_input_and_multi_database_pool_pass_the_cli() {
    let guard = tempfile::TempDir::with_prefix("prepare-external-case-").expect("test tree");
    let root = guard.path().to_path_buf();
    let generated = Command::new("python3")
        .current_dir(Path::new(env!("CARGO_MANIFEST_DIR")).join("../../experiments/kv-comparison"))
        .args([
            "-B",
            "-c",
            "import json, sys\n\
             from local_dry_run_targets import _kernel_live_prepare_input\n\
             print(json.dumps(_kernel_live_prepare_input(sys.argv[1], '127.0.0.1:9400',\n\
                 'tcp', '127.0.0.1', 4420, 1000, 'native', 8 * 1024**2)))",
        ])
        .arg(root.join("guest"))
        .output()
        .unwrap();
    assert_success(&generated);
    let input = root.join("input.json");
    fs::write(&input, &generated.stdout).unwrap();
    let kernel = root.join("kernel");
    assert_success(&prepare(&input, &kernel));
    check_owner(&kernel, 0, 1, (1000, 4420, 1001, 4421));
    let remote: Value =
        serde_json::from_slice(&fs::read(kernel.join("remote.json")).unwrap()).unwrap();
    assert_eq!(remote["storage"]["wal_file_bytes"], 8 * 1024 * 1024);
    assert!(remote["storage"].get("database").is_none());

    let mut config: Value = serde_json::from_slice(&generated.stdout).unwrap();
    config["addresses"] = json!(["127.0.0.1:9400", "127.0.0.1:9401"]);
    config["live_nvmeof"]["addresses"] = json!(["127.0.0.1", "127.0.0.1"]);
    config["live_nvmeof"]["max_databases"] = json!(4);
    fs::write(&input, serde_json::to_vec(&config).unwrap()).unwrap();
    let shared = root.join("shared");
    assert_success(&prepare(&input, &shared));
    check_owner(&shared, 0, 4, (1000, 4420, 1004, 4424));
    check_owner(&shared, 1, 4, (1008, 4428, 1012, 4432));

    for (slots, service) in [(0, 4420), (17, 4420), (16, 65473)] {
        config["live_nvmeof"]["max_databases"] = json!(slots);
        config["live_nvmeof"]["service"] = json!(service);
        fs::write(&input, serde_json::to_vec(&config).unwrap()).unwrap();
        let rejected = root.join("rejected");
        let output = prepare(&input, &rejected);
        assert!(!output.status.success());
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("target pool") || stderr.contains("max_databases"),
            "{stderr}"
        );
        assert!(!rejected.exists());
    }
}
