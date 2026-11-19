//! Q1 staged index runner, local proof: the real CLI runs build, pilot,
//! overwrite, cold-reopen and verify on one retained database served by two
//! supervised ordinary-file fixture OwnerHost children (partitions 0 and 1),
//! in a fresh process per phase. Receipts must chain, bind the stage and
//! name the engine-provisioned database; a completed phase never reruns.
//!
//! Two tests: the round trip skips the pilot's open-loop confirmations
//! (`pilot.qualify: false`), so it holds on any machine, CI included, and
//! checks every correctness invariant of the five phases; the open-loop pilot
//! qualification is the timed capacity claim (closed-loop median, confirmed
//! rate, zero shed, non-growing suffix debt) and runs only in a granted
//! quiet window.
#![cfg(all(feature = "test-utils", target_os = "linux"))]

use serde_json::{Value, json};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

const KEYS: u64 = 4096;

fn sha256_hex(bytes: &[u8]) -> String {
    ring::digest::digest(&ring::digest::SHA256, bytes)
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn free_address() -> String {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .to_string()
}

/// One local-proof stage. `qualify` selects the pilot route: confirmed
/// open-loop rate (the qualification test) or closed-loop median only (the
/// CI round trip). The qualification pilot also runs more operations so a
/// fast machine still produces complete seconds.
fn stage(case_root: &Path, tier: &str, qualify: bool) -> Value {
    let roots: Vec<PathBuf> = (0..2)
        .map(|partition| case_root.join(format!("fixture/owner-p{partition}")))
        .collect();
    json!({
        "schema_version": 1,
        "kind": "q1_index_stage",
        "case_id": format!("q1-local-proof-{tier}"),
        "mode": "local_fixture_proof",
        "index": {"version": 1,
            "owner_roots": roots.iter()
                .map(|root| sha256_hex(root.as_os_str().as_encoded_bytes()))
                .collect::<Vec<_>>(),
            "tier": tier, "exact": "E4", "keys": KEYS, "minimum_puts": 2000,
            "max_puts": 100_000, "queue": 64, "read_cache": false,
            "rss_bytes": 68_719_476_736_u64, "cgroup_bytes": 103_079_215_104_u64,
            "owner_allocation_bytes": [4_294_967_296_u64, 4_294_967_296_u64]},
        "dataset": {"key_namespace_hex": "a1a0a1a0a1a0a1a0a1a0a1a0a1a0a1a0", "seed": 11,
            "value_size": 1024, "entropy": 1.0},
        "case_root": case_root,
        "owners": [
            {"partition": 0, "address": free_address(), "server_name": "leased-fixture",
                "root": roots[0]},
            {"partition": 1, "address": free_address(), "server_name": "leased-fixture",
                "root": roots[1]},
        ],
        "fixture": {"max_admitted_batches": 262_144, "max_index_bytes": 536_870_912,
            "fragment_bytes": 4_194_304, "wal_file_bytes": 67_108_864},
        "workload": {"clients": 4, "request_timeout_ms": 30000, "deadline_seconds": 900,
            "shutdown_grace_seconds": 120,
            "pilot": {"operations": if qualify { 200_000 } else { 20_000 },
                "confirm_seconds": 9, "skip_seconds": 0, "min_complete_seconds": 1,
                "qualify": qualify},
            "overwrite_operations": 4000},
    })
}

fn phase(stage: &Path, root: &Path, name: &str) -> (Output, PathBuf) {
    let output = root.join(format!("out-{name}"));
    let result = Command::new(env!("CARGO_BIN_EXE_kv-research"))
        .arg("--q1-index-stage")
        .arg(stage)
        .arg("--phase")
        .arg(name)
        .arg("--output")
        .arg(&output)
        .output()
        .unwrap();
    (result, output)
}

fn read(path: PathBuf) -> Value {
    serde_json::from_slice(&std::fs::read(&path).unwrap())
        .unwrap_or_else(|error| panic!("{}: {error}", path.display()))
}

/// Run one phase to completion and return its receipt.
fn complete(stage: &Path, root: &Path, name: &str) -> Value {
    let (result, output) = phase(stage, root, name);
    let receipt_path = output.join(format!("{name}-receipt.json"));
    let receipt = if receipt_path.is_file() {
        read(receipt_path.clone())
    } else {
        Value::Null
    };
    assert!(
        result.status.success() && receipt["status"] == "complete",
        "{name}: root={}; receipt error={}; stderr={}",
        root.display(),
        receipt["error"],
        String::from_utf8_lossy(&result.stderr)
    );
    // The registry copy is byte-identical.
    assert_eq!(
        std::fs::read(&receipt_path).unwrap(),
        std::fs::read(
            root.join("case/receipts")
                .join(format!("{name}-receipt.json"))
        )
        .unwrap()
    );
    receipt
}

fn file_sha(path: &Path) -> String {
    sha256_hex(&std::fs::read(path).unwrap())
}

/// The five correctness phases plus the owner observation, with every
/// invariant that holds on both pilot routes.
fn phases(stage_path: &Path, root: &Path) -> (Value, Value, Value, Value, Value, Value) {
    let build = complete(stage_path, root, "build");
    let database = build["database_id"].as_str().unwrap().to_owned();
    assert_eq!(database.len(), 32);
    assert_eq!(build["input_sha256"], json!(file_sha(stage_path)));
    assert_eq!(build["mode"], json!("local_fixture_proof"));
    assert_eq!(build["terminal"]["distinct_rows"], json!(KEYS));
    assert_eq!(build["terminal"]["successful"], json!(KEYS));
    assert_eq!(build["terminal"]["normally_adopted"], json!(true));
    assert_eq!(build["terminal"]["persistence_covered"], json!(true));
    assert_eq!(build["terminal"]["owners_hold_database"], json!(true));
    assert_eq!(build["owners"].as_array().unwrap().len(), 2);
    assert!(build["predecessors"].as_array().unwrap().is_empty());
    // Never rerun on retained data.
    let (rerun, _) = phase(stage_path, root, "build");
    assert!(!rerun.status.success());

    let pilot = complete(stage_path, root, "pilot");
    assert_eq!(pilot["database_id"], json!(database));
    assert_eq!(
        pilot["terminal"]["counts_toward_qualification"],
        json!(false)
    );
    let build_sha = file_sha(&root.join("case/receipts/build-receipt.json"));
    assert_eq!(pilot["predecessors"][0]["sha256"], json!(build_sha));
    assert!(root.join("out-pilot/pilot-touched.bin").is_file());

    let overwrite = complete(stage_path, root, "overwrite");
    let terminal = &overwrite["terminal"];
    assert_eq!(terminal["stop"]["forced_root"], json!(false));
    assert!(!terminal["stop"]["frontier"].is_null());
    assert_eq!(overwrite["predecessors"].as_array().unwrap().len(), 2);
    assert_eq!(
        terminal["owner_allocation_peaks"].as_array().unwrap().len(),
        2
    );
    for name in [
        "timed-touched.bin",
        "overwrite-stop.json",
        "write-diagnostics.json",
    ] {
        assert!(root.join("out-overwrite").join(name).is_file(), "{name}");
    }

    let cold = complete(stage_path, root, "cold-reopen");
    let terminal = &cold["terminal"];
    assert_eq!(cold["database_id"], json!(database));
    assert_eq!(terminal["first_correct"], json!(true));
    assert_eq!(terminal["tail_matches"], json!(true));
    assert_eq!(terminal["frontier_matches"], json!(true));
    assert_eq!(terminal["fresh_owner_processes"], json!(true));
    assert_eq!(terminal["discarded"], json!(0));
    assert_eq!(terminal["warmup"], json!(false));
    assert!(terminal["open_to_read_ns"].as_u64().is_some());
    assert!(terminal["wal_pages"]["selected"].as_u64().unwrap() > 0);

    let verify = complete(stage_path, root, "verify");
    let terminal = &verify["terminal"];
    assert_eq!(terminal["rows"], json!(KEYS));
    assert_eq!(terminal["missing"], json!(0));
    assert_eq!(terminal["wrong"], json!(0));
    assert_eq!(terminal["extra_rows"], json!(0));
    assert_eq!(verify["predecessors"].as_array().unwrap().len(), 4);
    assert!(
        terminal["owner_inventory"]["files"]
            .as_array()
            .unwrap()
            .iter()
            .any(|file| file["scope"] == "fixture_live_target_shutdown")
    );
    assert!(
        terminal["owner_inventory"]["foreign_target_databases"]
            .as_array()
            .unwrap()
            .is_empty()
    );

    // Owner-guest observation of one owner root (the fleet residency
    // input); the database the build provisioned is on that owner.
    let observe = root.join("out-observe");
    let result = Command::new(env!("CARGO_BIN_EXE_kv-research"))
        .args(["--q1-index-stage"])
        .arg(stage_path)
        .args(["--phase", "owner-observe", "--partition", "1", "--output"])
        .arg(&observe)
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let observed = read(observe.join("owner-observe-receipt.json"));
    assert_eq!(observed["terminal"]["databases"], json!([database]));
    assert_eq!(observed["terminal"]["evicted"], json!(true));
    (build, pilot, overwrite, cold, verify, observed)
}

/// Correctness round trip with the pilot's open-loop confirmations skipped:
/// it holds on any machine, CI included, because no offered-rate or
/// suffix-debt claim is made.
#[test]
fn staged_phases_round_trip_on_two_local_fixture_owners() {
    for tier in ["native", "synchronous"] {
        let root = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!(
            "q1-index-stage-{tier}-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let case_root = root.join("case");
        let stage_path = root.join("stage.json");
        std::fs::write(
            &stage_path,
            serde_json::to_vec_pretty(&stage(&case_root, tier, false)).unwrap(),
        )
        .unwrap();

        let (_, pilot, overwrite, _, _, _) = phases(&stage_path, &root);
        // The unconfirmed route selects the primary rate and says so.
        assert_eq!(pilot["terminal"]["closed_loop"]["qualify"], json!(false));
        let qualified = pilot["terminal"]["closed_loop"]["qualified_rate"]
            .as_u64()
            .unwrap();
        assert_eq!(
            pilot["terminal"]["selected_rate"].as_u64().unwrap(),
            qualified * 70 / 100
        );
        assert_eq!(pilot["terminal"]["selected_attempt"], json!("primary"));
        assert_eq!(pilot["terminal"]["fallback_used"], json!(false));
        assert_eq!(overwrite["terminal"]["rate"], json!(qualified * 70 / 100));
        assert_eq!(overwrite["terminal"]["offered"], json!(4000));
        std::fs::remove_dir_all(&root).unwrap();
    }
}

/// Timed capacity claim: the closed-loop median, confirmed open-loop rate,
/// zero shed at the offered rate and a non-growing uncheckpointed suffix.
/// Run only in a granted quiet window; a loaded machine sheds offers and
/// grows suffix debt for real.
#[test]
#[ignore = "timed open-loop qualification: run in a quiet window (gate quiet.sh), not in CI"]
fn staged_open_loop_pilot_qualification_on_two_local_fixture_owners() {
    for tier in ["native", "synchronous"] {
        let root = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!(
            "q1-pilot-qualification-{tier}-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let case_root = root.join("case");
        let stage_path = root.join("stage.json");
        std::fs::write(
            &stage_path,
            serde_json::to_vec_pretty(&stage(&case_root, tier, true)).unwrap(),
        )
        .unwrap();

        let (_, pilot, overwrite, _, _, _) = phases(&stage_path, &root);
        assert_eq!(pilot["terminal"]["closed_loop"]["qualify"], json!(true));
        let qualified = pilot["terminal"]["closed_loop"]["qualified_rate"]
            .as_u64()
            .unwrap();
        let selected = pilot["terminal"]["selected_rate"].as_u64().unwrap();
        assert!(selected == qualified * 70 / 100 || selected == qualified * 56 / 100);
        assert_eq!(overwrite["terminal"]["rate"], json!(selected));
        assert_eq!(overwrite["terminal"]["offered"], json!(4000));
        std::fs::remove_dir_all(&root).unwrap();
    }
}
