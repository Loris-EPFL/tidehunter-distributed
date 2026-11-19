//! A1 index fixture through the real CLI and a supervised owner child:
//! build, receipt-bound overwrite with a crash-style coordinator stop, and a
//! cold reopen in a fresh process with exact final-row verification.
//!
//! Two tests: the round trip runs a closed-loop overwrite, so it holds on any
//! machine (CI included) and checks every correctness invariant; the
//! open-loop admission qualification is the timed capacity claim (production
//! pilot rate selection, zero shed, exact offered admission) and runs only in
//! a granted quiet window.
#![cfg(all(feature = "test-utils", target_os = "linux"))]

use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

const KEYS: u64 = 2048;

/// Production A1 client geometry (`experiments/index-a1/a1_cases.py`): 16
/// callers, 64-deep open-loop window. The load generator refuses an offer
/// once `pending >= queue_capacity` and records it as `admission_capacity`,
/// before the engine is reached; a 4-caller 8-deep window at these rates
/// holds 5-8 of its 8 slots and sheds a few percent of offers, so the
/// zero-shed stage needs the production window.
const CONCURRENCY: u64 = 16;
const QUEUE_CAPACITY: u64 = 64;

/// Production A1 pilot selection (`experiments/index-a1/run_a1.py`): a
/// closed-loop pilot on its own prepared case (one overwrite per prepared
/// case), offer floor(0.70 x median complete-second rate), one 0.8x step.
/// Production pilots 1,000,000 operations; the qualification ladders smaller
/// runs while fewer than five complete seconds exist, so the slow tiers
/// stay short and a fast machine still reaches five seconds.
const PILOT_OPERATIONS: [u64; 3] = [32_768, 262_144, 2_097_152];
/// Complete seconds the production pilot requires (`PILOT_MIN_BUCKETS`).
const PILOT_MIN_BUCKETS: usize = 5;
/// Mean API occupancy the production pilot requires: half the callers.
const PILOT_MIN_OCCUPANCY: f64 = 8.0;

/// One stage configuration. `overwrite` with `None` is a closed-loop
/// overwrite (the round-trip test); `overwrite` with `Some(rate)` is the
/// measured open-loop overwrite offering `rate` operations per second.
fn config(case: &Path, stage: &str, seed: u64, tier: &str, offered: Option<u64>) -> Value {
    let overwrite = stage == "overwrite";
    let mut execution = json!({"mode": "closed_loop", "concurrency": CONCURRENCY, "client_shards": 1,
        "queue_capacity": QUEUE_CAPACITY, "request_timeout_ms": 30000});
    if let Some(rate) = offered {
        assert!(
            overwrite && rate > 0,
            "an offered rate belongs to the overwrite stage"
        );
        execution["mode"] = json!("open_loop");
        execution["offered_ops_per_second"] = json!(rate);
    }
    json!({
        "schema_version": 1,
        "case_id": format!("n{KEYS}-e4-{tier}-r0-{stage}"),
        "backend": {"kind": "tidehunter", "options": {
            "path": case, "owners": 1, "transport": {"kind": "tcp-trusted"},
            "coordinator_read_cache_bytes": 0, "durability_tier": tier,
            "replay_limits": {"max_batches": 4096, "max_encoded_bytes": 268435456},
            "index_fixture": {"stage": stage, "arm": "exact", "max_admitted_batches": 262144,
                "max_index_bytes": 536870912, "fragment_bytes": 4194304,
                "wal_file_bytes": 67108864}}},
        "seed": seed,
        "key_namespace_hex": "a1a0a1a0a1a0a1a0a1a0a1a0a1a0a1a0",
        "dataset": {"keys": KEYS, "value_size": 1024, "entropy": 1.0},
        "workload": {"kind": "synthetic", "distribution": "uniform",
            "put_key_policy": if overwrite { "overwrite" } else { "new" },
            "mix": {"read": 0, "put": 100, "delete": 0, "missing_read": 0, "get_many": 0,
                "put_batch": 0}, "batch_size": 32},
        "execution": execution,
        "phases": {"preload": false, "verify_existing_dataset": false, "warmup_operations": 0,
            "aging_operations": 0, "measurement_operations": if overwrite { 2 * KEYS } else { KEYS }},
        "qualification_only": false,
        "diagnostics": {"index_checkpoint": true, "write_profile_limit": 0,
            "read_profile_limit": 0, "write_timeline_limit": 0},
        "limits": {"max_keys": 10000, "max_bytes": 67108864, "max_operations": 100000,
            "deadline_seconds": 600, "shutdown_grace_seconds": 120}
    })
}

fn run(root: &Path, name: &str, config: &Value) -> Output {
    let path = root.join(format!("{name}.json"));
    std::fs::write(&path, serde_json::to_vec_pretty(config).unwrap()).unwrap();
    Command::new(env!("CARGO_BIN_EXE_kv-research"))
        .arg("--config")
        .arg(&path)
        .arg("--output")
        .arg(root.join(format!("{name}-result")))
        .output()
        .unwrap()
}

fn read(path: PathBuf) -> Value {
    serde_json::from_slice(&std::fs::read(&path).unwrap())
        .unwrap_or_else(|error| panic!("{}: {error}", path.display()))
}

fn assert_success(output: &Output, root: &Path) {
    assert!(
        output.status.success(),
        "root={}; stderr={}",
        root.display(),
        String::from_utf8_lossy(&output.stderr)
    );
}

/// The cold reopen of one overwrite configuration, in a fresh process.
fn cold_reopen(root: &Path, overwrite_config: &str) -> Value {
    let cold = Command::new(env!("CARGO_BIN_EXE_kv-research"))
        .arg("--index-a1-cold-reopen")
        .arg(root.join(format!("{overwrite_config}.json")))
        .arg("--output")
        .arg(root.join("cold-result"))
        .output()
        .unwrap();
    assert_success(&cold, root);
    read(root.join("cold-result/index-a1-cold.json"))
}

fn assert_cold(cold: &Value) {
    assert_eq!(cold["correct"], true);
    assert_eq!(cold["verification"]["mismatched"], 0);
    assert_eq!(cold["first_read"]["correct"], true);
    assert_eq!(cold["tail_matches_stop_record"], true);
    assert_eq!(cold["fencing_generation_advanced"], true);
    assert!(
        cold["first_read"]["open_start_to_first_correct_read_s"]
            .as_f64()
            .unwrap()
            > 0.0
    );
}

/// Assert the build stage of one prepared case.
fn assert_build(root: &Path, case: &Path, name: &str, tier: &str) {
    let stage = read(root.join(format!("{name}-result/index-a1-stage.json")));
    assert_eq!(stage["status"], "verified");
    assert_eq!(stage["verification"]["dataset_rows"]["mismatched"], 0);
    let receipt = read(case.join("fixture-receipt.json"));
    assert_eq!(receipt["dataset"]["keys"], KEYS);
    assert_eq!(receipt["spec"]["tier"], tier);
}

/// Closed-loop pilot evidence, the production `a1_metrics.py` computation:
/// median successful puts per complete second (the last, partial second is
/// excluded), mean API occupancy, the complete-second count and the
/// non-success sample count.
struct PilotEvidence {
    complete_seconds: usize,
    median_puts_per_s: Option<f64>,
    mean_in_flight: Option<f64>,
    non_success: u64,
}

fn pilot_evidence(samples: &Path) -> PilotEvidence {
    let content = std::fs::read_to_string(samples)
        .unwrap_or_else(|error| panic!("{}: {error}", samples.display()));
    let mut lines = content.lines();
    let header = lines
        .next()
        .unwrap_or_else(|| panic!("{}: empty samples file", samples.display()))
        .split(',')
        .collect::<Vec<_>>();
    let column = |name: &str| {
        header
            .iter()
            .position(|field| *field == name)
            .unwrap_or_else(|| panic!("{}: no {name} column", samples.display()))
    };
    let (phase, operation, outcome) = (column("phase"), column("operation"), column("outcome"));
    let (scheduled, completed) = (column("scheduled_ns"), column("completed_ns"));
    let (dispatched, observed) = (column("dispatched_ns"), column("dispatch_observed"));
    let api = column("api_ns");
    // Every column read here precedes `error_kind`, the only field that can
    // hold a comma, so a prefix split is exact for the columns used.
    let need = scheduled.max(dispatched).max(completed).max(api) + 1;
    let (mut first_scheduled, mut first_dispatched) = (None, None);
    let (mut last_completed, mut non_success, mut api_total) = (0_u64, 0_u64, 0_u128);
    let mut buckets: BTreeMap<u64, u64> = BTreeMap::new();
    for line in lines {
        let fields = line.split(',').collect::<Vec<_>>();
        if fields.len() < need || fields[phase] != "measurement" {
            continue;
        }
        let (Ok(scheduled), Ok(completed)) = (
            fields[scheduled].parse::<u64>(),
            fields[completed].parse::<u64>(),
        ) else {
            continue;
        };
        first_scheduled =
            Some(first_scheduled.map_or(scheduled, |first: u64| first.min(scheduled)));
        last_completed = last_completed.max(completed);
        if fields[outcome] != "success" {
            non_success += 1;
        }
        if fields[observed] == "true"
            && let (Ok(dispatched), Ok(api_ns)) = (
                fields[dispatched].parse::<u64>(),
                fields[api].parse::<u64>(),
            )
        {
            first_dispatched =
                Some(first_dispatched.map_or(dispatched, |first: u64| first.min(dispatched)));
            api_total += u128::from(api_ns);
        }
        if fields[outcome] == "success" && fields[operation] == "put" {
            *buckets.entry(completed / 1_000_000_000).or_default() += 1;
        }
    }
    let mut evidence = PilotEvidence {
        complete_seconds: 0,
        median_puts_per_s: None,
        mean_in_flight: None,
        non_success,
    };
    let Some(first) = first_scheduled else {
        return evidence;
    };
    let last = last_completed / 1_000_000_000;
    let mut rates: Vec<u64> = (first / 1_000_000_000..last)
        .map(|second| buckets.get(&second).copied().unwrap_or(0))
        .collect();
    evidence.complete_seconds = rates.len();
    if !rates.is_empty() {
        rates.sort_unstable();
        let middle = rates.len() / 2;
        let median = if rates.len() % 2 == 1 {
            rates[middle] as f64
        } else {
            (rates[middle - 1] + rates[middle]) as f64 / 2.0
        };
        evidence.median_puts_per_s = Some(median);
    }
    if let Some(dispatched) = first_dispatched {
        let window = last_completed.saturating_sub(dispatched);
        if window > 0 {
            evidence.mean_in_flight = Some(api_total as f64 / window as f64);
        }
    }
    evidence
}

/// The production offer ladder: floor(0.70 x median), then one 0.8x step.
fn offered_rates(median: f64) -> Vec<u64> {
    let primary = (0.70 * median).floor();
    vec![primary as u64, (0.8 * primary).floor() as u64]
}

#[test]
fn stock_local_route_reuses_endpoint_and_database_identity() {
    let root = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "leased-stock-{}-{:016x}",
        std::process::id(),
        rand::random::<u64>(),
    ));
    std::fs::create_dir_all(&root).unwrap();
    let case = root.join("case");
    let mut config = config(&case, "build", 7, "native", None);
    config.as_object_mut().unwrap().remove("key_namespace_hex");
    config["backend"]["options"]
        .as_object_mut()
        .unwrap()
        .remove("index_fixture");
    config["dataset"]["keys"] = json!(64);
    config["phases"]["measurement_operations"] = json!(8);

    assert_success(&run(&root, "create", &config), &root);
    let spec_path = case.join("fixture/spec.json");
    let original = std::fs::read(&spec_path).unwrap();
    let mut changed = config.clone();
    changed["backend"]["options"]["durability_tier"] = json!("synchronous");
    let refusal = run(&root, "changed-tier", &changed);
    assert!(!refusal.status.success());
    assert!(String::from_utf8_lossy(&refusal.stderr).contains("configuration changed on reopen"));
    assert_success(&run(&root, "reopen", &config), &root);
    assert_eq!(std::fs::read(&spec_path).unwrap(), original);
    // A new authority ID would create a second owner database.
    let catalog = case.join("fixture/owner-p0");
    let databases = std::fs::read_dir(catalog)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .filter(|name| name.to_string_lossy().starts_with("db-"))
        .count();
    assert_eq!(databases, 1);
    std::fs::remove_dir_all(root).unwrap();
}

/// Correctness round trip on a closed-loop overwrite: it holds on any
/// machine, CI included, because a closed-loop driver blocks at its window
/// instead of shedding offers.
#[test]
fn build_overwrite_cold_reopen_round_trip_on_the_leased_fixture() {
    for tier in ["native", "synchronous"] {
        let root = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!(
            "index-a1-{tier}-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let case = root.join("case");

        assert_success(
            &run(&root, "build", &config(&case, "build", 7, tier, None)),
            &root,
        );
        assert_build(&root, &case, "build", tier);

        // A different dataset identity fails before any owner start or write.
        let foreign = run(&root, "foreign", &config(&case, "overwrite", 8, tier, None));
        assert!(!foreign.status.success());
        assert!(String::from_utf8_lossy(&foreign.stderr).contains("build receipt mismatch"));
        assert!(!case.join("overwrite-stop.json").exists());

        let overwrite = run(
            &root,
            "overwrite",
            &config(&case, "overwrite", 7, tier, None),
        );
        assert_success(&overwrite, &root);
        let stop = read(case.join("overwrite-stop.json"));
        let timed = &stop["timed"];
        assert_eq!(timed["complete"], true);
        assert_eq!(timed["ack_matches_publication"], true);
        // A closed-loop driver dispatches every operation: the count is a
        // correctness claim, not a capacity one.
        assert_eq!(timed["successful_puts"], 2 * KEYS);
        assert!(timed["owner_wal_data_frame_bytes"].as_u64().unwrap() > 0);
        assert!(stop["touched"]["count"].as_u64().unwrap() > 0);
        let summary = read(root.join("overwrite-result/summary.json"));
        assert_eq!(summary["status"], "completed");
        assert!(summary["index_checkpoint"].is_object());

        // One overwrite per prepared case.
        let again = run(
            &root,
            "again",
            &config(&case, "overwrite", 7, tier, Some(1)),
        );
        assert!(!again.status.success());

        let cold = cold_reopen(&root, "overwrite");
        assert_cold(&cold);
        std::fs::remove_dir_all(&root).unwrap();
    }
}

/// Timed capacity claim: production rate selection, zero shed at the offered
/// rate, exact admission of every offered put. Run only in a granted quiet
/// window; a loaded machine sheds offers and grows suffix debt for real.
#[test]
#[ignore = "timed open-loop qualification: run in a quiet window (gate quiet.sh), not in CI"]
fn open_loop_admission_qualification_on_the_leased_fixture() {
    for tier in ["native", "synchronous"] {
        let root = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!(
            "index-a1-qualification-{tier}-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        ));
        std::fs::create_dir_all(&root).unwrap();

        // Rate selection: the production pilot on its own prepared case,
        // extended while fewer than five complete seconds exist.
        let mut median = None;
        let mut last_evidence = String::new();
        for (attempt, operations) in PILOT_OPERATIONS.iter().enumerate() {
            let case = root.join(format!("pilot-{attempt}"));
            let mut pilot = config(&case, "overwrite", 7, tier, None);
            pilot["case_id"] = json!(format!("n{KEYS}-e4-{tier}-r0-pilot"));
            pilot["phases"]["measurement_operations"] = json!(operations);
            // The ladder can reach 2,097,152 operations on a fast machine;
            // the measured stage stays at 2 x KEYS.
            pilot["limits"]["max_operations"] = json!(4_000_000);
            pilot["limits"]["deadline_seconds"] = json!(1200);
            let name = format!("pilot-{attempt}");
            assert_success(
                &run(
                    &root,
                    &format!("{name}-build"),
                    &config(&case, "build", 7, tier, None),
                ),
                &root,
            );
            let label = format!("{name}-run");
            assert_success(&run(&root, &label, &pilot), &root);
            let evidence = pilot_evidence(&root.join(format!("{label}-result/samples.csv")));
            last_evidence = format!(
                "operations={operations}, complete_seconds={}, median_puts_per_s={:?}, mean_in_flight={:?}, non_success={}",
                evidence.complete_seconds,
                evidence.median_puts_per_s,
                evidence.mean_in_flight,
                evidence.non_success
            );
            assert_eq!(
                evidence.non_success, 0,
                "pilot {label} shed offers: {last_evidence}"
            );
            if evidence.complete_seconds >= PILOT_MIN_BUCKETS
                && evidence
                    .mean_in_flight
                    .is_some_and(|occupancy| occupancy >= PILOT_MIN_OCCUPANCY)
                && evidence.median_puts_per_s.is_some_and(|rate| rate > 0.0)
            {
                median = evidence.median_puts_per_s;
                break;
            }
        }
        let median =
            median.unwrap_or_else(|| panic!("pilot found no rate on {tier}: {last_evidence}"));

        // The measured stage: one prepared case per offered rate, because an
        // overwrite stops crash-style and never reruns on retained data.
        let mut measured = None;
        for (step, rate) in offered_rates(median).into_iter().enumerate() {
            let name = if step == 0 {
                "case".to_owned()
            } else {
                format!("case-{step}")
            };
            let case = root.join(&name);
            assert_success(
                &run(
                    &root,
                    &format!("{name}-build"),
                    &config(&case, "build", 7, tier, None),
                ),
                &root,
            );
            assert_build(&root, &case, &format!("{name}-build"), tier);

            // A different dataset identity fails before any owner start or write.
            let foreign = run(
                &root,
                &format!("{name}-foreign"),
                &config(&case, "overwrite", 8, tier, Some(rate)),
            );
            assert!(!foreign.status.success());
            assert!(String::from_utf8_lossy(&foreign.stderr).contains("build receipt mismatch"));
            assert!(!case.join("overwrite-stop.json").exists());

            let overwrite = run(
                &root,
                &format!("{name}-overwrite"),
                &config(&case, "overwrite", 7, tier, Some(rate)),
            );
            assert_success(&overwrite, &root);
            let stop = read(case.join("overwrite-stop.json"));
            let timed = &stop["timed"];
            assert_eq!(timed["complete"], true);
            assert_eq!(timed["ack_matches_publication"], true);
            if timed["successful_puts"] == json!(2 * KEYS) {
                measured = Some((case, name, stop));
                break;
            }
        }
        let (case, name, stop) = measured.unwrap_or_else(|| {
            panic!(
                "no offered rate admitted all {} puts on {tier} (median {median})",
                2 * KEYS
            )
        });
        let timed = &stop["timed"];
        assert_eq!(timed["successful_puts"], 2 * KEYS);
        assert!(timed["owner_wal_data_frame_bytes"].as_u64().unwrap() > 0);
        assert!(stop["touched"]["count"].as_u64().unwrap() > 0);
        let summary = read(root.join(format!("{name}-overwrite-result/summary.json")));
        assert_eq!(summary["status"], "completed");
        assert!(summary["index_checkpoint"].is_object());

        // One overwrite per prepared case.
        let again = run(
            &root,
            "again",
            &config(&case, "overwrite", 7, tier, Some(1)),
        );
        assert!(!again.status.success());

        let cold = cold_reopen(&root, &format!("{name}-overwrite"));
        assert_cold(&cold);
        std::fs::remove_dir_all(&root).unwrap();
    }
}
