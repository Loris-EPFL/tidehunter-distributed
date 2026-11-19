use super::super::RowCheck;
use super::super::cold::Residency;
use super::evidence::{self, Measurement};
use super::input::{ExactSetting, RunKind, StageInput, StageMode, local_root_fingerprint};
use super::recovery::{Domains, expected_domains, pages, superset_cold};
use super::*;
use crate::config::{ExecutionMode, PutKeyPolicy};
use std::io::Write as _;

const NAMESPACE: &str = "a1a0a1a0a1a0a1a0a1a0a1a0a1a0a1a0";

fn scratch(name: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "{name}-{}-{}",
        std::process::id(),
        rand::random::<u64>()
    ));
    fs::create_dir_all(&root).unwrap();
    root
}

fn local_stage(case_root: &Path) -> Value {
    let roots: Vec<PathBuf> = (0..2)
        .map(|partition| case_root.join(format!("fixture/owner-p{partition}")))
        .collect();
    json!({
        "schema_version": 1,
        "kind": "q1_index_stage",
        "case_id": "q1-local-proof",
        "mode": "local_fixture_proof",
        "index": {"version": 1,
            "owner_roots": roots.iter().map(|root| local_root_fingerprint(root)).collect::<Vec<_>>(),
            "tier": "native", "exact": "E4", "keys": 4096, "minimum_puts": 2000,
            "max_puts": 8000, "queue": 8, "read_cache": false,
            "rss_bytes": 68_719_476_736_u64, "cgroup_bytes": 103_079_215_104_u64,
            "owner_allocation_bytes": [1_073_741_824_u64, 1_073_741_824_u64]},
        "dataset": {"key_namespace_hex": NAMESPACE, "seed": 7, "value_size": 1024, "entropy": 1.0},
        "case_root": case_root,
        "owners": [
            {"partition": 0, "address": "127.0.0.1:4100", "server_name": "leased-fixture",
                "root": roots[0]},
            {"partition": 1, "address": "127.0.0.1:4101", "server_name": "leased-fixture",
                "root": roots[1]},
        ],
        "fixture": {"max_admitted_batches": 262_144, "max_index_bytes": 536_870_912,
            "fragment_bytes": 4_194_304, "wal_file_bytes": 67_108_864},
        "workload": {"clients": 4, "request_timeout_ms": 30000, "deadline_seconds": 600,
            "shutdown_grace_seconds": 120,
            "pilot": {"operations": 4000, "confirm_seconds": 9, "skip_seconds": 1,
                "min_complete_seconds": 1},
            "overwrite_operations": 4000},
    })
}

/// A fleet stage: the local remote configuration moved to mTLS on two
/// remote owner addresses, with the fleet dataset size.
fn fleet_stage() -> Value {
    let case_root = PathBuf::from("/q1/case");
    let local: StageInput = serde_json::from_value(local_stage(&case_root)).unwrap();
    let mut remote = local.local_spec([3; 16]).unwrap().remote_config().unwrap();
    remote.tls.transport = tidehunter::distributed::tcp::Transport::Tcp;
    remote.tls.ca = PathBuf::from("/q1/tls/ca.pem");
    remote.tls.cert = PathBuf::from("/q1/tls/coordinator.pem");
    remote.tls.key = PathBuf::from("/q1/tls/coordinator.key");
    let addresses = ["10.0.0.2:9400", "10.0.0.3:9400"];
    for (endpoint, address) in remote.endpoints.iter_mut().zip(addresses) {
        endpoint.address = address.to_owned();
        endpoint.server_name = format!("owner-{}", endpoint.partition);
    }
    let mut stage = local_stage(&case_root);
    stage["mode"] = json!("fleet");
    stage["index"]["keys"] = json!(100_000_000);
    stage["index"]["minimum_puts"] = json!(10_000_000);
    stage["index"]["max_puts"] = json!(30_000_000);
    stage["index"]["owner_roots"] = json!(["1".repeat(64), "2".repeat(64)]);
    stage["workload"]["overwrite_operations"] = json!(10_000_000);
    stage["owners"] = json!([
        {"partition": 0, "address": addresses[0], "server_name": "owner-0", "root": "/srv/owner"},
        {"partition": 1, "address": addresses[1], "server_name": "owner-1", "root": "/srv/owner"},
    ]);
    stage.as_object_mut().unwrap().remove("fixture");
    stage["remote"] = serde_json::to_value(&remote).unwrap();
    stage
}

fn parse(value: Value) -> BackendResult<StageInput> {
    let stage: StageInput = serde_json::from_value(value)?;
    stage.validate()?;
    Ok(stage)
}

#[test]
fn local_and_fleet_stage_inputs_validate() {
    let local = parse(local_stage(Path::new("/q1/case"))).unwrap();
    assert_eq!(local.mode, StageMode::LocalFixtureProof);
    assert_eq!(local.replay_limits(), ExactSetting::E4.replay_limits());
    let fleet = parse(fleet_stage()).unwrap();
    assert_eq!(fleet.mode, StageMode::Fleet);
    assert_eq!(ExactSetting::E4.soft_bytes(), 64 << 20);
    assert_eq!(ExactSetting::E16.soft_bytes(), 256 << 20);
}

#[test]
fn stage_input_rejects_every_unsafe_or_inconsistent_field() {
    type StageMutation = (&'static str, Box<dyn Fn(&mut Value)>);
    let local = || local_stage(Path::new("/q1/case"));
    let mutations: Vec<StageMutation> = vec![
        ("unknown field", Box::new(|stage| stage["extra"] = json!(1))),
        (
            "schema",
            Box::new(|stage| stage["schema_version"] = json!(2)),
        ),
        (
            "read cache",
            Box::new(|stage| stage["index"]["read_cache"] = json!(true)),
        ),
        (
            "one owner root",
            Box::new(|stage| {
                stage["index"]["owner_roots"] = json!([stage["index"]["owner_roots"][0]]);
            }),
        ),
        (
            "overwrite below minimum",
            Box::new(|stage| {
                stage["workload"]["overwrite_operations"] = json!(1999);
            }),
        ),
        (
            "clients above queue",
            Box::new(|stage| stage["workload"]["clients"] = json!(9)),
        ),
        (
            "relative case root",
            Box::new(|stage| stage["case_root"] = json!("case")),
        ),
        (
            "partition order",
            Box::new(|stage| stage["owners"][1]["partition"] = json!(0)),
        ),
        (
            "remote owner in local proof",
            Box::new(|stage| {
                stage["owners"][1]["address"] = json!("10.0.0.3:4101");
            }),
        ),
        (
            "owner root outside the fixture",
            Box::new(|stage| {
                stage["owners"][0]["root"] = json!("/elsewhere/owner-p0");
            }),
        ),
        (
            "namespace",
            Box::new(|stage| {
                stage["dataset"]["key_namespace_hex"] = json!("A1A0");
            }),
        ),
        (
            "remote in local proof",
            Box::new(|stage| stage["remote"] = fleet_stage()["remote"].clone()),
        ),
    ];
    for (name, mutate) in &mutations {
        let mut stage = local();
        mutate(&mut stage);
        assert!(parse(stage).is_err(), "{name}");
    }
    let fleet_mutations: Vec<StageMutation> = vec![
        (
            "preseeded database",
            Box::new(|stage| {
                stage["remote"]["storage"]["database"] = json!(vec![1_u8; 16]);
            }),
        ),
        (
            "trusted control",
            Box::new(|stage| {
                stage["remote"]["tls"]["transport"] = json!({"kind": "tcp-trusted"});
            }),
        ),
        (
            "read cache bytes",
            Box::new(|stage| stage["remote"]["read_cache_bytes"] = json!(4096)),
        ),
        (
            "endpoint differs",
            Box::new(|stage| {
                stage["remote"]["endpoints"][1]["address"] = json!("10.0.0.9:9400");
            }),
        ),
        (
            "replay limits differ",
            Box::new(|stage| stage["index"]["exact"] = json!("E16")),
        ),
        (
            "tier differs",
            Box::new(|stage| stage["index"]["tier"] = json!("synchronous")),
        ),
        (
            "fleet size",
            Box::new(|stage| stage["index"]["keys"] = json!(4096)),
        ),
        (
            "fixture in fleet",
            Box::new(|stage| {
                stage["fixture"] = local_stage(Path::new("/q1/case"))["fixture"].clone();
            }),
        ),
        (
            "no remote",
            Box::new(|stage| {
                stage.as_object_mut().unwrap().remove("remote");
            }),
        ),
    ];
    for (name, mutate) in &fleet_mutations {
        let mut stage = fleet_stage();
        mutate(&mut stage);
        assert!(parse(stage).is_err(), "{name}");
    }
}

#[test]
fn run_configs_follow_the_phase_contract() {
    let stage = parse(local_stage(Path::new("/q1/case"))).unwrap();
    let build = stage.run_config(RunKind::Build).unwrap();
    assert_eq!(build.execution.mode, ExecutionMode::ClosedLoop);
    assert_eq!(build.workload.put_key_policy, PutKeyPolicy::New);
    assert_eq!(build.phases.measurement_operations, 4096);
    assert_eq!(build.execution.queue_capacity, 8);
    assert_eq!(build.key_namespace_hex.as_deref(), Some(NAMESPACE));
    let pilot = stage.run_config(RunKind::PilotClosed).unwrap();
    assert_eq!(pilot.execution.mode, ExecutionMode::ClosedLoop);
    assert_eq!(pilot.workload.put_key_policy, PutKeyPolicy::Overwrite);
    assert_eq!(pilot.phases.measurement_operations, 4000);
    let confirm = stage.run_config(RunKind::PilotConfirm(700)).unwrap();
    assert_eq!(confirm.execution.mode, ExecutionMode::OpenLoop);
    assert_eq!(confirm.execution.offered_ops_per_second, Some(700.0));
    assert_eq!(confirm.phases.measurement_operations, 6300);
    let overwrite = stage.run_config(RunKind::Overwrite(560)).unwrap();
    assert_eq!(overwrite.execution.offered_ops_per_second, Some(560.0));
    assert_eq!(overwrite.phases.measurement_operations, 4000);
    assert_eq!(
        overwrite.durability_tier().unwrap(),
        Some(tidehunter::distributed::durability::DurabilityTier::Native)
    );
    assert!(stage.run_config(RunKind::Overwrite(0)).is_err());
}

#[test]
fn pilot_qualify_controls_the_confirmation_bound_and_defaults_to_true() {
    let case_root = Path::new("/q1/case");
    // Absent `qualify` defaults to true, so confirmations need 9 s.
    assert!(parse(local_stage(case_root)).is_ok());
    let mut stage = local_stage(case_root);
    stage["workload"]["pilot"]["confirm_seconds"] = json!(3);
    assert!(parse(stage.clone()).is_err());
    // The unconfirmed route only needs the original 3 s bound.
    stage["workload"]["pilot"]["qualify"] = json!(false);
    let input = parse(stage.clone()).unwrap();
    assert!(!input.workload.pilot.qualify);
    // The phase configurations are unchanged by the route.
    let confirm = input.run_config(RunKind::PilotConfirm(700)).unwrap();
    assert_eq!(confirm.execution.offered_ops_per_second, Some(700.0));
    assert_eq!(confirm.phases.measurement_operations, 2100);
    stage["workload"]["pilot"]["confirm_seconds"] = json!(2);
    assert!(parse(stage).is_err());
}

#[test]
fn phases_parse_and_chain_in_order() {
    let chain: Vec<_> = ["build", "pilot", "overwrite", "cold-reopen", "verify"]
        .iter()
        .map(|name| Phase::parse(name).unwrap())
        .collect();
    for pair in chain.windows(2) {
        assert_eq!(pair[1].predecessor(), Some(pair[0]));
    }
    assert_eq!(Phase::Build.predecessor(), None);
    assert_eq!(Phase::OwnerObserve.predecessor(), None);
    assert_eq!(Phase::parse("cold"), None);
    assert_eq!(Phase::ColdReopen.receipt_file(), "cold-reopen-receipt.json");
}

fn samples_csv(directory: &Path, rows: &[&str]) {
    let mut file = fs::File::create(directory.join("samples.csv")).unwrap();
    writeln!(file, "phase,operation,outcome,scheduled_ns,dispatched_ns,dispatch_observed,completed_ns,api_ns,response_ns,scheduling_ns,logical_keys,bytes,error_kind,caller_sample_id,released_ns,stream,source_checkpoint,source_tx,repetition,db,table,batch_phase,parent_batch,members").unwrap();
    for row in rows {
        writeln!(file, "{row}").unwrap();
    }
}

fn row(outcome: &str, scheduled: u64, completed: u64, api: u64, error: &str) -> String {
    let observed = outcome != "rejected";
    format!(
        "measurement,put,{outcome},{scheduled},{scheduled},{observed},{completed},{api},{api},0,1,1024,{error},,{completed},,,,,,,,,"
    )
}

#[test]
fn measurement_counts_outcomes_quantiles_seconds_and_quoted_fields() {
    let directory = scratch("samples");
    let second = 1_000_000_000;
    let mut rows = vec![
        "qualification,put,success,0,0,true,1,1,1,0,1,256,,,1,,,,,,,,,".to_owned(),
        row("rejected", 10, 10, 0, ""),
        row(
            "timeout",
            20,
            30 + second,
            second,
            "\"deadline, \"\"late\"\"\nsecond line\"",
        ),
    ];
    for index in 0..300_u64 {
        // 100 successes in each of seconds 0, 1 and 2, then a partial second.
        let completed = (index / 100) * second + index;
        rows.push(row(
            "success",
            completed.saturating_sub(5),
            completed,
            5 + index,
            "",
        ));
    }
    rows.push(row("success", 3 * second, 3 * second + 7, 400, ""));
    let rows: Vec<&str> = rows.iter().map(String::as_str).collect();
    samples_csv(&directory, &rows);
    let measured: Measurement = evidence::measurement(&directory, 0).unwrap();
    assert_eq!(measured.offered, 303);
    assert_eq!(measured.accepted, 301);
    assert_eq!(measured.rejected, 1);
    assert_eq!(measured.unknown, 1);
    assert_eq!(measured.non_success(), 2);
    assert_eq!(measured.complete_seconds, vec![100, 100, 100]);
    assert_eq!(measured.median_puts_per_s, Some(100.0));
    assert_eq!(measured.success_api_ns["p50"], Some(155));
    assert_eq!(measured.success_api_ns["p999"], Some(400));
    assert_eq!(measured.failure_api_ns["p50"], Some(second));
    assert!(measured.mean_in_flight.is_some());
    let skipped = evidence::measurement(&directory, 1).unwrap();
    assert_eq!(skipped.complete_seconds, vec![100, 100]);
    fs::remove_dir_all(directory).unwrap();
}

fn diagnostics_row(status: &str, automatic: bool, adopted: u64) -> Value {
    json!({
        "checkpoint_generation": adopted,
        "automatic": automatic,
        "trigger_names": if automatic { json!(["published_batches_soft"]) } else { json!(["harness_drain"]) },
        "attribution": {"database": vec![0xab_u8; 16], "fencing_generation": 3, "status": status,
            "adopt_end_at_ns": adopted, "live_install_end_at_ns": adopted + 1,
            "credits_returned_at_ns": adopted + 2},
    })
}

#[test]
fn generations_and_seconds_follow_the_python_contract() {
    let second = 1_000_000_000_u64;
    let start = 10 * second;
    let samples: Vec<Value> = (0..6)
        .filter(|index| *index != 4)
        .map(|index| {
            json!({"at_ns": start + index * second + 5, "rss_bytes": 100 + index, "databases": [
                {"db": "coordinator", "suffix_bytes": 10 * index, "suffix_batches": index,
                    "oldest_age_ns": 7}]})
        })
        .collect();
    let details = json!({
        "windows": [{"name": "timed", "start_ns": start, "end_ns": start + 6 * second + 9}],
        "samples_1s": samples,
        "generations": [
            diagnostics_row("Cleaned", true, start + 1),
            diagnostics_row("Cleaned", false, start + 2),
            diagnostics_row("Failed", true, start + 3),
            diagnostics_row("Cleaned", true, start + 7 * second),
        ],
        "ring": {"lost_unread_rows": 1},
        "spool": {"overflow_rows": 2},
        "sample_overflow": 0,
        "status": "incomplete",
    });
    let (rows, collector) = evidence::generations(&details);
    let kinds: Vec<_> = rows.iter().map(|row| row["kind"].clone()).collect();
    assert_eq!(
        kinds,
        [
            json!("normal"),
            json!("requested"),
            json!("failed"),
            json!("normal")
        ]
    );
    assert_eq!(rows[0]["database_id"], json!("ab".repeat(16)));
    assert_eq!(collector["lost_generations"], json!(3));
    assert_eq!(
        evidence::normal_generations_within(&rows, start, start + 6 * second),
        1
    );
    let seconds = evidence::timed_seconds(&details);
    assert_eq!(seconds["complete_seconds"], json!(6));
    assert_eq!(seconds["seconds"].as_array().unwrap().len(), 5);
    assert_eq!(seconds["missing_seconds"], json!([4]));
    assert_eq!(
        seconds["seconds"][2],
        json!({"second": 2, "age_ns": 7, "bytes": 20, "batches": 2})
    );
    assert_eq!(seconds["sampled_rss_peak_bytes"], json!(105));
    // The 5-row window above is too short for a debt verdict: fewer than
    // 8 complete seconds cannot decide growth without flipping on phase.
    assert!(evidence::debt(&seconds, 64 << 20).is_none());
    assert!(evidence::debt(&json!({"seconds": []}), 1).is_none());
}

#[test]
fn debt_judges_growth_over_at_least_eight_complete_seconds() {
    let rows = |ages: &[u64], bytes: &[u64]| {
        json!({"seconds": ages.iter().zip(bytes).map(|(age, byte)| json!({
            "age_ns": age, "bytes": byte, "batches": byte / 10})).collect::<Vec<_>>()})
    };
    let rising = rows(
        &[1, 2, 3, 4, 5, 6, 7, 8, 9],
        &[10, 20, 30, 40, 50, 60, 70, 80, 90],
    );
    assert_eq!(
        evidence::debt(&rising, 64 << 20).unwrap()["growing"],
        json!(true)
    );
    // Age falls while bytes rise below the soft trigger: not growing.
    let falling = rows(
        &[9, 8, 7, 6, 5, 4, 3, 2, 1],
        &[10, 20, 30, 40, 50, 60, 70, 80, 90],
    );
    assert_eq!(
        evidence::debt(&falling, 64 << 20).unwrap()["growing"],
        json!(false)
    );
    // Bytes rise past the soft trigger within the window: growing whatever
    // the age does.
    assert_eq!(
        evidence::debt(&falling, 10).unwrap()["growing"],
        json!(true)
    );
    // Seven samples: not enough for a verdict.
    let seven = rows(&[1, 2, 3, 4, 5, 6, 7], &[10, 20, 30, 40, 50, 60, 70]);
    assert!(evidence::debt(&seven, 64 << 20).is_none());
}

#[test]
fn final_value_domains_prefer_timed_then_pilot_and_admit_uncertain_puts() {
    let bitmap = || TouchedBitmap::new(8).unwrap();
    let domains = Domains {
        timed: bitmap(),
        timed_uncertain: bitmap(),
        pilot: bitmap(),
        pilot_uncertain: bitmap(),
    };
    domains.timed.set(1).unwrap();
    domains.pilot.set(1).unwrap();
    domains.pilot.set(2).unwrap();
    domains.timed_uncertain.set(3).unwrap();
    domains.pilot_uncertain.set(4).unwrap();
    domains.timed_uncertain.set(5).unwrap();
    domains.pilot.set(5).unwrap();
    let expect = |id| {
        expected_domains(
            id,
            &domains.timed,
            &domains.timed_uncertain,
            &domains.pilot,
            &domains.pilot_uncertain,
        )
        .unwrap()
    };
    assert_eq!(expect(0), (ValueDomain::Build, vec![]));
    assert_eq!(expect(1), (ValueDomain::Timed, vec![]));
    assert_eq!(expect(2), (ValueDomain::Pilot, vec![]));
    assert_eq!(expect(3), (ValueDomain::Build, vec![ValueDomain::Timed]));
    assert_eq!(expect(4), (ValueDomain::Build, vec![ValueDomain::Pilot]));
    assert_eq!(expect(5), (ValueDomain::Pilot, vec![ValueDomain::Timed]));
    let dataset = DatasetIdentity {
        key_namespace_hex: NAMESPACE.to_owned(),
        seed: 7,
        keys: 8,
        value_size: 1024,
        entropy: 1.0,
    };
    let value = |domain, id| dataset.value(domain, id).unwrap();
    let check = |id, value: Option<&[u8]>| domains.check(&dataset, id, value).unwrap();
    assert_eq!(
        check(1, Some(&value(ValueDomain::Timed, 1))),
        RowCheck::Correct
    );
    assert_eq!(
        check(1, Some(&value(ValueDomain::Pilot, 1))),
        RowCheck::Wrong
    );
    assert_eq!(
        check(3, Some(&value(ValueDomain::Timed, 3))),
        RowCheck::Ambiguous
    );
    assert_eq!(
        check(3, Some(&value(ValueDomain::Build, 3))),
        RowCheck::Correct
    );
    assert_eq!(check(0, None), RowCheck::Missing);
    assert_eq!(
        check(0, Some(&value(ValueDomain::Build, 1))),
        RowCheck::Wrong
    );
}

#[test]
fn superset_residency_qualifies_only_with_zero_resident_pages() {
    let measured = |selected_pages, resident_pages| Residency {
        selected_pages,
        resident_pages,
        ..Residency::default()
    };
    // 100 hot replay pages behind 10,000 cold unrelated pages: under 1 %,
    // but never cold.
    assert!(!superset_cold(&measured(10_100, 100)));
    assert_eq!(pages(&measured(10_100, 100)), Value::Null);
    assert!(!superset_cold(&measured(0, 0)));
    assert!(superset_cold(&measured(10_100, 0)));
    assert_eq!(
        pages(&measured(10_100, 0)),
        json!({"selected": 10_100, "resident": 0})
    );
}

#[test]
fn memory_cap_events_count_increases_only() {
    let start = json!({"memory_events": {"max": 2, "oom": 0, "oom_kill": 0}});
    let end = json!({"memory_events": {"max": 5, "oom": 1, "oom_kill": 0}});
    assert_eq!(evidence::cap_events(&start, &end), Some(4));
    assert_eq!(
        evidence::cap_events(&start, &json!({"memory_events": null})),
        None
    );
    let observed = evidence::memory();
    assert!(observed["vm_rss_bytes"].as_u64().is_some_and(|rss| rss > 0));
}

#[test]
fn owner_inventory_lists_host_databases_and_allocation() {
    let root = scratch("inventory");
    fs::create_dir_all(root.join(format!("db-{}", "ab".repeat(16)))).unwrap();
    fs::create_dir_all(root.join("dropped")).unwrap();
    fs::write(
        root.join(format!("db-{}/wal", "ab".repeat(16))),
        vec![1_u8; 8192],
    )
    .unwrap();
    assert_eq!(evidence::host_databases(&root).unwrap(), ["ab".repeat(16)]);
    let total = evidence::tree_allocation(&root).unwrap();
    assert_eq!((total.files, total.bytes), (1, 8192));
    assert!(total.allocated_bytes >= 8192);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn receipts_bind_the_stage_and_refuse_reruns() {
    let case_root = scratch("registry");
    let stage = parse(local_stage(&case_root)).unwrap();
    let output = case_root.join("out-build");
    fs::create_dir_all(&output).unwrap();
    let context = Context {
        stage,
        stage_sha256: "5".repeat(64),
        executable_sha256: "6".repeat(64),
        phase: Phase::Build,
        output: output.clone(),
    };
    begin(&context).unwrap();
    assert!(begin(&context).is_err(), "a started phase never reruns");
    let mut report = PhaseReport::failed("probe".to_owned());
    report.status = Status::Complete;
    report.error = None;
    report.database = Some([0xcd; 16]);
    report.artifacts = vec![(PILOT_TOUCHED.to_owned(), vec![1, 2, 3])];
    finish(&context, report).unwrap();
    let receipt = evidence::read_json(&output.join("build-receipt.json")).unwrap();
    assert_eq!(receipt["schema_version"], json!(1));
    assert_eq!(receipt["input_sha256"], json!("5".repeat(64)));
    assert_eq!(receipt["executable_sha256"], json!("6".repeat(64)));
    assert_eq!(receipt["database_id"], json!("cd".repeat(16)));
    assert_eq!(receipt["mode"], json!("local_fixture_proof"));
    assert_eq!(
        receipt["artifacts"][0]["sha256"],
        json!(sha256_hex(&[1, 2, 3]))
    );
    assert_eq!(context.database().unwrap(), [0xcd; 16]);
    let pilot = Context {
        phase: Phase::Pilot,
        ..context
    };
    let (_, sha) = pilot.receipt(Phase::Build).unwrap();
    assert_eq!(
        predecessors(&pilot).unwrap(),
        [predecessor_entry(Phase::Build, &sha)]
    );
    let other = Context {
        stage_sha256: "7".repeat(64),
        ..pilot
    };
    assert!(other.receipt(Phase::Build).is_err(), "another stage input");
    fs::remove_dir_all(case_root).unwrap();
}
