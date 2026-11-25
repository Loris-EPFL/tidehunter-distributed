//! Admission and shutdown tests of the local latency replay fixture.
use super::*;

fn driver(extra: &str) -> Driver {
    let document = format!(
        r#"{{"manifest_path":"/x","manifest_sha256":"00","repetitions":1,
            "concurrency":1{extra}}}"#
    );
    serde_json::from_str(&document).expect("test driver parses")
}

fn rejection(extra: &str) -> String {
    driver(extra)
        .validate()
        .expect_err("driver must be rejected")
        .to_string()
}

#[test]
fn driver_defaults_to_one_owner_native_tier_and_engine_durable_cut() {
    let driver = driver(r#","node_databases":[0]"#);
    assert_eq!(driver.owners, 1, "default owner count: {}", driver.owners);
    assert_eq!(
        driver.tier,
        DurabilityTier::Native,
        "default tier: {:?}",
        driver.tier
    );
    assert!(
        driver.durable_cut.is_none(),
        "default durable_cut: {:?}",
        driver.durable_cut
    );
    driver.validate().expect("default driver is valid");
}

#[test]
fn driver_with_zero_owners_is_rejected() {
    let error = rejection(r#","node_databases":[0],"owners":0"#);
    assert!(
        error.contains("owners must be within"),
        "unexpected refusal: {error}"
    );
}

#[test]
fn driver_with_more_owners_than_a_slot_range_is_rejected() {
    let extra = format!(r#","node_databases":[0],"owners":{}"#, MAX_PARTITIONS + 1);
    let error = rejection(&extra);
    assert!(
        error.contains("owners must be within"),
        "unexpected refusal: {error}"
    );
}

#[test]
fn driver_with_a_node_database_above_the_slot_range_is_rejected() {
    let extra = format!(r#","node_databases":[{}]"#, MAX_NODE_DATABASE_ID + 1);
    let error = rejection(&extra);
    assert!(
        error.contains("strictly increasing ids"),
        "unexpected refusal: {error}"
    );
}

#[test]
fn driver_with_repeated_or_empty_node_databases_is_rejected() {
    for extra in [r#","node_databases":[1,1]"#, r#","node_databases":[]"#] {
        let error = rejection(extra);
        assert!(
            error.contains("strictly increasing ids"),
            "{extra}: unexpected refusal: {error}"
        );
    }
}

#[test]
fn driver_with_a_zero_durable_cut_lag_is_rejected() {
    let extra = r#","node_databases":[0],"durable_cut":{"lag_ms":0,"lag_bytes":1}"#;
    let error = rejection(extra);
    assert!(error.contains("durable_cut"), "unexpected refusal: {error}");
}

#[test]
fn driver_with_zero_concurrency_or_repetitions_is_rejected() {
    let mut zero_concurrency = driver(r#","node_databases":[0]"#);
    zero_concurrency.concurrency = 0;
    let error = zero_concurrency
        .validate()
        .expect_err("zero concurrency must be rejected");
    assert!(
        error.to_string().contains("concurrency"),
        "unexpected refusal: {error}"
    );
    let mut zero_repetitions = driver(r#","node_databases":[0]"#);
    zero_repetitions.repetitions = 0;
    let error = zero_repetitions
        .validate()
        .expect_err("zero repetitions must be rejected");
    assert!(
        error.to_string().contains("repetitions"),
        "unexpected refusal: {error}"
    );
}

#[test]
fn driver_at_the_owner_and_node_database_bounds_is_accepted() {
    let extra =
        format!(r#","node_databases":[0,{MAX_NODE_DATABASE_ID}],"owners":{MAX_PARTITIONS}"#);
    driver(&extra).validate().expect("bounds are inclusive");
}

fn owners_with(servers: Vec<JoinHandle<Result<(), TcpError>>>) -> Owners {
    Owners {
        stop: Arc::new(AtomicBool::new(false)),
        servers,
    }
}

#[test]
fn stop_joins_every_owner_and_reports_each_failure_by_partition() {
    let failing = std::thread::spawn(|| Err(TcpError::from(std::io::Error::other("bind lost"))));
    let panicking = std::thread::spawn(|| -> Result<(), TcpError> { panic!("reactor exploded") });
    let healthy = std::thread::spawn(|| Ok(()));
    let owners = owners_with(vec![healthy, failing, panicking]);
    let stop = Arc::clone(&owners.stop);
    let failures = owners.stop();
    assert!(
        stop.load(Ordering::Acquire),
        "stop must raise the shutdown flag"
    );
    assert_eq!(
        failures.len(),
        2,
        "one failure per unhealthy owner, got {failures:?}"
    );
    assert!(
        failures[0].starts_with("owner 1: serve failed:") && failures[0].contains("bind lost"),
        "unexpected first failure: {}",
        failures[0]
    );
    assert!(
        failures[1].starts_with("owner 2: server thread panicked:")
            && failures[1].contains("reactor exploded"),
        "unexpected second failure: {}",
        failures[1]
    );
}

#[test]
fn stop_with_only_healthy_owners_reports_nothing() {
    let owners = owners_with(vec![std::thread::spawn(|| Ok(()))]);
    let failures = owners.stop();
    assert!(failures.is_empty(), "unexpected failures: {failures:?}");
}
