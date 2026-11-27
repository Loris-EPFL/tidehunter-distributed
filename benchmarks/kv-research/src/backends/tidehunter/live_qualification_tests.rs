use super::*;

#[test]
fn local_file_qualification_persists_and_recovers_real_frames() {
    let guard = tempfile::TempDir::with_prefix("live-file-qualification-").expect("test tree");
    let root = guard.path().to_path_buf();
    let input = root.join("input.json");
    fs::write(
        &input,
        serde_json::to_vec(&serde_json::json!({
            "coordinator_path": root.join("stream"),
            "namespace": [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1],
            "expected_route": "local_file",
            "durability_tier": "synchronous",
            "value_bytes": 256
        }))
        .unwrap(),
    )
    .unwrap();
    let output = root.join("result");
    run(&input, &output).unwrap();
    let proof: serde_json::Value =
        serde_json::from_slice(&fs::read(output.join("qualification.json")).unwrap()).unwrap();
    assert_eq!(proof["kind"], "live_nvmeof_file_component_qualification");
    assert_eq!(proof["frame_count"], 3);
    assert_eq!(proof["owner_validate_persist_retry_recovery"], true);
}

#[test]
fn qualification_requires_explicit_valid_tier() {
    let base = serde_json::json!({
        "coordinator_path": "/unused",
        "namespace": vec![1; 16],
        "expected_route": "local_file",
        "value_bytes": 256
    });
    assert!(serde_json::from_value::<Input>(base.clone()).is_err());
    for tier in ["native", "synchronous", "invalid"] {
        let mut value = base.clone();
        value["durability_tier"] = serde_json::json!(tier);
        let input: Input = serde_json::from_value(value).unwrap();
        assert_eq!(validate_tier(&input).is_ok(), tier != "invalid");
    }
}

#[test]
fn qualification_rejects_fixture_fences_and_unverified_routes() {
    let mut sample = serde_json::json!({
        "route_evidence_dropped": 0, "fence_evidence_dropped": 0,
        "routes": [{
            "transport":"rdma", "address":"10.0.0.2", "service":4420,
            "namespace_identity":{"namespace_path":"/dev/nvme0n1",
                "nqn":"nqn.2026-01.test:wal", "nsid":1, "bytes":4096,
                "controller_instance":0, "block_size":4096, "uuid":[1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
                "controller":{"queue_count":2, "sqsize":31, "sysfs_device":22, "sysfs_inode":9}}
        }],
        "fences":[{"partition":0,"authority_epoch":2,
            "target":{"fixture_only":false,"fence_count":1,
                "write_protect_successes":1,"errors":0}}]
    });
    assert!(verify_routes(&sample, "rdma").is_ok());
    assert!(verify_routes(&sample, "tcp").is_err());
    assert!(verify_fences(&sample, &[0]).is_ok());
    sample["fences"][0]["target"]["fixture_only"] = serde_json::json!(true);
    assert!(verify_fences(&sample, &[0]).is_err());
    sample["fences"][0]["target"]["fixture_only"] = serde_json::json!(false);
    sample["fences"][0]["target"]["write_protect_successes"] = serde_json::json!(0);
    assert!(verify_fences(&sample, &[0]).is_err());
    sample["fences"][0]["target"]["write_protect_successes"] = serde_json::json!(1);
    sample["fences"][0]["target"]["fence_count"] = serde_json::json!(0);
    assert!(verify_fences(&sample, &[0]).is_err());
    sample["fences"][0]["target"]["fence_count"] = serde_json::json!(1);
    assert!(verify_fences(&sample, &[0]).is_ok());
    sample["route_evidence_dropped"] = serde_json::json!(1);
    assert!(verify_routes(&sample, "rdma").is_err());
    sample["route_evidence_dropped"] = serde_json::json!(0);
    let record = sample["routes"][0]["namespace_identity"]["controller"].clone();
    for key in ["queue_count", "sqsize", "sysfs_device", "sysfs_inode"] {
        for bad in [serde_json::json!(0), serde_json::Value::Null] {
            let mut broken = sample.clone();
            broken["routes"][0]["namespace_identity"]["controller"][key] = bad;
            assert!(verify_routes(&broken, "rdma").is_err(), "{key} accepted");
        }
        let mut missing = sample.clone();
        missing["routes"][0]["namespace_identity"]["controller"]
            .as_object_mut()
            .unwrap()
            .remove(key);
        assert!(verify_routes(&missing, "rdma").is_err(), "{key} optional");
    }
    sample["routes"][0]["namespace_identity"]["controller"] = serde_json::Value::Null;
    assert!(verify_routes(&sample, "rdma").is_err());
    sample["routes"][0]["namespace_identity"]["controller"] = record;
    assert!(verify_routes(&sample, "rdma").is_ok());
}

#[test]
fn progress_events_carry_elapsed_time_and_detail() {
    let guard = tempfile::TempDir::with_prefix("live-qualification-progress-").expect("test tree");
    let root = guard.path().to_path_buf();
    let mut progress = QualificationProgress {
        directory: root.clone(),
        file_name: "progress.json",
        started: Instant::now(),
        events: Vec::new(),
    };
    progress.mark_value("phase", None);
    progress.record(
        "phase_writes",
        None,
        serde_json::json!({ "writes_acknowledged": 8 }),
    );
    let document: serde_json::Value =
        serde_json::from_slice(&fs::read(root.join("progress.json")).unwrap()).unwrap();
    let events = document["events"].as_array().unwrap();
    assert_eq!(events.len(), 2);
    assert!(events.iter().all(|event| event["elapsed_ms"].is_u64()));
    assert!(events[0]["detail"].is_null());
    assert_eq!(events[1]["phase"], "phase_writes");
    assert_eq!(events[1]["detail"]["writes_acknowledged"], 8);
}

#[test]
fn adoption_window_counts_only_the_child_initiator_controller() {
    let window = AdoptionWindow {
        nqns: ["nqn.2026-01.test:sub".to_string()].into(),
        host_nqns: ["nqn.2026-01.test:p0-host".to_string()].into(),
        services: ["addr 127.0.0.1:4420".to_string()].into(),
        instances: ["nvme3".to_string()].into(),
    };
    assert!(
        window.is_churn(
            "nvmet: Created nvm controller 2 for subsystem nqn.2026-01.test:sub for NQN nqn.2026-01.test:p0-host"
        ),
        "the target-side create of the child's initiator is churn"
    );
    assert!(
        window.is_churn(r#"nvme nvme3: new ctrl: NQN "nqn.2026-01.test:sub", addr 127.0.0.1:4420"#),
        "a public host connect of the child's subsystem is churn"
    );
    assert!(
        window.is_churn(r#"nvme nvme3: Removing ctrl: NQN "nqn.2026-01.test:sub""#),
        "removing the child's public controller is churn"
    );
    assert!(
        !window.is_churn(
            "nvmet: Created nvm controller 3 for subsystem nqn.2026-01.test:sub for NQN nqn.2026-01.test:commission"
        ),
        "the owner's commissioning controller uses another host NQN"
    );
    assert!(
        !window
            .is_churn(r#"nvme nvme7: new ctrl: NQN "nqn.2026-01.test:sub", addr 127.0.0.1:4421"#),
        "the owner's commissioning controller uses another service"
    );
    assert!(
        !window.is_churn(r#"nvme nvme7: Removing ctrl: NQN "nqn.2026-01.test:sub""#),
        "the owner's commissioning controller uses another host instance"
    );
    assert!(
        !window.is_churn(r#"nvme nvme3: Removing ctrl: NQN "nqn.2026-01.test:other""#),
        "another subsystem's churn is not the child's"
    );
}

#[test]
fn controller_loss_is_opt_in_and_never_implicit() {
    let base = serde_json::json!({
        "coordinator_path": "/unused",
        "namespace": vec![1; 16],
        "expected_route": "tcp",
        "durability_tier": "native",
        "value_bytes": 256
    });
    let input: Input = serde_json::from_value(base.clone()).unwrap();
    assert!(
        !input.controller_loss,
        "a caller that does not supervise the loss must not request it"
    );
    let mut enabled = base.clone();
    enabled["controller_loss"] = serde_json::json!(true);
    let input: Input = serde_json::from_value(enabled).unwrap();
    assert!(input.controller_loss, "the supervisor opts in explicitly");
    let mut unknown = base;
    unknown["controller-loss"] = serde_json::json!(true);
    assert!(
        serde_json::from_value::<Input>(unknown).is_err(),
        "an unknown key stays rejected"
    );
}

#[test]
fn controller_loss_targets_the_current_writer_route() {
    let mut sample = serde_json::json!({
        "route_evidence_dropped": 0,
        "routes": [
            {"transport": "tcp", "address": "10.0.0.2", "service": 4420, "writer": null,
             "namespace_identity": {"nqn": "nqn.2026-01.test:read", "nsid": 1,
                 "uuid": [9,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]}},
            {"transport": "tcp", "address": "10.0.0.2", "service": 4420,
             "writer": {"epoch": 1, "file_index": 0},
             "namespace_identity": {"nqn": "nqn.2026-01.test:sub", "nsid": 1,
                 "uuid": [1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]}},
            {"transport": "tcp", "address": "10.0.0.2", "service": 4420,
             "writer": {"epoch": 1, "file_index": 1},
             "namespace_identity": {"nqn": "nqn.2026-01.test:sub", "nsid": 2,
                 "uuid": [2,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]}}
        ]
    });
    // The member transition leaves two writer routes; the newest one is the
    // member the write path opened last.
    let route = current_write_route(&sample).expect("the last writer route is current");
    assert_eq!(
        route["writer"]["file_index"], 1,
        "the post-transition member is the current one"
    );
    assert_eq!(
        route["namespace_identity"]["nsid"], 2,
        "the current route names the transitioned member"
    );
    // A read route is never a controller-loss target.
    let read_only = sample["routes"][0].clone();
    sample["routes"] = serde_json::json!([read_only]);
    assert!(
        current_write_route(&sample).is_err(),
        "a read route does not name a write controller"
    );
    // Dropped evidence breaks the append order, so the selection is refused.
    sample["routes"] = serde_json::json!([]);
    sample["route_evidence_dropped"] = serde_json::json!(1);
    assert!(
        current_write_route(&sample).is_err(),
        "dropped route evidence cannot name the current writer"
    );
}

#[test]
fn read_controller_loss_targets_the_current_read_route() {
    let read = |nqn: &str, nsid: u64| {
        serde_json::json!({
            "transport": "tcp", "address": "10.0.0.2", "service": 4420,
            "writer": null,
            "namespace_identity": {"nqn": nqn, "nsid": nsid}
        })
    };
    let mut sample = serde_json::json!({
        "route_evidence_dropped": 0,
        "routes": [
            {"transport": "tcp", "address": "10.0.0.2", "service": 4420,
             "writer": {"epoch": 1, "file_index": 0},
             "namespace_identity": {"nqn": "nqn.2026-01.test:sub", "nsid": 1}},
            read("nqn.2026-01.test:read-a", 1),
            read("nqn.2026-01.test:read-b", 2)
        ]
    });
    // A reconnect appends a route: the newest read route is the live one.
    let route = current_read_route(&sample).expect("the newest read route is current");
    assert_eq!(
        route["namespace_identity"]["nqn"], "nqn.2026-01.test:read-b",
        "the read loss targets the controller the read path opened last"
    );
    assert_eq!(
        route["namespace_identity"]["nsid"], 2,
        "the current route names the member the read path opened last"
    );
    // A writer route is never a read-loss target.
    let writer_only = sample["routes"][0].clone();
    sample["routes"] = serde_json::json!([writer_only]);
    assert!(
        current_read_route(&sample).is_err(),
        "a writer-only snapshot names no read controller"
    );
    // Dropped evidence breaks the append order, so the selection is refused.
    sample["routes"] = serde_json::json!([read("nqn.2026-01.test:read-a", 1)]);
    sample["route_evidence_dropped"] = serde_json::json!(1);
    assert!(
        current_read_route(&sample).is_err(),
        "dropped route evidence cannot name the current read controller"
    );
}
