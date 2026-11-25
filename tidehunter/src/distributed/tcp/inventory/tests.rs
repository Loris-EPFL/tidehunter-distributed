use super::*;
use crate::distributed::tcp::tests::{config, metrics};

#[cfg(all(target_os = "linux", feature = "test-utils"))]
#[test]
fn benchmark_eviction_includes_sealed_source_epochs() {
    let temporary = tempdir::TempDir::new("node-evict-retained").unwrap();
    let config = config(&temporary.path().join("node"));
    let mut inventory = NodeInventory::open(config.clone(), metrics()).unwrap();
    inventory.fence_to(1).unwrap();
    let batch = crate::distributed::tcp::tests::batch(&config, 1, 0);
    let source = inventory.active(1).unwrap();
    let receipt = source
        .append_complete_batch(RequestAuthority { epoch: 1 }, &batch)
        .unwrap();
    source.persist_through(receipt).unwrap();
    drop(source);
    inventory.seal(1).unwrap();
    inventory.fence_to(2).unwrap();
    let source_bytes = inventory.logs[&1]
        .read_evidence_for_benchmark(false)
        .unwrap()
        .file_bytes;
    let expected: u64 = inventory
        .logs
        .values()
        .map(|log| log.read_evidence_for_benchmark(false).unwrap().file_bytes)
        .sum();
    assert!(source_bytes > 0);
    let evidence = inventory.read_evidence_for_benchmark(2, true).unwrap();
    assert_eq!(evidence.file_bytes, expected);
    assert!(inventory.read_evidence_for_benchmark(1, true).is_err());
}

/// Write `value` as `file` in a closed owner root and check that open
/// fails with legacy `mode` before any mutation.
fn assert_legacy_open_rejected(file: &str, value: serde_yaml::Value, mode: &str) {
    let temporary = tempdir::TempDir::new("node-legacy-state").unwrap();
    let config = config(&temporary.path().join("node"));
    let mut inventory = NodeInventory::open(config.clone(), metrics()).unwrap();
    inventory.fence_to(1).unwrap();
    drop(inventory);
    install_metadata(&config.root, file, &encode_metadata(&value).unwrap()).unwrap();
    let listing = || {
        let mut names: Vec<_> = fs::read_dir(&config.root)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        names.sort();
        names
    };
    let (before, manifest) = (listing(), fs::read(config.root.join(MANIFEST)).unwrap());
    let error = match NodeInventory::open(config.clone(), metrics()) {
        Ok(_) => panic!("legacy owner state {mode} opened"),
        Err(error) => error.to_string(),
    };
    assert!(
        error.contains(&format!(
            "unsupported legacy distributed mode {mode}: use archive/multi-design"
        )),
        "{error}"
    );
    assert_eq!(listing(), before);
    assert_eq!(fs::read(config.root.join(MANIFEST)).unwrap(), manifest);
}

fn legacy_marker(spdk: serde_yaml::Value, export_mode: Option<&str>) -> serde_yaml::Value {
    let mut config = serde_yaml::Mapping::new();
    config.insert("address".into(), "127.0.0.1".into());
    config.insert("spdk".into(), spdk);
    if let Some(mode) = export_mode {
        config.insert("export_mode".into(), mode.into());
    }
    let mut marker = serde_yaml::Mapping::new();
    marker.insert("config".into(), config.into());
    marker.insert("identity".into(), serde_yaml::Mapping::new().into());
    marker.into()
}

#[test]
fn sealed_export_admission_fails_open_before_mutation() {
    let entry: serde_yaml::Value = serde_yaml::from_str("{owner_epoch: 1}").unwrap();
    for (entries, pins) in [(vec![entry.clone()], vec![]), (vec![], vec![entry])] {
        let mut registry = serde_yaml::Mapping::new();
        registry.insert("version".into(), 1.into());
        registry.insert("entries".into(), entries.into());
        registry.insert("epoch_pins".into(), pins.into());
        assert_legacy_open_rejected(LEGACY_EXPORT_LEASES, registry.into(), "sealed_export");
    }
}

#[test]
fn empty_sealed_export_registry_is_ignored() {
    let temporary = tempdir::TempDir::new("node-empty-registry").unwrap();
    let config = config(&temporary.path().join("node"));
    drop(NodeInventory::open(config.clone(), metrics()).unwrap());
    let mut registry = serde_yaml::Mapping::new();
    registry.insert("version".into(), 1.into());
    registry.insert("entries".into(), serde_yaml::Sequence::new().into());
    install_metadata(
        &config.root,
        LEGACY_EXPORT_LEASES,
        &encode_metadata(&registry).unwrap(),
    )
    .unwrap();
    assert!(NodeInventory::open(config, metrics()).is_ok());
}

#[test]
fn spdk_live_target_marker_fails_open_before_mutation() {
    let spdk: serde_yaml::Value =
        serde_yaml::from_str("{rpc_socket: /run/spdk.sock, executable: /usr/bin/spdk}").unwrap();
    assert_legacy_open_rejected(
        LIVE_TARGET_CONFIG,
        legacy_marker(spdk, Some("on_demand")),
        "spdk",
    );
}

#[test]
fn on_demand_and_per_file_live_target_markers_fail_open_before_mutation() {
    for mode in ["on_demand", "per_file"] {
        assert_legacy_open_rejected(
            LIVE_TARGET_CONFIG,
            legacy_marker(serde_yaml::Value::Null, Some(mode)),
            mode,
        );
    }
}

#[test]
fn pre_refactor_per_owner_live_target_marker_fails_open_before_mutation() {
    for export_mode in [Some("per_owner"), None] {
        assert_legacy_open_rejected(
            LIVE_TARGET_CONFIG,
            legacy_marker(serde_yaml::Value::Null, export_mode),
            "pre_refactor_live_target_marker",
        );
    }
}
#[test]
fn interrupted_preparing_epoch_is_retained_and_never_activated() {
    let temporary = tempdir::TempDir::new("node-preparing-fence").unwrap();
    let config = config(&temporary.path().join("node"));
    let mut inventory = NodeInventory::open(config.clone(), metrics()).unwrap();
    inventory.fence_to(1).unwrap();
    inventory.seal(1).unwrap();
    inventory.manifest.highest_fenced_generation = 2;
    inventory.manifest.epochs.push(Epoch {
        epoch: 2,
        state: EpochState::Preparing,
    });
    persist(&config.root, &inventory.manifest).unwrap();
    // Simulate interrupted creation after the durable reservation, before
    // any registered stream or active reply could become eligible.
    fs::create_dir(epoch_path(&config.root, 2)).unwrap();
    drop(inventory);
    let mut inventory = NodeInventory::open(config.clone(), metrics()).unwrap();
    let status = inventory.status().unwrap();
    assert_eq!(status.highest_fenced_generation, 2);
    assert_eq!(status.active_epoch, None);
    assert!(
        inventory
            .manifest
            .epochs
            .iter()
            .map(|e| (e.epoch, e.state == EpochState::Active))
            .eq([(1, false), (2, false)])
    );
    assert!(inventory.manifest.epochs[1].state == EpochState::Abandoned);
    assert!(inventory.fence_to(2).is_err());
    inventory.fence_to(3).unwrap();
    assert!(epoch_path(&config.root, 2).is_dir());
}

#[test]
fn surviving_log_retains_node_root_lock() {
    let temporary = tempdir::TempDir::new("node-root-lock").unwrap();
    let config = config(&temporary.path().join("node"));
    let mut inventory = NodeInventory::open(config.clone(), metrics()).unwrap();
    inventory.fence_to(1).unwrap();
    let log = inventory.active(1).unwrap();
    drop(inventory);
    assert!(NodeInventory::open(config.clone(), metrics()).is_err());
    drop(log);
    let inventory = NodeInventory::open(config, metrics()).unwrap();
    assert_eq!(inventory.status().unwrap().active_epoch, None);
}
