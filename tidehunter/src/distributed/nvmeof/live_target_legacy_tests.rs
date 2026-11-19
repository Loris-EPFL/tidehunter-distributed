//! A live-target registry written before the refactor names its removed mode
//! instead of failing as a corrupt file.
use super::*;
use tempdir::TempDir;

fn rewrite_bindings(state: &Path, spdk: serde_json::Value) {
    let bytes = fs::read(state).unwrap();
    let mut registry: serde_json::Value = serde_json::from_slice(&bytes[4..]).unwrap();
    let exports = registry["exports"].as_array_mut().unwrap();
    assert!(!exports.is_empty(), "the registry must hold an export");
    for export in exports {
        export["binding"]
            .as_object_mut()
            .unwrap()
            .insert("spdk".into(), spdk.clone());
    }
    let payload = serde_json::to_vec(&registry).unwrap();
    let mut out = crc32fast::hash(&payload).to_le_bytes().to_vec();
    out.extend_from_slice(&payload);
    fs::write(state, out).unwrap();
}

fn reopen_error(spdk: serde_json::Value) -> io::Error {
    let tmp = TempDir::new("live-target-legacy").unwrap();
    let state = tmp.path().join("state");
    let configfs = tmp.path().join("configfs");
    fs::create_dir(&state).unwrap();
    fs::create_dir(&configfs).unwrap();
    for dir in ["hosts", "ports", "subsystems"] {
        fs::create_dir(configfs.join(dir)).unwrap();
    }
    let path = tmp.path().join("wal");
    fs::write(&path, vec![7u8; 4096]).unwrap();
    let metadata = fs::metadata(&path).unwrap();
    let file = LiveFileIdentity {
        path,
        device: metadata.dev(),
        inode: metadata.ino(),
        bytes: metadata.len(),
        block_size: 4096,
    };
    let config = LiveTargetConfig {
        address: "127.0.0.1".into(),
        service: 4420,
        transport: NvmeofTransport::Tcp,
        host_nqn: "nqn.2026-01.local.test:host".into(),
        port_id: 42,
        state_dir: state.clone(),
        commissioning_port_id: 43,
        commissioning_service: 4421,
    };
    let mut manager =
        LiveTargetManager::open_inner(config.clone(), configfs.clone(), true, None, None).unwrap();
    let identity = manager.identity().unwrap();
    manager.export_writable(file).unwrap();
    drop(manager);
    rewrite_bindings(&state.join(STATE), spdk);
    match LiveTargetManager::open_inner(config, configfs, true, Some(identity), None) {
        Ok(_) => panic!("a pre-refactor registry was accepted"),
        Err(error) => error,
    }
}

#[test]
fn pre_refactor_registry_with_null_spdk_names_legacy_mode() {
    let error = reopen_error(serde_json::Value::Null);
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert_eq!(
        error.to_string(),
        "unsupported legacy distributed mode pre_refactor_live_target_registry: use archive/multi-design"
    );
}

#[test]
fn registry_with_spdk_target_names_spdk_mode() {
    let error = reopen_error(serde_json::json!({ "bdev": "legacy" }));
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert_eq!(
        error.to_string(),
        "unsupported legacy distributed mode spdk: use archive/multi-design"
    );
}
