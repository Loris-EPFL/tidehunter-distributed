//! A protocol-35 owner journal is refused instead of being recovered from.
use super::*;
use crate::distributed::tcp::host_identity::effective_host_nqn;
use tempdir::TempDir;

#[test]
fn legacy_journal_is_refused() {
    let tmp = TempDir::new("live-target-legacy").unwrap();
    let state = tmp.path().join("state");
    let configfs = tmp.path().join("configfs");
    fs::create_dir(&state).unwrap();
    fs::create_dir(&configfs).unwrap();
    for dir in ["hosts", "ports", "subsystems"] {
        fs::create_dir(configfs.join(dir)).unwrap();
    }
    fs::write(state.join(LEGACY_STATE), b"{\"version\":1}").unwrap();
    let config = LiveTargetConfig {
        address: "127.0.0.1".into(),
        service: 4420,
        transport: NvmeofTransport::Tcp,
        host_nqn: effective_host_nqn(&[1; 16], 0),
        port_id: 42,
        state_dir: state,
        commissioning_port_id: 43,
        commissioning_service: 4421,
    };
    let error = match LiveTargetManager::open_inner(config, configfs, true, None, None) {
        Ok(_) => panic!("a legacy target journal was accepted"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert_eq!(
        error.to_string(),
        "unsupported legacy live target journal live-target.json: shut the owner down with protocol 35 (ShutdownLiveTarget), then remove the live-target state directory"
    );
}
