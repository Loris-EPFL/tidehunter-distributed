use super::*;
#[test]
fn production_open_rejects_regular_file() {
    let dir = tempdir::TempDir::new("live-binding").unwrap();
    let path = dir.path().join("wal");
    let file = File::create(&path).unwrap();
    file.set_len(4096).unwrap();
    let binding = LiveNamespaceBinding {
        namespace_path: path.clone(),
        transport: NvmeofTransport::Tcp,
        nqn: "nqn.2026-01.test:wal".to_owned(),
        uuid: [1; 16],
        nsid: 1,
        bytes: 4096,
        block_size: 4096,
        access: LiveNamespaceAccess::ReadWrite,
    };
    assert!(LiveNamespace::open(&binding).is_err());
    assert!(
        LiveNamespace::open_local_file(&path, 4096, 4096, LiveNamespaceAccess::ReadOnly).is_ok()
    );
    assert!(
        LiveNamespace::open_local_file(&path, 8192, 4096, LiveNamespaceAccess::ReadOnly).is_err()
    );
}

#[test]
fn access_policy_keeps_kernel_protection_and_requires_host_access() {
    use LiveNamespaceAccess::{ReadOnly, ReadWrite};
    assert!(verify_access_policy(ReadOnly, true, true).is_ok());
    assert!(verify_access_policy(ReadOnly, true, false).is_err());
    assert!(verify_access_policy(ReadWrite, false, false).is_ok());
    assert!(verify_access_policy(ReadWrite, false, true).is_err());
    assert!(verify_access_policy(ReadOnly, false, true).is_err());
    assert!(verify_access_policy(ReadWrite, true, false).is_err());
    for (host_readonly, identify) in [(true, true), (false, false), (false, true)] {
        assert!(
            verify_access_policy(LiveNamespaceAccess::ReadShared, host_readonly, identify).is_ok()
        );
    }
}
