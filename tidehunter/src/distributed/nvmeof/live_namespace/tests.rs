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

/// A controller the kernel is deleting is a typed lost cause, not an identity
/// mismatch: the CI cell's namespace open saw exactly this state while the
/// node, `wwid`, `uuid` and `nsid` still matched.
#[test]
fn a_deleting_controller_is_the_typed_lost_cause() {
    let root = tempdir::TempDir::new("live-verify-state").unwrap();
    let nqn = "nqn.2026-01.test:sub";
    let nvme = root.path().join("class-nvme");
    let controller = nvme.join("nvme1");
    std::fs::create_dir_all(controller.join("nvme1n1")).unwrap();
    std::fs::write(controller.join("subsysnqn"), format!("{nqn}\n")).unwrap();
    std::fs::write(controller.join("transport"), "tcp\n").unwrap();
    std::fs::write(controller.join("state"), "deleting\n").unwrap();
    let sysfs = controller.join("nvme1n1");
    let error = verify_transport_at(
        &sysfs,
        nqn,
        NvmeofTransport::Tcp,
        &nvme,
        &root.path().join("class-nvme-subsystem"),
    )
    .expect_err("a deleting controller cannot serve a namespace");
    assert!(
        controller_lost(&error),
        "the deleting state is the typed lost cause: {error}"
    );
    assert_eq!(
        error.kind(),
        io::ErrorKind::ConnectionAborted,
        "a lost controller is the resumable class: {error}"
    );

    // The same identity on a live controller keeps the fatal class.
    std::fs::write(controller.join("state"), "live\n").unwrap();
    std::fs::write(controller.join("subsysnqn"), "nqn.2026-01.test:other\n").unwrap();
    let error = verify_transport_at(
        &sysfs,
        nqn,
        NvmeofTransport::Tcp,
        &nvme,
        &root.path().join("class-nvme-subsystem"),
    )
    .expect_err("a live controller with another NQN is a real mismatch");
    assert!(
        !controller_lost(&error),
        "a live identity mismatch is fatal: {error}"
    );
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);

    // A vanished controller is lost, never an identity mismatch.
    std::fs::remove_dir_all(&controller).unwrap();
    let error = verify_transport_at(
        &sysfs,
        nqn,
        NvmeofTransport::Tcp,
        &nvme,
        &root.path().join("class-nvme-subsystem"),
    )
    .expect_err("a vanished controller cannot resolve its namespace");
    assert!(
        controller_lost(&error),
        "a vanished controller is the lost cause: {error}"
    );
}
