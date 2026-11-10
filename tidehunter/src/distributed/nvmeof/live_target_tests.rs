//! These tests exercise the journal and ownership state machine only; ordinary
//! files are not evidence that a kernel nvmet namespace drains target I/O.
use super::*;
use tempdir::TempDir;

struct Fixture {
    _tmp: TempDir,
    configfs: PathBuf,
    config: LiveTargetConfig,
    file: LiveFileIdentity,
}
impl Fixture {
    fn new() -> Self {
        let tmp = TempDir::new("live-target").unwrap();
        let state = tmp.path().join("state");
        let configfs = tmp.path().join("configfs");
        fs::create_dir(&state).unwrap();
        fs::create_dir(&configfs).unwrap();
        for dir in ["hosts", "ports", "subsystems"] {
            fs::create_dir(configfs.join(dir)).unwrap();
        }
        let path = tmp.path().join("wal");
        fs::write(&path, vec![7u8; 4096]).unwrap();
        let m = fs::metadata(&path).unwrap();
        let file = LiveFileIdentity {
            path,
            device: m.dev(),
            inode: m.ino(),
            bytes: m.len(),
            block_size: 4096,
        };
        let config = LiveTargetConfig {
            address: "127.0.0.1".into(),
            service: 4420,
            transport: NvmeofTransport::Tcp,
            host_nqn: "nqn.2026-01.local.test:host".into(),
            port_id: 42,
            state_dir: state,
            commissioning_port_id: 43,
            commissioning_service: 4421,
            spdk: None,
        };
        Self {
            _tmp: tmp,
            configfs,
            config,
            file,
        }
    }
    fn open(&self) -> io::Result<LiveTargetManager> {
        LiveTargetManager::open_inner(self.config.clone(), self.configfs.clone(), true, None)
    }
    fn open_existing(&self, identity: LiveTargetIdentity) -> io::Result<LiveTargetManager> {
        LiveTargetManager::open_inner(
            self.config.clone(),
            self.configfs.clone(),
            true,
            Some(identity),
        )
    }
}

#[test]
fn rollover_and_retained_readonly_use_new_capabilities() {
    let f = Fixture::new();
    let mut manager = f.open().unwrap();
    let first = manager.export_writable(f.file.clone()).unwrap();
    assert!(!first.readonly);
    assert_eq!(manager.binding_for_file(&f.file), Some(first.clone()));
    assert!(manager.export_writable(f.file.clone()).is_err());
    manager.revoke(&first).unwrap();
    assert!(!f.configfs.join("subsystems").join(&first.nqn).exists());
    assert!(manager.export_writable(f.file.clone()).is_err());
    let retained = manager.export_readonly(f.file.clone()).unwrap();
    assert_ne!(first.uuid, retained.uuid);
    assert_ne!(first.nqn, retained.nqn);
    assert!(retained.readonly);
    let evidence = manager.stats().unwrap();
    assert!(evidence.fixture_only);
    assert_eq!(evidence.namespaces_exposed, 2);
    assert_eq!(evidence.owned_namespace_count, 1);
    assert_eq!(evidence.disable_drain_successes, 0);
    assert_eq!(evidence.readonly_commission_successes, 0);
    assert!(
        manager
            .state
            .exports
            .iter()
            .any(|e| e.binding == retained && e.readonly)
    );
    assert!(manager.retire_deleted_file(&f.file).is_err());
    manager.revoke(&retained).unwrap();
    assert!(manager.retire_deleted_file(&f.file).is_err());
    fs::remove_file(&f.file.path).unwrap();
    manager.retire_deleted_file(&f.file).unwrap();
    assert!(manager.sealed_file(&f.file.path).unwrap().is_none());
    manager.shutdown().unwrap();
    assert!(manager.state.exports.is_empty());
    assert!(manager.state.objects.is_empty());
}

#[test]
fn crash_reopen_drains_registered_namespace_before_reexport() {
    let f = Fixture::new();
    let mut manager = f.open().unwrap();
    let old = manager.export_writable(f.file.clone()).unwrap();
    let ns = f
        .configfs
        .join("subsystems")
        .join(&old.nqn)
        .join("namespaces/1");
    assert_eq!(attribute(&ns.join("enable")).unwrap(), "1");
    drop(manager); // Simulates losing the process without graceful shutdown.
    let mut reopened = f.open().unwrap();
    assert!(!ns.exists());
    assert!(reopened.state.exports.is_empty());
    assert!(reopened.export_writable(f.file.clone()).is_err());
    let path = f._tmp.path().join("wal-next");
    fs::write(&path, vec![8u8; 4096]).unwrap();
    let m = fs::metadata(&path).unwrap();
    let file = LiveFileIdentity {
        path,
        device: m.dev(),
        inode: m.ino(),
        bytes: m.len(),
        block_size: 4096,
    };
    let next = reopened.export_writable(file).unwrap();
    assert_ne!(old.uuid, next.uuid);
    reopened.shutdown().unwrap();
}

#[test]
fn crash_before_namespace_configuration_cleans_disabled_object() {
    let f = Fixture::new();
    let mut manager = f.open().unwrap();
    let id = random_id().unwrap();
    let binding = LiveTargetBinding {
        nqn: format!("nqn.2026-01.local.tidehunter:live-{}", uuid(id)),
        uuid: id,
        nsid: 1,
        bytes: f.file.bytes,
        block_size: f.file.block_size,
        address: f.config.address.clone(),
        service: f.config.service,
        transport: f.config.transport,
        readonly: false,
        spdk: None,
    };
    manager.state.exports.push(Export {
        binding: binding.clone(),
        file: f.file.clone(),
        readonly: false,
        spdk_phase: None,
    });
    manager.save().unwrap();
    let sub = PathBuf::from("subsystems").join(&binding.nqn);
    manager
        .record_create(id, sub.clone(), Kind::Directory, None)
        .unwrap();
    manager
        .record_create(id, sub.join("namespaces/1"), Kind::Directory, None)
        .unwrap();
    drop(manager);
    let mut reopened = f.open().unwrap();
    assert!(!f.configfs.join(sub).exists());
    reopened.shutdown().unwrap();
}

#[test]
fn owned_relative_configfs_link_resolves_to_registered_target() {
    let f = Fixture::new();
    let mut manager = f.open().unwrap();
    let binding = manager.export_writable(f.file.clone()).unwrap();
    let relative = PathBuf::from("ports/42/subsystems").join(&binding.nqn);
    let link = f.configfs.join(&relative);
    fs::remove_file(&link).unwrap();
    symlink(
        PathBuf::from("../../../subsystems").join(&binding.nqn),
        &link,
    )
    .unwrap();
    // Model configfs's relative symlink body with its newly created inode.
    let registered = manager
        .state
        .objects
        .iter_mut()
        .find(|object| object.path == relative)
        .unwrap();
    registered.identity = Some(object_id(&link, Kind::Link).unwrap());
    manager.save().unwrap();
    manager.revoke(&binding).unwrap();
    assert!(!link.exists());
    manager.shutdown().unwrap();
}

#[test]
fn replaced_owned_link_is_not_mutated() {
    let f = Fixture::new();
    let mut manager = f.open().unwrap();
    let binding = manager.export_writable(f.file.clone()).unwrap();
    let link = f.configfs.join("ports/42/subsystems").join(&binding.nqn);
    let moved = link.with_extension("original");
    fs::rename(&link, &moved).unwrap();
    symlink(f.configfs.join("subsystems").join(&binding.nqn), &link).unwrap();
    assert!(manager.revoke(&binding).is_err());
    assert!(
        fs::symlink_metadata(&link)
            .unwrap()
            .file_type()
            .is_symlink()
    );
    assert!(
        fs::symlink_metadata(&moved)
            .unwrap()
            .file_type()
            .is_symlink()
    );
}

#[test]
fn replaced_backing_file_blocks_recovery_and_revocation_claim() {
    let f = Fixture::new();
    let mut manager = f.open().unwrap();
    let binding = manager.export_writable(f.file.clone()).unwrap();
    let namespace = f
        .configfs
        .join("subsystems")
        .join(&binding.nqn)
        .join("namespaces/1");
    fs::rename(&f.file.path, f._tmp.path().join("old-wal")).unwrap();
    fs::write(&f.file.path, vec![9u8; 4096]).unwrap();
    assert!(manager.revoke(&binding).is_err());
    assert_eq!(attribute(&namespace.join("enable")).unwrap(), "1");
    drop(manager);
    assert!(f.open().is_err());
}

#[test]
fn uncertain_creation_and_changed_file_fail_closed() {
    let f = Fixture::new();
    let mut manager = f.open().unwrap();
    let mut wrong = f.file.clone();
    wrong.inode += 1;
    assert!(manager.export_writable(wrong).is_err());
    let path = PathBuf::from("subsystems/nqn.2026-01.local.test:unknown");
    manager.state.objects.push(Object {
        path: path.clone(),
        group: [1; 16],
        kind: Kind::Directory,
        identity: None,
        destination: None,
    });
    manager.save().unwrap();
    fs::create_dir(f.configfs.join(path)).unwrap();
    drop(manager);
    assert!(f.open().is_err());
}

#[test]
fn existing_unowned_port_is_never_adopted() {
    let f = Fixture::new();
    let port = f.configfs.join("ports/42");
    fs::create_dir(&port).unwrap();
    assert!(f.open().is_err());
    assert!(port.is_dir());
}

#[test]
fn corrupt_journal_never_allows_recovery() {
    let f = Fixture::new();
    let mut manager = f.open().unwrap();
    let old = manager.export_writable(f.file.clone()).unwrap();
    drop(manager);
    let state = f.config.state_dir.join(STATE);
    let mut bytes = fs::read(&state).unwrap();
    bytes[0] ^= 1;
    fs::write(&state, bytes).unwrap();
    assert!(f.open().is_err());
    assert!(f.configfs.join("subsystems").join(old.nqn).exists());
}

#[test]
fn persisted_owner_identity_rejects_missing_or_replaced_journal() {
    let f = Fixture::new();
    let mut manager = f.open().unwrap();
    let identity = manager.identity().unwrap();
    let old = manager.export_writable(f.file.clone()).unwrap();
    drop(manager);
    let mut wrong = identity;
    wrong.registry_id[0] ^= 1;
    assert!(f.open_existing(wrong).is_err());
    fs::remove_file(f.config.state_dir.join(STATE)).unwrap();
    assert!(f.open_existing(identity).is_err());
    assert!(f.configfs.join("subsystems").join(old.nqn).exists());
}

#[test]
fn inode_reuse_on_different_wal_path_does_not_match_marker() {
    let f = Fixture::new();
    let mut manager = f.open().unwrap();
    let mut old = f.file.clone();
    old.path = f._tmp.path().join("previous-wal");
    manager.write_sealed(&old).unwrap();
    assert_eq!(manager.sealed_file(&old.path).unwrap(), Some(old));
    assert!(manager.sealed_file(&f.file.path).unwrap().is_none());
    let binding = manager.export_writable(f.file.clone()).unwrap();
    manager.revoke(&binding).unwrap();
    manager.shutdown().unwrap();
}

#[test]
fn replaced_sealed_directory_blocks_recovery() {
    let f = Fixture::new();
    let mut manager = f.open().unwrap();
    let identity = manager.identity().unwrap();
    let old = manager.export_writable(f.file.clone()).unwrap();
    drop(manager);
    let sealed = f.config.state_dir.join("sealed");
    fs::rename(&sealed, f.config.state_dir.join("sealed-original")).unwrap();
    fs::create_dir(&sealed).unwrap();
    assert!(f.open_existing(identity).is_err());
    assert!(f.configfs.join("subsystems").join(old.nqn).exists());
}

#[test]
fn uncertain_spdk_operations_remain_durable_and_block_fencing() {
    for phase in [
        spdk::Phase::Creating,
        spdk::Phase::Removing,
        spdk::Phase::Cleaning,
    ] {
        let f = Fixture::new();
        let mut manager = f.open().unwrap();
        let id = random_id().unwrap();
        let binding = LiveTargetBinding {
            nqn: format!("nqn.2026-01.local.tidehunter:live-{}", uuid(id)),
            uuid: id,
            nsid: 1,
            bytes: f.file.bytes,
            block_size: f.file.block_size,
            address: f.config.address.clone(),
            service: f.config.service,
            transport: f.config.transport,
            readonly: false,
            spdk: None,
        };
        manager.state.exports.push(Export {
            binding,
            file: f.file.clone(),
            readonly: false,
            spdk_phase: Some(phase),
        });
        manager.save().unwrap();
        assert!(manager.spdk_fence(None).is_err());
        assert_eq!(manager.state.exports[0].spdk_phase, Some(phase));
        assert!(manager.sealed_file(&f.file.path).unwrap().is_none());
        let bytes = fs::read(f.config.state_dir.join(STATE)).unwrap();
        let persisted: State = serde_json::from_slice(&bytes[4..]).unwrap();
        assert_eq!(persisted.exports[0].spdk_phase, Some(phase));
    }
}

#[test]
fn spdk_drained_journal_seals_exact_writable_inode() {
    let f = Fixture::new();
    let mut manager = f.open().unwrap();
    let id = random_id().unwrap();
    manager.state.exports.push(Export {
        binding: LiveTargetBinding {
            nqn: format!("nqn.2026-01.local.tidehunter:live-{}", uuid(id)),
            uuid: id,
            nsid: 1,
            bytes: f.file.bytes,
            block_size: f.file.block_size,
            address: f.config.address.clone(),
            service: f.config.service,
            transport: f.config.transport,
            readonly: false,
            spdk: None,
        },
        file: f.file.clone(),
        readonly: false,
        spdk_phase: Some(spdk::Phase::Drained),
    });
    manager.save().unwrap();
    manager.spdk_fence(None).unwrap();
    assert_eq!(
        manager.sealed_file(&f.file.path).unwrap(),
        Some(f.file.clone())
    );
    assert_eq!(
        manager.state.exports[0].spdk_phase,
        Some(spdk::Phase::Drained)
    );
}
