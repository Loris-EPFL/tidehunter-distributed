//! These tests exercise the journal and ownership state machine only; ordinary
//! files are not evidence that a kernel nvmet namespace drains target I/O.
use super::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
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
        };
        Self {
            _tmp: tmp,
            configfs,
            config,
            file,
        }
    }
    fn open(&self) -> io::Result<LiveTargetManager> {
        LiveTargetManager::open_inner(self.config.clone(), self.configfs.clone(), true, None, None)
    }
    fn open_existing(&self, identity: LiveTargetIdentity) -> io::Result<LiveTargetManager> {
        LiveTargetManager::open_inner(
            self.config.clone(),
            self.configfs.clone(),
            true,
            Some(identity),
            None,
        )
    }
}

#[test]
fn missing_state_directory_reports_operation_and_exact_path() {
    let f = Fixture::new();
    let path = f.config.state_dir.join("missing-live-nvmeof");
    let error = check_path(&path, true).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::NotFound);
    let message = error.to_string();
    assert!(message.contains("inspect protected target path"));
    assert!(message.contains(path.to_str().unwrap()));
    assert!(!path.exists());
}

#[test]
fn missing_configfs_attribute_reports_operation_and_exact_path() {
    let f = Fixture::new();
    let mut manager = f.open().unwrap();
    let relative = Path::new("ports/42/addr_traddr");
    let path = f.configfs.join(relative);
    fs::remove_file(&path).unwrap();
    let error = manager.put(relative, "127.0.0.1").unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::NotFound);
    let message = error.to_string();
    assert!(message.contains("open target attribute for write"));
    assert!(message.contains(path.to_str().unwrap()));
    // Restore the fixture attribute so normal cleanup can verify ownership.
    fs::write(path, "127.0.0.1").unwrap();
    manager.shutdown().unwrap();
}

#[test]
fn cohosted_targets_reject_shared_initiator_host_object() {
    let f = Fixture::new();
    let mut first = f.open().unwrap();
    let mut second = f.config.clone();
    second.state_dir = f._tmp.path().join("second-state");
    second.port_id += 2;
    second.commissioning_port_id += 2;
    second.service += 2;
    second.commissioning_service += 2;
    fs::create_dir(&second.state_dir).unwrap();
    let error = match LiveTargetManager::open_fixture(second, f.configfs.clone()) {
        Ok(_) => panic!("shared host object was accepted"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    let message = error.to_string();
    assert!(message.contains("refusing to share configfs object"));
    assert!(
        message.contains(
            f.configfs
                .join("hosts")
                .join(&f.config.host_nqn)
                .to_str()
                .unwrap()
        )
    );
    first.shutdown().unwrap();
}

#[test]
fn fifteen_exclusive_owners_share_configfs_without_sharing_objects() {
    let f = Fixture::new();
    let mut managers = Vec::new();
    let mut subsystems = std::collections::BTreeSet::new();
    for index in 0..15u32 {
        let mut config = f.config.clone();
        config.state_dir = f._tmp.path().join(format!("owner-{index}"));
        config.host_nqn = format!("nqn.2026-01.test:owner-{index}");
        config.port_id += 2 * index as u16;
        config.commissioning_port_id += 2 * index as u16;
        config.service += 2 * index as u16;
        config.commissioning_service += 2 * index as u16;
        fs::create_dir(&config.state_dir).unwrap();
        let path = f._tmp.path().join(format!("wal-{index}"));
        fs::write(&path, [0; 4096]).unwrap();
        let metadata = fs::metadata(&path).unwrap();
        let file = LiveFileIdentity {
            path,
            device: metadata.dev(),
            inode: metadata.ino(),
            bytes: metadata.len(),
            block_size: 4096,
        };
        let mut manager = LiveTargetManager::open_fixture(config, f.configfs.clone()).unwrap();
        let name = super::super::live_storage::wal_export_name(
            1024,
            [index as u8 / 5 + 1; 16],
            index % 5,
            1,
            0,
        );
        let binding = manager.export_permanent(&name, file).unwrap();
        assert!(subsystems.insert(binding.nqn));
        managers.push(manager);
    }
    assert_eq!(subsystems.len(), 15);
    for manager in &mut managers {
        manager.shutdown().unwrap();
    }
    for name in ["hosts", "ports", "subsystems"] {
        assert_eq!(fs::read_dir(f.configfs.join(name)).unwrap().count(), 0);
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
        permanent: false,
    };
    manager.state.exports.push(Export {
        binding: binding.clone(),
        file: f.file.clone(),
        readonly: false,
    });
    manager.save().unwrap();
    let sub = PathBuf::from("subsystems").join(&binding.nqn);
    manager
        .record_create(id, &sub, Kind::Directory, None)
        .unwrap();
    manager
        .record_create(id, &sub.join("namespaces/1"), Kind::Directory, None)
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
fn permanent_namespaces_share_subsystem_and_revoke_independently() {
    use super::super::live_storage::wal_export_name;
    let f = Fixture::new();
    let mut manager = f.open().unwrap();
    let name = wal_export_name(1024, [1; 16], 2, 3, 0);
    let first = manager.export_permanent(&name, f.file.clone()).unwrap();
    assert_eq!(
        manager.export_permanent(&name, f.file.clone()).unwrap(),
        first
    );
    let path = f._tmp.path().join("wal-1");
    fs::write(&path, [8; 4096]).unwrap();
    let metadata = fs::metadata(&path).unwrap();
    let file = LiveFileIdentity {
        path,
        device: metadata.dev(),
        inode: metadata.ino(),
        bytes: 4096,
        block_size: 4096,
    };
    let second = manager
        .export_permanent(&wal_export_name(1024, [1; 16], 2, 3, 1), file)
        .unwrap();
    let sub = f.configfs.join("subsystems").join(&name.nqn);
    assert!(sub.join("namespaces/1").is_dir());
    assert!(sub.join("namespaces/2").is_dir());
    assert_eq!(manager.stats().unwrap().namespaces_exposed, 2);
    manager.revoke(&first).unwrap();
    assert!(!sub.join("namespaces/1").exists());
    assert!(sub.join("namespaces/2").is_dir());
    manager.revoke(&second).unwrap();
    assert!(!sub.exists());
    manager.shutdown().unwrap();
}

fn extra_wal(f: &Fixture, name: &str) -> LiveFileIdentity {
    let path = f._tmp.path().join(name);
    fs::write(&path, [9; 4096]).unwrap();
    let metadata = fs::metadata(&path).unwrap();
    LiveFileIdentity {
        path,
        device: metadata.dev(),
        inode: metadata.ino(),
        bytes: metadata.len(),
        block_size: 4096,
    }
}

#[test]
fn stale_registry_after_batched_revoke_recovers_and_keeps_markers() {
    let f = Fixture::new();
    let mut manager = f.open().unwrap();
    let files = [
        f.file.clone(),
        extra_wal(&f, "wal-1"),
        extra_wal(&f, "wal-2"),
    ];
    let writable = [
        manager.export_writable(files[0].clone()).unwrap(),
        manager.export_writable(files[1].clone()).unwrap(),
    ];
    let readonly = manager.export_readonly(files[2].clone()).unwrap();
    let registry = f.config.state_dir.join(STATE);
    let before = fs::read(&registry).unwrap();
    manager
        .revoke_all(&[&writable[0], &readonly, &writable[1]])
        .unwrap();
    assert_eq!(
        fs::read_dir(f.configfs.join("subsystems")).unwrap().count(),
        0
    );
    assert!(manager.state.exports.is_empty());
    // A crash after the configfs removals but before any later save leaves
    // the older registry, which still lists every removed object.
    drop(manager);
    fs::write(&registry, before).unwrap();
    let mut reopened = f.open().unwrap();
    assert!(reopened.state.exports.is_empty());
    assert!(
        !reopened
            .state
            .objects
            .iter()
            .any(|object| object.path.starts_with("subsystems"))
    );
    for file in &files[..2] {
        assert_eq!(
            reopened.sealed_file(&file.path).unwrap().as_ref(),
            Some(file)
        );
        assert!(reopened.export_writable(file.clone()).is_err());
    }
    assert!(reopened.sealed_file(&files[2].path).unwrap().is_none());
    reopened.shutdown().unwrap();
}

#[test]
fn batched_revoke_removes_only_fully_drained_permanent_subsystems() {
    use super::super::live_storage::wal_export_name;
    let f = Fixture::new();
    let mut manager = f.open().unwrap();
    let names: Vec<_> = (0..4)
        .map(|file| wal_export_name(2, [1; 16], 2, 3, file))
        .collect();
    let bindings: Vec<_> = names
        .iter()
        .enumerate()
        .map(|(index, name)| {
            let file = if index == 0 {
                f.file.clone()
            } else {
                extra_wal(&f, &format!("wal-{index}"))
            };
            manager.export_permanent(name, file).unwrap()
        })
        .collect();
    assert_eq!(names[0].nqn, names[1].nqn);
    assert_eq!(names[2].nqn, names[3].nqn);
    assert_ne!(names[0].nqn, names[2].nqn);
    let first = f.configfs.join("subsystems").join(&names[0].nqn);
    let second = f.configfs.join("subsystems").join(&names[2].nqn);
    // One batch drains the first subsystem and half of the second.
    manager
        .revoke_all(&[&bindings[1], &bindings[2], &bindings[0]])
        .unwrap();
    assert!(!first.exists());
    assert!(second.is_dir());
    assert!(
        !second
            .join(format!("namespaces/{}", bindings[2].nsid))
            .exists()
    );
    assert!(
        second
            .join(format!("namespaces/{}", bindings[3].nsid))
            .is_dir()
    );
    assert_eq!(manager.stats().unwrap().owned_namespace_count, 1);
    manager.revoke_all(&[&bindings[3]]).unwrap();
    assert!(!second.exists());
    assert!(manager.state.exports.is_empty());
    manager.revoke_all(&[]).unwrap();
    manager.shutdown().unwrap();
}

// Crash cuts of the target state journal. Each `save` has two persistence
// steps: the synced pending record (old registry still current) and the
// renamed record with its directory synced. Every configfs effect runs after a
// save returned and before the next save begins, so a crash at each step of
// each save covers every crash point of an operation: the state on disk is
// "registry k-1, effects through k-1" (Prepared, also a lost rename) or
// "registry k, effects through k-1" (Published).

/// Crash at the `save`-th journal save (0-based, counted from installation)
/// and `step`. The failing save returns as if the process died there; every
/// later hook call is counted, because a dead process persists nothing more.
fn crash_hook(save: usize, step: SaveStep, after_crash: Arc<AtomicUsize>) -> SaveHook {
    let mut prepared = 0usize;
    let mut crashed = false;
    Box::new(move |current| {
        if crashed {
            after_crash.fetch_add(1, AtomicOrdering::Relaxed);
            return Err(io::Error::other("journal save after injected crash"));
        }
        if current == SaveStep::Prepared {
            prepared += 1;
        }
        if prepared == save + 1 && current == step {
            crashed = true;
            return Err(io::Error::other("injected crash"));
        }
        Ok(())
    })
}

fn counting_hook(saves: Arc<AtomicUsize>) -> SaveHook {
    Box::new(move |step| {
        if step == SaveStep::Published {
            saves.fetch_add(1, AtomicOrdering::Relaxed);
        }
        Ok(())
    })
}

/// The registry a restarted process reads, or `None` before the first save.
fn durable_state(f: &Fixture) -> Option<State> {
    let bytes = match fs::read(f.config.state_dir.join(STATE)) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return None,
        Err(error) => panic!("read durable registry: {error}"),
    };
    let (checksum, payload) = bytes.split_at(4);
    assert_eq!(
        crc32fast::hash(payload),
        u32::from_le_bytes(checksum.try_into().unwrap()),
        "durable registry is torn"
    );
    Some(serde_json::from_slice(payload).unwrap())
}

/// Every exclusive configfs object (directory or link) under the fixture.
fn configfs_objects(root: &Path) -> Vec<PathBuf> {
    fn children(path: &Path) -> Vec<std::ffi::OsString> {
        match fs::read_dir(path) {
            Ok(entries) => entries.map(|entry| entry.unwrap().file_name()).collect(),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Vec::new(),
            Err(error) => panic!("list {}: {error}", path.display()),
        }
    }
    let mut objects = Vec::new();
    for top in ["hosts", "ports", "subsystems"] {
        for name in children(&root.join(top)) {
            let object = PathBuf::from(top).join(name);
            let nested: &[&str] = match top {
                "ports" => &["subsystems"],
                "subsystems" => &["namespaces", "allowed_hosts"],
                _ => &[],
            };
            for inner in nested {
                for child in children(&root.join(&object).join(inner)) {
                    objects.push(object.join(inner).join(child));
                }
            }
            objects.push(object);
        }
    }
    objects
}

/// Write-ahead invariant at the crash: every object that exists has a
/// durable registry record, so recovery can find and revoke it.
fn assert_write_ahead(f: &Fixture, durable: Option<&State>) {
    let registered: std::collections::HashSet<&Path> = durable
        .map(|state| state.objects.iter().map(|o| o.path.as_path()).collect())
        .unwrap_or_default();
    for object in configfs_objects(&f.configfs) {
        assert!(
            registered.contains(object.as_path()),
            "configfs object {} has no durable registry record",
            object.display()
        );
    }
}

/// Reopen after the crash and check what the fencing protocol requires.
/// Either recovery revokes every capability and seals every WAL that was
/// ever exposed writable, or it fails closed on an object whose creation
/// record has no durable identity yet. Returns whether recovery succeeded.
fn assert_recovery(
    f: &Fixture,
    identity: Option<LiveTargetIdentity>,
    exposed: &[LiveFileIdentity],
    durable: Option<&State>,
) -> bool {
    // The only cut that may fail closed: an object exists while its durable
    // record has no identity yet (between `mkdir`/`symlink` and the identity
    // save). Every other cut must recover.
    let uncertain = durable.is_some_and(|durable| {
        durable.objects.iter().any(|object| {
            object.identity.is_none() && fs::symlink_metadata(f.configfs.join(&object.path)).is_ok()
        })
    });
    let reopened = match identity {
        Some(identity) => f.open_existing(identity),
        None => f.open(),
    };
    let mut reopened = match reopened {
        Ok(manager) => {
            assert!(!uncertain, "an uncertain creation was adopted");
            manager
        }
        Err(error) => {
            assert!(uncertain, "recoverable cut failed: {error}");
            assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{error}");
            assert!(
                error.to_string().contains("creation was interrupted"),
                "{error}"
            );
            return false;
        }
    };
    assert!(reopened.state.exports.is_empty());
    assert!(
        reopened
            .state
            .objects
            .iter()
            .all(|object| !object.path.starts_with("subsystems"))
    );
    assert_eq!(
        fs::read_dir(f.configfs.join("subsystems")).unwrap().count(),
        0
    );
    let mut sealed = exposed.to_vec();
    if let Some(durable) = durable {
        sealed.extend(
            durable
                .exports
                .iter()
                .filter(|export| !export.readonly)
                .map(|export| export.file.clone()),
        );
    }
    for file in &sealed {
        assert_eq!(
            reopened.sealed_file(&file.path).unwrap().as_ref(),
            Some(file)
        );
        assert!(reopened.export_writable(file.clone()).is_err());
    }
    let fresh = extra_wal(f, "wal-after-crash");
    assert!(!reopened.export_writable(fresh).unwrap().readonly);
    reopened.shutdown().unwrap();
    for top in ["hosts", "ports", "subsystems"] {
        assert_eq!(fs::read_dir(f.configfs.join(top)).unwrap().count(), 0);
    }
    true
}

/// Files a scenario exported before the crash window; `true` = writable.
type Prepared = Vec<(LiveFileIdentity, bool)>;

/// Crash at both steps of every save of `operation`, then recover. Returns
/// (recovered, failed closed) cut counts; every cut is one of the two.
fn crash_cuts(
    prepare: fn(&Fixture, &mut LiveTargetManager) -> Prepared,
    operation: fn(&Fixture, &mut LiveTargetManager, &Prepared) -> io::Result<()>,
) -> (usize, usize) {
    let saves = {
        let f = Fixture::new();
        let mut manager = f.open().unwrap();
        let prepared = prepare(&f, &mut manager);
        let count = Arc::new(AtomicUsize::new(0));
        manager.set_save_hook(counting_hook(count.clone()));
        operation(&f, &mut manager, &prepared).unwrap();
        count.load(AtomicOrdering::Relaxed)
    };
    assert!(saves > 0);
    let mut outcomes = (0, 0);
    for save in 0..saves {
        for step in [SaveStep::Prepared, SaveStep::Published] {
            let f = Fixture::new();
            let mut manager = f.open().unwrap();
            let identity = manager.identity().unwrap();
            let prepared = prepare(&f, &mut manager);
            let after_crash = Arc::new(AtomicUsize::new(0));
            manager.set_save_hook(crash_hook(save, step, after_crash.clone()));
            assert!(
                operation(&f, &mut manager, &prepared).is_err(),
                "save {save} {step:?} did not reach the injected crash"
            );
            drop(manager);
            assert_eq!(after_crash.load(AtomicOrdering::Relaxed), 0);
            let durable = durable_state(&f);
            assert_write_ahead(&f, durable.as_ref());
            let exposed: Vec<_> = prepared
                .iter()
                .filter(|(_, writable)| *writable)
                .map(|(file, _)| file.clone())
                .collect();
            if assert_recovery(&f, Some(identity), &exposed, durable.as_ref()) {
                outcomes.0 += 1;
            } else {
                outcomes.1 += 1;
            }
        }
    }
    assert_eq!(outcomes.0 + outcomes.1, 2 * saves);
    outcomes
}

fn nothing(_: &Fixture, _: &mut LiveTargetManager) -> Prepared {
    Vec::new()
}

fn writable_and_readonly(f: &Fixture, manager: &mut LiveTargetManager) -> Prepared {
    let readonly = extra_wal(f, "wal-readonly");
    manager.export_writable(f.file.clone()).unwrap();
    manager.export_readonly(readonly.clone()).unwrap();
    vec![(f.file.clone(), true), (readonly, false)]
}

/// Export two WAL files into one permanent subsystem.
fn export_permanent_pair(f: &Fixture, manager: &mut LiveTargetManager) -> io::Result<Prepared> {
    use super::super::live_storage::wal_export_name;
    let second = extra_wal(f, "wal-1");
    for (index, file) in [&f.file, &second].into_iter().enumerate() {
        manager.export_permanent(
            &wal_export_name(2, [1; 16], 2, 3, index as u64),
            file.clone(),
        )?;
    }
    Ok(vec![(f.file.clone(), true), (second, true)])
}

fn permanent_pair(f: &Fixture, manager: &mut LiveTargetManager) -> Prepared {
    export_permanent_pair(f, manager).unwrap()
}

fn bindings(manager: &LiveTargetManager, prepared: &Prepared) -> Vec<LiveTargetBinding> {
    prepared
        .iter()
        .map(|(file, _)| manager.binding_for_file(file).unwrap())
        .collect()
}

/// A crash during the open that commissions the target ports and hosts.
#[test]
fn crash_at_every_journal_step_of_open_recovers_or_fails_closed() {
    let open = |f: &Fixture, hook| {
        LiveTargetManager::open_inner(f.config.clone(), f.configfs.clone(), true, None, Some(hook))
    };
    let saves = {
        let f = Fixture::new();
        let count = Arc::new(AtomicUsize::new(0));
        open(&f, counting_hook(count.clone())).unwrap();
        count.load(AtomicOrdering::Relaxed)
    };
    assert!(saves > 0);
    let mut recovered = 0;
    for save in 0..saves {
        for step in [SaveStep::Prepared, SaveStep::Published] {
            let f = Fixture::new();
            let after_crash = Arc::new(AtomicUsize::new(0));
            assert!(open(&f, crash_hook(save, step, after_crash.clone())).is_err());
            assert_eq!(after_crash.load(AtomicOrdering::Relaxed), 0);
            let durable = durable_state(&f);
            assert_write_ahead(&f, durable.as_ref());
            recovered += usize::from(assert_recovery(&f, None, &[], durable.as_ref()));
        }
    }
    assert!(recovered > 0);
}

#[test]
fn crash_at_every_journal_step_of_writable_export_recovers_or_fails_closed() {
    let (recovered, _) = crash_cuts(nothing, |f, manager, _| {
        manager.export_writable(f.file.clone()).map(drop)
    });
    assert!(recovered > 0);
}

#[test]
fn crash_at_every_journal_step_of_readonly_export_recovers_or_fails_closed() {
    let (recovered, _) = crash_cuts(nothing, |f, manager, _| {
        manager.export_readonly(f.file.clone()).map(drop)
    });
    assert!(recovered > 0);
}

#[test]
fn crash_at_every_journal_step_of_permanent_export_recovers_or_fails_closed() {
    let (recovered, _) = crash_cuts(nothing, |f, manager, _| {
        export_permanent_pair(f, manager).map(drop)
    });
    assert!(recovered > 0);
}

#[test]
fn crash_at_every_journal_step_of_revocation_recovers() {
    for prepare in [
        writable_and_readonly as fn(&Fixture, &mut LiveTargetManager) -> Prepared,
        permanent_pair,
    ] {
        // Revocation creates nothing, so no cut can leave an uncertain
        // creation: every crash must recover.
        let (_, failed_closed) = crash_cuts(prepare, |_, manager, prepared| {
            let bindings = bindings(manager, prepared);
            manager.revoke_all(&bindings.iter().collect::<Vec<_>>())
        });
        assert_eq!(failed_closed, 0);
    }
}

#[test]
fn crash_at_every_journal_step_of_shutdown_recovers() {
    let (_, failed_closed) = crash_cuts(writable_and_readonly, |_, manager, _| manager.shutdown());
    assert_eq!(failed_closed, 0);
}

/// Retiring a deleted WAL syncs its directory, then removes the marker and
/// syncs the marker directory; it writes no registry record. A crash before
/// the marker removal is durable leaves the marker of an absent file, and a
/// restarted owner repeats the retirement.
#[test]
fn crash_before_tombstone_release_is_durable_repeats_retirement() {
    let f = Fixture::new();
    let mut manager = f.open().unwrap();
    let identity = manager.identity().unwrap();
    let binding = manager.export_writable(f.file.clone()).unwrap();
    manager.revoke(&binding).unwrap();
    fs::remove_file(&f.file.path).unwrap();
    drop(manager);
    let mut reopened = f.open_existing(identity).unwrap();
    assert_eq!(
        reopened.sealed_file(&f.file.path).unwrap().as_ref(),
        Some(&f.file)
    );
    reopened.retire_deleted_file(&f.file).unwrap();
    assert!(reopened.sealed_file(&f.file.path).unwrap().is_none());
    reopened.shutdown().unwrap();
}
