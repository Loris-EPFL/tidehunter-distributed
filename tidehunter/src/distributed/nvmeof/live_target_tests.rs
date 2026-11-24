//! These tests exercise the configfs ownership and recovery state machine only;
//! ordinary files are not evidence that a kernel nvmet namespace drains target
//! I/O.
use super::*;
use crate::distributed::tcp::host_identity::effective_host_nqn;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use tempdir::TempDir;

fn wal(database: [u8; 16], partition: u32) -> (LiveTargetConfig, PathBuf) {
    let state = PathBuf::from(format!("state-{}-{partition}", uuid(database)));
    (
        LiveTargetConfig {
            address: "127.0.0.1".into(),
            service: 4420,
            transport: NvmeofTransport::Tcp,
            host_nqn: effective_host_nqn(&database, partition),
            port_id: 42,
            state_dir: state,
            commissioning_port_id: 43,
            commissioning_service: 4421,
        },
        PathBuf::from("unused"),
    )
}

struct Fixture {
    _tmp: TempDir,
    configfs: PathBuf,
    config: LiveTargetConfig,
    file: LiveFileIdentity,
}
impl Fixture {
    fn new() -> Self {
        Self::with_identity([1; 16], 0)
    }
    fn with_identity(database: [u8; 16], partition: u32) -> Self {
        let tmp = TempDir::new("live-target").unwrap();
        let mut config = wal(database, partition).0;
        config.state_dir = tmp.path().join("state");
        let configfs = tmp.path().join("configfs");
        fs::create_dir(&config.state_dir).unwrap();
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
    fn open_with(&self, hook: ConfigfsHook) -> io::Result<LiveTargetManager> {
        LiveTargetManager::open_inner(
            self.config.clone(),
            self.configfs.clone(),
            true,
            None,
            Some(hook),
        )
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
    fn scope(&self) -> String {
        owner_scope(&self.config.host_nqn).unwrap().to_owned()
    }
    fn extra_wal(&self, name: &str) -> LiveFileIdentity {
        let path = self._tmp.path().join(name);
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
    fn name(&self, file_index: u64) -> super::super::live_storage::WalExportName {
        super::super::live_storage::wal_export_name(1024, [1; 16], 0, 1, file_index)
    }
}

/// Every subsystem directory whose name belongs to this fixture's scope.
fn owned_subsystems(f: &Fixture) -> Vec<PathBuf> {
    let prefix = format!("{}sub-", f.scope());
    let mut owned = Vec::new();
    for entry in fs::read_dir(f.configfs.join("subsystems")).unwrap() {
        let entry = entry.unwrap();
        if entry
            .file_name()
            .to_str()
            .is_some_and(|name| name.starts_with(&prefix))
        {
            owned.push(entry.path());
        }
    }
    owned
}

/// WAL identities of every namespace that is enabled right now.
fn enabled_files(f: &Fixture) -> Vec<LiveFileIdentity> {
    let mut files = Vec::new();
    for sub in owned_subsystems(f) {
        let Ok(entries) = fs::read_dir(sub.join("namespaces")) else {
            continue;
        };
        for entry in entries {
            let ns = entry.unwrap().path();
            if optional_attribute(&ns.join("enable"))
                .ok()
                .flatten()
                .as_deref()
                != Some("1")
            {
                continue;
            }
            let Some(path) = optional_attribute(&ns.join("device_path"))
                .ok()
                .flatten()
                .filter(|path| !path.is_empty())
            else {
                continue;
            };
            let Ok(meta) = fs::symlink_metadata(&path) else {
                continue;
            };
            files.push(LiveFileIdentity {
                path: PathBuf::from(path),
                device: meta.dev(),
                inode: meta.ino(),
                bytes: meta.len(),
                block_size: 4096,
            });
        }
    }
    files
}

/// Count hook calls of one operation, then fail at each call in turn.
fn fail_at(target: usize) -> ConfigfsHook {
    let mut seen = 0usize;
    let mut crashed = false;
    Box::new(move |_index| {
        if crashed {
            return Err(io::Error::other("configfs mutation after injected crash"));
        }
        let current = seen;
        seen += 1;
        if current == target {
            crashed = true;
            return Err(io::Error::other("injected crash"));
        }
        Ok(())
    })
}
fn counting_hook(count: Arc<AtomicUsize>) -> ConfigfsHook {
    Box::new(move |_index| {
        count.fetch_add(1, AtomicOrdering::Relaxed);
        Ok(())
    })
}

/// Reopen after a crash: it must always succeed, remove every owned object,
/// seal every listed file, and refuse to export such a file writable again.
fn assert_recovery(f: &Fixture, identity: Option<LiveTargetIdentity>, sealed: &[LiveFileIdentity]) {
    let mut reopened = match identity {
        Some(identity) => f.open_existing(identity),
        None => f.open(),
    }
    .unwrap();
    assert!(
        owned_subsystems(f).is_empty(),
        "owned subsystems remained after recovery"
    );
    for (index, file) in sealed.iter().enumerate() {
        assert_eq!(
            reopened.sealed_file(&file.path).unwrap().as_ref(),
            Some(file),
            "file {} has no sealed marker",
            file.path.display()
        );
        let error = reopened
            .export_permanent(&f.name(index as u64), file.clone(), false)
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("fenced WAL cannot be exported writable again"),
            "writable re-export error was {error}"
        );
    }
    let fresh = f.extra_wal("wal-after-crash");
    assert!(
        !reopened
            .export_permanent(&f.name(999), fresh, false)
            .unwrap()
            .readonly
    );
    reopened.shutdown().unwrap();
    for top in ["hosts", "ports", "subsystems"] {
        assert_eq!(
            fs::read_dir(f.configfs.join(top)).unwrap().count(),
            0,
            "{top} was not empty after shutdown"
        );
    }
}

/// Run `operation` once to count its configfs steps, then crash it at every
/// step in a fresh fixture and check recovery.
fn crash_cuts(
    prepare: fn(&Fixture, &mut LiveTargetManager) -> Prepared,
    operation: fn(&Fixture, &mut LiveTargetManager, &Prepared) -> io::Result<()>,
) {
    let steps = {
        let f = Fixture::new();
        let mut manager = f.open().unwrap();
        let prepared = prepare(&f, &mut manager);
        let count = Arc::new(AtomicUsize::new(0));
        manager.set_configfs_hook(counting_hook(count.clone()));
        operation(&f, &mut manager, &prepared).unwrap();
        count.load(AtomicOrdering::Relaxed)
    };
    assert!(steps > 0, "operation mutated no configfs object");
    for step in 0..steps {
        let f = Fixture::new();
        let mut manager = f.open().unwrap();
        let identity = manager.identity().unwrap();
        let prepared = prepare(&f, &mut manager);
        manager.set_configfs_hook(fail_at(step));
        assert!(
            operation(&f, &mut manager, &prepared).is_err(),
            "step {step} did not reach the injected crash"
        );
        let mut sealed = enabled_files(&f);
        // A prepared writable export must stay sealed even when the cut
        // disabled or removed its namespace; a read-only export never held a
        // writable capability and needs no marker.
        for (binding, file) in prepared.bindings.iter().zip(&prepared.files) {
            if !binding.readonly && !sealed.contains(file) {
                sealed.push(file.clone());
            }
        }
        drop(manager);
        assert_recovery(&f, Some(identity), &sealed);
    }
}

fn nothing(_: &Fixture, _: &mut LiveTargetManager) -> Prepared {
    Prepared {
        bindings: Vec::new(),
        files: Vec::new(),
    }
}

fn writable(f: &Fixture, manager: &mut LiveTargetManager) -> Prepared {
    let binding = manager
        .export_permanent(&f.name(0), f.file.clone(), false)
        .unwrap();
    Prepared {
        bindings: vec![binding],
        files: vec![f.file.clone()],
    }
}

fn writable_and_readonly(f: &Fixture, manager: &mut LiveTargetManager) -> Prepared {
    let readonly = f.extra_wal("wal-readonly");
    let first = manager
        .export_permanent(&f.name(0), f.file.clone(), false)
        .unwrap();
    let second = manager
        .export_permanent(&f.name(1), readonly.clone(), true)
        .unwrap();
    Prepared {
        bindings: vec![first, second],
        files: vec![f.file.clone(), readonly],
    }
}

/// Bindings and file identities a scenario created before its crash window.
struct Prepared {
    bindings: Vec<LiveTargetBinding>,
    files: Vec<LiveFileIdentity>,
}

#[test]
fn commissioning_host_is_a_valid_initiator_nqn() {
    let f = Fixture::new();
    let mut manager = f.open().unwrap();
    let host = manager.commissioning_host();
    assert!(
        super::super::live_connect::valid_nqn(&host),
        "commissioning host is not a valid NQN: {host}"
    );
    assert!(host.starts_with(NQN_PREFIX), "{host}");
    assert!(host.ends_with("commission"), "{host}");
    manager.shutdown().unwrap();
}

#[test]
fn empty_sealed_directory_without_target_id_is_adopted() {
    let f = Fixture::new();
    let manager = f.open().unwrap();
    let identity = manager.identity().unwrap();
    drop(manager);
    fs::remove_file(f.config.state_dir.join(TARGET_ID)).unwrap();
    let mut reopened = f.open().unwrap();
    assert_ne!(
        reopened.identity().unwrap().registry_id,
        identity.registry_id
    );
    assert!(f.config.state_dir.join(TARGET_ID).is_file());
    reopened.shutdown().unwrap();
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
    fs::write(path, "127.0.0.1").unwrap();
    manager.shutdown().unwrap();
}

#[test]
fn second_manager_for_scope_is_refused() {
    let f = Fixture::new();
    let mut first = f.open().unwrap();
    let mut second = f.config.clone();
    second.state_dir = f._tmp.path().join("second-state");
    fs::create_dir(&second.state_dir).unwrap();
    let error = match LiveTargetManager::open_fixture(second, f.configfs.clone()) {
        Ok(_) => panic!("a second manager adopted the same configfs scope"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
    assert!(
        error
            .to_string()
            .contains("another live target owns configfs scope"),
        "{error}"
    );
    first.shutdown().unwrap();
}

#[test]
fn fifteen_exclusive_owners_share_configfs_without_sharing_objects() {
    let tmp = TempDir::new("live-target-fifteen").unwrap();
    let configfs = tmp.path().join("configfs");
    fs::create_dir(&configfs).unwrap();
    for dir in ["hosts", "ports", "subsystems"] {
        fs::create_dir(configfs.join(dir)).unwrap();
    }
    let mut managers = Vec::new();
    let mut subsystems = std::collections::BTreeSet::new();
    for index in 0..15u32 {
        let database = [index as u8 / 5 + 1; 16];
        let partition = index % 5;
        let mut config = wal(database, partition).0;
        config.state_dir = tmp.path().join(format!("owner-{index}"));
        config.port_id += 2 * index as u16;
        config.commissioning_port_id += 2 * index as u16;
        config.service += 2 * index as u16;
        config.commissioning_service += 2 * index as u16;
        fs::create_dir(&config.state_dir).unwrap();
        let path = tmp.path().join(format!("wal-{index}"));
        fs::write(&path, [0; 4096]).unwrap();
        let metadata = fs::metadata(&path).unwrap();
        let file = LiveFileIdentity {
            path,
            device: metadata.dev(),
            inode: metadata.ino(),
            bytes: metadata.len(),
            block_size: 4096,
        };
        let mut manager = LiveTargetManager::open_fixture(config, configfs.clone()).unwrap();
        let name = super::super::live_storage::wal_export_name(1024, database, partition, 1, 0);
        let binding = manager.export_permanent(&name, file, false).unwrap();
        assert!(subsystems.insert(binding.nqn));
        managers.push(manager);
    }
    assert_eq!(subsystems.len(), 15);
    for manager in &mut managers {
        manager.shutdown().unwrap();
    }
    for name in ["hosts", "ports", "subsystems"] {
        assert_eq!(fs::read_dir(configfs.join(name)).unwrap().count(), 0);
    }
}

#[test]
fn permanent_namespaces_share_subsystem_and_revoke_independently() {
    let f = Fixture::new();
    let mut manager = f.open().unwrap();
    let first = manager
        .export_permanent(&f.name(0), f.file.clone(), false)
        .unwrap();
    assert_eq!(
        manager
            .export_permanent(&f.name(0), f.file.clone(), false)
            .unwrap(),
        first
    );
    let second = manager
        .export_permanent(&f.name(1), f.extra_wal("wal-1"), false)
        .unwrap();
    let sub = f.configfs.join("subsystems").join(&first.nqn);
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

#[test]
fn batched_revoke_removes_only_fully_drained_permanent_subsystems() {
    let f = Fixture::new();
    let mut manager = f.open().unwrap();
    let names: Vec<_> = (0..4)
        .map(|file| super::super::live_storage::wal_export_name(2, [1; 16], 0, 1, file))
        .collect();
    let bindings: Vec<_> = names
        .iter()
        .enumerate()
        .map(|(index, name)| {
            let file = if index == 0 {
                f.file.clone()
            } else {
                f.extra_wal(&format!("wal-{index}"))
            };
            manager.export_permanent(name, file, false).unwrap()
        })
        .collect();
    assert_eq!(names[0].nqn, names[1].nqn);
    assert_eq!(names[2].nqn, names[3].nqn);
    assert_ne!(names[0].nqn, names[2].nqn);
    let first = f.configfs.join("subsystems").join(&names[0].nqn);
    let second = f.configfs.join("subsystems").join(&names[2].nqn);
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
    assert!(manager.exports.is_empty());
    manager.revoke_all(&[]).unwrap();
    manager.shutdown().unwrap();
}

#[test]
fn reopen_removes_unrecorded_scoped_subsystem() {
    let f = Fixture::new();
    let name = format!("{}sub-{}", f.scope(), "ab".repeat(16));
    let sub = f.configfs.join("subsystems").join(&name);
    fs::create_dir(&sub).unwrap();
    fs::create_dir(sub.join("namespaces")).unwrap();
    fs::create_dir(sub.join("allowed_hosts")).unwrap();
    fs::write(sub.join("attr_allow_any_host"), "0").unwrap();
    let ns = sub.join("namespaces/1");
    fs::create_dir(&ns).unwrap();
    fs::write(ns.join("device_path"), f.file.path.to_str().unwrap()).unwrap();
    fs::write(ns.join("device_uuid"), uuid([3; 16])).unwrap();
    fs::write(ns.join("buffered_io"), "1").unwrap();
    fs::write(ns.join("enable"), "1").unwrap();
    let mut manager = f.open().unwrap();
    assert!(!sub.exists());
    assert_eq!(
        manager.sealed_file(&f.file.path).unwrap().as_ref(),
        Some(&f.file)
    );
    manager.shutdown().unwrap();
}

#[test]
fn foreign_scope_objects_survive_reopen() {
    let f = Fixture::new();
    let foreign = f
        .configfs
        .join("subsystems")
        .join("nqn.2026-01.local.other:sub-x");
    fs::create_dir(&foreign).unwrap();
    fs::create_dir(foreign.join("namespaces")).unwrap();
    let port = f.configfs.join("ports/99");
    fs::create_dir(&port).unwrap();
    for (key, value) in [
        ("addr_trtype", "tcp"),
        ("addr_adrfam", "ipv4"),
        ("addr_traddr", "192.0.2.1"),
        ("addr_trsvcid", "4420"),
    ] {
        fs::write(port.join(key), value).unwrap();
    }
    fs::create_dir(port.join("subsystems")).unwrap();
    let mut manager = f.open().unwrap();
    assert!(foreign.is_dir());
    assert!(port.is_dir());
    manager.shutdown().unwrap();
    assert!(foreign.is_dir());
    assert!(port.is_dir());
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
fn write_protect_keeps_namespace_and_marks_readonly() {
    let f = Fixture::new();
    let mut manager = f.open().unwrap();
    let binding = manager
        .export_permanent(&f.name(0), f.file.clone(), false)
        .unwrap();
    let ns = f
        .configfs
        .join("subsystems")
        .join(&binding.nqn)
        .join("namespaces/1");
    manager
        .write_protect(std::slice::from_ref(&binding))
        .unwrap();
    assert_eq!(attribute(&ns.join("enable")).unwrap(), "1");
    assert_eq!(attribute(&ns.join("write_protect")).unwrap(), "1");
    assert_eq!(
        manager.sealed_file(&f.file.path).unwrap().as_ref(),
        Some(&f.file)
    );
    assert!(manager.exports.iter().all(|export| export.readonly));
    let error = manager
        .export_permanent(&f.name(0), f.file.clone(), false)
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("fenced WAL cannot be exported writable again"),
        "{error}"
    );
    manager.shutdown().unwrap();
}

#[test]
fn write_protect_commissions_one_controller_per_subsystem() {
    let f = Fixture::new();
    let mut manager = f.open().unwrap();
    let first = manager
        .export_permanent(&f.name(0), f.file.clone(), false)
        .unwrap();
    let second = manager
        .export_permanent(&f.name(1), f.extra_wal("wal-1"), false)
        .unwrap();
    assert_eq!(
        first.nqn, second.nqn,
        "the two files must share one permanent export subsystem"
    );
    let nqn = second.nqn.clone();
    manager.write_protect(&[first, second]).unwrap();
    assert_eq!(
        manager.commission_groups.get(),
        1,
        "one commissioning controller serves every namespace of the subsystem"
    );
    for nsid in [1, 2] {
        let path = f
            .configfs
            .join("subsystems")
            .join(&nqn)
            .join(format!("namespaces/{nsid}/write_protect"));
        assert_eq!(
            attribute(&path).unwrap(),
            "1",
            "namespace {nsid} is write-protected"
        );
    }
    manager.shutdown().unwrap();
}

#[test]
fn write_protect_seals_only_the_named_namespaces_of_a_subsystem() {
    let f = Fixture::new();
    let mut manager = f.open().unwrap();
    let sealed = manager
        .export_permanent(&f.name(0), f.file.clone(), false)
        .unwrap();
    let second_file = f.extra_wal("wal-1");
    let writable = manager
        .export_permanent(&f.name(1), second_file.clone(), false)
        .unwrap();
    assert_eq!(
        sealed.nqn, writable.nqn,
        "both exports must share one subsystem"
    );
    manager
        .write_protect(std::slice::from_ref(&sealed))
        .unwrap();
    let subsystem = f.configfs.join("subsystems").join(&sealed.nqn);
    assert_eq!(
        attribute(&subsystem.join("namespaces/1/write_protect")).unwrap(),
        "1",
        "the sealed namespace is write-protected"
    );
    assert_eq!(
        optional_attribute(&subsystem.join("namespaces/2/write_protect")).unwrap(),
        None,
        "the writable namespace in the same subsystem stays untouched"
    );
    let again = manager
        .export_permanent(&f.name(1), second_file, false)
        .unwrap();
    assert!(
        !again.readonly,
        "an unsealed namespace in a fenced subsystem stays writable"
    );
    manager.shutdown().unwrap();
}

#[test]
fn write_protect_is_not_blocked_by_another_host_controller_on_the_subsystem() {
    // A stale coordinator keeps its controller attached to the same subsystem.
    // Configfs models each attached host as an allowed_hosts link; write
    // protection must fence the namespace through the owner's own controller
    // and must not depend on being the only controller of the subsystem.
    let f = Fixture::new();
    let mut manager = f.open().unwrap();
    let binding = manager
        .export_permanent(&f.name(0), f.file.clone(), false)
        .unwrap();
    let subsystem = f.configfs.join("subsystems").join(&binding.nqn);
    let foreign = "nqn.2026-01.test:stale-coordinator";
    let foreign_host = f.configfs.join("hosts").join(foreign);
    fs::create_dir(&foreign_host).unwrap();
    let foreign_link = subsystem.join("allowed_hosts").join(foreign);
    symlink(&foreign_host, &foreign_link).unwrap();
    assert!(
        foreign_link.is_symlink(),
        "the fixture must present a second host on the subsystem"
    );

    manager
        .write_protect(std::slice::from_ref(&binding))
        .unwrap();

    assert_eq!(
        attribute(&subsystem.join("namespaces/1/write_protect")).unwrap(),
        "1",
        "the namespace is write-protected while another host is attached"
    );
    assert_eq!(
        manager.sealed_file(&f.file.path).unwrap().as_ref(),
        Some(&f.file),
        "write protection must leave a durable sealed marker"
    );
    assert!(
        foreign_link.is_symlink(),
        "write protection must not remove another host's controller claim"
    );
    assert_eq!(
        manager.commission_groups.get(),
        1,
        "one commissioning controller serves the subsystem"
    );
    manager.shutdown().unwrap();
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
    let binding = manager
        .export_permanent(&f.name(0), f.file.clone(), false)
        .unwrap();
    manager.revoke(&binding).unwrap();
    manager.shutdown().unwrap();
}

#[test]
fn replaced_sealed_directory_blocks_recovery() {
    let f = Fixture::new();
    let mut manager = f.open().unwrap();
    let identity = manager.identity().unwrap();
    manager
        .export_permanent(&f.name(0), f.file.clone(), false)
        .unwrap();
    drop(manager);
    let sealed = f.config.state_dir.join("sealed");
    fs::rename(&sealed, f.config.state_dir.join("sealed-original")).unwrap();
    fs::create_dir(&sealed).unwrap();
    let error = match f.open_existing(identity) {
        Ok(_) => panic!("a replaced sealed directory was accepted"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{error}");
    assert_eq!(
        error.to_string(),
        "sealed marker directory identity changed"
    );
}

#[test]
fn persisted_owner_identity_rejects_missing_or_replaced_identity() {
    let f = Fixture::new();
    let mut manager = f.open().unwrap();
    let identity = manager.identity().unwrap();
    manager
        .export_permanent(&f.name(0), f.file.clone(), false)
        .unwrap();
    drop(manager);
    let mut wrong = identity;
    wrong.registry_id[0] ^= 1;
    assert!(f.open_existing(wrong).is_err());
    fs::remove_file(f.config.state_dir.join(TARGET_ID)).unwrap();
    assert!(f.open_existing(identity).is_err());
}

#[test]
fn replaced_backing_file_blocks_revocation_claim() {
    let f = Fixture::new();
    let mut manager = f.open().unwrap();
    let binding = manager
        .export_permanent(&f.name(0), f.file.clone(), false)
        .unwrap();
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
    // Recovery disables the namespace and seals the file it named.
    let mut reopened = f.open().unwrap();
    assert!(!namespace.exists());
    assert!(reopened.sealed_file(&f.file.path).unwrap().is_some());
    reopened.shutdown().unwrap();
}

#[test]
fn crash_at_every_configfs_step_of_open_recovers() {
    let steps = {
        let f = Fixture::new();
        let count = Arc::new(AtomicUsize::new(0));
        let manager = f.open_with(counting_hook(count.clone())).unwrap();
        drop(manager);
        count.load(AtomicOrdering::Relaxed)
    };
    assert!(steps > 0);
    for step in 0..steps {
        let f = Fixture::new();
        assert!(
            f.open_with(fail_at(step)).is_err(),
            "step {step} did not reach the injected crash"
        );
        assert_recovery(&f, None, &[]);
    }
}

#[test]
fn crash_at_every_configfs_step_of_writable_export_recovers() {
    crash_cuts(nothing, |f, manager, _| {
        manager
            .export_permanent(&f.name(0), f.file.clone(), false)
            .map(drop)
    });
}

#[test]
fn crash_at_every_configfs_step_of_readonly_export_recovers() {
    crash_cuts(nothing, |f, manager, _| {
        manager
            .export_permanent(&f.name(0), f.file.clone(), true)
            .map(drop)
    });
}

#[test]
fn crash_at_every_configfs_step_of_permanent_export_recovers() {
    crash_cuts(writable, |f, manager, _| {
        manager
            .export_permanent(&f.name(1), f.extra_wal("wal-1"), false)
            .map(drop)
    });
}

#[test]
fn crash_at_every_configfs_step_of_write_protect_recovers() {
    crash_cuts(writable_and_readonly, |_, manager, prepared| {
        manager.write_protect(&prepared.bindings)
    });
}

#[test]
fn crash_at_every_configfs_step_of_revocation_recovers() {
    crash_cuts(writable_and_readonly, |_, manager, prepared| {
        manager.revoke_all(&prepared.bindings.iter().collect::<Vec<_>>())
    });
}

#[test]
fn crash_at_every_configfs_step_of_retirement_recovers() {
    let steps = {
        let f = Fixture::new();
        let mut manager = f.open().unwrap();
        let binding = manager
            .export_permanent(&f.name(0), f.file.clone(), false)
            .unwrap();
        manager.revoke(&binding).unwrap();
        fs::remove_file(&f.file.path).unwrap();
        let count = Arc::new(AtomicUsize::new(0));
        manager.set_configfs_hook(counting_hook(count.clone()));
        manager.retire_deleted_file(&f.file).unwrap();
        count.load(AtomicOrdering::Relaxed)
    };
    assert!(steps > 0);
    for step in 0..steps {
        let f = Fixture::new();
        let mut manager = f.open().unwrap();
        let identity = manager.identity().unwrap();
        let binding = manager
            .export_permanent(&f.name(0), f.file.clone(), false)
            .unwrap();
        manager.revoke(&binding).unwrap();
        fs::remove_file(&f.file.path).unwrap();
        manager.set_configfs_hook(fail_at(step));
        assert!(
            manager.retire_deleted_file(&f.file).is_err(),
            "step {step} did not reach the injected crash"
        );
        drop(manager);
        let mut reopened = f.open_existing(identity).unwrap();
        if reopened.sealed_file(&f.file.path).unwrap().is_some() {
            reopened.retire_deleted_file(&f.file).unwrap();
        }
        assert!(reopened.sealed_file(&f.file.path).unwrap().is_none());
        reopened.shutdown().unwrap();
    }
}

#[test]
fn crash_at_every_configfs_step_of_shutdown_recovers() {
    crash_cuts(writable_and_readonly, |_, manager, _| manager.shutdown());
}

/// Retiring a deleted WAL syncs its directory, then removes the marker and
/// syncs the marker directory. A crash before the marker removal is durable
/// leaves the marker of an absent file, and a restarted owner repeats the
/// retirement.
#[test]
fn crash_before_tombstone_release_is_durable_repeats_retirement() {
    let f = Fixture::new();
    let mut manager = f.open().unwrap();
    let identity = manager.identity().unwrap();
    let binding = manager
        .export_permanent(&f.name(0), f.file.clone(), false)
        .unwrap();
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

/// A file whose inventory witness is durable was preallocated before any byte
/// was granted, so a restart must refuse it at any other length even when no
/// durable end record survives (a Native leased acknowledgement releases on its
/// target completion, before the owner's persistence barrier). An unwitnessed
/// empty successor was never granted any byte and stays legal.
#[test]
fn a_witnessed_wal_file_must_keep_its_preallocated_size() {
    let temp = tempdir::TempDir::new("witnessed-wal-geometry").unwrap();
    let epoch = temp.path().join("epoch_0000000000000001");
    fs::create_dir(&epoch).unwrap();
    let first = epoch.join("wal_0000000000000000");
    let successor = epoch.join("wal_0000000000000001");
    fs::write(&first, []).unwrap();
    fs::write(&successor, []).unwrap();
    super::verify_live_epoch_sizes(&epoch, 4096, None, &[])
        .expect("an unwitnessed empty successor is legal");
    let error = super::verify_live_epoch_sizes(&epoch, 4096, None, &[0])
        .expect_err("a witnessed file below its preallocated size is truncated");
    assert!(
        error
            .to_string()
            .contains("WAL backing file identity, size or allocation changed"),
        "{error}"
    );
    fs::write(&first, [0u8; 4096]).unwrap();
    super::verify_live_epoch_sizes(&epoch, 4096, None, &[0]).expect("the witnessed size is intact");
}
