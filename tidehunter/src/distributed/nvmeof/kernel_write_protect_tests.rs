//! Kernel proof that NVMe namespace write protection (Set Features FID 0x84)
//! stops a remote controller from writing a permanent export, while the
//! owner's export and sealed marker stay in place.
//!
//! Ignored by default: it needs root, `nvmet`, `nvmet-tcp`, `nvme-tcp`, a
//! mounted configfs and a loopback listener. CI runs it in the kernel proof
//! job. The fixture is a real configfs target, never the ordinary-file model.
use super::live_connect::{
    ControllerAdoption, LiveConnectTarget, LiveConnectionRegistry, RegistryMode,
};
use super::live_namespace::LiveNamespaceAccess;
use super::live_storage::{FILES_PER_SUBSYSTEM, wal_export_name};
use super::live_target::{LiveFileIdentity, LiveTargetManager};
use super::{LiveTargetConfig, NvmeofTransport};
use crate::distributed::tcp::host_identity::{NQN_PREFIX, effective_host_nqn, subsystem_scope};
use std::fs::{self, File, OpenOptions};
use std::io;
use std::os::unix::fs::{DirBuilderExt, FileExt, MetadataExt};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

const NVMET: &str = "/sys/kernel/config/nvmet";

fn write_block(path: &Path, fill: u8) -> io::Result<()> {
    let file = OpenOptions::new().read(true).write(true).open(path)?;
    file.write_all_at(&[fill; 4096], 0)?;
    file.sync_all()
}

fn read_block(path: &Path) -> io::Result<[u8; 4096]> {
    let file = File::open(path)?;
    let mut block = [0; 4096];
    file.read_exact_at(&mut block, 0)?;
    Ok(block)
}

#[test]
#[ignore = "needs root and nvmet-tcp"]
fn write_protect_blocks_a_remote_write_and_keeps_the_export() {
    let root = PathBuf::from(format!("/run/tidehunter-kernel-wp-{}", std::process::id()));
    let _ = fs::remove_dir_all(&root);
    fs::DirBuilder::new()
        .mode(0o700)
        .create(&root)
        .expect("create kernel write-protect root");
    let state = root.join("state");
    let registry = root.join("registry");
    let wal = root.join("wal");
    fs::DirBuilder::new()
        .mode(0o700)
        .create(&state)
        .expect("create target state directory");
    fs::write(&wal, [0u8; 4096]).expect("create WAL file");
    let metadata = fs::metadata(&wal).expect("stat WAL file");
    let file = LiveFileIdentity {
        path: wal.clone(),
        device: metadata.dev(),
        inode: metadata.ino(),
        bytes: metadata.len(),
        block_size: 4096,
    };
    let database = [11; 16];
    let partition = 0;
    let service = 5_000 + u16::try_from(std::process::id() % 1_000).expect("port range");
    let config = LiveTargetConfig {
        address: "127.0.0.1".into(),
        service,
        transport: NvmeofTransport::Tcp,
        host_nqn: effective_host_nqn(&database, partition),
        port_id: 42,
        state_dir: state,
        commissioning_port_id: 43,
        commissioning_service: service + 1,
    };
    let mut manager = LiveTargetManager::open(config.clone()).expect("open the kernel target");
    let name = wal_export_name(FILES_PER_SUBSYSTEM, database, partition, 1, 0);
    let binding = manager
        .export_permanent(&name, file.clone(), false)
        .expect("export the permanent writable namespace");
    let target = LiveConnectTarget {
        nqn: binding.nqn.clone(),
        uuid: binding.uuid,
        nsid: binding.nsid,
        bytes: binding.bytes,
        block_size: binding.block_size,
        address: binding.address.clone(),
        service: binding.service,
        transport: binding.transport,
        geometry: Default::default(),
    };
    let connections = LiveConnectionRegistry::open(&registry, RegistryMode::Adopt)
        .expect("open controller registry");
    let deadline = Instant::now() + Duration::from_secs(20);
    let (mut controller, namespace) = connections
        .connect(
            &target,
            &config.host_nqn,
            rand::random::<[u8; 16]>(),
            LiveNamespaceAccess::ReadWrite,
            // An admin-only proof owns no namespace I/O; never adopt another
            // process's controller.
            ControllerAdoption::Fresh {
                owner_incarnation: None,
            },
            deadline,
        )
        .expect("connect a remote controller");
    let namespace_path = namespace.namespace_path.clone();
    drop(namespace);
    write_block(&namespace_path, 0xab).expect("write before write protection");

    manager
        .write_protect(std::slice::from_ref(&binding))
        .expect("write-protect the namespace");
    assert!(
        write_block(&namespace_path, 0xcd).is_err(),
        "a write-protected namespace accepted a remote write"
    );
    let block = read_block(&namespace_path).expect("read the protected block");
    assert_eq!(
        &block[..16],
        &[0xab; 16],
        "the protected namespace lost the last durable bytes"
    );
    assert_eq!(
        manager
            .sealed_file(&wal)
            .expect("read sealed marker")
            .as_ref(),
        Some(&file),
        "write protection must leave a durable sealed marker"
    );
    controller
        .disconnect(Instant::now() + Duration::from_secs(20))
        .expect("disconnect the remote controller");
    manager.shutdown().expect("shut the target down");

    let scope = format!("{NQN_PREFIX}{}", subsystem_scope(&database, partition));
    let prefix = format!("{scope}sub-");
    for entry in fs::read_dir(format!("{NVMET}/subsystems")).expect("list subsystems") {
        let entry = entry.expect("subsystem entry");
        let name = entry.file_name().to_string_lossy().into_owned();
        assert!(
            !name.starts_with(&prefix),
            "shutdown left owned subsystem {name}"
        );
    }
    for host in [config.host_nqn.clone(), format!("{scope}commission")] {
        assert!(
            !Path::new(&format!("{NVMET}/hosts/{host}")).exists(),
            "shutdown left owned host {host}"
        );
    }
    for port in [config.port_id, config.commissioning_port_id] {
        assert!(
            !Path::new(&format!("{NVMET}/ports/{port}")).exists(),
            "shutdown left owned port {port}"
        );
    }
    fs::remove_dir_all(&root).expect("remove the kernel write-protect root");
}
