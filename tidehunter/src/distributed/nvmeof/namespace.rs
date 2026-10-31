//! Kernel namespace identity and read-only verification. Management and connect
//! operations are intentionally not available through this data-plane module.

use super::ExportManifest;
use std::fs::{File, OpenOptions};
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NvmeofTransport {
    Tcp,
    Rdma,
}

/// Production input accepts only a kernel block namespace, never a regular file.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NamespaceBinding {
    pub namespace_path: PathBuf,
    pub transport: NvmeofTransport,
}

#[repr(C)]
#[derive(Default)]
struct NvmeAdminCommand {
    opcode: u8,
    flags: u8,
    reserved: u16,
    nsid: u32,
    cdw2: u32,
    cdw3: u32,
    metadata: u64,
    addr: u64,
    metadata_len: u32,
    data_len: u32,
    cdw10: u32,
    cdw11: u32,
    cdw12: u32,
    cdw13: u32,
    cdw14: u32,
    cdw15: u32,
    timeout_ms: u32,
    result: u32,
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn attribute(path: &Path) -> io::Result<String> {
    Ok(std::fs::read_to_string(path)?.trim().to_owned())
}

fn uuid_string(bytes: [u8; 16]) -> String {
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0],
        bytes[1],
        bytes[2],
        bytes[3],
        bytes[4],
        bytes[5],
        bytes[6],
        bytes[7],
        bytes[8],
        bytes[9],
        bytes[10],
        bytes[11],
        bytes[12],
        bytes[13],
        bytes[14],
        bytes[15]
    )
}

fn controller_name(name: &str) -> bool {
    name.strip_prefix("nvme")
        .is_some_and(|suffix| !suffix.is_empty() && suffix.bytes().all(|b| b.is_ascii_digit()))
}

// Native multipath exports a namespace head under an nvme-subsystem rather
// than a controller. Restrict it to exactly one live, verified controller so
// another path or transport cannot silently serve requests.
fn verify_transport(sysfs: &Path, nqn: &str, transport: NvmeofTransport) -> io::Result<()> {
    verify_transport_at(sysfs, Path::new("/sys/class"), nqn, transport)
}
fn verify_transport_at(
    sysfs: &Path,
    class_root: &Path,
    nqn: &str,
    transport: NvmeofTransport,
) -> io::Result<()> {
    let resolved = std::fs::canonicalize(sysfs)?;
    let parent = resolved
        .parent()
        .ok_or_else(|| invalid("namespace has no sysfs parent"))?;
    let name = parent
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| invalid("invalid namespace sysfs parent"))?;
    let expected = match transport {
        NvmeofTransport::Tcp => "tcp",
        NvmeofTransport::Rdma => "rdma",
    };
    let verify_controller = |controller: &Path| -> io::Result<()> {
        if attribute(&controller.join("subsysnqn"))? != nqn
            || attribute(&controller.join("transport"))? != expected
            || attribute(&controller.join("state"))? != "live"
        {
            return Err(invalid(
                "namespace controller identity or transport mismatch",
            ));
        }
        Ok(())
    };
    if controller_name(name) {
        if std::fs::canonicalize(class_root.join("nvme").join(name))? != parent {
            return Err(invalid("namespace controller is not registered"));
        }
        return verify_controller(parent);
    }
    if !name.starts_with("nvme-subsys")
        || std::fs::canonicalize(class_root.join("nvme-subsystem").join(name))? != parent
        || attribute(&parent.join("subsysnqn"))? != nqn
    {
        return Err(invalid("namespace subsystem identity mismatch"));
    }
    let mut controllers = 0usize;
    for entry in std::fs::read_dir(parent)? {
        let entry = entry?;
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        if !controller_name(&name) {
            continue;
        }
        controllers += 1;
        if controllers > 1 {
            return Err(invalid("multiple namespace controllers are not supported"));
        }
        let controller = std::fs::canonicalize(entry.path())?;
        if controller != std::fs::canonicalize(class_root.join("nvme").join(&name))? {
            return Err(invalid("subsystem controller is not registered"));
        }
        verify_controller(&controller)?;
    }
    if controllers != 1 {
        return Err(invalid("namespace subsystem has no live controller"));
    }
    Ok(())
}

/// Opens an actual read-only, direct-I/O NVMe namespace and verifies kernel
/// identity plus the target's identify-namespace write-protection bit. The
/// caller must separately authenticate the manifest's catalog with the owner.
pub(crate) fn open_namespace(
    binding: &NamespaceBinding,
    manifest: &ExportManifest,
) -> io::Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECT | libc::O_CLOEXEC)
        .open(&binding.namespace_path)?;
    let metadata = file.metadata()?;
    if !metadata.file_type().is_block_device() {
        return Err(invalid("not a block namespace"));
    }
    let device = metadata.rdev();
    let sysfs = PathBuf::from(format!(
        "/sys/dev/block/{}:{}",
        libc::major(device),
        libc::minor(device)
    ));
    let expected_uuid = uuid_string(manifest.export_uuid);
    // wwid must explicitly carry UUID: sysfs uuid alone may fall back to NGUID.
    if attribute(&sysfs.join("wwid"))? != format!("uuid.{expected_uuid}")
        || attribute(&sysfs.join("uuid"))? != expected_uuid
        || attribute(&sysfs.join("nsid"))? != manifest.nsid.to_string()
    {
        return Err(invalid("namespace identity or transport mismatch"));
    }
    verify_transport(&sysfs, &manifest.nqn, binding.transport)?;
    let fd = file.as_raw_fd();
    let mut readonly: libc::c_int = 0;
    let mut block_size: libc::c_int = 0;
    let mut size: u64 = 0;
    // SAFETY: each ioctl receives a valid pointer to the output type specified
    // by the Linux block-device ABI, and the descriptor remains open throughout.
    // Capture errno immediately after the first failing operation.
    let ro_result = unsafe { libc::ioctl(fd, 0x125e, &mut readonly) };
    if ro_result < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: the descriptor and correctly typed output pointer are valid.
    let bs_result = unsafe { libc::ioctl(fd, 0x1268, &mut block_size) };
    if bs_result < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: BLKGETSIZE64 writes a u64 into this live output pointer.
    let size_result = unsafe { libc::ioctl(fd, 0x80081272, &mut size) };
    if size_result < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: NVME_IOCTL_ID requires no pointer and the descriptor remains live.
    let nsid = unsafe { libc::ioctl(fd, 0x4e40) };
    if nsid < 0 {
        return Err(io::Error::last_os_error());
    }
    if readonly != 1
        || block_size != manifest.block_size as i32
        || size != manifest.image_len
        || nsid as u32 != manifest.nsid
    {
        return Err(invalid("namespace geometry or read-only state mismatch"));
    }
    let mut identify = [0u8; 4096];
    let mut command = NvmeAdminCommand {
        opcode: 0x06,
        nsid: manifest.nsid,
        addr: identify.as_mut_ptr() as u64,
        data_len: identify.len() as u32,
        ..Default::default()
    };
    // SAFETY: the NVMe passthrough ABI reads the initialized command and writes
    // at most data_len bytes into the live identify array before ioctl returns.
    let result = unsafe { libc::ioctl(fd, 0xc0484e41, &mut command) };
    if result < 0 {
        return Err(io::Error::last_os_error());
    }
    if result != 0 || identify[99] & 1 == 0 {
        return Err(invalid("target namespace is not write protected"));
    }
    Ok(file)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::symlink;

    #[test]
    fn subsystem_head_requires_one_matching_live_controller() {
        let temp = tempdir::TempDir::new("nvmeof-sysfs").unwrap();
        let root = temp.path();
        let class = root.join("class");
        std::fs::create_dir_all(class.join("nvme")).unwrap();
        std::fs::create_dir_all(class.join("nvme-subsystem")).unwrap();
        let subsystem = root.join("devices/nvme-subsys0");
        let head = subsystem.join("nvme0n1");
        std::fs::create_dir_all(&head).unwrap();
        std::fs::write(subsystem.join("subsysnqn"), "nqn.test:wal\n").unwrap();
        symlink(&subsystem, class.join("nvme-subsystem/nvme-subsys0")).unwrap();
        let ctrl = root.join("controllers/nvme0");
        std::fs::create_dir_all(&ctrl).unwrap();
        std::fs::write(ctrl.join("subsysnqn"), "nqn.test:wal\n").unwrap();
        std::fs::write(ctrl.join("transport"), "tcp\n").unwrap();
        std::fs::write(ctrl.join("state"), "live\n").unwrap();
        symlink(&ctrl, class.join("nvme/nvme0")).unwrap();
        symlink(&ctrl, subsystem.join("nvme0")).unwrap();
        assert!(verify_transport_at(&head, &class, "nqn.test:wal", NvmeofTransport::Tcp).is_ok());
        assert!(verify_transport_at(&head, &class, "nqn.test:wal", NvmeofTransport::Rdma).is_err());
        assert!(
            verify_transport_at(&head, &class, "nqn.test:other", NvmeofTransport::Tcp).is_err()
        );
        let other = root.join("controllers/nvme1");
        std::fs::create_dir_all(&other).unwrap();
        symlink(&other, class.join("nvme/nvme1")).unwrap();
        symlink(&other, subsystem.join("nvme1")).unwrap();
        assert!(verify_transport_at(&head, &class, "nqn.test:wal", NvmeofTransport::Tcp).is_err());
    }
}
