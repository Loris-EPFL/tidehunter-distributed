//! Verified Linux live namespaces. Control-plane authentication and lease lifetime
//! remain the caller's responsibility; a device identity is not an authority grant.
use super::NvmeofTransport;
use std::fs::{File, OpenOptions};
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LiveNamespaceAccess {
    ReadOnly,
    ReadWrite,
}

#[derive(Clone, Debug)]
pub struct LiveNamespaceBinding {
    pub namespace_path: PathBuf,
    pub transport: NvmeofTransport,
    pub nqn: String,
    pub uuid: [u8; 16],
    pub nsid: u32,
    pub bytes: u64,
    pub block_size: u32,
    pub access: LiveNamespaceAccess,
    /// Authenticated owner proof for the exact SPDK AIO export, if present.
    pub spdk: Option<std::sync::Arc<super::SpdkTarget>>,
}

pub struct LiveNamespace {
    pub(crate) file: File,
    pub(crate) bytes: u64,
    pub(crate) block_size: u32,
    pub(crate) access: LiveNamespaceAccess,
    pub(crate) controller_lease: Option<std::sync::Arc<()>>,
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
fn attribute(path: &Path) -> io::Result<String> {
    Ok(std::fs::read_to_string(path)?.trim().to_owned())
}
fn controller_name(name: &str) -> bool {
    name.strip_prefix("nvme")
        .is_some_and(|s| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()))
}
fn verify_transport(sysfs: &Path, nqn: &str, transport: NvmeofTransport) -> io::Result<()> {
    let parent = std::fs::canonicalize(sysfs)?
        .parent()
        .ok_or_else(|| invalid("namespace has no sysfs parent"))?
        .to_owned();
    let name = parent
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| invalid("invalid namespace parent"))?;
    let expected = match transport {
        NvmeofTransport::Tcp => "tcp",
        NvmeofTransport::Rdma => "rdma",
    };
    let verify = |p: &Path| -> io::Result<()> {
        if attribute(&p.join("subsysnqn"))? != nqn
            || attribute(&p.join("transport"))? != expected
            || attribute(&p.join("state"))? != "live"
        {
            return Err(invalid("controller identity or transport mismatch"));
        }
        Ok(())
    };
    if controller_name(name) {
        if std::fs::canonicalize(Path::new("/sys/class/nvme").join(name))? != parent {
            return Err(invalid("unregistered controller"));
        }
        return verify(&parent);
    }
    if !name.starts_with("nvme-subsys")
        || std::fs::canonicalize(Path::new("/sys/class/nvme-subsystem").join(name))? != parent
        || attribute(&parent.join("subsysnqn"))? != nqn
    {
        return Err(invalid("subsystem identity mismatch"));
    }
    let mut count = 0;
    for entry in std::fs::read_dir(&parent)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if !controller_name(name) {
            continue;
        }
        count += 1;
        if count > 1 {
            return Err(invalid("multiple controllers"));
        }
        let controller = std::fs::canonicalize(entry.path())?;
        if controller != std::fs::canonicalize(Path::new("/sys/class/nvme").join(name))? {
            return Err(invalid("unregistered controller"));
        }
        verify(&controller)?;
    }
    if count != 1 {
        return Err(invalid("no live controller"));
    }
    Ok(())
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

fn verify_access_policy(
    access: LiveNamespaceAccess,
    host_readonly: bool,
    spdk: bool,
    identify_readonly: Option<bool>,
) -> io::Result<()> {
    let expected_ro = access == LiveNamespaceAccess::ReadOnly;
    if host_readonly != expected_ro {
        return Err(invalid("namespace access mismatch"));
    }
    if !spdk && identify_readonly != Some(expected_ro) {
        return Err(invalid("target write protection mismatch"));
    }
    Ok(())
}

impl LiveNamespace {
    fn valid_geometry(bytes: u64, block_size: u32) -> bool {
        block_size >= 512
            && block_size.is_power_of_two()
            && bytes != 0
            && bytes.is_multiple_of(u64::from(block_size))
    }
    pub fn open(binding: &LiveNamespaceBinding) -> io::Result<Self> {
        if !Self::valid_geometry(binding.bytes, binding.block_size)
            || binding.uuid == [0; 16]
            || binding.nsid == 0
            || !binding.nqn.starts_with("nqn.")
            || binding.nqn.len() > 223
        {
            return Err(invalid("invalid namespace binding"));
        }
        let file = OpenOptions::new()
            .read(true)
            .write(binding.access == LiveNamespaceAccess::ReadWrite)
            .custom_flags(libc::O_DIRECT | libc::O_CLOEXEC)
            .open(&binding.namespace_path)?;
        let meta = file.metadata()?;
        if !meta.file_type().is_block_device() {
            return Err(invalid("not a block namespace"));
        }
        let dev = meta.rdev();
        let sysfs = PathBuf::from(format!(
            "/sys/dev/block/{}:{}",
            libc::major(dev),
            libc::minor(dev)
        ));
        let u = binding.uuid;
        let uuid = format!(
            "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
            u[0],
            u[1],
            u[2],
            u[3],
            u[4],
            u[5],
            u[6],
            u[7],
            u[8],
            u[9],
            u[10],
            u[11],
            u[12],
            u[13],
            u[14],
            u[15]
        );
        if attribute(&sysfs.join("wwid"))? != format!("uuid.{uuid}")
            || attribute(&sysfs.join("uuid"))? != uuid
            || attribute(&sysfs.join("nsid"))? != binding.nsid.to_string()
        {
            return Err(invalid("namespace identity mismatch"));
        }
        verify_transport(&sysfs, &binding.nqn, binding.transport)?;
        let fd = file.as_raw_fd();
        // SAFETY: F_GETFL reads status flags from the live descriptor.
        let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
        if flags < 0 {
            return Err(io::Error::last_os_error());
        }
        let expected_mode = if binding.access == LiveNamespaceAccess::ReadWrite {
            libc::O_RDWR
        } else {
            libc::O_RDONLY
        };
        if flags & libc::O_DIRECT == 0 || flags & libc::O_ACCMODE != expected_mode {
            return Err(invalid("namespace descriptor access mismatch"));
        }
        let mut readonly: libc::c_int = 0;
        let mut block_size: libc::c_int = 0;
        let mut bytes: u64 = 0;
        // SAFETY: Linux block ioctls write the indicated output types to live pointers.
        if unsafe { libc::ioctl(fd, 0x125e, &mut readonly) } < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: BLKSSZGET writes an int to the valid pointer.
        if unsafe { libc::ioctl(fd, 0x1268, &mut block_size) } < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: BLKGETSIZE64 writes a u64 to the valid pointer.
        if unsafe { libc::ioctl(fd, 0x80081272, &mut bytes) } < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: NVME_IOCTL_ID uses the open descriptor without a pointer.
        let nsid = unsafe { libc::ioctl(fd, 0x4e40) };
        if nsid < 0 {
            return Err(io::Error::last_os_error());
        }
        if block_size != binding.block_size as i32
            || bytes != binding.bytes
            || nsid as u32 != binding.nsid
        {
            return Err(invalid("namespace geometry or access mismatch"));
        }
        let identify_readonly = if binding.spdk.is_none() {
            let mut identify = [0u8; 4096];
            let mut cmd = NvmeAdminCommand {
                opcode: 6,
                nsid: binding.nsid,
                addr: identify.as_mut_ptr() as u64,
                data_len: 4096,
                ..Default::default()
            };
            // SAFETY: passthrough reads the command and writes only the live identify buffer before returning.
            let result = unsafe { libc::ioctl(fd, 0xc0484e41, &mut cmd) };
            if result < 0 {
                return Err(io::Error::last_os_error());
            }
            if result != 0 {
                return Err(invalid("target identify failed"));
            }
            Some(identify[99] & 1 != 0)
        } else {
            None
        };
        verify_access_policy(
            binding.access,
            readonly != 0,
            binding.spdk.is_some(),
            identify_readonly,
        )?;
        Ok(Self {
            file,
            bytes,
            block_size: binding.block_size,
            access: binding.access,
            controller_lease: None,
        })
    }
    /// Local-file functional qualification only. This does not qualify NVMe hardware.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn open_local_file(
        path: &Path,
        bytes: u64,
        block_size: u32,
        access: LiveNamespaceAccess,
    ) -> io::Result<Self> {
        if !Self::valid_geometry(bytes, block_size) {
            return Err(invalid("invalid fixture geometry"));
        }
        let file = OpenOptions::new()
            .read(true)
            .write(access == LiveNamespaceAccess::ReadWrite)
            .custom_flags(libc::O_DIRECT | libc::O_CLOEXEC)
            .open(path)?;
        let meta = file.metadata()?;
        if !meta.is_file() || meta.len() != bytes {
            return Err(invalid("invalid local file fixture"));
        }
        Ok(Self {
            file,
            bytes,
            block_size,
            access,
            controller_lease: None,
        })
    }
    pub fn bytes(&self) -> u64 {
        self.bytes
    }
    pub fn block_size(&self) -> u32 {
        self.block_size
    }
    pub fn access(&self) -> LiveNamespaceAccess {
        self.access
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn production_open_rejects_regular_file() {
        let dir = tempdir::TempDir::new("live-binding").unwrap();
        let path = dir.path().join("wal");
        let file = File::create(&path).unwrap();
        file.set_len(4096).unwrap();
        let mut binding = LiveNamespaceBinding {
            namespace_path: path.clone(),
            transport: NvmeofTransport::Tcp,
            nqn: "nqn.2026-01.test:wal".to_owned(),
            uuid: [1; 16],
            nsid: 1,
            bytes: 4096,
            block_size: 4096,
            access: LiveNamespaceAccess::ReadWrite,
            spdk: None,
        };
        assert!(LiveNamespace::open(&binding).is_err());
        binding.spdk = Some(std::sync::Arc::new(super::super::SpdkTarget {
            rpc_socket: dir.path().join("rpc.sock"),
            pid: 1,
            process_start_ticks: 1,
            bdev_name: "test".into(),
            image_path: path.clone(),
        }));
        assert!(LiveNamespace::open(&binding).is_err());
        assert!(
            LiveNamespace::open_local_file(&path, 4096, 4096, LiveNamespaceAccess::ReadOnly)
                .is_ok()
        );
        assert!(
            LiveNamespace::open_local_file(&path, 8192, 4096, LiveNamespaceAccess::ReadOnly)
                .is_err()
        );
    }

    #[test]
    fn access_policy_keeps_kernel_protection_and_requires_host_access() {
        use LiveNamespaceAccess::{ReadOnly, ReadWrite};
        assert!(verify_access_policy(ReadOnly, true, false, Some(true)).is_ok());
        assert!(verify_access_policy(ReadOnly, true, false, Some(false)).is_err());
        assert!(verify_access_policy(ReadOnly, true, false, None).is_err());
        assert!(verify_access_policy(ReadWrite, false, false, Some(false)).is_ok());
        assert!(verify_access_policy(ReadWrite, false, false, Some(true)).is_err());
        assert!(verify_access_policy(ReadOnly, true, true, None).is_ok());
        assert!(verify_access_policy(ReadWrite, false, true, None).is_ok());
        assert!(verify_access_policy(ReadOnly, false, true, None).is_err());
        assert!(verify_access_policy(ReadWrite, true, true, None).is_err());
    }
}
