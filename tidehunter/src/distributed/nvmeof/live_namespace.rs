//! Verified Linux live namespaces. Control-plane authentication and lease lifetime
//! remain the caller's responsibility; a device identity is not an authority grant.
use super::NvmeofTransport;
use crate::distributed::sys;
use std::fs::{File, OpenOptions};
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

#[cfg(test)]
mod tests;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LiveNamespaceAccess {
    ReadOnly,
    /// Read-only descriptor shared with a writer on the same host.
    ReadShared,
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

/// Why a pinned namespace failed verification.
///
/// The write and read paths resume from `ControllerLost` only: the kernel
/// stopped, removed, or replaced the controller the lease pinned. A live
/// controller or namespace whose identity or transport differs is a real
/// mismatch and stays fatal.
#[derive(Debug)]
pub(crate) enum VerifyError {
    ControllerLost,
    Identity(&'static str),
}

impl std::fmt::Display for VerifyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ControllerLost => f.write_str("pinned controller is not live"),
            Self::Identity(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for VerifyError {}

/// A namespace the lease pinned can no longer use its controller.
pub(crate) fn controller_lost_error() -> io::Error {
    io::Error::new(
        io::ErrorKind::ConnectionAborted,
        VerifyError::ControllerLost,
    )
}
/// A live controller or namespace names another identity than the binding.
pub(crate) fn identity_error(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, VerifyError::Identity(message))
}
fn vanished(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::NotFound || error.raw_os_error() == Some(libc::ENODEV)
}
/// A sysfs read that failed because the entry vanished means the controller is
/// gone, not that its identity differs.
pub(crate) fn lost_if_vanished(error: io::Error) -> io::Error {
    if vanished(&error) {
        controller_lost_error()
    } else {
        error
    }
}
fn read_attribute(path: &Path) -> io::Result<String> {
    attribute(path).map_err(lost_if_vanished)
}

/// Whether `error` reports a controller the write or read path may reconnect.
///
/// Walks the source chain, so a context wrapper around the typed cause still
/// classifies it. Never matches a message.
pub(crate) fn controller_lost(error: &io::Error) -> bool {
    let Some(cause) = error.get_ref() else {
        return false;
    };
    let mut current: &(dyn std::error::Error + 'static) = cause;
    loop {
        if matches!(
            current.downcast_ref::<VerifyError>(),
            Some(VerifyError::ControllerLost)
        ) {
            return true;
        }
        match current.source() {
            Some(next) => current = next,
            None => return false,
        }
    }
}
fn verify_transport(sysfs: &Path, nqn: &str, transport: NvmeofTransport) -> io::Result<()> {
    verify_transport_at(
        sysfs,
        nqn,
        transport,
        Path::new("/sys/class/nvme"),
        Path::new("/sys/class/nvme-subsystem"),
    )
}

/// The transport check against explicit class roots, so a fixture can drive it.
fn verify_transport_at(
    sysfs: &Path,
    nqn: &str,
    transport: NvmeofTransport,
    nvme_root: &Path,
    subsystem_root: &Path,
) -> io::Result<()> {
    let resolved = std::fs::canonicalize(sysfs).map_err(lost_if_vanished)?;
    let parent = resolved
        .parent()
        .ok_or_else(|| identity_error("namespace has no sysfs parent"))?;
    let name = parent
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| identity_error("invalid namespace parent"))?;
    let expected = match transport {
        NvmeofTransport::Tcp => "tcp",
        NvmeofTransport::Rdma => "rdma",
    };
    let verify = |p: &Path| -> io::Result<()> {
        if read_attribute(&p.join("subsysnqn"))? != nqn
            || read_attribute(&p.join("transport"))? != expected
        {
            return Err(identity_error("controller identity or transport mismatch"));
        }
        match read_attribute(&p.join("state"))? {
            state if state == "live" => Ok(()),
            // A controller the kernel is deleting, resetting, or has stopped
            // cannot serve I/O. The write and read paths reconnect from here.
            _ => Err(controller_lost_error()),
        }
    };
    if controller_name(name) {
        if std::fs::canonicalize(nvme_root.join(name)).map_err(lost_if_vanished)? != parent {
            return Err(controller_lost_error());
        }
        return verify(parent);
    }
    if !name.starts_with("nvme-subsys")
        || std::fs::canonicalize(subsystem_root.join(name)).map_err(lost_if_vanished)? != parent
        || read_attribute(&parent.join("subsysnqn"))? != nqn
    {
        return Err(identity_error("subsystem identity mismatch"));
    }
    let mut count = 0;
    for entry in std::fs::read_dir(parent)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if !controller_name(name) {
            continue;
        }
        count += 1;
        if count > 1 {
            return Err(identity_error("multiple controllers"));
        }
        let controller = std::fs::canonicalize(entry.path()).map_err(lost_if_vanished)?;
        if controller != std::fs::canonicalize(nvme_root.join(name)).map_err(lost_if_vanished)? {
            return Err(controller_lost_error());
        }
        verify(&controller)?;
    }
    if count != 1 {
        return Err(controller_lost_error());
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
    identify_readonly: bool,
) -> io::Result<()> {
    if access == LiveNamespaceAccess::ReadShared {
        return Ok(());
    }
    let expected_ro = access == LiveNamespaceAccess::ReadOnly;
    if host_readonly != expected_ro {
        return Err(invalid("namespace access mismatch"));
    }
    if identify_readonly != expected_ro {
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
        // The lease resolved this path on the pinned controller. A node that
        // vanished since then is that controller's loss, not an identity
        // mismatch: the write and read paths reconnect from it.
        let file = Self::open_device(binding).map_err(lost_if_vanished)?;
        Self::verify_opened_device(binding, file)
    }

    /// Open separately so readiness retries cannot swallow verification errors.
    pub(crate) fn open_device(binding: &LiveNamespaceBinding) -> io::Result<File> {
        if !Self::valid_geometry(binding.bytes, binding.block_size)
            || binding.uuid == [0; 16]
            || binding.nsid == 0
            || !binding.nqn.starts_with("nqn.")
            || binding.nqn.len() > 223
        {
            return Err(invalid("invalid namespace binding"));
        }
        OpenOptions::new()
            .read(true)
            .write(binding.access == LiveNamespaceAccess::ReadWrite)
            .custom_flags(sys::direct_io_flag()? | libc::O_CLOEXEC)
            .open(&binding.namespace_path)
    }

    pub(crate) fn verify_opened_device(
        binding: &LiveNamespaceBinding,
        file: File,
    ) -> io::Result<Self> {
        let meta = file.metadata()?;
        if !meta.file_type().is_block_device() {
            return Err(identity_error("not a block namespace"));
        }
        let sysfs = PathBuf::from(format!(
            "/sys/dev/block/{}",
            sys::device_number(meta.rdev())
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
        if read_attribute(&sysfs.join("wwid"))? != format!("uuid.{uuid}")
            || read_attribute(&sysfs.join("uuid"))? != uuid
            || read_attribute(&sysfs.join("nsid"))? != binding.nsid.to_string()
        {
            return Err(identity_error("namespace identity mismatch"));
        }
        verify_transport(&sysfs, &binding.nqn, binding.transport)?;
        let fd = file.as_raw_fd();
        // SAFETY: F_GETFL reads status flags from the live descriptor.
        let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
        if flags < 0 {
            return Err(lost_if_vanished(io::Error::last_os_error()));
        }
        let expected_mode = if binding.access == LiveNamespaceAccess::ReadWrite {
            libc::O_RDWR
        } else {
            libc::O_RDONLY
        };
        if flags & sys::direct_io_flag()? == 0 || flags & libc::O_ACCMODE != expected_mode {
            return Err(identity_error("namespace descriptor access mismatch"));
        }
        let mut readonly: libc::c_int = 0;
        let mut block_size: libc::c_int = 0;
        let mut bytes: u64 = 0;
        // SAFETY: BLKROGET writes an int to readonly; fd and the output stay live.
        if unsafe { libc::ioctl(fd, 0x125e, &mut readonly) } < 0 {
            return Err(lost_if_vanished(io::Error::last_os_error()));
        }
        // SAFETY: BLKSSZGET writes an int to the valid pointer.
        if unsafe { libc::ioctl(fd, 0x1268, &mut block_size) } < 0 {
            return Err(lost_if_vanished(io::Error::last_os_error()));
        }
        // SAFETY: BLKGETSIZE64 writes a u64 to the valid pointer.
        if unsafe { libc::ioctl(fd, 0x80081272, &mut bytes) } < 0 {
            return Err(lost_if_vanished(io::Error::last_os_error()));
        }
        // SAFETY: NVME_IOCTL_ID uses the open descriptor without a pointer.
        let nsid = unsafe { libc::ioctl(fd, 0x4e40) };
        if nsid < 0 {
            return Err(lost_if_vanished(io::Error::last_os_error()));
        }
        if block_size != binding.block_size as i32
            || bytes != binding.bytes
            || nsid as u32 != binding.nsid
        {
            return Err(identity_error("namespace geometry or access mismatch"));
        }
        let mut identify = [0u8; 4096];
        let mut cmd = NvmeAdminCommand {
            opcode: 6,
            nsid: binding.nsid,
            addr: identify.as_mut_ptr() as u64,
            data_len: 4096,
            ..Default::default()
        };
        // SAFETY: cmd has the Linux NVMe passthrough layout. Its data_len
        // matches the live 4096-byte identify buffer; ioctl is synchronous.
        let result = unsafe { libc::ioctl(fd, 0xc0484e41, &mut cmd) };
        if result < 0 {
            return Err(io::Error::last_os_error());
        }
        if result != 0 {
            return Err(identity_error("target identify failed"));
        }
        verify_access_policy(binding.access, readonly != 0, identify[99] & 1 != 0)?;
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
        Self::open_fixture_file(path, bytes, block_size, access, true)
    }

    /// Buffered regular-file control for workstation measurements, not NVMe qualification.
    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn open_local_file_buffered(
        path: &Path,
        bytes: u64,
        block_size: u32,
        access: LiveNamespaceAccess,
    ) -> io::Result<Self> {
        Self::open_fixture_file(path, bytes, block_size, access, false)
    }

    #[cfg(any(test, feature = "test-utils"))]
    fn open_fixture_file(
        path: &Path,
        bytes: u64,
        block_size: u32,
        access: LiveNamespaceAccess,
        direct: bool,
    ) -> io::Result<Self> {
        if !Self::valid_geometry(bytes, block_size) {
            return Err(invalid("invalid fixture geometry"));
        }
        let direct = if direct { sys::direct_io_flag()? } else { 0 };
        let file = OpenOptions::new()
            .read(true)
            .write(access == LiveNamespaceAccess::ReadWrite)
            .custom_flags(libc::O_CLOEXEC | direct)
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
