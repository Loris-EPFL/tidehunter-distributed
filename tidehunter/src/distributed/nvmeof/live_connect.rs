//! Owned Linux fabrics connections. The caller must drain and close every
//! namespace I/O handle before disconnecting the controller. A connection is
//! not a durability or authorization receipt.
pub use super::LiveQueueGeometry;
use super::NvmeofTransport;
use super::live_namespace::{LiveNamespace, LiveNamespaceAccess, LiveNamespaceBinding};
use crate::distributed::sys;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::net::IpAddr;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{DirBuilderExt, FileTypeExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};

mod registry;
mod sysfs;

#[cfg(test)]
mod tests;

pub use registry::LiveConnectionRegistry;
pub(crate) use sysfs::{pin_current_thread, queue_cpus};

use registry::*;
use sysfs::*;

const CLEANUP_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct LiveConnectTarget {
    pub nqn: String,
    pub uuid: [u8; 16],
    pub nsid: u32,
    pub bytes: u64,
    pub block_size: u32,
    pub address: String,
    pub service: u16,
    pub transport: NvmeofTransport,
    #[serde(default)]
    pub geometry: LiveQueueGeometry,
}

fn connect_options(target: &LiveConnectTarget, host_nqn: &str, host_id: [u8; 16]) -> String {
    let geometry = target.geometry;
    let mut options = format!(
        "nqn={},transport={},traddr={},trsvcid={},hostnqn={},hostid={},duplicate_connect,ctrl_loss_tmo=0,nr_io_queues={},queue_size={}",
        target.nqn,
        transport(target.transport),
        target.address,
        target.service,
        host_nqn,
        uuid_string(host_id),
        geometry.io_queues,
        geometry.queue_size
    );
    if geometry.poll_queues > 0 {
        options.push_str(&format!(",nr_poll_queues={}", geometry.poll_queues));
    }
    options
}

fn invalid(s: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, s)
}
fn timeout() -> io::Error {
    io::Error::new(io::ErrorKind::TimedOut, "NVMe controller deadline")
}
fn remaining(deadline: Instant) -> io::Result<Duration> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|d| !d.is_zero())
        .ok_or_else(timeout)
}

fn with_context(operation: &str, path: &Path, error: io::Error) -> io::Error {
    io::Error::new(
        error.kind(),
        format!("{operation} {}: {error}", path.display()),
    )
}

fn connect_error_after_cleanup(primary: io::Error, cleanup: io::Result<()>) -> io::Error {
    match cleanup {
        Ok(()) => primary,
        Err(error) => io::Error::new(
            primary.kind(),
            format!("{primary}; controller cleanup failed: {error}"),
        ),
    }
}

fn uuid_string(u: [u8; 16]) -> String {
    format!(
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
    )
}
fn valid_nqn(nqn: &str) -> bool {
    nqn.starts_with("nqn.")
        && nqn.len() <= 223
        && nqn
            .bytes()
            .all(|b| b.is_ascii_graphic() && b != b',' && b != b'=')
}
fn transport(t: NvmeofTransport) -> &'static str {
    match t {
        NvmeofTransport::Tcp => "tcp",
        NvmeofTransport::Rdma => "rdma",
    }
}

/// Holds the exact controller returned by the fabrics control device. The
/// pinned sysfs directory prevents a reused nvme number from being deleted.
pub struct LiveController {
    resolver: Arc<LiveNamespaceResolver>,
    instance: u32,
    deletion: DeletionState,
    registry: LiveConnectionRegistry,
    intent_path: PathBuf,
    /// Teardown `LOCK_EX` descriptors on this controller's namespace disks,
    /// held from just before the delete until deletion is confirmed.
    teardown_locks: Vec<File>,
}

pub(crate) struct LiveNamespaceResolver {
    target: LiveConnectTarget,
    host_nqn: String,
    host_id: [u8; 16],
    name: String,
    directory: File,
    inode: u64,
    device: u64,
}
impl LiveController {
    /// Controller instance returned by the Linux fabrics control response.
    pub fn instance(&self) -> u32 {
        self.instance
    }
    /// Creates a distinct controller with explicit host identity. The kernel's
    /// control response, not a sysfs inventory difference, establishes ownership.
    fn connect(
        registry: &LiveConnectionRegistry,
        target: &LiveConnectTarget,
        host_nqn: &str,
        host_id: [u8; 16],
        access: LiveNamespaceAccess,
        deadline: Instant,
    ) -> io::Result<(Self, LiveNamespaceBinding)> {
        target.geometry.validate().map_err(invalid)?;
        if !valid_nqn(&target.nqn)
            || !valid_nqn(host_nqn)
            || host_id == [0; 16]
            || target.uuid == [0; 16]
            || target.nsid == 0
            || target.service == 0
            || target.address.parse::<IpAddr>().is_err()
        {
            return Err(invalid("invalid fabrics identity"));
        }
        remaining(deadline)?;
        let intent = Intent {
            target: target.clone(),
            host_nqn: host_nqn.to_owned(),
            host_id,
        };
        let intent_path = registry.reserve(&intent)?;
        let registry = registry.clone();
        let target_copy = intent.target;
        let host_nqn = intent.host_nqn;
        let (tx, rx) = mpsc::sync_channel(0);
        std::thread::Builder::new()
            .name("live-nvme-connect".into())
            .spawn(move || {
                let result = Self::create(target_copy, host_nqn, host_id, registry, intent_path);
                if let Err(mpsc::SendError(Ok(mut controller))) = tx.send(result)
                    && let Err(error) =
                        controller.disconnect_inner(Instant::now() + CLEANUP_TIMEOUT)
                {
                    eprintln!("late NVMe controller cleanup failed; intent quarantined: {error}");
                }
            })?;
        let mut controller = match rx.recv_timeout(remaining(deadline)?) {
            Ok(result) => result?,
            Err(mpsc::RecvTimeoutError::Timeout) => return Err(timeout()),
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                return Err(invalid("connection worker failed"));
            }
        };
        match controller
            .verify_queue_geometry()
            .and_then(|()| controller.resolve_verified(access, deadline))
        {
            Ok(binding) => Ok((controller, binding)),
            Err(error) => Err(connect_error_after_cleanup(
                error,
                controller.disconnect_inner(Instant::now() + CLEANUP_TIMEOUT),
            )),
        }
    }
    fn create(
        target: LiveConnectTarget,
        host_nqn: String,
        host_id: [u8; 16],
        registry: LiveConnectionRegistry,
        intent_path: PathBuf,
    ) -> io::Result<Self> {
        let mut control = OpenOptions::new()
            .read(true)
            .write(true)
            .open("/dev/nvme-fabrics")
            .map_err(|error| {
                with_context(
                    "open fabrics control",
                    Path::new("/dev/nvme-fabrics"),
                    error,
                )
            })?;
        let options = connect_options(&target, &host_nqn, host_id);
        control.write_all(options.as_bytes()).map_err(|error| {
            with_context(
                "connect fabrics control",
                Path::new("/dev/nvme-fabrics"),
                error,
            )
        })?;
        let mut response = String::new();
        control.read_to_string(&mut response).map_err(|error| {
            with_context(
                "read fabrics response",
                Path::new("/dev/nvme-fabrics"),
                error,
            )
        })?;
        let instance = response
            .trim()
            .split(',')
            .find_map(|part| part.strip_prefix("instance="))
            .ok_or_else(|| invalid("missing fabrics controller instance"))?
            .parse::<u32>()
            .map_err(|_| invalid("invalid fabrics controller instance"))?;
        let name = format!("nvme{instance}");
        let controller_path = Path::new("/sys/class/nvme").join(&name);
        let directory = File::open(&controller_path).map_err(|error| {
            with_context("open owned controller sysfs", &controller_path, error)
        })?;
        let metadata = directory.metadata()?;
        let controller = Self {
            resolver: Arc::new(LiveNamespaceResolver {
                target,
                host_nqn,
                host_id,
                name,
                directory,
                inode: metadata.ino(),
                device: metadata.dev(),
            }),
            instance,
            deletion: DeletionState::NotStarted,
            registry,
            intent_path,
            teardown_locks: Vec::new(),
        };
        if let Err(error) = controller.verify() {
            // An identity mismatch cannot authorize deletion of this object.
            return Err(with_context(
                "verify owned controller",
                &controller_path,
                error,
            ));
        }
        Ok(controller)
    }
}
impl LiveNamespaceResolver {
    fn pinned(&self) -> PathBuf {
        PathBuf::from(format!("/proc/self/fd/{}", self.directory.as_raw_fd()))
    }
    fn verify(&self) -> io::Result<()> {
        let p = self.pinned();
        let meta = self.directory.metadata()?;
        if meta.ino() != self.inode || meta.dev() != self.device {
            return Err(invalid("controller inode changed"));
        }
        let current = std::fs::metadata(Path::new("/sys/class/nvme").join(&self.name))?;
        if current.ino() != self.inode
            || current.dev() != self.device
            || attr(&p.join("subsysnqn"))? != self.target.nqn
            || attr(&p.join("transport"))? != transport(self.target.transport)
        {
            return Err(invalid("controller identity changed"));
        }
        if attr(&p.join("hostnqn"))? != self.host_nqn
            || attr(&p.join("hostid"))? != uuid_string(self.host_id)
        {
            return Err(invalid("controller host identity changed"));
        }
        let address = attr(&p.join("address"))?;
        let mut traddr = None;
        let mut trsvcid = None;
        for field in address.split(',') {
            if let Some((k, v)) = field.split_once('=') {
                match k {
                    "traddr" => traddr = Some(v),
                    "trsvcid" => trsvcid = Some(v),
                    _ => {}
                }
            }
        }
        if traddr.and_then(|s| s.parse::<IpAddr>().ok())
            != self.target.address.parse::<IpAddr>().ok()
            || trsvcid.and_then(|s| s.parse::<u16>().ok()) != Some(self.target.service)
        {
            return Err(invalid("controller endpoint mismatch"));
        }
        Ok(())
    }

    /// Resolve the single namespace owned by this controller, then perform the
    /// block, NVMe identity, geometry, access and transport verification.
    pub(crate) fn resolve_namespace(
        &self,
        expected_nsid: u32,
        expected_uuid: [u8; 16],
        access: LiveNamespaceAccess,
        deadline: Instant,
    ) -> io::Result<LiveNamespaceBinding> {
        loop {
            remaining(deadline)?;
            self.verify()?;
            let ctrl = std::fs::canonicalize(self.pinned())?;
            let mut candidates = Vec::new();
            for (name, p) in owned_visible_disks(Path::new("/sys/class/block"), &ctrl, &self.name)?
            {
                let Some(nsid) = scan_attribute(&p.join("nsid"))? else {
                    continue;
                };
                let nsid = nsid
                    .parse::<u32>()
                    .map_err(|_| invalid("invalid namespace id"))?;
                if nsid != expected_nsid {
                    continue;
                }
                let Some(actual_uuid) = scan_attribute(&p.join("uuid"))? else {
                    continue;
                };
                let Some(wwid) = scan_attribute(&p.join("wwid"))? else {
                    continue;
                };
                let uuid = uuid_string(expected_uuid);
                if actual_uuid != uuid || wwid != format!("uuid.{uuid}") {
                    return Err(invalid("namespace UUID mismatch"));
                }
                let Some(dev) = scan_attribute(&p.join("dev"))? else {
                    continue;
                };
                candidates.push((Path::new("/dev").join(name), dev));
            }
            if candidates.len() > 1 {
                return Err(invalid("ambiguous namespace"));
            }
            if let Some((path, dev)) = candidates.pop() {
                let file = wait_for_device(
                    "open namespace device",
                    &path,
                    &self.name,
                    DeviceNode::Namespace,
                    deadline,
                    || self.verify(),
                    || OpenOptions::new().read(true).open(&path),
                )?;
                let meta = file.metadata()?;
                if !meta.file_type().is_block_device() || sys::device_number(meta.rdev()) != dev {
                    return Err(invalid("namespace device number mismatch"));
                }
                if access == LiveNamespaceAccess::ReadOnly {
                    let readonly: libc::c_int = 1;
                    // SAFETY: BLKROSET reads an initialized integer while fd is live.
                    if unsafe { libc::ioctl(file.as_raw_fd(), 0x125d, &readonly) } < 0 {
                        return Err(with_context(
                            "set namespace read-only",
                            &path,
                            io::Error::last_os_error(),
                        ));
                    }
                }
                drop(file);
                let binding = LiveNamespaceBinding {
                    namespace_path: path,
                    transport: self.target.transport,
                    nqn: self.target.nqn.clone(),
                    uuid: expected_uuid,
                    nsid: expected_nsid,
                    bytes: self.target.bytes,
                    block_size: self.target.block_size,
                    access,
                };
                let file = wait_for_device(
                    "open verified namespace",
                    &binding.namespace_path,
                    &self.name,
                    DeviceNode::Namespace,
                    deadline,
                    || self.verify(),
                    || LiveNamespace::open_device(&binding),
                )?;
                drop(LiveNamespace::verify_opened_device(&binding, file)?);
                self.verify()?;
                return Ok(binding);
            }
            std::thread::sleep(remaining(deadline)?.min(Duration::from_millis(20)));
        }
    }
    /// Locks every visible namespace disk of this controller for teardown. A
    /// controller already gone has no paths left whose removal a scan could race.
    fn lock_owned_disks(&self, deadline: Instant) -> io::Result<Vec<File>> {
        let controller = Path::new("/sys/class/nvme").join(&self.name);
        if original_removed(&controller, self.device, self.inode)? {
            return Ok(Vec::new());
        }
        let ctrl = match std::fs::canonicalize(self.pinned()) {
            Ok(path) => path,
            Err(error) if vanished(&error) => return Ok(Vec::new()),
            Err(error) => {
                return Err(with_context("resolve owned controller", &controller, error));
            }
        };
        lock_controller_disks(
            Path::new("/sys/class/block"),
            Path::new("/dev"),
            &ctrl,
            &self.name,
            deadline,
        )
    }
}
impl LiveController {
    fn pinned(&self) -> PathBuf {
        self.resolver.pinned()
    }
    fn verify(&self) -> io::Result<()> {
        self.resolver.verify()
    }
    pub(crate) fn resolver(&self) -> Arc<LiveNamespaceResolver> {
        self.resolver.clone()
    }
    pub fn resolve_verified(
        &mut self,
        access: LiveNamespaceAccess,
        deadline: Instant,
    ) -> io::Result<LiveNamespaceBinding> {
        self.resolver.resolve_namespace(
            self.resolver.target.nsid,
            self.resolver.target.uuid,
            access,
            deadline,
        )
    }
    fn verify_queue_geometry(&self) -> io::Result<()> {
        self.verify()?;
        verify_queue_geometry(&self.pinned(), self.resolver.target.geometry)?;
        self.verify()
    }
    /// Set and read back namespace write protection on the owned controller.
    /// This is a point-in-time guard under the trusted-initiator assumption.
    pub fn set_read_only(&mut self, nsid: u32, deadline: Instant) -> io::Result<()> {
        if nsid != self.resolver.target.nsid {
            return Err(invalid("namespace id mismatch"));
        }
        let controller_path = Path::new("/dev").join(&self.resolver.name);
        let file = wait_for_device(
            "open controller admin device",
            &controller_path,
            &self.resolver.name,
            DeviceNode::Controller,
            deadline,
            || self.verify(),
            || {
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&controller_path)
            },
        )?;
        let metadata = file.metadata()?;
        let device = sys::device_number(metadata.rdev());
        if !metadata.file_type().is_char_device() || device != attr(&self.pinned().join("dev"))? {
            return Err(invalid("controller device number mismatch"));
        }
        self.verify()?;
        for (opcode, value) in [(0x09, 1), (0x0a, 0)] {
            let ms = remaining(deadline)?.as_millis().clamp(1, u32::MAX as u128) as u32;
            let mut cmd = NvmeAdminCommand {
                opcode,
                nsid,
                cdw10: 0x84,
                cdw11: value,
                timeout_ms: ms,
                ..Default::default()
            };
            // SAFETY: the command matches nvme_passthru_cmd and remains valid for the synchronous ioctl.
            let status = unsafe { libc::ioctl(file.as_raw_fd(), 0xc0484e41, &mut cmd) };
            if status < 0 {
                return Err(with_context(
                    "namespace write-protection admin command",
                    &controller_path,
                    io::Error::last_os_error(),
                ));
            }
            if status != 0 || (opcode == 0x0a && cmd.result != 1) {
                return Err(invalid("namespace write protection failed"));
            }
        }
        self.verify()
    }
    /// The caller must first close and drain all namespace users.
    pub fn disconnect(&mut self, deadline: Instant) -> io::Result<()> {
        remaining(deadline)?;
        self.disconnect_inner(deadline)
    }
    fn delete_owned(&mut self, deadline: Instant) -> io::Result<()> {
        let controller_path = Path::new("/sys/class/nvme").join(&self.resolver.name);
        let delete_path = self.pinned().join("delete_controller");
        let mut state = self.deletion;
        let result = retire_controller(
            &mut state,
            &controller_path,
            self.resolver.device,
            self.resolver.inode,
            deadline,
            || self.verify(),
            || {
                std::fs::write(&delete_path, b"1").map_err(|error| {
                    // Preserve ENODEV until retirement classifies the teardown race.
                    if vanished(&error) {
                        error
                    } else {
                        with_context("delete owned controller", &delete_path, error)
                    }
                })
            },
        );
        self.deletion = state;
        result
    }
    /// Locks the namespace disks (see `lock_controller_disks`) before issuing
    /// the delete, and releases them once the controller is gone.
    fn retire(&mut self, deadline: Instant) -> io::Result<()> {
        let resolver = self.resolver.clone();
        let mut locks = std::mem::take(&mut self.teardown_locks);
        let result = retire_locked(
            &mut locks,
            self.deletion == DeletionState::NotStarted,
            || resolver.lock_owned_disks(deadline),
            || self.delete_owned(deadline),
        );
        self.teardown_locks = locks;
        result
    }
    fn disconnect_inner(&mut self, deadline: Instant) -> io::Result<()> {
        self.retire(deadline)?;
        self.registry.remove_intent(&self.intent_path)
    }
}
#[cfg(any(test, feature = "test-utils"))]
impl LiveController {
    /// A controller whose kernel object is already gone, so `disconnect` only
    /// retires its (absent) intent. For controller-pool state tests.
    pub(super) fn deleted_for_test(
        registry: &LiveConnectionRegistry,
        sysfs_stand_in: &Path,
        target: &LiveConnectTarget,
    ) -> io::Result<Self> {
        let directory = File::open(sysfs_stand_in)?;
        let metadata = directory.metadata()?;
        Ok(Self {
            resolver: Arc::new(LiveNamespaceResolver {
                target: target.clone(),
                host_nqn: "nqn.2026-01.test:host".into(),
                host_id: [2; 16],
                name: "nvme-test".into(),
                inode: metadata.ino(),
                device: metadata.dev(),
                directory,
            }),
            instance: 0,
            deletion: DeletionState::Gone,
            registry: registry.clone(),
            intent_path: sysfs_stand_in.join("no-intent"),
            teardown_locks: Vec::new(),
        })
    }
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
