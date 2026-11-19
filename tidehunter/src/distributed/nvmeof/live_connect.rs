//! Owned Linux fabrics connections. The caller must drain and close every
//! namespace I/O handle before disconnecting the controller. A connection is
//! not a durability or authorization receipt.
pub use super::LiveQueueGeometry;
use super::NvmeofTransport;
use super::live_namespace::{LiveNamespace, LiveNamespaceAccess, LiveNamespaceBinding};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::net::IpAddr;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{DirBuilderExt, FileTypeExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};

const CLEANUP_TIMEOUT: Duration = Duration::from_secs(30);

/// CPUs serving each hardware queue, in numeric queue-index order.
/// Native multipath heads have no `mq`: use the first per-path disk in sorted
/// path-name order. Our connections normally have exactly one such path.
pub(crate) fn queue_cpus(namespace_device: &Path) -> io::Result<Vec<Vec<usize>>> {
    let device = std::fs::canonicalize(namespace_device)
        .map_err(|error| with_context("resolve namespace", namespace_device, error))?;
    queue_cpus_at(Path::new("/sys/block"), &device)
}

fn queue_cpus_at(block_root: &Path, namespace_device: &Path) -> io::Result<Vec<Vec<usize>>> {
    let name = namespace_device.file_name().ok_or_else(|| {
        with_context(
            "namespace device name",
            namespace_device,
            invalid("missing device name"),
        )
    })?;
    let mut path = block_root.join(name).join("mq");
    let entries = match std::fs::read_dir(&path) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let multipath = block_root.join(name).join("multipath");
            let paths = std::fs::read_dir(&multipath)
                .map_err(|error| with_context("read namespace paths", &multipath, error))?;
            let mut first: Option<PathBuf> = None;
            for entry in paths {
                let entry = entry
                    .map_err(|error| with_context("read namespace path", &multipath, error))?;
                let candidate = entry.path();
                if first.as_ref().is_none_or(|path| candidate < *path) {
                    first = Some(candidate);
                }
            }
            path = first
                .ok_or_else(|| {
                    with_context(
                        "read namespace paths",
                        &multipath,
                        invalid("no namespace paths"),
                    )
                })?
                .join("mq");
            std::fs::read_dir(&path)
                .map_err(|error| with_context("read hardware queues", &path, error))?
        }
        Err(error) => return Err(with_context("read hardware queues", &path, error)),
    };
    let mut queues = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|error| with_context("read hardware queue", &path, error))?;
        let index = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<usize>().ok())
            .ok_or_else(|| {
                with_context(
                    "hardware queue index",
                    &entry.path(),
                    invalid("invalid index"),
                )
            })?;
        let cpu_path = entry.path().join("cpu_list");
        let list = std::fs::read_to_string(&cpu_path)
            .map_err(|error| with_context("read queue CPUs", &cpu_path, error))?;
        let cpus = parse_cpu_list(&list)
            .map_err(|error| with_context("parse queue CPUs", &cpu_path, error))?;
        queues.push((index, cpus));
    }
    if queues.is_empty() {
        return Err(with_context(
            "read hardware queues",
            &path,
            invalid("no hardware queues"),
        ));
    }
    queues.sort_unstable_by_key(|(index, _)| *index);
    Ok(queues.into_iter().map(|(_, cpus)| cpus).collect())
}

fn parse_cpu_list(list: &str) -> io::Result<Vec<usize>> {
    let mut cpus = Vec::new();
    for part in list.trim().split(',') {
        let (start, end) = part.split_once('-').map_or((part, part), |range| range);
        let start = start
            .parse::<usize>()
            .map_err(|_| invalid("invalid CPU index"))?;
        let end = end
            .parse::<usize>()
            .map_err(|_| invalid("invalid CPU index"))?;
        // Bound expansion to the mask supported by pin_current_thread.
        if start > end || end >= libc::CPU_SETSIZE as usize {
            return Err(invalid("CPU range outside affinity mask"));
        }
        cpus.extend(start..=end);
    }
    cpus.sort_unstable();
    cpus.dedup();
    Ok(cpus)
}

/// Pin only the calling thread. Failure leaves its existing affinity unchanged.
pub(crate) fn pin_current_thread(cpu: usize) -> io::Result<()> {
    let path = Path::new("/proc/thread-self");
    if cpu >= libc::CPU_SETSIZE as usize {
        return Err(with_context(
            "set thread affinity",
            path,
            invalid("CPU outside affinity mask"),
        ));
    }
    // SAFETY: cpu is inside cpu_set_t's bounds; the initialized mask lives for
    // the whole syscall. pid 0 changes only the calling thread's affinity.
    let result = unsafe {
        let mut mask: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_ZERO(&mut mask);
        libc::CPU_SET(cpu, &mut mask);
        libc::sched_setaffinity(0, std::mem::size_of_val(&mask), &mask)
    };
    if result != 0 {
        return Err(with_context(
            "set thread affinity",
            path,
            io::Error::last_os_error(),
        ));
    }
    Ok(())
}

/// A sysfs attribute read during controller teardown can return ENODEV, which
/// Rust does not map to NotFound. Keep each call site's existing NotFound
/// semantics for both errors; ENODEV proves no more absence than NotFound does.
fn vanished(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::NotFound || error.raw_os_error() == Some(libc::ENODEV)
}

fn verify_queue_geometry(path: &Path, geometry: LiveQueueGeometry) -> io::Result<()> {
    geometry.validate().map_err(invalid)?;
    let count_path = path.join("queue_count");
    let size_path = path.join("sqsize");
    let count = attr(&count_path)
        .map_err(|e| with_context("read controller queue count", &count_path, e))?;
    let size =
        attr(&size_path).map_err(|e| with_context("read controller queue size", &size_path, e))?;
    let count = count
        .parse::<u32>()
        .map_err(|_| invalid("invalid controller queue count"))?;
    let size = size
        .parse::<u32>()
        .map_err(|_| invalid("invalid controller queue size"))?;
    // queue_count includes the admin queue; sqsize is zero-based.
    if count != geometry.io_queues + geometry.poll_queues + 1 || size != geometry.queue_size - 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unexpected controller queue geometry: queue_count={count}, sqsize={size}"),
        ));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DeletionState {
    NotStarted,
    Initiated,
    Uncertain,
    Gone,
}

fn original_removed(path: &Path, device: u64, inode: u64) -> io::Result<bool> {
    match std::fs::metadata(path) {
        Ok(meta) => Ok(meta.dev() != device || meta.ino() != inode),
        Err(error) if vanished(&error) => Ok(true),
        Err(error) => Err(with_context("inspect owned controller", path, error)),
    }
}

fn wait_for_original_removal(
    path: &Path,
    device: u64,
    inode: u64,
    deadline: Instant,
) -> io::Result<()> {
    loop {
        if original_removed(path, device, inode)? {
            return Ok(());
        }
        std::thread::sleep(remaining(deadline)?.min(Duration::from_millis(20)));
    }
}

fn retire_controller(
    state: &mut DeletionState,
    controller_path: &Path,
    device: u64,
    inode: u64,
    deadline: Instant,
    verify: impl FnOnce() -> io::Result<()>,
    delete: impl FnOnce() -> io::Result<()>,
) -> io::Result<()> {
    if *state == DeletionState::Gone {
        return Ok(());
    }
    if *state == DeletionState::NotStarted {
        remaining(deadline)?;
        // The pinned old inode cannot become a replacement.
        if original_removed(controller_path, device, inode)? {
            *state = DeletionState::Gone;
            return Ok(());
        }
        match verify() {
            Ok(()) => {
                // Never issue a second store, even when the first returns an error.
                *state = DeletionState::Uncertain;
                match delete() {
                    Ok(()) => *state = DeletionState::Initiated,
                    Err(error) if vanished(&error) => {}
                    Err(error) => return Err(error),
                }
            }
            Err(error) if vanished(&error) => {
                *state = DeletionState::Uncertain;
            }
            Err(error)
                if error.kind() == io::ErrorKind::InvalidData
                    && original_removed(controller_path, device, inode)? =>
            {
                *state = DeletionState::Gone;
                return Ok(());
            }
            Err(error) => return Err(error),
        }
    }
    wait_for_original_removal(controller_path, device, inode, deadline)?;
    *state = DeletionState::Gone;
    Ok(())
}

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
fn attr(p: &Path) -> io::Result<String> {
    Ok(std::fs::read_to_string(p)?.trim().to_owned())
}

// Sysfs entries can disappear during enumeration. Permission and I/O errors
// remain fatal.
fn scan_attribute(path: &Path) -> io::Result<Option<String>> {
    match attr(path) {
        Ok(value) => Ok(Some(value)),
        Err(error) if vanished(&error) => Ok(None),
        Err(error) => Err(with_context("read namespace sysfs", path, error)),
    }
}

fn controller_attribute(
    path: &Path,
    directory: &File,
    attribute: &str,
) -> io::Result<Option<String>> {
    let pinned_attribute = PathBuf::from(format!(
        "/proc/self/fd/{}/{}",
        directory.as_raw_fd(),
        attribute
    ));
    match attr(&pinned_attribute) {
        Ok(value) => Ok(Some(value)),
        Err(error) if vanished(&error) => {
            let original = directory.metadata()?;
            match std::fs::metadata(path) {
                Ok(current)
                    if current.dev() == original.dev() && current.ino() == original.ino() =>
                {
                    Err(with_context(
                        "read controller sysfs",
                        &path.join(attribute),
                        error,
                    ))
                }
                Ok(_) => Ok(None),
                Err(missing) if vanished(&missing) => Ok(None),
                Err(other) => Err(with_context("inspect controller sysfs", path, other)),
            }
        }
        Err(error) => Err(with_context(
            "read controller sysfs",
            &path.join(attribute),
            error,
        )),
    }
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

#[derive(Clone, Copy)]
enum DeviceNode {
    Namespace,
    Controller,
}

// ENXIO can mean a namespace has not been scanned yet, but must never mean
// confirmed disappearance during cleanup, or be retried for controller admin I/O.
fn wait_for_device<T>(
    operation: &str,
    path: &Path,
    controller: &str,
    node: DeviceNode,
    deadline: Instant,
    mut verify: impl FnMut() -> io::Result<()>,
    mut open: impl FnMut() -> io::Result<T>,
) -> io::Result<T> {
    let mut last_error: Option<io::Error> = None;
    loop {
        if remaining(deadline).is_err() {
            let reason = if last_error
                .as_ref()
                .is_some_and(|error| error.raw_os_error() == Some(libc::ENXIO))
            {
                "ENXIO persisted until namespace readiness deadline"
            } else {
                "device node did not appear before deadline"
            };
            let message = match last_error {
                Some(error) => format!(
                    "{operation} {} (controller {controller}): {reason}: {error}",
                    path.display()
                ),
                None => format!(
                    "{operation} {} (controller {controller}): {reason}",
                    path.display()
                ),
            };
            return Err(io::Error::new(io::ErrorKind::TimedOut, message));
        }
        // Recheck the pinned controller before EVERY open. Preserve the existing
        // ENOENT/ENODEV wait, but do not retry ENXIO from identity verification.
        let attempt = match verify() {
            Ok(()) => open(),
            Err(error) if vanished(&error) => Err(error),
            Err(error) => {
                return Err(with_context(
                    "verify controller before device open",
                    path,
                    error,
                ));
            }
        };
        match attempt {
            Ok(value) => return Ok(value),
            Err(error)
                if vanished(&error)
                    || (matches!(node, DeviceNode::Namespace)
                        && error.raw_os_error() == Some(libc::ENXIO)) =>
            {
                last_error = Some(error);
                if let Ok(delay) = remaining(deadline) {
                    std::thread::sleep(delay.min(Duration::from_millis(20)));
                }
            }
            Err(error) => return Err(with_context(operation, path, error)),
        }
    }
}

/// Takes `LOCK_EX` on a namespace disk. A udev worker probing the disk holds
/// `LOCK_SH` until its rules finish, and systemd expects the exclusive locker
/// to wait for it. `flock` has no timeout, so a contended lock waits on a helper
/// thread. A result after the deadline is dropped, which releases the lock; the
/// helper then lives until the holder releases (udev bounds its own events).
fn lock_disk(disk: File, path: &Path, controller: &str, deadline: Instant) -> io::Result<File> {
    match disk.try_lock() {
        Ok(()) => return Ok(disk),
        Err(std::fs::TryLockError::WouldBlock) => {}
        Err(std::fs::TryLockError::Error(error)) => {
            return Err(with_context("lock namespace device", path, error));
        }
    }
    let (tx, rx) = mpsc::sync_channel(0);
    std::thread::Builder::new()
        .name("live-nvme-disk-lock".into())
        .spawn(move || {
            let result = disk.lock().map(|()| disk);
            // Fails only after the caller's deadline; dropping the result unlocks.
            drop(tx.send(result));
        })?;
    match rx.recv_timeout(remaining(deadline)?) {
        Ok(result) => result.map_err(|error| with_context("lock namespace device", path, error)),
        Err(mpsc::RecvTimeoutError::Timeout) => Err(io::Error::new(
            io::ErrorKind::TimedOut,
            format!(
                "lock namespace device {} (controller {controller}): another holder kept the disk lock until the deadline",
                path.display()
            ),
        )),
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            Err(invalid("namespace disk lock worker failed"))
        }
    }
}

/// Visible namespace disks of the controller at `ctrl` (canonical sysfs path):
/// its own disks and the native multipath heads that link to it. Entries that
/// disappear during the scan are skipped.
fn owned_visible_disks(
    block_root: &Path,
    ctrl: &Path,
    controller: &str,
) -> io::Result<Vec<(String, PathBuf)>> {
    let mut disks = Vec::new();
    for entry in std::fs::read_dir(block_root)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if !namespace_name(name) {
            continue;
        }
        let p = entry.path();
        let parent_path = p.join("device");
        let parent = match std::fs::canonicalize(&parent_path) {
            Ok(parent) => parent,
            Err(error) if vanished(&error) => continue,
            Err(error) => {
                return Err(with_context(
                    "resolve namespace parent",
                    &parent_path,
                    error,
                ));
            }
        };
        let owned = if parent == ctrl {
            true
        } else {
            let linked = parent.join(controller);
            match std::fs::canonicalize(&linked) {
                Ok(path) => path == ctrl,
                Err(error) if error.kind() == io::ErrorKind::NotFound => false,
                Err(error) => {
                    return Err(with_context("resolve namespace controller", &linked, error));
                }
            }
        };
        if !owned {
            continue;
        }
        let visible = match namespace_visible(&p) {
            Ok(visible) => visible,
            Err(error) if vanished(&error) => continue,
            Err(error) => return Err(with_context("read namespace visibility", &p, error)),
        };
        if visible {
            disks.push((name.to_owned(), p));
        }
    }
    Ok(disks)
}

/// Takes BSD `LOCK_EX` on every visible namespace disk of a controller before
/// it is deleted. systemd-udevd watches NVMe disks: after any close of a
/// descriptor opened for writing it runs `BLKRRPART` under `LOCK_EX|LOCK_NB`,
/// and its workers probe under `LOCK_SH|LOCK_NB`
/// (<https://systemd.io/BLOCK_DEVICE_LOCKING>). A partition scan whose reads
/// follow the removal of the last path fails with kernel I/O errors. Holding
/// the lock makes udev skip later rescans, and waiting for it lets a rescan
/// already running finish first. Descriptors are read-only, so their close
/// never triggers a rescan. A node that vanished, or that now names another
/// device, belongs to a disk already deleted. Locks are taken in device-number
/// order, so two teardowns sharing a multipath head cannot wait on each other.
fn lock_controller_disks(
    block_root: &Path,
    dev_root: &Path,
    ctrl: &Path,
    controller: &str,
    deadline: Instant,
) -> io::Result<Vec<File>> {
    let mut opened = Vec::new();
    for (name, sysfs) in owned_visible_disks(block_root, ctrl, controller)? {
        let Some(dev) = scan_attribute(&sysfs.join("dev"))? else {
            continue;
        };
        let path = dev_root.join(name);
        let disk = match File::open(&path) {
            Ok(disk) => disk,
            // ENXIO: the disk is no longer live, so nothing can scan it.
            Err(error) if vanished(&error) || error.raw_os_error() == Some(libc::ENXIO) => {
                continue;
            }
            Err(error) => {
                return Err(with_context(
                    "open namespace device for teardown",
                    &path,
                    error,
                ));
            }
        };
        let rdev = disk.metadata()?.rdev();
        if format!("{}:{}", libc::major(rdev), libc::minor(rdev)) == dev {
            opened.push((rdev, path, disk));
        }
    }
    opened.sort_unstable_by_key(|(rdev, ..)| *rdev);
    opened
        .into_iter()
        .map(|(_, path, disk)| lock_disk(disk, &path, controller, deadline))
        .collect()
}

/// Teardown order: lock the namespace disks, delete, and release the locks only
/// once deletion is confirmed. Locks are taken only before a delete is issued;
/// if locking fails no delete is issued, so the caller keeps its intent. An
/// unconfirmed deletion keeps the locks for the next attempt.
fn retire_locked(
    locks: &mut Vec<File>,
    delete_not_issued: bool,
    lock: impl FnOnce() -> io::Result<Vec<File>>,
    retire: impl FnOnce() -> io::Result<()>,
) -> io::Result<()> {
    if delete_not_issued && locks.is_empty() {
        *locks = lock()?;
    }
    retire()?;
    locks.clear();
    Ok(())
}
fn namespace_visible(path: &Path) -> io::Result<bool> {
    match attr(&path.join("hidden"))?.as_str() {
        "0" => Ok(true),
        "1" => Ok(false),
        _ => Err(invalid("invalid namespace hidden state")),
    }
}
fn namespace_name(name: &str) -> bool {
    let Some((controller, namespace)) = name.strip_prefix("nvme").and_then(|s| s.split_once('n'))
    else {
        return false;
    };
    let digits = |s: &str| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit());
    let controller_valid = match controller.split_once('c') {
        Some((subsystem, path)) => digits(subsystem) && digits(path),
        None => digits(controller),
    };
    controller_valid && digits(namespace)
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

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct Intent {
    target: LiveConnectTarget,
    host_nqn: String,
    host_id: [u8; 16],
}
struct RegistryInner {
    directory: PathBuf,
    _lock: File,
    admission: Mutex<()>,
    uncertain: AtomicBool,
}
#[derive(Clone)]
pub struct LiveConnectionRegistry {
    inner: Arc<RegistryInner>,
}
impl LiveConnectionRegistry {
    /// Takes exclusive ownership of a private persistent directory and retires
    /// recorded controllers before returning. Host identities must be unique to
    /// this registry; authority open reconciles here before fencing the targets.
    pub fn open(directory: &Path) -> io::Result<Self> {
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(directory)?;
        let meta = std::fs::metadata(directory)?;
        if !meta.is_dir() || meta.mode() & 0o077 != 0 {
            return Err(invalid("connection registry must be private"));
        }
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(directory.join("lock"))?;
        // SAFETY: flock operates on the live lock file descriptor.
        if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } < 0 {
            return Err(io::Error::last_os_error());
        }
        let registry = Self {
            inner: Arc::new(RegistryInner {
                directory: directory.to_owned(),
                _lock: lock,
                admission: Mutex::new(()),
                uncertain: AtomicBool::new(false),
            }),
        };
        registry.reconcile()?;
        Ok(registry)
    }
    fn reconcile(&self) -> io::Result<()> {
        let deadline = Instant::now() + CLEANUP_TIMEOUT;
        for entry in std::fs::read_dir(&self.inner.directory)? {
            remaining(deadline)?;
            let entry = entry?;
            if entry.path().extension().and_then(|s| s.to_str()) != Some("json") {
                continue;
            }
            let intent: Intent = serde_json::from_slice(&std::fs::read(entry.path())?)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let controllers = match std::fs::read_dir("/sys/class/nvme") {
                Ok(v) => v,
                Err(e) if e.kind() == io::ErrorKind::NotFound => {
                    self.remove_intent(&entry.path())?;
                    continue;
                }
                Err(e) => return Err(e),
            };
            for controller in controllers {
                let controller = controller?;
                let name = controller.file_name();
                let Some(name) = name.to_str() else { continue };
                if !name
                    .strip_prefix("nvme")
                    .is_some_and(|s| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()))
                {
                    continue;
                }
                let path = controller.path();
                let directory = match File::open(&path) {
                    Ok(directory) => directory,
                    Err(error) if vanished(&error) => continue,
                    Err(error) => return Err(with_context("open controller sysfs", &path, error)),
                };
                let Some(nqn) = controller_attribute(&path, &directory, "subsysnqn")? else {
                    continue;
                };
                if nqn != intent.target.nqn {
                    continue;
                }
                let Some(host_nqn) = controller_attribute(&path, &directory, "hostnqn")? else {
                    continue;
                };
                let Some(host_id) = controller_attribute(&path, &directory, "hostid")? else {
                    continue;
                };
                if host_nqn != intent.host_nqn || host_id != uuid_string(intent.host_id) {
                    continue;
                }
                let metadata = directory.metadata()?;
                let instance = name
                    .strip_prefix("nvme")
                    .and_then(|s| s.parse().ok())
                    .ok_or_else(|| invalid("invalid controller instance"))?;
                let mut owned = LiveController {
                    resolver: Arc::new(LiveNamespaceResolver {
                        target: intent.target.clone(),
                        host_nqn: intent.host_nqn.clone(),
                        host_id: intent.host_id,
                        name: name.to_owned(),
                        directory,
                        inode: metadata.ino(),
                        device: metadata.dev(),
                    }),
                    instance,
                    deletion: DeletionState::NotStarted,
                    registry: self.clone(),
                    intent_path: entry.path(),
                    teardown_locks: Vec::new(),
                };
                owned.verify()?;
                // A previous process may have issued the delete before dying.
                // A controller already deleting is only observed, never deleted
                // again; its original sysfs identity still must disappear.
                let state = attr(&owned.pinned().join("state"))?;
                if matches!(state.as_str(), "deleting" | "deleting (no IO)" | "dead") {
                    owned.deletion = DeletionState::Initiated;
                }
                owned.retire(deadline)?;
            }
            self.remove_intent(&entry.path())?;
        }
        Ok(())
    }
    fn remove_intent(&self, path: &Path) -> io::Result<()> {
        let _guard = self
            .inner
            .admission
            .lock()
            .map_err(|_| invalid("registry lock poisoned"))?;
        match std::fs::remove_file(path) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        if let Err(error) = File::open(&self.inner.directory).and_then(|dir| dir.sync_all()) {
            self.inner.uncertain.store(true, Ordering::Release);
            return Err(error);
        }
        Ok(())
    }
    fn reserve(&self, intent: &Intent) -> io::Result<PathBuf> {
        let _guard = self
            .inner
            .admission
            .lock()
            .map_err(|_| invalid("registry lock poisoned"))?;
        if self.inner.uncertain.load(Ordering::Acquire) {
            return Err(invalid("registry durability is uncertain"));
        }
        for entry in std::fs::read_dir(&self.inner.directory)? {
            let entry = entry?;
            if entry.path().extension().and_then(|s| s.to_str()) != Some("json") {
                continue;
            }
            let previous: Intent = serde_json::from_slice(&std::fs::read(entry.path())?)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            if previous.target.nqn == intent.target.nqn && previous.host_nqn == intent.host_nqn {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "connection intent remains active or quarantined",
                ));
            }
        }
        let mut nonce = [0u8; 16];
        File::open("/dev/urandom")?.read_exact(&mut nonce)?;
        let nonce = uuid_string(nonce);
        let path = self.inner.directory.join(format!("{nonce}.json"));
        let temporary = self.inner.directory.join(format!("{nonce}.tmp"));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temporary)?;
        serde_json::to_writer(&mut file, intent).map_err(io::Error::other)?;
        file.sync_all()?;
        // Publish a complete record atomically without replacing an existing
        // intent. Recovery ignores abandoned temporary files.
        std::fs::hard_link(&temporary, &path)?;
        std::fs::remove_file(&temporary)?;
        File::open(&self.inner.directory)?.sync_all()?;
        Ok(path)
    }
    pub(crate) fn has_intent(&self, nqn: &str, host_nqn: &str) -> io::Result<bool> {
        let _guard = self
            .inner
            .admission
            .lock()
            .map_err(|_| invalid("registry lock poisoned"))?;
        if self.inner.uncertain.load(Ordering::Acquire) {
            return Err(invalid("registry durability is uncertain"));
        }
        for entry in std::fs::read_dir(&self.inner.directory)? {
            let entry = entry?;
            if entry.path().extension().and_then(|s| s.to_str()) != Some("json") {
                continue;
            }
            let intent: Intent = serde_json::from_slice(&std::fs::read(entry.path())?)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            if intent.target.nqn == nqn && intent.host_nqn == host_nqn {
                return Ok(true);
            }
        }
        Ok(false)
    }
    pub fn connect(
        &self,
        target: &LiveConnectTarget,
        host_nqn: &str,
        host_id: [u8; 16],
        access: LiveNamespaceAccess,
        deadline: Instant,
    ) -> io::Result<(LiveController, LiveNamespaceBinding)> {
        LiveController::connect(self, target, host_nqn, host_id, access, deadline)
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
                if !meta.file_type().is_block_device()
                    || format!("{}:{}", libc::major(meta.rdev()), libc::minor(meta.rdev())) != dev
                {
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
        let device = format!(
            "{}:{}",
            libc::major(metadata.rdev()),
            libc::minor(metadata.rdev())
        );
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cpu_lists_parse_single_ranges_and_mixed_sets() {
        assert_eq!(parse_cpu_list("7\n").unwrap(), vec![7]);
        assert_eq!(parse_cpu_list("0-3").unwrap(), vec![0, 1, 2, 3]);
        assert_eq!(
            parse_cpu_list("0-3,8,10-11\n").unwrap(),
            vec![0, 1, 2, 3, 8, 10, 11]
        );
        for invalid in ["", "\n", "3-1", "a", "0,", "1-2-3"] {
            assert!(parse_cpu_list(invalid).is_err(), "{invalid:?}");
        }
        assert!(parse_cpu_list(&format!("0-{}", usize::MAX)).is_err());
        assert!(parse_cpu_list(&libc::CPU_SETSIZE.to_string()).is_err());
    }

    #[test]
    fn queue_cpus_reads_sysfs_in_numeric_queue_order_and_names_errors() {
        let temp = tempdir::TempDir::new("queue-cpus").unwrap();
        let root = temp.path();
        let mq = root.join("nvme0n1/mq");
        for (queue, cpus) in [("10", "8"), ("2", "0-3,8"), ("0", "4-5")] {
            let path = mq.join(queue);
            std::fs::create_dir_all(&path).unwrap();
            std::fs::write(path.join("cpu_list"), cpus).unwrap();
        }
        assert_eq!(
            queue_cpus_at(root, Path::new("/dev/nvme0n1")).unwrap(),
            vec![vec![4, 5], vec![0, 1, 2, 3, 8], vec![8]]
        );
        let path = mq.join("2/cpu_list");
        std::fs::write(&path, "").unwrap();
        let error = queue_cpus_at(root, Path::new("/dev/nvme0n1")).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains(path.to_str().unwrap()));
        std::fs::remove_file(&path).unwrap();
        let error = queue_cpus_at(root, Path::new("/dev/nvme0n1")).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::NotFound);
        assert!(error.to_string().contains(path.to_str().unwrap()));
    }

    #[test]
    fn queue_cpus_follows_native_multipath_head_in_sorted_path_order() {
        let temp = tempdir::TempDir::new("queue-cpus-multipath").unwrap();
        let root = temp.path();
        let paths = root.join("nvme0n1/multipath");
        std::fs::create_dir_all(&paths).unwrap();
        for (name, cpus) in [("nvme0c2n1", "8-9"), ("nvme0c1n1", "2-3")] {
            let disk = root.join(name);
            std::fs::create_dir_all(disk.join("mq/0")).unwrap();
            std::fs::write(disk.join("mq/0/cpu_list"), cpus).unwrap();
            std::os::unix::fs::symlink(&disk, paths.join(name)).unwrap();
        }
        assert_eq!(
            queue_cpus_at(root, Path::new("/dev/nvme0n1")).unwrap(),
            vec![vec![2, 3]]
        );
    }

    #[test]
    fn live_queue_geometry_defaults_to_four_128_entry_queues() {
        assert_eq!(
            LiveQueueGeometry::default(),
            LiveQueueGeometry {
                io_queues: 4,
                queue_size: 128,
                poll_queues: 0
            }
        );
    }

    #[test]
    fn live_queue_geometry_validates_all_bounds() {
        assert!(LiveQueueGeometry::default().validate().is_ok());
        for io_queues in [1, 16] {
            for queue_size in [16, 128] {
                for poll_queues in [0, 4] {
                    assert!(
                        LiveQueueGeometry {
                            io_queues,
                            queue_size,
                            poll_queues
                        }
                        .validate()
                        .is_ok()
                    );
                }
            }
        }
        for geometry in [
            LiveQueueGeometry {
                io_queues: 0,
                ..Default::default()
            },
            LiveQueueGeometry {
                io_queues: 17,
                ..Default::default()
            },
            LiveQueueGeometry {
                queue_size: 15,
                ..Default::default()
            },
            LiveQueueGeometry {
                queue_size: 129,
                ..Default::default()
            },
            LiveQueueGeometry {
                poll_queues: 5,
                ..Default::default()
            },
        ] {
            assert!(geometry.validate().is_err());
        }
        assert!(
            serde_json::from_value::<LiveQueueGeometry>(serde_json::json!({
                "io_queues": 1, "queue_size": 32, "poll_queues": 0, "unknown": 0
            }))
            .is_err()
        );
    }

    #[test]
    fn old_connect_target_defaults_geometry_and_pins_connect_options() {
        let mut target: LiveConnectTarget = serde_json::from_value(serde_json::json!({
            "nqn": "nqn.2026-01.test:wal", "uuid": vec![1; 16], "nsid": 1,
            "bytes": 4096, "block_size": 4096, "address": "127.0.0.1",
            "service": 4420, "transport": "tcp"
        }))
        .unwrap();
        assert_eq!(target.geometry, LiveQueueGeometry::default());
        let expected = "nqn=nqn.2026-01.test:wal,transport=tcp,traddr=127.0.0.1,trsvcid=4420,hostnqn=nqn.2026-01.test:host,hostid=02020202-0202-0202-0202-020202020202,duplicate_connect,ctrl_loss_tmo=0,nr_io_queues=4,queue_size=128";
        assert_eq!(
            connect_options(&target, "nqn.2026-01.test:host", [2; 16]),
            expected
        );
        target.geometry = LiveQueueGeometry {
            io_queues: 1,
            queue_size: 32,
            poll_queues: 1,
        };
        assert_eq!(
            connect_options(&target, "nqn.2026-01.test:host", [2; 16]),
            expected.replace(
                "nr_io_queues=4,queue_size=128",
                "nr_io_queues=1,queue_size=32,nr_poll_queues=1"
            )
        );
        let other = connect_options(&target, "nqn.2026-01.test:other-owner", [3; 16]);
        assert!(other.contains(
            "hostnqn=nqn.2026-01.test:other-owner,hostid=03030303-0303-0303-0303-030303030303"
        ));
        assert!(!other.contains("hostnqn=nqn.2026-01.test:host,"));
    }

    #[test]
    fn vanished_accepts_only_missing_or_torn_down_devices() {
        assert!(vanished(&io::Error::from(io::ErrorKind::NotFound)));
        assert!(vanished(&io::Error::from_raw_os_error(libc::ENOENT)));
        assert!(vanished(&io::Error::from_raw_os_error(libc::ENODEV)));
        assert!(!vanished(&io::Error::from(io::ErrorKind::PermissionDenied)));
        assert!(!vanished(&io::Error::from_raw_os_error(libc::EIO)));
        assert!(!vanished(&io::Error::from_raw_os_error(libc::ENXIO)));
    }

    #[test]
    fn namespace_attribute_disappearance_skips_but_other_errors_fail() {
        let temp = tempdir::TempDir::new("namespace-attribute").unwrap();
        let path = temp.path().join("nsid");
        assert_eq!(scan_attribute(&path).unwrap(), None);
        std::fs::write(&path, "1\n").unwrap();
        assert_eq!(scan_attribute(&path).unwrap().as_deref(), Some("1"));
        assert!(scan_attribute(temp.path()).is_err());
    }

    #[test]
    fn queue_geometry_admission_requires_exact_readable_kernel_attributes() {
        let temp = tempdir::TempDir::new("controller-queues").unwrap();
        let path = temp.path();
        std::fs::write(path.join("queue_count"), "5\n").unwrap();
        std::fs::write(path.join("sqsize"), "127\n").unwrap();
        verify_queue_geometry(path, LiveQueueGeometry::default()).unwrap();
        for (geometry, count, size) in [
            (
                LiveQueueGeometry {
                    io_queues: 1,
                    queue_size: 32,
                    poll_queues: 0,
                },
                "2",
                "31",
            ),
            (
                LiveQueueGeometry {
                    poll_queues: 1,
                    ..Default::default()
                },
                "6",
                "127",
            ),
        ] {
            std::fs::write(path.join("queue_count"), count).unwrap();
            std::fs::write(path.join("sqsize"), size).unwrap();
            verify_queue_geometry(path, geometry).unwrap();
        }

        std::fs::write(path.join("queue_count"), "17\n").unwrap();
        assert_eq!(
            verify_queue_geometry(path, LiveQueueGeometry::default())
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData
        );
        std::fs::write(path.join("queue_count"), "5\n").unwrap();
        std::fs::write(path.join("sqsize"), "31\n").unwrap();
        assert_eq!(
            verify_queue_geometry(path, LiveQueueGeometry::default())
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData
        );
        std::fs::write(path.join("sqsize"), "not-a-number\n").unwrap();
        assert_eq!(
            verify_queue_geometry(path, LiveQueueGeometry::default())
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData
        );
        std::fs::remove_file(path.join("sqsize")).unwrap();
        assert_eq!(
            verify_queue_geometry(path, LiveQueueGeometry::default())
                .unwrap_err()
                .kind(),
            io::ErrorKind::NotFound
        );
    }

    #[test]
    fn registry_excludes_duplicate_intents_and_holds_lock() {
        let temp = tempdir::TempDir::new("live-connect").unwrap();
        let path = temp.path().join("registry");
        let registry = LiveConnectionRegistry::open(&path).unwrap();
        assert!(LiveConnectionRegistry::open(&path).is_err());
        let intent = Intent {
            target: LiveConnectTarget {
                nqn: "nqn.2026-01.test:wal".into(),
                uuid: [1; 16],
                nsid: 1,
                bytes: 4096,
                block_size: 4096,
                address: "127.0.0.1".into(),
                service: 4420,
                transport: NvmeofTransport::Tcp,
                geometry: Default::default(),
            },
            host_nqn: "nqn.2026-01.test:host".into(),
            host_id: [2; 16],
        };
        std::fs::write(path.join("interrupted.tmp"), b"{\"target\":").unwrap();
        assert!(
            !registry
                .has_intent(&intent.target.nqn, &intent.host_nqn)
                .unwrap()
        );
        let journal = registry.reserve(&intent).unwrap();
        assert!(
            registry
                .has_intent(&intent.target.nqn, &intent.host_nqn)
                .unwrap()
        );
        assert!(registry.reserve(&intent).is_err());
        let restored: Intent = serde_json::from_slice(&std::fs::read(&journal).unwrap()).unwrap();
        assert_eq!(restored.host_nqn, intent.host_nqn);
        assert_eq!(restored.target.uuid, intent.target.uuid);
        registry.remove_intent(&journal).unwrap();
        assert!(
            !registry
                .has_intent(&intent.target.nqn, &intent.host_nqn)
                .unwrap()
        );
        drop(registry);
        let registry = LiveConnectionRegistry::open(&path).unwrap();
        assert!(registry.reserve(&intent).is_ok());
    }

    #[test]
    fn retirement_does_not_touch_missing_or_replaced_original() {
        let temp = tempdir::TempDir::new("controller-retire").unwrap();
        let path = temp.path().join("controller");
        let original = File::create(&path).unwrap();
        let metadata = original.metadata().unwrap();
        std::fs::remove_file(&path).unwrap();
        let mut state = DeletionState::NotStarted;
        retire_controller(
            &mut state,
            &path,
            metadata.dev(),
            metadata.ino(),
            Instant::now() + Duration::from_secs(1),
            || panic!("removed controller must not be verified"),
            || panic!("removed controller must not be deleted"),
        )
        .unwrap();
        assert_eq!(state, DeletionState::Gone);
        let replacement = File::create(&path).unwrap();
        assert_ne!(replacement.metadata().unwrap().ino(), metadata.ino());
        state = DeletionState::NotStarted;
        retire_controller(
            &mut state,
            &path,
            metadata.dev(),
            metadata.ino(),
            Instant::now() + Duration::from_secs(1),
            || panic!("replacement must not be verified"),
            || panic!("replacement must not be deleted"),
        )
        .unwrap();
        assert_eq!(state, DeletionState::Gone);
        assert!(path.exists());
    }

    #[test]
    fn retirement_missing_attribute_remains_uncertain_until_original_disappears() {
        for errno in [libc::ENOENT, libc::ENODEV] {
            let temp = tempdir::TempDir::new("controller-uncertain").unwrap();
            let path = temp.path().join("controller");
            let original = File::create(&path).unwrap();
            let metadata = original.metadata().unwrap();
            let mut state = DeletionState::NotStarted;
            let result = retire_controller(
                &mut state,
                &path,
                metadata.dev(),
                metadata.ino(),
                Instant::now() + Duration::from_millis(100),
                || Err(io::Error::from_raw_os_error(errno)),
                || panic!("missing verification attribute must not issue delete"),
            );
            assert_eq!(result.unwrap_err().kind(), io::ErrorKind::TimedOut);
            assert_eq!(state, DeletionState::Uncertain);
            std::fs::remove_file(&path).unwrap();
            retire_controller(
                &mut state,
                &path,
                metadata.dev(),
                metadata.ino(),
                Instant::now() + Duration::from_secs(1),
                || panic!("uncertain retry must not verify again"),
                || panic!("uncertain retry must not issue delete"),
            )
            .unwrap();
            assert_eq!(state, DeletionState::Gone);
        }
    }

    #[test]
    fn retirement_missing_delete_attribute_observes_without_reissue() {
        for errno in [libc::ENOENT, libc::ENODEV] {
            let temp = tempdir::TempDir::new("controller-delete-missing").unwrap();
            let path = temp.path().join("controller");
            let original = File::create(&path).unwrap();
            let metadata = original.metadata().unwrap();
            let mut state = DeletionState::NotStarted;
            let result = retire_controller(
                &mut state,
                &path,
                metadata.dev(),
                metadata.ino(),
                Instant::now() + Duration::from_millis(100),
                || Ok(()),
                || Err(io::Error::from_raw_os_error(errno)),
            );
            assert_eq!(result.unwrap_err().kind(), io::ErrorKind::TimedOut);
            assert_eq!(state, DeletionState::Uncertain);
            std::fs::remove_file(&path).unwrap();
            retire_controller(
                &mut state,
                &path,
                metadata.dev(),
                metadata.ino(),
                Instant::now() + Duration::from_secs(1),
                || panic!("uncertain retry must not verify again"),
                || panic!("uncertain retry must not issue delete"),
            )
            .unwrap();
            assert_eq!(state, DeletionState::Gone);
        }
    }

    #[test]
    fn retirement_store_io_error_retains_uncertainty_without_retry() {
        let temp = tempdir::TempDir::new("controller-store-error").unwrap();
        let path = temp.path().join("controller");
        let original = File::create(&path).unwrap();
        let metadata = original.metadata().unwrap();
        let mut state = DeletionState::NotStarted;
        let error = retire_controller(
            &mut state,
            &path,
            metadata.dev(),
            metadata.ino(),
            Instant::now() + Duration::from_secs(1),
            || Ok(()),
            || Err(io::Error::from_raw_os_error(libc::EIO)),
        )
        .unwrap_err();
        assert_eq!(error.raw_os_error(), Some(libc::EIO));
        assert_eq!(state, DeletionState::Uncertain);
        let error = retire_controller(
            &mut state,
            &path,
            metadata.dev(),
            metadata.ino(),
            Instant::now() + Duration::from_millis(100),
            || panic!("uncertain retry must not verify again"),
            || panic!("uncertain retry must not issue delete"),
        )
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert_eq!(state, DeletionState::Uncertain);
    }

    #[test]
    fn missing_device_retries_but_other_failures_do_not() {
        let temp = tempdir::TempDir::new("device-readiness").unwrap();
        let path = temp.path().join("device");
        let created_path = path.clone();
        let creator = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(10));
            File::create(created_path).unwrap();
        });
        let file = wait_for_device(
            "open test device",
            &path,
            "nvme5",
            DeviceNode::Namespace,
            Instant::now() + Duration::from_secs(2),
            || Ok(()),
            || File::open(&path),
        )
        .unwrap();
        drop(file);
        creator.join().unwrap();
        let mut calls = 0;
        wait_for_device(
            "open test device",
            &path,
            "nvme5",
            DeviceNode::Namespace,
            Instant::now() + Duration::from_secs(2),
            || Ok(()),
            || {
                calls += 1;
                if calls == 1 {
                    Err(io::Error::from_raw_os_error(libc::ENODEV))
                } else {
                    Ok(())
                }
            },
        )
        .unwrap();
        assert_eq!(calls, 2);
        let mut calls = 0;
        let error = wait_for_device::<()>(
            "open test device",
            &path,
            "nvme5",
            DeviceNode::Namespace,
            Instant::now() + Duration::from_secs(2),
            || Ok(()),
            || {
                calls += 1;
                Err(io::Error::from_raw_os_error(libc::EIO))
            },
        )
        .unwrap_err();
        assert_eq!(calls, 1);
        assert_eq!(error.kind(), io::Error::from_raw_os_error(libc::EIO).kind());
        assert!(error.to_string().contains("open test device"));
        assert!(error.to_string().contains(path.to_str().unwrap()));
    }

    #[test]
    fn namespace_enxio_retries_reverify_the_controller_before_each_open() {
        let temp = tempdir::TempDir::new("namespace-enxio").unwrap();
        let path = temp.path().join("nvme5n1");
        File::create(&path).unwrap();
        let mut verifies = 0;
        let mut opens = 0;
        let file = wait_for_device(
            "open namespace device",
            &path,
            "nvme5",
            DeviceNode::Namespace,
            Instant::now() + Duration::from_secs(2),
            || {
                verifies += 1;
                Ok(())
            },
            || {
                opens += 1;
                if opens <= 2 {
                    Err(io::Error::from_raw_os_error(libc::ENXIO))
                } else {
                    File::open(&path)
                }
            },
        )
        .unwrap();
        assert_eq!(
            file.metadata().unwrap().ino(),
            path.metadata().unwrap().ino()
        );
        assert_eq!(opens, 3);
        assert_eq!(verifies, 3);
    }

    #[test]
    fn namespace_enxio_deadline_names_the_device_and_owned_controller() {
        let start = Instant::now();
        let deadline = start + Duration::from_millis(40);
        let mut opens = 0;
        let mut verifies = 0;
        let error = wait_for_device::<()>(
            "open namespace device",
            Path::new("/dev/nvme5n1"),
            "nvme5",
            DeviceNode::Namespace,
            deadline,
            || {
                verifies += 1;
                Ok(())
            },
            || {
                opens += 1;
                Err(io::Error::from_raw_os_error(libc::ENXIO))
            },
        )
        .unwrap_err();
        assert!(Instant::now() >= deadline);
        assert!(start.elapsed() < Duration::from_secs(2));
        assert!(opens > 0);
        assert_eq!(verifies, opens);
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        let message = error.to_string();
        assert!(message.contains("/dev/nvme5n1"));
        assert!(message.contains("controller nvme5"));
        assert!(message.contains("ENXIO persisted"));
    }

    #[test]
    fn namespace_retry_rejects_reused_controller_identity_before_foreign_open() {
        let temp = tempdir::TempDir::new("namespace-reused-controller").unwrap();
        let controller = temp.path().join("nvme5");
        std::fs::create_dir(&controller).unwrap();
        for (name, value) in [
            ("subsysnqn", "nqn.2026-01.test:owned"),
            ("hostnqn", "nqn.2026-01.test:host"),
            ("hostid", "02020202-0202-0202-0202-020202020202"),
        ] {
            std::fs::write(controller.join(name), value).unwrap();
        }
        let directory = File::open(&controller).unwrap();
        let original = directory.metadata().unwrap();
        let mut opens = 0;
        let mut verifies = 0;
        let error = wait_for_device::<()>(
            "open namespace device",
            Path::new("/dev/nvme5n1"),
            "nvme5",
            DeviceNode::Namespace,
            Instant::now() + Duration::from_secs(2),
            || {
                verifies += 1;
                let current = controller.metadata()?;
                if current.dev() != original.dev()
                    || current.ino() != original.ino()
                    || controller_attribute(&controller, &directory, "subsysnqn")?.as_deref()
                        != Some("nqn.2026-01.test:owned")
                    || controller_attribute(&controller, &directory, "hostnqn")?.as_deref()
                        != Some("nqn.2026-01.test:host")
                    || controller_attribute(&controller, &directory, "hostid")?.as_deref()
                        != Some("02020202-0202-0202-0202-020202020202")
                {
                    return Err(invalid("controller identity changed"));
                }
                Ok(())
            },
            || {
                opens += 1;
                assert_eq!(opens, 1, "must never open a foreign controller's device");
                std::fs::rename(&controller, temp.path().join("old-controller"))?;
                std::fs::create_dir(&controller)?;
                Err(io::Error::from_raw_os_error(libc::ENXIO))
            },
        )
        .unwrap_err();
        assert_eq!(opens, 1);
        assert_eq!(verifies, 2);
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("controller identity changed"));
    }

    #[test]
    fn namespace_open_io_and_permission_errors_and_admin_enxio_do_not_retry() {
        for (node, errno) in [
            (DeviceNode::Namespace, libc::EIO),
            (DeviceNode::Namespace, libc::EACCES),
            (DeviceNode::Controller, libc::ENXIO),
        ] {
            let mut opens = 0;
            let error = wait_for_device::<()>(
                "open test device",
                Path::new("/dev/nvme5n1"),
                "nvme5",
                node,
                Instant::now() + Duration::from_secs(2),
                || Ok(()),
                || {
                    opens += 1;
                    Err(io::Error::from_raw_os_error(errno))
                },
            )
            .unwrap_err();
            assert_eq!(opens, 1);
            assert_eq!(error.kind(), io::Error::from_raw_os_error(errno).kind());
        }
    }

    #[test]
    fn namespace_identity_enxio_is_not_a_retryable_device_open_error() {
        let mut verifies = 0;
        let error = wait_for_device::<()>(
            "open namespace device",
            Path::new("/dev/nvme5n1"),
            "nvme5",
            DeviceNode::Namespace,
            Instant::now() + Duration::from_secs(2),
            || {
                verifies += 1;
                Err(io::Error::from_raw_os_error(libc::ENXIO))
            },
            || panic!("identity check failed: device must not open"),
        )
        .unwrap_err();
        assert_eq!(verifies, 1);
        assert_eq!(
            error.kind(),
            io::Error::from_raw_os_error(libc::ENXIO).kind()
        );
    }

    #[test]
    fn failed_connect_cleanup_preserves_the_primary_readiness_diagnostic() {
        let primary = "open namespace device /dev/nvme5n1 (controller nvme5): ENXIO persisted";
        let error = connect_error_after_cleanup(
            io::Error::new(io::ErrorKind::TimedOut, primary),
            Err(io::Error::from_raw_os_error(libc::EACCES)),
        );
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        let message = error.to_string();
        assert!(message.starts_with(primary));
        assert!(message.contains("; controller cleanup failed: "));
        assert!(message.contains(&io::Error::from_raw_os_error(libc::EACCES).to_string()));
        // Successful cleanup preserves even the original OS error representation.
        let error = connect_error_after_cleanup(io::Error::from_raw_os_error(libc::EIO), Ok(()));
        assert_eq!(error.raw_os_error(), Some(libc::EIO));
    }

    #[test]
    fn disappeared_controller_is_distinct_from_missing_live_attribute() {
        let temp = tempdir::TempDir::new("controller-attribute").unwrap();
        let path = temp.path().join("controller");
        std::fs::create_dir(&path).unwrap();
        let directory = File::open(&path).unwrap();
        assert!(controller_attribute(&path, &directory, "hostid").is_err());
        std::fs::remove_dir(&path).unwrap();
        assert!(
            controller_attribute(&path, &directory, "hostid")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn deletion_confirmation_waits_and_never_touches_replacement() {
        let temp = tempdir::TempDir::new("controller-confirmation").unwrap();
        let path = temp.path().join("controller");
        let original = File::create(&path).unwrap();
        let metadata = original.metadata().unwrap();
        assert!(!original_removed(&path, metadata.dev(), metadata.ino()).unwrap());
        let error =
            wait_for_original_removal(&path, metadata.dev(), metadata.ino(), Instant::now())
                .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        std::fs::remove_file(&path).unwrap();
        assert!(original_removed(&path, metadata.dev(), metadata.ino()).unwrap());
        let replacement = File::create(&path).unwrap();
        assert_ne!(replacement.metadata().unwrap().ino(), metadata.ino());
        assert!(original_removed(&path, metadata.dev(), metadata.ino()).unwrap());
        wait_for_original_removal(
            &path,
            metadata.dev(),
            metadata.ino(),
            Instant::now() + Duration::from_secs(1),
        )
        .unwrap();
        assert!(path.exists());
    }

    #[test]
    fn deletion_confirmation_observes_delayed_disappearance() {
        let temp = tempdir::TempDir::new("controller-delayed").unwrap();
        let path = temp.path().join("controller");
        let original = File::create(&path).unwrap();
        let metadata = original.metadata().unwrap();
        let removed_path = path.clone();
        let remover = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(10));
            std::fs::remove_file(removed_path).unwrap();
        });
        wait_for_original_removal(
            &path,
            metadata.dev(),
            metadata.ino(),
            Instant::now() + Duration::from_secs(2),
        )
        .unwrap();
        remover.join().unwrap();
    }

    #[test]
    fn namespace_visibility_rejects_hidden_and_invalid_paths() {
        assert!(namespace_name("nvme3n1"));
        assert!(namespace_name("nvme3c2n1"));
        assert!(!namespace_name("nvme3n1p1"));
        assert!(!namespace_name("nvme3"));
        let temp = tempdir::TempDir::new("namespace-hidden").unwrap();
        let hidden = temp.path().join("hidden");
        std::fs::write(&hidden, "1\n").unwrap();
        assert!(!namespace_visible(temp.path()).unwrap());
        std::fs::write(&hidden, "0\n").unwrap();
        assert!(namespace_visible(temp.path()).unwrap());
        std::fs::write(&hidden, "unexpected\n").unwrap();
        assert!(namespace_visible(temp.path()).is_err());
    }

    #[test]
    fn fabrics_options_reject_delimiters() {
        assert!(valid_nqn("nqn.2026-01.test:host"));
        assert!(!valid_nqn("nqn.2026-01.test:host,duplicate_connect"));
        assert!(!valid_nqn("nqn.2026-01.test:host=other"));
    }

    fn would_block(file: &File) -> bool {
        matches!(file.try_lock(), Err(std::fs::TryLockError::WouldBlock))
            && matches!(
                file.try_lock_shared(),
                Err(std::fs::TryLockError::WouldBlock)
            )
    }

    fn unlocked(path: &Path) -> bool {
        File::open(path).unwrap().try_lock().is_ok()
    }

    /// Fake sysfs and /dev for controller `nvme9`: a plain disk `nvme9n1`, a
    /// multipath head `nvme9n2` linking to it, a hidden path disk, a disk of
    /// `nvme8`, a node `nvme9n3` now naming another device, and a visible disk
    /// `nvme9n4` whose node vanished. Returns `(block, dev, ctrl)`.
    fn fake_controller_disks(root: &Path) -> (PathBuf, PathBuf, PathBuf) {
        let (block, dev) = (root.join("block"), root.join("dev"));
        for dir in ["ctrl/nvme9", "ctrl/nvme8", "subsys", "block", "dev"] {
            std::fs::create_dir_all(root.join(dir)).unwrap();
        }
        let ctrl = std::fs::canonicalize(root.join("ctrl/nvme9")).unwrap();
        std::os::unix::fs::symlink(&ctrl, root.join("subsys/nvme9")).unwrap();
        for (name, parent, hidden, number, node) in [
            ("nvme9n1", "ctrl/nvme9", "0", "0:0", true),
            ("nvme9n2", "subsys", "0", "0:0", true),
            ("nvme9c9n1", "ctrl/nvme9", "1", "0:0", true),
            ("nvme8n1", "ctrl/nvme8", "0", "0:0", true),
            ("nvme9n3", "ctrl/nvme9", "0", "7:3", true),
            ("nvme9n4", "ctrl/nvme9", "0", "0:0", false),
        ] {
            let disk = block.join(name);
            std::fs::create_dir(&disk).unwrap();
            std::os::unix::fs::symlink(root.join(parent), disk.join("device")).unwrap();
            std::fs::write(disk.join("hidden"), hidden).unwrap();
            // Regular files stand in for nodes: their device number is 0:0.
            std::fs::write(disk.join("dev"), number).unwrap();
            if node {
                File::create(dev.join(name)).unwrap();
            }
        }
        (block, dev, ctrl)
    }

    #[test]
    fn reconcile_teardown_locks_every_visible_disk_without_resolve_history() {
        let temp = tempdir::TempDir::new("teardown-disks").unwrap();
        let (block, dev, ctrl) = fake_controller_disks(temp.path());
        let mut names: Vec<String> = owned_visible_disks(&block, &ctrl, "nvme9")
            .unwrap()
            .into_iter()
            .map(|(name, _)| name)
            .collect();
        names.sort();
        assert_eq!(names, ["nvme9n1", "nvme9n2", "nvme9n3", "nvme9n4"]);
        let deadline = Instant::now() + Duration::from_secs(10);
        let locks = lock_controller_disks(&block, &dev, &ctrl, "nvme9", deadline).unwrap();
        assert_eq!(locks.len(), 2);
        // udevd's close-after-write reread takes LOCK_EX|LOCK_NB and its
        // workers LOCK_SH|LOCK_NB: both must skip the controller's disks.
        assert!(would_block(&File::open(dev.join("nvme9n1")).unwrap()));
        assert!(would_block(&File::open(dev.join("nvme9n2")).unwrap()));
        for other in ["nvme9c9n1", "nvme8n1", "nvme9n3"] {
            assert!(unlocked(&dev.join(other)), "{other} must not be locked");
        }
        drop(locks);
        assert!(unlocked(&dev.join("nvme9n1")));
    }

    #[test]
    fn disk_lock_is_taken_only_at_teardown_and_released_after_deletion() {
        let temp = tempdir::TempDir::new("teardown-order").unwrap();
        let (block, dev, ctrl) = fake_controller_disks(temp.path());
        let disk = dev.join("nvme9n1");
        let deadline = Instant::now() + Duration::from_secs(10);
        // Normal use: namespace I/O opens and closes the disk read-write, and
        // udev may process it; nothing holds the lock.
        drop(
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(&disk)
                .unwrap(),
        );
        let udev = File::open(&disk).unwrap();
        udev.try_lock().unwrap();
        udev.unlock().unwrap();
        let mut locks = Vec::new();
        let error = retire_locked(
            &mut locks,
            true,
            || lock_controller_disks(&block, &dev, &ctrl, "nvme9", deadline),
            || {
                assert!(would_block(&udev), "delete issued without the lock");
                Err(io::Error::other("deletion uncertain"))
            },
        )
        .unwrap_err();
        assert_eq!(error.to_string(), "deletion uncertain");
        assert!(would_block(&udev), "unconfirmed deletion released the lock");
        // A later attempt after the store keeps the same locks until confirmation.
        retire_locked(
            &mut locks,
            false,
            || panic!("teardown relocked after the delete was issued"),
            || {
                assert!(would_block(&udev));
                Ok(())
            },
        )
        .unwrap();
        assert!(locks.is_empty());
        udev.try_lock().unwrap();
    }

    #[test]
    fn teardown_lock_timeout_issues_no_delete() {
        let temp = tempdir::TempDir::new("teardown-timeout").unwrap();
        let (block, dev, ctrl) = fake_controller_disks(temp.path());
        let probe = File::open(dev.join("nvme9n1")).unwrap();
        probe.lock_shared().unwrap();
        let mut locks = Vec::new();
        let mut deleted = false;
        let error = retire_locked(
            &mut locks,
            true,
            || {
                lock_controller_disks(
                    &block,
                    &dev,
                    &ctrl,
                    "nvme9",
                    Instant::now() + Duration::from_millis(50),
                )
            },
            || {
                deleted = true;
                Ok(())
            },
        )
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert!(!deleted, "delete issued without the lock");
        assert!(locks.is_empty());
        // Locks already taken in the failed attempt are released.
        assert!(unlocked(&dev.join("nvme9n2")));
    }

    #[test]
    fn disk_lock_waits_for_udev_probe_and_never_outlives_its_deadline() {
        let temp = tempdir::TempDir::new("disk-lock-wait").unwrap();
        let disk = temp.path().join("nvme9n1");
        File::create(&disk).unwrap();
        let probe = File::open(&disk).unwrap();
        probe.lock_shared().unwrap();
        let error = lock_disk(
            File::open(&disk).unwrap(),
            &disk,
            "nvme9",
            Instant::now() + Duration::from_millis(50),
        )
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert!(
            error
                .to_string()
                .contains(&format!("{} (controller nvme9)", disk.display()))
        );
        let waiter = {
            let disk = disk.clone();
            std::thread::spawn(move || {
                lock_disk(
                    File::open(&disk).unwrap(),
                    &disk,
                    "nvme9",
                    Instant::now() + Duration::from_secs(10),
                )
            })
        };
        // Ending the probe lets the waiter in; the timed-out helper also wakes,
        // and must drop its late lock instead of keeping it.
        drop(probe);
        let held = waiter.join().unwrap().unwrap();
        assert!(would_block(&File::open(&disk).unwrap()));
        drop(held);
        // The late helper may still take the lock briefly; it must let it go.
        lock_disk(
            File::open(&disk).unwrap(),
            &disk,
            "nvme9",
            Instant::now() + Duration::from_secs(10),
        )
        .unwrap();
    }
}
