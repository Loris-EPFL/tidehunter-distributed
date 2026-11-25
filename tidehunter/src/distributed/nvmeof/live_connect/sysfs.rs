use super::*;

/// CPUs serving each hardware queue, in numeric queue-index order.
/// Native multipath heads have no `mq`: use the first per-path disk in sorted
/// path-name order. Our connections normally have exactly one such path.
pub(crate) fn queue_cpus(namespace_device: &Path) -> io::Result<Vec<Vec<usize>>> {
    let device = std::fs::canonicalize(namespace_device)
        .map_err(|error| with_context("resolve namespace", namespace_device, error))?;
    queue_cpus_at(Path::new("/sys/block"), &device)
}

pub(super) fn queue_cpus_at(
    block_root: &Path,
    namespace_device: &Path,
) -> io::Result<Vec<Vec<usize>>> {
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

pub(super) fn parse_cpu_list(list: &str) -> io::Result<Vec<usize>> {
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
        if start > end || end >= sys::affinity_cpu_limit() {
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
    sys::set_thread_affinity(cpu)
        .map_err(|error| with_context("set thread affinity", Path::new("/proc/thread-self"), error))
}

/// A sysfs attribute read during controller teardown can return ENODEV, which
/// Rust does not map to NotFound. Keep each call site's existing NotFound
/// semantics for both errors; ENODEV proves no more absence than NotFound does.
pub(super) fn vanished(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::NotFound || error.raw_os_error() == Some(libc::ENODEV)
}

/// Verifies the queue geometry and returns the observed `(queue_count, sqsize)`.
pub(super) fn verify_queue_geometry(
    path: &Path,
    geometry: LiveQueueGeometry,
) -> io::Result<(u32, u32)> {
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
    Ok((count, size))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DeletionState {
    NotStarted,
    Initiated,
    Uncertain,
    Gone,
}

pub(super) fn original_removed(path: &Path, device: u64, inode: u64) -> io::Result<bool> {
    match std::fs::metadata(path) {
        Ok(meta) => Ok(meta.dev() != device || meta.ino() != inode),
        Err(error) if vanished(&error) => Ok(true),
        Err(error) => Err(with_context("inspect owned controller", path, error)),
    }
}

pub(super) fn wait_for_original_removal(
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

pub(super) fn retire_controller(
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

pub(super) fn attr(p: &Path) -> io::Result<String> {
    Ok(std::fs::read_to_string(p)?.trim().to_owned())
}

// Sysfs entries can disappear during enumeration. Permission and I/O errors
// remain fatal.
pub(super) fn scan_attribute(path: &Path) -> io::Result<Option<String>> {
    match attr(path) {
        Ok(value) => Ok(Some(value)),
        Err(error) if vanished(&error) => Ok(None),
        Err(error) => Err(with_context("read namespace sysfs", path, error)),
    }
}

pub(super) fn controller_attribute(
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

#[derive(Clone, Copy)]
pub(super) enum DeviceNode {
    Namespace,
    Controller,
}

// ENXIO can mean a namespace has not been scanned yet, but must never mean
// confirmed disappearance during cleanup, or be retried for controller admin I/O.
pub(super) fn wait_for_device<T>(
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
pub(super) fn lock_disk(
    disk: File,
    path: &Path,
    controller: &str,
    deadline: Instant,
) -> io::Result<File> {
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
pub(super) fn owned_visible_disks(
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

/// Waits for systemd-udevd to drain its queue at `queue`. A host without udev
/// has no queue file and needs no wait. This runs before the teardown disk
/// locks, so a probe that is already reading can finish instead of losing its
/// paths. Production passes `/run/udev/queue`; the path is injectable so a
/// fixture teardown never waits on the host's queue.
pub(super) fn wait_for_udev_idle_at(queue: &Path, deadline: Instant) -> io::Result<()> {
    loop {
        match std::fs::symlink_metadata(queue) {
            Err(error) if vanished(&error) => return Ok(()),
            Err(error) => return Err(with_context("inspect udev queue", queue, error)),
            Ok(_) => {}
        }
        std::thread::sleep(remaining(deadline)?.min(Duration::from_millis(20)));
    }
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
pub(super) fn lock_controller_disks(
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
        if sys::device_number(rdev) == dev {
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
pub(super) fn retire_locked(
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
pub(super) fn namespace_visible(path: &Path) -> io::Result<bool> {
    match attr(&path.join("hidden"))?.as_str() {
        "0" => Ok(true),
        "1" => Ok(false),
        _ => Err(invalid("invalid namespace hidden state")),
    }
}
pub(super) fn namespace_name(name: &str) -> bool {
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
