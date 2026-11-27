//! Owned Linux fabrics connections. The caller must drain and close every
//! namespace I/O handle before disconnecting the controller. A connection is
//! not a durability or authorization receipt.
pub use super::LiveQueueGeometry;
use super::NvmeofTransport;
use super::live_metrics::LiveControllerEvidence;
use super::live_namespace::{
    LiveNamespace, LiveNamespaceAccess, LiveNamespaceBinding, controller_lost_error,
    identity_error, lost_if_vanished,
};
use crate::distributed::sys;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::net::IpAddr;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{DirBuilderExt, FileTypeExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, mpsc};
use std::time::{Duration, Instant};

mod registry;
mod sysfs;

#[cfg(test)]
mod tests;

pub use registry::LiveConnectionRegistry;
pub(crate) use registry::RegistryMode;
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

/// How a connect treats a controller a durable intent records.
///
/// The write path adopts: after an abrupt coordinator exit a healthy controller
/// must not be torn down and recreated. A recovery read adopts only when the
/// intent records the same owner incarnation it now sees. A different or
/// unknown incarnation may name a target a new owner process replaced, so the
/// recovery retires the leftover and connects fresh.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ControllerAdoption {
    /// Reuse a matching live controller (write and read path).
    Adopt { owner_incarnation: Option<[u8; 16]> },
    /// Reuse a matching live controller only when its intent records the same
    /// `owner_incarnation`; otherwise retire the identity and connect fresh.
    Recovery { owner_incarnation: Option<[u8; 16]> },
    /// Retire any matching intent and controller, then connect a new one.
    Fresh { owner_incarnation: Option<[u8; 16]> },
}

impl ControllerAdoption {
    /// The owner incarnation this connect is made against, recorded in the
    /// durable intent. `None` means unknown (fixtures and admin-only users).
    pub(crate) fn owner_incarnation(self) -> Option<[u8; 16]> {
        match self {
            Self::Adopt { owner_incarnation }
            | Self::Recovery { owner_incarnation }
            | Self::Fresh { owner_incarnation } => owner_incarnation,
        }
    }
}

/// The controller keep-alive timeout this engine requests, in seconds.
///
/// The kernel default is also 5 s, but a resume reconnects with the same host
/// NQN, so the target can refuse it with `-EALREADY` until its own keep-alive
/// expires. A longer value would delay that expiry and can spend the resume
/// budget, so the value is pinned explicitly and recorded in the route evidence.
pub(crate) const KEEP_ALIVE_TMO_SECS: u32 = 5;

/// The keep-alive the engine requests for one configured resume budget.
///
/// A resume reconnects with the same host NQN, and the target refuses that with
/// `-EALREADY` until its keep-alive expires. The expiry must fit inside the
/// resume budget of `2 x io_timeout_ms`, so the request is the largest whole
/// second below that budget, capped at the kernel default of 5 s. The kernel's
/// own minimum is 1 s, which a budget below 2 s cannot outlast; such a
/// configuration simply fail-stops on a break, as before W1.
pub(crate) fn keep_alive_tmo(io_timeout_ms: u64) -> u32 {
    u32::try_from((io_timeout_ms / 500).clamp(1, u64::from(KEEP_ALIVE_TMO_SECS))).unwrap_or(5)
}

fn connect_options(
    target: &LiveConnectTarget,
    host_nqn: &str,
    host_id: [u8; 16],
    keep_alive_tmo: u32,
) -> String {
    let geometry = target.geometry;
    let mut options = format!(
        "nqn={},transport={},traddr={},trsvcid={},hostnqn={},hostid={},ctrl_loss_tmo=0,keep_alive_tmo={},nr_io_queues={},queue_size={}",
        target.nqn,
        transport(target.transport),
        target.address,
        target.service,
        host_nqn,
        uuid_string(host_id),
        keep_alive_tmo,
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

/// One failed operation with its path. The typed cause stays in the source
/// chain, so `controller_lost` still classifies a wrapped verify error.
#[derive(Debug)]
struct ContextError {
    message: String,
    cause: io::Error,
}

impl std::fmt::Display for ContextError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for ContextError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.cause)
    }
}

fn with_context(operation: &str, path: &Path, error: io::Error) -> io::Error {
    io::Error::new(
        error.kind(),
        ContextError {
            message: format!("{operation} {}: {error}", path.display()),
            cause: error,
        },
    )
}

/// A connect whose worker may still create a controller after the caller
/// stopped waiting. Its durable intent must not be cleared or reused until a
/// process restart reconciles it: a late worker could otherwise create a
/// controller no reopen can retire.
#[derive(Debug)]
pub(crate) struct ConnectWorkerUncertain;

impl std::fmt::Display for ConnectWorkerUncertain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("NVMe connect worker outcome is uncertain")
    }
}

impl std::error::Error for ConnectWorkerUncertain {}

fn worker_uncertain_error(kind: io::ErrorKind) -> io::Error {
    io::Error::new(kind, ConnectWorkerUncertain)
}

/// Whether `error` reports a connect whose worker may still create a
/// controller. Walks the source chain, so a context wrapper still classifies;
/// never matches a message.
pub(crate) fn connect_worker_uncertain(error: &io::Error) -> bool {
    let Some(cause) = error.get_ref() else {
        return false;
    };
    let mut current: &(dyn std::error::Error + 'static) = cause;
    loop {
        if current.downcast_ref::<ConnectWorkerUncertain>().is_some() {
            return true;
        }
        match current.source() {
            Some(next) => current = next,
            None => return false,
        }
    }
}

/// A connect failure with its cleanup verdict. The primary error stays the
/// source, so a typed lost cause survives the cleanup note and the acquire
/// callers can still classify it.
fn connect_error_after_cleanup(primary: io::Error, cleanup: io::Result<()>) -> io::Error {
    match cleanup {
        Ok(()) => primary,
        Err(error) => io::Error::new(
            primary.kind(),
            ContextError {
                message: format!("{primary}; controller cleanup failed: {error}"),
                cause: primary,
            },
        ),
    }
}

/// The kernel rejects a second controller for an identity it still tracks with
/// `-EALREADY` once `duplicate_connect` is no longer requested.
fn already_connected(error: &io::Error) -> bool {
    error.raw_os_error() == Some(libc::EALREADY)
}

/// The fabrics control write is the call that returns `-EALREADY`. Keep that
/// errno so the bounded reconcile and the typed fail-stop can recognise it;
/// every other failure gets the control-device path context.
fn fabrics_connect_error(error: io::Error) -> io::Error {
    if already_connected(&error) {
        error
    } else {
        with_context(
            "connect fabrics control",
            Path::new("/dev/nvme-fabrics"),
            error,
        )
    }
}

fn write_connect_options(control: &mut impl Write, options: &str) -> io::Result<()> {
    control
        .write_all(options.as_bytes())
        .map_err(fabrics_connect_error)
}

/// Fresh-path fabrics identity validation. Adoption runs the same check first,
/// so a bad request can never retire an existing controller.
fn validate_connect_target(
    target: &LiveConnectTarget,
    host_nqn: &str,
    host_id: [u8; 16],
) -> io::Result<()> {
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
    Ok(())
}

/// Operator-visible fail-stop: a controller for this exact identity exists but
/// no durable intent authorizes adopting it, so only an operator can resolve it.
fn unowned_identity(target: &LiveConnectTarget, host_nqn: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "unowned NVMe controller for {} at {}:{} with host {}; refusing to create a second controller",
            target.nqn, target.address, target.service, host_nqn
        ),
    )
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
pub(super) fn valid_nqn(nqn: &str) -> bool {
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
    /// Queue count and submission queue size read from sysfs by the geometry check.
    queue: OnceLock<(u32, u32)>,
    /// Controller sysfs root; `/sys/class/nvme` in production.
    controller_root: PathBuf,
    /// systemd-udevd queue path; `/run/udev/queue` in production.
    udev_queue: PathBuf,
    /// The keep-alive the connect requested for this controller.
    keep_alive_tmo: u32,
}
impl LiveController {
    /// Controller instance returned by the Linux fabrics control response.
    pub fn instance(&self) -> u32 {
        self.instance
    }
    /// Creates a distinct controller with explicit host identity. The kernel's
    /// control response, not a sysfs inventory difference, establishes ownership.
    ///
    /// A write connect (`Adopt`) reuses a durable intent's live controller
    /// instead of deleting and recreating it; a declined adoption is retired
    /// with its intent before the fresh connect, so a live blocker is never
    /// left behind. A recovery connect (`Recovery`) adopts only when the
    /// intent records the owner incarnation this reopen sees; a different or
    /// unknown incarnation retires the identity and connects fresh. `Fresh`
    /// never adopts: it retires the identity's intent and controller first.
    fn connect(
        registry: &LiveConnectionRegistry,
        target: &LiveConnectTarget,
        host_nqn: &str,
        host_id: [u8; 16],
        access: LiveNamespaceAccess,
        adoption: ControllerAdoption,
        deadline: Instant,
    ) -> io::Result<(Self, LiveNamespaceBinding)> {
        // Reject an invalid request before it can be compared against a durable
        // intent: a bad request must not retire a healthy controller.
        validate_connect_target(target, host_nqn, host_id)?;
        let adopt = match adoption {
            ControllerAdoption::Adopt { .. } => registry.adopts(),
            ControllerAdoption::Recovery { owner_incarnation } => {
                registry.adopts()
                    && registry.recovery_may_adopt(target, host_nqn, host_id, owner_incarnation)?
            }
            ControllerAdoption::Fresh { .. } => false,
        };
        if adopt {
            match registry.try_adopt_controller(target, host_nqn, host_id, deadline) {
                Ok(Some(mut controller)) => match controller.resolve_verified(access, deadline) {
                    Ok(binding) => return Ok((controller, binding)),
                    Err(error) => {
                        let cleanup = controller.disconnect_inner(Instant::now() + CLEANUP_TIMEOUT);
                        if cleanup.is_err() {
                            return Err(connect_error_after_cleanup(error, cleanup));
                        }
                    }
                },
                Ok(None) => {}
                Err(error) => return Err(error),
            }
        } else if !matches!(adoption, ControllerAdoption::Adopt { .. }) {
            // A recovery with a different or unknown owner, or a retry after a
            // verified loss: retire the identity (counting what Adopt would
            // have kept) before the fresh connect.
            registry.retire_identity(target, host_nqn, host_id, deadline)?;
        }
        let mut controller = Self::connect_controller(
            registry,
            target,
            host_nqn,
            host_id,
            adoption.owner_incarnation(),
            deadline,
        )?;
        match controller.resolve_verified(access, deadline) {
            Ok(binding) => Ok((controller, binding)),
            Err(error) => Err(connect_error_after_cleanup(
                error,
                controller.disconnect_inner(Instant::now() + CLEANUP_TIMEOUT),
            )),
        }
    }
    /// Establish and verify the owned controller without resolving a namespace
    /// block device. Write protection runs admin commands on this controller;
    /// only the data path needs the namespace, so a subsystem shared with
    /// another host's controller must not block it.
    pub(super) fn connect_controller(
        registry: &LiveConnectionRegistry,
        target: &LiveConnectTarget,
        host_nqn: &str,
        host_id: [u8; 16],
        owner_incarnation: Option<[u8; 16]>,
        deadline: Instant,
    ) -> io::Result<Self> {
        match Self::connect_controller_once(
            registry,
            target,
            host_nqn,
            host_id,
            owner_incarnation,
            deadline,
        ) {
            Ok(controller) => Ok(controller),
            Err(error) if already_connected(&error) => {
                // One bounded reconcile, then retry once only when the kernel
                // no longer tracks any controller for this identity.
                if let Some(failure) =
                    registry.already_connected_outcome(target, host_nqn, host_id, deadline)?
                {
                    return Err(failure);
                }
                Self::connect_controller_once(
                    registry,
                    target,
                    host_nqn,
                    host_id,
                    owner_incarnation,
                    deadline,
                )
            }
            Err(error) => Err(error),
        }
    }
    fn connect_controller_once(
        registry: &LiveConnectionRegistry,
        target: &LiveConnectTarget,
        host_nqn: &str,
        host_id: [u8; 16],
        owner_incarnation: Option<[u8; 16]>,
        deadline: Instant,
    ) -> io::Result<Self> {
        validate_connect_target(target, host_nqn, host_id)?;
        remaining(deadline)?;
        let intent = Intent {
            target: target.clone(),
            host_nqn: host_nqn.to_owned(),
            host_id,
            owner_incarnation,
        };
        let intent_path = registry.reserve(&intent)?;
        #[cfg(test)]
        if registry.take_after_reserve_loss() {
            // The controller this reserve created is gone. The intent stays
            // for reconciliation, exactly as a `create` verify loss leaves it.
            return Err(controller_lost_error());
        }
        // Kept after the worker takes its own copy: an `-EALREADY` means no
        // controller was created and the durable intent must not block the
        // bounded retry.
        let intent_cleanup = intent_path.clone();
        let worker_registry = registry.clone();
        let target_copy = intent.target;
        let host_nqn = intent.host_nqn;
        let (tx, rx) = mpsc::sync_channel(0);
        std::thread::Builder::new()
            .name("live-nvme-connect".into())
            .spawn(move || {
                #[cfg(test)]
                let result = match worker_registry.pause_worker() {
                    Ok(()) => {
                        Self::create(target_copy, host_nqn, host_id, worker_registry, intent_path)
                    }
                    Err(error) => Err(error),
                };
                #[cfg(not(test))]
                let result =
                    Self::create(target_copy, host_nqn, host_id, worker_registry, intent_path);
                if let Err(mpsc::SendError(Ok(mut controller))) = tx.send(result)
                    && let Err(error) =
                        controller.disconnect_inner(Instant::now() + CLEANUP_TIMEOUT)
                {
                    eprintln!("late NVMe controller cleanup failed; intent quarantined: {error}");
                }
            })?;
        // The worker exists from here on: an expired wait is a worker whose
        // outcome is unknown, not a connect that completed without effect.
        let wait = remaining(deadline).map_err(|error| worker_uncertain_error(error.kind()))?;
        let mut controller = match rx.recv_timeout(wait) {
            Ok(Ok(controller)) => controller,
            Ok(Err(error)) => {
                if already_connected(&error) {
                    registry.remove_intent(&intent_cleanup)?;
                }
                return Err(error);
            }
            // The worker may still create a controller: its intent must stay
            // sealed for reconciliation, not be cleared by a recovery retry.
            Err(mpsc::RecvTimeoutError::Timeout) => {
                return Err(worker_uncertain_error(io::ErrorKind::TimedOut));
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                return Err(worker_uncertain_error(io::ErrorKind::InvalidData));
            }
        };
        match controller.verify_queue_geometry() {
            Ok(()) => Ok(controller),
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
        let keep_alive_tmo = registry.keep_alive_tmo();
        let options = connect_options(&target, &host_nqn, host_id, keep_alive_tmo);
        write_connect_options(&mut control, &options)?;
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
                queue: OnceLock::new(),
                controller_root: PathBuf::from("/sys/class/nvme"),
                udev_queue: PathBuf::from("/run/udev/queue"),
                keep_alive_tmo,
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
    /// What the connect-time geometry check observed on this controller. `None`
    /// before that check, so a fixture controller never reports a geometry.
    pub(crate) fn evidence(&self) -> Option<LiveControllerEvidence> {
        let (queue_count, sqsize) = *self.queue.get()?;
        Some(LiveControllerEvidence {
            queue_count,
            sqsize,
            keep_alive_tmo: self.keep_alive_tmo,
            sysfs_device: self.device,
            sysfs_inode: self.inode,
        })
    }
    /// Whether the pinned controller is verifiably not live: its state file is
    /// not `live`, its sysfs entry vanished or was reused, or its pinned
    /// directory is gone. A controller the kernel is still deleting counts as
    /// lost, because no new I/O can succeed on it.
    ///
    /// This reads sysfs, so callers run it only on an error path, after an
    /// ambiguous errno. An unreadable state is not proof of loss: it keeps the
    /// fatal verdict.
    pub(crate) fn lost(&self) -> bool {
        let pinned = self.pinned();
        if std::fs::metadata(&pinned).is_err() {
            return true;
        }
        match std::fs::metadata(self.controller_root.join(&self.name)) {
            Ok(meta) if meta.ino() == self.inode && meta.dev() == self.device => {}
            // A vanished or renumbered sysfs entry cannot be this controller.
            Ok(_) => return true,
            Err(error) if vanished(&error) => return true,
            Err(_) => return false,
        }
        match attr(&pinned.join("state")) {
            Ok(state) => state != "live",
            Err(error) => vanished(&error),
        }
    }
    fn pinned(&self) -> PathBuf {
        PathBuf::from(format!("/proc/self/fd/{}", self.directory.as_raw_fd()))
    }
    fn verify(&self) -> io::Result<()> {
        // Every failure is typed: a vanished or replaced controller is the
        // resumable lost cause, a live object with other identity facts is a
        // real mismatch. The write and read open paths classify by that cause.
        let read = |path: &Path| attr(path).map_err(lost_if_vanished);
        let p = self.pinned();
        let meta = self.directory.metadata()?;
        if meta.ino() != self.inode || meta.dev() != self.device {
            return Err(controller_lost_error());
        }
        let current = match std::fs::metadata(self.controller_root.join(&self.name)) {
            Ok(current) => current,
            // The name no longer resolves: the pinned controller is gone.
            Err(error) if vanished(&error) => return Err(controller_lost_error()),
            Err(error) => return Err(error),
        };
        if current.ino() != self.inode || current.dev() != self.device {
            // The name now resolves to a replacement, so the pinned object is
            // gone even if the path exists.
            return Err(controller_lost_error());
        }
        if read(&p.join("subsysnqn"))? != self.target.nqn
            || read(&p.join("transport"))? != transport(self.target.transport)
        {
            return Err(identity_error("controller identity changed"));
        }
        if read(&p.join("hostnqn"))? != self.host_nqn
            || read(&p.join("hostid"))? != uuid_string(self.host_id)
        {
            return Err(identity_error("controller host identity changed"));
        }
        let address = read(&p.join("address"))?;
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
            return Err(identity_error("controller endpoint mismatch"));
        }
        Ok(())
    }

    /// Waits, within `deadline`, until the pinned controller reports `live`.
    ///
    /// A controller the kernel is still connecting or resetting cannot serve a
    /// namespace open yet. Its identity is re-verified on every pass, so a
    /// mismatch stays fatal, and a vanished controller stays the typed lost
    /// cause; only a live object with a non-`live` state waits.
    fn wait_live(&self, deadline: Instant) -> io::Result<()> {
        loop {
            remaining(deadline)?;
            self.verify()?;
            match attr(&self.pinned().join("state")) {
                Ok(state) if state == "live" => return Ok(()),
                Ok(_) => {}
                Err(error) if vanished(&error) => return Err(controller_lost_error()),
                Err(error) => return Err(error),
            }
            // The same bounded poll the namespace scan uses: a namespace of a
            // controller that is still connecting may need the connect window.
            std::thread::sleep(remaining(deadline)?.min(Duration::from_millis(20)));
        }
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
            // A leftover controller from a killed process can read live while
            // the kernel still connects its replacement; wait for readiness
            // instead of letting its disk reach the verify and fail fatally.
            self.wait_live(deadline)?;
            let ctrl = std::fs::canonicalize(self.pinned()).map_err(lost_if_vanished)?;
            let mut candidates = Vec::new();
            for (name, p) in owned_visible_disks(Path::new("/sys/class/block"), &ctrl, &self.name)
                .map_err(lost_if_vanished)?
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
        let controller = self.controller_root.join(&self.name);
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
        self.record_queue_geometry(&self.pinned())?;
        self.verify()
    }
    /// Checks the geometry under `path` and keeps what it observed. Recorded
    /// once per controller lifetime: a reconnect creates a new resolver.
    fn record_queue_geometry(&self, path: &Path) -> io::Result<()> {
        let observed = verify_queue_geometry(path, self.resolver.target.geometry)?;
        self.resolver
            .queue
            .set(observed)
            .map_err(|_| invalid("controller queue geometry recorded twice"))
    }
    /// Set and read back namespace write protection on the owned controller.
    /// This is a point-in-time guard under the trusted-initiator assumption.
    pub fn set_read_only(&mut self, nsid: u32, deadline: Instant) -> io::Result<()> {
        if nsid != self.resolver.target.nsid {
            return Err(invalid("namespace id mismatch"));
        }
        let uuid = self.resolver.target.uuid;
        self.set_read_only_admin(nsid, uuid, deadline)
    }
    /// Write-protect another namespace of the same subsystem. The namespace is
    /// identified through this controller's admin queue, never through the
    /// namespace block device: the kernel may group several host controllers
    /// under one subsystem, which is exactly the case write protection fences.
    pub(super) fn set_read_only_member(
        &mut self,
        nsid: u32,
        uuid: [u8; 16],
        deadline: Instant,
    ) -> io::Result<()> {
        if nsid == 0 || uuid == [0; 16] {
            return Err(invalid("invalid namespace identity"));
        }
        self.set_read_only_admin(nsid, uuid, deadline)
    }
    /// Runs the write-protection sequence on the owned controller's admin
    /// queue: the namespace identity proof, Set Features (FID 0x84), and the
    /// identify read-back. The UUID descriptor proves the nsid belongs to the
    /// expected namespace of this subsystem, and the identify write-protect
    /// bit proves the policy is applied.
    fn set_read_only_admin(
        &mut self,
        nsid: u32,
        uuid: [u8; 16],
        deadline: Instant,
    ) -> io::Result<()> {
        let (file, controller_path) = self.open_admin_device(deadline)?;
        write_protect_namespace(
            &mut KernelWriteProtectAdmin {
                file: &file,
                controller_path: &controller_path,
            },
            nsid,
            uuid,
            deadline,
        )?;
        self.verify()
    }
    /// Opens the controller's admin character device and verifies that it is
    /// still the pinned controller of this connection.
    fn open_admin_device(&self, deadline: Instant) -> io::Result<(File, PathBuf)> {
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
        Ok((file, controller_path))
    }
    /// The caller must first close and drain all namespace users.
    pub fn disconnect(&mut self, deadline: Instant) -> io::Result<()> {
        remaining(deadline)?;
        self.disconnect_inner(deadline)
    }
    fn delete_owned(&mut self, deadline: Instant) -> io::Result<()> {
        let controller_path = self.resolver.controller_root.join(&self.resolver.name);
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
            || {
                // A namespace change can leave a probe reading block 0 after
                // the admin command returned. Let udev finish before locking.
                wait_for_udev_idle_at(&resolver.udev_queue, deadline)?;
                resolver.lock_owned_disks(deadline)
            },
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
                queue: OnceLock::new(),
                controller_root: PathBuf::from("/sys/class/nvme"),
                udev_queue: PathBuf::from("/run/udev/queue"),
                keep_alive_tmo: KEEP_ALIVE_TMO_SECS,
            }),
            instance: 0,
            deletion: DeletionState::Gone,
            registry: registry.clone(),
            intent_path: sysfs_stand_in.join("no-intent"),
            teardown_locks: Vec::new(),
        })
    }
}
/// Identify Namespace (`cns` 0) or its Identification Descriptor List (`cns`
/// 0x03) on a controller's admin queue. The nsid is scoped to the subsystem
/// the verified controller is attached to, so no block device or subsystem
/// controller count is involved.
fn identify_namespace(
    file: &File,
    controller_path: &Path,
    nsid: u32,
    cns: u32,
    deadline: Instant,
) -> io::Result<[u8; 4096]> {
    let mut identify = [0u8; 4096];
    let ms = remaining(deadline)?.as_millis().clamp(1, u32::MAX as u128) as u32;
    let mut cmd = NvmeAdminCommand {
        opcode: 0x06,
        nsid,
        addr: identify.as_mut_ptr() as u64,
        data_len: 4096,
        cdw10: cns,
        timeout_ms: ms,
        ..Default::default()
    };
    // SAFETY: the command matches nvme_passthru_cmd, its data_len matches the
    // live 4096-byte identify buffer, and the ioctl is synchronous.
    let status = unsafe { libc::ioctl(file.as_raw_fd(), 0xc0484e41, &mut cmd) };
    if status < 0 {
        return Err(with_context(
            "namespace identify admin command",
            controller_path,
            io::Error::last_os_error(),
        ));
    }
    if status != 0 {
        return Err(invalid("namespace identify failed"));
    }
    Ok(identify)
}

/// The admin queue commands namespace write protection issues. A test double
/// implements it, so the sequence below runs without a kernel controller.
pub(super) trait WriteProtectAdmin {
    /// Identify Namespace Identification Descriptor List (`cns` 0x03) or
    /// Identify Namespace (`cns` 0).
    fn identify(&mut self, nsid: u32, cns: u32, deadline: Instant) -> io::Result<[u8; 4096]>;
    /// Set Features for the namespace: `fid` in CDW10, `value` in CDW11.
    fn set_features(
        &mut self,
        nsid: u32,
        fid: u32,
        value: u32,
        deadline: Instant,
    ) -> io::Result<()>;
}

/// Prove the namespace identity, apply write protection, then prove the device
/// applied it (identify byte 99 bit 0). One body serves the kernel admin queue
/// and its test double, so a wrong order or a missing read-back fails both.
pub(super) fn write_protect_namespace(
    admin: &mut impl WriteProtectAdmin,
    nsid: u32,
    uuid: [u8; 16],
    deadline: Instant,
) -> io::Result<()> {
    let descriptors = admin.identify(nsid, 0x03, deadline)?;
    if namespace_uuid(&descriptors) != Some(uuid) {
        return Err(invalid("namespace identify UUID mismatch"));
    }
    admin.set_features(nsid, 0x84, 1, deadline)?;
    let after = admin.identify(nsid, 0, deadline)?;
    if after[99] & 1 == 0 {
        return Err(invalid("namespace write protection not applied"));
    }
    Ok(())
}

/// The kernel admin queue of one owned controller.
struct KernelWriteProtectAdmin<'a> {
    file: &'a File,
    controller_path: &'a Path,
}

impl WriteProtectAdmin for KernelWriteProtectAdmin<'_> {
    fn identify(&mut self, nsid: u32, cns: u32, deadline: Instant) -> io::Result<[u8; 4096]> {
        identify_namespace(self.file, self.controller_path, nsid, cns, deadline)
    }

    fn set_features(
        &mut self,
        nsid: u32,
        fid: u32,
        value: u32,
        deadline: Instant,
    ) -> io::Result<()> {
        let ms = remaining(deadline)?.as_millis().clamp(1, u32::MAX as u128) as u32;
        let mut cmd = NvmeAdminCommand {
            opcode: 0x09,
            nsid,
            cdw10: fid,
            cdw11: value,
            timeout_ms: ms,
            ..Default::default()
        };
        // SAFETY: the command matches nvme_passthru_cmd and remains valid for the synchronous ioctl.
        let status = unsafe { libc::ioctl(self.file.as_raw_fd(), 0xc0484e41, &mut cmd) };
        if status < 0 {
            return Err(with_context(
                "namespace write-protection admin command",
                self.controller_path,
                io::Error::last_os_error(),
            ));
        }
        if status != 0 {
            return Err(invalid("namespace write protection failed"));
        }
        Ok(())
    }
}

/// The namespace UUID from an Identify Namespace Identification Descriptor
/// List (`cns` 0x03). Linux nvmet reports the configfs `device_uuid` as a
/// 16-byte UUID descriptor; Identify Namespace itself starts with the size and
/// capacity, so it cannot prove the namespace identity.
fn namespace_uuid(descriptors: &[u8]) -> Option<[u8; 16]> {
    let mut rest = descriptors;
    while rest.len() >= 4 {
        let kind = rest[0];
        let len = usize::from(rest[1]);
        if len == 0 || rest.len() < 4 + len {
            return None;
        }
        if kind == 0x03 && len == 16 {
            let mut uuid = [0u8; 16];
            uuid.copy_from_slice(&rest[4..20]);
            return Some(uuid);
        }
        rest = &rest[4 + len..];
    }
    None
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
