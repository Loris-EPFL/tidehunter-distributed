//! Readiness primitives of the durable-progress lane (`Request::DurableProgress`).
//!
//! Design: `checkpoint-work/research/DURABLE-PROGRESS-WAIT-DESIGN-2026-11-18.md`,
//! option A. The coordinator poller and the owner session thread each wait in one
//! `poll(2)` on a fixed set of descriptors instead of repeating empty sweeps:
//!
//! - transport readiness: the session socket on TCP, or the provider CQ wait fd
//!   armed by `fi_trywait` plus the TCP liveness socket on OFI verbs;
//! - local wakes: an eventfd that the owner's durable-progress completion path
//!   signals after it publishes the reply, the listener's shutdown eventfd, or the
//!   coordinator poller's stop eventfd;
//! - the nearest existing deadline.
//!
//! A wake is never evidence of durability. It only makes the session sweep again.
//! No wire message or persisted format changes.

use super::*;
use std::fs::File;
use std::net::TcpStream;
use std::os::fd::{AsRawFd, RawFd};

#[cfg(test)]
use std::sync::atomic::AtomicU64;
#[cfg(test)]
use std::sync::mpsc;

/// Prefix of every error that reports a missing or lost readiness capability on
/// the progress lane. The owner sends it in its close notice (plain text, not a
/// wire field); the client classifies the notice with it, so a coordinator stops
/// selecting a route that cannot wait instead of reconnecting to it.
pub(super) const WAIT_UNAVAILABLE: &str = "durable progress wait unavailable";

/// A nonblocking Linux eventfd used as a coalescing wake. A saturated counter
/// already means readable, so `signal` never blocks and never fails on overflow.
pub(crate) struct WakeFd(File);

impl WakeFd {
    pub(super) fn new() -> io::Result<Self> {
        super::super::sys::eventfd().map(|fd| Self(File::from(fd)))
    }

    /// A wake of the progress lane for `purpose`. A failed allocation (for
    /// example EMFILE) is a missing readiness capability: the lane fails closed
    /// with the named error instead of running without the wake.
    pub(crate) fn for_progress(purpose: &str) -> Result<Self, TcpError> {
        Self::named(Self::new(), purpose)
    }

    pub(super) fn named(created: io::Result<Self>, purpose: &str) -> Result<Self, TcpError> {
        created.map_err(|error| TcpError::wait_unavailable(format!("{purpose} eventfd: {error}")))
    }

    /// Make the fd readable. Idempotent until `consume`.
    pub(crate) fn signal(&self) -> io::Result<()> {
        match (&self.0).write(&1u64.to_ne_bytes()) {
            Ok(_) => Ok(()),
            // The counter is at its maximum: the fd is already readable.
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(()),
            Err(error) => Err(error),
        }
    }

    /// Clear every coalesced signal. Returns whether one was pending.
    pub(super) fn consume(&self) -> io::Result<bool> {
        let mut counter = [0u8; 8];
        match (&self.0).read(&mut counter) {
            Ok(_) => Ok(true),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(false),
            Err(error) => Err(error),
        }
    }

    pub(super) fn raw(&self) -> RawFd {
        self.0.as_raw_fd()
    }
}

/// Wakes the owner session of one durable-progress request after its reply is
/// published. Owner: the request's `ControlWork`, then its hub `Waiter`. It is
/// dropped only after `control::deliver` sent the `WorkCompletion` to the bounded
/// session inbox (or after the reply was abandoned by a panic), so a session that
/// consumes the wake before it harvests the inbox either finds the reply or sees
/// the fd readable again. Every completion path (immediate result, nudge error,
/// worker panic, full or stopped hub, fence or seal retirement, hub stop) ends in
/// that drop. It holds no log reference and never blocks.
pub(super) struct CompletionNotify {
    wake: Arc<WakeFd>,
    #[cfg(test)]
    hook: Option<Arc<WaitHook>>,
}

impl CompletionNotify {
    /// Test hook: the hub registered the poll that owns this notify.
    #[cfg(test)]
    pub(super) fn parked(&self, id: u64) {
        if let Some(hook) = &self.hook {
            hook.send(HookEvent::Parked(id));
        }
    }
}

impl Drop for CompletionNotify {
    fn drop(&mut self) {
        // An eventfd write fails only for a wrong buffer size or a full counter;
        // neither can occur (`signal` maps the full counter to success).
        let _ = self.wake.signal();
    }
}

/// What ended one parked wait. A caller always sweeps again afterwards.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Woke {
    /// Socket or CQ readiness (including peer close or socket error).
    Transport,
    /// The owner's durable-progress completion eventfd.
    Completion,
    /// The listener shutdown or coordinator stop eventfd.
    Stop,
    /// The wait deadline passed.
    Deadline,
    /// EINTR or readiness that the next sweep does not confirm.
    Spurious,
}

#[derive(Clone, Copy)]
enum Role {
    Socket,
    #[cfg(any(test, feature = "experimental-fabric"))]
    Cq,
    Completion,
    Stop,
}

/// CQ fd, liveness socket, completion wake, listener shutdown and database close.
const WAIT_FDS: usize = 5;

/// The fixed descriptor set of one parked wait, on the stack.
pub(super) struct WaitSet {
    fds: [libc::pollfd; WAIT_FDS],
    roles: [Role; WAIT_FDS],
    len: usize,
    deadline: Instant,
    #[cfg(test)]
    hook: Option<(Arc<WaitHook>, u64)>,
}

impl WaitSet {
    pub(super) fn new(deadline: Instant) -> Self {
        Self {
            fds: [libc::pollfd {
                fd: -1,
                events: 0,
                revents: 0,
            }; WAIT_FDS],
            roles: [Role::Socket; WAIT_FDS],
            len: 0,
            deadline,
            #[cfg(test)]
            hook: None,
        }
    }

    /// Test hook: record this wait (outstanding request `id`) in `hook`.
    #[cfg(test)]
    pub(super) fn instrument(&mut self, hook: Option<&Arc<WaitHook>>, id: u64) {
        self.hook = hook.map(|hook| (hook.clone(), id));
    }

    fn add(&mut self, fd: RawFd, role: Role) -> io::Result<()> {
        let (Some(slot), Some(slot_role)) =
            (self.fds.get_mut(self.len), self.roles.get_mut(self.len))
        else {
            return Err(io::Error::other("durable progress wait set is full"));
        };
        *slot = libc::pollfd {
            fd,
            // Read interest only: progress waits only after all output retired.
            events: libc::POLLIN,
            revents: 0,
        };
        *slot_role = role;
        self.len += 1;
        Ok(())
    }

    pub(super) fn socket(&mut self, socket: &TcpStream) -> io::Result<()> {
        self.add(socket.as_raw_fd(), Role::Socket)
    }

    /// A provider CQ wait fd, valid only after `fi_trywait` armed it.
    #[cfg(any(test, feature = "experimental-fabric"))]
    fn cq(&mut self, fd: RawFd) -> io::Result<()> {
        self.add(fd, Role::Cq)
    }

    /// Adds an OFI session's readiness from the result of arming its CQ wait fd
    /// (`Endpoint::arm_wait_fd`): the armed CQ fd plus the TCP control socket,
    /// which reports peer loss while replies travel over OFI. `Ok(false)`: the
    /// provider has queued work or the endpoint is not quiescent; the caller
    /// does not block and returns to its normal progress (no fixed delay).
    /// # Errors
    /// A failed arm: the readiness capability is lost on this route.
    #[cfg(any(test, feature = "experimental-fabric"))]
    pub(super) fn armed_cq(
        &mut self,
        armed: io::Result<Option<RawFd>>,
        liveness: &TcpStream,
    ) -> Result<bool, TcpError> {
        let Some(fd) = armed.map_err(TcpError::wait_unavailable)? else {
            return Ok(false);
        };
        self.cq(fd)?;
        self.socket(liveness)?;
        Ok(true)
    }

    pub(super) fn completion(&mut self, wake: &WakeFd) -> io::Result<()> {
        self.add(wake.raw(), Role::Completion)
    }

    pub(super) fn stop(&mut self, wake: &WakeFd) -> io::Result<()> {
        self.add(wake.raw(), Role::Stop)
    }

    /// One readiness wait. The timeout rounds up to whole milliseconds, so a
    /// timeout return is at or past the deadline and the caller never re-enters
    /// for a sub-millisecond remainder.
    /// # Errors
    /// A failed `poll`, an invalid descriptor, or an error condition on a CQ or
    /// eventfd: the readiness capability is lost. Socket errors and hangups are
    /// `Woke::Transport`, so ordinary progress reports the peer loss.
    pub(super) fn wait(&mut self) -> io::Result<Woke> {
        #[cfg(test)]
        if let Some((hook, id)) = self.hook.take() {
            let woke = match hook.enter(id) {
                Some(scripted) => scripted?,
                None => self.poll_once()?,
            };
            hook.woke(woke);
            return Ok(woke);
        }
        self.poll_once()
    }

    fn poll_once(&mut self) -> io::Result<Woke> {
        let remaining = self.deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Ok(Woke::Deadline);
        }
        let timeout = libc::c_int::try_from(remaining.as_nanos().div_ceil(1_000_000))
            .unwrap_or(libc::c_int::MAX);
        let fds = self
            .fds
            .get_mut(..self.len)
            .ok_or_else(|| io::Error::other("durable progress wait set is invalid"))?;
        let count = libc::nfds_t::try_from(fds.len())
            .map_err(|_| io::Error::other("durable progress wait set is too large"))?;
        // SAFETY: `fds` is an initialized, writable stack array of `count`
        // pollfd entries for the whole call. poll retains neither the pointer nor
        // the descriptors, which their owners keep open during this call.
        let ready = unsafe { libc::poll(fds.as_mut_ptr(), count, timeout) };
        if ready < 0 {
            let error = io::Error::last_os_error();
            return if error.kind() == io::ErrorKind::Interrupted {
                Ok(Woke::Spurious)
            } else {
                Err(error)
            };
        }
        if ready == 0 {
            return Ok(if Instant::now() >= self.deadline {
                Woke::Deadline
            } else {
                Woke::Spurious
            });
        }
        let mut woke = Woke::Spurious;
        for (descriptor, role) in fds.iter().zip(self.roles) {
            if descriptor.revents == 0 {
                continue;
            }
            if descriptor.revents & libc::POLLNVAL != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "durable progress wait descriptor is invalid",
                ));
            }
            let failed = descriptor.revents & libc::POLLERR != 0;
            let next = match role {
                Role::Socket => Woke::Transport,
                #[cfg(any(test, feature = "experimental-fabric"))]
                Role::Cq if failed => {
                    return Err(io::Error::other("fabric completion queue wait fd failed"));
                }
                #[cfg(any(test, feature = "experimental-fabric"))]
                Role::Cq => Woke::Transport,
                Role::Completion | Role::Stop if failed => {
                    return Err(io::Error::other("durable progress wake fd failed"));
                }
                Role::Completion => Woke::Completion,
                Role::Stop => Woke::Stop,
            };
            // Stop wins, then a local completion, then transport readiness.
            woke = match (woke, next) {
                (Woke::Stop, _) | (_, Woke::Stop) => Woke::Stop,
                (Woke::Completion, _) | (_, Woke::Completion) => Woke::Completion,
                _ => next,
            };
        }
        Ok(woke)
    }
}

impl TcpError {
    /// A missing or lost readiness capability on the progress lane.
    pub(crate) fn wait_unavailable(detail: impl fmt::Display) -> Self {
        let mut error = Self::new(format!("{WAIT_UNAVAILABLE}: {detail}"));
        error.wait_unavailable = true;
        error
    }

    /// The owner's close notice, classified by its stable prefix.
    pub(super) fn owner_closed(reason: &str) -> Self {
        let mut error = Self::new(format!("owner closed the session: {reason}"));
        error.wait_unavailable = reason.starts_with(WAIT_UNAVAILABLE);
        error
    }

    /// Whether the progress lane cannot wait without spinning on this route.
    /// A coordinator does not reconnect to such a route.
    pub(crate) fn is_wait_unavailable(&self) -> bool {
        self.wait_unavailable
    }
}

/// Watchdog for scripted waits: only a hang reaches it.
#[cfg(test)]
const SCRIPT_WATCHDOG: Duration = Duration::from_secs(120);

/// Consecutive empty sweeps after which a hook reports a spin. A loop that waits
/// on readiness never has more than one empty sweep before a wait entry.
#[cfg(test)]
pub(super) const SPIN_LIMIT: u64 = 64;

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum HookEvent {
    /// The owner hub registered request `id`.
    Parked(u64),
    /// Wait entry number `entry` while request `id` was outstanding.
    Waiting {
        entry: u64,
        id: u64,
    },
    Woke(Woke),
    /// The owner session harvested the completion of request `id`.
    Completed(u64),
    /// `SPIN_LIMIT` consecutive empty sweeps without a wait entry.
    Spin,
}

/// One scripted readiness result. Each step applies to one wait; later waits
/// use real readiness.
#[cfg(test)]
pub(super) enum ScriptStep {
    Finish(Woke),
    /// The wait fails as `poll` does when the readiness capability is lost.
    Fail(&'static str),
}

#[cfg(test)]
type Script = Arc<Mutex<mpsc::Receiver<ScriptStep>>>;
#[cfg(test)]
type Injection = Box<dyn FnOnce() + Send>;

/// Test-only instrumentation of one side of the progress lane (one client, or
/// every progress session of one node). Each side's loop is single-threaded;
/// counters use Relaxed and tests read them after a channel event, which orders
/// them.
#[cfg(test)]
pub(super) struct WaitHook {
    events: mpsc::SyncSender<HookEvent>,
    lost: AtomicBool,
    readiness_disabled: AtomicBool,
    script: Mutex<Option<Script>>,
    before_poll: Mutex<Option<Injection>>,
    entries: AtomicU64,
    empty_run: AtomicU64,
    repeated: AtomicU64,
}

#[cfg(test)]
impl WaitHook {
    pub(super) fn new() -> (Arc<Self>, mpsc::Receiver<HookEvent>) {
        let (events, received) = mpsc::sync_channel(4096);
        (
            Arc::new(Self {
                events,
                lost: AtomicBool::new(false),
                readiness_disabled: AtomicBool::new(false),
                script: Mutex::new(None),
                before_poll: Mutex::new(None),
                entries: AtomicU64::new(0),
                empty_run: AtomicU64::new(0),
                repeated: AtomicU64::new(0),
            }),
            received,
        )
    }

    fn send(&self, event: HookEvent) {
        if self.events.try_send(event).is_err() {
            self.lost.store(true, Ordering::Relaxed);
        }
    }

    pub(super) fn lost_events(&self) -> bool {
        self.lost.load(Ordering::Relaxed)
    }

    /// Scripted readiness: the next wait blocks on the returned channel.
    pub(super) fn script(&self) -> mpsc::SyncSender<ScriptStep> {
        let (steps, received) = mpsc::sync_channel(16);
        *self.script.lock() = Some(Arc::new(Mutex::new(received)));
        steps
    }

    /// Negative control: the loop keeps its active progress path.
    pub(super) fn disable_readiness(&self) {
        self.readiness_disabled.store(true, Ordering::Relaxed);
    }

    pub(super) fn readiness_enabled(&self) -> bool {
        !self.readiness_disabled.load(Ordering::Relaxed)
    }

    /// Run `inject` once at the next wait entry, immediately before `poll`.
    pub(super) fn inject_before_poll(&self, inject: impl FnOnce() + Send + 'static) {
        *self.before_poll.lock() = Some(Box::new(inject));
    }

    pub(super) fn entries(&self) -> u64 {
        self.entries.load(Ordering::Relaxed)
    }

    /// Empty sweeps that followed another empty sweep with no wait between.
    pub(super) fn repeated_empty_sweeps(&self) -> u64 {
        self.repeated.load(Ordering::Relaxed)
    }

    pub(super) fn sweep(&self, progressed: bool) {
        if progressed {
            self.empty_run.store(0, Ordering::Relaxed);
            return;
        }
        let run = self.empty_run.fetch_add(1, Ordering::Relaxed) + 1;
        if run > 1 {
            self.repeated.fetch_add(1, Ordering::Relaxed);
        }
        if run == SPIN_LIMIT {
            self.send(HookEvent::Spin);
        }
    }

    pub(super) fn completed(&self, id: u64) {
        self.send(HookEvent::Completed(id));
    }

    pub(super) fn woke(&self, woke: Woke) {
        self.send(HookEvent::Woke(woke));
    }

    /// A wait entry. Returns the scripted result in scripted mode.
    pub(super) fn enter(&self, id: u64) -> Option<io::Result<Woke>> {
        self.empty_run.store(0, Ordering::Relaxed);
        let entry = self.entries.fetch_add(1, Ordering::Relaxed) + 1;
        self.send(HookEvent::Waiting { entry, id });
        if let Some(inject) = self.before_poll.lock().take() {
            inject();
        }
        let script = self.script.lock().clone()?;
        let step = script
            .lock()
            .recv_timeout(SCRIPT_WATCHDOG)
            .expect("scripted readiness watchdog: the test hung");
        *self.script.lock() = None;
        Some(match step {
            ScriptStep::Finish(woke) => Ok(woke),
            ScriptStep::Fail(reason) => Err(io::Error::other(reason)),
        })
    }
}

/// Owner side of the wake, kept by the session (lazily) and by its notifies.
pub(super) struct CompletionWake {
    wake: Option<Arc<WakeFd>>,
    #[cfg(test)]
    pub(super) hook: Option<Arc<WaitHook>>,
}

impl CompletionWake {
    pub(super) fn new() -> Self {
        Self {
            wake: None,
            #[cfg(test)]
            hook: None,
        }
    }

    /// The session's eventfd, allocated at its first durable-progress request.
    pub(super) fn fd(&self) -> Option<&WakeFd> {
        self.wake.as_deref()
    }

    pub(super) fn notify(&mut self) -> Result<CompletionNotify, TcpError> {
        let wake = match &self.wake {
            Some(wake) => wake.clone(),
            None => {
                let wake = Arc::new(WakeFd::for_progress("owner completion wake")?);
                self.wake = Some(wake.clone());
                wake
            }
        };
        Ok(CompletionNotify {
            wake,
            #[cfg(test)]
            hook: self.hook.clone(),
        })
    }

    /// Consume the coalesced wake. Call at the start of a sweep, before the
    /// session harvests its completion inbox.
    pub(super) fn consume(&self) -> Result<(), TcpError> {
        if let Some(wake) = &self.wake {
            wake.consume().map_err(TcpError::wait_unavailable)?;
        }
        Ok(())
    }
}

/// Coordinator side: the dedicated progress client's opt-in state.
pub(super) struct ClientProgressWait {
    /// Level-triggered stop wake of the owning poller. Never drained here.
    pub(super) stop: Arc<WakeFd>,
    #[cfg(test)]
    pub(super) hook: Option<Arc<WaitHook>>,
}

impl ClientProgressWait {
    pub(super) fn new(stop: Arc<WakeFd>) -> Self {
        Self {
            stop,
            #[cfg(test)]
            hook: None,
        }
    }
}
