//! Readiness waits of the durable-progress lane on the two reactor loops: the
//! owner session (`serve_session`) and the coordinator's dedicated progress
//! client (`Client::exchange_correlated`). Primitives and the design reference
//! are in `progress_wait`.
//!
//! Each loop keeps its single owner of transport, CQ and frame state; no thread,
//! queue, shared CQ consumer or lock is added. A loop waits only when its sweep
//! made no progress and no local work can need another sweep. General idle
//! handling, data lanes and the Busy/Adaptive policies are unchanged.
use super::super::progress_wait::{CompletionNotify, CompletionWake, WaitSet, WakeFd, Woke};
use super::*;

#[cfg(test)]
#[path = "reactor_wait_tests.rs"]
mod tests;

/// poll(2) timeout for one readiness wait: never past the deadline, and never
/// past the shutdown recheck interval.
pub(super) fn idle_wait_timeout(deadline: Instant, now: Instant) -> libc::c_int {
    // Milliseconds, rounded DOWN: the final sub-millisecond turn is nonblocking.
    deadline
        .saturating_duration_since(now)
        .min(IDLE_SHUTDOWN_CHECK_INTERVAL)
        .as_millis() as libc::c_int
}

/// A control exchange that spent its budget is a typed timeout: an idempotent
/// request may be repeated, and no caller poisons on it.
pub(super) fn correlated_deadline() -> TcpError {
    TcpError::with_kind(io::ErrorKind::TimedOut, "remote operation deadline expired")
}

/// The output-admission budget is spent before the request was sent: nothing
/// entered the output queue, so the typed timeout is retry-safe.
pub(super) fn admission_deadline() -> TcpError {
    TcpError::with_kind(
        io::ErrorKind::TimedOut,
        "control output admission deadline expired",
    )
}

impl Session {
    /// No buffered input or output that a readiness wait could leave unprocessed:
    /// every frame retired (so no write interest), no decrypted plaintext pending
    /// and no observed peer close. A partial frame or TLS record needs more
    /// socket bytes, which is exactly what the wait observes.
    fn wait_quiet(&self) -> bool {
        self.output.is_empty()
            && !self.wants_tls_write()
            && self.plaintext_pending == 0
            && !self.peer_closed
    }

    /// Adds this session's transport readiness to `set`. `Ok(false)`: the OFI
    /// provider has queued work (`fi_trywait` returned `-FI_EAGAIN`) or the
    /// endpoint is not quiescent; the caller returns to its normal progress.
    fn add_transport(&mut self, set: &mut WaitSet) -> Result<bool, TcpError> {
        #[cfg(feature = "experimental-fabric")]
        if let Some(endpoint) = self.sock.fabric.as_mut() {
            // `OwnerProgress::refusal` keeps an owner endpoint without a wait fd
            // out of here; a lost capability (`Unsupported` or a fatal arm) is a
            // transport error of this session.
            return set.armed_cq(endpoint.arm_wait_fd(), &self.sock.socket);
        }
        set.socket(&self.sock.socket)?;
        Ok(true)
    }
}

/// Owner-session state of the durable-progress lane.
pub(super) struct OwnerProgress<'a> {
    /// The listener's level-triggered shutdown wake. Never drained here.
    shutdown: &'a WakeFd,
    close: &'a WakeFd,
    completion: CompletionWake,
    /// The outstanding durable-progress request of this session.
    request: Option<u64>,
}

impl<'a> OwnerProgress<'a> {
    pub(super) fn new(node: &'a StorageNode, shutdown: &'a WakeFd) -> Self {
        #[cfg_attr(not(test), allow(unused_mut))]
        let mut completion = CompletionWake::new();
        #[cfg(test)]
        {
            completion.hook = node.progress_wait_hook.lock().clone();
        }
        Self {
            shutdown,
            close: &node.close_wake,
            completion,
            request: None,
        }
    }

    /// Start of a sweep, before the completion inbox is harvested: a racing
    /// producer is then either harvested or leaves the eventfd readable.
    pub(super) fn begin_sweep(&self) -> Result<(), TcpError> {
        self.completion.consume()
    }

    /// The completion wake for `request` if it is a durable-progress poll.
    pub(super) fn notify(
        &mut self,
        request: &Request,
    ) -> Result<Option<CompletionNotify>, TcpError> {
        if matches!(request, Request::DurableProgress { .. }) {
            self.completion.notify().map(Some)
        } else {
            Ok(None)
        }
    }

    /// A durable-progress poll this session cannot park without spinning: its
    /// OFI endpoint has no CQ wait fd. The session refuses the poll before
    /// control admission with a correlated `Response::Error` (an existing
    /// variant) and stays open, so the coordinator can tell this refusal from
    /// peer loss (`TcpError::is_wait_unavailable`) and stop selecting the route.
    pub(super) fn refusal(stream: &Session, request: &Request) -> Option<String> {
        #[cfg(feature = "experimental-fabric")]
        if matches!(request, Request::DurableProgress { .. })
            && let Some(endpoint) = stream.sock.fabric.as_ref()
            && !endpoint.has_wait_fd()
        {
            let provider = endpoint.provider_info();
            return Some(format!(
                "{}: owner provider {} has no completion queue wait fd ({})",
                super::super::progress_wait::WAIT_UNAVAILABLE,
                provider.provider,
                provider.geometry.cq_wait
            ));
        }
        #[cfg(not(feature = "experimental-fabric"))]
        let _ = (stream, request);
        None
    }

    /// The control owner admitted durable-progress request `id`.
    pub(super) fn submitted(&mut self, id: u64) {
        self.request = Some(id);
    }

    /// The session harvested the completion of request `id`.
    pub(super) fn completed(&mut self, id: u64) {
        if self.request == Some(id) {
            self.request = None;
            #[cfg(test)]
            if let Some(hook) = &self.completion.hook {
                hook.completed(id);
            }
        }
    }

    /// End of a sweep. Returns `true` when the sweep ended in a readiness wait;
    /// `false` keeps the session's ordinary idle or active path (including an
    /// OFI CQ that could not be armed now). `quiet`: no held, deferred, planned
    /// or queued work and exactly one worker in flight.
    pub(super) fn park(
        &mut self,
        stream: &mut Session,
        progressed: bool,
        quiet: bool,
        pending: &HashMap<u64, ServerPending>,
        profile: Option<&mut ConnectionProfile>,
    ) -> Result<bool, TcpError> {
        let Some(id) = self.request else {
            return Ok(false);
        };
        #[cfg(test)]
        if let Some(hook) = &self.completion.hook {
            hook.sweep(progressed);
            if !hook.readiness_enabled() {
                return Ok(false);
            }
        }
        let Some(request) = pending.get(&id) else {
            return Ok(false);
        };
        if progressed
            || !quiet
            || pending.len() != 1
            || request.response_queued
            || request.ready_response.is_some()
            || !stream.wait_quiet()
            // A partial request frame keeps its own I/O deadline on the active
            // path; the lane's client sends nothing while its poll is parked.
            || stream.input_started.is_some()
        {
            return Ok(false);
        }
        let Some(completion) = self.completion.fd() else {
            return Ok(false);
        };
        // The owner deadline backs up the hub's own reply at the poll deadline.
        let mut set = WaitSet::new(request.deadline);
        if !stream.add_transport(&mut set)? {
            return Ok(false);
        }
        set.completion(completion)?;
        set.stop(self.shutdown)?;
        set.stop(self.close)?;
        #[cfg(test)]
        set.instrument(self.completion.hook.as_ref(), id);
        let began = profile.as_ref().map(|_| Instant::now());
        // Every outcome sweeps again: shutdown is the loop condition, a
        // completion is harvested, transport readiness is read, and an expired
        // deadline is reported by the deadline checks.
        if set.wait().map_err(TcpError::wait_unavailable)? == Woke::Transport {
            // An OFI liveness socket is readable only at peer close or on
            // unexpected data; check it now, not after its periodic interval.
            stream.check_control_peer(true)?;
        }
        if let Some(profile) = profile {
            profile.polling.idle_wait_calls += 1;
            profile.polling.idle_wait_ns += began.map_or(0, elapsed_ns);
        }
        Ok(true)
    }
}

impl Client {
    /// Coordinator progress lane: use readiness instead of the active pause
    /// while control request `id` awaits its reply. Returns `true` when this
    /// sweep ended in a wait, `false` when the lane is not
    /// opted in or local work remains. A stop wake ends the exchange with an
    /// error; it never certifies or skips anything.
    pub(super) fn park_progress(&mut self, id: u64) -> Result<bool, TcpError> {
        match self.wait_progress(id) {
            Ok(Some(Woke::Stop)) => {
                self.fail_connection();
                Err(TcpError::new(
                    "durable progress wait stopped: the coordinator poller is stopping",
                ))
            }
            Ok(parked) => Ok(parked.is_some()),
            Err(error) => {
                self.fail_connection();
                Err(error)
            }
        }
    }

    fn wait_progress(&mut self, id: u64) -> Result<Option<Woke>, TcpError> {
        let Some(wait) = self.progress_wait.as_ref() else {
            return Ok(None);
        };
        #[cfg(test)]
        if let Some(hook) = &wait.hook {
            hook.sweep(self.reactor.progressed);
            if !hook.readiness_enabled() {
                return Ok(None);
            }
        }
        let Some((control, deadline)) = self.reactor.control else {
            return Ok(None);
        };
        if control != id
            || self.reactor.progressed
            || self.reactor.control_result.is_some()
            || self.reactor.position.is_some()
            || !self.stream.wait_quiet()
        {
            return Ok(None);
        }
        let mut set = WaitSet::new(deadline);
        if !self.stream.add_transport(&mut set)? {
            return Ok(None);
        }
        set.stop(&wait.stop)?;
        #[cfg(test)]
        set.instrument(wait.hook.as_ref(), id);
        let woke = set.wait().map_err(TcpError::wait_unavailable)?;
        if woke == Woke::Transport {
            self.stream.check_control_peer(true)?;
        }
        Ok(Some(woke))
    }

    /// Test hook: instrument this client's progress-lane waits.
    #[cfg(test)]
    pub(in crate::distributed::tcp) fn instrument_progress_wait(
        &mut self,
        hook: Arc<super::super::progress_wait::WaitHook>,
    ) {
        if let Some(wait) = self.progress_wait.as_mut() {
            wait.hook = Some(hook);
        }
    }
}
