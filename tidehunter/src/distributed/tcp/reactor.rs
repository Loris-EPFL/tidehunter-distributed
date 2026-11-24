//! Exclusive-owner protocol progress. Adapted mechanisms (not copied code):
//! eRPC §§3.1–3.2, 4.2.2, 4.3; erpc-io/eRPC at
//! de83dab3eab4a0fb19bfc4881c11d4a6b89ff17d, src/session.h and
//! src/rpc_impl/{rpc_ev_loop,rpc_resp}.cc. IDs name live slots, not durability.
//! ReFlex §§3.1/4.1: bounded two-stage progress, never disk waits on this owner.
//! HERD §4.1.1: advance ready pipeline work without waiting for another arrival.
//! Page-cache reads and bounded output progress in the same sweep;
//! cold io_uring/worker execution stays off the network owner.
//! Provider contract: fabric endpoints request `FI_PROGRESS_MANUAL` data
//! progress (`tidehunter_fabric` hints), so this owner drives CQ progress;
//! it does not require a TCP syscall per CQ poll.
//! Adaptive TCP parks only a drained owner on socket readability, never on a
//! periodic sleep. An adaptive fabric owner with no RPC pending parks on the
//! provider CQ wait object when the endpoint has one (software RxM, or `verbs`
//! through a wait fd, see `tidehunter_fabric::Endpoint::open_idle_parking`) and
//! otherwise keeps bounded manual progress; busy polling continues while any
//! request is outstanding.
//! Completion and reply submission stay inline; cold I/O stays asynchronous.
//! Sync durability and inventory try-locks for admission and response epoch
//! validation remain. Read admission uses atomics, per-session buffer pools,
//! and a sharded per-device credit lock.
//! https://www.usenix.org/system/files/nsdi19-kalia.pdf
//! ReFlex source: papers/parsed/ReFlex-2017.md, §§3.1 and 4.1.
use super::*;
use std::collections::{HashMap, VecDeque};
use std::net::{Shutdown, TcpStream};
use std::os::fd::AsRawFd;
use std::sync::mpsc;

mod host_session;
#[cfg(test)]
#[path = "reactor_output_tests.rs"]
mod output_tests;
#[cfg(test)]
#[path = "reactor_plain_tests.rs"]
mod plain_tests;
#[path = "reactor_replay_page.rs"]
mod replay_page;
#[path = "reactor_reply.rs"]
mod reply;
#[cfg(test)]
#[path = "reactor_session_tests.rs"]
mod session_tests;
#[path = "reactor_wait.rs"]
mod wait;

pub(crate) use host_session::HostClient;
pub(super) use host_session::serve_host_session;
pub(super) use reply::Envelope;
use reply::{flush_responses, pop_retired, queue_response, request_kind_code, retire_responses};
use wait::idle_wait_timeout;

pub(super) const TLS_BUFFER_BYTES: usize = 256 * 1024;
pub(super) const SESSION_REQUESTS: usize = 32;
pub(super) const SESSION_BYTES: usize = 256 * 1024 * 1024;
const PROGRESS_BYTES: usize = 64 * 1024;

/// A live fabric control socket is checked at least once per millisecond of
/// active progress, plus one bounded sweep/scheduler delay. An idle established
/// session has no silent-partition detection deadline; pending work and partial
/// frames remain bounded by their operation and I/O deadlines.
const CONTROL_PEER_CHECK_INTERVAL: Duration = Duration::from_millis(1);

/// Socket readiness ends the wait; OS scheduling can still add delay. This is
/// the maximum requested wait before checking the atomic shutdown flag again,
/// not an arrival latency floor or a scheduler-latency guarantee.
const IDLE_SHUTDOWN_CHECK_INTERVAL: Duration = Duration::from_millis(10);

/// One allocation-free readiness wait. EINTR returns to the owner so shutdown
/// and deadlines are rechecked; EOF/errors are handled by ordinary TLS progress.
fn wait_for_readable(socket: &TcpStream, deadline: Instant) -> io::Result<bool> {
    let mut descriptor = libc::pollfd {
        fd: socket.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    let timeout = idle_wait_timeout(deadline, Instant::now());
    // SAFETY: one initialized stack pollfd is writable for this call, and the
    // borrowed socket keeps its fd live. poll retains neither pointer nor fd.
    // std has no equivalent readiness wait; libc is already a dependency.
    let ready = unsafe { libc::poll(&mut descriptor, 1, timeout) };
    if ready < 0 {
        let error = io::Error::last_os_error();
        return if error.kind() == io::ErrorKind::Interrupted {
            Ok(false)
        } else {
            Err(error)
        };
    }
    if descriptor.revents & libc::POLLNVAL != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "idle session socket is invalid",
        ));
    }
    Ok(ready != 0)
}

struct ControlPeerHealth {
    next_check: Instant,
}
impl ControlPeerHealth {
    fn check(&mut self, socket: &TcpStream, now: Instant, force: bool) -> io::Result<bool> {
        if !force && now < self.next_check {
            return Ok(false);
        }
        match socket.peek(&mut [0]) {
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
            Err(error) => return Err(error),
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "fabric control peer closed",
                ));
            }
            Ok(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unexpected fabric control data",
                ));
            }
        }
        self.next_check = now + CONTROL_PEER_CHECK_INTERVAL;
        Ok(true)
    }
}

/// Scheduling policy affects only this bounded network-owner sweep, not locks
/// in the storage engine, TLS, provider or OS.
#[derive(Default)]
pub(super) struct ActivePoll {
    spins: u8,
}
impl ActivePoll {
    /// Returns (scheduler yield, spin hint) for opt-in diagnostics.
    pub(super) fn pause(
        &mut self,
        policy: PollingPolicy,
        fabric: bool,
        progressed: bool,
    ) -> (bool, bool) {
        if progressed {
            self.spins = 0;
            (false, false)
        } else if policy == PollingPolicy::Busy {
            std::hint::spin_loop();
            (false, true)
        } else if fabric && self.spins < 64 {
            self.spins += 1;
            std::hint::spin_loop();
            (false, true)
        } else {
            self.spins = 0;
            thread::yield_now();
            (true, false)
        }
    }
}

struct Output {
    id: u64,
    bytes: Vec<u8>,
    offset: usize,
    len: usize,
    retained_bytes: usize,
    fence: Option<u64>,
    /// Opt-in phase clock: offset zero = queued, no fence = submitting,
    /// fence present = awaiting local retirement. Reset never changes ownership.
    stage_started: Option<Instant>,
}

impl Output {
    #[cfg(test)]
    fn flatten(&self) -> Vec<u8> {
        self.bytes.clone()
    }
}

const OUTPUT_FRAME_BUDGET: usize = 4;
const OUTPUT_TLS_WRITE_BUDGET: usize = 8;

#[derive(Clone, Copy, Debug)]
struct OutputBudget {
    plaintext: usize,
    tls_writes: usize,
    frames: usize,
    frame_gate_observed: bool,
}

impl OutputBudget {
    fn reset(&mut self) {
        self.plaintext = PROGRESS_BYTES;
        self.tls_writes = OUTPUT_TLS_WRITE_BUDGET;
        self.frames = OUTPUT_FRAME_BUDGET;
        self.frame_gate_observed = false;
    }
}

/// Framing and optional TLS state have one mutable owner. Endpoint's fixed
/// slots own DMA-visible copies; request/result vectors never are DMA targets.
pub(super) struct Session {
    conn: Option<rustls::Connection>,
    pub(super) sock: DeadlineIo,
    output: VecDeque<Output>,
    /// Prefix already accepted by transport, still charged until fence retirement.
    submitted: usize,
    output_budget: OutputBudget,
    closed: bool,
    stats: TransportProgressStats,
    time_work: bool,
    output_bytes: usize,
    spare_output: Vec<u8>,
    header: [u8; 4],
    header_used: usize,
    input: Vec<u8>,
    input_used: usize,
    input_len: usize,
    input_started: Option<Instant>,
    plaintext_pending: usize,
    peer_closed: bool,
    control_peer: ControlPeerHealth,
    /// Server sessions publish their send-path counters to the owner-wide
    /// aggregate; client sessions have none.
    #[cfg(feature = "experimental-fabric")]
    owner_transport: Option<super::owner_transport::OwnerTransportSink>,
}
impl Session {
    /// One owner for every constructor so the transports cannot drift apart.
    fn from_parts(
        conn: Option<rustls::Connection>,
        sock: DeadlineIo,
        plaintext_pending: usize,
        peer_closed: bool,
    ) -> Self {
        Self {
            conn,
            sock,
            output: VecDeque::new(),
            submitted: 0,
            output_budget: OutputBudget {
                plaintext: PROGRESS_BYTES,
                tls_writes: OUTPUT_TLS_WRITE_BUDGET,
                frames: OUTPUT_FRAME_BUDGET,
                frame_gate_observed: false,
            },
            closed: false,
            stats: TransportProgressStats::default(),
            time_work: false,
            output_bytes: 0,
            header: [0; 4],
            header_used: 0,
            input: Vec::new(),
            input_used: 0,
            input_len: 0,
            input_started: None,
            plaintext_pending,
            peer_closed,
            spare_output: Vec::new(),
            control_peer: ControlPeerHealth {
                next_check: Instant::now(),
            },
            #[cfg(feature = "experimental-fabric")]
            owner_transport: None,
        }
    }
    pub(super) fn new(mut conn: rustls::Connection, sock: DeadlineIo) -> Result<Self, TcpError> {
        sock.socket.set_nonblocking(true)?;
        let tls_state = conn.process_new_packets().map_err(TcpError::component)?;
        Ok(Self::from_parts(
            Some(conn),
            sock,
            tls_state.plaintext_bytes_to_read(),
            tls_state.peer_has_closed(),
        ))
    }
    #[cfg(feature = "experimental-fabric")]
    pub(super) fn new_native(sock: DeadlineIo) -> Result<Self, TcpError> {
        if sock.fabric.is_none() {
            return Err(TcpError::new("native session requires a fabric endpoint"));
        }
        sock.socket.set_nonblocking(true)?;
        Ok(Self::from_parts(None, sock, 0, false))
    }
    /// Trusted-network TCP: frames cross the socket with no TLS, exactly as the
    /// trusted OFI session carries them with no TLS over the fabric.
    pub(super) fn new_plain(sock: DeadlineIo) -> Result<Self, TcpError> {
        #[cfg(feature = "experimental-fabric")]
        if sock.fabric.is_some() {
            return Err(TcpError::new(
                "plain TCP session cannot own a fabric endpoint",
            ));
        }
        sock.socket.set_nonblocking(true)?;
        Ok(Self::from_parts(None, sock, 0, false))
    }
    fn queue<T: Serialize>(&mut self, id: u64, body: &T, limit: usize) -> Result<(), TcpError> {
        let began = self.time_work.then(Instant::now);
        let queued = self.queue_inner(id, body, limit);
        self.stats.frame_encode.record(began, &queued);
        queued.map(|_| ())
    }

    fn queue_response(
        &mut self,
        id: u64,
        response: Response,
        limit: usize,
    ) -> Result<(), TcpError> {
        self.queue(id, &response, limit)
    }
    fn queue_size<T: Serialize>(&self, id: u64, body: &T, limit: usize) -> Result<usize, TcpError> {
        if self.closed {
            return Err(TcpError::new("session is closed"));
        }
        let message = Envelope {
            protocol: PROTOCOL,
            id,
            body,
        };
        let size = wire_codec(limit)
            .serialized_size(&message)
            .map_err(TcpError::component)? as usize;
        if size == 0 || size > limit {
            return Err(TcpError::new("frame length exceeds limit"));
        }
        Ok(size)
    }
    /// Whether `retained` more bytes of frame allocation fit the session budget.
    /// Every admission uses this one rule, against allocation capacity and never
    /// against logical length, so `output_bytes` cannot pass `SESSION_BYTES`.
    fn output_fits(&self, retained: usize) -> bool {
        retained <= SESSION_BYTES.saturating_sub(self.output_bytes)
    }
    fn can_queue(&self, size: usize) -> bool {
        self.output.len() < SESSION_REQUESTS
            && self.output_fits(self.spare_output.capacity().max(size.saturating_add(4)))
    }
    /// Queues one frame and returns its length, for the encode statistics.
    fn queue_inner<T: Serialize>(
        &mut self,
        id: u64,
        body: &T,
        limit: usize,
    ) -> Result<usize, TcpError> {
        if self.closed {
            return Err(TcpError::new("session is closed"));
        }
        let message = Envelope {
            protocol: PROTOCOL,
            id,
            body,
        };
        let size = wire_codec(limit)
            .serialized_size(&message)
            .map_err(TcpError::component)? as usize;
        if size == 0 || size > limit {
            return Err(TcpError::new("frame length exceeds limit"));
        }
        let retained = self.spare_output.capacity().max(size + 4);
        if self.output.len() == SESSION_REQUESTS || !self.output_fits(retained) {
            return Err(TcpError::new("session output credit exceeded"));
        }
        let mut bytes = std::mem::take(&mut self.spare_output);
        encode_sized_frame_into(&message, limit, size, &mut bytes)?;
        let len = bytes.len();
        // The allocator may return more than the exact request that the check
        // above assumed. Charge, and re-check, the capacity actually retained;
        // an over-allocation is dropped here, before it joins the queue.
        let retained = bytes.capacity();
        if !self.output_fits(retained) {
            return Err(TcpError::new("session output credit exceeded"));
        }
        self.output_bytes += retained;
        self.output.push_back(Output {
            id,
            bytes,
            len,
            retained_bytes: retained,
            offset: 0,
            fence: None,
            stage_started: self.time_work.then(Instant::now),
        });
        self.stats.peak_output_frames = self.stats.peak_output_frames.max(self.output.len() as u64);
        self.stats.peak_output_bytes = self.stats.peak_output_bytes.max(self.output_bytes as u64);
        Ok(len)
    }
    /// Tells a TCP peer why this side ends the session before `abort` closes it:
    /// one unsolicited `Response::Error` frame with ID 0. Best effort and never
    /// blocking: if the frame does not fit the output credit or the socket buffer
    /// is full, the peer sees only the close. Fabric sessions skip it, because
    /// `abort` releases provider ownership without waiting for TX completions.
    fn send_close_notice(&mut self, message: &str, limit: usize) {
        if self.closed || self.is_fabric() {
            return;
        }
        let notice = Response::Error(message.into());
        if !self
            .queue_size(0, &notice, limit)
            .is_ok_and(|size| self.can_queue(size))
            || self.queue(0, &notice, limit).is_err()
        {
            return;
        }
        // Every turn that reports progress wrote or retired bytes of a finite
        // queue; a turn without progress (drained or WouldBlock) ends the drain.
        loop {
            self.output_budget.reset();
            if !matches!(self.progress_output(), Ok(true)) {
                break;
            }
        }
    }
    /// TLS close_notify on a TCP-carried session, so the peer can tell a
    /// deliberate close from a truncated connection. One nonblocking drain of
    /// rustls's buffered records; a full socket buffer drops the alert and the
    /// peer then reports a close without close_notify.
    fn send_close_notify(&mut self) {
        if self.closed || self.is_fabric() {
            return;
        }
        let Some(conn) = self.conn.as_mut() else {
            return;
        };
        conn.send_close_notify();
        while conn.wants_write() {
            match conn.write_tls(&mut NonblockingIo(&mut self.sock)) {
                Ok(written) if written != 0 => {}
                _ => break,
            }
        }
    }
    fn abort(&mut self) {
        self.send_close_notify();
        #[cfg(feature = "experimental-fabric")]
        self.flush_owner_transport();
        self.stats = self.transport_stats();
        self.closed = true;
        let _ = self.sock.socket.shutdown(Shutdown::Both);
        // Endpoint::drop closes before releasing registrations/slots. If close
        // cannot prove DMA quiescence it retains them, never frees live memory.
        #[cfg(feature = "experimental-fabric")]
        self.sock.fabric.take();
        self.output.clear();
        self.output_bytes = 0;
        self.submitted = 0;
        self.stats.retained_reply_credit_bytes = 0;
        self.input.clear();
    }
    #[cfg(feature = "experimental-fabric")]
    fn write_native(&mut self, budget: usize) -> Result<usize, TcpError> {
        if budget == 0 {
            return Ok(0);
        }
        let Some(output) = self.output.get(self.submitted) else {
            return Ok(0);
        };
        if output.offset == output.len {
            return Ok(0);
        }
        let start = output.offset;
        let cap = budget.min(PROGRESS_BYTES);
        let len = (output.len - start).min(cap);
        let slices = [io::IoSlice::new(&output.bytes[start..start + len])];
        let began = self.time_work.then(Instant::now);
        let result = self
            .sock
            .fabric
            .as_mut()
            .ok_or_else(|| TcpError::new("native session lost its fabric endpoint"))?
            .try_send_vectored_no_poll(&slices);
        self.stats.plaintext_write.record(began, &result);
        match result {
            Ok(n) => {
                let output = &mut self.output[self.submitted];
                if n != 0
                    && start == 0
                    && let Some(queued) = output.stage_started
                    && let Some(started) = began
                {
                    self.stats.output_queue_ns = self.stats.output_queue_ns.saturating_add(
                        started
                            .duration_since(queued)
                            .as_nanos()
                            .min(u64::MAX as u128) as u64,
                    );
                    self.stats.output_queue_samples =
                        self.stats.output_queue_samples.saturating_add(1);
                    output.stage_started = Some(started);
                }
                output.offset += n;
                Ok(n)
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(0),
            Err(error) => Err(error.into()),
        }
    }
    fn write_plaintext(&mut self, budget: usize) -> Result<usize, TcpError> {
        #[cfg(feature = "experimental-fabric")]
        if self.conn.is_none() && self.is_fabric() {
            return self.write_native(budget);
        }
        if budget == 0 {
            return Ok(0);
        }
        let Some(output) = self.output.get_mut(self.submitted) else {
            return Ok(0);
        };
        if output.offset == output.bytes.len() {
            return Ok(0);
        }
        let end = output.bytes.len().min(
            output
                .offset
                .saturating_add(budget)
                .min(output.offset + PROGRESS_BYTES),
        );
        let began = self.time_work.then(Instant::now);
        let chunk = &output.bytes[output.offset..end];
        // TLS sessions encrypt into rustls; a trusted-network TCP session has no
        // connection and hands the same plaintext frame bytes to the socket.
        let result = match self.conn.as_mut() {
            Some(conn) => conn.writer().write(chunk),
            None => NonblockingIo(&mut self.sock).write(chunk),
        };
        self.stats.plaintext_write.record(began, &result);
        match result {
            Ok(n) => {
                if n != 0
                    && output.offset == 0
                    && let Some(queued) = output.stage_started
                    && let Some(started) = began
                {
                    self.stats.output_queue_ns = self.stats.output_queue_ns.saturating_add(
                        started
                            .duration_since(queued)
                            .as_nanos()
                            .min(u64::MAX as u128) as u64,
                    );
                    self.stats.output_queue_samples =
                        self.stats.output_queue_samples.saturating_add(1);
                    output.stage_started = Some(started);
                }
                output.offset += n;
                Ok(n)
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => Ok(0),
            Err(e) => Err(e.into()),
        }
    }
    fn wants_tls_write(&self) -> bool {
        self.conn.as_ref().is_some_and(|conn| conn.wants_write())
    }
    fn is_fabric(&self) -> bool {
        #[cfg(feature = "experimental-fabric")]
        return self.sock.fabric.is_some();
        #[cfg(not(feature = "experimental-fabric"))]
        false
    }
    fn supports_fabric_idle_wait(&self) -> bool {
        #[cfg(feature = "experimental-fabric")]
        return self
            .sock
            .fabric
            .as_ref()
            .is_some_and(tidehunter_fabric::Endpoint::supports_idle_wait);
        #[cfg(not(feature = "experimental-fabric"))]
        false
    }
    /// Pending work includes accepted requests, deferred admission, workers and
    /// io_uring reads. None can rely on socket readability for its next event.
    /// Once Status initializes the authenticated session, idle TCP waits remain
    /// bounded for shutdown/peer-death checks instead of expiring the session.
    /// TLS/output queues must also be drained before a read-only wait is safe.
    fn idle_wait_deadline(
        &self,
        limits: TcpLimits,
        progressed: bool,
        work_pending: bool,
        initialized: bool,
        idle_since: Instant,
    ) -> Option<Instant> {
        if limits.polling != PollingPolicy::Adaptive
            || self.is_fabric()
                && (!initialized
                    || self.input_started.is_some()
                    || !self.supports_fabric_idle_wait())
            || progressed
            || work_pending
            || !self.output.is_empty()
            || self.wants_tls_write()
            || self.plaintext_pending != 0
            || self.peer_closed
        {
            return None;
        }
        let idle_deadline = if initialized {
            // The software provider caps its own CQ wait at one millisecond.
            // Keep enough permission for that full, deadline-rounded wait.
            Instant::now() + IDLE_SHUTDOWN_CHECK_INTERVAL
        } else {
            idle_since + limits.io_timeout
        };
        Some(self.input_started.map_or(idle_deadline, |began| {
            idle_deadline.min(began + limits.io_timeout)
        }))
    }
    fn check_control_peer(&mut self, force: bool) -> Result<(), TcpError> {
        if self.is_fabric()
            && self
                .control_peer
                .check(&self.sock.socket, Instant::now(), force)?
        {
            self.stats.control_peer_checks = self.stats.control_peer_checks.saturating_add(1);
        }
        Ok(())
    }
    /// One provider CQ sweep shared by TLS transmit, receive and fence
    /// retirement. Neither no-poll data adapter performs extra CQ reads.
    fn progress_fabric_data(
        &mut self,
        idle_deadline: Option<Instant>,
    ) -> Result<(bool, Option<u64>), TcpError> {
        #[cfg(feature = "experimental-fabric")]
        if let Some(endpoint) = &mut self.sock.fabric {
            let began = (idle_deadline.is_some() && self.time_work).then(Instant::now);
            return match endpoint.try_progress_for_sweep(idle_deadline) {
                Ok((changed, waited)) => Ok((changed, waited.then(|| began.map_or(0, elapsed_ns)))),
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok((false, None)),
                Err(error) => Err(error.into()),
            };
        }
        #[cfg(not(feature = "experimental-fabric"))]
        let _ = idle_deadline;
        Ok((false, None))
    }
    fn fabric_receive_ready(&mut self) -> Result<bool, TcpError> {
        #[cfg(feature = "experimental-fabric")]
        if let Some(endpoint) = &mut self.sock.fabric {
            return Ok(endpoint.receive_ready()?);
        }
        Ok(true)
    }
    /// One bounded output turn, without another CQ poll or empty transport read.
    /// A newly ready reply can use this turn immediately in its dispatch sweep.
    fn progress_output(&mut self) -> Result<bool, TcpError> {
        if self.closed {
            return Err(TcpError::new("session is closed"));
        }
        let mut changed = false;
        loop {
            if self.output_budget.frames == 0 {
                break;
            }
            let plaintext = self.write_plaintext(self.output_budget.plaintext)?;
            self.output_budget.plaintext -= plaintext;
            changed |= plaintext != 0;

            let mut transport_progress = false;
            let mut transport_blocked = false;
            if let Some(conn) = self.conn.as_mut()
                && conn.wants_write()
                && self.output_budget.tls_writes != 0
            {
                self.output_budget.tls_writes -= 1;
                let began = self.time_work.then(Instant::now);
                let result = conn.write_tls(&mut NonblockingIo(&mut self.sock));
                self.stats.tls_write.record(began, &result);
                match result {
                    Ok(n) => {
                        transport_progress = n != 0;
                        transport_blocked = n == 0;
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                        transport_blocked = true;
                    }
                    Err(e) => return Err(e.into()),
                }
                changed |= transport_progress;
            }
            // A fence covers accepted ciphertext, never plaintext still in rustls.
            // This turn shares the sweep's single CQ acquisition and budget.
            #[cfg(feature = "experimental-fabric")]
            let (accepted, completed) = if let Some(endpoint) = &mut self.sock.fabric {
                (
                    endpoint.accepted_send_fence()?,
                    endpoint.completed_send_fence()?,
                )
            } else {
                (0, 0)
            };
            #[cfg(not(feature = "experimental-fabric"))]
            let (accepted, completed) = (0, 0);
            let submitted_before = self.stats.frames_submitted;
            changed |= self.finish_output(accepted, completed);
            let submitted_now =
                usize::try_from(self.stats.frames_submitted.saturating_sub(submitted_before))
                    .unwrap_or(usize::MAX);
            self.output_budget.frames = self.output_budget.frames.saturating_sub(submitted_now);
            if transport_blocked {
                break;
            }

            if plaintext == 0 && !transport_progress && submitted_now == 0 {
                break;
            }
            if self.output_budget.plaintext == 0
                || self.output_budget.tls_writes == 0 && self.wants_tls_write()
                || self.output_budget.frames == 0
            {
                break;
            }
        }
        Ok(changed)
    }
    fn progress(
        &mut self,
        idle_deadline: Option<Instant>,
    ) -> Result<(bool, Option<u64>), TcpError> {
        if self.closed {
            return Err(TcpError::new("session is closed"));
        }
        self.output_budget.reset();
        self.check_control_peer(false)?;
        let (mut changed, idle_wait_ns) = self.progress_fabric_data(idle_deadline)?;
        changed |= self.progress_output()?;
        if self.conn.as_ref().is_some_and(|conn| conn.wants_read())
            && self.plaintext_pending == 0
            && !self.peer_closed
            && (!self.is_fabric() || self.fabric_receive_ready()?)
        {
            let began = self.time_work.then(Instant::now);
            let conn = self
                .conn
                .as_mut()
                .ok_or_else(|| TcpError::new("TLS session lost its rustls connection"))?;
            let result = conn.read_tls(&mut NonblockingIo(&mut self.sock));
            self.stats.tls_read.record(began, &result);
            match result {
                Ok(0) => {
                    return Err(TcpError::new(
                        "TLS session truncated: peer closed the connection without close_notify",
                    ));
                }
                Ok(n) => {
                    let began = self.time_work.then(Instant::now);
                    let result = conn.process_new_packets();
                    self.stats
                        .tls_process
                        .record(began, &result.as_ref().map(|_| n));
                    let state = result.map_err(TcpError::component)?;
                    self.plaintext_pending = state.plaintext_bytes_to_read();
                    self.peer_closed = state.peer_has_closed();
                    self.input_started.get_or_insert_with(Instant::now);
                    changed = true;
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                Err(e) => return Err(e.into()),
            }
        }
        Ok((changed, idle_wait_ns))
    }
    /// Keep submission separate from retirement. Native and TLS output frames
    /// share the same bounded provider ownership and contiguous completion rules.
    fn finish_output(&mut self, accepted: u64, completed: u64) -> bool {
        if !self.output_budget.frame_gate_observed
            && self.output.len() > self.submitted + 1
            && self.wants_tls_write()
        {
            // Only the active frame's pending TLS ciphertext blocks the next
            // plaintext frame here; CQ retirement is not a submission gate.
            self.stats.frame_gate_stalls = self.stats.frame_gate_stalls.saturating_add(1);
            self.output_budget.frame_gate_observed = true;
        }
        let mut changed = false;
        let mut transition_time = None;
        if !self.wants_tls_write()
            && let Some(output) = self.output.get_mut(self.submitted)
            && output.offset == output.len
        {
            output.fence = Some(accepted);
            if let Some(started) = output.stage_started {
                let now = *transition_time.get_or_insert_with(Instant::now);
                self.stats.output_submit_ns = self.stats.output_submit_ns.saturating_add(
                    now.duration_since(started).as_nanos().min(u64::MAX as u128) as u64,
                );
                self.stats.output_submit_samples =
                    self.stats.output_submit_samples.saturating_add(1);
                output.stage_started = Some(now);
            }
            self.submitted += 1;
            self.stats.frames_submitted = self.stats.frames_submitted.saturating_add(1);
            changed = true;
        }
        while let Some(output) = pop_retired(&mut self.output, completed) {
            if let Some(submitted) = output.stage_started {
                let now = *transition_time.get_or_insert_with(Instant::now);
                self.stats.output_retire_ns = self.stats.output_retire_ns.saturating_add(
                    now.duration_since(submitted)
                        .as_nanos()
                        .min(u64::MAX as u128) as u64,
                );
                self.stats.output_retire_samples =
                    self.stats.output_retire_samples.saturating_add(1);
            }
            self.submitted -= 1;
            self.output_bytes -= output.retained_bytes;
            if output.bytes.capacity() <= 1024 * 1024
                && output.bytes.capacity() > self.spare_output.capacity()
            {
                self.spare_output = output.bytes;
                self.spare_output.clear();
            }
            self.stats.frames_retired = self.stats.frames_retired.saturating_add(1);
            changed = true;
        }
        if self.submitted != 0 {
            self.stats.retirement_stalls = self.stats.retirement_stalls.saturating_add(1);
        }
        changed
    }
    fn transport_stats(&self) -> TransportProgressStats {
        let mut stats = self.stats;
        stats.output_frames = self.output.len() as u64;
        stats.output_bytes = self.output_bytes as u64;
        stats.awaiting_retirement_frames = self.submitted as u64;
        #[cfg(feature = "experimental-fabric")]
        if let Some(endpoint) = &self.sock.fabric {
            stats.set_provider(endpoint.progress_stats());
        }
        stats
    }
    /// Server sessions only: registers this session's provider counters with
    /// the owner-wide aggregate.
    #[cfg(feature = "experimental-fabric")]
    pub(super) fn attach_owner_transport(
        &mut self,
        counters: &std::sync::Arc<super::OwnerTransportCounters>,
    ) {
        self.owner_transport = Some(counters.attach());
    }
    /// Publishes counter growth since the last flush. Call before the endpoint
    /// counters are reset or the endpoint is dropped.
    #[cfg(feature = "experimental-fabric")]
    pub(super) fn flush_owner_transport(&mut self) {
        if let (Some(sink), Some(endpoint)) = (&mut self.owner_transport, &self.sock.fabric) {
            sink.flush(super::OwnerTransportStats::from_progress(
                &endpoint.progress_stats(),
            ));
        }
    }
    pub(super) fn configure_transport_stats(&mut self, enabled: bool, reset: bool) {
        self.time_work = enabled;
        if reset || !enabled {
            // Never attribute a frame crossing a reset/disable to the new window.
            // Ownership, offsets and completion fences are left untouched.
            for output in &mut self.output {
                output.stage_started = None;
            }
        }
        // Publish what the endpoint counted before its counters restart at zero.
        #[cfg(feature = "experimental-fabric")]
        if reset {
            self.flush_owner_transport();
        }
        #[cfg(feature = "experimental-fabric")]
        if let Some(endpoint) = &mut self.sock.fabric {
            endpoint.configure_progress_stats(enabled, reset);
        }
        #[cfg(feature = "experimental-fabric")]
        if reset && let Some(sink) = &mut self.owner_transport {
            sink.rebase();
        }
        if reset {
            self.stats = TransportProgressStats {
                peak_output_frames: self.output.len() as u64,
                peak_output_bytes: self.output_bytes as u64,
                retained_reply_credit_bytes: self.stats.retained_reply_credit_bytes,
                peak_retained_reply_credit_bytes: self.stats.retained_reply_credit_bytes,
                ..TransportProgressStats::default()
            };
        }
    }
    fn read_transport(
        conn: &mut Option<rustls::Connection>,
        sock: &mut DeadlineIo,
        bytes: &mut [u8],
    ) -> io::Result<usize> {
        if let Some(conn) = conn {
            return conn.reader().read(bytes);
        }
        #[cfg(feature = "experimental-fabric")]
        if let Some(endpoint) = sock.fabric.as_mut() {
            return endpoint.try_receive_no_poll(bytes);
        }
        // Trusted-network TCP: the nonblocking socket is the plaintext transport.
        NonblockingIo(sock).read(bytes)
    }
    fn recycle_input(&mut self, bytes: Vec<u8>) {
        if bytes.capacity() <= 1024 * 1024 {
            self.input = bytes;
        }
    }
    fn consume_plaintext(&mut self, bytes: usize) -> Result<(), TcpError> {
        self.plaintext_pending = self
            .plaintext_pending
            .checked_sub(bytes)
            .ok_or_else(|| TcpError::new("rustls plaintext readiness underflow"))?;
        Ok(())
    }
    /// The peer ended the session at a frame boundary: TLS close_notify with all
    /// plaintext consumed, or plain-TCP EOF. No request or reply was cut short.
    fn peer_finished(&self) -> bool {
        self.peer_closed
            && self.plaintext_pending == 0
            && self.header_used == 0
            && self.input_used == 0
    }
    fn peer_close_error(&self) -> TcpError {
        TcpError::new(match (self.peer_finished(), self.conn.is_some()) {
            (true, true) => "peer closed the session (TLS close_notify)",
            (true, false) => "peer closed the connection",
            (false, _) => "truncated transport frame: peer closed the session inside a frame",
        })
    }
    fn receive(&mut self, limit: usize) -> Result<Option<(Vec<u8>, usize)>, TcpError> {
        if self.conn.is_some() && self.plaintext_pending == 0 {
            return if self.peer_closed {
                Err(self.peer_close_error())
            } else {
                Ok(None)
            };
        }
        if self.header_used < 4 {
            let began = self.time_work.then(Instant::now);
            let result = Self::read_transport(
                &mut self.conn,
                &mut self.sock,
                &mut self.header[self.header_used..],
            );
            self.stats.plaintext_read.record(began, &result);
            match result {
                // Plain TCP has no close_notify; EOF before a header is its clean close.
                Ok(0) if self.conn.is_none() && !self.is_fabric() && self.header_used == 0 => {
                    self.peer_closed = true;
                    return Err(self.peer_close_error());
                }
                Ok(0) => return Err(TcpError::new("truncated transport frame")),
                Ok(n) => {
                    if self.conn.is_some() {
                        self.consume_plaintext(n)?;
                    }
                    self.input_started.get_or_insert_with(Instant::now);
                    self.header_used += n;
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(None),
                Err(e) => return Err(e.into()),
            }
            if self.header_used != 4 {
                return Ok(None);
            }
            let len = u32::from_be_bytes(self.header) as usize;
            if len < 12 || len > limit {
                return Err(TcpError::new("frame length exceeds limit"));
            }
            if self.input.len() < len {
                let began = self.time_work.then(Instant::now);
                self.input.resize(len, 0);
                self.stats
                    .plaintext_buffer_resize
                    .record(began, &Ok::<_, TcpError>(len));
            }
            self.input_len = len;
        }
        if self.conn.is_some() && self.plaintext_pending == 0 {
            return if self.peer_closed {
                Err(self.peer_close_error())
            } else {
                Ok(None)
            };
        }
        let began = self.time_work.then(Instant::now);
        let result = Self::read_transport(
            &mut self.conn,
            &mut self.sock,
            &mut self.input[self.input_used..self.input_len],
        );
        self.stats.plaintext_read.record(began, &result);
        match result {
            Ok(0) => return Err(TcpError::new("truncated transport frame")),
            Ok(n) => {
                if self.conn.is_some() {
                    self.consume_plaintext(n)?;
                }
                self.input_used += n;
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(None),
            Err(e) => return Err(e.into()),
        }
        if self.input_used != self.input_len {
            return Ok(None);
        }
        self.header_used = 0;
        self.input_used = 0;
        self.input_started = None;
        let input_len = std::mem::replace(&mut self.input_len, 0);
        // Keep the full initialized length when recycling this Vec. Subsequent
        // smaller frames overwrite only their logical range without truncate /
        // resize zero-filling; consumers must decode only the reported length.
        let frame = std::mem::take(&mut self.input);
        Ok(Some((frame, input_len)))
    }
    fn output_pending(&self, id: u64) -> bool {
        self.output.iter().any(|output| output.id == id)
    }
    fn partial_expired(&self, now: Instant, timeout: Duration) -> bool {
        self.input_started
            .is_some_and(|began| now.duration_since(began) >= timeout)
    }
    fn decode<T: serde::de::DeserializeOwned>(
        &mut self,
        frame: &[u8],
        limit: usize,
    ) -> Result<T, TcpError> {
        let began = self.time_work.then(Instant::now);
        let result = wire_codec(limit)
            .deserialize(frame)
            .map_err(TcpError::component);
        self.stats
            .frame_decode
            .record(began, &result.as_ref().map(|_| frame.len()));
        result
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        // A session dropped without `abort` (a client going away) still ends its
        // TLS stream deliberately; after `abort` this is a no-op.
        self.send_close_notify();
    }
}

/// No complete_io/StreamOwned operation is used after bootstrap: rustls must
/// return partial progress instead of waiting for the rest of a TLS record.
struct NonblockingIo<'a>(&'a mut DeadlineIo);
impl Read for NonblockingIo<'_> {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        let length = bytes.len().min(PROGRESS_BYTES);
        let io = &mut self.0;
        let began = io.profile.map(|_| Instant::now());
        #[cfg(feature = "experimental-fabric")]
        let result = if let Some(endpoint) = &mut io.fabric {
            // Fabric-only data progress: never syscall-poll the TCP control
            // socket here. Session owns its separately bounded health checks.
            endpoint.try_receive_no_poll(&mut bytes[..length])
        } else {
            io.socket.read(&mut bytes[..length])
        };
        #[cfg(not(feature = "experimental-fabric"))]
        let result = io.socket.read(&mut bytes[..length]);
        if let Some(profile) = io.profile.as_mut() {
            profile.read_ns += began.map_or(0, elapsed_ns);
            profile.read_calls += 1;
            profile.read_bytes += result.as_ref().copied().unwrap_or(0) as u64;
        }
        result
    }
}
impl Write for NonblockingIo<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.write_vectored(&[io::IoSlice::new(bytes)])
    }
    fn write_vectored(&mut self, bytes: &[io::IoSlice<'_>]) -> io::Result<usize> {
        let io = &mut self.0;
        let began = io.profile.map(|_| Instant::now());
        #[cfg(feature = "experimental-fabric")]
        let result = if let Some(endpoint) = &mut io.fabric {
            endpoint.try_send_vectored_no_poll(bytes)
        } else {
            io.socket.write_vectored(bytes)
        };
        #[cfg(not(feature = "experimental-fabric"))]
        let result = io.socket.write_vectored(bytes);
        if let Some(profile) = io.profile.as_mut() {
            profile.write_ns += began.map_or(0, elapsed_ns);
            profile.write_calls += 1;
            profile.write_bytes += result.as_ref().copied().unwrap_or(0) as u64;
        }
        result
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[derive(Default)]
pub(super) struct ClientState {
    /// Per-call cap of one control exchange's budget, applied on top of
    /// [`Request::timeout`]. `None` keeps the request's own policy; a session
    /// replacement and its tests set it to bound the exchange by a deadline.
    pub(super) control_cap: Option<Duration>,
    next_id: u64,
    control: Option<(u64, Instant)>,
    control_result: Option<Response>,
    control_decode_ns: u64,
    control_profiled: bool,
    control_timeline: Option<(
        std::sync::Arc<super::super::write_timeline::WriteTimeline>,
        u32,
    )>,
    control_target: Option<super::super::write_timeline::ControlTimelineTarget>,
    // One fixed-cut position proof, independent of the lease control slot.
    // Its envelope still consumes the session's bounded request/byte credits.
    position: Option<(u64, Instant)>,
    position_result: Option<Response>,
    position_target: Option<super::super::write_timeline::ControlTimelineTarget>,
    progressed: bool,
}
impl ClientState {
    fn allocate_id(&mut self) -> Result<u64, TcpError> {
        self.next_id = self
            .next_id
            .checked_add(1)
            .ok_or_else(|| TcpError::new("request ID space exhausted"))?;
        Ok(self.next_id)
    }
}

impl Client {
    pub(crate) fn trace_controls(
        &mut self,
        timeline: std::sync::Arc<super::super::write_timeline::WriteTimeline>,
        owner: u32,
    ) {
        self.reactor.control_timeline = Some((timeline, owner));
    }

    fn trace_control_submit(&mut self, id: u64, request: &Request) {
        use super::super::write_timeline::ControlTimelineTarget;
        let Some((timeline, owner)) = self.reactor.control_timeline.as_ref() else {
            return;
        };
        if !timeline.enabled() {
            return;
        }
        let mut target = ControlTimelineTarget {
            kind: "",
            epoch: 0,
            extent: None,
            through: None,
            prefix: None,
        };
        match request {
            Request::CheckpointPosition { epoch, through, .. } => {
                target.kind = "position";
                target.epoch = *epoch;
                target.through = Some(*through);
            }
            Request::AdoptLiveExtent {
                epoch,
                token,
                through,
                close,
                ..
            } => {
                target.kind = if *close {
                    "closing_adoption"
                } else {
                    "adoption"
                };
                target.epoch = *epoch;
                target.extent = Some(*token);
                target.through = Some(*through);
            }
            Request::PreleaseLiveExtent { epoch, after } => {
                target.kind = "prelease";
                target.epoch = *epoch;
                target.extent = Some(*after);
            }
            Request::LeaseLiveExtent { epoch } => {
                target.kind = "lease";
                target.epoch = *epoch;
            }
            _ => return,
        }
        timeline.control_event("submit", *owner, id, target, None);
        if self
            .reactor
            .position
            .is_some_and(|(position, _)| position == id)
        {
            self.reactor.position_target = Some(target);
        } else {
            self.reactor.control_target = Some(target);
        }
    }

    fn trace_control_complete(&mut self, id: u64, success: bool) {
        let target = if self
            .reactor
            .position
            .is_some_and(|(position, _)| position == id)
        {
            self.reactor.position_target.take()
        } else {
            self.reactor.control_target.take()
        };
        if let Some(target) = target
            && let Some((timeline, owner)) = self.reactor.control_timeline.as_ref()
        {
            timeline.control_event("complete", *owner, id, target, Some(success));
        }
    }

    /// Local connection transport counters; no RPC or progress is performed.
    /// Server output/reply ownership is in `ConnectionProfile::transport`.
    #[must_use]
    pub fn transport_progress_stats(&self) -> TransportProgressStats {
        self.stream.transport_stats()
    }
    fn ensure_connection(&self) -> Result<(), TcpError> {
        let message = "connection requires replacement; request outcome may be unknown";
        if self.broken {
            Err(TcpError::with_kind(io::ErrorKind::NotConnected, message))
        } else {
            Ok(())
        }
    }
    fn fail_connection(&mut self) {
        if let Some((id, _)) = self.reactor.control {
            self.trace_control_complete(id, false);
        }
        if let Some((id, _)) = self.reactor.position {
            self.trace_control_complete(id, false);
        }
        self.broken = true;
        self.stream.abort();
        self.reactor.control = None;
        self.reactor.control_result = None;
        self.reactor.control_profiled = false;
        self.reactor.position = None;
        self.reactor.position_result = None;
    }
    pub(crate) fn has_pending_io(&self) -> bool {
        !self.stream.output.is_empty()
            || self.reactor.control.is_some()
            || self.reactor.position.is_some()
    }
    #[cfg(test)]
    pub(crate) fn checkpoint_control_pending(&self) -> bool {
        self.reactor.control.is_some()
    }
    #[cfg(test)]
    pub(crate) fn checkpoint_position_pending(&self) -> bool {
        self.reactor.position.is_some()
    }
    /// Progress the session's control and position exchanges (the leased
    /// writer's adoption and lease replies). A failure fails the connection.
    pub(crate) fn progress_session(&mut self) -> Result<(), TcpError> {
        if let Err(error) = self.progress_correlated() {
            self.fail_connection();
            return Err(error);
        }
        Ok(())
    }
    pub(super) fn pause_session_poll(&mut self, polling: &mut ActivePoll) {
        polling.pause(
            self.polling,
            self.stream.is_fabric(),
            self.reactor.progressed,
        );
    }
    fn progress_correlated(&mut self) -> Result<(), TcpError> {
        self.progress_correlated_at(Instant::now())
    }

    pub(super) fn progress_correlated_at(&mut self, now: Instant) -> Result<(), TcpError> {
        self.ensure_connection()?;
        if self
            .reactor
            .control
            .is_some_and(|(_, deadline)| now >= deadline)
            || self
                .reactor
                .position
                .is_some_and(|(_, deadline)| now >= deadline)
        {
            return Err(wait::correlated_deadline());
        }
        let (progressed, _) = self.stream.progress(None)?;
        self.reactor.progressed = progressed;
        for _ in 0..SESSION_REQUESTS {
            let Some((frame, frame_len)) = self.stream.receive(self.frame_limit)? else {
                break;
            };
            self.reactor.progressed = true;
            let measured = self.diagnostics.is_some() || self.reactor.control_profiled;
            let began = measured.then(Instant::now);
            let envelope: Envelope<Response> =
                self.stream.decode(&frame[..frame_len], self.frame_limit)?;
            let decoded = began.map_or(0, elapsed_ns);
            self.stream.recycle_input(frame);
            if envelope.protocol != PROTOCOL {
                return Err(TcpError::new("invalid response protocol"));
            }
            if envelope.id == 0 {
                // ID 0 is never a request: an owner that ends the session sends
                // its reason (`send_close_notice`) right before closing.
                return Err(match envelope.body {
                    Response::Error(reason) => TcpError::owner_closed(&reason.message),
                    _ => TcpError::new("invalid unsolicited response"),
                });
            }
            if self
                .reactor
                .position
                .is_some_and(|(id, _)| id == envelope.id)
            {
                if self.reactor.position_result.is_some() {
                    return Err(TcpError::new("duplicate position response"));
                }
                self.reactor.position_result = Some(envelope.body);
                continue;
            }
            if self
                .reactor
                .control
                .is_some_and(|(id, _)| id == envelope.id)
            {
                if self.reactor.control_result.is_some() {
                    return Err(TcpError::new("duplicate control response"));
                }
                self.reactor.control_result = Some(envelope.body);
                self.reactor.control_decode_ns += decoded;
                continue;
            }
            return Err(TcpError::new("unknown or duplicate response ID"));
        }
        self.reactor.progressed |= self.stream.progress_output()?;
        Ok(())
    }
    pub(super) fn exchange_position(&mut self, request: &Request) -> Result<Response, TcpError> {
        if self.reactor.position.is_some() {
            return Err(TcpError::new("position exchange already active"));
        }
        let deadline = Instant::now() + self.timeout;
        let mut polling = ActivePoll::default();
        let mut submitted = false;
        loop {
            if !submitted {
                submitted = self.submit_position(request, deadline)?;
            }
            if let Err(error) = self.progress_correlated() {
                self.fail_connection();
                return Err(error);
            }
            if submitted && let Some(result) = self.take_position() {
                return result;
            }
            polling.pause(
                self.polling,
                self.stream.is_fabric(),
                self.reactor.progressed,
            );
        }
    }

    #[cfg(test)]
    pub(crate) fn expire_position_for_test(&mut self) {
        self.reactor.position.as_mut().unwrap().1 = Instant::now();
    }

    pub(super) fn submit_position(
        &mut self,
        request: &Request,
        deadline: Instant,
    ) -> Result<bool, TcpError> {
        self.ensure_connection()?;
        if self.reactor.position.is_some() {
            return Ok(false);
        }
        let id = self
            .reactor
            .next_id
            .checked_add(1)
            .ok_or_else(|| TcpError::new("request ID space exhausted"))?;
        let size = self.stream.queue_size(id, request, self.frame_limit)?;
        if Instant::now() >= deadline {
            return Err(TcpError::new("position output admission deadline expired"));
        }
        if !self.stream.can_queue(size) {
            return Ok(false);
        }
        let id = self.reactor.allocate_id()?;
        self.stream.queue(id, request, self.frame_limit)?;
        self.reactor.position = Some((id, deadline));
        self.trace_control_submit(id, request);
        Ok(true)
    }

    pub(super) fn take_position(&mut self) -> Option<Result<Response, TcpError>> {
        let (id, _) = self.reactor.position?;
        if self.stream.output_pending(id) {
            return None;
        }
        let response = self.reactor.position_result.take()?;
        self.trace_control_complete(id, !matches!(&response, Response::Error(_)));
        self.reactor.position = None;
        Some(match response {
            Response::Error(error) => Err(error.into()),
            response => Ok(response),
        })
    }

    /// Nonblocking control slot of the live route (and of reactor tests).
    pub(super) fn submit_control(
        &mut self,
        request: &Request,
        deadline: Instant,
    ) -> Result<bool, TcpError> {
        self.ensure_connection()?;
        if self.reactor.control.is_some() {
            return Ok(false);
        }
        let id = self
            .reactor
            .next_id
            .checked_add(1)
            .ok_or_else(|| TcpError::new("request ID space exhausted"))?;
        let size = self.stream.queue_size(id, request, self.frame_limit)?;
        if Instant::now() >= deadline {
            return Err(wait::admission_deadline());
        }
        if !self.stream.can_queue(size) {
            return Ok(false);
        }
        let id = self.reactor.allocate_id()?;
        self.stream.queue(id, request, self.frame_limit)?;
        self.reactor.control = Some((id, deadline));
        self.reactor.control_decode_ns = 0;
        self.reactor.control_profiled = false;
        self.trace_control_submit(id, request);
        Ok(true)
    }

    pub(super) fn take_control(&mut self) -> Option<Result<Response, TcpError>> {
        let (id, _) = self.reactor.control?;
        if self.reactor.control_result.is_none() || self.stream.output_pending(id) {
            return None;
        }
        let response = self.reactor.control_result.take()?;
        self.trace_control_complete(id, !matches!(&response, Response::Error(_)));
        self.reactor.control = None;
        self.reactor.control_profiled = false;
        self.reactor.control_decode_ns = 0;
        Some(match response {
            Response::Error(error) => Err(error.into()),
            response => Ok(response),
        })
    }
    pub(super) fn exchange_correlated(
        &mut self,
        request: &Request,
        mut profile: Option<&mut TcpReadProfile>,
    ) -> Result<Response, TcpError> {
        self.ensure_connection()?;
        if self.reactor.control.is_some() {
            return Err(TcpError::new("control exchange already active"));
        }
        let id = self.reactor.allocate_id()?;
        let budget = request.timeout(self.timeout);
        let cap = self.reactor.control_cap.unwrap_or(budget);
        let budget = budget.min(cap);
        let deadline = Instant::now() + budget;
        let size_began = profile.as_ref().map(|_| Instant::now());
        let queue_size = self.stream.queue_size(id, request, self.frame_limit)?;
        let size_ns = size_began.map_or(0, elapsed_ns);
        let mut polling = ActivePoll::default();
        loop {
            if self.stream.can_queue(queue_size) {
                break;
            }
            if Instant::now() >= deadline {
                return Err(wait::admission_deadline());
            }
            if let Err(error) = self.progress_correlated() {
                self.fail_connection();
                return Err(error);
            }
            polling.pause(
                self.polling,
                self.stream.is_fabric(),
                self.reactor.progressed,
            );
        }
        let queue_began = profile.as_ref().map(|_| Instant::now());
        self.stream.queue(id, request, self.frame_limit)?;
        if let Some(profile) = profile.as_deref_mut() {
            profile.request_encode_ns += size_ns + queue_began.map_or(0, elapsed_ns);
            profile.request_encode_count += 1;
        }
        self.reactor.control = Some((id, deadline));
        self.reactor.control_decode_ns = 0;
        self.reactor.control_profiled = profile.is_some();
        self.trace_control_submit(id, request);
        let began = profile.as_ref().map(|_| Instant::now());
        loop {
            if let Err(error) = self.progress_correlated() {
                self.fail_connection();
                return Err(error);
            }
            if self.reactor.control_result.is_some()
                && !self.stream.output_pending(id)
                && let Some(response) = self.reactor.control_result.take()
            {
                self.trace_control_complete(id, !matches!(&response, Response::Error(_)));
                self.reactor.control = None;
                self.reactor.control_profiled = false;
                if let Some(profile) = profile.as_deref_mut() {
                    profile.exchange_ns += began.map_or(0, elapsed_ns);
                    profile.response_decode_ns += self.reactor.control_decode_ns;
                    profile.exchange_count += 1;
                    profile.response_decode_count += 1;
                    profile.requests += 1;
                }
                return match response {
                    Response::Error(error) => Err(error.into()),
                    response => Ok(response),
                };
            }
            if !self.park_progress(id)? {
                polling.pause(
                    self.polling,
                    self.stream.is_fabric(),
                    self.reactor.progressed,
                );
            }
        }
    }
}

struct ServerPending {
    partition: u32,
    deadline: Instant,
    /// Request kind code and owner timeout, kept only to name an expired deadline.
    kind: u64,
    timeout: Duration,
    charge: usize,
    response_queued: bool,
    ready_response: Option<Response>,
    measured: bool,
    output_started: Option<Instant>,
    control_queued: Option<Instant>,
    is_append: bool,
    owner_trace: bool,
}

impl ServerPending {
    /// Names the owner deadline that ends the session: which request, at which
    /// stage, after how long. The owner logs it and sends it to the peer before
    /// closing, so the peer does not see only a TLS teardown. `detail` is the
    /// replay-page stage note, empty for every other kind.
    fn deadline_error(&self, id: u64, stage: &str, now: Instant, detail: String) -> TcpError {
        let age = self
            .deadline
            .checked_sub(self.timeout)
            .map_or(self.timeout, |accepted| {
                now.saturating_duration_since(accepted)
            });
        TcpError::new(format!(
            "owner request deadline expired: request {id} (kind {}) {stage} after {age:?} \
             (owner deadline {:?}){detail}; its outcome is unknown and the session is closed",
            self.kind, self.timeout
        ))
    }
}

pub(super) struct WorkCompletion {
    pub(super) id: u64,
    pub(super) response: Result<Response, TcpError>,
    pub(super) profile: ConnectionProfile,
}

struct OwnerTimeline {
    limit: usize,
    active: bool,
    selected: usize,
    dropped: u64,
    records: Vec<super::super::write_timeline::OwnerTimelineRecord>,
}

impl OwnerTimeline {
    #[cold]
    fn page(&self, id: u64, offset: usize, frame_limit: usize) -> Result<Response, TcpError> {
        if offset > self.records.len() {
            return Ok(Response::Error("invalid owner timeline offset".into()));
        }
        let empty = Envelope {
            protocol: PROTOCOL,
            id,
            body: Response::OwnerTimelinePage {
                records: Vec::new(),
                dropped: self.dropped,
                incomplete: self.selected.saturating_sub(self.records.len()) as u64,
                more: true,
            },
        };
        let mut bytes = wire_codec(HARD_FRAME_LIMIT)
            .serialized_size(&empty)
            .map_err(TcpError::component)?;
        let mut end = offset;
        for record in self.records[offset..].iter().take(512) {
            let size = wire_codec(HARD_FRAME_LIMIT)
                .serialized_size(record)
                .map_err(TcpError::component)?;
            if bytes.saturating_add(size) > frame_limit as u64 {
                break;
            }
            bytes += size;
            end += 1;
        }
        if end == offset && offset < self.records.len() {
            Ok(Response::Error(
                "owner timeline record exceeds frame bound".into(),
            ))
        } else {
            Ok(Response::OwnerTimelinePage {
                records: self.records[offset..end].to_vec(),
                dropped: self.dropped,
                incomplete: self.selected.saturating_sub(self.records.len()) as u64,
                more: end < self.records.len(),
            })
        }
    }
}
fn revalidate_client_peer(
    server: Option<&rustls::Connection>,
    verifier: Option<&dyn rustls::server::danger::ClientCertVerifier>,
    now: rustls::pki_types::UnixTime,
) -> Result<(), TcpError> {
    let (Some(server), Some(verifier)) = (server, verifier) else {
        if server.is_none() && verifier.is_none() {
            return Ok(());
        }
        return Err(TcpError::new("incomplete TLS peer-revalidation state"));
    };
    let certificates = server
        .peer_certificates()
        .ok_or_else(|| TcpError::new("missing authenticated client certificate"))?;
    let (leaf, chain) = certificates
        .split_first()
        .ok_or_else(|| TcpError::new("empty authenticated client certificate chain"))?;
    verifier
        .verify_client_cert(leaf, chain, now)
        .map_err(TcpError::component)?;
    Ok(())
}

pub(super) fn serve_session(
    node: &Arc<StorageNode>,
    mut stream: Session,
    limits: TcpLimits,
    shutdown: &AtomicBool,
    shutdown_wake: &super::WakeFd,
    client_verifier: Option<&dyn rustls::server::danger::ClientCertVerifier>,
    first_request: Option<(Envelope<Request>, usize, Instant)>,
) -> Result<(), TcpError> {
    let (completed, completions) = mpsc::sync_channel::<WorkCompletion>(SESSION_REQUESTS);
    let mut pending = HashMap::<u64, ServerPending>::new();
    let mut replay_stages = replay_page::ReplayStages::new();
    let mut controls = VecDeque::<(u64, Request)>::new();
    // One decoded input frame may await byte credits; never drain more input
    // behind it. Its original deadline and the negotiated frame bound apply.
    let mut held_request = first_request;
    let mut active_appends = 0usize;
    let mut barrier_active = false;
    let mut workers = 0usize;
    let mut retained_bytes = 0usize;
    let mut initialized = false;
    let mut last_id = 0;
    let mut diagnostics = None::<ConnectionProfile>;
    let mut owner_timeline = None::<OwnerTimeline>;
    let mut idle_since = Instant::now();
    let mut polling = ActivePoll::default();
    let mut previous_sweep = None::<Instant>;
    let mut fabric_idle_deadline = None;
    let mut progress = wait::OwnerProgress::new(node, shutdown_wake);
    #[cfg(feature = "experimental-fabric")]
    stream.attach_owner_transport(&node.control_context.transport);
    let result = (|| {
        while !shutdown.load(Ordering::Acquire) && !node.is_closed() {
            progress.begin_sweep()?;
            let now = Instant::now();
            let sweep_gap = if let Some(profile) = diagnostics.as_mut() {
                profile.polling.progress_sweeps += 1;
                previous_sweep.replace(now).map_or(0, |previous| {
                    now.duration_since(previous)
                        .as_nanos()
                        .min(u64::MAX as u128) as u64
                })
            } else {
                previous_sweep = None;
                0
            };
            // io_timeout bounds initial Status, partial frames and accepted work;
            // a healthy established connection may remain idle indefinitely.
            if let Some((&id, request)) = pending.iter().find(|(_, read)| now >= read.deadline) {
                let stage = if request.response_queued {
                    "was still sending its response"
                } else if request.ready_response.is_some() {
                    "had a response ready to send"
                } else {
                    "was still executing"
                };
                return Err(request.deadline_error(
                    id,
                    stage,
                    now,
                    format!(
                        "{}{}",
                        replay_stages.describe(id, request, now),
                        replay_page::test_queue_note(node)
                    ),
                ));
            }
            if let Some((envelope, _, _)) = held_request
                .as_ref()
                .filter(|(_, _, deadline)| now >= *deadline)
            {
                return Err(TcpError::new(format!(
                    "owner request deadline expired: request {} (kind {}) was still waiting \
                     for session byte credits; the session is closed",
                    envelope.id,
                    request_kind_code(&envelope.body)
                )));
            }
            if stream.partial_expired(now, limits.io_timeout) {
                return Err(TcpError::new(format!(
                    "owner I/O deadline expired: a request frame stayed incomplete for {:?}; \
                     the session is closed",
                    limits.io_timeout
                )));
            }
            if !initialized
                && pending.is_empty()
                && now.duration_since(idle_since) >= limits.io_timeout
            {
                return Err(TcpError::new(format!(
                    "owner I/O deadline expired: no Status request within {:?} of connect; \
                     the session is closed",
                    limits.io_timeout
                )));
            }
            // An idle provider wait replaces this sweep's CQ acquisition. It
            // never adds a second consumer or parks an active client request.
            let (mut progressed, idle_wait_ns) = stream.progress(fabric_idle_deadline.take())?;
            if let Some(wait_ns) = idle_wait_ns
                && let Some(profile) = diagnostics.as_mut()
            {
                profile.polling.idle_wait_calls += 1;
                profile.polling.idle_wait_ns += wait_ns;
            }
            retire_responses(
                &mut stream,
                &mut pending,
                &mut retained_bytes,
                diagnostics.as_mut(),
                &mut replay_stages,
            );
            while let Ok(completion) = completions.try_recv() {
                progressed = true;
                workers -= 1;
                progress.completed(completion.id);
                if pending
                    .get(&completion.id)
                    .is_some_and(|request| request.is_append)
                {
                    active_appends = active_appends.checked_sub(1).ok_or_else(|| {
                        TcpError::new("stream completion without active session admission")
                    })?;
                } else {
                    barrier_active = false;
                }
                if let Some(request) = pending.get(&completion.id)
                    && request.owner_trace
                    && let Some(timeline) = owner_timeline.as_mut()
                {
                    let profile = completion.profile;
                    let storage = profile.storage_write;
                    let phase = |ns| {
                        (storage.append_calls != 0
                            && ((storage.append_errors == 0 && storage.append_retries == 0)
                                || ns != 0))
                            .then_some(ns)
                    };
                    let mut error_truncated = false;
                    let error = completion.response.as_ref().err().map(|error| {
                        let mut message = error.to_string();
                        if message.len() > 128 {
                            let mut end = 128;
                            while !message.is_char_boundary(end) {
                                end -= 1;
                            }
                            message.truncate(end);
                            error_truncated = true;
                        }
                        message
                    });
                    timeline
                        .records
                        .push(super::super::write_timeline::OwnerTimelineRecord {
                            owner: 0,
                            envelope_id: completion.id,
                            epoch: None,
                            success: completion.response.is_ok(),
                            error,
                            error_truncated,
                            control_queue_ns: (profile.control_queue_samples != 0)
                                .then_some(profile.control_queue_ns),
                            session_admission_wait_ns: (profile.control_queue_samples != 0)
                                .then_some(profile.session_admission_wait_ns),
                            appender_queue_ns: (profile.control_queue_samples != 0)
                                .then_some(profile.appender_queue_ns),
                            flusher_queue_ns: (storage.persist_calls != 0)
                                .then_some(profile.flusher_queue_ns),
                            append_barrier_wait_ns: (storage.append_calls != 0)
                                .then_some(profile.append_barrier_wait_ns),
                            append_wal_ns: (storage.append_calls != 0)
                                .then_some(profile.append_wal_ns),
                            append_validation_ns: (storage.append_calls != 0)
                                .then_some(storage.append_validation_ns),
                            append_basic_validation_ns: phase(storage.append_basic_validation_ns),
                            append_conflict_lookup_ns: phase(storage.append_conflict_lookup_ns),
                            append_geometry_capacity_ns: phase(storage.append_geometry_capacity_ns),
                            append_metadata_reservation_ns: phase(
                                storage.append_metadata_reservation_ns,
                            ),
                            append_descriptor_preparation_ns: phase(
                                storage.append_descriptor_preparation_ns,
                            ),
                            append_errors: storage.append_errors,
                            append_retries: storage.append_retries,
                            append_persist_ns: (storage.persist_calls != 0)
                                .then_some(profile.append_persist_ns),
                            wal_file_persist_ns: (storage.file_persist_calls != 0)
                                .then_some(storage.persist_wal_file_ns),
                            handler_ns: Some(profile.handler_ns),
                            response_ready_ns: request.control_queued.map(elapsed_ns),
                            namespace_persist_ns: (storage.namespace_persist_calls != 0)
                                .then_some(storage.persist_namespace_ns),
                            file_persist_calls: storage.file_persist_calls,
                            namespace_persist_calls: storage.namespace_persist_calls,
                            appended_groups: storage.appended_groups,
                            batch_count: None,
                            encoded_bytes: None,
                            persistence_capture: storage.persistence_capture,
                            ready_append_prefix: profile.ready_append_prefix,
                        });
                }
                if let Some(profile) = diagnostics.as_mut() {
                    profile.add(completion.profile);
                }
                let response = completion
                    .response
                    .unwrap_or_else(|error| Response::Error(error.into()));
                queue_response(&mut pending, completion.id, response)?;
            }
            for _ in 0..SESSION_REQUESTS {
                // Peer receipt can precede local TX completion. Backpressure,
                // rather than reject, the next valid window while credits live.
                if pending.len() == SESSION_REQUESTS {
                    break;
                }
                let (envelope, frame_bytes, deadline) = if let Some(request) = held_request.take() {
                    request
                } else {
                    let starting_frame = stream.header_used == 0;
                    let received = match stream.receive(limits.max_frame_bytes) {
                        Ok(received) => received,
                        // A peer that closed at a frame boundary neither sends nor
                        // reads again. End the session; report only cut-off work.
                        Err(_) if stream.peer_finished() && pending.is_empty() => {
                            return Ok(());
                        }
                        Err(_) if stream.peer_finished() => {
                            let note = replay_stages.closed_session_note(&pending);
                            return Err(TcpError::new(format!(
                                "peer closed the session with {} accepted requests outstanding\
                                 {note}{}",
                                pending.len(),
                                replay_page::test_queue_note(node)
                            )));
                        }
                        Err(error) => return Err(error),
                    };
                    if starting_frame
                        && (received.is_some() || stream.header_used != 0)
                        && let Some(profile) = diagnostics.as_mut()
                    {
                        profile.polling.request_first_poll_samples += 1;
                        profile.polling.request_first_poll_gap_ns += sweep_gap;
                        profile.polling.request_first_poll_gap_max_ns =
                            profile.polling.request_first_poll_gap_max_ns.max(sweep_gap);
                    }
                    let Some((frame, frame_len)) = received else {
                        break;
                    };
                    progressed = true;
                    idle_since = Instant::now();
                    let began = diagnostics.as_ref().map(|_| Instant::now());
                    let envelope: Envelope<Request> =
                        stream.decode(&frame[..frame_len], limits.max_frame_bytes)?;
                    let frame_bytes = frame_len;
                    stream.recycle_input(frame);
                    if let Some(profile) = diagnostics.as_mut() {
                        profile.request_decode_ns += began.map_or(0, elapsed_ns);
                    }
                    let timeout = envelope.body.timeout(limits.io_timeout);
                    (envelope, frame_bytes, Instant::now() + timeout)
                };
                if envelope.protocol != PROTOCOL || envelope.id <= last_id {
                    return Err(TcpError::new(
                        "invalid, duplicate or nonmonotonic request ID",
                    ));
                }
                if !initialized
                    && !matches!(
                        &envelope.body,
                        Request::Status {
                            protocol: PROTOCOL,
                            ..
                        }
                    )
                {
                    return Err(TcpError::new("status/protocol handshake required"));
                }
                if initialized && matches!(&envelope.body, Request::Status { .. }) {
                    // Reusing a live TLS channel must not let an expired client
                    // credential create a new authority. Initial status already
                    // follows a fresh mTLS handshake; ordinary I/O stays unchanged.
                    revalidate_client_peer(
                        stream.conn.as_ref(),
                        client_verifier,
                        rustls::pki_types::UnixTime::now(),
                    )?;
                }
                let charge = frame_bytes.max(limits.max_frame_bytes).saturating_add(512);
                if charge > SESSION_BYTES {
                    return Err(TcpError::new("request exceeds session byte credits"));
                }
                if charge > SESSION_BYTES.saturating_sub(retained_bytes) {
                    held_request = Some((envelope, frame_bytes, deadline));
                    break;
                }
                last_id = envelope.id;
                initialized = true;
                retained_bytes += charge;
                let trace_control = matches!(
                    &envelope.body,
                    Request::CheckpointPosition { .. }
                        | Request::AdoptLiveExtent { .. }
                        | Request::PreleaseLiveExtent { .. }
                        | Request::LeaseLiveExtent { .. }
                );
                let owner_trace = trace_control
                    && owner_timeline
                        .as_mut()
                        .filter(|timeline| timeline.active)
                        .is_some_and(|timeline| {
                            if timeline.selected < timeline.limit {
                                timeline.selected += 1;
                                true
                            } else {
                                timeline.dropped += 1;
                                false
                            }
                        });
                let measured = (diagnostics.is_some() && !envelope.body.is_measurement_control())
                    || owner_trace;
                if let Some(profile) = diagnostics.as_mut() {
                    profile.requests += u64::from(measured);
                }
                super::super::owner_events::record(
                    super::super::owner_events::OwnerEventKind::ControlReceived,
                    node.control_context.partition.partition,
                    envelope.id,
                    request_kind_code(&envelope.body),
                );
                pending.insert(
                    envelope.id,
                    ServerPending {
                        partition: node.control_context.partition.partition,
                        deadline,
                        kind: request_kind_code(&envelope.body),
                        timeout: envelope.body.timeout(limits.io_timeout),
                        charge,
                        response_queued: false,
                        ready_response: None,
                        measured,
                        owner_trace,
                        is_append: control::is_stream_request(&envelope.body),
                        control_queued: measured.then(Instant::now),
                        output_started: None,
                    },
                );
                if matches!(&envelope.body, Request::ReplayLiveWal { .. }) {
                    replay_stages.accept(envelope.id);
                }
                // A lease renewal must not wait behind an exclusive control.
                if reply::answer_renewal(&mut pending, &envelope, &node.control_context)? {
                    continue;
                }
                controls.push_back((envelope.id, envelope.body));
            }
            // Dispatch newly decoded requests in this sweep, not after another
            // socket/CQ poll. Each queued request gets one bounded attempt; a
            // busy control owner cannot block unrelated requests.
            let mut blocked_control = false;
            for _ in 0..controls.len() {
                let Some((id, request)) = controls.pop_front() else {
                    // Unreachable: each pass pops one entry and pushes at most one
                    // back, so `controls.len()` at loop entry bounds the pops.
                    break;
                };
                let is_append = control::is_stream_request(&request);
                // A sealed-epoch page reads immutable bytes: no stream wait.
                let waits_for_appends =
                    !is_append && !control::is_sealed_replay_page(&request) && active_appends != 0;
                if blocked_control || barrier_active || waits_for_appends {
                    #[cfg(test)]
                    node.control.note_session_barrier_wait();
                    controls.push_back((id, request));
                    blocked_control = true;
                    continue;
                }
                if let Request::OwnerTimelineArm { limit } = request {
                    let response = if limit > 65_536 {
                        Response::Error("owner timeline limit exceeds 65536".into())
                    } else {
                        owner_timeline = (limit != 0).then(|| OwnerTimeline {
                            limit,
                            selected: 0,
                            dropped: 0,
                            active: true,
                            records: Vec::with_capacity(limit),
                        });
                        Response::OwnerTimelineArmed
                    };
                    queue_response(&mut pending, id, response)?;
                } else if let Request::OwnerTimelineFetch { offset } = request {
                    let response = if let Some(timeline) = owner_timeline.as_mut() {
                        timeline.active = false;
                        timeline.page(id, offset, limits.max_frame_bytes)?
                    } else {
                        Response::Error("owner timeline is not armed".into())
                    };
                    queue_response(&mut pending, id, response)?;
                } else if let Request::ConnectionProfile { enabled, reset } = request {
                    let mut previous = diagnostics.unwrap_or_default();
                    previous.socket = stream.sock.profile.take().unwrap_or_default();
                    previous.transport = stream.transport_stats();
                    stream.configure_transport_stats(enabled, reset);
                    if reset || !enabled {
                        previous_sweep = None;
                    }
                    diagnostics = enabled.then(|| {
                        if reset {
                            ConnectionProfile::default()
                        } else {
                            previous
                        }
                    });
                    stream.sock.profile = enabled.then(|| {
                        if reset {
                            SocketProfile::default()
                        } else {
                            previous.socket
                        }
                    });
                    queue_response(
                        &mut pending,
                        id,
                        Response::ConnectionProfile(Box::new(previous)),
                    )?;
                } else if let Some(reason) = wait::OwnerProgress::refusal(&stream, &request) {
                    // Correlated refusal before admission; the session stays open.
                    queue_response(&mut pending, id, Response::Error(reason.into()))?;
                } else {
                    let notify = progress.notify(&request)?;
                    let is_progress = notify.is_some();
                    let work = control::ControlWork {
                        id,
                        request,
                        limits,
                        measured: pending[&id].measured,
                        queued: pending[&id].control_queued,
                        submitted: pending[&id].measured.then(Instant::now),
                        charge: pending[&id].charge,
                        completed: completed.clone(),
                        progress: notify,
                    };
                    match node.control.try_submit(work) {
                        Ok(()) => {
                            if is_progress {
                                progress.submitted(id);
                            }
                            if is_append {
                                active_appends += 1;
                            } else {
                                barrier_active = true;
                            }
                            workers += 1;
                            replay_stages.dispatch(id);
                        }
                        Err(work) => {
                            // Do not let a later fence/seal pass a backpressured
                            // stream request on this authenticated lane.
                            controls.push_back((id, work.request));
                            blocked_control = true;
                            continue;
                        }
                    }
                }
                progressed = true;
            }
            if flush_responses(
                &mut stream,
                &mut pending,
                limits,
                diagnostics.as_mut(),
                &replay_stages,
            )? {
                // Do not defer a bounded resident reply behind the next sweep's
                // socket/CQ read. This is output-only; cold I/O stays off-owner.
                stream.progress_output()?;
                progressed = true;
            }
            // Every send of this sweep has been posted; publish its counters.
            #[cfg(feature = "experimental-fabric")]
            stream.flush_owner_transport();
            if progressed {
                idle_since = Instant::now();
            }
            let quiet = held_request.is_none() && controls.is_empty() && workers == 1;
            if progress.park(
                &mut stream,
                progressed,
                quiet,
                &pending,
                diagnostics.as_mut(),
            )? {
                // The durable-progress lane waited on readiness.
            } else if let Some(deadline) = stream.idle_wait_deadline(
                limits,
                progressed,
                !pending.is_empty() || held_request.is_some(),
                initialized,
                idle_since,
            ) {
                if stream.is_fabric() {
                    fabric_idle_deadline = Some(deadline);
                } else {
                    let began = diagnostics.as_ref().map(|_| Instant::now());
                    wait_for_readable(&stream.sock.socket, deadline)?;
                    if let Some(profile) = diagnostics.as_mut() {
                        profile.polling.idle_wait_calls += 1;
                        profile.polling.idle_wait_ns += began.map_or(0, elapsed_ns);
                    }
                }
            } else {
                let (yielded, spun) = polling.pause(limits.polling, stream.is_fabric(), progressed);
                if let Some(profile) = diagnostics.as_mut() {
                    profile.polling.scheduler_yields += u64::from(yielded);
                    profile.polling.busy_spin_hints +=
                        u64::from(spun && limits.polling == PollingPolicy::Busy);
                }
            }
        }
        Ok(())
    })();
    // Tell a live peer why its session ends, so its error names the owner's
    // deadline or failure instead of a bare connection close. `Ok` with a live
    // peer means the node's shutdown flag ended the session.
    if !stream.peer_closed {
        match &result {
            Err(error) => stream.send_close_notice(&error.to_string(), limits.max_frame_bytes),
            Ok(()) => {
                stream.send_close_notice("owner node is shutting down", limits.max_frame_bytes)
            }
        }
    }
    stream.abort();
    // A disconnected caller cancels delivery, not accepted control work. Join
    // the bounded worker completions before releasing this session's credits.
    while workers != 0 {
        completions.recv().map_err(TcpError::component)?;
        workers -= 1;
    }
    result
}
