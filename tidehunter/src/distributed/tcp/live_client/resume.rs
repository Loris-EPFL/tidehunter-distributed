//! W1: resume a leased write after a transient transport break.
//!
//! A break of the coordinator's connection to one owner WAL, while that owner
//! process keeps running and its coordinator lease stays valid, reconnects the
//! control session, asks the owner how far each open extent is adopted, and
//! rewrites only the frames the owner did not adopt as identical bytes at
//! identical offsets. Every other failure still fail-stops.
//!
//! The rewrite is safe because this coordinator is the only writer of the
//! extent's byte range: the payload is a fixed-offset overwrite of whole
//! aligned blocks, and the owner's adoption frontier is prefix-monotone, so a
//! repeat below it is a no-op and a repeat above it is the first execution.
//! The receipt digest the owner recomputes during adoption then proves that the
//! rewritten bytes were identical (design W1, sections 4 and 5).
//!
//! Bounds: one reconnect after the first break (`MAX_RESUME_ATTEMPTS`), at most
//! three resumes without an adopted frame between them
//! (`MAX_CONSECUTIVE_RESUMES`), and one budget of `2 x io_timeout_ms` capped by
//! the coordinator lease's own deadline (D-W1-5). That budget bounds **every**
//! step, not only the reconnect: each control exchange is capped by the budget
//! that remains, each queue open derives its controller and namespace deadlines
//! from it, and the lease and the budget are rechecked before the resume
//! reports success. Nothing here creates a session or a thread: the reconnect
//! reuses the authenticated route a read lane already rebuilds from, and a
//! detached data queue goes to the database's one DMA reaper.

use std::io;
use std::time::{Duration, Instant};

use super::super::LiveLeased;
use super::{LiveWriteQueue, PendingAdoption, PendingTarget, RetainedFrame};
use crate::distributed::nvmeof::live_io::{LiveIoError, LiveIoResult, LiveWriteSync};

use super::super::{Client, TcpError, is_transient_break, is_transient_errno};

/// One reconnect after the first break. A link that flaps twice inside one
/// write is not transient, so its second break fail-stops.
pub(super) const MAX_RESUME_ATTEMPTS: u32 = 2;
/// Resumes without a frame submitted *and* adopted between them. Without this
/// bound a link that flaps without carrying a frame could live forever.
pub(super) const MAX_CONSECUTIVE_RESUMES: u32 = 3;
/// A resume never runs longer than this many of the write path's own I/O
/// deadlines, and never past the coordinator lease's deadline.
const RESUME_IO_TIMEOUTS: u32 = 2;

/// Which side of the write path lost its transport. It decides one thing: a
/// data-queue break detaches the queue and rewrites the retained suffix, while
/// a session break keeps the live queue and only reconnects the control session.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BreakKind {
    /// The control session is unusable; the NVMe-oF queue may still progress.
    Session,
    /// The NVMe-oF queue lost its controller, so no CQE of a submitted frame
    /// can be trusted and the unadopted suffix must be rewritten.
    Data,
}

impl BreakKind {
    /// A timeout is a probe, not a verdict: the owner may be overloaded and the
    /// queue may still progress, so it keeps the queue first.
    fn of(error: &TcpError) -> Self {
        if error.kind() == io::ErrorKind::TimedOut {
            Self::Session
        } else {
            Self::Data
        }
    }
}

impl Client {
    /// Install the database's write-resume decision. Only a write worker's
    /// client enables it; every other client keeps the fail-stop behaviour.
    pub(crate) fn set_resume_writes(
        &mut self,
        enabled: bool,
        reaper: Option<std::sync::mpsc::SyncSender<LiveWriteQueue>>,
    ) {
        self.resume_writes = enabled;
        self.resume_reaper = reaper;
    }

    /// The break class of `error`, or `None` when the error must fail-stop.
    pub(crate) fn break_kind(&self, error: &TcpError) -> Option<BreakKind> {
        if !self.resume_writes || !is_transient_break(error) {
            return None;
        }
        Some(BreakKind::of(error))
    }

    /// Run one leased operation, and if it fails with a transient transport
    /// break, resume the write path once and run the operation again. A
    /// resumed operation returns its own `Ok`, so the caller's wrapper never
    /// sees the error and never poisons the writer.
    pub(crate) fn leased_step<T>(
        &mut self,
        kind: BreakKind,
        mut step: impl FnMut(&mut Self) -> Result<T, TcpError>,
    ) -> Result<T, TcpError> {
        match step(self) {
            Ok(value) => Ok(value),
            Err(error) => {
                if self.break_kind(&error).is_none() {
                    return Err(error);
                }
                self.resume_after_break(kind)?;
                step(self)
            }
        }
    }

    /// Classify a queue error at the boundary, before its kind is lost: a lost
    /// controller is a transient break, a rejected or unknown write is not.
    pub(crate) fn io_error(&self, error: LiveIoError) -> TcpError {
        classify_io_error(error, self.latest_write_errno())
    }

    /// Errno of the latest completed namespace write, if that write failed.
    pub(crate) fn latest_write_errno(&self) -> Option<i32> {
        self.live_writer
            .as_ref()
            .and_then(|writer| writer.io.last_write_errno())
    }

    /// Resume the leased write path after a transient break (W1).
    ///
    /// Decision rule (D25): the same owner incarnation, a valid unexpired
    /// coordinator lease, and a healthy recovery-clean owner. A new
    /// incarnation, a lapsed lease or a dirty owner fails the database, and the
    /// application reopens it.
    pub(crate) fn resume_after_break(&mut self, kind: BreakKind) -> Result<(), TcpError> {
        let now = Instant::now();
        let deadline = match self.resume_budget(now) {
            Ok(deadline) => deadline,
            Err(error) => {
                self.live_leased.resume_failed = true;
                self.record_resume_refused();
                return Err(error);
            }
        };
        if self.live_leased.resuming
            || self.live_writer_closing
            || self.live_leased.resume_consecutive >= MAX_CONSECUTIVE_RESUMES
        {
            self.live_leased.resume_failed = true;
            self.record_resume_refused();
            return Err(TcpError::with_kind(
                io::ErrorKind::ConnectionAborted,
                format!(
                    "live write resume is not available: resuming {}, closing {}, {} consecutive resumes",
                    self.live_leased.resuming,
                    self.live_writer_closing,
                    self.live_leased.resume_consecutive
                ),
            ));
        }
        self.live_leased.resuming = true;
        self.live_leased.resume_failed = false;
        #[cfg(test)]
        {
            self.live_leased.resume_steps = 0;
        }
        let result = self.resume_episode(kind, deadline);
        self.live_leased.resuming = false;
        match result {
            Ok(()) => {
                self.live_leased.resume_consecutive += 1;
                Ok(())
            }
            Err(error) => {
                self.live_leased.resume_failed = true;
                self.record_resume_refused();
                Err(error)
            }
        }
    }

    /// Whether the failure being reported came from a resume, and clear it. The
    /// worker calls this once per failure record, on the same thread that ran
    /// the resume.
    pub(crate) fn take_resume_failure(&mut self) -> bool {
        std::mem::take(&mut self.live_leased.resume_failed)
    }

    fn record_resume_refused(&self) {
        if let Some(counters) = &self.live_counters {
            counters.record_resume_refused();
        }
    }

    /// The resume's budget: `now + 2 x io_timeout_ms`, capped by the
    /// coordinator lease's deadline, which is the authority on the right to
    /// write. A leaseless fixture uses the time bound alone.
    fn resume_budget(&self, now: Instant) -> Result<Instant, TcpError> {
        let config = self
            .live_config
            .as_ref()
            .ok_or_else(|| TcpError::new("live configuration missing"))?;
        if let Some(validity) = &self.lease_validity {
            validity.ensure_writable(self.expected.partition)?;
        }
        let window = Duration::from_millis(
            config
                .io_timeout_ms
                .saturating_mul(u64::from(RESUME_IO_TIMEOUTS)),
        );
        let own = now
            .checked_add(window)
            .ok_or_else(|| TcpError::new("live resume budget overflow"))?;
        let deadline = self
            .lease_validity
            .as_ref()
            .map_or(own, |validity| own.min(validity.admitted_until()));
        if deadline <= now {
            return Err(TcpError::with_kind(
                io::ErrorKind::TimedOut,
                "live write resume budget expired",
            ));
        }
        Ok(deadline)
    }

    /// One resume, retried once when its own reconnect or rewrite fails
    /// transiently.
    fn resume_episode(&mut self, kind: BreakKind, deadline: Instant) -> Result<(), TcpError> {
        let mut attempt = 1;
        loop {
            if let Some(counters) = &self.live_counters {
                counters.record_resume_attempt();
            }
            match self.resume_once(kind, deadline) {
                Err(error) if attempt < MAX_RESUME_ATTEMPTS && is_transient_break(&error) => {
                    attempt += 1;
                }
                other => return other,
            }
        }
    }

    fn resume_once(&mut self, kind: BreakKind, deadline: Instant) -> Result<(), TcpError> {
        // Steps 1-2: a fresh control session on the same authenticated route,
        // then the decision checks.
        self.rebuild_session(deadline)?;
        #[cfg(test)]
        self.after_resume_step();
        // Step 3: only the state the old session owned is reset. The flight
        // deque, the receipts and the leased replies belong to the worker.
        self.live_leased.pending = None;
        self.live_leased.pending_started = None;
        #[cfg(test)]
        self.after_resume_step();
        // Step 4: the owner is the oracle for each extent's real frontier. The
        // probe declares `through = adopted.end`, which is always at or below
        // the owner's frontier, so the owner answers from memory without a scan.
        self.probe_leased_extents(deadline)?;
        #[cfg(test)]
        self.after_resume_step();
        // Step 5: rebuild the data path, or keep a live queue as it is.
        match kind {
            BreakKind::Data => {
                self.detach_live_queue()?;
                let frames = self.rewrite_retained(deadline)?;
                if let Some(counters) = &self.live_counters
                    && frames != 0
                {
                    counters.record_resume_rewrite(frames);
                }
            }
            BreakKind::Session => self.retime_leased_work(),
        }
        #[cfg(test)]
        self.after_resume_step();
        // Step 6: declare what the owner can now prove, and settle the flights
        // it covers. Nothing is popped and nothing is reordered.
        self.declare_resumed_adoption(kind == BreakKind::Data, deadline)?;
        #[cfg(test)]
        self.after_resume_step();
        // Step 7: a detached queue left the settled flights without a device to
        // report on. Reopen one so their release, and the covering flush the
        // synchronous tier needs, run on a live namespace.
        if kind == BreakKind::Data {
            self.reopen_detached_queue(deadline)?;
        }
        #[cfg(test)]
        self.after_resume_step();
        // The lease is the authority on the right to write, and the budget is
        // the bound the caller was promised: neither may have lapsed while the
        // last exchange ran.
        self.remaining_resume(deadline)?;
        #[cfg(test)]
        self.after_resume_step();
        Ok(())
    }

    /// Count one completed resume step and let a test lapse the coordinator
    /// lease at a chosen boundary. The lapse lands between two steps, so the
    /// step that runs next is the one that must observe it.
    #[cfg(test)]
    fn after_resume_step(&mut self) {
        self.live_leased.resume_steps += 1;
        let Some((after, validity)) = self.live_leased.lapse_lease_after.as_ref() else {
            return;
        };
        if *after == self.live_leased.resume_steps {
            validity.lapse_for_test();
        }
    }

    /// The budget left for the next resume step, after rechecking the right to
    /// write. A lapsed lease and a spent budget both refuse the step, so no
    /// exchange, queue open or rewrite starts past the episode's bound.
    pub(super) fn remaining_resume(&self, deadline: Instant) -> Result<Duration, TcpError> {
        if let Some(validity) = &self.lease_validity {
            validity.ensure_writable(self.expected.partition)?;
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(TcpError::with_kind(
                io::ErrorKind::TimedOut,
                "live write resume budget expired",
            ));
        }
        Ok(remaining)
    }

    /// Run one control exchange inside the episode budget. The client's request
    /// timeouts do not know that budget, so the session timeout is capped for
    /// the call and restored afterwards; a spent budget never reaches the wire.
    pub(super) fn within_resume_budget<T>(
        &mut self,
        deadline: Instant,
        step: impl FnOnce(&mut Self) -> Result<T, TcpError>,
    ) -> Result<T, TcpError> {
        let remaining = self.remaining_resume(deadline)?;
        let previous = self.timeout;
        self.timeout = previous.min(remaining);
        let result = step(self);
        self.timeout = previous;
        result
    }

    /// Open a queue on `leased`, with the controller and namespace deadlines
    /// derived from the episode budget. The shared open derives both from
    /// `connect_timeout_ms`, so that value is capped for the call.
    pub(super) fn open_queue_within(
        &mut self,
        leased: &LiveLeased,
        deadline: Instant,
    ) -> Result<(), TcpError> {
        let remaining = self.remaining_resume(deadline)?;
        let capped_ms = u64::try_from(remaining.as_millis())
            .unwrap_or(u64::MAX)
            .max(1);
        let previous = self
            .live_config
            .as_ref()
            .map(|config| config.connect_timeout_ms);
        if let Some(config) = self.live_config.as_mut() {
            config.connect_timeout_ms = config.connect_timeout_ms.min(capped_ms);
        }
        let result = self.open_leased_queue_with(leased);
        if let (Some(config), Some(previous)) = (self.live_config.as_mut(), previous) {
            config.connect_timeout_ms = previous;
        }
        result
    }

    /// Replace only the transport session, so every live field of this client
    /// (the writer state, the flights, the pools and the counters) survives.
    fn rebuild_session(&mut self, deadline: Instant) -> Result<(), TcpError> {
        let route = self.route.clone();
        let mut fresh = super::super::connect_shared::connect(
            &route.endpoint,
            &route.tls,
            route.config.clone(),
            route.expected.clone(),
            &route.shape,
            super::super::connect_shared::SessionSetup {
                deadline,
                timeout: route.timeout,
                progress: None,
            },
        )?;
        fresh.route = route;
        let status = fresh.connected_status().clone();
        let partition = self.expected.partition;
        let expected_incarnation = self.connected_status.as_ref().map(|old| old.incarnation);
        if let Some(incarnation) = expected_incarnation
            && status.incarnation != incarnation
        {
            return Err(TcpError::with_kind(
                io::ErrorKind::PermissionDenied,
                format!("owner {partition} restarted; reopen required"),
            ));
        }
        if let Some(epoch) = self.leased_epoch()
            && status.active_epoch != Some(epoch)
        {
            return Err(TcpError::with_kind(
                io::ErrorKind::PermissionDenied,
                format!("owner {partition} is fenced at another authority epoch; reopen required"),
            ));
        }
        // A dirty or unhealthy incarnation may have lost frames, so its
        // adopted prefix proves nothing about the bytes this coordinator holds.
        if !status.healthy || !status.recovery_clean {
            return Err(TcpError::with_kind(
                io::ErrorKind::PermissionDenied,
                format!("owner {partition} is not recovery-clean; reopen required"),
            ));
        }
        // Swap, not assign: the old session must be dropped, not leaked, and
        // only the transport fields move.
        std::mem::swap(&mut self.stream, &mut fresh.stream);
        std::mem::swap(&mut self.reactor, &mut fresh.reactor);
        self.broken = false;
        self.connected_status = Some(status);
        self.connect_profile = fresh.connect_profile;
        drop(fresh);
        Ok(())
    }

    fn leased_epoch(&self) -> Option<u64> {
        self.live_leased
            .current
            .as_ref()
            .or(self.live_leased.closing.as_ref())
            .map(|extent| extent.lease.extent.segment.epoch)
    }

    /// Probe the closing extent first, then the current one: a rollover closes
    /// before the new extent is written. Each probe is one bounded exchange.
    fn probe_leased_extents(&mut self, deadline: Instant) -> Result<(), TcpError> {
        for target in [PendingTarget::Closing, PendingTarget::Current] {
            let Some((epoch, token, through)) = self.extent_adoption(target) else {
                continue;
            };
            let pending = PendingAdoption {
                through,
                close: false,
                lease_next: false,
                target,
            };
            let reply = self.within_resume_budget(deadline, |client| {
                client.probe_live_extent(epoch, token, through)
            })?;
            self.check_leased_adoption(pending, &reply)?;
            #[cfg(test)]
            self.live_leased.resume_targets.push(target);
            if reply.adoption.closed {
                // The owner applied the close whose reply died with the old
                // session: the rollover is complete, so retire the closing
                // extent with the harvest path's own bookkeeping.
                self.recycle_closing()?;
            }
            if let Some(extent) = self.extent_mut(target) {
                // The owner's frontier is the only truth: a declaration whose
                // reply died with the old session must not be claimed done.
                extent.declared_through = extent.adopted.end;
            }
        }
        Ok(())
    }

    fn extent_mut(&mut self, target: PendingTarget) -> Option<&mut super::ExtentState> {
        match target {
            PendingTarget::Closing => self.live_leased.closing.as_mut(),
            PendingTarget::Current | PendingTarget::Prelease => self.live_leased.current.as_mut(),
        }
    }

    /// `(epoch, token, adopted end)` of the extent a probe names, or `None`
    /// when that extent is not open.
    fn extent_adoption(&self, target: PendingTarget) -> Option<(u64, [u8; 16], u64)> {
        let extent = match target {
            PendingTarget::Closing => self.live_leased.closing.as_ref(),
            PendingTarget::Current | PendingTarget::Prelease => self.live_leased.current.as_ref(),
        }?;
        Some((
            extent.lease.extent.segment.epoch,
            extent.lease.extent.token,
            extent.adopted.end,
        ))
    }

    /// Hand a dead queue to the database's reaper. `LiveIo::shutdown` is
    /// unbounded and a lost command can stay outstanding until the kernel
    /// keep-alive fires, so the worker thread never drains it.
    fn detach_live_queue(&mut self) -> Result<(), TcpError> {
        // At most one queue per client waits for the shutdown drain. Parking a
        // second would overwrite the first, and the overwritten queue's `Drop`
        // calls the unbounded `LiveIo::shutdown` on this thread, which aborts
        // when the drain times out. A parked queue is already a fail-stop, so
        // refuse before anything is taken.
        if self.live_leased.undrained.is_some() {
            return Err(TcpError::new(
                "live write queue is already parked for the shutdown drain",
            ));
        }
        let Some(mut queue) = self.live_writer.take() else {
            return Ok(());
        };
        self.live_leased.worker_wake_registered = false;
        // No flight in the deque is owed a CQE by any queue any more.
        self.live_leased.queue_detached = true;
        // No frame is proven flushed on a queue that no longer exists. Every
        // non-FUA flight still in the deque is covered by a new flush before
        // release, so nothing becomes releasable earlier than before.
        self.live_leased.flush_in_flight = None;
        self.live_leased.flush_submitted_at = None;
        self.live_leased.flushed_through_seq = 0;
        match self.resume_reaper.as_ref() {
            // A full or stopped reaper must not make the worker wait for DMA, so
            // the queue is parked for the shutdown drain instead of dropped.
            Some(reaper) => {
                let parked = match reaper.try_send(queue) {
                    Ok(()) => None,
                    Err(std::sync::mpsc::TrySendError::Full(queue)) => Some(queue),
                    Err(std::sync::mpsc::TrySendError::Disconnected(queue)) => Some(queue),
                };
                match parked {
                    Some(queue) => {
                        // A full or stopped reaper is a capacity condition, not
                        // a break: a transient class would let the episode
                        // reconnect and report success with this queue still
                        // parked, so the worker must fail-stop here. The parked
                        // queue goes to the shutdown drain off this thread and
                        // is never replaced.
                        debug_assert!(self.live_leased.undrained.is_none());
                        self.live_leased.undrained = Some(queue);
                        Err(TcpError::new(
                            "live resume reaper cannot retire the detached queue",
                        ))
                    }
                    None => Ok(()),
                }
            }
            // Fixture path: a local-file queue owns no kernel controller, so its
            // drain completes without waiting.
            None => queue.drain_failed().map_err(|error| {
                TcpError::with_kind(
                    io::ErrorKind::ConnectionAborted,
                    format!("detached live queue drain failed: {error}"),
                )
            }),
        }
    }

    /// Rewrite every retained frame in ascending order, one command at a time.
    /// Sequential order is what keeps a torn frame at the tail: if two reissued
    /// frames were in flight together, the first could fail below a valid
    /// second and leave a hole.
    fn rewrite_retained(&mut self, deadline: Instant) -> Result<u64, TcpError> {
        let retained = std::mem::take(&mut self.live_leased.retained);
        let mut rewritten = 0u64;
        let mut result = Ok(());
        for frame in &retained {
            if let Err(error) = self.rewrite_frame(frame, deadline) {
                result = Err(error);
                break;
            }
            rewritten += 1;
        }
        self.live_leased.retained = retained;
        result.map(|()| rewritten)
    }

    fn rewrite_frame(&mut self, frame: &RetainedFrame, deadline: Instant) -> Result<(), TcpError> {
        // One frame is one round trip: the episode budget bounds the sequence,
        // so a long rewrite never outlives the lease it started under.
        self.remaining_resume(deadline)?;
        let Some(extent) = self.retained_extent(frame.token) else {
            return Err(TcpError::new(
                "live resume retained a frame of an extent that is no longer open",
            ));
        };
        if !self
            .live_writer
            .as_ref()
            .is_some_and(|writer| writer.matches(&extent.binding))
        {
            if self.live_writer.is_some() {
                self.detach_live_queue()?;
            }
            self.open_queue_within(&extent, deadline)?;
        }
        let config = self
            .live_config
            .as_ref()
            .ok_or_else(|| TcpError::new("live configuration missing"))?;
        let frame_deadline = deadline.min(
            Instant::now()
                .checked_add(Duration::from_millis(config.io_timeout_ms))
                .unwrap_or(deadline),
        );
        let sync = if extent.tier == crate::distributed::durability::DurabilityTier::Synchronous
            && frame.durable_at_completion
        {
            LiveWriteSync::Data
        } else {
            LiveWriteSync::None
        };
        let id = self.next_live_io_id()?;
        let offset = frame.start % extent.extent.segment.bytes;
        self.write_frame(
            super::FrameWrite {
                id,
                offset,
                len: frame.frame.len(),
                sync,
                deadline: frame_deadline,
            },
            &|buffer| frame.frame.encode_into(buffer).map_err(TcpError::component),
            None,
        )?;
        #[cfg(test)]
        self.live_leased.rewrites.push(super::ResumeRewrite {
            token: frame.token,
            start: frame.start,
            len: frame.frame.len(),
            id,
        });
        self.harvest_rewrite(id, frame_deadline)
    }

    /// The grant that still owns `token`. Cloned so the client stays mutable
    /// while the queue is rebound; an extent open is a cold path.
    fn retained_extent(&self, token: [u8; 16]) -> Option<LiveLeased> {
        [
            self.live_leased.current.as_ref(),
            self.live_leased.closing.as_ref(),
        ]
        .into_iter()
        .flatten()
        .find(|extent| extent.lease.extent.token == token)
        .map(|extent| extent.lease.clone())
    }

    /// Harvest the CQE of one rewritten frame before the next is submitted.
    fn harvest_rewrite(&mut self, id: u64, deadline: Instant) -> Result<(), TcpError> {
        loop {
            let Some(writer) = self.live_writer.as_mut() else {
                return Err(TcpError::new("live queue missing after a rewrite"));
            };
            // A local sink: the client's completion vector belongs to the
            // flight progress path, and a rewrite must not disturb it.
            let mut completions = Vec::new();
            if let Err(error) = writer.io.progress_into(&mut completions) {
                return Err(self.io_error(error));
            }
            if let Some(completion) = completions.into_iter().find(|entry| entry.id == id) {
                return match completion.result {
                    Ok(LiveIoResult::Written) => Ok(()),
                    Ok(_) => Err(TcpError::new(
                        "live write resume rewrite completed without a write",
                    )),
                    Err(error) => Err(self.io_error(error)),
                };
            }
            if Instant::now() >= deadline {
                return Err(TcpError::with_kind(
                    io::ErrorKind::TimedOut,
                    "live write resume rewrite deadline expired",
                ));
            }
            let Some(writer) = self.live_writer.as_mut() else {
                return Err(TcpError::new("live queue missing after a rewrite"));
            };
            let wait = deadline
                .saturating_duration_since(Instant::now())
                .min(Duration::from_millis(1));
            writer.io.wait(wait).map_err(|error| self.io_error(error))?;
        }
    }

    fn next_live_io_id(&mut self) -> Result<u64, TcpError> {
        let id = self
            .live_leased
            .next_flight_id
            .checked_add(1)
            .ok_or_else(|| TcpError::new("leased flight ID exhausted"))?;
        self.live_leased.next_flight_id = id;
        Ok(id)
    }

    /// A kept queue keeps its deadlines: the break consumed the budget the
    /// frame was granted, so the outstanding work gets one more window. This is
    /// the caller-visible cost of a resume (at most `2 x io_timeout_ms`).
    fn retime_leased_work(&mut self) {
        let Some(timeout_ms) = self.live_config.as_ref().map(|config| config.io_timeout_ms) else {
            return;
        };
        // A test grants the probe an exhausted window so it observes the
        // escalation on its next pass instead of waiting it out on the clock.
        #[cfg(test)]
        let window = self
            .live_leased
            .retime_window
            .unwrap_or(Duration::from_millis(timeout_ms));
        #[cfg(not(test))]
        let window = Duration::from_millis(timeout_ms);
        let now = Instant::now();
        if let Some(deadline) = now.checked_add(window) {
            for flight in &mut self.live_leased.flights {
                if !flight.completed {
                    flight.deadline = deadline;
                }
            }
            if let Some((id, covers, _)) = self.live_leased.flush_in_flight {
                self.live_leased.flush_in_flight = Some((id, covers, deadline));
            }
        }
    }

    /// Declare the recovered frontier and settle the flights it proves written.
    ///
    /// `complete` is true only when every submitted frame is complete on the
    /// target: either this resume rewrote and harvested it, or the owner had
    /// already adopted it. The frontier is then `cursor`; a kept queue proves
    /// only what it already released, so it uses `written_end`.
    ///
    /// The settlement is independent of the declaration exchange: a probe
    /// resets `declared_through` to the owner's frontier, so when the frontier
    /// already covers this prefix no request is sent, and those flights must
    /// still be marked. Their CQEs died with the detached queue, so no later
    /// progress pass can ever complete them.
    fn declare_resumed_adoption(
        &mut self,
        complete: bool,
        deadline: Instant,
    ) -> Result<(), TcpError> {
        for target in [PendingTarget::Closing, PendingTarget::Current] {
            // A skip still needs the budget: the settlement below runs on the
            // proof this step would have refreshed.
            self.remaining_resume(deadline)?;
            let Some(extent) = self.extent_mut(target) else {
                continue;
            };
            let through = if complete {
                extent.cursor
            } else {
                extent.written_end
            };
            let (epoch, token, start, declared) = (
                extent.lease.extent.segment.epoch,
                extent.lease.extent.token,
                extent.lease.extent.start,
                extent.declared_through,
            );
            // A closing extent is closed at its whole written prefix: the close
            // is what moves the owner's open extent to the current one, so the
            // current extent's own declaration is accepted. It is idempotent:
            // when the owner already applied the close, the reply is the closed
            // summary and the extent is retired here.
            let close = target == PendingTarget::Closing;
            let declare = if close { extent.cursor } else { through };
            // `declared_through` is the owner's validated frontier (the probe
            // set it), so a skip below it proves the same prefix the request
            // would have. A close never skips: nothing else retires the extent.
            if declare > declared || close {
                let pending = PendingAdoption {
                    through: declare,
                    close,
                    lease_next: false,
                    target,
                };
                let reply = self.within_resume_budget(deadline, |client| {
                    client.adopt_live_extent(epoch, token, declare, close, false)
                })?;
                self.check_leased_adoption(pending, &reply)?;
                if close {
                    self.recycle_closing()?;
                } else {
                    let Some(extent) = self.extent_mut(target) else {
                        continue;
                    };
                    extent.declared_through = extent.declared_through.max(declare);
                }
            }
            // Adoption reads the file on the owner, independently of this
            // coordinator's CQEs. Marking is not release: the deque still
            // waits for its own barrier or a covering flush.
            //
            // Only a rewritten path may settle past `written_end`: a kept queue
            // still owns the CQE of every uncompleted flight, and marking one
            // would make its own completion an unknown id.
            let frontier = if complete {
                through.max(declared)
            } else {
                through
            };
            for flight in &mut self.live_leased.flights {
                if flight.start >= start && flight.end <= frontier {
                    flight.completed = true;
                }
            }
        }
        Ok(())
    }

    /// Bind a live queue when a detached one left flights behind. The release
    /// path and the covering flush of the synchronous tier both need a live
    /// namespace; the next submission's `ensure_leased_queue` would open the
    /// same queue one turn later, but the release must not wait for it.
    ///
    /// A rewrite opens the queue of the extent it last wrote, which is the
    /// closing extent when the current one had no retained frame. The queue
    /// then owns no DMA, so it is drained and rebound rather than detached.
    fn reopen_detached_queue(&mut self, deadline: Instant) -> Result<(), TcpError> {
        if self.live_leased.flights.is_empty() {
            return Ok(());
        }
        let Some(leased) = self
            .live_leased
            .current
            .as_ref()
            .or(self.live_leased.closing.as_ref())
            .map(|extent| extent.lease.clone())
        else {
            return Ok(());
        };
        if self
            .live_writer
            .as_ref()
            .is_some_and(|writer| writer.matches(&leased.binding))
        {
            return Ok(());
        }
        if self.live_writer.is_some() {
            self.drain_live_queue()?;
        }
        self.open_queue_within(&leased, deadline)
    }
}

/// Classify a queue error without borrowing the client, so it can run while the
/// write queue is mutably borrowed. `errno` is the latest completed write's
/// errno, read by the caller under that borrow.
pub(super) fn classify_io_error(error: LiveIoError, errno: Option<i32>) -> TcpError {
    match error {
        // Classify here, before `io::ErrorKind` folds the errno into
        // `Uncategorized`: a lost device is a resumable break, a rejected
        // command is not.
        LiveIoError::Io(error) if error.raw_os_error().is_some_and(is_transient_errno) => {
            TcpError::with_kind(
                io::ErrorKind::ConnectionAborted,
                format!("live NVMe operation lost its controller: {error}"),
            )
        }
        LiveIoError::Io(error) => TcpError::from(error),
        LiveIoError::Timeout => TcpError::with_kind(
            io::ErrorKind::TimedOut,
            "live NVMe operation deadline expired at submission",
        ),
        LiveIoError::UnknownWrite => match errno {
            Some(errno) if is_transient_errno(errno) => TcpError::with_kind(
                io::ErrorKind::ConnectionAborted,
                format!("leased write failed: the controller is gone, errno {errno}"),
            ),
            Some(errno) => TcpError::with_kind(
                io::ErrorKind::Other,
                format!("leased write failed, errno {errno}"),
            ),
            None => TcpError::new("leased write failed: the outcome is unknown"),
        },
        LiveIoError::Full => TcpError::new("live NVMe write admission full"),
        LiveIoError::Invalid(message) => TcpError::new(message),
        LiveIoError::Cancelled => {
            TcpError::with_kind(io::ErrorKind::Interrupted, "live NVMe operation cancelled")
        }
    }
}
