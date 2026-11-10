//! Bounded, owner-pinned live WAL member reads. Only a dedicated lane worker
//! (or a caller owning its client) may run these blocking control and CQ waits.
//!
//! # Multi-flight lane
//!
//! [`LiveReadCache`] is the state of one read lane: its cached file grants and
//! its requests in flight. The lane worker drives it with four calls and never
//! waits for one request while others could progress:
//!
//! * [`LiveReadCache::submit`] admits a request and starts its I/O. It may run
//!   the blocking owner control exchange and controller connect for a file not
//!   yet granted, but never waits for a CQE.
//! * [`LiveReadCache::poll`] harvests the CQEs of every request, verifies and
//!   decodes their members, starts parked work and returns finished requests
//!   in completion order. It never blocks on I/O.
//! * [`LiveReadCache::wait`] blocks in `poll(2)` on the grant rings and the
//!   lane's [`LiveReadWaker`] until a CQE, a wake or the timeout. It replaces
//!   a `yield_now` spin.
//! * [`LiveReadCache::shutdown`] cancels, drains and closes the lane.
//!
//! In-flight work is bounded by the lane request bound, and for each grant by
//! the `LiveIo` member slots and byte credit. A request larger than the free
//! credit of its grant is admitted in waves as credit returns. Verified member
//! values are slices of the aligned I/O blocks (no copy); a retained value keeps
//! its whole block alive, so a long-lived cache should copy before retaining.
use super::live_control::LiveRecordLocation;
use super::{Client, CompleteBatch, Mutation, TcpError, ValueRecordReference};
use crate::distributed::nvmeof::live_connect::LiveConnectTarget;
use crate::distributed::nvmeof::live_io::{
    LiveCompletion, LiveIo, LiveIoResult, LiveIoStats, LiveSubmitOutcome,
};
use crate::distributed::nvmeof::live_metrics::{
    LiveNamespaceIdentity, LiveNvmeCounters, LiveRouteEvidence, LiveTimingStage,
};
use crate::distributed::nvmeof::live_namespace::LiveNamespaceAccess;
use crate::distributed::nvmeof::live_pool::{LiveControllerLease, LivePoolAcquisition};
use crate::distributed::nvmeof::live_storage::LiveWalSegment;
use crate::distributed::remote_db::ReadTraceChild;
use std::collections::{BTreeMap, HashMap};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::sync::Arc;
use std::time::{Duration, Instant};

pub(crate) const LIVE_READ_CACHE_CAPACITY: usize = 8;
/// Default bound on requests one lane holds in flight.
pub(crate) const LIVE_READ_LANE_REQUESTS: usize = 32;
/// Longest drain in `shutdown`. `LiveIo` aborts the process at 30 s on its own.
const DRAIN_BOUND: Duration = Duration::from_secs(35);
/// Pause before a request retries a controller pool that had no free slot.
const POOL_RETRY: Duration = Duration::from_millis(1);

pub(super) struct LiveReadTiming {
    counters: Arc<crate::distributed::nvmeof::live_metrics::LiveNvmeCounters>,
    stage: LiveTimingStage,
    started: Instant,
}
impl LiveReadTiming {
    pub(super) fn start(
        counters: &Option<Arc<crate::distributed::nvmeof::live_metrics::LiveNvmeCounters>>,
        stage: LiveTimingStage,
    ) -> Option<Self> {
        counters.as_ref().map(|counters| Self {
            counters: counters.clone(),
            stage,
            started: Instant::now(),
        })
    }
}
impl Drop for LiveReadTiming {
    fn drop(&mut self) {
        self.counters.record_timing(
            self.stage,
            self.started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            1,
        );
    }
}

pub(super) fn record_read_io_stats(
    counters: &LiveNvmeCounters,
    before: &LiveIoStats,
    after: &LiveIoStats,
    recovery: bool,
) {
    if after.submissions == before.submissions
        && after.completions == before.completions
        && after.copy_bytes_avoided == before.copy_bytes_avoided
    {
        return;
    }
    let operations = after
        .completed_read_ops
        .saturating_sub(before.completed_read_ops);
    let bytes = after
        .completed_read_bytes
        .saturating_sub(before.completed_read_bytes);
    if recovery {
        counters.record_recovery_read(operations, bytes);
    } else {
        counters.record_read(operations, bytes);
    }
    counters.record_copy_bytes_avoided(
        after
            .copy_bytes_avoided
            .saturating_sub(before.copy_bytes_avoided),
    );
    let ns = |n: u128| n.min(u64::MAX as u128) as u64;
    counters.record_timing(
        LiveTimingStage::Queue,
        ns(after.queue_ns.saturating_sub(before.queue_ns)),
        after.submissions.saturating_sub(before.submissions),
    );
    counters.record_timing(
        LiveTimingStage::Submit,
        ns(after.submit_ns.saturating_sub(before.submit_ns)),
        after.submissions.saturating_sub(before.submissions),
    );
    counters.record_timing(
        LiveTimingStage::Completion,
        ns(after.submit_to_cq_ns.saturating_sub(before.submit_to_cq_ns)),
        after.completions.saturating_sub(before.completions),
    );
}

/// Identifies one request in a lane; unique for the lane's lifetime.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct LiveReadToken(u64);

/// Members of one logical read, in caller order.
pub(crate) struct LiveReadRequest {
    pub(crate) references: Vec<ValueRecordReference>,
    pub(crate) deadline: Instant,
    pub(crate) trace: Option<ReadTraceChild>,
}

#[must_use]
pub(crate) enum LiveReadSubmitError {
    /// The lane holds its maximum requests. The request is returned unchanged;
    /// retry after a `poll` returned finished requests.
    Full(LiveReadRequest),
    Failed(TcpError),
}

/// A finished request. On success the values keep the request's member order.
pub(crate) struct LiveReadDone {
    pub(crate) token: LiveReadToken,
    pub(crate) result: Result<Vec<Mutation>, TcpError>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LiveReadWait {
    /// A CQE is ready to harvest.
    Ready,
    /// A `LiveReadWaker` fired.
    Woken,
    TimedOut,
}

/// Retries of a syscall that returned EINTR before the caller sees it.
const EINTR_RETRIES: usize = 64;

/// Wakes one lane out of `wait` from another thread, for example after the
/// sender queued a new request. Clones share one eventfd.
#[derive(Clone)]
pub(crate) struct LiveReadWaker(Arc<OwnedFd>);
impl LiveReadWaker {
    pub(crate) fn new() -> Result<Self, TcpError> {
        // SAFETY: eventfd takes no pointer arguments; a failure is checked.
        let fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) };
        if fd < 0 {
            return Err(TcpError::component(std::io::Error::last_os_error()));
        }
        // SAFETY: `fd` was just created and nothing else owns it.
        Ok(Self(Arc::new(unsafe { OwnedFd::from_raw_fd(fd) })))
    }
    /// Makes the next or current `wait` return. Wakes coalesce. `Err` means the
    /// eventfd failed: the notification may be lost, so the caller must stop
    /// the lane rather than assume it will wake.
    #[must_use = "a failed wake can leave the lane asleep"]
    pub(crate) fn wake(&self) -> std::io::Result<()> {
        let one = 1u64;
        for _ in 0..EINTR_RETRIES {
            // SAFETY: the descriptor is a live eventfd owned by `self`; the
            // buffer is eight readable bytes.
            let written = unsafe { libc::write(self.0.as_raw_fd(), (&raw const one).cast(), 8) };
            if written == 8 {
                return Ok(());
            }
            let error = std::io::Error::last_os_error();
            match error.kind() {
                std::io::ErrorKind::Interrupted => {}
                // EAGAIN: the counter is saturated, so a wake is already pending.
                std::io::ErrorKind::WouldBlock if written < 0 => return Ok(()),
                _ if written >= 0 => {
                    return Err(std::io::Error::other("short eventfd write"));
                }
                _ => return Err(error),
            }
        }
        Err(std::io::ErrorKind::Interrupted.into())
    }
    fn drain(&self) -> std::io::Result<()> {
        let mut count = 0u64;
        for _ in 0..EINTR_RETRIES {
            // SAFETY: the descriptor is a live non-blocking eventfd owned by
            // `self`; the buffer is eight writable bytes.
            let read = unsafe { libc::read(self.0.as_raw_fd(), (&raw mut count).cast(), 8) };
            if read == 8 {
                return Ok(());
            }
            let error = std::io::Error::last_os_error();
            match error.kind() {
                std::io::ErrorKind::Interrupted => {}
                // EAGAIN: no wake pending, for example one already drained.
                std::io::ErrorKind::WouldBlock if read < 0 => return Ok(()),
                _ if read >= 0 => return Err(std::io::Error::other("short eventfd read")),
                _ => return Err(error),
            }
        }
        Err(std::io::ErrorKind::Interrupted.into())
    }
}

/// Blocks until a ring in `ios` has a CQE, the waker fires or `timeout` ends.
/// At most `LIVE_READ_CACHE_CAPACITY` rings are watched.
fn wait_ready<'a>(
    ios: impl Iterator<Item = &'a mut LiveIo>,
    waker: Option<&LiveReadWaker>,
    timeout: Duration,
) -> Result<LiveReadWait, TcpError> {
    let mut fds = [libc::pollfd {
        fd: -1,
        events: 0,
        revents: 0,
    }; LIVE_READ_CACHE_CAPACITY + 1];
    let mut rings = 0;
    for io in ios.take(LIVE_READ_CACHE_CAPACITY) {
        // A CQE already queued would not wake a poll that starts after it.
        if io.completion_ready() {
            return Ok(LiveReadWait::Ready);
        }
        fds[rings].fd = io.ring_fd();
        fds[rings].events = libc::POLLIN;
        rings += 1;
    }
    let mut count = rings;
    if let Some(waker) = waker {
        fds[count].fd = waker.0.as_raw_fd();
        fds[count].events = libc::POLLIN;
        count += 1;
    }
    let until = Instant::now() + timeout;
    let mut remaining = timeout;
    let mut ready = 0;
    for _ in 0..EINTR_RETRIES {
        let millis = i32::try_from(remaining.as_nanos().div_ceil(1_000_000)).unwrap_or(i32::MAX);
        // SAFETY: `fds[..count]` are initialized pollfd entries for descriptors
        // that stay open for the call (grant rings and the waker eventfd are
        // borrowed from live owners); `count` is at most the array length.
        ready = unsafe { libc::poll(fds.as_mut_ptr(), count as libc::nfds_t, millis) };
        if ready >= 0 {
            break;
        }
        let error = std::io::Error::last_os_error();
        if error.kind() != std::io::ErrorKind::Interrupted {
            return Err(TcpError::component(error));
        }
        // EINTR: a signal, or io_uring task work that posts a CQE. Retry with
        // the time that is left.
        remaining = until.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Ok(LiveReadWait::TimedOut);
        }
    }
    if ready < 0 {
        // A signal storm: let the caller poll the rings and call again.
        return Ok(LiveReadWait::Woken);
    }
    if ready == 0 {
        return Ok(LiveReadWait::TimedOut);
    }
    if fds[..rings].iter().any(|fd| fd.revents != 0) {
        return Ok(LiveReadWait::Ready);
    }
    if let Some(waker) = waker {
        waker.drain().map_err(TcpError::component)?;
    }
    Ok(LiveReadWait::Woken)
}

struct Grant {
    id: u64,
    location: LiveRecordLocation,
    token: [u8; 16],
    io: LiveIo,
    /// Statistics already folded into the lane counters.
    recorded: LiveIoStats,
    /// Lane clock value at the last use; the smallest is evicted first.
    last_used: u64,
    /// Its ring failed: never submit again, retire when drained.
    broken: bool,
    // `io` drops first: the controller lease outlives every descriptor on it.
    // `None` only for grants over local files in tests.
    _lease: Option<Arc<LiveControllerLease>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Slot {
    Pending,
    InFlight,
    Done,
}

struct Flight {
    token: LiveReadToken,
    grant: u64,
    positions: Vec<usize>,
}

struct InFlightRequest {
    references: Vec<ValueRecordReference>,
    deadline: Instant,
    trace: Option<ReadTraceChild>,
    slots: Vec<Slot>,
    output: Vec<Option<Mutation>>,
    /// Members not yet submitted.
    pending: usize,
    /// Index below which no member is `Pending`.
    first_pending: usize,
    /// Flights submitted and not yet harvested.
    flights: usize,
    error: Option<TcpError>,
    submitted_at: Option<Instant>,
    last_cqe: Option<Instant>,
    /// Do not retry admission before this time (controller pool was full).
    retry_at: Option<Instant>,
    connect_since: Option<Instant>,
}
impl InFlightRequest {
    fn fail(&mut self, error: TcpError) {
        if self.error.is_none() {
            self.error = Some(error);
        }
    }
}

enum Opened {
    Grant(usize),
    /// Every grant slot is busy; retry after a flight completes.
    NoCapacity,
    /// The controller pool had no free slot; retry after `retry_at`.
    Retry,
}
enum Group {
    Submitted,
    /// The grant has no credit now; retry after one of its flights completes.
    Blocked,
}
enum ConnectError {
    /// The controller pool has no free slot; may clear soon.
    PoolFull(std::io::Error),
    Failed(TcpError),
}
fn failed(error: impl std::fmt::Display) -> ConnectError {
    ConnectError::Failed(TcpError::component(error))
}

fn grant_matches(segment: &LiveWalSegment, reference: &ValueRecordReference) -> bool {
    segment.epoch == reference.batch_version.epoch
        && segment.partition == reference.batch_address.partition
        && segment.bytes != 0
        && segment.file_index == reference.batch_address.offset / segment.bytes
}

/// The grant to evict: the least recently used one that is idle and healthy.
/// Items are `(index, evictable, last_used)`.
fn lru_victim(grants: impl Iterator<Item = (usize, bool, u64)>) -> Option<usize> {
    grants
        .filter(|&(_, evictable, _)| evictable)
        .min_by_key(|&(_, _, last_used)| last_used)
        .map(|(index, _, _)| index)
}

fn fold_stats(counters: Option<&LiveNvmeCounters>, grant: &mut Grant) {
    let after = grant.io.stats();
    if let Some(counters) = counters {
        record_read_io_stats(counters, &grant.recorded, &after, false);
    }
    grant.recorded = after;
}

/// Lane state: file grants, requests in flight and unreleased owner pins.
///
/// Owner: one lane worker thread, which alone calls these methods. Bounds:
/// `max_requests` requests, `LIVE_READ_CACHE_CAPACITY` grants, and the
/// `LiveIo` credit of each grant. A full lane returns the request to the caller.
pub(crate) struct LiveReadCache {
    grants: Vec<Grant>,
    // A release error is ambiguous: owner revoke can fail while retaining the
    // pin, or a successful response can be lost. Never discard that token.
    unreleased: Vec<[u8; 16]>,
    /// In submission order, which is the order parked work gets credit.
    requests: BTreeMap<LiveReadToken, InFlightRequest>,
    flights: HashMap<u64, Flight>,
    /// Requests with at least one flight outstanding.
    active_requests: usize,
    max_requests: usize,
    next_token: u64,
    next_flight: u64,
    next_grant: u64,
    clock: u64,
    waker: Option<LiveReadWaker>,
    scratch: Vec<LiveReadToken>,
}
impl Default for LiveReadCache {
    fn default() -> Self {
        Self::with_max_requests(LIVE_READ_LANE_REQUESTS)
    }
}

impl LiveReadCache {
    pub(crate) fn with_max_requests(max_requests: usize) -> Self {
        Self {
            grants: Vec::with_capacity(LIVE_READ_CACHE_CAPACITY),
            unreleased: Vec::new(),
            requests: BTreeMap::new(),
            flights: HashMap::new(),
            active_requests: 0,
            max_requests,
            next_token: 1,
            next_flight: 1,
            next_grant: 1,
            clock: 0,
            waker: None,
            scratch: Vec::with_capacity(max_requests),
        }
    }

    /// Requests admitted and not yet returned by `poll`.
    pub(crate) fn in_flight(&self) -> usize {
        self.requests.len()
    }

    /// The lane's waker. Create it once; every clone wakes `wait`.
    pub(crate) fn waker(&mut self) -> Result<LiveReadWaker, TcpError> {
        if let Some(waker) = &self.waker {
            return Ok(waker.clone());
        }
        let waker = LiveReadWaker::new()?;
        self.waker = Some(waker.clone());
        Ok(waker)
    }

    /// Admits `request` and starts its I/O for every member whose grant has
    /// credit. Remaining members start from `poll` as credit returns. A request
    /// that fails admission is returned by `poll` with its error.
    pub(crate) fn submit(
        &mut self,
        client: &mut Client,
        epoch: u64,
        request: LiveReadRequest,
    ) -> Result<LiveReadToken, LiveReadSubmitError> {
        if self.requests.len() >= self.max_requests {
            return Err(LiveReadSubmitError::Full(request));
        }
        if !self.unreleased.is_empty()
            && let Err(error) = self.release_pending(client, epoch)
        {
            if let Some(counters) = &client.live_counters {
                counters.record_error();
            }
            return Err(LiveReadSubmitError::Failed(error));
        }
        let token = LiveReadToken(self.next_token);
        self.next_token += 1;
        let members = request.references.len();
        let mut state = InFlightRequest {
            references: request.references,
            deadline: request.deadline,
            trace: request.trace,
            slots: vec![Slot::Pending; members],
            output: (0..members).map(|_| None).collect(),
            pending: members,
            first_pending: 0,
            flights: 0,
            error: None,
            submitted_at: None,
            last_cqe: None,
            retry_at: None,
            connect_since: None,
        };
        self.advance(client, epoch, token, &mut state);
        self.requests.insert(token, state);
        Ok(token)
    }

    /// Harvests every ready CQE, starts parked work and appends finished
    /// requests to `done`. `Err` means the lane state is inconsistent: stop it.
    pub(crate) fn poll(
        &mut self,
        client: &mut Client,
        epoch: u64,
        done: &mut Vec<LiveReadDone>,
    ) -> Result<(), TcpError> {
        if self.requests.is_empty() {
            return Ok(());
        }
        for index in 0..self.grants.len() {
            if self.grants[index].io.outstanding() == 0 {
                continue;
            }
            match self.grants[index].io.progress() {
                Ok(completions) => {
                    fold_stats(client.live_counters.as_deref(), &mut self.grants[index]);
                    for completion in completions {
                        self.complete_flight(client, completion)?;
                    }
                }
                Err(error) => self.fail_grant(client, index, &error),
            }
        }
        self.retire_broken(client, epoch);
        let mut tokens = std::mem::take(&mut self.scratch);
        tokens.clear();
        tokens.extend(self.requests.keys().copied());
        for &token in &tokens {
            let Some(mut state) = self.requests.remove(&token) else {
                continue;
            };
            self.advance(client, epoch, token, &mut state);
            if state.flights == 0 && (state.error.is_some() || state.pending == 0) {
                done.push(Self::finish(client, token, state));
            } else {
                self.requests.insert(token, state);
            }
        }
        self.scratch = tokens;
        Ok(())
    }

    /// Blocks until a CQE is ready, a waker fires or `max` passes, whichever is
    /// first. It never spins: with nothing in flight it sleeps in `poll(2)`.
    /// The timeout also stops at the nearest deadline or retry time of a
    /// request that still has members to start. Pass a `max` of at most a
    /// second: `LiveIo` expires overdue operations from its progress calls.
    /// EINTR is retried; any other `poll(2)` or eventfd failure is returned and
    /// means the lane cannot be trusted to wake: stop it.
    pub(crate) fn wait(&mut self, max: Duration) -> Result<LiveReadWait, TcpError> {
        let now = Instant::now();
        let mut timeout = max;
        for state in self.requests.values() {
            if state.error.is_none() && state.pending > 0 {
                let at = state
                    .retry_at
                    .map_or(state.deadline, |retry| retry.min(state.deadline));
                timeout = timeout.min(at.saturating_duration_since(now));
            }
        }
        wait_ready(
            self.grants
                .iter_mut()
                .filter(|grant| grant.io.outstanding() > 0)
                .map(|grant| &mut grant.io),
            self.waker.as_ref(),
            timeout,
        )
    }

    /// Cancels every request and waits until all their I/O retired. The failed
    /// requests are appended to `done`. The lane stays usable afterwards.
    pub(crate) fn drain(
        &mut self,
        client: &mut Client,
        epoch: u64,
        done: &mut Vec<LiveReadDone>,
    ) -> Result<(), TcpError> {
        let tokens: Vec<LiveReadToken> = self.requests.keys().copied().collect();
        for token in tokens {
            if let Some(state) = self.requests.get_mut(&token) {
                state.fail(TcpError::new("live read lane is closing"));
            }
            self.cancel_flights(token);
        }
        let bound = Instant::now() + DRAIN_BOUND;
        while !self.requests.is_empty() {
            self.poll(client, epoch, done)?;
            if self.requests.is_empty() {
                break;
            }
            if Instant::now() >= bound {
                return Err(TcpError::new("live read lane drain timed out"));
            }
            self.wait(Duration::from_millis(100))?;
        }
        Ok(())
    }

    /// `drain`, then `close`.
    pub(crate) fn shutdown(
        &mut self,
        client: &mut Client,
        epoch: u64,
        done: &mut Vec<LiveReadDone>,
    ) -> Result<(), TcpError> {
        self.drain(client, epoch, done)?;
        self.close(client, epoch)
    }

    /// Closes every grant and releases the owner pins. Requires an idle lane.
    pub(crate) fn close(&mut self, client: &mut Client, epoch: u64) -> Result<(), TcpError> {
        if !self.requests.is_empty() {
            return Err(TcpError::new("live read lane has requests in flight"));
        }
        for grant in self.grants.drain(..) {
            let token = grant.token;
            drop(grant);
            self.unreleased.push(token);
        }
        self.release_pending(client, epoch)
    }

    pub(crate) fn take_unreleased(&mut self) -> Vec<[u8; 16]> {
        std::mem::take(&mut self.unreleased)
    }

    fn retire(&mut self, client: &mut Client, epoch: u64, index: usize) -> Result<(), TcpError> {
        let grant = self.grants.remove(index);
        let token = grant.token;
        // No operation remains: only drained grants are retired, and a drained
        // ring has returned every CQE.
        drop(grant);
        self.unreleased.push(token);
        self.release_pending(client, epoch)
    }
    fn release_pending(&mut self, client: &mut Client, epoch: u64) -> Result<(), TcpError> {
        let mut first = None;
        let mut index = 0;
        while index < self.unreleased.len() {
            if let Some(counters) = &client.live_counters {
                counters.record_metadata_request();
            }
            let _control = LiveReadTiming::start(&client.live_counters, LiveTimingStage::Control);
            match client.release_live_epoch_pin(epoch, self.unreleased[index]) {
                Ok(()) => {
                    self.unreleased.remove(index);
                }
                Err(error) => {
                    if first.is_none() {
                        first = Some(error);
                    }
                    index += 1;
                }
            }
        }
        match first {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    fn find_grant(&self, reference: &ValueRecordReference) -> Option<usize> {
        self.grants
            .iter()
            .position(|grant| !grant.broken && grant_matches(&grant.location.segment, reference))
    }

    /// Starts as many of the request's pending members as credit allows. A
    /// failure is recorded on the request and its flights are cancelled.
    fn advance(
        &mut self,
        client: &mut Client,
        epoch: u64,
        token: LiveReadToken,
        state: &mut InFlightRequest,
    ) {
        if state.error.is_some() || state.pending == 0 {
            return;
        }
        let now = Instant::now();
        if state.retry_at.is_some_and(|at| now < at) {
            return;
        }
        state.retry_at = None;
        let failure = if now >= state.deadline {
            Some(TcpError::new("live read deadline expired"))
        } else {
            self.admit(client, epoch, token, state).err()
        };
        if let Some(error) = failure {
            state.fail(error);
            self.cancel_flights(token);
        }
    }

    fn admit(
        &mut self,
        client: &mut Client,
        epoch: u64,
        token: LiveReadToken,
        state: &mut InFlightRequest,
    ) -> Result<(), TcpError> {
        let (io_timeout, connect_timeout) = match &client.live_config {
            Some(config) => (
                Duration::from_millis(config.io_timeout_ms),
                Duration::from_millis(config.connect_timeout_ms),
            ),
            None => return Err(TcpError::new("live NVMe is not configured")),
        };
        let mut blocked: Vec<u64> = Vec::new();
        let mut no_new_grant = false;
        // Each pass starts members, blocks one grant, or stops new grants.
        for _ in 0..state.references.len() + LIVE_READ_CACHE_CAPACITY + 2 {
            let Some((position, found)) = self.next_group(state, &blocked, no_new_grant) else {
                return Ok(());
            };
            let index = if let Some(index) = found {
                if let Some(counters) = &client.live_counters {
                    counters.record_grant_hit();
                }
                index
            } else {
                match self.open_grant(client, epoch, state, position, connect_timeout)? {
                    Opened::Grant(index) => index,
                    Opened::NoCapacity => {
                        no_new_grant = true;
                        continue;
                    }
                    Opened::Retry => return Ok(()),
                }
            };
            match self.submit_group(client, state, token, index, io_timeout)? {
                Group::Submitted => {}
                Group::Blocked => blocked.push(self.grants[index].id),
            }
        }
        Err(TcpError::new("live read admission did not converge"))
    }

    /// The first pending member whose group can be tried: its grant, if any, is
    /// not blocked, and a new grant is allowed when it has none.
    fn next_group(
        &self,
        state: &InFlightRequest,
        blocked: &[u64],
        no_new_grant: bool,
    ) -> Option<(usize, Option<usize>)> {
        for position in state.first_pending..state.slots.len() {
            if state.slots[position] != Slot::Pending {
                continue;
            }
            match self.find_grant(&state.references[position]) {
                Some(index) if !blocked.contains(&self.grants[index].id) => {
                    return Some((position, Some(index)));
                }
                None if !no_new_grant => return Some((position, None)),
                _ => {}
            }
        }
        None
    }

    /// Resolves the owner grant for `position`, connects its controller and
    /// caches it. An idle least recently used grant makes room when full.
    fn open_grant(
        &mut self,
        client: &mut Client,
        epoch: u64,
        state: &mut InFlightRequest,
        position: usize,
        connect_timeout: Duration,
    ) -> Result<Opened, TcpError> {
        if self.grants.len() >= LIVE_READ_CACHE_CAPACITY {
            let victim = lru_victim(self.grants.iter().enumerate().map(|(index, grant)| {
                (
                    index,
                    !grant.broken && grant.io.outstanding() == 0,
                    grant.last_used,
                )
            }));
            let Some(victim) = victim else {
                return Ok(Opened::NoCapacity);
            };
            self.retire(client, epoch, victim)?;
            if let Some(counters) = &client.live_counters {
                counters.record_grant_eviction();
            }
        }
        if let Some(counters) = &client.live_counters {
            counters.record_grant_miss();
            counters.record_metadata_request();
        }
        if let Some(trace) = &state.trace {
            trace.event("live_resolve_started");
        }
        let locations = {
            let _control = LiveReadTiming::start(&client.live_counters, LiveTimingStage::Control);
            client.resolve_live_records(epoch, std::slice::from_ref(&state.references[position]))?
        };
        if let Some(trace) = &state.trace {
            trace.event("live_resolve_completed");
        }
        let pin = locations.epoch_pin_token;
        let Some(location) = locations.entries.into_iter().next() else {
            self.unreleased.push(pin);
            self.release_pending(client, epoch)?;
            return Err(TcpError::new("missing live location"));
        };
        let since = *state.connect_since.get_or_insert_with(Instant::now);
        let connect_deadline = state.deadline.min(since + connect_timeout);
        let id = self.next_grant;
        match Self::connect_grant(client, location, pin, id, connect_deadline) {
            Ok(mut grant) => {
                self.next_grant += 1;
                self.clock += 1;
                grant.last_used = self.clock;
                state.connect_since = None;
                self.grants.push(grant);
                Ok(Opened::Grant(self.grants.len() - 1))
            }
            Err(error) => {
                // The owner pin is released; a retry resolves a fresh one.
                self.unreleased.push(pin);
                self.release_pending(client, epoch)?;
                match error {
                    ConnectError::PoolFull(_) if Instant::now() < connect_deadline => {
                        // Other lanes retire idle grants on their own. Retry
                        // later instead of sleeping with I/O in flight.
                        state.retry_at = Some(Instant::now() + POOL_RETRY);
                        Ok(Opened::Retry)
                    }
                    ConnectError::PoolFull(error) => Err(TcpError::component(error)),
                    ConnectError::Failed(error) => Err(error),
                }
            }
        }
    }

    fn connect_grant(
        client: &Client,
        location: LiveRecordLocation,
        token: [u8; 16],
        id: u64,
        connect_deadline: Instant,
    ) -> Result<Grant, ConnectError> {
        let (Some(config), Some(pool)) = (&client.live_config, &client.live_pool) else {
            return Err(ConnectError::Failed(TcpError::new(
                "live controller pool unavailable",
            )));
        };
        let binding = &location.binding;
        let target = LiveConnectTarget {
            nqn: binding.nqn.clone(),
            uuid: binding.uuid,
            nsid: binding.nsid,
            bytes: binding.bytes,
            block_size: binding.block_size,
            address: binding.address.clone(),
            service: binding.service,
            transport: binding.transport,
            spdk: binding.spdk.clone(),
        };
        let access = if binding.readonly {
            LiveNamespaceAccess::ReadOnly
        } else {
            LiveNamespaceAccess::ReadWrite
        };
        let setup = LiveReadTiming::start(&client.live_counters, LiveTimingStage::NamespaceSetup);
        let (lease, acquisition, timings) = pool
            .acquire(
                &target,
                &config.host_nqn,
                config.host_id,
                access,
                connect_deadline,
            )
            .map_err(|error| {
                if error.kind() == std::io::ErrorKind::WouldBlock {
                    ConnectError::PoolFull(error)
                } else {
                    ConnectError::Failed(TcpError::component(error))
                }
            })?;
        let namespace = lease.open_namespace().map_err(failed)?;
        drop(setup);
        if let Some(counters) = &client.live_counters {
            let ns = |d: Duration| d.as_nanos().min(u128::from(u64::MAX)) as u64;
            if let Some(connect) = timings.connect {
                counters.record_timing(LiveTimingStage::ControllerConnect, ns(connect), 1);
            }
            if let Some(disconnect) = timings.disconnect {
                counters.record_timing(LiveTimingStage::ControllerDisconnect, ns(disconnect), 1);
            }
            if matches!(acquisition, LivePoolAcquisition::Connected) {
                counters.record_namespace_connect();
            }
        }
        if let Some(metrics) = &client.live_metrics {
            let observed = lease.binding();
            metrics.record_route(LiveRouteEvidence {
                transport: observed.transport,
                address: binding.address.clone(),
                service: binding.service,
                host_nqn: config.host_nqn.clone(),
                host_id: config.host_id,
                target_implementation:
                    crate::distributed::nvmeof::live_metrics::target_implementation(&observed.spdk),
                spdk: observed.spdk.clone(),
                writer: None,
                namespace_identity: LiveNamespaceIdentity {
                    namespace_path: observed.namespace_path.clone(),
                    controller_instance: lease.controller_instance(),
                    nqn: observed.nqn.clone(),
                    uuid: observed.uuid,
                    nsid: observed.nsid,
                    bytes: observed.bytes,
                    block_size: observed.block_size,
                    target_implementation:
                        crate::distributed::nvmeof::live_metrics::target_implementation(
                            &observed.spdk,
                        ),
                },
            });
        }
        let io = LiveIo::new(
            namespace,
            config.max_inflight_requests,
            config.max_inflight_bytes,
        )
        .map_err(failed)?;
        let recorded = io.stats();
        Ok(Grant {
            id,
            location,
            token,
            io,
            recorded,
            last_used: 0,
            broken: false,
            _lease: Some(lease),
        })
    }

    /// Selects the pending members of one grant's file that fit its free credit
    /// and submits them as one flight.
    fn submit_group(
        &mut self,
        client: &Client,
        state: &mut InFlightRequest,
        token: LiveReadToken,
        index: usize,
        io_timeout: Duration,
    ) -> Result<Group, TcpError> {
        self.clock += 1;
        let grant = &mut self.grants[index];
        grant.last_used = self.clock;
        let (free_slots, free_bytes) = grant.io.credit();
        if free_slots == 0 {
            return Ok(Group::Blocked);
        }
        let (slot_capacity, byte_capacity) = grant.io.capacity();
        let segment = &grant.location.segment;
        let mut selected = Vec::new();
        let mut ranges = Vec::new();
        let mut charge = 0usize;
        {
            let _validation =
                LiveReadTiming::start(&client.live_counters, LiveTimingStage::Validation);
            for position in state.first_pending..state.slots.len() {
                if state.slots[position] != Slot::Pending {
                    continue;
                }
                let reference = &state.references[position];
                if !grant_matches(segment, reference) {
                    continue;
                }
                let offset = (reference.batch_address.offset % segment.bytes)
                    .checked_add(8)
                    .and_then(|n| n.checked_add(u64::from(reference.descriptor.offset)))
                    .ok_or_else(|| TcpError::new("live record offset overflow"))?;
                if !client.valid_live_record_geometry(
                    segment,
                    &grant.location.binding,
                    reference,
                    offset,
                    u64::from(reference.descriptor.len),
                ) {
                    return Err(TcpError::new("invalid cached live record geometry"));
                }
                let len = reference.descriptor.len as usize;
                let cost = grant
                    .io
                    .read_charge(offset, len)
                    .map_err(TcpError::component)?;
                let Some(next) = charge.checked_add(cost) else {
                    return Err(TcpError::new("live read charge overflow"));
                };
                if selected.len() == free_slots || next > free_bytes {
                    break;
                }
                charge = next;
                selected.push(position);
                ranges.push((offset, len));
            }
        }
        if ranges.is_empty() {
            if free_slots == slot_capacity && free_bytes == byte_capacity {
                return Err(TcpError::new("live record exceeds queue capacity"));
            }
            return Ok(Group::Blocked);
        }
        let flight = self.next_flight;
        let deadline = state.deadline.min(Instant::now() + io_timeout);
        match grant
            .io
            .submit_reads_traced(flight, &ranges, deadline, state.trace.as_ref())
            .map_err(TcpError::component)?
        {
            LiveSubmitOutcome::Submitted => {}
            LiveSubmitOutcome::Full => return Ok(Group::Blocked),
        }
        self.next_flight += 1;
        fold_stats(client.live_counters.as_deref(), grant);
        for &position in &selected {
            state.slots[position] = Slot::InFlight;
        }
        state.pending -= selected.len();
        while state.first_pending < state.slots.len()
            && state.slots[state.first_pending] != Slot::Pending
        {
            state.first_pending += 1;
        }
        if state.flights == 0 {
            self.active_requests += 1;
        }
        state.flights += 1;
        state.submitted_at.get_or_insert_with(Instant::now);
        self.flights.insert(
            flight,
            Flight {
                token,
                grant: grant.id,
                positions: selected,
            },
        );
        if let Some(counters) = &client.live_counters {
            let operations: usize = self.grants.iter().map(|g| g.io.outstanding()).sum();
            counters.record_lane_inflight(self.active_requests as u64, operations as u64);
        }
        Ok(Group::Submitted)
    }

    /// Cancels the outstanding flights of `token`. Each still returns its CQE
    /// through `poll`; cancellation never frees a buffer early.
    fn cancel_flights(&mut self, token: LiveReadToken) {
        for (&flight, entry) in &self.flights {
            if entry.token == token
                && let Some(grant) = self.grants.iter_mut().find(|g| g.id == entry.grant)
            {
                grant.io.cancel(flight);
            }
        }
    }

    /// Books the end of one flight of `token`; `None` if the request is gone.
    fn release_flight(&mut self, token: LiveReadToken) -> Option<&mut InFlightRequest> {
        let state = self.requests.get_mut(&token)?;
        state.flights -= 1;
        if state.flights == 0 {
            self.active_requests -= 1;
        }
        Some(state)
    }

    fn complete_flight(
        &mut self,
        client: &Client,
        completion: LiveCompletion,
    ) -> Result<(), TcpError> {
        let Some(flight) = self.flights.remove(&completion.id) else {
            return Err(TcpError::new("unexpected live read completion"));
        };
        let Some(state) = self.release_flight(flight.token) else {
            return Err(TcpError::new("live read completion without a request"));
        };
        state.last_cqe = Some(completion.completed_at);
        let mut cancel = false;
        match completion.result {
            Ok(LiveIoResult::Reads(records)) => {
                if state.error.is_none()
                    && let Err(error) =
                        Self::decode_flight(client, state, &flight.positions, records)
                {
                    state.fail(error);
                    cancel = true;
                }
            }
            Ok(LiveIoResult::Written) => {
                state.fail(TcpError::new("unexpected live write completion"));
                cancel = true;
            }
            Err(error) => {
                cancel = state.error.is_none();
                state.fail(TcpError::component(error));
            }
        }
        if cancel {
            self.cancel_flights(flight.token);
        }
        Ok(())
    }

    fn decode_flight(
        client: &Client,
        state: &mut InFlightRequest,
        positions: &[usize],
        records: Vec<minibytes::Bytes>,
    ) -> Result<(), TcpError> {
        if records.len() != positions.len() {
            return Err(TcpError::new("live read count mismatch"));
        }
        let _validation = LiveReadTiming::start(&client.live_counters, LiveTimingStage::Validation);
        for (&position, bytes) in positions.iter().zip(records) {
            let mutation =
                CompleteBatch::decode_record(&client.shape, &state.references[position], bytes)
                    .map_err(TcpError::component)?;
            state.output[position] = Some(mutation);
            state.slots[position] = Slot::Done;
        }
        if let Some(trace) = &state.trace {
            trace.event("live_validation_completed");
        }
        Ok(())
    }

    /// A ring error: drain the grant, fail its flights and stop using it.
    fn fail_grant(&mut self, client: &Client, index: usize, error: &impl std::fmt::Display) {
        let message = error.to_string();
        let grant = &mut self.grants[index];
        // Drained completions are dropped: every flight on this grant fails.
        // If the drain itself fails the grant stays, undroppable work and all.
        let _ = grant.io.shutdown();
        grant.broken = true;
        fold_stats(client.live_counters.as_deref(), grant);
        let id = grant.id;
        let failed: Vec<u64> = self
            .flights
            .iter()
            .filter(|(_, flight)| flight.grant == id)
            .map(|(&flight, _)| flight)
            .collect();
        for flight in failed {
            let Some(entry) = self.flights.remove(&flight) else {
                continue;
            };
            if let Some(state) = self.release_flight(entry.token) {
                state.fail(TcpError::new(message.clone()));
            }
        }
    }

    /// Retires drained broken grants so the next request resolves a fresh one.
    /// A failed pin release stays in `unreleased` for the next attempt.
    fn retire_broken(&mut self, client: &mut Client, epoch: u64) {
        for index in (0..self.grants.len()).rev() {
            let grant = &self.grants[index];
            if grant.broken && grant.io.outstanding() == 0 {
                let _ = self.retire(client, epoch, index);
            }
        }
    }

    fn finish(client: &Client, token: LiveReadToken, state: InFlightRequest) -> LiveReadDone {
        let now = Instant::now();
        let counters = client.live_counters.as_deref();
        if let Some(counters) = counters {
            let ns = |d: Duration| d.as_nanos().min(u128::from(u64::MAX)) as u64;
            if let Some(started) = state.submitted_at {
                counters.record_timing(
                    LiveTimingStage::ReadIo,
                    ns(now.saturating_duration_since(started)),
                    1,
                );
            }
            if let Some(cqe) = state.last_cqe {
                counters.record_timing(
                    LiveTimingStage::Delivery,
                    ns(now.saturating_duration_since(cqe)),
                    1,
                );
            }
        }
        let result = match state.error {
            Some(error) => Err(error),
            None => state
                .output
                .into_iter()
                .map(|value| value.ok_or_else(|| TcpError::new("missing live read result")))
                .collect(),
        };
        if result.is_err()
            && let Some(counters) = counters
        {
            counters.record_error();
        }
        LiveReadDone { token, result }
    }
}

impl Client {
    /// Blocking read of `references` through `cache`. For callers that own a
    /// lane exclusively and keep one request at a time; lane workers use the
    /// multi-flight calls of [`LiveReadCache`] instead.
    pub(crate) fn live_read_records(
        &mut self,
        cache: &mut LiveReadCache,
        epoch: u64,
        references: &[ValueRecordReference],
        deadline: Instant,
        trace: Option<&ReadTraceChild>,
    ) -> Result<Vec<Mutation>, TcpError> {
        let request = LiveReadRequest {
            references: references.to_vec(),
            deadline,
            trace: trace.cloned(),
        };
        let token = match cache.submit(self, epoch, request) {
            Ok(token) => token,
            Err(LiveReadSubmitError::Full(_)) => {
                return Err(TcpError::new("live read lane is full"));
            }
            Err(LiveReadSubmitError::Failed(error)) => return Err(error),
        };
        let give_up = deadline + DRAIN_BOUND;
        let mut done = Vec::new();
        while Instant::now() < give_up {
            cache.poll(self, epoch, &mut done)?;
            if let Some(position) = done.iter().position(|d| d.token == token) {
                return done.swap_remove(position).result;
            }
            cache.wait(Duration::from_millis(100))?;
        }
        Err(TcpError::new("live read did not finish"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed::nvmeof::live_namespace::LiveNamespace;
    use crate::distributed::types::PhysicalValueAddress;
    use std::io::{Seek, SeekFrom, Write};

    #[test]
    fn local_file_reads_decode_order_duplicates_and_corruption() {
        let dir = tempdir::TempDir::new("live-read-members").unwrap();
        let config = super::super::tests::config(dir.path());
        let shape = crate::key_shape::KeyShape::from_yaml(&config.shape_yaml).unwrap();
        let first = super::super::tests::batch(&config, 1, 0);
        let second = super::super::tests::batch(&config, 1, 1);
        let address = |offset, batch: &CompleteBatch| PhysicalValueAddress {
            partition: 2,
            segment_generation: 1,
            offset,
            len: (batch.encoded().len() + 8) as u32,
        };
        let a = first.record_reference(address(0, &first), 0).unwrap();
        let b = second.record_reference(address(2048, &second), 0).unwrap();
        let path = dir.path().join("wal");
        let mut file = std::fs::File::create(&path).unwrap();
        file.set_len(8192).unwrap();
        file.seek(SeekFrom::Start(8)).unwrap();
        file.write_all(first.encoded()).unwrap();
        file.seek(SeekFrom::Start(2056)).unwrap();
        file.write_all(second.encoded()).unwrap();
        drop(file);
        let namespace =
            LiveNamespace::open_local_file(&path, 8192, 4096, LiveNamespaceAccess::ReadOnly)
                .unwrap();
        let mut io = LiveIo::new(namespace, 4, 65536).unwrap();
        let refs = [b, a, b];
        let ranges: Vec<_> = refs
            .iter()
            .map(|r| {
                (
                    r.batch_address.offset + 8 + u64::from(r.descriptor.offset),
                    r.descriptor.len as usize,
                )
            })
            .collect();
        let deadline = Instant::now() + Duration::from_secs(10);
        assert_eq!(
            io.submit_reads(1, &ranges, deadline).unwrap(),
            LiveSubmitOutcome::Submitted
        );
        let completion = loop {
            if let Some(completion) = io.progress().unwrap().pop() {
                break completion;
            }
            assert!(Instant::now() < deadline);
            std::thread::yield_now();
        };
        let LiveIoResult::Reads(records) = completion.result.unwrap() else {
            panic!("expected member reads")
        };
        let values: Vec<_> = refs
            .iter()
            .zip(&records)
            .map(|(reference, bytes)| {
                CompleteBatch::decode_record(&shape, reference, bytes.clone()).unwrap()
            })
            .collect();
        assert_eq!(
            values,
            vec![
                second.mutations()[0].clone(),
                first.mutations()[0].clone(),
                second.mutations()[0].clone()
            ]
        );
        let mut corrupt = records[0].to_vec();
        let last = corrupt.len() - 1;
        corrupt[last] ^= 1;
        assert!(CompleteBatch::decode_record(&shape, &b, corrupt.into()).is_err());
        assert!(io.shutdown().unwrap().is_empty());
    }

    #[test]
    fn least_recently_used_idle_grant_is_the_victim() {
        // (index, evictable, last_used): a busy or broken grant is never chosen,
        // and a recently used one outlives an older one.
        let grants = [(0, true, 5), (1, true, 2), (2, false, 1), (3, true, 9)];
        assert_eq!(lru_victim(grants.into_iter()), Some(1));
        assert_eq!(lru_victim([(0, false, 1), (1, false, 2)].into_iter()), None);
        assert_eq!(lru_victim(std::iter::empty()), None);
    }

    fn thread_cpu() -> Duration {
        let mut now = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        // SAFETY: `now` is a valid timespec and the clock id is a constant.
        assert_eq!(
            unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut now) },
            0
        );
        Duration::new(now.tv_sec as u64, now.tv_nsec as u32)
    }

    #[test]
    fn idle_wait_blocks_without_spinning_and_a_waker_ends_it() {
        let mut cache = LiveReadCache::default();
        let cpu = thread_cpu();
        let started = Instant::now();
        assert_eq!(
            cache.wait(Duration::from_millis(200)).unwrap(),
            LiveReadWait::TimedOut
        );
        assert!(started.elapsed() >= Duration::from_millis(190));
        // A spin would use about 200 ms of CPU; a sleeping thread uses almost none.
        assert!(thread_cpu() - cpu < Duration::from_millis(40));

        let waker = cache.waker().unwrap();
        let sender = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            waker.wake().unwrap();
        });
        let started = Instant::now();
        assert_eq!(
            cache.wait(Duration::from_secs(10)).unwrap(),
            LiveReadWait::Woken
        );
        assert!(started.elapsed() < Duration::from_secs(5));
        sender.join().unwrap();
        // The wake was consumed: the next wait blocks again.
        assert_eq!(
            cache.wait(Duration::from_millis(30)).unwrap(),
            LiveReadWait::TimedOut
        );
    }

    struct Lane {
        _dir: tempdir::TempDir,
        client: Client,
        cache: LiveReadCache,
        metrics: Arc<crate::distributed::nvmeof::live_metrics::LiveNvmeMetrics>,
        first: CompleteBatch,
        second: CompleteBatch,
        a: ValueRecordReference,
        b: ValueRecordReference,
        // Keeps the node behind the client alive.
        _running: super::super::tests::multiplexed::Running,
    }

    /// A lane over two local-file grants: `a` lives in file 0, `b` in file 1.
    fn lane(slots: usize) -> Lane {
        use crate::distributed::nvmeof::NvmeofTransport;
        use crate::distributed::nvmeof::live_metrics::LiveNvmeMetrics;
        use crate::distributed::nvmeof::live_target::LiveTargetBinding;
        use crate::distributed::storage::WalMemberReadPolicy;
        let running = super::super::tests::multiplexed::Running::new(
            super::super::Transport::Tcp,
            WalMemberReadPolicy::Async,
        );
        let dir = tempdir::TempDir::new("live-lane").unwrap();
        let first = super::super::tests::batch(&running.config, 1, 0);
        let second = super::super::tests::batch(&running.config, 1, 1);
        let address = |offset, batch: &CompleteBatch| PhysicalValueAddress {
            partition: 2,
            segment_generation: 1,
            offset,
            len: (batch.encoded().len() + 8) as u32,
        };
        let a = first.record_reference(address(0, &first), 0).unwrap();
        let b = second.record_reference(address(8192, &second), 0).unwrap();
        let mut client = running.client();
        let metrics = LiveNvmeMetrics::new();
        client.live_counters = Some(metrics.register().unwrap());
        client.live_config = Some(super::super::live_client::LiveClientConfig {
            host_nqn: "nqn.2026-01.test:host".into(),
            host_id: [1; 16],
            max_inflight_requests: slots,
            max_inflight_bytes: 16 * (4096 + 4095),
            connect_timeout_ms: 1000,
            io_timeout_ms: 10_000,
        });
        let mut cache = LiveReadCache::with_max_requests(4);
        for (file, (batch, reference)) in [(&first, a), (&second, b)].into_iter().enumerate() {
            let path = dir.path().join(format!("wal-{file}"));
            let mut data = std::fs::File::create(&path).unwrap();
            data.set_len(8192).unwrap();
            data.seek(SeekFrom::Start(8)).unwrap();
            data.write_all(batch.encoded()).unwrap();
            drop(data);
            let namespace =
                LiveNamespace::open_local_file(&path, 8192, 4096, LiveNamespaceAccess::ReadOnly)
                    .unwrap();
            let binding = LiveTargetBinding {
                nqn: "nqn.2026-01.test:wal".into(),
                uuid: [1; 16],
                nsid: 1,
                bytes: 8192,
                block_size: 4096,
                address: "127.0.0.1".into(),
                service: 4420,
                transport: NvmeofTransport::Tcp,
                readonly: true,
                spdk: None,
            };
            let segment = LiveWalSegment {
                database: [51; 16],
                partition: 2,
                epoch: 1,
                generation: 1,
                file_index: file as u64,
                path,
                bytes: 8192,
                device: 0,
                inode: 0,
                block_size: 4096,
            };
            let io = LiveIo::new(namespace, slots, 16 * (4096 + 4095)).unwrap();
            let recorded = io.stats();
            cache.grants.push(Grant {
                id: file as u64 + 1,
                location: LiveRecordLocation {
                    reference,
                    segment,
                    binding,
                    offset: 0,
                    len: 0,
                },
                token: [file as u8; 16],
                io,
                recorded,
                last_used: 0,
                broken: false,
                _lease: None,
            });
        }
        Lane {
            _dir: dir,
            client,
            cache,
            metrics,
            first,
            second,
            a,
            b,
            _running: running,
        }
    }

    impl Lane {
        fn submit(&mut self, references: Vec<ValueRecordReference>) -> LiveReadToken {
            let request = LiveReadRequest {
                references,
                deadline: Instant::now() + Duration::from_secs(10),
                trace: None,
            };
            match self.cache.submit(&mut self.client, 1, request) {
                Ok(token) => token,
                Err(_) => panic!("lane rejected the request"),
            }
        }
        fn collect(&mut self, count: usize) -> Vec<LiveReadDone> {
            let mut done = Vec::new();
            for _ in 0..1000 {
                self.cache.poll(&mut self.client, 1, &mut done).unwrap();
                if done.len() >= count {
                    return done;
                }
                self.cache.wait(Duration::from_secs(1)).unwrap();
            }
            panic!("requests did not finish")
        }
    }

    #[test]
    fn lane_runs_requests_for_different_files_in_flight_together() {
        let mut lane = lane(4);
        let (a, b) = (lane.a, lane.b);
        let first = lane.submit(vec![a]);
        let second = lane.submit(vec![b, a, b]);
        // Both are on the rings before any CQE is harvested.
        assert_eq!(lane.cache.in_flight(), 2);
        assert_eq!(lane.cache.active_requests, 2);
        assert_eq!(lane.cache.grants[0].io.outstanding(), 2);
        assert_eq!(lane.cache.grants[1].io.outstanding(), 2);
        let counts = lane.metrics.snapshot().counts;
        assert_eq!(counts.lane_inflight_request_max, 2);
        assert_eq!(counts.lane_inflight_op_max, 4);

        let done = lane.collect(2);
        let result = |token| {
            let found = done.iter().find(|d| d.token == token).unwrap();
            found.result.as_ref().ok().unwrap().clone()
        };
        let (one, two) = (
            lane.first.mutations()[0].clone(),
            lane.second.mutations()[0].clone(),
        );
        assert_eq!(result(first), vec![one.clone()]);
        assert_eq!(result(second), vec![two.clone(), one, two]);
        assert_eq!(lane.cache.in_flight(), 0);
        assert_eq!(lane.cache.active_requests, 0);
        let counts = lane.metrics.snapshot().counts;
        assert_eq!(counts.completed_read_ops, 4);
        assert_eq!((counts.grant_hits, counts.grant_misses), (3, 0));
        assert_eq!(counts.delivery_samples, 2);
        assert!(counts.copy_bytes_avoided > 0);
        assert_eq!(counts.errors, 0);
        // A hit refreshes recency: reading file 0 again makes it newest.
        lane.submit(vec![b]);
        lane.submit(vec![a]);
        lane.collect(2);
        assert!(lane.cache.grants[0].last_used > lane.cache.grants[1].last_used);
    }

    #[test]
    fn lane_admits_a_request_larger_than_the_grant_credit_in_waves() {
        let mut lane = lane(4);
        let a = lane.a;
        let token = lane.submit(vec![a; 6]);
        let state = &lane.cache.requests[&token];
        // Four member slots: one flight now, two members parked.
        assert_eq!((state.flights, state.pending), (1, 2));
        // A second request on the full grant parks without failing.
        let other = lane.submit(vec![a]);
        assert_eq!(lane.cache.requests[&other].pending, 1);
        let done = lane.collect(2);
        let one = lane.first.mutations()[0].clone();
        for finished in &done {
            let values = finished.result.as_ref().ok().unwrap();
            assert!(values.iter().all(|value| *value == one));
            assert_eq!(values.len(), if finished.token == token { 6 } else { 1 });
        }
        assert_eq!(lane.cache.grants[0].io.outstanding(), 0);
        assert!(lane.cache.grants[0].io.stats().read_requests >= 3);
    }

    #[test]
    fn lane_rejects_invalid_cached_member_geometry_before_io() {
        let mut lane = lane(4);
        let mut outside = lane.a;
        outside.descriptor.offset = outside.batch_address.len;
        let invalid = lane.submit(vec![outside]);
        assert!(
            lane.cache
                .grants
                .iter()
                .all(|grant| grant.io.outstanding() == 0)
        );
        let valid = lane.submit(vec![lane.b]);
        let done = lane.collect(2);
        assert!(
            done.iter()
                .find(|result| result.token == invalid)
                .unwrap()
                .result
                .is_err()
        );
        assert!(
            done.iter()
                .find(|result| result.token == valid)
                .unwrap()
                .result
                .is_ok()
        );
        assert_eq!(lane.metrics.snapshot().counts.completed_read_ops, 1);
    }

    #[test]
    fn lane_fails_a_corrupt_member_alone_and_keeps_serving_other_requests() {
        let mut lane = lane(4);
        let (a, b) = (lane.a, lane.b);
        let mut corrupt = a;
        corrupt.descriptor.checksum.0 ^= 1;
        let bad = lane.submit(vec![corrupt]);
        let good = lane.submit(vec![b]);
        let done = lane.collect(2);
        assert!(
            done.iter()
                .find(|d| d.token == bad)
                .unwrap()
                .result
                .is_err()
        );
        assert!(
            done.iter()
                .find(|d| d.token == good)
                .unwrap()
                .result
                .is_ok()
        );
        assert_eq!(lane.metrics.snapshot().counts.errors, 1);
        // The grant survived the verification failure and serves the next read.
        let again = lane.submit(vec![a]);
        let done = lane.collect(1);
        assert_eq!(done[0].token, again);
        assert!(done[0].result.is_ok());
        assert_eq!(lane.cache.grants.len(), 2);
    }

    #[test]
    fn lane_rejects_when_full_and_fails_an_expired_deadline() {
        let mut lane = lane(4);
        let a = lane.a;
        for _ in 0..4 {
            lane.submit(vec![a]);
        }
        let request = LiveReadRequest {
            references: vec![a],
            deadline: Instant::now() + Duration::from_secs(10),
            trace: None,
        };
        match lane.cache.submit(&mut lane.client, 1, request) {
            Err(LiveReadSubmitError::Full(request)) => assert_eq!(request.references.len(), 1),
            _ => panic!("a full lane must hand the request back"),
        }
        lane.collect(4);
        let expired = LiveReadRequest {
            references: vec![a],
            deadline: Instant::now(),
            trace: None,
        };
        let Ok(token) = lane.cache.submit(&mut lane.client, 1, expired) else {
            panic!("admission failure is reported through poll")
        };
        let done = lane.collect(1);
        assert_eq!(done[0].token, token);
        assert!(
            done[0]
                .result
                .as_ref()
                .err()
                .unwrap()
                .to_string()
                .contains("deadline")
        );
        assert_eq!(lane.cache.grants[0].io.outstanding(), 0);
    }

    #[test]
    fn drain_cancels_in_flight_requests_and_waits_for_every_cqe() {
        let mut lane = lane(4);
        let (a, b) = (lane.a, lane.b);
        lane.submit(vec![a, a]);
        lane.submit(vec![b]);
        let mut done = Vec::new();
        lane.cache.drain(&mut lane.client, 1, &mut done).unwrap();
        assert_eq!(done.len(), 2);
        assert!(done.iter().all(|d| d.result.is_err()));
        // No operation, and so no DMA buffer, is left when drain returns.
        assert!(lane.cache.grants.iter().all(|g| g.io.outstanding() == 0));
        assert_eq!(lane.cache.in_flight(), 0);
        // Closing with requests in flight is refused.
        lane.submit(vec![a]);
        assert!(lane.cache.close(&mut lane.client, 1).is_err());
        lane.collect(1);
    }

    #[test]
    fn owner_release_control_retires_real_file_pin_and_rejects_wrong_epoch() {
        use super::super::{Request, Response, StorageNode, TcpLimits};
        let dir = tempdir::TempDir::new("live-pin-release").unwrap();
        let config = super::super::tests::config(&dir.path().join("node"));
        let node = StorageNode::open(config.clone(), super::super::tests::metrics()).unwrap();
        node.inventory.lock().fence_to(1).unwrap();
        let batch = super::super::tests::batch(&config, 1, 0);
        let response = node
            .handle(
                Request::Append {
                    epoch: 1,
                    batches: vec![batch.encoded().clone().into()],
                },
                TcpLimits::default(),
            )
            .unwrap();
        let Response::Appended(receipts) = response else {
            panic!("expected receipt")
        };
        let reference = batch.record_reference(receipts[0].address, 0).unwrap();
        let log = node.inventory.lock().active(1).unwrap();
        let lease = log.resolve_live_record(&reference).unwrap();
        let token = [42; 16];
        node.inventory
            .lock()
            .live_pins
            .insert(token, (1, vec![lease]));
        assert!(
            node.handle(
                Request::ReleaseLiveEpochPin { epoch: 2, token },
                TcpLimits::default()
            )
            .is_err()
        );
        assert!(node.inventory.lock().live_pins.contains_key(&token));
        assert!(matches!(
            node.handle(
                Request::ReleaseLiveEpochPin { epoch: 1, token },
                TcpLimits::default()
            ),
            Ok(Response::NvmeofEpochReleased)
        ));
        assert!(!node.inventory.lock().live_pins.contains_key(&token));
    }

    #[test]
    fn failed_release_retains_token_for_later_cleanup() {
        use crate::distributed::storage::WalMemberReadPolicy;
        let running = super::super::tests::multiplexed::Running::new(
            super::super::Transport::Tcp,
            WalMemberReadPolicy::Async,
        );
        let mut client = running.client();
        client.fence_to(1).unwrap();
        let batch = super::super::tests::batch(&running.config, 1, 0);
        let receipts = client.append_many(1, std::slice::from_ref(&batch)).unwrap();
        let reference = batch
            .record_reference(receipts[0].appended().address(), 0)
            .unwrap();
        let log = running.node.inventory.lock().active(1).unwrap();
        let lease = log.resolve_live_record(&reference).unwrap();
        let token = [43; 16];
        running
            .node
            .inventory
            .lock()
            .live_pins
            .insert(token, (1, vec![lease]));
        let mut cache = LiveReadCache::default();
        cache.unreleased.push(token);
        assert!(cache.close(&mut client, 2).is_err());
        assert_eq!(cache.unreleased, vec![token]);
        assert!(running.node.inventory.lock().live_pins.contains_key(&token));
        cache.close(&mut client, 1).unwrap();
        assert!(cache.unreleased.is_empty());
        assert!(!running.node.inventory.lock().live_pins.contains_key(&token));
    }

    #[test]
    fn io_counter_deltas_do_not_recount_prior_completions() {
        use crate::distributed::nvmeof::live_io::LiveIoStats;
        use crate::distributed::nvmeof::live_metrics::LiveNvmeMetrics;
        let metrics = LiveNvmeMetrics::new();
        let producer = metrics.register().unwrap();
        let before = LiveIoStats {
            completed_read_ops: 2,
            completed_read_bytes: 4096,
            submissions: 2,
            completions: 2,
            queue_ns: 10,
            submit_ns: 20,
            submit_to_cq_ns: 30,
            ..Default::default()
        };
        let after = LiveIoStats {
            completed_read_ops: 3,
            completed_read_bytes: 8192,
            submissions: 3,
            completions: 3,
            queue_ns: 17,
            submit_ns: 28,
            submit_to_cq_ns: 39,
            ..Default::default()
        };
        record_read_io_stats(&producer, &before, &after, false);
        let counts = metrics.snapshot().counts;
        assert_eq!(
            (counts.completed_read_ops, counts.completed_read_bytes),
            (1, 4096)
        );
        assert_eq!((counts.queue_ns, counts.queue_samples), (7, 1));
        assert_eq!((counts.submit_ns, counts.submit_samples), (8, 1));
        assert_eq!((counts.cq_ns, counts.cq_samples), (9, 1));
        assert_eq!(counts.recovery_read_ops, 0);
    }
}
