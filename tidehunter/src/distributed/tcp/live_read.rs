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
use super::LiveRecordLocation;
use super::{Client, TcpError};
use crate::crc::CrcFrame;
use crate::distributed::hybrid::position::VersionPosition;
use crate::distributed::nvmeof::live_connect::LiveConnectTarget;
use crate::distributed::nvmeof::live_io::{
    LiveCompletion, LiveIo, LiveIoError, LiveIoResult, LiveIoStats, LiveSubmitOutcome,
    NamespaceSlot,
};
use crate::distributed::nvmeof::live_metrics::{
    LiveNamespaceIdentity, LiveNvmeCounters, LiveRouteEvidence, LiveTimingStage,
};
use crate::distributed::nvmeof::live_namespace::LiveNamespaceAccess;
use crate::distributed::nvmeof::live_pool::{LiveControllerLease, LivePoolAcquisition};
use crate::distributed::nvmeof::live_storage::{
    FILES_PER_SUBSYSTEM, LiveWalSegment, wal_export_name,
};
use crate::distributed::remote_db::ReadTraceChild;
use connect::{ConnectError, connect_budget, failed, failed_io, pool_deadline};
use minibytes::Bytes;
use smallvec::SmallVec;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::os::fd::{AsRawFd, OwnedFd};
use std::sync::Arc;
use std::time::{Duration, Instant};

pub(crate) const LIVE_READ_CACHE_CAPACITY: usize = 8;
/// Default bound on requests one lane holds in flight.
pub(crate) const LIVE_READ_LANE_REQUESTS: usize = 32;
/// Longest drain in `shutdown`. `LiveIo` aborts the process at 30 s on its own.
const DRAIN_BOUND: Duration = Duration::from_secs(35);
/// Pause before a request retries a controller pool that had no free slot.
const POOL_RETRY: Duration = Duration::from_millis(1);
/// At most this many submission-time read retries per request. A retry is
/// allowed only when no member of the request reached the ring, so no buffer
/// can still receive DMA; the retry must also start before the call deadline.
const MAX_READ_START_RETRIES: u8 = 2;
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

/// Keep a live I/O deadline typed. `TcpError::component` maps every class to
/// `Other`, which erases the one class the value path must tell apart: a
/// deadline (transient, never submitted or retired) from a broken ring.
pub(super) fn live_error(error: LiveIoError) -> TcpError {
    match error {
        LiveIoError::Timeout => TcpError::with_kind(
            std::io::ErrorKind::TimedOut,
            "live read I/O deadline expired",
        ),
        other => TcpError::component(other),
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
    pub(crate) positions: Vec<VersionPosition>,
    /// Absolute call deadline: no flight may start with a deadline past this
    /// and no retry may start after it. Time a member waits for grant credit
    /// is bounded by it, not by a per-I/O timer.
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

/// A finished request. On success the member frames keep the request's order.
pub(crate) struct LiveReadDone {
    pub(crate) token: LiveReadToken,
    pub(crate) result: Result<Vec<Bytes>, TcpError>,
    /// Retries of a submission that never reached the ring. Operator data;
    /// zero for the normal path.
    pub(crate) retries: u8,
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
        let fd = crate::distributed::sys::eventfd().map_err(TcpError::component)?;
        Ok(Self(Arc::new(fd)))
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
    scope: GrantScope,
    token: Option<[u8; 16]>,
    io: LiveIo,
    inline_io: Option<LiveIo>,
    inline_recorded: LiveIoStats,
    /// Statistics already folded into the lane counters.
    recorded: LiveIoStats,
    /// Lane clock value at the last use; the smallest is evicted first.
    last_used: u64,
    /// Its ring failed: never submit again, retire when drained.
    broken: bool,
    // Both rings drop first: the lease outlives every descriptor on it.
    // `None` only for grants over local files in tests.
    lease: Option<Arc<LiveControllerLease>>,
}

// Keep the existing file grant inline; boxing adds an allocation at grant open.
#[allow(clippy::large_enum_variant)]
enum GrantScope {
    File(LiveRecordLocation),
    Subsystem {
        /// Owner WAL file generation these exports belong to. Grants are keyed
        /// by the address, never the batch epoch: a relocated position keeps its
        /// logical batch and names the generation that holds its bytes.
        generation: u64,
        partition: u32,
        subsystem: u64,
        files_per_subsystem: u64,
        file_bytes: u64,
        slots: Vec<Option<NamespaceSlot>>,
    },
}

impl GrantScope {
    fn file_bytes(&self) -> u64 {
        match self {
            Self::File(location) => location.segment.bytes,
            Self::Subsystem { file_bytes, .. } => *file_bytes,
        }
    }
    fn covers(&self, position: &VersionPosition) -> bool {
        match self {
            Self::File(location) => grant_matches(&location.segment, position),
            Self::Subsystem {
                generation,
                partition,
                subsystem,
                files_per_subsystem,
                file_bytes,
                ..
            } => {
                position.address.segment_generation == *generation
                    && position.address.partition == *partition
                    && position.address.offset / file_bytes / files_per_subsystem == *subsystem
            }
        }
    }
    /// Whether retiring `generation`'s `files` reaches this grant. A pooled
    /// controller serves every namespace of one subsystem, so a grant of a
    /// victim file's subsystem must be released too: an idle sibling would
    /// otherwise keep the subsystem lease live and defer the retirement.
    fn covers_retirement(&self, generation: u64, files: &[u64]) -> bool {
        let subsystem_of = |file: u64, files_per_subsystem: u64| file / files_per_subsystem;
        match self {
            Self::File(location) => {
                location.segment.generation == generation
                    && files.iter().any(|&start| {
                        subsystem_of(start / location.segment.bytes, FILES_PER_SUBSYSTEM)
                            == subsystem_of(location.segment.file_index, FILES_PER_SUBSYSTEM)
                    })
            }
            Self::Subsystem {
                generation: source,
                subsystem,
                files_per_subsystem,
                file_bytes,
                ..
            } => {
                *source == generation
                    && files.iter().any(|&start| {
                        subsystem_of(start / file_bytes, *files_per_subsystem) == *subsystem
                    })
            }
        }
    }
    fn slot(&self, position: &VersionPosition) -> Option<NamespaceSlot> {
        match self {
            Self::File(_) => None,
            Self::Subsystem {
                files_per_subsystem,
                file_bytes,
                slots,
                ..
            } => slots[(position.address.offset / file_bytes % files_per_subsystem) as usize],
        }
    }
    fn ready(&self, position: &VersionPosition) -> bool {
        self.covers(position) && (matches!(self, Self::File(_)) || self.slot(position).is_some())
    }
}

impl Grant {
    fn inline_io(&self) -> &LiveIo {
        self.inline_io.as_ref().unwrap_or(&self.io)
    }

    fn inline_io_mut(&mut self) -> &mut LiveIo {
        self.inline_io.as_mut().unwrap_or(&mut self.io)
    }

    fn outstanding(&self) -> usize {
        self.io.outstanding() + self.inline_io.as_ref().map_or(0, LiveIo::outstanding)
    }
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
    positions: SmallVec<[usize; 1]>,
}

struct InFlightRequest {
    positions: Vec<VersionPosition>,
    /// Absolute deadline of the caller's read. Every flight starts with
    /// `min(io_timeout, deadline - now)`, so no observed I/O outlives it.
    deadline: Instant,
    /// Submission-time retries already spent; the bound is
    /// [`MAX_READ_START_RETRIES`].
    retries: u8,
    /// Deadline of the latest submitted flight; `None` while no flight is
    /// outstanding.
    io_deadline: Option<Instant>,
    trace: Option<ReadTraceChild>,
    slots: Vec<Slot>,
    output: Vec<Option<Bytes>>,
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
    /// Time left before the caller's absolute deadline. Zero means no new
    /// flight and no retry may start.
    fn remaining(&self, now: Instant) -> Duration {
        self.deadline.saturating_duration_since(now)
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

/// A file grant covers a position when the position's address names a member of
/// that grant's generation and file.
///
/// The batch epoch is deliberately not compared: relocation and owner
/// compaction move a value between generations without changing its logical
/// version, and the bytes live in the generation the address names.
fn grant_matches(segment: &LiveWalSegment, position: &VersionPosition) -> bool {
    segment.generation == position.address.segment_generation
        && segment.partition == position.address.partition
        && segment.bytes != 0
        && segment.file_index == position.address.offset / segment.bytes
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
    if let Some(io) = &grant.inline_io {
        let after = io.stats();
        if let Some(counters) = counters {
            record_read_io_stats(counters, &grant.inline_recorded, &after, false);
        }
        grant.inline_recorded = after;
    }
}

fn drain_failed_io(grant: &mut Grant) {
    let _ = grant.io.shutdown();
    if let Some(io) = &mut grant.inline_io {
        let _ = io.shutdown();
    }
    // Index/file pins may retire only after all terminal CQEs. A failed drain
    // with outstanding DMA cannot return safely; match LiveIo's fail-stop rule.
    if grant.outstanding() != 0 {
        std::process::abort();
    }
}

fn member_range(grant: &Grant, position: &VersionPosition) -> Result<(u64, usize), TcpError> {
    // The position names the member frame itself: `offset` already includes the
    // batch's 8-byte CRC header and the member's descriptor offset, and `len`
    // covers the member header plus its native entry. A member never straddles
    // a WAL file, so the file index computed from it equals the batch's.
    let bytes = grant.scope.file_bytes();
    let offset = position.address.offset % bytes;
    let len = position.address.len as usize;
    if len < CrcFrame::CRC_HEADER_LENGTH
        || !grant.scope.covers(position)
        || offset.checked_add(len as u64).is_none_or(|end| end > bytes)
    {
        return Err(TcpError::new("invalid cached live member geometry"));
    }
    Ok((offset, len))
}

/// Lane state: file grants, requests in flight and unreleased owner pins.
///
/// Owner: the lane's driver lock, borrowed by its worker or an idle caller.
/// Bounds: `max_requests` requests, `LIVE_READ_CACHE_CAPACITY` grants and each
/// grant's `LiveIo` credit. A full lane returns the request to the caller.
pub(crate) struct LiveReadCache {
    grants: Vec<Grant>,
    // At most one entry per failed subsystem of the current authority epoch.
    // New authority drops old entries; database reopen creates a new lane.
    failed_subsystems: HashSet<(u64, u64)>,
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
    #[cfg(test)]
    inline_submitted_pause: Option<(
        std::sync::mpsc::SyncSender<()>,
        std::sync::mpsc::Receiver<()>,
    )>,
    /// Fail the next member submission with a typed `TimedOut` before it
    /// reaches the ring. Test-only: it models the one retry-safe timeout (no
    /// SQE and no buffer owned), so a retry can be proved.
    #[cfg(test)]
    fail_next_read_submission: bool,
}
impl Default for LiveReadCache {
    fn default() -> Self {
        Self::with_max_requests(LIVE_READ_LANE_REQUESTS)
    }
}

impl LiveReadCache {
    /// Resolve this grant's descriptor, not the controller pool's original
    /// binding: a shared controller may now serve a different namespace.
    /// Resolve while the caller exclusively owns the cache, before an inline
    /// borrower can retire a grant and allow its descriptor number to be reused.
    pub(crate) fn namespace_device(&self) -> Option<std::io::Result<std::path::PathBuf>> {
        self.grants.first().map(|grant| {
            let path = format!("/proc/self/fd/{}", grant.io.namespace().file.as_raw_fd());
            std::fs::canonicalize(&path).map_err(|error| {
                std::io::Error::new(error.kind(), format!("resolve namespace {path}: {error}"))
            })
        })
    }

    pub(crate) fn with_max_requests(max_requests: usize) -> Self {
        Self {
            grants: Vec::with_capacity(LIVE_READ_CACHE_CAPACITY),
            failed_subsystems: HashSet::new(),
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
            #[cfg(test)]
            inline_submitted_pause: None,
            #[cfg(test)]
            fail_next_read_submission: false,
        }
    }

    /// Arm one submission-time timeout on the next member group. Test-only.
    #[cfg(test)]
    pub(crate) fn fail_next_read_submission(&mut self) {
        self.fail_next_read_submission = true;
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
        let members = request.positions.len();
        let mut state = InFlightRequest {
            positions: request.positions,
            deadline: request.deadline,
            retries: 0,
            io_deadline: None,
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
                        self.complete_flight(completion)?;
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
    /// The timeout also stops at the nearest retry time of a request that
    /// still has members to start. Pass a `max` of at most a second: `LiveIo`
    /// expires overdue operations from its progress calls.
    /// EINTR is retried; any other `poll(2)` or eventfd failure is returned and
    /// means the lane cannot be trusted to wake: stop it.
    pub(crate) fn wait(&mut self, max: Duration) -> Result<LiveReadWait, TcpError> {
        let now = Instant::now();
        let mut timeout = max;
        for state in self.requests.values() {
            if state.error.is_none()
                && state.pending > 0
                && let Some(at) = state.retry_at
            {
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
        for mut grant in self.grants.drain(..) {
            drain_failed_io(&mut grant);
            fold_stats(client.live_counters.as_deref(), &mut grant);
            let token = grant.token;
            drop(grant);
            if let Some(token) = token {
                self.unreleased.push(token);
            }
        }
        self.release_pending(client, epoch)
    }

    /// Retire idle grants of the retired files' subsystems. A pooled
    /// controller serves every namespace of one subsystem, so an idle cached
    /// grant of a sibling file would otherwise keep the subsystem lease live
    /// and defer the retirement indefinitely. In-flight grants keep their pins
    /// and the owner can defer the retirement with `Busy`.
    pub(crate) fn evict_retirement_files(
        &mut self,
        client: &mut Client,
        epoch: u64,
        generation: u64,
        files: &[u64],
    ) -> Result<(), TcpError> {
        let mut index = 0;
        while index < self.grants.len() {
            let grant = &self.grants[index];
            if grant.scope.covers_retirement(generation, files) && grant.outstanding() == 0 {
                self.retire(client, epoch, index)?;
            } else {
                index += 1;
            }
        }
        self.release_pending(client, epoch)
    }

    pub(crate) fn take_unreleased(&mut self) -> Vec<[u8; 16]> {
        std::mem::take(&mut self.unreleased)
    }

    fn retire(&mut self, client: &mut Client, epoch: u64, index: usize) -> Result<(), TcpError> {
        if self.grants[index].outstanding() != 0 {
            return Err(TcpError::new("live read grant has I/O in flight"));
        }
        let mut grant = self.grants.remove(index);
        if grant.broken
            && let GrantScope::Subsystem {
                generation,
                subsystem,
                ..
            } = &grant.scope
        {
            self.failed_subsystems.insert((*generation, *subsystem));
        }
        fold_stats(client.live_counters.as_deref(), &mut grant);
        let token = grant.token;
        // No operation remains: only drained grants are retired, and a drained
        // ring has returned every CQE.
        drop(grant);
        if let Some(token) = token {
            self.unreleased.push(token);
        }
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

    fn find_grant(&self, position: &VersionPosition) -> Option<usize> {
        self.grants
            .iter()
            .position(|grant| !grant.broken && grant.scope.ready(position))
    }

    /// Starts as many of the request's pending members as credit allows. A
    /// failure is recorded on the request and its flights are cancelled.
    ///
    /// One class is retried: a `TimedOut` submission that left
    /// `state.flights == 0`, so no SQE reached the ring and no buffer can
    /// still receive DMA. The retry is bounded by
    /// [`MAX_READ_START_RETRIES`] and must start before the absolute call
    /// deadline. Every other failure is final; a deadline that passes with
    /// I/O outstanding is never retried here.
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
        // Parked members never expire: each flight's deadline starts at its
        // submission (`submit_group`), as a native read waits for the disk.
        loop {
            let failure = self.admit(client, epoch, token, state).err();
            let Some(error) = failure else {
                return;
            };
            let retry_safe = error.kind() == std::io::ErrorKind::TimedOut
                && state.flights == 0
                && state.pending > 0
                && state.retries < MAX_READ_START_RETRIES
                && Instant::now() < state.deadline;
            if !retry_safe {
                state.fail(error);
                self.cancel_flights(token);
                return;
            }
            state.retries += 1;
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
        if state.remaining(Instant::now()).is_zero() {
            // No time left for a flight or a connect. Typed, so the caller
            // does not mistake a spent budget for a broken lane.
            return Err(TcpError::with_kind(
                std::io::ErrorKind::TimedOut,
                "live read deadline expired before admission",
            ));
        }
        let mut blocked: Vec<u64> = Vec::new();
        let mut no_new_grant = false;
        // Each pass starts members, blocks one grant, or stops new grants.
        for _ in 0..state.positions.len() + LIVE_READ_CACHE_CAPACITY + 2 {
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
            match self.find_grant(&state.positions[position]) {
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
        // A member whose address names this open's generation is read through
        // the permanent per-owner export; another generation (a relocated or
        // older position) resolves a pinned read-only grant. Selection is by the
        // address, never the batch epoch.
        if client.live_config.is_some()
            && state.positions[position].address.segment_generation == epoch
        {
            return self.open_permanent(client, epoch, state, position, connect_timeout);
        }
        if self.grants.len() >= LIVE_READ_CACHE_CAPACITY {
            let victim = lru_victim(self.grants.iter().enumerate().map(|(index, grant)| {
                (
                    index,
                    !grant.broken && grant.outstanding() == 0,
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
            client.resolve_live_records(epoch, &[state.positions[position].address])?
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
        let connect_deadline = connect_budget(state.deadline, since, connect_timeout);
        let id = self.next_grant;
        match Self::connect_grant(client, epoch, location, pin, id, connect_deadline) {
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
                    ConnectError::PoolFull(error) => Err(pool_deadline(error)),
                    ConnectError::Failed(error) => Err(error),
                }
            }
        }
    }

    fn open_permanent(
        &mut self,
        client: &mut Client,
        epoch: u64,
        state: &mut InFlightRequest,
        position: usize,
        connect_timeout: Duration,
    ) -> Result<Opened, TcpError> {
        let position_ref = &state.positions[position];
        let file_bytes = client.expected.wal_file_bytes;
        let file = position_ref.address.offset / file_bytes;
        let files = FILES_PER_SUBSYSTEM;
        self.failed_subsystems
            .retain(|(current, _)| *current == epoch);
        if self.failed_subsystems.contains(&(epoch, file / files)) {
            return Err(TcpError::new("live subsystem failed; reopen the database"));
        }
        if client
            .live_exports
            .as_ref()
            .is_none_or(|(current, _, count)| *current != epoch || file >= *count)
            && let Err(error) = client.open_live_exports(epoch)
        {
            // A control timeout says nothing about the subsystem and is not
            // cached; the next attempt asks the owner again.
            if error.kind() != std::io::ErrorKind::TimedOut {
                self.failed_subsystems.insert((epoch, file / files));
            }
            return Err(error);
        }
        let (_, info, count) = client
            .live_exports
            .as_ref()
            .ok_or_else(|| TcpError::new("live exports unavailable"))?;
        if file >= *count {
            return Err(TcpError::new("live WAL file is not exported"));
        }
        let config = client
            .live_config
            .as_ref()
            .ok_or_else(|| TcpError::new("live NVMe is not configured"))?;
        let name = wal_export_name(
            files,
            client.expected.inventory.database,
            client.expected.partition,
            epoch,
            file,
        );
        let since = *state.connect_since.get_or_insert_with(Instant::now);
        let deadline = connect_budget(state.deadline, since, connect_timeout);
        if let Some(index) = self
            .grants
            .iter()
            .position(|grant| !grant.broken && grant.scope.covers(position_ref))
        {
            let grant = &mut self.grants[index];
            let _setup =
                LiveReadTiming::start(&client.live_counters, LiveTimingStage::NamespaceSetup);
            let attached = (|| {
                let (handle, _) = grant
                    .lease
                    .as_ref()
                    .ok_or_else(|| TcpError::new("live controller lease unavailable"))?
                    .namespace(
                        name.nsid,
                        name.uuid,
                        LiveNamespaceAccess::ReadShared,
                        deadline,
                    )
                    .map_err(|error| {
                        if error.kind() == std::io::ErrorKind::TimedOut {
                            TcpError::with_kind(error.kind(), error.to_string())
                        } else {
                            TcpError::component(error)
                        }
                    })?;
                let slot = grant
                    .io
                    .attach(handle.clone())
                    .map_err(TcpError::component)?;
                if let Some(inline) = &mut grant.inline_io
                    && inline.attach(handle).map_err(TcpError::component)? != slot
                {
                    return Err(TcpError::new("inline queue slot differs"));
                }
                if let GrantScope::Subsystem { slots, .. } = &mut grant.scope {
                    slots[(file % files) as usize] = Some(slot);
                }
                Ok::<(), TcpError>(())
            })();
            if let Err(error) = attached {
                // A namespace setup that hit its deadline is transient: the
                // grant keeps its other namespaces, so a retry may attach again.
                if error.kind() != std::io::ErrorKind::TimedOut {
                    self.fail_grant(client, index, &error);
                }
                return Err(error);
            }
            if let Some(counters) = &client.live_counters {
                counters.record_grant_miss();
            }
            state.connect_since = None;
            return Ok(Opened::Grant(index));
        }
        let target = LiveConnectTarget {
            nqn: name.nqn,
            uuid: name.uuid,
            nsid: name.nsid,
            bytes: file_bytes,
            block_size: 4096,
            address: info.address.clone(),
            service: info.service,
            transport: info.transport,
            geometry: config.queue_geometry,
        };
        if self.grants.len() >= LIVE_READ_CACHE_CAPACITY {
            let victim = lru_victim(self.grants.iter().enumerate().map(|(index, grant)| {
                (
                    index,
                    !grant.broken && grant.outstanding() == 0,
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
        let scope = GrantScope::Subsystem {
            // Entering this path means the member's address names the active
            // generation, which is this open's fencing epoch.
            generation: epoch,
            partition: client.expected.partition,
            subsystem: name.subsystem,
            files_per_subsystem: files,
            file_bytes,
            slots: vec![None; files as usize],
        };
        match Self::connect_scope(
            client,
            epoch,
            target,
            scope,
            None,
            self.next_grant,
            LiveNamespaceAccess::ReadShared,
            deadline,
            true,
        ) {
            Ok(mut grant) => {
                self.next_grant += 1;
                self.clock += 1;
                grant.last_used = self.clock;
                state.connect_since = None;
                if let Some(counters) = &client.live_counters {
                    counters.record_grant_miss();
                }
                self.grants.push(grant);
                Ok(Opened::Grant(self.grants.len() - 1))
            }
            Err(ConnectError::PoolFull(_)) if Instant::now() < deadline => {
                state.retry_at = Some(Instant::now() + POOL_RETRY);
                Ok(Opened::Retry)
            }
            // The connect budget is spent with the pool still full: a
            // deadline, not a broken subsystem.
            Err(ConnectError::PoolFull(error)) => Err(pool_deadline(error)),
            Err(ConnectError::Failed(error)) => {
                // A connect that hit its deadline is retried inside the call;
                // caching it would fail-stop the authority on the retry.
                if error.kind() != std::io::ErrorKind::TimedOut {
                    self.failed_subsystems.insert((epoch, name.subsystem));
                }
                Err(error)
            }
        }
    }

    fn connect_grant(
        client: &Client,
        authority_epoch: u64,
        location: LiveRecordLocation,
        token: [u8; 16],
        id: u64,
        connect_deadline: Instant,
    ) -> Result<Grant, ConnectError> {
        let Some(config) = &client.live_config else {
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
            geometry: config.queue_geometry,
        };
        let access = if binding.readonly {
            LiveNamespaceAccess::ReadOnly
        } else {
            LiveNamespaceAccess::ReadWrite
        };
        Self::connect_scope(
            client,
            authority_epoch,
            target,
            GrantScope::File(location),
            Some(token),
            id,
            access,
            connect_deadline,
            false,
        )
    }

    // One setup path for file grants and subsystem grants. Each argument is a
    // distinct input of the two callers; a parameter struct would only rename them.
    #[allow(clippy::too_many_arguments)]
    fn connect_scope(
        client: &Client,
        _authority_epoch: u64,
        target: LiveConnectTarget,
        mut scope: GrantScope,
        token: Option<[u8; 16]>,
        id: u64,
        access: LiveNamespaceAccess,
        connect_deadline: Instant,
        shared: bool,
    ) -> Result<Grant, ConnectError> {
        let (Some(config), Some(pool)) = (&client.live_config, &client.live_pool) else {
            return Err(failed("live controller pool unavailable"));
        };
        // The ordinary-file fixture has no kernel target: name the file behind
        // a file grant, so reopened authorities read earlier WAL files too.
        #[cfg(any(test, feature = "test-utils"))]
        if client.live_write_local_fixture
            && let GrantScope::File(location) = &scope
        {
            pool.register_local_target(&target, access, location.segment.path.clone())
                .map_err(failed_io)?;
        }
        let setup = LiveReadTiming::start(&client.live_counters, LiveTimingStage::NamespaceSetup);
        let acquisition = if shared {
            pool.acquire_shared(
                &target,
                &config.host_nqn,
                config.host_id,
                access,
                connect_deadline,
            )
        } else if let GrantScope::File(location) = &scope {
            pool.acquire_file(
                &target,
                &config.host_nqn,
                config.host_id,
                access,
                connect_deadline,
                (
                    location.segment.partition,
                    location.segment.epoch,
                    location.segment.file_index,
                ),
            )
        } else {
            pool.acquire(
                &target,
                &config.host_nqn,
                config.host_id,
                access,
                connect_deadline,
            )
        };
        let (lease, acquisition, timings) = acquisition.map_err(|error| {
            if error.kind() == std::io::ErrorKind::WouldBlock {
                ConnectError::PoolFull(error)
            } else {
                failed_io(error)
            }
        })?;
        let (namespace, opened) = lease
            .namespace(target.nsid, target.uuid, access, connect_deadline)
            .map_err(failed_io)?;
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
            // A shared controller serves every member of its subsystem: name
            // the member this grant opened, not the one used to connect.
            let observed = &opened;
            metrics.record_route(LiveRouteEvidence {
                transport: observed.transport,
                address: target.address.clone(),
                service: target.service,
                host_nqn: config.host_nqn.clone(),
                host_id: config.host_id,
                writer: None,
                namespace_identity: LiveNamespaceIdentity {
                    namespace_path: observed.namespace_path.clone(),
                    controller_instance: lease.controller_instance(),
                    nqn: observed.nqn.clone(),
                    uuid: observed.uuid,
                    nsid: observed.nsid,
                    bytes: observed.bytes,
                    block_size: observed.block_size,
                    controller: lease.evidence(),
                },
            });
        }
        let mut io = LiveIo::new(
            namespace.clone(),
            config.max_inflight_requests,
            config.max_inflight_bytes,
            false,
        )
        .map_err(failed)?;
        let recorded = io.stats();
        let mut inline_io = if config.queue_geometry.poll_queues > 0 {
            Some(
                LiveIo::new(
                    namespace.clone(),
                    config.max_inflight_requests,
                    config.max_inflight_bytes,
                    true,
                )
                .map_err(failed)?,
            )
        } else {
            None
        };
        if let GrantScope::Subsystem { slots, .. } = &mut scope {
            let slot = io.attach(namespace.clone()).map_err(failed)?;
            if let Some(inline) = &mut inline_io
                && inline.attach(namespace).map_err(failed)? != slot
            {
                return Err(failed("inline queue slot differs"));
            }
            slots[(target.nsid - 1) as usize] = Some(slot);
        }
        Ok(Grant {
            id,
            scope,
            token,
            io,
            inline_io,
            inline_recorded: LiveIoStats::default(),
            recorded,
            last_used: 0,
            broken: false,
            lease: Some(lease),
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
        #[cfg(test)]
        if self.fail_next_read_submission {
            self.fail_next_read_submission = false;
            return Err(TcpError::with_kind(
                std::io::ErrorKind::TimedOut,
                "forced submission-time read timeout",
            ));
        }
        self.clock += 1;
        let grant = &mut self.grants[index];
        grant.last_used = self.clock;
        let (free_slots, free_bytes) = grant.io.credit();
        if free_slots == 0 {
            return Ok(Group::Blocked);
        }
        let (slot_capacity, byte_capacity) = grant.io.capacity();
        let mut selected_file = None;
        let mut selected = SmallVec::<[usize; 1]>::new();
        let mut ranges = SmallVec::<[(u64, usize); 1]>::new();
        {
            let _validation =
                LiveReadTiming::start(&client.live_counters, LiveTimingStage::Validation);
            for position in state.first_pending..state.slots.len() {
                if state.slots[position] != Slot::Pending {
                    continue;
                }
                let member = &state.positions[position];
                let file = member.address.offset / grant.scope.file_bytes();
                if !grant.scope.ready(member) || selected_file.is_some_and(|first| first != file) {
                    continue;
                }
                selected_file = Some(file);
                let (offset, len) = member_range(grant, member)?;
                if selected.len() == 32 {
                    break;
                }
                selected.push(position);
                ranges.push((offset, len));
            }
        }
        // Usually the full group fits and needs one plan. Under pressure,
        // shorten at most 32 times. Do not binary-search: a later bridging
        // range can reduce the SQE count of an earlier prefix.
        for _ in 0..ranges.len() {
            let slot = grant.scope.slot(&state.positions[selected[0]]);
            let charge = match slot {
                Some(slot) => grant.io.read_request_charge_at(slot, &ranges),
                None => grant.io.read_request_charge(&ranges),
            };
            let (operations, bytes) = charge.map_err(TcpError::component)?;
            if operations <= free_slots && bytes <= free_bytes {
                break;
            }
            ranges.pop();
            selected.pop();
        }
        if ranges.is_empty() {
            if free_slots == slot_capacity && free_bytes == byte_capacity {
                return Err(TcpError::new("live record exceeds queue capacity"));
            }
            return Ok(Group::Blocked);
        }
        // The flight deadline never outlives the caller's absolute deadline:
        // a retry or a first submission can only consume what is left.
        let remaining = state.remaining(Instant::now());
        if remaining.is_zero() {
            return Err(TcpError::with_kind(
                std::io::ErrorKind::TimedOut,
                "live read deadline expired before submission",
            ));
        }
        let flight = self.next_flight;
        let deadline = Instant::now() + io_timeout.min(remaining);
        let slot = grant.scope.slot(&state.positions[selected[0]]);
        let submitted = match slot {
            Some(slot) => {
                grant
                    .io
                    .submit_reads_at(flight, slot, &ranges, deadline, state.trace.as_ref())
            }
            None => grant
                .io
                .submit_reads_traced(flight, &ranges, deadline, state.trace.as_ref()),
        };
        match submitted.map_err(live_error)? {
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
        state.io_deadline = Some(deadline);
        self.flights.insert(
            flight,
            Flight {
                token,
                grant: grant.id,
                positions: selected,
            },
        );
        if let Some(counters) = &client.live_counters {
            let operations: usize = self.grants.iter().map(Grant::outstanding).sum();
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
            state.io_deadline = None;
        }
        Some(state)
    }

    /// Deadline of the latest flight `token` has outstanding; `None` while
    /// none is outstanding or the request is gone.
    pub(crate) fn io_deadline(&self, token: LiveReadToken) -> Option<Instant> {
        self.requests.get(&token)?.io_deadline
    }

    fn complete_flight(&mut self, completion: LiveCompletion) -> Result<(), TcpError> {
        let Some(flight) = self.flights.remove(&completion.id) else {
            return Err(TcpError::new("unexpected live read completion"));
        };
        let Some(state) = self.release_flight(flight.token) else {
            return Err(TcpError::new("live read completion without a request"));
        };
        state.last_cqe = Some(completion.completed_at);
        let mut cancel = false;
        let mut io_failed = false;
        match completion.result {
            Ok(LiveIoResult::Reads(records)) => {
                if state.error.is_none()
                    && let Err(error) = Self::deliver_members(state, &flight.positions, records)
                {
                    state.fail(error);
                    cancel = true;
                }
            }
            Ok(LiveIoResult::Written | LiveIoResult::Flushed) => {
                state.fail(TcpError::new("unexpected live write completion"));
                cancel = true;
            }
            Err(error) => {
                io_failed = matches!(
                    error,
                    crate::distributed::nvmeof::live_io::LiveIoError::Io(_)
                );
                cancel = state.error.is_none();
                // Keep the deadline class: the caller and the counters must
                // tell a transient timeout from a broken ring.
                state.fail(live_error(error));
            }
        }
        if cancel {
            self.cancel_flights(flight.token);
        }
        if io_failed
            && let Some(grant) = self.grants.iter_mut().find(|grant| {
                grant.id == flight.grant && matches!(grant.scope, GrantScope::Subsystem { .. })
            })
        {
            // Other CQEs can already be in this poll's completion list. Keep
            // their flight records until that list and the ring are drained.
            grant.broken = true;
            if let GrantScope::Subsystem {
                generation,
                subsystem,
                ..
            } = &grant.scope
            {
                self.failed_subsystems.insert((*generation, *subsystem));
            }
        }
        Ok(())
    }

    /// Hand the member frames of one harvested flight to their request slots.
    ///
    /// No keyspace, key or descriptor is available here by design: the lane
    /// transfers exactly the bytes the position names, and the caller verifies
    /// the position's CRC and decodes the native entry.
    fn deliver_members(
        state: &mut InFlightRequest,
        slots: &[usize],
        records: Vec<minibytes::Bytes>,
    ) -> Result<(), TcpError> {
        if records.len() != slots.len() {
            return Err(TcpError::new("live read count mismatch"));
        }
        for (&position, bytes) in slots.iter().zip(records) {
            state.output[position] = Some(bytes);
            state.slots[position] = Slot::Done;
        }
        if let Some(trace) = &state.trace {
            trace.event("live_members_delivered");
        }
        Ok(())
    }

    /// A ring error: drain the grant, fail its flights and stop using it.
    fn fail_grant(&mut self, client: &Client, index: usize, error: &impl std::fmt::Display) {
        let message = error.to_string();
        let grant = &mut self.grants[index];
        // Drained completions are dropped: every flight on this grant fails.
        // Do not release any flight's index pin until its terminal CQE retired.
        drain_failed_io(grant);
        grant.broken = true;
        if let GrantScope::Subsystem {
            generation,
            subsystem,
            ..
        } = &grant.scope
        {
            self.failed_subsystems.insert((*generation, *subsystem));
        }
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
            if grant.broken && grant.outstanding() == 0 {
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
        LiveReadDone {
            token,
            result,
            retries: state.retries,
        }
    }
}

impl Client {
    pub(crate) fn read_lane_pin_counters(&self) -> Option<Arc<LiveNvmeCounters>> {
        self.live_counters.clone()
    }
    #[cfg(test)]
    /// Blocking read of `positions` through `cache`. For callers that own a
    /// lane exclusively and keep one request at a time; lane workers use the
    /// multi-flight calls of [`LiveReadCache`] instead.
    pub(crate) fn live_read_records(
        &mut self,
        cache: &mut LiveReadCache,
        epoch: u64,
        positions: &[VersionPosition],
        deadline: Instant,
        trace: Option<&ReadTraceChild>,
    ) -> Result<Vec<Bytes>, TcpError> {
        let request = LiveReadRequest {
            positions: positions.to_vec(),
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

mod connect;
mod inline;

#[cfg(test)]
pub(crate) mod tests;
