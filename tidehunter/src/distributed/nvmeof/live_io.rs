//! Bounded live namespace data plane. A successful write CQE is not a durable
//! receipt; the owner must validate the frame and perform its persistence barrier.
//! Cancellation and deadlines report only after all kernel operations retire.
use super::live_namespace::{LiveNamespace, LiveNamespaceAccess};
use io_uring::{IoUring, opcode, types};
use minibytes::Bytes;
use std::collections::{HashMap, VecDeque};
use std::io;
use std::os::fd::{AsRawFd, RawFd};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

const DRAIN_TIMEOUT: Duration = Duration::from_secs(30);
// Assigned lazily only to queues carrying a sampled read.
static NEXT_TRACE_QUEUE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

#[derive(Debug)]
pub enum LiveIoError {
    Invalid(&'static str),
    Io(io::Error),
    Timeout,
    Cancelled,
    UnknownWrite,
}
impl std::fmt::Display for LiveIoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for LiveIoError {}
impl From<io::Error> for LiveIoError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LiveSubmitOutcome {
    Submitted,
    Full,
}
#[derive(Debug)]
pub enum LiveIoResult {
    Written,
    /// Member slices of aligned I/O blocks, in request order. A block returns to
    /// the queue's pool only after every slice and clone of it dropped; long-term
    /// retention of a slice keeps its whole block (at least one device block)
    /// alive, so copy before retaining.
    Reads(Vec<Bytes>),
}
pub struct LiveCompletion {
    pub id: u64,
    /// Observation time of the request's last terminal CQE.
    pub completed_at: Instant,
    pub result: Result<LiveIoResult, LiveIoError>,
}

/// Times are cumulative nanoseconds. A barrier is never issued by this queue.
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct LiveIoStats {
    pub read_requests: u64,
    pub write_requests: u64,
    pub read_operations: u64,
    pub write_operations: u64,
    pub requested_read_bytes: u64,
    pub requested_write_bytes: u64,
    pub completed_read_bytes: u64,
    pub completed_write_bytes: u64,
    pub completed_read_ops: u64,
    pub completed_write_ops: u64,
    pub submissions: u64,
    pub completions: u64,
    pub errors: u64,
    pub timeouts: u64,
    pub cancellations: u64,
    pub queue_ns: u128,
    pub submit_ns: u128,
    pub submit_to_cq_ns: u128,
    pub barrier_count: u64,
    pub barrier_ns: u128,
    /// Member bytes delivered as slices of the I/O block instead of a copy.
    #[serde(default)]
    pub copy_bytes_avoided: u64,
}

struct Buffer {
    data: Vec<u8>,
    start: usize,
    len: usize,
}
impl Buffer {
    fn new(len: usize, alignment: usize) -> Self {
        let data = vec![0; len + alignment - 1];
        let start = (alignment - data.as_ptr() as usize % alignment) % alignment;
        Self { data, start, len }
    }
    fn ptr(&mut self) -> *mut u8 {
        self.data.as_mut_ptr().wrapping_add(self.start)
    }
    fn slice(&self) -> &[u8] {
        &self.data[self.start..self.start + self.len]
    }
    fn slice_mut(&mut self) -> &mut [u8] {
        &mut self.data[self.start..self.start + self.len]
    }
}

/// Where dropped member blocks go back to their queue. Owner of the receiving
/// end: the `LiveIo`. Capacity: one block per slot, and at most `max_bytes` of
/// block memory queued at once. Full policy: a block that exceeds either bound,
/// or arrives after the queue dropped, is freed by its dropper, so a drop never
/// blocks and the queue never holds more than its byte budget in returns.
struct ReturnLane {
    sender: mpsc::SyncSender<Buffer>,
    /// Bytes of blocks sent and not yet received. Relaxed: an independent
    /// counter; the blocks themselves are handed over by the channel, which
    /// orders a block's `fetch_add` before the receiver's `fetch_sub`.
    queued_bytes: AtomicUsize,
    max_bytes: usize,
}
impl ReturnLane {
    fn give_back(&self, buffer: Buffer) {
        let len = buffer.data.len();
        let mut queued = self.queued_bytes.load(Ordering::Relaxed);
        for _ in 0..16 {
            let Some(next) = queued.checked_add(len).filter(|&n| n <= self.max_bytes) else {
                return;
            };
            match self.queued_bytes.compare_exchange_weak(
                queued,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    if self.sender.try_send(buffer).is_err() {
                        self.queued_bytes.fetch_sub(len, Ordering::Relaxed);
                    }
                    return;
                }
                Err(current) => queued = current,
            }
        }
        // Contended past the retry bound: freeing the block is always safe.
    }
}

/// Owner of one verified read member: the slice is `member` of the aligned block.
/// Dropping the last `Bytes` over it hands the block back to its queue through
/// its `ReturnLane`. The block is lent only after its terminal CQE, so no kernel
/// operation can still target it.
struct MemberBuffer {
    buffer: Option<Buffer>,
    member: std::ops::Range<usize>,
    lane: Arc<ReturnLane>,
}
impl AsRef<[u8]> for MemberBuffer {
    fn as_ref(&self) -> &[u8] {
        match &self.buffer {
            Some(buffer) => &buffer.slice()[self.member.clone()],
            None => &[],
        }
    }
}
impl minibytes::BytesOwner for MemberBuffer {}
impl Drop for MemberBuffer {
    fn drop(&mut self) {
        if let Some(buffer) = self.buffer.take() {
            self.lane.give_back(buffer);
        }
    }
}
struct Operation {
    request: u64,
    index: usize,
    buffer: Buffer,
    member_start: usize,
    member_len: usize,
    write: bool,
    queued: Instant,
    submitted: Option<Instant>,
}
struct Request {
    remaining: usize,
    records: Vec<Option<Bytes>>,
    write: bool,
    deadline: Instant,
    cancelled_at: Option<Instant>,
    error: Option<LiveIoError>,
    charge: usize,
    trace: Option<crate::distributed::remote_db::ReadTraceChild>,
}

pub struct LiveIo {
    ring: IoUring,
    namespace: LiveNamespace,
    slots: usize,
    max_bytes: usize,
    allocated_bytes: usize,
    pool: Vec<Buffer>,
    /// Blocks returned by dropped member slices; drained only by this queue.
    returns: mpsc::Receiver<Buffer>,
    lane: Arc<ReturnLane>,
    operations: HashMap<u64, Operation>,
    requests: HashMap<u64, Request>,
    queued: VecDeque<u64>,
    next_token: u64,
    trace_queue: Option<u64>,
    closed: bool,
    shutdown_at: Option<Instant>,
    fatal_at: Option<Instant>,
    stats: LiveIoStats,
    used_bytes: usize,
    #[cfg(test)]
    fail_buffer_after: Option<usize>,
}
impl LiveIo {
    pub fn new(
        namespace: LiveNamespace,
        max_inflight_slots: usize,
        max_inflight_bytes: usize,
    ) -> Result<Self, LiveIoError> {
        if max_inflight_slots == 0 || max_inflight_slots > 32768 || max_inflight_bytes == 0 {
            return Err(LiveIoError::Invalid("invalid queue limits"));
        }
        let ring = IoUring::builder()
            .dontfork()
            .build(max_inflight_slots.next_power_of_two() as u32)?;
        // At most one returned block per slot is queued, and no more than
        // `max_inflight_bytes` of them (see `ReturnLane`).
        let (sender, returns) = mpsc::sync_channel(max_inflight_slots);
        let lane = Arc::new(ReturnLane {
            sender,
            queued_bytes: AtomicUsize::new(0),
            max_bytes: max_inflight_bytes,
        });
        Ok(Self {
            ring,
            namespace,
            slots: max_inflight_slots,
            max_bytes: max_inflight_bytes,
            allocated_bytes: 0,
            pool: Vec::new(),
            returns,
            lane,
            operations: HashMap::with_capacity(max_inflight_slots),
            requests: HashMap::with_capacity(max_inflight_slots),
            queued: VecDeque::with_capacity(max_inflight_slots),
            next_token: 1,
            trace_queue: None,
            closed: false,
            shutdown_at: None,
            fatal_at: None,
            stats: LiveIoStats::default(),
            used_bytes: 0,
            #[cfg(test)]
            fail_buffer_after: None,
        })
    }
    pub fn stats(&self) -> LiveIoStats {
        self.stats.clone()
    }
    pub fn namespace(&self) -> &LiveNamespace {
        &self.namespace
    }
    /// Descriptor that polls readable while the completion queue holds an entry.
    pub(crate) fn ring_fd(&self) -> RawFd {
        self.ring.as_raw_fd()
    }
    /// Accepted member operations without a terminal CQE.
    pub(crate) fn outstanding(&self) -> usize {
        self.operations.len()
    }
    /// Whether any CQE awaits harvest. A progress call is then non-blocking work.
    pub(crate) fn completion_ready(&mut self) -> bool {
        !self.ring.completion().is_empty()
    }
    /// Free request credit: `(member slots, bytes)` another read may take now.
    pub(crate) fn credit(&self) -> (usize, usize) {
        (
            self.slots.saturating_sub(self.operations.len()),
            self.max_bytes.saturating_sub(self.used_bytes),
        )
    }
    /// Total request credit, for the check that a read fits an idle queue.
    pub(crate) fn capacity(&self) -> (usize, usize) {
        (self.slots, self.max_bytes)
    }
    /// Byte credit one read takes: its aligned I/O block plus alignment slack,
    /// exactly as `submit_reads` charges it.
    pub(crate) fn read_charge(&self, offset: u64, len: usize) -> Result<usize, LiveIoError> {
        let (_, size, _) = self.range(offset, len)?;
        let alignment = (self.namespace.block_size as usize).max(4096);
        size.checked_add(alignment - 1)
            .ok_or(LiveIoError::Invalid("request charge overflow"))
    }
    fn range(&self, offset: u64, len: usize) -> Result<(u64, usize, usize), LiveIoError> {
        if len == 0 {
            return Err(LiveIoError::Invalid("empty range"));
        }
        let block = u64::from(self.namespace.block_size);
        let end = offset
            .checked_add(u64::try_from(len).map_err(|_| LiveIoError::Invalid("range overflow"))?)
            .ok_or(LiveIoError::Invalid("range overflow"))?;
        if end > self.namespace.bytes {
            return Err(LiveIoError::Invalid("range exceeds namespace"));
        }
        let start = offset / block * block;
        let aligned_end = end
            .checked_add(block - 1)
            .ok_or(LiveIoError::Invalid("range overflow"))?
            / block
            * block;
        let size = usize::try_from(aligned_end - start)
            .map_err(|_| LiveIoError::Invalid("range overflow"))?;
        if size > u32::MAX as usize {
            return Err(LiveIoError::Invalid("range too large"));
        }
        Ok((start, size, (offset - start) as usize))
    }
    /// Re-admits blocks that dropped member slices returned. Lent blocks left
    /// the byte budget, so a block re-enters only while the budget has room;
    /// retained slices therefore cannot starve this queue. Bounded by `slots`.
    fn reclaim(&mut self) {
        for _ in 0..self.slots {
            let Ok(buffer) = self.returns.try_recv() else {
                return;
            };
            let len = buffer.data.len();
            self.lane.queued_bytes.fetch_sub(len, Ordering::Relaxed);
            if self
                .allocated_bytes
                .checked_add(len)
                .is_some_and(|n| n <= self.max_bytes)
            {
                self.allocated_bytes += len;
                self.pool.push(buffer);
            }
        }
    }
    fn buffer(&mut self, len: usize) -> Result<Buffer, LiveIoError> {
        #[cfg(test)]
        if let Some(left) = self.fail_buffer_after.as_mut() {
            if *left == 0 {
                self.fail_buffer_after = None;
                return Err(LiveIoError::Invalid("injected buffer failure"));
            }
            *left -= 1;
        }
        self.reclaim();
        if let Some(index) = self.pool.iter().position(|b| b.len == len) {
            return Ok(self.pool.swap_remove(index));
        }
        let align = (self.namespace.block_size as usize).max(4096);
        let charge = len
            .checked_add(align - 1)
            .ok_or(LiveIoError::Invalid("buffer overflow"))?;
        while self
            .allocated_bytes
            .checked_add(charge)
            .is_none_or(|n| n > self.max_bytes)
        {
            let Some(buffer) = self.pool.pop() else {
                return Err(LiveIoError::Invalid("buffer capacity exhausted"));
            };
            self.allocated_bytes -= buffer.data.len();
        }
        let b = Buffer::new(len, align);
        self.allocated_bytes += b.data.len();
        Ok(b)
    }
    pub fn submit_write(
        &mut self,
        id: u64,
        offset: u64,
        data: &[u8],
        deadline: Instant,
    ) -> Result<LiveSubmitOutcome, LiveIoError> {
        if self.namespace.access != LiveNamespaceAccess::ReadWrite {
            return Err(LiveIoError::Invalid("namespace is read only"));
        }
        let (start, size, member) = self.range(offset, data.len())?;
        if member != 0 || size != data.len() {
            return Err(LiveIoError::Invalid("write must cover aligned whole frame"));
        }
        self.submit(id, &[(start, size, 0, size)], Some(data), deadline, None)
    }
    pub fn submit_reads(
        &mut self,
        id: u64,
        ranges: &[(u64, usize)],
        deadline: Instant,
    ) -> Result<LiveSubmitOutcome, LiveIoError> {
        self.submit_reads_traced(id, ranges, deadline, None)
    }
    pub(crate) fn submit_reads_traced(
        &mut self,
        id: u64,
        ranges: &[(u64, usize)],
        deadline: Instant,
        trace: Option<&crate::distributed::remote_db::ReadTraceChild>,
    ) -> Result<LiveSubmitOutcome, LiveIoError> {
        if ranges.len() > self.slots {
            return Err(LiveIoError::Invalid("request exceeds slot limit"));
        }
        let mut aligned = Vec::with_capacity(ranges.len());
        for &(offset, len) in ranges {
            let (start, size, member) = self.range(offset, len)?;
            aligned.push((start, size, member, len));
        }
        let outcome = self.submit(id, &aligned, None, deadline, trace)?;
        if outcome == LiveSubmitOutcome::Submitted {
            // Start the I/O now: a lane keeps several requests in flight and
            // must not wait for its next progress call to submit.
            self.flush_submissions();
        }
        Ok(outcome)
    }
    fn submit(
        &mut self,
        id: u64,
        ranges: &[(u64, usize, usize, usize)],
        data: Option<&[u8]>,
        deadline: Instant,
        trace: Option<&crate::distributed::remote_db::ReadTraceChild>,
    ) -> Result<LiveSubmitOutcome, LiveIoError> {
        if self.closed || self.fatal_at.is_some() {
            return Err(LiveIoError::Invalid("queue closed"));
        }
        if ranges.is_empty() || self.requests.contains_key(&id) {
            return Err(LiveIoError::Invalid("empty or duplicate request"));
        }
        if Instant::now() >= deadline {
            return Err(LiveIoError::Timeout);
        }
        if ranges.len() > self.slots {
            return Err(LiveIoError::Invalid("request exceeds slot limit"));
        }
        let alignment = (self.namespace.block_size as usize).max(4096);
        // Read members are delivered as slices of their block: no copy charge.
        let charge = ranges
            .iter()
            .try_fold(0usize, |total, r| {
                total.checked_add(r.1.checked_add(alignment - 1)?)
            })
            .ok_or(LiveIoError::Invalid("request charge overflow"))?;
        if charge > self.max_bytes {
            return Err(LiveIoError::Invalid("request exceeds byte limit"));
        }
        if self.operations.len() + ranges.len() > self.slots
            || self
                .used_bytes
                .checked_add(charge)
                .is_none_or(|n| n > self.max_bytes)
        {
            return Ok(LiveSubmitOutcome::Full);
        }
        let end = self
            .next_token
            .checked_add(ranges.len() as u64)
            .ok_or(LiveIoError::Invalid("token exhausted"))?;
        let mut entries = Vec::with_capacity(ranges.len());
        let mut prepared: Vec<(u64, Operation)> = Vec::with_capacity(ranges.len());
        for (index, &(offset, len, member_start, member_len)) in ranges.iter().enumerate() {
            let mut buffer = match self.buffer(len) {
                Ok(buffer) => buffer,
                Err(error) => {
                    for (_, op) in prepared {
                        self.pool.push(op.buffer);
                    }
                    return Err(error);
                }
            };
            if let Some(data) = data {
                buffer.slice_mut().copy_from_slice(data);
            }
            let token = self.next_token + index as u64;
            let entry = if data.is_some() {
                opcode::Write::new(
                    types::Fd(self.namespace.file.as_raw_fd()),
                    buffer.ptr(),
                    len as u32,
                )
                .offset(offset)
                .build()
            } else {
                opcode::Read::new(
                    types::Fd(self.namespace.file.as_raw_fd()),
                    buffer.ptr(),
                    len as u32,
                )
                .offset(offset)
                .build()
            }
            .user_data(token);
            entries.push(entry);
            prepared.push((
                token,
                Operation {
                    request: id,
                    index,
                    buffer,
                    member_start,
                    member_len,
                    write: data.is_some(),
                    queued: Instant::now(),
                    submitted: None,
                },
            ));
        }
        // SAFETY: SQEs reference allocations moved into operations below; they and
        // the namespace descriptor remain alive until every terminal CQE. No SQPOLL
        // is enabled, so the kernel cannot observe these SQEs before submit.
        if unsafe { self.ring.submission().push_multiple(&entries) }.is_err() {
            for (_, op) in prepared {
                self.pool.push(op.buffer);
            }
            return Ok(LiveSubmitOutcome::Full);
        }
        for (token, op) in prepared {
            self.operations.insert(token, op);
        }
        if let Some(trace) = trace
            && self.trace_queue.is_none()
        {
            self.trace_queue = NEXT_TRACE_QUEUE
                .fetch_update(
                    std::sync::atomic::Ordering::Relaxed,
                    std::sync::atomic::Ordering::Relaxed,
                    |value| value.checked_add(1),
                )
                .ok();
            if self.trace_queue.is_none() {
                trace.event("live_io_queue_id_exhausted");
            }
        }
        self.requests.insert(
            id,
            Request {
                remaining: ranges.len(),
                records: (0..ranges.len()).map(|_| None).collect(),
                write: data.is_some(),
                deadline,
                cancelled_at: None,
                error: None,
                charge,
                trace: trace.cloned(),
            },
        );
        self.used_bytes += charge;
        self.queued.extend(self.next_token..end);
        self.next_token = end;
        if data.is_some() {
            self.stats.write_requests += 1;
            self.stats.write_operations += ranges.len() as u64;
            self.stats.requested_write_bytes += ranges.iter().map(|r| r.1 as u64).sum::<u64>();
        } else {
            self.stats.read_requests += 1;
            self.stats.read_operations += ranges.len() as u64;
            self.stats.requested_read_bytes += ranges.iter().map(|r| r.1 as u64).sum::<u64>();
        }
        Ok(LiveSubmitOutcome::Submitted)
    }
    /// Cancellation does not release ownership or imply that a write had no effect.
    pub fn cancel(&mut self, id: u64) {
        if let Some(r) = self.requests.get_mut(&id) {
            r.cancelled_at.get_or_insert_with(Instant::now);
        }
    }
    pub fn begin_shutdown(&mut self) {
        self.closed = true;
        self.shutdown_at.get_or_insert_with(Instant::now);
    }
    /// Hands queued SQEs to the kernel. A transient EBUSY/EINTR leaves them
    /// queued for the next call.
    fn flush_submissions(&mut self) {
        if !self.ring.submission().is_empty() {
            let started = Instant::now();
            match self.ring.submit() {
                Ok(n) => {
                    self.stats.submit_ns += started.elapsed().as_nanos();
                    self.stats.submissions += n as u64;
                    for _ in 0..n {
                        let Some(token) = self.queued.pop_front() else {
                            std::process::abort()
                        };
                        let Some(op) = self.operations.get_mut(&token) else {
                            std::process::abort()
                        };
                        self.stats.queue_ns += started.duration_since(op.queued).as_nanos();
                        op.submitted = Some(started);
                        if let (Some(trace), Some(queue)) = (
                            self.requests
                                .get(&op.request)
                                .and_then(|r| r.trace.as_ref()),
                            self.trace_queue,
                        ) {
                            trace.event_io("live_io_sqe_submitted", queue, token);
                        }
                    }
                }
                Err(e)
                    if matches!(
                        e.kind(),
                        io::ErrorKind::Interrupted | io::ErrorKind::WouldBlock
                    ) || e.raw_os_error() == Some(libc::EBUSY) => {}
                Err(e) => {
                    self.stats.errors += 1;
                    self.fatal_at.get_or_insert_with(Instant::now);
                    for r in self.requests.values_mut() {
                        if r.error.is_none() {
                            r.error = Some(if r.write {
                                LiveIoError::UnknownWrite
                            } else {
                                LiveIoError::Io(io::Error::new(e.kind(), e.to_string()))
                            });
                        }
                    }
                }
            }
        }
    }
    pub fn progress(&mut self) -> Result<Vec<LiveCompletion>, LiveIoError> {
        self.flush_submissions();
        let mut out = Vec::new();
        for _ in 0..self.operations.len() {
            let Some(cqe) = self.ring.completion().next() else {
                break;
            };
            let Some(op) = self.operations.remove(&cqe.user_data()) else {
                std::process::abort()
            };
            let Operation {
                request,
                index,
                buffer,
                member_start,
                member_len,
                write,
                submitted,
                ..
            } = op;
            let Some(req) = self.requests.get_mut(&request) else {
                std::process::abort()
            };
            if let (Some(trace), Some(queue)) = (&req.trace, self.trace_queue) {
                trace.event_io("live_io_cqe_observed", queue, cqe.user_data());
            }
            let result = cqe.result();
            self.stats.completions += 1;
            if let Some(started) = submitted {
                self.stats.submit_to_cq_ns += started.elapsed().as_nanos();
            }
            let complete = usize::try_from(result).is_ok_and(|n| n == buffer.len);
            if !complete {
                self.stats.errors += 1;
                if req.error.is_none() {
                    req.error = Some(if write {
                        LiveIoError::UnknownWrite
                    } else if result < 0 {
                        LiveIoError::Io(io::Error::from_raw_os_error(-result))
                    } else {
                        LiveIoError::Io(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "short namespace read",
                        ))
                    });
                }
            } else if write {
                self.stats.completed_write_bytes += buffer.len as u64;
                self.stats.completed_write_ops += 1;
            } else {
                self.stats.completed_read_bytes += buffer.len as u64;
                self.stats.completed_read_ops += 1;
            }
            if complete && !write && req.cancelled_at.is_none() && Instant::now() < req.deadline {
                // Lend the block: it leaves the byte budget until it returns.
                self.allocated_bytes -= buffer.data.len();
                self.stats.copy_bytes_avoided += member_len as u64;
                req.records[index] = Some(Bytes::from_owner(MemberBuffer {
                    buffer: Some(buffer),
                    member: member_start..member_start + member_len,
                    lane: self.lane.clone(),
                }));
            } else {
                self.pool.push(buffer);
            }
            req.remaining -= 1;
            if req.remaining == 0 {
                let Some(mut req) = self.requests.remove(&request) else {
                    std::process::abort()
                };
                self.used_bytes -= req.charge;
                let timed_out = Instant::now() >= req.deadline;
                if timed_out {
                    self.stats.timeouts += 1;
                }
                if req.cancelled_at.is_some() {
                    self.stats.cancellations += 1;
                }
                let result = if let Some(e) = req.error.take() {
                    Err(e)
                } else if req.write && (timed_out || req.cancelled_at.is_some()) {
                    Err(LiveIoError::UnknownWrite)
                } else if req.cancelled_at.is_some() {
                    Err(LiveIoError::Cancelled)
                } else if timed_out {
                    Err(LiveIoError::Timeout)
                } else if req.write {
                    Ok(LiveIoResult::Written)
                } else {
                    match req.records.into_iter().collect::<Option<Vec<_>>>() {
                        Some(records) => Ok(LiveIoResult::Reads(records)),
                        None => Err(LiveIoError::Invalid("missing read result")),
                    }
                };
                out.push(LiveCompletion {
                    id: request,
                    completed_at: Instant::now(),
                    result,
                });
            }
        }
        let now = Instant::now();
        let expired = |at: Instant| {
            now.checked_duration_since(at)
                .is_some_and(|d| d >= DRAIN_TIMEOUT)
        };
        if !self.operations.is_empty()
            && (self.fatal_at.is_some_and(expired)
                || self.shutdown_at.is_some_and(expired)
                || self
                    .requests
                    .values()
                    .any(|r| expired(r.deadline) || r.cancelled_at.is_some_and(expired)))
        {
            std::process::abort();
        }
        Ok(out)
    }
    /// Drains accepted operations and returns their completions; caller must
    /// retain them rather than interpreting a drained write as durable.
    pub fn shutdown(&mut self) -> Result<Vec<LiveCompletion>, LiveIoError> {
        self.begin_shutdown();
        let mut completed = Vec::new();
        while !self.operations.is_empty() {
            completed.extend(self.progress()?);
            if !self.operations.is_empty() {
                std::thread::sleep(Duration::from_micros(50));
            }
        }
        Ok(completed)
    }
}
impl Drop for LiveIo {
    fn drop(&mut self) {
        if !self.operations.is_empty() {
            std::process::abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::OpenOptions;

    fn fixture() -> (tempdir::TempDir, LiveIo) {
        let dir = tempdir::TempDir::new("live-namespace").unwrap();
        let path = dir.path().join("wal");
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)
            .unwrap();
        file.set_len(8192).unwrap();
        let namespace =
            LiveNamespace::open_local_file(&path, 8192, 4096, LiveNamespaceAccess::ReadWrite)
                .unwrap();
        let io = LiveIo::new(namespace, 4, 4 * (4096 + 4095)).unwrap();
        (dir, io)
    }
    fn completion(io: &mut LiveIo) -> LiveCompletion {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(c) = io.progress().unwrap().pop() {
                return c;
            }
            assert!(
                Instant::now() < deadline,
                "local file operation did not retire"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
    }
    #[test]
    fn local_file_exact_write_and_ordered_ranged_reads() {
        let (_dir, mut io) = fixture();
        let data: Vec<u8> = (0..4096).map(|n| (n % 251) as u8).collect();
        let deadline = Instant::now() + Duration::from_secs(10);
        assert_eq!(
            io.submit_write(1, 0, &data, deadline).unwrap(),
            LiveSubmitOutcome::Submitted
        );
        assert!(matches!(
            completion(&mut io).result,
            Ok(LiveIoResult::Written)
        ));
        assert_eq!(
            io.submit_reads(2, &[(300, 30), (0, 10), (4090, 6)], deadline)
                .unwrap(),
            LiveSubmitOutcome::Submitted
        );
        let LiveIoResult::Reads(records) = completion(&mut io).result.unwrap() else {
            panic!("expected reads")
        };
        let records: Vec<Vec<u8>> = records.iter().map(|record| record[..].to_vec()).collect();
        assert_eq!(
            records,
            vec![
                data[300..330].to_vec(),
                data[..10].to_vec(),
                data[4090..].to_vec()
            ]
        );
        assert_eq!(io.stats().copy_bytes_avoided, 46);
        assert_eq!(io.stats().completions, 4);
        assert_eq!(io.stats().barrier_count, 0);
        assert!(io.shutdown().unwrap().is_empty());
    }
    fn write_pattern(io: &mut LiveIo, offset: u64, seed: u8) -> Vec<u8> {
        let data: Vec<u8> = (0..4096).map(|n| (n % 250) as u8 ^ seed).collect();
        let deadline = Instant::now() + Duration::from_secs(10);
        assert_eq!(
            io.submit_write(900 + u64::from(seed), offset, &data, deadline)
                .unwrap(),
            LiveSubmitOutcome::Submitted
        );
        assert!(matches!(completion(io).result, Ok(LiveIoResult::Written)));
        data
    }
    fn read_member(io: &mut LiveIo, id: u64, offset: u64, len: usize) -> Bytes {
        let deadline = Instant::now() + Duration::from_secs(10);
        assert_eq!(
            io.submit_reads(id, &[(offset, len)], deadline).unwrap(),
            LiveSubmitOutcome::Submitted
        );
        let completed = completion(io);
        assert_eq!(completed.id, id);
        let LiveIoResult::Reads(mut records) = completed.result.unwrap() else {
            panic!("expected reads")
        };
        records.pop().unwrap()
    }
    #[test]
    fn member_slice_pins_its_block_until_every_slice_drops_then_recycles() {
        let (_dir, mut io) = fixture();
        let first = write_pattern(&mut io, 0, 1);
        let second = write_pattern(&mut io, 4096, 2);
        let slice = read_member(&mut io, 1, 10, 20);
        let part = slice.slice(0..5);
        assert_eq!(&slice[..], &first[10..30]);
        // The block is lent: outside the byte budget and not in the pool.
        io.reclaim();
        assert_eq!((io.pool.len(), io.allocated_bytes), (0, 0));
        // A later read must not reuse or overwrite the lent block.
        let other = read_member(&mut io, 2, 4096 + 10, 20);
        assert_eq!(&other[..], &second[10..30]);
        assert_eq!(&slice[..], &first[10..30]);
        drop(other);
        drop(slice);
        io.reclaim();
        // Another slice still references the first block.
        assert_eq!(io.pool.len(), 1);
        assert_eq!(&part[..], &first[10..15]);
        drop(part);
        io.reclaim();
        assert_eq!(io.pool.len(), 2);
        assert_eq!(io.allocated_bytes, 2 * (4096 + 4095));
        assert_eq!(io.stats().copy_bytes_avoided, 40);
        // A recycled block serves the next read without a new allocation.
        let again = read_member(&mut io, 3, 20, 4);
        assert_eq!(&again[..], &first[20..24]);
        assert_eq!(io.allocated_bytes, 4096 + 4095);
        assert!(io.shutdown().unwrap().is_empty());
    }
    #[test]
    fn blocks_returned_together_never_queue_more_bytes_than_the_budget() {
        let block = 4096 + 4095;
        let dir = tempdir::TempDir::new("live-return-budget").unwrap();
        let path = dir.path().join("wal");
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)
            .unwrap();
        file.set_len(8192).unwrap();
        let namespace =
            LiveNamespace::open_local_file(&path, 8192, 4096, LiveNamespaceAccess::ReadWrite)
                .unwrap();
        // Eight slots (so the channel could hold eight blocks) but a budget of two.
        let mut io = LiveIo::new(namespace, 8, 2 * block).unwrap();
        let data = write_pattern(&mut io, 0, 9);
        // Lent blocks leave the budget, so all eight reads succeed and stay retained.
        let retained: Vec<Bytes> = (0..8)
            .map(|n| read_member(&mut io, 20 + n, n * 4, 4))
            .collect();
        for (n, slice) in retained.iter().enumerate() {
            assert_eq!(&slice[..], &data[n * 4..n * 4 + 4]);
        }
        // Dropping them together offers eight blocks; only the budget is queued.
        drop(retained);
        let queued = io.lane.queued_bytes.load(Ordering::Relaxed);
        assert!(queued <= 2 * block, "queued {queued} > budget");
        assert_eq!(queued, 2 * block);
        io.reclaim();
        assert_eq!(io.lane.queued_bytes.load(Ordering::Relaxed), 0);
        assert_eq!(io.pool.len(), 2);
        assert!(io.allocated_bytes <= 2 * block);
        assert!(io.shutdown().unwrap().is_empty());
    }
    #[test]
    fn retained_slices_neither_starve_the_queue_nor_dangle_after_it_drops() {
        let (_dir, mut io) = fixture();
        let data = write_pattern(&mut io, 0, 3);
        // The budget holds four blocks; lent blocks leave it.
        let retained: Vec<Bytes> = (0..8)
            .map(|n| read_member(&mut io, 10 + n, n * 4, 4))
            .collect();
        assert_eq!(io.allocated_bytes, 0);
        assert!(io.shutdown().unwrap().is_empty());
        drop(io);
        for (n, slice) in retained.iter().enumerate() {
            assert_eq!(&slice[..], &data[n * 4..n * 4 + 4]);
        }
        drop(retained);
    }
    #[test]
    fn requests_run_in_flight_together_and_complete_independently() {
        let (_dir, mut io) = fixture();
        let first = write_pattern(&mut io, 0, 4);
        let second = write_pattern(&mut io, 4096, 5);
        let deadline = Instant::now() + Duration::from_secs(10);
        for (id, offset) in [(1, 0), (2, 4096), (3, 100)] {
            assert_eq!(
                io.submit_reads(id, &[(offset, 8)], deadline).unwrap(),
                LiveSubmitOutcome::Submitted
            );
        }
        // Submission starts the I/O; none of it waits for a progress call.
        assert_eq!(io.stats().submissions, 3 + 2);
        assert_eq!(io.outstanding(), 3);
        let mut done = Vec::new();
        let patience = Instant::now() + Duration::from_secs(10);
        while done.len() < 3 {
            assert!(Instant::now() < patience, "reads did not retire");
            done.extend(io.progress().unwrap());
        }
        done.sort_by_key(|c| c.id);
        let expected = [&first[..8], &second[..8], &first[100..108]];
        for (completion, want) in done.into_iter().zip(expected) {
            let LiveIoResult::Reads(records) = completion.result.unwrap() else {
                panic!("expected reads")
            };
            assert_eq!(&records[0][..], want);
        }
    }
    #[test]
    fn truncated_local_file_short_read_retires_without_retry() {
        let (dir, mut io) = fixture();
        OpenOptions::new()
            .write(true)
            .open(dir.path().join("wal"))
            .unwrap()
            .set_len(4096)
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        assert_eq!(
            io.submit_reads(1, &[(0, 8192)], deadline).unwrap(),
            LiveSubmitOutcome::Submitted
        );
        let result = completion(&mut io);
        assert_eq!(result.id, 1);
        assert!(
            matches!(&result.result, Err(LiveIoError::Io(e)) if e.kind() == io::ErrorKind::UnexpectedEof)
        );
        let stats = io.stats();
        assert_eq!(stats.read_operations, 1);
        assert_eq!(stats.submissions, 1);
        assert_eq!(stats.completions, 1);
        assert_eq!(stats.errors, 1);
        assert!(io.shutdown().unwrap().is_empty());
    }
    #[test]
    fn failed_partial_preparation_restores_buffer_capacity() {
        let (_dir, mut io) = fixture();
        io.fail_buffer_after = Some(1);
        let deadline = Instant::now() + Duration::from_secs(10);
        assert!(io.submit_reads(1, &[(0, 1), (4096, 1)], deadline).is_err());
        assert_eq!(io.pool.len(), 1);
        assert_eq!(
            io.submit_reads(2, &[(0, 1)], deadline).unwrap(),
            LiveSubmitOutcome::Submitted
        );
        assert!(matches!(
            completion(&mut io).result,
            Ok(LiveIoResult::Reads(_))
        ));
        assert_eq!(io.stats().submissions, 1);
        assert_eq!(io.stats().completions, 1);
        assert!(io.shutdown().unwrap().is_empty());
    }
    #[test]
    fn rejects_bad_ranges_and_cancelled_write_is_unknown_after_retirement() {
        let (_dir, mut io) = fixture();
        let deadline = Instant::now() + Duration::from_secs(10);
        assert!(io.submit_write(1, 1, &[0; 4096], deadline).is_err());
        assert!(io.submit_write(1, 0, &[0; 17], deadline).is_err());
        assert!(io.submit_reads(1, &[(8190, 4)], deadline).is_err());
        assert!(io.submit_reads(1, &[(0, 0)], deadline).is_err());
        assert_eq!(
            io.submit_write(2, 0, &[7; 4096], deadline).unwrap(),
            LiveSubmitOutcome::Submitted
        );
        io.cancel(2);
        assert!(matches!(
            completion(&mut io).result,
            Err(LiveIoError::UnknownWrite)
        ));
        assert_eq!(io.stats().completions, 1);
        assert_eq!(io.stats().cancellations, 1);
    }
}
