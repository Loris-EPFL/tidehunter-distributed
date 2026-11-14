//! Bounded live namespace data plane. Data-sync writes request RWF_DSYNC; plain
//! write CQEs do not prove persistence. Receipts and publication belong to the
//! caller. Cancellation and deadlines report only after all operations retire.
use super::live_namespace::{LiveNamespace, LiveNamespaceAccess};
use io_uring::{IoUring, opcode, types};
use minibytes::Bytes;
use smallvec::SmallVec;
use std::collections::{HashMap, VecDeque};
use std::io;
use std::os::fd::{AsRawFd, RawFd};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

const DRAIN_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_MERGE_MEMBERS: usize = 32;
// Assigned lazily only to queues carrying a sampled read.
static NEXT_TRACE_QUEUE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

#[derive(Debug)]
pub enum LiveIoError {
    Invalid(&'static str),
    Io(io::Error),
    Timeout,
    Cancelled,
    UnknownWrite,
    Full,
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

/// Persistence requested on the write itself, not a separate queue barrier.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LiveWriteSync {
    None,
    Data,
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
    /// Writes submitted with RWF_DSYNC (FUA on a block namespace).
    #[serde(default)]
    pub data_sync_writes: u64,
    /// Read members served in addition to the first member of each SQE.
    #[serde(default)]
    pub merged_members: u64,
    #[serde(default)]
    pub read_sqes_saved: u64,
    /// Terminal negative write CQEs, independent of acknowledgement eligibility.
    #[serde(default)]
    pub failed_write_ops: u64,
    /// Terminal nonnegative write CQEs shorter than the submitted buffer.
    #[serde(default)]
    pub short_write_ops: u64,
    /// Bytes reported by short write CQEs; acknowledgement is still unknown.
    #[serde(default)]
    pub short_write_bytes: u64,
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

enum WriteData<'a> {
    Borrowed(&'a [u8]),
    Owned(&'a mut Option<Buffer>),
}

/// Exclusive loan from a queue's aligned pool. The mutable queue borrow prevents
/// other admissions until submission or drop, so its exact allocation charge
/// cannot be spent twice. Drop returns an unsubmitted buffer directly to the pool.
pub struct LiveWriteBuffer<'a> {
    queue: &'a mut LiveIo,
    buffer: Option<Buffer>,
}
impl std::ops::Deref for LiveWriteBuffer<'_> {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        match &self.buffer {
            Some(buffer) => buffer.slice(),
            None => &[],
        }
    }
}
impl std::ops::DerefMut for LiveWriteBuffer<'_> {
    fn deref_mut(&mut self) -> &mut [u8] {
        match &mut self.buffer {
            Some(buffer) => buffer.slice_mut(),
            None => &mut [],
        }
    }
}
impl LiveWriteBuffer<'_> {
    /// Moves the aligned allocation into the queue until its terminal CQE.
    pub fn submit_write_buffer(
        mut self,
        id: u64,
        offset: u64,
        sync: LiveWriteSync,
        deadline: Instant,
    ) -> Result<LiveSubmitOutcome, LiveIoError> {
        let (start, size, member) =
            self.queue
                .range(self.queue.namespace.bytes, offset, self.len())?;
        if member != 0 || size != self.len() {
            return Err(LiveIoError::Invalid("write must cover aligned whole frame"));
        }
        self.queue.submit(
            self.queue.namespace.file.as_raw_fd(),
            id,
            smallvec::smallvec![PlannedRead {
                offset: start,
                len: size,
                members: smallvec::smallvec![MemberSlice {
                    index: 0,
                    start: 0,
                    len: size,
                }],
            }],
            Some((WriteData::Owned(&mut self.buffer), sync)),
            deadline,
            None,
        )
    }
}
impl Drop for LiveWriteBuffer<'_> {
    fn drop(&mut self) {
        if let Some(buffer) = self.buffer.take() {
            self.queue.pool.push(buffer);
        }
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

/// Shared owner of one aligned read block. Every member is a slice of this
/// owner. The last slice returns the block, only after its terminal CQE.
struct MemberBuffer {
    buffer: Option<Buffer>,
    lane: Arc<ReturnLane>,
}
impl AsRef<[u8]> for MemberBuffer {
    fn as_ref(&self) -> &[u8] {
        match &self.buffer {
            Some(buffer) => buffer.slice(),
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
#[derive(Clone, Copy)]
struct MemberSlice {
    index: usize,
    start: usize,
    len: usize,
}
struct PlannedRead {
    offset: u64,
    len: usize,
    members: SmallVec<[MemberSlice; 1]>,
}
struct Operation {
    request: u64,
    members: SmallVec<[MemberSlice; 1]>,
    buffer: Buffer,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct NamespaceSlot(u32);

pub struct LiveIo {
    ring: IoUring,
    namespace: Arc<LiveNamespace>,
    attached: Vec<Arc<LiveNamespace>>,
    polled: bool,
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
    #[cfg(test)]
    next_completion_result: Option<i32>,
    #[cfg(test)]
    last_write_errno: Option<i32>,
    #[cfg(test)]
    held_ids: Vec<u64>,
    #[cfg(test)]
    completion_results: Vec<(u64, i32)>,
    #[cfg(test)]
    held_completions: Vec<LiveCompletion>,
}
impl LiveIo {
    /// Polled queues require kernel and device IOPOLL support. A setup or I/O
    /// error is returned; the queue never switches to interrupt mode.
    pub fn new(
        namespace: impl Into<Arc<LiveNamespace>>,
        max_inflight_slots: usize,
        max_inflight_bytes: usize,
        polled: bool,
    ) -> Result<Self, LiveIoError> {
        if max_inflight_slots == 0 || max_inflight_slots > 32768 || max_inflight_bytes == 0 {
            return Err(LiveIoError::Invalid("invalid queue limits"));
        }
        let mut builder = IoUring::builder();
        builder.dontfork();
        if polled {
            builder.setup_iopoll();
        }
        let ring = builder.build(max_inflight_slots.next_power_of_two() as u32)?;
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
            namespace: namespace.into(),
            attached: Vec::new(),
            polled,
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
            #[cfg(test)]
            next_completion_result: None,
            #[cfg(test)]
            last_write_errno: None,
            #[cfg(test)]
            held_ids: Vec::new(),
            #[cfg(test)]
            completion_results: Vec::new(),
            #[cfg(test)]
            held_completions: Vec::new(),
        })
    }
    pub(crate) fn register_worker_wake(&self, fd: std::os::fd::RawFd) -> io::Result<()> {
        self.ring.submitter().register_eventfd(fd)
    }

    #[cfg(test)]
    pub(crate) fn hold_completions(&mut self, ids: &[u64]) {
        self.held_ids.clear();
        self.held_ids.extend_from_slice(ids);
    }

    #[cfg(test)]
    pub(crate) fn release_held(&mut self) {
        self.held_ids.clear();
    }

    pub(crate) fn attach(
        &mut self,
        namespace: Arc<LiveNamespace>,
    ) -> Result<NamespaceSlot, LiveIoError> {
        if self.closed {
            return Err(LiveIoError::Invalid("queue closed"));
        }
        if namespace.block_size != self.namespace.block_size {
            return Err(LiveIoError::Invalid(
                "attached namespace block size differs",
            ));
        }
        if self.attached.len() >= 1024 {
            return Err(LiveIoError::Invalid("too many attached namespaces"));
        }
        let slot = NamespaceSlot(self.attached.len() as u32);
        self.attached.push(namespace);
        Ok(slot)
    }

    pub(crate) fn read_request_charge_at(
        &self,
        slot: NamespaceSlot,
        ranges: &[(u64, usize)],
    ) -> Result<(usize, usize), LiveIoError> {
        let namespace = self
            .attached
            .get(slot.0 as usize)
            .ok_or(LiveIoError::Invalid("invalid namespace slot"))?;
        let reads = self.plan_reads(namespace.bytes, ranges)?;
        Ok((reads.len(), self.plan_charge(&reads)?))
    }

    pub(crate) fn submit_reads_at(
        &mut self,
        id: u64,
        slot: NamespaceSlot,
        ranges: &[(u64, usize)],
        deadline: Instant,
        trace: Option<&crate::distributed::remote_db::ReadTraceChild>,
    ) -> Result<LiveSubmitOutcome, LiveIoError> {
        let namespace = self
            .attached
            .get(slot.0 as usize)
            .ok_or(LiveIoError::Invalid("invalid namespace slot"))?;
        let reads = self.plan_reads(namespace.bytes, ranges)?;
        let fd = namespace.file.as_raw_fd();
        let outcome = self.submit(fd, id, reads, None, deadline, trace)?;
        if outcome == LiveSubmitOutcome::Submitted {
            let _ = self.flush_submissions();
        }
        Ok(outcome)
    }
    /// Override one harvested CQE only after the real kernel operation retires.
    /// Test-only: no submission or buffer lifetime is simulated.
    #[cfg(test)]
    pub(crate) fn override_next_completion_result(&mut self, result: i32) {
        self.next_completion_result = Some(result);
    }
    #[cfg(test)]
    pub(crate) fn override_completion_result(&mut self, id: u64, result: i32) {
        self.completion_results.push((id, result));
    }
    #[cfg(test)]
    pub(crate) fn last_write_errno(&self) -> Option<i32> {
        self.last_write_errno
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
    /// Accepted SQE operations without a terminal CQE.
    pub(crate) fn outstanding(&self) -> usize {
        self.operations.len()
    }
    /// Whether any CQE awaits harvest. A progress call is then non-blocking work.
    pub(crate) fn completion_ready(&mut self) -> bool {
        !self.ring.completion().is_empty()
    }
    /// Free request credit: `(SQE slots, bytes)` another read may take now.
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
    /// Safe upper bound for one member: its aligned block plus alignment slack.
    /// Summing member charges remains safe when a request merges reads.
    pub(crate) fn read_charge(&self, offset: u64, len: usize) -> Result<usize, LiveIoError> {
        let (_, size, _) = self.range(self.namespace.bytes, offset, len)?;
        let alignment = (self.namespace.block_size as usize).max(4096);
        size.checked_add(alignment - 1)
            .ok_or(LiveIoError::Invalid("request charge overflow"))
    }
    /// Exact `(SQE slots, bytes)` taken by this complete request after merging.
    /// Like submission, validates every range and allows duplicate members.
    pub(crate) fn read_request_charge(
        &self,
        ranges: &[(u64, usize)],
    ) -> Result<(usize, usize), LiveIoError> {
        if let &[(offset, len)] = ranges {
            return Ok((1, self.read_charge(offset, len)?));
        }
        let reads = self.plan_reads(self.namespace.bytes, ranges)?;
        Ok((reads.len(), self.plan_charge(&reads)?))
    }
    fn plan_charge(&self, reads: &[PlannedRead]) -> Result<usize, LiveIoError> {
        let slack = (self.namespace.block_size as usize).max(4096) - 1;
        reads
            .iter()
            .try_fold(0usize, |total, read| {
                total.checked_add(read.len.checked_add(slack)?)
            })
            .ok_or(LiveIoError::Invalid("request charge overflow"))
    }
    fn plan_reads(
        &self,
        bytes: u64,
        ranges: &[(u64, usize)],
    ) -> Result<SmallVec<[PlannedRead; 1]>, LiveIoError> {
        if ranges.is_empty() || ranges.len() > self.slots * MAX_MERGE_MEMBERS {
            return Err(LiveIoError::Invalid("request exceeds member limit"));
        }
        let mut aligned = SmallVec::<[_; 32]>::with_capacity(ranges.len());
        for (index, &(offset, len)) in ranges.iter().enumerate() {
            let (start, size, member) = self.range(bytes, offset, len)?;
            aligned.push((start, size, member, len, index));
        }
        aligned.sort_unstable_by_key(|&(offset, _, _, _, index)| (offset, index));
        let slack = (self.namespace.block_size as usize).max(4096) - 1;
        let max_read = self.max_bytes.saturating_sub(slack).min(u32::MAX as usize);
        let mut reads = SmallVec::<[PlannedRead; 1]>::with_capacity(ranges.len());
        for (offset, len, member, member_len, index) in aligned {
            let end = offset + len as u64; // range() checked namespace bounds.
            if let Some(read) = reads.last_mut() {
                let read_end = read.offset + read.len as u64;
                let merged_len = end.max(read_end) - read.offset;
                if offset <= read_end
                    && read.members.len() < MAX_MERGE_MEMBERS
                    && merged_len <= max_read as u64
                {
                    read.len = merged_len as usize;
                    read.members.push(MemberSlice {
                        index,
                        start: (offset - read.offset) as usize + member,
                        len: member_len,
                    });
                    continue;
                }
            }
            reads.push(PlannedRead {
                offset,
                len,
                members: smallvec::smallvec![MemberSlice {
                    index,
                    start: member,
                    len: member_len
                }],
            });
        }
        Ok(reads)
    }
    fn range(
        &self,
        bytes: u64,
        offset: u64,
        len: usize,
    ) -> Result<(u64, usize, usize), LiveIoError> {
        if len == 0 {
            return Err(LiveIoError::Invalid("empty range"));
        }
        let block = u64::from(self.namespace.block_size);
        let end = offset
            .checked_add(u64::try_from(len).map_err(|_| LiveIoError::Invalid("range overflow"))?)
            .ok_or(LiveIoError::Invalid("range overflow"))?;
        if end > bytes {
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
    /// Tests slot and byte credit without borrowing a buffer.
    pub(crate) fn has_credit(&self, len: usize) -> bool {
        self.operations.len() < self.slots
            && len
                .checked_add((self.namespace.block_size as usize).max(4096) - 1)
                .and_then(|charge| self.used_bytes.checked_add(charge))
                .is_some_and(|bytes| bytes <= self.max_bytes)
    }

    /// Borrows one exact-charge aligned buffer, or returns Full without taking
    /// memory. Bytes are initialized but may hold a prior write. Callers must
    /// fill the whole frame, including padding, before submitting the loan.
    pub fn write_buffer(&mut self, len: usize) -> Result<LiveWriteBuffer<'_>, LiveIoError> {
        if self.closed || self.fatal_at.is_some() {
            return Err(LiveIoError::Invalid("queue closed"));
        }
        if self.namespace.access != LiveNamespaceAccess::ReadWrite {
            return Err(LiveIoError::Invalid("namespace is read only"));
        }
        let (_, size, member) = self.range(self.namespace.bytes, 0, len)?;
        if member != 0 || size != len {
            return Err(LiveIoError::Invalid("write must cover aligned whole frame"));
        }
        let charge = len
            .checked_add((self.namespace.block_size as usize).max(4096) - 1)
            .ok_or(LiveIoError::Invalid("buffer overflow"))?;
        if self.operations.len() == self.slots
            || self
                .used_bytes
                .checked_add(charge)
                .is_none_or(|n| n > self.max_bytes)
        {
            return Err(LiveIoError::Full);
        }
        let buffer = self.buffer(len)?;
        Ok(LiveWriteBuffer {
            queue: self,
            buffer: Some(buffer),
        })
    }

    pub fn submit_write(
        &mut self,
        id: u64,
        offset: u64,
        data: &[u8],
        sync: LiveWriteSync,
        deadline: Instant,
    ) -> Result<LiveSubmitOutcome, LiveIoError> {
        if self.namespace.access != LiveNamespaceAccess::ReadWrite {
            return Err(LiveIoError::Invalid("namespace is read only"));
        }
        let (start, size, member) = self.range(self.namespace.bytes, offset, data.len())?;
        if member != 0 || size != data.len() {
            return Err(LiveIoError::Invalid("write must cover aligned whole frame"));
        }
        self.submit(
            self.namespace.file.as_raw_fd(),
            id,
            smallvec::smallvec![PlannedRead {
                offset: start,
                len: size,
                members: smallvec::smallvec![MemberSlice {
                    index: 0,
                    start: 0,
                    len: size,
                }],
            }],
            Some((WriteData::Borrowed(data), sync)),
            deadline,
            None,
        )
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
        let reads = self.plan_reads(self.namespace.bytes, ranges)?;
        let outcome = self.submit(
            self.namespace.file.as_raw_fd(),
            id,
            reads,
            None,
            deadline,
            trace,
        )?;
        if outcome == LiveSubmitOutcome::Submitted {
            // Start the I/O now: a lane keeps several requests in flight and
            // must not wait for its next progress call to submit.
            let _ = self.flush_submissions();
        }
        Ok(outcome)
    }
    fn submit(
        &mut self,
        fd: RawFd,
        id: u64,
        ranges: SmallVec<[PlannedRead; 1]>,
        mut data: Option<(WriteData<'_>, LiveWriteSync)>,
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
        let charge = self.plan_charge(&ranges)?;
        if ranges.len() > self.slots || charge > self.max_bytes {
            return if data.is_some() {
                Err(LiveIoError::Invalid("request exceeds queue limit"))
            } else {
                Ok(LiveSubmitOutcome::Full)
            };
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
        let members: usize = ranges.iter().map(|read| read.members.len()).sum();
        let read_bytes: u64 = ranges.iter().map(|read| read.len as u64).sum();
        let operations = ranges.len();
        // Scalar reads and writes keep all transient SQE metadata inline.
        let mut entries = SmallVec::<[io_uring::squeue::Entry; 1]>::with_capacity(ranges.len());
        let mut prepared = SmallVec::<[(u64, Operation); 1]>::with_capacity(ranges.len());
        for (index, read) in ranges.into_iter().enumerate() {
            let PlannedRead {
                offset,
                len,
                members,
            } = read;
            let buffer = match data.as_mut() {
                Some((WriteData::Owned(buffer), _)) => buffer
                    .take()
                    .ok_or(LiveIoError::Invalid("write buffer missing")),
                _ => self.buffer(len),
            };
            let mut buffer = match buffer {
                Ok(buffer) => buffer,
                Err(error) => {
                    for (_, op) in prepared {
                        self.pool.push(op.buffer);
                    }
                    return Err(error);
                }
            };
            if let Some((WriteData::Borrowed(data), _)) = &data {
                buffer.slice_mut().copy_from_slice(data);
            }
            let token = self.next_token + index as u64;
            let entry = if data.is_some() {
                opcode::Write::new(types::Fd(fd), buffer.ptr(), len as u32)
                    .offset(offset)
                    .rw_flags(if matches!(data, Some((_, LiveWriteSync::Data))) {
                        libc::RWF_DSYNC
                    } else {
                        0
                    })
                    .build()
            } else {
                opcode::Read::new(types::Fd(fd), buffer.ptr(), len as u32)
                    .offset(offset)
                    .build()
            }
            .user_data(token);
            entries.push(entry);
            prepared.push((
                token,
                Operation {
                    request: id,
                    members,
                    buffer,
                    write: data.is_some(),
                    queued: Instant::now(),
                    submitted: None,
                },
            ));
        }
        // SAFETY: SQEs reference allocations moved into operations below; they and
        // every namespace descriptor remain alive until every terminal CQE.
        // Attached handles stay in `attached` until drop, which aborts if operations remain.
        // No SQPOLL observes SQEs before submit. push_multiple is all-or-nothing.
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
                remaining: operations,
                records: if data.is_some() {
                    Vec::new()
                } else {
                    (0..members).map(|_| None).collect()
                },
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
            self.stats.write_operations += operations as u64;
            self.stats.requested_write_bytes += read_bytes;
            if matches!(data, Some((_, LiveWriteSync::Data))) {
                self.stats.data_sync_writes += operations as u64;
            }
        } else {
            self.stats.read_requests += 1;
            self.stats.read_operations += operations as u64;
            self.stats.requested_read_bytes += read_bytes;
            self.stats.merged_members += (members - operations) as u64;
            self.stats.read_sqes_saved += (members - operations) as u64;
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
    /// Submit queued SQEs and wait for a CQE until the timeout. Polled queues
    /// spin with non-blocking kernel enters; interrupt queues sleep.
    /// Does not harvest completions; `progress` retains that responsibility.
    pub fn wait(&mut self, timeout: Duration) -> Result<bool, LiveIoError> {
        if self.polled {
            let deadline = Instant::now()
                .checked_add(timeout)
                .ok_or(LiveIoError::Invalid("wait deadline overflow"))?;
            while !self.completion_ready() {
                if Instant::now() >= deadline {
                    return Ok(false);
                }
                self.flush_submissions()?;
                std::hint::spin_loop();
            }
            return Ok(true);
        }
        let pending = self.ring.submission().len();
        let started = Instant::now();
        let timespec = types::Timespec::from(timeout);
        let result = self
            .ring
            .submitter()
            .submit_with_args(1, &types::SubmitArgs::new().timespec(&timespec));
        self.account_submissions(pending, started);
        match result {
            Ok(_) => Ok(self.completion_ready()),
            Err(error)
                if matches!(
                    error.raw_os_error(),
                    Some(libc::ETIME | libc::EINTR | libc::EBUSY)
                ) =>
            {
                Ok(false)
            }
            Err(error) => {
                self.mark_submission_error(&error);
                Err(LiveIoError::Io(error))
            }
        }
    }
    /// Flush a leased writer's SQE without waiting for its CQE. False means a
    /// transient submit left work queued; the caller retains ownership and retries.
    pub(crate) fn submit_pending_write(&mut self) -> Result<bool, LiveIoError> {
        self.flush_submissions()?;
        Ok(self.ring.submission().is_empty())
    }
    /// Hands queued SQEs to the kernel. A transient EBUSY/EINTR leaves any
    /// unconsumed SQEs queued for the next call.
    fn flush_submissions(&mut self) -> Result<(), LiveIoError> {
        let pending = self.ring.submission().len();
        if pending == 0 && !self.polled {
            return Ok(());
        }
        let started = Instant::now();
        let result = if self.polled {
            self.ring.submit_and_wait(0)
        } else {
            self.ring.submit()
        };
        self.account_submissions(pending, started);
        if let Err(error) = result {
            if matches!(
                error.kind(),
                io::ErrorKind::Interrupted | io::ErrorKind::WouldBlock
            ) || error.raw_os_error() == Some(libc::EBUSY)
            {
                return Ok(());
            }
            self.mark_submission_error(&error);
            return Err(LiveIoError::Io(error));
        }
        Ok(())
    }
    fn account_submissions(&mut self, pending: usize, started: Instant) {
        // Even ETIME/EINTR may follow consumed SQEs. With no SQPOLL and one
        // queue owner, the SQ head gives the exact number consumed by this enter.
        let submitted = pending - self.ring.submission().len();
        // A polled enter with no queued SQE only reaps completions: it is not
        // submission time. The interrupt ring keeps its accounting unchanged.
        if pending > 0 || !self.polled {
            self.stats.submit_ns += started.elapsed().as_nanos();
        }
        self.stats.submissions += submitted as u64;
        for _ in 0..submitted {
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
    fn mark_submission_error(&mut self, error: &io::Error) {
        self.stats.errors += 1;
        self.fatal_at.get_or_insert_with(Instant::now);
        for request in self.requests.values_mut() {
            if request.error.is_none() {
                request.error = Some(if request.write {
                    LiveIoError::UnknownWrite
                } else {
                    LiveIoError::Io(io::Error::new(error.kind(), error.to_string()))
                });
            }
        }
    }
    pub fn progress(&mut self) -> Result<Vec<LiveCompletion>, LiveIoError> {
        let mut out = Vec::new();
        self.progress_with(|completion| out.push(completion))?;
        Ok(out)
    }

    /// Appends completions to the caller's reusable buffer.
    pub(crate) fn progress_into(
        &mut self,
        out: &mut Vec<LiveCompletion>,
    ) -> Result<usize, LiveIoError> {
        let before = out.len();
        self.progress_with(|completion| out.push(completion))?;
        Ok(out.len() - before)
    }

    /// Scalar retirement without allocating a completion Vec.
    #[cfg(test)]
    pub(crate) fn progress_one(&mut self) -> Result<Option<LiveCompletion>, LiveIoError> {
        let mut out = None;
        let mut multiple = false;
        self.progress_with(|completion| {
            multiple |= out.is_some();
            out = Some(completion);
        })?;
        if multiple {
            return Err(LiveIoError::Invalid("multiple scalar completions"));
        }
        Ok(out)
    }

    fn progress_with(
        &mut self,
        mut on_completion: impl FnMut(LiveCompletion),
    ) -> Result<(), LiveIoError> {
        #[cfg(test)]
        {
            let mut index = 0;
            while index < self.held_completions.len() {
                if self.held_ids.contains(&self.held_completions[index].id) {
                    index += 1;
                } else {
                    on_completion(self.held_completions.remove(index));
                }
            }
        }
        let _ = self.flush_submissions();
        for _ in 0..self.operations.len() {
            let Some(cqe) = self.ring.completion().next() else {
                break;
            };
            let Some(op) = self.operations.remove(&cqe.user_data()) else {
                std::process::abort()
            };
            let Operation {
                request,
                members,
                buffer,
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
            #[cfg(test)]
            if write {
                self.last_write_errno = (result < 0).then_some(-result);
            }
            #[cfg(test)]
            let result = if let Some(index) = self
                .completion_results
                .iter()
                .position(|entry| entry.0 == request)
            {
                self.completion_results.swap_remove(index).1
            } else {
                self.next_completion_result.take().unwrap_or(result)
            };
            self.stats.completions += 1;
            if let Some(started) = submitted {
                self.stats.submit_to_cq_ns += started.elapsed().as_nanos();
            }
            let complete = usize::try_from(result).is_ok_and(|n| n == buffer.len);
            if !complete {
                self.stats.errors += 1;
                if write {
                    if result < 0 {
                        eprintln!(
                            "live namespace write CQE failed: errno={}, error={}",
                            -result,
                            io::Error::from_raw_os_error(-result)
                        );
                        self.stats.failed_write_ops += 1;
                    } else if usize::try_from(result).is_ok_and(|n| n < buffer.len) {
                        self.stats.short_write_ops += 1;
                        self.stats.short_write_bytes += result as u64;
                    }
                }
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
                let whole = Bytes::from_owner(MemberBuffer {
                    buffer: Some(buffer),
                    lane: self.lane.clone(), // Shared return lane outlives retained slices.
                });
                for member in members {
                    self.stats.copy_bytes_avoided += member.len as u64;
                    req.records[member.index] =
                        Some(whole.slice(member.start..member.start + member.len));
                }
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
                let completion = LiveCompletion {
                    id: request,
                    completed_at: Instant::now(),
                    result,
                };
                #[cfg(test)]
                if self.held_ids.contains(&request) {
                    self.held_completions.push(completion);
                    continue;
                }
                on_completion(completion);
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
        Ok(())
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
        fixture_limits(8192, 4, 4 * (4096 + 4095))
    }

    #[test]
    fn polled_ring_reads_or_reports_unsupported() {
        fn unsupported(error: LiveIoError) {
            let LiveIoError::Io(error) = error else {
                panic!("polled read failed without an OS unsupported error: {error}");
            };
            assert!(
                matches!(
                    error.raw_os_error(),
                    Some(libc::EINVAL | libc::EOPNOTSUPP | libc::ENOSYS)
                ),
                "unexpected polled I/O error: {error}"
            );
            eprintln!("local filesystem or kernel rejects polled I/O: {error}");
        }

        let (_dir, mut interrupt) = fixture();
        let expected = write_pattern(&mut interrupt, 0, 17);
        let mut polled = match LiveIo::new(interrupt.namespace.clone(), 4, 65536, true) {
            Ok(io) => io,
            Err(error) => {
                unsupported(error);
                return;
            }
        };
        let timeout = Duration::from_millis(10);
        let before_wait = (polled.stats.submit_ns, polled.stats.submissions);
        let started = Instant::now();
        assert!(!polled.wait(timeout).unwrap());
        assert!(started.elapsed() >= timeout);
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "polled wait exceeded its bound"
        );
        assert_eq!(
            (polled.stats.submit_ns, polled.stats.submissions),
            before_wait
        );
        assert_eq!(
            polled
                .submit_reads(1, &[(20, 64)], Instant::now() + Duration::from_secs(10))
                .unwrap(),
            LiveSubmitOutcome::Submitted
        );
        assert!(polled.wait(Duration::from_secs(10)).unwrap());
        let completed = polled
            .progress_one()
            .unwrap()
            .expect("polled CQE must retire");
        match completed.result {
            Ok(LiveIoResult::Reads(records)) => {
                assert_eq!(records.len(), 1);
                assert_eq!(&records[0][..], &expected[20..84]);
            }
            Err(error) => unsupported(error),
            Ok(LiveIoResult::Written) => panic!("polled read returned a write completion"),
        }
        assert_eq!(polled.outstanding(), 0);
        assert!(polled.shutdown().unwrap().is_empty());
    }

    #[test]
    fn polled_ring_writes_or_reports_unsupported() {
        let (_dir, interrupt) = fixture();
        let mut polled = match LiveIo::new(interrupt.namespace.clone(), 4, 65536, true) {
            Ok(io) => io,
            Err(LiveIoError::Io(error)) => {
                assert_iopoll_unsupported(error.raw_os_error());
                return;
            }
            Err(error) => panic!("polled write ring setup failed: {error}"),
        };
        let expected = vec![0xa5; 4096];
        for (id, sync) in [(1, LiveWriteSync::None), (2, LiveWriteSync::Data)] {
            assert_eq!(
                polled
                    .submit_write(
                        id,
                        0,
                        &expected,
                        sync,
                        Instant::now() + Duration::from_secs(10)
                    )
                    .unwrap(),
                LiveSubmitOutcome::Submitted
            );
            assert!(polled.wait(Duration::from_secs(10)).unwrap());
            let completed = polled.progress_one().unwrap().unwrap();
            assert_eq!(completed.id, id);
            match completed.result {
                Ok(LiveIoResult::Written) => {}
                Err(LiveIoError::UnknownWrite) => {
                    assert_iopoll_unsupported(polled.last_write_errno());
                    assert_eq!(polled.outstanding(), 0);
                    assert!(polled.shutdown().unwrap().is_empty());
                    return;
                }
                result => panic!("unexpected polled write completion: {result:?}"),
            }
        }
        let mut actual = vec![0; 4096];
        std::os::unix::fs::FileExt::read_exact_at(
            &std::fs::File::open(_dir.path().join("wal")).unwrap(),
            &mut actual,
            0,
        )
        .unwrap();
        assert_eq!(actual, expected);
        assert_eq!(polled.stats().completed_write_ops, 2);
        assert!(polled.shutdown().unwrap().is_empty());
    }

    fn assert_iopoll_unsupported(errno: Option<i32>) {
        assert!(
            matches!(errno, Some(libc::EINVAL | libc::EOPNOTSUPP | libc::ENOSYS)),
            "unexpected polled I/O errno: {errno:?}"
        );
        eprintln!("local filesystem or kernel rejects polled I/O: errno={errno:?}");
    }

    #[test]
    fn owned_write_buffer_recycles_on_drop_success_and_failed_cqe() {
        let (_dir, mut io) = fixture_limits(8192, 2, 8191);
        let initial = io.credit();
        let pointer = {
            let mut buffer = io.write_buffer(4096).unwrap();
            assert_eq!(buffer.as_ptr() as usize % 4096, 0);
            buffer.fill(0xa5);
            buffer.as_ptr()
        };
        assert_eq!(io.credit(), initial);
        assert_eq!(io.allocated_bytes, 8191);
        assert_eq!(io.pool.len(), 1);
        let deadline = Instant::now() + Duration::from_secs(10);
        for (id, result) in [(1, None), (2, Some(-libc::EIO)), (3, Some(2048))] {
            if let Some(result) = result {
                io.override_next_completion_result(result);
            }
            let mut buffer = io.write_buffer(4096).unwrap();
            assert_eq!(buffer.as_ptr(), pointer);
            for (index, byte) in buffer.iter_mut().enumerate() {
                *byte = (index % 251) as u8;
            }
            assert_eq!(
                buffer
                    .submit_write_buffer(id, 0, LiveWriteSync::Data, deadline)
                    .unwrap(),
                LiveSubmitOutcome::Submitted
            );
            assert_eq!(io.credit(), (1, 0));
            assert!(matches!(io.write_buffer(4096), Err(LiveIoError::Full)));
            let result = completion(&mut io).result;
            if id == 1 {
                assert!(matches!(result, Ok(LiveIoResult::Written)));
            } else {
                assert!(matches!(result, Err(LiveIoError::UnknownWrite)));
            }
            assert_eq!(io.credit(), initial);
            assert_eq!(io.pool.len(), 1);
            assert_eq!(io.allocated_bytes, 8191);
        }
        io.submit_reads(4, &[(0, 4096)], deadline).unwrap();
        let LiveIoResult::Reads(records) = completion(&mut io).result.unwrap() else {
            panic!("expected read result");
        };
        assert!(
            records[0]
                .iter()
                .enumerate()
                .all(|(index, byte)| *byte == (index % 251) as u8)
        );
        assert_eq!(io.credit(), initial);
    }

    #[test]
    fn owned_write_buffer_rejects_unaligned_geometry_and_restores_credit() {
        let (_dir, mut io) = fixture();
        let initial = io.credit();
        assert!(matches!(
            io.write_buffer(4095),
            Err(LiveIoError::Invalid(_))
        ));
        assert!(matches!(
            io.write_buffer(4097),
            Err(LiveIoError::Invalid(_))
        ));
        let deadline = Instant::now() + Duration::from_secs(10);
        assert!(matches!(
            io.write_buffer(4096)
                .unwrap()
                .submit_write_buffer(1, 1, LiveWriteSync::None, deadline),
            Err(LiveIoError::Invalid(_))
        ));
        assert_eq!(io.credit(), initial);
        assert_eq!(io.pool.len(), 1);
        assert!(io.operations.is_empty());
        assert!(io.requests.is_empty());
    }
    fn fixture_limits(bytes: u64, slots: usize, budget: usize) -> (tempdir::TempDir, LiveIo) {
        let dir = tempdir::TempDir::new("live-namespace").unwrap();
        let path = dir.path().join("wal");
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)
            .unwrap();
        file.set_len(bytes).unwrap();
        let namespace =
            LiveNamespace::open_local_file(&path, bytes, 4096, LiveNamespaceAccess::ReadWrite)
                .unwrap();
        let io = LiveIo::new(namespace, slots, budget, false).unwrap();
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
    fn attached_files_read_the_selected_namespace_and_reject_wrong_block_size() {
        let (dir, mut io) = fixture();
        let mut slots = Vec::new();
        for (index, value) in [11u8, 22].into_iter().enumerate() {
            let path = dir.path().join(format!("attached-{index}"));
            std::fs::write(&path, [value; 8192]).unwrap();
            let namespace = Arc::new(
                LiveNamespace::open_local_file(&path, 8192, 4096, LiveNamespaceAccess::ReadShared)
                    .unwrap(),
            );
            slots.push(io.attach(namespace).unwrap());
        }
        let deadline = Instant::now() + Duration::from_secs(10);
        for (index, value) in [11u8, 22].into_iter().enumerate() {
            io.submit_reads_at(index as u64, slots[index], &[(17, 13)], deadline, None)
                .unwrap();
            let LiveIoResult::Reads(records) = completion(&mut io).result.unwrap() else {
                panic!("expected reads")
            };
            assert_eq!(&records[0][..], &[value; 13]);
        }
        let path = dir.path().join("attached-0");
        let different = Arc::new(
            LiveNamespace::open_local_file(&path, 8192, 512, LiveNamespaceAccess::ReadShared)
                .unwrap(),
        );
        assert!(matches!(
            io.attach(different),
            Err(LiveIoError::Invalid(
                "attached namespace block size differs"
            ))
        ));
        let namespace =
            LiveNamespace::open_local_file(&path, 8192, 4096, LiveNamespaceAccess::ReadShared)
                .unwrap();
        let mut readonly = LiveIo::new(namespace, 2, 16384, false).unwrap();
        assert!(matches!(
            readonly.submit_write(3, 0, &[0; 4096], LiveWriteSync::None, deadline),
            Err(LiveIoError::Invalid("namespace is read only"))
        ));
        assert!(matches!(
            readonly.write_buffer(4096),
            Err(LiveIoError::Invalid("namespace is read only"))
        ));
        assert_eq!(io.attached.len(), 2);
        io.shutdown().unwrap();
        assert!(matches!(
            io.attach(Arc::new(
                LiveNamespace::open_local_file(&path, 8192, 4096, LiveNamespaceAccess::ReadShared)
                    .unwrap()
            )),
            Err(LiveIoError::Invalid("queue closed"))
        ));
    }
    #[test]
    fn scalar_plans_and_member_metadata_stay_inline() {
        let (_dir, mut io) = fixture();
        let plan = io.plan_reads(io.namespace.bytes, &[(20, 8)]).unwrap();
        assert!(!plan.spilled());
        assert_eq!(plan.len(), 1);
        assert!(!plan[0].members.spilled());
        assert_eq!(io.read_request_charge(&[(20, 8)]).unwrap(), (1, 8191));
        let deadline = Instant::now() + Duration::from_secs(10);
        io.submit_write(1, 0, &[7; 4096], LiveWriteSync::Data, deadline)
            .unwrap();
        assert_eq!(io.requests[&1].records.capacity(), 0);
        assert!(!io.operations.values().next().unwrap().members.spilled());
        assert!(matches!(
            completion(&mut io).result,
            Ok(LiveIoResult::Written)
        ));
        io.submit_reads(2, &[(20, 8)], deadline).unwrap();
        assert!(!io.operations.values().next().unwrap().members.spilled());
        let LiveIoResult::Reads(records) = completion(&mut io).result.unwrap() else {
            panic!("expected reads")
        };
        assert_eq!(&records[0][..], &[7; 8]);
    }
    #[test]
    fn local_file_exact_write_and_ordered_ranged_reads() {
        let (_dir, mut io) = fixture();
        let data: Vec<u8> = (0..4096).map(|n| (n % 251) as u8).collect();
        let deadline = Instant::now() + Duration::from_secs(10);
        assert_eq!(
            io.submit_write(1, 0, &data, LiveWriteSync::Data, deadline)
                .unwrap(),
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
        assert_eq!(io.stats().completions, 2);
        assert_eq!(io.stats().data_sync_writes, 1);
        assert_eq!(io.stats().merged_members, 2);
        assert_eq!(io.stats().read_sqes_saved, 2);
        assert_eq!(io.stats().barrier_count, 0);
        assert!(io.shutdown().unwrap().is_empty());
    }
    fn write_pattern(io: &mut LiveIo, offset: u64, seed: u8) -> Vec<u8> {
        let data: Vec<u8> = (0..4096).map(|n| (n % 250) as u8 ^ seed).collect();
        let deadline = Instant::now() + Duration::from_secs(10);
        assert_eq!(
            io.submit_write(
                900 + u64::from(seed),
                offset,
                &data,
                LiveWriteSync::None,
                deadline
            )
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
        let mut io = LiveIo::new(namespace, 8, 2 * block, false).unwrap();
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
        let (_dir, mut io) = fixture_limits(16384, 4, 4 * (4096 + 4095));
        io.fail_buffer_after = Some(1);
        let deadline = Instant::now() + Duration::from_secs(10);
        assert!(io.submit_reads(1, &[(0, 1), (8192, 1)], deadline).is_err());
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
        assert!(
            io.submit_write(1, 1, &[0; 4096], LiveWriteSync::None, deadline)
                .is_err()
        );
        assert!(
            io.submit_write(1, 0, &[0; 17], LiveWriteSync::None, deadline)
                .is_err()
        );
        assert!(io.submit_reads(1, &[(8190, 4)], deadline).is_err());
        assert!(io.submit_reads(1, &[(0, 0)], deadline).is_err());
        assert_eq!(
            io.submit_write(2, 0, &[7; 4096], LiveWriteSync::None, deadline)
                .unwrap(),
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
    #[test]
    fn wait_times_out_idle_and_submits_without_harvesting() {
        let (_dir, mut io) = fixture();
        let timeout = Duration::from_millis(10);
        let started = Instant::now();
        assert!(!io.wait(timeout).unwrap());
        assert!(started.elapsed() >= timeout);
        let deadline = Instant::now() + Duration::from_secs(10);
        // Internal submit keeps SQEs queued, unlike the eager public read API.
        let reads = io
            .plan_reads(io.namespace.bytes, &[(20, 4), (40, 4)])
            .unwrap();
        assert_eq!(
            io.submit(
                io.namespace.file.as_raw_fd(),
                1,
                reads,
                None,
                deadline,
                None
            )
            .unwrap(),
            LiveSubmitOutcome::Submitted
        );
        assert_eq!(io.stats().submissions, 0);
        assert!(io.wait(Duration::from_secs(10)).unwrap());
        assert_eq!(io.stats().submissions, 1);
        assert_eq!(io.stats().completions, 0);
        assert_eq!(io.outstanding(), 1);
        assert!(io.queued.is_empty());
        assert!(io.operations.values().all(|op| op.submitted.is_some()));
        assert!(matches!(
            completion(&mut io).result,
            Ok(LiveIoResult::Reads(_))
        ));
        assert_eq!(io.stats().completions, 1);
        // Also exercise the eager public API without a preceding progress.
        io.submit_reads(2, &[(0, 1)], deadline).unwrap();
        assert!(io.wait(Duration::from_secs(10)).unwrap());
        assert_eq!(completion(&mut io).id, 2);
    }

    #[test]
    fn merged_reads_preserve_order_duplicates_touching_and_gaps() {
        let (_dir, mut io) = fixture_limits(16384, 4, 4 * (4096 + 4095));
        let first = write_pattern(&mut io, 0, 1);
        let second = write_pattern(&mut io, 4096, 2);
        let fourth = write_pattern(&mut io, 12288, 3);
        // Out of order, duplicate, overlapping, touching and separated by a gap.
        let ranges = [(4098, 6), (20, 8), (4090, 12), (20, 8), (12290, 4)];
        let (sqes, bytes) = io.read_request_charge(&ranges).unwrap();
        assert_eq!((sqes, bytes), (2, 8192 + 4095 + 4096 + 4095));
        let upper: usize = ranges
            .iter()
            .map(|&(o, n)| io.read_charge(o, n).unwrap())
            .sum();
        assert!(bytes < upper);
        let before = io.stats();
        io.submit_reads(1, &ranges, Instant::now() + Duration::from_secs(10))
            .unwrap();
        assert_eq!(io.outstanding(), 2);
        assert_eq!(io.used_bytes, bytes);
        assert_eq!(io.credit(), (2, io.max_bytes - bytes));
        let LiveIoResult::Reads(records) = completion(&mut io).result.unwrap() else {
            panic!("reads")
        };
        let crossing: Vec<u8> = first[4090..].iter().chain(&second[..6]).copied().collect();
        let expected: [&[u8]; 5] = [
            &second[2..8],
            &first[20..28],
            &crossing,
            &first[20..28],
            &fourth[2..6],
        ];
        for (record, expected) in records.iter().zip(expected) {
            assert_eq!(&record[..], expected);
        }
        let after = io.stats();
        assert_eq!(after.read_operations - before.read_operations, 2);
        assert_eq!(
            after.completed_read_bytes - before.completed_read_bytes,
            12288
        );
        assert_eq!(after.merged_members - before.merged_members, 3);
        assert_eq!(after.read_sqes_saved - before.read_sqes_saved, 3);
        assert_eq!(after.copy_bytes_avoided - before.copy_bytes_avoided, 38);
        assert_eq!(io.credit(), io.capacity());
        assert_eq!(io.allocated_bytes, 0);
        // The duplicate slices share their block, not separate return entries.
        let retained = records[1].clone();
        drop(records);
        io.reclaim();
        assert_eq!(io.pool.len(), 1);
        assert_eq!(io.allocated_bytes, 4096 + 4095);
        drop(retained);
        io.reclaim();
        assert_eq!(io.pool.len(), 2);
        assert_eq!(io.allocated_bytes, bytes);
    }

    #[test]
    fn thirty_three_same_block_members_split_at_thirty_two() {
        let (_dir, mut io) = fixture_limits(8192, 2, 2 * (4096 + 4095));
        let data = write_pattern(&mut io, 0, 4);
        let ranges: Vec<_> = (0..33).map(|index| (index * 7, 3)).collect();
        assert_eq!(
            io.read_request_charge(&ranges).unwrap(),
            (2, 2 * (4096 + 4095))
        );
        io.submit_reads(1, &ranges, Instant::now() + Duration::from_secs(10))
            .unwrap();
        assert_eq!(io.outstanding(), 2);
        let LiveIoResult::Reads(records) = completion(&mut io).result.unwrap() else {
            panic!("reads")
        };
        assert_eq!(records.len(), 33);
        for (index, record) in records.iter().enumerate() {
            assert_eq!(&record[..], &data[index * 7..index * 7 + 3]);
        }
        assert_eq!(io.stats().read_operations, 2);
        assert_eq!(io.stats().read_sqes_saved, 31);
        assert_eq!(io.used_bytes, 0);
    }

    #[test]
    fn merged_range_never_exceeds_byte_budget() {
        let (_dir, mut io) = fixture_limits(8192, 2, 4096 + 4095);
        let ranges = [(0, 1), (4096, 1)];
        let plan = io.plan_reads(io.namespace.bytes, &ranges).unwrap();
        assert_eq!(plan.len(), 2);
        assert!(plan.iter().all(|read| read.len + 4095 <= io.max_bytes));
        let deadline = Instant::now() + Duration::from_secs(10);
        assert_eq!(
            io.submit_reads(1, &ranges, deadline).unwrap(),
            LiveSubmitOutcome::Full
        );
        assert_eq!(
            io.submit_reads(2, &[(0, 8192)], deadline).unwrap(),
            LiveSubmitOutcome::Full
        );
        assert_eq!(io.outstanding(), 0);
        assert_eq!(io.used_bytes, 0);
        assert_eq!(io.allocated_bytes, 0);
        assert_eq!(
            io.submit_reads(3, &[(0, 1), (20, 1)], deadline).unwrap(),
            LiveSubmitOutcome::Submitted
        );
        assert_eq!(io.outstanding(), 1);
        assert_eq!(io.used_bytes, 8191);
        assert!(matches!(
            completion(&mut io).result,
            Ok(LiveIoResult::Reads(_))
        ));
        io.reclaim();
        assert_eq!(io.allocated_bytes, 8191);
    }

    #[test]
    fn failed_merged_read_fails_request_and_restores_credit() {
        let (dir, mut io) = fixture();
        OpenOptions::new()
            .write(true)
            .open(dir.path().join("wal"))
            .unwrap()
            .set_len(4096)
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        io.submit_reads(1, &[(0, 8), (4096, 8), (4096, 8)], deadline)
            .unwrap();
        assert_eq!(io.outstanding(), 1);
        assert_eq!(io.used_bytes, 8192 + 4095);
        let completed = completion(&mut io);
        assert!(
            matches!(&completed.result, Err(LiveIoError::Io(error)) if error.kind() == io::ErrorKind::UnexpectedEof)
        );
        assert_eq!(io.credit(), io.capacity());
        assert!(io.requests.is_empty());
        assert_eq!(io.pool.len(), 1);
        assert_eq!(io.allocated_bytes, 8192 + 4095);
        assert_eq!(io.stats().copy_bytes_avoided, 0);
        assert_eq!(io.stats().errors, 1);
        assert_eq!(io.stats().completions, 1);
        assert_eq!(io.lane.queued_bytes.load(Ordering::Relaxed), 0);
        assert_eq!(&read_member(&mut io, 2, 0, 8)[..], &[0; 8]);
        assert_eq!(io.credit(), io.capacity());
    }

    #[test]
    fn failed_merged_sqe_discards_successful_sibling_and_returns_both_blocks() {
        let (dir, mut io) = fixture_limits(16384, 4, 4 * 8191);
        OpenOptions::new()
            .write(true)
            .open(dir.path().join("wal"))
            .unwrap()
            .set_len(12288)
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        io.submit_reads(1, &[(0, 8), (8192, 8), (12288, 8)], deadline)
            .unwrap();
        assert_eq!(io.outstanding(), 2);
        assert!(matches!(
            completion(&mut io).result,
            Err(LiveIoError::Io(_))
        ));
        io.reclaim();
        assert_eq!(io.credit(), io.capacity());
        assert!(io.requests.is_empty());
        assert_eq!(io.pool.len(), 2);
        assert_eq!(io.allocated_bytes, 4096 + 4095 + 8192 + 4095);
        assert_eq!(io.lane.queued_bytes.load(Ordering::Relaxed), 0);
        assert_eq!(io.stats().completions, 2);
        assert_eq!(io.stats().errors, 1);
    }

    #[test]
    fn merged_requests_use_exact_credit_and_cancel_only_after_retirement() {
        let (_dir, mut io) = fixture_limits(8192, 2, 2 * 8191);
        let deadline = Instant::now() + Duration::from_secs(10);
        io.submit_reads(1, &[(0, 8), (20, 8)], deadline).unwrap();
        assert_eq!(io.credit(), (1, 8191));
        io.submit_reads(2, &[(4096, 8), (4116, 8)], deadline)
            .unwrap();
        assert_eq!(io.credit(), (0, 0));
        assert_eq!(
            io.submit_reads(3, &[(0, 1)], deadline).unwrap(),
            LiveSubmitOutcome::Full
        );
        io.cancel(1);
        io.cancel(2);
        assert_eq!(io.credit(), (0, 0));
        let mut done = Vec::new();
        for _ in 0..100 {
            io.wait(Duration::from_millis(100)).unwrap();
            done.extend(io.progress().unwrap());
            if done.len() == 2 {
                break;
            }
        }
        assert_eq!(done.len(), 2);
        assert!(
            done.iter()
                .all(|c| matches!(c.result, Err(LiveIoError::Cancelled)))
        );
        assert_eq!(io.credit(), io.capacity());
        assert_eq!(io.pool.len(), 2);
        assert_eq!(io.allocated_bytes, 2 * 8191);
        assert_eq!(io.stats().copy_bytes_avoided, 0);
    }

    #[test]
    fn failed_and_expired_data_sync_writes_remain_unknown() {
        let (_dir, mut io) = fixture();
        let deadline = Instant::now() + Duration::from_secs(10);
        io.submit_write(1, 0, &[9; 4096], LiveWriteSync::Data, deadline)
            .unwrap();
        // Force expiry after admission but before harvesting a terminal CQE.
        io.requests.get_mut(&1).unwrap().deadline = Instant::now();
        assert!(matches!(
            completion(&mut io).result,
            Err(LiveIoError::UnknownWrite)
        ));
        assert_eq!(io.stats().timeouts, 1);
        assert_eq!(io.stats().data_sync_writes, 1);
        assert_eq!(io.credit(), io.capacity());
        let (dir, mut failed) = fixture();
        // A read-only file descriptor forces a real failing write CQE.
        Arc::get_mut(&mut failed.namespace).unwrap().file =
            std::fs::File::open(dir.path().join("wal")).unwrap();
        failed
            .submit_write(2, 0, &[9; 4096], LiveWriteSync::Data, deadline)
            .unwrap();
        assert!(matches!(
            completion(&mut failed).result,
            Err(LiveIoError::UnknownWrite)
        ));
        assert_eq!(failed.stats().errors, 1);
        assert_eq!(failed.credit(), failed.capacity());
        assert_eq!(failed.allocated_bytes, 8191);
    }

    #[test]
    fn terminal_write_result_override_is_one_shot_and_keeps_unknown_outcomes() {
        let (_dir, mut io) = fixture();
        let deadline = Instant::now() + Duration::from_secs(10);
        for (id, result) in [(1, 2048), (2, -libc::EIO)] {
            io.override_next_completion_result(result);
            io.submit_write(id, 0, &[7; 4096], LiveWriteSync::Data, deadline)
                .unwrap();
            assert_eq!(io.outstanding(), 1);
            assert!(matches!(
                completion(&mut io).result,
                Err(LiveIoError::UnknownWrite)
            ));
            assert!(io.next_completion_result.is_none());
            assert_eq!(io.credit(), io.capacity());
            assert_eq!(io.allocated_bytes, 8191);
        }
        io.submit_write(3, 0, &[8; 4096], LiveWriteSync::Data, deadline)
            .unwrap();
        assert!(matches!(
            completion(&mut io).result,
            Ok(LiveIoResult::Written)
        ));
        assert_eq!(io.stats().errors, 2);
        assert_eq!(io.stats().failed_write_ops, 1);
        assert_eq!(io.stats().short_write_ops, 1);
        assert_eq!(io.stats().short_write_bytes, 2048);
        assert_eq!(io.stats().completed_write_ops, 1);
        assert_eq!(io.stats().completions, 3);
    }
}
