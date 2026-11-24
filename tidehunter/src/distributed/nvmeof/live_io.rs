//! Bounded live namespace data plane. Data-sync writes request RWF_DSYNC; plain
//! write CQEs do not prove persistence. Receipts and publication belong to the
//! caller. Cancellation and deadlines report only after all operations retire.
use super::live_namespace::{LiveNamespace, LiveNamespaceAccess};
use crate::distributed::sys::{Ring, Sqe};
use minibytes::Bytes;
use smallvec::SmallVec;
use std::collections::{HashMap, VecDeque};
use std::io;
use std::os::fd::{AsRawFd, RawFd};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

mod buffers;

#[cfg(test)]
mod tests;

pub use buffers::LiveWriteBuffer;

use buffers::*;

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
    Flushed,
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

/// Times are cumulative nanoseconds. Namespace flushes use barrier counters.
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
    /// io_uring enter calls that consumed at least one SQE.
    #[serde(default)]
    pub submit_calls: u64,
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
    flush: bool,
    deadline: Instant,
    cancelled_at: Option<Instant>,
    error: Option<LiveIoError>,
    charge: usize,
    trace: Option<crate::distributed::remote_db::ReadTraceChild>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct NamespaceSlot(u32);

pub struct LiveIo {
    ring: Ring,
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
    next_submit_result: Option<i32>,
    /// `(offset, len)` of every accepted write, in submission order. Test only:
    /// a resume test proves where a rewrite landed.
    #[cfg(test)]
    submitted_writes: Vec<(u64, usize)>,
    /// Errno of the latest completed namespace write when that write failed.
    /// A resume classifies a break from it, so it is recorded in production.
    last_write_errno: Option<i32>,
    #[cfg(test)]
    held_ids: Vec<u64>,
    #[cfg(test)]
    completion_results: Vec<(u64, i32)>,
    #[cfg(test)]
    held_completions: Vec<LiveCompletion>,
    /// Expire the next accepted read at submission time, after its SQEs are
    /// queued, so its success CQE is observed past the deadline.
    #[cfg(test)]
    expire_read_after_submit: bool,
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
        let ring = Ring::new(max_inflight_slots.next_power_of_two() as u32, polled)?;
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
            next_submit_result: None,
            #[cfg(test)]
            submitted_writes: Vec::new(),
            last_write_errno: None,
            #[cfg(test)]
            held_ids: Vec::new(),
            #[cfg(test)]
            completion_results: Vec::new(),
            #[cfg(test)]
            held_completions: Vec::new(),
            #[cfg(test)]
            expire_read_after_submit: false,
        })
    }
    pub(crate) fn register_worker_wake(&self, fd: std::os::fd::RawFd) -> io::Result<()> {
        self.ring.register_eventfd(fd)
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

    /// The next accepted read is submitted normally, then its deadline is set
    /// to the insertion time. The kernel CQE arrives for a request already
    /// past its deadline: a terminal success, observed late. No sleep and no
    /// device delay are involved.
    #[cfg(test)]
    pub(crate) fn expire_read_after_submit(&mut self) {
        self.expire_read_after_submit = true;
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
    /// Errno of the last completed namespace write, when it failed. Read by the
    /// write resume to decide whether a lost write is a transport break.
    pub(crate) fn last_write_errno(&self) -> Option<i32> {
        self.last_write_errno
    }
    /// Test only: fail the next submission with `errno`, before any SQE.
    #[cfg(test)]
    pub(crate) fn override_submit_result(&mut self, errno: i32) {
        self.next_submit_result = Some(errno);
    }
    /// Test only: `(offset, len)` of every accepted write, in order.
    #[cfg(test)]
    pub(crate) fn submitted_writes(&self) -> &[(u64, usize)] {
        &self.submitted_writes
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
        self.ring.completion_ready()
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
                total.checked_add(if read.len == 0 {
                    0
                } else {
                    read.len.checked_add(slack)?
                })
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
        #[cfg(test)]
        if let Some(errno) = self.next_submit_result.take() {
            self.stats.errors += 1;
            return Err(LiveIoError::Io(io::Error::from_raw_os_error(errno)));
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
    /// Flush all writes completed before admission on this namespace. A flush
    /// owns one slot and no DMA buffer until its terminal CQE; an uncertain
    /// completion has the same unknown-outcome class as a data-sync write.
    pub(crate) fn submit_flush(&mut self, id: u64, deadline: Instant) -> Result<u64, LiveIoError> {
        if self.namespace.access != LiveNamespaceAccess::ReadWrite {
            return Err(LiveIoError::Invalid("namespace is read only"));
        }
        // Only flush admission passes a zero-length operation to `submit`.
        match self.submit(
            self.namespace.file.as_raw_fd(),
            id,
            smallvec::smallvec![PlannedRead {
                offset: 0,
                len: 0,
                members: SmallVec::new(),
            }],
            Some((WriteData::Borrowed(&[]), LiveWriteSync::None)),
            deadline,
            None,
        )? {
            LiveSubmitOutcome::Submitted => {
                // Submit before the worker waits on its CQE eventfd.
                let _ = self.flush_submissions();
                Ok(id)
            }
            LiveSubmitOutcome::Full => Err(LiveIoError::Full),
        }
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
            // Nothing was submitted and no buffer left the pool: the caller
            // may retry this read. A reused buffer could still receive DMA, so
            // only a request that never reached the ring is retry-safe.
            self.stats.timeouts += 1;
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
        let flush = data.is_some() && read_bytes == 0;
        // Scalar reads and writes keep all transient SQE metadata inline.
        let mut entries = SmallVec::<[(Sqe, u64); 1]>::with_capacity(ranges.len());
        let mut prepared = SmallVec::<[(u64, Operation); 1]>::with_capacity(ranges.len());
        for (index, read) in ranges.into_iter().enumerate() {
            let PlannedRead {
                offset,
                len,
                members,
            } = read;
            let buffer = if flush {
                Ok(Buffer {
                    data: Vec::new(),
                    start: 0,
                    len: 0,
                })
            } else {
                match data.as_mut() {
                    Some((WriteData::Owned(buffer), _)) => buffer
                        .take()
                        .ok_or(LiveIoError::Invalid("write buffer missing")),
                    _ => self.buffer(len),
                }
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
            let entry = if flush {
                Sqe::DataSync { fd }
            } else if data.is_some() {
                Sqe::Write {
                    fd,
                    buf: buffer.ptr(),
                    len: len as u32,
                    offset,
                    data_sync: matches!(data, Some((_, LiveWriteSync::Data))),
                }
            } else {
                Sqe::Read {
                    fd,
                    buf: buffer.ptr(),
                    len: len as u32,
                    offset,
                }
            };
            entries.push((entry, token));
            #[cfg(test)]
            if data.is_some() && !flush {
                self.submitted_writes.push((offset, len));
            }
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
        if !unsafe { self.ring.push(&entries) } {
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
        // Test-only: an accepted read can be made already overdue at insertion
        // so its successful CQE is observed past the deadline, with no sleep.
        #[cfg(test)]
        let deadline = if data.is_none() && std::mem::take(&mut self.expire_read_after_submit) {
            Instant::now()
        } else {
            deadline
        };
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
                flush,
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
        if flush {
            self.stats.barrier_count += 1;
        } else if data.is_some() {
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
        let pending = self.ring.queued();
        let started = Instant::now();
        let result = self.ring.submit_and_wait_timeout(timeout);
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
        Ok(self.ring.queued() == 0)
    }
    /// Hands queued SQEs to the kernel. A transient EBUSY/EINTR leaves any
    /// unconsumed SQEs queued for the next call.
    fn flush_submissions(&mut self) -> Result<(), LiveIoError> {
        let pending = self.ring.queued();
        if pending == 0 && !self.polled {
            return Ok(());
        }
        let started = Instant::now();
        let result = self.ring.submit(self.polled);
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
        let submitted = pending - self.ring.queued();
        // A polled enter with no queued SQE only reaps completions: it is not
        // submission time. The interrupt ring keeps its accounting unchanged.
        if pending > 0 || !self.polled {
            self.stats.submit_ns += started.elapsed().as_nanos();
        }
        self.stats.submissions += submitted as u64;
        self.stats.submit_calls += u64::from(submitted != 0);
        for _ in 0..submitted {
            let Some(token) = self.queued.pop_front() else {
                std::process::abort()
            };
            let Some(op) = self.operations.get_mut(&token) else {
                std::process::abort()
            };
            self.stats.queue_ns += started.duration_since(op.queued).as_nanos();
            op.submitted = Some(started);
            #[cfg(test)]
            crate::failpoints::write_model::submitted(
                op.write,
                self.requests
                    .get(&op.request)
                    .is_some_and(|request| request.flush),
            );
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
            let Some((user_data, result)) = self.ring.next_completion() else {
                break;
            };
            let Some(op) = self.operations.remove(&user_data) else {
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
                trace.event_io("live_io_cqe_observed", queue, user_data);
            }
            if write {
                // A successful write clears the record, so the value always
                // names the latest write's outcome and never an older failure.
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
                if req.flush {
                    self.stats.barrier_ns += started.elapsed().as_nanos();
                }
            }
            let complete = usize::try_from(result).is_ok_and(|n| n == buffer.len);
            if !complete {
                self.stats.errors += 1;
                if write && !req.flush {
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
            } else if req.flush {
                // Fsync returns zero and transfers no bytes.
            } else if write {
                self.stats.completed_write_bytes += buffer.len as u64;
                self.stats.completed_write_ops += 1;
            } else {
                self.stats.completed_read_bytes += buffer.len as u64;
                self.stats.completed_read_ops += 1;
            }
            // A terminal success is data in hand. The deadline bounds how long
            // the caller waits for an outstanding I/O; it does not discard a
            // CQE that arrived. Lending here releases the block only when every
            // slice of it drops, so a late observer never sees it refilled.
            if complete && !write && req.cancelled_at.is_none() {
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
            } else if !req.flush {
                self.pool.push(buffer);
            }
            req.remaining -= 1;
            if req.remaining == 0 {
                let Some(mut req) = self.requests.remove(&request) else {
                    std::process::abort()
                };
                self.used_bytes -= req.charge;
                // A read whose every CQE reports success is a success even when
                // the queue observes it after the deadline: the deadline bounds
                // the wait, not the data. Writes and flushes keep the late rule
                // because an acknowledgement time is part of their contract.
                let read_complete =
                    !req.write && !req.flush && req.error.is_none() && req.cancelled_at.is_none();
                let timed_out = Instant::now() >= req.deadline && !read_complete;
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
                } else if req.flush {
                    Ok(LiveIoResult::Flushed)
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
