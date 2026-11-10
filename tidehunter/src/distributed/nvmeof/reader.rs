//! Nonblocking io_uring queue. The namespace and every aligned allocation stay
//! owned until their terminal CQE; cancellation never releases DMA ownership.
//!
//! The reader is split in two. [`NvmeofCatalog`] is immutable and shared: any
//! thread can check eligibility and plan reads without a lock. [`NvmeofReader`]
//! owns the ring, the operation table and the buffer pool; exactly one thread
//! at a time (the owner actor of the export's partition) drives it.
use super::metrics::{NvmeofStats, ReaderStats, SharedStats};
use super::namespace::open_namespace;
use super::{ExportManifest, NamespaceBinding};
use crate::distributed::types::{PhysicalValueAddress, ValueRecordReference};
use io_uring::{IoUring, opcode, types};
use minibytes::Bytes;
use smallvec::SmallVec;
use std::collections::{HashMap, VecDeque};
use std::io;
use std::os::fd::AsRawFd;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

#[derive(Debug)]
pub enum NvmeofError {
    Invalid(&'static str),
    Io(io::Error),
    Read {
        source: io::Error,
        result: i32,
        offset: u64,
        requested: usize,
        token: u64,
        partition: u32,
        namespace: std::path::PathBuf,
        export_uuid: [u8; 16],
    },
    Timeout,
    Cancelled,
}
impl std::fmt::Display for NvmeofError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for NvmeofError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) | Self::Read { source: error, .. } => Some(error),
            _ => None,
        }
    }
}
impl From<io::Error> for NvmeofError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NvmeofReaderConfig {
    pub binding: NamespaceBinding,
    pub manifest: ExportManifest,
    pub max_inflight_requests: usize,
    pub max_inflight_members: usize,
    pub max_inflight_bytes: usize,
    pub max_catalog_bytes: usize,
}
impl NvmeofReaderConfig {
    /// Upper bound of idle aligned buffers retained by the reader's pool: a
    /// quarter of the in-flight byte envelope, at most `POOL_MAX_BYTES`.
    pub fn pool_bytes(&self) -> usize {
        (self.max_inflight_bytes / 4).min(POOL_MAX_BYTES)
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Eligibility {
    Eligible,
    Ineligible,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SubmitOutcome {
    Submitted,
    Full,
}
pub struct Completion {
    pub id: u64,
    pub result: Result<Vec<Bytes>, NvmeofError>,
}

/// Most members one merged SQE may serve.
const MAX_MERGE_MEMBERS: usize = 32;
/// Pooled buffer classes: class `n` holds reads of `n + 1` alignment units,
/// so up to 128 KiB at 4 KiB alignment. Longer reads are not pooled.
const POOL_CLASSES: usize = 32;
/// Idle buffers kept per class at most; the byte budget may allow fewer or none.
const POOL_CLASS_MAX_BUFFERS: usize = 256;

/// Bytes of aligned buffer a read of `read_len` occupies: reads of up to
/// `POOL_CLASSES` alignment units round up to whole units (their pooled size),
/// longer reads are allocated exactly.
fn pooled_len(read_len: usize, alignment: usize) -> usize {
    let units = read_len.div_ceil(alignment);
    if (1..=POOL_CLASSES).contains(&units) {
        units * alignment
    } else {
        read_len
    }
}
const POOL_MAX_BYTES: usize = 8 << 20;

/// Aligned device range of one member, computed once per member.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MemberRange {
    /// Identity of the catalog that authorised this member.
    reader: u64,
    /// Block-aligned device offset of the read.
    offset: u64,
    /// Block-aligned read length.
    read_len: usize,
    /// Exact member offset inside the aligned read.
    member_start: usize,
    member_len: usize,
    /// Pending-read bytes charged for this member (aligned buffer, alignment
    /// slack, member and fixed metadata).
    charge: usize,
}
impl MemberRange {
    pub fn charge(&self) -> usize {
        self.charge
    }
    pub fn aligned_offset(&self) -> u64 {
        self.offset
    }
    pub fn aligned_len(&self) -> usize {
        self.read_len
    }
}

/// Where one member lives inside a merged read.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct MemberSlice {
    index: usize,
    start: usize,
    len: usize,
}
#[derive(Clone, Debug, Eq, PartialEq)]
struct PlannedRead {
    offset: u64,
    len: usize,
    slices: std::ops::Range<usize>,
}

/// Immutable SQE layout of one request. Built once, so a `Full` retry or a
/// retried admission does not repeat alignment or merging.
#[derive(Debug)]
pub struct ReadPlan {
    reader: u64,
    members: usize,
    reads: Vec<PlannedRead>,
    slices: Vec<MemberSlice>,
    charge: usize,
}
impl ReadPlan {
    /// Identity of the reader this plan was built for.
    pub fn reader_id(&self) -> u64 {
        self.reader
    }
    pub fn members(&self) -> usize {
        self.members
    }
    /// Number of SQEs after merging.
    pub fn reads(&self) -> usize {
        self.reads.len()
    }
    pub fn charge(&self) -> usize {
        self.charge
    }
    /// Aligned device `(offset, length)` of every SQE, for tests and tracing.
    pub fn read_ranges(&self) -> impl Iterator<Item = (u64, usize)> + '_ {
        self.reads.iter().map(|read| (read.offset, read.len))
    }
}

static NEXT_READER_ID: AtomicU64 = AtomicU64::new(1);

/// Immutable authority view of one sealed export. Shared by every thread that
/// plans reads; nothing here is mutated after `open`, except atomic counters.
pub struct NvmeofCatalog {
    id: u64,
    config: NvmeofReaderConfig,
    catalog: HashMap<ValueRecordReference, (u64, usize)>,
    identities: HashMap<(PhysicalValueAddress, u32), ValueRecordReference>,
    stats: SharedStats,
}
impl NvmeofCatalog {
    /// Process-unique identity; a `ReadPlan` is valid only for its own reader.
    pub fn id(&self) -> u64 {
        self.id
    }
    pub fn manifest(&self) -> &ExportManifest {
        &self.config.manifest
    }
    pub fn max_request_members(&self) -> usize {
        self.config.max_inflight_members
    }
    pub fn max_request_bytes(&self) -> usize {
        self.config.max_inflight_bytes
    }
    pub(crate) fn stats(&self) -> &SharedStats {
        &self.stats
    }
    pub fn snapshot(&self) -> NvmeofStats {
        self.stats.snapshot()
    }
    /// Record consumer-side checksum and decoding time; this is not device time.
    pub fn record_verification(&self, elapsed: Duration) {
        self.stats.verification.add(elapsed);
    }
    /// Count a routing fallback once per logical member, not per eligibility probe.
    pub fn record_ineligible_fallbacks(&self, members: usize) {
        self.stats
            .ineligible_fallbacks
            .fetch_add(members as u64, Ordering::Relaxed);
    }
    pub fn eligibility(
        &self,
        reference: &ValueRecordReference,
    ) -> Result<Eligibility, NvmeofError> {
        let m = self.manifest();
        // Every admitted entry and identity has this partition and generation,
        // so another pair can neither match nor conflict.
        if reference.batch_address.partition != m.partition
            || reference.batch_address.segment_generation != m.generation
        {
            return Ok(Eligibility::Ineligible);
        }
        if self.catalog.contains_key(reference) {
            return Ok(Eligibility::Eligible);
        }
        if self.identities.contains_key(&(
            reference.batch_address,
            reference.descriptor.operation_index,
        )) {
            return Err(NvmeofError::Invalid(
                "reference conflicts with owner authority",
            ));
        }
        Ok(Eligibility::Ineligible)
    }
    fn alignment(&self) -> usize {
        (self.manifest().block_size as usize).max(4096)
    }
    /// Aligned range and byte charge of one admitted member. Counts one
    /// alignment observation; callers keep the result instead of repeating it.
    pub fn plan_member(
        &self,
        reference: &ValueRecordReference,
    ) -> Result<MemberRange, NvmeofError> {
        let started = Instant::now();
        let &(offset, len) = self
            .catalog
            .get(reference)
            .ok_or(NvmeofError::Invalid("reference not admitted"))?;
        let block = u64::from(self.manifest().block_size);
        let start = offset / block * block;
        let end = offset
            .checked_add(len as u64)
            .and_then(|n| n.checked_add(block - 1))
            .map(|n| n / block * block)
            .ok_or(NvmeofError::Invalid("aligned range overflow"))?;
        if end > self.manifest().image_len || end - start > u64::from(u32::MAX) {
            return Err(NvmeofError::Invalid("aligned range exceeds namespace"));
        }
        let read_len = (end - start) as usize;
        // Charge the buffer actually allocated: pooled reads round up to a
        // whole number of alignment units.
        let buffer = pooled_len(read_len, self.alignment());
        let charge = buffer
            .checked_add(self.alignment() - 1)
            .and_then(|n| n.checked_add(len))
            .and_then(|n| n.checked_add(2048))
            .ok_or(NvmeofError::Invalid("read charge overflow"))?;
        self.stats.alignment.add(started.elapsed());
        Ok(MemberRange {
            reader: self.id,
            offset: start,
            read_len,
            member_start: (offset - start) as usize,
            member_len: len,
            charge,
        })
    }
    /// Plan every reference of a request. A forged reference keeps its
    /// authority-conflict error instead of the generic "not admitted" one.
    pub fn plan_references(
        &self,
        references: &[ValueRecordReference],
    ) -> Result<ReadPlan, NvmeofError> {
        if references.is_empty() {
            return Err(NvmeofError::Invalid("empty or duplicate request"));
        }
        // Reject an oversized request before computing any alignment.
        if references.len() > self.config.max_inflight_members {
            return Err(NvmeofError::Invalid("request exceeds reader capacity"));
        }
        let mut members = SmallVec::<[MemberRange; 32]>::with_capacity(references.len());
        for reference in references {
            match self.plan_member(reference) {
                Ok(member) => members.push(member),
                Err(error) => {
                    self.eligibility(reference)?;
                    return Err(error);
                }
            }
        }
        self.plan_request(&members)
    }
    /// Merge members that hit the same, overlapping or adjacent aligned bytes
    /// into one read. Never reads a gap: every merged byte belongs to at least
    /// one member's aligned range. A merged read serves at most
    /// `MAX_MERGE_MEMBERS` members and at most `max_inflight_bytes` bytes, so
    /// its buffer never exceeds the sum of the member charges.
    pub fn plan_request(&self, members: &[MemberRange]) -> Result<ReadPlan, NvmeofError> {
        if members.is_empty() || members.len() > self.config.max_inflight_members {
            return Err(NvmeofError::Invalid("request exceeds reader capacity"));
        }
        // A range carries the identity of the catalog that authorised it; a
        // range planned by another reader was never checked against this one.
        if members.iter().any(|member| member.reader != self.id) {
            return Err(NvmeofError::Invalid(
                "member range belongs to another reader",
            ));
        }
        let mut charge = 0usize;
        for member in members {
            charge = charge
                .checked_add(member.charge)
                .ok_or(NvmeofError::Invalid("request charge overflow"))?;
        }
        if charge > self.config.max_inflight_bytes {
            return Err(NvmeofError::Invalid("request exceeds reader capacity"));
        }
        let max_read = self.config.max_inflight_bytes.min(u32::MAX as usize) as u64;
        let (reads, slices) = merge_reads(members, max_read)?;
        Ok(ReadPlan {
            reader: self.id,
            members: members.len(),
            reads,
            slices,
            charge,
        })
    }
}

/// Sort members by device offset and fold neighbours that overlap or touch
/// into one read, at most `MAX_MERGE_MEMBERS` members and `max_read` bytes per
/// read. The sort key includes the member index, so the layout is deterministic.
fn merge_reads(
    members: &[MemberRange],
    max_read: u64,
) -> Result<(Vec<PlannedRead>, Vec<MemberSlice>), NvmeofError> {
    let mut order = SmallVec::<[u32; 32]>::new();
    order.extend(0..members.len() as u32);
    order.sort_unstable_by_key(|&index| (members[index as usize].offset, index));
    let mut reads: Vec<PlannedRead> = Vec::with_capacity(members.len());
    let mut slices: Vec<MemberSlice> = Vec::with_capacity(members.len());
    for index in order {
        let index = index as usize;
        let member = &members[index];
        let end = member
            .offset
            .checked_add(member.read_len as u64)
            .ok_or(NvmeofError::Invalid("aligned range overflow"))?;
        let extends = reads.last().is_some_and(|read| {
            let read_end = read.offset + read.len as u64;
            member.offset <= read_end
                && read.slices.len() < MAX_MERGE_MEMBERS
                && end.max(read_end) - read.offset <= max_read
        });
        if extends {
            let read = reads.last_mut().expect("extends implies a previous read");
            read.len = (end.max(read.offset + read.len as u64) - read.offset) as usize;
            read.slices.end += 1;
        } else {
            reads.push(PlannedRead {
                offset: member.offset,
                len: member.read_len,
                slices: slices.len()..slices.len() + 1,
            });
        }
        let read_offset = reads.last().expect("a read exists").offset;
        slices.push(MemberSlice {
            index,
            start: (member.offset - read_offset) as usize + member.member_start,
            len: member.member_len,
        });
    }
    Ok((reads, slices))
}

/// Aligned buffer that returns to its pool when the last owner drops. After
/// the terminal CQE it backs a `Bytes` (shared by every member slice of one
/// read), so it is recycled only when all slices are gone.
struct PooledStorage {
    storage: Vec<u8>,
    aligned_start: usize,
    /// Bytes the kernel reads into the aligned window.
    len: usize,
    class: Option<usize>,
    pool: Arc<PoolShared>,
}
impl PooledStorage {
    fn as_mut_ptr(&mut self) -> *mut u8 {
        self.storage.as_mut_ptr().wrapping_add(self.aligned_start)
    }
}
impl AsRef<[u8]> for PooledStorage {
    fn as_ref(&self) -> &[u8] {
        &self.storage[self.aligned_start..self.aligned_start + self.len]
    }
}
impl minibytes::BytesOwner for PooledStorage {}
impl Drop for PooledStorage {
    fn drop(&mut self) {
        let storage = std::mem::take(&mut self.storage);
        let stats = &self.pool.stats;
        match self
            .class
            .map(|class| self.pool.returns[class].try_send(storage))
        {
            Some(Ok(())) => {
                stats.pool_recycled.fetch_add(1, Ordering::Relaxed);
            }
            // Class full or oversize: free the buffer, which bounds idle memory.
            Some(Err(mpsc::TrySendError::Full(_))) | None => {
                stats.pool_discarded.fetch_add(1, Ordering::Relaxed);
            }
            // The reader is gone, so nobody can reuse the buffer.
            Some(Err(mpsc::TrySendError::Disconnected(_))) => {}
        }
    }
}
/// Return side of the pool. Dropping a buffer from any thread is a bounded,
/// non-blocking `try_send`; only the reader's own thread receives.
struct PoolShared {
    returns: Vec<mpsc::SyncSender<Vec<u8>>>,
    alignment: usize,
    stats: SharedStats,
}
struct BufferPool {
    shared: Arc<PoolShared>,
    idle: Vec<mpsc::Receiver<Vec<u8>>>,
}
impl BufferPool {
    /// Idle buffers a class may hold: its share of the budget divided by the
    /// whole allocation (aligned window plus alignment slack), so the sum over
    /// classes never exceeds `pool_bytes`. A small budget gives zero capacity.
    fn class_capacity(pool_bytes: usize, alignment: usize, class: usize) -> usize {
        let allocation = (class + 1) * alignment + alignment - 1;
        (pool_bytes / POOL_CLASSES / allocation).min(POOL_CLASS_MAX_BUFFERS)
    }
    fn new(alignment: usize, pool_bytes: usize, stats: SharedStats) -> Self {
        let mut returns = Vec::with_capacity(POOL_CLASSES);
        let mut idle = Vec::with_capacity(POOL_CLASSES);
        for class in 0..POOL_CLASSES {
            let capacity = Self::class_capacity(pool_bytes, alignment, class);
            let (sender, receiver) = mpsc::sync_channel(capacity);
            returns.push(sender);
            idle.push(receiver);
        }
        Self {
            shared: Arc::new(PoolShared {
                returns,
                alignment,
                stats,
            }),
            idle,
        }
    }
    /// Pool class of a read: its length in alignment units, so a pooled buffer
    /// is exactly as large as an unpooled one and the byte charge holds.
    fn class_for(&self, len: usize) -> Option<usize> {
        let units = len.div_ceil(self.shared.alignment);
        (1..=POOL_CLASSES).contains(&units).then(|| units - 1)
    }
    /// A buffer whose aligned window holds `len` bytes. Fresh buffers are
    /// zero-filled only because safe Rust cannot hand out uninitialized
    /// `Vec<u8>`; a pooled buffer keeps stale bytes, which no one can observe:
    /// a `Bytes` is built only after a CQE reports exactly `len` bytes written
    /// and it exposes at most that window.
    fn take(&mut self, len: usize) -> PooledStorage {
        let alignment = self.shared.alignment;
        let class = self.class_for(len);
        let pooled = class.and_then(|class| self.idle[class].try_recv().ok());
        let storage = if let Some(storage) = pooled {
            self.shared.stats.pool_hits.fetch_add(1, Ordering::Relaxed);
            storage
        } else {
            self.shared
                .stats
                .pool_misses
                .fetch_add(1, Ordering::Relaxed);
            let capacity = class.map_or(len, |class| (class + 1) * alignment);
            vec![0; capacity + alignment - 1]
        };
        let aligned_start = (alignment - (storage.as_ptr() as usize % alignment)) % alignment;
        PooledStorage {
            storage,
            aligned_start,
            len,
            class,
            pool: self.shared.clone(),
        }
    }
}

struct Operation {
    request: u64,
    offset: u64,
    storage: PooledStorage,
    members: SmallVec<[MemberSlice; 4]>,
    queued: Instant,
    submitted: Option<Instant>,
}
struct Request {
    remaining: usize,
    records: Vec<Option<Bytes>>,
    error: Option<NvmeofError>,
    deadline: Instant,
    cancelled: bool,
    cancelled_at: Option<Instant>,
    charge: usize,
}

const DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

// Testable independently of a kernel ring. A late CQ still retires ownership;
// only an outstanding request at or beyond this boundary requires fail-stop.
fn drain_expired(
    deadline: Instant,
    cancelled_at: Option<Instant>,
    shutdown_at: Option<Instant>,
    now: Instant,
) -> bool {
    now.checked_duration_since(deadline)
        .is_some_and(|elapsed| elapsed >= DRAIN_TIMEOUT)
        || cancelled_at
            .and_then(|at| now.checked_duration_since(at))
            .is_some_and(|elapsed| elapsed >= DRAIN_TIMEOUT)
        || shutdown_at
            .and_then(|at| now.checked_duration_since(at))
            .is_some_and(|elapsed| elapsed >= DRAIN_TIMEOUT)
}

pub struct NvmeofReader {
    ring: IoUring,
    namespace: std::fs::File,
    catalog: Arc<NvmeofCatalog>,
    pool: BufferPool,
    operations: HashMap<u64, Operation>,
    requests: HashMap<u64, Request>,
    queued_tokens: VecDeque<u64>,
    next_token: u64,
    used_bytes: usize,
    closed: bool,
    shutdown_at: Option<Instant>,
    failed: bool,
    fatal_deadline: Option<Instant>,
    #[cfg(test)]
    submit_errors: VecDeque<i32>,
}
impl NvmeofReader {
    /// Must only be called after the authenticated owner control plane has
    /// validated every exact catalog entry and its physical provenance.
    pub(crate) fn open_verified(config: NvmeofReaderConfig) -> Result<Self, NvmeofError> {
        Self::open_inner(config, None)
    }
    /// Explicit file-backed test fixture; production always uses namespace identity.
    #[cfg(test)]
    pub(crate) fn open_verified_file(
        config: NvmeofReaderConfig,
        path: &std::path::Path,
    ) -> Result<Self, NvmeofError> {
        use std::os::unix::fs::OpenOptionsExt;
        let file = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECT | libc::O_CLOEXEC)
            .open(path)?;
        let meta = file.metadata()?;
        if !meta.is_file() || meta.len() != config.manifest.image_len {
            return Err(NvmeofError::Invalid("invalid file fixture"));
        }
        Self::open_inner(config, Some(file))
    }
    fn open_inner(
        config: NvmeofReaderConfig,
        fixture: Option<std::fs::File>,
    ) -> Result<Self, NvmeofError> {
        let m = &config.manifest;
        if m.version != 1 || m.catalog.is_empty() || m.wal_file_bytes == 0
            || !m.block_size.is_power_of_two() || m.block_size < 512
            || m.image_len == 0 || !m.image_len.is_multiple_of(u64::from(m.block_size))
            || config.max_inflight_requests == 0 || config.max_inflight_members == 0
            || config.max_inflight_members > 32768 || config.max_inflight_bytes == 0
            || config.max_inflight_requests > config.max_inflight_members
            // Charge catalog maps, request maps, operation/SQ slots and fixed
            // ring metadata before any constructor preallocation. The ring
            // rounds its entry count up to a power of two.
            || m.catalog.len().checked_mul(4096)
                .and_then(|n| n.checked_add(m.nqn.len()))
                .and_then(|n| config.max_inflight_requests.checked_mul(4096).and_then(|v| n.checked_add(v)))
                .and_then(|n| config.max_inflight_members.checked_next_power_of_two()
                    .and_then(|slots| slots.checked_mul(8192))
                    .and_then(|v| n.checked_add(v)))
                .and_then(|n| n.checked_add(4096))
                .is_none_or(|n| n > config.max_catalog_bytes)
        {
            return Err(NvmeofError::Invalid("invalid reader limits or manifest"));
        }
        let mut catalog = HashMap::with_capacity(m.catalog.len());
        let mut identities = HashMap::with_capacity(m.catalog.len());
        for e in &m.catalog {
            let r = e.reference;
            let expected = (r.batch_address.offset % m.wal_file_bytes)
                .checked_add(8)
                .and_then(|n| n.checked_add(u64::from(r.descriptor.offset)))
                .ok_or(NvmeofError::Invalid("member offset overflow"))?;
            if r.batch_id.database != m.database_uuid
                || r.batch_address.partition != m.partition
                || r.batch_address.segment_generation != m.generation
                || r.batch_address.offset / m.wal_file_bytes != m.source_file_index
                || r.batch_address
                    .offset
                    .checked_add(u64::from(r.batch_address.len))
                    .is_none_or(|end| end > m.durable_frontier)
                || expected != e.offset
                || e.len != r.descriptor.len as usize
                || e.offset
                    .checked_add(e.len as u64)
                    .is_none_or(|end| end > m.image_len)
                || catalog.insert(r, (e.offset, e.len)).is_some()
                || identities
                    .insert((r.batch_address, r.descriptor.operation_index), r)
                    .is_some()
            {
                return Err(NvmeofError::Invalid("invalid catalog mapping"));
            }
        }
        let transport = fixture.is_none().then_some(config.binding.transport);
        let namespace = match fixture {
            Some(file) => file,
            None => open_namespace(&config.binding, m)?,
        };
        let ring = IoUring::builder()
            .dontfork()
            .build(config.max_inflight_members.next_power_of_two() as u32)?;
        let requests = HashMap::with_capacity(config.max_inflight_requests);
        let operations = HashMap::with_capacity(config.max_inflight_members);
        let queued_tokens = VecDeque::with_capacity(config.max_inflight_members);
        let stats: SharedStats = Arc::new(ReaderStats::new(NvmeofStats {
            database_uuid: m.database_uuid,
            epoch: m.epoch,
            partition: m.partition,
            export_uuid: m.export_uuid,
            transport,
            ..NvmeofStats::default()
        }));
        let alignment = (m.block_size as usize).max(4096);
        let pool = BufferPool::new(alignment, config.pool_bytes(), stats.clone());
        let catalog = Arc::new(NvmeofCatalog {
            id: NEXT_READER_ID.fetch_add(1, Ordering::Relaxed),
            config,
            catalog,
            identities,
            stats,
        });
        Ok(Self {
            ring,
            namespace,
            catalog,
            pool,
            operations,
            requests,
            queued_tokens,
            next_token: 1,
            used_bytes: 0,
            closed: false,
            shutdown_at: None,
            failed: false,
            fatal_deadline: None,
            #[cfg(test)]
            submit_errors: VecDeque::new(),
        })
    }
    /// Lock-free planning view; clone it to plan without touching the reader.
    pub fn catalog(&self) -> &Arc<NvmeofCatalog> {
        &self.catalog
    }
    pub fn manifest(&self) -> &ExportManifest {
        self.catalog.manifest()
    }
    pub fn snapshot(&self) -> NvmeofStats {
        self.catalog.snapshot()
    }
    pub fn validate_authority(
        &self,
        database: [u8; 16],
        current_epoch: u64,
        partitions: &[u32],
    ) -> Result<(), NvmeofError> {
        let m = self.manifest();
        if m.database_uuid != database
            || m.epoch > current_epoch
            || !partitions.contains(&m.partition)
        {
            return Err(NvmeofError::Invalid("database inventory mismatch"));
        }
        Ok(())
    }
    pub fn eligibility(
        &self,
        reference: &ValueRecordReference,
    ) -> Result<Eligibility, NvmeofError> {
        self.catalog.eligibility(reference)
    }
    /// Plan and submit in one step; the coordinator plans once and calls
    /// [`Self::submit_planned`] so retries do not repeat the planning.
    pub fn submit(
        &mut self,
        id: u64,
        refs: Vec<ValueRecordReference>,
        deadline: Instant,
    ) -> Result<SubmitOutcome, NvmeofError> {
        let plan = self.catalog.plan_references(&refs)?;
        self.submit_planned(id, &plan, deadline)
    }
    pub fn submit_planned(
        &mut self,
        id: u64,
        plan: &ReadPlan,
        deadline: Instant,
    ) -> Result<SubmitOutcome, NvmeofError> {
        let admission_start = Instant::now();
        if self.closed || self.failed {
            return Err(NvmeofError::Invalid("reader closed"));
        }
        if plan.reader != self.catalog.id {
            return Err(NvmeofError::Invalid("plan belongs to another reader"));
        }
        if self.requests.contains_key(&id) {
            return Err(NvmeofError::Invalid("empty or duplicate request"));
        }
        let config = &self.catalog.config;
        if self.requests.len() >= config.max_inflight_requests
            || self
                .operations
                .len()
                .checked_add(plan.reads.len())
                .is_none_or(|n| n > config.max_inflight_members)
            || self
                .used_bytes
                .checked_add(plan.charge)
                .is_none_or(|n| n > config.max_inflight_bytes)
        {
            return Ok(SubmitOutcome::Full);
        }
        if Instant::now() >= deadline {
            return Err(NvmeofError::Timeout);
        }
        let stats = &self.catalog.stats;
        stats.admission.add(admission_start.elapsed());
        let final_token = self
            .next_token
            .checked_add(plan.reads.len() as u64)
            .ok_or(NvmeofError::Invalid("read token exhausted"))?;
        let mut entries =
            SmallVec::<[io_uring::squeue::Entry; 32]>::with_capacity(plan.reads.len());
        let mut prepared = SmallVec::<[(u64, Operation); 32]>::with_capacity(plan.reads.len());
        let queued = Instant::now();
        for (index, read) in plan.reads.iter().enumerate() {
            let mut storage = self.pool.take(read.len);
            let token = self.next_token + index as u64;
            let entry = opcode::Read::new(
                types::Fd(self.namespace.as_raw_fd()),
                storage.as_mut_ptr(),
                read.len as u32,
            )
            .offset(read.offset)
            .build()
            .user_data(token);
            entries.push(entry);
            prepared.push((
                token,
                Operation {
                    request: id,
                    offset: read.offset,
                    storage,
                    members: plan.slices[read.slices.clone()].iter().copied().collect(),
                    queued,
                    submitted: None,
                },
            ));
        }
        for (token, operation) in prepared {
            self.operations.insert(token, operation);
        }
        self.requests.insert(
            id,
            Request {
                remaining: plan.reads.len(),
                records: vec![None; plan.members],
                error: None,
                deadline,
                cancelled: false,
                cancelled_at: None,
                charge: plan.charge,
            },
        );
        // SAFETY: each SQE points into a pooled Vec owned by an Operation kept
        // in `operations`; the namespace FD and allocations outlive terminal CQ
        // retirement, and a buffer only returns to the pool after its CQE.
        // Drop fail-stops if operations remain. push_multiple is all-or-nothing.
        if unsafe { self.ring.submission().push_multiple(&entries) }.is_err() {
            for token in self.next_token..final_token {
                self.operations.remove(&token);
            }
            self.requests.remove(&id);
            return Ok(SubmitOutcome::Full);
        }
        self.queued_tokens.extend(self.next_token..final_token);
        let members = plan.members as u64;
        let sqes = plan.reads.len() as u64;
        stats.logical_requests.fetch_add(1, Ordering::Relaxed);
        stats.logical_members.fetch_add(members, Ordering::Relaxed);
        stats.eligible_hits.fetch_add(members, Ordering::Relaxed);
        stats.admitted_sqes.fetch_add(sqes, Ordering::Relaxed);
        stats
            .merged_members
            .fetch_add(members - sqes, Ordering::Relaxed);
        stats.aligned_requested_bytes.fetch_add(
            plan.reads.iter().map(|read| read.len as u64).sum(),
            Ordering::Relaxed,
        );
        self.next_token = final_token;
        self.used_bytes += plan.charge;
        Ok(SubmitOutcome::Submitted)
    }
    pub fn cancel(&mut self, id: u64) {
        if let Some(request) = self.requests.get_mut(&id) {
            request.cancelled = true;
            request.cancelled_at.get_or_insert_with(Instant::now);
        }
    }
    /// True when no request is accepted and not yet completed.
    pub fn is_idle(&self) -> bool {
        self.operations.is_empty()
    }
    /// Close admission without waiting; accepted requests retain their normal
    /// results and complete only after all their kernel operations retire.
    pub fn begin_shutdown(&mut self) {
        self.closed = true;
        self.shutdown_at.get_or_insert_with(Instant::now);
    }
    #[cfg(test)]
    pub(crate) fn fail_next_submit(&mut self, errno: i32) {
        self.submit_errors.push_back(errno);
    }
    fn submit_ring(&mut self) -> io::Result<usize> {
        #[cfg(test)]
        if let Some(errno) = self.submit_errors.pop_front() {
            return Err(io::Error::from_raw_os_error(errno));
        }
        self.ring.submit()
    }
    pub fn progress(&mut self) -> Result<Vec<Completion>, NvmeofError> {
        let stats = self.catalog.stats.clone();
        if !self.ring.submission().is_empty() {
            let started = Instant::now();
            match self.submit_ring() {
                Ok(n) => {
                    let now = started;
                    stats.io_submissions.fetch_add(n as u64, Ordering::Relaxed);
                    for _ in 0..n {
                        if let Some(token) = self.queued_tokens.pop_front()
                            && let Some(op) = self.operations.get_mut(&token)
                        {
                            stats.queue.add(now.duration_since(op.queued));
                            op.submitted = Some(now);
                        }
                    }
                }
                Err(e)
                    if matches!(
                        e.kind(),
                        io::ErrorKind::Interrupted | io::ErrorKind::WouldBlock
                    ) || e.raw_os_error() == Some(libc::EBUSY) => {}
                Err(e) => {
                    if !self.failed {
                        stats.io_errors.fetch_add(1, Ordering::Relaxed);
                    }
                    self.failed = true;
                    self.fatal_deadline
                        .get_or_insert_with(|| Instant::now() + Duration::from_secs(30));
                    let kind = e.kind();
                    let message = e.to_string();
                    for request in self.requests.values_mut() {
                        if request.error.is_none() {
                            request.error =
                                Some(NvmeofError::Io(io::Error::new(kind, message.clone())));
                        }
                    }
                }
            }
        }
        let mut out = Vec::new();
        for _ in 0..self.operations.len() {
            let Some(cqe) = self.ring.completion().next() else {
                break;
            };
            let token = cqe.user_data();
            // A foreign CQ token or missing request means our kernel ownership
            // accounting is corrupt; fail-stop instead of releasing live DMA.
            let Some(operation) = self.operations.remove(&token) else {
                std::process::abort()
            };
            let Some(request) = self.requests.get_mut(&operation.request) else {
                std::process::abort()
            };
            let result = cqe.result();
            let read_len = operation.storage.len;
            stats.io_completions.fetch_add(1, Ordering::Relaxed);
            if let Some(started) = operation.submitted {
                stats.submit_to_cq.add(started.elapsed());
            }
            if result < 0 || result as usize != read_len {
                stats.io_errors.fetch_add(1, Ordering::Relaxed);
            } else {
                stats
                    .aligned_read_bytes
                    .fetch_add(result as u64, Ordering::Relaxed);
            }
            if result < 0 || result as usize != read_len {
                if request.error.is_none() {
                    request.error = Some(NvmeofError::Read {
                        source: if result < 0 {
                            io::Error::from_raw_os_error(-result)
                        } else {
                            io::Error::new(
                                io::ErrorKind::UnexpectedEof,
                                "short direct namespace read",
                            )
                        },
                        result,
                        offset: operation.offset,
                        requested: read_len,
                        token,
                        partition: self.catalog.config.manifest.partition,
                        namespace: self.catalog.config.binding.namespace_path.clone(),
                        export_uuid: self.catalog.config.manifest.export_uuid,
                    });
                }
            } else if !request.cancelled && Instant::now() < request.deadline {
                let extraction_start = Instant::now();
                // The terminal CQE retired DMA ownership and wrote exactly
                // `read_len` bytes. One shared owner backs every member slice;
                // the buffer returns to the pool when the last slice drops.
                let whole = Bytes::from_owner(operation.storage);
                for member in &operation.members {
                    request.records[member.index] =
                        Some(whole.slice(member.start..member.start + member.len));
                }
                stats.extraction.add(extraction_start.elapsed());
            }
            request.remaining -= 1;
            if request.remaining == 0 {
                let mut request = self
                    .requests
                    .remove(&operation.request)
                    .ok_or(NvmeofError::Invalid("missing request"))?;
                self.used_bytes -= request.charge;
                let result = if let Some(error) = request.error.take() {
                    Err(error)
                } else if request.cancelled {
                    Err(NvmeofError::Cancelled)
                } else if Instant::now() >= request.deadline {
                    Err(NvmeofError::Timeout)
                } else {
                    request
                        .records
                        .into_iter()
                        .collect::<Option<Vec<_>>>()
                        .ok_or(NvmeofError::Invalid("missing read result"))
                };
                if matches!(&result, Err(NvmeofError::Timeout)) {
                    stats.io_timeouts.fetch_add(1, Ordering::Relaxed);
                }
                out.push(Completion {
                    id: operation.request,
                    result,
                });
            }
        }
        // No expired/cancelled request can retain DMA ownership forever.
        // Harvest CQEs first, then fail-stop if any remaining request or broken
        // ring exceeded its drain window. No credit is released early.
        let now = Instant::now();
        if (!self.operations.is_empty()
            && self.fatal_deadline.is_some_and(|deadline| now >= deadline))
            || self.requests.values().any(|request| {
                drain_expired(
                    request.deadline,
                    request.cancelled_at,
                    self.shutdown_at,
                    now,
                )
            })
        {
            std::process::abort();
        }
        Ok(out)
    }
    pub fn shutdown(&mut self) -> Result<(), NvmeofError> {
        self.begin_shutdown();
        let deadline = Instant::now() + Duration::from_secs(30);
        while !self.operations.is_empty() {
            self.progress()?;
            if Instant::now() >= deadline {
                return Err(NvmeofError::Timeout);
            }
            if !self.operations.is_empty() {
                std::thread::sleep(Duration::from_micros(50));
            }
        }
        Ok(())
    }
}
impl Drop for NvmeofReader {
    fn drop(&mut self) {
        // The reactor must drive begin_shutdown/progress before dropping.
        // Never block its thread or release buffers still owned by the kernel.
        if !self.operations.is_empty() {
            std::process::abort();
        }
    }
}

#[cfg(test)]
mod merge_tests {
    use super::*;

    fn member(offset: u64, read_len: usize, member_start: usize, member_len: usize) -> MemberRange {
        MemberRange {
            reader: 0,
            offset,
            read_len,
            member_start,
            member_len,
            charge: read_len + 4096 + member_len + 2048,
        }
    }

    /// `(index, start, len)` of each member slice of one merged read.
    type Slices = Vec<(usize, usize, usize)>;

    /// `(device offset, length, slices)` of each merged read.
    fn layout(members: &[MemberRange], max_read: u64) -> Vec<(u64, usize, Slices)> {
        let (reads, slices) = merge_reads(members, max_read).unwrap();
        reads
            .iter()
            .map(|read| {
                (
                    read.offset,
                    read.len,
                    slices[read.slices.clone()]
                        .iter()
                        .map(|slice| (slice.index, slice.start, slice.len))
                        .collect(),
                )
            })
            .collect()
    }

    #[test]
    fn same_block_members_share_one_read_and_keep_their_exact_slices() {
        // Members of one block, listed in request order.
        let members = [
            member(8192, 4096, 3000, 500),
            member(8192, 4096, 100, 200),
            member(8192, 4096, 100, 200),
        ];
        assert_eq!(
            layout(&members, 1 << 20),
            [(
                8192,
                4096,
                vec![(0, 3000, 500), (1, 100, 200), (2, 100, 200)]
            )]
        );
    }

    #[test]
    fn overlapping_and_adjacent_reads_merge_without_reading_a_gap() {
        let members = [
            member(0, 8192, 10, 100),
            member(4096, 8192, 5, 50),    // overlaps [4096, 8192)
            member(12288, 4096, 0, 4096), // adjacent to the end at 12288
        ];
        assert_eq!(
            layout(&members, 1 << 20),
            [(
                0,
                16384,
                vec![(0, 10, 100), (1, 4096 + 5, 50), (2, 12288, 4096)]
            )]
        );
    }

    #[test]
    fn disjoint_reads_stay_separate() {
        // One block between the members: merging would read 4096 unneeded bytes.
        let members = [member(0, 4096, 0, 10), member(8192, 4096, 0, 10)];
        assert_eq!(
            layout(&members, 1 << 20),
            [(0, 4096, vec![(0, 0, 10)]), (8192, 4096, vec![(1, 0, 10)])]
        );
    }

    #[test]
    fn a_contained_member_does_not_shrink_the_read() {
        let members = [member(0, 16384, 0, 16384), member(4096, 4096, 7, 9)];
        assert_eq!(
            layout(&members, 1 << 20),
            [(0, 16384, vec![(0, 0, 16384), (1, 4096 + 7, 9)])]
        );
    }

    #[test]
    fn merged_reads_respect_member_and_byte_limits() {
        let same = vec![member(0, 4096, 0, 1); MAX_MERGE_MEMBERS + 1];
        let reads = layout(&same, 1 << 20);
        assert_eq!(reads.len(), 2);
        assert_eq!(reads[0].2.len(), MAX_MERGE_MEMBERS);
        assert_eq!(reads[1].2.len(), 1);
        // Four touching blocks, but a read may be at most two blocks long.
        let touching: Vec<_> = (0..4).map(|i| member(i * 4096, 4096, 0, 1)).collect();
        let reads = layout(&touching, 8192);
        assert_eq!(
            reads
                .iter()
                .map(|read| (read.0, read.1))
                .collect::<Vec<_>>(),
            [(0, 8192), (8192, 8192)]
        );
    }
}

#[cfg(test)]
mod pool_tests {
    use super::*;

    fn pool(pool_bytes: usize) -> BufferPool {
        let stats: SharedStats = Arc::new(ReaderStats::new(NvmeofStats::default()));
        BufferPool::new(4096, pool_bytes, stats)
    }

    #[test]
    fn buffer_is_not_reused_while_a_slice_is_alive() {
        let mut pool = pool(1 << 20);
        let mut storage = pool.take(4096);
        assert_eq!(storage.as_mut_ptr() as usize % 4096, 0);
        let first = storage.as_mut_ptr() as usize;
        // Simulate the terminal CQE having filled the window.
        // SAFETY: the window is `len` bytes inside the owned Vec.
        unsafe { std::ptr::write_bytes(storage.as_mut_ptr(), 0xAB, 4096) };
        let whole = Bytes::from_owner(storage);
        let slice = whole.slice(10..20);
        drop(whole);
        // The slice keeps the only owner alive: a new request must not get it.
        let mut other = pool.take(4096);
        assert_ne!(other.as_mut_ptr() as usize, first);
        assert_eq!(&slice[..], &[0xAB; 10]);
        assert_eq!(pool.shared.stats.snapshot().pool_recycled, 0);
        drop(slice);
        assert_eq!(pool.shared.stats.snapshot().pool_recycled, 1);
        drop(other);
        // Both buffers are idle now; the next two takes reuse them.
        let mut a = pool.take(4096);
        let mut b = pool.take(4096);
        let seen = [a.as_mut_ptr() as usize, b.as_mut_ptr() as usize];
        assert!(seen.contains(&first));
        let stats = pool.shared.stats.snapshot();
        assert_eq!((stats.pool_hits, stats.pool_misses), (2, 2));
    }

    #[test]
    fn idle_pool_never_exceeds_its_byte_budget() {
        // Total idle bytes counts each buffer's whole allocation, slack included.
        let alignment = 4096;
        for budget in [0, 1, 8191, 1 << 16, 1 << 20, 8 << 20] {
            let total: usize = (0..POOL_CLASSES)
                .map(|class| {
                    BufferPool::class_capacity(budget, alignment, class)
                        * ((class + 1) * alignment + alignment - 1)
                })
                .sum();
            assert!(total <= budget, "budget {budget} retains {total}");
        }
        // A zero-capacity class retains nothing: the buffer is freed on drop.
        let mut pool = pool(0);
        drop(pool.take(4096));
        let stats = pool.shared.stats.snapshot();
        assert_eq!((stats.pool_recycled, stats.pool_discarded), (0, 1));
    }

    #[test]
    fn allocation_matches_the_charged_buffer_size() {
        // The byte charge assumes `pooled_len`; the allocation must not exceed it.
        let mut pool = pool(1 << 20);
        for len in [512, 4096, 4608, 3 * 4096, 32 * 4096, 33 * 4096 + 512] {
            let storage = pool.take(len);
            assert_eq!(storage.storage.len(), pooled_len(len, 4096) + 4095);
            assert!(storage.aligned_start + len <= storage.storage.len());
        }
    }

    #[test]
    fn pool_is_bounded_and_oversize_buffers_are_not_retained() {
        // Budget for exactly one idle 4 KiB buffer (8191 bytes with its slack).
        let mut pool = pool(POOL_CLASSES * 8191);
        let first = pool.take(4096);
        let second = pool.take(4096);
        drop(first);
        drop(second);
        let stats = pool.shared.stats.snapshot();
        assert_eq!((stats.pool_recycled, stats.pool_discarded), (1, 1));
        // More alignment units than the largest class: allocated exactly, never retained.
        let huge = pool.take((POOL_CLASSES + 1) * 4096);
        drop(huge);
        let stats = pool.shared.stats.snapshot();
        assert_eq!((stats.pool_recycled, stats.pool_discarded), (1, 2));
    }

    #[test]
    fn dropping_a_slice_after_the_reader_is_gone_is_harmless() {
        let mut pool = pool(1 << 20);
        let storage = pool.take(8192);
        let slice = Bytes::from_owner(storage).slice(..16);
        drop(pool);
        drop(slice);
    }
}

#[cfg(test)]
mod watchdog_tests {
    use super::{DRAIN_TIMEOUT, drain_expired};
    use std::time::{Duration, Instant};

    #[test]
    fn read_error_retains_errno_and_completion_context() {
        use super::NvmeofError;
        use std::error::Error;

        let error = NvmeofError::Read {
            source: std::io::Error::from_raw_os_error(libc::EINVAL),
            result: -libc::EINVAL,
            offset: 8192,
            requested: 8192,
            token: 17,
            partition: 3,
            namespace: "/dev/nvme3n1".into(),
            export_uuid: [7; 16],
        };
        assert_eq!(
            error
                .source()
                .unwrap()
                .downcast_ref::<std::io::Error>()
                .unwrap()
                .raw_os_error(),
            Some(libc::EINVAL)
        );
        let message = error.to_string();
        for detail in [
            "result: -22",
            "offset: 8192",
            "requested: 8192",
            "token: 17",
            "partition: 3",
            "/dev/nvme3n1",
        ] {
            assert!(message.contains(detail), "{message}");
        }
    }

    #[test]
    fn timeout_cancel_and_shutdown_have_independent_drain_boundaries() {
        let start = Instant::now();
        let deadline = start + Duration::from_secs(10);
        let cancelled = start + Duration::from_secs(2);
        let shutdown = start + Duration::from_secs(4);
        let before = DRAIN_TIMEOUT - Duration::from_nanos(1);
        assert!(!drain_expired(deadline, None, None, deadline + before));
        assert!(drain_expired(
            deadline,
            None,
            None,
            deadline + DRAIN_TIMEOUT
        ));
        assert!(!drain_expired(
            deadline,
            Some(cancelled),
            None,
            cancelled + before
        ));
        assert!(drain_expired(
            deadline,
            Some(cancelled),
            None,
            cancelled + DRAIN_TIMEOUT
        ));
        assert!(!drain_expired(
            deadline,
            None,
            Some(shutdown),
            shutdown + before
        ));
        assert!(drain_expired(
            deadline,
            None,
            Some(shutdown),
            shutdown + DRAIN_TIMEOUT
        ));
        // The earliest applicable event starts the safety deadline.
        assert!(drain_expired(
            deadline,
            Some(cancelled),
            Some(shutdown),
            cancelled + DRAIN_TIMEOUT
        ));
    }
}
