//! Nonblocking io_uring queue. The namespace and every aligned allocation stay
//! owned until their terminal CQE; cancellation never releases DMA ownership.
use super::metrics::{NvmeofStats, SharedStats};
use super::namespace::open_namespace;
use super::{ExportManifest, NamespaceBinding};
use crate::distributed::types::{PhysicalValueAddress, ValueRecordReference};
use io_uring::{IoUring, opcode, types};
use minibytes::Bytes;
use std::collections::{HashMap, VecDeque};
use std::io;
use std::os::fd::AsRawFd;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Debug)]
pub enum NvmeofError {
    Invalid(&'static str),
    Io(io::Error),
    Timeout,
    Cancelled,
}
impl std::fmt::Display for NvmeofError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for NvmeofError {}
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

struct Operation {
    request: u64,
    index: usize,
    storage: Vec<u8>,
    aligned_start: usize,
    read_len: usize,
    member_start: usize,
    member_len: usize,
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
    config: NvmeofReaderConfig,
    catalog: HashMap<ValueRecordReference, (u64, usize)>,
    identities: HashMap<(PhysicalValueAddress, u32), ValueRecordReference>,
    operations: HashMap<u64, Operation>,
    requests: HashMap<u64, Request>,
    queued_tokens: VecDeque<u64>,
    pub(super) stats: SharedStats,
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
        let stats = NvmeofStats {
            database_uuid: m.database_uuid,
            epoch: m.epoch,
            partition: m.partition,
            export_uuid: m.export_uuid,
            transport,
            ..NvmeofStats::default()
        };
        Ok(Self {
            ring,
            namespace,
            config,
            catalog,
            identities,
            operations,
            requests,
            queued_tokens,
            stats: Arc::new(Mutex::new(stats)),
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
    pub fn manifest(&self) -> &ExportManifest {
        &self.config.manifest
    }
    pub fn snapshot(&self) -> NvmeofStats {
        self.stats.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
    /// Record consumer-side checksum and decoding time; this is not device time.
    pub fn record_verification(&self, elapsed: Duration) {
        self.stats
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .verification
            .add(elapsed);
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
    /// Count a routing fallback once per logical member, not per eligibility probe.
    pub fn record_ineligible_fallbacks(&self, members: usize) {
        let mut stats = self.stats.lock().unwrap_or_else(|e| e.into_inner());
        stats.ineligible_fallbacks = stats.ineligible_fallbacks.saturating_add(members as u64);
    }
    fn range(
        &self,
        reference: &ValueRecordReference,
    ) -> Result<(u64, usize, usize, usize), NvmeofError> {
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
        if end > self.manifest().image_len || end - start > u32::MAX as u64 {
            return Err(NvmeofError::Invalid("aligned range exceeds namespace"));
        }
        Ok((
            start,
            (end - start) as usize,
            (offset - start) as usize,
            len,
        ))
    }
    pub fn aligned_charge(&self, reference: &ValueRecordReference) -> Result<usize, NvmeofError> {
        let started = Instant::now();
        let (_, size, _, member) = self.range(reference)?;
        let align = (self.manifest().block_size as usize).max(4096);
        let charge = size
            .checked_add(align - 1)
            .and_then(|n| n.checked_add(member))
            .and_then(|n| n.checked_add(2048))
            .ok_or(NvmeofError::Invalid("read charge overflow"))?;
        self.stats
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .alignment
            .add(started.elapsed());
        Ok(charge)
    }
    pub fn max_request_members(&self) -> usize {
        self.config.max_inflight_members
    }
    pub fn max_request_bytes(&self) -> usize {
        self.config.max_inflight_bytes
    }
    pub fn submit(
        &mut self,
        id: u64,
        refs: Vec<ValueRecordReference>,
        deadline: Instant,
    ) -> Result<SubmitOutcome, NvmeofError> {
        let admission_start = Instant::now();
        if self.closed || self.failed {
            return Err(NvmeofError::Invalid("reader closed"));
        }
        if refs.is_empty() || self.requests.contains_key(&id) {
            return Err(NvmeofError::Invalid("empty or duplicate request"));
        }
        let mut charge = 0usize;
        for reference in &refs {
            if self.eligibility(reference)? != Eligibility::Eligible {
                return Err(NvmeofError::Invalid("reference not admitted"));
            }
            charge = charge
                .checked_add(self.aligned_charge(reference)?)
                .ok_or(NvmeofError::Invalid("request charge overflow"))?;
        }
        if refs.len() > self.config.max_inflight_members || charge > self.config.max_inflight_bytes
        {
            return Err(NvmeofError::Invalid("request exceeds reader capacity"));
        }
        if self.requests.len() >= self.config.max_inflight_requests
            || self
                .operations
                .len()
                .checked_add(refs.len())
                .is_none_or(|n| n > self.config.max_inflight_members)
            || self
                .used_bytes
                .checked_add(charge)
                .is_none_or(|n| n > self.config.max_inflight_bytes)
        {
            return Ok(SubmitOutcome::Full);
        }
        if Instant::now() >= deadline {
            return Err(NvmeofError::Timeout);
        }
        self.stats
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .admission
            .add(admission_start.elapsed());
        let final_token = self
            .next_token
            .checked_add(refs.len() as u64)
            .ok_or(NvmeofError::Invalid("read token exhausted"))?;
        let mut entries = Vec::with_capacity(refs.len());
        let mut prepared = Vec::with_capacity(refs.len());
        let alignment = (self.manifest().block_size as usize).max(4096);
        for (index, reference) in refs.iter().enumerate() {
            let (offset, read_len, member_start, member_len) = self.range(reference)?;
            let mut storage = vec![0; read_len + alignment - 1];
            let aligned_start = (alignment - (storage.as_ptr() as usize % alignment)) % alignment;
            let token = self.next_token + index as u64;
            let entry = opcode::Read::new(
                types::Fd(self.namespace.as_raw_fd()),
                storage.as_mut_ptr().wrapping_add(aligned_start),
                read_len as u32,
            )
            .offset(offset)
            .build()
            .user_data(token);
            entries.push(entry);
            prepared.push((
                token,
                Operation {
                    request: id,
                    index,
                    storage,
                    aligned_start,
                    read_len,
                    member_start,
                    member_len,
                    queued: Instant::now(),
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
                remaining: refs.len(),
                records: vec![None; refs.len()],
                error: None,
                deadline,
                cancelled: false,
                cancelled_at: None,
                charge,
            },
        );
        // SAFETY: each SQE points into a Vec retained in operations; the
        // namespace FD and allocations outlive terminal CQ retirement. Drop
        // fail-stops if operations remain. push_multiple is all-or-nothing.
        if unsafe { self.ring.submission().push_multiple(&entries) }.is_err() {
            for token in self.next_token..final_token {
                self.operations.remove(&token);
            }
            self.requests.remove(&id);
            return Ok(SubmitOutcome::Full);
        }
        self.queued_tokens.extend(self.next_token..final_token);
        {
            let mut stats = self.stats.lock().unwrap_or_else(|e| e.into_inner());
            stats.logical_requests += 1;
            stats.logical_members += refs.len() as u64;
            stats.eligible_hits += refs.len() as u64;
            for token in self.next_token..final_token {
                if let Some(op) = self.operations.get(&token) {
                    stats.aligned_requested_bytes += op.read_len as u64;
                }
            }
        }
        self.next_token = final_token;
        self.used_bytes += charge;
        Ok(SubmitOutcome::Submitted)
    }
    pub fn cancel(&mut self, id: u64) {
        if let Some(request) = self.requests.get_mut(&id) {
            request.cancelled = true;
            request.cancelled_at.get_or_insert_with(Instant::now);
        }
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
        if !self.ring.submission().is_empty() {
            let started = Instant::now();
            match self.submit_ring() {
                Ok(n) => {
                    let now = started;
                    let mut stats = self.stats.lock().unwrap_or_else(|e| e.into_inner());
                    stats.io_submissions += n as u64;
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
                        self.stats
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .io_errors += 1;
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
            {
                let mut stats = self.stats.lock().unwrap_or_else(|e| e.into_inner());
                stats.io_completions += 1;
                if let Some(started) = operation.submitted {
                    stats.submit_to_cq.add(started.elapsed());
                }
                if result < 0 || result as usize != operation.read_len {
                    stats.io_errors += 1;
                } else {
                    stats.aligned_read_bytes += result as u64;
                }
            }
            if result < 0 || result as usize != operation.read_len {
                if request.error.is_none() {
                    request.error = Some(NvmeofError::Io(if result < 0 {
                        io::Error::from_raw_os_error(-result)
                    } else {
                        io::Error::new(io::ErrorKind::UnexpectedEof, "short direct namespace read")
                    }));
                }
            } else if !request.cancelled && Instant::now() < request.deadline {
                let extraction_start = Instant::now();
                let start = operation.aligned_start + operation.member_start;
                // The terminal CQE retired DMA ownership. Retain the aligned
                // allocation and expose only the exact member, without copying.
                let bytes: Bytes = operation.storage.into();
                request.records[operation.index] =
                    Some(bytes.slice(start..start + operation.member_len));
                self.stats
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .extraction
                    .add(extraction_start.elapsed());
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
                    self.stats
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .io_timeouts += 1;
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
mod watchdog_tests {
    use super::{DRAIN_TIMEOUT, drain_expired};
    use std::time::{Duration, Instant};

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
