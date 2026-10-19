//! Bounded kernel-asynchronous immutable member reads; no blocking-pool fallback.
//!
//! KVell §5.4 supplies the submit/poll and immediate resident-hit mechanism, not
//! its placement or whole-database cache assumptions. Unlike its wait-for-batch
//! io_getevents loop, ordinary progress here never waits for disk completion.
//! Source: BLepers/KVell a6cbc0cb186c7934dc6be785225d809f62730fea,
//! ioengine.c::read_page_async/worker_do_io and slabworker.c.
//! io-uring 0.7.14 (MIT OR Apache-2.0), tokio-rs/io-uring
//! ba9149010ccda85bcdf28623d9268b10dd353a93: src/squeue.rs::push_multiple
//! requires buffers and fds alive through completion. Its IoUring::drop does NOT
//! drain operations. This module owns that lifetime explicitly, including Drop.
//! Resident-first reads follow eRPC §§3.1–3.2's short-handler/offloaded-work
//! split, not its RAM-only workload: RWF_NOWAIT never waits for file data/locks.
//! Linux v6.17 io_uring/rw.c::__io_read returns NOWAIT misses without reissue;
//! only EAGAIN/EOPNOTSUPP retry once with IOSQE_ASYNC and the same owners.
//! https://github.com/torvalds/linux/blob/v6.17/io_uring/rw.c
mod cache;
#[cfg(all(test, target_os = "linux"))]
mod tests;

#[cfg(test)]
use super::super::storage::PartitionLog;
use super::super::storage::{RecordReadPlan, StorageError};
use super::super::types::ValueRecordReference;
use super::ReadClass;
#[cfg(target_os = "linux")]
use io_uring::{IoUring, opcode, squeue, types};
use minibytes::Bytes;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io;
use std::mem::{MaybeUninit, size_of};
#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ReadExecutorConfig {
    pub(crate) cache_bytes: usize,
    pub(crate) memory_allowance_bytes: usize,
    pub(crate) max_requests: usize,
    pub(crate) max_bytes: usize,
    pub(crate) max_device_reads: usize,
    pub(crate) resident_first: bool,
}
impl Default for ReadExecutorConfig {
    fn default() -> Self {
        Self {
            cache_bytes: 0,
            memory_allowance_bytes: 512 * 1024 * 1024,
            max_requests: 32,
            max_bytes: 64 * 1024 * 1024,
            max_device_reads: 128,
            resident_first: false,
        }
    }
}
impl ReadExecutorConfig {
    pub(crate) fn cache_metadata_bytes(self) -> usize {
        cache::Cache::metadata_bytes(self.cache_bytes)
    }
    pub(crate) fn validate(self) -> Result<(), StorageError> {
        if self.memory_allowance_bytes == 0
            || self
                .cache_bytes
                .checked_add(self.max_bytes)
                .is_none_or(|bytes| bytes > self.memory_allowance_bytes)
            || !(1..=4096).contains(&self.max_requests)
            || !(1..=256 * 1024 * 1024).contains(&self.max_bytes)
            || !(1..=4096).contains(&self.max_device_reads)
        {
            return Err(StorageError::Invalid(
                "invalid asynchronous read/cache bounds",
            ));
        }
        Ok(())
    }
}

/// Node-wide counters. Bytes are requested retained allocation sizes, not RSS:
/// cache includes fixed slots and exact payload/Arc owners; pending includes
/// buffers and conservative request/member/map metadata. Idle reusable buffers
/// share max_bytes with pending reservations, not an additional memory budget.
/// Their bounded pointer table (at most min(max_device_reads, 128)) and reusable
/// request tables/SQE scratch/kernel rings are metadata outside payload budgets.
/// Allocator rounding is excluded. Leases retain credit through reply retirement.
/// Cache and credit snapshots are sampled independently. Cache/response clones
/// share storage; budgets remain conservative, not unique RSS.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReadExecutorStats {
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub cache_inserts: u64,
    pub cache_evictions: u64,
    pub cache_oversized: u64,
    pub cache_bypass_disabled: u64,
    pub cache_bypass_eviction_limit: u64,
    pub cache_bypass_scan: u64,
    pub cache_bypass_generation: u64,
    /// Charged exact member allocation bytes admitted; excludes fixed slots.
    pub cache_admitted_bytes: u64,
    /// Encoded member bytes served by hits, not currently caller-held bytes.
    pub cache_served_bytes: u64,
    pub cache_entries: usize,
    pub cache_bytes: usize,
    pub cache_metadata_bytes: usize,
    pub cache_budget: usize,
    /// Logical WAL member reads admitted, including synchronous diagnostics.
    /// Retries do not add another member/byte count; these are not disk IOPS.
    pub wal_reads: u64,
    pub wal_read_bytes: u64,
    /// RWF_NOWAIT SQEs admitted, without forced io-wq offload.
    pub resident_read_attempts: u64,
    /// Exact-length successful NOWAIT CQEs, not explicit-cache hits.
    pub resident_read_successes: u64,
    /// NOWAIT misses, non-support or partial prefixes followed by one async read.
    pub resident_read_fallbacks: u64,
    /// EOPNOTSUPP subset of fallbacks. Linux can also report non-support as
    /// EAGAIN, so zero does not establish filesystem support.
    pub resident_read_unsupported: u64,
    /// Partial resident prefixes completed by asynchronously reading only the suffix.
    pub resident_read_partial: u64,
    /// IOSQE_ASYNC SQEs admitted directly or after a resident fallback.
    /// This plus resident_read_attempts counts asynchronous-executor filesystem
    /// read submissions, NOT physical NVMe I/O or synchronous diagnostics.
    pub async_read_submissions: u64,
    pub completed_reads: u64,
    pub errors: u64,
    pub cancellations: u64,
    pub inflight_requests: usize,
    pub inflight_bytes: usize,
    pub inflight_reads: usize,
    pub peak_inflight_requests: usize,
    pub peak_inflight_bytes: usize,
    pub peak_inflight_reads: usize,
    pub peak_device_reads: usize,
    /// Reusable, exact-sized buffers not owned by any SQE or Bytes clone.
    /// Idle plus inflight reservations never exceeds max_bytes.
    pub idle_buffer_bytes: usize,
    pub idle_buffers: usize,
    pub buffer_reuses: u64,
    pub buffer_allocations: u64,
}
struct AdmissionState {
    stats: ReadExecutorStats,
    devices: HashMap<u64, usize>,
    buffers: Vec<Box<[MaybeUninit<u8>]>>,
}
struct CacheState {
    cache: cache::Cache,
    stats: ReadExecutorStats,
    generation: u64,
}
/// Shared by ALL node connection rings: cache and request/byte/device limits
/// never multiply with connection count. No mutex is held during a syscall.
/// Cache lookup/eviction never holds the admission mutex on entry. Final-owner
/// recycling may take admission under cache; admission never takes cache.
/// The admission lock makes cross-connection budgets and buffer recycling
/// atomic; the cache lock protects CLOCK mutation, clones and clear generations.
/// Neither is replaced by unprotected reclamation or held across submission.
pub(crate) struct ReadAdmission {
    config: ReadExecutorConfig,
    state: Mutex<AdmissionState>,
    cache: Mutex<CacheState>,
}
impl ReadAdmission {
    pub(crate) fn new(config: ReadExecutorConfig) -> Result<Self, StorageError> {
        config.validate()?;
        let mut stats = ReadExecutorStats::default();
        let cache = cache::Cache::new(config.cache_bytes, &mut stats);
        Ok(Self {
            config,
            cache: Mutex::new(CacheState {
                cache,
                stats,
                generation: 0,
            }),
            state: Mutex::new(AdmissionState {
                stats: ReadExecutorStats::default(),
                devices: HashMap::new(),
                buffers: Vec::with_capacity(config.max_device_reads.min(128)),
            }),
        })
    }
    pub(crate) fn stats(&self) -> ReadExecutorStats {
        let cache = self.cache.lock().stats;
        let mut stats = self.state.lock().stats;
        stats.cache_inserts = cache.cache_inserts;
        stats.cache_evictions = cache.cache_evictions;
        stats.cache_oversized = cache.cache_oversized;
        stats.cache_entries = cache.cache_entries;
        stats.cache_bytes = cache.cache_bytes;
        stats.cache_metadata_bytes = cache.cache_metadata_bytes;
        stats.cache_budget = cache.cache_budget;
        stats.cache_bypass_disabled = cache.cache_bypass_disabled;
        stats.cache_bypass_eviction_limit = cache.cache_bypass_eviction_limit;
        stats.cache_bypass_scan = cache.cache_bypass_scan;
        stats.cache_bypass_generation = cache.cache_bypass_generation;
        stats.cache_admitted_bytes = cache.cache_admitted_bytes;
        stats
    }
    /// Diagnostic read admission, retained through response send retirement.
    /// Caller includes all retained work in `bytes`; this is not device credit.
    pub(crate) fn reserve_reply(
        self: &Arc<Self>,
        bytes: usize,
    ) -> Result<Option<ReadLease>, StorageError> {
        if bytes == 0 || bytes > self.config.max_bytes {
            return Err(StorageError::Capacity);
        }
        let mut state = self.state.lock();
        if state.stats.inflight_requests >= self.config.max_requests
            || bytes > self.config.max_bytes - state.stats.inflight_bytes
        {
            return Ok(None);
        }
        // Idle storage shares the existing byte allowance, not a second pool
        // budget. Evict before admission; active CQ/response owners never move.
        while bytes
            > self.config.max_bytes - state.stats.inflight_bytes - state.stats.idle_buffer_bytes
        {
            let buffer = state.buffers.pop().expect("idle buffer accounting");
            state.stats.idle_buffer_bytes -= buffer.len();
            state.stats.idle_buffers -= 1;
        }
        state.stats.inflight_requests += 1;
        state.stats.inflight_bytes += bytes;
        state.stats.peak_inflight_requests = state
            .stats
            .peak_inflight_requests
            .max(state.stats.inflight_requests);
        state.stats.peak_inflight_bytes = state
            .stats
            .peak_inflight_bytes
            .max(state.stats.inflight_bytes);
        Ok(Some(ReadLease {
            admission: self.clone(),
            charge: bytes,
        }))
    }
    /// Diagnostic mapped/positional paths use the same evidence names, but do
    /// not claim kernel asynchronous scheduling or populate the resident cache.
    pub(crate) fn record_sync_read(&self, bytes: usize, success: bool) {
        let mut state = self.state.lock();
        state.stats.wal_reads += 1;
        state.stats.wal_read_bytes += bytes as u64;
        state.stats.cache_misses += 1;
        if success {
            state.stats.completed_reads += 1;
        } else {
            state.stats.errors += 1;
        }
    }
    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn clear_cache(&self) {
        let mut state = self.cache.lock();
        let CacheState {
            cache,
            stats,
            generation,
        } = &mut *state;
        cache.clear(stats);
        // Clear and completion fills share only the cache lock, never credits.
        // Exhaustion is fail-stop rather than permitting a generation ABA.
        *generation = generation
            .checked_add(1)
            .expect("cache generation exhausted");
    }
    fn take_buffer(&self, len: usize) -> Box<[MaybeUninit<u8>]> {
        let mut state = self.state.lock();
        // Exact-size classes avoid padding the wire client's retained-byte
        // reservation. The scan is bounded by max_device_reads (and 128).
        if let Some(index) = state.buffers.iter().position(|buffer| buffer.len() == len) {
            let buffer = state.buffers.swap_remove(index);
            state.stats.idle_buffer_bytes -= len;
            state.stats.idle_buffers -= 1;
            state.stats.buffer_reuses += 1;
            return buffer;
        }
        state.stats.buffer_allocations += 1;
        drop(state);
        Box::<[u8]>::new_uninit_slice(len)
    }
    fn return_buffer(&self, buffer: Box<[MaybeUninit<u8>]>) {
        let mut state = self.state.lock();
        if state.buffers.len() < self.config.max_device_reads.min(128)
            && buffer.len()
                <= self.config.max_bytes
                    - state.stats.inflight_bytes
                    - state.stats.idle_buffer_bytes
        {
            state.stats.idle_buffer_bytes += buffer.len();
            state.stats.idle_buffers += 1;
            state.buffers.push(buffer);
        }
    }
    fn release_reads(&self, device: u64, count: usize) {
        let mut state = self.state.lock();
        state.stats.inflight_reads -= count;
        let current = state.devices.get_mut(&device).expect("reserved device");
        *current -= count;
        if *current == 0 {
            state.devices.remove(&device);
        }
        // Keep the bounded high-water table: the next scalar read would
        // otherwise allocate it again after every last completion.
    }
}

/// Transfers request/payload admission through completed response ownership.
/// Transport retains this guard until its TLS/provider send is retired.
pub(crate) struct ReadLease {
    admission: Arc<ReadAdmission>,
    charge: usize,
}
impl Drop for ReadLease {
    fn drop(&mut self) {
        let mut state = self.admission.state.lock();
        state.stats.inflight_requests -= 1;
        state.stats.inflight_bytes -= self.charge;
    }
}

pub(crate) type PlannedRead = (ValueRecordReference, RecordReadPlan);

pub(crate) enum SubmitOutcome {
    Ready {
        records: Vec<Bytes>,
        lease: ReadLease,
    },
    Pending,
    Full(Vec<PlannedRead>),
}
pub(crate) struct ReadCompletion {
    pub(crate) request_id: u64,
    pub(crate) result: Result<Vec<Bytes>, StorageError>,
    pub(crate) lease: Option<ReadLease>,
}
struct PendingRequest {
    records: Vec<Option<Bytes>>,
    remaining: usize,
    lease: Option<ReadLease>,
    devices: Vec<(u64, usize)>,
    generation: u64,
    class: ReadClass,
    error: Option<StorageError>,
    cancelled: bool,
}

/// The single final Bytes owner returns the allocation, never a CQ callback
/// while a cache/response clone can still read it. Weak avoids a cache cycle.
struct ReadBuffer {
    buffer: Option<Box<[MaybeUninit<u8>]>>,
    admission: Weak<ReadAdmission>,
}
impl AsRef<[u8]> for ReadBuffer {
    fn as_ref(&self) -> &[u8] {
        let buffer = self.buffer.as_ref().expect("owned read buffer");
        // SAFETY: constructed only after an exact-length successful CQE.
        unsafe { std::slice::from_raw_parts(buffer.as_ptr().cast(), buffer.len()) }
    }
}
impl minibytes::BytesOwner for ReadBuffer {}
impl Drop for ReadBuffer {
    fn drop(&mut self) {
        if let Some(admission) = self.admission.upgrade() {
            admission.return_buffer(self.buffer.take().expect("owned read buffer"));
        }
    }
}
#[derive(Clone, Copy, Eq, PartialEq)]
enum ReadStage {
    Resident,
    Async,
}
struct Operation {
    request_id: u64,
    index: usize,
    reference: ValueRecordReference,
    plan: RecordReadPlan,
    buffer: Box<[MaybeUninit<u8>]>,
    stage: ReadStage,
    /// Initialized resident prefix; no Bytes is published until the suffix completes.
    filled: usize,
}

#[cfg(target_os = "linux")]
impl Operation {
    fn entry(&mut self, token: u64) -> squeue::Entry {
        let read = opcode::Read::new(
            types::Fd(self.plan.member.file.as_raw_fd()),
            self.buffer.as_mut_ptr().wrapping_add(self.filled).cast(),
            (self.plan.member.len - self.filled) as u32,
        )
        .offset(self.plan.member.offset + self.filled as u64);
        match self.stage {
            // io-uring 0.7.14 opcode::Read::rw_flags uses preadv2(2) flags.
            // Merely dropping ASYNC would allow kernel-managed blocking retry.
            ReadStage::Resident => read.rw_flags(libc::RWF_NOWAIT).build(),
            ReadStage::Async => read.build().flags(squeue::Flags::ASYNC),
        }
        .user_data(token)
    }
}
const REQUEST_METADATA_BYTES: usize = 4 * (size_of::<PendingRequest>() + 2 * size_of::<u64>());
const MEMBER_METADATA_BYTES: usize = 4
    * (size_of::<Operation>() + size_of::<Option<Bytes>>() + 4 * size_of::<u64>())
    + cache::OWNER_BYTES;
// Wire clients reserve 4096 + sum(member.len + 4096) when chunking. Keep that
// conservative contract valid if internal owner layouts change on any target.
const _: () = assert!(REQUEST_METADATA_BYTES <= 4096 && MEMBER_METADATA_BYTES <= 4096);

pub(crate) struct ReadExecutor {
    #[cfg(target_os = "linux")]
    ring: IoUring,
    #[cfg(target_os = "linux")]
    entries: Vec<squeue::Entry>,
    #[cfg(all(test, target_os = "linux"))]
    submit_errors: std::collections::VecDeque<i32>,
    #[cfg(all(test, target_os = "linux"))]
    completion_results: std::collections::VecDeque<(ReadStage, i32)>,
    admission: Arc<ReadAdmission>,
    config: ReadExecutorConfig,
    requests: HashMap<u64, PendingRequest>,
    operations: HashMap<u64, Operation>,
    next_token: u64,
    closed: bool,
    failed: bool,
}
impl ReadExecutor {
    pub(crate) fn new(
        config: ReadExecutorConfig,
        admission: Arc<ReadAdmission>,
    ) -> Result<Self, StorageError> {
        config.validate()?;
        if config != admission.config {
            return Err(StorageError::Invalid("inconsistent node read admission"));
        }
        #[cfg(not(target_os = "linux"))]
        return Err(StorageError::Io(io::Error::new(
            io::ErrorKind::Unsupported,
            "async WAL reads require Linux io_uring",
        )));
        #[cfg(target_os = "linux")]
        {
            let ring = IoUring::builder()
                .dontfork()
                .build(config.max_device_reads.next_power_of_two() as u32)?;
            Ok(Self {
                ring,
                entries: Vec::with_capacity(config.max_device_reads),
                #[cfg(test)]
                submit_errors: std::collections::VecDeque::new(),
                #[cfg(test)]
                completion_results: std::collections::VecDeque::new(),
                admission,
                config,
                requests: HashMap::with_capacity(config.max_requests),
                operations: HashMap::with_capacity(config.max_device_reads),
                next_token: 1,
                closed: false,
                failed: false,
            })
        }
    }

    #[cfg(test)]
    pub(crate) fn submit(
        &mut self,
        request_id: u64,
        members: Vec<(Arc<PartitionLog>, ValueRecordReference)>,
        class: ReadClass,
    ) -> Result<SubmitOutcome, StorageError> {
        // Lower-level executor tests prepare real authority synchronously. The
        // serving reactor only calls submit_planned with already pinned plans.
        let plans = members
            .into_iter()
            .map(|(log, reference)| {
                log.plan_record_read(&reference)
                    .map(|plan| (reference, plan))
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.submit_planned(request_id, plans, class, &mut None)
    }

    /// Plans already own validated immutable authority and native file pins.
    /// No cold metadata lookup runs here. Full/errors retain the caller's reply
    /// reservation; only Ready or an admitted kernel request takes its lease.
    pub(crate) fn submit_planned(
        &mut self,
        request_id: u64,
        plans: Vec<PlannedRead>,
        class: ReadClass,
        lease: &mut Option<ReadLease>,
    ) -> Result<SubmitOutcome, StorageError> {
        if self.closed || self.failed {
            return Err(StorageError::Poisoned);
        }
        if self.requests.contains_key(&request_id) {
            return Err(StorageError::Invalid("duplicate pending read id"));
        }
        if plans.is_empty() || plans.len() > 4096 {
            return Err(StorageError::Invalid("invalid read member count"));
        }
        // Conservatively charge retained hash-map slots (including control bytes
        // and spare capacity), result slots, exact buffer owners and device slots.
        let mut charge = REQUEST_METADATA_BYTES;
        for (_, plan) in &plans {
            charge = charge
                .checked_add(plan.member.len)
                .and_then(|n| n.checked_add(MEMBER_METADATA_BYTES))
                .ok_or(StorageError::Capacity)?;
        }
        if charge > self.config.max_bytes {
            return Err(StorageError::Capacity);
        }
        // Reserve reply ownership before cloning a cache entry. Cache eviction
        // can now run independently without leaving a detached clone uncharged.
        if lease.is_none() {
            *lease = self.admission.reserve_reply(charge)?;
        }
        let Some(reservation) = lease.as_ref() else {
            return Ok(SubmitOutcome::Full(plans));
        };
        if !Arc::ptr_eq(&reservation.admission, &self.admission) || reservation.charge < charge {
            return Err(StorageError::Invalid(
                "read plan exceeds its reply reservation",
            ));
        }
        let mut records = Vec::with_capacity(plans.len());
        let mut devices = Vec::<(u64, usize)>::new();
        let mut wal_bytes = 0u64;
        let mut misses = 0;
        let mut served_bytes = 0u64;
        let mut cache = self.admission.cache.lock();
        let generation = cache.generation;
        for (reference, plan) in &plans {
            let bytes = cache.cache.get(reference);
            if bytes.is_none() {
                misses += 1;
                wal_bytes += plan.member.len as u64;
                if let Some((_, count)) = devices.iter_mut().find(|(id, _)| *id == plan.device) {
                    *count += 1;
                } else {
                    devices.push((plan.device, 1));
                }
            } else if let Some(bytes) = &bytes {
                served_bytes += bytes.len() as u64;
            }
            records.push(bytes);
        }
        drop(cache);
        let mut state = self.admission.state.lock();
        if misses == 0 {
            // The reply reservation and full-reference checks still apply.
            // No SQ access, token arithmetic or device-table work on a hit.
            state.stats.cache_hits += records.len() as u64;
            state.stats.cache_served_bytes += served_bytes;
            drop(state);
            return Ok(SubmitOutcome::Ready {
                records: records.into_iter().map(Option::unwrap).collect(),
                lease: lease.take().expect("reserved read reply"),
            });
        }
        if devices
            .iter()
            .any(|(_, count)| *count > self.config.max_device_reads)
            || misses > self.config.max_device_reads
        {
            return Err(StorageError::Capacity);
        }
        if self.requests.len() >= self.config.max_requests
            || self.operations.len() + misses > self.config.max_device_reads
            || devices.iter().any(|(id, count)| {
                state.devices.get(id).copied().unwrap_or(0) + count > self.config.max_device_reads
            })
        {
            return Ok(SubmitOutcome::Full(plans));
        }
        #[cfg(target_os = "linux")]
        {
            let sq = self.ring.submission();
            if sq.capacity() - sq.len() < misses {
                return Ok(SubmitOutcome::Full(plans));
            }
        }
        self.next_token
            .checked_add(misses as u64)
            .ok_or(StorageError::Capacity)?;
        state.stats.cache_hits += (records.len() - misses) as u64;
        state.stats.cache_misses += misses as u64;
        state.stats.cache_served_bytes += served_bytes;
        state.stats.inflight_reads += misses;
        state.stats.peak_inflight_reads = state
            .stats
            .peak_inflight_reads
            .max(state.stats.inflight_reads);
        for (id, count) in &devices {
            let current = state.devices.entry(*id).or_default();
            *current += count;
            let current = *current;
            state.stats.peak_device_reads = state.stats.peak_device_reads.max(current);
        }
        drop(state);
        self.requests.insert(
            request_id,
            PendingRequest {
                records,
                remaining: misses,
                lease: lease.take(),
                devices,
                generation,
                class,
                error: None,
                cancelled: false,
            },
        );
        #[cfg(target_os = "linux")]
        self.entries.clear();
        for (index, (reference, plan)) in plans.into_iter().enumerate() {
            if self.requests[&request_id].records[index].is_some() {
                continue;
            }
            let token = self.next_token;
            self.next_token += 1;
            let mut op = Operation {
                request_id,
                index,
                reference,
                buffer: self.admission.take_buffer(plan.member.len),
                plan,
                stage: if self.config.resident_first {
                    ReadStage::Resident
                } else {
                    ReadStage::Async
                },
                filled: 0,
            };
            #[cfg(target_os = "linux")]
            self.entries.push(op.entry(token));
            self.operations.insert(token, op);
        }
        #[cfg(target_os = "linux")]
        {
            // SAFETY: heap buffer addresses do not move; Operation retains the
            // exact file plus PartitionLog. No buffer is read or freed until
            // its CQE; all error/drop paths drain before releasing owners.
            if unsafe { self.ring.submission().push_multiple(&self.entries) }.is_err() {
                self.operations.retain(|_, op| op.request_id != request_id);
                let request = self.requests.remove(&request_id).unwrap();
                for (device, count) in &request.devices {
                    self.admission.release_reads(*device, *count);
                }
                return Err(StorageError::Invalid(
                    "read submission queue unexpectedly full",
                ));
            }
        }
        {
            let mut state = self.admission.state.lock();
            state.stats.wal_reads += misses as u64;
            state.stats.wal_read_bytes += wal_bytes;
            if self.config.resident_first {
                state.stats.resident_read_attempts += misses as u64;
            } else {
                state.stats.async_read_submissions += misses as u64;
            }
        }
        self.flush()?;
        Ok(SubmitOutcome::Pending)
    }

    #[cfg(target_os = "linux")]
    fn submit_io(&mut self) -> io::Result<usize> {
        #[cfg(test)]
        if let Some(errno) = self.submit_errors.pop_front() {
            return Err(io::Error::from_raw_os_error(errno));
        }
        self.ring.submit()
    }

    fn flush(&mut self) -> Result<(), StorageError> {
        #[cfg(target_os = "linux")]
        if !self.ring.submission().is_empty() {
            match self.submit_io() {
                Ok(_) => {} // A partial submit remains queued for the next poll.
                Err(e)
                    if matches!(
                        e.kind(),
                        io::ErrorKind::Interrupted | io::ErrorKind::WouldBlock
                    ) || e.raw_os_error() == Some(libc::EBUSY) => {}
                Err(e) => {
                    self.failed = true;
                    self.admission.state.lock().stats.errors += 1;
                    return Err(e.into());
                }
            }
        }
        Ok(())
    }
    /// Wait only in tests, without consuming the CQE that the reactor must
    /// harvest. The real kernel still owns every submitted file and buffer.
    #[cfg(all(test, target_os = "linux"))]
    pub(super) fn wait_for_ready_completion(&mut self) -> Result<(), StorageError> {
        self.ring.submit_and_wait(1)?;
        Ok(())
    }

    pub(crate) fn poll_completed(&mut self) -> Result<Vec<ReadCompletion>, StorageError> {
        self.flush()?;
        let mut completed = Vec::new();
        #[cfg(target_os = "linux")]
        for _ in 0..self.operations.len() {
            let Some(cqe) = self.ring.completion().next() else {
                break;
            };
            let token = cqe.user_data();
            let Some(mut op) = self.operations.remove(&token) else {
                self.failed = true;
                return Err(StorageError::Invalid("unknown read completion token"));
            };
            let result = cqe.result();
            // Inject only after the real CQE retires kernel ownership. A
            // positive resident injection must initialize its promised prefix.
            #[cfg(test)]
            let result = if self
                .completion_results
                .front()
                .is_some_and(|(stage, _)| *stage == op.stage)
            {
                let (_, injected) = self.completion_results.pop_front().unwrap();
                assert!(injected < op.plan.member.len as i32);
                if op.stage == ReadStage::Resident && injected > 0 {
                    use std::os::unix::fs::FileExt;
                    let mut prefix = vec![0; injected as usize];
                    op.plan
                        .member
                        .file
                        .read_exact_at(&mut prefix, op.plan.member.offset)
                        .unwrap();
                    for (slot, byte) in op.buffer.iter_mut().zip(prefix) {
                        slot.write(byte);
                    }
                }
                injected
            } else {
                result
            };
            // NOWAIT may copy resident pages before reaching an absent page.
            // Preserve that prefix and submit only the missing suffix.
            let partial = result > 0 && (result as usize) < op.plan.member.len;
            if op.stage == ReadStage::Resident
                && (result == -libc::EAGAIN || result == -libc::EOPNOTSUPP || partial)
            {
                op.stage = ReadStage::Async;
                op.filled = if partial { result as usize } else { 0 };
                let entry = op.entry(token);
                // The removed slot preserves table capacity; restore ownership
                // before publishing another kernel-visible pointer.
                self.operations.insert(token, op);
                // A completed operation has consumed its old SQ slot. With
                // <= max_device_reads operations and a ring at least that big,
                // one replacement fits even if all other SQEs are still queued.
                // SAFETY: keep the same buffer, fd/log, token and all credits.
                // The retired CQE initializes filled bytes; the replacement
                // targets the disjoint remaining suffix within that allocation.
                if unsafe { self.ring.submission().push(&entry) }.is_err() {
                    self.failed = true;
                    return Err(StorageError::Invalid(
                        "read fallback submission queue unexpectedly full",
                    ));
                }
                let mut state = self.admission.state.lock();
                state.stats.resident_read_fallbacks += 1;
                state.stats.resident_read_unsupported += u64::from(result == -libc::EOPNOTSUPP);
                state.stats.resident_read_partial += u64::from(partial);
                state.stats.async_read_submissions += 1;
                continue;
            }
            let request = self
                .requests
                .get_mut(&op.request_id)
                .expect("owned read request");
            if result < 0 || result as usize != op.plan.member.len - op.filled {
                self.admission.state.lock().stats.errors += 1;
                if request.error.is_none() {
                    request.error = Some(StorageError::Io(if result < 0 {
                        io::Error::from_raw_os_error(-result)
                    } else {
                        io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "short asynchronous WAL member read",
                        )
                    }));
                }
                self.admission.return_buffer(op.buffer);
            } else {
                // The CQE retires kernel ownership, but all Bytes clones must
                // retire too before this allocation can back another SQE.
                let bytes = Bytes::from_owner(ReadBuffer {
                    buffer: Some(op.buffer),
                    admission: Arc::downgrade(&self.admission),
                });
                {
                    let mut state = self.admission.state.lock();
                    state.stats.completed_reads += 1;
                    state.stats.resident_read_successes +=
                        u64::from(op.stage == ReadStage::Resident);
                }
                if !request.cancelled {
                    let mut state = self.admission.cache.lock();
                    if request.generation != state.generation {
                        state.stats.cache_bypass_generation += 1;
                    } else if request.class == ReadClass::ScanNoFill {
                        state.stats.cache_bypass_scan += 1;
                    } else {
                        let CacheState { cache, stats, .. } = &mut *state;
                        cache.insert(op.reference, &bytes, stats);
                    }
                }
                request.records[op.index] = Some(bytes);
            }
            request.remaining -= 1;
            self.admission.release_reads(op.plan.device, 1);
            if request.remaining == 0 {
                let mut request = self.requests.remove(&op.request_id).unwrap();
                // Suppress cancelled successes, never a late storage failure.
                if !request.cancelled || request.error.is_some() {
                    let result = if let Some(error) = request.error {
                        Err(error)
                    } else {
                        Ok(request.records.into_iter().map(Option::unwrap).collect())
                    };
                    completed.push(ReadCompletion {
                        request_id: op.request_id,
                        result,
                        lease: request.lease.take(),
                    });
                }
            }
        }
        Ok(completed)
    }
    /// Cancellation suppresses delivery, not kernel ownership or byte credits.
    /// Submitted reads run to CQ retirement; no unsafe close-and-free shortcut.
    pub(crate) fn cancel(&mut self, request_id: u64) {
        if let Some(request) = self.requests.get_mut(&request_id)
            && !request.cancelled
        {
            request.cancelled = true;
            self.admission.state.lock().stats.cancellations += 1;
        }
    }
    /// Only shutdown waits, for at most 30 seconds. All operation owners survive
    /// retryable SQ pressure; a fatal error or expired drain leaves them retained.
    /// Drop fail-stops rather than freeing memory still owned by the kernel.
    pub(crate) fn shutdown(&mut self) -> Result<(), StorageError> {
        self.closed = true;
        for request in self.requests.values_mut() {
            request.cancelled = true;
        }
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut failure = None;
        for _ in 0..600_000 {
            if self.operations.is_empty() {
                break;
            }
            for completion in self.poll_completed()? {
                if let Err(error) = completion.result
                    && failure.is_none()
                {
                    failure = Some(error);
                }
            }
            if !self.operations.is_empty() {
                if Instant::now() >= deadline {
                    break;
                }
                // Use the same nonblocking submission/retry path as progress.
                // A blocking enter could wait forever on a stalled device.
                std::thread::sleep(Duration::from_micros(50));
            }
        }
        if !self.operations.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "asynchronous WAL read drain expired",
            )
            .into());
        }
        if let Some(error) = failure {
            Err(error)
        } else {
            Ok(())
        }
    }
}
impl Drop for ReadExecutor {
    fn drop(&mut self) {
        let drained = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.shutdown()));
        if drained.is_err() || !self.operations.is_empty() {
            // Neither an invariant panic nor a fatal ring failure proves that
            // kernel pointers are retired. Never unwind/free live I/O owners.
            std::process::abort();
        }
    }
}
