//! Bounded nonblocking resident reads plus kernel-asynchronous immutable reads;
//! no blocking-pool fallback.
//!
//! Resident-first reads use a small synchronous preadv2(RWF_NOWAIT) budget on
//! already-authoritative plans. The eRPC short-handler split motivates keeping
//! only this tightly bounded work on the serving thread; TeRM's page-residency
//! metadata is not used as proof and its custom RDMA ODP mechanism is unrelated.
//! NOWAIT never waits for backing storage/locks, but may return a resident prefix.
//! https://man7.org/linux/man-pages/man2/preadv2.2.html
//! https://github.com/erpc-io/eRPC/blob/de83dab3eab4a0fb19bfc4881c11d4a6b89ff17d/src/rpc_impl/rpc_req.cc
//! https://github.com/thustorage/TeRM/blob/a94d44a59238d8d51266a7a2ea8bbf2daf3151d9/libterm/ibverbs-pdp/mr.cc
//! The eRPC foreground request handler runs on its dispatch thread and limits
//! borrowed RX-buffer lifetime to the handler; this path likewise keeps its
//! bounded synchronous work on the executor owner and transfers owned buffers.
//! Remaining reads use io_uring. io-uring 0.7.14 (MIT OR Apache-2.0),
//! tokio-rs/io-uring ba9149010ccda85bcdf28623d9268b10dd353a93: src/squeue.rs
//! requires buffers and fds alive through completion. IoUring::drop does NOT
//! drain operations. This module owns that lifetime explicitly, including Drop.
// Shared across all submit_planned calls until the owner loop begins its next sweep.
const RESIDENT_NOWAIT_MAX_CALLS: usize = 4;
const RESIDENT_NOWAIT_MAX_BYTES: usize = 64 * 1024;
const RESIDENT_NOWAIT_MAX_MEMBER_BYTES: usize = 16 * 1024;
// Coalesce only ranges whose union contains no unauthorized gaps. Bounded
// original-order runs avoid sorting and keep scalar/nonadjacent reads cheap.
const COALESCED_MAX_MEMBERS: usize = 32;
const COALESCED_MAX_BYTES: usize = 64 * 1024;
// At most 32 successful probes per request, spread over fair owner sweeps.
const RESIDENT_NOWAIT_MAX_SWEEPS: usize = 8;
#[cfg(all(test, target_os = "linux"))]
mod tests;

#[cfg(test)]
use super::super::storage::PartitionLog;
use super::super::storage::{RecordReadPlan, StorageError};
use super::super::types::ValueRecordReference;
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
#[cfg(all(test, target_os = "linux"))]
use std::os::unix::fs::FileExt;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ReadExecutorConfig {
    pub(crate) memory_allowance_bytes: usize,
    pub(crate) max_requests: usize,
    pub(crate) max_bytes: usize,
    pub(crate) max_device_reads: usize,
    pub(crate) resident_first: bool,
}
impl Default for ReadExecutorConfig {
    fn default() -> Self {
        Self {
            memory_allowance_bytes: 512 * 1024 * 1024,
            max_requests: 32,
            max_bytes: 64 * 1024 * 1024,
            max_device_reads: 128,
            resident_first: false,
        }
    }
}
impl ReadExecutorConfig {
    pub(crate) fn validate(self) -> Result<(), StorageError> {
        if self.memory_allowance_bytes == 0
            || self.max_bytes > self.memory_allowance_bytes
            || !(1..=4096).contains(&self.max_requests)
            || !(1..=256 * 1024 * 1024).contains(&self.max_bytes)
            || !(1..=4096).contains(&self.max_device_reads)
        {
            return Err(StorageError::Invalid("invalid asynchronous read bounds"));
        }
        Ok(())
    }
}

/// Node-wide counters. Pending bytes include buffers, prepared retry state and
/// conservative request/member/map metadata. Idle reusable buffers share
/// max_bytes with pending reservations; allocator rounding is excluded.
/// Leases retain credit through prepared retries and reply retirement.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReadExecutorStats {
    /// Shard authority-cache snapshot; `None` means unavailable, not zero.
    pub authority: Option<super::super::storage::AuthorityCacheStats>,
    /// Logical WAL member reads admitted, including synchronous diagnostics.
    /// Retries do not add another member/byte count; these are not disk IOPS.
    pub wal_reads: u64,
    pub wal_read_bytes: u64,
    /// Synchronous preadv2(RWF_NOWAIT) calls actually issued on resident-first
    /// reads. A later Full retry reuses prepared progress without repeating calls.
    pub resident_read_attempts: u64,
    /// Exact-length successful NOWAIT calls; these created no SQE/CQE.
    pub resident_read_successes: u64,
    /// EAGAIN/EOPNOTSUPP/short NOWAIT results ultimately followed by one async read.
    pub resident_read_fallbacks: u64,
    /// EOPNOTSUPP subset of attempted fallbacks. EAGAIN may mean unsupported too.
    pub resident_read_unsupported: u64,
    /// Partial NOWAIT prefixes completed by asynchronously reading only the suffix.
    pub resident_read_partial: u64,
    /// Members left async because the owner-loop sweep's call/byte/member bounds expired.
    pub resident_read_deferred: u64,
    /// IOSQE_ASYNC reads admitted directly or after a resident fallback. Not
    /// a count of physical NVMe I/O.
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
    /// Idle buffers dropped so a new reply fits the shared byte allowance.
    pub idle_buffer_evictions: u64,
    /// Per-device read-credit shard locks: the only lock on a read's async path.
    pub device_credit_lock: LockProfileStats,
    /// Session-pool registry: touched at session open/close, eviction, snapshots.
    pub pool_registry_lock: LockProfileStats,
    /// Send-path counters summed across all sessions of this owner, including
    /// closed ones. Zero without an OFI transport.
    pub transport: super::OwnerTransportStats,
}

/// Evidence for one remaining lock. Wait time is measured only for acquisitions
/// that found the lock held; hold time covers every acquisition.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct LockProfileStats {
    pub acquisitions: u64,
    pub contended: u64,
    pub wait_ns: u64,
    pub hold_ns: u64,
}

/// Counters for one lock. Relaxed ordering: they are independent statistics
/// and publish no data.
#[derive(Default)]
struct LockProfile {
    acquisitions: AtomicU64,
    contended: AtomicU64,
    wait_ns: AtomicU64,
    hold_ns: AtomicU64,
}
impl LockProfile {
    /// Runs `work` under `mutex` without returning the guard.
    /// Callers must not do I/O inside `work`.
    fn run<T, R>(&self, mutex: &Mutex<T>, work: impl FnOnce(&mut T) -> R) -> R {
        let (mut guard, waited) = if let Some(guard) = mutex.try_lock() {
            (guard, 0)
        } else {
            let began = Instant::now();
            let guard = mutex.lock();
            self.contended.fetch_add(1, Ordering::Relaxed);
            (guard, super::elapsed_ns(began))
        };
        let held = Instant::now();
        let result = work(&mut *guard);
        drop(guard);
        self.acquisitions.fetch_add(1, Ordering::Relaxed);
        self.wait_ns.fetch_add(waited, Ordering::Relaxed);
        self.hold_ns
            .fetch_add(super::elapsed_ns(held), Ordering::Relaxed);
        result
    }
    fn snapshot(&self) -> LockProfileStats {
        LockProfileStats {
            acquisitions: self.acquisitions.load(Ordering::Relaxed),
            contended: self.contended.load(Ordering::Relaxed),
            wait_ns: self.wait_ns.load(Ordering::Relaxed),
            hold_ns: self.hold_ns.load(Ordering::Relaxed),
        }
    }
}

/// Event counters. A session's executor increments its own set; the node sums
/// them in `stats()`. Relaxed ordering: independent statistics, no data hand-off.
#[derive(Default)]
struct Counters {
    wal_reads: AtomicU64,
    wal_read_bytes: AtomicU64,
    resident_read_attempts: AtomicU64,
    resident_read_successes: AtomicU64,
    resident_read_fallbacks: AtomicU64,
    resident_read_unsupported: AtomicU64,
    resident_read_partial: AtomicU64,
    resident_read_deferred: AtomicU64,
    async_read_submissions: AtomicU64,
    completed_reads: AtomicU64,
    errors: AtomicU64,
    buffer_reuses: AtomicU64,
    buffer_allocations: AtomicU64,
}
fn bump(counter: &AtomicU64, amount: u64) {
    counter.fetch_add(amount, Ordering::Relaxed);
}
impl Counters {
    fn add_to(&self, stats: &mut ReadExecutorStats) {
        let load = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        stats.wal_reads += load(&self.wal_reads);
        stats.wal_read_bytes += load(&self.wal_read_bytes);
        stats.resident_read_attempts += load(&self.resident_read_attempts);
        stats.resident_read_successes += load(&self.resident_read_successes);
        stats.resident_read_fallbacks += load(&self.resident_read_fallbacks);
        stats.resident_read_unsupported += load(&self.resident_read_unsupported);
        stats.resident_read_partial += load(&self.resident_read_partial);
        stats.resident_read_deferred += load(&self.resident_read_deferred);
        stats.async_read_submissions += load(&self.async_read_submissions);
        stats.completed_reads += load(&self.completed_reads);
        stats.errors += load(&self.errors);
        stats.buffer_reuses += load(&self.buffer_reuses);
        stats.buffer_allocations += load(&self.buffer_allocations);
    }
    /// Moves a closing session's totals into the node's retired set.
    fn fold_into(&self, retired: &Counters) {
        macro_rules! fold {
            ($($field:ident),* $(,)?) => {$(
                bump(&retired.$field, self.$field.load(Ordering::Relaxed));
            )*};
        }
        fold!(
            wal_reads,
            wal_read_bytes,
            resident_read_attempts,
            resident_read_successes,
            resident_read_fallbacks,
            resident_read_unsupported,
            resident_read_partial,
            resident_read_deferred,
            async_read_submissions,
            completed_reads,
            errors,
            buffer_reuses,
            buffer_allocations,
        );
    }
}

/// Bounded retries for every compare-exchange loop below. Exhaustion reports
/// "no room right now" (`None`/`false`); callers already retry on Full.
const CAS_ATTEMPTS: usize = 64;
const DEVICE_SHARDS: usize = 16;

/// Node-wide per-device in-flight read credits, sharded by device so reads on
/// different devices never share a lock. Lock order: a thread holds at most one
/// shard lock at a time (multi-device requests lock shards one after another and
/// roll back on failure), so no ordering between shards is needed.
struct DeviceCredits {
    shards: [Mutex<HashMap<u64, usize>>; DEVICE_SHARDS],
    lock: LockProfile,
}
impl DeviceCredits {
    fn new() -> Self {
        Self {
            shards: std::array::from_fn(|_| Mutex::new(HashMap::new())),
            lock: LockProfile::default(),
        }
    }
    fn shard(&self, device: u64) -> &Mutex<HashMap<u64, usize>> {
        // Fibonacci hashing: device ids are inode/dev numbers, often sequential.
        let index = (device.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 60) as usize;
        &self.shards[index % DEVICE_SHARDS]
    }
    /// All-or-nothing: credits taken for earlier devices are returned when a
    /// later device is at `limit`. Reports the highest per-device depth reached.
    fn try_reserve(&self, devices: &[(u64, usize)], limit: usize, peak: &AtomicUsize) -> bool {
        for (taken, (device, count)) in devices.iter().enumerate() {
            let depth = self.lock.run(self.shard(*device), |shard| {
                let current = shard.entry(*device).or_default();
                if *current + count > limit {
                    if *current == 0 {
                        shard.remove(device);
                    }
                    return None;
                }
                *current += count;
                Some(*current)
            });
            let Some(depth) = depth else {
                for (device, count) in &devices[..taken] {
                    self.release(*device, *count);
                }
                return false;
            };
            peak.fetch_max(depth, Ordering::Relaxed);
        }
        true
    }
    fn release(&self, device: u64, count: usize) {
        self.lock.run(self.shard(device), |shard| {
            let current = shard.get_mut(&device).expect("reserved device");
            *current -= count;
            if *current == 0 {
                shard.remove(&device);
            }
        });
    }
}

/// Reserved reply bytes (high half) and idle pooled-buffer bytes (low half) in
/// one word, so admission, eviction and recycling each decide and update both
/// in a single compare-exchange. `max_bytes <= 256 MiB` fits 32 bits.
const IDLE_MASK: u64 = u32::MAX as u64;

/// Node-wide admission. Reply requests and bytes are atomics; nothing here
/// serializes unrelated sessions. Buffer reuse and event counters belong to each
/// session's `SessionPool`. The only remaining locks are the per-device credit
/// shards (one device's reads) and the pool registry (cold: session open/close,
/// idle eviction under byte pressure, stats snapshots); both report
/// acquisition, contention, wait and hold time in `ReadExecutorStats`.
/// Lock order: registry, then one pool's buffer lock via `try_lock` only; a
/// device shard lock is never held together with either.
pub(crate) struct ReadAdmission {
    config: ReadExecutorConfig,
    /// Acquire/Release: a thread that wins the byte CAS owns that room.
    bytes: AtomicU64,
    inflight_requests: AtomicUsize,
    idle_buffers: AtomicUsize,
    inflight_reads: AtomicUsize,
    peak_inflight_requests: AtomicUsize,
    peak_inflight_bytes: AtomicUsize,
    peak_inflight_reads: AtomicUsize,
    peak_device_reads: AtomicUsize,
    cancellations: AtomicU64,
    idle_evictions: AtomicU64,
    /// Diagnostic synchronous reads have no session; their counters live here.
    node_counters: Counters,
    /// Totals of closed sessions, so a snapshot never loses history.
    retired: Counters,
    devices: DeviceCredits,
    pools: Mutex<Vec<Weak<SessionPool>>>,
    /// Send-path counters of every session, flushed by the session threads.
    pub(super) transport: Arc<super::OwnerTransportCounters>,
    pools_lock: LockProfile,
}
impl ReadAdmission {
    pub(crate) fn new(config: ReadExecutorConfig) -> Result<Self, StorageError> {
        config.validate()?;
        Ok(Self {
            config,
            bytes: AtomicU64::new(0),
            inflight_requests: AtomicUsize::new(0),
            idle_buffers: AtomicUsize::new(0),
            inflight_reads: AtomicUsize::new(0),
            peak_inflight_requests: AtomicUsize::new(0),
            peak_inflight_bytes: AtomicUsize::new(0),
            peak_inflight_reads: AtomicUsize::new(0),
            peak_device_reads: AtomicUsize::new(0),
            cancellations: AtomicU64::new(0),
            idle_evictions: AtomicU64::new(0),
            node_counters: Counters::default(),
            retired: Counters::default(),
            transport: Arc::default(),
            devices: DeviceCredits::new(),
            pools: Mutex::new(Vec::new()),
            pools_lock: LockProfile::default(),
        })
    }
    /// A new session-owned buffer pool and counter set, registered for stats.
    pub(crate) fn session_pool(self: &Arc<Self>) -> Arc<SessionPool> {
        let pool = Arc::new(SessionPool {
            admission: self.clone(),
            counters: Counters::default(),
            buffers: Mutex::new(Vec::with_capacity(self.pool_capacity())),
        });
        self.pools_lock.run(&self.pools, |pools| {
            pools.retain(|pool| pool.strong_count() > 0);
            pools.push(Arc::downgrade(&pool));
        });
        pool
    }
    fn pool_capacity(&self) -> usize {
        self.config.max_device_reads.min(128)
    }
    pub(crate) fn stats(&self) -> ReadExecutorStats {
        // A monitor read is not part of the contention evidence it reports, so
        // this acquisition is deliberately not counted in `pools_lock`.
        let pools = {
            let mut pools = self.pools.lock();
            pools.retain(|pool| pool.strong_count() > 0);
            pools.iter().filter_map(Weak::upgrade).collect::<Vec<_>>()
        };
        self.snapshot(&pools)
    }
    /// Nonblocking operational snapshot. Contention on the registry makes the
    /// whole sample unavailable. Not transactional across sessions.
    pub(crate) fn try_stats(&self) -> Option<ReadExecutorStats> {
        let pools = self
            .pools
            .try_lock()?
            .iter()
            .filter_map(Weak::upgrade)
            .collect::<Vec<_>>();
        Some(self.snapshot(&pools))
    }
    fn snapshot(&self, pools: &[Arc<SessionPool>]) -> ReadExecutorStats {
        let mut stats = ReadExecutorStats::default();
        self.node_counters.add_to(&mut stats);
        self.retired.add_to(&mut stats);
        for pool in pools {
            pool.counters.add_to(&mut stats);
        }
        let word = self.bytes.load(Ordering::Acquire);
        stats.inflight_bytes = (word >> 32) as usize;
        stats.idle_buffer_bytes = (word & IDLE_MASK) as usize;
        stats.idle_buffers = self.idle_buffers.load(Ordering::Relaxed);
        stats.inflight_requests = self.inflight_requests.load(Ordering::Acquire);
        stats.inflight_reads = self.inflight_reads.load(Ordering::Acquire);
        stats.peak_inflight_requests = self.peak_inflight_requests.load(Ordering::Relaxed);
        stats.peak_inflight_bytes = self.peak_inflight_bytes.load(Ordering::Relaxed);
        stats.peak_inflight_reads = self.peak_inflight_reads.load(Ordering::Relaxed);
        stats.peak_device_reads = self.peak_device_reads.load(Ordering::Relaxed);
        stats.cancellations = self.cancellations.load(Ordering::Relaxed);
        stats.idle_buffer_evictions = self.idle_evictions.load(Ordering::Relaxed);
        stats.device_credit_lock = self.devices.lock.snapshot();
        stats.pool_registry_lock = self.pools_lock.snapshot();
        stats.transport = self.transport.snapshot();
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
        if !self.acquire_request() {
            return Ok(None);
        }
        if !self.acquire_bytes(bytes) {
            // Release: nothing else was published for this request.
            self.inflight_requests.fetch_sub(1, Ordering::Release);
            return Ok(None);
        }
        Ok(Some(ReadLease {
            admission: self.clone(),
            charge: bytes,
            prepared: None,
        }))
    }
    fn acquire_request(&self) -> bool {
        let mut current = self.inflight_requests.load(Ordering::Relaxed);
        for _ in 0..CAS_ATTEMPTS {
            if current >= self.config.max_requests {
                return false;
            }
            match self.inflight_requests.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    self.peak_inflight_requests
                        .fetch_max(current + 1, Ordering::Relaxed);
                    return true;
                }
                Err(seen) => current = seen,
            }
        }
        false
    }
    /// Reserves `bytes` of reply credit. Idle pooled buffers share the byte
    /// allowance: when only they stand in the way they are evicted first, so
    /// reserved plus idle bytes never exceed `max_bytes`.
    fn acquire_bytes(&self, bytes: usize) -> bool {
        let max = self.config.max_bytes as u64;
        let need = bytes as u64;
        let mut word = self.bytes.load(Ordering::Acquire);
        for _ in 0..CAS_ATTEMPTS {
            let reserved = word >> 32;
            let idle = word & IDLE_MASK;
            if need > max.saturating_sub(reserved) {
                return false;
            }
            let excess = (reserved + need + idle).saturating_sub(max);
            if excess > 0 {
                if self.evict_idle(excess as usize) == 0 {
                    return false;
                }
                word = self.bytes.load(Ordering::Acquire);
                continue;
            }
            match self.bytes.compare_exchange_weak(
                word,
                word + (need << 32),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.peak_inflight_bytes
                        .fetch_max((reserved + need) as usize, Ordering::Relaxed);
                    return true;
                }
                Err(seen) => word = seen,
            }
        }
        false
    }
    /// Drops idle buffers from session pools until `want` bytes are freed.
    /// A pool whose owner is mid-operation is skipped, never waited for.
    fn evict_idle(&self, want: usize) -> usize {
        let pools = self.pools_lock.run(&self.pools, |pools| {
            pools.iter().filter_map(Weak::upgrade).collect::<Vec<_>>()
        });
        let mut freed = 0;
        for pool in pools {
            let Some(mut buffers) = pool.buffers.try_lock() else {
                continue;
            };
            while freed < want {
                let Some(buffer) = buffers.pop() else { break };
                self.remove_idle(buffer.len());
                self.idle_evictions.fetch_add(1, Ordering::Relaxed);
                freed += buffer.len();
            }
            if freed >= want {
                break;
            }
        }
        freed
    }
    /// Takes `len` idle bytes out of the shared allowance (buffer leaves a pool).
    fn remove_idle(&self, len: usize) {
        self.bytes.fetch_sub(len as u64, Ordering::AcqRel);
        self.idle_buffers.fetch_sub(1, Ordering::Relaxed);
    }
    /// Adds `len` idle bytes if reserved plus idle bytes still fit `max_bytes`.
    fn try_add_idle(&self, len: usize) -> bool {
        let max = self.config.max_bytes as u64;
        let mut word = self.bytes.load(Ordering::Acquire);
        for _ in 0..CAS_ATTEMPTS {
            let used = (word >> 32) + (word & IDLE_MASK);
            if len as u64 > max.saturating_sub(used) {
                return false;
            }
            match self.bytes.compare_exchange_weak(
                word,
                word + len as u64,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.idle_buffers.fetch_add(1, Ordering::Relaxed);
                    return true;
                }
                Err(seen) => word = seen,
            }
        }
        false
    }
    /// Diagnostic mapped/positional paths use the same evidence names, but do
    /// not claim kernel asynchronous scheduling.
    pub(crate) fn record_sync_read(&self, bytes: usize, success: bool) {
        bump(&self.node_counters.wal_reads, 1);
        bump(&self.node_counters.wal_read_bytes, bytes as u64);
        bump(
            if success {
                &self.node_counters.completed_reads
            } else {
                &self.node_counters.errors
            },
            1,
        );
    }
    /// Node-wide per-device and total in-flight read credits for `io_count`
    /// kernel reads; all-or-nothing.
    fn reserve_device_reads(&self, devices: &[(u64, usize)], io_count: usize) -> bool {
        if !self.devices.try_reserve(
            devices,
            self.config.max_device_reads,
            &self.peak_device_reads,
        ) {
            return false;
        }
        let reads = self.inflight_reads.fetch_add(io_count, Ordering::AcqRel) + io_count;
        self.peak_inflight_reads.fetch_max(reads, Ordering::Relaxed);
        true
    }
    fn release_reads(&self, device: u64, count: usize) {
        self.devices.release(device, count);
        self.inflight_reads.fetch_sub(count, Ordering::AcqRel);
    }
}

/// One session's reusable read buffers and event counters (share nothing: only
/// this session's executor and its outstanding response owners touch them). The
/// buffer lock is therefore uncontended; byte accounting stays node-wide so the
/// shared allowance still bounds idle plus reserved memory.
/// Lock order: the buffer lock is a leaf; eviction only `try_lock`s it.
pub(crate) struct SessionPool {
    admission: Arc<ReadAdmission>,
    counters: Counters,
    buffers: Mutex<Vec<Box<[MaybeUninit<u8>]>>>,
}
impl SessionPool {
    fn take_buffer(&self, len: usize) -> Box<[MaybeUninit<u8>]> {
        // Exact-size classes avoid padding the wire client's retained-byte
        // reservation. The scan is bounded by the pool capacity (<= 128).
        let reused = {
            let mut buffers = self.buffers.lock();
            buffers
                .iter()
                .position(|buffer| buffer.len() == len)
                .map(|index| buffers.swap_remove(index))
        };
        if let Some(buffer) = reused {
            self.admission.remove_idle(len);
            bump(&self.counters.buffer_reuses, 1);
            return buffer;
        }
        bump(&self.counters.buffer_allocations, 1);
        Box::<[u8]>::new_uninit_slice(len)
    }
    fn return_buffer(&self, buffer: Box<[MaybeUninit<u8>]>) {
        let mut buffers = self.buffers.lock();
        if buffers.len() < self.admission.pool_capacity()
            && self.admission.try_add_idle(buffer.len())
        {
            buffers.push(buffer);
        }
    }
}
impl Drop for SessionPool {
    fn drop(&mut self) {
        self.counters.fold_into(&self.admission.retired);
        for buffer in self.buffers.get_mut().drain(..) {
            self.admission.remove_idle(buffer.len());
        }
    }
}

/// Transfers request/payload admission through completed response ownership.
/// Transport retains this guard until its TLS/provider send is retired.
pub(crate) struct ReadLease {
    admission: Arc<ReadAdmission>,
    charge: usize,
    prepared: Option<PreparedRead>,
}
impl Drop for ReadLease {
    fn drop(&mut self) {
        let prepared = self.prepared.take();
        let cancelled = prepared.is_some();
        if let Some(mut prepared) = prepared {
            // Retire payload owners before releasing their lease charge. Bytes
            // owners may recycle through their pool, so none are dropped under
            // a lock.
            drop(std::mem::take(&mut prepared.records));
            for buffer in prepared
                .resident_buffers
                .iter_mut()
                .filter_map(Option::take)
            {
                prepared.pool.return_buffer(buffer.bytes);
            }
            drop(prepared);
        }
        // Release: the freed room must be visible before another reservation
        // can observe it.
        self.admission
            .bytes
            .fetch_sub((self.charge as u64) << 32, Ordering::Release);
        self.admission
            .inflight_requests
            .fetch_sub(1, Ordering::Release);
        if cancelled {
            bump(&self.admission.cancellations, 1);
        }
    }
}

pub(crate) type PlannedRead = (ValueRecordReference, RecordReadPlan);
fn read_plan_signature(plans: &[PlannedRead]) -> Vec<(ValueRecordReference, u64, usize, u64)> {
    plans
        .iter()
        .map(|(reference, plan)| (*reference, plan.member.offset, plan.member.len, plan.device))
        .collect()
}

pub(crate) enum SubmitOutcome {
    Ready {
        records: Vec<Bytes>,
        lease: ReadLease,
    },
    Pending,
    Deferred(Vec<PlannedRead>),
    Full(Vec<PlannedRead>),
}
pub(crate) struct ReadCompletion {
    pub(crate) request_id: u64,
    pub(crate) result: Result<Vec<Bytes>, StorageError>,
    pub(crate) lease: Option<ReadLease>,
    pub(crate) profile: Option<super::WalReadProfile>,
}
struct PendingRequest {
    records: Vec<Option<Bytes>>,
    remaining: usize,
    lease: Option<ReadLease>,
    devices: Vec<(u64, usize)>,
    error: Option<StorageError>,
    cancelled: bool,
    profile: Option<super::WalReadProfile>,
}
struct ResidentPrefix {
    bytes: Box<[MaybeUninit<u8>]>,
    filled: usize,
}

/// Resident progress held across sweep continuations and async admission retries.
/// The caller's reply lease charges this state while it remains unsubmitted.
struct PreparedRead {
    signature: Vec<(ValueRecordReference, u64, usize, u64)>,
    records: Vec<Option<Bytes>>,
    resident_buffers: Vec<Option<ResidentPrefix>>,
    wal_bytes: u64,
    resident_fallbacks: usize,
    resident_unsupported: usize,
    resident_partial: usize,
    warm_sweeps: usize,
    /// Pool that owns the buffers in `resident_buffers`.
    pool: Arc<SessionPool>,
}

/// The final Bytes owner returns the allocation only after response clones retire.
struct ReadBuffer {
    buffer: Option<Box<[MaybeUninit<u8>]>>,
    pool: Arc<SessionPool>,
    // Every response owner keeps its exact authority and native file pins.
    _first_plan: RecordReadPlan,
    _coalesced_rest: Vec<(usize, RecordReadPlan)>,
}
impl AsRef<[u8]> for ReadBuffer {
    fn as_ref(&self) -> &[u8] {
        let buffer = self.buffer.as_ref().expect("owned read buffer");
        // SAFETY: an exact NOWAIT read or a CQE for the full remaining suffix
        // initialized every byte. This owner retains the allocation, and no
        // kernel read or pool reuse can write to it while this borrow exists.
        unsafe { std::slice::from_raw_parts(buffer.as_ptr().cast(), buffer.len()) }
    }
}
impl minibytes::BytesOwner for ReadBuffer {}
impl Drop for ReadBuffer {
    fn drop(&mut self) {
        self.pool
            .return_buffer(self.buffer.take().expect("owned read buffer"));
    }
}
struct Operation {
    request_id: u64,
    index: usize,
    plan: RecordReadPlan,
    /// Additional independently authorized members sharing this buffer.
    siblings: Vec<(usize, RecordReadPlan)>,
    buffer: Box<[MaybeUninit<u8>]>,
    /// Initialized resident prefix; no Bytes is published until the suffix completes.
    filled: usize,
    queued_at: Option<Instant>,
}

// Return the exclusive end of an original-order, gap-free run. Arc identity
// pins both the native file and the epoch/log authority; equal offsets or
// device numbers alone are never sufficient. A partial resident prefix keeps
// its existing suffix-only operation rather than being read again.
fn read_run_end(
    plans: &[PlannedRead],
    records: &[Option<Bytes>],
    resident: &[Option<ResidentPrefix>],
    start: usize,
) -> (usize, usize) {
    let first = &plans[start].1;
    let mut end = start + 1;
    let mut range_end = match first.member.offset.checked_add(first.member.len as u64) {
        Some(end) => end,
        None => return (end, first.member.len),
    };
    if resident
        .get(start)
        .and_then(Option::as_ref)
        .is_some_and(|p| p.filled != 0)
        || first.member.len > COALESCED_MAX_BYTES
    {
        return (end, first.member.len);
    }
    while end < plans.len() && end - start < COALESCED_MAX_MEMBERS {
        let next = &plans[end].1;
        if records[end].is_some()
            || resident
                .get(end)
                .and_then(Option::as_ref)
                .is_some_and(|p| p.filled != 0)
            || !Arc::ptr_eq(&first._log, &next._log)
            || !Arc::ptr_eq(&first.member.file, &next.member.file)
            || first.device != next.device
            || next.member.offset < first.member.offset
            || next.member.offset > range_end
        {
            break;
        }
        let Some(next_end) = next.member.offset.checked_add(next.member.len as u64) else {
            break;
        };
        let combined_end = range_end.max(next_end);
        if combined_end - first.member.offset > COALESCED_MAX_BYTES as u64 {
            break;
        }
        range_end = combined_end;
        end += 1;
    }
    (end, (range_end - first.member.offset) as usize)
}

#[cfg(target_os = "linux")]
impl Operation {
    fn entry(&mut self, token: u64) -> squeue::Entry {
        let read = opcode::Read::new(
            types::Fd(self.plan.member.file.as_raw_fd()),
            self.buffer.as_mut_ptr().wrapping_add(self.filled).cast(),
            (self.buffer.len() - self.filled) as u32,
        )
        .offset(self.plan.member.offset + self.filled as u64);
        read.build().flags(squeue::Flags::ASYNC).user_data(token)
    }
}
#[cfg(target_os = "linux")]
fn resident_preadv2(
    plan: &RecordReadPlan,
    buffer: &mut [MaybeUninit<u8>],
    attempted: &mut bool,
) -> io::Result<usize> {
    let offset = libc::off_t::try_from(plan.member.offset)
        .map_err(|_| io::Error::from_raw_os_error(libc::EOVERFLOW))?;
    let iovec = libc::iovec {
        iov_base: buffer.as_mut_ptr().cast(),
        iov_len: buffer.len(),
    };
    // SAFETY: the iovec points to this exclusively borrowed allocation for the
    // duration of preadv2; the kernel initializes at most iov_len bytes. The plan
    // pins the file and its authorized range. A short result stays private until
    // an async read initializes the remaining suffix.
    *attempted = true;
    let result = unsafe {
        libc::preadv2(
            plan.member.file.as_raw_fd(),
            &iovec,
            1,
            offset,
            libc::RWF_NOWAIT,
        )
    };
    if result < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(result as usize)
    }
}

#[cfg(all(test, target_os = "linux"))]
fn resident_test_preadv2(
    result: ResidentTestResult,
    plan: &RecordReadPlan,
    buffer: &mut [MaybeUninit<u8>],
    attempted: &mut bool,
) -> io::Result<usize> {
    // Model a NOWAIT call result so attempt accounting follows the production path.
    *attempted = true;
    match result {
        ResidentTestResult::Unsupported => Err(io::Error::from_raw_os_error(libc::EOPNOTSUPP)),
        ResidentTestResult::WouldBlock => Err(io::Error::from_raw_os_error(libc::EAGAIN)),
        ResidentTestResult::Complete => {
            let mut bytes = vec![0; buffer.len()];
            plan.member
                .file
                .read_exact_at(&mut bytes, plan.member.offset)?;
            for (slot, byte) in buffer.iter_mut().zip(bytes) {
                slot.write(byte);
            }
            Ok(buffer.len())
        }
        ResidentTestResult::Partial(read) if read > 0 && read < buffer.len() => {
            let mut prefix = vec![0; read];
            plan.member
                .file
                .read_exact_at(&mut prefix, plan.member.offset)?;
            for (slot, byte) in buffer.iter_mut().zip(prefix) {
                slot.write(byte);
            }
            Ok(read)
        }
        ResidentTestResult::Partial(_) => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "test partial NOWAIT result must be a nonempty prefix",
        )),
    }
}

#[cfg(not(target_os = "linux"))]
fn resident_preadv2(
    _plan: &RecordReadPlan,
    _buffer: &mut [MaybeUninit<u8>],
    _attempted: &mut bool,
) -> io::Result<usize> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "resident NOWAIT reads require Linux",
    ))
}

const REQUEST_METADATA_BYTES: usize =
    4 * (size_of::<PendingRequest>() + size_of::<PreparedRead>() + 2 * size_of::<u64>());
const MEMBER_METADATA_BYTES: usize = 4
    * (size_of::<Operation>()
        + size_of::<Option<Bytes>>()
        + size_of::<(ValueRecordReference, u64, usize, u64)>()
        + 4 * size_of::<u64>())
    + size_of::<ReadBuffer>()
    + 2 * size_of::<usize>();
// Wire clients reserve 4096 + sum(member.len + 4096) when chunking. Keep that
// conservative contract valid if internal owner layouts change on any target.
const _: () = assert!(REQUEST_METADATA_BYTES <= 4096 && MEMBER_METADATA_BYTES <= 4096);

#[cfg(all(test, target_os = "linux"))]
enum ResidentTestResult {
    Unsupported,
    WouldBlock,
    Complete,
    Partial(usize),
}

pub(crate) struct ReadExecutor {
    #[cfg(target_os = "linux")]
    ring: IoUring,
    #[cfg(target_os = "linux")]
    entries: Vec<squeue::Entry>,
    #[cfg(all(test, target_os = "linux"))]
    resident_test_results: std::collections::VecDeque<ResidentTestResult>,
    #[cfg(all(test, target_os = "linux"))]
    submit_errors: std::collections::VecDeque<i32>,
    admission: Arc<ReadAdmission>,
    /// This session's reusable buffers and counters (share nothing).
    pool: Arc<SessionPool>,
    config: ReadExecutorConfig,
    requests: HashMap<u64, PendingRequest>,
    operations: HashMap<u64, Operation>,
    next_token: u64,
    resident_calls_remaining: usize,
    resident_bytes_remaining: usize,
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
                #[cfg(all(test, target_os = "linux"))]
                resident_test_results: std::collections::VecDeque::new(),
                #[cfg(all(test, target_os = "linux"))]
                submit_errors: std::collections::VecDeque::new(),
                pool: admission.session_pool(),
                admission,
                config,
                requests: HashMap::with_capacity(config.max_requests),
                operations: HashMap::with_capacity(config.max_device_reads),
                next_token: 1,
                resident_calls_remaining: RESIDENT_NOWAIT_MAX_CALLS,
                resident_bytes_remaining: RESIDENT_NOWAIT_MAX_BYTES,
                closed: false,
                failed: false,
            })
        }
    }

    /// Reset the bounded synchronous NOWAIT allowance for one owner-loop sweep.
    /// Outstanding async operations are unaffected.
    pub(crate) fn begin_sweep(&mut self) {
        self.resident_calls_remaining = RESIDENT_NOWAIT_MAX_CALLS;
        self.resident_bytes_remaining = RESIDENT_NOWAIT_MAX_BYTES;
    }

    #[cfg(test)]
    pub(crate) fn submit(
        &mut self,
        request_id: u64,
        members: Vec<(Arc<PartitionLog>, ValueRecordReference)>,
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
        self.submit_planned(request_id, plans, &mut None)
    }

    #[cfg(test)]
    pub(crate) fn submit_planned(
        &mut self,
        request_id: u64,
        plans: Vec<PlannedRead>,
        lease: &mut Option<ReadLease>,
    ) -> Result<SubmitOutcome, StorageError> {
        self.submit_planned_profiled(request_id, plans, lease, None)
    }

    /// Plans already own validated immutable authority and native file pins.
    /// No cold metadata lookup runs here. Full/errors retain the caller's reply
    /// reservation; only Ready or an admitted kernel request takes its lease.
    pub(crate) fn submit_planned_profiled(
        &mut self,
        request_id: u64,
        plans: Vec<PlannedRead>,
        lease: &mut Option<ReadLease>,
        mut profile: Option<&mut super::WalReadProfile>,
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
        // Reserve reply ownership before allocating retained read buffers.
        if lease.is_none() {
            *lease = self.admission.reserve_reply(charge)?;
        }
        let Some(reservation) = lease.as_mut() else {
            return Ok(SubmitOutcome::Full(plans));
        };
        if !Arc::ptr_eq(&reservation.admission, &self.admission) || reservation.charge < charge {
            return Err(StorageError::Invalid(
                "read plan exceeds its reply reservation",
            ));
        }
        let prepared = reservation.prepared.take();
        let resuming_prepared = prepared.is_some();
        let mut retained_signature = None;
        let (
            mut records,
            mut resident_buffers,
            wal_bytes,
            mut resident_fallbacks,
            mut resident_unsupported,
            mut resident_partial,
            warm_sweeps,
        ) = if let Some(prepared) = prepared {
            if prepared.signature.len() != plans.len()
                || prepared
                    .signature
                    .iter()
                    .zip(&plans)
                    .any(|(saved, (reference, plan))| {
                        *saved != (*reference, plan.member.offset, plan.member.len, plan.device)
                    })
            {
                reservation.prepared = Some(prepared);
                return Err(StorageError::Invalid(
                    "retried read does not match prepared resident progress",
                ));
            }
            retained_signature = Some(prepared.signature);
            (
                prepared.records,
                prepared.resident_buffers,
                prepared.wal_bytes,
                prepared.resident_fallbacks,
                prepared.resident_unsupported,
                prepared.resident_partial,
                prepared.warm_sweeps,
            )
        } else {
            (
                (0..plans.len()).map(|_| None).collect(),
                if self.config.resident_first {
                    (0..plans.len()).map(|_| None).collect()
                } else {
                    Vec::new()
                },
                plans.iter().map(|(_, plan)| plan.member.len as u64).sum(),
                0,
                0,
                0,
                0,
            )
        };

        // The NOWAIT budget applies only to plans already pinned and authorized
        // by the caller. Buffers stay private until fully initialized.
        let mut resident_attempts = 0usize;
        let mut resident_successes = 0usize;
        let mut resident_deferred = 0usize;
        if self.config.resident_first && (!resuming_prepared || warm_sweeps > 0) {
            for (index, (_, plan)) in plans.iter().enumerate() {
                if records[index].is_some() || resident_buffers[index].is_some() {
                    continue;
                }
                let len = plan.member.len;
                if len > RESIDENT_NOWAIT_MAX_MEMBER_BYTES
                    || self.resident_calls_remaining == 0
                    || self.resident_bytes_remaining < len
                {
                    resident_deferred += 1;
                    // Count only members ultimately sent to async I/O, not
                    // the same member on each continuation.
                    continue;
                }
                let mut buffer = self.pool.take_buffer(len);

                let mut syscall_attempted = false;
                let nowait_started = profile.as_ref().map(|_| Instant::now());
                let result = {
                    #[cfg(all(test, target_os = "linux"))]
                    {
                        if let Some(injected) = self.resident_test_results.pop_front() {
                            resident_test_preadv2(
                                injected,
                                plan,
                                &mut buffer,
                                &mut syscall_attempted,
                            )
                        } else {
                            resident_preadv2(plan, &mut buffer, &mut syscall_attempted)
                        }
                    }
                    #[cfg(not(all(test, target_os = "linux")))]
                    {
                        resident_preadv2(plan, &mut buffer, &mut syscall_attempted)
                    }
                };
                if syscall_attempted
                    && let (Some(profile), Some(started)) = (profile.as_deref_mut(), nowait_started)
                {
                    let ns = super::elapsed_ns(started);
                    profile.nowait_ns += ns;
                    profile.nowait_count += 1;
                    profile.nowait_max_ns = profile.nowait_max_ns.max(ns);
                    profile.nowait_errors += u64::from(result.is_err());
                    profile.nowait_partial += u64::from(matches!(&result, Ok(n) if *n < len));
                }
                if syscall_attempted {
                    resident_attempts += 1;
                    self.resident_calls_remaining -= 1;
                    self.resident_bytes_remaining -= len;
                }
                match result {
                    Ok(read) if read == len => {
                        resident_successes += 1;
                        let bytes = Bytes::from_owner(ReadBuffer {
                            buffer: Some(buffer),
                            pool: self.pool.clone(),
                            _first_plan: plan.clone(),
                            _coalesced_rest: Vec::new(),
                        });
                        records[index] = Some(bytes);
                        bump(&self.pool.counters.completed_reads, 1);
                    }
                    Ok(read) => {
                        resident_fallbacks += 1;
                        resident_partial += usize::from(read > 0);
                        resident_buffers[index] = Some(ResidentPrefix {
                            bytes: buffer,
                            filled: read,
                        });
                    }
                    Err(error)
                        if matches!(
                            error.raw_os_error(),
                            Some(libc::EAGAIN) | Some(libc::EOPNOTSUPP)
                        ) =>
                    {
                        resident_fallbacks += 1;
                        resident_unsupported +=
                            usize::from(error.raw_os_error() == Some(libc::EOPNOTSUPP));
                        resident_buffers[index] = Some(ResidentPrefix {
                            bytes: buffer,
                            filled: 0,
                        });
                    }
                    Err(error) => {
                        self.pool.return_buffer(buffer);
                        let counters = &self.pool.counters;
                        bump(&counters.resident_read_attempts, resident_attempts as u64);
                        bump(&counters.resident_read_successes, resident_successes as u64);
                        bump(&counters.resident_read_deferred, resident_deferred as u64);
                        bump(&counters.errors, 1);
                        return Err(error.into());
                    }
                }
            }
        }
        let can_continue = resident_deferred > 0
            && resident_fallbacks == 0
            && (resident_successes > 0 || warm_sweeps > 0)
            && warm_sweeps + 1 < RESIDENT_NOWAIT_MAX_SWEEPS
            && plans.iter().enumerate().all(|(index, (_, plan))| {
                records[index].is_some() || plan.member.len <= RESIDENT_NOWAIT_MAX_MEMBER_BYTES
            });
        if can_continue {
            let counters = &self.pool.counters;
            bump(&counters.resident_read_attempts, resident_attempts as u64);
            bump(&counters.resident_read_successes, resident_successes as u64);
            reservation.prepared = Some(PreparedRead {
                signature: retained_signature.unwrap_or_else(|| read_plan_signature(&plans)),
                records,
                resident_buffers,
                wal_bytes,
                resident_fallbacks,
                resident_unsupported,
                resident_partial,
                warm_sweeps: warm_sweeps + 1,
                pool: self.pool.clone(),
            });
            return Ok(SubmitOutcome::Deferred(plans));
        }
        if let Some(profile) = profile.as_deref_mut() {
            profile.nowait_deferred += resident_deferred as u64;
        }
        let misses = records.iter().filter(|record| record.is_none()).count();
        if misses == 0 {
            let counters = &self.pool.counters;
            bump(&counters.resident_read_attempts, resident_attempts as u64);
            bump(&counters.resident_read_successes, resident_successes as u64);
            bump(&counters.resident_read_deferred, resident_deferred as u64);
            bump(&counters.wal_reads, plans.len() as u64);
            bump(&counters.wal_read_bytes, wal_bytes);
            return Ok(SubmitOutcome::Ready {
                records: records.into_iter().map(Option::unwrap).collect(),
                lease: lease.take().expect("reserved read reply"),
            });
        }
        let mut devices = Vec::<(u64, usize)>::new();
        let mut io_count = 0;
        let mut index = 0;
        while index < plans.len() {
            if records[index].is_some() {
                index += 1;
                continue;
            }
            let plan = &plans[index].1;
            let (end, _) = read_run_end(&plans, &records, &resident_buffers, index);
            io_count += 1;
            if let Some((_, count)) = devices.iter_mut().find(|(id, _)| *id == plan.device) {
                *count += 1;
            } else {
                devices.push((plan.device, 1));
            }
            index = end;
        }
        let counters = &self.pool.counters;
        bump(&counters.resident_read_attempts, resident_attempts as u64);
        bump(&counters.resident_read_successes, resident_successes as u64);
        bump(&counters.resident_read_deferred, resident_deferred as u64);
        if devices
            .iter()
            .any(|(_, count)| *count > self.config.max_device_reads)
            || io_count > self.config.max_device_reads
        {
            return Err(StorageError::Capacity);
        }
        let mut full = self.requests.len() >= self.config.max_requests
            || self.operations.len() + io_count > self.config.max_device_reads;
        #[cfg(target_os = "linux")]
        if !full {
            let sq = self.ring.submission();
            full = sq.capacity() - sq.len() < io_count;
        }
        if !full {
            self.next_token
                .checked_add(io_count as u64)
                .ok_or(StorageError::Capacity)?;
            // Node-wide per-device credits come last: all-or-nothing, so a
            // failure leaves no credit taken and the request stays prepared.
            full = !self.admission.reserve_device_reads(&devices, io_count);
        }
        if full {
            reservation.prepared = Some(PreparedRead {
                signature: retained_signature.unwrap_or_else(|| read_plan_signature(&plans)),
                records,
                resident_buffers,
                wal_bytes,
                resident_fallbacks,
                resident_unsupported,
                resident_partial,
                warm_sweeps: 0,
                pool: self.pool.clone(),
            });
            return Ok(SubmitOutcome::Full(plans));
        }
        self.requests.insert(
            request_id,
            PendingRequest {
                records,
                remaining: io_count,
                lease: lease.take(),
                devices,
                error: None,
                cancelled: false,
                profile: profile.as_ref().map(|_| super::WalReadProfile::default()),
            },
        );
        #[cfg(target_os = "linux")]
        self.entries.clear();
        let mut plans = plans.into_iter();
        let mut index = 0;
        while !plans.as_slice().is_empty() {
            if self.requests[&request_id].records[index].is_some() {
                plans.next();
                index += 1;
                continue;
            }
            let (run_len, len) = read_run_end(
                plans.as_slice(),
                &self.requests[&request_id].records[index..],
                resident_buffers.get(index..).unwrap_or(&[]),
                0,
            );
            let (_, plan) = plans.next().expect("nonempty planned run");
            let token = self.next_token;
            self.next_token += 1;
            let mut siblings = Vec::new();
            let (buffer, filled) = if run_len == 1 {
                let prefix = resident_buffers
                    .get_mut(index)
                    .and_then(Option::take)
                    .unwrap_or_else(|| ResidentPrefix {
                        bytes: self.pool.take_buffer(len),
                        filled: 0,
                    });
                (prefix.bytes, prefix.filled)
            } else {
                // Zero-byte NOWAIT fallbacks have no useful prefix. Retire
                // their allocations before allocating the gap-free union.
                for slot in index..index + run_len {
                    if let Some(prefix) = resident_buffers.get_mut(slot).and_then(Option::take) {
                        self.pool.return_buffer(prefix.bytes);
                    }
                }
                siblings.reserve_exact(run_len - 1);
                for slot in index + 1..index + run_len {
                    let (_, sibling) = plans.next().expect("complete planned run");
                    siblings.push((slot, sibling));
                }
                (self.pool.take_buffer(len), 0)
            };
            let mut op = Operation {
                request_id,
                index,
                buffer,
                plan,
                siblings,
                filled,
                queued_at: profile.as_ref().map(|_| Instant::now()),
            };
            #[cfg(target_os = "linux")]
            self.entries.push(op.entry(token));
            self.operations.insert(token, op);
            index += run_len;
        }
        #[cfg(target_os = "linux")]
        {
            // SAFETY: Operation owns stable heap buffers and pins each file and
            // PartitionLog until its CQE. No buffer is read or reused before
            // completion. Shutdown drains the ring or Drop aborts before live
            // kernel pointers can be freed.
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
        let counters = &self.pool.counters;
        bump(
            &counters.wal_reads,
            self.requests[&request_id].records.len() as u64,
        );
        bump(&counters.wal_read_bytes, wal_bytes);
        bump(&counters.resident_read_fallbacks, resident_fallbacks as u64);
        bump(
            &counters.resident_read_unsupported,
            resident_unsupported as u64,
        );
        bump(&counters.resident_read_partial, resident_partial as u64);
        bump(&counters.async_read_submissions, io_count as u64);
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
                    bump(&self.pool.counters.errors, 1);
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
            let Some(op) = self.operations.remove(&token) else {
                self.failed = true;
                return Err(StorageError::Invalid("unknown read completion token"));
            };
            let result = cqe.result();
            let device = op.plan.device;
            let request = self
                .requests
                .get_mut(&op.request_id)
                .expect("owned read request");
            if let (Some(profile), Some(started)) = (&mut request.profile, op.queued_at) {
                let ns = super::elapsed_ns(started);
                profile.async_ns += ns;
                profile.async_count += 1;
                profile.async_max_ns = profile.async_max_ns.max(ns);
                profile.async_errors +=
                    u64::from(result < 0 || result as usize != op.buffer.len() - op.filled);
                profile.async_partial +=
                    u64::from(result >= 0 && result as usize != op.buffer.len() - op.filled);
            }
            if result < 0 || result as usize != op.buffer.len() - op.filled {
                bump(&self.pool.counters.errors, 1);
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
                self.pool.return_buffer(op.buffer);
            } else {
                // The CQE retires kernel ownership, but all Bytes clones must
                // retire too before this allocation can back another SQE.
                let first_len = op.plan.member.len;
                let first_offset = op.plan.member.offset;
                let scatter = op
                    .siblings
                    .iter()
                    .map(|(index, plan)| {
                        let start = (plan.member.offset - first_offset) as usize;
                        (*index, start, start + plan.member.len)
                    })
                    .collect::<smallvec::SmallVec<[_; COALESCED_MAX_MEMBERS]>>();
                let member_count = 1 + scatter.len();
                let coalesced = !op.siblings.is_empty();
                let bytes = Bytes::from_owner(ReadBuffer {
                    buffer: Some(op.buffer),
                    pool: self.pool.clone(),
                    _first_plan: op.plan,
                    _coalesced_rest: op.siblings,
                });
                bump(&self.pool.counters.completed_reads, member_count as u64);
                if coalesced {
                    request.records[op.index] = Some(bytes.slice(..first_len));
                    for (index, start, end) in scatter {
                        request.records[index] = Some(bytes.slice(start..end));
                    }
                } else {
                    request.records[op.index] = Some(bytes);
                }
            }
            request.remaining -= 1;
            self.admission.release_reads(device, 1);
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
                        profile: request.profile,
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
            bump(&self.admission.cancellations, 1);
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
