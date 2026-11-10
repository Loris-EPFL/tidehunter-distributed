//! Sharded lock wrappers with per-shard wait and hold accounting, plus the
//! deterministic routing hash shared by the sharded index and read cache.
//!
//! Tidehunter paper §4.1 protects index rows with sharded mutexes so that
//! different key ranges never synchronize. These wrappers keep that model and
//! make every shard's load visible. Accounting cost is bounded:
//!
//! * An uncontended acquisition adds one relaxed `fetch_add` on a counter that
//!   shares the lock's cache line, nothing else.
//! * Wait time is measured only on the slow path (`try_*` failed).
//! * Hold time is sampled: every write/exclusive hold, one in
//!   [`HOLD_SAMPLE_PERIOD`] read holds.
//!
//! No wrapper here ever holds two locks. Callers that need several shards lock
//! them in ascending shard index and document why.

use parking_lot::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// One in this many shared acquisitions measures its hold time. Power of two.
pub(crate) const HOLD_SAMPLE_PERIOD: u64 = 64;

/// Cumulative lock activity of one shard. `hold_ns` covers only the
/// `hold_samples` sampled holds, not every acquisition.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ShardLockStats {
    pub acquisitions: u64,
    pub contended: u64,
    pub wait_ns: u64,
    pub hold_samples: u64,
    pub hold_ns: u64,
}

#[derive(Default)]
struct LockCounters {
    acquisitions: AtomicU64,
    contended: AtomicU64,
    wait_ns: AtomicU64,
    hold_samples: AtomicU64,
    hold_ns: AtomicU64,
}

fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

impl LockCounters {
    /// Returns the acquisition ordinal. Relaxed: these are independent
    /// statistics; no data is published through them.
    fn enter(&self) -> u64 {
        self.acquisitions.fetch_add(1, Ordering::Relaxed)
    }

    fn waited(&self, since: Instant) {
        self.contended.fetch_add(1, Ordering::Relaxed);
        self.wait_ns
            .fetch_add(nanos(since.elapsed()), Ordering::Relaxed);
    }

    fn held(&self, since: Instant) {
        self.hold_samples.fetch_add(1, Ordering::Relaxed);
        self.hold_ns
            .fetch_add(nanos(since.elapsed()), Ordering::Relaxed);
    }

    fn snapshot(&self) -> ShardLockStats {
        ShardLockStats {
            acquisitions: self.acquisitions.load(Ordering::Relaxed),
            contended: self.contended.load(Ordering::Relaxed),
            wait_ns: self.wait_ns.load(Ordering::Relaxed),
            hold_samples: self.hold_samples.load(Ordering::Relaxed),
            hold_ns: self.hold_ns.load(Ordering::Relaxed),
        }
    }
}

fn sampled(ordinal: u64) -> Option<Instant> {
    ordinal
        .is_multiple_of(HOLD_SAMPLE_PERIOD)
        .then(Instant::now)
}

/// Reader/writer lock of one shard. Aligned to a cache line so neighbouring
/// shards in a slice never share one.
#[repr(align(64))]
pub(crate) struct ShardRwLock<T> {
    lock: RwLock<T>,
    counters: LockCounters,
}

pub(crate) struct ShardReadGuard<'a, T> {
    guard: RwLockReadGuard<'a, T>,
    counters: &'a LockCounters,
    held_since: Option<Instant>,
}

pub(crate) struct ShardWriteGuard<'a, T> {
    guard: RwLockWriteGuard<'a, T>,
    counters: &'a LockCounters,
    held_since: Instant,
}

impl<T> ShardRwLock<T> {
    pub(crate) fn new(value: T) -> Self {
        Self {
            lock: RwLock::new(value),
            counters: LockCounters::default(),
        }
    }

    pub(crate) fn read(&self) -> ShardReadGuard<'_, T> {
        let ordinal = self.counters.enter();
        let guard = self.lock.try_read().unwrap_or_else(|| {
            let waiting = Instant::now();
            let guard = self.lock.read();
            self.counters.waited(waiting);
            guard
        });
        ShardReadGuard {
            guard,
            counters: &self.counters,
            held_since: sampled(ordinal),
        }
    }

    pub(crate) fn try_read(&self) -> Option<ShardReadGuard<'_, T>> {
        let ordinal = self.counters.enter();
        let guard = self.lock.try_read()?;
        Some(ShardReadGuard {
            guard,
            counters: &self.counters,
            held_since: sampled(ordinal),
        })
    }

    pub(crate) fn write(&self) -> ShardWriteGuard<'_, T> {
        self.counters.enter();
        let guard = self.lock.try_write().unwrap_or_else(|| {
            let waiting = Instant::now();
            let guard = self.lock.write();
            self.counters.waited(waiting);
            guard
        });
        ShardWriteGuard {
            guard,
            counters: &self.counters,
            held_since: Instant::now(),
        }
    }

    /// Exclusive access without locking, for single-owner construction.
    pub(crate) fn get_mut(&mut self) -> &mut T {
        self.lock.get_mut()
    }

    pub(crate) fn stats(&self) -> ShardLockStats {
        self.counters.snapshot()
    }
}

impl<T> Deref for ShardReadGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.guard
    }
}

impl<T> Drop for ShardReadGuard<'_, T> {
    fn drop(&mut self) {
        if let Some(since) = self.held_since {
            self.counters.held(since);
        }
    }
}

impl<T> Deref for ShardWriteGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.guard
    }
}

impl<T> DerefMut for ShardWriteGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.guard
    }
}

impl<T> Drop for ShardWriteGuard<'_, T> {
    fn drop(&mut self) {
        self.counters.held(self.held_since);
    }
}

/// Mutex of one shard, with the same accounting as [`ShardRwLock`].
#[repr(align(64))]
pub(crate) struct ShardMutex<T> {
    lock: Mutex<T>,
    counters: LockCounters,
}

pub(crate) struct ShardMutexGuard<'a, T> {
    guard: MutexGuard<'a, T>,
    counters: &'a LockCounters,
    held_since: Instant,
}

impl<T> ShardMutex<T> {
    pub(crate) fn new(value: T) -> Self {
        Self {
            lock: Mutex::new(value),
            counters: LockCounters::default(),
        }
    }

    pub(crate) fn lock(&self) -> ShardMutexGuard<'_, T> {
        self.counters.enter();
        let guard = self.lock.try_lock().unwrap_or_else(|| {
            let waiting = Instant::now();
            let guard = self.lock.lock();
            self.counters.waited(waiting);
            guard
        });
        ShardMutexGuard {
            guard,
            counters: &self.counters,
            held_since: Instant::now(),
        }
    }

    pub(crate) fn stats(&self) -> ShardLockStats {
        self.counters.snapshot()
    }
}

impl<T> Deref for ShardMutexGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.guard
    }
}

impl<T> DerefMut for ShardMutexGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.guard
    }
}

impl<T> Drop for ShardMutexGuard<'_, T> {
    fn drop(&mut self) {
        self.counters.held(self.held_since);
    }
}

/// Deterministic 64-bit routing hash: FNV-1a over explicit little-endian
/// bytes, then the murmur3 finalizer so every output bit depends on every
/// input bit and the low bits are usable as a power-of-two mask.
///
/// Never `RandomState`: routing must be reproducible across runs and nodes.
/// The shard map is in-memory only. No persisted format depends on this hash
/// or on the shard count.
pub(crate) struct StableHasher(u64);

impl StableHasher {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;

    pub(crate) fn new() -> Self {
        Self(Self::OFFSET)
    }

    pub(crate) fn bytes(mut self, bytes: &[u8]) -> Self {
        for &byte in bytes {
            self.0 = (self.0 ^ u64::from(byte)).wrapping_mul(Self::PRIME);
        }
        self
    }

    pub(crate) fn byte(self, byte: u8) -> Self {
        self.bytes(&[byte])
    }

    pub(crate) fn u64(self, value: u64) -> Self {
        self.bytes(&value.to_le_bytes())
    }

    pub(crate) fn finish(self) -> u64 {
        let mut hash = self.0;
        hash ^= hash >> 33;
        hash = hash.wrapping_mul(0xff51_afd7_ed55_8ccd);
        hash ^= hash >> 33;
        hash = hash.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
        hash ^ (hash >> 33)
    }
}

/// `Some(mask)` when `count` is a nonzero power of two.
pub(crate) fn shard_mask(count: usize) -> Option<usize> {
    count.is_power_of_two().then(|| count - 1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;

    #[test]
    fn routing_hash_is_deterministic_and_spreads_sequential_inputs() {
        let hash = |n: u64| StableHasher::new().byte(3).u64(n).finish();
        assert_eq!(hash(7), hash(7));
        assert_ne!(hash(0), hash(1));
        let mut buckets = [0usize; 16];
        for n in 0..4096 {
            buckets[(hash(n) & 15) as usize] += 1;
        }
        // Sequential cell ids must not pile on a few shards.
        assert!(buckets.iter().all(|&count| (192..=320).contains(&count)));
    }

    #[test]
    fn shard_mask_accepts_only_powers_of_two() {
        assert_eq!(shard_mask(1), Some(0));
        assert_eq!(shard_mask(64), Some(63));
        assert_eq!(shard_mask(0), None);
        assert_eq!(shard_mask(48), None);
    }

    #[test]
    fn uncontended_acquisitions_record_no_wait_and_contention_records_wait() {
        let lock = Arc::new(ShardRwLock::new(0u32));
        drop(lock.read());
        drop(lock.write());
        let stats = lock.stats();
        assert_eq!((stats.acquisitions, stats.contended), (2, 0));
        assert!(stats.hold_samples >= 1, "write holds are always sampled");

        let writer = lock.write();
        let released = Arc::new(AtomicBool::new(false));
        let waiter = {
            let lock = Arc::clone(&lock);
            let released = Arc::clone(&released);
            std::thread::spawn(move || {
                let _read = lock.read();
                assert!(released.load(Ordering::SeqCst));
            })
        };
        while lock.stats().acquisitions < 4 {
            std::thread::yield_now();
        }
        std::thread::sleep(Duration::from_millis(5));
        released.store(true, Ordering::SeqCst);
        drop(writer);
        waiter.join().unwrap();
        let stats = lock.stats();
        assert_eq!(stats.contended, 1);
        assert!(stats.wait_ns >= 1_000_000, "wait covers the blocked period");
    }
}
