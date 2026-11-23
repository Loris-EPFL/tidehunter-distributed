//! Mutex wrappers with per-shard wait and hold accounting, plus the
//! deterministic routing hash shared by the read cache shards.
//!
//! Tidehunter paper §4.1 protects index rows with sharded mutexes so that
//! different key ranges never synchronize. The read cache keeps that model and
//! these wrappers make every shard's load visible. Accounting cost is bounded:
//!
//! * An uncontended acquisition adds one relaxed `fetch_add` on a counter that
//!   shares the lock's cache line, nothing else.
//! * Wait time is measured only on the slow path (`try_lock` failed).
//! * Every hold is timed.
//!
//! No wrapper here ever holds two locks. Callers that need several shards lock
//! them in ascending shard index and document why.

use parking_lot::{Mutex, MutexGuard};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

#[cfg(test)]
mod tests;

/// Cumulative lock activity of one shard. `hold_ns` covers the `hold_samples`
/// timed holds.
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
    /// Test observable: set once a caller has taken its wait timestamp and is
    /// about to block, so a reader that observes it knows the wait is queued.
    #[cfg(test)]
    blocked: AtomicU64,
}

fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

impl LockCounters {
    /// Relaxed: these are independent statistics; no data is published
    /// through them.
    fn enter(&self) {
        self.acquisitions.fetch_add(1, Ordering::Relaxed);
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

/// Mutex of one shard. A cache line apart from its neighbours.
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
            // Release publishes the queued wait; a test reader pairs it with an
            // Acquire load, while the statistics below stay Relaxed because no
            // data is published through them.
            #[cfg(test)]
            self.counters.blocked.fetch_add(1, Ordering::Release);
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

    /// Number of acquisitions that entered the blocking path. The wait timer
    /// starts before this count moves, so a reader that observes it knows the
    /// acquisition is queued and the eventual `wait_ns` covers the interval.
    #[cfg(test)]
    fn blocked_waits(&self) -> u64 {
        self.counters.blocked.load(Ordering::Acquire)
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

    pub(crate) fn finish(self) -> u64 {
        let mut hash = self.0;
        hash ^= hash >> 33;
        hash = hash.wrapping_mul(0xff51_afd7_ed55_8ccd);
        hash ^= hash >> 33;
        hash = hash.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
        hash ^ (hash >> 33)
    }
}
