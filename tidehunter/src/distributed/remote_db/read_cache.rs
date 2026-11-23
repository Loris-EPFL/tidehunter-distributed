//! Bounded, derivative cache of verified immutable Value-WAL members.
//!
//! The native index is authoritative and yields the position of a read's newest
//! write. Here the cache key is that write's *logical identity* only: the batch
//! version and the member's operation index. The physical address is excluded on
//! purpose, because relocation re-points a value's address without changing the
//! version, and a re-pointed value must still hit its cached bytes. Key bytes are
//! not retained either: the index already has the key, so an entry holds the
//! decoded value bytes alone.
//!
//! Membership and integrity are settled before insertion: the read path verified
//! the coordinator's per-member CRC-32 and decoded the native entry with the
//! native helper. Reference digests and per-member identity checksums are not
//! part of the hybrid design (owner-accepted parity weakening).
//!
//! This cache is private to one open authority and starts empty after replay.
//! The native checkpoint latch and the index cut decide which position a read
//! resolves to; a cached identity cannot authorize an expired cut or make a
//! retired WAL file reachable. As in native `DecompressCache::get`, a cached good
//! copy is not evidence of current node health or absence of later disk
//! corruption, and must never be used as a durability acknowledgement.
//!
//! RocksDB's unique file identity plus offset cache keys inspire exact immutable
//! identity; its CLOCK cache and Tidehunter's native decompression cache inspire
//! second-chance eviction. A flat four-way table stores handles and identity
//! tags. Tag matches dereference one Entry; full equality still decides a hit.
//!
//! # Concurrency
//!
//! The cache is `N` independent shards (power of two, chosen once at
//! construction, never persisted). An identity routes to its shard by a stable
//! hash of the logical version ([`StableHasher`], never `RandomState`). A shard
//! owns its CLOCK table, admission history and byte budget (`budget / N`); no
//! operation touches two shards, so there is no lock order to respect. Each table
//! sits behind its own mutex whose critical section is one bounded probe or
//! insertion: no I/O or callback. Payload detachment stays outside the lock. A
//! vacant slot allocates one small Entry; replacement reuses that slot's Box.
//! Hits allocate nothing.
//!
//! Counters are per-shard relaxed atomics in their own cache line, outside the
//! mutex, and are summed on read. Statistics never lock. A disabled cache has
//! no table to protect: lookups and insertions only bump striped atomics and
//! take no lock at all.
//!
//! The largest resident charge is the shard budget minus fixed arrays. A sharded
//! cache keeps at least [`MIN_SHARD_BYTES`] per shard; smaller budgets use one
//! shard. A protected keyspace reserves one quarter of the resident allowance.
//!
//! # Retained memory
//!
//! Every fill copies the value into an exact-size allocation before it is
//! admitted (`detach`), whatever backs the caller's `Bytes`. A value that is a
//! slice of a pooled or aligned read block (live/sealed NVMe-oF reads, up to
//! several KiB for a 1 KiB value) therefore never pins that block, and the
//! charged payload equals the bytes the cache really retains. There is no
//! size threshold: the copy cost is one small memcpy per admitted fill.
//!
//! Every live entry pays value bytes, the existing nonempty byte-owner header,
//! and `size_of::<Entry>() + ENTRY_ALLOCATOR_BYTES`. The 8-byte Entry allowance
//! models allocator overhead; its exact size depends on the allocator.
//! This is an accounting model, not an RSS bound: allocator rounding of byte
//! owners, instrumentation, shard locks and caller-held clones remain outside it.
//! Per-slot fixed bytes are one optional Box handle, one u64 tag and one history
//! item (32 bytes here). For shard budget b, allocate
//! `4 * floor(b / (FIXED_SLOT_BYTES + MIN_ENTRY_CHARGE) / 4)` slots.
//! The minimum charge includes a conservative record-size estimate. Fixed
//! arrays use at most 8% of b. No payload floor is reserved per slot.
//! Disabled tiny caches allocate no slot arrays. CLOCK uses at most 64 steps.
//!
//! Sources:
//! <https://github.com/facebook/rocksdb/blob/main/cache/cache_key.h>
//! <https://github.com/facebook/rocksdb/wiki/Block-Cache>
//! Admission adapts CacheLib's bounded RejectFirstAP access history, but keeps
//! first-touch probationary entries for native-style read-after-write. A reused
//! entry beats a first-touch candidate; repeated misses can earn admission.
//! Second chance is applied to exact identities, not mutable key chains.
//! <https://github.com/facebook/CacheLib/blob/a24370092f0bd6fa7145b2f01c3e50e63edd6a6e/cachelib/allocator/NvmAdmissionPolicy.h#L175-L249>.
//! FASTER Appendix D and F2 §7 likewise use a bounded read-hot copy while the
//! log/index remains authoritative. Placement hashes the identity; full
//! immutable-identity equality still decides a hit, including when a physical
//! address differs or tags collide.
//! <https://doi.org/10.1145/3183713.3196898> (FASTER, Appendix D)
//! <https://arxiv.org/abs/2305.01516v3> (F2, §7)
//! <https://github.com/microsoft/garnet/blob/v1.0.82/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/ReadCache.cs>

use crate::distributed::shard_lock::{ShardLockStats, ShardMutex, StableHasher};
use crate::distributed::types::MutationVersion;
use minibytes::Bytes;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hash};
use std::mem::size_of;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

const WAYS: usize = 4;
// Bound insertion latency independently of configured capacity.
const MAX_EVICTION_STEPS: usize = 64;
// Account for Entry allocator overhead without claiming a portable chunk size.
// This is a model, not an allocator or RSS guarantee.
const ENTRY_ALLOCATOR_BYTES: usize = 8;
const ENTRY_BYTES: usize = size_of::<Entry>() + ENTRY_ALLOCATOR_BYTES;
const FIXED_SLOT_BYTES: usize =
    size_of::<Option<Box<Entry>>>() + size_of::<u64>() + size_of::<AccessHistory>();
// This conservative sizing estimate includes 32 bytes and an extra byte-owner
// header, although entries retain only value bytes.
const COMPUTED_MIN_ENTRY_CHARGE: usize = ENTRY_BYTES + 32 + 64 + 2 * BYTE_OWNER_BYTES;
// The fixed slot arrays are metadata held outside the byte budget: they may
// not exceed 8 % of (metadata + charged bytes) even for the smallest record, so
// the charge of one entry is floored at the value that makes the ratio exactly
// 8 % (`FIXED * 92 / 8`, rounded up). Cost: very small entries hold fewer slots
// per byte of budget.
const BUDGET_MIN_ENTRY_CHARGE: usize = (FIXED_SLOT_BYTES * 92).div_ceil(8);
const MIN_ENTRY_CHARGE: usize = if COMPUTED_MIN_ENTRY_CHARGE > BUDGET_MIN_ENTRY_CHARGE {
    COMPUTED_MIN_ENTRY_CHARGE
} else {
    BUDGET_MIN_ENTRY_CHARGE
};
const _: () = assert!(FIXED_SLOT_BYTES * 100 <= (FIXED_SLOT_BYTES + MIN_ENTRY_CHARGE) * 8);
// `Bytes::from_owner(Box<[u8]>)` owns an Arc allocation containing a box header
// and its strong/weak counts, plus the exactly sized byte allocation itself.
const BYTE_OWNER_BYTES: usize = size_of::<Box<[u8]>>() + 2 * size_of::<usize>();
/// Most shards one cache uses. More shards than concurrent readers add only
/// per-shard memory (one counter block of about 8 KiB each).
const MAX_SHARDS: usize = 64;
/// Smallest budget of one shard when the cache has several. This is also the
/// largest record a sharded cache can hold.
const MIN_SHARD_BYTES: usize = 8 * 1024 * 1024;
/// Counter stripes of a disabled cache. Striping keeps concurrent misses off
/// one cache line; there is no table behind any stripe.
const DISABLED_STRIPES: usize = 16;

/// Private per-open cache placement policy. The Sui exceptions are resolved
/// from the persisted shape's live names once, never from positional guesses.
#[derive(Clone, Copy, Default)]
pub(super) struct ReadCachePolicy {
    protected_keyspace: Option<u8>,
    skip_post_publish_warming: [u64; 4],
}

impl ReadCachePolicy {
    pub(super) fn for_sui_shape(shape: &crate::key_shape::KeyShape) -> Self {
        let mut policy = Self {
            protected_keyspace: shape
                .iter_ks()
                .find(|keyspace| {
                    !keyspace.destroyed() && keyspace.name() == "certified_checkpoints"
                })
                .map(|keyspace| keyspace.id().0),
            ..Self::default()
        };
        if policy.protected_keyspace.is_some() {
            for keyspace in shape.iter_ks().filter(|keyspace| !keyspace.destroyed()) {
                if matches!(
                    keyspace.name(),
                    "checkpoint_by_digest"
                        | "checkpoint_sequence_by_contents_digest"
                        | "watermarks"
                ) {
                    let id = usize::from(keyspace.id().0);
                    policy.skip_post_publish_warming[id / 64] |= 1u64 << (id % 64);
                }
            }
        }
        policy
    }

    fn skips_post_publish_warming(&self, keyspace: u8) -> bool {
        let id = usize::from(keyspace);
        self.skip_post_publish_warming[id / 64] & (1u64 << (id % 64)) != 0
    }
}

#[derive(Clone, Copy, Default)]
struct AccessHistory {
    fingerprint: u64,
    observations: u8,
}

/// Immutable logical identity of a cached member, with its keyspace.
///
/// The physical address is deliberately absent: relocation rewrites only the
/// address, so a re-pointed value must still hit its cached bytes. The keyspace
/// is part of the key because per-keyspace admission, protection and statistics
/// are decided on it and the cache does not retain the user key.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct CacheKey {
    identity: MutationVersion,
    keyspace: u8,
}

struct Entry {
    key: CacheKey,
    value: Bytes,
    benefit: u8,
    protected: bool,
}

impl Entry {
    fn payload_bytes(&self) -> usize {
        self.value.len() + BYTE_OWNER_BYTES * usize::from(!self.value.is_empty())
    }

    fn charge(&self) -> usize {
        self.payload_bytes() + ENTRY_BYTES
    }
}

pub(super) struct PreparedEntry {
    key: CacheKey,
    value: Bytes,
    payload_bytes: usize,
    fingerprint: u64,
}

/// Prepared entries are transient stack-owned values; boxing this enum would
/// add a per-candidate allocation before its detached payload is retained.
#[allow(clippy::large_enum_variant)]
pub(super) enum InsertPreparation {
    Prepared(PreparedEntry),
    Disabled,
    Oversize,
    Admission,
}

/// A preparation bound to the shard that admitted it. Insertion must reach the
/// same shard, because the entry's placement fingerprint is shard-local.
pub(crate) struct ShardedPreparation {
    shard: usize,
    preparation: InsertPreparation,
}

/// Bounded counters indexed by the stable one-byte native keyspace id.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PerKeyspaceReadCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub inserts: u64,
    pub evictions: u64,
}

pub const KEYSPACE_COUNTER_COUNT: usize = u8::MAX as usize + 1;

/// Counters are cumulative for this open authority. Byte figures describe
/// fixed arrays, detached byte-owner metadata and live Entry charges under the
/// module's allocator model. Other allocator rounding and application-held
/// clones are excluded. `metadata_bytes` counts only the fixed arrays.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ReadCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub inserts: u64,
    pub evictions: u64,
    pub rejected_oversize: u64,
    pub bypass_disabled: u64,
    pub bypass_admission: u64,
    pub bypass_eviction_limit: u64,
    /// Charged detached payload bytes admitted/served, including owner metadata;
    /// not current caller ownership.
    pub admitted_bytes: u64,
    pub served_bytes: u64,
    pub retained_bytes: usize,
    pub metadata_bytes: usize,
    /// Fixed instrumentation arrays (one 8 KiB array per shard); outside
    /// retained-cache accounting.
    pub per_keyspace_counters_bytes: usize,
    pub entries: usize,
    pub budget_bytes: usize,
}

impl ReadCacheStats {
    fn accumulate(&mut self, other: &Self) {
        self.hits += other.hits;
        self.misses += other.misses;
        self.inserts += other.inserts;
        self.evictions += other.evictions;
        self.rejected_oversize += other.rejected_oversize;
        self.bypass_disabled += other.bypass_disabled;
        self.bypass_admission += other.bypass_admission;
        self.bypass_eviction_limit += other.bypass_eviction_limit;
        self.admitted_bytes += other.admitted_bytes;
        self.served_bytes += other.served_bytes;
        self.retained_bytes += other.retained_bytes;
        self.metadata_bytes += other.metadata_bytes;
        self.per_keyspace_counters_bytes += other.per_keyspace_counters_bytes;
        self.entries += other.entries;
    }
}

/// Load of one cache shard.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ReadCacheShardLoad {
    pub entries: usize,
    pub hits: u64,
    pub misses: u64,
    /// Table mutex wait and sampled hold time. Zero for a disabled cache.
    pub locks: ShardLockStats,
}

/// Per-shard load, so a hot shard is visible.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ReadCacheShardStats {
    pub shards: Vec<ReadCacheShardLoad>,
}

#[derive(Default)]
struct KeyspaceCounters {
    hits: AtomicU64,
    misses: AtomicU64,
    inserts: AtomicU64,
    evictions: AtomicU64,
}

/// Counters of one shard, outside the table mutex and in their own cache line.
///
/// Every update is `Relaxed`: the values are independent statistics and
/// publish no other data. Writers are the table's mutex holder, or, for a
/// disabled cache, any thread.
#[repr(align(64))]
struct ShardCounters {
    hits: AtomicU64,
    misses: AtomicU64,
    inserts: AtomicU64,
    evictions: AtomicU64,
    rejected_oversize: AtomicU64,
    bypass_disabled: AtomicU64,
    bypass_admission: AtomicU64,
    bypass_eviction_limit: AtomicU64,
    admitted_bytes: AtomicU64,
    served_bytes: AtomicU64,
    resident_bytes: AtomicUsize,
    entries: AtomicUsize,
    per_keyspace: Box<[KeyspaceCounters]>,
}

fn bump(counter: &AtomicU64) {
    counter.fetch_add(1, Ordering::Relaxed);
}

fn load(counter: &AtomicU64) -> u64 {
    counter.load(Ordering::Relaxed)
}

impl ShardCounters {
    fn new() -> Self {
        Self {
            hits: AtomicU64::default(),
            misses: AtomicU64::default(),
            inserts: AtomicU64::default(),
            evictions: AtomicU64::default(),
            rejected_oversize: AtomicU64::default(),
            bypass_disabled: AtomicU64::default(),
            bypass_admission: AtomicU64::default(),
            bypass_eviction_limit: AtomicU64::default(),
            admitted_bytes: AtomicU64::default(),
            served_bytes: AtomicU64::default(),
            resident_bytes: AtomicUsize::default(),
            entries: AtomicUsize::default(),
            per_keyspace: (0..KEYSPACE_COUNTER_COUNT)
                .map(|_| KeyspaceCounters::default())
                .collect(),
        }
    }

    fn keyspace(&self, keyspace: u8) -> &KeyspaceCounters {
        &self.per_keyspace[usize::from(keyspace)]
    }

    fn record_hit(&self, keyspace: u8, payload_bytes: usize) {
        bump(&self.hits);
        self.served_bytes
            .fetch_add(payload_bytes as u64, Ordering::Relaxed);
        bump(&self.keyspace(keyspace).hits);
    }

    fn record_miss(&self, keyspace: u8) {
        bump(&self.misses);
        bump(&self.keyspace(keyspace).misses);
    }

    /// Counts an insertion that never reaches a table. Needs no lock.
    fn record_rejection(&self, preparation: &InsertPreparation) {
        match preparation {
            InsertPreparation::Prepared(_) => {}
            InsertPreparation::Disabled => bump(&self.bypass_disabled),
            InsertPreparation::Oversize => bump(&self.rejected_oversize),
            InsertPreparation::Admission => bump(&self.bypass_admission),
        }
    }

    fn reset(&self) {
        for counter in [
            &self.hits,
            &self.misses,
            &self.inserts,
            &self.evictions,
            &self.rejected_oversize,
            &self.bypass_disabled,
            &self.bypass_admission,
            &self.bypass_eviction_limit,
            &self.admitted_bytes,
            &self.served_bytes,
        ] {
            counter.store(0, Ordering::Relaxed);
        }
        self.resident_bytes.store(0, Ordering::Relaxed);
        self.entries.store(0, Ordering::Relaxed);
        for keyspace in self.per_keyspace.iter() {
            for counter in [
                &keyspace.hits,
                &keyspace.misses,
                &keyspace.inserts,
                &keyspace.evictions,
            ] {
                counter.store(0, Ordering::Relaxed);
            }
        }
    }

    fn snapshot(&self, budget_bytes: usize, metadata_bytes: usize) -> ReadCacheStats {
        ReadCacheStats {
            hits: load(&self.hits),
            misses: load(&self.misses),
            inserts: load(&self.inserts),
            evictions: load(&self.evictions),
            rejected_oversize: load(&self.rejected_oversize),
            bypass_disabled: load(&self.bypass_disabled),
            bypass_admission: load(&self.bypass_admission),
            bypass_eviction_limit: load(&self.bypass_eviction_limit),
            admitted_bytes: load(&self.admitted_bytes),
            served_bytes: load(&self.served_bytes),
            retained_bytes: metadata_bytes + self.resident_bytes.load(Ordering::Relaxed),
            metadata_bytes,
            per_keyspace_counters_bytes: self.per_keyspace.len() * size_of::<KeyspaceCounters>(),
            entries: self.entries.load(Ordering::Relaxed),
            budget_bytes,
        }
    }

    fn add_per_keyspace(&self, total: &mut [PerKeyspaceReadCacheStats; KEYSPACE_COUNTER_COUNT]) {
        for (sum, keyspace) in total.iter_mut().zip(self.per_keyspace.iter()) {
            sum.hits += load(&keyspace.hits);
            sum.misses += load(&keyspace.misses);
            sum.inserts += load(&keyspace.inserts);
            sum.evictions += load(&keyspace.evictions);
        }
    }
}

/// One shard's CLOCK table. Mutated only under its shard mutex; its counters
/// live in a shared block that is readable without that mutex.
struct CacheTable {
    slots: Box<[Option<Box<Entry>>]>,
    tags: Box<[u64]>,
    history: Box<[AccessHistory]>,
    history_clock: usize,
    hash: RandomState,
    clock: usize,
    policy: ReadCachePolicy,
    budget_bytes: usize,
    metadata_bytes: usize,
    // Reserve a quarter of resident bytes from unrelated keyspaces; protected
    // entries may use the rest of the shared budget when it is available.
    protected_reserve_bytes: usize,
    protected_retained_bytes: usize,
    regular_retained_bytes: usize,
    counters: Arc<ShardCounters>,
}

impl CacheTable {
    fn slot_count(budget_bytes: usize) -> usize {
        (budget_bytes / (FIXED_SLOT_BYTES + MIN_ENTRY_CHARGE) / WAYS) * WAYS
    }

    fn metadata_bytes(budget_bytes: usize) -> usize {
        Self::slot_count(budget_bytes) * FIXED_SLOT_BYTES
    }

    #[cfg(test)]
    fn new(budget_bytes: usize) -> Self {
        Self::new_with_policy(budget_bytes, ReadCachePolicy::default())
    }

    #[cfg(test)]
    fn new_with_policy(budget_bytes: usize, policy: ReadCachePolicy) -> Self {
        Self::with_counters(budget_bytes, policy, Arc::new(ShardCounters::new()))
    }

    fn with_counters(
        budget_bytes: usize,
        policy: ReadCachePolicy,
        counters: Arc<ShardCounters>,
    ) -> Self {
        let slot_count = Self::slot_count(budget_bytes);
        let slots: Box<[_]> = (0..slot_count).map(|_| None).collect();
        let tags = vec![0; slot_count].into_boxed_slice();
        let history = vec![AccessHistory::default(); slot_count].into_boxed_slice();
        let metadata_bytes = Self::metadata_bytes(budget_bytes);
        let payload_budget = budget_bytes.saturating_sub(metadata_bytes);
        Self {
            slots,
            tags,
            history,
            history_clock: 0,
            hash: RandomState::new(),
            clock: 0,
            policy,
            budget_bytes,
            metadata_bytes,
            protected_reserve_bytes: if policy.protected_keyspace.is_some() {
                payload_budget / 4
            } else {
                0
            },
            protected_retained_bytes: 0,
            regular_retained_bytes: 0,
            counters,
        }
    }

    #[cfg(test)]
    fn stats(&self) -> ReadCacheStats {
        self.counters
            .snapshot(self.budget_bytes, self.metadata_bytes)
    }

    #[cfg(test)]
    fn per_keyspace_stats(&self) -> [PerKeyspaceReadCacheStats; KEYSPACE_COUNTER_COUNT] {
        let mut total = [PerKeyspaceReadCacheStats::default(); KEYSPACE_COUNTER_COUNT];
        self.counters.add_per_keyspace(&mut total);
        total
    }

    fn set_start(&self, fingerprint: u64) -> usize {
        (fingerprint as usize % (self.slots.len() / WAYS)) * WAYS
    }

    // Seed identity placement with RandomState, then fold every identity field
    // into the tag. This is not an identity check: full equality remains
    // mandatory, even when folded tags collide.
    fn fingerprint(&self, key: &CacheKey) -> u64 {
        self.hash.hash_one(key.identity)
            ^ key.identity.batch.epoch.rotate_left(3)
            ^ key.identity.batch.sequence.rotate_left(7)
            ^ u64::from(key.identity.operation_index).rotate_left(11)
            ^ u64::from(key.keyspace).rotate_left(17)
    }

    fn observe(&mut self, fingerprint: u64) -> u8 {
        let slot = fingerprint as usize % self.history.len();
        let history = &mut self.history[slot];
        if history.observations != 0 && history.fingerprint == fingerprint {
            history.observations = history.observations.saturating_add(1).min(3);
        } else {
            *history = AccessHistory {
                fingerprint,
                observations: 1,
            };
        }
        history.observations
    }

    /// Return the value bytes of `key`, or `None` when absent.
    ///
    /// The index already holds the user key, so a resident identity is a hit
    /// without comparing keys; the value is the only payload retained.
    fn get(&mut self, key: &CacheKey) -> Option<Bytes> {
        let found = if !self.slots.is_empty() {
            let fingerprint = self.fingerprint(key);
            let start = self.set_start(fingerprint);
            let found = self.slots[start..start + WAYS]
                .iter_mut()
                .zip(&self.tags[start..start + WAYS])
                .filter(|(_, tag)| **tag == fingerprint)
                .filter_map(|(slot, _)| slot.as_mut())
                .find(|entry| entry.key == *key)
                .map(|entry| {
                    entry.benefit = entry.benefit.saturating_add(1).min(3);
                    (entry.value.clone(), entry.payload_bytes())
                });
            // Only a miss feeds admission history, matching the scalar path the
            // RejectFirst policy was calibrated on.
            if found.is_none() {
                self.observe(fingerprint);
            }
            found
        } else {
            None
        };
        if let Some((value, payload_bytes)) = found {
            self.counters.record_hit(key.keyspace, payload_bytes);
            Some(value)
        } else {
            self.counters.record_miss(key.keyspace);
            None
        }
    }

    fn preflight(&mut self, key: &CacheKey, payload_bytes: usize) -> Option<u64> {
        let protected = self.policy.protected_keyspace == Some(key.keyspace);
        let reserve = if protected {
            0
        } else {
            self.protected_reserve_bytes
        };
        let limit = self
            .budget_bytes
            .saturating_sub(self.metadata_bytes)
            .saturating_sub(reserve);
        if payload_bytes + ENTRY_BYTES > limit {
            return None;
        }
        let fingerprint = self.fingerprint(key);
        let history = &self.history[fingerprint as usize % self.history.len()];
        if history.observations != 0 && history.fingerprint == fingerprint {
            return Some(fingerprint);
        }
        let start = self.set_start(fingerprint);
        if self.slots[start..start + WAYS].iter().any(|slot| {
            slot.as_ref().is_none_or(|entry| {
                entry.key == *key || (entry.benefit == 0 && self.can_evict_entry(entry, protected))
            })
        }) {
            return Some(fingerprint);
        }
        self.observe(fingerprint);
        None
    }

    fn can_evict_entry(&self, entry: &Entry, protected_candidate: bool) -> bool {
        !entry.protected
            || protected_candidate
            || self.protected_retained_bytes.saturating_sub(entry.charge())
                >= self.protected_reserve_bytes
    }

    fn configure_policy(&mut self, policy: ReadCachePolicy) {
        debug_assert_eq!(self.counters.entries.load(Ordering::Relaxed), 0);
        self.policy = policy;
        let payload_budget = self.budget_bytes.saturating_sub(self.metadata_bytes);
        self.protected_reserve_bytes = if policy.protected_keyspace.is_some() {
            payload_budget / 4
        } else {
            0
        };
    }

    /// `budget_bytes` is this shard's budget. `admit` runs the advisory
    /// preflight under the shard mutex; everything else here is lock-free.
    fn prepare_with(
        budget_bytes: usize,
        key: CacheKey,
        value: &Bytes,
        admit: impl FnOnce(usize) -> Option<u64>,
    ) -> InsertPreparation {
        let metadata_bytes = Self::metadata_bytes(budget_bytes);
        if metadata_bytes == 0 {
            return InsertPreparation::Disabled;
        }
        let Some(payload_bytes) = value
            .len()
            .checked_add(BYTE_OWNER_BYTES * usize::from(!value.is_empty()))
        else {
            return InsertPreparation::Oversize;
        };
        if payload_bytes
            .checked_add(ENTRY_BYTES)
            .is_none_or(|charge| charge > budget_bytes.saturating_sub(metadata_bytes))
        {
            return InsertPreparation::Oversize;
        }
        let Some(fingerprint) = admit(payload_bytes) else {
            return InsertPreparation::Admission;
        };
        InsertPreparation::Prepared(PreparedEntry {
            key,
            value: detach(value),
            payload_bytes,
            fingerprint,
        })
    }

    #[cfg(test)]
    fn insert_verified(&mut self, key: CacheKey, value: &Bytes) -> bool {
        let prepared = Self::prepare_with(self.budget_bytes, key, value, |payload_bytes| {
            self.preflight(&key, payload_bytes)
        });
        self.insert_prepared(prepared)
    }

    #[cfg(test)]
    fn insert_prepared(&mut self, prepared: InsertPreparation) -> bool {
        match prepared {
            InsertPreparation::Prepared(entry) => self.insert_entry(entry),
            rejected => {
                self.counters.record_rejection(&rejected);
                false
            }
        }
    }

    /// Admit one prepared entry, rechecking the current table state.
    #[allow(
        clippy::expect_used,
        reason = "the candidate set only contains occupied slots"
    )]
    fn insert_entry(&mut self, prepared: PreparedEntry) -> bool {
        let PreparedEntry {
            key,
            value,
            payload_bytes,
            fingerprint,
        } = prepared;
        let keyspace_id = usize::from(key.keyspace);
        let protected = self.policy.protected_keyspace == Some(key.keyspace);
        let start = self.set_start(fingerprint);
        if self.slots[start..start + WAYS]
            .iter()
            .flatten()
            .any(|entry| entry.key == key)
        {
            return true;
        }
        let seen = self.observe(fingerprint);
        // Age the finite history incrementally; no key/ghost map grows with churn.
        self.history_clock = (self.history_clock + 1) % self.history.len();
        self.history[self.history_clock].observations = self.history[self.history_clock]
            .observations
            .saturating_sub(1);
        let slot = if let Some(slot) = (start..start + WAYS).find(|&i| self.slots[i].is_none()) {
            slot
        } else {
            let replaceable = (start..start + WAYS).filter(|&i| {
                self.slots[i]
                    .as_ref()
                    .is_some_and(|entry| self.can_evict_entry(entry, protected))
            });
            let slot = replaceable.min_by_key(|&i| {
                let entry = self.slots[i].as_ref().expect("occupied slot");
                (u8::from(entry.protected), entry.benefit)
            });
            let Some(slot) = slot else {
                bump(&self.counters.bypass_admission);
                return false;
            };
            if seen < 2 && self.slots[slot].as_ref().expect("occupied slot").benefit != 0 {
                bump(&self.counters.bypass_admission);
                return false;
            }
            slot
        };
        let replacing_regular_bytes = self.slots[slot]
            .as_ref()
            .filter(|entry| !entry.protected)
            .map_or(0, |entry| entry.charge());
        for _ in 0..MAX_EVICTION_STEPS {
            if self.can_fit_replacing(payload_bytes, protected, slot) {
                break;
            }
            let index = self.clock;
            self.clock = (self.clock + 1) % self.slots.len();
            if index != slot
                && self.slots[index].as_ref().is_some_and(|entry| {
                    self.can_evict_entry(entry, protected)
                        && (!protected
                            || !entry.protected
                            || self.regular_retained_bytes <= replacing_regular_bytes)
                })
            {
                if self.slots[index]
                    .as_ref()
                    .is_some_and(|entry| entry.benefit != 0)
                {
                    if seen >= 2
                        && let Some(entry) = self.slots[index].as_mut()
                    {
                        entry.benefit -= 1;
                    }
                } else {
                    self.evict(index);
                }
            }
        }
        if !self.can_fit_replacing(payload_bytes, protected, slot) {
            bump(&self.counters.bypass_eviction_limit);
            return false;
        }
        let allocation = self.remove_entry(slot);
        let entry = Entry {
            key,
            value,
            benefit: u8::from(seen >= 2),
            protected,
        };
        self.slots[slot] = Some(if let Some(mut allocation) = allocation {
            *allocation = entry;
            allocation
        } else {
            // Only a vacant slot needs a new allocation. Replacement reuses it.
            Box::new(entry)
        });
        self.tags[slot] = fingerprint;
        let charge = payload_bytes + ENTRY_BYTES;
        self.counters
            .resident_bytes
            .fetch_add(charge, Ordering::Relaxed);
        if protected {
            self.protected_retained_bytes += charge;
        } else {
            self.regular_retained_bytes += charge;
        }
        self.counters.entries.fetch_add(1, Ordering::Relaxed);
        bump(&self.counters.inserts);
        self.counters
            .admitted_bytes
            .fetch_add(payload_bytes as u64, Ordering::Relaxed);
        bump(&self.counters.keyspace(keyspace_id as u8).inserts);
        true
    }

    fn can_fit_replacing(&self, payload_bytes: usize, protected: bool, slot: usize) -> bool {
        let charge = payload_bytes + ENTRY_BYTES;
        let replaced = self.slots[slot].as_ref();
        let released_protected_bytes = replaced
            .filter(|entry| entry.protected)
            .map_or(0, |entry| entry.charge());
        let released_regular_bytes = replaced
            .filter(|entry| !entry.protected)
            .map_or(0, |entry| entry.charge());
        let payload_budget = self.budget_bytes.saturating_sub(self.metadata_bytes);
        let protected_after_replace = self
            .protected_retained_bytes
            .saturating_sub(released_protected_bytes);
        if charge
            > payload_budget.saturating_sub(
                protected_after_replace.saturating_add(
                    self.regular_retained_bytes
                        .saturating_sub(released_regular_bytes),
                ),
            )
        {
            return false;
        }
        if !protected
            && released_protected_bytes != 0
            && protected_after_replace < self.protected_reserve_bytes
        {
            return false;
        }
        protected
            || charge
                <= payload_budget
                    .saturating_sub(self.protected_reserve_bytes)
                    .saturating_sub(
                        self.regular_retained_bytes
                            .saturating_sub(released_regular_bytes),
                    )
    }

    fn evict(&mut self, slot: usize) {
        drop(self.remove_entry(slot));
    }

    fn remove_entry(&mut self, slot: usize) -> Option<Box<Entry>> {
        let entry = self.slots[slot].take()?;
        self.tags[slot] = 0;
        let charge = entry.charge();
        self.counters
            .resident_bytes
            .fetch_sub(charge, Ordering::Relaxed);
        if entry.protected {
            self.protected_retained_bytes -= charge;
        } else {
            self.regular_retained_bytes -= charge;
        }
        self.counters.entries.fetch_sub(1, Ordering::Relaxed);
        bump(&self.counters.evictions);
        bump(&self.counters.keyspace(entry.key.keyspace).evictions);
        Some(entry)
    }

    #[cfg(test)]
    fn set_start_for_key(&self, key: &CacheKey) -> usize {
        self.set_start(self.fingerprint(key))
    }
}

fn detach(bytes: &Bytes) -> Bytes {
    if bytes.is_empty() {
        Bytes::new()
    } else {
        Bytes::from(bytes.as_ref().to_vec().into_boxed_slice())
    }
}

struct CacheShard {
    table: ShardMutex<CacheTable>,
    counters: Arc<ShardCounters>,
}

/// The sharded cache. See the module documentation for the concurrency model.
pub(crate) struct ReadCache {
    shards: Box<[CacheShard]>,
    mask: usize,
    shard_budget_bytes: usize,
    shard_metadata_bytes: usize,
    budget_bytes: usize,
    enabled: bool,
}

impl ReadCache {
    fn shard_count_for(budget_bytes: usize) -> usize {
        if CacheTable::metadata_bytes(budget_bytes) == 0 {
            return DISABLED_STRIPES;
        }
        let wanted = (budget_bytes / MIN_SHARD_BYTES).clamp(1, MAX_SHARDS);
        1 << wanted.ilog2()
    }

    /// Fixed table and history bytes for `budget_bytes`; zero when disabled.
    pub(super) fn metadata_bytes(budget_bytes: usize) -> usize {
        let shards = Self::shard_count_for(budget_bytes);
        shards * CacheTable::metadata_bytes(budget_bytes / shards)
    }

    pub(crate) fn new(budget_bytes: usize) -> Self {
        Self::new_with_policy(budget_bytes, ReadCachePolicy::default())
    }

    pub(super) fn new_with_policy(budget_bytes: usize, policy: ReadCachePolicy) -> Self {
        Self::build(budget_bytes, policy, Self::shard_count_for(budget_bytes))
    }

    /// `shards` must be a nonzero power of two; callers pass constants.
    #[cfg(test)]
    fn new_with_shards(budget_bytes: usize, policy: ReadCachePolicy, shards: usize) -> Self {
        assert!(shards.is_power_of_two(), "shard count is a power of two");
        Self::build(budget_bytes, policy, shards)
    }

    fn build(budget_bytes: usize, policy: ReadCachePolicy, shards: usize) -> Self {
        let shard_budget_bytes = budget_bytes / shards;
        let shard_metadata_bytes = CacheTable::metadata_bytes(shard_budget_bytes);
        let shards_vec: Box<[CacheShard]> = (0..shards)
            .map(|_| {
                let counters = Arc::new(ShardCounters::new());
                CacheShard {
                    table: ShardMutex::new(CacheTable::with_counters(
                        shard_budget_bytes,
                        policy,
                        Arc::clone(&counters),
                    )),
                    counters,
                }
            })
            .collect();
        Self {
            shards: shards_vec,
            mask: shards - 1,
            shard_budget_bytes,
            shard_metadata_bytes,
            budget_bytes,
            enabled: shard_metadata_bytes != 0,
        }
    }

    /// False when the budget holds no table slot. A disabled cache takes no lock.
    #[cfg(test)]
    pub(super) fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Stable routing: logical version and member index. Independent of the
    /// per-table placement hash, which keeps its own random seed.
    fn route(&self, identity: MutationVersion) -> usize {
        let hash = StableHasher::new()
            .bytes(&identity.batch.epoch.to_le_bytes())
            .bytes(&identity.batch.sequence.to_le_bytes())
            .bytes(&identity.operation_index.to_le_bytes())
            .finish();
        (hash as usize) & self.mask
    }

    /// Value bytes for `identity`, or `None` when it is not resident.
    pub(crate) fn get(&self, identity: MutationVersion, keyspace: u8) -> Option<Bytes> {
        let key = CacheKey { identity, keyspace };
        let shard = &self.shards[self.route(identity)];
        if !self.enabled {
            shard.counters.record_miss(keyspace);
            return None;
        }
        shard.table.lock().get(&key)
    }

    /// Prepare a fill with the value bytes the read path just verified.
    ///
    /// The cache retains only the value; a tombstone or a missing member is
    /// never inserted, so `None` never needs an entry of its own.
    pub(crate) fn prepare(
        &self,
        identity: MutationVersion,
        keyspace: u8,
        value: &Bytes,
    ) -> ShardedPreparation {
        let key = CacheKey { identity, keyspace };
        let shard = self.route(identity);
        let table = &self.shards[shard].table;
        let preparation =
            CacheTable::prepare_with(self.shard_budget_bytes, key, value, |payload| {
                table.lock().preflight(&key, payload)
            });
        ShardedPreparation { shard, preparation }
    }

    /// Warm the cache from a published put, exactly as native's
    /// `DecompressCache` fills after a write. Shape checks are unnecessary: the
    /// caller hands the bytes it just published and verified.
    pub(super) fn prepare_post_publish(
        &self,
        identity: MutationVersion,
        keyspace: u8,
        value: &Bytes,
    ) -> ShardedPreparation {
        let key = CacheKey { identity, keyspace };
        let shard = self.route(identity);
        let table = &self.shards[shard].table;
        let preparation =
            CacheTable::prepare_with(self.shard_budget_bytes, key, value, |payload| {
                let mut table = table.lock();
                if table.policy.skips_post_publish_warming(keyspace) {
                    None
                } else {
                    table.preflight(&key, payload)
                }
            });
        ShardedPreparation { shard, preparation }
    }

    /// Only a prepared entry reaches a table. Every rejection is counted with
    /// atomics alone, so a disabled cache never locks here.
    pub(crate) fn insert_prepared(&self, prepared: ShardedPreparation) -> bool {
        let shard = &self.shards[prepared.shard & self.mask];
        match prepared.preparation {
            InsertPreparation::Prepared(entry) => shard.table.lock().insert_entry(entry),
            rejected => {
                shard.counters.record_rejection(&rejected);
                false
            }
        }
    }

    #[cfg(test)]
    fn insert_verified(&self, key: CacheKey, value: &Bytes) -> bool {
        self.insert_prepared(self.prepare(key.identity, key.keyspace, value))
    }

    /// Sum of every shard's counters. Lock-free; a concurrent update may be
    /// counted by some shards and not yet by others.
    pub(super) fn stats(&self) -> ReadCacheStats {
        let mut total = ReadCacheStats {
            budget_bytes: self.budget_bytes,
            ..ReadCacheStats::default()
        };
        for shard in self.shards.iter() {
            total.accumulate(
                &shard
                    .counters
                    .snapshot(self.shard_budget_bytes, self.shard_metadata_bytes),
            );
        }
        total
    }

    pub(super) fn per_keyspace_stats(&self) -> [PerKeyspaceReadCacheStats; KEYSPACE_COUNTER_COUNT] {
        let mut total = [PerKeyspaceReadCacheStats::default(); KEYSPACE_COUNTER_COUNT];
        for shard in self.shards.iter() {
            shard.counters.add_per_keyspace(&mut total);
        }
        total
    }

    pub(super) fn shard_stats(&self) -> ReadCacheShardStats {
        ReadCacheShardStats {
            shards: self
                .shards
                .iter()
                .map(|shard| ReadCacheShardLoad {
                    entries: shard.counters.entries.load(Ordering::Relaxed),
                    hits: load(&shard.counters.hits),
                    misses: load(&shard.counters.misses),
                    locks: shard.table.stats(),
                })
                .collect(),
        }
    }

    /// Install a placement policy on an empty cache. Rare; locks each shard in
    /// turn and never holds two.
    pub(super) fn configure_policy(&self, policy: ReadCachePolicy) {
        for shard in self.shards.iter() {
            shard.table.lock().configure_policy(policy);
        }
    }

    /// Evict every entry of `keyspace` older than the `DropCells` batch at
    /// `version`. The cache no longer retains keys, so the dropped cell range
    /// cannot be matched here; evicting the older entries of the affected
    /// keyspace is conservative (a cache hit is never authoritative) and a
    /// dropped range belongs to a keyspace whose rows are being removed
    /// wholesale. Index masks already hide these identities from reads. Locks
    /// one shard at a time and never holds two.
    pub(super) fn evict_dropped(
        &self,
        keyspace: crate::key_shape::KeySpace,
        version: crate::distributed::types::BatchVersion,
    ) {
        if !self.enabled {
            return;
        }
        for shard in self.shards.iter() {
            let mut table = shard.table.lock();
            for slot in 0..table.slots.len() {
                let evict = table.slots[slot].as_ref().is_some_and(|entry| {
                    entry.key.keyspace == keyspace.0 && entry.key.identity.batch < version
                });
                if evict {
                    table.evict(slot);
                }
            }
        }
    }

    /// Drop every entry and reset every counter, keeping the policy. Rare;
    /// locks each shard in turn and never holds two.
    pub(super) fn clear(&self) {
        for shard in self.shards.iter() {
            let mut table = shard.table.lock();
            shard.counters.reset();
            let policy = table.policy;
            *table = CacheTable::with_counters(
                self.shard_budget_bytes,
                policy,
                Arc::clone(&shard.counters),
            );
        }
    }
}

#[cfg(test)]
#[path = "read_cache/tests.rs"]
mod tests;

#[cfg(test)]
#[path = "read_cache/entry_charge_tests.rs"]
mod entry_charge_tests;
