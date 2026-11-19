//! Bounded, derivative cache of verified immutable Value-WAL members.
//!
//! Paper §3.2 keeps the index authoritative and follows its WAL position for a
//! read. Here the key is the *entire* published reference: database, operation,
//! logical version, physical generation/address and both member/batch digests.
//! An overwrite, delete, checkpoint cut or reused offset cannot select a stale
//! entry merely because its user key or offset matches. Fill only after the
//! authenticated storage client has verified membership and member integrity.
//!
//! This cache is private to one open authority and starts empty after replay.
//! Authoritative index cuts and read pins gate retention and reclamation before
//! lookup. A cached reference cannot authorize an expired cut or make a retired
//! WAL file reachable. As in native `DecompressCache::get`, a cached
//! good copy is not evidence of current node health or absence of later disk
//! corruption, and must never be used as a durability acknowledgement.
//!
//! RocksDB's unique file identity plus offset cache keys inspire exact immutable
//! identity; its CLOCK cache and Tidehunter's native decompression cache inspire
//! second-chance eviction. A flat four-way table stores handles and full-reference
//! tags. Tag matches dereference one Entry; full equality still decides a hit.
//!
//! # Concurrency
//!
//! The cache is `N` independent shards (power of two, chosen once at
//! construction, never persisted). A reference routes to its shard by a stable
//! hash of its operation id and member index ([`StableHasher`], never
//! `RandomState`). A shard owns its CLOCK table, admission history and byte
//! budget (`budget / N`); no operation touches two shards, so there is no lock
//! order to respect. Each table sits behind its own mutex whose critical
//! section is one bounded probe or insertion: no I/O or callback. Payload
//! detachment stays outside the lock. A vacant slot allocates one small Entry;
//! replacement reuses that slot's Box. Hits allocate nothing.
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
//! Every fill copies key and value into exact-size allocations before it is
//! admitted (`detach`), whatever backs the caller's `Bytes`. A value that is a
//! slice of a pooled or aligned read block (live/sealed NVMe-oF reads, up to
//! several KiB for a 1 KiB value) therefore never pins that block, and the
//! charged payload equals the bytes the cache really retains. There is no
//! size threshold: the copy cost is one small memcpy per admitted fill.
//!
//! Every live entry pays key/value bytes, the existing nonempty byte-owner
//! headers, and `size_of::<Entry>() + ENTRY_ALLOCATOR_BYTES`. The 8-byte Entry
//! allowance models a 216-byte Box in a 224-byte glibc chunk on this build.
//! This is an accounting model, not an RSS bound: allocator rounding of byte
//! owners, instrumentation, shard locks and caller-held clones remain outside it.
//! Per-slot fixed bytes are one optional Box handle, one u64 tag and one history
//! item (32 bytes here). For shard budget b, allocate
//! `4 * floor(b / (FIXED_SLOT_BYTES + MIN_ENTRY_CHARGE) / 4)` slots.
//! The minimum realistic charge uses a 32-byte key and 64-byte value (384 bytes
//! here). Fixed arrays use at most 7.70% of b. No payload floor is reserved per
//! slot. Disabled tiny caches allocate no slot arrays. CLOCK uses at most 64 steps.
//!
//! Sources:
//! <https://github.com/facebook/rocksdb/blob/main/cache/cache_key.h>
//! <https://github.com/facebook/rocksdb/wiki/Block-Cache>
//! Admission adapts CacheLib's bounded RejectFirstAP access history, but keeps
//! first-touch probationary entries for native-style read-after-write. A reused
//! entry beats a first-touch candidate; repeated misses can earn admission.
//! F2 §7's second chance is applied to exact references, not mutable key chains.
//! <https://github.com/facebook/CacheLib/blob/a24370092f0bd6fa7145b2f01c3e50e63edd6a6e/cachelib/allocator/NvmAdmissionPolicy.h#L175-L249>.
//! FASTER Appendix D and F2 §7 likewise use a bounded read-hot copy while the
//! log/index remains authoritative. Placement hashes the full reference; full
//! immutable-reference equality still decides a hit, including when a physical
//! generation differs or tags collide.
//! <https://doi.org/10.1145/3183713.3196898> (FASTER, Appendix D)
//! <https://arxiv.org/abs/2305.01516v3> (F2, §7)
//! <https://github.com/microsoft/garnet/blob/v1.0.82/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/ReadCache.cs>

use super::Mutation;
use super::ValueRecordReference;
use crate::distributed::shard_lock::{ShardLockStats, ShardMutex, StableHasher};
use crate::distributed::sui::kv_capture;
use crate::distributed::types::RecordKind;
use minibytes::Bytes;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hash, Hasher};
use std::mem::size_of;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

#[cfg(test)]
mod trace_eval;

#[derive(Clone, Copy, Default)]
struct CacheLayout {
    #[cfg(test)]
    placement_seed: Option<u64>,
}

#[cfg(not(test))]
type PlacementState = RandomState;

#[cfg(test)]
enum PlacementState {
    Random(RandomState),
    Seeded(u64),
}

#[cfg(test)]
impl BuildHasher for PlacementState {
    type Hasher = std::collections::hash_map::DefaultHasher;

    fn build_hasher(&self) -> Self::Hasher {
        match self {
            Self::Random(state) => state.build_hasher(),
            Self::Seeded(seed) => {
                let mut hasher = Self::Hasher::new();
                hasher.write_u64(*seed);
                hasher
            }
        }
    }
}

impl CacheLayout {
    fn placement(self) -> PlacementState {
        #[cfg(not(test))]
        {
            RandomState::new()
        }
        #[cfg(test)]
        {
            self.placement_seed.map_or_else(
                || PlacementState::Random(RandomState::new()),
                PlacementState::Seeded,
            )
        }
    }
}

const WAYS: usize = 4;
// Bound insertion latency independently of configured capacity.
const MAX_EVICTION_STEPS: usize = 64;
// One 216-byte Entry uses a 224-byte glibc chunk on this build. This is an
// effective accounting allowance, not a portable allocator or RSS guarantee.
const ENTRY_ALLOCATOR_BYTES: usize = 8;
const ENTRY_BYTES: usize = size_of::<Entry>() + ENTRY_ALLOCATOR_BYTES;
const FIXED_SLOT_BYTES: usize =
    size_of::<Option<Box<Entry>>>() + size_of::<u64>() + size_of::<AccessHistory>();
// Size the fixed arrays for a 32-byte key and a 64-byte value. Smaller records
// can reach the slot ceiling; larger records reach the byte ceiling first.
const MIN_ENTRY_CHARGE: usize = ENTRY_BYTES + 32 + 64 + 2 * BYTE_OWNER_BYTES;
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

struct Entry {
    reference: ValueRecordReference,
    mutation: Mutation,
    benefit: u8,
    protected: bool,
}

impl Entry {
    fn payload_bytes(&self) -> usize {
        let descriptor = self.reference.descriptor;
        descriptor.key_len as usize
            + descriptor.value_len as usize
            + BYTE_OWNER_BYTES
                * (usize::from(descriptor.key_len != 0) + usize::from(descriptor.value_len != 0))
    }

    fn charge(&self) -> usize {
        self.payload_bytes() + ENTRY_BYTES
    }
}

pub(super) struct PreparedEntry {
    reference: ValueRecordReference,
    mutation: Mutation,
    payload_bytes: usize,
    fingerprint: u64,
}

/// Prepared entries are transient stack-owned values; boxing this enum would
/// add a per-candidate allocation before its detached payload is retained.
#[allow(clippy::large_enum_variant)]
pub(super) enum InsertPreparation {
    Prepared(PreparedEntry),
    Disabled,
    Mismatch,
    Oversize,
    Admission,
}

/// A preparation bound to the shard that admitted it. Insertion must reach the
/// same shard, because the entry's placement fingerprint is shard-local.
pub(super) struct ShardedPreparation {
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
    pub rejected_mismatch: u64,
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
        self.rejected_mismatch += other.rejected_mismatch;
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
    rejected_mismatch: AtomicU64,
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
            rejected_mismatch: AtomicU64::default(),
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
            InsertPreparation::Mismatch => bump(&self.rejected_mismatch),
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
            &self.rejected_mismatch,
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
            rejected_mismatch: load(&self.rejected_mismatch),
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
    hash: PlacementState,
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
        Self::with_counters(
            budget_bytes,
            policy,
            Arc::new(ShardCounters::new()),
            CacheLayout::default(),
        )
    }

    fn with_counters(
        budget_bytes: usize,
        policy: ReadCachePolicy,
        counters: Arc<ShardCounters>,
        layout: CacheLayout,
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
            hash: layout.placement(),
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

    // Seed operation placement with RandomState, then fold every remaining
    // reference field into the tag. This is not a cryptographic identity check:
    // full equality remains mandatory, even when folded tags collide.
    fn fingerprint(&self, reference: &ValueRecordReference) -> u64 {
        let mut hasher = self.hash.build_hasher();
        reference.batch_id.operation.hash(&mut hasher);
        reference.descriptor.operation_index.hash(&mut hasher);
        let mut tag = hasher.finish();
        for word in reference.batch_id.database.as_chunks::<8>().0 {
            tag ^= u64::from_le_bytes(*word);
        }
        for word in reference.batch_digest.0.as_chunks::<8>().0 {
            tag ^= u64::from_le_bytes(*word).rotate_left(17);
        }
        let descriptor = reference.descriptor;
        tag ^ reference.batch_version.epoch.rotate_left(3)
            ^ reference.batch_version.sequence.rotate_left(7)
            ^ reference.batch_address.segment_generation.rotate_left(11)
            ^ reference.batch_address.offset.rotate_left(19)
            ^ (u64::from(reference.batch_address.partition) << 32
                | u64::from(reference.batch_address.len))
            .rotate_left(23)
            ^ (u64::from(descriptor.offset) << 32 | u64::from(descriptor.len)).rotate_left(29)
            ^ (u64::from(descriptor.key_len) << 32 | u64::from(descriptor.value_offset))
                .rotate_left(31)
            ^ (u64::from(descriptor.value_len) << 32 | u64::from(descriptor.checksum.0))
                .rotate_left(37)
            ^ u64::from(descriptor.keyspace).rotate_left(41)
            ^ u64::from(descriptor.kind == RecordKind::Delete).rotate_left(43)
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
    fn get(&mut self, reference: &ValueRecordReference) -> Option<Mutation> {
        self.lookup(reference, true)
    }

    fn get_scan(&mut self, reference: &ValueRecordReference) -> Option<Mutation> {
        self.lookup(reference, false)
    }

    /// Scalar hits verify the requested key in place and retain only the value
    /// owner. Exact-reference lookup remains the authority; the key comparison
    /// prevents a same-reference association error from becoming a false hit.
    fn get_point(
        &mut self,
        reference: &ValueRecordReference,
        keyspace: crate::key_shape::KeySpace,
        key: &[u8],
    ) -> Option<Bytes> {
        let found = if reference.descriptor.keyspace == keyspace.0 && !self.slots.is_empty() {
            let fingerprint = self.fingerprint(reference);
            let start = self.set_start(fingerprint);
            let found = self.slots[start..start + WAYS]
                .iter_mut()
                .zip(&self.tags[start..start + WAYS])
                .filter(|(_, tag)| **tag == fingerprint)
                .filter_map(|(slot, _)| slot.as_mut())
                .find(|entry| entry.reference == *reference)
                .and_then(|entry| {
                    if let Mutation::Put {
                        keyspace: stored_keyspace,
                        key: stored_key,
                        value,
                    } = &entry.mutation
                    {
                        if *stored_keyspace == keyspace && stored_key.as_ref() == key {
                            entry.benefit = entry.benefit.saturating_add(1).min(3);
                            Some((value.clone(), entry.payload_bytes()))
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                });
            // Resident hits update their entry benefit; history is for misses
            // that may earn admission after repeated demand.
            if found.is_none() {
                self.observe(fingerprint);
            }
            found
        } else {
            None
        };
        if let Some((value, payload_bytes)) = found {
            self.counters
                .record_hit(reference.descriptor.keyspace, payload_bytes);
            Some(value)
        } else {
            self.counters.record_miss(reference.descriptor.keyspace);
            None
        }
    }

    fn lookup(&mut self, reference: &ValueRecordReference, track_access: bool) -> Option<Mutation> {
        let found = if !self.slots.is_empty() {
            let fingerprint = self.fingerprint(reference);
            if track_access {
                self.observe(fingerprint);
            }
            let start = self.set_start(fingerprint);
            self.slots[start..start + WAYS]
                .iter_mut()
                .zip(&self.tags[start..start + WAYS])
                .filter(|(_, tag)| **tag == fingerprint)
                .filter_map(|(slot, _)| slot.as_mut())
                .find(|entry| entry.reference == *reference)
                .map(|entry| {
                    if track_access {
                        entry.benefit = entry.benefit.saturating_add(1).min(3);
                    }
                    (entry.mutation.clone(), entry.payload_bytes())
                })
        } else {
            None
        };
        if let Some((mutation, payload_bytes)) = found {
            self.counters
                .record_hit(reference.descriptor.keyspace, payload_bytes);
            Some(mutation)
        } else {
            self.counters.record_miss(reference.descriptor.keyspace);
            None
        }
    }

    fn preflight(&mut self, reference: &ValueRecordReference, payload_bytes: usize) -> Option<u64> {
        let protected = self.policy.protected_keyspace == Some(reference.descriptor.keyspace);
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
        let fingerprint = self.fingerprint(reference);
        let history = &self.history[fingerprint as usize % self.history.len()];
        if history.observations != 0 && history.fingerprint == fingerprint {
            return Some(fingerprint);
        }
        let start = self.set_start(fingerprint);
        if self.slots[start..start + WAYS].iter().any(|slot| {
            slot.as_ref().is_none_or(|entry| {
                entry.reference == *reference
                    || (entry.benefit == 0 && self.can_evict_entry(entry, protected))
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
        reference: ValueRecordReference,
        mutation: &Mutation,
        admit: impl FnOnce(usize) -> Option<u64>,
    ) -> InsertPreparation {
        let metadata_bytes = Self::metadata_bytes(budget_bytes);
        if metadata_bytes == 0 {
            return InsertPreparation::Disabled;
        }
        let descriptor = reference.descriptor;
        // A DropCells member has no value reference and is never cached.
        let (kind, key, value_len) = match mutation {
            Mutation::Put { key, value, .. } => (RecordKind::Put, key, value.len()),
            Mutation::Delete { key, .. } => (RecordKind::Delete, key, 0),
            Mutation::DropCells { .. } => return InsertPreparation::Mismatch,
        };
        if descriptor.kind != kind
            || descriptor.keyspace != mutation.keyspace().0
            || descriptor.key_len as usize != key.len()
            || descriptor.value_len as usize != value_len
        {
            return InsertPreparation::Mismatch;
        }
        let Some(payload_bytes) = key.len().checked_add(value_len).and_then(|n| {
            let owners = usize::from(!key.is_empty()) + usize::from(value_len > 0);
            n.checked_add(owners * BYTE_OWNER_BYTES)
        }) else {
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
        let mutation = match mutation {
            Mutation::Put {
                keyspace,
                key,
                value,
            } => Mutation::Put {
                keyspace: *keyspace,
                key: detach(key),
                value: detach(value),
            },
            Mutation::Delete { keyspace, key } => Mutation::Delete {
                keyspace: *keyspace,
                key: detach(key),
            },
            Mutation::DropCells { .. } => return InsertPreparation::Mismatch,
        };
        InsertPreparation::Prepared(PreparedEntry {
            reference,
            mutation,
            payload_bytes,
            fingerprint,
        })
    }

    #[cfg(test)]
    fn insert_verified(&mut self, reference: ValueRecordReference, mutation: &Mutation) -> bool {
        let prepared =
            Self::prepare_with(self.budget_bytes, reference, mutation, |payload_bytes| {
                self.preflight(&reference, payload_bytes)
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
    fn insert_entry(&mut self, prepared: PreparedEntry) -> bool {
        let PreparedEntry {
            reference,
            mutation,
            payload_bytes,
            fingerprint,
        } = prepared;
        let keyspace_id = reference.descriptor.keyspace as usize;
        let protected = self.policy.protected_keyspace == Some(keyspace_id as u8);
        let start = self.set_start(fingerprint);
        if self.slots[start..start + WAYS]
            .iter()
            .flatten()
            .any(|entry| entry.reference == reference)
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
                let entry = self.slots[i].as_ref().unwrap();
                (u8::from(entry.protected), entry.benefit)
            });
            let Some(slot) = slot else {
                bump(&self.counters.bypass_admission);
                return false;
            };
            if seen < 2 && self.slots[slot].as_ref().unwrap().benefit != 0 {
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
            reference,
            mutation,
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
        bump(
            &self
                .counters
                .keyspace(entry.reference.descriptor.keyspace)
                .evictions,
        );
        Some(entry)
    }

    #[cfg(test)]
    fn set_start_for_ref(&self, reference: &ValueRecordReference) -> usize {
        self.set_start(self.fingerprint(reference))
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
pub(super) struct ReadCache {
    shards: Box<[CacheShard]>,
    mask: usize,
    shard_budget_bytes: usize,
    shard_metadata_bytes: usize,
    budget_bytes: usize,
    enabled: bool,
    layout: CacheLayout,
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

    pub(super) fn new(budget_bytes: usize) -> Self {
        Self::new_with_policy(budget_bytes, ReadCachePolicy::default())
    }

    pub(super) fn new_with_policy(budget_bytes: usize, policy: ReadCachePolicy) -> Self {
        Self::build(
            budget_bytes,
            policy,
            Self::shard_count_for(budget_bytes),
            CacheLayout::default(),
        )
    }

    /// `shards` must be a nonzero power of two; callers pass constants.
    #[cfg(test)]
    fn new_with_shards(budget_bytes: usize, policy: ReadCachePolicy, shards: usize) -> Self {
        assert!(shards.is_power_of_two(), "shard count is a power of two");
        Self::build(budget_bytes, policy, shards, CacheLayout::default())
    }

    #[cfg(test)]
    fn new_seeded(budget_bytes: usize, seed: u64) -> Self {
        let layout = CacheLayout {
            placement_seed: Some(seed),
        };
        Self::build(
            budget_bytes,
            ReadCachePolicy::default(),
            Self::shard_count_for(budget_bytes),
            layout,
        )
    }

    fn build(
        budget_bytes: usize,
        policy: ReadCachePolicy,
        shards: usize,
        layout: CacheLayout,
    ) -> Self {
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
                        layout,
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
            layout,
        }
    }

    /// False when the budget holds no table slot. A disabled cache takes no lock.
    pub(super) fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Stable routing: operation id and member index. Independent of the
    /// per-table placement hash, which keeps its own random seed.
    fn route(&self, reference: &ValueRecordReference) -> usize {
        let hash = StableHasher::new()
            .bytes(&reference.batch_id.operation)
            .bytes(&reference.descriptor.operation_index.to_le_bytes())
            .finish();
        (hash as usize) & self.mask
    }

    pub(super) fn get(&self, reference: &ValueRecordReference) -> Option<Mutation> {
        let shard = &self.shards[self.route(reference)];
        if !self.enabled {
            shard.counters.record_miss(reference.descriptor.keyspace);
            kv_capture::reference(12, reference, None, 2, 0);
            return None;
        }
        let result = shard.table.lock().get(reference);
        kv_capture::reference(12, reference, None, u64::from(result.is_some()), 0);
        result
    }

    pub(super) fn get_scan(&self, reference: &ValueRecordReference) -> Option<Mutation> {
        let shard = &self.shards[self.route(reference)];
        if !self.enabled {
            shard.counters.record_miss(reference.descriptor.keyspace);
            kv_capture::reference(12, reference, None, 2, 1);
            return None;
        }
        let result = shard.table.lock().get_scan(reference);
        kv_capture::reference(12, reference, None, u64::from(result.is_some()), 1);
        result
    }

    pub(super) fn get_point(
        &self,
        reference: &ValueRecordReference,
        keyspace: crate::key_shape::KeySpace,
        key: &[u8],
    ) -> Option<Bytes> {
        let shard = &self.shards[self.route(reference)];
        if !self.enabled {
            shard.counters.record_miss(reference.descriptor.keyspace);
            kv_capture::reference(12, reference, Some(key), 2, 0);
            return None;
        }
        let result = shard.table.lock().get_point(reference, keyspace, key);
        kv_capture::reference(12, reference, Some(key), u64::from(result.is_some()), 0);
        result
    }

    /// Admit only a mutation returned by the verified storage-client read of
    /// this exact reference, or a canonical mutation bound to an authenticated
    /// durable receipt after successful contiguous publication. Shape checks
    /// catch association errors; they do not replace either provenance proof.
    /// Rejections leave serving correctness and the existing entry untouched.
    /// Advisory preflight precedes detached allocation. No slot reservation or
    /// guard crosses copying, and insertion rechecks current cache state.
    pub(super) fn prepare_verified(
        &self,
        reference: ValueRecordReference,
        mutation: &Mutation,
    ) -> ShardedPreparation {
        let shard = self.route(&reference);
        let table = &self.shards[shard].table;
        let preparation =
            CacheTable::prepare_with(self.shard_budget_bytes, reference, mutation, |payload| {
                table.lock().preflight(&reference, payload)
            });
        trace_preparation(&reference, mutation, &preparation, false);
        ShardedPreparation { shard, preparation }
    }

    pub(super) fn prepare_post_publish(
        &self,
        reference: ValueRecordReference,
        mutation: &Mutation,
    ) -> ShardedPreparation {
        let shard = self.route(&reference);
        let table = &self.shards[shard].table;
        let preparation =
            CacheTable::prepare_with(self.shard_budget_bytes, reference, mutation, |payload| {
                let mut table = table.lock();
                if table
                    .policy
                    .skips_post_publish_warming(reference.descriptor.keyspace)
                {
                    None
                } else {
                    table.preflight(&reference, payload)
                }
            });
        trace_preparation(&reference, mutation, &preparation, true);
        ShardedPreparation { shard, preparation }
    }

    /// Only a prepared entry reaches a table. Every rejection is counted with
    /// atomics alone, so a disabled cache never locks here.
    pub(super) fn insert_prepared(&self, prepared: ShardedPreparation) -> bool {
        let shard = &self.shards[prepared.shard & self.mask];
        match prepared.preparation {
            InsertPreparation::Prepared(entry) => {
                if kv_capture::enabled() {
                    let reference = entry.reference;
                    let inserted = shard.table.lock().insert_entry(entry);
                    kv_capture::reference(14, &reference, None, u64::from(inserted), 0);
                    inserted
                } else {
                    shard.table.lock().insert_entry(entry)
                }
            }
            rejected => {
                shard.counters.record_rejection(&rejected);
                false
            }
        }
    }

    #[cfg(test)]
    fn insert_verified(&self, reference: ValueRecordReference, mutation: &Mutation) -> bool {
        self.insert_prepared(self.prepare_verified(reference, mutation))
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
    /// `version` whose key `dropped` places in the dropped cell range. Index
    /// masks already hide these references from current reads; this returns
    /// their memory. Locks one shard at a time and never holds two; the
    /// matcher runs under that shard lock and takes no other lock. A newer
    /// point reference in the range is kept.
    pub(super) fn evict_dropped(
        &self,
        keyspace: crate::key_shape::KeySpace,
        version: crate::distributed::types::BatchVersion,
        dropped: impl Fn(&[u8]) -> bool,
    ) {
        if !self.enabled {
            return;
        }
        for shard in self.shards.iter() {
            let mut table = shard.table.lock();
            for slot in 0..table.slots.len() {
                let evict = table.slots[slot].as_ref().is_some_and(|entry| {
                    entry.reference.descriptor.keyspace == keyspace.0
                        && entry.reference.batch_version < version
                        && entry.mutation.point_key().is_some_and(|key| dropped(key))
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
                self.layout,
            );
        }
    }
}

fn trace_preparation(
    reference: &ValueRecordReference,
    mutation: &Mutation,
    preparation: &InsertPreparation,
    post_publish: bool,
) {
    if !kv_capture::enabled() {
        return;
    }
    let outcome = match preparation {
        InsertPreparation::Prepared(_) => 1,
        InsertPreparation::Disabled => 2,
        InsertPreparation::Mismatch => 3,
        InsertPreparation::Oversize => 4,
        InsertPreparation::Admission => 5,
    };
    kv_capture::reference(
        13,
        reference,
        mutation.point_key().map(|key| &key[..]),
        outcome,
        u64::from(post_publish),
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed::types::{
        BatchId, BatchVersion, MemberChecksum, PayloadDigest, PhysicalValueAddress,
        RecordDescriptor,
    };
    use crate::key_shape::{KeyShapeBuilder, KeySpace, KeyType};

    const SINGLE_SET_BUDGET: usize = WAYS * (FIXED_SLOT_BYTES + MIN_ENTRY_CHARGE);

    fn protected_policy(keyspace: u8) -> ReadCachePolicy {
        ReadCachePolicy {
            protected_keyspace: Some(keyspace),
            ..ReadCachePolicy::default()
        }
    }
    fn record(version: u64, value: &[u8]) -> (ValueRecordReference, Mutation) {
        let key = Bytes::from_static(b"same-key");
        let reference = ValueRecordReference {
            batch_id: BatchId {
                database: [1; 16],
                operation: version.to_le_bytes().repeat(2).try_into().unwrap(),
            },
            batch_version: BatchVersion {
                epoch: 1,
                sequence: version,
            },
            batch_digest: PayloadDigest([2; 32]),
            batch_address: PhysicalValueAddress {
                partition: 0,
                segment_generation: 1,
                offset: version * 1024,
                len: 1024,
            },
            descriptor: RecordDescriptor {
                operation_index: 0,
                offset: 0,
                len: 1024,
                keyspace: 0,
                kind: RecordKind::Put,
                key_len: key.len() as u32,
                value_offset: 32,
                value_len: value.len() as u32,
                checksum: MemberChecksum(3),
            },
        };
        (
            reference,
            Mutation::Put {
                keyspace: KeySpace(0),
                key,
                value: Bytes::copy_from_slice(value),
            },
        )
    }

    fn record_for_keyspace(
        version: u64,
        value: &[u8],
        keyspace: u8,
    ) -> (ValueRecordReference, Mutation) {
        let (mut reference, mut mutation) = record(version, value);
        reference.descriptor.keyspace = keyspace;
        if let Mutation::Put {
            keyspace: mutation_keyspace,
            ..
        } = &mut mutation
        {
            *mutation_keyspace = KeySpace(keyspace);
        }
        (reference, mutation)
    }

    #[test]
    fn insertion_bypasses_after_bounded_clock_work() {
        let budget = 1024 * 1024;
        let mut cache = CacheTable::new(budget);
        for version in 0..4096 {
            let (reference, mutation) = record(version, &[9; 1024]);
            assert!(cache.get(&reference).is_none()); // Demand fill, not one-pass writes.
            assert!(cache.insert_verified(reference, &mutation));
            cache.get(&reference).unwrap();
            if cache.stats().entries >= 128 {
                break;
            }
        }
        assert!(cache.stats().entries >= 128);
        let before = cache.stats();
        let payload = budget - before.metadata_bytes - ENTRY_BYTES - 8 - 2 * BYTE_OWNER_BYTES;
        let (reference, mutation) = record(10000, &vec![7; payload]);
        assert!(cache.get(&reference).is_none());
        assert!(!cache.insert_verified(reference, &mutation));
        let after = cache.stats();
        assert_eq!(
            after.bypass_eviction_limit,
            before.bypass_eviction_limit + 1
        );
        assert!(after.evictions - before.evictions <= MAX_EVICTION_STEPS as u64 + 1);
        assert!(after.retained_bytes <= budget);
        assert!(cache.get(&reference).is_none());
    }

    #[test]
    fn large_budget_metadata_is_predictable_without_allocating() {
        let budget = 4usize * 1024 * 1024 * 1024;
        let metadata = ReadCache::metadata_bytes(budget);
        assert!(metadata > ReadCache::metadata_bytes(256 * 1024 * 1024));
        assert!(metadata < budget);
        assert!(ReadCache::metadata_bytes(usize::MAX) < usize::MAX);
    }

    #[test]
    fn disabled_and_too_small_budgets_allocate_nothing() {
        let (reference, mutation) = record(1, b"value");
        for budget in [0, 1, SINGLE_SET_BUDGET - 1] {
            let mut cache = CacheTable::new(budget);
            assert!(cache.slots.is_empty());
            assert!(!cache.insert_verified(reference, &mutation));
            assert!(cache.get(&reference).is_none());
            assert_eq!(cache.stats().retained_bytes, 0);
            assert_eq!(cache.stats().misses, 1);
            let protected_cache = CacheTable::new_with_policy(budget, protected_policy(0));
            assert!(protected_cache.slots.is_empty());
            assert_eq!(protected_cache.stats().retained_bytes, 0);
        }
    }

    #[test]
    fn exact_reference_separates_current_and_checkpoint_versions() {
        let mut cache = CacheTable::new(64 * 1024);
        let (old_ref, old) = record(1, b"old");
        let (new_ref, new) = record(2, b"new");
        assert!(cache.insert_verified(old_ref, &old));
        assert!(cache.get(&new_ref).is_none());
        assert!(cache.insert_verified(new_ref, &new));
        assert_eq!(cache.get(&new_ref), Some(new));
        assert_eq!(cache.get(&old_ref), Some(old));
        for altered in [
            ValueRecordReference {
                batch_address: PhysicalValueAddress {
                    segment_generation: 2,
                    ..old_ref.batch_address
                },
                ..old_ref
            },
            ValueRecordReference {
                batch_digest: PayloadDigest([4; 32]),
                ..old_ref
            },
            ValueRecordReference {
                descriptor: RecordDescriptor {
                    checksum: MemberChecksum(4),
                    ..old_ref.descriptor
                },
                ..old_ref
            },
            ValueRecordReference {
                batch_id: BatchId {
                    database: [8; 16],
                    ..old_ref.batch_id
                },
                ..old_ref
            },
        ] {
            assert!(cache.get(&altered).is_none());
        }
    }

    #[test]
    fn point_lookup_checks_full_reference_after_fingerprint_collision() {
        let mut cache = CacheTable::new(64 * 1024);
        let (reference, mutation) = record(1, b"value");
        assert!(cache.insert_verified(reference, &mutation));
        let changed_generation = ValueRecordReference {
            batch_address: PhysicalValueAddress {
                segment_generation: reference.batch_address.segment_generation + 1,
                ..reference.batch_address
            },
            ..reference
        };
        let original = cache.slots.iter().position(Option::is_some).unwrap();
        let target = cache.set_start_for_ref(&changed_generation);
        cache.slots.swap(original, target);
        cache.tags.swap(original, target);
        // Force a tag collision. The different full reference must still miss.
        cache.tags[target] = cache.fingerprint(&changed_generation);
        let keyspace = crate::key_shape::KeySpace(reference.descriptor.keyspace);
        assert_eq!(
            cache.get_point(&changed_generation, keyspace, b"same-key"),
            None
        );
        cache.slots.swap(original, target);
        cache.tags.swap(original, target);
        cache.tags[original] = cache.fingerprint(&reference);
        assert_eq!(
            cache.get_point(&reference, keyspace, b"same-key"),
            Some(Bytes::copy_from_slice(b"value"))
        );
        assert_eq!(cache.stats().hits, 1);
        assert_eq!(cache.stats().misses, 1);
    }

    #[test]
    fn duplicate_fill_does_not_reallocate_or_change_accounting() {
        let mut cache = CacheTable::new(16 * 1024);
        let (reference, mutation) = record(1, b"value");
        assert!(cache.insert_verified(reference, &mutation));
        let before = cache.stats();
        let stored = cache.get(&reference).unwrap();
        assert!(cache.insert_verified(reference, &mutation));
        let after = cache.stats();
        assert_eq!(before.retained_bytes, after.retained_bytes);
        assert_eq!(after.inserts, 1);
        assert_eq!(after.entries, 1);
        assert_eq!(
            cache.get(&reference).unwrap().point_key().unwrap().as_ptr(),
            stored.point_key().unwrap().as_ptr()
        );
    }

    #[test]
    fn every_fill_is_detached_from_large_allocations() {
        let owner = Bytes::from(vec![42; 2 * 1024 * 1024]);
        let (reference, _) = record(1, &[42; 16]);
        let mutation = Mutation::Put {
            keyspace: KeySpace(0),
            key: owner.slice(0..8),
            value: owner.slice(8..24),
        };
        let mut cache = CacheTable::new(16 * 1024);
        assert!(cache.insert_verified(reference, &mutation));
        let Mutation::Put { key, value, .. } = cache.get(&reference).unwrap() else {
            panic!()
        };
        assert_ne!(key.as_ptr(), mutation.point_key().unwrap().as_ptr());
        assert_eq!(key.downcast_ref::<Box<[u8]>>().unwrap().len(), 8);
        assert_eq!(value.downcast_ref::<Box<[u8]>>().unwrap().len(), 16);
        assert_eq!(
            cache.stats().retained_bytes - cache.stats().metadata_bytes,
            24 + 2 * BYTE_OWNER_BYTES + ENTRY_BYTES
        );
    }

    #[test]
    fn policy_oversized_demand_preserves_resident_entries() {
        let budget = 8 * 1024;
        let cache = ReadCache::new_with_policy(budget, protected_policy(0));
        let resident: Vec<_> = (0..WAYS as u64)
            .map(|version| record_for_keyspace(version, &[7; 512], 1))
            .collect();
        for (reference, mutation) in &resident {
            assert!(cache.insert_verified(*reference, mutation));
        }
        let regular_limit = {
            let table = cache.shards[0].table.lock();
            budget - table.metadata_bytes - table.protected_reserve_bytes
        };
        let (reference, mutation) = record_for_keyspace(100, &vec![5; regular_limit + 1], 1);
        let evictions = cache.stats().evictions;
        for _ in 0..2 {
            let prepared = cache.prepare_verified(reference, &mutation);
            assert!(!cache.insert_prepared(prepared));
        }
        assert_eq!(cache.stats().evictions, evictions);
        for (reference, mutation) in resident {
            assert_eq!(cache.get_scan(&reference), Some(mutation));
        }
    }

    #[test]
    fn oversized_and_mismatched_fills_leave_existing_data_untouched() {
        let mut cache = CacheTable::new(16 * 1024);
        let (reference, mutation) = record(1, b"value");
        assert!(cache.insert_verified(reference, &mutation));
        let (large_ref, large) = record(2, &[7; 16 * 1024]);
        assert!(!cache.insert_verified(large_ref, &large));
        let mut mismatch = reference;
        mismatch.descriptor.value_len += 1;
        assert!(!cache.insert_verified(mismatch, &mutation));
        assert_eq!(cache.get(&reference), Some(mutation));
        assert_eq!(cache.stats().entries, 1);
        assert_eq!(cache.stats().rejected_oversize, 1);
        assert_eq!(cache.stats().rejected_mismatch, 1);
    }

    #[test]
    fn sustained_pressure_bounds_payload_plus_all_reserved_metadata() {
        let mut cache = CacheTable::new(16 * 1024);
        let mut held = Vec::new();
        for i in 1..200 {
            let bytes = vec![i as u8; 256 + (i as usize % 16) * 64];
            let (reference, mutation) = record(i, &bytes);
            assert!(cache.get(&reference).is_none());
            let before = cache.stats();
            if cache.insert_verified(reference, &mutation) {
                assert_eq!(cache.get(&reference), Some(mutation));
                if held.len() < 4 {
                    held.push(cache.get(&reference).unwrap());
                }
            } else {
                // More slots can require more than the bounded 64 CLOCK steps.
                let after = cache.stats();
                assert_eq!(after.inserts, before.inserts);
                assert_eq!(
                    after.bypass_admission + after.bypass_eviction_limit,
                    before.bypass_admission + before.bypass_eviction_limit + 1
                );
                assert!(cache.get(&reference).is_none());
            }
            let stats = cache.stats();
            assert!(stats.retained_bytes <= stats.budget_bytes);
            assert_eq!(stats.metadata_bytes, cache.slots.len() * FIXED_SLOT_BYTES);
            assert_eq!(stats.entries, cache.slots.iter().flatten().count());
            assert_eq!(
                stats.retained_bytes,
                stats.metadata_bytes
                    + cache
                        .slots
                        .iter()
                        .flatten()
                        .map(|entry| entry.charge())
                        .sum::<usize>()
            );
        }
        assert!(cache.stats().evictions > 0);
        assert_eq!(held.len(), 4); // Application-owned clones survive safe eviction.
    }

    #[test]
    fn tombstone_and_empty_value_remain_distinct() {
        let mut cache = CacheTable::new(16 * 1024);
        let (put_ref, put) = record(1, b"");
        let (mut del_ref, _) = record(2, b"");
        del_ref.descriptor.kind = RecordKind::Delete;
        let delete = Mutation::Delete {
            keyspace: KeySpace(0),
            key: put.point_key().unwrap().clone(),
        };
        assert!(cache.insert_verified(put_ref, &put));
        assert!(cache.insert_verified(del_ref, &delete));
        assert_eq!(cache.get(&put_ref), Some(put));
        assert_eq!(cache.get(&del_ref), Some(delete));
    }

    #[test]
    fn local_clock_protects_reused_entries_before_single_use_collisions() {
        let mut cache = CacheTable::new(32 * 1024);
        let mut records = Vec::new();
        let target_set = cache.set_start_for_ref(&record(1, b"value").0);
        for i in 1..10_000 {
            let pair = record(i, b"value");
            if cache.set_start_for_ref(&pair.0) == target_set {
                records.push(pair);
                if records.len() == WAYS + 1 {
                    break;
                }
            }
        }
        assert_eq!(records.len(), WAYS + 1);
        for (reference, mutation) in &records[..WAYS] {
            assert!(cache.insert_verified(*reference, mutation));
        }
        assert!(cache.get(&records[0].0).is_some());
        assert!(cache.insert_verified(records[WAYS].0, &records[WAYS].1));
        assert!(cache.get(&records[0].0).is_some());
        assert!(cache.get(&records[1].0).is_none());
        assert_eq!(cache.stats().entries, WAYS);
        assert_eq!(cache.stats().evictions, 1);
    }

    #[test]
    fn write_churn_reuses_probation_without_displacing_read_hot_values() {
        let mut cache = CacheTable::new(SINGLE_SET_BUDGET);
        assert_eq!(cache.slots.len(), WAYS); // One deterministic collision set.
        let records: Vec<_> = (0..100).map(|version| record(version, &[7; 64])).collect();
        for (reference, mutation) in &records[..WAYS] {
            assert!(cache.insert_verified(*reference, mutation));
        }
        let allocations: Vec<_> = cache
            .slots
            .iter()
            .flatten()
            .map(|entry| std::ptr::from_ref(entry.as_ref()))
            .collect();
        for (reference, _) in &records[..WAYS - 1] {
            for _ in 0..3 {
                assert!(cache.get(reference).is_some());
            }
        }
        for (reference, mutation) in &records[WAYS..] {
            assert!(cache.insert_verified(*reference, mutation));
            assert!(cache.stats().retained_bytes <= cache.stats().budget_bytes);
        }
        for (reference, mutation) in &records[..WAYS - 1] {
            assert_eq!(cache.get(reference), Some(mutation.clone()));
        }
        // New writes still get probationary space and immediate read-after-write.
        let (reference, mutation) = records.last().unwrap();
        assert_eq!(cache.get(reference), Some(mutation.clone()));
        assert_eq!(cache.history.len(), cache.slots.len());
        assert!(
            cache
                .slots
                .iter()
                .flatten()
                .all(|entry| allocations.contains(&std::ptr::from_ref(entry.as_ref())))
        );
    }

    #[test]
    fn repeated_miss_earns_admission_but_one_touch_cannot_replace_a_hot_set() {
        let mut cache = CacheTable::new(SINGLE_SET_BUDGET);
        for version in 0..WAYS as u64 {
            let (reference, mutation) = record(version, b"value");
            assert!(cache.insert_verified(reference, &mutation));
            assert!(cache.get(&reference).is_some());
        }
        let (reference, mutation) = record(100, b"new-value");
        let before = cache.stats();
        assert!(!cache.insert_verified(reference, &mutation));
        assert_eq!(cache.stats().bypass_admission, before.bypass_admission + 1);
        assert_eq!(cache.stats().evictions, before.evictions);
        assert!(cache.get(&reference).is_none());
        assert!(cache.insert_verified(reference, &mutation));
        assert_eq!(cache.get(&reference), Some(mutation));
    }

    #[test]
    fn scan_hits_and_misses_do_not_create_admission_or_reuse_history() {
        let mut cache = CacheTable::new(8 * 1024);
        let (reference, mutation) = record(1, b"value");
        assert!(cache.insert_verified(reference, &mutation));
        let before: Vec<_> = cache
            .history
            .iter()
            .map(|entry| (entry.fingerprint, entry.observations))
            .collect();
        assert_eq!(cache.get_scan(&reference), Some(mutation));
        assert!(cache.get_scan(&record(2, b"other").0).is_none());
        let after: Vec<_> = cache
            .history
            .iter()
            .map(|entry| (entry.fingerprint, entry.observations))
            .collect();
        assert_eq!(before, after);
        assert_eq!(cache.slots.iter().flatten().next().unwrap().benefit, 0);
    }

    #[test]
    fn admission_preflight_skips_detachment_without_double_counting_cold_writes() {
        let budget = SINGLE_SET_BUDGET;
        let cache = ReadCache::new(budget);
        for version in 0..WAYS as u64 {
            let (reference, mutation) = record(version, b"value");
            let prepared = cache.prepare_verified(reference, &mutation);
            assert!(cache.insert_prepared(prepared));
        }
        // Passing both preparation and insertion did not promote any cold write.
        assert!(
            cache.shards[0]
                .table
                .lock()
                .slots
                .iter()
                .flatten()
                .all(|entry| entry.benefit == 0)
        );
        for version in 0..WAYS as u64 {
            assert!(cache.get(&record(version, b"value").0).is_some());
        }
        let (reference, mutation) = record(100, b"new-value");
        let rejected = cache.prepare_verified(reference, &mutation);
        assert!(matches!(rejected.preparation, InsertPreparation::Admission));
        assert!(!cache.insert_prepared(rejected));
        let mut mismatch = reference;
        mismatch.descriptor.value_len += 1;
        let invalid = cache.prepare_verified(mismatch, &mutation);
        assert!(matches!(invalid.preparation, InsertPreparation::Mismatch));
        assert!(!cache.insert_prepared(invalid));
        let accepted = cache.prepare_verified(reference, &mutation);
        assert!(matches!(
            accepted.preparation,
            InsertPreparation::Prepared(_)
        ));
        assert!(cache.insert_prepared(accepted));
        assert_eq!(cache.get(&reference), Some(mutation));
        assert_eq!(cache.stats().bypass_admission, 1);
        assert_eq!(cache.stats().rejected_mismatch, 1);
    }

    #[test]
    fn per_keyspace_counters_follow_lookup_and_evicted_entry_ownership() {
        let mut cache = CacheTable::new(8 * 1024);
        let (target, target_mutation) = record_for_keyspace(0, b"target", 0);
        let target_set = cache.set_start_for_ref(&target);
        assert!(cache.get_scan(&target).is_none());
        assert!(cache.insert_verified(target, &target_mutation));

        let mut inserted = Vec::new();
        for version in 1..10_000 {
            let pair = record_for_keyspace(version, b"value", 1);
            if cache.set_start_for_ref(&pair.0) == target_set {
                assert!(cache.get(&pair.0).is_none());
                assert!(cache.insert_verified(pair.0, &pair.1));
                inserted.push(pair);
                if inserted.len() == WAYS - 1 {
                    break;
                }
            }
        }
        assert_eq!(inserted.len(), WAYS - 1);
        assert!(cache.get(&inserted[0].0).is_some());

        let mut overflow = None;
        for version in 10_000..20_000 {
            let pair = record_for_keyspace(version, b"overflow", 1);
            if cache.set_start_for_ref(&pair.0) == target_set {
                overflow = Some(pair);
                break;
            }
        }
        let (overflow_ref, overflow_mutation) = overflow.expect("collision candidate");
        assert!(cache.insert_verified(overflow_ref, &overflow_mutation));

        let stats = cache.per_keyspace_stats();
        assert_eq!(
            stats[0],
            PerKeyspaceReadCacheStats {
                misses: 1,
                inserts: 1,
                evictions: 1,
                ..PerKeyspaceReadCacheStats::default()
            }
        );
        assert_eq!(stats[1].hits, 1);
        assert_eq!(stats[1].misses, (WAYS - 1) as u64);
        assert_eq!(stats[1].inserts, WAYS as u64);
        assert_eq!(stats[1].evictions, 0);
        assert_eq!(
            cache.stats().per_keyspace_counters_bytes,
            256 * size_of::<PerKeyspaceReadCacheStats>()
        );
    }
    #[test]
    fn cached_fill_does_not_retain_a_custom_large_byte_owner() {
        use std::sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        };
        struct Owner {
            bytes: Vec<u8>,
            dropped: Arc<AtomicBool>,
        }
        impl AsRef<[u8]> for Owner {
            fn as_ref(&self) -> &[u8] {
                &self.bytes
            }
        }
        impl minibytes::BytesOwner for Owner {}
        impl Drop for Owner {
            fn drop(&mut self) {
                self.dropped.store(true, Ordering::SeqCst);
            }
        }
        let dropped = Arc::new(AtomicBool::new(false));
        let owner = Bytes::from_owner(Owner {
            bytes: vec![42; 1024 * 1024],
            dropped: dropped.clone(),
        });
        let (reference, _) = record(1, &[42; 16]);
        let mutation = Mutation::Put {
            keyspace: KeySpace(0),
            key: owner.slice(..8),
            value: owner.slice(8..24),
        };
        let mut cache = CacheTable::new(16 * 1024);
        assert!(cache.insert_verified(reference, &mutation));
        drop(mutation);
        drop(owner);
        assert!(dropped.load(Ordering::SeqCst));
        assert!(cache.get(&reference).is_some());
    }
    #[test]
    fn scalar_hits_clone_only_value_and_reject_wrong_key_or_keyspace() {
        let mut cache = CacheTable::new(16 * 1024);
        let (reference, mutation) = record(1, b"value");
        assert!(cache.insert_verified(reference, &mutation));
        let stored_value_ptr = match &cache.slots.iter().flatten().next().unwrap().mutation {
            Mutation::Put { value, .. } => value.as_ptr(),
            _ => panic!("inserted record is a put"),
        };
        let hit = cache
            .get_point(&reference, KeySpace(0), b"same-key")
            .unwrap();
        assert_eq!(hit.as_ptr(), stored_value_ptr);
        assert!(
            cache
                .get_point(&reference, KeySpace(1), b"same-key")
                .is_none()
        );
        assert!(
            cache
                .get_point(&reference, KeySpace(0), b"wrong-key")
                .is_none()
        );
        let (other_reference, _) = record(2, b"value");
        assert!(
            cache
                .get_point(&other_reference, KeySpace(0), b"same-key")
                .is_none()
        );
        assert_eq!(cache.stats().hits, 1);
        assert_eq!(cache.stats().misses, 3);
    }

    #[test]
    fn regular_demand_can_fill_before_protected_writes() {
        let mut cache = CacheTable::new_with_policy(8 * 1024, protected_policy(0));
        let (reference, mutation) = record_for_keyspace(1, b"contents", 1);
        assert!(cache.insert_verified(reference, &mutation));
        assert_eq!(cache.get_scan(&reference), Some(mutation));
        assert!(cache.stats().retained_bytes <= cache.stats().budget_bytes);
    }

    #[test]
    fn protected_summary_entries_survive_streaming_warm_and_demand_churn() {
        let (hot_reference, hot_mutation) = record_for_keyspace(0, b"summary", 0);
        let mut cache = CacheTable::new_with_policy(8 * 1024, protected_policy(0));
        let hot_set = cache.set_start_for_ref(&hot_reference);
        assert!(cache.insert_verified(hot_reference, &hot_mutation));
        for _ in 0..3 {
            assert!(cache.get(&hot_reference).is_some());
        }

        let mut stream = Vec::new();
        for version in 1..10_000 {
            let pair = record_for_keyspace(version, &[7; 512], 1);
            if cache.set_start_for_ref(&pair.0) == hot_set {
                assert!(cache.insert_verified(pair.0, &pair.1));
                stream.push(pair);
                if stream.len() == WAYS - 1 {
                    break;
                }
            }
        }
        assert_eq!(stream.len(), WAYS - 1);

        for version in 10_000..20_000 {
            let (reference, mutation) = record_for_keyspace(version, &[9; 512], 1);
            if cache.set_start_for_ref(&reference) == hot_set {
                cache.insert_verified(reference, &mutation);
            }
            assert_eq!(cache.get(&hot_reference), Some(hot_mutation.clone()));
            assert!(cache.stats().retained_bytes <= cache.stats().budget_bytes);
        }
        let payload_budget = cache.stats().budget_bytes - cache.stats().metadata_bytes;
        assert!(cache.regular_retained_bytes <= payload_budget - cache.protected_reserve_bytes);
        assert!(cache.protected_retained_bytes > 0);
    }
    #[test]
    fn sui_shape_skip_set_exempts_only_low_reuse_aliases() {
        let mut builder = KeyShapeBuilder::new();
        for name in [
            "checkpoint_content",
            "certified_checkpoints",
            "checkpoint_by_digest",
            "full_checkpoint_content_v2",
            "checkpoint_sequence_by_contents_digest",
            "watermarks",
            "ordinary_application_data",
        ] {
            builder.add_key_space(name, 1, 1, KeyType::uniform(1));
        }
        let shape = builder.build();
        let budget = 32 * 1024;
        let cache = ReadCache::new_with_policy(budget, ReadCachePolicy::for_sui_shape(&shape));

        for (version, keyspace) in shape.iter_ks().enumerate() {
            let (reference, mutation) =
                record_for_keyspace(version as u64 + 1, b"checkpoint-value", keyspace.id().0);
            let skip_warm = matches!(
                keyspace.name(),
                "checkpoint_by_digest" | "checkpoint_sequence_by_contents_digest" | "watermarks"
            );
            let prepared = cache.prepare_post_publish(reference, &mutation);
            if skip_warm {
                assert!(!cache.insert_prepared(prepared));
                assert_eq!(cache.get_scan(&reference), None);
                let demand_fill = cache.prepare_verified(reference, &mutation);
                assert!(cache.insert_prepared(demand_fill));
            } else {
                assert!(cache.insert_prepared(prepared));
            }
            assert_eq!(cache.get_scan(&reference), Some(mutation));
        }

        // Sui epoch/perpetual shapes lack the protected checkpoint family and
        // therefore keep ordinary read-after-write warming for alias-like names.
        let mut builder = KeyShapeBuilder::new();
        builder.add_key_space("checkpoint_by_digest", 1, 1, KeyType::uniform(1));
        builder.add_key_space("watermarks", 1, 1, KeyType::uniform(1));
        let non_checkpoint_shape = builder.build();
        let non_checkpoint = ReadCache::new_with_policy(
            budget,
            ReadCachePolicy::for_sui_shape(&non_checkpoint_shape),
        );
        let (reference, mutation) = record_for_keyspace(20, b"contents", 0);
        let warmed = non_checkpoint.prepare_post_publish(reference, &mutation);
        assert!(non_checkpoint.insert_prepared(warmed));
        assert_eq!(non_checkpoint.get_scan(&reference), Some(mutation));

        let generic = ReadCache::new(budget);
        let (reference, mutation) = record_for_keyspace(21, b"digest-alias", 2);
        let warmed = generic.prepare_post_publish(reference, &mutation);
        assert!(generic.insert_prepared(warmed));
        assert_eq!(generic.get_scan(&reference), Some(mutation));
    }

    #[test]
    fn regular_demand_reclaims_only_protected_capacity_above_the_floor() {
        let mut cache = CacheTable::new_with_policy(16 * 1024, protected_policy(0));
        let target_set = cache.set_start_for_ref(&record_for_keyspace(0, b"x", 0).0);
        let mut protected_records = Vec::new();
        for version in 0..10_000 {
            let pair = record_for_keyspace(version, &[3; 2048], 0);
            if cache.set_start_for_ref(&pair.0) == target_set {
                protected_records.push(pair);
                if protected_records.len() == 3 {
                    break;
                }
            }
        }
        assert_eq!(protected_records.len(), 3);
        for (reference, mutation) in &protected_records {
            assert!(cache.insert_verified(*reference, mutation));
            for _ in 0..3 {
                assert!(cache.get(reference).is_some());
            }
        }
        let protected_before = cache.protected_retained_bytes;
        assert!(protected_before > cache.protected_reserve_bytes);

        let mut reclaimed_borrowed_bytes = false;
        for version in 10_000..20_000 {
            let (reference, mutation) = record_for_keyspace(version, &[8; 2048], 1);
            assert!(cache.get(&reference).is_none());
            cache.insert_verified(reference, &mutation);
            if cache.protected_retained_bytes < protected_before {
                reclaimed_borrowed_bytes = true;
                break;
            }
        }
        assert!(reclaimed_borrowed_bytes);
        assert!(cache.protected_retained_bytes >= cache.protected_reserve_bytes);
        assert!(cache.stats().retained_bytes <= cache.stats().budget_bytes);
    }

    /// Runs `work` on another thread and fails the test, instead of hanging,
    /// when it does not finish: that means it blocked on a lock the caller holds.
    fn finishes_within_deadline<R: Send + 'static>(work: impl FnOnce() -> R + Send + 'static) -> R {
        let (done, result) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            let _ = done.send(work());
        });
        let value = result
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("operation blocked on a lock held by the test");
        worker.join().unwrap();
        value
    }

    #[test]
    fn disabled_cache_takes_no_lock_and_counts_with_atomics() {
        let cache = Arc::new(ReadCache::new(0));
        assert!(!cache.is_enabled());
        assert_eq!(ReadCache::metadata_bytes(0), 0);
        // Every table lock is held by this thread. A disabled operation that
        // tried to lock any shard would block and fail the deadline.
        let guards: Vec<_> = cache
            .shards
            .iter()
            .map(|shard| shard.table.lock())
            .collect();
        let operations = 64u64;
        let worker_cache = Arc::clone(&cache);
        finishes_within_deadline(move || {
            for version in 0..operations {
                let (reference, mutation) = record(version, b"value");
                assert!(worker_cache.get(&reference).is_none());
                assert!(worker_cache.get_scan(&reference).is_none());
                assert!(
                    worker_cache
                        .get_point(&reference, KeySpace(0), b"same-key")
                        .is_none()
                );
                let prepared = worker_cache.prepare_verified(reference, &mutation);
                assert!(matches!(prepared.preparation, InsertPreparation::Disabled));
                assert!(!worker_cache.insert_prepared(prepared));
                let warmed = worker_cache.prepare_post_publish(reference, &mutation);
                assert!(!worker_cache.insert_prepared(warmed));
            }
        });
        drop(guards);
        for shard in cache.shards.iter() {
            assert_eq!(
                shard.table.stats().acquisitions,
                1,
                "only the test itself locked the shard"
            );
        }
        let stats = cache.stats();
        assert_eq!(stats.misses, 3 * operations);
        assert_eq!(stats.bypass_disabled, 2 * operations);
        assert_eq!(stats.retained_bytes, 0);
        assert_eq!(
            cache.per_keyspace_stats()[0].misses,
            3 * operations,
            "per-keyspace misses are summed across stripes"
        );
        // Disabled misses spread over several stripes, not one hot counter.
        let loaded = cache
            .shard_stats()
            .shards
            .iter()
            .filter(|shard| shard.misses != 0)
            .count();
        assert!(loaded > 1);
    }

    #[test]
    fn shards_are_independent_and_statistics_sum_across_them() {
        let cache = Arc::new(ReadCache::new_with_shards(
            4 * 1024 * 1024,
            ReadCachePolicy::default(),
            4,
        ));
        assert!(cache.is_enabled());
        let mut by_shard: Vec<Vec<_>> = vec![Vec::new(); 4];
        for version in 0..200 {
            let pair = record(version, b"value");
            by_shard[cache.route(&pair.0)].push(pair);
        }
        assert!(by_shard.iter().all(|records| records.len() >= 20));

        // A thread holding shard 0's table mutex does not delay another shard.
        let held = cache.shards[0].table.lock();
        let worker_cache = Arc::clone(&cache);
        let others: Vec<_> = by_shard[1..].iter().flatten().take(30).cloned().collect();
        let expected = others.clone();
        finishes_within_deadline(move || {
            for (reference, mutation) in &others {
                assert!(worker_cache.get(reference).is_none());
                assert!(worker_cache.insert_verified(*reference, mutation));
                assert_eq!(worker_cache.get(reference).as_ref(), Some(mutation));
            }
        });
        drop(held);

        for (reference, mutation) in by_shard[0].iter().take(10) {
            assert!(cache.get(reference).is_none());
            assert!(cache.insert_verified(*reference, mutation));
        }
        let load = cache.shard_stats();
        assert_eq!(load.shards.len(), 4);
        assert_eq!(load.shards[0].entries, 10);
        assert_eq!(
            load.shards.iter().map(|shard| shard.entries).sum::<usize>(),
            40
        );
        let stats = cache.stats();
        assert_eq!(stats.entries, 40);
        assert_eq!(stats.inserts, 40);
        assert_eq!(stats.hits, 30);
        assert_eq!(stats.misses, 40);
        assert_eq!(
            stats.per_keyspace_counters_bytes,
            4 * 256 * size_of::<PerKeyspaceReadCacheStats>()
        );
        assert!(stats.retained_bytes <= stats.budget_bytes);
        for (reference, mutation) in expected {
            assert_eq!(cache.get_scan(&reference), Some(mutation));
        }
    }

    #[test]
    fn clear_resets_entries_and_counters_but_keeps_the_policy() {
        let cache = ReadCache::new_with_shards(1024 * 1024, protected_policy(0), 2);
        for version in 0..20 {
            let (reference, mutation) = record(version, b"value");
            assert!(cache.insert_verified(reference, &mutation));
        }
        assert!(cache.stats().entries > 0);
        cache.clear();
        let stats = cache.stats();
        assert_eq!((stats.entries, stats.inserts, stats.hits), (0, 0, 0));
        assert_eq!(stats.retained_bytes, stats.metadata_bytes);
        assert!(cache.get(&record(1, b"value").0).is_none());
        let table = cache.shards[0].table.lock();
        assert_eq!(table.policy.protected_keyspace, Some(0));
        assert!(table.protected_reserve_bytes > 0);
    }

    #[test]
    fn shard_count_follows_budget_and_small_budgets_stay_single_table() {
        // The pure routing rule: no allocation of multi-gigabyte tables here.
        assert_eq!(ReadCache::shard_count_for(64 * 1024), 1);
        assert_eq!(ReadCache::shard_count_for(MIN_SHARD_BYTES), 1);
        assert_eq!(ReadCache::shard_count_for(3 * MIN_SHARD_BYTES), 2);
        assert_eq!(ReadCache::shard_count_for(64 * MIN_SHARD_BYTES), MAX_SHARDS);
        assert_eq!(ReadCache::shard_count_for(1usize << 40), MAX_SHARDS);
        assert_eq!(ReadCache::shard_count_for(0), DISABLED_STRIPES);
        assert_eq!(ReadCache::new(64 * 1024).shards.len(), 1);
    }

    #[test]
    fn block_backed_fills_never_pin_their_block_and_budget_bounds_retained_memory() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        // A read block returns to its pool only when every slice drops.
        const BLOCK: usize = 8 * 1024;
        static LIVE_BLOCKS: AtomicUsize = AtomicUsize::new(0);
        struct Block(Vec<u8>);
        impl AsRef<[u8]> for Block {
            fn as_ref(&self) -> &[u8] {
                &self.0
            }
        }
        impl minibytes::BytesOwner for Block {}
        impl Drop for Block {
            fn drop(&mut self) {
                LIVE_BLOCKS.fetch_sub(1, Ordering::SeqCst);
            }
        }
        let budget = 64 * 1024;
        let cache = ReadCache::new(budget);
        for version in 0..2000u64 {
            LIVE_BLOCKS.fetch_add(1, Ordering::SeqCst);
            let block = Bytes::from_owner(Block(vec![9; BLOCK]));
            let (reference, _) = record(version, &[9; 1024]);
            let mutation = Mutation::Put {
                keyspace: KeySpace(0),
                key: block.slice(0..8),
                value: block.slice(8..8 + 1024),
            };
            let mut reference = reference;
            reference.descriptor.key_len = 8;
            cache.insert_verified(reference, &mutation);
            drop(mutation);
            drop(block);
            // No cached entry keeps the block alive.
            assert_eq!(LIVE_BLOCKS.load(Ordering::SeqCst), 0);
            let stats = cache.stats();
            assert!(stats.retained_bytes <= stats.budget_bytes);
        }
        let stats = cache.stats();
        assert!(stats.entries > 0);
        // Retained payload is exactly the charged exact-size copies, far below
        // `entries * BLOCK`.
        assert!(stats.retained_bytes - stats.metadata_bytes < stats.entries * 2 * 1024);
    }
}
