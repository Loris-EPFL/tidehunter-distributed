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
//! second-chance eviction. A fixed four-way table keeps every metadata allocation
//! visible to byte accounting without a hash map's retained spare capacity.
//! RemoteDatabase uses `&mut self`; ConcurrentRemoteDatabase adds one short
//! cache mutex around lookup/fill only, never around index or transport work.
//!
//! Sources:
//! <https://github.com/facebook/rocksdb/blob/main/cache/cache_key.h>
//! <https://github.com/facebook/rocksdb/wiki/Block-Cache>
//! Admission adapts CacheLib's bounded RejectFirstAP access history, but keeps
//! first-touch probationary entries for native-style read-after-write. A reused
//! entry beats a first-touch candidate; repeated misses can earn admission.
//! F2 §7's second chance is applied to exact references, not mutable key chains.
//! <https://github.com/facebook/CacheLib/blob/a24370092f0bd6fa7145b2f01c3e50e63edd6a6e/cachelib/allocator/NvmAdmissionPolicy.h#L175-L249>.

use super::Mutation;
use super::ValueRecordReference;
use crate::distributed::types::RecordKind;
use minibytes::Bytes;
use parking_lot::Mutex;
use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;
use std::mem::size_of;

const WAYS: usize = 4;
// Bound insertion latency independently of configured capacity.
const MAX_EVICTION_STEPS: usize = 64;
// Reserve slots for small records without consuming the entire budget on an
// empty table. Larger records use the same global byte allowance and fewer slots.
const MIN_PAYLOAD_PER_SLOT: usize = 1024;
// `Bytes::from_owner(Box<[u8]>)` owns an Arc allocation containing a box header
// and its strong/weak counts, plus the exactly sized byte allocation itself.
const BYTE_OWNER_BYTES: usize = size_of::<Box<[u8]>>() + 2 * size_of::<usize>();

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
    payload_bytes: usize,
    benefit: u8,
    protected: bool,
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
/// requested allocation sizes retained by the cache, including the fixed table
/// and detached byte-owner metadata. Allocator bookkeeping/rounding, the 8 KiB
/// per-keyspace counter array below, and application-held clones are excluded.
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
    /// Fixed 8 KiB instrumentation array; outside retained-cache accounting.
    pub per_keyspace_counters_bytes: usize,
    pub entries: usize,
    pub budget_bytes: usize,
}

pub(super) struct ReadCache {
    slots: Box<[Option<Entry>]>,
    history: Box<[AccessHistory]>,
    history_clock: usize,
    hash: RandomState,
    clock: usize,
    policy: ReadCachePolicy,
    // Reserve a quarter of payload bytes from unrelated keyspaces; protected
    // entries may use the rest of the shared budget when it is available.
    protected_reserve_bytes: usize,
    protected_retained_bytes: usize,
    regular_retained_bytes: usize,
    stats: ReadCacheStats,
    per_keyspace: [PerKeyspaceReadCacheStats; KEYSPACE_COUNTER_COUNT],
}

impl ReadCache {
    pub(super) fn metadata_bytes(budget_bytes: usize) -> usize {
        let slots = (budget_bytes
            / (size_of::<Option<Entry>>() + size_of::<AccessHistory>() + MIN_PAYLOAD_PER_SLOT)
            / WAYS)
            * WAYS;
        slots * (size_of::<Option<Entry>>() + size_of::<AccessHistory>())
    }
    pub(super) fn new(budget_bytes: usize) -> Self {
        Self::new_with_policy(budget_bytes, ReadCachePolicy::default())
    }
    pub(super) fn new_with_policy(budget_bytes: usize, policy: ReadCachePolicy) -> Self {
        let slot_count = (budget_bytes
            / (size_of::<Option<Entry>>() + size_of::<AccessHistory>() + MIN_PAYLOAD_PER_SLOT)
            / WAYS)
            * WAYS;
        let slots: Box<[_]> = (0..slot_count).map(|_| None).collect();
        let history = vec![AccessHistory::default(); slot_count].into_boxed_slice();
        let metadata_bytes = slot_count * (size_of::<Option<Entry>>() + size_of::<AccessHistory>());
        let payload_budget = budget_bytes.saturating_sub(metadata_bytes);
        Self {
            slots,
            history,
            history_clock: 0,
            hash: RandomState::new(),
            clock: 0,
            policy,
            protected_reserve_bytes: if policy.protected_keyspace.is_some() {
                payload_budget / 4
            } else {
                0
            },
            protected_retained_bytes: 0,
            regular_retained_bytes: 0,
            per_keyspace: [PerKeyspaceReadCacheStats::default(); KEYSPACE_COUNTER_COUNT],
            stats: ReadCacheStats {
                retained_bytes: metadata_bytes,
                metadata_bytes,
                budget_bytes,
                per_keyspace_counters_bytes: size_of::<
                    [PerKeyspaceReadCacheStats; KEYSPACE_COUNTER_COUNT],
                >(),
                ..ReadCacheStats::default()
            },
        }
    }

    pub(super) fn stats(&self) -> ReadCacheStats {
        self.stats
    }
    pub(super) fn per_keyspace_stats(&self) -> [PerKeyspaceReadCacheStats; KEYSPACE_COUNTER_COUNT] {
        self.per_keyspace
    }

    fn set_start(&self, fingerprint: u64) -> usize {
        (fingerprint as usize % (self.slots.len() / WAYS)) * WAYS
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
    pub(super) fn get(&mut self, reference: &ValueRecordReference) -> Option<Mutation> {
        self.lookup(reference, true)
    }

    pub(super) fn get_scan(&mut self, reference: &ValueRecordReference) -> Option<Mutation> {
        self.lookup(reference, false)
    }

    /// Scalar hits verify the requested key in place and retain only the value
    /// owner. Exact-reference lookup remains the authority; the key comparison
    /// prevents a same-reference association error from becoming a false hit.
    pub(super) fn get_point(
        &mut self,
        reference: &ValueRecordReference,
        keyspace: crate::key_shape::KeySpace,
        key: &[u8],
    ) -> Option<Bytes> {
        let found = if reference.descriptor.keyspace == keyspace.0 && !self.slots.is_empty() {
            let fingerprint = self.hash.hash_one(reference);
            self.observe(fingerprint);
            let start = self.set_start(fingerprint);
            self.slots[start..start + WAYS]
                .iter_mut()
                .flatten()
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
                            Some((value.clone(), entry.payload_bytes))
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                })
        } else {
            None
        };
        if let Some((value, payload_bytes)) = found {
            self.record_hit(reference, payload_bytes);
            Some(value)
        } else {
            self.record_miss(reference);
            None
        }
    }

    fn lookup(&mut self, reference: &ValueRecordReference, track_access: bool) -> Option<Mutation> {
        let found = if !self.slots.is_empty() {
            let fingerprint = self.hash.hash_one(reference);
            if track_access {
                self.observe(fingerprint);
            }
            let start = self.set_start(fingerprint);
            self.slots[start..start + WAYS]
                .iter_mut()
                .flatten()
                .find(|entry| entry.reference == *reference)
                .map(|entry| {
                    if track_access {
                        entry.benefit = entry.benefit.saturating_add(1).min(3);
                    }
                    (entry.mutation.clone(), entry.payload_bytes)
                })
        } else {
            None
        };
        if let Some((mutation, payload_bytes)) = found {
            self.record_hit(reference, payload_bytes);
            Some(mutation)
        } else {
            self.record_miss(reference);
            None
        }
    }

    fn record_hit(&mut self, reference: &ValueRecordReference, payload_bytes: usize) {
        self.stats.hits = self.stats.hits.saturating_add(1);
        self.stats.served_bytes = self.stats.served_bytes.saturating_add(payload_bytes as u64);
        let keyspace = &mut self.per_keyspace[reference.descriptor.keyspace as usize];
        keyspace.hits = keyspace.hits.saturating_add(1);
    }

    fn record_miss(&mut self, reference: &ValueRecordReference) {
        self.stats.misses = self.stats.misses.saturating_add(1);
        let keyspace = &mut self.per_keyspace[reference.descriptor.keyspace as usize];
        keyspace.misses = keyspace.misses.saturating_add(1);
    }

    /// Admit only a mutation returned by the verified storage-client read of
    /// this exact reference, or a canonical mutation bound to an authenticated
    /// durable receipt after successful contiguous publication. Shape checks
    /// catch association errors; they do not replace either provenance proof.
    /// Rejections leave serving correctness and the existing entry untouched.
    /// Advisory preflight precedes detached allocation. No slot reservation or
    /// guard crosses copying, and insertion rechecks current cache state.
    pub(super) fn prepare_verified(
        cache: &Mutex<Self>,
        budget_bytes: usize,
        reference: ValueRecordReference,
        mutation: &Mutation,
    ) -> InsertPreparation {
        Self::prepare_with(budget_bytes, reference, mutation, |payload_bytes| {
            cache.lock().preflight(&reference, payload_bytes)
        })
    }

    fn preflight(&mut self, reference: &ValueRecordReference, payload_bytes: usize) -> Option<u64> {
        let protected = self.policy.protected_keyspace == Some(reference.descriptor.keyspace);
        let reserve = if protected {
            0
        } else {
            self.protected_reserve_bytes
        };
        let limit = self
            .stats
            .budget_bytes
            .saturating_sub(self.stats.metadata_bytes)
            .saturating_sub(reserve);
        if payload_bytes > limit {
            return None;
        }
        let fingerprint = self.hash.hash_one(reference);
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
            || self
                .protected_retained_bytes
                .saturating_sub(entry.payload_bytes)
                >= self.protected_reserve_bytes
    }

    pub(super) fn prepare_post_publish(
        cache: &Mutex<Self>,
        budget_bytes: usize,
        reference: ValueRecordReference,
        mutation: &Mutation,
    ) -> InsertPreparation {
        Self::prepare_with(budget_bytes, reference, mutation, |payload_bytes| {
            let mut cache = cache.lock();
            if cache
                .policy
                .skips_post_publish_warming(reference.descriptor.keyspace)
            {
                None
            } else {
                cache.preflight(&reference, payload_bytes)
            }
        })
    }

    pub(super) fn configure_policy(&mut self, policy: ReadCachePolicy) {
        debug_assert_eq!(self.stats.entries, 0);
        self.policy = policy;
        let payload_budget = self
            .stats
            .budget_bytes
            .saturating_sub(self.stats.metadata_bytes);
        self.protected_reserve_bytes = if policy.protected_keyspace.is_some() {
            payload_budget / 4
        } else {
            0
        };
    }

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
        let (kind, value_len) = match mutation {
            Mutation::Put { value, .. } => (RecordKind::Put, value.len()),
            Mutation::Delete { .. } => (RecordKind::Delete, 0),
        };
        if descriptor.kind != kind
            || descriptor.keyspace != mutation.keyspace().0
            || descriptor.key_len as usize != mutation.key().len()
            || descriptor.value_len as usize != value_len
        {
            return InsertPreparation::Mismatch;
        }
        let Some(payload_bytes) = mutation.key().len().checked_add(value_len).and_then(|n| {
            let owners = usize::from(!mutation.key().is_empty()) + usize::from(value_len > 0);
            n.checked_add(owners * BYTE_OWNER_BYTES)
        }) else {
            return InsertPreparation::Oversize;
        };
        if payload_bytes > budget_bytes.saturating_sub(metadata_bytes) {
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
        };
        InsertPreparation::Prepared(PreparedEntry {
            reference,
            mutation,
            payload_bytes,
            fingerprint,
        })
    }

    #[cfg(test)]
    pub(super) fn insert_verified(
        &mut self,
        reference: ValueRecordReference,
        mutation: &Mutation,
    ) -> bool {
        let prepared = Self::prepare_with(
            self.stats.budget_bytes,
            reference,
            mutation,
            |payload_bytes| self.preflight(&reference, payload_bytes),
        );
        self.insert_prepared(prepared)
    }

    pub(super) fn insert_prepared(&mut self, prepared: InsertPreparation) -> bool {
        let PreparedEntry {
            reference,
            mutation,
            payload_bytes,
            fingerprint,
        } = match prepared {
            InsertPreparation::Prepared(prepared) => prepared,
            InsertPreparation::Disabled => {
                self.stats.bypass_disabled = self.stats.bypass_disabled.saturating_add(1);
                return false;
            }
            InsertPreparation::Mismatch => {
                self.stats.rejected_mismatch = self.stats.rejected_mismatch.saturating_add(1);
                return false;
            }
            InsertPreparation::Oversize => {
                self.stats.rejected_oversize = self.stats.rejected_oversize.saturating_add(1);
                return false;
            }
            InsertPreparation::Admission => {
                self.stats.bypass_admission = self.stats.bypass_admission.saturating_add(1);
                return false;
            }
        };
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
                self.stats.bypass_admission = self.stats.bypass_admission.saturating_add(1);
                return false;
            };
            if seen < 2 && self.slots[slot].as_ref().unwrap().benefit != 0 {
                self.stats.bypass_admission = self.stats.bypass_admission.saturating_add(1);
                return false;
            }
            slot
        };
        let replacing_regular_bytes = self.slots[slot]
            .as_ref()
            .filter(|entry| !entry.protected)
            .map_or(0, |entry| entry.payload_bytes);
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
            self.stats.bypass_eviction_limit = self.stats.bypass_eviction_limit.saturating_add(1);
            return false;
        }
        self.evict(slot);
        self.slots[slot] = Some(Entry {
            reference,
            mutation,
            payload_bytes,
            benefit: u8::from(seen >= 2),
            protected,
        });
        self.stats.retained_bytes += payload_bytes;
        if protected {
            self.protected_retained_bytes += payload_bytes;
        } else {
            self.regular_retained_bytes += payload_bytes;
        }
        self.stats.entries += 1;
        self.stats.inserts = self.stats.inserts.saturating_add(1);
        self.stats.admitted_bytes = self
            .stats
            .admitted_bytes
            .saturating_add(payload_bytes as u64);
        let keyspace = &mut self.per_keyspace[keyspace_id];
        keyspace.inserts = keyspace.inserts.saturating_add(1);
        true
    }

    fn can_fit_replacing(&self, payload_bytes: usize, protected: bool, slot: usize) -> bool {
        let replaced = self.slots[slot].as_ref();
        let released_protected_bytes = replaced
            .filter(|entry| entry.protected)
            .map_or(0, |entry| entry.payload_bytes);
        let released_regular_bytes = replaced
            .filter(|entry| !entry.protected)
            .map_or(0, |entry| entry.payload_bytes);
        let payload_budget = self
            .stats
            .budget_bytes
            .saturating_sub(self.stats.metadata_bytes);
        let protected_after_replace = self
            .protected_retained_bytes
            .saturating_sub(released_protected_bytes);
        if payload_bytes
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
            || payload_bytes
                <= payload_budget
                    .saturating_sub(self.protected_reserve_bytes)
                    .saturating_sub(
                        self.regular_retained_bytes
                            .saturating_sub(released_regular_bytes),
                    )
    }

    fn evict(&mut self, slot: usize) {
        if let Some(entry) = self.slots[slot].take() {
            self.stats.retained_bytes -= entry.payload_bytes;
            if entry.protected {
                self.protected_retained_bytes -= entry.payload_bytes;
            } else {
                self.regular_retained_bytes -= entry.payload_bytes;
            }
            self.stats.entries -= 1;
            self.stats.evictions = self.stats.evictions.saturating_add(1);
            let keyspace = &mut self.per_keyspace[entry.reference.descriptor.keyspace as usize];
            keyspace.evictions = keyspace.evictions.saturating_add(1);
        }
    }

    #[cfg(test)]
    fn set_start_for_ref(&self, reference: &ValueRecordReference) -> usize {
        self.set_start(self.hash.hash_one(reference))
    }
}

fn detach(bytes: &Bytes) -> Bytes {
    if bytes.is_empty() {
        Bytes::new()
    } else {
        Bytes::from(bytes.as_ref().to_vec().into_boxed_slice())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed::types::{
        BatchId, BatchVersion, PayloadDigest, PhysicalValueAddress, RecordDescriptor,
    };
    use crate::key_shape::{KeyShapeBuilder, KeySpace, KeyType};

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
                digest: PayloadDigest([3; 32]),
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
        let mut cache = ReadCache::new(budget);
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
        let payload = budget - before.metadata_bytes - 8 - 2 * BYTE_OWNER_BYTES;
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
        for budget in [0, 1, size_of::<Option<Entry>>()] {
            let mut cache = ReadCache::new(budget);
            assert!(cache.slots.is_empty());
            assert!(!cache.insert_verified(reference, &mutation));
            assert!(cache.get(&reference).is_none());
            assert_eq!(cache.stats().retained_bytes, 0);
            assert_eq!(cache.stats().misses, 1);
            let protected_cache = ReadCache::new_with_policy(budget, protected_policy(0));
            assert!(protected_cache.slots.is_empty());
            assert_eq!(protected_cache.stats().retained_bytes, 0);
        }
    }

    #[test]
    fn exact_reference_separates_current_and_checkpoint_versions() {
        let mut cache = ReadCache::new(64 * 1024);
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
                    digest: PayloadDigest([4; 32]),
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
    fn duplicate_fill_does_not_reallocate_or_change_accounting() {
        let mut cache = ReadCache::new(16 * 1024);
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
            cache.get(&reference).unwrap().key().as_ptr(),
            stored.key().as_ptr()
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
        let mut cache = ReadCache::new(16 * 1024);
        assert!(cache.insert_verified(reference, &mutation));
        let Mutation::Put { key, value, .. } = cache.get(&reference).unwrap() else {
            panic!()
        };
        assert_ne!(key.as_ptr(), mutation.key().as_ptr());
        assert_eq!(key.downcast_ref::<Box<[u8]>>().unwrap().len(), 8);
        assert_eq!(value.downcast_ref::<Box<[u8]>>().unwrap().len(), 16);
        assert_eq!(
            cache.stats().retained_bytes - cache.stats().metadata_bytes,
            24 + 2 * BYTE_OWNER_BYTES
        );
    }

    #[test]
    fn policy_oversized_demand_preserves_resident_entries() {
        let budget = 8 * 1024;
        let cache = Mutex::new(ReadCache::new_with_policy(budget, protected_policy(0)));
        let resident: Vec<_> = (0..WAYS as u64)
            .map(|version| record_for_keyspace(version, &[7; 512], 1))
            .collect();
        for (reference, mutation) in &resident {
            assert!(cache.lock().insert_verified(*reference, mutation));
        }
        let regular_limit = {
            let cache = cache.lock();
            budget - cache.stats.metadata_bytes - cache.protected_reserve_bytes
        };
        let (reference, mutation) = record_for_keyspace(100, &vec![5; regular_limit + 1], 1);
        let evictions = cache.lock().stats().evictions;
        for _ in 0..2 {
            let prepared = ReadCache::prepare_verified(&cache, budget, reference, &mutation);
            assert!(!cache.lock().insert_prepared(prepared));
        }
        assert_eq!(cache.lock().stats().evictions, evictions);
        for (reference, mutation) in resident {
            assert_eq!(cache.lock().get_scan(&reference), Some(mutation));
        }
    }

    #[test]
    fn oversized_and_mismatched_fills_leave_existing_data_untouched() {
        let mut cache = ReadCache::new(16 * 1024);
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
        let mut cache = ReadCache::new(16 * 1024);
        let mut held = Vec::new();
        for i in 1..200 {
            let bytes = vec![i as u8; 256 + (i as usize % 16) * 64];
            let (reference, mutation) = record(i, &bytes);
            assert!(cache.get(&reference).is_none());
            assert!(cache.insert_verified(reference, &mutation));
            assert_eq!(cache.get(&reference), Some(mutation));
            if i % 40 == 0 {
                held.push(cache.get(&reference).unwrap());
            }
            let stats = cache.stats();
            assert!(stats.retained_bytes <= stats.budget_bytes);
            assert_eq!(
                stats.metadata_bytes,
                cache.slots.len() * (size_of::<Option<Entry>>() + size_of::<AccessHistory>())
            );
            assert_eq!(stats.entries, cache.slots.iter().flatten().count());
            assert_eq!(
                stats.retained_bytes,
                stats.metadata_bytes
                    + cache
                        .slots
                        .iter()
                        .flatten()
                        .map(|e| e.payload_bytes)
                        .sum::<usize>()
            );
        }
        assert!(cache.stats().evictions > 0);
        assert_eq!(held.len(), 4); // Application-owned clones survive safe eviction.
    }

    #[test]
    fn tombstone_and_empty_value_remain_distinct() {
        let mut cache = ReadCache::new(16 * 1024);
        let (put_ref, put) = record(1, b"");
        let (mut del_ref, _) = record(2, b"");
        del_ref.descriptor.kind = RecordKind::Delete;
        let delete = Mutation::Delete {
            keyspace: KeySpace(0),
            key: put.key().clone(),
        };
        assert!(cache.insert_verified(put_ref, &put));
        assert!(cache.insert_verified(del_ref, &delete));
        assert_eq!(cache.get(&put_ref), Some(put));
        assert_eq!(cache.get(&del_ref), Some(delete));
    }

    #[test]
    fn local_clock_protects_reused_entries_before_single_use_collisions() {
        let mut cache = ReadCache::new(32 * 1024);
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
        let mut cache = ReadCache::new(8 * 1024);
        assert_eq!(cache.slots.len(), WAYS); // One deterministic collision set.
        let records: Vec<_> = (0..100).map(|version| record(version, &[7; 512])).collect();
        for (reference, mutation) in &records[..WAYS] {
            assert!(cache.insert_verified(*reference, mutation));
        }
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
    }

    #[test]
    fn repeated_miss_earns_admission_but_one_touch_cannot_replace_a_hot_set() {
        let mut cache = ReadCache::new(8 * 1024);
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
        let mut cache = ReadCache::new(8 * 1024);
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
        let budget = 8 * 1024;
        let cache = Mutex::new(ReadCache::new(budget));
        for version in 0..WAYS as u64 {
            let (reference, mutation) = record(version, b"value");
            let prepared = ReadCache::prepare_verified(&cache, budget, reference, &mutation);
            assert!(cache.lock().insert_prepared(prepared));
        }
        // Passing both preparation and insertion did not promote any cold write.
        assert!(
            cache
                .lock()
                .slots
                .iter()
                .flatten()
                .all(|entry| entry.benefit == 0)
        );
        for version in 0..WAYS as u64 {
            assert!(cache.lock().get(&record(version, b"value").0).is_some());
        }
        let (reference, mutation) = record(100, b"new-value");
        let rejected = ReadCache::prepare_verified(&cache, budget, reference, &mutation);
        assert!(matches!(rejected, InsertPreparation::Admission));
        assert!(!cache.lock().insert_prepared(rejected));
        let mut mismatch = reference;
        mismatch.descriptor.value_len += 1;
        let invalid = ReadCache::prepare_verified(&cache, budget, mismatch, &mutation);
        assert!(matches!(invalid, InsertPreparation::Mismatch));
        assert!(!cache.lock().insert_prepared(invalid));
        let accepted = ReadCache::prepare_verified(&cache, budget, reference, &mutation);
        assert!(matches!(accepted, InsertPreparation::Prepared(_)));
        assert!(cache.lock().insert_prepared(accepted));
        assert_eq!(cache.lock().get(&reference), Some(mutation));
        assert_eq!(cache.lock().stats().bypass_admission, 1);
        assert_eq!(cache.lock().stats().rejected_mismatch, 1);
    }

    #[test]
    fn per_keyspace_counters_follow_lookup_and_evicted_entry_ownership() {
        let mut cache = ReadCache::new(8 * 1024);
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
        let mut cache = ReadCache::new(16 * 1024);
        assert!(cache.insert_verified(reference, &mutation));
        drop(mutation);
        drop(owner);
        assert!(dropped.load(Ordering::SeqCst));
        assert!(cache.get(&reference).is_some());
    }
    #[test]
    fn scalar_hits_clone_only_value_and_reject_wrong_key_or_keyspace() {
        let mut cache = ReadCache::new(16 * 1024);
        let (reference, mutation) = record(1, b"value");
        assert!(cache.insert_verified(reference, &mutation));
        let stored_value_ptr = match &cache.slots.iter().flatten().next().unwrap().mutation {
            Mutation::Put { value, .. } => value.as_ptr(),
            Mutation::Delete { .. } => panic!("inserted record is a put"),
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
        let mut cache = ReadCache::new_with_policy(8 * 1024, protected_policy(0));
        let (reference, mutation) = record_for_keyspace(1, b"contents", 1);
        assert!(cache.insert_verified(reference, &mutation));
        assert_eq!(cache.get_scan(&reference), Some(mutation));
        assert!(cache.stats().retained_bytes <= cache.stats().budget_bytes);
    }

    #[test]
    fn protected_summary_entries_survive_streaming_warm_and_demand_churn() {
        let (hot_reference, hot_mutation) = record_for_keyspace(0, b"summary", 0);
        let mut cache = ReadCache::new_with_policy(8 * 1024, protected_policy(0));
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
        let cache = Mutex::new(ReadCache::new_with_policy(
            budget,
            ReadCachePolicy::for_sui_shape(&shape),
        ));

        for (version, keyspace) in shape.iter_ks().enumerate() {
            let (reference, mutation) =
                record_for_keyspace(version as u64 + 1, b"checkpoint-value", keyspace.id().0);
            let skip_warm = matches!(
                keyspace.name(),
                "checkpoint_by_digest" | "checkpoint_sequence_by_contents_digest" | "watermarks"
            );
            let prepared = ReadCache::prepare_post_publish(&cache, budget, reference, &mutation);
            if skip_warm {
                assert!(!cache.lock().insert_prepared(prepared));
                assert_eq!(cache.lock().get_scan(&reference), None);
                let demand_fill = ReadCache::prepare_verified(&cache, budget, reference, &mutation);
                assert!(cache.lock().insert_prepared(demand_fill));
            } else {
                assert!(cache.lock().insert_prepared(prepared));
            }
            assert_eq!(cache.lock().get_scan(&reference), Some(mutation));
        }

        // Sui epoch/perpetual shapes lack the protected checkpoint family and
        // therefore keep ordinary read-after-write warming for alias-like names.
        let mut builder = KeyShapeBuilder::new();
        builder.add_key_space("checkpoint_by_digest", 1, 1, KeyType::uniform(1));
        builder.add_key_space("watermarks", 1, 1, KeyType::uniform(1));
        let non_checkpoint_shape = builder.build();
        let non_checkpoint = Mutex::new(ReadCache::new_with_policy(
            budget,
            ReadCachePolicy::for_sui_shape(&non_checkpoint_shape),
        ));
        let (reference, mutation) = record_for_keyspace(20, b"contents", 0);
        let warmed = ReadCache::prepare_post_publish(&non_checkpoint, budget, reference, &mutation);
        assert!(non_checkpoint.lock().insert_prepared(warmed));
        assert_eq!(non_checkpoint.lock().get_scan(&reference), Some(mutation));

        let generic = Mutex::new(ReadCache::new(budget));
        let (reference, mutation) = record_for_keyspace(21, b"digest-alias", 2);
        let warmed = ReadCache::prepare_post_publish(&generic, budget, reference, &mutation);
        assert!(generic.lock().insert_prepared(warmed));
        assert_eq!(generic.lock().get_scan(&reference), Some(mutation));
    }

    #[test]
    fn regular_demand_reclaims_only_protected_capacity_above_the_floor() {
        let mut cache = ReadCache::new_with_policy(16 * 1024, protected_policy(0));
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
}
