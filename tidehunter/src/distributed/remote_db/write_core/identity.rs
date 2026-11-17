use super::shard_count;
use crate::distributed::types::{BatchVersion, PayloadDigest};
use parking_lot::{Mutex, MutexGuard};
use std::collections::{HashMap, hash_map::RandomState};
use std::hash::BuildHasher;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum IdentityClass {
    Active,
    Suffix,
    Recent,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct IdentityEntry {
    pub(super) version: BatchVersion,
    pub(super) digest: Option<PayloadDigest>,
    pub(super) seq: u64,
    pub(super) class: IdentityClass,
}

type Identities = HashMap<[u8; 16], IdentityEntry>;

#[derive(Default)]
struct IdentityShard {
    entries: Identities,
    /// Advances, under this shard's lock, whenever checkpoint retirement
    /// removes an entry. A negative lookup captured at generation `g` is still
    /// valid under the same lock only while the generation is `g`: otherwise
    /// a retired identity may have moved to a newer receipt-history root.
    retirements: u64,
}

pub(super) struct IdentityTable {
    shards: Box<[Mutex<IdentityShard>]>,
    hash: RandomState,
}

pub(super) struct LockedIdentities<'a> {
    shards: Vec<(usize, MutexGuard<'a, IdentityShard>)>,
    hash: &'a RandomState,
    mask: usize,
}

impl IdentityTable {
    pub(super) fn new(parallelism: usize, hash: RandomState) -> Self {
        Self {
            shards: (0..shard_count(parallelism))
                .map(|_| Mutex::new(IdentityShard::default()))
                .collect(),
            hash,
        }
    }

    pub(super) fn with_operation<T>(
        &self,
        operation: [u8; 16],
        apply: impl FnOnce(&mut Identities) -> T,
    ) -> T {
        apply(&mut self.shard(operation).lock().entries)
    }

    /// The entry and its shard's retirement generation, read under one lock.
    pub(super) fn lookup(&self, operation: [u8; 16]) -> (Option<IdentityEntry>, u64) {
        let shard = self.shard(operation).lock();
        (shard.entries.get(&operation).copied(), shard.retirements)
    }

    fn shard(&self, operation: [u8; 16]) -> &Mutex<IdentityShard> {
        &self.shards[self.hash.hash_one(operation) as usize & (self.shards.len() - 1)]
    }

    pub(super) fn with_operations<T>(
        &self,
        operations: &[[u8; 16]],
        apply: impl FnOnce(&mut LockedIdentities<'_>) -> T,
    ) -> T {
        let mask = self.shards.len() - 1;
        let mut indices: Vec<_> = operations
            .iter()
            .map(|operation| self.hash.hash_one(operation) as usize & mask)
            .collect();
        indices.sort_unstable();
        indices.dedup();
        // A bounded submit_many window freezes every identity before key or
        // owner locks. Ascending deduplicated acquisition prevents cycles.
        let shards = indices
            .into_iter()
            .map(|index| (index, self.shards[index].lock()))
            .collect();
        apply(&mut LockedIdentities {
            shards,
            hash: &self.hash,
            mask,
        })
    }

    pub(super) fn checkpoint(
        &self,
        cut: Option<BatchVersion>,
        recent: &std::collections::VecDeque<super::super::RecentReceipt>,
    ) -> usize {
        let retained: HashMap<_, _> = recent
            .iter()
            .map(|receipt| (receipt.id.operation, (receipt.version, receipt.digest)))
            .collect();
        let mut returned = 0;
        for shard in &self.shards {
            let mut shard = shard.lock();
            let held = shard.entries.len();
            shard.entries.retain(|operation, entry| {
                if entry.class != IdentityClass::Recent
                    && cut.is_some_and(|cut| entry.version <= cut)
                {
                    // A published checkpoint can race the publisher's final
                    // Active→Suffix bookkeeping after dropping publisher. Both
                    // classes own one credit; retire it exactly once here.
                    returned += 1;
                    entry.class = IdentityClass::Recent;
                }
                entry.class != IdentityClass::Recent
                    || retained.get(operation).is_some_and(|(version, digest)| {
                        *version == entry.version && Some(*digest) == entry.digest
                    })
            });
            if shard.entries.len() != held {
                shard.retirements += 1;
            }
            // Same policy as DependencyRegistry cleanup, on this cold path:
            // retired completed identities must not pin a shard's historical
            // peak, so retained buckets stay tied to live entries.
            if shard.entries.is_empty() && shard.entries.capacity() != 0 {
                shard.entries = HashMap::new();
            } else if shard.entries.capacity() > shard.entries.len().saturating_mul(4) {
                let len = shard.entries.len();
                shard.entries.shrink_to(len);
            }
        }
        returned
    }

    /// Resident bucket bytes for diagnostics; each shard is locked briefly.
    pub(super) fn resident_bytes(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| Self::shard_bytes(&shard.lock().entries))
            .sum()
    }

    /// Metrics variant: a shard held by a writer makes the value unavailable,
    /// so sampling never blocks allocation, publication or retirement.
    pub(super) fn try_resident_bytes(&self) -> Option<usize> {
        self.shards
            .iter()
            .map(|shard| {
                shard
                    .try_lock()
                    .map(|shard| Self::shard_bytes(&shard.entries))
            })
            .sum()
    }

    fn shard_bytes(shard: &Identities) -> usize {
        shard.capacity() * std::mem::size_of::<([u8; 16], IdentityEntry)>()
    }

    #[cfg(test)]
    pub(super) fn count(&self, class: IdentityClass) -> usize {
        self.shards
            .iter()
            .map(|shard| {
                shard
                    .lock()
                    .entries
                    .values()
                    .filter(|entry| entry.class == class)
                    .count()
            })
            .sum()
    }
}

impl LockedIdentities<'_> {
    fn locked(&mut self, operation: [u8; 16]) -> &mut IdentityShard {
        let index = self.hash.hash_one(operation) as usize & self.mask;
        let position = self.shards.partition_point(|(shard, _)| *shard < index);
        // Only operations in the prelocked window reach these accessors.
        assert_eq!(
            self.shards[position].0, index,
            "identity outside locked window"
        );
        &mut self.shards[position].1
    }

    pub(super) fn get(&mut self, operation: [u8; 16]) -> Option<IdentityEntry> {
        self.locked(operation).entries.get(&operation).copied()
    }

    /// The shard's retirement generation, stable while this window is locked.
    pub(super) fn retirements(&mut self, operation: [u8; 16]) -> u64 {
        self.locked(operation).retirements
    }

    pub(super) fn insert(&mut self, operation: [u8; 16], entry: IdentityEntry) {
        self.locked(operation).entries.insert(operation, entry);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identities_survive_slot_reuse_until_checkpoint_retirement() {
        let table = IdentityTable::new(1, RandomState::new());
        let version = BatchVersion {
            epoch: 7,
            sequence: 3,
        };
        table.with_operation([1; 16], |entries| {
            entries.insert(
                [1; 16],
                IdentityEntry {
                    version,
                    digest: Some(PayloadDigest([2; 32])),
                    seq: 3,
                    class: IdentityClass::Suffix,
                },
            );
        });
        table.with_operation([1; 16], |entries| {
            let entry = entries.get(&[1; 16]).unwrap();
            assert_eq!(entry.version, version);
            assert_eq!(entry.digest, Some(PayloadDigest([2; 32])));
            assert_eq!(entry.seq, 3);
        });
        // The live checkpoint retirement path: a cut below the entry keeps it,
        // a covering cut returns its one credit and drops it (not in `recent`).
        let recent = std::collections::VecDeque::new();
        let below = BatchVersion {
            epoch: 7,
            sequence: 2,
        };
        assert_eq!(table.checkpoint(Some(below), &recent), 0);
        table.with_operation([1; 16], |entries| assert_eq!(entries.len(), 1));
        // Only a removal advances the shard's retirement generation.
        assert_eq!(table.lookup([1; 16]).1, 0);
        assert_eq!(table.checkpoint(Some(version), &recent), 1);
        let (entry, retirements) = table.lookup([1; 16]);
        assert!(entry.is_none());
        assert_eq!(retirements, 1);
        // Retirement also frees the emptied shard's buckets, so resident
        // bookkeeping does not keep a lifetime high-water mark.
        table.with_operation([1; 16], |entries| {
            assert!(entries.is_empty());
            assert_eq!(entries.capacity(), 0);
        });
    }
}
