//! In-flight write identities: operation id → the generation that owns it.
//!
//! A caller may retry one operation id while the original is still in flight
//! (Sui retries after an uncertain timeout). Two live generations for one id
//! would let an intervening write to the same key be overwritten by the later
//! duplicate, so exactly one generation may own an id between allocation and
//! publication. This map is that ownership record and nothing else: no class,
//! no retained version, no admission credit. It is bounded by the number of
//! admitted writes (one entry per unresolved generation), and an entry is
//! removed when the batch publishes (its receipt enters the retry window) or
//! aborts.
//!
//! Sharded by operation id so independent writers never share a lock. The
//! allocation critical section takes every shard of its window in ascending
//! index order before the key shards and the owner order; nothing takes them
//! while holding those.

use super::shard_count;
use crate::distributed::types::BatchVersion;
use parking_lot::{Mutex, MutexGuard};
use std::collections::{HashMap, hash_map::RandomState};
use std::hash::BuildHasher;
#[cfg(test)]
use std::ops::DerefMut;

/// The generation that currently owns an operation id.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct InFlightEntry {
    pub(super) version: BatchVersion,
    pub(super) seq: u64,
}

type Entries = HashMap<[u8; 16], InFlightEntry>;

pub(super) struct InFlight {
    shards: Box<[Mutex<Entries>]>,
    hash: RandomState,
}

/// Every shard of one submit window, locked in ascending index order.
pub(super) struct LockedInFlight<'a> {
    shards: Vec<(usize, MutexGuard<'a, Entries>)>,
    hash: &'a RandomState,
    mask: usize,
}

impl InFlight {
    pub(super) fn new(parallelism: usize, hash: RandomState) -> Self {
        Self {
            shards: (0..shard_count(parallelism))
                .map(|_| Mutex::new(Entries::new()))
                .collect(),
            hash,
        }
    }

    pub(super) fn lookup(&self, operation: [u8; 16]) -> Option<InFlightEntry> {
        self.shard(operation).lock().get(&operation).copied()
    }

    #[cfg(test)]
    pub(super) fn with_operation<T>(
        &self,
        operation: [u8; 16],
        apply: impl FnOnce(&mut Entries) -> T,
    ) -> T {
        apply(self.shard(operation).lock().deref_mut())
    }

    /// Remove the entry of `operation` if `sequence` still owns it. A later
    /// generation of the same id is never removed by an earlier one.
    pub(super) fn retire(&self, operation: [u8; 16], sequence: u64) {
        let mut shard = self.shard(operation).lock();
        if shard
            .get(&operation)
            .is_some_and(|entry| entry.seq == sequence)
        {
            shard.remove(&operation);
        }
    }

    pub(super) fn with_operations<T>(
        &self,
        operations: &[[u8; 16]],
        apply: impl FnOnce(&mut LockedInFlight<'_>) -> T,
    ) -> T {
        let mask = self.shards.len() - 1;
        let mut indices: Vec<_> = operations
            .iter()
            .map(|operation| self.hash.hash_one(operation) as usize & mask)
            .collect();
        indices.sort_unstable();
        indices.dedup();
        // A bounded submit window freezes every identity before the key or
        // owner locks. Ascending deduplicated acquisition prevents cycles.
        let shards = indices
            .into_iter()
            .map(|index| (index, self.shards[index].lock()))
            .collect();
        apply(&mut LockedInFlight {
            shards,
            hash: &self.hash,
            mask,
        })
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.shards.iter().map(|shard| shard.lock().len()).sum()
    }

    pub(super) fn resident_bytes(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| Self::shard_bytes(&shard.lock()))
            .sum()
    }

    /// Metrics variant: a shard held by a writer makes the value unavailable,
    /// so sampling never blocks allocation, publication or retirement.
    pub(super) fn try_resident_bytes(&self) -> Option<usize> {
        self.shards
            .iter()
            .map(|shard| shard.try_lock().map(|shard| Self::shard_bytes(&shard)))
            .sum()
    }

    fn shard_bytes(shard: &Entries) -> usize {
        shard.capacity() * std::mem::size_of::<([u8; 16], InFlightEntry)>()
    }

    /// Bytes per resident slot, as charged by [`Self::resident_bytes`].
    #[cfg(test)]
    pub(super) const fn entry_bytes() -> usize {
        std::mem::size_of::<([u8; 16], InFlightEntry)>()
    }

    #[cfg(test)]
    pub(super) fn shard_count(&self) -> usize {
        self.shards.len()
    }

    fn shard(&self, operation: [u8; 16]) -> &Mutex<Entries> {
        &self.shards[self.hash.hash_one(operation) as usize & (self.shards.len() - 1)]
    }
}

impl LockedInFlight<'_> {
    fn locked(&mut self, operation: [u8; 16]) -> &mut Entries {
        let index = self.hash.hash_one(operation) as usize & self.mask;
        let position = self.shards.partition_point(|(shard, _)| *shard < index);
        // Only operations of the prelocked window reach these accessors.
        assert_eq!(
            self.shards[position].0, index,
            "operation outside the locked window"
        );
        &mut self.shards[position].1
    }

    pub(super) fn get(&mut self, operation: [u8; 16]) -> Option<InFlightEntry> {
        self.locked(operation).get(&operation).copied()
    }

    pub(super) fn insert(&mut self, operation: [u8; 16], entry: InFlightEntry) {
        self.locked(operation).insert(operation, entry);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(sequence: u64) -> InFlightEntry {
        InFlightEntry {
            version: BatchVersion { epoch: 1, sequence },
            seq: sequence,
        }
    }

    #[test]
    fn one_generation_owns_an_operation_until_retirement() {
        let table = InFlight::new(2, RandomState::new());
        let operation = [3; 16];
        table.with_operation(operation, |entries| {
            entries.insert(operation, entry(0));
        });
        assert_eq!(table.lookup(operation), Some(entry(0)));
        // A later generation of the same id is not removed by an older one.
        table.retire(operation, 1);
        assert_eq!(table.lookup(operation), Some(entry(0)));
        table.retire(operation, 0);
        assert_eq!(table.lookup(operation), None);
        assert_eq!(table.len(), 0);
    }

    #[test]
    fn locked_window_freezes_and_updates_every_operation() {
        let table = InFlight::new(2, RandomState::new());
        let operations = [[1; 16], [2; 16]];
        table.with_operations(&operations, |locked| {
            for (index, operation) in operations.iter().enumerate() {
                assert!(locked.get(*operation).is_none());
                locked.insert(*operation, entry(index as u64));
            }
        });
        let seen = table.with_operations(&operations, |locked| {
            operations
                .iter()
                .filter_map(|operation| locked.get(*operation))
                .count()
        });
        assert_eq!(seen, 2);
        assert_eq!(table.len(), 2);
    }
}
