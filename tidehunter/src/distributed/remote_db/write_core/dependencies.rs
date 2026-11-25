use super::{Dependency, shard_count};
use parking_lot::{Mutex, MutexGuard};
use std::collections::{HashMap, hash_map::RandomState};
use std::hash::BuildHasher;

pub(super) struct DependencyRegistry {
    shards: Box<[Mutex<HashMap<Dependency, u64>>]>,
    hash: RandomState,
}

pub(super) struct LockedDependencies<'a> {
    shards: Vec<(usize, MutexGuard<'a, HashMap<Dependency, u64>>)>,
    hash: &'a RandomState,
    mask: usize,
}

impl DependencyRegistry {
    pub(super) fn new(parallelism: usize, hash: RandomState) -> Self {
        Self {
            shards: (0..shard_count(parallelism))
                .map(|_| Mutex::new(HashMap::new()))
                .collect(),
            hash,
        }
    }

    fn shard(&self, key: &Dependency) -> usize {
        self.hash.hash_one(key) as usize & (self.shards.len() - 1)
    }

    pub(super) fn shard_indices<'a>(
        &self,
        keys: impl Iterator<Item = &'a Dependency>,
    ) -> Vec<usize> {
        keys.map(|key| self.shard(key)).collect()
    }

    pub(super) fn with_shards<T>(
        &self,
        mut indices: Vec<usize>,
        operation: impl FnOnce(&mut LockedDependencies<'_>) -> T,
    ) -> T {
        indices.sort_unstable();
        indices.dedup();
        // Multi-key allocation must order all keys together. Ascending,
        // deduplicated shard acquisition prevents cycles and self-deadlock.
        let shards = indices
            .into_iter()
            .map(|index| (index, self.shards[index].lock()))
            .collect();
        operation(&mut LockedDependencies {
            shards,
            hash: &self.hash,
            mask: self.shards.len() - 1,
        })
    }

    pub(super) fn remove(&self, keys: &[Dependency], sequence: u64) {
        for key in keys {
            let mut shard = self.shards[self.shard(key)].lock();
            if shard.get(key) == Some(&sequence) {
                shard.remove(key);
            }
            if shard.is_empty() && shard.capacity() != 0 {
                *shard = HashMap::new();
            } else if shard.capacity() > shard.len().saturating_mul(4) {
                // A stalled writer must not pin this shard's historical peak.
                // Geometric shrinking keeps retained buckets tied to live keys.
                let len = shard.len();
                shard.shrink_to(len);
            }
        }
    }

    /// Resident bucket bytes, including spilled key storage, for diagnostics.
    pub(super) fn resident_bytes(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| Self::shard_bytes(&shard.lock()))
            .sum()
    }

    /// Metrics variant: a shard held by a writer makes the value unavailable,
    /// so sampling never blocks allocation or publication cleanup.
    pub(super) fn try_resident_bytes(&self) -> Option<usize> {
        self.shards
            .iter()
            .map(|shard| shard.try_lock().map(|shard| Self::shard_bytes(&shard)))
            .sum()
    }

    fn shard_bytes(shard: &HashMap<Dependency, u64>) -> usize {
        shard.capacity() * std::mem::size_of::<(Dependency, u64)>()
            + shard
                .keys()
                .filter(|(_, key)| key.spilled())
                .map(|(_, key)| key.capacity())
                .sum::<usize>()
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.shards.iter().map(|shard| shard.lock().len()).sum()
    }

    #[cfg(test)]
    pub(super) fn capacity(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| shard.lock().capacity())
            .sum()
    }

    /// Bytes per resident slot, as charged by `resident_bytes` (spilled key
    /// storage is charged separately).
    #[cfg(test)]
    pub(super) const fn entry_bytes() -> usize {
        std::mem::size_of::<(Dependency, u64)>()
    }

    #[cfg(test)]
    pub(super) fn shard_count(&self) -> usize {
        self.shards.len()
    }
}

impl LockedDependencies<'_> {
    pub(super) fn register(
        &mut self,
        keys: &[Dependency],
        sequence: u64,
        predecessors: &mut Vec<u64>,
        resolved: impl Fn(u64) -> bool,
    ) {
        for key in keys {
            let index = self.hash.hash_one(key) as usize & self.mask;
            // Every index was locked by with_shards before entering this callback.
            let position = self.shards.partition_point(|(shard, _)| *shard < index);
            if let Some(prior) = self.shards[position].1.insert(key.clone(), sequence)
                && prior != sequence
                && !resolved(prior)
                && !predecessors.contains(&prior)
            {
                predecessors.push(prior);
            }
        }
    }
}

#[cfg(test)]
mod tests;
