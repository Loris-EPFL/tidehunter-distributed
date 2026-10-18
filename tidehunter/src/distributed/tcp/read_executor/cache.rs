//! Same fixed four-way/CLOCK accounting convention as remote_db/read_cache.rs.
//! Encoded members are derivative, not trusted: validate membership before get,
//! and verify integrity at the consuming client even on a resident hit.
use super::{ReadExecutorStats, ValueRecordReference};
use minibytes::Bytes;
use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;
use std::mem::size_of;

const WAYS: usize = 4;
const MAX_EVICTION_STEPS: usize = 64;
pub(super) const OWNER_BYTES: usize = size_of::<super::ReadBuffer>() + 2 * size_of::<usize>();
struct Entry {
    reference: ValueRecordReference,
    bytes: Bytes,
    referenced: bool,
}
pub(super) struct Cache {
    slots: Box<[Option<Entry>]>,
    hash: RandomState,
    clock: usize,
}
impl Cache {
    pub(super) fn metadata_bytes(budget: usize) -> usize {
        budget / (size_of::<Option<Entry>>() + 1024) / WAYS * WAYS * size_of::<Option<Entry>>()
    }
    pub(super) fn new(budget: usize, stats: &mut ReadExecutorStats) -> Self {
        let count = budget / (size_of::<Option<Entry>>() + 1024) / WAYS * WAYS;
        stats.cache_budget = budget;
        stats.cache_metadata_bytes = count * size_of::<Option<Entry>>();
        stats.cache_bytes = stats.cache_metadata_bytes;
        Self {
            slots: (0..count).map(|_| None).collect(),
            hash: RandomState::new(),
            clock: 0,
        }
    }
    fn start(&self, reference: &ValueRecordReference) -> usize {
        self.hash.hash_one(reference) as usize % (self.slots.len() / WAYS) * WAYS
    }
    pub(super) fn get(&mut self, reference: &ValueRecordReference) -> Option<Bytes> {
        if self.slots.is_empty() {
            return None;
        }
        let start = self.start(reference);
        for entry in self.slots[start..start + WAYS].iter_mut().flatten() {
            if entry.reference == *reference {
                entry.referenced = true;
                return Some(entry.bytes.clone());
            }
        }
        None
    }
    // Only exact-size, CQ-retired ReadBuffer owners enter this cache. Its final
    // clone recycles storage; no mmap, padded slab or full-batch view is retained.
    pub(super) fn insert(
        &mut self,
        reference: ValueRecordReference,
        bytes: &Bytes,
        stats: &mut ReadExecutorStats,
    ) {
        let Some(charge) = bytes.len().checked_add(OWNER_BYTES) else {
            stats.cache_oversized += 1;
            return;
        };
        if stats.cache_budget == 0 {
            stats.cache_bypass_disabled += 1;
            return;
        }
        if self.slots.is_empty() || charge > stats.cache_budget - stats.cache_metadata_bytes {
            stats.cache_oversized += 1;
            return;
        }
        let start = self.start(&reference);
        if self.slots[start..start + WAYS]
            .iter()
            .flatten()
            .any(|e| e.reference == reference)
        {
            return;
        }
        let slot = if let Some(i) = (start..start + WAYS).find(|&i| self.slots[i].is_none()) {
            i
        } else if let Some(i) =
            (start..start + WAYS).find(|&i| !self.slots[i].as_ref().unwrap().referenced)
        {
            i
        } else {
            for entry in self.slots[start..start + WAYS].iter_mut().flatten() {
                entry.referenced = false;
            }
            start + self.clock % WAYS
        };
        self.evict(slot, stats);
        for _ in 0..MAX_EVICTION_STEPS {
            if charge <= stats.cache_budget - stats.cache_bytes {
                break;
            }
            let i = self.clock;
            self.clock = (self.clock + 1) % self.slots.len();
            if let Some(entry) = self.slots[i].as_mut() {
                if entry.referenced {
                    entry.referenced = false;
                } else {
                    self.evict(i, stats);
                }
            }
        }
        if charge > stats.cache_budget - stats.cache_bytes {
            stats.cache_bypass_eviction_limit += 1;
            return;
        }
        self.slots[slot] = Some(Entry {
            reference,
            bytes: bytes.clone(),
            referenced: false,
        });
        stats.cache_bytes += charge;
        stats.cache_entries += 1;
        stats.cache_inserts += 1;
        stats.cache_admitted_bytes += charge as u64;
    }
    fn evict(&mut self, i: usize, stats: &mut ReadExecutorStats) {
        if let Some(entry) = self.slots[i].take() {
            stats.cache_bytes -= entry.bytes.len() + OWNER_BYTES;
            stats.cache_entries -= 1;
            stats.cache_evictions += 1;
        }
    }
    #[cfg(any(test, feature = "test-utils"))]
    pub(super) fn clear(&mut self, stats: &mut ReadExecutorStats) {
        for i in 0..self.slots.len() {
            self.evict(i, stats);
        }
    }
}
