//! Bounded retry window of published batches.
//!
//! A repeated operation id must return its original version: Sui retries a
//! write after an uncertain timeout, and a retry that appended again would
//! spend owner bytes for no effect. Native single-node TideHunter has no
//! operation ids at all, so the distributed layer keeps the last
//! [`WAL_REPLAY_WINDOW_BATCHES`] published receipts and re-applies anything
//! older (owner-accepted weakening). This is deliberately not an identity
//! table: it holds no in-flight generation, no class and no admission credit.
//!
//! Two structures, one truth. `order` is the ring itself and is mutated only
//! under the publisher lock (publication pushes, eviction pops, open and the
//! checkpoint capture replace or read it). `index` answers the write hot path's
//! probe behind one per-shard `parking_lot::Mutex`; it **never** takes the
//! publisher lock, so a put never waits behind index insertion or another
//! owner's publication. The shard count is
//! `crate::distributed::remote_db::write_core::shard_count(parallelism)`, i.e.
//! `parallelism * 8` clamped to `[64, 4096]` and rounded up to a power of two —
//! the mask below requires exactly that. Every mutation of `order` performs its
//! matching `index` mutation under the same publisher lock, so the two never
//! disagree; only `get` reads the index alone.
//!
//! Ordering against in-flight ownership (write_core): a receipt enters `order`
//! **before** the generation's in-flight entry is retired, so a retry that
//! probes the in-flight table first and the window second observes one of them
//! at every instant.

use super::super::{RecentReceipt, WAL_REPLAY_WINDOW_BATCHES};
use super::shard_count;
use parking_lot::Mutex;
use std::collections::{HashMap, VecDeque, hash_map::RandomState};
use std::hash::BuildHasher;

/// The last published receipts, oldest first, with an operation index.
pub(in crate::distributed::remote_db) struct ReceiptWindow {
    /// Hard bound on `order.len()`. Production is the native-derived replay
    /// window; a test may construct a shorter one to observe the documented
    /// re-apply behaviour instead of an abort.
    cap: usize,
    order: VecDeque<RecentReceipt>,
    index: std::sync::Arc<ReceiptIndex>,
}

/// One shard of the receipt probe: operation id to its recent receipt.
type ReceiptShard = Mutex<HashMap<[u8; 16], RecentReceipt>>;

/// Sharded operation → receipt probe. Written only under the publisher lock, so
/// each shard has a single logical writer; read without the publisher lock by
/// every write admission.
///
/// Lock tree: a receipt shard is a **leaf**. Nothing is taken while one is held
/// (no publisher, no in-flight shard, no key shard, no owner order, no slot and
/// no call-out), which is what keeps `publisher → receipt shard` compatible with
/// the allocation order `in-flight shards → key shards → owner order → slot`.
/// A future edit that calls out under a shard would create the cycle.
pub(in crate::distributed::remote_db) struct ReceiptIndex {
    shards: Box<[ReceiptShard]>,
    hash: RandomState,
}

impl ReceiptIndex {
    fn new(parallelism: usize) -> Self {
        let shards = shard_count(parallelism);
        // `shard` masks with `& (len - 1)`; a non-power-of-two count would alias
        // shards silently and skew the probe instead of failing.
        assert!(
            shards.is_power_of_two(),
            "receipt index shard count {shards} is not a power of two",
        );
        Self {
            shards: (0..shards).map(|_| Mutex::new(HashMap::new())).collect(),
            hash: RandomState::new(),
        }
    }

    /// The receipt a retry of `operation` answers with; the write path's
    /// lock-free probe (`WriteCore::receipts`).
    pub(in crate::distributed::remote_db) fn get(
        &self,
        operation: &[u8; 16],
    ) -> Option<RecentReceipt> {
        self.shard(operation).lock().get(operation).copied()
    }

    /// Insert and return the receipt this operation previously answered with.
    fn insert(&self, receipt: RecentReceipt) -> Option<RecentReceipt> {
        self.shard(&receipt.id.operation)
            .lock()
            .insert(receipt.id.operation, receipt)
    }

    /// Remove `receipt` if it is still this operation's answer. A newer receipt
    /// for the same id is never evicted by the older one's departure.
    fn remove(&self, receipt: &RecentReceipt) {
        let mut shard = self.shard(&receipt.id.operation).lock();
        if shard
            .get(&receipt.id.operation)
            .is_some_and(|entry| entry.version == receipt.version)
        {
            shard.remove(&receipt.id.operation);
        }
    }

    fn len(&self) -> usize {
        self.shards.iter().map(|shard| shard.lock().len()).sum()
    }

    #[cfg(test)]
    fn clear(&self) {
        for shard in &self.shards {
            shard.lock().clear();
        }
    }

    fn resident_bytes(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| shard.lock().capacity() * std::mem::size_of::<([u8; 16], RecentReceipt)>())
            .sum()
    }

    #[cfg(test)]
    fn shard_count(&self) -> usize {
        self.shards.len()
    }

    fn shard(&self, operation: &[u8; 16]) -> &Mutex<HashMap<[u8; 16], RecentReceipt>> {
        &self.shards[self.hash.hash_one(operation) as usize & (self.shards.len() - 1)]
    }
}

impl ReceiptWindow {
    pub(in crate::distributed::remote_db) fn new() -> Self {
        Self::with_capacity(WAL_REPLAY_WINDOW_BATCHES)
    }

    pub(in crate::distributed::remote_db) fn with_capacity(capacity: usize) -> Self {
        let cap = capacity.min(WAL_REPLAY_WINDOW_BATCHES);
        Self {
            cap,
            order: VecDeque::with_capacity(cap),
            index: std::sync::Arc::new(ReceiptIndex::new(crate::parallelism::available())),
        }
    }

    #[cfg(test)]
    pub(in crate::distributed::remote_db) fn len(&self) -> usize {
        self.order.len()
    }

    #[cfg(test)]
    pub(in crate::distributed::remote_db) fn is_empty(&self) -> bool {
        self.order.is_empty()
    }

    #[cfg(test)]
    pub(in crate::distributed::remote_db) fn iter(&self) -> impl Iterator<Item = &RecentReceipt> {
        self.order.iter()
    }

    /// The published receipt of `operation`, when it is still in the window.
    ///
    /// Read on the write hot path, so it touches one index shard and never the
    /// publisher lock.
    #[cfg(test)]
    pub(in crate::distributed::remote_db) fn get(
        &self,
        operation: &[u8; 16],
    ) -> Option<RecentReceipt> {
        self.index.get(operation)
    }

    /// A handle that answers [`Self::get`] without the publisher lock. The
    /// engine keeps one next to the publisher mutex; it is the same index, so a
    /// push under the publisher lock is visible to every probe at once.
    pub(in crate::distributed::remote_db) fn probe(&self) -> std::sync::Arc<ReceiptIndex> {
        std::sync::Arc::clone(&self.index)
    }

    /// Record one published batch. The oldest entry leaves once the window is
    /// full; a re-applied operation id replaces its older entry in place.
    ///
    /// The caller holds the publisher lock, which is what serializes every
    /// `order` mutation against `iter` and the checkpoint capture.
    pub(in crate::distributed::remote_db) fn push(&mut self, receipt: RecentReceipt) {
        if let Some(previous) = self.index.insert(receipt) {
            // Re-applied operation id: only the newer version may answer a retry.
            self.order.retain(|entry| entry.version != previous.version);
        }
        self.order.push_back(receipt);
        while self.order.len() > self.cap {
            if let Some(previous) = self.order.pop_front() {
                self.index.remove(&previous);
            }
        }
    }

    /// Install the window a persisted snapshot carried. Entries keep their
    /// order; anything beyond the cap is dropped from the oldest end, exactly
    /// as a head of publication would have done.
    #[cfg(test)]
    pub(in crate::distributed::remote_db) fn replace(
        &mut self,
        order: impl IntoIterator<Item = RecentReceipt>,
    ) {
        self.order.clear();
        // The ring was cleared, so every index entry is stale: a push below
        // would keep the old operation's entry forever otherwise.
        self.index.clear();
        for receipt in order {
            self.push(receipt);
        }
    }

    /// Bytes held by the ring and the index, for the resident-memory report.
    pub(in crate::distributed::remote_db) fn resident_bytes(&self) -> usize {
        self.order.capacity() * std::mem::size_of::<RecentReceipt>() + self.index.resident_bytes()
    }

    /// Upper bound of [`Self::resident_bytes`].
    ///
    /// The ring holds at most `cap` receipts and never grows past its requested
    /// capacity. Each index shard holds at most the operation ids of the ring, so
    /// at most `cap` live entries in total, and a std `HashMap` keeps at most
    /// twice that many slots plus one round-up step per shard. Both containers
    /// therefore fit in twice the element count of each, plus one slot per index
    /// shard.
    #[cfg(test)]
    pub(in crate::distributed::remote_db) fn resident_bound() -> usize {
        let cap = WAL_REPLAY_WINDOW_BATCHES;
        let shards = shard_count(crate::parallelism::available());
        2 * cap * std::mem::size_of::<RecentReceipt>()
            + (2 * cap + shards) * std::mem::size_of::<([u8; 16], RecentReceipt)>()
    }
}

impl Drop for ReceiptWindow {
    fn drop(&mut self) {
        debug_assert_eq!(
            self.order.len(),
            self.index.len(),
            "retry window ring and index disagree",
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed::types::{BatchId, BatchVersion, PayloadDigest};

    fn receipt(operation: u8, sequence: u64) -> RecentReceipt {
        RecentReceipt {
            id: BatchId {
                database: [1; 16],
                operation: [operation; 16],
            },
            version: BatchVersion { epoch: 3, sequence },
            digest: PayloadDigest([sequence as u8; 32]),
        }
    }

    #[test]
    fn the_probe_index_is_power_of_two_sharded() {
        for parallelism in [1, 3, 7, 64, 1000] {
            let index = ReceiptIndex::new(parallelism);
            assert!(index.shard_count().is_power_of_two());
            assert!((64..=4096).contains(&index.shard_count()));
            assert_eq!(index.len(), 0);
        }
    }

    #[test]
    fn window_keeps_the_newest_receipts_and_evicts_the_oldest() {
        let mut window = ReceiptWindow::with_capacity(2);
        window.push(receipt(1, 1));
        window.push(receipt(2, 2));
        window.push(receipt(3, 3));
        assert_eq!(window.len(), 2);
        assert!(window.get(&[1; 16]).is_none());
        assert_eq!(window.get(&[2; 16]).unwrap().version.sequence, 2);
        assert_eq!(window.get(&[3; 16]).unwrap().version.sequence, 3);
    }

    #[test]
    fn reapplied_operation_keeps_only_its_newer_version() {
        let mut window = ReceiptWindow::with_capacity(2);
        window.push(receipt(7, 1));
        window.push(receipt(7, 9));
        assert_eq!(window.len(), 1);
        assert_eq!(window.get(&[7; 16]).unwrap().version.sequence, 9);
        // The replacement left one entry, so the ring is not full: the next two
        // pushes fill it, and the third evicts the oldest operation by order.
        window.push(receipt(8, 10));
        assert_eq!(window.len(), 2);
        window.push(receipt(9, 11));
        assert_eq!(window.len(), 2);
        assert!(window.get(&[7; 16]).is_none());
        assert_eq!(window.get(&[8; 16]).unwrap().version.sequence, 10);
        assert_eq!(window.get(&[9; 16]).unwrap().version.sequence, 11);
    }

    /// The S7 negative control: a window shorter than the retry distance makes
    /// the write re-apply, and a re-application is a fresh, effect-free write —
    /// never an abort and never a wrong version.
    #[test]
    fn a_short_window_reapplies_instead_of_aborting() {
        let mut window = ReceiptWindow::with_capacity(8);
        for index in 0..8 {
            window.push(receipt(index as u8 + 1, index));
        }
        assert!(window.get(&[1; 16]).is_some());
        window.push(receipt(9, 8));
        assert!(window.get(&[1; 16]).is_none());
        assert_eq!(window.len(), 8);
        // Re-applying the forgotten operation is accepted and becomes the
        // newest entry, not a conflict.
        window.push(receipt(1, 9));
        assert_eq!(window.get(&[1; 16]).unwrap().version.sequence, 9);
    }

    #[test]
    fn replace_rebuilds_the_index_and_honours_the_cap() {
        let mut window = ReceiptWindow::with_capacity(4);
        window.push(receipt(1, 1));
        window.replace(vec![receipt(2, 2), receipt(3, 3)]);
        assert_eq!(window.len(), 2);
        assert!(window.get(&[1; 16]).is_none());
        assert_eq!(window.get(&[3; 16]).unwrap().version.sequence, 3);
        // Distinct operation ids: a repeated id deliberately collapses to its
        // newest entry, which is not what this test measures.
        let overflow = (0..WAL_REPLAY_WINDOW_BATCHES + 8).map(|index| {
            let mut receipt = receipt(0, index as u64);
            receipt.id.operation[..8].copy_from_slice(&(index as u64).to_be_bytes());
            receipt
        });
        window.replace(overflow);
        assert_eq!(window.len(), 4);
        assert_eq!(window.iter().count(), 4);
    }

    #[test]
    fn resident_bound_covers_the_fixed_window() {
        let mut window = ReceiptWindow::new();
        // The bound must hold at the full cap, not only below it: a full ring is
        // the production steady state.
        for index in 0..WAL_REPLAY_WINDOW_BATCHES {
            let mut operation = [0u8; 16];
            operation[..8].copy_from_slice(&(index as u64).to_be_bytes());
            let mut receipt = receipt(0, index as u64);
            receipt.id.operation = operation;
            window.push(receipt);
        }
        assert_eq!(window.len(), WAL_REPLAY_WINDOW_BATCHES);
        assert!(window.resident_bytes() <= ReceiptWindow::resident_bound());
        assert!(
            ReceiptWindow::resident_bound()
                >= WAL_REPLAY_WINDOW_BATCHES * std::mem::size_of::<RecentReceipt>()
        );
    }
}
