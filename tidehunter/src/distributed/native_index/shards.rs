//! Shard layout, routing and load accounting of the native reference index.
//!
//! # Model
//!
//! Tidehunter paper §4.1 partitions the index into cells grouped in rows, and
//! protects rows with sharded mutexes so that different key ranges never
//! synchronize. The distributed index keeps that model: every
//! `(keyspace, cell)` pair routes to one of `N` shards by a stable hash
//! ([`StableHasher`], never `RandomState`). `N` is a power of two chosen when the
//! index is built. The shard map lives only in memory: no persisted image,
//! manifest or wire format records `N` or the hash, and opening a snapshot
//! rebuilds the map from cell identities.
//!
//! A shard owns its cells. A point lookup or a staged/promoted publication entry
//! locks exactly one shard. No operation holds two shard locks, so there is no
//! lock cycle among shards by construction.
//!
//! # Lock order
//!
//! `writer` mutex, then at most one shard lock, then the retention `cold` mutex,
//! then at most one pin-shard mutex. Where a rare operation needs every pin
//! shard (checkpoint floor selection) it takes them in ascending index while
//! holding `cold`; readers hold a pin shard alone and never `cold`.
//!
//! # Why a reader never sees half of a batch
//!
//! Visibility is decided by a cut, never by which shards a writer has finished.
//!
//! 1. A publisher applies a batch's entries shard by shard (`promote_committed`).
//!    Each entry carries its logical `MutationVersion`. An applied entry is
//!    invisible to every cut that does not contain its version.
//! 2. Only after every entry of every batch in the round is applied does the
//!    publisher install the new cut (`publish_latest` or
//!    `install_reserved_live_view`). That is one `ArcSwap` store: the single
//!    publication watermark.
//! 3. A reader resolves its cut once, from that watermark (or takes an explicit
//!    pinned cut), and filters every shard lookup by it.
//!
//! A reader holding cut `C` therefore sees a batch entirely or not at all:
//! `C` was installed after all entries of the batches it contains were applied
//! (`ArcSwap` store is the release point, the load is the acquire point, so the
//! shard writes happen before any lookup made with `C`), and `C` excludes every
//! batch applied later or still unpublished (higher version, or listed in the
//! sparse view's `excluded` set).
//!
//! `IndexReadCut::At` is public, so a caller can name a version that is applied
//! to some shards but not published. Every entry point therefore also checks
//! an explicit `At` cut against the watermark (`check_published`: the cut's
//! batch must not exceed the watermark's newest batch, and no excluded
//! unpublished batch may lie at or below it) and fails with
//! `NativeIndexError::UnpublishedCut`. `get`, scans, histories, `pin_cut`,
//! `validate_cut` and capture all apply it. The watermark only grows, so a cut
//! that was covered stays covered. `Sparse` views are minted by the index
//! after their entries are applied, and `BeforeAll` is empty, so they need no
//! check. Hence every cut a reader can hold is covered by the watermark.

use super::*;
use crate::distributed::shard_lock::{ShardLockStats, ShardRwLock, StableHasher, shard_mask};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

/// Shards of a default-built index.
pub const DEFAULT_INDEX_SHARDS: usize = 64;
/// Largest accepted shard count; keeps the publication histogram bounded.
pub const MAX_INDEX_SHARDS: usize = 4096;
/// Buckets of the shards-touched histogram: bucket `b` counts publications that
/// locked `n` shards with `2^(b-1) < n <= 2^b` (bucket 0: exactly one shard).
pub const PUBLICATION_HISTOGRAM_BUCKETS: usize = 13;

pub(super) type CellKey = (KeySpace, CellId);
pub(super) type Cells = HashMap<CellKey, CellIndex>;

/// `Some(mask)` for an accepted shard count.
pub(super) fn checked_mask(shards: usize) -> Option<usize> {
    if shards > MAX_INDEX_SHARDS {
        return None;
    }
    shard_mask(shards)
}

pub(super) fn new_shards(count: usize) -> Box<[ShardRwLock<Cells>]> {
    (0..count).map(|_| ShardRwLock::new(Cells::new())).collect()
}

/// Stable routing hash of a cell identity.
pub(super) fn cell_hash(keyspace: KeySpace, cell: &CellId) -> u64 {
    let hasher = StableHasher::new().byte(keyspace.0);
    match cell {
        CellId::Integer(id) => hasher.byte(0).u64(*id as u64),
        CellId::Bytes(bytes) => hasher.byte(1).u64(bytes.len() as u64).bytes(bytes),
    }
    .finish()
}

/// Byte and version totals that capacity admission reads without a lock.
///
/// Only the writer (under the `writer` mutex) and exclusive construction change
/// them, so each field has one writer at a time. Readers use `Relaxed` loads:
/// admission needs a recent total, not an ordering with other memory. The cache
/// line is private to this block because admission polls it from other threads.
#[repr(align(64))]
#[derive(Default)]
pub(super) struct Accounting {
    pub overlay: AtomicUsize,
    pub directory: AtomicUsize,
    pub loaded: AtomicUsize,
    pub retained_bytes: AtomicUsize,
    pub retained_versions: AtomicUsize,
}

impl Accounting {
    pub(super) fn retained_bytes(&self) -> usize {
        self.retained_bytes.load(Ordering::Relaxed)
    }

    pub(super) fn retained_versions(&self) -> usize {
        self.retained_versions.load(Ordering::Relaxed)
    }

    pub(super) fn overlay(&self) -> usize {
        self.overlay.load(Ordering::Relaxed)
    }

    /// Replace the derived totals after a full recount of an exclusively
    /// owned index (opening a snapshot).
    pub(super) fn set_recounted(
        &self,
        directory: usize,
        loaded: usize,
        versions: usize,
        retained_bytes: usize,
    ) {
        self.directory.store(directory, Ordering::Relaxed);
        self.loaded.store(loaded, Ordering::Relaxed);
        self.retained_versions.store(versions, Ordering::Relaxed);
        self.retained_bytes.store(retained_bytes, Ordering::Relaxed);
    }

    pub(super) fn resident(&self) -> usize {
        self.overlay.load(Ordering::Relaxed)
            + self.directory.load(Ordering::Relaxed)
            + self.loaded.load(Ordering::Relaxed)
    }
}

pub(super) fn add(counter: &AtomicUsize, amount: usize) {
    counter.fetch_add(amount, Ordering::Relaxed);
}

pub(super) fn sub(counter: &AtomicUsize, amount: usize) {
    let previous = counter.fetch_sub(amount, Ordering::Relaxed);
    debug_assert!(previous >= amount, "index accounting underflow");
}

/// Count of publications by number of shards they locked. Relaxed counters:
/// independent statistics.
#[repr(align(64))]
#[derive(Default)]
pub(super) struct PublicationHistogram {
    buckets: [AtomicU64; PUBLICATION_HISTOGRAM_BUCKETS],
}

impl PublicationHistogram {
    pub(super) fn bucket(shards_touched: usize) -> usize {
        if shards_touched <= 1 {
            0
        } else {
            ((shards_touched - 1).ilog2() as usize + 1).min(PUBLICATION_HISTOGRAM_BUCKETS - 1)
        }
    }

    pub(super) fn record(&self, shards_touched: usize) {
        if shards_touched != 0 {
            self.buckets[Self::bucket(shards_touched)].fetch_add(1, Ordering::Relaxed);
        }
    }

    fn snapshot(&self) -> [u64; PUBLICATION_HISTOGRAM_BUCKETS] {
        std::array::from_fn(|bucket| self.buckets[bucket].load(Ordering::Relaxed))
    }
}

/// Load of one index shard.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct IndexShardLoad {
    pub cells: usize,
    /// Shard lock acquisitions, contention and sampled hold time.
    pub locks: ShardLockStats,
}

/// Per-shard load, so a hot shard is visible, and the publication fan-out.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct IndexShardStats {
    pub shards: Vec<IndexShardLoad>,
    /// Publications by shards locked; see [`PUBLICATION_HISTOGRAM_BUCKETS`].
    pub publication_shards_touched: [u64; PUBLICATION_HISTOGRAM_BUCKETS],
}

impl NativeIndex {
    pub(super) fn shard_index(&self, keyspace: KeySpace, cell: &CellId) -> usize {
        (cell_hash(keyspace, cell) as usize) & self.shard_mask
    }

    pub(super) fn shard_for(&self, identity: &CellKey) -> &ShardRwLock<Cells> {
        &self.shards[self.shard_index(identity.0, &identity.1)]
    }

    /// Number of index shards.
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    /// Shard that owns `(keyspace, cell)`.
    pub fn shard_of_cell(&self, keyspace: KeySpace, cell: &CellId) -> usize {
        self.shard_index(keyspace, cell)
    }

    pub(super) fn contains_cell(&self, keyspace: KeySpace, cell: &CellId) -> bool {
        let identity = (keyspace, cell.clone());
        self.shard_for(&identity).read().contains_key(&identity)
    }

    /// Per-shard cells, lock activity and publication fan-out. Locks each
    /// shard in turn for its cell count and never holds two.
    pub fn shard_stats(&self) -> IndexShardStats {
        IndexShardStats {
            shards: self
                .shards
                .iter()
                .map(|shard| IndexShardLoad {
                    cells: shard.read().len(),
                    locks: shard.stats(),
                })
                .collect(),
            publication_shards_touched: self.publication_shards.snapshot(),
        }
    }
}
