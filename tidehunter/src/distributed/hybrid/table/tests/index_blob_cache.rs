//! The native index blob cache on the hybrid index stack (D39).
//!
//! Two hybrid tables run the same workload over separate directories and
//! differ in one setting: `HybridIndexConfig::index_blob_cache_bytes`. Every
//! read of both must agree after the initial flush, after a promotion, after a
//! relocation and after a reopen. The cached table must also show the cache
//! doing its work: a blob outside the mmap window is filled on first read, and
//! a blob a flush replaced leaves the cache (the retirement path native `Db`
//! runs from `update_flushed_index`/`update_relocated_index`).
//!
//! Negative control: remove the `retire_index_blobs` call in
//! `FlusherHost::update_flushed_index` and the promotion assertions fail with
//! 0 drops and the replaced blob still resident.
//!
//! `update_relocated_index` reports the same positions, but only a replacement
//! L1 written by the relocation driver produces them. `relocation_apply` marks
//! the cell dirty and leaves persistence to the next store, which appends an L0
//! above the L1 and replaces nothing, so this test asserts the retirement on
//! the promotion only.

use super::{HybridTable, PublishedMutation, RelocationApply, member_offset, position, version};
use crate::cell::CellId;
use crate::distributed::codec::RecordKind;
use crate::distributed::durability::DurabilityTier;
use crate::distributed::hybrid::config::HybridIndexConfig;
use crate::distributed::hybrid::loader::CutSources;
use crate::distributed::hybrid::position::VersionPosition;
use crate::distributed::types::BatchVersion;
use crate::key_shape::{KeyShape, KeySpace, KeyType};
use crate::metrics::Metrics;
use minibytes::Bytes;
use std::sync::Arc;
use tempdir::TempDir;

/// Keys in the cell before the promotion round: below `l0_max_entries`, so the
/// first forced pass writes one L0 blob.
const FIRST_ROUND: u8 = 3;
/// Keys in the cell after the promotion round: the merged L0 then exceeds
/// `l0_max_entries`, so the flusher promotes instead of appending an L0.
const PROMOTION_ROUND: u8 = 8;
/// The cell this workload reads. The other cells only push the writer past
/// this cell's fragment.
const READ_CELL: u8 = 3;

/// 16 cells; a key's cell is its first byte `>> 4` (the native fixture shape).
fn shape() -> Arc<KeyShape> {
    Arc::new(KeyShape::new_single(4, 16, KeyType::uniform(1)))
}

fn cell_key(cell: u8, slot: u8) -> [u8; 4] {
    [cell << 4, 0, 0, slot]
}

/// Distinct byte offset for one key, so a repoint is visible in a read.
fn offset(cell: u8, slot: u8) -> u64 {
    1000 + u64::from(cell) * 100 + u64::from(slot)
}

/// A fragment small enough that a few flushed blobs push an earlier blob out
/// of the one-fragment mmap window, and a flush threshold small enough that
/// the second round of one cell promotes its L0.
fn config(cached: bool) -> HybridIndexConfig {
    let mut config = HybridIndexConfig {
        frag_size: 4096,
        wal_file_size: 16 * 1024,
        // `WalMaps` requires at least three maps; the window still slides.
        max_maps: 3,
        max_index_maps: Some(3),
        max_dirty_keys: 8,
        l0_max_entries: Some(8),
        snapshot_unload_threshold: 0,
        num_flusher_threads: 1,
        // 0 keeps occupancy-based force-relocation out of the run: only the
        // forced passes below rewrite blobs.
        index_min_occupancy_pct: 0,
        direct_io: false,
        ..HybridIndexConfig::default()
    };
    if cached {
        // One blob is a few hundred bytes here; 64 KiB holds every blob and
        // admits from the first byte.
        config.index_blob_cache_bytes = Some(64 * 1024);
        config.index_blob_cache_min_bytes = 1;
    }
    config
}

struct Differential {
    dir: TempDir,
    table: HybridTable,
    cuts: CutSources,
    metrics: Arc<Metrics>,
}

impl Differential {
    fn open(name: &str, cached: bool) -> Self {
        let dir = TempDir::new(name).expect("scratch directory");
        let cuts = CutSources::new(1, DurabilityTier::Native);
        let metrics = Metrics::new();
        let table = HybridTable::open(
            dir.path(),
            shape(),
            &config(cached),
            cuts.clone(),
            metrics.clone(),
        )
        .expect("open table");
        Self {
            dir,
            table,
            cuts,
            metrics,
        }
    }

    fn reopen(&mut self) {
        let config = self.table.config().clone();
        self.cuts = CutSources::new(2, DurabilityTier::Native);
        self.table = HybridTable::open(
            self.dir.path(),
            shape(),
            &config,
            self.cuts.clone(),
            self.metrics.clone(),
        )
        .expect("reopen table");
    }

    fn ks(&self) -> KeySpace {
        self.table
            .shape()
            .iter_ks()
            .next()
            .expect("one keyspace")
            .id()
    }

    fn cell(&self) -> CellId {
        self.table
            .shape()
            .ks(self.ks())
            .cell_id(&cell_key(READ_CELL, 0))
    }

    /// Publishes `slots` puts of one cell, each at a distinct offset, and
    /// advances the publication prefix past them.
    fn publish_cell(&self, cell: u8, slots: std::ops::Range<u8>, sequence: u64) {
        for slot in slots {
            let key = cell_key(cell, slot);
            let value = Bytes::from_static(b"v");
            self.table
                .insert_published(
                    &[PublishedMutation {
                        keyspace: self.ks(),
                        key: &key,
                        kind: RecordKind::Put,
                        position: position(sequence, offset(cell, slot)),
                        value: Some(&value),
                    }],
                    64,
                )
                .expect("publish");
        }
        self.table.advance_applied(version(sequence + 1));
        self.cuts.set_durable(version(sequence + 1));
    }

    /// One forced pass: flushes every dirty cell, waits for the flushers, and
    /// stores the region.
    fn flush(&self) {
        let cut = self.table.last_processed();
        self.table
            .snapshot_for_relocation(cut)
            .expect("forced pass");
    }

    /// The read cell's keys at `cut`: the whole compared surface.
    fn reads(&self, cut: BatchVersion) -> Vec<([u8; 4], Option<VersionPosition>)> {
        (0..(FIRST_ROUND + PROMOTION_ROUND))
            .map(|slot| {
                let key = cell_key(READ_CELL, slot);
                let found = self.table.get(self.ks(), &key, cut).expect("index read");
                (key, found)
            })
            .collect()
    }
}

fn assert_identical(cached: &Differential, uncached: &Differential, cut: BatchVersion, what: &str) {
    let cached_reads = cached.reads(cut);
    let uncached_reads = uncached.reads(cut);
    assert_eq!(
        cached_reads, uncached_reads,
        "{what}: the cached and uncached tables disagreed"
    );
    assert!(
        cached_reads.iter().any(|(_, found)| found.is_some()),
        "{what}: the workload read nothing back"
    );
}

#[test]
fn cached_and_uncached_hybrids_read_the_same_across_flush_promotion_relocation_and_reopen() {
    let mut cached = Differential::open("hybrid-cache-differential-cached", true);
    let mut uncached = Differential::open("hybrid-cache-differential-uncached", false);

    // Round 1: the read cell is flushed into an L0 blob while the writer's
    // window still maps it.
    for table in [&cached, &uncached] {
        table.publish_cell(READ_CELL, 0..FIRST_ROUND, 2);
        table.flush();
    }
    let cut = version(3);
    assert_identical(&cached, &uncached, cut, "after the first flush");
    assert_eq!(
        cached.metrics.index_blob_cache_fills.get(),
        0,
        "the fresh blob is still mapped, so nothing is admitted yet"
    );

    // Round 2: churn other cells until the read cell's blob is behind the
    // one-fragment window. Its reads then go through the syscall path.
    let fragment = cached.table.config().frag_size;
    let mut sequence = 10;
    for table in [&cached, &uncached] {
        for _ in 0..64 {
            if table.table.loader().index_writer().position() >= 6 * fragment {
                break;
            }
            for cell in 4..16u8 {
                table.publish_cell(cell, 0..2, sequence);
            }
            table.flush();
            sequence += 2;
        }
        assert!(
            table.table.loader().index_writer().position() >= 6 * fragment,
            "the churn did not move the index writer past the read cell's fragment"
        );
    }
    let cut = version(sequence);
    assert_identical(&cached, &uncached, cut, "outside the mmap window");
    assert_eq!(
        cached.metrics.index_blob_cache_fills.get(),
        1,
        "the first read outside the window fills the read cell's blob"
    );
    assert!(
        cached.metrics.index_blob_cache_bytes.get() > 0,
        "the blob is resident after the fill"
    );
    assert!(
        cached.metrics.index_blob_cache_hits.get() > 0,
        "the remaining keys of the same walk are served from the filled blob"
    );

    // A second read pass is served from the cache: no new fill.
    assert_identical(&cached, &uncached, cut, "second read of the filled blob");
    assert_eq!(
        cached.metrics.index_blob_cache_fills.get(),
        1,
        "a resident blob is served without a second fill"
    );

    // Round 3: the read cell crosses `l0_max_entries` on the next flush, so
    // the flusher promotes the merged L0 into an L1 blob and drops the old L0.
    let sequence = sequence + 2;
    for table in [&cached, &uncached] {
        table.publish_cell(
            READ_CELL,
            FIRST_ROUND..FIRST_ROUND + PROMOTION_ROUND,
            sequence,
        );
        table.flush();
    }
    let cut = version(sequence + 1);
    assert_identical(&cached, &uncached, cut, "after the promotion");
    assert!(
        cached.metrics.index_blob_cache_drops.get() >= 1,
        "the replaced L0 blob must be retired, drops {}",
        cached.metrics.index_blob_cache_drops.get()
    );
    assert_eq!(
        cached.metrics.index_blob_cache_bytes.get(),
        0,
        "the retired blob left the cache and the replacement is still mapped"
    );

    // Round 4: relocation re-points the cell's live entries at new addresses.
    for table in [&cached, &uncached] {
        let ks = table.ks();
        let cell = table.cell();
        let (view, latch) = table
            .table
            .relocation_view(ks, &cell, cut)
            .expect("relocation view");
        let repointed: Vec<_> = view
            .iter()
            .map(|(key, found)| {
                (
                    key.clone(),
                    found,
                    position(found.batch.sequence, found.address.offset + 1_000_000),
                )
            })
            .collect();
        assert!(
            !repointed.is_empty(),
            "the view must hold the cell's entries"
        );
        table
            .table
            .relocation_apply(
                ks,
                &cell,
                RelocationApply::Repoint(&repointed),
                &[(0u32, 1u64, 2_000_000u64)],
            )
            .expect("relocation apply");
        drop(latch);
    }
    assert_identical(&cached, &uncached, cut, "after the relocation");
    for table in [&cached, &uncached] {
        let key = cell_key(READ_CELL, 0);
        let found = table
            .table
            .get(table.ks(), &key, cut)
            .expect("index read")
            .expect("the relocated key");
        // The re-pointed position is built through the same fixture helper as
        // the original, so the shift applies to the member frame address.
        assert_eq!(
            found.address.offset,
            member_offset(member_offset(offset(READ_CELL, 0)) + 1_000_000),
            "the read must follow the repointed address"
        );
    }
    // Round 5: store, drop and reopen both directories. The forced pass writes
    // the repointed cell's next level; the reads of that level must match.
    for table in [&cached, &uncached] {
        table.flush();
    }
    cached.reopen();
    uncached.reopen();
    let reopened = BatchVersion {
        epoch: 2,
        sequence: 1,
    };
    assert_identical(&cached, &uncached, reopened, "after the reopen");
    assert!(
        cached
            .reads(reopened)
            .iter()
            .all(|(_, found)| found.is_some()),
        "the flushed cell must survive the reopen with every key"
    );
}
