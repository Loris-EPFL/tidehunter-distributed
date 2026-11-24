//! Event-driven pending-promotion threads on the hybrid table (D41).
//!
//! Native `Db::open` starts one promotion thread per mutex shard and a batch
//! commit unparks the shards it touched; the hybrid had neither, so a cell that
//! crossed the flush threshold while a flush was in flight waited for the next
//! write or snapshot. These tests pin the two halves of the wiring: the opener
//! registers a named thread per shard, and a publication unparks the shard that
//! owns its cell.
//!
//! Negative control: remove the `unpark` loop in `insert_published` and the
//! arrival assertion times out, because nothing else wakes the thread.

use super::{HybridTable, PublishedMutation, position, version};
use crate::distributed::codec::RecordKind;
use crate::distributed::durability::DurabilityTier;
use crate::distributed::hybrid::config::HybridIndexConfig;
use crate::distributed::hybrid::loader::CutSources;
use crate::failpoints::FailPoint;
use crate::key_shape::{KeyShape, KeyType};
use crate::latch::Latch;
use crate::metrics::Metrics;
use minibytes::Bytes;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tempdir::TempDir;

/// A promotion thread must run within this bound. It is far below any timer a
/// fallback could use (native's flat-promotion job sleeps 10 s), so a wakeup is
/// the only explanation for the arrival.
const DEADLINE: Duration = Duration::from_secs(5);

fn shape() -> Arc<KeyShape> {
    Arc::new(KeyShape::new_single(4, 1, KeyType::uniform(1)))
}

fn config(num_threads: usize) -> HybridIndexConfig {
    HybridIndexConfig {
        frag_size: 4096,
        wal_file_size: 16 * 1024,
        max_dirty_keys: 4,
        num_flusher_threads: 1,
        num_pending_promotion_threads: num_threads,
        ..HybridIndexConfig::default()
    }
}

fn open(name: &str, num_threads: usize) -> (TempDir, HybridTable, CutSources, Arc<Metrics>) {
    let dir = TempDir::new(name).expect("scratch directory");
    let cuts = CutSources::new(1, DurabilityTier::Native);
    let metrics = Metrics::new();
    let table = HybridTable::open(
        dir.path(),
        shape(),
        &config(num_threads),
        cuts.clone(),
        metrics.clone(),
    )
    .expect("open table");
    (dir, table, cuts, metrics)
}

fn publish(table: &HybridTable, cut_prefix: u64, keys: u8) {
    let keyspace = table.shape().iter_ks().next().expect("one keyspace").id();
    // One cell: every key reduces to the same cell.
    let keys: Vec<[u8; 4]> = (0..keys).map(|slot| [0, 0, 0, slot]).collect();
    let value = Bytes::from_static(b"v");
    let mutations: Vec<PublishedMutation<'_>> = keys
        .iter()
        .enumerate()
        .map(|(slot, key)| PublishedMutation {
            keyspace,
            key,
            kind: RecordKind::Put,
            position: position(cut_prefix, 100 + slot as u64),
            value: Some(&value),
        })
        .collect();
    table.insert_published(&mutations, 64).expect("publish");
    table.advance_applied(version(cut_prefix + 1));
}

#[test]
fn the_opener_registers_one_named_promotion_thread_per_shard() {
    for num_threads in [0, 1, 4] {
        let (_dir, table, _cuts, _metrics) = open("hybrid-pending-promotion-register", num_threads);
        assert_eq!(
            table.pending_promotion_threads.len(),
            num_threads.max(1),
            "the shard count is the configured thread count, at least one",
        );
        for (shard, slot) in table.pending_promotion_threads.iter().enumerate() {
            let worker = slot
                .get()
                .expect("the opener registers every thread before it returns");
            assert_eq!(
                worker.name(),
                Some(format!("pending-promotion-{shard}").as_str()),
                "shard {shard} must run the named promotion thread",
            );
        }
    }
}

#[test]
fn a_publication_wakes_the_promotion_thread_of_its_shard() {
    let (_dir, table, cuts, metrics) = open("hybrid-pending-promotion-wake", 2);
    // Park whichever promotion thread runs first, and count the arrivals.
    let (latch, guard) = Latch::new();
    let arrivals = Arc::new(AtomicUsize::new(0));
    table.large_table().fp.0.write().fp_promote_dirty_pending =
        FailPoint::latch_counting(latch, arrivals.clone());

    // Six keys in one cell cross `max_dirty_keys` (4). Every insert after the
    // crossing finds a flush in flight and skips its own flush request, so only
    // the event-driven job can complete the promotion.
    publish(&table, 2, 6);
    cuts.set_durable(version(3));

    let deadline = Instant::now() + DEADLINE;
    while arrivals.load(Ordering::SeqCst) == 0 {
        assert!(
            Instant::now() < deadline,
            "the publication did not wake a promotion thread",
        );
        std::thread::yield_now();
    }

    // Release the parked job and let it check the threshold. The run itself is
    // observable in the native per-job timer.
    drop(guard);
    let deadline = Instant::now() + DEADLINE;
    while metrics.pending_promotion_job_time_mcs.get() == 0 {
        assert!(
            Instant::now() < deadline,
            "the promotion thread did not finish a job",
        );
        std::thread::yield_now();
    }
}
