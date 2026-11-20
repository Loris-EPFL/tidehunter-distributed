//! Cut-front, latch and allocation-watermark contract (plan S4 acceptance).

use super::{
    AllocationView, AppliedFront, CutCell, CutFront, CutLatches, CutSources, PublishedFront,
    UNSET_OFFSET, ValueBytes, allocation_capacity, compose_last_processed, next_stored_target,
};
use crate::distributed::durability::DurabilityTier;
use crate::distributed::hybrid::region::StreamFront;
use crate::distributed::types::BatchVersion;
use crate::wal::position::IndexCut;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

fn v(epoch: u64, sequence: u64) -> BatchVersion {
    BatchVersion { epoch, sequence }
}

#[test]
fn frontier_is_the_minimum_of_durable_applied_and_latch() {
    let durable = CutFront::new(v(2, 5));
    let applied = AppliedFront::new(v(2, 3));
    let latches = CutLatches::new();

    assert_eq!(
        compose_last_processed(durable.get(), applied.get(), latches.min_held()),
        v(2, 3),
    );

    let held = latches.pin(v(2, 1));
    assert_eq!(
        compose_last_processed(durable.get(), applied.get(), latches.min_held()),
        v(2, 1),
    );
    drop(held);
    assert_eq!(
        compose_last_processed(durable.get(), applied.get(), latches.min_held()),
        v(2, 3),
    );
}

#[test]
fn a_latch_at_none_still_clamps_and_release_restores() {
    let latches = CutLatches::new();
    let held = latches.pin(<BatchVersion as IndexCut>::NONE);
    assert_eq!(latches.min_held(), Some(<BatchVersion as IndexCut>::NONE));
    assert_eq!(
        compose_last_processed(v(3, 9), v(3, 9), latches.min_held()),
        <BatchVersion as IndexCut>::NONE,
    );
    drop(held);
    assert_eq!(latches.min_held(), None);
}

/// Finding B: releasing the last latch must never expose `Some(NONE)` to a
/// lock-free reader (a flush requested then would commit NONE and regress the
/// cell). Bounded iterations, no sleeps.
#[test]
fn releasing_the_last_latch_never_exposes_a_none_minimum() {
    let latches = CutLatches::new();
    let stop = Arc::new(AtomicBool::new(false));
    let reader = {
        let latches = Arc::clone(&latches);
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            let mut seen = 0u64;
            while !stop.load(Ordering::Acquire) || seen < 1_000 {
                if let Some(min) = latches.min_held() {
                    assert_eq!(min, v(1, 5), "a reader saw a transient minimum");
                }
                seen += 1;
            }
        })
    };
    for _ in 0..20_000 {
        drop(latches.pin(v(1, 5)));
    }
    stop.store(true, Ordering::Release);
    reader.join().expect("reader");
}

/// "No latch" is a sentinel in the single seqlocked cell, so there is no second
/// word a reader could pair with a stale one (two loads race in either store
/// order). A latch at NONE stays distinct from "no latch".
#[test]
fn the_no_latch_sentinel_is_distinct_from_a_latch_at_none() {
    let latches = CutLatches::new();
    assert_eq!(latches.min_held(), None);
    let none = latches.pin(<BatchVersion as IndexCut>::NONE);
    assert_eq!(latches.min_held(), Some(<BatchVersion as IndexCut>::NONE));
    let real = latches.pin(v(1, 5));
    drop(none);
    assert_eq!(latches.min_held(), Some(v(1, 5)));
    drop(real);
    assert_eq!(latches.min_held(), None);
}

#[test]
fn cut_fronts_advance_monotonically_and_across_epochs() {
    let front = CutFront::new(<BatchVersion as IndexCut>::NONE);
    assert!(front.advance(v(1, 1)));
    assert!(!front.advance(v(1, 1)));
    assert!(!front.advance(v(1, 0)));
    assert!(front.advance(v(2, 0)), "a later epoch is always later");
    assert_eq!(front.get(), v(2, 0));

    let published = PublishedFront::new(v(1, 2));
    assert!(!published.advance(v(1, 1)));
    // The latch mirror's non-monotone store.
    let mirror = CutCell::new(v(1, 2));
    mirror.store(v(1, 1));
    assert_eq!(mirror.get(), v(1, 1));
}

#[test]
fn seqlock_never_exposes_a_torn_cut() {
    // One writer alternates two very different cuts through the non-monotone
    // store (the latch-mirror path); readers must never observe a mixture.
    let cut = Arc::new(CutCell::new(v(1, 111)));
    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let cut = Arc::clone(&cut);
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            for i in 0..20_000u64 {
                if i % 2 == 0 {
                    cut.store(v(1, 111));
                } else {
                    cut.store(v(2, 222));
                }
            }
            stop.store(true, Ordering::Release);
        })
    };
    let readers: Vec<_> = (0..3)
        .map(|_| {
            let cut = Arc::clone(&cut);
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                let mut observed = 0u64;
                while !stop.load(Ordering::Acquire) || observed < 1_000 {
                    let seen = cut.get();
                    assert!(
                        seen == v(1, 111) || seen == v(2, 222),
                        "torn cut observed: {seen:?}",
                    );
                    observed += 1;
                }
                observed
            })
        })
        .collect();
    writer.join().expect("writer");
    for reader in readers {
        assert!(reader.join().expect("reader") >= 1_000);
    }
}

#[test]
fn value_bytes_accumulate() {
    let bytes = ValueBytes::new(0);
    assert_eq!(bytes.add(100), 0);
    assert_eq!(bytes.get(), 100);
    assert_eq!(bytes.add(50), 100);
    assert_eq!(bytes.get(), 150);
}

#[test]
fn cut_sources_expose_the_epoch_setters_and_tier() {
    let cuts = CutSources::new(4, DurabilityTier::Synchronous);
    assert_eq!(cuts.durable(), v(4, 0));
    assert_eq!(cuts.tier(), DurabilityTier::Synchronous);

    cuts.set_durable(v(4, 3));
    cuts.set_applied(v(4, 2));
    cuts.set_published(v(4, 5));
    assert_eq!(cuts.durable(), v(4, 3));
    assert_eq!(cuts.applied(), v(4, 2));
    assert_eq!(cuts.published(), v(4, 5));

    cuts.set_durable(v(4, 1));
    assert_eq!(cuts.durable(), v(4, 3), "setters are monotone");

    cuts.add_bytes(100);
    cuts.add_bytes(50);
    assert_eq!(cuts.bytes(), 150);

    let held = cuts.pin_cut(v(4, 1));
    assert_eq!(cuts.lowest_held(), Some(v(4, 1)));
    drop(held);
    assert_eq!(cuts.lowest_held(), None);
}

fn view(generation: u64) -> AllocationView {
    let view = AllocationView::new(1);
    view.publish_generation(0, generation);
    view
}

#[test]
fn allocation_fronts_are_the_minimum_of_front_tail_and_floor() {
    let view = view(7);
    view.publish_tail(0, 30);
    view.publish_front(0, Some(10));
    view.publish_outstanding(0, 2);
    assert_eq!(
        view.fronts(),
        vec![StreamFront {
            partition: 0,
            generation: 7,
            replay_from: 10,
        }],
    );

    // A dropped floor below the head narrows the replay start further.
    view.publish_dropped_floor(0, 5);
    assert_eq!(view.fronts()[0].replay_from, 5);
}

#[test]
fn fronts_emit_the_published_partition() {
    let view = view(1);
    view.publish_partition(0, 5);
    view.publish_tail(0, 10);
    view.publish_front(0, None);
    assert_eq!(view.fronts()[0].partition, 5);
}

#[test]
fn an_empty_stream_replays_from_its_tail() {
    let view = view(3);
    view.publish_tail(0, 40);
    view.publish_front(0, None);
    assert_eq!(
        view.fronts(),
        vec![StreamFront {
            partition: 0,
            generation: 3,
            replay_from: 40,
        }],
    );
}

#[test]
fn a_dropped_floor_persists_until_the_worker_clears_it() {
    let view = view(1);
    view.publish_tail(0, 310);
    view.publish_front(0, None);
    view.publish_dropped_floor(0, 100);
    assert_eq!(view.fronts()[0].replay_from, 100);

    // The worker clears the floor only after the target passed every dropped
    // extent; until then `fronts` stays at the dropped offset.
    view.publish_front(0, None);
    assert_eq!(view.fronts()[0].replay_from, 100);

    view.publish_dropped_floor(0, UNSET_OFFSET);
    assert_eq!(view.fronts()[0].replay_from, 310);
}

#[test]
fn a_cleared_floor_keeps_a_later_extent_represented() {
    let view = view(1);
    // Queue [v5@100, v7@200]; v7 was dropped at the cap (floor 200).
    view.publish_tail(0, 210);
    view.publish_front(0, Some(100));
    view.publish_dropped_floor(0, 200);

    // DrainTo(t <= v7) pops everything: front UNSET, floor stays.
    view.publish_front(0, None);
    assert_eq!(view.fronts()[0].replay_from, 200);

    // v9 is reserved at 300 while the floor is set. The worker publishes the
    // front before the tail (Amendment 14), then clears the floor.
    view.publish_front(0, Some(300));
    view.publish_tail(0, 310);
    view.publish_dropped_floor(0, UNSET_OFFSET);
    assert!(view.fronts()[0].replay_from <= 300);
}

#[test]
fn clearing_the_floor_with_an_unset_front_would_skip_the_extent() {
    // Negative control for the caller contract: with an unset front and a
    // live v9 extent, clearing the floor returns the tail, which is above v9.
    // S7's ExtentQueue must publish the front before clearing the floor.
    let view = view(1);
    view.publish_tail(0, 210);
    view.publish_front(0, Some(100));
    view.publish_dropped_floor(0, 200);
    view.publish_front(0, None);
    view.publish_tail(0, 310);
    view.publish_dropped_floor(0, UNSET_OFFSET);
    assert_eq!(
        view.fronts()[0].replay_from,
        310,
        "the unsafe ordering skips an unrepresented extent",
    );
}

#[test]
fn an_interleaved_reservation_is_never_behind_the_returned_front() {
    // Deterministic model of the reader: tail load, then a reserve stores its
    // front, then the reader loads front. `fronts` must not exceed the
    // reserved offset.
    let view = view(1);
    view.publish_tail(0, 0);
    view.publish_front(0, None);

    let (tail_seen, front_before, dropped_seen, _) = view.raw(0);
    assert_eq!(front_before, UNSET_OFFSET);

    // The worker reserves offset 50 (end 100): front = min(MAX, 50), then tail.
    view.publish_front(0, Some(front_before.min(50)));
    view.publish_tail(0, 100);

    let (_, front_after, _, _) = view.raw(0);
    let replay_from = tail_seen.min(front_after).min(dropped_seen);
    assert!(
        replay_from <= 50,
        "a reservation after the tail load must still be replayed",
    );
}

#[test]
fn the_front_never_exceeds_the_oldest_unapplied_offset() {
    let view = Arc::new(AllocationView::new(1));
    view.publish_generation(0, 1);
    let oldest = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    let writer = {
        let view = Arc::clone(&view);
        let oldest = Arc::clone(&oldest);
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            let mut published = 0u64;
            for step in 1..=5_000u64 {
                let end = step * 10;
                // Batch `step` is applied at this step; the next unapplied
                // offset is `end`.
                oldest.store(end, Ordering::Release);
                view.publish_front(0, Some(end));
                view.publish_tail(0, end);
                assert!(end >= published, "the worker publishes monotonically");
                published = end;
            }
            stop.store(true, Ordering::Release);
        })
    };

    let readers: Vec<_> = (0..3)
        .map(|_| {
            let view = Arc::clone(&view);
            let oldest = Arc::clone(&oldest);
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                let mut reads = 0u64;
                while !stop.load(Ordering::Acquire) || reads < 1_000 {
                    let front = view.fronts()[0].replay_from;
                    // Read the limit *after* fronts: if the reader observed a
                    // newer front, the later `oldest` load observes its store.
                    let limit = oldest.load(Ordering::Acquire);
                    assert!(
                        front <= limit,
                        "front {front} is newer than the oldest unapplied {limit}",
                    );
                    reads += 1;
                }
                reads
            })
        })
        .collect();

    writer.join().expect("writer");
    for reader in readers {
        assert!(reader.join().expect("reader") >= 1_000);
    }
}

#[test]
fn stored_target_is_monotone() {
    let previous = v(1, 1);
    let target = next_stored_target(previous, v(1, 3), v(1, 4), None);
    assert_eq!(target, v(1, 3));

    // A checkpoint taken below the stored target must not lower it.
    let with_latch = next_stored_target(target, v(1, 5), v(1, 5), Some(v(1, 2)));
    assert_eq!(with_latch, target);
}

#[test]
fn the_max_configuration_queue_holds_one_snapshot_interval() {
    use crate::distributed::hybrid::config::HybridIndexConfig;

    let config = HybridIndexConfig::default();
    let capacity = allocation_capacity(config.snapshot_written_bytes, 20 * 1024, 1_024);
    assert_eq!(capacity.get(), 419_431 + 1_024);
}

#[test]
fn advance_with_an_equal_or_lower_cut_publishes_nothing() {
    let cell = super::CutCell::new(v(1, 5));
    let marker = cell.marker();
    assert!(!cell.advance(v(1, 5)));
    assert!(!cell.advance(v(1, 4)));
    assert_eq!(cell.marker(), marker, "a skipped advance writes no marker");
    assert_eq!(cell.get(), v(1, 5));
}

#[test]
fn a_paused_writer_never_yields_a_torn_cut() {
    use std::sync::mpsc;

    let cell = Arc::new(super::CutCell::new(v(1, 111)));
    let (odd_tx, odd_rx) = mpsc::sync_channel(1);
    let (resume_tx, resume_rx) = mpsc::sync_channel(1);

    let writer = {
        let cell = Arc::clone(&cell);
        std::thread::spawn(move || {
            cell.store_with_pause(v(2, 222), || {
                odd_tx.send(()).expect("signal odd marker");
                resume_rx.recv().expect("resume");
            });
        })
    };
    odd_rx.recv().expect("odd marker published");

    let reader = {
        let cell = Arc::clone(&cell);
        std::thread::spawn(move || {
            let mut seen = Vec::with_capacity(1_000);
            for _ in 0..1_000 {
                seen.push(cell.get());
            }
            seen
        })
    };
    resume_tx.send(()).expect("resume writer");
    writer.join().expect("writer");

    for cut in reader.join().expect("reader") {
        assert!(
            cut == v(1, 111) || cut == v(2, 222),
            "torn cut observed: {cut:?}",
        );
    }
}
