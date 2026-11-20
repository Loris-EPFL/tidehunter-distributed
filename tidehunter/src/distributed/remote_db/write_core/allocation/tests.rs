use super::*;

const STREAM: u32 = 0;

fn version(sequence: u64) -> BatchVersion {
    BatchVersion { epoch: 7, sequence }
}

fn view() -> Arc<AllocationView> {
    Arc::new(AllocationView::new(1))
}

fn queue(view: &Arc<AllocationView>, capacity: usize) -> ExtentQueue {
    let capacity = NonZeroUsize::new(capacity).expect("test capacity is nonzero");
    ExtentQueue::new(Arc::clone(view), STREAM, 5, 9, capacity)
}

fn replay_from(view: &AllocationView) -> u64 {
    let fronts = view.fronts();
    assert_eq!(fronts.len(), 1);
    fronts[0].replay_from
}

/// An empty stream replays from its tail, which starts at zero: no extent has
/// been allocated yet.
#[test]
fn an_empty_stream_replays_from_the_tail() {
    let view = view();
    assert_eq!(replay_from(&view), 0);
}

/// The start is published before the end, and it is the oldest outstanding
/// extent: recovery must not begin after a batch the index has not seen.
#[test]
fn reserve_keeps_the_oldest_outstanding_start() {
    let view = view();
    let mut queue = queue(&view, 8);
    queue.reserve(version(1), 0, 100);
    assert_eq!(replay_from(&view), 0);
    queue.reserve(version(2), 100, 250);
    assert_eq!(replay_from(&view), 0);
    assert_eq!(queue.queued(), 2);
    // Tail is the end of the newest extent.
    assert_eq!(view.raw(STREAM).0, 250);
}

/// Draining is exclusive at the target: only batches strictly below the stored
/// cut are represented by the region the snapshot writes.
#[test]
fn drain_pops_only_below_the_stored_target() {
    let view = view();
    let mut queue = queue(&view, 8);
    queue.reserve(version(1), 0, 100);
    queue.reserve(version(2), 100, 200);
    queue.reserve(version(3), 200, 300);
    queue.drain_to(version(3));
    // Batch 3 is not below its own target, so it stays.
    assert_eq!(queue.queued(), 1);
    assert_eq!(replay_from(&view), 200);
    queue.drain_to(version(4));
    assert_eq!(queue.queued(), 0);
    // Empty again: the tail is the start.
    assert_eq!(replay_from(&view), 300);
}

/// A reservation the cap cannot hold is never silent: its offset becomes the
/// floor, which keeps `replay_from` at or below it until the stored cut passes
/// its version.
#[test]
fn a_cap_hit_pins_the_floor_until_the_target_passes_it() {
    let view = view();
    let mut queue = queue(&view, 1);
    queue.reserve(version(1), 0, 100);
    queue.reserve(version(2), 100, 200);
    assert_eq!(queue.cap_hits(), 1);
    assert_eq!(queue.queued(), 1);
    // Head 0 is still older than the dropped extent, so the floor is hidden.
    assert_eq!(replay_from(&view), 0);
    queue.drain_to(version(2));
    // Head popped, but the dropped batch 2 is *not* below target 2, so the
    // floor still starts replay at the dropped extent.
    assert_eq!(replay_from(&view), 100);
    queue.drain_to(version(3));
    // Target passed the dropped version: the floor is released and only the
    // tail remains.
    assert_eq!(replay_from(&view), 200);
    assert_eq!(queue.cap_hits(), 1);
}

/// RustReview's WL sequence: releasing the dropped floor must never leave the
/// start at the tail above an extent the stored cut does not cover. The negative
/// control is the earlier code, which published the front only when something
/// popped and only when no floor was set: step 4 then returned `tail` (E's end),
/// above E's offset, so recovery never replayed E.
///
/// To reproduce the failure deliberately, remove the `was_empty` head publish in
/// `reserve` — the hazard is that *fill-while-floor-set* store, not the floor's
/// release; dropping the cap arm's bookkeeping alone would not fail this test.
#[test]
fn releasing_the_floor_restores_the_start_above_the_new_head() {
    let view = view();
    let mut queue = queue(&view, 1);
    // v7 is dropped at the cap (v6 already holds the single slot).
    queue.reserve(version(6), 0, 100);
    queue.reserve(version(7), 100, 200);
    assert_eq!(queue.cap_hits(), 1);
    // Drain to the *dropped* version: v6 pops (`6 < 7`) and the floor stays
    // (`7 > 7` is false), so replay still starts at the dropped extent. A drain
    // to 6 would pop nothing, which is a different state.
    queue.drain_to(version(7));
    assert_eq!(queue.queued(), 0);
    assert_eq!(replay_from(&view), 100);
    // E (v9) is reserved while the floor is set.
    queue.reserve(version(9), 900, 1000);
    assert_eq!(replay_from(&view), 100);
    // A drain whose target passes the dropped version but not E clears the
    // floor. The start must still be at E's offset, not E's end.
    queue.drain_to(version(8));
    let e_offset = 900;
    assert!(
        replay_from(&view) <= e_offset,
        "replay start {} is above the uncovered extent at {e_offset}",
        replay_from(&view),
    );
    assert_eq!(replay_from(&view), e_offset);
}

/// A drain that removes nothing must leave the published start untouched.
#[test]
fn a_no_op_drain_does_not_move_the_start() {
    let view = view();
    let mut queue = queue(&view, 8);
    queue.reserve(version(5), 4096, 8192);
    queue.drain_to(version(5));
    assert_eq!(queue.queued(), 1);
    assert_eq!(replay_from(&view), 4096);
    // A later real store advances it.
    queue.drain_to(version(6));
    assert_eq!(replay_from(&view), 8192);
}

/// The allocated-but-unpublished crash cut, at the queue that defines it.
///
/// A store drained the stream through `v1`; `v2` is then written at the owner and
/// not yet published. A snapshot taken at that instant must still start replay at
/// `v2`'s offset: `v2` can publish before the process dies, and its member frame
/// is then the only copy of a published value. Skipping the frame loses it.
///
/// Negative control: the drained queue is empty, which is exactly the state in
/// which `front` is [`UNSET_OFFSET`]. A `reserve` that published only `tail`
/// leaves `fronts()` at the tail — the *end* of the frame the store does not
/// cover — so the assertion below fails with `replay_from == 200`. The store
/// half of the cut is the load-bearing one: `drain_to` already republishes the
/// head it leaves behind, and its own test (`a_no_op_drain_does_not_move_the_start`)
/// keeps that covered.
#[test]
fn an_unpublished_reservation_after_a_store_stays_in_the_replay_window() {
    let view = view();
    let mut queue = queue(&view, 8);
    queue.reserve(version(1), 0, 100);
    queue.drain_to(version(2));
    assert_eq!(queue.queued(), 0);
    assert_eq!(replay_from(&view), 100);
    // v2 is allocated at [100, 200) and its publication has not run.
    queue.reserve(version(2), 100, 200);
    let (tail, ..) = view.raw(STREAM);
    assert_eq!(tail, 200, "the reservation must move the tail");
    assert_eq!(
        replay_from(&view),
        100,
        "a snapshot taken now would skip the unpublished frame at 100",
    );
    // The next store covers it and only then may the start move past it.
    queue.drain_to(version(3));
    assert_eq!(queue.queued(), 0);
    assert_eq!(replay_from(&view), 200);
}
