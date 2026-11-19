use super::*;

#[test]
fn overlapping_waiters_union_once_and_sum_per_waiter() {
    let mut tracker = HardWaitTracker::default();
    let (mut first, mut second) = (WaiterWait::default(), WaiterWait::default());
    tracker.set(&mut first, WAIT_IDENTITY, 100);
    tracker.set(&mut second, WAIT_IDENTITY, 150);
    tracker.set(&mut first, 0, 200);
    tracker.set(&mut second, 0, 300);
    let snapshot = tracker.snapshot(1_000);
    // Union [100, 300); waiter sum 100 + 150; two waits; longest 150.
    assert_eq!(snapshot.identity.union_ns, 200);
    assert_eq!(snapshot.identity.waiter_ns, 250);
    assert_eq!(snapshot.identity.waits, 2);
    assert_eq!(snapshot.identity.max_ns, 150);
    assert_eq!(snapshot.identity.waiting_now, 0);
    assert_eq!(snapshot.any_hard_wait_ns, 200);
    assert_eq!(snapshot.other, WaitReasonTotals::default());
}

#[test]
fn simultaneous_reasons_overlap_and_other_is_not_hard() {
    let mut tracker = HardWaitTracker::default();
    let mut waiter = WaiterWait::default();
    tracker.set(&mut waiter, WAIT_IDENTITY | WAIT_INDEX, 10);
    // The identity dimension is relieved; index still blocks.
    tracker.set(&mut waiter, WAIT_INDEX, 40);
    tracker.set(&mut waiter, WAIT_OTHER, 70);
    tracker.set(&mut waiter, 0, 100);
    let snapshot = tracker.snapshot(100);
    assert_eq!(snapshot.identity.union_ns, 30);
    assert_eq!(snapshot.index.union_ns, 60);
    assert_eq!(snapshot.other.union_ns, 30);
    // Hard union covers [10, 70) once, although two reasons overlapped.
    assert_eq!(snapshot.any_hard_wait_ns, 60);
    assert_eq!(snapshot.replay_tail, WaitReasonTotals::default());
}

#[test]
fn snapshot_includes_open_interval_without_completing_it() {
    let mut tracker = HardWaitTracker::default();
    let mut waiter = WaiterWait::default();
    tracker.set(&mut waiter, WAIT_REPLAY_TAIL, 5);
    let open = tracker.snapshot(25);
    assert_eq!(open.replay_tail.union_ns, 20);
    assert_eq!(open.replay_tail.waiter_ns, 0);
    assert_eq!(open.replay_tail.waiting_now, 1);
    assert_eq!(open.any_hard_wait_ns, 20);
    tracker.set(&mut waiter, 0, 45);
    let closed = tracker.snapshot(1_000);
    assert_eq!(closed.replay_tail.union_ns, 40);
    assert_eq!(closed.replay_tail.waiter_ns, 40);
    assert_eq!(closed.replay_tail.waiting_now, 0);
}

#[test]
fn ending_an_idle_waiter_changes_nothing() {
    let mut tracker = HardWaitTracker::default();
    let mut waiter = WaiterWait::default();
    tracker.set(&mut waiter, 0, 10);
    assert_eq!(tracker.snapshot(20), HardWaitSnapshot::default());
}
