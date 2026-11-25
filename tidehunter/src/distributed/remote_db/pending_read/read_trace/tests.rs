use super::*;
#[test]
fn bounded_groups_status_and_generations() {
    let trace = Arc::new(ReadTrace::default());
    assert!(trace.begin(1, 2).is_none());
    trace.arm(1);
    let call = trace.begin(7, 3).unwrap();
    let first = call.child(1, vec![0, 2]).unwrap();
    let second = call.child(2, vec![1]).unwrap();
    first.classify_members(std::iter::once(1));
    first.lane(4, 12);
    first.event_io("sqe", 1, 42);
    second.event("failed");
    assert!(trace.begin(8, 1).is_none());
    call.finish(false);
    let snapshot = trace.snapshot();
    assert_eq!(snapshot.dropped_calls, 1);
    assert_eq!(snapshot.calls[0].status, "error");
    assert_eq!(snapshot.calls[0].groups[0].members, [0, 2]);
    assert!(snapshot.calls[0].groups[0].membership_classified);
    assert_eq!(snapshot.calls[0].groups[0].coalesced_members, [2]);
    assert_eq!(snapshot.calls[0].groups[0].events[0].io_token, Some(42));
    assert_eq!(snapshot.calls[0].groups[1].owner, 2);
    trace.arm(1);
    let next = trace.begin(9, 1).unwrap();
    first.event("stale");
    drop(next);
    let snapshot = trace.snapshot();
    assert_eq!(snapshot.calls[0].status, "cancelled_or_incomplete");
    assert!(snapshot.calls[0].groups.is_empty());
    trace.arm(0);
    assert!(trace.begin(10, 1).is_none());
}
#[test]
fn tokens_overflow_and_contention_are_explicit() {
    let trace = Arc::new(ReadTrace::default());
    trace.arm(1);
    let call = trace.begin(1, 2).unwrap();
    let child = call.child(0, vec![0, 1]).unwrap();
    child.event_io("live_io_sqe_submitted", 7, 19);
    child.event_io("live_io_cqe_observed", 7, 19);
    for _ in 2..=MAX_EVENTS {
        child.event("extra");
    }
    for _ in 1..MAX_CHILDREN {
        assert!(call.child(0, vec![]).is_some());
    }
    assert!(call.child(0, vec![]).is_none());
    let held = trace.inner.lock();
    child.event("contended");
    drop(held);
    call.finish(true);
    let snapshot = trace.snapshot();
    let call = &snapshot.calls[0];
    assert_eq!(call.dropped_children, 1);
    assert_eq!(call.groups[0].dropped_events, 1);
    assert_eq!(snapshot.contention_drops, 1);
    assert_eq!(call.groups[0].events[0].io_queue_id, Some(7));
    assert_eq!(
        call.groups[0].events[0].io_token,
        call.groups[0].events[1].io_token
    );
}
/// A group whose creation meets a held lock is shed and counted exactly
/// once, so a caller that sees fewer groups than it submitted can account
/// for every missing group with `contention_drops`.
#[test]
fn contended_group_creation_is_shed_and_counted_once() {
    let trace = Arc::new(ReadTrace::default());
    trace.arm(1);
    let call = trace.begin(1, 3).unwrap();
    let first = call.child(0, vec![0, 2]).unwrap();
    let held = trace.inner.lock();
    assert!(call.child(1, vec![1]).is_none());
    drop(held);
    first.event("delivered");
    call.finish(true);
    let snapshot = trace.snapshot();
    let call = &snapshot.calls[0];
    assert_eq!(call.status, "complete");
    assert_eq!(call.groups.len(), 1);
    assert_eq!(call.groups[0].members, [0, 2]);
    assert_eq!(call.dropped_children, 0);
    assert_eq!(snapshot.contention_drops, 1);
}
#[test]
fn byte_budget_drops_calls_and_children() {
    let trace = Arc::new(ReadTrace::default());
    trace.arm(10);
    let call = trace.begin(1, 1).unwrap();
    trace.inner.lock().snapshot.budget_used = TRACE_BUDGET - 1;
    assert!(call.child(0, vec![0]).is_none());
    assert!(trace.begin(2, 1).is_none());
    call.finish(true);
    let snapshot = trace.snapshot();
    assert_eq!(snapshot.budget_drops, 2);
    assert_eq!(snapshot.dropped_calls, 1);
    assert_eq!(snapshot.calls[0].dropped_children, 1);
}
