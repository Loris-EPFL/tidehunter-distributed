//! Best-effort bounded caller-local spans. Clocks from different hosts are never mixed.
use parking_lot::Mutex;
use serde::Serialize;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::Instant;

const MAX_CALLS: usize = 65_536;
const MAX_CHILDREN: usize = 256;
const MAX_EVENTS: usize = 512;
// Conservative serialized-size charging also bounds retained allocations.
const TRACE_BUDGET: usize = 4 * 1024 * 1024;

#[derive(Clone, Serialize)]
pub struct ReadTraceEvent {
    pub name: &'static str,
    pub elapsed_ns: u64,
    pub io_queue_id: Option<u64>,
    pub io_token: Option<u64>,
}
#[derive(Clone, Serialize)]
pub struct ReadTraceGroup {
    pub owner: usize,
    pub members: Vec<usize>,
    pub coalesced_members: Vec<usize>,
    pub membership_classified: bool,
    pub lane: Option<usize>,
    pub flight_slot: Option<usize>,
    pub events: Vec<ReadTraceEvent>,
    pub dropped_events: u64,
}
#[derive(Clone, Serialize)]
pub struct ReadTraceCall {
    pub caller_id: u64,
    pub members: usize,
    pub status: &'static str,
    pub events: Vec<ReadTraceEvent>,
    pub groups: Vec<ReadTraceGroup>,
    pub dropped_children: u64,
    pub dropped_events: u64,
}
#[derive(Default, Clone, Serialize)]
pub struct ReadTraceSnapshot {
    pub limit: usize,
    pub calls: Vec<ReadTraceCall>,
    pub dropped_calls: u64,
    pub contention_drops: u64,
    pub budget_drops: u64,
    #[serde(skip)]
    budget_used: usize,
}
#[derive(Default)]
struct Inner {
    generation: u64,
    snapshot: ReadTraceSnapshot,
}
#[derive(Default)]
pub(super) struct ReadTrace {
    inner: Mutex<Inner>,
    limit: AtomicU64,
    contention: AtomicU64,
}
impl ReadTrace {
    pub fn arm(&self, limit: u64) {
        self.limit.store(0, Ordering::Release);
        if limit != 0 {
            let mut inner = self.inner.lock();
            inner.generation = inner.generation.wrapping_add(1);
            inner.snapshot = ReadTraceSnapshot {
                limit: (limit as usize).min(MAX_CALLS),
                ..Default::default()
            };
            self.contention.store(0, Ordering::Relaxed);
            self.limit
                .store(limit.min(MAX_CALLS as u64), Ordering::Release);
        }
    }
    pub fn snapshot(&self) -> ReadTraceSnapshot {
        let mut snapshot = self.inner.lock().snapshot.clone();
        snapshot.contention_drops = self.contention.load(Ordering::Relaxed);
        snapshot
    }
    pub fn begin(self: &Arc<Self>, caller_id: u64, members: usize) -> Option<ReadTraceCallGuard> {
        if self.limit.load(Ordering::Acquire) == 0 {
            return None;
        }
        let Some(mut inner) = self.inner.try_lock() else {
            self.contention.fetch_add(1, Ordering::Relaxed);
            return None;
        };
        if self.limit.load(Ordering::Acquire) == 0 {
            return None;
        }
        if inner.snapshot.calls.len() >= inner.snapshot.limit {
            inner.snapshot.dropped_calls += 1;
            return None;
        }
        if !reserve(&mut inner.snapshot, 512) {
            inner.snapshot.dropped_calls += 1;
            return None;
        }
        let index = inner.snapshot.calls.len();
        let generation = inner.generation;
        inner.snapshot.calls.push(ReadTraceCall {
            caller_id,
            members,
            status: "incomplete",
            events: Vec::new(),
            groups: Vec::new(),
            dropped_children: 0,
            dropped_events: 0,
        });
        Some(ReadTraceCallGuard {
            trace: self.clone(),
            generation,
            index,
            origin: Instant::now(),
            finished: false,
        })
    }
    fn update(&self, generation: u64, f: impl FnOnce(&mut ReadTraceSnapshot)) {
        if let Some(mut inner) = self.inner.try_lock() {
            if inner.generation == generation {
                f(&mut inner.snapshot);
            }
        } else {
            self.contention.fetch_add(1, Ordering::Relaxed);
        }
    }
}
fn reserve(snapshot: &mut ReadTraceSnapshot, bytes: usize) -> bool {
    if bytes > TRACE_BUDGET.saturating_sub(snapshot.budget_used) {
        snapshot.budget_drops += 1;
        false
    } else {
        snapshot.budget_used += bytes;
        true
    }
}
fn stamp(origin: Instant, name: &'static str) -> ReadTraceEvent {
    ReadTraceEvent {
        name,
        elapsed_ns: origin.elapsed().as_nanos().min(u64::MAX as u128) as u64,
        io_queue_id: None,
        io_token: None,
    }
}
pub(crate) struct ReadTraceCallGuard {
    trace: Arc<ReadTrace>,
    generation: u64,
    index: usize,
    origin: Instant,
    finished: bool,
}
impl ReadTraceCallGuard {
    pub(crate) fn event(&self, name: &'static str) {
        let event = stamp(self.origin, name);
        self.trace.update(self.generation, |s| {
            if self.index >= s.calls.len() {
                return;
            }
            let room = s.calls[self.index].events.len() < MAX_EVENTS && reserve(s, 256);
            let call = &mut s.calls[self.index];
            if room {
                call.events.push(event);
            } else {
                call.dropped_events += 1;
            }
        });
    }
    pub(super) fn child(&self, owner: usize, members: Vec<usize>) -> Option<ReadTraceChild> {
        let mut index = None;
        self.trace.update(self.generation, |s| {
            if self.index >= s.calls.len() {
                return;
            }
            let room = s.calls[self.index].groups.len() < MAX_CHILDREN
                && reserve(s, 512usize.saturating_add(members.len().saturating_mul(64)));
            let call = &mut s.calls[self.index];
            if room {
                index = Some(call.groups.len());
                call.groups.push(ReadTraceGroup {
                    owner,
                    members,
                    coalesced_members: Vec::new(),
                    membership_classified: false,
                    lane: None,
                    flight_slot: None,
                    events: Vec::new(),
                    dropped_events: 0,
                });
            } else {
                call.dropped_children += 1;
            }
        });
        index.map(|child| ReadTraceChild {
            trace: self.trace.clone(),
            generation: self.generation,
            call: self.index,
            child,
            origin: self.origin,
        })
    }
    pub(crate) fn finish(mut self, success: bool) {
        self.finished = true;
        self.trace.update(self.generation, |s| {
            if let Some(call) = s.calls.get_mut(self.index) {
                call.status = if success { "complete" } else { "error" };
            }
        });
    }
}
impl Drop for ReadTraceCallGuard {
    fn drop(&mut self) {
        if !self.finished {
            self.trace.update(self.generation, |s| {
                if let Some(call) = s.calls.get_mut(self.index) {
                    call.status = "cancelled_or_incomplete";
                }
            });
        }
    }
}
#[derive(Clone)]
pub(crate) struct ReadTraceChild {
    trace: Arc<ReadTrace>,
    generation: u64,
    call: usize,
    child: usize,
    origin: Instant,
}
impl ReadTraceChild {
    pub(super) fn classify_members(&self, positions: impl Iterator<Item = usize>) {
        self.trace.update(self.generation, |s| {
            if let Some(group) = s
                .calls
                .get_mut(self.call)
                .and_then(|call| call.groups.get_mut(self.child))
            {
                for position in positions {
                    if let Some(member) = group.members.get(position) {
                        group.coalesced_members.push(*member);
                    }
                }
                group.membership_classified = true;
            }
        });
    }
    pub(super) fn lane(&self, lane: usize, flight_slot: usize) {
        self.trace.update(self.generation, |s| {
            if let Some(group) = s
                .calls
                .get_mut(self.call)
                .and_then(|call| call.groups.get_mut(self.child))
            {
                group.lane = Some(lane);
                group.flight_slot = Some(flight_slot);
            }
        });
    }
    pub(crate) fn event_io(&self, name: &'static str, queue_id: u64, token: u64) {
        let mut event = stamp(self.origin, name);
        event.io_queue_id = Some(queue_id);
        event.io_token = Some(token);
        self.push(event);
    }
    pub(crate) fn event(&self, name: &'static str) {
        self.push(stamp(self.origin, name));
    }
    fn push(&self, event: ReadTraceEvent) {
        self.trace.update(self.generation, |s| {
            if s.calls
                .get(self.call)
                .is_none_or(|call| self.child >= call.groups.len())
            {
                return;
            }
            let room =
                s.calls[self.call].groups[self.child].events.len() < MAX_EVENTS && reserve(s, 256);
            let group = &mut s.calls[self.call].groups[self.child];
            if room {
                group.events.push(event);
            } else {
                group.dropped_events += 1;
            }
        });
    }
}
#[cfg(test)]
mod tests {
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
}
