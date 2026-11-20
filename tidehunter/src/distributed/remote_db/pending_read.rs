//! Bounded coordinator-owned reads use one actor per owner endpoint. Each lane
//! has one progress owner at a time: its actor or an idle caller. Owner-local
//! progress follows eRPC's per-core ownership principle, not its wire protocol.
//! Exact-reference fill sharing adapts native DecompressCache::FillCell and
//! CacheLib's fill-context waiters, not its mutable-key tombstone protocol:
//! <https://github.com/facebook/CacheLib/blob/a24370092f0bd6fa7145b2f01c3e50e63edd6a6e/cachelib/allocator/nvmcache/NvmCache.h#L767-L848>.
//! Fixed weak slots and an exact-reference index are reserved from the pending
//! byte allowance. Every caller owns bounded result/slot credits; only new
//! storage work owns a request credit. Pins and authenticated completion checks
//! survive delivery cancellation.
use super::*;
use crate::distributed::native_index::IndexReadPin;
use smallvec::SmallVec;
use std::hash::BuildHasher;
use std::sync::Weak;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
/// A batch member with the plan computed once at batch creation.
struct PlannedMember {
    slot: usize,
    reference: ValueRecordReference,
    plan: MemberPlan,
}
#[cfg(test)]
mod kv_capture_tests;
mod live_inline;
#[cfg(test)]
mod live_inline_tests;
mod live_lane;
mod read_trace;

/// One member's admission facts, computed once per request or batch: pending
/// bytes to charge.
#[derive(Clone, Copy)]
struct MemberPlan {
    charge: usize,
}
/// Plans made once for one request or batch.
#[derive(Clone, Copy)]
struct PlannedMembers<'a> {
    members: &'a [MemberPlan],
}
/// One member's plan, kept across admission retries.
struct CachedPlan {
    member: [MemberPlan; 1],
}
use crate::distributed::sui::kv_capture;
use read_trace::ReadTrace;
pub use read_trace::ReadTraceSnapshot;
pub(crate) use read_trace::{ReadTraceCallGuard, ReadTraceChild};

const NO_INDEX: usize = usize::MAX;

struct FlightIndexEntry {
    node: usize,
    reference: ValueRecordReference,
    slot: usize,
    member: usize,
    bucket_next: usize,
    slot_next: usize,
}

// Fixed-capacity separate chaining avoids hash-table growth and retains no
// uncharged allocations when the last consumer drops a flight. Stale entries
// are removed when their slot is reused or when index capacity is needed.
struct FlightRegistry {
    slots: Box<[Weak<ReadFlight>]>,
    slot_heads: Box<[usize]>,
    buckets: Box<[usize]>,
    entries: Box<[Option<FlightIndexEntry>]>,
    free: Vec<usize>,
    next_slot: usize,
    hasher: std::collections::hash_map::RandomState,
}

impl FlightRegistry {
    const ENTRY_BYTES: usize =
        std::mem::size_of::<Option<FlightIndexEntry>>() + 3 * std::mem::size_of::<usize>(); // free stack and two buckets

    fn new(slots: usize, capacity: usize) -> Self {
        Self {
            slots: (0..slots).map(|_| Weak::new()).collect(),
            slot_heads: vec![NO_INDEX; slots].into_boxed_slice(),
            buckets: vec![NO_INDEX; capacity * 2].into_boxed_slice(),
            entries: std::iter::repeat_with(|| None).take(capacity).collect(),
            free: (0..capacity).rev().collect(),
            next_slot: 0,
            hasher: std::collections::hash_map::RandomState::new(),
        }
    }

    fn available_slot(&mut self, ready: &[FlightSignal]) -> Option<usize> {
        for offset in 0..self.slots.len() {
            let slot = (self.next_slot + offset) % self.slots.len();
            if self.slots[slot].strong_count() == 0 && ready[slot].is_ready() {
                self.next_slot = (slot + 1) % self.slots.len();
                return Some(slot);
            }
        }
        None
    }

    fn bucket(&self, node: usize, reference: &ValueRecordReference) -> usize {
        (self.hasher.hash_one((node, reference)) as usize) % self.buckets.len()
    }

    #[allow(
        clippy::expect_used,
        reason = "the bucket chain only holds allocated entries, so every index resolves"
    )]
    fn find(
        &self,
        node: usize,
        reference: &ValueRecordReference,
    ) -> Option<(Arc<ReadFlight>, usize)> {
        let mut index = self.buckets[self.bucket(node, reference)];
        while index != NO_INDEX {
            let entry = self.entries[index]
                .as_ref()
                .expect("allocated flight entry");
            if entry.node == node
                && entry.reference == *reference
                && let Some(flight) = self.slots[entry.slot].upgrade()
            {
                return Some((flight, entry.member));
            }
            index = entry.bucket_next;
        }
        None
    }

    #[allow(
        clippy::expect_used,
        reason = "the bucket chain only holds allocated entries, so every index resolves"
    )]
    fn clear_slot(&mut self, slot: usize) {
        let mut index = std::mem::replace(&mut self.slot_heads[slot], NO_INDEX);
        while index != NO_INDEX {
            let entry = self.entries[index].take().expect("allocated flight entry");
            let bucket = self.bucket(entry.node, &entry.reference);
            let mut current = self.buckets[bucket];
            let mut previous = NO_INDEX;
            while current != index {
                previous = current;
                current = self.entries[current]
                    .as_ref()
                    .expect("allocated flight entry")
                    .bucket_next;
            }
            if previous == NO_INDEX {
                self.buckets[bucket] = entry.bucket_next;
            } else {
                self.entries[previous]
                    .as_mut()
                    .expect("allocated flight entry")
                    .bucket_next = entry.bucket_next;
            }
            self.free.push(index);
            index = entry.slot_next;
        }
        self.slots[slot] = Weak::new();
    }

    fn reserve_entries(&mut self, count: usize, ready: &[FlightSignal]) -> bool {
        if self.free.len() < count {
            for (slot, signal) in ready.iter().enumerate() {
                if self.slots[slot].strong_count() == 0 && signal.is_ready() {
                    self.clear_slot(slot);
                    if self.free.len() >= count {
                        break;
                    }
                }
            }
        }
        self.free.len() >= count
    }

    #[allow(
        clippy::expect_used,
        reason = "flight index capacity is reserved per admitted request"
    )]
    fn insert(&mut self, flight: &Arc<ReadFlight>) {
        let slot = flight.slot;
        self.clear_slot(slot);
        self.slots[slot] = Arc::downgrade(flight);
        for (member, reference) in flight.references.iter().enumerate() {
            let bucket = self.bucket(flight.node, reference);
            let index = self.free.pop().expect("flight index capacity reserved");
            self.entries[index] = Some(FlightIndexEntry {
                node: flight.node,
                reference: *reference,
                slot,
                member,
                bucket_next: self.buckets[bucket],
                slot_next: self.slot_heads[slot],
            });
            self.buckets[bucket] = index;
            self.slot_heads[slot] = index;
        }
    }
}

const BACKPRESSURE: &str = "remote read admission is full";
const BYTE_BUDGET: &str = "request exceeds pending read byte budget";
const MIN_MEMBER_BYTES: usize = 3 * 256
    + 3 * std::mem::size_of::<ValueRecordReference>()
    + 64
    + 3 * (std::mem::size_of::<(ValueRecordReference, usize)>() + 1);

#[derive(Clone, Copy, Eq, PartialEq)]
enum ReadClass {
    Point,
    ScanNoFill,
}

fn member_charge(reference: &ValueRecordReference) -> Result<usize, LocalDbError> {
    let wire = (reference.descriptor.len as usize)
        .checked_add(256)
        .filter(|n| *n <= super::super::tcp::HARD_FRAME_LIMIT - 32)
        .ok_or_else(|| LocalDbError::new("record exceeds read frame bound"))?;
    // Three buckets per member cover HashMap's power-of-two growth and
    // control bytes even immediately after a resize.
    Ok(wire * 3
        + 3 * std::mem::size_of::<ValueRecordReference>()
        + 64
        + 3 * (std::mem::size_of::<(ValueRecordReference, usize)>() + 1)
        // Cover geometric growth and the minimum allocation of both vectors.
        + 4 * (std::mem::size_of::<Weak<BatchSignal>>() + std::mem::size_of::<usize>())
        // Every admitted batch has at least one member; this covers its Arc.
        + std::mem::size_of::<BatchSignal>() + 2 * std::mem::size_of::<usize>())
}

pub(super) fn admission_error(error: &LocalDbError) -> bool {
    matches!(error.message(), BACKPRESSURE | BYTE_BUDGET)
}

struct ReadContext {
    pin: Option<IndexReadPin>,
    trace: Option<ReadTraceChild>,
}

/// A caller-bounded collection, submitted as the largest fitting shard groups.
/// Only its admitted requests own transport/result credits; completed values
/// move into the caller's collection before another internal wave is admitted.
pub(super) struct ReadBatch {
    pub(super) values: Vec<Option<Mutation>>,
    pub(super) reads: VecDeque<(Vec<usize>, ReadRequest)>,
    class: ReadClass,
    pub(super) pin: Option<IndexReadPin>,
    groups: VecDeque<(usize, VecDeque<PlannedMember>)>,
    signal: Option<Arc<BatchSignal>>,
}

impl ReadBatch {
    pub(super) fn new_scan(
        service: &ReadService,
        values: Vec<Option<Mutation>>,
        groups: Vec<Vec<(usize, ValueRecordReference)>>,
    ) -> Result<Self, LocalDbError> {
        let mut batch = Self::new(service, values, groups)?;
        batch.class = ReadClass::ScanNoFill;
        Ok(batch)
    }
    pub(super) fn new(
        service: &ReadService,
        values: Vec<Option<Mutation>>,
        groups: Vec<Vec<(usize, ValueRecordReference)>>,
    ) -> Result<Self, LocalDbError> {
        // Plan every member once: dispatch and every admission retry reuse
        // these plans.
        let mut planned = VecDeque::with_capacity(groups.len());
        for (node, group) in groups.into_iter().enumerate() {
            if group.is_empty() {
                continue;
            }
            let mut members = VecDeque::with_capacity(group.len());
            for (slot, reference) in group {
                let plan = service.plan_member(&reference)?;
                // Reject an impossible member before issuing any of this batch's RPCs.
                if plan.charge > service.max_bytes().saturating_sub(512) {
                    return Err(LocalDbError::new(BYTE_BUDGET));
                }
                members.push_back(PlannedMember {
                    slot,
                    reference,
                    plan,
                });
            }
            planned.push_back((node, members));
        }
        Ok(Self {
            values,
            class: ReadClass::Point,
            pin: None,
            reads: VecDeque::new(),
            signal: None,
            groups: planned,
        })
    }

    pub(super) fn has_unsubmitted(&self) -> bool {
        !self.groups.is_empty()
    }

    pub(super) fn complete(&self) -> bool {
        self.groups.is_empty() && self.reads.is_empty()
    }

    pub(super) fn dispatch(&mut self, service: &ReadService) -> Result<(), LocalDbError> {
        self.dispatch_traced(service, None, 0)
    }

    #[allow(
        clippy::expect_used,
        reason = "the loop bound proves the group deque is nonempty"
    )]
    fn dispatch_traced(
        &mut self,
        service: &ReadService,
        trace: Option<&ReadTraceCallGuard>,
        offset: usize,
    ) -> Result<(), LocalDbError> {
        let mut skipped = 0;
        while skipped < self.groups.len() {
            let (node, group) = self.groups.front_mut().expect("nonempty groups");
            let retained = *service.state.retained.lock();
            let available = service.max_bytes() - retained.1;
            let mut bytes = 512usize;
            let mut count = 0;
            for member in group.iter().take(256) {
                let charge = member.plan.charge;
                if charge > available.saturating_sub(bytes) {
                    break;
                }
                bytes += charge;
                count += 1;
            }
            if count == 0 {
                self.groups.rotate_left(1);
                skipped += 1;
                continue;
            }
            // Other callers can reserve credits after the snapshot above.
            // Retain every source slot until this request is actually admitted.
            let mut slots = Vec::with_capacity(count);
            let mut references = Vec::with_capacity(count);
            let mut plans = SmallVec::<[MemberPlan; 32]>::new();
            for member in group.iter().take(count) {
                slots.push(member.slot);
                references.push(member.reference);
                plans.push(member.plan);
            }
            let child = trace.and_then(|trace| {
                trace.child(*node, slots.iter().map(|slot| slot + offset).collect())
            });
            let mut request = match service.submit_until_planned(
                *node,
                &references,
                0,
                false,
                ReadContext {
                    pin: self.pin.clone(),
                    trace: child.clone(),
                },
                Some(PlannedMembers { members: &plans }),
            ) {
                Ok(request) => request,
                Err(error) if RemoteDatabase::read_backpressured(&error) => {
                    if let Some(trace) = &child {
                        trace.event("admission_backpressured");
                    }
                    self.groups.rotate_left(1);
                    skipped += 1;
                    continue;
                }
                Err(error) => {
                    if let Some(trace) = &child {
                        trace.event("admission_error");
                    }
                    return Err(error);
                }
            };
            let signal = self
                .signal
                .get_or_insert_with(|| Arc::new(BatchSignal::new()));
            request.subscribe(signal);
            drop(group.drain(..count));
            skipped = 0;
            self.reads.push_back((slots, request));
            if group.is_empty() {
                self.groups.pop_front();
            }
        }
        Ok(())
    }

    #[allow(
        clippy::expect_used,
        reason = "an admitted batch owns its signal, and the ready loop picked an existing request"
    )]
    pub(super) fn finish_one(&mut self) -> Result<(), LocalDbError> {
        let (state, timeout) = {
            let request = &self
                .reads
                .front()
                .ok_or_else(|| LocalDbError::new(BACKPRESSURE))?
                .1;
            (&request.credit.state, request.timeout)
        };
        let signal = self.signal.as_ref().expect("admitted batch has a signal");
        // Check only readiness atomics while holding this batch's predicate
        // mutex; producers take the same mutex before notifying.
        let ready = loop {
            let mut wait_guard = signal.wait_lock.lock();
            if let Some(index) = self.reads.iter().position(|(_, request)| {
                request
                    .parts
                    .iter()
                    .all(|(flight, _)| state.flight_ready[flight.slot].is_ready())
            }) {
                break index;
            }
            if state.failed.load(Ordering::Acquire) || state.stopped.load(Ordering::Acquire) {
                return Err(LocalDbError::new("read authority is poisoned or closed"));
            }
            // Time queued for a lane or device credit is load, not a failed read.
            let now = Instant::now();
            let io_deadline = self
                .reads
                .iter()
                .filter_map(|(_, request)| request.io_deadline())
                .min();
            if io_deadline.is_some_and(|deadline| now >= deadline) {
                drop(wait_guard);
                state.poison();
                return Err(LocalDbError::new(
                    "remote read deadline expired or worker stopped",
                ));
            }
            // Also recheck health and deadlines set by later submissions.
            let wake = io_deadline.map_or(now + timeout, |deadline| deadline.min(now + timeout));
            #[cfg(test)]
            signal.waiters.fetch_add(1, Ordering::Release);
            signal.changed.wait_until(&mut wait_guard, wake);
            #[cfg(test)]
            signal.waiters.fetch_sub(1, Ordering::Release);
        };
        let (slots, request) = self.reads.remove(ready).expect("ready request exists");
        if self.reads.is_empty() {
            // The removed request still owns its subscription and credit until
            // finish returns; no idle batch retains an uncharged signal.
            self.signal = None;
        }
        let values = request.finish()?;
        if slots.len() != values.len() {
            return Err(LocalDbError::new("remote read count mismatch"));
        }
        for (slot, value) in slots.into_iter().zip(values) {
            self.values[slot] = Some(value);
        }
        Ok(())
    }
}

// A finite opt-in sample of real actor requests. No connection-control RPCs or
// extra locks are introduced on either the read owner or metrics scrape path.
macro_rules! read_diagnostics {
    ($($name:ident),+ $(,)?) => {
        #[derive(Default)]
        struct ReadDiagnostics {
            limit: AtomicU64,
            remaining: AtomicU64,
            $($name: AtomicU64,)+
        }
        impl ReadDiagnostics {
            fn visit(&self, set: &mut impl FnMut(&'static str, u64)) {
                set("read_profile_limit", self.limit.load(Ordering::Relaxed));
                set("read_profile_enabled", u64::from(self.limit.load(Ordering::Relaxed) != 0));
                $(set(concat!("read_profile_", stringify!($name)),
                    self.$name.load(Ordering::Relaxed));)+
            }
        }
    };
}
read_diagnostics!(
    selected,
    completed,
    errors,
    failed_owner_profile_not_applicable,
    inline_completed,
    inline_ns,
    actor_completed,
    nvme_completed,
    nvme_flight_ns,
    nvme_flight_count,
    nvme_flight_max_ns,
    // Why admission returned backpressure; credit shortage is real load, the
    // others are structural.
    admission_retry_credit,
    admission_retry_flight,
    admission_retry_lane,
    admission_retry_paused,
    // Live lane inbox pressure: inbox-full events, deepest inbox (an upper
    // bound on occupancy; see `LiveLane` in `drive_live_reads`), deepest
    // actor-side overflow queue, and time requests spent in that queue.
    live_lane_full,
    live_lane_depth_max,
    live_lane_pending_max,
    live_pending_wait_ns,
    live_inline,
    live_inline_fallback_drain,
    live_inline_fallback_busy,
    live_inline_fallback_grant,
    live_inline_fallback_credit,
    live_inline_fallback_paused,
    live_inline_fallback_coalesced,
);
impl ReadDiagnostics {
    fn inline_complete(&self, elapsed_ns: u64) {
        self.completed.fetch_add(1, Ordering::Relaxed);
        self.inline_completed.fetch_add(1, Ordering::Relaxed);
        self.inline_ns.fetch_add(elapsed_ns, Ordering::Relaxed);
    }
    fn select(&self) -> bool {
        // Once exhausted (and by default), no RMW or timestamp is performed.
        if self.remaining.load(Ordering::Relaxed) == 0 {
            return false;
        }
        let selected = self
            .remaining
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |left| {
                left.checked_sub(1)
            })
            .is_ok();
        if selected {
            self.selected.fetch_add(1, Ordering::Relaxed);
        }
        selected
    }
    fn nvme_complete(&self, elapsed_ns: Option<u64>, failed: bool) {
        self.nvme_completed.fetch_add(1, Ordering::Relaxed);
        if let Some(ns) = elapsed_ns {
            self.nvme_flight_ns.fetch_add(ns, Ordering::Relaxed);
            self.nvme_flight_count.fetch_add(1, Ordering::Relaxed);
            self.nvme_flight_max_ns.fetch_max(ns, Ordering::Relaxed);
        }
        self.completed.fetch_add(1, Ordering::Relaxed);
        self.actor_completed.fetch_add(1, Ordering::Relaxed);
        if failed {
            self.errors.fetch_add(1, Ordering::Relaxed);
            self.failed_owner_profile_not_applicable
                .fetch_add(1, Ordering::Relaxed);
        }
    }
}
/// Lock-free local snapshot handle; keeps no connection or database owner alive.
#[derive(Clone)]
pub struct ReadProfileMonitor {
    state: Arc<State>,
}
impl ReadProfileMonitor {
    pub(super) fn select_resolution(
        &self,
        bulk: bool,
        members: usize,
    ) -> Option<ResolutionSample<'_>> {
        self.state.coordinator_diagnostics.select(bulk, members)
    }
    /// Cumulative independent first-N API-resolution samples. Drain callers before
    /// taking a boundary snapshot; stage counts determine availability.
    pub fn coordinator_resolution_snapshot(&self) -> CoordinatorResolutionSnapshot {
        self.state.coordinator_diagnostics.snapshot()
    }
    pub fn visit(&self, set: &mut impl FnMut(&'static str, u64)) {
        self.state.diagnostics.visit(set);
        set(
            "read_singleflight_coalesced_members",
            self.state.coalesced_members.load(Ordering::Relaxed),
        );
        set(
            "read_singleflight_metadata_bytes",
            self.state.flight_metadata_bytes as u64,
        );
    }
    /// Sequential, lock-free snapshots of configured partitions; not a global cut.
    pub fn visit_partitions(&self, set: &mut impl FnMut(u32, &'static str, u64)) {
        for (partition, diagnostics) in self.state.owner_diagnostics.iter() {
            diagnostics.visit(&mut |name, value| {
                if name.starts_with("read_profile_live_")
                    || matches!(
                        name,
                        "read_profile_completed"
                            | "read_profile_inline_completed"
                            | "read_profile_actor_completed"
                            | "read_profile_errors"
                            | "read_profile_failed_owner_profile_not_applicable"
                    )
                {
                    set(*partition, name, value);
                }
            });
        }
    }
}
struct BatchSignal {
    wait_lock: Mutex<()>,
    changed: parking_lot::Condvar,
    #[cfg(test)]
    waiters: AtomicUsize,
    #[cfg(test)]
    notifications: AtomicUsize,
}
impl BatchSignal {
    fn new() -> Self {
        Self {
            wait_lock: Mutex::new(()),
            changed: parking_lot::Condvar::new(),
            #[cfg(test)]
            waiters: AtomicUsize::new(0),
            #[cfg(test)]
            notifications: AtomicUsize::new(0),
        }
    }
    fn notify(&self) {
        let _guard = self.wait_lock.lock();
        #[cfg(test)]
        self.notifications.fetch_add(1, Ordering::Relaxed);
        self.changed.notify_all();
    }
}

struct FlightSignal {
    ready: AtomicBool,
    wait_lock: Mutex<SmallVec<[Weak<BatchSignal>; 1]>>,
    changed: parking_lot::Condvar,
    #[cfg(test)]
    waiters: AtomicUsize,
    #[cfg(test)]
    retiring: AtomicUsize,
}
impl FlightSignal {
    fn new() -> Self {
        Self {
            ready: AtomicBool::new(true),
            wait_lock: Mutex::new(SmallVec::new()),
            changed: parking_lot::Condvar::new(),
            #[cfg(test)]
            waiters: AtomicUsize::new(0),
            #[cfg(test)]
            retiring: AtomicUsize::new(0),
        }
    }
    fn begin(&self) {
        self.ready.store(false, Ordering::Release);
    }
    fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }
    fn retire(&self) {
        // Publish retirement only while holding the subscription lock. A
        // reused slot may begin immediately after ready becomes visible, but
        // its subscriber cannot enter this generation's drain.
        #[cfg(test)]
        self.retiring.fetch_add(1, Ordering::Release);
        let mut subscribers = self.wait_lock.lock();
        #[cfg(test)]
        self.retiring.fetch_sub(1, Ordering::Release);
        self.ready.store(true, Ordering::Release);
        self.changed.notify_all();
        for subscriber in std::mem::take(&mut *subscribers) {
            if let Some(signal) = subscriber.upgrade() {
                signal.notify();
            }
        }
    }
    fn wait_until(&self, deadline: Instant) -> bool {
        let mut wait_lock = self.wait_lock.lock();
        while !self.is_ready() {
            if Instant::now() >= deadline {
                return false;
            }
            #[cfg(test)]
            self.waiters.fetch_add(1, Ordering::Release);
            let _ = self.changed.wait_until(&mut wait_lock, deadline);
            #[cfg(test)]
            self.waiters.fetch_sub(1, Ordering::Release);
        }
        true
    }
    fn subscribe(&self, signal: &Arc<BatchSignal>) -> bool {
        let mut subscribers = self.wait_lock.lock();
        if self.is_ready() {
            return false;
        }
        subscribers.push(Arc::downgrade(signal));
        true
    }
    fn unsubscribe(&self, signal: &Arc<BatchSignal>) {
        let mut subscribers = self.wait_lock.lock();
        if let Some(index) = subscribers
            .iter()
            .position(|entry| entry.as_ptr() == Arc::as_ptr(signal))
        {
            subscribers.swap_remove(index);
            if subscribers.is_empty() {
                *subscribers = SmallVec::new();
            } else if subscribers.capacity() > 2 * subscribers.len() {
                subscribers.shrink_to_fit();
            }
        }
    }
    fn notify_batches(&self) {
        let subscribers = self.wait_lock.lock();
        for subscriber in subscribers.iter() {
            if let Some(signal) = subscriber.upgrade() {
                signal.notify();
            }
        }
    }
}

#[cfg(test)]
type ReactorPause = Mutex<Option<(mpsc::SyncSender<()>, mpsc::Receiver<()>)>>;

type UnreleasedLiveGrant = (Arc<Mutex<Client>>, u64, Vec<[u8; 16]>);

struct State {
    failed: AtomicBool,
    first_failure: Arc<std::sync::OnceLock<super::FirstFailure>>,
    live_inline_reads: bool,
    pin_read_lanes: bool,
    /// Drain commands accepted by live workers and not yet completed.
    live_drains: AtomicUsize,
    live_unreleased: Mutex<Vec<UnreleasedLiveGrant>>,
    stopped: AtomicBool,
    retained: Mutex<(usize, usize)>,
    admission_paused: AtomicBool,
    admission_generation: AtomicU64,
    admission_changed: parking_lot::Condvar,
    #[cfg(test)]
    wake_pending: Box<[AtomicBool]>,
    // Fixed flight signals, weak slots and exact-reference index are reserved from pending bytes.
    flight_ready: Box<[FlightSignal]>,
    live_read_submissions: AtomicU64,
    diagnostics: ReadDiagnostics,
    trace: Arc<ReadTrace>,
    coordinator_diagnostics: CoordinatorResolutionDiagnostics,
    owner_diagnostics: Box<[(u32, ReadDiagnostics)]>,
    flights: Mutex<FlightRegistry>,
    coalesced_members: AtomicU64,
    flight_metadata_bytes: usize,
    #[cfg(test)]
    inline_mutex_fallbacks: AtomicUsize,
    #[cfg(test)]
    inline_result_copies: AtomicUsize,
    #[cfg(test)]
    inline_retirement_pause: ReactorPause,
    #[cfg(test)]
    inline_pause: Mutex<Option<(mpsc::SyncSender<()>, mpsc::Receiver<()>)>>,
    #[cfg(test)]
    completion_pause: Mutex<Option<(mpsc::SyncSender<()>, mpsc::Receiver<()>)>>,
    #[cfg(test)]
    reactor_pause: Box<[ReactorPause]>,
}

impl State {
    fn poison(&self) {
        self.failed.store(true, Ordering::Release);
        self.notify_batches();
        self.notify_admission();
    }
    fn notify_batches(&self) {
        for flight in &self.flight_ready {
            flight.notify_batches();
        }
    }
    fn notify_admission(&self) {
        let _retained = self.retained.lock();
        self.admission_generation.fetch_add(1, Ordering::Release);
        self.admission_changed.notify_all();
    }
}
struct PoisonOnPanic<'a>(&'a State);
impl Drop for PoisonOnPanic<'_> {
    fn drop(&mut self) {
        if thread::panicking() {
            self.0.poison();
        }
    }
}
struct Credit {
    state: Arc<State>,
    bytes: usize,
    requests: usize,
}
impl Drop for Credit {
    fn drop(&mut self) {
        let mut retained = self.state.retained.lock();
        retained.0 -= self.requests;
        retained.1 -= self.bytes;
        self.state
            .admission_generation
            .fetch_add(1, Ordering::Release);
        self.state.admission_changed.notify_all();
    }
}
pub(super) struct AdmissionPause(Arc<State>);
impl Drop for AdmissionPause {
    fn drop(&mut self) {
        let _retained = self.0.retained.lock();
        if !self.0.failed.load(Ordering::Acquire) && !self.0.stopped.load(Ordering::Acquire) {
            self.0.admission_paused.store(false, Ordering::Release);
            self.0.admission_generation.fetch_add(1, Ordering::Release);
            self.0.admission_changed.notify_all();
        }
    }
}

struct LaneScheduling {
    inline_reserved: bool,
}
struct InlineLaneReservation(Arc<Mutex<LaneScheduling>>);
impl Drop for InlineLaneReservation {
    fn drop(&mut self) {
        self.0.lock().inline_reserved = false;
    }
}

struct LaneCredit {
    outstanding: Arc<AtomicUsize>,
}
impl Drop for LaneCredit {
    fn drop(&mut self) {
        self.outstanding.fetch_sub(1, Ordering::Release);
    }
}
struct Request {
    lane: usize,
    references: Vec<ValueRecordReference>,
    /// Bound of the transport I/O, counted from its submission.
    timeout: Duration,
    /// Deadline of this request's outstanding transport I/O; `None` while
    /// it is queued or parked for device credit. Mirrored in its flight.
    io_deadline: Option<Instant>,
    submitted: Option<Instant>,
    sampled: bool,
    trace: Option<ReadTraceChild>,
    flight: Arc<ReadFlight>,
    _credit: Arc<Credit>,
    _lane_credit: LaneCredit,
}
enum ReadCommand {
    Request(Request),
    /// Test barrier: the actor clears `wake_pending[node]` when it takes this.
    #[cfg(test)]
    Wake,
    DrainLive(mpsc::SyncSender<Result<(), String>>),
}

enum LiveLaneCommand {
    Request(Request),
    Drain(mpsc::SyncSender<Result<(), String>>),
}

impl Request {
    /// Publish the deadline of outstanding device I/O, or `None` when parked.
    fn set_io_deadline(&mut self, deadline: Option<Instant>) {
        if self.io_deadline != deadline {
            self.io_deadline = deadline;
            *self.flight.io_deadline.lock() = deadline;
        }
    }

    fn complete_nvme(self, result: ReadCompletion) {
        if self.sampled {
            let duration = self.submitted.map(elapsed);
            self._credit
                .state
                .diagnostics
                .nvme_complete(duration, result.is_err());
            self._credit.state.owner_diagnostics[self.flight.node]
                .1
                .nvme_complete(duration, result.is_err());
        }
        kv_capture::routes(&self.flight.references, true, self.flight.slot as u64);
        if let Some(trace) = &self.trace {
            trace.event(if result.is_err() {
                "flight_error"
            } else {
                "flight_completed"
            });
        }
        if let Err(error) = &result {
            let partition = self
                .flight
                .references
                .first()
                .map(|reference| reference.batch_address.partition);
            super::record_first_failure(
                &self._credit.state.first_failure,
                partition,
                None,
                "read_completion",
                error,
            );
            self._credit.state.poison();
        }
        // Transport has relinquished this request's application buffers. The
        // endpoint separately owns any fixed registered slots still in DMA.
        let state = self._credit.state.clone();
        let flight_slot = self.flight.slot;
        let credit = self._credit.clone();
        *self.flight.completion.result.lock() = Some(result);
        #[cfg(test)]
        if let Some((entered, resume)) = state.completion_pause.lock().take() {
            entered.send(()).unwrap();
            resume.recv().unwrap();
        }
        // Keep pending bytes charged until this notifier no longer owns the
        // completed payload; live flights keep their own charge for consumers.
        drop(self);
        drop(credit);
        state.flight_ready[flight_slot].retire();
        state.notify_admission();
    }
}

/// One authenticated storage operation; exact reference matching happens before
/// admission, under the finite registry lock. Results/credits outlive transport
/// until every caller has consumed or dropped its part.
/// Scan and point callers can join: only the initiating RPC chooses its cache
/// class. Joining a scan never creates another fill or changes ScanNoFill work.
struct ReadFlight {
    node: usize,
    references: Vec<ValueRecordReference>,
    slot: usize,
    completion: FlightCompletion,
    /// Deadline of the outstanding transport I/O, set by the transport at
    /// submission; `None` before submission. Waiting callers time out only
    /// against it, never against time spent queued.
    io_deadline: Mutex<Option<Instant>>,
    _pin: Option<IndexReadPin>,
    // Keep retained-result credit while any caller can consume the flight.
    _credit: Arc<Credit>,
}
impl ReadFlight {
    fn io_deadline(&self) -> Option<Instant> {
        *self.io_deadline.lock()
    }
}

struct FlightCompletion {
    result: Mutex<Option<ReadCompletion>>,
}
impl FlightCompletion {
    fn new() -> Self {
        Self {
            result: Mutex::new(None),
        }
    }
}

fn copied_read_error(error: &LocalDbError) -> LocalDbError {
    let mut copy = LocalDbError::new(error.to_string());
    copy.uncertain_version = error.uncertain_version;
    copy
}

#[derive(Clone)]
struct Lane {
    index: usize,
    client: Arc<Mutex<Client>>,
    outstanding: Arc<AtomicUsize>,
    scheduling: Arc<Mutex<LaneScheduling>>,
    driver: Arc<Mutex<ReactorLane>>,
}
// Live processing takes `driver` before `client`. Callers try both locks;
// the live endpoint actor uses neither, so one inline I/O cannot stall it.
struct ReactorLane {
    /// Endpoint whose reactor drives this lane; tests pause that reactor.
    #[cfg(test)]
    node: usize,
    live_cache: Option<super::super::tcp::live_read::LiveReadCache>,
}
struct ReadLanePlacement {
    device: std::io::Result<PathBuf>,
    counters: Option<Arc<super::super::nvmeof::live_metrics::LiveNvmeCounters>>,
}
/// Affinity is a best-effort placement hint, never part of read correctness.
fn pin_live_read_lane(placement: Option<ReadLanePlacement>, lane: usize) {
    let Some(ReadLanePlacement { device, counters }) = placement else {
        return;
    };
    let result = device
        .and_then(|device| super::super::nvmeof::live_connect::queue_cpus(&device))
        .and_then(|queues| {
            super::super::nvmeof::live_connect::pin_current_thread(queues[lane % queues.len()][0])
        });
    if result.is_err()
        && let Some(counters) = counters
    {
        counters.record_read_lane_pin_failure();
    }
}

#[cfg(test)]
#[test]
fn live_read_lane_pin_failure_is_counted_without_failing_the_read() {
    let temp = tempdir::TempDir::new("read-lane-pin-failure").unwrap();
    let metrics = super::super::nvmeof::live_metrics::LiveNvmeMetrics::new();
    let counters = metrics.register().unwrap();
    pin_live_read_lane(
        Some(ReadLanePlacement {
            device: Ok(temp.path().join("missing-device")),
            counters: Some(counters),
        }),
        0,
    );
    assert_eq!(metrics.snapshot().counts.read_lane_pin_failures, 1);
}

// Live control and CQ waits must not occupy the shared multi-lane reactor.
// Global flight credits bound all channel entries, including these lane queues.
fn drive_live_reads(
    state: Arc<State>,
    receiver: mpsc::Receiver<ReadCommand>,
    lanes: Vec<Lane>,
    node: usize,
    base: usize,
    epoch: u64,
    max_requests: usize,
) {
    /// A lane's inbox as seen by the actor. `pending` holds requests the full
    /// inbox did not accept; it is bounded by `max_requests` because every
    /// request holds a global flight credit. The inbox channel itself holds
    /// at most `max_requests` entries.
    ///
    /// `depth` counts requests offered and not yet taken in by the worker.
    /// It is a diagnostic upper bound on inbox occupancy, not the occupancy.
    /// The actor counts before `try_send` and the worker uncounts after its
    /// receive, so a recorded depth may include the request the worker has
    /// received but not yet uncounted, and the request it receives between
    /// the actor's count and its send. The recorded maximum can therefore
    /// exceed `max_requests` by up to two while the worker is receiving.
    /// An exiting worker sets `live_lane::LANE_CLOSED` in `depth`; from then
    /// on the actor fails each request itself instead of sending it.
    struct LiveLane {
        sender: mpsc::SyncSender<LiveLaneCommand>,
        depth: Arc<AtomicUsize>,
        waker: live_lane::LaneWaker,
        pending: VecDeque<(Instant, Request)>,
    }
    /// Hand `request` to the lane without blocking; false if the inbox is full.
    fn offer(lane: &mut LiveLane, request: Request, state: &State, node: usize) -> Option<Request> {
        // Count before sending so the worker's decrement cannot underflow.
        // Relaxed: the count and the worker's close are read-modify-writes of
        // one atomic, so their modification order alone decides who completes
        // a racing request (see `live_lane::LANE_CLOSED`); the channel
        // publishes the request itself.
        let previous = lane.depth.fetch_add(1, Ordering::Relaxed);
        if previous & live_lane::LANE_CLOSED != 0 {
            // The worker is exiting and takes nothing more: fail it here.
            lane.depth.fetch_sub(1, Ordering::Relaxed);
            request.complete_nvme(Err(LocalDbError::new("read authority is poisoned")));
            return None;
        }
        let depth = previous + 1;
        match lane.sender.try_send(LiveLaneCommand::Request(request)) {
            Ok(()) => {
                // Interrupt a worker sleeping on its CQ so it admits this at once.
                if let Some(waker) = lane.waker.get()
                    && let Err(error) = waker.wake()
                {
                    // A lost wake could strand this request behind a sleeping
                    // worker: fail closed instead of waiting for its timeout.
                    super::record_first_failure(
                        &state.first_failure,
                        None,
                        None,
                        "live_lane_wake",
                        &error,
                    );
                    state.poison();
                }
                state
                    .diagnostics
                    .live_lane_depth_max
                    .fetch_max(depth as u64, Ordering::Relaxed);
                state.owner_diagnostics[node]
                    .1
                    .live_lane_depth_max
                    .fetch_max(depth as u64, Ordering::Relaxed);
                None
            }
            Err(mpsc::TrySendError::Full(LiveLaneCommand::Request(request))) => {
                lane.depth.fetch_sub(1, Ordering::Relaxed);
                state
                    .diagnostics
                    .live_lane_full
                    .fetch_add(1, Ordering::Relaxed);
                state.owner_diagnostics[node]
                    .1
                    .live_lane_full
                    .fetch_add(1, Ordering::Relaxed);
                Some(request)
            }
            Err(mpsc::TrySendError::Disconnected(LiveLaneCommand::Request(request))) => {
                lane.depth.fetch_sub(1, Ordering::Relaxed);
                request.complete_nvme(Err(LocalDbError::new("live read worker stopped")));
                None
            }
            Err(_) => None,
        }
    }
    /// Move pending requests into their inboxes, oldest first, until one is full.
    fn flush(lanes: &mut [LiveLane], state: &State, node: usize) {
        for lane in lanes {
            while let Some((queued, request)) = lane.pending.pop_front() {
                // Time queued here is not I/O time: the lane worker starts
                // the I/O deadline when it submits the request's I/O.
                match offer(lane, request, state, node) {
                    None => {
                        let waited = elapsed(queued);
                        state
                            .diagnostics
                            .live_pending_wait_ns
                            .fetch_add(waited, Ordering::Relaxed);
                        state.owner_diagnostics[node]
                            .1
                            .live_pending_wait_ns
                            .fetch_add(waited, Ordering::Relaxed);
                    }
                    Some(request) => {
                        lane.pending.push_front((queued, request));
                        break;
                    }
                }
            }
        }
    }
    let _poison = PoisonOnPanic(&state);
    let mut inboxes = Vec::with_capacity(lanes.len());
    let mut workers = Vec::with_capacity(lanes.len());
    for lane in lanes {
        let (sender, lane_receiver) = mpsc::sync_channel::<LiveLaneCommand>(max_requests);
        let depth = Arc::new(AtomicUsize::new(0));
        let waker = live_lane::LaneWaker::default();
        let worker_state = state.clone();
        let worker_depth = depth.clone();
        let worker_waker = waker.clone();
        let worker = thread::Builder::new()
            .name(format!("wal-live-read-{}", lane.index))
            .spawn(move || {
                live_lane::run(
                    worker_state,
                    lane,
                    lane_receiver,
                    worker_depth,
                    worker_waker,
                    epoch,
                    max_requests,
                );
            });
        match worker {
            Ok(worker) => {
                inboxes.push(LiveLane {
                    sender,
                    depth,
                    waker,
                    pending: VecDeque::new(),
                });
                workers.push(worker);
            }
            Err(_) => {
                state.poison();
                drop(sender);
                break;
            }
        }
    }
    let mut disconnected = false;
    loop {
        flush(&mut inboxes, &state, node);
        let queued = inboxes.iter().map(|lane| lane.pending.len()).sum::<usize>();
        if queued != 0 {
            let queued = queued as u64;
            state
                .diagnostics
                .live_lane_pending_max
                .fetch_max(queued, Ordering::Relaxed);
            state.owner_diagnostics[node]
                .1
                .live_lane_pending_max
                .fetch_max(queued, Ordering::Relaxed);
        }
        let command = if disconnected {
            if queued == 0 {
                break;
            }
            // Closing: only the pending requests remain; wait for lane room.
            thread::sleep(Duration::from_micros(100));
            continue;
        } else if queued == 0 {
            // Nothing to retry: block until the next command.
            match receiver.recv() {
                Ok(command) => command,
                Err(_) => {
                    disconnected = true;
                    continue;
                }
            }
        } else {
            match receiver.recv_timeout(Duration::from_micros(100)) {
                Ok(command) => command,
                Err(mpsc::RecvTimeoutError::Timeout) => continue,
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    disconnected = true;
                    continue;
                }
            }
        };
        match command {
            ReadCommand::Request(request) => {
                if let Some(trace) = &request.trace {
                    trace.event("actor_dequeued");
                }
                let index = request.lane.checked_sub(base);
                let Some(lane) = index.and_then(|index| inboxes.get_mut(index)) else {
                    request.complete_nvme(Err(LocalDbError::new("live read worker unavailable")));
                    continue;
                };
                // Keep lane FIFO: never pass requests already waiting.
                if lane.pending.is_empty() {
                    if let Some(request) = offer(lane, request, &state, node) {
                        lane.pending.push_back((Instant::now(), request));
                    }
                } else {
                    lane.pending.push_back((Instant::now(), request));
                }
            }
            #[cfg(test)]
            ReadCommand::Wake => state.wake_pending[node].store(false, Ordering::Release),
            ReadCommand::DrainLive(reply) => {
                for lane in &inboxes {
                    // Release: publish the drain before the worker can receive it.
                    state.live_drains.fetch_add(1, Ordering::Release);
                    if lane
                        .sender
                        .try_send(LiveLaneCommand::Drain(reply.clone()))
                        .is_err()
                    {
                        // Release: this lane did not accept the published drain.
                        state.live_drains.fetch_sub(1, Ordering::Release);
                        let _ = reply.send(Err("live read lane drain admission failed".into()));
                    }
                }
            }
        }
    }
    drop(inboxes);
    for worker in workers {
        if worker.join().is_err() {
            state.poison();
        }
    }
}

pub(super) struct ReadService {
    state: Arc<State>,
    lanes: RwLock<Vec<Vec<Lane>>>,
    sender: Mutex<Vec<mpsc::SyncSender<ReadCommand>>>,
    thread: Mutex<Vec<JoinHandle<()>>>,
    next: Vec<AtomicUsize>,
    max_requests: usize,
    max_bytes: usize,
    timeout: Duration,
    #[cfg(test)]
    epoch: u64,
    /// Test-only override of `max_bytes`; `usize::MAX` means none. Relaxed:
    /// a knob read on admission, set by the test thread between operations.
    #[cfg(test)]
    test_max_bytes: AtomicUsize,
    /// Test-only override of `timeout` in nanoseconds; `u64::MAX` means none.
    #[cfg(test)]
    test_timeout_ns: AtomicU64,
    /// Test-only override of `live_inline_reads`: 0 none, 1 off, 2 on.
    #[cfg(test)]
    test_inline_reads: std::sync::atomic::AtomicU8,
}
impl ReadService {
    #[cfg(not(test))]
    fn max_bytes(&self) -> usize {
        self.max_bytes
    }
    #[cfg(test)]
    fn max_bytes(&self) -> usize {
        match self.test_max_bytes.load(Ordering::Relaxed) {
            usize::MAX => self.max_bytes,
            bytes => bytes,
        }
    }
    #[cfg(not(test))]
    fn timeout(&self) -> Duration {
        self.timeout
    }
    #[cfg(test)]
    fn timeout(&self) -> Duration {
        match self.test_timeout_ns.load(Ordering::Relaxed) {
            u64::MAX => self.timeout,
            nanos => Duration::from_nanos(nanos),
        }
    }
    #[cfg(not(test))]
    fn live_inline_reads(&self) -> bool {
        self.state.live_inline_reads
    }
    #[cfg(test)]
    fn live_inline_reads(&self) -> bool {
        match self.test_inline_reads.load(Ordering::Relaxed) {
            0 => self.state.live_inline_reads,
            value => value == 2,
        }
    }
    /// Switch the immutable live inline-read policy for later reads, in place
    /// on the same lanes and dataset (tests only).
    #[cfg(test)]
    pub(super) fn set_inline_reads_for_test(&self, inline_reads: bool) {
        self.test_inline_reads
            .store(if inline_reads { 2 } else { 1 }, Ordering::Relaxed);
    }
    /// Replace the immutable read limits for later admissions (tests only).
    #[cfg(test)]
    pub(super) fn set_limits_for_test(&self, max_bytes: Option<usize>, timeout: Option<Duration>) {
        if let Some(bytes) = max_bytes {
            assert_ne!(bytes, usize::MAX);
            self.test_max_bytes.store(bytes, Ordering::Relaxed);
        }
        if let Some(timeout) = timeout {
            let nanos = u64::try_from(timeout.as_nanos()).unwrap();
            assert_ne!(nanos, u64::MAX);
            self.test_timeout_ns.store(nanos, Ordering::Relaxed);
        }
    }
    pub(super) fn resolution_monitor(&self) -> Option<ReadProfileMonitor> {
        self.state
            .coordinator_diagnostics
            .enabled()
            .then(|| ReadProfileMonitor {
                state: self.state.clone(),
            })
    }
}
struct BatchSubscription {
    signal: Arc<BatchSignal>,
    slots: SmallVec<[usize; 1]>,
}
pub(super) struct ReadRequest {
    parts: SmallVec<[(Arc<ReadFlight>, usize); 1]>,
    credit: Arc<Credit>,
    admitted: Instant,
    /// Longest wait between health and deadline rechecks.
    timeout: Duration,
    trace: Option<ReadTraceChild>,
    subscription: Option<BatchSubscription>,
}
impl ReadRequest {
    /// The earliest deadline of this request's outstanding transport I/O;
    /// `None` while none of its incomplete flights is submitted.
    fn io_deadline(&self) -> Option<Instant> {
        self.parts
            .iter()
            .filter(|(flight, _)| !flight._credit.state.flight_ready[flight.slot].is_ready())
            .filter_map(|(flight, _)| flight.io_deadline())
            .min()
    }
    fn subscribe(&mut self, signal: &Arc<BatchSignal>) {
        let mut slots = SmallVec::new();
        for (index, (flight, _)) in self.parts.iter().enumerate() {
            if self.parts[..index]
                .iter()
                .any(|(previous, _)| previous.slot == flight.slot)
            {
                continue;
            }
            if self.credit.state.flight_ready[flight.slot].subscribe(signal) {
                slots.push(flight.slot);
            }
        }
        self.subscription = Some(BatchSubscription {
            signal: signal.clone(),
            slots,
        });
    }
    fn failure(&self, message: &str) -> LocalDbError {
        self.credit.state.poison();
        LocalDbError::new(message)
    }
    pub(super) fn try_result(&self) -> Result<Option<Vec<Mutation>>, LocalDbError> {
        // Do not allocate a result vector until all constituent operations have
        // completed. The original slot order (including duplicates) is retained.
        for (flight, _) in &self.parts {
            if !flight._credit.state.flight_ready[flight.slot].is_ready() {
                return Ok(None);
            }
            let result = flight.completion.result.lock();
            match result.as_ref() {
                None => return Ok(None),
                Some(Err(error)) => return Err(copied_read_error(error)),
                Some(Ok(_)) => {}
            }
        }
        let mut values = Vec::with_capacity(self.parts.len());
        for (flight, slot) in &self.parts {
            let result = flight.completion.result.lock();
            let Some(Ok(members)) = result.as_ref() else {
                unreachable!("a completed flight cannot change");
            };
            let Some(member) = members.get(*slot) else {
                return Err(self.failure("remote read count mismatch"));
            };
            values.push(member.clone());
        }
        // Acquire pairs with begin_poison even if every flight was already
        // complete when publication failed.
        if self.credit.state.failed.load(Ordering::Acquire) {
            return Err(LocalDbError::new("read authority is poisoned"));
        }
        Ok(Some(values))
    }

    pub(super) fn finish(self) -> ReadCompletion {
        for (flight, _) in &self.parts {
            let state = &flight._credit.state;
            // As in native, load never fails the read: only a submitted
            // flight's I/O deadline or a failed or stopped authority ends this
            // wait. It wakes at least once per timeout to recheck both.
            loop {
                let now = Instant::now();
                let wake = flight.io_deadline().map_or(now + self.timeout, |deadline| {
                    deadline.min(now + self.timeout)
                });
                if state.flight_ready[flight.slot].wait_until(wake) {
                    break;
                }
                if flight
                    .io_deadline()
                    .is_some_and(|deadline| Instant::now() >= deadline)
                {
                    return Err(self.failure("remote read deadline expired or worker stopped"));
                }
                if state.failed.load(Ordering::Acquire) || state.stopped.load(Ordering::Acquire) {
                    return Err(LocalDbError::new("read authority is poisoned or closed"));
                }
            }
        }
        let result = self
            .try_result()?
            .ok_or_else(|| self.failure("remote read did not complete"))?;
        if let Some(trace) = &self.trace {
            trace.event("request_result_delivered");
        }
        if self.credit.state.failed.load(Ordering::Acquire) {
            return Err(LocalDbError::new("read authority is poisoned"));
        }
        Ok(result)
    }
}
impl Drop for ReadRequest {
    fn drop(&mut self) {
        // All flight Arcs in parts remain live until this Drop returns, so
        // slots cannot be reused before their subscriptions are removed.
        if let Some(subscription) = self.subscription.take() {
            for slot in subscription.slots {
                self.credit.state.flight_ready[slot].unsubscribe(&subscription.signal);
            }
        }
    }
}
impl ReadService {
    /// Charge of one member; callers keep the result.
    fn plan_member(&self, reference: &ValueRecordReference) -> Result<MemberPlan, LocalDbError> {
        Ok(MemberPlan {
            charge: member_charge(reference)?,
        })
    }

    /// Backpressure error that records why admission was refused.
    fn backpressure(&self, cause: &AtomicU64) -> LocalDbError {
        cause.fetch_add(1, Ordering::Relaxed);
        LocalDbError::new(BACKPRESSURE)
    }

    /// Plan of one member kept across admission retries of a blocking caller;
    /// released credit never recomputes it.
    fn cached_plan<'a>(
        &self,
        cached: &'a mut Option<CachedPlan>,
        reference: &ValueRecordReference,
    ) -> Result<PlannedMembers<'a>, LocalDbError> {
        let plan = match cached.take() {
            Some(plan) => plan,
            None => CachedPlan {
                member: [self.plan_member(reference)?],
            },
        };
        let plan = cached.insert(plan);
        Ok(PlannedMembers {
            members: &plan.member,
        })
    }

    const FLIGHT_SLOT_BYTES: usize = std::mem::size_of::<Weak<ReadFlight>>()
        + std::mem::size_of::<ReadFlight>()
        + std::mem::size_of::<FlightCompletion>()
        + std::mem::size_of::<FlightSignal>()
        + 3 * std::mem::size_of::<usize>();
    // Leave room for a small real encoded member, not merely an empty reference.
    pub(super) const MIN_FLIGHT_BYTES: usize = std::mem::size_of::<FlightRegistry>()
        + Self::FLIGHT_SLOT_BYTES
        + 2 * FlightRegistry::ENTRY_BYTES
        + 512
        + MIN_MEMBER_BYTES
        + 3 * 256;

    #[allow(
        clippy::expect_used,
        reason = "the client list is sized to endpoints times read lanes before this loop"
    )]
    pub(super) fn from_clients(
        config: &RemoteDatabaseConfig,
        epoch: u64,
        clients: Vec<Client>,
        recovery_stats: &mut RecoveryStats,
        first_failure: Arc<std::sync::OnceLock<super::FirstFailure>>,
    ) -> Result<Self, LocalDbError> {
        let index_capacity = (config.max_pending_read_bytes
            - std::mem::size_of::<FlightRegistry>()
            - Self::FLIGHT_SLOT_BYTES
            - 512)
            .div_ceil(MIN_MEMBER_BYTES + FlightRegistry::ENTRY_BYTES)
            .min(config.max_pending_reads.saturating_mul(256))
            .max(1);
        let index_bytes =
            std::mem::size_of::<FlightRegistry>() + index_capacity * FlightRegistry::ENTRY_BYTES;
        let flight_slots = config.max_pending_reads.min(
            (config.max_pending_read_bytes - index_bytes - MIN_MEMBER_BYTES - 3 * 256)
                / (Self::FLIGHT_SLOT_BYTES + 512),
        );
        let flight_metadata_bytes = flight_slots * Self::FLIGHT_SLOT_BYTES + index_bytes;
        let state = Arc::new(State {
            failed: AtomicBool::new(false),
            first_failure,
            pin_read_lanes: config.pin_read_lanes,
            live_inline_reads: config.live_nvme.inline_reads,
            live_drains: AtomicUsize::new(0),
            live_unreleased: Mutex::new(Vec::new()),
            stopped: AtomicBool::new(false),
            retained: Mutex::new((0, 0)),
            admission_paused: AtomicBool::new(false),
            admission_generation: AtomicU64::new(0),
            admission_changed: parking_lot::Condvar::new(),
            #[cfg(test)]
            wake_pending: (0..config.endpoints.len())
                .map(|_| AtomicBool::new(false))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            flight_ready: (0..flight_slots)
                .map(|_| FlightSignal::new())
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            live_read_submissions: AtomicU64::new(0),
            diagnostics: ReadDiagnostics::default(),
            trace: Arc::new(ReadTrace::default()),
            coordinator_diagnostics: CoordinatorResolutionDiagnostics::default(),
            owner_diagnostics: config
                .endpoints
                .iter()
                .map(|endpoint| (endpoint.partition, ReadDiagnostics::default()))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            flights: Mutex::new(FlightRegistry::new(flight_slots, index_capacity)),
            coalesced_members: AtomicU64::new(0),
            flight_metadata_bytes,
            #[cfg(test)]
            inline_mutex_fallbacks: AtomicUsize::new(0),
            #[cfg(test)]
            inline_result_copies: AtomicUsize::new(0),
            #[cfg(test)]
            inline_retirement_pause: Mutex::new(None),
            #[cfg(test)]
            inline_pause: Mutex::new(None),
            #[cfg(test)]
            completion_pause: Mutex::new(None),
            #[cfg(test)]
            reactor_pause: (0..config.endpoints.len())
                .map(|_| Mutex::new(None))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        });
        let mut service = Self {
            state,
            lanes: RwLock::new(Vec::new()),
            sender: Mutex::new(Vec::new()),
            thread: Mutex::new(Vec::new()),
            next: (0..config.endpoints.len())
                .map(|_| AtomicUsize::new(0))
                .collect(),
            max_requests: config.max_pending_reads,
            max_bytes: config
                .max_pending_read_bytes
                .saturating_sub(flight_metadata_bytes),
            timeout: config.timeout,
            #[cfg(test)]
            epoch,
            #[cfg(test)]
            test_max_bytes: AtomicUsize::new(usize::MAX),
            #[cfg(test)]
            test_timeout_ns: AtomicU64::new(u64::MAX),
            #[cfg(test)]
            test_inline_reads: std::sync::atomic::AtomicU8::new(0),
        };
        let mut clients = clients.into_iter();
        let read_lanes = config.effective_read_lanes();
        for node in 0..config.endpoints.len() {
            let base = node * read_lanes;
            let mut lanes = Vec::with_capacity(read_lanes);
            for _ in 0..read_lanes {
                let client = clients.next().expect("sized client list");
                recovery_stats.add_connection(client.connect_profile());
                let client = Arc::new(Mutex::new(client));
                let index = base + lanes.len();
                let scheduling = Arc::new(Mutex::new(LaneScheduling {
                    inline_reserved: false,
                }));
                let driver = Arc::new(Mutex::new(ReactorLane {
                    #[cfg(test)]
                    node,
                    live_cache: Some(super::super::tcp::live_read::LiveReadCache::default()),
                }));
                let lane = Lane {
                    index,
                    client,
                    outstanding: Arc::new(AtomicUsize::new(0)),
                    scheduling,
                    driver,
                };
                lanes.push(lane);
            }
            service.lanes.get_mut().push(lanes.clone());
            // At most endpoints * (flight_slots + 1) fixed channel slots.
            // Global flight credits bound aggregate request occupancy; each
            // owner can still use the full global allowance.
            let (sender, receiver) = mpsc::sync_channel(flight_slots + 1);
            let state = service.state.clone();
            let thread = thread::Builder::new()
                .name(format!("wal-read-reactor-{node}"))
                .spawn(move || {
                    drive_live_reads(state, receiver, lanes, node, base, epoch, flight_slots);
                });
            match thread {
                Ok(thread) => {
                    service.thread.get_mut().push(thread);
                    service.sender.get_mut().push(sender);
                }
                Err(error) => return Err(LocalDbError::component(error)),
            }
        }
        Ok(service)
    }
    fn pause_idle_nvmeof(&self, allow_failed: bool) -> Result<AdmissionPause, LocalDbError> {
        let retained = self.state.retained.lock();
        if (!allow_failed && self.failed()) || self.state.stopped.load(Ordering::Acquire) {
            return Err(LocalDbError::new("read authority is poisoned or closed"));
        }
        if retained.0 != 0
            || retained.1 != 0
            || self.state.admission_paused.swap(true, Ordering::AcqRel)
        {
            return Err(LocalDbError::new(
                "finish or drop outstanding reads before changing NVMe-oF",
            ));
        }
        let pause = AdmissionPause(self.state.clone());
        drop(retained);
        Ok(pause)
    }

    /// Pause new admissions and retire every lane's cached live file grants.
    /// Outstanding callers receive an explicit retryable error instead of
    /// relinquishing their read/cut pins on their behalf.
    pub(super) fn pause_live_for_retirement(&self) -> Result<AdmissionPause, LocalDbError> {
        let pause = self.pause_idle_nvmeof(false)?;
        let lanes = self.lanes.read().iter().map(Vec::len).sum::<usize>();
        let senders = self.sender.lock().clone();
        if lanes == 0 || senders.is_empty() {
            return Err(LocalDbError::new(
                "live read lanes unavailable for retirement",
            ));
        }
        let (reply, responses) = mpsc::sync_channel(lanes);
        for sender in senders {
            match sender.try_send(ReadCommand::DrainLive(reply.clone())) {
                Ok(()) => {}
                Err(mpsc::TrySendError::Full(_)) => {
                    return Err(LocalDbError::new(
                        "live read grant drain is busy; retry retirement",
                    ));
                }
                Err(mpsc::TrySendError::Disconnected(_)) => {
                    self.state.poison();
                    return Err(LocalDbError::new(
                        "live read reactor stopped during grant drain",
                    ));
                }
            }
        }
        drop(reply);
        let deadline = Instant::now() + self.timeout();
        for _ in 0..lanes {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(LocalDbError::new(
                    "live read grant drain timed out; retry retirement",
                ));
            }
            match responses.recv_timeout(remaining) {
                Ok(result) => result.map_err(LocalDbError::new)?,
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    return Err(LocalDbError::new(
                        "live read grant drain timed out; retry retirement",
                    ));
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    self.state.poison();
                    return Err(LocalDbError::new("live read grant drain lost a lane"));
                }
            }
        }
        Ok(pause)
    }

    pub(super) fn ensure_idle(&self) -> Result<(), LocalDbError> {
        if self.state.retained.lock().0 != 0 {
            return Err(LocalDbError::new(
                "finish or drop outstanding reads before diagnostics",
            ));
        }
        Ok(())
    }
    pub(super) fn failed(&self) -> bool {
        self.state.failed.load(Ordering::Acquire)
    }
    #[cfg(test)]
    pub(super) fn lane_client(&self, node: usize, lane: usize) -> Arc<Mutex<Client>> {
        self.lanes.read()[node][lane].client.clone()
    }
    pub(super) fn profiles(
        &self,
        enabled: bool,
        reset: bool,
    ) -> Result<Vec<super::super::tcp::ConnectionProfile>, LocalDbError> {
        self.lanes
            .read()
            .iter()
            .map(|lanes| {
                let mut profile = super::super::tcp::ConnectionProfile::default();
                for lane in lanes {
                    profile.add(
                        lane.client
                            .lock()
                            .connection_profile(enabled, reset)
                            .map_err(LocalDbError::component)?,
                    );
                }
                Ok(profile)
            })
            .collect()
    }
    pub(super) fn stop(&self) {
        self.stop_actor();
        self.lanes.write().clear();
    }
    fn stop_actor(&self) {
        self.state.stopped.store(true, Ordering::Release);
        self.state.notify_batches();
        self.notify_admission();
        self.sender.lock().clear();
        // Serialize all shutdown callers through the join, so a second caller
        // returns only after the reactor's terminal completions.
        let mut thread_guard = self.thread.lock();
        for thread in thread_guard.drain(..) {
            if thread.join().is_err() {
                self.poison();
            }
        }
        // Join any caller-owned lane before the authority directory lock drops.
        for lane in self.lanes.read().iter().flatten() {
            drop(lane.client.lock());
        }
    }

    /// Stop value delivery without taking read-side notification locks.
    /// Failure finalization performs the existing wakeups after publication
    /// locks have been dropped.
    pub(super) fn begin_poison(&self) {
        self.state.failed.store(true, Ordering::Release);
    }

    pub(super) fn poison(&self) {
        self.state.poison();
    }

    fn notify_admission(&self) {
        self.state.notify_admission();
    }

    pub(super) fn admission_generation(&self) -> u64 {
        self.state.admission_generation.load(Ordering::Acquire)
    }

    /// Wait until read credit changes after `observed`. `None` waits without
    /// a load deadline, as a native read never fails because of load: every
    /// held credit is released by its read's completion, failure or per-I/O
    /// timeout, and every release, poison, stop and unpause bumps the
    /// generation under `retained` before notifying, so no wakeup is lost.
    pub(super) fn wait_admission(
        &self,
        observed: u64,
        deadline: Option<Instant>,
    ) -> Result<(), LocalDbError> {
        let mut retained = self.state.retained.lock();
        loop {
            if self.failed() || self.state.stopped.load(Ordering::Acquire) {
                return Err(LocalDbError::new("read authority is poisoned or closed"));
            }
            if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                return Err(LocalDbError::new("remote read admission deadline expired"));
            }
            if self.admission_generation() != observed {
                return Ok(());
            }
            match deadline {
                Some(deadline) => {
                    let _ = self
                        .state
                        .admission_changed
                        .wait_until(&mut retained, deadline);
                }
                None => self.state.admission_changed.wait(&mut retained),
            }
        }
    }
    /// A blocking scalar caller can own an idle lane directly. Deferred reads
    /// retain the reactor path; no active lane or outstanding credit is stolen.
    pub(super) fn read_inline(
        &self,
        node: usize,
        epoch: u64,
        reference: &ValueRecordReference,
        extra_bytes: usize,
        pin: Option<IndexReadPin>,
    ) -> Result<Option<Vec<Mutation>>, LocalDbError> {
        self.read_inline_traced(node, epoch, reference, extra_bytes, pin, None)
    }

    fn read_inline_traced(
        &self,
        node: usize,
        epoch: u64,
        reference: &ValueRecordReference,
        extra_bytes: usize,
        pin: Option<IndexReadPin>,
        trace: Option<&ReadTraceChild>,
    ) -> Result<Option<Vec<Mutation>>, LocalDbError> {
        if self.failed() || self.state.stopped.load(Ordering::Acquire) {
            return Err(LocalDbError::new("read authority is poisoned or closed"));
        }
        if self.live_inline_reads() {
            return self.read_live_inline(node, epoch, reference, extra_bytes, pin, trace);
        }
        Ok(None)
    }
    pub(super) fn submit(
        &self,
        node: usize,
        references: Vec<ValueRecordReference>,
        extra_bytes: usize,
        profiled: bool,
        pin: Option<IndexReadPin>,
    ) -> Result<ReadRequest, LocalDbError> {
        self.submit_until(
            node,
            &references,
            extra_bytes,
            profiled,
            ReadContext { pin, trace: None },
        )
    }
    fn submit_until(
        &self,
        node: usize,
        references: &[ValueRecordReference],
        extra_bytes: usize,
        profiled: bool,
        context: ReadContext,
    ) -> Result<ReadRequest, LocalDbError> {
        self.submit_until_planned(node, references, extra_bytes, profiled, context, None)
    }
    // Admission takes its context and optional precomputed member plans
    // explicitly; each belongs to a distinct concern.
    #[allow(
        clippy::expect_used,
        reason = "a new request always reserves its flight slot and flight before planning"
    )]
    fn submit_until_planned(
        &self,
        node: usize,
        references: &[ValueRecordReference],
        extra_bytes: usize,
        profiled: bool,
        context: ReadContext,
        planned: Option<PlannedMembers<'_>>,
    ) -> Result<ReadRequest, LocalDbError> {
        let admitted = Instant::now();
        let timeout = self.timeout();
        let ReadContext { pin, trace } = context;
        if let Some(trace) = &trace {
            trace.event("admission_started");
        }
        if self.failed() || self.state.stopped.load(Ordering::Acquire) {
            return Err(LocalDbError::new("read authority is poisoned or closed"));
        }
        if references.is_empty() || references.len() > 256 {
            return Err(LocalDbError::new("read request count exceeds bound"));
        }
        // Each member's charge is computed once here, or by the batch that
        // owns the plans, and reused by the charge sum and every retry.
        let mut computed = SmallVec::<[MemberPlan; 32]>::new();
        let plans: &[MemberPlan] = if let Some(planned) = planned {
            planned.members
        } else {
            for reference in references {
                computed.push(self.plan_member(reference)?);
            }
            &computed
        };
        if plans.len() != references.len() {
            return Err(LocalDbError::new("read plan does not match request"));
        }
        let bytes = plans.iter().try_fold(extra_bytes + 512, |bytes, plan| {
            bytes
                .checked_add(plan.charge)
                .ok_or_else(|| LocalDbError::new("read budget overflow"))
        })?;
        if bytes > self.max_bytes() {
            return Err(LocalDbError::new(BYTE_BUDGET));
        }
        let mut flights = self.state.flights.lock();
        let mut missing = Vec::new();
        let mut parts: SmallVec<[(Option<Arc<ReadFlight>>, usize); 1]> =
            SmallVec::with_capacity(references.len());
        let mut missing_index = (references.len() > 1).then(HashMap::new);
        for reference in references {
            let existing = flights.find(node, reference);
            if let Some((flight, slot)) = existing {
                if let Some(trace) = &trace {
                    trace.event_io("coalesced_flight_slot", 0, flight.slot as u64);
                }
                parts.push((Some(flight), slot));
            } else {
                let slot = if let Some(index) = missing_index.as_mut() {
                    *index.entry(*reference).or_insert_with(|| {
                        missing.push(*reference);
                        missing.len() - 1
                    })
                } else {
                    missing.push(*reference);
                    0
                };
                parts.push((None, slot));
            }
        }
        if let Some(trace) = &trace {
            trace.classify_members(
                parts
                    .iter()
                    .enumerate()
                    .filter_map(|(position, (flight, _))| flight.as_ref().map(|_| position)),
            );
        }
        let coalesced_members = references.len() - missing.len();
        let new_request = usize::from(!missing.is_empty());
        let flight_slot = if new_request != 0 {
            Some(
                flights
                    .available_slot(&self.state.flight_ready)
                    .ok_or_else(|| {
                        self.backpressure(&self.state.diagnostics.admission_retry_flight)
                    })?,
            )
        } else {
            None
        };
        if !flights.reserve_entries(missing.len(), &self.state.flight_ready) {
            return Err(self.backpressure(&self.state.diagnostics.admission_retry_flight));
        }
        let (credit, lane) = {
            let mut retained = self.state.retained.lock();
            if self.failed() {
                return Err(LocalDbError::new("read authority is poisoned"));
            }
            if self.state.stopped.load(Ordering::Acquire) {
                return Err(LocalDbError::new("read authority is closed"));
            }
            if self.state.admission_paused.load(Ordering::Acquire) {
                return Err(self.backpressure(&self.state.diagnostics.admission_retry_paused));
            }
            if (new_request != 0 && retained.0 == self.max_requests)
                || bytes > self.max_bytes() - retained.1
            {
                // Real shortage: the request or byte credit is exhausted now,
                // checked under the same mutex that reserves it.
                return Err(self.backpressure(&self.state.diagnostics.admission_retry_credit));
            }
            let lane_guard = self.lanes.read();
            let lanes = lane_guard
                .get(node)
                .ok_or_else(|| LocalDbError::new("read reactor stopped"))?;
            // Prefer an idle lane. Busy grouped reservations accept joined
            // work but ask their caller to hand progress back to the actor.
            let next = self.next[node].fetch_add(1, Ordering::Relaxed);
            let lane = if new_request == 0 {
                None
            } else {
                Some(
                    [true, false]
                        .into_iter()
                        .find_map(|idle_only| {
                            (0..lanes.len()).find_map(|offset| {
                                let lane = &lanes[next.wrapping_add(offset) % lanes.len()];
                                let scheduling = lane.scheduling.lock();
                                if scheduling.inline_reserved
                                    || (idle_only && lane.outstanding.load(Ordering::Acquire) != 0)
                                {
                                    return None;
                                }
                                lane.outstanding.fetch_add(1, Ordering::AcqRel);
                                Some(lane.clone())
                            })
                        })
                        .ok_or_else(|| {
                            self.backpressure(&self.state.diagnostics.admission_retry_lane)
                        })?,
                )
            };
            retained.0 += new_request;
            retained.1 += bytes;
            let credit = Arc::new(Credit {
                state: self.state.clone(),
                bytes,
                requests: new_request,
            });
            (credit, lane)
        };
        if let Some(trace) = &trace {
            if let Some(lane) = &lane {
                trace.lane(
                    lane.index,
                    flight_slot.expect("new request reserved its slot"),
                );
                trace.event("admitted");
            } else {
                trace.event("coalesced_existing_flight");
            }
        }
        let sampled = new_request != 0 && self.state.diagnostics.select();
        let flight = if let Some(lane) = lane {
            let slot = flight_slot.expect("a new request reserves a flight slot");
            self.state.flight_ready[slot].begin();
            let flight = Arc::new(ReadFlight {
                node,
                references: missing.clone(),
                slot,
                completion: FlightCompletion::new(),
                io_deadline: Mutex::new(None),
                _pin: pin,
                _credit: credit.clone(),
            });
            flights.insert(&flight);
            let request = Request {
                lane: lane.index,
                references: missing,
                timeout,
                io_deadline: None,
                submitted: (profiled || sampled).then(Instant::now),
                sampled,
                trace: trace.clone(),
                flight: flight.clone(),
                _credit: credit.clone(),
                _lane_credit: LaneCredit {
                    outstanding: lane.outstanding.clone(),
                },
            };
            let sender_guard = self.sender.lock();
            let Some(sender) = sender_guard.get(node) else {
                drop(request);
                drop(sender_guard);
                self.state.flight_ready[slot].retire();
                return Err(LocalDbError::new("read reactor stopped"));
            };
            let sender = sender.clone();
            drop(sender_guard);
            let sent = sender.try_send(ReadCommand::Request(request));
            if let Err(error) = sent {
                if sampled {
                    self.state
                        .diagnostics
                        .completed
                        .fetch_add(1, Ordering::Relaxed);
                    self.state
                        .diagnostics
                        .actor_completed
                        .fetch_add(1, Ordering::Relaxed);
                    self.state
                        .diagnostics
                        .errors
                        .fetch_add(1, Ordering::Relaxed);
                }
                let (error, disconnected) = match error {
                    mpsc::TrySendError::Full(request) => {
                        drop(request);
                        (
                            self.backpressure(&self.state.diagnostics.admission_retry_lane),
                            false,
                        )
                    }
                    mpsc::TrySendError::Disconnected(request) => {
                        drop(request);
                        (LocalDbError::new("read reactor stopped unexpectedly"), true)
                    }
                };
                if disconnected {
                    self.state.poison();
                }
                self.state.flight_ready[slot].retire();
                return Err(error);
            }
            // One remote value request handed to the live lane worker. Relaxed:
            // a diagnostic count; no data is published through it.
            self.state
                .live_read_submissions
                .fetch_add(1, Ordering::Relaxed);
            Some(flight)
        } else {
            None
        };
        self.state
            .coalesced_members
            .fetch_add(coalesced_members as u64, Ordering::Relaxed);
        let parts = parts
            .into_iter()
            .map(|(existing, slot)| {
                (
                    existing.unwrap_or_else(|| flight.as_ref().expect("planned flight").clone()),
                    slot,
                )
            })
            .collect();
        drop(flights);
        Ok(ReadRequest {
            parts,
            credit,
            admitted,
            timeout,
            trace,
            subscription: None,
        })
    }
}
impl Drop for ReadService {
    fn drop(&mut self) {
        self.stop();
    }
}

// Keep a miss handle inline: boxing the larger variant would allocate for every
// miss merely to shrink an already allocation-free cache-hit handle.
#[allow(clippy::large_enum_variant)]
enum GetState {
    Ready(Option<Bytes>),
    Pending {
        read: ReadRequest,
        reference: ValueRecordReference,
        ks: KeySpace,
        key: Bytes,
    },
    Resolved {
        reference: ValueRecordReference,
        ks: KeySpace,
        key: Bytes,
    },
    Consumed,
}
/// A bounded read captured at submission. Dropping cancels delivery, not transport
/// ownership; already-observed failures still poison its originating authority.
pub struct PendingGet {
    owner: Arc<State>,
    state: GetState,
}

/// Exact immutable scan member; transport ownership is independent of Db locks.
pub struct PendingRecord {
    owner: Arc<State>,
    reference: ValueRecordReference,
    ready: Option<Mutation>,
    read: Option<ReadRequest>,
}

pub struct CompletedRecord {
    owner: Arc<State>,
    reference: ValueRecordReference,
    result: Result<Mutation, LocalDbError>,
    wait_ns: u64,
}

impl PendingRecord {
    #[allow(
        clippy::expect_used,
        reason = "an unfinished record owns its read, and the count check above leaves one member"
    )]
    pub fn wait(self) -> CompletedRecord {
        let started = Instant::now();
        let result = match self.ready {
            Some(value) => Ok(value),
            None => self
                .read
                .expect("pending record owns read")
                .finish()
                .and_then(|mut values| {
                    if values.len() != 1 {
                        return Err(LocalDbError::new("scan member count mismatch"));
                    }
                    Ok(values.pop().expect("one verified member"))
                }),
        };
        CompletedRecord {
            owner: self.owner,
            reference: self.reference,
            result,
            wait_ns: elapsed(started),
        }
    }
}

type ReadCompletion = Result<Vec<Mutation>, LocalDbError>;

/// A completed network wait, still requiring the authority's health/cache check.
pub struct CompletedGet {
    pending: PendingGet,
    result: Option<ReadCompletion>,
    wait_ns: u64,
}

impl PendingGet {
    /// Wait without holding the publication/index owner lock.
    pub fn wait(self) -> CompletedGet {
        let PendingGet { owner, state } = self;
        match state {
            GetState::Pending {
                read,
                reference,
                ks,
                key,
            } => {
                let started = Instant::now();
                let result = read.finish();
                CompletedGet {
                    pending: PendingGet {
                        owner,
                        state: GetState::Resolved { reference, ks, key },
                    },
                    result: Some(result),
                    wait_ns: elapsed(started),
                }
            }
            state => CompletedGet {
                pending: PendingGet { owner, state },
                result: None,
                wait_ns: 0,
            },
        }
    }
}
impl RemoteDatabase {
    /// Configure once before serving Sui requests. The finite budget counts
    /// actor enqueue attempts after byte/request admission, including failures.
    pub fn set_read_profile_limit(&mut self, limit: u64) {
        self.core
            .reads
            .state
            .diagnostics
            .limit
            .store(limit, Ordering::Relaxed);
        self.core
            .reads
            .state
            .diagnostics
            .remaining
            .store(limit, Ordering::Relaxed);
        self.core.reads.state.coordinator_diagnostics.arm(limit);
    }
    /// Cumulative first-N sampled RPC counters, never per-request means.
    /// Server spans nest inside exchange/service. Failures without a profiled
    /// owner reply are explicitly counted as unavailable.
    pub fn read_profile_monitor(&self) -> ReadProfileMonitor {
        ReadProfileMonitor {
            state: self.core.reads.state.clone(),
        }
    }
    pub fn live_read_submission_count(&self) -> u64 {
        self.core
            .reads
            .state
            .live_read_submissions
            .load(Ordering::Relaxed)
    }
    pub fn submit_record(
        &mut self,
        reference: ValueRecordReference,
    ) -> Result<PendingRecord, LocalDbError> {
        self.ensure_healthy()?;
        let pin = self
            .core
            .index
            .pin_cut(IndexReadCut::At(MutationVersion {
                batch: reference.batch_version,
                operation_index: reference.descriptor.operation_index,
            }))
            .map_err(LocalDbError::component)?;
        let ready = self.core.read_cache.get_scan(&reference);
        let read = if ready.is_none() {
            let node = *self
                .core
                .partition_indexes
                .get(&reference.batch_address.partition)
                .ok_or_else(|| LocalDbError::new("reference names unregistered partition"))?;
            Some(self.core.reads.submit_until(
                node,
                std::slice::from_ref(&reference),
                0,
                false,
                ReadContext {
                    pin: Some(pin),
                    trace: None,
                },
            )?)
        } else {
            None
        };
        Ok(PendingRecord {
            owner: self.core.reads.state.clone(),
            reference,
            ready,
            read,
        })
    }

    pub fn finish_record(
        &mut self,
        completed: CompletedRecord,
        ks: KeySpace,
        reduced_key: &[u8],
    ) -> Result<Mutation, LocalDbError> {
        self.core
            .value_read_ns
            .fetch_add(completed.wait_ns, Ordering::Relaxed);
        if !Arc::ptr_eq(&self.core.reads.state, &completed.owner) {
            return Err(LocalDbError::new("record belongs to another authority"));
        }
        self.ensure_healthy()?;
        let mutation = completed.result.inspect_err(|error| self.poison(error))?;
        if !matches!(mutation, Mutation::Put { .. })
            || mutation.keyspace() != ks
            || completed.reference.descriptor.keyspace != ks.0
            || mutation
                .point_key()
                .is_none_or(|key| self.core.shape.ks(ks).reduce_key(key).as_ref() != reduced_key)
        {
            let error = LocalDbError::new("scan member does not match its live index reference");
            self.poison(&error);
            return Err(error);
        }
        self.ensure_healthy()?;
        Ok(mutation)
    }

    #[allow(
        clippy::expect_used,
        reason = "the resolved state is only set with a detached completion"
    )]
    pub fn finish_ready_get(
        &mut self,
        completed: CompletedGet,
    ) -> Result<Option<Bytes>, LocalDbError> {
        self.core
            .value_read_ns
            .fetch_add(completed.wait_ns, Ordering::Relaxed);
        self.check_handle(&completed.pending)?;
        match completed.pending.state {
            GetState::Ready(value) => Ok(value),
            GetState::Resolved { reference, ks, key } => {
                let values = completed
                    .result
                    .expect("resolved read has completion")
                    .inspect_err(|error| self.poison(error))?;
                self.complete_get(reference, ks, &key, values)
            }
            _ => Err(LocalDbError::new("read is not a detached completion")),
        }
    }
    /// Submit at the current published cut without retaining a coordinator borrow.
    /// Admission errors are side-effect-free; use `read_backpressured` to drain a
    /// previous handle and retry. Cache hits allocate/copy no key or value bytes.
    pub fn submit_get(&mut self, ks: KeySpace, key: &[u8]) -> Result<PendingGet, LocalDbError> {
        self.submit_get_cut(ks, key, IndexReadCut::Latest)
    }
    pub fn read_backpressured(error: &LocalDbError) -> bool {
        error.message() == BACKPRESSURE
    }
    pub fn submit_get_cut(
        &mut self,
        ks: KeySpace,
        key: &[u8],
        cut: IndexReadCut,
    ) -> Result<PendingGet, LocalDbError> {
        self.prepare_get(ks, key, cut, None, false)
    }
    #[allow(
        clippy::expect_used,
        reason = "inline and dispatch timers are armed on exactly the branch that reads them"
    )]
    pub(super) fn prepare_get(
        &mut self,
        ks: KeySpace,
        key: &[u8],
        cut: IndexReadCut,
        mut profile: Option<&mut RemoteReadProfile>,
        inline: bool,
    ) -> Result<PendingGet, LocalDbError> {
        let mut sample = self
            .core
            .reads
            .state
            .coordinator_diagnostics
            .select(false, 1);
        self.ensure_healthy()?;
        let index_started = Instant::now();
        let (reference, pin, cached) = {
            // Resolve the cut once; the lookup and a miss's pin must see the
            // same cut. No lock or guard is held past each call.
            let cut = self.core.index.resolve_cut(cut);
            let clock = ResolutionSample::start(&sample);
            let reference = self
                .core
                .index
                .get(ks, key, cut.clone())
                .map_err(LocalDbError::component);
            if let Some(sample) = &sample {
                sample.stage(ResolutionStage::IndexLookup, clock);
            }
            let reference = match reference {
                Ok(reference) => reference,
                Err(error) => {
                    if let Some(sample) = &sample {
                        sample.member(MemberOutcome::Error);
                    }
                    return Err(error);
                }
            };
            let cached = if let Some(reference) = reference {
                let clock = ResolutionSample::start(&sample);
                let cached = self.core.read_cache.get_point(&reference, ks, key);
                if let Some(sample) = &sample {
                    sample.stage(ResolutionStage::CacheLookup, clock);
                }
                cached
            } else {
                None
            };
            let pin = if reference.is_some() && cached.is_none() {
                let clock = ResolutionSample::start(&sample);
                // The lookup is done: only the retention floor is pinned.
                let pin = self
                    .core
                    .index
                    .pin_call_floor(cut)
                    .map_err(LocalDbError::component);
                if let Some(sample) = &sample {
                    sample.stage(ResolutionStage::PinPreparation, clock);
                }
                Some(pin?)
            } else {
                None
            };
            if let Some(sample) = &sample {
                sample.member(if reference.is_none() {
                    MemberOutcome::IndexMiss
                } else if cached.is_some() {
                    MemberOutcome::CacheHit
                } else {
                    MemberOutcome::RemoteMiss
                });
            }
            (reference, pin, cached)
        };
        let index_ns = elapsed(index_started);
        self.core
            .read_resolve_ns
            .fetch_add(index_ns, Ordering::Relaxed);
        if let Some(profile) = profile.as_deref_mut() {
            profile.index_ns += index_ns;
        }
        let state = if let Some(reference) = reference {
            if let Some(value) = cached {
                if let Some(sample) = &mut sample {
                    sample.success();
                }
                drop(sample);
                if let Some(profile) = profile {
                    profile.cache_hits += 1;
                }
                GetState::Ready(Some(value))
            } else {
                let dispatch_started = profile.as_ref().map(|_| Instant::now());
                if let Some(profile) = profile.as_deref_mut() {
                    profile.cache_misses += 1;
                }
                let clock = ResolutionSample::start(&sample);
                let node = self
                    .core
                    .partition_indexes
                    .get(&reference.batch_address.partition)
                    .copied()
                    .ok_or_else(|| LocalDbError::new("reference names unregistered partition"));
                if let Some(sample) = &sample {
                    sample.stage(ResolutionStage::RoutePreparation, clock);
                }
                let node = node?;
                if let Some(sample) = &mut sample {
                    sample.success();
                }
                drop(sample);
                let inline_started = inline.then(Instant::now);
                let immediate = if inline {
                    self.core
                        .reads
                        .read_inline(node, self.core.epoch, &reference, key.len(), pin.clone())
                        .inspect_err(|error| {
                            if !admission_error(error) {
                                self.poison(error);
                            }
                        })?
                } else {
                    None
                };
                if let Some(values) = immediate {
                    let elapsed = elapsed(inline_started.expect("inline read"));
                    self.core
                        .value_read_ns
                        .fetch_add(elapsed, Ordering::Relaxed);
                    if let Some(profile) = profile {
                        profile.inline_read_ns += elapsed;
                    }
                    GetState::Ready(self.complete_get(reference, ks, key, values)?)
                } else {
                    let read = self.core.reads.submit(
                        node,
                        vec![reference],
                        key.len(),
                        profile.is_some(),
                        pin,
                    )?;
                    if let Some(profile) = profile {
                        profile.dispatch_ns += elapsed(dispatch_started.expect("dispatched read"));
                    }
                    GetState::Pending {
                        read,
                        reference,
                        ks,
                        key: key.to_vec().into(),
                    }
                }
            }
        } else {
            if let Some(sample) = &mut sample {
                sample.success();
            }
            drop(sample);
            GetState::Ready(None)
        };
        self.ensure_healthy()?;
        Ok(PendingGet {
            owner: self.core.reads.state.clone(),
            state,
        })
    }
    pub(super) fn point_value(
        ks: KeySpace,
        key: &[u8],
        mutation: Mutation,
    ) -> Result<Option<Bytes>, LocalDbError> {
        match mutation {
            Mutation::Put {
                keyspace,
                key: actual,
                value,
            } if keyspace == ks && actual.as_ref() == key => Ok(Some(value)),
            Mutation::Put { .. } => Ok(None),
            Mutation::Delete { .. } => Err(LocalDbError::new(
                "live index reference resolves to tombstone",
            )),
            Mutation::DropCells { .. } => Err(LocalDbError::new(
                "DropCells does not have a value reference",
            )),
        }
    }
    fn check_handle(&mut self, pending: &PendingGet) -> Result<(), LocalDbError> {
        if !Arc::ptr_eq(&self.core.reads.state, &pending.owner) {
            return Err(LocalDbError::new(
                "read handle belongs to another authority/epoch",
            ));
        }
        // A read-service failure not yet seen by the core is a new detection:
        // fail-stop the core once. A core-initiated failure (begin_failure
        // stores concurrent_failure before poisoning reads) is finalized by
        // its own thread; observers only return the recorded error and never
        // re-enter finish_failure, whose publisher lock that thread may hold.
        if self.core.reads.failed()
            && !self
                .core
                .concurrent_failure
                .load(std::sync::atomic::Ordering::Acquire)
        {
            self.poison(&LocalDbError::new(failure_context(
                &self.core.first_failure,
                "asynchronous read failed; recovery required",
            )));
        }
        self.ensure_healthy()
    }
    #[allow(
        clippy::expect_used,
        reason = "the count check above leaves exactly one member"
    )]
    fn complete_get(
        &mut self,
        reference: ValueRecordReference,
        ks: KeySpace,
        key: &[u8],
        mut values: Vec<Mutation>,
    ) -> Result<Option<Bytes>, LocalDbError> {
        self.ensure_healthy()?;
        if values.len() != 1 {
            let error = LocalDbError::new("point read count mismatch");
            self.poison(&error);
            return Err(error);
        }
        let mutation = values.pop().expect("one verified member");
        if matches!(mutation, Mutation::Delete { .. }) {
            let error = LocalDbError::new("live index reference resolves to tombstone");
            self.poison(&error);
            return Err(error);
        }
        let prepared = self.core.read_cache.prepare_verified(reference, &mutation);
        self.core.read_cache.insert_prepared(prepared);
        let result = Self::point_value(ks, key, mutation);
        self.ensure_healthy()?;
        result
    }
    /// `None` means pending; `Some(None)` is a completed absence. A completion
    /// consumes the handle's result exactly once and refunds retained capacity.
    pub fn poll_get(
        &mut self,
        pending: &mut PendingGet,
    ) -> Result<Option<Option<Bytes>>, LocalDbError> {
        self.check_handle(pending)?;
        let result = match &pending.state {
            GetState::Ready(_) => {
                let GetState::Ready(value) =
                    std::mem::replace(&mut pending.state, GetState::Consumed)
                else {
                    unreachable!()
                };
                return Ok(Some(value));
            }
            GetState::Consumed | GetState::Resolved { .. } => {
                return Err(LocalDbError::new("read handle already consumed"));
            }
            GetState::Pending { read, .. } => match read.try_result() {
                Ok(Some(completion)) => Ok(completion),
                // A queued read stays pending; only submitted I/O expires.
                Ok(None)
                    if read
                        .io_deadline()
                        .is_none_or(|deadline| Instant::now() < deadline) =>
                {
                    return Ok(None);
                }
                Ok(None) => {
                    return Err(read.failure("remote read deadline expired or worker stopped"));
                }
                Err(error) => Err(error),
            },
        };
        let state = std::mem::replace(&mut pending.state, GetState::Consumed);
        let GetState::Pending {
            reference,
            ks,
            key,
            read,
        } = state
        else {
            unreachable!()
        };
        self.core
            .value_read_ns
            .fetch_add(elapsed(read.admitted), Ordering::Relaxed);
        let values = result?;
        self.complete_get(reference, ks, &key, values).map(Some)
    }
    pub fn finish_get(&mut self, pending: PendingGet) -> Result<Option<Bytes>, LocalDbError> {
        self.finish_get_profiled(pending, None)
    }
    #[allow(
        clippy::expect_used,
        reason = "wait_started is set whenever a profile samples this path"
    )]
    pub(super) fn finish_get_profiled(
        &mut self,
        pending: PendingGet,
        profile: Option<&mut RemoteReadProfile>,
    ) -> Result<Option<Bytes>, LocalDbError> {
        self.check_handle(&pending)?;
        match pending.state {
            GetState::Ready(value) => Ok(value),
            GetState::Pending {
                read,
                reference,
                ks,
                key,
            } => {
                let value_started = read.admitted;
                let wait_started = profile.as_ref().map(|_| Instant::now());
                let result = read.finish();
                self.core
                    .value_read_ns
                    .fetch_add(elapsed(value_started), Ordering::Relaxed);
                let values = result.inspect_err(|error| self.poison(error))?;
                if let Some(profile) = profile {
                    profile.wait_ns += elapsed(wait_started.expect("profiled wait"));
                }
                self.complete_get(reference, ks, &key, values)
            }
            GetState::Consumed | GetState::Resolved { .. } => {
                Err(LocalDbError::new("read handle already consumed"))
            }
        }
    }
}

mod concurrent;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod live_worker_tests {
    use super::*;
    use crate::distributed::remote_db::pipeline_tests::Fixture;

    #[test]
    fn live_grant_barrier_waits_for_lane_owner() {
        let mut fixture = Fixture::new();
        fixture.config.read_lanes = Some(1);
        let db = fixture.open();
        let lanes = db.core.reads.lanes.read()[0].clone();
        let client = lanes[0].client.clone();
        let base = lanes[0].index;
        let (sender, receiver) = mpsc::sync_channel(db.core.reads.max_requests + 1);
        // Every lane is live since R3: stop the fixture's own reactor for this
        // node so that two workers never drive the same lanes.
        let (detached, _) = mpsc::sync_channel(1);
        db.core.reads.sender.lock()[0] = detached;
        let fixture_reactor = db.core.reads.thread.lock().remove(0);
        fixture_reactor.join().unwrap();
        let state = db.core.reads.state.clone();
        let epoch = db.core.reads.epoch;
        let max_requests = db.core.reads.max_requests;
        let held = client.lock();
        let worker = thread::spawn(move || {
            drive_live_reads(state, receiver, lanes, 0, base, epoch, max_requests)
        });
        let (reply, response) = mpsc::sync_channel(1);
        assert!(sender.send(ReadCommand::DrainLive(reply)).is_ok());
        assert!(matches!(
            response.recv_timeout(Duration::from_millis(20)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
        drop(held);
        assert!(
            response
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
                .is_ok()
        );
        drop(sender);
        worker.join().unwrap();
        assert!(!db.core.reads.failed());
        db.shutdown().unwrap();
    }

    #[test]
    fn retirement_pause_preserves_outstanding_read_and_can_retry() {
        let mut fixture = Fixture::new();
        fixture.config.read_cache_bytes = 0;
        fixture.config.read_lanes = Some(1);
        let db = fixture.open();
        db.write_batch(fixture.operation(0, 0), vec![super::tests::put(0, 7)])
            .unwrap();
        let reference = db
            .index_reference(KeySpace(0), &0u32.to_be_bytes(), IndexReadCut::Latest)
            .unwrap()
            .unwrap();
        let node = db.core.partition_indexes[&reference.batch_address.partition];
        let lane = db.core.reads.lane_client(node, 0);
        let held = lane.lock();
        let request = db
            .core
            .reads
            .submit(node, vec![reference], 0, false, None)
            .unwrap();
        assert!(db.core.reads.pause_idle_nvmeof(false).is_err());
        assert!(!db.core.reads.failed());
        drop(held);
        let values = request.finish().unwrap();
        assert_eq!(values, vec![super::tests::put(0, 7)]);
        drop(db.core.reads.pause_idle_nvmeof(false).unwrap());
        db.shutdown().unwrap();
    }

    #[test]
    fn missing_and_before_all_cut_do_not_send_live_payload_work() {
        let mut fixture = Fixture::new();
        fixture.config.read_lanes = Some(1);
        fixture.config.read_cache_bytes = 0;
        let original = fixture.open();
        original
            .write_batch(fixture.operation(0, 0), vec![super::tests::put(0, 7)])
            .unwrap();
        let db = ConcurrentRemoteDatabase::new(original).unwrap();
        // Every lane is live since R3: stop the fixture's own reactors before
        // driving the same lanes, so that two lane workers never share one
        // lane (an exiting fixture worker would shut down the lane's live
        // cache under the test's worker and poison the read service).
        let nodes = db.reads.lanes.read().len();
        for node in 0..nodes {
            let (detached, _) = mpsc::sync_channel(1);
            db.reads.sender.lock()[node] = detached;
        }
        let fixture_reactors: Vec<_> = db.reads.thread.lock().drain(..).collect();
        for reactor in fixture_reactors {
            reactor.join().unwrap();
        }
        assert!(!db.reads.failed());
        let mut workers = Vec::new();
        for (node, lanes) in db.reads.lanes.read().iter().enumerate() {
            let lanes = lanes.clone();
            let base = lanes[0].index;
            let (sender, receiver) = mpsc::sync_channel(db.reads.max_requests + 1);
            let state = db.reads.state.clone();
            let epoch = db.reads.epoch;
            let max_requests = db.reads.max_requests;
            workers.push(thread::spawn(move || {
                drive_live_reads(state, receiver, lanes, node, base, epoch, max_requests)
            }));
            db.reads.sender.lock()[node] = sender;
        }
        // Every live lane client is configured since R3, so an accidentally
        // submitted payload read would succeed: count submissions instead.
        let keys = [0u32.to_be_bytes(), 2u32.to_be_bytes(), 0u32.to_be_bytes()];
        let before = db
            .get_many_cut(KeySpace(0), &keys, IndexReadCut::BeforeAll)
            .unwrap();
        assert!(before.into_iter().all(|result| matches!(result, Ok(None))));
        let missing = db
            .get_many_cut(KeySpace(0), &[2u32.to_be_bytes()], IndexReadCut::Latest)
            .unwrap();
        assert!(missing.into_iter().all(|result| matches!(result, Ok(None))));
        assert!(!db.reads.failed());
        assert_eq!(db.live_read_submission_count(), 0);
        assert_eq!(*db.reads.state.retained.lock(), (0, 0));
        db.reads.sender.lock().clear();
        for worker in workers {
            worker.join().unwrap();
        }
        db.shutdown().unwrap();
    }

    /// At least five references on one owner, each in its own flight.
    fn one_owner_references(
        fixture: &Fixture,
        db: &RemoteDatabase,
    ) -> (usize, Vec<ValueRecordReference>) {
        // A batch holds at most four operations; two batches put eight members
        // on one owner.
        for batch in 0u32..2 {
            let writes: Vec<_> = (batch * 4..batch * 4 + 4)
                .map(|key| super::tests::put(key, 7))
                .collect();
            db.write_batch(fixture.operation(0, u64::from(batch)), writes)
                .unwrap();
        }
        let references: Vec<_> = (0u32..8)
            .map(|key| {
                db.index_reference(KeySpace(0), &key.to_be_bytes(), IndexReadCut::Latest)
                    .unwrap()
                    .unwrap()
            })
            .collect();
        let node = db.core.partition_indexes[&references[0].batch_address.partition];
        let same_owner: Vec<_> = references
            .into_iter()
            .filter(|reference| {
                db.core.partition_indexes[&reference.batch_address.partition] == node
            })
            .take(6)
            .collect();
        assert!(same_owner.len() >= 5, "need distinct flights on one lane");
        (node, same_owner)
    }

    /// A live read actor for `node` whose single lane has a two-entry inbox.
    /// Its worker parks at the unlocked top of its loop, after its first idle
    /// receive and before it takes a lane mutex or drains the inbox, until
    /// `resume` is sent. A parked worker receives nothing.
    struct PausedLiveActor {
        sender: mpsc::SyncSender<ReadCommand>,
        parked: mpsc::Receiver<()>,
        resume: mpsc::SyncSender<()>,
        client: Arc<Mutex<Client>>,
        actor: JoinHandle<()>,
    }

    fn paused_live_actor(db: &RemoteDatabase, node: usize) -> PausedLiveActor {
        let lanes = db.core.reads.lanes.read()[node].clone();
        let base = lanes[0].index;
        let client = lanes[0].client.clone();
        let (sender, receiver) = mpsc::sync_channel(db.core.reads.max_requests + 1);
        db.core.reads.sender.lock()[node] = sender.clone();
        // Replacing the sender stops the fixture's own reactor for this node.
        // Join it so it cannot take the worker pause hook installed below.
        let fixture_reactor = db.core.reads.thread.lock().remove(node);
        fixture_reactor.join().unwrap();
        let state = db.core.reads.state.clone();
        let epoch = db.core.reads.epoch;
        let (entered, parked) = mpsc::sync_channel(1);
        let (resume, resumed) = mpsc::sync_channel(1);
        *db.core.reads.state.reactor_pause[node].lock() = Some((entered, resumed));
        let actor =
            thread::spawn(move || drive_live_reads(state, receiver, lanes, node, base, epoch, 2));
        PausedLiveActor {
            sender,
            parked,
            resume,
            client,
            actor,
        }
    }

    fn submit_one(
        db: &RemoteDatabase,
        node: usize,
        reference: &ValueRecordReference,
    ) -> ReadRequest {
        db.core
            .reads
            .submit(node, vec![*reference], 0, false, None)
            .unwrap()
    }

    /// Wait until the actor takes a wake sent now. The actor takes commands in
    /// order and retries its overflow queue at the top of each loop, so after
    /// this returns it has handled every earlier command.
    fn actor_takes_wake(db: &RemoteDatabase, node: usize, sender: &mpsc::SyncSender<ReadCommand>) {
        db.core.reads.state.wake_pending[node].store(true, Ordering::Release);
        assert!(sender.try_send(ReadCommand::Wake).is_ok());
        let deadline = Instant::now() + db.core.config.timeout;
        while db.core.reads.state.wake_pending[node].load(Ordering::Acquire) {
            assert!(
                Instant::now() < deadline,
                "the actor blocked on a full lane"
            );
            thread::yield_now();
        }
    }

    #[test]
    fn full_lane_inbox_never_blocks_the_actor() {
        let mut fixture = Fixture::new();
        fixture.config.read_lanes = Some(1);
        fixture.config.read_cache_bytes = 0;
        let db = fixture.open();
        let (node, same_owner) = one_owner_references(&fixture, &db);
        // Once the worker is parked the inbox depth counter equals the inbox
        // occupancy. While the worker is still taking requests the counter
        // may run up to two above the inbox capacity: see `LiveLane`.
        let paused = paused_live_actor(&db, node);
        let mut pending = vec![submit_one(&db, node, &same_owner[0])];
        // The worker parks after taking the first request, or after its idle
        // wait if that request arrives later; it takes nothing while parked.
        paused.parked.recv_timeout(db.core.config.timeout).unwrap();
        pending.extend(
            same_owner[1..]
                .iter()
                .map(|reference| submit_one(&db, node, reference)),
        );
        // The actor has offered every request above, and found the lane full,
        // before it takes this wake: a full lane does not block the actor.
        actor_takes_wake(&db, node, &paused.sender);
        let diagnostics = &db.core.reads.state.diagnostics;
        assert!(diagnostics.live_lane_full.load(Ordering::Relaxed) >= 1);
        // The inbox held exactly its capacity. The parked worker holds at
        // most the first request, so the actor queued every other request
        // beyond the two inbox entries.
        assert_eq!(diagnostics.live_lane_depth_max.load(Ordering::Relaxed), 2);
        assert!(
            diagnostics.live_lane_pending_max.load(Ordering::Relaxed)
                >= same_owner.len() as u64 - 3
        );
        paused.resume.send(()).unwrap();
        // Every live client is configured since R3: each queued request
        // completes with its one member; none may be lost.
        for request in pending {
            assert_eq!(request.finish().unwrap().len(), 1);
        }
        assert_eq!(*db.core.reads.state.retained.lock(), (0, 0));
        drop(paused.sender);
        db.core.reads.sender.lock().clear();
        paused.actor.join().unwrap();
        drop(db);
    }

    #[test]
    fn poisoned_live_worker_fails_inbox_requests_promptly() {
        let mut fixture = Fixture::new();
        fixture.config.read_lanes = Some(1);
        fixture.config.read_cache_bytes = 0;
        let db = fixture.open();
        let (node, same_owner) = one_owner_references(&fixture, &db);
        let paused = paused_live_actor(&db, node);
        // With nothing submitted the worker parks after its idle wait, holding
        // no request; the inbox is empty.
        paused.parked.recv_timeout(db.core.config.timeout).unwrap();
        let mut pending: VecDeque<_> = same_owner
            .iter()
            .map(|reference| submit_one(&db, node, reference))
            .collect();
        // Requests 0 and 1 fill the inbox; the actor queues the rest.
        actor_takes_wake(&db, node, &paused.sender);
        db.core.reads.state.poison();
        // Resumed, the worker drains the inbox, failing 0 and 1 as poisoned,
        // then blocks on the held client mutex without receiving.
        let held = paused.client.lock();
        paused.resume.send(()).unwrap();
        for _ in 0..2 {
            assert!(pending.pop_front().unwrap().finish().is_err());
        }
        // The first wake may be taken by a loop that retried the overflow
        // queue before that drain; the loop taking the second one retried it
        // after, so the inbox again holds two requests the worker never takes.
        actor_takes_wake(&db, node, &paused.sender);
        actor_takes_wake(&db, node, &paused.sender);
        let started = Instant::now();
        // The worker now sees the poison and exits with a full inbox.
        drop(held);
        for request in pending {
            let error = request
                .finish()
                .expect_err("a poisoned live read must fail");
            assert!(
                error.to_string().contains("poisoned")
                    || error.to_string().contains("live read worker stopped"),
                "{error}"
            );
        }
        // A request left in the inbox would only fail at its deadline, one
        // configured timeout after submission.
        assert!(started.elapsed() < db.core.config.timeout / 2);
        assert_eq!(*db.core.reads.state.retained.lock(), (0, 0));
        drop(paused.sender);
        db.core.reads.sender.lock().clear();
        paused.actor.join().unwrap();
        drop(db);
    }
}
